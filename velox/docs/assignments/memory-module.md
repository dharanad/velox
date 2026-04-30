# Assignment: Velox Memory Module

Build a minimal but realistic memory subsystem from scratch, replicating the
key design ideas from Velox's `velox/common/memory/` module. Each exercise
introduces one concept, builds on the previous one, and ends with a test you
can run to verify correctness.

**Reference document:** `velox/docs/designs/memory-module.md`
**Reference source:** `velox/common/memory/`

Work in a scratch directory outside the Velox tree so you don't accidentally
break the build. Each exercise is a self-contained `.cpp` file that compiles
with:

```bash
g++ -std=c++20 -O0 -g <file>.cpp -o <file> && ./<file>
```

---

## Exercise 1 — Flat Byte Allocator

**Goal:** Understand the simplest possible memory allocator with capacity
enforcement.

**What to build:** A `FlatAllocator` class that:
- Takes a `capacity` in bytes at construction.
- Exposes `void* allocate(size_t bytes)` that returns aligned memory and
  throws `std::bad_alloc` if capacity would be exceeded.
- Exposes `void free(void* ptr, size_t bytes)` that frees memory and credits
  the capacity back.
- Tracks `allocatedBytes()` with an atomic counter.

**Constraints:**
- Use `std::malloc` / `std::free` for the actual allocation.
- `allocate` must be thread-safe. Use `std::atomic<int64_t>` for the counter
  and `fetch_add` + a rollback on failure (no mutex needed).

**Test to pass:**
```cpp
FlatAllocator a(1024);
void* p1 = a.allocate(512);
assert(a.allocatedBytes() == 512);
void* p2 = a.allocate(512);
assert(a.allocatedBytes() == 1024);
try {
    a.allocate(1);        // must throw — capacity exhausted
    assert(false);
} catch (const std::bad_alloc&) {}
a.free(p1, 512);
assert(a.allocatedBytes() == 512);
void* p3 = a.allocate(512);  // must succeed now
assert(p3 != nullptr);
```

**Questions to answer (in a comment at the top of your file):**
1. Why does `MallocAllocator` use `fetch_add` + rollback instead of a mutex?
2. What happens if two threads both see capacity as available but together they
   exceed it? How does the atomic rollback prevent this?

---

## Exercise 2 — Sharded Counter Optimization

**Goal:** Understand why `MallocAllocator` uses sharded counters for small
allocations.

**What to build:** Extend `FlatAllocator` with a `ShardedFlatAllocator` that:
- Splits capacity tracking into N shards, each with its own `std::mutex` and a
  local `reserved` bucket.
- For allocations `< reservationLimit`, deduct from the local shard's bucket.
  If the bucket is empty, grab `reservationLimit` bytes from the global atomic
  counter first, then fill the bucket.
- For allocations `>= reservationLimit`, hit the global counter directly
  (same as Exercise 1).
- When freeing small allocations, return bytes to the shard's bucket. If the
  bucket exceeds `2 * reservationLimit`, return `reservationLimit` bytes back
  to the global counter.

**Parameters to make configurable:**
- `numShards` (default 8, one per CPU)
- `reservationLimit` (default 1 MB, same as `MallocAllocator`)

**Test to pass:**
```cpp
ShardedFlatAllocator a(/*capacity=*/256 << 20, /*reservationLimit=*/1 << 20);
// Hammer from 8 threads, each doing 10k small allocations of 1KB
std::vector<std::thread> threads;
std::atomic<int> failures{0};
for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&] {
        for (int i = 0; i < 10'000; ++i) {
            void* p = a.allocate(1024);
            if (!p) { ++failures; return; }
            a.free(p, 1024);
        }
    });
}
for (auto& t : threads) t.join();
assert(failures == 0);
assert(a.allocatedBytes() == 0);  // all memory returned
```

**Questions to answer:**
1. How does the shard index get chosen per thread? (Hint: `thread_local` or
   `std::this_thread::get_id() % numShards`.)
2. Why is the threshold `2 * reservationLimit` for releasing back to global,
   not just `reservationLimit`?

---

## Exercise 3 — Memory Pool with Quantized Reservation

**Goal:** Understand the quantized reservation protocol that separates
`MemoryPool` from `MemoryAllocator`.

**What to build:** A `MemoryPool` class that:
- Wraps a `FlatAllocator` (from Exercise 1).
- Tracks `reservedBytes_` and `usedBytes_` separately.
- Implements `void* allocate(size_t bytes)`:
  - If `usedBytes_ + bytes <= reservedBytes_`, just increment `usedBytes_`.
  - Otherwise, compute the quantized delta needed, reserve it from the parent
    (or the allocator directly if root), then increment `usedBytes_`.
- Implements `void free(void* ptr, size_t bytes)`:
  - Decrement `usedBytes_`.
  - If `reservedBytes_ - usedBytes_ >= quantizedSize(usedBytes_)`, release a
    quantum back.
- Uses this quantization function (from `MemoryPool.h`):
  ```cpp
  static uint64_t quantizedSize(uint64_t size) {
      constexpr uint64_t kMB = 1 << 20;
      if (size < 16 * kMB) return roundUp(size, kMB);
      if (size < 64 * kMB) return roundUp(size, 4 * kMB);
      return roundUp(size, 8 * kMB);
  }
  ```

**Test to pass:**
```cpp
FlatAllocator allocator(64 << 20);  // 64 MB
MemoryPool pool(&allocator);

// 10 small allocations should cause at most ceil(10 * 100KB / 1MB) = 1 reservation
std::vector<void*> ptrs;
for (int i = 0; i < 10; ++i)
    ptrs.push_back(pool.allocate(100 * 1024));  // 100 KB each

assert(pool.reservedBytes() == 1 << 20);  // exactly 1 MB reserved
assert(pool.usedBytes() == 10 * 100 * 1024);

for (auto p : ptrs)
    pool.free(p, 100 * 1024);

assert(pool.usedBytes() == 0);
// reservation may have been released back — allocator should reflect that
```

**Questions to answer:**
1. Why does `MemoryPool` have both `capacity_` and `maxCapacity_` in Velox?
   When do they differ?
2. What would happen (in terms of mutex contention) if every `allocate` call
   went straight to the root pool with no quantization?

---

## Exercise 4 — Pool Tree

**Goal:** Understand how the pool hierarchy propagates reservations up the tree.

**What to build:** Extend `MemoryPool` to support a parent-child tree:
- `MemoryPool::addChild(name)` creates and returns a child `MemoryPool`.
- Children hold a `shared_ptr` to their parent; parents track children as
  `weak_ptr`s.
- When a child needs to increment its reservation, it calls
  `parent_->incrementReservation(delta)`, which checks parent's capacity and
  propagates up the tree recursively.
- Only the root pool talks to `FlatAllocator`.
- `MemoryPool::usedBytes()` on a non-leaf returns the sum of all descendants'
  `usedBytes_`.

**Test to pass:**
```cpp
FlatAllocator allocator(64 << 20);
auto root  = std::make_shared<MemoryPool>(&allocator, "root", 64 << 20);
auto task  = root->addChild("task");
auto op1   = task->addChild("op1");
auto op2   = task->addChild("op2");

void* p1 = op1->allocate(3 << 20);   // 3 MB
void* p2 = op2->allocate(5 << 20);   // 5 MB

assert(root->usedBytes() == 8 << 20);   // 8 MB total
assert(task->usedBytes() == 8 << 20);
assert(op1->usedBytes()  == 3 << 20);

// Enforce capacity: root is capped at 64 MB
try {
    op1->allocate(60 << 20);  // 60 MB would exceed root's 64 MB cap
    assert(false);
} catch (...) {}

op1->free(p1, 3 << 20);
op2->free(p2, 5 << 20);
assert(root->usedBytes() == 0);
```

**Questions to answer:**
1. Why does the parent hold `weak_ptr`s to children, not `shared_ptr`s?
2. Explain in one paragraph how a leaf pool's `free()` propagates the released
   reservation up to the root.

---

## Exercise 5 — Size Classes and Non-Contiguous Allocation

**Goal:** Understand how `MmapAllocator` uses size classes to serve large
page-granular allocations without fragmentation.

**What to build:** A `SizeClassAllocator` that:
- Defines size classes of 1, 2, 4, 8, 16, 32 pages (4KB pages, same as Velox).
- For a request of N pages, uses `allocationSize(N)` to split it into a mix
  of size class runs (greedy from largest class down).
- Allocates each run with `mmap(MAP_ANONYMOUS | MAP_PRIVATE)` and stores the
  runs in a `std::vector<PageRun>` struct returned to the caller.
- `freeNonContiguous(PageRunList&)` unmaps each run with `munmap`.
- Tracks `numAllocatedPages_` and `numAllocatedRuns_`.

**Helper struct to implement:**
```cpp
struct PageRun { void* address; size_t numPages; };
struct PageRunList { std::vector<PageRun> runs; };

// Returns how many pages of each size class to use for numPages total.
SizeMix allocationSize(size_t numPages);
```

**Test to pass:**
```cpp
SizeClassAllocator a;
PageRunList out = a.allocate(11);  // 11 pages = 8 + 2 + 1
assert(out.runs.size() == 3);
assert(totalPages(out) == 11);

// Each run should be page-aligned
for (auto& r : out.runs)
    assert(reinterpret_cast<uintptr_t>(r.address) % 4096 == 0);

a.free(out);
assert(a.numAllocatedPages() == 0);
```

**Questions to answer:**
1. Why does Velox prefer non-contiguous allocation for hash tables and IO
   buffers instead of one large `malloc`?
2. What is `madvise(MADV_DONTNEED)` and when does `MmapAllocator` use it?
   (Hint: see the `adviseAway()` discussion in the design doc.)

---

## Exercise 6 — Simple Memory Arbitrator

**Goal:** Understand how the arbitrator mediates capacity among pools when the
system is under memory pressure.

**What to build:** A `SimpleArbitrator` that:
- Owns a global capacity budget of `totalCapacity` bytes.
- On `addPool(pool, initialCapacity)`: grants `initialCapacity` to the pool
  from the budget. Throws if budget is exhausted.
- On `growCapacity(pool, requestBytes)`:
  1. If free budget >= `requestBytes`, grant it.
  2. Otherwise, iterate all other pools and call `pool->shrink(target)` on
     the one with the most free (reserved but unused) bytes until enough is
     freed. Grant the freed bytes to the requesting pool.
  3. If still not enough, throw `std::runtime_error("out of memory")`.
- On `removePool(pool)`: reclaim the pool's remaining capacity back to budget.

**Test to pass:**
```cpp
SimpleArbitrator arb(/*totalCapacity=*/32 << 20);  // 32 MB

auto pool1 = std::make_shared<MemoryPool>(/*cap=*/16 << 20);
auto pool2 = std::make_shared<MemoryPool>(/*cap=*/16 << 20);
arb.addPool(pool1);
arb.addPool(pool2);

void* p = pool1->allocate(14 << 20);   // pool1 uses 14 MB

// pool2 wants 20 MB — budget is exhausted, must reclaim from pool1's free cap
// pool1 has 2 MB free (16 MB cap - 14 MB used), not enough
// arbitrator must fail
try {
    arb.growCapacity(pool2.get(), 20 << 20);
    assert(false);
} catch (const std::runtime_error&) {}

pool1->free(p, 14 << 20);
arb.removePool(pool1);

// Now pool2 should be able to get 28 MB (budget freed from pool1)
arb.growCapacity(pool2.get(), 12 << 20);  // grow pool2 by 12 MB (to 28 MB)
assert(pool2->capacity() == 28 << 20);
```

**Questions to answer:**
1. What is the difference between local and global arbitration in
   `SharedArbitrator`? When does each kick in?
2. Why does the `SharedArbitrator` background global arbitration run on a
   separate thread rather than the requesting thread itself?

---

## Capstone — Mini Memory System

**Goal:** Wire all the pieces together into a cohesive system.

**What to build:** Combine Exercises 1–6 into a single `MiniMemorySystem` with:
- A `ShardedFlatAllocator` as the physical backend.
- A 3-level pool tree: `root → task → operator`.
- A `SimpleArbitrator` managing capacity across multiple root pools.
- A `Buffer` class (simplified `AlignedBuffer`) that:
  - Allocates from a `MemoryPool` using `pool->allocate(capacity + 8)`.
  - Writes a guard word (`0xdeadbeef`) at `data + capacity` on construction.
  - Checks the guard word on `free()` and throws if it has been overwritten.
  - Tracks reference count with `std::atomic<int>` (like `BufferPtr`).

**Scenario to implement and pass:**

1. Create a `MiniMemorySystem` with 128 MB total capacity.
2. Create two queries (root pools), each with 64 MB initial capacity.
3. Query 1 allocates a 50 MB `Buffer` through its operator pool.
4. Query 2 tries to allocate a 30 MB `Buffer` — this should trigger arbitration,
   reclaim free capacity from Query 1 (has 14 MB free), fail to get 30 MB, and
   throw.
5. Query 1 frees its buffer. Query 2 tries again and succeeds.
6. Verify the guard word is intact on free.
7. Verify that after both queries finish, `MiniMemorySystem::totalUsedBytes()`
   returns 0.

---

## Learning Checklist

After completing all exercises, you should be able to explain:

- [ ] Why Velox has three allocation modes (bytes, non-contiguous, contiguous) and when each is used.
- [ ] How quantized reservation reduces root pool contention without sacrificing accounting accuracy.
- [ ] Why `MmapAllocator` bounds RSS strictly while `MallocAllocator` does not.
- [ ] The difference between `capacity_` and `maxCapacity_` on a `MemoryPool` and how the arbitrator manipulates each.
- [ ] How `MemoryReclaimer::enterArbitration()` prevents deadlock during reclaim.
- [ ] Why `AlignedBuffer` stores its header and data in a single pool allocation.
- [ ] What `madvise(MADV_DONTNEED)` does and why `MmapAllocator` calls it on free pages.

## Tips

- Read the corresponding source file before starting each exercise.
  The comments in the headers are detailed and will answer most design questions.
- For exercises 3–4, print the reservation state after every operation to build
  intuition before adding assertions.
- For the capstone, sketch the object graph on paper before writing code.
  The ownership relationships (who holds `shared_ptr` vs `weak_ptr` vs raw
  pointer) are the hardest part to get right.
