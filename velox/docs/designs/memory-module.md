# Velox Memory Module

## Overview

The Velox memory module (`velox/common/memory/`) is a multi-layered system
responsible for memory allocation, usage tracking, capacity enforcement, and
memory reclamation under pressure. It sits beneath every data structure in
Velox that touches heap memory — vectors, hash tables, IO buffers, caches —
and gives the query system visibility and control over all of it.

The module has four distinct layers, each with its own role:

```
┌──────────────────────────────────────────────────────────────────┐
│                        MemoryManager                             │  process-wide singleton
├───────────────────────────┬──────────────────────────────────────┤
│         MemoryPool tree   │        MemoryArbitrator               │  logical accounting & enforcement
├───────────────────────────┴──────────────────────────────────────┤
│                        MemoryAllocator                           │  physical page management
│              MallocAllocator │ MmapAllocator                     │
└──────────────────────────────────────────────────────────────────┘
```

---

## Layer 1: MemoryManager

`MemoryManager` (`Memory.h`) is the process-wide entry point. There is exactly
one instance per process, created with `initializeMemoryManager()` and
retrieved via `memoryManager()`.

### Responsibilities

- Creates and owns the `MemoryAllocator` (either `MallocAllocator` or
  `MmapAllocator`, chosen via `Options::useMmapAllocator`).
- Creates and owns the `MemoryArbitrator` (optional; `SharedArbitrator` is the
  production implementation).
- Manages the lifecycle of root memory pools. Pools are stored as weak pointers
  in `pools_` so that the manager does not keep them alive.
- Provides process-wide shared leaf pools for spilling (`spillPool_`), SSD
  caching (`cachePool_`), and query tracing (`tracePool_`).

### Key Configuration (`Options`)

| Field | Default | Meaning |
|---|---|---|
| `allocatorCapacity` | unlimited | Hard cap on bytes the allocator can hand out |
| `useMmapAllocator` | false | Use `MmapAllocator` instead of `MallocAllocator` |
| `arbitratorKind` | (empty) | Which arbitrator to use; empty disables arbitration |
| `arbitratorCapacity` | unlimited | Budget available to query pools (can be less than `allocatorCapacity` to reserve space for caches) |
| `allocationSizeThresholdWithReservation` | 1MB | Below this size, `MallocAllocator` uses sharded counters |

### Creating Pools

```
manager->addRootPool("query-1", 8 * 1024 * 1024 * 1024)  // 8 GB root pool
    ->addAggregateChild("task")
    ->addLeafChild("operator.HashAggregation");
```

`addRootPool` creates a `MemoryPoolImpl` with no parent and registers it with
the arbitrator. `addLeafPool` is a convenience that creates a leaf directly
under the manager's internal system root.

---

## Layer 2: MemoryPool

`MemoryPool` (`MemoryPool.h`) is the primary interface used by query operators.
Pools form a tree that mirrors the query execution plan.

### Pool Tree Structure

```
query pool (root, kAggregate)           ← QueryCtx
  └── task pool (kAggregate)            ← Task
        ├── node pool (kAggregate)      ← PlanNode
        │     └── operator pool (kLeaf) ← Operator  ← actual allocations happen here
        └── node pool (kAggregate)
              └── operator pool (kLeaf)
```

Only **leaf** (`kLeaf`) pools perform actual allocations. **Aggregate**
(`kAggregate`) pools exist purely to aggregate usage up the tree for capacity
enforcement. Users cannot call `allocate()` on an aggregate pool.

Each child holds a `shared_ptr` to its parent, ensuring the parent outlives all
its children. The parent holds `weak_ptr`s to its children in `children_`,
protected by `poolMutex_`.

### Memory Reservation Protocol

Velox uses a **quantized reservation** scheme to avoid hammering the root pool
on every small allocation. When a leaf pool needs more memory than it has
reserved, it does not ask for exactly what it needs. Instead it asks for a
rounded-up quantum:

```cpp
static uint64_t quantizedSize(uint64_t size) {
    if (size < 16 * kMB)  return roundUp(size, kMB);      // 1MB granularity
    if (size < 64 * kMB)  return roundUp(size, 4 * kMB);  // 4MB granularity
    return roundUp(size, 8 * kMB);                         // 8MB granularity
}
```

The leaf pool keeps a local `reservationBytes_` bucket. An allocation only
propagates up to the root when `usedReservationBytes_` exceeds
`reservationBytes_`. This dramatically reduces contention on the root pool's
mutex under concurrent execution.

### Thread Safety

Thread safety is optional and pool-level:

- **Thread-safe leaf pool** (`threadSafe = true`): Uses `mutex_` to serialize
  `reserve()` calls. Used by operators with multiple driver threads.
- **Non-thread-safe leaf pool** (`threadSafe = false`): Skips the mutex.
  Used for single-threaded expression evaluation to minimize CPU overhead.

Aggregate pools always update their `reservationBytes_` atomically without a
mutex, relying on the leaf pools to serialize concurrent updates.

### Key Pool State

| Field | Meaning |
|---|---|
| `capacity_` | Current capacity granted by the arbitrator (can grow/shrink) |
| `maxCapacity_` | Immutable hard limit set at creation |
| `reservationBytes_` | Bytes reserved (but not necessarily used) from the parent |
| `usedReservationBytes_` | Bytes actually handed to the user |
| `minReservationBytes_` | Floor below which reservation will not be released (set by `maybeReserve()`) |
| `peakBytes_` | High watermark for diagnostics |

### Memory Accounting Flow

```
allocate(size) on leaf pool
  └── reserve(size)
        ├── if availableReservation() >= size → just increment usedReservationBytes_
        └── else incrementReservation(quantizedDelta)
              └── toImpl(parent_)->incrementReservationThreadSafe(this, delta)
                    └── ... propagates up to root pool
                          └── if exceeds capacity_ → call arbitrator->growCapacity()
```

Conversely, `free(size)` decrements `usedReservationBytes_`. When the
reservation bucket has enough slack it releases quanta back up the tree via
`decrementReservation()`.

### Debug Mode

When enabled (via `DebugOptions`), `MemoryPoolImpl` records every allocation's
call stack in `debugAllocRecords_`. On pool destruction, any unreleased entries
indicate memory leaks. On a capacity-exceeded exception the full allocation
tree is annotated with call stacks of all outstanding allocations.

---

## Layer 3: MemoryAllocator

`MemoryAllocator` (`MemoryAllocator.h`) is the physical memory backend. Pools
delegate actual OS-level allocation to it. There are two implementations.

### Allocation Modes

All allocators support three allocation modes:

| Mode | API | Use case |
|---|---|---|
| **Bytes** | `allocateBytes(bytes, alignment)` | Small arbitrary-size allocations (Buffer, hash table buckets) |
| **Non-contiguous** | `allocateNonContiguous(numPages, out)` | Large allocations that do not need one contiguous range. Returns an `Allocation` of `PageRun`s |
| **Contiguous** | `allocateContiguous(numPages, out)` | Large allocations that must be contiguous (e.g., sort buffers, large joins). Returns a `ContiguousAllocation` |

Non-contiguous mode fills the `Allocation` with a list of `PageRun`s, each a
pointer to a range of consecutive machine pages. To get 11 pages, the allocator
might return runs of 8 + 2 + 1 pages from different size classes.

### Size Classes

Both allocators use a shared set of size classes (in pages):

```
1, 2, 4, 8, 16, 32, 64, 128, 256   (powers of two, up to 256 * 4KB = 1MB)
```

`allocationSize()` in the base class computes the optimal mix of size classes to
cover a request with minimal waste.

### Cache Integration

A `Cache` can register itself with the allocator via `registerCache()`. If an
allocation fails due to capacity, the allocator calls `cache->makeSpace()`,
which can evict cached data to free pages, then retries. This is the primary
mechanism by which SSD cache and query memory compete for the same byte budget.

---

## MallocAllocator

`MallocAllocator` is the default backend. It delegates every allocation to
`std::malloc` / `std::free`.

### Capacity Enforcement

`MallocAllocator` maintains an atomic `allocatedBytes_` counter and checks it
before every allocation. For allocations below `reservationByteLimit_` (default
1MB), it uses `ConcurrentCounter<uint32_t>` — a sharded counter with
thread-local reservations — to reduce atomic contention:

```
Small alloc (< 1MB):
  reservations_.update(bytes, reserveFunc_)
    └── if local shard has enough → deduct locally, no global atomic
    └── else reserve 1MB from global allocatedBytes_, fill shard

Large alloc (≥ 1MB):
  allocatedBytes_.fetch_add(bytes)
  if > capacity_ → fetch_sub(bytes), return false
```

For contiguous allocations `MallocAllocator` can use either `mmap`/`munmap` or
`malloc` directly, controlled by `mallocContiguousEnabled`.

---

## MmapAllocator

`MmapAllocator` manages its entire capacity as a single upfront `mmap` per size
class. It is designed for long-running services like Prestissimo where repeated
`malloc`/`free` cycles cause RSS fragmentation.

### Architecture

At construction, each size class is mmapped for the full `capacity`. No physical
pages are committed yet — the mapping is virtual. Physical pages are only
backed when an allocation is first touched.

Each `SizeClass` maintains three per-page bitmaps:
- `pageAllocated_` — set when a page is given to a caller
- `pageMapped_` — set when the OS has committed physical memory for the page
- `mappedFreeLookup_` — coarse lookup bitmap: set if a 512-page group has any
  mapped-free pages (avoids scanning all of `pageMapped_` on every allocation)

### Allocation Strategy

When a new allocation needs pages that are not yet backed:

1. `ensureEnoughMappedPages()` checks if `numMapped_ + newPages > capacity_`.
2. If yes, `adviseAway()` scans size classes for free (allocated=0) mapped pages
   and calls `madvise(MADV_DONTNEED)` on them, reducing `numMapped_`.
3. Only after enough pages have been freed does the actual allocation proceed.

This means physical memory is always bounded by `capacity_`, but virtual
address space is pre-committed. The OS only charges RSS for pages in the
`mapped & allocated` state.

### Small Allocation Delegation

Allocations below `maxMallocBytes_` (default 3KB) are delegated to `malloc`.
A separate `mallocReservedBytes_` quota controls how much of the total capacity
can be used for these small mallocs.

### Large Allocation (Beyond Size Classes)

Allocations larger than the biggest size class (default 256 pages = 1MB) use
`ManagedMmapArenas` instead of calling `mmap` for each request. `MmapArena`
manages a large pre-allocated virtual address region and hands out slices,
amortizing the cost of `mmap` system calls.

---

## Layer 4: Memory Arbitration

When a pool exceeds its `capacity_` while still below `maxCapacity_`, it does
not immediately fail. Instead it asks the `MemoryArbitrator` to grow its
capacity. The arbitrator mediates memory among all running queries.

### MemoryArbitrator Interface

```
growCapacity(pool, requestBytes)    ← pool needs more memory
shrinkCapacity(pool, targetBytes)   ← voluntarily return unused capacity
shrinkCapacity(targetBytes, ...)    ← global shrink under system pressure
addPool / removePool                ← lifecycle hooks
```

If no arbitrator kind is configured, a no-op arbitrator is used that simply
grants unlimited capacity to every pool.

### SharedArbitrator

`SharedArbitrator` is the production implementation. It manages a global
capacity budget shared among all root pools.

#### Local Arbitration

When a pool's reservation exceeds its capacity:
1. The pool calls `arbitrator->growCapacity(pool, requestBytes)`.
2. The arbitrator first tries to shrink the pool's own free capacity (it may
   have capacity it granted before but isn't fully using).
3. If the pool needs more than its `maxCapacity_`, a local reclaim runs —
   the pool's own `MemoryReclaimer` is invoked to spill to disk.

#### Global Arbitration

If local arbitration cannot satisfy the request:
1. The requesting pool waits on a `VeloxPromise`.
2. A background thread runs **global arbitration**: iterates all registered pools
   (sorted by reclaimable bytes), spilling from the most over-using pools until
   enough capacity is freed.
3. If spilling cannot free enough memory, younger/larger queries are aborted.
4. The waiting pool's promise is fulfilled and it retries.

The default timeout for global arbitration is 5 minutes (`kDefaultMaxMemoryArbitrationTime`).

#### Key Config (`SharedArbitrator::ExtraConfig`)

| Config key | Default | Meaning |
|---|---|---|
| `memory-pool-initial-capacity` | 256MB | Capacity granted on pool creation |
| `memory-pool-reserved-capacity` | 0 | Minimum capacity reserved per pool |
| `reserved-capacity` | 0 | Capacity always kept aside for existing pools |
| `max-memory-arbitration-time` | 5m | Max wait for arbitration before failure |

### MemoryReclaimer

`MemoryReclaimer` is the per-pool hook for the arbitrator to reclaim memory.
The default implementation recursively reclaims from the child pool with the
most reclaimable bytes. Custom reclaimers are attached to operator pools and
implement spilling (writing in-memory hash tables or sort buffers to disk).

Reclaimers also implement `enterArbitration()` / `leaveArbitration()` which
driver threads call to put themselves in a suspended state. This prevents
deadlock when the arbitrator needs to pause the task that owns the requesting
pool.

Priority ordering (`priority()`) allows the arbitrator to prefer certain pools
for reclamation (lower number = reclaimed first).

---

## Allocation Object Types

### `Allocation` (non-contiguous)

A list of `PageRun` objects. Each `PageRun` packs a 48-bit pointer and a 16-bit
page count into a single `uint64_t`. The total pages may be spread across
multiple non-contiguous ranges from different size classes. Used by the
`HashStringAllocator` (for hash tables) and similar structures.

### `ContiguousAllocation`

A single contiguous virtual address range from one `mmap` call (or
`MmapArena`). Supports `growContiguous()` to extend the declared-used range up
to a pre-allocated `maxPages` limit without remapping — enabling huge page
support for large ranges.

---

## Buffer and Pool Integration

`AlignedBuffer` (`buffer/Buffer.h`) allocates its memory through a `MemoryPool`:

```cpp
void* memory = pool->allocate(preferredSize);
auto* buffer = new (memory) AlignedBuffer{pool, preferredSize - kPaddedSize};
```

The buffer header and data live in one contiguous pool allocation. When the
`AlignedBuffer`'s ref count drops to zero, `freeToPool()` calls
`pool_->free(this, capacity + kPaddedSize)`, returning exactly the bytes that
were allocated. This allows the pool to accurately track memory down to the
individual buffer level.

---

## STL Integration

`StlAllocator<T>` is a standard-conforming allocator backed by a `MemoryPool`.
It enables STL containers (`std::vector`, `std::unordered_map`, etc.) to
allocate from a tracked pool:

```cpp
std::vector<int32_t, StlAllocator<int32_t>> tracked(pool);
```

---

## Design Summary

| Design choice | Rationale |
|---|---|
| Quantized reservation | Reduces contention on root pool mutex under high concurrency |
| Separate `capacity_` and `maxCapacity_` | Allows arbitrator to dynamically lend/reclaim capacity without changing the hard per-query limit |
| Size classes + bitmaps in MmapAllocator | Prevents fragmentation in long-running processes; OS RSS is strictly bounded |
| Cache integration in allocator | Allows cache and query memory to share one byte budget with dynamic balancing |
| Non-thread-safe leaf pools | Avoids mutex overhead for single-driver expression evaluation |
| Background global arbitration | Decouples the requesting query from reclaim latency; multiple queries can reclaim in parallel via the thread pool |
| MemoryReclaimer hierarchy | Each level (operator, node, task) can add its own reclaim and suspend logic, enabling deadlock-free pause-and-reclaim |
