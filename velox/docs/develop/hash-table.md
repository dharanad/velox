# Hash Table

The Velox hash table is the core data structure behind hash joins and hash
aggregations. It is designed for high-throughput vectorized workloads and
adapts its internal layout at runtime based on key cardinality. This document
covers its design, memory layout, key operations, operator integration, and
performance optimizations.

## Source Files

| File | Purpose |
|------|---------|
| `velox/exec/HashTable.h` | Class definitions, constants, interfaces |
| `velox/exec/HashTable.cpp` | All probe, insert, and rehash logic |
| `velox/exec/VectorHasher.h/.cpp` | Per-column hash computation |
| `velox/exec/RowContainer.h/.cpp` | Key and payload row storage |
| `velox/exec/HashBuild.h/.cpp` | Build-side join operator |
| `velox/exec/HashProbe.h/.cpp` | Probe-side join operator |
| `velox/exec/HashAggregation.h/.cpp` | Aggregation operator |
| `velox/exec/GroupingSet.h/.cpp` | Grouping context used by aggregation |

---

## Class Hierarchy

```
BaseHashTable          (abstract, velox/exec/HashTable.h:121)
└── HashTable<ignoreNullKeys>   (template, velox/exec/HashTable.h:537)
        ├── HashTable<true>     specialization — join build (nulls ignored)
        └── HashTable<false>    specialization — aggregation (nulls grouped)
```

`BaseHashTable` exposes the pure-virtual interface used by operators:
`groupProbe`, `joinProbe`, `prepareForGroupProbe`, `prepareForJoinProbe`,
`listJoinResults`, and `prepareJoinTable`.

`HashTable<ignoreNullKeys>` holds all state and implements every method. The
template parameter controls whether null keys are silently discarded
(join build, where `NULL = NULL` is false) or kept as their own group
(aggregation, where `GROUP BY NULL` is valid).

---

## Hash Modes

The hash table operates in one of three modes. The mode is chosen once enough
data has been observed and may be upgraded as more rows arrive.

### kArray

Used when every key value maps to a small non-negative integer in the range
`[0, 2M)`.

```
table_[value_id] → row pointer
```

The table is a flat array of pointers. Probe is a single array dereference.
This mode is ideal for low-cardinality columns such as booleans, small enums,
or short integer ranges.

**Constant**: `kArrayHashMaxSize = 2L << 20` (roughly 2 million entries, 16 MB).

### kNormalizedKey

Used when all key columns together fit within 59 bits of combined value-space.
`VectorHasher` assigns compact integer IDs to each distinct value and packs
them into a single 64-bit "normalized key" stored immediately below each row
in `RowContainer`.

During a probe, the normalized key is compared directly (`RowContainer::normalizedKey(row) == lookup.normalizedKeys[i]`),
avoiding individual column-by-column comparisons entirely.

### kHash

Full open-addressing hash table with a bucket structure (described below). This
is the fallback mode for high-cardinality or string keys.

**Constant**: `kHashTableLoadFactor = 0.7` — the table is rehashed when
occupancy exceeds 70%.

---

## Memory Layout (kHash Mode)

The table is an array of fixed-size **buckets**, each holding 16 slots:

```
Bucket (128 bytes = 2 cache lines)
┌──────────────────────────────────────────┐
│  Tags      [16 bytes]  — one byte/slot   │
│  Pointers  [96 bytes]  — 6 bytes/slot    │
│  Padding   [16 bytes]                    │
└──────────────────────────────────────────┘
```

**Tag** (1 byte per slot): the top 7 bits of the hash value, with bit 7 forced
to 1 (`hashTag(hash) = (hash >> 38) | 0x80`). A zero byte means the slot is
empty; `0x7F` is a tombstone.

**Pointer** (6 bytes per slot): the lower 48 bits of the row address extracted
with `kPointerMask`. The upper 16 bits of a 64-bit word carry flags used by
`RowContainer` (probed flag, count, etc.).

Keeping 16 slots in one 128-byte bucket means a SIMD register can compare all
16 tags simultaneously. If no matching tag is found in the current bucket, the
probe moves to the next bucket (linear probing at bucket granularity).

---

## Row Storage: RowContainer

All actual key data and aggregation state lives in a `RowContainer`
(`velox/exec/RowContainer.h`), not in the bucket array itself. The hash table
holds only 6-byte pointers into the `RowContainer`.

For `kNormalizedKey` mode, 8 bytes are reserved immediately before each row
for the normalized key digest. For join builds, an additional `nextOffset_`
pointer per row chains duplicate keys.

---

## Hash Computation: VectorHasher

`VectorHasher` (`velox/exec/VectorHasher.h`) processes one key column at a
time. It operates in three sub-modes that mirror the table modes:

- **kHash** — full 64-bit folly hash.
- **kArray** — assigns each distinct value a compact integer ID starting at 1.
- **kNormalizedKey** — same as kArray but the IDs from all columns are packed
  into a single 64-bit digest.

`prepareForGroupProbe` calls `computeValueIds()` (which may extend the ID
mapping with newly seen values) while `prepareForJoinProbe` calls the
read-only `lookupValueIds()`.

---

## Key Operations

### prepareForGroupProbe

```
BaseHashTable::prepareForGroupProbe(lookup, input, rows, spillBit)
```

1. Decodes each key column using its `VectorHasher`.
2. Removes rows with null keys if `ignoreNullKeys = true`.
3. Builds combined hashes / normalized keys in `lookup.hashes`.
4. Triggers a hash-mode upgrade and rehash if the current mode can no longer
   represent the new values.

### groupProbe

Dispatches to the appropriate mode-specific implementation:

- **kArray** → `arrayGroupProbe`: direct indexed insertion.
- **kNormalizedKey** → `groupNormalizedKeyProbe`: compare normalized key digest.
- **kHash** → `fullProbe<false>`: full bucket scan.

All paths process rows **4 at a time** to overlap memory latency across
independent rows:

```cpp
// Pseudocode for 4-at-a-time probe
for (; i + 4 <= numProbes; i += 4) {
  state[0..3].preProbe(table, hash[row], row);   // prefetch bucket
  state[0..3].firstProbe(table, 0);              // load tags via SIMD
  fullProbe(lookup, state[0..3], false);          // insert or return hit
}
```

Hits (existing groups) are written to `lookup.hits[row]`. Newly allocated
groups are collected in `lookup.newGroups`.

### joinProbe

The read-only equivalent of `groupProbe` for the probe side of a join.
`lookup.hits[row]` is set to the first matching build-side row, or `nullptr`
for a miss. Callers then iterate additional matches via `listJoinResults`.

### listJoinResults

Iterates all build-side rows matching a probe key by following the
`nextOffset_` chain installed during `insertForJoin`. Returns up to a
caller-specified number of output rows per call, enabling output batching.

### insertForJoin / insertForGroupBy

Both methods insert a batch of rows into the hash table after a bulk hash
computation. `insertForJoin` installs the `nextOffset_` chain for duplicate
keys; `insertForGroupBy` does not, because each key is unique in aggregation.

Both use software prefetching 10 rows ahead:

```cpp
__builtin_prefetch(reinterpret_cast<uint8_t*>(table_) + futureOffset);
```

### Rehashing

The table rehashes when:

```
numDistinct + numNewRows > capacity * kHashTableLoadFactor
```

The new capacity is the next power of two above the current capacity.
All rows are re-read from `RowContainer`, their hashes are recomputed, and
they are re-inserted into the newly allocated bucket array.

---

## HashLookup: The Probe Carrier

`HashLookup` (`velox/exec/HashTable.h:57`) is the single struct that carries
all inputs and outputs for `groupProbe` and `joinProbe`. Every operator that
touches the hash table allocates one `HashLookup` and reuses it for the
lifetime of the operator, calling `reset(size)` before each new input batch
rather than allocating a fresh object.

### Definition

```cpp
// velox/exec/HashTable.h:57
struct HashLookup {
  HashLookup(
      const std::vector<std::unique_ptr<VectorHasher>>& h,
      memory::MemoryPool* pool)
      : hashers(h),
        rows(raw_vector<vector_size_t>(pool)),
        hashes(raw_vector<uint64_t>(pool)),
        hits(raw_vector<char*>(pool)),
        normalizedKeys(raw_vector<uint64_t>(pool)) {}

  void reset(vector_size_t size) {
    rows.resize(size);
    hashes.resize(size);
    hits.resize(size);
    newGroups.clear();
  }

  /// One entry per group-by or join key.
  const std::vector<std::unique_ptr<VectorHasher>>& hashers;

  /// Scratch memory used to call VectorHasher::lookupValueIds.
  VectorHasher::ScratchMemory scratchMemory;

  // ── Inputs (written by prepareForGroupProbe / prepareForJoinProbe) ────────

  /// Set of row numbers to probe.
  raw_vector<vector_size_t> rows;

  /// Hash values or value IDs for each row. Not aligned with 'rows'.
  /// Indexed by row number.
  raw_vector<uint64_t> hashes;

  // ── Outputs (written by groupProbe / joinProbe) ───────────────────────────

  /// Per-row result pointer; indexed by row number.
  /// groupProbe: pointer to the matching (existing or new) RowContainer row.
  /// joinProbe:  pointer to the first matching build-side row, or nullptr.
  raw_vector<char*> hits;

  /// Row numbers for which a new entry was inserted by groupProbe.
  /// Always empty after joinProbe.
  std::vector<vector_size_t> newGroups;

  /// Concatenated value IDs used in kNormalizedKey mode. 1:1 with 'hashes'.
  /// Populated by groupProbe and joinProbe.
  raw_vector<uint64_t> normalizedKeys;
};
```

### Fields

| Field | Type | Direction | Description |
|-------|------|-----------|-------------|
| `hashers` | `const vector<unique_ptr<VectorHasher>>&` | Reference | One entry per key column. Defines the key schema and performs per-column hash/ID computation. Bound at construction; never changes. |
| `scratchMemory` | `VectorHasher::ScratchMemory` | Temp | Temporary buffer for `VectorHasher::lookupValueIds` in `prepareForJoinProbe`. Reused across calls. |
| `rows` | `raw_vector<vector_size_t>` | Input | Compacted list of row indices to probe. Written by `prepareForGroupProbe` / `prepareForJoinProbe` via `populateLookupRows`. |
| `hashes` | `raw_vector<uint64_t>` | Input | Full 64-bit hash values (`kHash` mode) or packed value IDs (`kArray` / `kNormalizedKey` mode). Indexed by **row number**, not by position within `rows`. Written by the same prepare step. |
| `hits` | `raw_vector<char*>` | Output | Per-row result pointer, also indexed by row number. See post-probe semantics below. |
| `newGroups` | `vector<vector_size_t>` | Output | Row indices where `groupProbe` created a new group. Always empty after `joinProbe`. Used by aggregate functions to zero-initialize new accumulators. |
| `normalizedKeys` | `raw_vector<uint64_t>` | Output | Packed value IDs written by `populateNormalizedKeys` during `kNormalizedKey` probes. 1:1 with `hashes`. Used by `fullProbe` to compare keys without touching individual columns. |

### Indexing Convention

`rows`, `hashes`, and `hits` are all indexed by the **input row number**, not
by position within `rows`. This mirrors the `SelectivityVector` convention used
throughout Velox: filtered-out rows simply have `hits[row] == nullptr` without
any array compaction.

For example, if a batch has 1 024 rows and rows 5, 200, 300 are active:

```
rows   = [5, 200, 300]         ← only active row numbers
hashes = [?, ?, ?, ?, ?, h5, ..., h200, ..., h300, ...]   ← indexed by row
hits   = [?, ?, ?, ?, ?, r5, ..., r200, ..., r300, ...]   ← indexed by row
```

### Post-Probe Semantics

After `groupProbe`:

- `hits[row]` points to the existing or newly created group row in
  `RowContainer`. Every active row in `rows` has a non-null hit.
- `newGroups` contains the row indices of every group that was just created so
  aggregate functions can call `initializeNewGroups` on them.

After `joinProbe`:

- `hits[row]` points to the **first** matching build-side row, or `nullptr`
  for a miss.
- Additional matches are retrieved by following the `nextOffset_` chain via
  `listJoinResults`.

### Lifecycle

```
Operator::initialize()
  └─ lookup_ = make_unique<HashLookup>(hashers_, pool())
       // 'hashers_' is the operator's per-column VectorHasher vector.
       // All raw_vectors are pool-allocated once.

Per input batch:
  ├─ prepareForGroupProbe(lookup, input, activeRows, spillBit)
  │    ├─ lookup.reset(input.size())          // resize all vectors, clear newGroups
  │    ├─ hasher.decode(*key, activeRows)     // decode each key column
  │    ├─ hasher.computeValueIds() or hash()  // fill lookup.hashes[row]
  │    └─ populateLookupRows(activeRows, lookup.rows)  // compact active indices
  │
  ├─ groupProbe(lookup, spillBit)   OR   joinProbe(lookup)
  │    // Reads lookup.rows and lookup.hashes[row]
  │    // Writes lookup.hits[row] and (for groupProbe) lookup.newGroups
  │
  └─ Read results:
       for groupProbe: for each row in newGroups → initializeNewGroups(lookup.hits, newGroups)
                       for each active row       → addRawInput(lookup.hits, activeRows, inputs)
       for joinProbe:  resultIter.reset(lookup)
                       listJoinResults(resultIter, ...) → iterate hits + nextOffset_ chains
```

### Callers

| Operator | File | Usage |
|----------|------|-------|
| `HashProbe` | `velox/exec/HashProbe.cpp:155` | Allocates one `HashLookup`; calls `prepareForJoinProbe` + `joinProbe` per probe batch. |
| `HashBuild` | `velox/exec/HashBuild.cpp` | Calls `prepareForGroupProbe` + `groupProbe` per build batch to insert rows. |
| `GroupingSet` | `velox/exec/GroupingSet.cpp` | Calls `prepareForGroupProbe` + `groupProbe`; reads `newGroups` to initialize new accumulators. |
| `RowNumber` | `velox/exec/RowNumber.cpp:85` | Calls `prepareForGroupProbe` + `groupProbe`; reads `newGroups` to zero-initialize per-partition row counts. |
| `TopNRowNumber` | `velox/exec/TopNRowNumber.cpp` | Same pattern as `RowNumber`. |

### Code Examples

**HashProbe allocating and using a lookup (join):**

```cpp
// velox/exec/HashProbe.cpp:155
lookup_ = std::make_unique<HashLookup>(hashers_, pool());

// per probe batch:
table_->prepareForJoinProbe(*lookup_, input_, activeRows_, /*decodeNulls=*/false);
if (!lookup_->rows.empty()) {
  table_->joinProbe(*lookup_);
}
resultIter_->reset(*lookup_);
// listJoinResults() then iterates lookup_->hits following nextOffset_ chains
```

**RowNumber using newGroups to initialize state (group probe):**

```cpp
// velox/exec/RowNumber.cpp:85
SelectivityVector rows(numInput);
table_->prepareForGroupProbe(*lookup_, input, rows, kNoSpillInputStartPartitionBit);
table_->groupProbe(*lookup_, kNoSpillInputStartPartitionBit);
// Initialize count to 0 for every brand-new partition.
for (auto i : lookup_->newGroups) {
  setNumRows(lookup_->hits[i], 0);
}
```

**GroupingSet driving aggregate functions:**

```cpp
// GroupingSet::addInputForActiveRows (simplified)
table_->prepareForGroupProbe(lookup_, batch, activeRows, spillBit);
table_->groupProbe(lookup_, spillBit);

// Zero-initialize accumulators for groups seen for the first time.
for (auto& aggregate : aggregates_) {
  aggregate->initializeNewGroups(lookup_.hits.data(), lookup_.newGroups);
}
// Update all accumulators (existing and new) with the current batch.
for (auto& aggregate : aggregates_) {
  aggregate->addRawInput(lookup_.hits.data(), activeRows, inputs);
}
```

---

## ProbeState: The Per-Row SIMD State Machine

`ProbeState` (`velox/exec/HashTable.cpp:89`) is the core per-row state machine
that drives every probe and insert into the hash table. It encapsulates all
transient state for one in-flight lookup: which bucket it is examining, the
SIMD tag comparison result, and the row pointer of the current candidate match.

Callers allocate several `ProbeState` instances on the stack — four for most
probe paths, 64 for normalized-key probes — and interleave their three phases
so that independent probes overlap each other's cache-miss latency.

### SIMD Types

`ProbeState` uses types defined in `BaseHashTable` (`velox/exec/HashTable.h:123`):

```cpp
// SSE2 (x86-64):
using TagVector = xsimd::batch<uint8_t, xsimd::sse2>;  // 16 × uint8_t

// NEON (ARM):
using TagVector = xsimd::batch<uint8_t, xsimd::neon>;  // 16 × uint8_t

using MaskType = uint16_t;  // 1 bit per slot
```

Both platforms expose the same C++ interface through xsimd. The `TagVector`
holds exactly 16 bytes — one per slot in a bucket — so a single register
instruction loads or compares all tags at once.

### Operation Enum

```cpp
// velox/exec/HashTable.cpp:91
enum class Operation { kProbe, kInsert, kErase };
```

The `Operation` template parameter changes the behavior of `fullProbe`:

| Value | Used by | Behavior on empty slot | Behavior on match |
|-------|---------|------------------------|-------------------|
| `kProbe` | `joinProbe` | Return `nullptr` (miss) | Return row pointer |
| `kInsert` | `groupProbe` build | Allocate new row via `insert` lambda | Return existing row pointer |
| `kErase` | Spill eviction | `VELOX_FAIL` (key must exist) | Call `eraseHit`, return row |

### Constants

| Constant | Value | Meaning |
|----------|-------|---------|
| `kEmptyTag` | `0x00` | Slot is free; probe stops here |
| `kTombstoneTag` | `0x7f` | Slot was erased; probe skips, insert may reuse |
| `kFullMask` | `0xffff` | 16-bit mask covering all slots in a bucket |
| `kNotSet` | `0xff` | Sentinel for `indexInTags_` — no tombstone seen yet |

### Member Fields

| Field | Type | Description |
|-------|------|-------------|
| `group_` | `char*` | Row pointer of the current candidate match; set by `loadNextHit`. Non-null after `firstProbe` if the first bucket had a tag match. |
| `wantedTags_` | `TagVector` | The target tag byte broadcast 16 times into a SIMD register. Computed once in `preProbe` and reused across all bucket comparisons. |
| `tagsInTable_` | `TagVector` | The 16 tags loaded from the current bucket. Reloaded each time the probe advances to a new bucket. |
| `row_` | `int32_t` | The input row index being probed. Used to index `lookup.hashes`, `lookup.normalizedKeys`, and `lookup.hits`. |
| `bucketOffset_` | `int64_t` | Byte offset of the current bucket within `table_`. Advances by `kBucketSize` (128 bytes) each time the probe moves to the next bucket. |
| `hits_` | `MaskType` (`uint16_t`) | Bitmask with one bit per slot. A set bit means that slot's tag matches `wantedTags_`. Bits are cleared one at a time by `loadNextHit`. |
| `indexInTags_` | `uint8_t` | Dual-purpose: for `kErase`, the slot index of the current hit; for `kInsert`, the slot index of the first tombstone encountered (used to prefer tombstone reuse over empty-slot insertion). |

### Definition

```cpp
// velox/exec/HashTable.cpp:89
class ProbeState {
 public:
  enum class Operation { kProbe, kInsert, kErase };

  static constexpr uint8_t kTombstoneTag = 0x7f;
  static constexpr uint8_t kEmptyTag     = 0x00;
  static constexpr int32_t kFullMask     = 0xffff;

  int32_t row() const { return row_; }

  template <typename Table>
  inline void preProbe(const Table& table, uint64_t hash, int32_t row);

  template <Operation op = Operation::kProbe, typename Table>
  inline void firstProbe(const Table& table, int32_t firstKey);

  template <Operation op, typename Compare, typename Insert, typename Table>
  inline char* fullProbe(Table& table, int32_t firstKey,
                         Compare compare, Insert insert,
                         int64_t& numTombstones, bool extraCheck,
                         TableInsertPartitionInfo* partitionInfo = nullptr);

  template <typename Table>
  FOLLY_ALWAYS_INLINE char* joinNormalizedKeyFullProbe(
      const Table& table, const uint64_t* keys);

 private:
  static constexpr uint8_t kNotSet = 0xff;

  template <Operation op, typename Table>
  inline void loadNextHit(Table& table, int32_t firstKey);

  template <typename Table>
  void eraseHit(Table& table, int64_t& numTombstones);

  char*                      group_;
  BaseHashTable::TagVector   wantedTags_;
  BaseHashTable::TagVector   tagsInTable_;
  int32_t                    row_;
  int64_t                    bucketOffset_;
  BaseHashTable::MaskType    hits_;
  uint8_t                    indexInTags_ = kNotSet;
};
```

### Phase 1 — preProbe (`HashTable.cpp:111`)

Initializes the state for one probe row: records the row index, computes the
starting bucket, broadcasts the tag byte into a SIMD register, and issues a
non-blocking prefetch so the bucket arrives in L1 cache before `firstProbe`
runs.

```cpp
template <typename Table>
inline void preProbe(const Table& table, uint64_t hash, int32_t row) {
  row_          = row;
  bucketOffset_ = table.bucketOffset(hash);        // low bits → bucket index
  const auto tag = BaseHashTable::hashTag(hash);   // 7 high bits | 0x80
  wantedTags_   = BaseHashTable::TagVector::broadcast(tag);  // 1 SIMD op
  group_        = nullptr;
  indexInTags_  = kNotSet;
  __builtin_prefetch(
      reinterpret_cast<uint8_t*>(table.table_) + bucketOffset_);
}
```

`hashTag(hash)` takes bits [38:45] of the hash and forces bit 7 to 1 so that
the tag is never `0x00` (which is reserved for empty slots) or `0x7f`
(tombstone). `bucketOffset(hash)` masks the low bits and aligns to 128-byte
bucket boundaries.

### Phase 2 — firstProbe (`HashTable.cpp:125`)

Executes two SIMD instructions: one load of 16 bytes (all tags in the
prefetched bucket) and one element-wise comparison against `wantedTags_`,
yielding a 16-bit bitmask. If any bit is set, the private `loadNextHit` helper
extracts the lowest set bit, fetches the corresponding row pointer, clears that
bit, and prefetches the row's key data.

```cpp
template <Operation op = Operation::kProbe, typename Table>
inline void firstProbe(const Table& table, int32_t firstKey) {
  tagsInTable_ = BaseHashTable::loadTags(
      reinterpret_cast<uint8_t*>(table.table_), bucketOffset_);
  table.incrementTagLoads();
  hits_ = simd::toBitMask(tagsInTable_ == wantedTags_);
  if (hits_) {
    loadNextHit<op>(table, firstKey);   // group_ ← first candidate row
  }
}
```

The `firstKey` parameter is the byte offset within the row where the first key
column lives (or the normalized-key offset `-sizeof(normalized_key_t)` in
`kNormalizedKey` mode). The prefetch warms that memory so `fullProbe`'s key
comparison is fast.

### Private: loadNextHit (`HashTable.cpp:270`)

Extracts the index of the lowest set bit from `hits_` using
`bits::getAndClearLastSetBit`, then fetches the row pointer from the bucket and
prefetches the row's key data.

```cpp
template <Operation op, typename Table>
inline void loadNextHit(Table& table, int32_t firstKey) {
  const int32_t hit = bits::getAndClearLastSetBit(hits_);  // __builtin_ctz + mask
  if (op == Operation::kErase) {
    indexInTags_ = hit;    // remember slot for eraseHit
  }
  group_ = table.row(bucketOffset_, hit);    // decode 6-byte pointer
  __builtin_prefetch(group_ + firstKey);     // warm the key bytes
  table.incrementRowLoads();
}
```

`bits::getAndClearLastSetBit(hits_)` uses `__builtin_ctz` to find the trailing
zero count (= slot index) and then clears that bit with `hits_ &= hits_ - 1`.

### Phase 3 — fullProbe (`HashTable.cpp:136`)

Resolves the probe to a final answer. It first tries the candidate row
already loaded by `firstProbe`. On a miss it enters a loop that walks
buckets until a definitive outcome (match, empty slot, or table exhaustion):

```
1. Early-exit: if group_ != nullptr and compare(group_, row_) → return group_
2. If extraCheck: reload tags from current bucket (avoid stale data from
   parallel inserts in the same batch)
3. For each bucket starting at bucketOffset_:
   a. While hits_ > 0:
        loadNextHit → group_ = next candidate
        if compare(group_, row_) → return group_  [match]
   b. Scan tagsInTable_ for empty slots (kEmptyTag = 0x00):
        kProbe:  return nullptr  [miss]
        kInsert: insert at tombstone (if seen) or at this empty slot
        kErase:  VELOX_FAIL — key must exist
   c. kInsert: scan for tombstones (kTombstoneTag = 0x7f);
        record first tombstone in insertBucketOffset + indexInTags_
   d. bucketOffset_ = nextBucketOffset(bucketOffset_)  [linear probe]
      Reload tags and recompute hits_ for next bucket
4. If all buckets scanned without result: VELOX_FAIL
```

The `partitionInfo` parameter is non-null only during parallel join build:
if the probe wanders outside the thread's assigned bucket range, it calls
the `insert` lambda with the overflow bucket offset so the owning thread can
handle it later.

### Tombstone-Aware Insert

When `op == kInsert`, `fullProbe` prefers to reuse a tombstone slot over
inserting at the first empty slot:

```
Pass 1: scan for a matching key (to avoid duplicate inserts)
         remember first tombstone seen → (insertBucketOffset, indexInTags_)
Pass 2: reach empty slot →
         if tombstone was seen: insert at tombstone (decrements numTombstones_)
         else: insert at this empty slot
```

This keeps tombstone density from growing unboundedly, since every insert
that passes a tombstone reclaims it.

### joinNormalizedKeyFullProbe (`HashTable.cpp:232`)

An optimized variant for join probes when the table is in `kNormalizedKey`
mode. Instead of calling the general `compare` lambda (which dereferences
each key column via `RowContainer`), it reads the 8-byte normalized key
stored immediately before each row and compares it with a single 64-bit
equality check:

```cpp
template <typename Table>
FOLLY_ALWAYS_INLINE char* joinNormalizedKeyFullProbe(
    const Table& table,
    const uint64_t* keys) {
  // Fast path: check the candidate already loaded by firstProbe
  if (group_ && RowContainer::normalizedKey(group_) == keys[row_]) {
    table.incrementHits();
    return group_;
  }
  // Slow path: walk buckets
  const auto kEmptyGroup = BaseHashTable::TagVector::broadcast(kEmptyTag);
  while (numProbedBuckets < table.numBuckets()) {
    if (!hits_) {
      if (simd::toBitMask(tagsInTable_ == kEmptyGroup)) return nullptr;
      // advance to next bucket ...
    } else {
      loadNextHit<Operation::kProbe>(table, -sizeof(normalized_key_t));
      if (RowContainer::normalizedKey(group_) == keys[row_]) {
        table.incrementHits();
        return group_;
      }
    }
  }
  VELOX_FAIL("Have looped through all the buckets in table");
}
```

`RowContainer::normalizedKey(group_)` reads the 8 bytes at `group_ - 8`
(below the row header), which is cache-hot because `firstProbe` prefetched
at that offset.

### Private: eraseHit (`HashTable.cpp:282`)

Called from `fullProbe<kErase>` when a match is found. It overwrites the
matched slot's tag: if the current bucket has any empty slots the tag is
cleared to `kEmptyTag` (so the probe chain is not extended unnecessarily);
otherwise it is set to `kTombstoneTag` (so probes over this range still
skip this slot):

```cpp
template <typename Table>
void eraseHit(Table& table, int64_t& numTombstones) {
  const bool hasEmpty =
      simd::toBitMask(tagsInTable_ == TagVector::broadcast(kEmptyTag)) != 0;
  table.bucketAt(bucketOffset_)
      ->setTag(indexInTags_, hasEmpty ? kEmptyTag : kTombstoneTag);
  numTombstones += !hasEmpty;
}
```

### HashTable::fullProbe Wrapper (`HashTable.cpp:398`)

`HashTable` has its own `fullProbe` wrapper that wires the `compare` and
`insert` lambdas before delegating to `ProbeState::fullProbe`. The `isJoin`
template parameter selects `kProbe` vs `kInsert`, and `isNormalizedKey`
switches the compare lambda:

```cpp
// velox/exec/HashTable.cpp:398
template <bool isJoin, bool isNormalizedKey = false>
FOLLY_ALWAYS_INLINE void HashTable<ignoreNullKeys>::fullProbe(
    HashLookup& lookup,
    ProbeState& state,
    bool extraCheck) {
  constexpr ProbeState::Operation op =
      isJoin ? ProbeState::Operation::kProbe : ProbeState::Operation::kInsert;

  lookup.hits[state.row()] = state.fullProbe<op>(
      *this,
      isNormalizedKey ? -int32_t(sizeof(normalized_key_t)) : 0,
      // compare lambda:
      [&](char* group, int32_t row) {
        if constexpr (isNormalizedKey) {
          return RowContainer::normalizedKey(group) == lookup.normalizedKeys[row];
        } else {
          return compareKeys(group, lookup, row);  // column-by-column compare
        }
      },
      // insert lambda:
      [&](int32_t row, uint64_t index) {
        return isJoin ? nullptr : insertEntry(lookup, index, row);
      },
      numTombstones_,
      !isJoin && extraCheck);
}
```

### 4-at-a-Time Batching (kHash and kNormalizedKey modes)

In `groupProbe` and `joinProbe`, four `ProbeState` instances are interleaved
so that each phase of one probe overlaps with the memory latency of another.
All four `preProbe` calls are issued first (four prefetches in flight), then
all four `firstProbe` calls (four SIMD loads, likely cache-warm by now), then
all four `fullProbe` calls:

```cpp
// velox/exec/HashTable.cpp:488 — groupProbe (kHash mode)
ProbeState state1, state2, state3, state4;
for (; probeIndex + 4 <= numProbes; probeIndex += 4) {
  // Issue four prefetches
  state1.preProbe(*this, lookup.hashes[rows[probeIndex + 0]], rows[probeIndex + 0]);
  state2.preProbe(*this, lookup.hashes[rows[probeIndex + 1]], rows[probeIndex + 1]);
  state3.preProbe(*this, lookup.hashes[rows[probeIndex + 2]], rows[probeIndex + 2]);
  state4.preProbe(*this, lookup.hashes[rows[probeIndex + 3]], rows[probeIndex + 3]);

  // Load 16 tags per bucket (now cache-warm); kInsert because groupProbe inserts
  state1.firstProbe<ProbeState::Operation::kInsert>(*this, 0);
  state2.firstProbe<ProbeState::Operation::kInsert>(*this, 0);
  state3.firstProbe<ProbeState::Operation::kInsert>(*this, 0);
  state4.firstProbe<ProbeState::Operation::kInsert>(*this, 0);

  // Resolve each probe; state1 uses extraCheck=false (first in group),
  // state2–4 use extraCheck=true to reload stale tags that state1 may have
  // modified (a new insert changes a tag in the same bucket)
  fullProbe<false>(lookup, state1, false);
  fullProbe<false>(lookup, state2, true);
  fullProbe<false>(lookup, state3, true);
  fullProbe<false>(lookup, state4, true);
}
// Tail: remaining rows handled one at a time
```

The `kNormalizedKey` group probe (`groupNormalizedKeyProbe`, `HashTable.cpp:524`)
follows the same 4-at-a-time pattern but passes `kKeyOffset =
-sizeof(normalized_key_t)` as the `firstKey` offset so `loadNextHit` prefetches
the normalized key word instead of the first key column.

**Why `extraCheck=true` for states 2–4?**  
All four states share the same physical bucket array. If `state1.fullProbe`
inserts a new entry into a bucket, it writes a new tag byte. States 2–4 in the
same batch may probe overlapping buckets with stale tags from `firstProbe`.
`extraCheck=true` forces a fresh tag reload at the start of `fullProbe` to
guarantee correctness.

**Why four states?**  
A typical L2/L3 cache miss takes 40–200 CPU cycles. The four independent
prefetches overlap the latency of all four concurrent misses. Fewer states
would leave pipeline slots idle; more would exceed register pressure and
spill to stack.

### 64-at-a-Time Batching (kNormalizedKey join probe)

`joinNormalizedKeyProbe` (`HashTable.cpp:698`) uses 64 `ProbeState` instances
because `joinNormalizedKeyFullProbe` is so fast (one 64-bit comparison per
candidate) that the bottleneck shifts entirely to memory latency. 64 prefetches
in flight cover a full L3 round-trip:

```cpp
// velox/exec/HashTable.cpp:700
constexpr int32_t kPrefetchSize = 64;
ProbeState states[kPrefetchSize];

for (; probeIndex + kPrefetchSize <= numProbes; probeIndex += kPrefetchSize) {
  // 64 prefetches in one burst
  for (int32_t i = 0; i < kPrefetchSize; ++i) {
    int32_t row = rows[probeIndex + i];
    states[i].preProbe(*this, hashes[row], row);
  }
  // 64 SIMD tag loads
  for (int32_t i = 0; i < kPrefetchSize; ++i) {
    states[i].firstProbe(*this, kKeyOffset);
  }
  // 64 normalized-key comparisons (each is a single 64-bit ==)
  for (int32_t i = 0; i < kPrefetchSize; ++i) {
    hits[states[i].row()] = states[i].joinNormalizedKeyFullProbe(*this, keys);
  }
}
```

The same 64-state pattern is used in `insertForJoin` during parallel join
build (`HashTable.cpp:1470`).

### Probe Flow Summary

```
preProbe(hash, row)
  ├─ bucketOffset_ = hash & bucketOffsetMask_      (low bits)
  ├─ wantedTags_   = broadcast(hashTag(hash))      (1 SIMD op)
  └─ prefetch(table_ + bucketOffset_)              (non-blocking)

firstProbe(firstKey)
  ├─ tagsInTable_  = load16Tags(bucketOffset_)     (1 SIMD load)
  ├─ hits_         = toBitMask(tags == wantedTags_) (1 SIMD cmp)
  └─ if hits_: loadNextHit → group_, prefetch key  (1 bit extract + prefetch)

fullProbe<op>(compare, insert)
  ├─ if group_ && compare(group_, row_): return group_   [fast path]
  └─ loop over buckets:
       ├─ while hits_: loadNextHit → compare → match?
       ├─ empty found:
       │    kProbe  → return nullptr
       │    kInsert → insert at tombstone or empty
       │    kErase  → VELOX_FAIL
       ├─ kInsert: track first tombstone for reuse
       └─ advance: bucketOffset_ += 128; reload tags
```

---

## Step-by-Step: A Single Lookup

The following traces a single inner-join probe row through the hash table.
The table is in `kHash` mode with 64-byte-aligned `Bucket` arrays.

```
Input: probe row 7, key = "velox"

1. prepareForJoinProbe()
   VectorHasher decodes column 0, row 7 → string "velox"
   VectorHasher::hash() → 0xABCD_EF01_2345_6789
   lookup.hashes[7] = 0xABCD_EF01_2345_6789
   lookup.rows      = [..., 7, ...]

2. joinProbe() — 4-at-a-time loop, row 7 assigned to state1
   state1.preProbe(table, 0xABCD_EF01_2345_6789, 7)
     bucketOffset_ = 0xABCD_EF01_2345_6789 & bucketOffsetMask_
                   = 0x...6780  (128-byte aligned)
     wantedTags_   = broadcast(0xAB)   ← top byte of hash
     prefetch(table_ + 0x6780)

   state1.firstProbe(table, 0)
     tagsInTable_ = load16bytes(table_ + 0x6780)
                  = [0x00, 0xAB, 0x33, 0xAB, ...]   ← slots 1 and 3 match
     hits_        = 0b...1010  (bits 1 and 3 set)
     loadNextHit  → group_ = row_pointer_at_slot_1, prefetch key

   fullProbe<isJoin=true>(lookup, state1, false)
     compare(group_, row_=7) → check RowContainer key == "velox"
       → false (slot 1 holds key "velo", hash collision)
     clear bit 1 from hits_, loadNextHit → group_ = row_pointer_at_slot_3
     compare(group_, 7) → key "velox" matches → return group_

3. lookup.hits[7] = group_  ← pointer to build-side row in RowContainer

4. resultIter_.reset(lookup)

5. listJoinResults()
   iter.nextHit = lookup.hits[7]             ← first match
   output inputRow=7, buildRow=nextHit
   nextHit = nextRow(nextHit)                ← follow nextOffset_ chain
   if nextHit != nullptr → output again (duplicate build row)
   else: advance to next probe row
```

---

## SIMD Acceleration

### Tag comparison

In `kHash` mode, probing loads all 16 tags of a bucket with a single SIMD
instruction and compares them against the wanted tag in parallel, yielding a
16-bit hit mask:

```cpp
TagVector tags = loadTags(bucket);           // _mm_loadu_si128 / vld1q_u8
uint16_t hits = simd::toBitMask(tags == wantedTag);
```

Each set bit identifies a candidate slot. The row pointer at that slot is then
fetched and the full key is compared.

### Gather for kArray mode

When probing in `kArray` mode with consecutive hash values, a SIMD gather
loads multiple table entries in one instruction:

```cpp
simd::gather(
    reinterpret_cast<const int64_t*>(table_),
    reinterpret_cast<const int64_t*>(hashes + firstRow))
    .store_unaligned(reinterpret_cast<int64_t*>(hits) + firstRow);
```

### Deep prefetching in kNormalizedKey mode

`joinNormalizedKeyProbe` prefetches 64 rows ahead for deep CPU pipeline
utilization, since the normalized key layout is predictable.

---

## Null Key Handling

| Mode | Behavior |
|------|----------|
| `HashTable<true>` (join build) | Rows with any null key are silently discarded via `deselectRowsWithNulls()` before insertion. |
| `HashTable<false>` (aggregation) | Null keys are treated as a valid group. All null-key rows update the same accumulator. |

---

## Duplicate Key Handling

In join builds, the same key may appear on the build side multiple times.
`insertForJoin` chains duplicate rows through a `nextOffset_` pointer stored
at a fixed offset within each `RowContainer` row. `listJoinResults` follows
this chain to return every match.

A `hasDuplicates_` flag is set atomically when the first duplicate is
inserted. Operators like left-semi join can set `allowDuplicates = false` to
retain only the first occurrence and skip chain traversal entirely.

---

## Parallel Build

When multiple drivers build the same join hash table they each construct an
independent sub-table. The last driver to finish calls `prepareJoinTable`,
which merges them:

```cpp
void prepareJoinTable(
    std::vector<std::unique_ptr<BaseHashTable>> tables,
    int8_t spillInputStartPartitionBit,
    size_t vectorHasherMaxNumDistinct,
    bool dropDuplicates = false,
    folly::Executor* executor = nullptr);
```

**Steps inside `prepareJoinTable`**:

1. Merge all `VectorHasher` value-ID mappings across sub-tables so every
   driver's row is representable in the unified table.
2. Sum `numDistinct` to determine the final table size.
3. Choose the best hash mode for the combined row count.
4. Optionally call `parallelJoinBuild` if the table is large enough.

### parallelJoinBuild

For large tables, insertion after the merge is parallelized across a thread
pool:

1. Divide the bucket array into contiguous partition ranges.
2. Assign each row to its partition by inspecting its hash.
3. Each thread inserts only the rows in its partition, so threads never
   access overlapping buckets and no locking is needed.
4. Rows that fall near partition boundaries ("overflow") are collected and
   inserted sequentially after all threads complete.

The minimum table size for parallel build is controlled by
`minTableSizeForParallelJoinBuild_`.

---

## Bloom Filter

For join builds over integer or bigint key columns, the hash table optionally
builds a per-column bloom filter during `parallelJoinBuild`. The filter is
partitioned across threads for efficient construction. The probe side uses it
to skip rows that cannot possibly match before touching the main table, saving
memory bandwidth on low-selectivity joins.

Bloom filter support is controlled by `bloomFilterMaxSize_`, set at table
creation time.

---

## Integration with Operators

### HashBuild

`HashBuild` (`velox/exec/HashBuild.h`) feeds rows into the hash table during
the build phase of a hash join.

**States**: `kRunning` → `kWaitForBuild` → `kWaitForProbe` → `kFinish`.

**Detailed flow:**

```
initialize()
  └─ setupTable()
       └─ HashTable<true>::createForJoin(keyHashers, dependentTypes, ...)

addInput(batch)                              ← called per input batch
  ├─ ensureInputFits()                       ← may trigger spill
  ├─ table_->prepareForGroupProbe(lookup, batch, activeRows_, spillBit)
  │     ├─ VectorHasher::decode() per key column
  │     ├─ deselectRowsWithNulls()           ← nulls do not match in joins
  │     └─ VectorHasher::computeValueIds() or hash()  → lookup.hashes
  ├─ table_->groupProbe(lookup, spillBit)    ← insert rows into table
  │     └─ For each row: fullProbe<kInsert>
  │          ├─ Tag match found? → duplicate: install nextOffset_ chain
  │          └─ Empty slot found? → insertEntry() → newRow() in RowContainer
  └─ storeDependent columns (non-key payload) into each inserted row

noMoreInput()
  └─ finishHashBuild()
       ├─ allPeersFinished() barrier         ← wait for all build drivers
       ├─ prepareJoinTable(otherTables, ...)
       │     ├─ Merge VectorHasher value-ID maps across sub-tables
       │     ├─ Sum numDistinct across sub-tables
       │     ├─ decideHashMode()             ← pick best mode for merged size
       │     └─ parallelJoinBuild()          ← optional parallel re-insert
       └─ Publish finished table to HashJoinBridge
```

### HashProbe

`HashProbe` (`velox/exec/HashProbe.h`) probes the finished hash table for each
batch of probe-side rows.

**States**: `kRunning` → `kWaitForBuild` → `kWaitForPeers` → `kFinish`.

**Detailed flow:**

```
initialize()
  └─ Wait on HashJoinBridge future until table_ is available

addInput(probeBatch)                         ← called per probe batch
  ├─ decodeAndDetectNonNullKeys()
  │     └─ Mark null-key rows in nonNullInputRows_ (they can never match)
  ├─ activeRows_ = nonNullInputRows_
  ├─ table_->prepareForJoinProbe(lookup, probeBatch, activeRows_, false)
  │     ├─ VectorHasher::decode() per key column
  │     ├─ VectorHasher::lookupValueIds() or hash()  → lookup.hashes
  │     └─ lookup.rows = active row indices
  ├─ For LEFT / FULL / ANTI joins:
  │     pre-fill lookup.hits[0..N] = nullptr  (misses included in output)
  │     table_->joinProbe(lookup)             ← probe only active rows
  │     expand lookup.rows to 0..N            ← include null-key rows
  └─ For INNER / RIGHT joins:
        table_->joinProbe(lookup)             ← probe active rows only
  └─ resultIter_->reset(lookup)              ← init JoinResultIterator

getOutput()
  └─ output generation loop (repeated until probe batch exhausted):
       ├─ table_->listJoinResults(
       │      resultIter_,
       │      includeMisses,          ← true for LEFT/FULL/ANTI
       │      inputRows[],            ← output: probe row indices
       │      outputTableRows[],      ← output: build row pointers
       │      maxOutputBytes)
       ├─ Copy probe columns from probeBatch[inputRows[i]]
       └─ Extract build columns from RowContainer at outputTableRows[i]
```

**Join type differences in `listJoinResults`:**

| Join type | `includeMisses` | Behaviour on `hits[row] == nullptr` |
|-----------|-----------------|-------------------------------------|
| INNER | false | Row silently skipped |
| LEFT / FULL | true | Row emitted with nulls for build columns |
| LEFT SEMI | false | First match only; chain not followed |
| ANTI | true | Only misses emitted; matches skipped |
| RIGHT | false | Build rows with `hasProbedFlag_` unset emitted in a second pass |

### HashAggregation and GroupingSet

`HashAggregation` (`velox/exec/HashAggregation.h`) delegates to `GroupingSet`
(`velox/exec/GroupingSet.h`), which owns a `HashTable<false>`.

**Detailed flow:**

```
addInput(batch)
  └─ GroupingSet::addInputForActiveRows(batch)
       ├─ table_->prepareForGroupProbe(lookup, batch, activeRows_, spillBit)
       ├─ table_->groupProbe(lookup, spillBit)
       │     ├─ Existing groups → lookup.hits[row] = existing group pointer
       │     └─ New groups     → lookup.hits[row] = new group pointer
       │                          lookup.newGroups += row
       ├─ for each aggregate function f:
       │     f.initializeNewGroups(lookup.hits, lookup.newGroups)
       │                                         ← zero-init new accumulators
       └─     f.addRawInput(lookup.hits, activeRows_, inputs)
                                                 ← update all accumulators

getOutput()
  └─ GroupingSet::getOutput(batchSize, maxBytes, iterator, result)
       ├─ table_->rows()->listRows(&iterator, batchSize, maxBytes, groups[])
       │     └─ Returns up to batchSize group pointers from RowContainer
       ├─ for each key column k:
       │     RowContainer::extractColumn(groups, k, result->childAt(k))
       └─ for each aggregate f:
             f.extractValues(groups, result->childAt(f.outputChannel))
```

**Memory pressure response (partial aggregation):**

```
After addInput():
  if groupingSet_->isPartialFull(maxPartialMemory):
    getOutput() until exhausted    ← flush current groups as intermediate rows
    groupingSet_->resetTable()     ← free hash table, start fresh
    continue accepting input
```

---

## End-to-End Example: Hash Join

The following example walks through a complete inner-join between two tables.

### Schema

```sql
SELECT o.order_id, c.name
FROM orders o JOIN customers c ON o.customer_id = c.id
```

Build side: `customers(id BIGINT, name VARCHAR)` — 1 000 rows  
Probe side: `orders(order_id BIGINT, customer_id BIGINT)` — 10 000 rows

### 1. HashBuild: Building the Table

```cpp
// setupTable() — HashBuild.cpp
auto table = HashTable<true>::createForJoin(
    /*keyHashers=*/ {VectorHasher(BIGINT, /*channel=*/0)},
    /*dependentTypes=*/ {VARCHAR},          // name column
    /*allowDuplicates=*/ false,             // customer id is unique
    /*hasProbedFlag=*/ false,
    /*hasCountFlag=*/ false,
    /*minParallelBuildSize=*/ 1000,
    pool,
    /*bloomFilterMaxSize=*/ 0);

// addInput() — 1 000 customer rows arrive in batches of 1 024
for (auto& batch : customerBatches) {
    SelectivityVector rows(batch->size());

    // Step 1: decode key column and compute hashes
    // VectorHasher for 'id' sees values 1–1000, fits in kNormalizedKey mode
    table->prepareForGroupProbe(lookup, batch, rows, kNoSpillBit);
    // lookup.hashes[i] = value-ID for customer id[i]

    // Step 2: insert into hash table
    table->groupProbe(lookup, kNoSpillBit);
    // For each row: fullProbe<kInsert>
    //   Empty slot found → insertEntry() allocates row in RowContainer
    //   lookup.newGroups = {0, 1, 2, ..., batch.size()-1}  (all new)

    // Step 3: store dependent column (name) into each new row
    rows_->extractColumn(batch->childAt(1), lookup.newGroups, nameOffset);
}

// noMoreInput() → prepareJoinTable()
// With 1 000 rows all in kNormalizedKey mode:
//   capacity = nextPowerOfTwo(1000 / 0.7) = 2048 slots
//   numBuckets = 2048 / 16 = 128 buckets
```

### 2. HashProbe: Probing Per Batch

```cpp
// addInput() — 1 024 order rows arrive
auto& probeBatch = orderBatch;          // columns: [order_id, customer_id]
SelectivityVector rows(probeBatch->size());

// Decode customer_id (column 1) and compute hashes
table->prepareForJoinProbe(lookup, probeBatch, rows, /*decodeNulls=*/false);
// VectorHasher::lookupValueIds() maps each customer_id to its value-ID
// Rows whose customer_id is not in the build side get hash=kUnmappedValueId
// → those rows are deselected from lookup.rows

// Probe the table
lookup.hits.resize(probeBatch->size());
table->joinProbe(lookup);
// For row 0: customer_id=42
//   bucketOffset = valueId(42) & bucketOffsetMask
//   firstProbe loads 16 tags, finds tag match at slot 5
//   fullProbe: normalizedKey(group) == valueId(42) → match
//   lookup.hits[0] = &customers_row_42

// Build output
resultIter.reset(lookup);
while (!resultIter.atEnd()) {
    int32_t n = table->listJoinResults(
        resultIter,
        /*includeMisses=*/false,
        folly::Range(inputRows, batchSize),
        folly::Range(buildRows, batchSize),
        preferredOutputBytes);

    for (int i = 0; i < n; ++i) {
        output->childAt(0) = probeBatch->childAt(0)[inputRows[i]]; // order_id
        output->childAt(1) = rows_->extractString(buildRows[i], nameOffset); // name
    }
}
```

### 3. What listJoinResults Does Internally

```
iter.lastRowIndex = 0, iter.nextHit = nullptr

Iteration 1:
  row = lookup.rows[0] = 0
  iter.nextHit = lookup.hits[0] = &customers_row_42  (first match)
  output: inputRows[0]=0, hits[0]=&customers_row_42
  nextRow(&customers_row_42) = nullptr  (no duplicates, allowDuplicates=false)
  iter.lastRowIndex = 1

Iteration 2:
  row = lookup.rows[1] = 3  (row 1 and 2 had unmapped customer_id, skipped)
  iter.nextHit = lookup.hits[3] = &customers_row_17
  output: inputRows[1]=3, hits[1]=&customers_row_17
  ...
```

### 4. Aggregation Variant

```sql
SELECT customer_id, count(*), sum(amount)
FROM orders
GROUP BY customer_id
```

```cpp
// GroupingSet owns HashTable<false> (nulls kept)
// addInputForActiveRows() per batch:

table->prepareForGroupProbe(lookup, orderBatch, rows, kNoSpillBit);
table->groupProbe(lookup, kNoSpillBit);

// New groups: initialize accumulators
countFn->initializeNewGroups(lookup.hits.data(), lookup.newGroups);
sumFn->initializeNewGroups(lookup.hits.data(), lookup.newGroups);

// All groups: update accumulators
//   lookup.hits[row] = pointer to group row in RowContainer
//   The accumulator for count lives at (group + countOffset)
//   The accumulator for sum   lives at (group + sumOffset)
countFn->addRawInput(lookup.hits.data(), rows, {});       // increments count
sumFn->addRawInput(lookup.hits.data(), rows, {amountVec}); // adds amount

// getOutput(): iterate RowContainer, extract key + accumulator values
table->rows()->listRows(&iterator, batchSize, maxBytes, groups);
RowContainer::extractColumn(groups, /*keyChannel=*/0, output->childAt(0));
countFn->extractValues(groups, output->childAt(1));
sumFn->extractValues(groups, output->childAt(2));
```

---

When `RowContainer` memory exceeds the available reservation:

1. `HashBuild::ensureInputFits()` selects the largest partition(s) for
   spilling.
2. Rows in those partitions are serialized with `ContainerRowSerde` and
   written to disk (one file per partition).
3. After the in-memory build completes, spilled partitions are restored one
   at a time: rows are read back, re-inserted into a fresh hash table, and
   the probe side is replayed against that table.
4. Spilling may recurse if the restored partition still exceeds memory.

The spill partition bit must not overlap with the bucket-index bits used by
the hash table (`checkHashBitsOverlap`).

---

## Creating a HashTable

### For a Join Build

```cpp
auto table = HashTable<true>::createForJoin(
    std::move(keyHashers),     // one VectorHasher per key column
    dependentTypes,            // payload column types
    /*allowDuplicates=*/true,
    /*hasProbedFlag=*/true,    // needed for RIGHT / FULL joins
    /*hasCountFlag=*/false,
    minTableSizeForParallelJoinBuild,
    pool,
    bloomFilterMaxSize);
```

### For Aggregation

```cpp
auto table = HashTable<true>::createForAggregation(
    std::move(keyHashers),
    accumulators,              // aggregation state descriptors
    pool);
```

---

## Statistics

`HashTable` exposes a `stats()` method returning `HashTableStats`:

```cpp
struct HashTableStats {
  int64_t capacity{0};        // Total allocated slots
  int64_t numRehashes{0};     // Rehash operations performed
  int64_t numDistinct{0};     // Distinct rows currently in table
  int64_t numTombstones{0};   // Deleted slot markers
};
```

Operators surface these through runtime metrics:

- `hashtable.capacity`
- `hashtable.numRehashes`
- `hashtable.numDistinct`
- `hashtable.hashMode`
- `hashtable.buildWallNanos`
- `hashtable.parallelJoinPartitionWallNanos`
- `hashtable.bloomFilterSize`

---

## Design Decisions Summary

| Decision | Rationale |
|----------|-----------|
| Three hash modes | Avoids paying full hashing cost for low-cardinality or bit-packable key sets; enables direct array indexing for the common case. |
| 16-slot buckets with SIMD tags | One SIMD comparison eliminates 15 of 16 candidate comparisons; 128-byte bucket fits in 2 cache lines. |
| 6-byte (48-bit) pointers | Halves pointer memory vs. 8-byte pointers while fitting all 64-bit Linux user-space addresses. |
| 4-at-a-time probing | Overlaps latency of independent row lookups without requiring wide vectors or complex scheduling. |
| RowContainer separation | Keeps bucket arrays compact (only 7 bytes/slot) and allows variable-length keys without disturbing the bucket layout. |
| Normalized key below row | Zero-cost storage (no extra allocation) and locality (key is adjacent to row data). |
| Parallel insert by partition | No locks needed because each thread owns a disjoint bucket range. |
