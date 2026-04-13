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

Every call to `groupProbe` or `joinProbe` passes a `HashLookup` struct
(`velox/exec/HashTable.h`). It is **reused across batches** — callers call
`lookup.reset(size)` at the start of each batch rather than constructing a
new object.

```cpp
struct HashLookup {
  // Set by the caller before the probe.
  const std::vector<std::unique_ptr<VectorHasher>>& hashers;
  raw_vector<vector_size_t> rows;       // Row indices to probe (sparse OK)
  raw_vector<uint64_t>      hashes;     // Hash values, indexed by row number

  // Filled in by groupProbe / joinProbe.
  raw_vector<char*>          hits;      // Per-row result pointer, indexed by row number
  std::vector<vector_size_t> newGroups; // Row indices where new groups were created
                                        // (groupProbe only; always empty after joinProbe)
  raw_vector<uint64_t>       normalizedKeys; // Used in kNormalizedKey mode
};
```

**Indexing convention** — `rows`, `hashes`, and `hits` are all indexed by the
**input row number**, not by position within `rows`. This lets a single sparse
`SelectivityVector` drive the probe without compacting the arrays. A row that
was filtered out before the probe simply has `hits[row] == nullptr`.

After a successful `groupProbe`:
- `hits[row]` points to the existing or newly created group row in
  `RowContainer`.
- `newGroups` contains the row indices of every group that was just created
  so accumulators can be initialized.

After a successful `joinProbe`:
- `hits[row]` points to the **first** matching build-side row, or `nullptr`
  for a miss.
- Duplicate matches are retrieved by following the `nextOffset_` chain via
  `listJoinResults`.

---

## ProbeState: The Per-Row SIMD State Machine

Each simultaneous in-flight probe is represented by a `ProbeState` object
(`velox/exec/HashTable.h`). The caller instantiates four of them to process
rows in groups of four, overlapping memory latency across independent probes.

```cpp
class ProbeState {
  char*    group_;          // Row pointer for the current candidate match
  TagVector wantedTags_;   // 16-wide broadcast of the target tag byte
  TagVector tagsInTable_;  // 16 tags loaded from the current bucket
  int32_t  row_;           // Row index being probed
  int64_t  bucketOffset_;  // Byte offset of the current bucket in table_
  MaskType hits_;          // 16-bit bitmask — set bits are tag matches
  uint8_t  indexInTags_;   // Slot index within the bucket (for insert/erase)
};
```

A probe advances through three phases:

### Phase 1 — preProbe

Computes the starting bucket offset from the hash value, broadcasts the tag
byte into a SIMD register, and issues a software prefetch so the bucket is
warm in L1 cache by the time `firstProbe` executes.

```cpp
// HashTable.h:111
void preProbe(const Table& table, uint64_t hash, int32_t row) {
  row_          = row;
  bucketOffset_ = hash & table.bucketOffsetMask_;       // low bits → bucket
  wantedTags_   = TagVector::broadcast(hash >> 56);     // high byte → tag
  group_        = nullptr;
  __builtin_prefetch(table.table_ + bucketOffset_);
}
```

### Phase 2 — firstProbe

Loads all 16 tag bytes from the prefetched bucket with one SIMD instruction
and compares them against `wantedTags_` in parallel, producing a 16-bit hit
mask. If any bit is set, the corresponding row pointer is prefetched.

```cpp
// HashTable.h:126
void firstProbe(const Table& table, int32_t firstKey) {
  tagsInTable_ = loadTags(table.table_, bucketOffset_); // _mm_loadu_si128
  hits_ = simd::toBitMask(tagsInTable_ == wantedTags_); // 16-way compare → mask
  if (hits_) loadNextHit(table, firstKey);              // prefetch row pointer
}
```

### Phase 3 — fullProbe

Resolves collisions. For every set bit in `hits_` it fetches the row pointer
and calls the caller-supplied `compare` lambda to verify full key equality.
If no match is found in the current bucket the probe advances to the next one
(linear probing at bucket granularity) until an empty slot or the whole table
has been scanned.

```
for each bucket starting at bucketOffset_:
  for each set bit in hits_:
    fetch row pointer
    if compare(row_pointer, probe_row) → match found, return
  if empty slot found:
    if op == kInsert → allocate new row, store pointer, return
    if op == kProbe  → no match, return nullptr
  bucketOffset_ = nextBucket(bucketOffset_)
  load tags, recompute hits_
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
