# Hash Tables in Velox: A Beginner's Guide

This document explains how Velox implements hash tables for query execution,
starting from first principles and building up to a complete picture of every
component. No prior knowledge of query engines or hash tables is assumed.

---

## Table of Contents

1. [Why Hash Tables Exist in a Query Engine](#1-why-hash-tables-exist-in-a-query-engine)
2. [The Big Picture — All the Pieces](#2-the-big-picture--all-the-pieces)
3. [Hash Fundamentals — Tags and Buckets](#3-hash-fundamentals--tags-and-buckets)
4. [RowContainer — Where Rows Actually Live](#4-rowcontainer--where-rows-actually-live)
5. [VectorHasher — Turning Columns Into Lookup Keys](#5-vectorhasher--turning-columns-into-lookup-keys)
6. [Hash Modes — Adapting to the Data](#6-hash-modes--adapting-to-the-data)
7. [HashLookup — The Message Passed to the Table](#7-hashlookup--the-message-passed-to-the-table)
8. [ProbeState — The Per-Row SIMD State Machine](#8-probestate--the-per-row-simd-state-machine)
9. [HashBuild — The Operator That Fills the Table](#9-hashbuild--the-operator-that-fills-the-table)
10. [HashProbe — The Operator That Queries the Table](#10-hashprobe--the-operator-that-queries-the-table)
11. [HashJoinBridge — Handing the Table from Build to Probe](#11-hashjoinbridge--handing-the-table-from-build-to-probe)
12. [Complete Join Example — Step by Step](#12-complete-join-example--step-by-step)

---

## 1. Why Hash Tables Exist in a Query Engine

### The problem

Suppose you are running this SQL query:

```sql
SELECT o.order_id, c.name
FROM orders o
JOIN customers c ON o.customer_id = c.id
```

You have 10 million order rows and 100 thousand customer rows. For each order
you need to find the customer with the matching `id`. A straightforward
approach is: for every order row, scan all customer rows until you find a
match. With 10 M × 100 K = 1 trillion comparisons, that is far too slow.

### The hash table solution

A hash table answers the question **"does value X exist in a set, and if so
where?"** in roughly constant time, regardless of how large the set is.

The idea:

1. Assign each possible key value a **bucket number** by running it through a
   **hash function** — a formula that turns any value into a small integer.
2. Store the row in that bucket.
3. To look up a key, hash it the same way and jump directly to its bucket.
   Only compare rows inside that one bucket.

With a good hash function, each bucket holds only a few rows, so each lookup
is very fast. This is called **hashing**.

In a query engine, hash tables power two fundamental operations:

| Operation | Question the table answers |
|-----------|---------------------------|
| **Hash join** | "Is this probe-side row's key present on the build side?" |
| **Hash aggregation** | "Which group does this row belong to (find or create it)?" |

This document focuses entirely on the **hash join** case.

---

## 2. The Big Picture — All the Pieces

Before diving into each component, here is a map of everything involved in a
hash join and how the pieces connect.

```
┌─────────────────────────────────────────────────────────────┐
│                      Query Execution                        │
│                                                             │
│   HashBuild operator          HashProbe operator           │
│   (reads build side)          (reads probe side)           │
│         │                           │                       │
│         │ calls                     │ calls                 │
│         ▼                           ▼                       │
│   ┌─────────────────────────────────────────────────────┐  │
│   │              BaseHashTable / HashTable<T>           │  │
│   │  ┌───────────────┐   ┌───────────────────────────┐ │  │
│   │  │  Bucket array  │   │      RowContainer         │ │  │
│   │  │  [tag|ptr]×16  │──▶│  [row][row][row]...       │ │  │
│   │  │  [tag|ptr]×16  │   │  each row: key cols +     │ │  │
│   │  │   ...          │   │            value cols +   │ │  │
│   │  └───────────────┘   │            null bits       │ │  │
│   │                       └───────────────────────────┘ │  │
│   │  ┌─────────────────┐  ┌─────────────────────────┐  │  │
│   │  │  VectorHasher[] │  │      HashLookup          │  │  │
│   │  │  one per key col│  │  rows[], hashes[],       │  │  │
│   │  │  decode/hash/   │  │  hits[], newGroups[]     │  │  │
│   │  │  valueIds       │  └─────────────────────────┘  │  │
│   │  └─────────────────┘                                │  │
│   │  ┌─────────────────────────────────────────────┐   │  │
│   │  │  ProbeState × 4 (or × 64)                   │   │  │
│   │  │  per-row SIMD state machine for probing      │   │  │
│   │  └─────────────────────────────────────────────┘   │  │
│   └─────────────────────────────────────────────────────┘  │
│                                                             │
│   HashJoinBridge — synchronizes build and probe pipelines  │
└─────────────────────────────────────────────────────────────┘
```

**Components at a glance:**

| Component | File | Role |
|-----------|------|------|
| `BaseHashTable` / `HashTable<T>` | `velox/exec/HashTable.h/.cpp` | The hash table itself — bucket array, probing logic, insert logic |
| `RowContainer` | `velox/exec/RowContainer.h/.cpp` | Stores the actual row bytes for every inserted row |
| `VectorHasher` | `velox/exec/VectorHasher.h/.cpp` | Converts one key column into hash values or compact IDs |
| `HashLookup` | `velox/exec/HashTable.h` | Struct passed to every probe/insert call — carries inputs and receives outputs |
| `ProbeState` | `velox/exec/HashTable.cpp` | Per-row state machine that walks buckets using SIMD |
| `HashBuild` | `velox/exec/HashBuild.h/.cpp` | Query operator — reads build-side rows and inserts them |
| `HashProbe` | `velox/exec/HashProbe.h/.cpp` | Query operator — reads probe-side rows and looks them up |
| `HashJoinBridge` | `velox/exec/HashJoinBridge.h/.cpp` | Passes the finished table from build to probe pipeline |

---

## 3. Hash Fundamentals — Tags and Buckets

### What is a hash function?

A hash function maps any value to a number. A good one spreads values evenly
and is cheap to compute. In Velox, hash values are always 64-bit unsigned
integers.

```
hash("Alice")   → 0xA3B2_C1D0_E5F4_0011
hash("Bob")     → 0x21DC_009A_FF31_8876
hash(42)        → 0x7F2C_AA01_2233_4455
```

### What is a bucket?

Velox stores the hash table as an array of **Buckets**. Each `Bucket` is 128
bytes (exactly 2 CPU cache lines) and holds **16 slots**:

```
Bucket (128 bytes)
┌─────────────────────────────────────────────────────────┐
│  Tags      [16 bytes]  — one 1-byte tag per slot        │
│  Pointers  [96 bytes]  — one 6-byte pointer per slot    │
│  Padding   [16 bytes]  — cache-line alignment           │
└─────────────────────────────────────────────────────────┘
```

**Tag** (1 byte): A compressed fingerprint of the hash value. Specifically, it
stores 7 bits extracted from the hash with bit 7 forced to 1:

```cpp
// velox/exec/HashTable.h
uint8_t hashTag(uint64_t hash) {
    return (hash >> 38) | 0x80;
}
```

The bit-7 convention means a tag is always at least `0x80`. The value `0x00`
signals an *empty* slot, and `0x7F` signals a *tombstone* (erased slot). This
keeps the sentinel values distinct from any real tag.

**Pointer** (6 bytes): The lower 48 bits of a pointer into `RowContainer`
where the actual row data lives.

### Finding a bucket

To find which bucket to search for a given hash:

```
bucketIndex = hash & bucketOffsetMask
```

`bucketOffsetMask` is computed so that `bucketIndex` always falls on a
128-byte boundary. The number of buckets is always a power of two, so this
mask operation is a single bitwise AND — very fast.

### Why a tag byte instead of full comparison?

When you land in a bucket, you need to check all 16 slots for a matching row.
If you had to read the full row (which may be 100s of bytes away in memory) for
every slot, you would generate many cache misses. Instead, the 7-bit tag is
kept *inline* in the bucket.

The CPU compares all 16 tags simultaneously with one SIMD instruction:

```cpp
// Load all 16 tags as a 128-bit SIMD register (one instruction).
TagVector tagsInBucket = loadTags(bucket);

// Compare all 16 against the wanted tag in parallel (one instruction).
// Result: a 16-bit bitmask — bit i is 1 if tags[i] == wantedTag.
uint16_t hits = simd::toBitMask(tagsInBucket == wantedTag);
```

Only the slots with a matching tag (very few, usually zero or one) need a full
key comparison. This eliminates most unnecessary row reads.

### Linear probing

When a bucket is full (all 16 slots occupied), the probe moves to the *next*
bucket in order, wrapping around at the end of the array. This is called
**linear probing**. Velox probes at bucket granularity (128-byte steps), not
slot granularity, which keeps SIMD loads efficient.

### Load factor and rehashing

The table rehashes when more than 70% of slots are occupied
(`kHashTableLoadFactor = 0.7`). At rehash, the capacity doubles to the next
power of two, and every row is re-inserted.

---

## 4. RowContainer — Where Rows Actually Live

### The separation of concerns

The bucket array holds only tiny 6-byte pointers. All actual row data — key
columns, value columns, aggregation state, null bits — lives in `RowContainer`
(`velox/exec/RowContainer.h`).

This separation is deliberate:
- Buckets stay small → more buckets fit in CPU cache → faster probing.
- Row data is only touched *after* the tag comparison confirms a likely match,
  amortizing the cost of cache misses across successful hits.

### How rows are laid out

Every row in `RowContainer` is a flat byte sequence. Its layout is computed
once at construction time and never changes:

```
Row (example: key=BIGINT, value=VARCHAR)
┌───────────────────────────────────────────┐
│ [null bits]   — 1 byte, one bit per col   │  ← always first
│ [key col 0]   — 8 bytes (int64_t)         │
│ [value col 0] — StringView or out-of-line │  ← may reference HashStringAllocator
│ [nextOffset]  — 8 bytes (char*)           │  ← for join duplicate chains
│ [other flags] — probe flag, count flag    │
└───────────────────────────────────────────┘
```

Variable-length strings (VARCHAR) are *not* stored inline in the row body.
Their headers (a `StringView` containing pointer + length) are inline, but the
character data lives in a `HashStringAllocator` that is part of the same
`RowContainer`.

### Normalized key prefix

In `kNormalizedKey` mode (explained in Section 6), 8 additional bytes are
reserved *immediately before* each row:

```
Memory:  [ normalized_key (8 bytes) ][ actual row bytes → ]
                                       ▲
                              char* pointer points here
```

This byte is accessed as:

```cpp
// velox/exec/RowContainer.h
static uint64_t& normalizedKey(char* row) {
    return reinterpret_cast<uint64_t*>(row)[-1];
}
```

### Key methods

**`newRow()`** — Allocates space for one new row and returns a `char*` to its
start. The pointer is also stored in the appropriate hash table bucket.

```cpp
char* row = rowContainer->newRow();
// 'row' now points to uninitialized row bytes.
// The caller must fill every column before the row is used.
```

**`store(decoded, rowIndex, row, columnIndex)`** — Writes a single value from
a decoded column vector into a specific column of a row:

```cpp
// Write column 0 (the key column) from the decoded vector into the row.
rowContainer->store(decodedKeyColumn, inputRow, row, /*columnIndex=*/0);
```

**`extractColumn(rows, numRows, col, hasNulls, resultOffset, result)`** — The
reverse of `store`. Reads a column out of multiple rows and populates a
`VectorPtr` output. This is how join output is produced.

```cpp
// Read the 'name' column from 50 matched build-side rows into the output vector.
RowContainer::extractColumn(
    buildRows,          // array of char* row pointers
    50,                 // number of rows
    nameColumn,         // which column to extract
    /*columnHasNulls=*/false,
    /*resultOffset=*/0,
    outputVector);
```

**`listRows(iterator, maxRows, maxBytes, result)`** — Iterates through all
stored rows in batches. Used by aggregation to extract final group results.

### The duplicate chain (nextOffset_)

In a join, the same key can appear multiple times on the build side (e.g.,
one customer can place many orders — or wait, that's the opposite way around —
let's say a customer appears multiple times in a de-normalized table). When two
rows have the same join key, both must be returned.

`RowContainer` stores a `char*` pointer at a fixed offset inside each row
pointing to the *next* row with the same key:

```
Row A (key=42) ──nextOffset_──▶ Row B (key=42) ──nextOffset_──▶ nullptr
```

`insertForJoin` installs these chain pointers when the second, third, etc.
row with the same key is inserted. `listJoinResults` follows the chain to
return all matches.

---

## 5. VectorHasher — Turning Columns Into Lookup Keys

### Why one VectorHasher per key column?

A join key can span multiple columns:

```sql
JOIN ON a.x = b.x AND a.y = b.y
```

Each column may have a different type (integer, string, date). `VectorHasher`
handles one column at a time. The hash table owns one `VectorHasher` per key
column; their outputs are **combined** into a single 64-bit value per row.

### What VectorHasher does

```cpp
// velox/exec/VectorHasher.h
class VectorHasher {
 public:
  // Step 1: Decode the raw column vector into a uniform form.
  void decode(const BaseVector& vector, const SelectivityVector& rows);

  // Step 2a: Compute 64-bit hash values (kHash mode).
  void hash(const SelectivityVector& rows, bool mix, raw_vector<uint64_t>& result);

  // Step 2b: Assign each distinct value a small integer ID (kNormalizedKey/kArray mode).
  bool computeValueIds(const SelectivityVector& rows, raw_vector<uint64_t>& result);

  // Step 2c: Probe-only: look up previously assigned IDs.
  // Removes rows whose values were never seen on the build side.
  bool lookupValueIds(
      const BaseVector& input,
      SelectivityVector& rows,
      ScratchMemory& scratchMemory,
      raw_vector<uint64_t>& result) const;
};
```

**`decode()`** handles all Velox vector encodings (flat, dictionary, constant)
and produces a uniform `DecodedVector` view. Everything after this works on
the decoded form.

**`hash()`** computes a full 64-bit hash for each row using `folly::hasher`.
When multiple columns are combined, the second column's hash is XOR-mixed into
the first, and so on.

**`computeValueIds()`** assigns a compact integer ID (1, 2, 3, …) to each
distinct value seen on the **build side**. These IDs are used in
`kNormalizedKey` and `kArray` modes to avoid expensive full hashing. The IDs
are packed into a single 64-bit word across all key columns.

**`lookupValueIds()`** is used on the **probe side**. It translates probe-side
values into the IDs that were assigned during build. Any probe row whose value
was never seen on the build side will never find a match — those rows are
removed from the active set immediately, before the table is even touched.

### UniqueValue — tracking distinct values

`VectorHasher` internally maintains a hash set of `UniqueValue` objects, one
per distinct value observed. Each `UniqueValue` stores the raw value (inline
for small scalars, by pointer for strings) and its assigned ID.

```cpp
// velox/exec/VectorHasher.h
class UniqueValue {
    uint64_t data_;   // the raw value (or pointer to string data)
    uint32_t size_;   // byte size of the value
    uint32_t id_;     // assigned compact ID (1, 2, 3, …)
};
```

When `kMaxDistinct = 100,000` is exceeded, `VectorHasher` gives up on compact
IDs and falls back to full 64-bit hashing.

---

## 6. Hash Modes — Adapting to the Data

The hash table does not use the same probing strategy for all data. It watches
the data as it arrives and picks the most efficient of three **hash modes**:

```cpp
// velox/exec/HashTable.h
enum class HashMode { kHash, kArray, kNormalizedKey };
```

### kArray — direct array index

When every key column's values can be mapped to a small non-negative integer in
the range `[0, 2M)`, the entire hash table collapses to a flat array:

```
table_[valueId] → row pointer
```

A lookup is a single array dereference. No hashing, no bucket scan. This is
ideal for low-cardinality columns like booleans, small enums, or short integer
IDs.

**Example**: A `status` column with values `{0, 1, 2}` triggers `kArray` mode.
`VectorHasher` assigns IDs 1, 2, 3 to those values. The array has 4 entries
(index 0 unused). Lookup: `table_[2]` → the row for `status = 1`.

### kNormalizedKey — one 64-bit comparison

When all key columns together fit within 59 bits, `VectorHasher` packs all
their IDs into a single 64-bit integer (the "normalized key"). This value is
stored both in `lookup.hashes` (for bucket selection) and immediately *before*
each row in `RowContainer`.

A key comparison becomes a single 64-bit integer equality check instead of a
column-by-column comparison:

```cpp
RowContainer::normalizedKey(candidateRow) == lookup.normalizedKeys[probeRow]
```

This is far faster than comparing individual columns, especially for multi-key
joins.

### kHash — full open-addressing hash table

When values are too numerous or too spread out for the above modes, the table
falls back to the full bucket-scan approach described in Section 3. Each row
carries a full 64-bit hash, tags identify candidate slots, and full key
comparison resolves collisions.

### Mode selection and upgrade

The mode starts as `kArray` and is upgraded as more data arrives:

```
kArray → kNormalizedKey → kHash
```

Each upgrade happens when the current mode can no longer represent the observed
values. An upgrade triggers a **rehash**: all existing rows are re-read from
`RowContainer`, their hashes are recomputed under the new mode, and they are
re-inserted into a newly allocated bucket array.

---

## 7. HashLookup — The Message Passed to the Table

`HashLookup` (`velox/exec/HashTable.h:57`) is the struct that carries input and
output between the caller (an operator) and the hash table's probe/insert
functions. The same `HashLookup` object is **reused across every batch** to
avoid repeated allocation.

```cpp
struct HashLookup {
    // ── Reference (set once at construction) ─────────────────────────────
    const std::vector<std::unique_ptr<VectorHasher>>& hashers;

    // ── Inputs (written by prepareForGroupProbe / prepareForJoinProbe) ───
    raw_vector<vector_size_t> rows;    // which input rows to probe
    raw_vector<uint64_t>      hashes;  // hash or value-ID per row

    // ── Outputs (written by groupProbe / joinProbe) ──────────────────────
    raw_vector<char*>          hits;       // matching row pointer per row
    std::vector<vector_size_t> newGroups;  // rows where a new entry was created
    raw_vector<uint64_t>       normalizedKeys; // normalized key per row (kNormalizedKey mode)
};
```

**`rows`** — Indices of the input rows that should be probed. Not every input
row needs probing: rows with null keys are excluded before the probe.

**`hashes`** — Hash values (or packed value IDs in `kNormalizedKey`/`kArray`
mode), one per row, indexed by **row number** (not by position within `rows`).
This is the critical `SelectivityVector` convention: if row 7 is active, its
hash is at `hashes[7]`, not at `hashes[0]`.

**`hits`** — After the probe, `hits[row]` holds a `char*` pointer into
`RowContainer` for the matching row. `nullptr` means no match (a miss).

**`newGroups`** — Used only by `groupProbe` (aggregation). Contains the indices
of rows for which a brand-new group was just created. Aggregate functions use
this list to zero-initialize their accumulators.

**The lifecycle of a HashLookup per batch:**

```
1. lookup.reset(batchSize)
   → resize rows/hashes/hits to batchSize, clear newGroups

2. prepareForGroupProbe(lookup, inputBatch, activeRows, spillBit)
   → VectorHasher.decode() each key column
   → VectorHasher.hash() or computeValueIds() → fills lookup.hashes[row]
   → populateLookupRows(activeRows) → fills lookup.rows

3. groupProbe(lookup, spillBit)   (or joinProbe(lookup))
   → for each row in lookup.rows, probe bucket using lookup.hashes[row]
   → write result to lookup.hits[row]
   → for groupProbe: append new rows to lookup.newGroups

4. caller reads lookup.hits[row] to process results
```

---

## 8. ProbeState — The Per-Row SIMD State Machine

### Why a state machine?

Looking up a single row in the hash table involves:

1. Computing which bucket to start in.
2. Issuing a prefetch for that bucket (non-blocking memory read).
3. Loading 16 tags and comparing them with SIMD.
4. For each matching tag, reading the row pointer and comparing the full key.
5. If no match and no empty slot, moving to the next bucket.

Steps 2 and 3 together have a **cache miss penalty** — fetching the bucket from
RAM takes 40–100 CPU cycles. If the code waited for each row's bucket to arrive
before processing the next row, those cycles would be wasted.

The solution: process **four rows simultaneously**. While waiting for row A's
bucket, issue prefetches for rows B, C, D. By the time rows B, C, D are done,
row A's bucket is ready. This overlaps memory latency across independent rows.

`ProbeState` (`velox/exec/HashTable.cpp:89`) holds all the in-flight state for
**one row** in this concurrent pipeline.

### The three phases

```cpp
class ProbeState {
 public:
  // Phase 1: Compute bucket, broadcast tag, issue prefetch.
  void preProbe(const Table& table, uint64_t hash, int32_t row);

  // Phase 2: Load 16 tags from (now-warm) bucket; SIMD compare; prefetch first hit.
  void firstProbe(const Table& table, int32_t firstKey);

  // Phase 3: Walk bucket(s) until match, empty slot, or full-table scan.
  char* fullProbe(Table& table, int32_t firstKey,
                  Compare compare, Insert insert,
                  int64_t& numTombstones, bool extraCheck);
};
```

### Phase 1 — preProbe

```cpp
void preProbe(const Table& table, uint64_t hash, int32_t row) {
    row_          = row;                          // remember which input row
    bucketOffset_ = table.bucketOffset(hash);     // low bits → bucket address
    wantedTags_   = TagVector::broadcast(         // replicate tag 16× in SIMD reg
                        BaseHashTable::hashTag(hash));
    group_        = nullptr;
    indexInTags_  = kNotSet;
    __builtin_prefetch(table.table_ + bucketOffset_);  // tell CPU: fetch this soon
}
```

The `__builtin_prefetch` is the key instruction. It starts a memory read in the
background without stalling the CPU. By the time `firstProbe` runs (several
instructions later), the bucket data should already be in L1 cache.

### Phase 2 — firstProbe

```cpp
void firstProbe(const Table& table, int32_t firstKey) {
    // One SIMD instruction: load 16 bytes (all 16 tags in this bucket).
    tagsInTable_ = loadTags(table.table_, bucketOffset_);

    // One SIMD instruction: compare all 16 tags vs wantedTags_ in parallel.
    // Result: 16-bit bitmask — bit i is set if tags[i] == wantedTag.
    hits_ = simd::toBitMask(tagsInTable_ == wantedTags_);

    if (hits_) {
        // At least one potential match. Load the row pointer and prefetch the
        // actual row data so fullProbe's key comparison is also fast.
        loadNextHit(table, firstKey);
    }
}
```

### Phase 3 — fullProbe

Resolves the probe to a definitive answer. It checks the candidate loaded by
`firstProbe`, then walks through any remaining tag matches, then (if no empty
slot is found) moves to the next bucket.

The caller supplies two lambdas:

- **`compare(group, row)`** — Returns `true` if the candidate row's key equals
  the probe row's key. For `kNormalizedKey` mode this is one 64-bit comparison;
  otherwise it calls into `RowContainer` to compare individual columns.

- **`insert(row, bucketOffset)`** — Called when an empty slot is found and the
  operation is an **insert** (aggregation). Allocates a new row in `RowContainer`
  and stores the pointer in the bucket.

### The 4-at-a-time loop

In `groupProbe` and `joinProbe`, four `ProbeState` objects are used:

```cpp
// velox/exec/HashTable.cpp:488
ProbeState state1, state2, state3, state4;

for (; probeIndex + 4 <= numProbes; probeIndex += 4) {
    // ── Phase 1 × 4: issue 4 prefetches (rows i, i+1, i+2, i+3) ──────
    state1.preProbe(*this, lookup.hashes[rows[probeIndex+0]], rows[probeIndex+0]);
    state2.preProbe(*this, lookup.hashes[rows[probeIndex+1]], rows[probeIndex+1]);
    state3.preProbe(*this, lookup.hashes[rows[probeIndex+2]], rows[probeIndex+2]);
    state4.preProbe(*this, lookup.hashes[rows[probeIndex+3]], rows[probeIndex+3]);

    // ── Phase 2 × 4: load+compare tags (buckets now warm in L1) ──────
    state1.firstProbe(*this, 0);
    state2.firstProbe(*this, 0);
    state3.firstProbe(*this, 0);
    state4.firstProbe(*this, 0);

    // ── Phase 3 × 4: resolve each probe ──────────────────────────────
    fullProbe<isJoin>(lookup, state1, /*extraCheck=*/false);
    fullProbe<isJoin>(lookup, state2, /*extraCheck=*/true);
    fullProbe<isJoin>(lookup, state3, /*extraCheck=*/true);
    fullProbe<isJoin>(lookup, state4, /*extraCheck=*/true);
}
```

`extraCheck=true` for states 2–4 because `state1` may have inserted a new row
into the bucket, changing a tag byte. States 2–4 reload the tag vector at the
start of `fullProbe` to see the updated state.

For `joinNormalizedKeyProbe`, the batch size is **64** instead of 4, because
normalized-key comparison is so cheap that more in-flight prefetches are needed
to keep the pipeline fully utilized.

---

## 9. HashBuild — The Operator That Fills the Table

`HashBuild` (`velox/exec/HashBuild.h/.cpp`) is a Velox operator that reads
rows from the build side of a join (typically the smaller table) and inserts
them into the hash table.

### State machine

```
kRunning ──► kWaitForBuild ──► kFinish
                  ▲
         (when all build drivers
          have finished their work)
```

Multiple CPU threads ("drivers") can run `HashBuild` simultaneously, each
building their own **sub-table** over a partition of the build-side data.

### addInput — inserting a batch

Every time the upstream operator produces a batch of rows, `HashBuild::addInput`
is called:

```
HashBuild::addInput(buildBatch):
  1. ensureInputFits()
        → if memory is tight: spill some partitions to disk

  2. table_->prepareForGroupProbe(lookup_, buildBatch, activeRows_, spillBit)
        → VectorHasher[0].decode(buildBatch->childAt(keyChannel_[0]), activeRows_)
          VectorHasher[0].hash(activeRows_, /*mix=*/false, lookup_.hashes)
          — repeat for each key column, XOR-mixing hashes together —
        → populateLookupRows(activeRows_) → lookup_.rows

  3. table_->groupProbe(lookup_, spillBit)
        → for each row in lookup_.rows:
             ProbeState.preProbe(hashes[row], row)   × 4 in parallel
             ProbeState.firstProbe(...)               × 4
             ProbeState.fullProbe(compare, insert, …)
               → if key found in bucket: return existing row pointer
               → if empty slot: call insert lambda →
                     newRow = RowContainer.newRow()
                     lookup_.hits[row] = newRow
                     lookup_.newGroups.push_back(row)

  4. for each newly created row (in lookup_.newGroups):
        for each dependent column c:
            RowContainer.store(decoded[c], row, newRow, columnIndex)
```

After all build batches are processed, `HashBuild::noMoreInput` is called,
which triggers the finalization step via `HashJoinBridge`.

### noMoreInput — finalizing the table

When the last build driver finishes, it calls:

```
HashBuild::finishHashBuild():
  1. Collect sub-tables from all parallel build drivers.
  2. table_->prepareJoinTable(otherTables, spillBit, ...)
        → merge VectorHasher value-ID mappings across all sub-tables
        → sum numDistinct across sub-tables
        → decideHashMode() — pick best mode for the total row count
        → optionally: parallelJoinBuild() — parallel re-insert across thread pool
  3. Publish finished table via HashJoinBridge.setHashTable()
```

---

## 10. HashProbe — The Operator That Queries the Table

`HashProbe` (`velox/exec/HashProbe.h/.cpp`) waits for the hash table to be
ready, then reads probe-side rows (the larger table) and looks them up one
batch at a time.

### State machine

```
kRunning ──► kWaitForBuild ──► kRunning ──► kFinish
               (blocked until
                table is ready)
```

### addInput — probing a batch

```
HashProbe::addInput(probeBatch):
  1. decodeAndDetectNonNullKeys()
        → mark null-key rows in nonNullInputRows_
          (they can never match any build row)

  2. table_->prepareForJoinProbe(lookup_, probeBatch, activeRows_, false)
        → VectorHasher[0].decode(probeBatch->childAt(probeKeyChannel_[0]), activeRows_)
          VectorHasher[0].lookupValueIds(...)
          — this REMOVES rows from activeRows_ whose values were never seen
            on the build side (they cannot match) —
        → hash remaining rows → lookup_.hashes[row]
        → lookup_.rows = remaining active rows

  3. (For LEFT/FULL/ANTI joins):
        pre-fill lookup_.hits[0..N] = nullptr
        (so missing rows are included in output)

  4. table_->joinProbe(lookup_)
        → for each row in lookup_.rows:
             ProbeState.preProbe / firstProbe / fullProbe
             → lookup_.hits[row] = first matching build row (or nullptr)

  5. resultIter_.reset(lookup_)
        → prepare to iterate through hits and their duplicate chains
```

### getOutput — producing result rows

```
HashProbe::getOutput():
  loop until output batch is full or probe batch is exhausted:

    n = table_->listJoinResults(
            resultIter_,
            includeMisses,           // true for LEFT/FULL/ANTI joins
            inputRows[],             // output: which probe row matched
            outputTableRows[],       // output: which build row matched
            maxOutputBytes)

    for i in 0..n:
        // Copy probe columns
        for each probe output column p:
            output->childAt(p)[i] = probeBatch->childAt(p)[inputRows[i]]

        // Extract build columns from RowContainer
        for each build output column b:
            RowContainer::extractColumn(outputTableRows, n, col, ...)
                → fills output->childAt(b) from the matched build rows
```

### listJoinResults — iterating the duplicate chain

```
JoinResultIterator state:
  rows      → &lookup_.rows       (list of probe row indices)
  hits      → &lookup_.hits       (first matching build row per probe row)
  lastRowIndex                    (where we are in rows[])
  nextHit                         (next build row to return for current probe row)

Each call:
  while output not full AND not at end:
    if nextHit == nullptr:
        advance to next probe row (lastRowIndex++)
        nextHit = hits[rows[lastRowIndex]]
    emit (inputRow=rows[lastRowIndex], buildRow=nextHit)
    nextHit = RowContainer.nextRow(nextHit)  // follow nextOffset_ chain
    if nextHit == nullptr: advance probe row
```

---

## 11. HashJoinBridge — Handing the Table from Build to Probe

`HashJoinBridge` (`velox/exec/HashJoinBridge.h`) solves a timing problem: the
probe pipeline cannot start until the build pipeline has finished. In a
multi-threaded execution engine, these run as separate pipelines on separate
threads.

```
Build pipeline:  [TableScan] → [HashBuild]  → (done) → setHashTable()
                                                            │
                                                            │ (future resolved)
                                                            ▼
Probe pipeline:  [TableScan] → [HashProbe] ← (blocked on) tableOrFuture()
```

**Key interactions:**

1. Each `HashBuild` driver calls `bridge->addBuilder()` in its constructor so
   the bridge knows how many builders to wait for.

2. As drivers finish, they call `bridge->setHashTable(table, spillPartitions)`.
   The **last** driver to call this triggers the merge (via `prepareJoinTable`)
   and unblocks all waiting probe drivers.

3. Each `HashProbe` driver calls `bridge->tableOrFuture(&future)`. If the
   table is not ready, `future` is set to a `folly::SemiFuture` that the
   driver waits on. Once the table is ready, the future resolves and the probe
   driver receives the shared table pointer.

4. After probing finishes, `bridge->probeFinished()` coordinates spill
   recovery: if some build partitions were spilled to disk, the bridge
   orchestrates re-building them one at a time.

The bridge is shared via `std::shared_ptr` between all build and probe
operators in the same join.

---

## 12. Complete Join Example — Step by Step

This section traces a concrete `INNER JOIN` from start to finish, naming every
component that participates at each step.

### The query

```sql
SELECT o.order_id, c.name
FROM orders o
JOIN customers c ON o.customer_id = c.id
```

**Build side**: `customers` — 3 rows (small table, so we build the hash table
over it):

| id (BIGINT) | name (VARCHAR) |
|-------------|----------------|
| 10          | Alice          |
| 20          | Bob            |
| 30          | Carol          |

**Probe side**: `orders` — 5 rows:

| order_id (BIGINT) | customer_id (BIGINT) |
|-------------------|----------------------|
| 1001              | 20                   |
| 1002              | 10                   |
| 1003              | 99                   |  ← no matching customer
| 1004              | 10                   |
| 1005              | 30                   |

**Expected output** (3 rows, order_id 1003 is dropped):

| order_id | name  |
|----------|-------|
| 1001     | Bob   |
| 1002     | Alice |
| 1004     | Alice |
| 1005     | Carol |

---

### Phase 1: Setup — what gets created

When the query plan is compiled, the planner sees the join and creates:

```
Plan:
  HashJoin
  ├── build: TableScan(customers)  → HashBuild
  └── probe: TableScan(orders)     → HashProbe
```

The execution engine creates:

- One `HashTable<true>` (join build; `true` = ignore null keys)
- One `RowContainer` (inside the hash table) with:
  - key column: `BIGINT` (the `id` column)
  - dependent column: `VARCHAR` (the `name` column)
  - `hasNext = true` (because duplicates are possible)
- One `VectorHasher` for the `id` key column
- One `HashLookup` for use by `HashBuild`
- One `HashJoinBridge` linking the two pipelines
- One `HashLookup` for use by `HashProbe`

---

### Phase 2: Build — inserting the 3 customer rows

**`HashBuild::addInput` is called with the customers batch.**

The batch is a `RowVector` with two columns:
- `childAt(0)` = `FlatVector<int64_t>` containing `[10, 20, 30]`
- `childAt(1)` = `FlatVector<StringView>` containing `["Alice", "Bob", "Carol"]`

#### Step B1 — Prepare the lookup

```
table_->prepareForGroupProbe(lookup_, customerBatch, activeRows_, kNoSpillBit)
```

Internally:

```
VectorHasher for 'id':
  .decode(customerBatch->childAt(0), allRows)
      → decodedVector_ now holds [10, 20, 30] in uniform form

  .computeValueIds(allRows, lookup_.hashes)
      → first time seeing 10: assign id=1, store in uniqueValues_
        first time seeing 20: assign id=2
        first time seeing 30: assign id=3
      → lookup_.hashes[0] = 1  (row 0: id=10 → valueId=1)
        lookup_.hashes[1] = 2  (row 1: id=20 → valueId=2)
        lookup_.hashes[2] = 3  (row 2: id=30 → valueId=3)

populateLookupRows(allRows):
  → lookup_.rows = [0, 1, 2]  (all 3 rows are active)
```

At this point the hash mode might still be `kArray` (only 3 distinct values,
very small range). The table detects this is representable as an array of size
4 (indices 0–3) and allocates it.

#### Step B2 — Group probe (insert)

```
table_->groupProbe(lookup_, kNoSpillBit)
```

Because mode is `kArray`, this becomes `arrayGroupProbe`:

```
For row 0 (hash=1):
  table_[1] == nullptr   (slot is empty)
  → call insert lambda:
       newRow = RowContainer.newRow()   → allocates ~20 bytes, returns char* ptr_A
       table_[1] = ptr_A
       lookup_.hits[0] = ptr_A
       lookup_.newGroups.push_back(0)

For row 1 (hash=2):
  table_[2] == nullptr
  → insert: ptr_B = newRow(); table_[2] = ptr_B
    lookup_.hits[1] = ptr_B; newGroups = [0, 1]

For row 2 (hash=3):
  table_[3] == nullptr
  → insert: ptr_C = newRow(); table_[3] = ptr_C
    lookup_.hits[2] = ptr_C; newGroups = [0, 1, 2]
```

Array state after inserts:

```
table_[0] = nullptr   (unused)
table_[1] = ptr_A     → RowContainer row for Alice   (id=10)
table_[2] = ptr_B     → RowContainer row for Bob     (id=20)
table_[3] = ptr_C     → RowContainer row for Carol   (id=30)
```

#### Step B3 — Store the key and dependent columns

```
for each row index i in lookup_.newGroups:
    RowContainer.store(decodedId, i, lookup_.hits[i], /*keyCol=*/0)
    RowContainer.store(decodedName, i, lookup_.hits[i], /*depCol=*/1)
```

After this, the `RowContainer` memory looks like:

```
ptr_A:  [ null bits=0x00 ][ id=10   (8 bytes) ][ "Alice" (StringView) ][ next=nullptr ]
ptr_B:  [ null bits=0x00 ][ id=20   (8 bytes) ][ "Bob"   (StringView) ][ next=nullptr ]
ptr_C:  [ null bits=0x00 ][ id=30   (8 bytes) ][ "Carol" (StringView) ][ next=nullptr ]
```

#### Step B4 — Finalize and hand to probe

`HashBuild::noMoreInput` is called. Since there is only one build driver, it
calls `HashJoinBridge::setHashTable(table)`. This unblocks the probe pipeline.

---

### Phase 3: Probe — looking up the 5 order rows

**`HashProbe::addInput` is called with the orders batch.**

The batch has:
- `childAt(0)` = `[1001, 1002, 1003, 1004, 1005]` (order_id)
- `childAt(1)` = `[20, 10, 99, 10, 30]` (customer_id — the join key)

#### Step P1 — Prepare the lookup

```
table_->prepareForJoinProbe(lookup_, ordersBatch, activeRows_, false)
```

Internally:

```
VectorHasher for 'customer_id' (same hasher as build side):
  .lookupValueIds(ordersBatch->childAt(1), activeRows_, scratch, lookup_.hashes)
      → value 20: found in uniqueValues_ → id=2 → lookup_.hashes[0] = 2
        value 10: found → id=1 → lookup_.hashes[1] = 1
        value 99: NOT FOUND → deselect row 2 from activeRows_
        value 10: found → id=1 → lookup_.hashes[3] = 1
        value 30: found → id=3 → lookup_.hashes[4] = 3

populateLookupRows(activeRows_):
  → lookup_.rows = [0, 1, 3, 4]   (row 2 was removed — customer_id=99 unknown)
```

Row 2 (order_id=1003, customer_id=99) is already known to have no match and is
dropped before any table access.

#### Step P2 — Join probe

```
table_->joinProbe(lookup_)
```

Mode is `kArray`, so this calls `arrayJoinProbe`:

```
For row 0 (hash=2):  table_[2] = ptr_B → lookup_.hits[0] = ptr_B   (Bob's row)
For row 1 (hash=1):  table_[1] = ptr_A → lookup_.hits[1] = ptr_A   (Alice's row)
For row 3 (hash=1):  table_[1] = ptr_A → lookup_.hits[3] = ptr_A   (Alice again)
For row 4 (hash=3):  table_[3] = ptr_C → lookup_.hits[4] = ptr_C   (Carol's row)
```

`lookup_.hits` state (indexed by probe row number):

```
hits[0] = ptr_B   (order 1001 → Bob)
hits[1] = ptr_A   (order 1002 → Alice)
hits[2] = nullptr (order 1003 → no match, not probed)
hits[3] = ptr_A   (order 1004 → Alice)
hits[4] = ptr_C   (order 1005 → Carol)
```

#### Step P3 — Iterate results

```
resultIter_.reset(lookup_)
```

`JoinResultIterator` state:
```
rows    = &lookup_.rows = [0, 1, 3, 4]
hits    = &lookup_.hits
lastRowIndex = 0
nextHit = nullptr
```

`HashProbe::getOutput` calls `listJoinResults` to fill the output batch:

```
Iteration 1:
  lastRowIndex=0 → probe row = rows[0] = 0
  nextHit = hits[0] = ptr_B   (Bob)
  emit: inputRow=0, buildRow=ptr_B
  nextHit = RowContainer.nextRow(ptr_B) = nullptr  (no duplicates)
  → advance lastRowIndex to 1

Iteration 2:
  lastRowIndex=1 → probe row = rows[1] = 1
  nextHit = hits[1] = ptr_A   (Alice)
  emit: inputRow=1, buildRow=ptr_A
  nextHit = RowContainer.nextRow(ptr_A) = nullptr
  → advance to lastRowIndex=2

Iteration 3:
  lastRowIndex=2 → probe row = rows[2] = 3
  nextHit = hits[3] = ptr_A   (Alice)
  emit: inputRow=3, buildRow=ptr_A
  nextHit = nullptr → advance

Iteration 4:
  lastRowIndex=3 → probe row = rows[3] = 4
  nextHit = hits[4] = ptr_C   (Carol)
  emit: inputRow=4, buildRow=ptr_C
  nextHit = nullptr → advance
  lastRowIndex=4 == rows.size() → atEnd() = true
```

Four (inputRow, buildRow) pairs collected:

| inputRow | buildRow | Means |
|----------|----------|-------|
| 0        | ptr_B    | order 1001 matched Bob |
| 1        | ptr_A    | order 1002 matched Alice |
| 3        | ptr_A    | order 1004 matched Alice |
| 4        | ptr_C    | order 1005 matched Carol |

#### Step P4 — Build the output vectors

```
// Probe columns: extract from the probe batch using inputRows[].
for i in 0..3:
    output_order_id[i] = probeBatch->childAt(0)->valueAt(inputRows[i])
    // [1001, 1002, 1004, 1005]

// Build columns: extract from RowContainer using buildRows[].
RowContainer::extractColumn(
    buildRows,    // [ptr_B, ptr_A, ptr_A, ptr_C]
    4,
    nameColumn,
    /*columnHasNulls=*/false,
    /*resultOffset=*/0,
    output_name_vector)
// Reads the 'name' field from each row: ["Bob", "Alice", "Alice", "Carol"]
```

Final output:

| order_id | name  |
|----------|-------|
| 1001     | Bob   |
| 1002     | Alice |
| 1004     | Alice |
| 1005     | Carol |

This matches the expected result.

---

### Component interaction summary for the join

```
SQL: orders JOIN customers ON customer_id = id

                ┌──────────────────────────────────────────────────────────────┐
                │                  BUILD PHASE                                 │
                │                                                              │
  customers  →  HashBuild::addInput(batch)                                    │
                  │                                                            │
                  ├─ prepareForGroupProbe()                                    │
                  │    └─ VectorHasher.computeValueIds()                       │
                  │         10→1, 20→2, 30→3  (stored in UniqueValue set)     │
                  │         lookup_.hashes = [1, 2, 3]                        │
                  │                                                            │
                  ├─ groupProbe()                                              │
                  │    └─ kArray mode: table_[valueId] = RowContainer.newRow()│
                  │         table_[1]=ptr_A, table_[2]=ptr_B, table_[3]=ptr_C │
                  │                                                            │
                  └─ RowContainer.store() for each new row                    │
                       rows contain id + name + null bits + next pointer      │
                                                                              │
                HashBuild::noMoreInput()                                       │
                  └─ HashJoinBridge.setHashTable(table)                       │
                └──────────────────────────────────────────────────────────────┘
                         │ (future resolved, probe unblocked)
                         ▼
                ┌──────────────────────────────────────────────────────────────┐
                │                  PROBE PHASE                                 │
                │                                                              │
  orders     →  HashProbe::addInput(batch)                                    │
                  │                                                            │
                  ├─ prepareForJoinProbe()                                    │
                  │    └─ VectorHasher.lookupValueIds()                        │
                  │         20→2, 10→1, 99→NOT FOUND (row 2 removed), 10→1   │
                  │         lookup_.rows = [0, 1, 3, 4]  (row 2 dropped)      │
                  │                                                            │
                  ├─ joinProbe()                                               │
                  │    └─ kArray mode: lookup_.hits[row] = table_[valueId]    │
                  │         hits[0]=ptr_B, hits[1]=ptr_A, hits[3]=ptr_A       │
                  │         hits[4]=ptr_C                                      │
                  │                                                            │
                  └─ resultIter_.reset(lookup_)                               │
                                                                              │
                HashProbe::getOutput()                                         │
                  └─ listJoinResults()                                         │
                       for each (probeRow, buildRow):                         │
                         copy probe columns from probeBatch                   │
                         RowContainer.extractColumn() for build columns       │
                └──────────────────────────────────────────────────────────────┘
                         │
                         ▼
              Output: [(1001,Bob),(1002,Alice),(1004,Alice),(1005,Carol)]
```

---

## Appendix: Key Constants and Limits

| Constant | Value | Meaning |
|----------|-------|---------|
| `kHashTableLoadFactor` | `0.7` | Rehash when 70% of slots are occupied |
| `kArrayHashMaxSize` | `2 << 20` ≈ 2M | Maximum array size for `kArray` mode |
| `VectorHasher::kMaxDistinct` | `100,000` | Switch to full hash after this many distinct values per column |
| `VectorHasher::kMaxRange` | `~0UL >> 5` = 59 bits | Maximum combined value range for `kNormalizedKey` mode |
| Bucket size | 128 bytes | Two CPU cache lines |
| Slots per bucket | 16 | One SIMD register wide |
| Pointer size in bucket | 6 bytes | 48-bit address (fits all Linux user-space addresses) |
| `ProbeState` batch size (kHash) | 4 | Rows probed simultaneously |
| `ProbeState` batch size (kNormalizedKey join) | 64 | Wider batching for fast compare |
| `ProbeState::kEmptyTag` | `0x00` | Empty slot sentinel |
| `ProbeState::kTombstoneTag` | `0x7f` | Erased slot sentinel |

## Appendix: Glossary

| Term | Definition |
|------|-----------|
| **build side** | The smaller table in a join whose rows are stored in the hash table |
| **probe side** | The larger table whose rows are looked up in the hash table |
| **bucket** | A 128-byte unit of the hash table holding 16 slots (tags + pointers) |
| **tag** | A 1-byte fingerprint of a hash value stored inline in the bucket for fast SIMD comparison |
| **hash mode** | The strategy the table uses: `kArray`, `kNormalizedKey`, or `kHash` |
| **normalized key** | A single 64-bit value packing value IDs from all key columns |
| **value ID** | A small integer assigned to each distinct value by `VectorHasher` |
| **load factor** | The fraction of slots in use; triggers a rehash when exceeded |
| **rehash** | Doubling the table size and re-inserting all rows |
| **duplicate chain** | A linked list of build rows sharing the same key, connected via `nextOffset_` |
| **linear probing** | Moving to the next bucket when the current one is full |
| **SIMD** | Single Instruction Multiple Data — one CPU instruction operating on 16 bytes at once |
| **prefetch** | A CPU hint to start loading memory before it is needed |
| **tombstone** | A marker left in a slot after a row is erased; keeps probe chains intact |
