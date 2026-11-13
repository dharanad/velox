# GroupingSet

## What Is a GroupingSet?

`GroupingSet` is the core execution component for hash-based aggregation in
Velox. It owns a hash table, manages aggregation accumulators, and drives the
full lifecycle of a GROUP BY query — from consuming input rows to producing
aggregated output.

Every `HashAggregation` operator holds exactly one `GroupingSet`. The
`GroupingSet` is responsible for:

- Maintaining a hash table that maps each unique combination of grouping key
  values to a row of accumulators.
- Initializing and updating those accumulators as input rows arrive.
- Extracting the final (or intermediate) aggregated values for output.
- Spilling overflow state to disk and merging it back when memory is tight.

`GroupingSet` is defined in `velox/exec/GroupingSet.h` and implemented in
`velox/exec/GroupingSet.cpp`.

---

## Problem It Solves

Aggregation must group an unbounded stream of input rows by key and accumulate
state (sums, counts, distinct values, ordered values, etc.) per group. Several
concerns make this non-trivial at query-engine scale:

- **Key diversity** — groups can be identified by a single integer, a tuple of
  strings, or anything in between. The hash table must adapt its layout to the
  actual cardinality at runtime.
- **Partial vs. final aggregation** — in a distributed plan the aggregation
  runs in two stages: a partial stage on each worker that emits intermediate
  state, and a final stage that merges those results. The same code path must
  handle both.
- **Streaming input** — when the input arrives pre-sorted on the grouping keys
  (e.g. from a sort operator upstream), groups can be flushed incrementally
  rather than waiting for all input.
- **Memory limits** — the hash table may grow too large to fit in memory. The
  component must detect pressure, spill partitions to disk, and merge them back
  without duplicating groups.
- **Complex aggregation** — SQL allows `ORDER BY` inside aggregate calls
  (`array_agg(x ORDER BY y)`) and `DISTINCT` inside calls (`count(DISTINCT x)`).
  These require auxiliary bookkeeping alongside the main accumulator.

`GroupingSet` addresses all of these concerns in a single, cohesive class.

---

## SQL Concepts Modelled

| SQL Syntax | How it maps |
|------------|-------------|
| `GROUP BY a, b` | One `GroupingSet` with key columns `{a, b}` |
| `GROUP BY GROUPING SETS ((a), (b), ())` | The query planner emits one `AggregationNode` per set; each produces its own `HashAggregation` / `GroupingSet` |
| `ROLLUP(a, b)` | Expanded to `GROUPING SETS ((a,b), (a), ())` — same mechanism |
| `CUBE(a, b)` | Expanded to `GROUPING SETS ((a,b), (a), (b), ())` |
| Global aggregation `SELECT count(*) FROM t` | `GroupingSet` with no key columns; uses a single accumulator row, no hash table |

The `groupIdChannel_` field carries the output column index that stores the
grouping-set identifier for queries that combine multiple sets.

---

## Architecture

```
HashAggregation (Operator)
└── GroupingSet
    ├── BaseHashTable ──────────────────── group key → row pointer
    │   └── RowContainer ───────────────── [keys | accumulators | sorted-agg | distinct-agg]
    ├── std::vector<AggregateInfo> ─────── one entry per aggregate function
    ├── SortedAggregations ─────────────── ORDER BY inside aggregate calls
    ├── std::vector<DistinctAggregations>  DISTINCT inside aggregate calls
    ├── AggregationMasks ───────────────── per-row filter masks
    └── Spill infrastructure
        ├── AggregationInputSpiller ─────── partitions & sorts during build
        └── AggregationOutputSpiller ────── spills remaining rows during output
            └── TreeOfLosers<SpillMergeStream>  k-way merge on recovery
```

---

## Key Data Members

| Member | Type | Role |
|--------|------|------|
| `table_` | `std::unique_ptr<BaseHashTable>` | Maps group keys to accumulator rows |
| `lookup_` | `std::unique_ptr<HashLookup>` | Reusable probe state for `table_` |
| `hashers_` | `std::vector<std::unique_ptr<VectorHasher>>` | Per-column hash computation |
| `aggregates_` | `std::vector<AggregateInfo>` | Aggregate function descriptors |
| `sortedAggregations_` | `std::unique_ptr<SortedAggregations>` | Handles `agg(x ORDER BY y)` |
| `distinctAggregations_` | `std::vector<std::unique_ptr<DistinctAggregations>>` | Handles `agg(DISTINCT x)` |
| `masks_` | `AggregationMasks` | Filters rows per aggregate |
| `keyChannels_` | `std::vector<column_index_t>` | Input column indices for grouping keys |
| `preGroupedKeyChannels_` | `std::vector<column_index_t>` | Keys that arrive pre-sorted |
| `globalGroupingSets_` | `std::vector<vector_size_t>` | GROUPING SETS IDs for default output |
| `groupIdChannel_` | `std::optional<column_index_t>` | Output column for group-set ID |
| `isPartial_` | `bool` | Partial aggregation mode flag |
| `isGlobal_` | `bool` | No grouping keys (global aggregation) |
| `isRawInput_` | `bool` | Input is raw rows vs. intermediate state |
| `ignoreNullKeys_` | `bool` | Drop rows where any key is null |
| `stringAllocator_` | `HashStringAllocator` | Accumulator memory for global aggregation |
| `mergeRows_` | `std::unique_ptr<RowContainer>` | Accumulates groups during spill merge |
| `inputSpiller_` | `std::unique_ptr<AggregationInputSpiller>` | Spilling during input phase |
| `outputSpiller_` | `std::unique_ptr<AggregationOutputSpiller>` | Spilling during output phase |
| `merge_` | `std::unique_ptr<TreeOfLosers<SpillMergeStream>>` | K-way merge tree for recovery |
| `remainingInput_` | `RowVectorPtr` | Buffered rows across pre-grouped key boundaries |

---

## Lifecycle

### 1. Construction

The constructor (`GroupingSet.cpp:44`) receives:

- `inputType` and `hashers` — one `VectorHasher` per grouping key column.
- `preGroupedKeys` — key columns known to arrive already sorted.
- `aggregates` — one `AggregateInfo` per aggregate call.
- `isPartial`, `isRawInput` — aggregation step flags.
- `globalGroupingSets`, `groupIdChannel` — GROUPING SETS metadata.
- `spillConfig`, `pool` — spill and memory configuration.

The constructor extracts key channel indices, classifies each aggregate as
regular / sorted / distinct, and wires up the mask infrastructure. It does
**not** create the hash table yet.

### 2. First Input: Hash Table Creation

`createHashTable()` is called lazily on the first `addInput()`. It constructs
a `HashTable<false>` (nulls kept as groups) for standard aggregation or
`HashTable<true>` (nulls discarded) when `ignoreNullKeys_` is true. For global
aggregation (no keys) it allocates a single accumulator row instead.

### 3. addInput

```cpp
void GroupingSet::addInput(RowVectorPtr input, bool mayPushdown);
```

For each input batch:

1. If `preGroupedKeyChannels_` is non-empty and the leading pre-grouped keys
   have changed since the last batch, the current hash table is flushed
   (`resetTable`) before processing the new batch. The transition rows are
   buffered in `remainingInput_`.
2. `addInputForActiveRows` calls `table_->prepareForGroupProbe` and
   `table_->groupProbe`. The lookup returns two sets:
   - `lookup_.hits` — existing groups, one pointer per input row.
   - `lookup_.newGroups` — groups that were just created.
3. New groups have their accumulators zero-initialized via
   `aggregate.function->initializeNewGroups`.
4. All active rows update their accumulators via `addRawInput` (partial/single
   step) or `addIntermediateResults` (final/intermediate step).
5. Sorted and distinct aggregations have their inputs stashed for later
   processing.

### 4. noMoreInput

```cpp
void GroupingSet::noMoreInput();
```

Signals end of input. If sorted or distinct aggregations have pending state,
they are finalized here. If spilling is active, the remaining in-memory rows
are spilled.

### 5. getOutput

```cpp
bool GroupingSet::getOutput(
    int32_t batchSize,
    int32_t maxBytes,
    RowContainerIterator& iterator,
    RowVectorPtr& result);
```

Dispatches to the appropriate path:

- **Global aggregation** — `getGlobalAggregationOutput`: returns a single row
  with the accumulated values.
- **Default grouping set output** — `getDefaultGlobalGroupingSetOutput`:
  produces one null-key row per grouping set when no input rows arrived.
- **Spilled** — `getOutputWithSpill`: reads and merges spilled partitions
  (see Spilling below).
- **Normal** — `table_->rows()->listRows` iterates the `RowContainer` in
  batches; `extractGroups` copies keys and aggregate values to output vectors.

### 6. Reset and Reuse

`resetTable(bool deleteAll)` clears the hash table and frees accumulator
memory. It is called after each partial output flush in streaming mode and when
transitioning between pre-grouped key ranges.

---

## Partial vs. Final Aggregation

Velox follows a two-stage aggregation model for distributed queries.

| | Partial | Final |
|-|---------|-------|
| Input | Raw rows from table scan | Intermediate state from partial stage |
| Accumulator update call | `addRawInput` | `addIntermediateResults` |
| Output extraction call | `extractAccumulators` | `extractValues` |
| Output types | Intermediate serialization types | Final result types |
| Hash table purpose | Reduce input volume before shuffle | Merge partial states per group |

The `isPartial_` and `isRawInput_` flags select the correct paths throughout
`addInput` and `getOutput`.

### Abandoning Partial Aggregation

If the hash table grows very large relative to the input size the partial
aggregation is no longer effective. `abandonPartialAggregation()` detects this
and switches to a pass-through mode: each input batch is converted to
intermediate representation directly via `toIntermediate`, bypassing the hash
table entirely. The hash table is freed.

---

## Streaming Mode (Pre-Grouped Keys)

When the input is clustered on a subset of the grouping keys (e.g. the scan
returns rows sorted by `date`), `GroupingSet` can produce output before
consuming all input.

- `preGroupedKeyChannels_` lists the columns that are pre-sorted.
- On each `addInput` call, if the pre-grouped key values differ from the
  previous batch, `hasOutput()` returns `true` and the operator emits the
  current groups before processing the new batch.
- This avoids buffering the entire input and lets downstream operators start
  consuming results early.

---

## Memory Management

### Measuring Usage

```cpp
uint64_t GroupingSet::allocatedBytes() const;
```

Returns the total bytes held by the hash table (including `RowContainer`) or,
for global aggregation, the `HashStringAllocator` and allocation pool.

### Compaction

```cpp
uint64_t GroupingSet::compact();
```

Calls `compact()` on each aggregate accumulator across all groups. Some
accumulators (e.g. `array_agg`) over-allocate their internal buffers; this
reclaims that slack without touching the hash table structure.

### Partial Flush

`isPartialFull(int64_t maxBytes)` returns `true` when the hash table has grown
beyond `maxBytes`. The `HashAggregation` operator responds by calling
`getOutput` to emit the current groups, then `resetTable` to free the hash
table and start fresh. This caps peak memory without spilling.

For `kArray` hash mode the check first attempts to switch the table to a
sparser internal layout; only if memory is still over the limit does it declare
the table full.

### Spill

When compaction and partial flushing are insufficient, `GroupingSet` spills to
disk. Two spill paths exist.

---

## Spilling

### Input Spilling

Triggered during `addInput` when `ensureInputFits` cannot grow the memory
reservation:

```cpp
void GroupingSet::spill();
```

1. Creates an `AggregationInputSpiller` that partitions the hash table rows by
   a range of hash bits.
2. Within each partition, rows are sorted by grouping key for efficient merge
   later.
3. Rows are serialized with `ContainerRowSerde` and written to per-partition
   spill files.
4. The hash table is cleared, freeing memory for new input.

Subsequent input batches that hash to a spilled partition are written directly
to the spill files rather than to the hash table.

### Output Spilling

Triggered during `getOutput` when `ensureOutputFits` detects memory pressure
after `noMoreInput`:

```cpp
void GroupingSet::spill(RowContainerIterator iterator);
```

Spills all rows starting at `iterator` (rows already output are not
re-spilled). An `AggregationOutputSpiller` is created for this purpose.

### Merge Recovery

`getOutputWithSpill` handles output when spilled data exists:

1. On the first call, the spill partition set is finalized and the first
   partition's sorted streams are opened.
2. A `TreeOfLosers<SpillMergeStream>` performs a k-way sorted merge across
   the partition's streams.
3. For each key boundary in the merged stream:
   - `initializeRow` allocates a new accumulator row in `mergeRows_` and
     copies the key values.
   - `updateRow` applies intermediate aggregate results to the accumulator.
4. When the key changes, `extractSpillResult` outputs the completed group.
5. After one partition is exhausted, `prepareNextSpillPartitionOutput` opens
   the next one.

For queries with only grouping keys and no aggregates (e.g. `SELECT DISTINCT`)
the merge path uses `mergeNextWithoutAggregates`, which deduplicates rows
without accumulator logic.

---

## Complex Aggregation

### Sorted Aggregations (`agg(x ORDER BY y)`)

Handled by a `SortedAggregations` instance. During `addInput`, the input rows
and their sort keys are stashed. At `noMoreInput`, the stashed data is sorted
per group and fed to the aggregate function in order.

### Distinct Aggregations (`agg(DISTINCT x)`)

Handled by per-aggregate `DistinctAggregations` instances. Each maintains a
secondary hash table per group that deduplicates the argument values before
passing them to the aggregate function.

### Aggregate Masks

Some aggregate calls include a filter: `sum(x) FILTER (WHERE y > 0)`. The
`AggregationMasks` component computes a boolean selection vector per aggregate
and passes it to `addRawInput`, so only matching rows contribute.

---

## Global Aggregation (No Grouping Keys)

When there are no grouping keys (`SELECT count(*) FROM t`), `GroupingSet`
skips the hash table entirely:

- A single accumulator row is allocated using `HashStringAllocator` and
  `rows_` (an `AllocationPool`).
- `addGlobalAggregationInput` calls `addRawInput` / `addIntermediateResults`
  directly on this row without any hashing.
- `getGlobalAggregationOutput` extracts the single result row.
- If the query uses GROUPING SETS and no rows arrive, `getDefaultGlobalGroupingSetOutput`
  emits one null-key row per grouping set.

---

## Integration with HashAggregation

`HashAggregation` (`velox/exec/HashAggregation.h`) creates and drives the
`GroupingSet`:

```cpp
// Construction (HashAggregation.cpp:113)
groupingSet_ = std::make_unique<GroupingSet>(
    inputType,
    std::move(hashers),
    std::move(preGroupedChannels),
    std::move(groupingKeyOutputChannels),
    std::move(aggregateInfos),
    aggregationNode_->ignoreNullKeys(),
    isPartialOutput_,
    isRawInput(aggregationNode_->step()),
    aggregationNode_->globalGroupingSets(),
    groupIdChannel,
    spillConfig_.has_value() ? &spillConfig_.value() : nullptr,
    &nonReclaimableSection_,
    &operatorCtx_->driverCtx()->queryConfig(),
    operatorCtx_->pool(),
    spillStats_.get());
```

| `HashAggregation` method | `GroupingSet` call | Purpose |
|--------------------------|--------------------|---------|
| `addInput` | `addInput` | Feed a batch of rows |
| `noMoreInput` | `noMoreInput` | Signal end of stream |
| `getOutput` | `getOutput` | Pull a batch of groups |
| `reclaim` | `compact` then `spill` | Respond to memory pressure |
| `isBlocked` | `hasOutput` | Check streaming readiness |

`HashAggregation` also monitors `groupingSet_->isPartialFull(maxBytes)` after
each `addInput`. When the partial table is full it calls `getOutput` to flush
current groups and `resetTable` to reclaim memory, then resumes input
processing.

---

## Key Methods Reference

| Method | Location | Description |
|--------|----------|-------------|
| `GroupingSet(...)` | `GroupingSet.cpp:44` | Constructor |
| `createForDistinct(...)` | `GroupingSet.cpp:139` | Factory for distinct-only grouping |
| `addInput(input, mayPushdown)` | `GroupingSet.cpp:180` | Process one input batch |
| `noMoreInput()` | `GroupingSet.cpp:213` | Signal end of input |
| `hasOutput()` | `GroupingSet.cpp:271` | True when streaming output is ready |
| `getOutput(batchSize, maxBytes, iter, result)` | `GroupingSet.cpp:776` | Pull one output batch |
| `isPartialFull(maxBytes)` | `GroupingSet.cpp:862` | Check memory threshold |
| `resetTable(deleteAll)` | `GroupingSet.cpp:856` | Clear hash table |
| `allocatedBytes()` | `GroupingSet.cpp:884` | Total memory used |
| `compact()` | `GroupingSet.cpp:239` | Lightweight accumulator compaction |
| `spill()` | `GroupingSet.cpp:1052` | Spill hash table (input phase) |
| `spill(iterator)` | `GroupingSet.cpp:1103` | Spill remaining rows (output phase) |
| `hasSpilled()` | `GroupingSet.cpp:231` | True if any spilling occurred |
| `abandonPartialAggregation()` | `GroupingSet.cpp:1483` | Switch to pass-through mode |
| `toIntermediate(input, result)` | `GroupingSet.cpp:1525` | Fast intermediate conversion |
| `numDistinct()` | `GroupingSet.h:102` | Count of unique groups |
| `table()` | `GroupingSet.h:154` | Access underlying hash table |

---

## Source Files

| File | Purpose |
|------|---------|
| `velox/exec/GroupingSet.h` | Class definition, nested spiller classes |
| `velox/exec/GroupingSet.cpp` | Full implementation |
| `velox/exec/HashAggregation.h/.cpp` | Operator that owns `GroupingSet` |
| `velox/exec/AggregateInfo.h` | Per-aggregate function descriptor |
| `velox/exec/SortedAggregations.h/.cpp` | `ORDER BY` inside aggregate calls |
| `velox/exec/DistinctAggregations.h/.cpp` | `DISTINCT` inside aggregate calls |
| `velox/exec/AggregationMasks.h/.cpp` | `FILTER` inside aggregate calls |
| `velox/exec/RowContainer.h/.cpp` | Row storage for keys and accumulators |
| `velox/exec/HashTable.h/.cpp` | Hash table used for group lookup |
| `velox/exec/VectorHasher.h/.cpp` | Per-column hash computation |
| `velox/exec/tests/AggregationTest.cpp` | Aggregation integration tests |
| `velox/exec/tests/StreamingAggregationTest.cpp` | Streaming aggregation tests |
