# DWRF Reader Architecture

DWRF is Velox's native columnar file format (a variant of ORC). The reader lives
under `velox/dwio/dwrf/reader/`.

---

## Top-Level Classes

**`DwrfReader`** (`reader/DwrfReader.h:234`) is the public entry point. It wraps
`ReaderBase` for file-level metadata and creates `DwrfRowReader` instances via
`createRowReader()`. A static `create()` factory registers with the connector
via `DwrfReaderFactory`.

**`DwrfRowReader`** (`reader/DwrfReader.h:45`) is the stripe/row orchestrator. It
inherits `StripeReaderBase` and `dwio::common::RowReader` and tracks the current
stripe and row position. Key methods: `next()`, `skipRows()`, `seekToRow()`, and
`checkSkipStrides()`.

**`ReaderBase`** (`reader/ReaderBase.h:61`) handles file-level metadata: footer,
postscript, schema, compression, encryption, and stripe metadata caching via
`StripeMetadataCache`.

---

## Two Column Reader Paths

Velox has two completely different column reader paths. The right one is selected
at stripe-load time based on whether a `ScanSpec` is present.

**Selective path** (with `ScanSpec`): `SelectiveDwrfReader::build()` creates
type-specific `SelectiveColumnReader` subclasses that filter and decode
simultaneously. This is the hot path for `TableScan`.

**Non-selective path** (without `ScanSpec`): `ColumnReaderFactory::build()` creates
`ColumnReader` subclasses that decode all rows unconditionally. Kept for backward
compatibility.

---

## `ColumnReader` — The Non-Selective Path

**Defined in:** `velox/dwio/dwrf/reader/ColumnReader.h:54`

### Design

`ColumnReader` is a simple recursive decoder. Each instance handles exactly one
column (or nested column). Its only job is: given N row slots, decode N values
from the raw stream and produce a `FlatVector`.

### Construction

```cpp
ColumnReader::ColumnReader(
    std::shared_ptr<const TypeWithId> fileType,
    StripeStreams& stripe,
    const StreamLabels& streamLabels,
    FlatMapContext flatMapContext)
```
(`ColumnReader.cpp:159`)

The base constructor opens the `PRESENT` stream from `StripeStreams` and wraps it
in a `ByteRleDecoder` as `notNullDecoder_`. Every type-specific subclass then
opens its own data stream (e.g., `DATA`, `DICTIONARY_DATA`) in its own constructor.

### Key APIs

| Method | Signature | Purpose |
|--------|-----------|---------|
| `next` | `virtual void next(uint64_t numValues, VectorPtr& result, const uint64_t* nulls) = 0` | Decode exactly `numValues` rows into `result`. Pure virtual. |
| `skip` | `virtual uint64_t skip(uint64_t numValues)` | Advance the stream past `numValues` rows; returns non-null count. |
| `filterRowGroups` | `virtual std::vector<uint32_t> filterRowGroups(uint64_t rowGroupSize, const StatsContext&)` | Returns stride indices that can be skipped based on column statistics. Default returns empty (skip nothing). |
| `seekToRowGroup` | `virtual void seekToRowGroup(uint32_t index)` | Reposition all internal decoders to the first row of stride `index`. |
| `readNulls` | `void readNulls(vector_size_t numValues, const uint64_t* incomingNulls, VectorPtr* result, BufferPtr& nulls)` | Reads the `PRESENT` stream via `notNullDecoder_` and merges with any parent-level nulls. |

### How Data Is Read: `IntegerDirectColumnReader::next()` Walk-Through

(`ColumnReader.cpp:580`)

```
next(numValues, result, incomingNulls)
  1. Try to reuse result as FlatVector<ReqT>. If wrong type or shared, reset it.
  2. readNulls(numValues, result, incomingNulls)
       → if notNullDecoder_ exists: decode PRESENT stream via ByteRLE into nulls buffer
       → merge with incomingNulls (parent struct nulls) using bitwise AND
       → returns BufferPtr nulls (null bitmap in Velox format: 0 = null)
  3. Count nulls and allocate values buffer (AlignedBuffer of ReqT).
  4. If result is null: create FlatVector<ReqT>(pool, type, nulls, nullCount, numValues, values)
     else:              resize existing FlatVector in-place.
  5. nextValues(*ints, values->asMutable<ReqT>(), numValues, nullsPtr)
       → calls ints->next() / nextInts() / nextShorts() on the IntDecoder
       → IntDecoder reads from the compressed DATA stream (DirectDecoder or RleDecoderV1/V2)
       → fills values buffer with decoded integers; null slots get 0 by convention
  6. Return. result now holds a fully decoded FlatVector.
```

The pattern is identical across all `ColumnReader` subtypes: read nulls first,
allocate/reuse a flat buffer, fill it from the decoder, wrap in a `FlatVector`.

### Data Flow Diagram

```
StripeStreams (compressed raw bytes)
  │
  ├─ PRESENT stream ──► ByteRleDecoder (notNullDecoder_)
  │                            │ decode N bits
  │                            ▼
  │                      BufferPtr nulls (Velox null bitmap)
  │
  └─ DATA stream ───────► IntDecoder / ByteRleDecoder / DirectDecoder
                                 │ decode N values (skipping null slots)
                                 ▼
                           BufferPtr values (typed flat array)
                                 │
                                 ▼
                        FlatVector<T>(pool, type, nulls, nullCount, N, values)
                                 │
                                 ▼
                          VectorPtr& result  (returned to caller)
```

---

## `SelectiveColumnReader` — The Selective Path

**Defined in:** `velox/dwio/common/SelectiveColumnReader.h:139`

### Design

`SelectiveColumnReader` is a two-phase decoder with integrated filtering:

1. **`read(offset, rows, incomingNulls)`** — decodes only the requested `rows`
   from the stream, applies filters inline during decoding, and accumulates
   passing values into internal scratch buffers.
2. **`getValues(rows, result)`** — packages the accumulated values into a
   `FlatVector` and hands it to the caller.

This split allows the struct reader to pass only the rows that survived a
struct-level null check down to field readers, avoiding decoding values for
null struct rows.

### Construction

```cpp
SelectiveColumnReader(
    const TypePtr& requestedType,
    std::shared_ptr<const TypeWithId> fileType,
    FormatParams& params,
    ScanSpec& scanSpec)
```
(`SelectiveColumnReader.cpp:45`)

The constructor calls `params.toFormatData(fileType, scanSpec)` to produce a
`FormatData` (for DWRF: a `DwrfData` object) that owns the null decoder and
row-group index for this column. It stores a non-owning pointer to `ScanSpec`.

### Key APIs

| Method | Signature | Purpose |
|--------|-----------|---------|
| `read` | `virtual void read(int64_t offset, const RowSet& rows, const uint64_t* incomingNulls) = 0` | Decode + filter the rows in `rows` (relative to stripe start). Pure virtual. |
| `getValues` | `virtual void getValues(const RowSet& rows, VectorPtr* result) = 0` | Package internal buffers into a `VectorPtr`. Pure virtual. |
| `next` | `virtual void next(uint64_t numValues, VectorPtr& result, const Mutation*)` | Top-level entry point; only implemented by `SelectiveStructColumnReader`, which orchestrates child field readers. |
| `seekTo` | `virtual void seekTo(int64_t offset, bool readsNullsOnly)` | Skip forward to `offset` by calling `skip()` or `FormatData::skipNulls()`. |
| `seekToRowGroup` | `virtual void seekToRowGroup(int64_t index)` | Reposition to stride `index`; resets parent-null tracking. |
| `filterRowGroups` | `virtual void filterRowGroups(uint64_t rowGroupSize, const StatsContext&, FormatData::FilterRowGroupsResult&)` | Delegates to `FormatData::filterRowGroups()` which checks column statistics against `ScanSpec` filter. |
| `outputRows` | `const RowSet outputRows() const` | Returns rows that survived the filter in the last `read()`. |
| `addOutputRow` | `inline void addOutputRow(vector_size_t row)` | Called by visitor to record a row that passed the filter. |
| `addValue<T>` | `inline void addValue(T value)` | Called by visitor to append a decoded value into `values_`. |
| `addNull<T>` | `template<typename T> inline void addNull()` | Called by visitor to append a null placeholder and mark `anyNulls_`. |

### Internal Scratch Buffers

`SelectiveColumnReader` maintains internal buffers that live for the lifetime of
one `read()` + `getValues()` cycle:

| Field | Type | Role |
|-------|------|------|
| `values_` | `BufferPtr` | Flat array of decoded (passing) values. Typed at `valueSize_` bytes per element. |
| `rawValues_` | `void*` | Writable pointer into `values_`. |
| `numValues_` | `vector_size_t` | How many values have been written into `rawValues_`. |
| `valueSize_` | `int8_t` | Fixed width per element (set during `prepareRead`). |
| `outputRows_` | `raw_vector<vector_size_t>` | Row indices that passed the filter. Used only when a filter exists. |
| `resultNulls_` | `BufferPtr` | Null bitmap for passing values, in Velox format. |
| `nullsInReadRange_` | `BufferPtr` | Full null bitmap for all `rows.back()+1` rows decoded from the `PRESENT` stream. |
| `scanState_` | `ScanState` | Dictionary and filter-cache state kept across calls. |

### `prepareRead<T>()` — Setting Up a Read

(`SelectiveColumnReaderInternal.h:60`)

Called at the top of every `read()` implementation:

```
prepareRead<T>(offset, rows, incomingNulls)
  1. numRows = rows.back() + 1   // worst-case span including gaps
  2. seekTo(offset, readsNullsOnly)
       → if offset > readOffset_: skip forward via FormatData::skipNulls or skip()
  3. FormatData::readNulls(numRows, incomingNulls, nullsInReadRange_, readsNullsOnly)
       → decode PRESENT stream for numRows bits into nullsInReadRange_
       → merge with incomingNulls (parent nulls)
  4. Detect allNull_ and clear nullsInReadRange_ if all bits are non-null.
  5. Reset outputRows_, numValues_, innerNonNullRows_, outerNonNullRows_.
  6. valueSize_ = sizeof(T)
  7. ensureValuesCapacity<T>(rows.size())  → allocate/reuse values_ buffer
  8. prepareNulls(rows, hasNulls)          → allocate/reuse resultNulls_ buffer
```

### The Visitor Pattern — How Decode + Filter Are Fused

The selective path avoids a separate filter pass by embedding filter evaluation
inside the decoder loop. This is done through the **Visitor** pattern.

A `ColumnVisitor<DataType, FilterType, ExtractValues, isDense>` is a template
struct that the decoder calls once per value. Its responsibilities:

- **`isDense`**: compile-time flag indicating whether `rows` are contiguous
  (`0,1,2,...`). Dense rows allow the decoder to skip the row-number check.
- **`FilterType`**: a filter object (e.g., `BigintRange`, `IsNull`, `AlwaysTrue`).
  Evaluated per value at the point of decoding.
- **`ExtractValues`**: one of `ExtractToReader` (stores value in `values_`),
  `ExtractToHook` (feeds value into an aggregation hook), or `DropValues`
  (filter-only, discard values).

The decoder calls `visitor.process(value, row)` for each decoded value, which:
1. Applies `FilterType::testInt64(value)` (or equivalent).
2. If passing: calls `ExtractValues::addValue(row, value)` which ultimately calls
   `reader.addValue(value)` and `reader.addOutputRow(row)`.
3. If failing: does nothing (value discarded).

### `readCommon<Reader, kEncodingHasNulls>()` — Choosing the Right Visitor

(`SelectiveIntegerColumnReader.h:254`)

```
readCommon(rows)
  isDense = (rows.back() == rows.size() - 1)
  if scanSpec_.valueHook():
      processValueHook<Reader, isDense>(rows, hook)
        → readHelper<Reader, AlwaysTrue, isDense>(rows, ExtractToHook(hook))
  else if scanSpec_.keepValues():
      processFilter<Reader, isDense, kEncodingHasNulls>(
          scanSpec_.filter(), ExtractToReader(this), rows)
  else (filter-only, no projection):
      processFilter<Reader, isDense, kEncodingHasNulls>(
          scanSpec_.filter(), DropValues(), rows)
```

`processFilter` then switches on `filter->kind()` at compile time to instantiate
the right `readHelper<Reader, ConcreteFilter, isDense>(filter, extractValues, rows)`.

`readHelper` switches on `valueSize_` to pick the right integer width (16/32/64/128
bit) and finally calls `reader.readWithVisitor(rows, ColumnVisitor<...>(...))`.

### `readWithVisitor()` in `SelectiveIntegerDirectColumnReader`

(`SelectiveIntegerDirectColumnReader.h:101`)

```cpp
template <typename ColumnVisitor>
void SelectiveIntegerDirectColumnReader::readWithVisitor(
    const RowSet& rows, ColumnVisitor visitor) {
  if (format_ == DwrfFormat::kDwrf) {
    decodeWithVisitor<DirectDecoder<true>>(intDecoder_.get(), visitor);
  } else {
    // ORC: wraps visitor in DirectRleColumnVisitor and delegates to RLE decoder
    decodeWithVisitor<RleDecoderV1<true> or RleDecoderV2<true>>(...);
  }
}
```

`decodeWithVisitor` (defined in `SelectiveColumnReader.h:617`) extracts
`nullsInReadRange_` and calls `decoder->readWithVisitor<hasNulls>(nulls, visitor)`.
The decoder then iterates over its internal run buffer and calls `visitor.process()`
per value, handling null skipping based on `nullsInReadRange_`.

### `getValues()` and `getFlatValues<T, TVector>()` — Packaging the Result

After `read()`, the caller calls `getValues(rows, result)`.

For integer types, `SelectiveIntegerColumnReader::getValues()` calls
`getIntValues(rows, requestedType_, result)`, which calls
`getFlatValues<int64_t, RequestedType>(rows, result, type)`.

(`SelectiveColumnReaderInternal.h:101`)

```
getFlatValues<T, TVector>(rows, result, type)
  if allNull_:
      *result = ConstantVector<TVector>(pool, rows.size(), /*isNull=*/true)
      return

  if valueSize_ == sizeof(TVector):
      compactScalarValues<TVector,TVector>(rows, isFinal)
        → keep only values at positions in `rows` (filter may have produced gaps)
        → compact in-place in rawValues_
  else if sizeof(T) >= sizeof(TVector):
      compactScalarValues<T,TVector>(rows, isFinal)   // downcast + compact
  else:
      upcastScalarValues<T,TVector>(rows)              // widen then compact

  *result = FlatVector<TVector>(
      pool, type,
      resultNulls(),    // null bitmap (nullsInReadRange_ if returnReaderNulls_, else resultNulls_)
      numValues_,       // number of passing values
      values_,          // compacted value buffer
      std::move(stringBuffers_))
```

### Data Flow Diagram

```
StripeStreams (compressed bytes)
  │
  ├─ PRESENT stream ──► FormatData::readNulls()
  │                            │
  │                            ▼
  │                     nullsInReadRange_  (full null bitmap, rows.back()+1 bits)
  │
  └─ DATA stream ───────► IntDecoder (DirectDecoder or RleDecoder)
                                 │
                                 │  decoder->readWithVisitor(nulls, visitor)
                                 │    per value:
                                 │      ├─ check row index (sparse path) or iterate (dense)
                                 │      ├─ check null bit from nullsInReadRange_
                                 │      │     → null: reader.addNull<T>()  → writes 0 to values_[numValues_++], marks resultNulls_
                                 │      └─ non-null: FilterType::test(value)
                                 │              pass:  reader.addValue(value) → values_[numValues_++] = value
                                 │                     reader.addOutputRow(row) → outputRows_.push_back(row)
                                 │              fail:  discard
                                 ▼
                           values_[0..numValues_-1]  (passing, uncompacted)
                           outputRows_[0..N-1]       (row indices that passed)
                                 │
                          getValues(rows, result)
                                 │
                          compactScalarValues()       (remove non-selected entries)
                                 │
                                 ▼
                        FlatVector<TVector>(pool, type, resultNulls(), numValues_, values_)
                                 │
                                 ▼
                          VectorPtr* result           (returned to SelectiveStructColumnReader)
```

---

## `SelectiveColumnReader` Member Variables In Depth

`SelectiveColumnReader` (`velox/dwio/common/SelectiveColumnReader.h:139`) has many
member variables that interact in non-obvious ways. The sections below group them
by responsibility and explain when and where each is written and read.

### Position Tracking

**`readOffset_`** `(int64_t, line 668)`

The row number of the next unread row, measured from the start of the current
stripe. This is the single source of truth for where the reader's stream
decoders are positioned.

- Written in `prepareRead()` (via `seekTo()`) and after every `read()`.
- Read in `seekTo()` to compute the skip distance:
  `skip(offset - readOffset_ - numParentNulls_)`.
- Must match the stream position exactly; desync causes wrong data.

---

### Parent-Null Tracking (for nested columns)

These two variables only matter for columns that live inside a nullable struct,
list, or map — where the parent can be null and the child therefore has no
encoded value for that row.

**`numParentNulls_`** `(int32_t, line 673)`

The count of null parent rows in the range `[readOffset_, parentNullsRecordedTo_)`.
Because the child stream has no value for a null parent row, skipping N top-level
rows actually advances the child stream by only `N - numParentNulls_` positions.

- Accumulated by `addParentNulls(firstRowInNulls, nulls, rows)` (called by the
  struct reader before it reads each child).
- Consumed and reset to zero in `seekTo()`.
- Also accumulated by `addSkippedParentNulls(from, to, numNulls)` when the struct
  reader skips a range without reading it.

**`parentNullsRecordedTo_`** `(int32_t, line 677)`

The top-level row number up to which `numParentNulls_` has been counted.
`addParentNulls()` sets this to `firstRowInNulls + rows.back() + 1`.
`addSkippedParentNulls()` sets it to `to`. This prevents double-counting across
consecutive calls.

---

### Row Sets

**`inputRows_`** `(RowSet, line 681)`

A non-owning view of the `RowSet` passed to `read()`. The values must remain
live (owned by the caller) until the next call to `read()`.

Used in `compactScalarValues()` as the fallback `sourceRows` when neither
`valueRows_` nor `outputRows_` is populated (i.e., no filter and no prior
`getValues()` call).

**`outputRows_`** `(raw_vector<vector_size_t>, line 684)`

Row indices (relative to stripe start) that passed the filter. One entry is
appended per passing row by `addOutputRow(row)` inside the visitor loop.

- Populated only when a filter exists (`useOutputRows()` is true), i.e. when
  `scanSpec_->hasFilter()` or there is row-level deletion.
- Reset to empty at the start of each `prepareRead()`.
- Read by `compactScalarValues()` as `sourceRows` when `valueRows_` is empty but
  a filter was active.
- Also doubles as the row-to-value mapping for the bool specialization of
  `compactScalarValues<bool,bool>()`.

**`valueRows_`** `(raw_vector<vector_size_t>, line 686)`

Row numbers corresponding to the elements currently in `values_`, maintained
across multiple `getValues()` calls on the same read result. This is needed
when a flat map or other caller requests values for a subset of `outputRows_`
more than once.

- Empty after `read()` completes; populated lazily by `compactScalarValues()`
  and `upcastScalarValues()` on the first `getValues()` call (when `isFinal` is
  false).
- On subsequent `getValues()` calls, `compactScalarValues()` uses `valueRows_`
  (not `outputRows_`) as `sourceRows` so it can correctly select from the
  already-compacted buffer.
- Cleared at the start of each `prepareRead()` indirectly (via `outputRows_.clear()`).

---

### Null Bitmaps

The reader maintains two separate null bitmaps — one over the full read range
and one over the passing values — because the filter may discard some rows.

**`nullsInReadRange_`** `(BufferPtr, line 691)`

A null bitmap covering all `rows.back() + 1` rows in the read range (including
rows that will be filtered out). Bit 0 = null in Velox convention.

- Filled by `FormatData::readNulls()` inside `prepareRead()`. Merges the column's
  own `PRESENT` stream with any incoming parent nulls.
- Passed directly to the decoder in `decodeWithVisitor()` so the visitor can
  branch per row: call `addNull<T>()` for null bits and `addValue(v)` for non-null.
- Set to `nullptr` when the bitmap is all-non-null (optimization in `prepareRead()`).
- Reused as the output null buffer by `resultNulls()` when `returnReaderNulls_`
  is true (see below).

**`resultNulls_` / `rawResultNulls_`** `(BufferPtr / uint64_t*, lines 694–695)`

A null bitmap indexed over passing values (position in `outputRows_`), not over
all rows. Written by `addNull<T>()` via `bits::setNull(rawResultNulls_, numValues_)`.

- Allocated/reused in `prepareNulls()`. Pre-cleared to all-non-null.
- Handed to the `FlatVector` in `getFlatValues()` via `resultNulls()`.
- Not allocated when `returnReaderNulls_` is true (the full bitmap can be reused).
- `rawResultNulls_` is the raw writable pointer into `resultNulls_`; kept as a
  separate field because `addNull<T>()` is called millions of times and avoiding
  the `BufferPtr` indirection matters.

**`returnReaderNulls_`** `(bool, line 730)`

An optimization flag: when true, `resultNulls_` is not maintained and
`nullsInReadRange_` is returned directly as the output null buffer from
`resultNulls()`.

This is valid only when all input rows are selected (no filter) and the rows are
dense (contiguous from 0), so the bitmap indices match exactly.

- Set to true by `initReturnReaderNulls()` when `useBulkPath() && !hasFilter() && isDense`.
- Reset to false in `shouldMoveNulls()` before compaction (compaction may select
  a subset, invalidating the assumption), and in `setNulls()`.

**`anyNulls_`** `(bool, line 737)`

True if at least one null was written during the current `read()`. Guards whether
`resultNulls()` returns the null buffer or a statically null `BufferPtr`.

- Set by `addNull<T>()` and by `initReturnReaderNulls()`.
- Reset to false in `prepareNulls()` at the start of each read.

**`allNull_`** `(bool, line 739)`

True if every row in the read range is null. Short-circuits `getFlatValues()`:
instead of building a `FlatVector`, it returns a `ConstantVector<TVector>` with
`isNull = true`, which is cheaper and signals to the caller that no values need
to be processed.

- Set in `prepareRead()` by checking `bits::isAllSet(..., bits::kNull)` on the
  null bitmap.

---

### Value Accumulation Buffer

**`values_`** `(BufferPtr, line 697)` and **`rawValues_`** `(void*, line 699)`

The flat scratch buffer into which decoded, passing values are written during the
visitor loop. Each element occupies exactly `valueSize_` bytes (set per call in
`prepareRead<T>()`).

- Allocated/resized in `ensureValuesCapacity<T>()`.
- Written by `addValue<T>(value)` (inline, at `rawValues_[numValues_]`) and by
  `addNull<T>()` (writes a zero-initialized `T` to keep the index consistent).
- After the visitor loop, `values_` holds `numValues_` packed values, but some
  may correspond to rows not in the final requested `rows` set (due to prior
  filter passes on a superset). `compactScalarValues()` removes those.
- Handed to `FlatVector` in `getFlatValues()` after compaction. The `FlatVector`
  takes shared ownership, so no copy occurs.
- `rawValues_` is the raw `void*` into `values_->mutable<char>()`, kept as a
  separate field to avoid `BufferPtr` indirection in the hot path.

**`numValues_`** `(vector_size_t, line 700)`

Index of the next free slot in `rawValues_`. Incremented by every `addValue()`
and `addNull<T>()` call. After compaction equals the number of selected rows.

**`valueSize_`** `(int8_t, line 704)`

The byte width of each element stored in `values_`. Set at the start of
`prepareRead<T>()` to `sizeof(T)`. Used by `getFlatValues<T,TVector>()` to
decide between in-place compaction (same size) and narrowing compaction or
widening copy (different sizes).

Concrete example: integers are always decoded at 64-bit width regardless of the
declared column type. `getIntValues()` passes the file type `T = int64_t` and the
requested type as `TVector` (e.g. `int32_t`). `getFlatValues` then calls
`compactScalarValues<int64_t, int32_t>()` to narrow and compact in one pass.

**`mayGetValues_`** `(bool, line 707)`

A correctness guard. Set to true in `prepareRead()` and to false in
`getFlatValues()` when `isFinal = true`. `getFlatValues()` asserts
`VELOX_CHECK(mayGetValues_)` at entry. This detects callers that call
`getValues()` twice with `isFinal=true` or call it without a preceding `read()`.

---

### Non-Null Row Index Helpers

These vectors support complex type readers (lists, maps) that need to translate
between top-level row indices and child-element indices, where the parent may be
null for some rows.

**`outerNonNullRows_`** `(raw_vector<int32_t>, line 717)`

Maps from a position in the sequence of non-null rows to the corresponding
position in the full row sequence (including nulls). In other words, if the
reader has a null at top-level row 3, `outerNonNullRows_[2]` (the third
non-null row) might be 4 (the fourth row overall). Used by list/map readers to
reconstruct offsets that reference into child element arrays.

**`innerNonNullRows_`** `(raw_vector<int32_t>, line 720)`

Rows of the qualifying set expressed in non-null row indices. When the parent
has some nulls, this holds the subset of `outputRows_` translated to non-null
positions, so child readers can be called with a correctly-sized row set.

---

### String Storage

String values (and large `StringView`s) are backed by owned buffers rather than
pointing into the stream's memory.

**`stringBuffers_`** `(std::vector<BufferPtr>, line 722)`

A chain of buffers holding character data for strings decoded into `values_`.
`StringView` elements in `rawValues_` point into these. Handed to the
`FlatVector` constructor in `getFlatValues()` via `std::move` so the vector
takes ownership without copying.

**`rawStringBuffer_` / `rawStringSize_` / `rawStringUsed_`** `(lines 724–734)`

Working state for the current (last) buffer in `stringBuffers_`. `copyStringValue()`
attempts a bump-allocator-style append into `rawStringBuffer_[rawStringUsed_]`.
When `rawStringUsed_ + size > rawStringSize_`, a new `BufferPtr` (minimum 16 KB)
is appended to `stringBuffers_` and the three fields are reset.

`rawStringSize_` is `capacity - simd::kPadding` so that SIMD stores at the end
of the buffer don't overrun.

---

### Encoding State

**`scanState_`** `(ScanState, line 745)`

Holds dictionary and filter-cache state that persists across `read()` calls
within a stripe (dictionaries are set up once at the start of a stripe or row
group and reused for every batch).

Key fields of `ScanState`:

| Field | Type | Role |
|-------|------|------|
| `dictionary` | `DictionaryValues` | Global (stripe-level) dictionary: a `BufferPtr` of decoded values and for strings a `BufferPtr` of character data. |
| `dictionary2` | `DictionaryValues` | Secondary (row-group-level) dictionary in ORC/DWRF stride-dictionary encoding. |
| `inDictionary` | `BufferPtr` | One bit per value: 1 = index is in `dictionary`, 0 = use `dictionary2` or literal. |
| `filterCache` | `raw_vector<uint8_t>` | One byte per dictionary entry: `kUnknown`, `kPass`, or `kFail`. Caches the result of applying the filter to a dictionary value so each unique value is tested at most once per filter reset. Reset by `resetFilterCaches()`. |
| `rawState` | `RawScanState` | Raw-pointer copies of the above for use inside the hot visitor loop without `BufferPtr` indirection. Kept in sync by `updateRawState()`. |

---

### Optimization Flags

**`isTopLevel_`** `(bool, line 713)`

True when this reader's row numbers correspond 1:1 to stripe row numbers, i.e.,
the reader is not nested inside a list, map, or nullable struct. Top-level
readers can use row group indices for long-distance seeks. Lazy vectors are also
only created for top-level readers.

Set by `setIsTopLevel()`, which is called recursively through non-nullable structs
by `DwrfRowReader`.

**`mayUseStreamBuffer_`** `(bool, line 727)`

If true, the reader is permitted to take a pin on the underlying compressed
stream's memory and use it directly as `values_`, avoiding a copy. Only set for
formats/encodings that guarantee the buffer remains live long enough.

---

### Flat Map State

These fields only apply when `isFlatMapValue_` is true, which happens when this
reader decodes a value column inside a flat map (`MAP_FLAT` encoding).

**`isFlatMapValue_`** `(bool, line 751)`

Signals that `nullsInReadRange_` and `values_` are never shared outside the file
reader, so they can be reused across reads regardless of their reference counts.
This avoids allocations in the flat map hot path.

**`flatMapValueNullsInReadRange_`** `(BufferPtr, line 757)`

Saved copy of `nullsInReadRange_` from the previous read for a flat map value.
When the flat map switches between a null-key and a non-null-key column, the null
bitmap can be reused rather than reallocated.

**`flatMapValueFlatValues_`** `(VectorPtr, line 758)`

Reused `FlatVector` returned from `getFlatValues()` for flat map value columns.
`getFlatValues()` updates its size, nulls, and values in place rather than
constructing a new `shared_ptr<FlatVector<TVector>>`.

**`flatMapValueConstantNullValues_`** `(VectorPtr, line 759)`

Reused `ConstantVector(isNull=true)` for when all flat map values in a read are
null. Avoids allocating a new constant vector each time.

---

## Differences Between `ColumnReader` and `SelectiveColumnReader`

| Aspect | `ColumnReader` | `SelectiveColumnReader` |
|--------|---------------|------------------------|
| **When used** | No `ScanSpec`; backward compat path | `ScanSpec` present; normal `TableScan` path |
| **Row set** | Always reads a contiguous block of `numValues` rows | Reads a sparse `RowSet` (arbitrary row indices within a stride) |
| **Filtering** | None — all rows decoded, all values returned | Filter applied per-value inside the decoder loop via `ColumnVisitor` |
| **Output rows** | Always `numValues` rows in result | Only rows passing the filter; `outputRows_` tracks which |
| **API shape** | Single `next(numValues, result, nulls)` call | Two-phase: `read(offset, rows, nulls)` then `getValues(rows, result)` |
| **Null handling** | Reads `PRESENT` stream, merges with `incomingNulls`, passes combined nulls to decoder | Reads nulls into `nullsInReadRange_`, visitor reads per-bit during decode to call `addNull<T>()` vs `addValue(v)` |
| **Output vector** | Directly allocates/reuses a `FlatVector`; the result buffer is owned by the vector | Accumulates into internal `values_` scratch buffer, then wraps in a `FlatVector` in `getValues()` |
| **Buffer reuse** | Reuses the incoming `VectorPtr` if type/uniqueness match | Reuses `values_`, `resultNulls_`, `outputRows_` across calls; new `FlatVector` is a thin wrapper each time |
| **Compaction** | No compaction needed — all decoded values are kept | `compactScalarValues()` removes values for non-selected rows after filter |
| **Aggregation hooks** | Not supported | Supported via `ExtractToHook` — values flow directly into aggregation without materializing a vector |
| **Stride seeking** | `seekToRowGroup(uint32_t index)` repositions decoders | `seekToRowGroup(int64_t index)` additionally resets `numParentNulls_` / `parentNullsRecordedTo_` |
| **Parent-null tracking** | Propagated via `incomingNulls` parameter | Tracked internally via `numParentNulls_` + `addParentNulls()` to correctly account for gaps when seeking |
| **Dictionary state** | Managed per-reader (e.g., `IntegerDictionaryColumnReader` holds dict buffer) | Shared via `ScanState scanState_` which also holds a `filterCache` indexed by dictionary index |
| **Encoding location** | `velox/dwio/dwrf/reader/ColumnReader.{h,cpp}` | Base: `velox/dwio/common/SelectiveColumnReader.{h,cpp}`; DWRF specifics: `velox/dwio/dwrf/reader/Selective*.h` |

---

## Stripe Loading: `DwrfUnit`

`DwrfUnit` (defined inline in `reader/DwrfReader.cpp:39`) implements the `LoadUnit`
interface for lazy stripe loading:

1. `ensureDecoders()` — fetches stripe metadata via `StripeReaderBase::fetchStripe()`,
   creates `StripeStreamsImpl`, and builds column readers.
2. `loadDecoders()` — triggers actual I/O via `stripeStreams_->loadReadPlan()`.
3. `unload()` — releases resources when done.

`StripeStreamsImpl` (`reader/StripeStream.h:221`) manages stream maps: `streams_`
maps `DwrfStreamIdentifier → StreamInformationImpl`, and `encodings_` maps
`EncodingKey → encoding index`.

A `UnitLoader` (referenced in `DwrfRowReader`) manages async/parallel loading of
multiple units and is notified via `onRead()` before each stripe is accessed.

---

## Row Group (Stride) Filtering

`DwrfRowReader::checkSkipStrides()` (`reader/DwrfReader.cpp:517`) calls
`filterRowGroups()` on the selective reader with per-stride statistics from the
`RowIndex` proto. Strides whose statistics don't satisfy the predicate are skipped
entirely — no I/O, no decode.

Each column reader implements `seekToRowGroup(index)` to advance its decoders to a
specific row group when a stride is not skipped.

---

## Encoding and Stream Kinds

A column's data is split into multiple sub-streams identified by
`DwrfStreamIdentifier = (nodeId, sequence, StreamKind)`.

| Stream | Purpose |
|--------|---------|
| `PRESENT` | Null bitmaps |
| `DATA` | Main column values |
| `LENGTH` | String/array lengths |
| `DICTIONARY_DATA` | Dictionary values |
| `DICTIONARY_COUNT` | Dictionary size/counts |
| `NANO_DATA` | Timestamp nanoseconds |
| `ROW_INDEX` | Per-row-group statistics and positions |
| `IN_DICTIONARY` | Which indices use the dictionary |
| `STRIDE_DICTIONARY` | Per-stride string dictionaries |
| `STRIDE_DICTIONARY_LENGTH` | Per-stride dictionary sizes |
| `IN_MAP` | Key presence in flat maps |

Supported encoding kinds: `DIRECT`, `DIRECT_V2`, `DICTIONARY`, `DICTIONARY_V2`,
`MAP_FLAT`. Integer sequences use RLEv1 (DWRF format) or RLEv2 (ORC format),
selected via `convertRleVersion()` in `DwrfData.h`.

---

## End-to-End Reading Flow

```
TableScan operator
  → DwrfReader::createRowReader()
    → DwrfRowReader::next()
        loadCurrentStripe()               // lazy: DwrfUnit::ensureDecoders + loadDecoders
        checkSkipStrides()                // stride-level stats filtering; no I/O on skipped strides
        SelectiveStructColumnReader::next(numValues, result, mutation)
          for each child field in ScanSpec:
            child->read(offset, rows, parentNulls)
              prepareRead<T>()            // seek, read nulls, allocate scratch buffers
              readCommon()                // pick Visitor based on filter + denseness
                readWithVisitor(rows, ColumnVisitor<...>)
                  decoder->readWithVisitor(nullsInReadRange_, visitor)
                    per value: test filter → addValue(v) + addOutputRow(r)   OR   addNull()
            child->getValues(outputRows, &fieldVector)
              getFlatValues<T,TVector>()
                compactScalarValues()     // remove non-selected entries
                wrap values_ in FlatVector
          assemble RowVector from field FlatVectors
          → result VectorPtr returned to TableScan
```
