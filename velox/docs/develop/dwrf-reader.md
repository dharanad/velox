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

### Worked Example: `ColumnReader` Reading an INTEGER Column

**Setup:** A DWRF stripe has an INTEGER column with 5 rows. The PRESENT stream
marks rows 1 and 3 as null (0-indexed). The DATA stream encodes the three
non-null values `[10, 30, 50]` in sequence.

```
PRESENT stream bytes (ByteRLE): 0b_0010_1011  → bits: 1,1,0,1,0  (0=null, 1=non-null)
  row 0 → non-null
  row 1 → null
  row 2 → non-null
  row 3 → null
  row 4 → non-null

DATA stream (RLEv1 direct): [10, 30, 50]   (only 3 values; nulls have no slot)
```

**`next(numValues=5, result, incomingNulls=nullptr)` trace:**

```
Step 1: resetIfWrongFlatVectorType<int32_t>(result)
        → result is null; flatVector = nullptr

Step 2: readNulls(5, result=null, incomingNulls=nullptr)
        notNullDecoder_->next(nullsBuf, 5, nullptr)
        → nullsBuf bits: [1, 0, 1, 0, 1]  (Velox: 1=valid, 0=null)
        nullCount = bits::countNulls(...) = 2
        returns BufferPtr nulls pointing to [1,0,1,0,1]

Step 3: values = AlignedBuffer::allocate<int32_t>(5)

Step 4: result = FlatVector<int32_t>(pool, INTEGER, nulls, nullCount=2, 5, values)

Step 5: ints->nextInts(values->asMutable<int32_t>(), 5, nullsPtr)
        Decoder reads DATA stream and writes:
          values[0] = 10   (row 0, non-null)
          values[1] = 0    (row 1, null — decoder writes 0 as placeholder)
          values[2] = 30   (row 2, non-null)
          values[3] = 0    (row 3, null — placeholder)
          values[4] = 50   (row 4, non-null)
```

**Result vector state:**

```
FlatVector<int32_t>:
  size    = 5
  nulls   = [1, 0, 1, 0, 1]   (bit 0 = row 0 valid, bit 1 = row 1 null, ...)
  values  = [10, 0, 30, 0, 50]
  nullCount = 2

vector->isNullAt(0) = false,  valueAt(0) = 10
vector->isNullAt(1) = true
vector->isNullAt(2) = false,  valueAt(2) = 30
vector->isNullAt(3) = true
vector->isNullAt(4) = false,  valueAt(4) = 50
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

### Worked Examples: `SelectiveColumnReader`

The following examples trace the state of the internal buffers step by step.
Each example uses a BIGINT column (values decoded as `int64_t`, `valueSize_ = 8`).

---

#### Example 1: Dense rows, filter `value > 25`, no nulls

**Stripe data:** 8 rows, all non-null. Encoded values: `[10, 20, 30, 40, 50, 15, 60, 5]`.

**Call:** `read(offset=0, rows=[0,1,2,3,4,5,6,7], incomingNulls=nullptr)`

```
prepareRead<int64_t>(offset=0, rows=[0..7], incomingNulls=nullptr):
  numRows = 7 + 1 = 8
  seekTo(0)                    → readOffset_ already 0, no-op
  readNulls(8, nullptr, ...)   → PRESENT stream absent; nullsInReadRange_ = nullptr
  allNull_      = false
  valueSize_    = 8
  numValues_    = 0
  outputRows_   = []           → pre-reserved (filter exists)
  ensureValuesCapacity<int64_t>(8)  → values_ has 8×8 = 64 bytes

readCommon<..., kEncodingHasNulls=false>(rows=[0..7]):
  isDense = true  (rows.back()=7 == rows.size()-1=7)
  filter  = BigintRange(26, INT64_MAX)     (value > 25)
  keepValues = true, no valueHook
  → processFilter<Reader, isDense=true, kEncodingHasNulls=false>(
        filter, ExtractToReader(this), rows)
  → readHelper<Reader, BigintRange, isDense=true>(filter, ExtractToReader(this), rows)
  → readWithVisitor(rows, ColumnVisitor<int64_t, BigintRange, ExtractToReader, true>(...))
```

**Visitor loop** (decoder calls `visitor.process(value, rowIndex)` for each row):

```
row 0: value=10  → BigintRange::testInt64(10) = false  → discard
row 1: value=20  → BigintRange::testInt64(20) = false  → discard
row 2: value=30  → BigintRange::testInt64(30) = true
                    addValue(30)        → rawValues_[0] = 30,  numValues_ = 1
                    addOutputRow(2)     → outputRows_ = [2]
row 3: value=40  → true
                    addValue(40)        → rawValues_[1] = 40,  numValues_ = 2
                    addOutputRow(3)     → outputRows_ = [2, 3]
row 4: value=50  → true
                    addValue(50)        → rawValues_[2] = 50,  numValues_ = 3
                    addOutputRow(4)     → outputRows_ = [2, 3, 4]
row 5: value=15  → false → discard
row 6: value=60  → true
                    addValue(60)        → rawValues_[3] = 60,  numValues_ = 4
                    addOutputRow(6)     → outputRows_ = [2, 3, 4, 6]
row 7: value=5   → false → discard
```

**State after `read()`:**

```
numValues_    = 4
outputRows_   = [2, 3, 4, 6]
values_       = [30, 40, 50, 60, ?, ?, ?, ?]   (first 4 slots used)
resultNulls_  = all-non-null (anyNulls_=false, no null bitmap needed)
anyNulls_     = false
```

**`getValues(rows=[2,3,4,6], result)`:**

```
getIntValues(rows, BIGINT, result)
  → getFlatValues<int64_t, int64_t>(rows, result, BIGINT)

  allNull_ = false
  valueSize_ = 8 == sizeof(int64_t) → compactScalarValues<int64_t,int64_t>(rows=[2,3,4,6])
    sourceRows = outputRows_ = [2,3,4,6]
    rows       = [2,3,4,6]
    rows.size() == numValues_ == 4  → no compaction needed
    values_->setSize(4 * 8 = 32)

  *result = FlatVector<int64_t>(pool, BIGINT, nulls=nullptr, 4, values_=[30,40,50,60])
```

**Final vector:**

```
FlatVector<int64_t>:  size=4, no nulls
  [0]=30, [1]=40, [2]=50, [3]=60
```

---

#### Example 2: Dense rows, filter `value > 25`, with nulls

**Stripe data:** 8 rows. PRESENT stream marks rows 1 and 3 as null.
DATA stream has 6 non-null values: `[10, 30, 40, 15, 60, 5]`
(rows 0, 2, 4, 5, 6, 7 in order).

**Call:** `read(offset=0, rows=[0,1,2,3,4,5,6,7], incomingNulls=nullptr)`

```
prepareRead<int64_t>:
  readNulls(8, nullptr, nullsInReadRange_):
    PRESENT stream decoded → nullsInReadRange_ bits = [1,0,1,0,1,1,1,1]
    (1=valid, 0=null; rows 1 and 3 are null)
  allNull_  = false  (not all null)
  nullsInReadRange_ NOT cleared (mixed nulls)
  prepareNulls(rows, hasNulls=true):
    isDense=true, useBulkPath()=true, !hasFilter=false → returnReaderNulls_ = false
    resultNulls_ allocated (8 bits, all set to kNotNull=1)
    rawResultNulls_ → pointer into resultNulls_
```

**Visitor loop** (decoder receives `nulls = nullsInReadRange_`):

```
row 0: nulls bit 0 = 1 (valid) → decode value=10
        BigintRange::testInt64(10) = false → discard
row 1: nulls bit 1 = 0 (null)  → addNull<int64_t>()
        anyNulls_ = true
        bits::setNull(rawResultNulls_, 0)   → resultNulls_ bit 0 = 0
        rawValues_[0] = 0                   → numValues_ = 1
        addOutputRow(1)                     → outputRows_ = [1]
row 2: valid → decode value=30
        testInt64(30) = true
        addValue(30)    → rawValues_[1]=30, numValues_=2
        addOutputRow(2) → outputRows_=[1, 2]
row 3: null → addNull<int64_t>()
        bits::setNull(rawResultNulls_, 2)   → resultNulls_ bit 2 = 0
        rawValues_[2] = 0                   → numValues_ = 3
        addOutputRow(3) → outputRows_=[1, 2, 3]
row 4: valid → value=40 → testInt64(40)=true
        addValue(40) → rawValues_[3]=40, numValues_=4
        addOutputRow(4) → outputRows_=[1,2,3,4]
row 5: valid → value=15 → testInt64(15)=false → discard
row 6: valid → value=60 → testInt64(60)=true
        addValue(60) → rawValues_[4]=60, numValues_=5
        addOutputRow(6) → outputRows_=[1,2,3,4,6]
row 7: valid → value=5 → false → discard
```

**State after `read()`:**

```
numValues_    = 5
outputRows_   = [1, 2, 3, 4, 6]
values_       = [0, 30, 0, 40, 60, ?, ?, ?]
                 ↑       ↑
              null     null
resultNulls_  bits = [0, 1, 0, 1, 1, ...]   (bit 0 and 2 cleared by addNull)
anyNulls_     = true
```

**`getValues(rows=[1,2,3,4,6], result)`:**

```
getFlatValues<int64_t, int64_t>(rows=[1,2,3,4,6], ...):
  rows.size()==5 == numValues_==5 && sizeof(int64_t)==sizeof(int64_t)
    → no compaction needed (no gaps, sizes match)

  resultNulls() → resultNulls_  (returnReaderNulls_=false, anyNulls_=true)
  *result = FlatVector<int64_t>(pool, BIGINT, resultNulls_, 5, values_)
```

**Final vector:**

```
FlatVector<int64_t>:  size=5, nullCount=2
  index 0 → null         (original row 1)
  index 1 → 30           (original row 2)
  index 2 → null         (original row 3)
  index 3 → 40           (original row 4)
  index 4 → 60           (original row 6)
```

---

#### Example 3: Sparse rows (parent struct has nulls), no column-level nulls

The struct reader determined that only rows `[0, 2, 5, 7]` have non-null struct
parents. It passes these as `rows` to the child column reader.

**Stripe data:** 8 rows in the column, all non-null. Encoded: `[10,20,30,40,50,15,60,5]`.
**incomingNulls:** bitmap `[1,0,1,0,0,1,0,1]` (1=struct parent was non-null).

**Call:** `read(offset=0, rows=[0,2,5,7], incomingNulls=[1,0,1,0,0,1,0,1])`

```
prepareRead<int64_t>(offset=0, rows=[0,2,5,7], incomingNulls):
  numRows = 7 + 1 = 8    (span to cover row 7)
  readNulls(8, incomingNulls, nullsInReadRange_):
    PRESENT stream absent → column has no own nulls
    → nullsInReadRange_ = incomingNulls copied:
      bits = [1,0,1,0,0,1,0,1]
  allNull_ = false
```

**Visitor loop** — `rows=[0,2,5,7]` is sparse (isDense=false).
The visitor checks whether the current row index is in the requested set.

```
row 0: in rows? yes. nullsInReadRange_ bit 0 = 1 (valid) → decode value=10
        filter=AlwaysTrue → addValue(10), addOutputRow(0)
        numValues_=1, outputRows_=[0]
row 1: not in rows=[0,2,5,7] → decoder skips (advances stream but visitor discards)
row 2: in rows. bit 2 = 1 → value=30 → addValue(30), addOutputRow(2)
        numValues_=2, outputRows_=[0,2]
row 3: not in rows → skip
row 4: not in rows → skip
row 5: in rows. bit 5 = 1 → value=15 → addValue(15), addOutputRow(5)
        numValues_=3, outputRows_=[0,2,5]
row 6: not in rows → skip
row 7: in rows. bit 7 = 1 → value=5 → addValue(5), addOutputRow(7)
        numValues_=4, outputRows_=[0,2,5,7]
```

**`getValues(rows=[0,2,5,7], result)`** → `FlatVector<int64_t>`:

```
size=4, no nulls
  [0]=10, [1]=30, [2]=15, [3]=5
```

---

#### Example 4: Sparse rows with both parent nulls and column nulls

**Same setup** as Example 3 (parent passes `rows=[0,2,5,7]`,
`incomingNulls=[1,0,1,0,0,1,0,1]`) but now the column itself also has nulls:
PRESENT stream marks row 2 as null.

```
prepareRead: readNulls merges PRESENT and incomingNulls:
  PRESENT bits:    [1,1,0,1,1,1,1,1]   (row 2 is null in column)
  incomingNulls:   [1,0,1,0,0,1,0,1]   (rows 1,3,4,6 have null struct parent)
  merged (AND):    [1,0,0,0,0,1,0,1]
  → nullsInReadRange_ = [1,0,0,0,0,1,0,1]
```

**Visitor loop** over rows `[0,2,5,7]`:

```
row 0: in rows. bit 0=1 (valid) → value=10 → addValue(10), addOutputRow(0)
row 2: in rows. bit 2=0 (null)  → addNull<int64_t>()
        resultNulls_ bit 1 = 0  (position 1 in output)
        rawValues_[1] = 0
        addOutputRow(2)
row 5: in rows. bit 5=1 (valid) → value=15 → addValue(15), addOutputRow(5)
row 7: in rows. bit 7=1 (valid) → value=5  → addValue(5),  addOutputRow(7)
```

**Final vector after `getValues(rows=[0,2,5,7])`:**

```
FlatVector<int64_t>:  size=4, nullCount=1
  index 0 → 10           (row 0: struct non-null, column non-null)
  index 1 → null         (row 2: struct non-null, column null)
  index 2 → 15           (row 5: struct non-null, column non-null)
  index 3 → 5            (row 7: struct non-null, column non-null)
```

---

#### Example 5: All values null — `allNull_` short-circuit

**Stripe data:** 5 rows, PRESENT stream marks all as null.

```
prepareRead:
  nullsInReadRange_ = [0,0,0,0,0]
  allNull_ = bits::isAllSet(..., kNull) = true
  nullsInReadRange_ retained (all-null)

read() body:
  if (allNull_) {
    // decoder is not called at all for data values — nothing to decode
    // outputRows_ stays empty because nothing passes any filter over null input
    return;
  }

getValues(rows, result):
  if (allNull_):
    *result = ConstantVector<int64_t>(pool, 5, /*isNull=*/true, BIGINT, 0)
    return
```

**Final result:** a `ConstantVector` of size 5 with `isNull=true`. No allocation
of a flat buffer, no bitmap, no data copy — cheapest possible null representation.

---

#### Example 6: `returnReaderNulls_` optimisation (bulk path, no filter, dense rows)

**Stripe data:** 5 rows, rows 1 and 3 null. Requested all 5 rows with no filter.

```
prepareRead:
  nullsInReadRange_ decoded → bits [1,0,1,0,1]
  allNull_ = false
  initReturnReaderNulls(rows=[0..4]):
    useBulkPath()=true (no filter, AVX2 available)
    hasFilter=false
    isDense=true
    anyNulls_=true (nullsInReadRange_ != nullptr)
    → returnReaderNulls_ = true     ← optimisation enabled

  prepareNulls(rows, hasNulls=true):
    returnReaderNulls_=true → resultNulls_ NOT allocated (skipped)
```

**Decoder runs in bulk mode** — fills `values_` for all 5 rows (nulls get 0),
does not call `addOutputRow` (no filter).

```
After read():
  numValues_    = 5
  values_       = [10, 0, 30, 0, 50]
  returnReaderNulls_ = true          ← nullsInReadRange_ will be reused as output nulls
  anyNulls_     = true

getValues(rows=[0..4], result):
  resultNulls() → nullsInReadRange_  (because returnReaderNulls_=true)
    → no copy of null bitmap, just returns the pointer

  *result = FlatVector<int64_t>(pool, BIGINT,
                nullsInReadRange_,   ← shared directly, zero copy
                5, values_)
```

**Saving:** no `resultNulls_` allocation, no null-bitmap copy — the PRESENT
stream buffer is reused directly as the output null bitmap.

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

---

## `ColumnVisitors.h` — The Visitor Pattern in Detail

**Defined in:** `velox/dwio/common/ColumnVisitors.h`

The Visitor pattern is the engine that fuses filter evaluation into the decoder
hot loop. Instead of decoding all values and filtering afterwards, the decoder
calls back into a `ColumnVisitor` for each decoded value, letting the visitor
decide whether to keep or drop it. Every branch in the visitor is resolved at
compile time via template parameters, producing a specialised, branch-free inner
loop for each (filter type × row layout × extract mode) combination.

### Template Parameters

```cpp
template <typename T, typename TFilter, typename ExtractValues, bool isDense>
class ColumnVisitor;
```

| Parameter | Meaning |
|-----------|---------|
| `T` | The decoded value type (`int64_t`, `float`, `StringView`, …) |
| `TFilter` | The compile-time filter type (e.g. `BigintRange`, `AlwaysTrue`) |
| `ExtractValues` | What to do with passing values: `DropValues`, `ExtractToReader`, `ExtractToHook` |
| `isDense` | `true` when `rows = {0,1,2,...,N-1}` — enables cheaper index arithmetic |

Four compile-time flags derived from these parameters:

| Flag | Expression | Meaning |
|------|-----------|---------|
| `kHasFilter` | `!is_same_v<TFilter, AlwaysTrue>` | There is a real filter to apply |
| `kHasHook` | `!is_same_v<HookType, NoHook>` | Values are pushed to a `ValueHook` |
| `kFilterOnly` | `is_same_v<ExtractValues, DropValues>` | Only counting/filtering; no values stored |
| `kHasBulkPath` | Template param | Whether the decoder has a SIMD fast path |

### `ExtractValues` Strategies

Three concrete implementations control what happens when a value passes the
filter:

**`DropValues`** — used when a parent needs the row set but not the values (e.g.
filter-only scan). `addValue()` and `addNull()` are no-ops. The visitor still
tracks which rows pass so the parent can reconcile null maps.

**`ExtractToReader`** — the normal case. `addValue(rowIndex, v)` calls
`reader_->addValue(v)`, which appends `v` to `values_` in `SelectiveColumnReader`.
`addNull<T>(rowIndex)` calls `reader_->addNull<T>()`, which appends a zero
and marks a bit in `resultNulls_`.

**`ExtractToHook`** / **`ExtractToGenericHook`** — used for aggregation push-down
(`LazyVector` hooks). `addValue(rowIndex, v)` calls `hook_.addValueTyped(rowIndex, v)`,
delivering values directly to the aggregate without materialising a `FlatVector`.

### `ColumnVisitor` — Core Class

**Construction** (`ColumnVisitors.h:165`):

```cpp
ColumnVisitor(filter, reader, rows, values)
```

State captured at construction:

| Member | Role |
|--------|------|
| `filter_` | The filter object (reference, zero-copy) |
| `reader_` | Back-pointer to `SelectiveColumnReader` for buffer access |
| `rows_` / `numRows_` | The sparse or dense row-number array |
| `rowIndex_` | Current position within `rows_` |
| `allowNulls_` | Precomputed: `filter.testNull() && values.acceptsNulls()` |
| `values_` | The `ExtractValues` strategy object |

#### `process(value, atEnd)` — Per-Value Hot Path

Called by the decoder for every non-null decoded value (`ColumnVisitors.h:319`):

```
applyFilter(filter_, value)
  true  → filterPassed(value) → addResult(value) + addOutputRow(currentRow())
  false → filterFailed()
advance rowIndex_
return skip count to decoder
```

`applyFilter` (defined in `Filter.h:2468`) dispatches to the correct `testXxx()`
method based on `T`:

```cpp
template <typename TFilter, typename T>
bool applyFilter(TFilter& filter, T value) {
  if constexpr (is_same_v<T, int64_t>)   return filter.testInt64(value);
  if constexpr (is_same_v<T, double>)    return filter.testDouble(value);
  if constexpr (is_same_v<T, float>)     return filter.testFloat(value);
  // ... etc.
}
```

The return value of `process()` is the number of non-null values to skip in the
stream before the next call. For dense rows it is always `0`; for sparse rows it
is `nextRow - currentRow - 1`.

#### `processNull(atEnd)` — Null Handling

Called when the PRESENT bitmap marks the current row as null (`ColumnVisitors.h:275`):

```
filter_.testNull()
  true  → filterPassedForNull() → addNull() + addOutputRow(currentRow())
  false → filterFailed()
advance rowIndex_
```

Only `IsNull` and `AlwaysTrue` filters have `testNull() == true`. Everything
else silently drops nulls.

#### `checkAndSkipNulls(nulls, current, atEnd)` — Bulk Null Skip

When the decoder has a PRESENT bitmap it calls `checkAndSkipNulls` before
reading a value (`ColumnVisitors.h:196`). The function:

1. Checks bit `current` in `nulls`.
2. If the bit is set (non-null), returns `0` immediately — fast path.
3. If the bit is clear (null), scans forward through the bitmap using
   `count_trailing_zeros` (dense mode) or manual bit counting (sparse mode)
   to find the next non-null row.
4. Returns the number of non-null *stream positions* between the current null
   and the next non-null row so the decoder can skip that many encoded values.

#### `filterFailed()` — Dropping Results

When a value fails the filter (`ColumnVisitors.h:509`):

```cpp
void ColumnVisitor::filterFailed() {
  auto preceding = filter_.getPrecedingPositionsToFail();
  auto succeeding = filter_.getSucceedingPositionsToFail();
  if (preceding) reader_->dropResults(preceding);  // undo buffered values
  if (succeeding) rowIndex_ += succeeding;          // skip rows inside complex type
}
```

`getPrecedingPositionsToFail()` and `getSucceedingPositionsToFail()` are only
non-zero for non-deterministic filters applied to nested columns (e.g. `a[1] > 10`).
For ordinary filters on flat columns both return `0` and the method is free.

#### `processLength(length, atEnd)` — String Pre-Filtering

Called before reading the actual string bytes (`ColumnVisitors.h:296`). If
`filter_.testLength(length)` returns `false` the string is rejected before any
bytes are copied from the stream. Only `BytesRange` (single-value equality) and
`BytesValues` (IN-list) implement `hasTestLength() == true`; range comparisons
cannot be decided by length alone so they skip this step.

---

### `DictionaryColumnVisitor` — Dictionary-Encoded Columns

**Defined in:** `ColumnVisitors.h:744`

Inherits `ColumnVisitor` and adds dictionary state from `RawScanState`:

| Member | Source | Purpose |
|--------|--------|---------|
| `dict()` | `state_.dictionary.values` | Pointer to the decoded dictionary values |
| `inDict()` | `state_.inDictionary` | Bit vector: which rows use the dictionary |
| `filterCache()` | `state_.filterCache` | Per-index `FilterResult` byte array |

#### Per-Value `process()` Path

For each index `value` from the stream (`ColumnVisitors.h:773`):

```
isInDict()
  false → treat index as literal value → super::process(signedValue, atEnd)
  true  → look up dict[value]
          check filterCache[value]:
            kSuccess → filterPassed(dictValue)
            kFailure → filterFailed()
            kUnknown → applyFilter(filter_, dictValue)
                         passed → filterCache[value] = kSuccess; filterPassed
                         failed → filterCache[value] = kFailure; filterFailed
```

The cache (`FilterResult` byte: `kUnknown=0x40`, `kSuccess=0x80`, `kFailure=0`)
means each distinct dictionary entry is tested against the filter exactly once per
stripe and the result is reused for all subsequent occurrences of that index.

#### SIMD Bulk Path — `processRun()`

When the decoder delivers a run of indices at once, `processRun()` processes 8
at a time using SIMD (`ColumnVisitors.h:840`):

```
1. Load 8 indices from input into an AVX2/NEON register.
2. Load the `inDict` bits for those 8 row numbers (sparse or dense mask).
3. Gather 8 filter-cache bytes in one masked gather instruction.
4. Extract `unknowns` bitmask (entries with kUnknown status).
5. Extract `passed` bitmask (entries with kSuccess status).
6. For each `unknown` bit:
     applyFilter(filter_, dict[index]) → update cache, update `passed`.
7. For entries not in dictionary: apply filter directly, update `passed`.
8. If `passed == 0`: no work, next batch.
9. If all passed: store 8 row numbers and 8 dict-translated values contiguously.
10. If partial: use SIMD permute (simd::filter) to compress passing entries
    to the left; store compressed row numbers and values.
```

This avoids per-value branches in the common case and compresses the results in
a single SIMD instruction.

#### RLE Path — `processRle()`

For RLE-encoded dictionary indices, `processRle()` first materialises the RLE
run into individual index values using SIMD arithmetic, then delegates to
`processRun()` (`ColumnVisitors.h:1010`):

```cpp
// delta-encode the row positions relative to currentRow into index values
numbers = (rows[rowIndex + i] - currentRow) * delta + value;
// then process the batch via processRun
```

---

### `StringDictionaryColumnVisitor`

**Defined in:** `ColumnVisitors.h:1203`

A specialisation of `DictionaryColumnVisitor<int32_t, ...>` where the dictionary
holds `StringView` entries rather than integers. The index stored in `values_`
at the end is a `StringView` index (not a decoded string) so that the actual
string bytes remain in the dictionary buffer and are not copied. The visitor
distinguishes between the stripe-level dictionary (`state_.dictionary`) and the
per-stride dictionary (`state_.dictionary2`):

```cpp
StringView valueInDictionary(int64_t index) {
  if (index < stripeDictSize)
    return stripe_dict[index];
  return stride_dict[index - stripeDictSize];
}
```

---

### `DirectRleColumnVisitor`

**Defined in:** `ColumnVisitors.h:1442`

Used for direct (non-dictionary) RLE-encoded integer columns. Its `processRun()`
delegates entirely to `processFixedWidthRun<T, filterOnly, scatter, isDense>()`,
which is a standalone SIMD template in `DecoderUtil.h` that fuses the filter
application, result compaction, and output-row bookkeeping into a single loop.

---

### `StringColumnReadWithVisitorHelper`

**Defined in:** `ColumnVisitors.h:1541`

A dispatcher helper used by string and binary column readers. It selects the
correct `ColumnVisitor` template instantiation based on the runtime `ScanSpec`:

```
scanSpec->keepValues()
  yes:
    has hook → ExtractToGenericHook + AlwaysTrue
    no hook  → ExtractToReader + switch(filter->kind()):
                 kIsNull         → filterNulls<T>(rows, true, true)  (no visitor)
                 kIsNotNull      → filterNulls<T>(rows, false, false) (no visitor)
                 kBytesRange     → ColumnVisitor<string_view, BytesRange, ...>
                 kBytesValues    → ColumnVisitor<string_view, BytesValues, ...>
                 ...
  no:
    DropValues + same filter dispatch
```

For `IsNull` / `IsNotNull` on columns with nulls, it bypasses the visitor
entirely and calls `filterNulls<T>()` directly because the answer depends only
on the PRESENT stream, not on any decoded value.

---

## `Filter.h` — The Filter Hierarchy

**Defined in:** `velox/type/Filter.h`

Filters are pure predicates pushed down from the query plan into the column
reader hot loop. Every filter is stateless once constructed. The hierarchy is
designed so that compile-time template specialisation on `FilterKind` eliminates
virtual dispatch inside the per-value hot path.

### `FilterKind` Enum

```
kAlwaysFalse, kAlwaysTrue,
kIsNull, kIsNotNull,
kBoolValue,
kBigintRange, kBigintValuesUsingHashTable, kBigintValuesUsingBitmask,
kNegatedBigintRange, kNegatedBigintValuesUsingHashTable, kNegatedBigintValuesUsingBitmask,
kBigintValuesUsingBloomFilter,
kDoubleRange, kFloatRange,
kBytesRange, kNegatedBytesRange, kBytesValues, kNegatedBytesValues,
kBigintMultiRange, kMultiRange,
kHugeintRange, kHugeintValuesUsingHashTable,
kTimestampRange
```

### `Filter` Base Class

**Defined in:** `Filter.h:68`

Key interface methods:

| Method | Description |
|--------|-------------|
| `testNull()` | Returns `nullAllowed_` — whether null passes this filter |
| `testInt64(v)` | Tests a scalar integer value |
| `testDouble(v)` / `testFloat(v)` | Tests a floating point value |
| `testBytes(ptr, len)` / `testStringView(v)` | Tests a string value |
| `testTimestamp(v)` | Tests a timestamp value |
| `testBool(v)` | Tests a boolean value |
| `testValues(batch<T>)` | SIMD batch test — returns `batch_bool<T>` |
| `testInt64Range(min, max, hasNull)` | Row-group pruning: can any value in [min,max] pass? |
| `testDoubleRange(...)` / `testBytesRange(...)` | Same for other types |
| `testLength(len)` | Pre-check for strings: can a string of this length pass? |
| `hasTestLength()` | Whether `testLength` is worth calling |
| `getPrecedingPositionsToFail()` | For complex type filters: entries to retroactively fail |
| `getSucceedingPositionsToFail()` | For complex type filters: entries to skip ahead |
| `mergeWith(other)` | AND-combine two filters |
| `clone(nullAllowed)` | Copy with optional null-flag override |

`deterministic` is a `static constexpr bool = true` on the base class. A filter
sets it to `false` only when applied to a nested column (e.g. `a[1] > 10` where
the filter is non-deterministic across top-level positions). The `ColumnVisitor`
checks this at compile time to decide whether to call `applyFilter` or
`isDeterministic()` at runtime.

### Key Filter Implementations

#### `AlwaysTrue` / `AlwaysFalse`

Sentinel filters. `AlwaysTrue` is the default when no filter is present in the
`ScanSpec`: all values pass, the visitor becomes a pure copy loop. `AlwaysFalse`
is rarely pushed down directly; it typically results from merging conflicting
filters.

#### `IsNull` / `IsNotNull`

Applied to any type. `IsNull::testNull() == true`, `testInt64() == false` (and
all other value tests are `false`). `IsNotNull` is the complement. The visitor's
`processNull()` path checks `filter_.testNull()`, so `IsNull` causes nulls to
pass and non-nulls to be dropped. For columns where the encoding has no nulls,
`StringColumnReadWithVisitorHelper` short-circuits to `filterNulls<T>()` rather
than running the full visitor loop.

#### `BigintRange` (`Filter.h:734`)

A closed interval `[lower, upper]` for 64-bit integers. The constructor
precomputes clamped `int32` and `int16` bounds so SIMD comparisons can use
narrower registers for `int32` and `int16` columns without widening:

```cpp
testInt64(v)  → v >= lower_ && v <= upper_
testValues(batch<int64_t>) → SIMD broadcast compare in two directions
testValues(batch<int32_t>) → uses lower32_ / upper32_, returns all-false if no overlap
testValues(batch<int16_t>) → uses lower16_ / upper16_
testInt64Range(min, max, hasNull)  // row-group pruning: !(min > upper || max < lower)
```

Used for `c > 25` (where 25 is stored as `lower=26, upper=INT64_MAX`) and range
predicates. `isSingleValue_` triggers an equality comparison instead of a range
check.

#### `BigintValuesUsingHashTable` (`Filter.h:991`)

IN-list filter for integers. Implemented as a fixed-size open-addressing hash
table using Murmur-inspired multiplicative hashing. The SIMD `testValues()` path
does a single masked gather to fetch all candidate buckets at once, produces
`passed` and `missed` bitmasks, and only falls back to scalar probing for the
unresolved lanes. Effective when the IN-list is large and values are spread
across a wide range.

#### `BigintValuesUsingBitmask` (`Filter.h:1226`)

IN-list filter for integers implemented as a dense boolean bitmask
`bitmask_[value - min_]`. Faster than hash table when `max - min` is small
(O(1) lookup via direct index). The planner calls `createBigintValues()` which
picks hash table vs. bitmask based on the range.

#### `BigintValuesUsingBloomFilter` (`Filter.h:1293`)

Probabilistic filter using a split-block Bloom filter. Used for runtime filter
push-down (build side of a hash join). False-positive rate targets 1%. `testInt64Range()` always returns `true` because the Bloom filter cannot prove
range absence.

#### `FloatingPointRange<T>` (`Filter.h:1597`)

Range filter for `float` or `double`. Handles open/closed bounds and unbounded
ends. A separate `testFloatingPoints(batch<T>)` SIMD path handles NaN: NaN
passes only when `upperUnbounded_` is true (NaN is treated as greater than
positive infinity). The cross-type specialisation `FloatingPointRange<double>::testValues(batch<float>)` falls back to scalar to handle schema evolution where a
`double` filter is applied to a `float` column.

#### `BytesRange` (`Filter.h:1882`)

Range filter for strings with optional single-value fast path:

- `singleValue_ == true`: triggered when `lower == upper` (equality). `testStringView()` does a direct `StringView ==` comparison. `testLength()` rejects immediately on wrong length. `testLengths(batch<int32_t>)` is a SIMD equality check on lengths.
- General range: compares `StringView` lexicographically against `lowerView_` / `upperView_`.
- Row-group pruning: `testBytesRange(min, max, hasNull)` checks if the column range overlaps the filter range.

#### `BytesValues` (`Filter.h:2244`)

IN-list filter for strings. Uses an `F14FastSet<std::string>` plus a
`F14FastSet<uint32_t>` of string lengths for a two-level pre-check: first
`lengths_.contains(len)`, then `values_.contains(string(ptr, len))`. This
avoids hashing strings whose length cannot possibly match any entry.

#### `BigintMultiRange` / `MultiRange`

OR-combinations. `BigintMultiRange` maintains a sorted list of
`BigintRange` objects and a precomputed `lowerBounds_` array for binary search.
`testInt64(v)` does a lower-bound search then a single range check.
`MultiRange` is the heterogeneous variant used for complex OR predicates on
strings or floats.

#### `TimestampRange` (`Filter.h:2171`)

Closed `[lower, upper]` range on `Timestamp`. `testTimestampRange()` is used
by row-group skipping.

---

### Row-Group Pruning via `testXxxRange()`

When `SelectiveColumnReader::filterRowGroups()` is called before entering a
stride, it extracts the per-stride `min` / `max` / `hasNull` statistics from the
`RowIndex` protobuf and calls the appropriate `testXxxRange()` method on the
filter. If it returns `false`, the entire stride is skipped without decoding a
single value.

```cpp
// Inside filterRowGroups() for an integer column:
bool passes = filter->testInt64Range(
    stats.getMinimum(), stats.getMaximum(), stats.hasNull());
if (!passes) stridesToSkip.set(stride);
```

The range test is conservative: it can only reject a stride, never incorrectly
accept one that would otherwise fail.

---

### Connecting Filters to Visitors: End-to-End Dispatch

The full chain from `ScanSpec` to a decoded value being tested:

```
ScanSpec::filter()                         // set by query planner; e.g. BigintRange(26, INT64_MAX)
  ↓
SelectiveIntegerColumnReader::readCommon() // SelectiveIntegerColumnReader.h
  → processFilter<isDense>(filter, rows)
      switch(filter->kind()):
        kBigintRange →
          readHelper<BigintRange, isDense>(
              static_cast<BigintRange*>(filter), rows, ExtractToReader(this))
              → ColumnVisitor<int64_t, BigintRange, ExtractToReader, isDense>(
                    *filter, this, rows, extractor)
              → readWithVisitor(rows, visitor)
                  decoder->readWithVisitor(nullsInReadRange_, visitor)
                    per-value loop:
                      value = decode_next()
                      skip = visitor.process(value, atEnd)
                               applyFilter(filter_, value)
                                 → BigintRange::testInt64(value)
                                     return value >= lower_ && value <= upper_
                               passed → ExtractToReader::addValue(rowIndex, v)
                                          → reader_->addValue(v)  // appended to values_
                                        addOutputRow(currentRow())
                               failed → filterFailed()  // no-op for simple filter
```

For dictionary-encoded columns, `readWithVisitor` calls
`visitor.toDictionaryColumnVisitor()` and the per-value loop uses the filter-cache
SIMD path. For direct-encoded columns it uses `DirectRleColumnVisitor::processRun`
which calls `processFixedWidthRun` with the filter baked into the loop.

The result: for a `BigintRange` filter on a direct integer column, the inner loop
is essentially:

```cpp
while (!atEnd) {
  auto value = rle_decode_one();
  if (value >= lower_ && value <= upper_) {
    rawValues_[numValues_] = value;
    outputRows_[numValues_] = currentRow;
    ++numValues_;
  }
  advance();
}
```

with no virtual dispatch, no heap allocation, and no branch on null (unless the
PRESENT stream has nulls, in which case `checkAndSkipNulls` handles the null
skip before any value is decoded).
