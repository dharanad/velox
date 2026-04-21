# DWRF Reader: Schema / Type Conversion

## Motivation

DWRF files written with one schema are often read with an evolved schema where
column types have changed. Common cases:

- Table schema updated to widen an `INTEGER` column to `BIGINT` while old files
  still store `INTEGER`.
- A column stored as `REAL` is re-declared as `DOUBLE` in the metastore.
- A decimal column's precision or scale changed between table versions.
- A numeric column is later exposed as `VARCHAR` for display.

Today the DWRF reader calls `checkTypeCompatibility()` in two places and throws
`SCHEMA_MISMATCH` for any pair not in a narrow whitelist of widening-only integer
promotions. This document proposes extending the reader to support the full set
of meaningful type conversions without adding the Apache ORC C++ library as a
dependency.

Reference: the ORC implementation lives in
`apache/orc:c++/src/ConvertColumnReader.cc`. We cannot use it directly but use
it as a reference for the conversion logic.

---

## Scope

### In scope (Phase 1)

| File type | Requested type | Behaviour |
|-----------|---------------|-----------|
| BOOLEAN | TINYINT / SMALLINT / INTEGER / BIGINT | Widen: false→0, true→1 |
| TINYINT | SMALLINT / INTEGER / BIGINT | Widen; always safe |
| SMALLINT | INTEGER / BIGINT | Widen; always safe |
| INTEGER | BIGINT | Widen; always safe |
| INTEGER | REAL / DOUBLE | Widen; precision loss acceptable |
| BIGINT | DOUBLE | Widen; precision loss acceptable |
| REAL | DOUBLE | Widen; already works |
| BIGINT | INTEGER / SMALLINT / TINYINT | Narrow; out-of-range → null |
| DOUBLE | REAL | Narrow; infinite/NaN / out-of-range → null |
| DOUBLE | INTEGER / BIGINT | Narrow; NaN / out-of-range → null |
| REAL | INTEGER | Narrow; NaN / out-of-range → null |
| SHORT\_DECIMAL / LONG\_DECIMAL | SHORT\_DECIMAL / LONG\_DECIMAL | Rescale; overflow → null |
| INTEGER / BIGINT / REAL / DOUBLE | SHORT\_DECIMAL / LONG\_DECIMAL | Scale up by 10^targetScale |
| SHORT\_DECIMAL / LONG\_DECIMAL | BIGINT / DOUBLE | Scale down, overflow → null |
| Any numeric | VARCHAR / VARBINARY | `std::to_string()` / fixed-point formatting |
| VARCHAR / VARBINARY | Any numeric | Parse; parse failure → null |
| TIMESTAMP | BIGINT | Extract epoch seconds |
| BIGINT | TIMESTAMP | Interpret as epoch seconds |

### Out of scope (Phase 1)

- Struct / Array / Map container-level conversion (element readers are handled
  recursively; the container structure itself is unchanged).
- CHAR type padding / truncation.
- TIMESTAMP\_WITH\_TIMEZONE ↔ TIMESTAMP (timezone-aware conversion).
- Non-selective (`ColumnReader`) path — only the `SelectiveColumnReader` path is
  addressed.

---

## Current State

### Where the guard sits today

```
SelectiveDwrfReader::build()            SelectiveDwrfReader.cpp:73
  └── checkTypeCompatibility(fileType, requestedType)   TypeUtils.cpp:165
        └── isCompatible(from, to)
              map: {BOOL→TINYINT, TINYINT→SMALLINT, ..., REAL→DOUBLE}
              throws SCHEMA_MISMATCH for anything else

DwrfRowReader::startNextStripe()        DwrfReader.cpp:327
  └── checkTypeCompatibility(schema, columnSelector)

ColumnReader::build()                   ColumnReader.cpp:2550  (non-selective)
  └── checkTypeCompatibility(fileType, requestedType)
```

### What already works implicitly

The integer selective readers (`SelectiveIntegerDirectColumnReader`,
`SelectiveIntegerDictionaryColumnReader`) always decode into `int64_t` in
`values_`, then `getIntValues()` dispatches on `requestedType_->kind()` and calls
`getFlatValues<int64_t, TVector>()` with the narrowing / upcasting handled by
`compactScalarValues`. This means **integer widening within the int family already
works correctly in the reader** — the only blocker is the `checkTypeCompatibility`
guard at build time.

`SelectiveFloatingPointColumnReader<float, double>` already converts REAL→DOUBLE.

---

## Can We Support This With Minimal Changes?

### Widening numeric conversions — yes, minimal changes work

For all integer widening cases (TINYINT→INTEGER, INTEGER→BIGINT, etc.) and
REAL→DOUBLE:

1. Expand `isCompatible()` in `TypeUtils.cpp` to include the new pairs.
2. In `SelectiveDwrfReader::build()`, after the compatibility check, keep the
   dispatch on `fileType->type()->kind()` but pass `requestedType` to the reader
   constructor (already done). The existing `getIntValues()` / `getFlatValues()`
   machinery handles the rest without any reader changes.

This is zero-risk: all existing tests pass, new widening cases just work.

**Why this is insufficient for everything else:**
- **Narrowing** (BIGINT→INTEGER): `compactScalarValues<int64_t, int32_t>()` does a
  `static_cast<int32_t>` with no overflow check. Values ≥ 2^31 silently wrap
  around. We must null them instead.
- **Cross-domain** (INTEGER→DOUBLE, INTEGER→VARCHAR): The visitor pattern is
  templated on `DataType`. `addValue<int64_t>()` writes an `int64_t` into
  `rawValues_`. There is no path that converts it to `double` or `StringView`
  in the same buffer without restructuring the visitor.
- **String↔numeric**: Completely different decoders; the reader for VARCHAR cannot
  share the integer decode path.
- **Decimal rescaling**: Requires per-element arithmetic that does not fit in any
  existing `getFlatValues<T, TVector>()` instantiation.

Attempting to handle these cases inside the existing readers would require each
reader to carry conversion logic for every possible target type, leading to an
O(M×N) combination of template instantiations and making the hot path larger.

**Conclusion:** A post-decode conversion wrapper, modelled on ORC's
`ConvertColumnReader`, is the right approach for non-trivial conversions.

---

## Proposed Design

### Overview

```
             SelectiveDwrfReader::build()
                        │
                        ├─ file type == requested type
                        │      └── existing reader (unchanged)
                        │
                        ├─ widening int / REAL→DOUBLE
                        │      └── existing reader with relaxed guard (minimal change)
                        │
                        └─ narrowing / cross-domain
                               ├── build inner reader for file type
                               │     (with a stripped ScanSpec: no filter)
                               └── wrap in ConvertingSelectiveColumnReader
                                     ├── read()     → inner_->read()
                                     ├── convert()  → element-wise type conversion
                                     ├── filter()   → apply original ScanSpec filter
                                     └── getValues()→ wrap converted buffer in FlatVector
```

### 1. New class: `ConvertingSelectiveColumnReader`

Location: `velox/dwio/common/ConvertingSelectiveColumnReader.h/.cpp`

The class wraps an inner `SelectiveColumnReader` that reads the file type. After
the inner reader finishes `read()`, the wrapper converts the result in `getValues()`.

```cpp
class ConvertingSelectiveColumnReader : public SelectiveColumnReader {
 public:
  ConvertingSelectiveColumnReader(
      const TypePtr& requestedType,
      std::shared_ptr<const dwio::common::TypeWithId> fileType,
      std::unique_ptr<SelectiveColumnReader> inner,
      velox::common::ScanSpec& scanSpec);

  void read(int64_t offset, const RowSet& rows, const uint64_t* incomingNulls)
      override;

  void getValues(const RowSet& rows, VectorPtr* result) override;

  uint64_t skip(uint64_t numValues) override;

  void seekTo(int64_t offset, bool readsNullsOnly) override;

  void seekToRowGroup(int64_t index) override;

  void filterRowGroups(
      uint64_t rowGroupSize,
      const StatsContext& context,
      FormatData::FilterRowGroupsResult& result) const override;

  const std::vector<SelectiveColumnReader*>& children() const override;

  void resetFilterCaches() override;

 private:
  // Converts the file-type vector in fileTypeVector_ into a requested-type
  // FlatVector, writing nulls for out-of-range values.
  void convert(
      const RowSet& rows,
      const VectorPtr& fileTypeVector,
      VectorPtr* result);

  // Applies the ScanSpec filter to the converted result and rewrites
  // outputRows_ to include only passing rows.
  void applyFilter(const RowSet& rows, const VectorPtr& converted);

  std::unique_ptr<SelectiveColumnReader> inner_;

  // ScanSpec that was modified for the inner reader (filter removed or replaced
  // with AlwaysTrue). Owned by this reader.
  std::unique_ptr<velox::common::ScanSpec> innerScanSpec_;

  // Temporary buffer that holds the file-type vector returned by inner_.
  VectorPtr fileTypeVector_;
};
```

#### `read()` implementation

```
ConvertingSelectiveColumnReader::read(offset, rows, incomingNulls):
  1. inner_->read(offset, rows, incomingNulls)
       inner_ uses innerScanSpec_ (no filter / AlwaysTrue),
       so inner_->outputRows() == rows (all rows survive)

  2. inner_->getValues(inner_->outputRows(), &fileTypeVector_)
       produces a FlatVector<FileType> (or DictionaryVector / ConstantVector)

  3. convert(rows, fileTypeVector_, &convertedBuffer_)
       element-wise: for each value in fileTypeVector_:
         if null: mark null in resultNulls_
         else:    convert to requestedType, check overflow/parse failure
                  on failure: mark null in resultNulls_
                  on success: write to values_

  4. if scanSpec_->hasFilter():
         applyFilter(rows, convertedVector_)
         → rebuild outputRows_ with rows that pass the filter

  5. readOffset_ = offset + rows.back() + 1
```

This design reuses `SelectiveColumnReader`'s scratch buffers (`values_`,
`resultNulls_`, `outputRows_`) for the converted output, so `getValues()` can
call the standard `getFlatValues()` path.

#### `getValues()` implementation

Since `read()` has already filled `values_` with converted data and set
`resultNulls_` and `outputRows_` correctly, `getValues()` simply calls
`getFlatValues<RequestedT, RequestedT>(rows, result, requestedType_)`.

### 2. Conversion functions

Location: `velox/dwio/common/TypeConversion.h`

```cpp
namespace facebook::velox::dwio::common {

// Result of converting a single value.
enum class ConvertResult { kOk, kNull };

// Converts a value from FileType to TargetType. Returns kNull if the
// value is out of range or otherwise unrepresentable.
template <typename FileType, typename TargetType>
ConvertResult convertValue(FileType in, TargetType& out);

// Batch versions for better codegen.
template <typename FileType, typename TargetType>
void convertBatch(
    const FileType* src,
    TargetType* dst,
    uint64_t* nullBits,   // output: bit cleared for failed conversions
    vector_size_t count,
    bool srcHasNulls,
    const uint64_t* srcNulls);

} // namespace facebook::velox::dwio::common
```

#### Narrowing integer: `int64_t → int32_t`

```cpp
template <>
ConvertResult convertValue(int64_t in, int32_t& out) {
  if (in < std::numeric_limits<int32_t>::min() ||
      in > std::numeric_limits<int32_t>::max()) {
    return ConvertResult::kNull;
  }
  out = static_cast<int32_t>(in);
  return ConvertResult::kOk;
}
```

The batch version uses SIMD where possible: a vectorized range-check followed
by a masked store.

#### Floating point → integer: `double → int64_t`

```cpp
template <>
ConvertResult convertValue(double in, int64_t& out) {
  if (std::isnan(in) || std::isinf(in) ||
      in < static_cast<double>(std::numeric_limits<int64_t>::min()) ||
      in > static_cast<double>(std::numeric_limits<int64_t>::max())) {
    return ConvertResult::kNull;
  }
  out = static_cast<int64_t>(in);
  return ConvertResult::kOk;
}
```

#### Decimal rescaling

```cpp
// DECIMAL(p1,s1) → DECIMAL(p2,s2)
// Multiply or divide the raw int128 value by 10^(s2-s1).
// Return kNull if the rescaled value doesn't fit in DECIMAL(p2,s2).
ConvertResult rescaleDecimal(
    int128_t srcValue, int32_t srcScale,
    int128_t& dstValue, int32_t dstScale, int32_t dstPrecision);
```

#### String → numeric

```cpp
template <typename TargetType>
ConvertResult convertStringToNumeric(std::string_view src, TargetType& out) {
  try {
    if constexpr (std::is_integral_v<TargetType>) {
      out = folly::to<TargetType>(src);
    } else {
      out = folly::to<double>(src);  // then narrow if needed
    }
    return ConvertResult::kOk;
  } catch (...) {
    return ConvertResult::kNull;
  }
}
```

#### Numeric → string

```cpp
template <typename FileType>
void convertNumericToString(
    const FileType* src,
    StringView* dst,
    char* stringPool,           // bump allocator
    size_t poolSize,
    vector_size_t count,
    const uint64_t* nullBits);
```

### 3. Changes to `TypeUtils.cpp`

Replace the single `isCompatible` function with two functions:

```cpp
// True for same-type or widening conversions that need no special handling.
// Controls whether the guard throws or defers to conversion wrapper.
bool isSafeWidening(TypeKind from, TypeKind to);

// True for any conversion pair we are willing to attempt at read time.
// If true, either isSafeWidening holds or a ConvertingSelectiveColumnReader
// will be created.
bool isConvertible(TypeKind from, TypeKind to);
```

`isConvertible` covers the full table in the Scope section above.
`checkTypeCompatibility` is updated to use `isConvertible` and no longer throws for
pairs in scope; it throws only for truly unsupported pairs.

### 4. Changes to `SelectiveDwrfReader::build()`

```cpp
std::unique_ptr<SelectiveColumnReader> SelectiveDwrfReader::build(
    ...,
    const TypePtr& requestedType,
    const std::shared_ptr<const TypeWithId>& fileType,
    DwrfParams& params,
    common::ScanSpec& scanSpec,
    bool isRoot) {

  // Guard: throw on truly unsupported pairs.
  typeutils::checkTypeCompatibility(*fileType->type(), *requestedType);
  // (checkTypeCompatibility now uses isConvertible instead of isCompatible)

  const bool needsConversion =
      fileType->type()->kind() != requestedType->kind() ||
      isDecimalRescale(*fileType->type(), *requestedType);

  if (needsConversion && !isSafeWidening(
          fileType->type()->kind(), requestedType->kind())) {
    // Build inner reader for the file type with the filter removed.
    auto innerScanSpec = scanSpec.cloneWithoutFilter();
    auto inner = buildForFileType(
        columnReaderOptions, fileType->type(), fileType,
        params, *innerScanSpec);
    return std::make_unique<ConvertingSelectiveColumnReader>(
        requestedType, fileType, std::move(inner),
        std::move(innerScanSpec), scanSpec);
  }

  // Existing dispatch on fileType->type()->kind() — unchanged.
  switch (fileType->type()->kind()) { ... }
}
```

A new helper `buildForFileType()` is the existing `switch` block extracted into
a function that always passes `fileType->type()` as the `requestedType` — so the
inner reader produces a vector of the file type.

### 5. Changes to `DwrfRowReader`

`DwrfReader.cpp:327` calls `checkTypeCompatibility` against the column selector.
Replace it with `checkConvertibility` (backed by `isConvertible`) so mismatched
types that are in scope are accepted rather than rejected.

### 6. `ScanSpec` cloning: `cloneWithoutFilter()`

Add to `ScanSpec`:

```cpp
// Returns a deep copy of this ScanSpec with all leaf filters replaced by
// AlwaysTrue. Used when the inner reader of a ConvertingSelectiveColumnReader
// must decode without filtering (filter is applied post-conversion).
std::unique_ptr<ScanSpec> cloneWithoutFilter() const;
```

This is necessary to avoid modifying the shared `ScanSpec` while building
the inner reader. The clone is owned by `ConvertingSelectiveColumnReader`.

---

## Filter and Row-Group Skipping

### Filter application

| Conversion kind | Inner reader filter | Post-conversion filter |
|-----------------|--------------------|-----------------------|
| Safe widening (int→bigint) | Original filter (safe in file-type domain) | Not needed |
| Narrowing (bigint→int) | AlwaysTrue (inner reads everything) | Original filter applied after conversion |
| Cross-domain (int→varchar) | AlwaysTrue | Original filter applied after conversion |
| Decimal rescale | AlwaysTrue | Original filter applied after conversion |
| String→numeric | AlwaysTrue | Original filter applied after conversion |

**Why narrowing needs post-conversion filter**: A BIGINT value of 3,000,000,000
might pass a BIGINT-domain filter (e.g., `> 0`) but becomes null after narrowing
to INT. Applying the filter before conversion would allow this null into the output,
while the caller expected a non-null positive INT. The post-conversion filter sees
the null and excludes it — which is correct behaviour.

**Why safe widening can reuse the filter directly**: For INT file → BIGINT
requested, any BIGINT filter comparison (`value > 1000`) produces the same result
when applied to an int64 holding an INT value as it would after widening —
because the int64 value is already the INT value widened to 64 bits.

### Row-group / stride filtering

`filterRowGroups()` uses column-level statistics (min/max, bloom filter) that
are in the **file-type domain**. For narrowing and cross-domain conversions,
these statistics cannot be reliably compared against a filter in the
**requested-type domain**.

`ConvertingSelectiveColumnReader::filterRowGroups()` behaviour:

```
if isSafeWidening(fileType, requestedType):
    // Statistics comparison is valid — delegate to inner_.
    inner_->filterRowGroups(rowGroupSize, context, result);
else:
    // Cannot reliably skip row groups. Return empty result (skip nothing).
    // Future: convert statistics min/max and re-evaluate.
    return;   // all row groups are considered potentially matching
```

This is conservative but correct. In practice the dominant use-case (integer
widening) benefits from row-group skipping. Narrowing/cross-domain conversions
lose this optimisation in Phase 1.

---

## Buffer Management

### Widening: output buffer is larger than input

When the file stores INT32 and the output is INT64, each value needs 8 bytes
instead of 4. The `ConvertingSelectiveColumnReader` allocates the `values_`
buffer sized for the **requested type** (`ensureValuesCapacity<TargetType>()`),
not the file type. The inner reader's temporary `fileTypeVector_` is discarded
after conversion.

For string output from numeric input, the conversion allocates a string pool
via `AlignedBuffer` (same bump-allocator pattern as the string readers). The
`StringView` entries in `values_` point into this pool, and `stringBuffers_`
holds ownership. This follows the existing `copyStringValue()` pattern from
`SelectiveColumnReader`.

### Narrowing: same or smaller output buffer

Output is smaller than or equal to the input. The `values_` buffer sized for
`TargetType` is sufficient. Out-of-range source values produce nulls and a
zero default value at the corresponding index, consistent with `addNull<T>()`
behaviour.

### Intermediate `fileTypeVector_` buffer reuse

`fileTypeVector_` is a member of `ConvertingSelectiveColumnReader` and is
reused across `read()` calls (same pattern as `nullsInReadRange_`). It is
reset at the start of each `read()` via `inner_->getValues()` which reuses
the inner reader's internal `FlatVector` wrapper.

---

## Null Handling

### Sources of nulls in the converted output

| Source | Mechanism |
|--------|-----------|
| File-level null (PRESENT stream) | Inner reader decodes null; `fileTypeVector_` has a null at that position; `convert()` propagates null to `resultNulls_`. |
| Parent struct null | Passed as `incomingNulls` to `read()`; inner reader merges via `nullsInReadRange_`; same propagation. |
| Overflow / range error during narrowing | `convertValue()` returns `kNull`; `convert()` marks the result bit in `resultNulls_` and writes a zero default. |
| Parse failure (string→numeric) | Same as overflow — `kNull` result. |
| NaN / Inf during float→int | Same. |

### Null bitmaps through the pipeline

```
fileTypeVector_->nulls()           (inner reader's resultNulls)
        │
        ▼
convert() merges:
  for each i in 0..numRows:
    if srcNull(i) or conversionFailed(i):
        bits::setNull(rawResultNulls_, i)
    else:
        rawValues_[i] = converted value

resultNulls_ → FlatVector<TargetType> in getValues()
```

The `anyNulls_` and `allNull_` flags on `ConvertingSelectiveColumnReader` are
set during `convert()`, following the same protocol as `addNull<T>()`.

---

## Impacted Code Paths

### `DwrfReader.cpp` — type check at stripe open

`startNextStripe()` at line 327 calls `checkTypeCompatibility` against the column
selector. This must be relaxed to `checkConvertibility` so that files written
with the old schema can be opened with the new schema. Failing here means the
reader can't even start; relaxing the check only needs to happen once per stripe.

### `ColumnReader.cpp` — non-selective path

`ColumnReader::build()` at line 2550 has the same guard. For Phase 1 we do not
implement conversion in the non-selective path. The guard must remain strict
there, or be relaxed with an explicit "not supported in non-selective path"
message so callers don't silently get wrong results.

### `checkTypeCompatibility` in `ColumnSelector.cpp`

`ColumnSelector` resolves column names from the file schema and validates types.
Lines 79 and 145 call `checkTypeCompatibility`. These must be updated to use
`isConvertible`. If they are not updated, the mismatch is caught at column
selection time before any readers are built.

### Row group statistics filtering

`DwrfData::filterRowGroups()` applies `ScanSpec`'s filter against column
statistics. For narrowing / cross-domain columns wrapped by
`ConvertingSelectiveColumnReader`, the wrapper intercepts `filterRowGroups()` and
returns nothing (no skipping), so `DwrfData::filterRowGroups()` is never called
with a mismatched filter. No change needed inside `DwrfData`.

### `SelectiveFloatingPointColumnReader`

Already handles `float → double` natively. No change needed for REAL→DOUBLE.

### `SelectiveStringDictionaryColumnReader` / `SelectiveStringDirectColumnReader`

For numeric→string conversion, the inner reader is one of these string readers
built for `VARCHAR` file type. The string reader already decodes correctly into
`StringView` values. The `ConvertingSelectiveColumnReader` then applies a numeric
filter on top of the parsed result. No change to the string readers.

### `SelectiveDecimalColumnReader`

For decimal→decimal rescaling, the inner reader is built for the file decimal
type. The wrapper handles rescaling in `convert()`. No change to
`SelectiveDecimalColumnReader`.

### Aggregation pushdown hooks (`ValueHook`)

The current `processValueHook()` path pushes values directly into an aggregation
accumulator, bypassing materialization. When a `ConvertingSelectiveColumnReader`
wraps the inner reader, the inner reader uses `AlwaysTrue` and no hook (to
avoid pushing unconverted values into the aggregation). The wrapper converts
values and then calls the hook explicitly. This is handled in
`ConvertingSelectiveColumnReader::read()` by checking `scanSpec_->valueHook()`
and applying it on the converted batch.

### Lazy vectors

Lazy vector loading (via `ColumnLoader`) calls `getValues()` on the reader at
the point of first access. For `ConvertingSelectiveColumnReader`, the conversion
must happen inside `getValues()` on demand. However, the inner `read()` must have
been called before `getValues()`. `ConvertingSelectiveColumnReader` is only used
for `isTopLevel_ = true` columns (top-level struct fields). Lazy vectors for
these are created by `SelectiveStructColumnReaderBase`. No structural change is
needed; the lazy loader calls the wrapper's `getValues()` which performs
conversion on demand.

---

## Complex Type Support (ARRAY/MAP element conversion)

`SelectiveListColumnReader` and `SelectiveMapColumnReader` build their element
readers via `SelectiveDwrfReader::build()`. If the element type has changed
(e.g., `ARRAY<INT>` file → `ARRAY<BIGINT>` requested), the build function
creates a `ConvertingSelectiveColumnReader` for the element reader automatically.
The list/map reader itself is unchanged. This falls out naturally from the
recursive build.

For MAP key type conversion: same mechanism applies to the key reader.

---

## New Files

| File | Content |
|------|---------|
| `velox/dwio/common/TypeConversion.h` | `convertValue<>`, `convertBatch<>` templates and specialisations |
| `velox/dwio/common/TypeConversion.cpp` | Non-template conversion implementations (string↔numeric, decimal) |
| `velox/dwio/common/ConvertingSelectiveColumnReader.h` | Class declaration |
| `velox/dwio/common/ConvertingSelectiveColumnReader.cpp` | `read()`, `getValues()`, `convert()`, `applyFilter()` |
| `velox/dwio/common/tests/TypeConversionTest.cpp` | Unit tests for all conversion pairs |
| `velox/dwio/dwrf/reader/tests/DwrfSchemaConversionTest.cpp` | Integration tests: read files with evolved schema |

---

## Modified Files

| File | Change |
|------|--------|
| `velox/dwio/common/TypeUtils.h` | Add `isConvertible(TypeKind, TypeKind)` |
| `velox/dwio/common/TypeUtils.cpp` | Implement `isConvertible`; update `isCompatible` to use it; update `makeCompatibilityMap` |
| `velox/dwio/common/ScanSpec.h` | Add `cloneWithoutFilter()` |
| `velox/dwio/common/ScanSpec.cpp` | Implement `cloneWithoutFilter()` |
| `velox/dwio/dwrf/reader/SelectiveDwrfReader.cpp` | Add conversion wrapper creation after inner reader build |
| `velox/dwio/dwrf/reader/DwrfReader.cpp` | Relax type check in `startNextStripe()` |
| `velox/dwio/common/ColumnSelector.cpp` | Relax type check at lines 79, 145 |

---

## Alternatives Considered

### Option A: Inline conversion in each reader's `getValues()`

Add conversion logic to every `SelectiveXxxColumnReader::getValues()` — e.g.,
`SelectiveIntegerDirectColumnReader::getValues()` casts int64 to double when
`requestedType_` is DOUBLE.

**Problem:** O(M×N) template instantiations across M reader classes and N target
types. Each reader must know about every target domain (float, string, decimal,
timestamp). Violates single-responsibility and bloats the hot path. Rejected.

### Option B: Post-vector-construction transformation in struct reader

The struct reader collects `VectorPtr` for each field after `getValues()`. We
could transform each field vector before assembling the `RowVector`.

**Problem:** Requires an intermediate FlatVector allocation then a second pass
of conversions. All filter application is already done by the time the struct
reader gets the vector — post-conversion filtering is impossible at this level
without rereading. Row group skipping is also impossible since struct reader
doesn't know per-field types. Rejected.

### Option C: Convert during `readWithVisitor()` inside the decoder

Extend the visitor to emit a different type than the decoded type — e.g., a
visitor that converts `int64_t` to `double` in `addValue()`.

**Problem:** The visitor's `DataType` template parameter controls the `values_`
buffer element size. Changing it requires instantiating a new visitor with a
different `DataType`, which is only possible if the decoder is templated over
both source and target types. String readers and decimal readers are not
int decoders and cannot participate in the visitor template hierarchy. Rejected
for cross-domain conversions; kept as a potential future optimisation for
widening-integer case (which already works without it).

---

## Testing Strategy

### Unit tests (`TypeConversionTest.cpp`)

- Each `convertValue<From, To>` specialisation tested with: normal values,
  boundary values (min/max of target type), overflow values, NaN/Inf (for floats),
  empty string, malformed string (for string→numeric).
- Verify that `convertBatch` produces identical results to per-element
  `convertValue` calls.

### Integration tests (`DwrfSchemaConversionTest.cpp`)

For each conversion pair in scope:

1. Write a DWRF file with the file type.
2. Read it back with a mismatched requested type.
3. Verify: output vector has correct type, values match expected, nulls appear
   where overflow occurred.
4. Verify predicate pushdown: rows matching a filter are included, rows that do
   not match are excluded even after type conversion.
5. Verify row group skipping for safe widening (row groups with all values
   outside filter range are skipped).

### Regression tests

Ensure all existing `DwrfReaderTest` cases pass unchanged — the new code must
not alter behaviour for matching types.
