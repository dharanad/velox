# Vectors

Velox processes data in a columnar format. Every column in a batch of rows is
represented as a **Vector** — a typed, possibly-nullable, array of values. This
document covers the full Vector API: the type hierarchy, every encoding, and
all functions for creating, reading, modifying, and encoding vectors.

## Source Files

| File | Purpose |
|------|---------|
| `velox/vector/BaseVector.h` | Abstract base class, static factory methods |
| `velox/vector/SimpleVector.h` | Typed base for all scalar vector types |
| `velox/vector/FlatVector.h` | Contiguous fixed-width or StringView storage |
| `velox/vector/ConstantVector.h` | Single-value vector of arbitrary length |
| `velox/vector/DictionaryVector.h` | Indices into a shared dictionary |
| `velox/vector/SequenceVector.h` | Run-length encoded values |
| `velox/vector/BiasVector.h` | Delta-encoded integers |
| `velox/vector/ComplexVector.h` | RowVector, ArrayVector, MapVector |
| `velox/vector/tests/utils/VectorMaker.h` | Test/dev helper for convenient construction |
| `velox/vector/VectorSaver.h` | Serialization to/from streams and files |

---

## Vector Type Hierarchy

```
BaseVector
├── SimpleVector<T>           (typed scalar base)
│   ├── FlatVector<T>         (contiguous array)
│   ├── ConstantVector<T>     (one value, N rows)
│   ├── DictionaryVector<T>   (index → dictionary)
│   ├── SequenceVector<T>     (run-length encoding)
│   └── BiasVector<T>         (delta encoding)
├── RowVector                 (struct / record)
├── ArrayVector               (variable-length arrays)
└── MapVector                 (key-value maps)
```

`VectorPtr` is `std::shared_ptr<BaseVector>` — the type used everywhere in
operator code.

---

## Encodings

Every vector has a `VectorEncoding::Simple` tag returned by `encoding()`:

| Encoding | Class | When to use |
|----------|-------|-------------|
| `FLAT` | `FlatVector<T>` | Default; random read/write |
| `CONSTANT` | `ConstantVector<T>` | All rows hold the same value |
| `DICTIONARY` | `DictionaryVector<T>` | Repeated values from a shared base |
| `SEQUENCE` | `SequenceVector<T>` | Long runs of the same value |
| `BIASED` | `BiasVector<T>` | Integers clustered in a small range |
| `ROW` | `RowVector` | Struct of named child vectors |
| `ARRAY` | `ArrayVector` | Variable-length arrays of one element type |
| `MAP` | `MapVector` | Variable-length key-value maps |
| `LAZY` | `LazyVector` | Values loaded on demand |

---

## Creating Vectors

### BaseVector::create — generic factory

```cpp
// velox/vector/BaseVector.h:735
template <typename T = BaseVector>
static std::shared_ptr<T> create(
    const TypePtr& type,
    vector_size_t size,
    memory::MemoryPool* pool);
```

Creates a flat writable vector of the given type. The returned vector has every
slot initialized to not-null and every value zero-initialized.

```cpp
// Create a BIGINT flat vector with 100 rows.
auto vec = BaseVector::create(BIGINT(), 100, pool);

// Create a VARCHAR flat vector.
auto strVec = BaseVector::create<FlatVector<StringView>>(VARCHAR(), 50, pool);

// Create a ROW(a BIGINT, b DOUBLE) vector.
auto rowVec = BaseVector::create(
    ROW({"a", "b"}, {BIGINT(), DOUBLE()}), 32, pool);
```

### BaseVector::createConstant — all rows the same scalar

```cpp
// velox/vector/BaseVector.h:601
static VectorPtr createConstant(
    const TypePtr& type,
    Variant value,
    vector_size_t size,
    memory::MemoryPool* pool);
```

```cpp
// A BIGINT constant with value 42, length 1024.
auto c42 = BaseVector::createConstant(BIGINT(), 42LL, 1024, pool);

// A VARCHAR constant.
auto hello = BaseVector::createConstant(VARCHAR(), "hello", 256, pool);
```

### BaseVector::createNullConstant — all rows null

```cpp
// velox/vector/BaseVector.h:607
static VectorPtr createNullConstant(
    const TypePtr& type,
    vector_size_t size,
    memory::MemoryPool* pool);
```

```cpp
auto nulls = BaseVector::createNullConstant(BIGINT(), 100, pool);
// nulls->isNullAt(i) == true for every i
```

### BaseVector::createFromVariants — from a list of Variant values

```cpp
// velox/vector/BaseVector.h:895
static VectorPtr createFromVariants(
    const TypePtr& type,
    const std::vector<Variant>& values,
    memory::MemoryPool* pool);
```

```cpp
auto vec = BaseVector::createFromVariants(
    INTEGER(),
    {Variant(1), Variant(2), Variant(), Variant(4)},  // Variant() = null
    pool);
// vec->isNullAt(2) == true
```

---

## FlatVector

`FlatVector<T>` stores values in a contiguous buffer — the simplest and most
common encoding. Fixed-width types (`int32_t`, `double`, etc.) are laid out as
a plain C array. Variable-length types use `StringView` headers that point into
separate string buffers.

### Constructing directly

```cpp
// Allocate a values buffer and build a FlatVector from scratch.
auto values = AlignedBuffer::allocate<int32_t>(100, pool);
auto* rawValues = values->asMutable<int32_t>();
for (int i = 0; i < 100; ++i) rawValues[i] = i * 2;

auto flat = std::make_shared<FlatVector<int32_t>>(
    pool,
    INTEGER(),        // TypePtr
    /*nulls=*/nullptr,
    /*length=*/100,
    std::move(values),
    /*stringBuffers=*/{});
```

For most code, `VectorMaker` (described later) is more convenient.

### Reading values

```cpp
auto flat = /* FlatVector<int32_t>* */;

// Single value — bounds-checked.
int32_t v = flat->valueAt(7);

// Fast path — no bounds check; use in inner loops.
int32_t vFast = flat->valueAtFast(7);

// Null check.
bool isNull = flat->isNullAt(7);

// Raw pointer for bulk access.
const int32_t* raw = flat->rawValues();
for (int i = 0; i < flat->size(); ++i) {
  if (!flat->isNullAt(i)) process(raw[i]);
}
```

### Writing values

```cpp
// Point access.
flat->set(7, 99);
flat->setNull(7, true);

// Bulk: get a mutable raw pointer.
int32_t* mut = flat->mutableRawValues();
for (int i = 0; i < flat->size(); ++i) mut[i] = i;

// Make a specific set of rows writable (copies buffer if multiply-referenced).
SelectivityVector rows(flat->size());
flat->ensureWritable(rows);
```

### String values

```cpp
auto strFlat = /* FlatVector<StringView>* */;

// Inline strings (≤ 12 bytes) are stored directly in the StringView.
// Longer strings live in string buffers.
StringView sv = strFlat->valueAt(3);
std::string s(sv.data(), sv.size());

// Writing an inline string.
strFlat->set(0, StringView("hi"));

// Writing a longer string: get scratch space in a string buffer.
constexpr int32_t kLen = 100;
char* buf = strFlat->getRawStringBufferWithSpace(kLen);
std::memcpy(buf, longString.data(), kLen);
strFlat->setNoCopy(0, StringView(buf, kLen));

// Copying string buffers from another vector (shared, zero-copy).
strFlat->acquireSharedStringBuffers(otherFlat.get());
```

---

## ConstantVector

`ConstantVector<T>` stores one value and pretends to have `N` rows. It is the
most memory-efficient representation when all rows share a value (e.g., a
literal in an expression).

### Creating from a scalar value

```cpp
// velox/vector/ConstantVector.h
auto c = std::make_shared<ConstantVector<int64_t>>(
    pool,
    /*length=*/1000,
    /*isNull=*/false,
    BIGINT(),
    int64_t{42});

// Or use the BaseVector factory:
auto c2 = BaseVector::createConstant(BIGINT(), 42LL, 1000, pool);
```

### Creating from an index into another vector

```cpp
// Makes a constant that refers to row 5 of flatVec.
auto c = BaseVector::wrapInConstant(/*length=*/500, /*index=*/5, flatVec);
```

### Reading

```cpp
auto* cv = c->as<ConstantVector<int64_t>>();
int64_t val = cv->valueAt(0);   // same for every index
bool isNull = cv->isNullAt(0);
```

---

## DictionaryVector

`DictionaryVector<T>` holds an index buffer (one `int32_t` per row) and a
shared "dictionary" base vector. Rows with the same value share the same
dictionary entry. It is the natural output of dictionary-compressed Parquet
columns and of expression evaluation over join keys.

### Creating

```cpp
// Build a 10-element dictionary over a 3-element base.
auto base = makeFlatVector<int32_t>({10, 20, 30});

auto indicesBuf = AlignedBuffer::allocate<int32_t>(10, pool);
auto* idx = indicesBuf->asMutable<int32_t>();
// Pattern: 0→10, 1→20, 2→30, 3→10, 4→20, …
for (int i = 0; i < 10; ++i) idx[i] = i % 3;

auto dict = BaseVector::wrapInDictionary(
    /*nulls=*/nullptr,
    indicesBuf,
    /*size=*/10,
    base);
// dict->valueAt(3) == 10
```

### Adding null rows

```cpp
// Rows 2 and 5 are null in the dictionary wrapper.
auto nullsBuf = AlignedBuffer::allocate<uint64_t>(
    bits::nwords(10), pool, /*initValue=*/bits::kNotNullByte);
bits::setNull(nullsBuf->asMutable<uint64_t>(), 2, true);
bits::setNull(nullsBuf->asMutable<uint64_t>(), 5, true);

auto dictWithNulls = BaseVector::wrapInDictionary(
    nullsBuf, indicesBuf, 10, base);
```

### Reading

```cpp
auto* dv = dict->as<DictionaryVector<int32_t>>();

// Logical value (transparently follows the index).
int32_t v = dv->valueAt(3);

// Raw index.
int32_t rawIdx = dv->getDictionaryIndex(3);

// Access the underlying dictionary.
const VectorPtr& dictionary = dv->valueVector();
```

### Flattening a dictionary

```cpp
// Flatten in-place: dict becomes a FlatVector.
BaseVector::flattenVector(dict);
```

---

## SequenceVector

`SequenceVector<T>` uses run-length encoding: a "values" vector lists distinct
values and a "lengths" buffer records how many times each is repeated.

### Creating with VectorMaker

```cpp
// {1, 1, 2, 2, 2, 3} — three runs
auto seq = vectorMaker.sequenceVector<int32_t>({1, 1, 2, 2, 2, 3});
// seq->valueAt(0) == 1, seq->valueAt(4) == 2, seq->valueAt(5) == 3
```

### Reading

```cpp
auto* sv = seq->as<SequenceVector<int32_t>>();
int32_t v = sv->valueAt(4);        // 2
int32_t numSeq = sv->numSequences();  // 3
```

---

## BiasVector

`BiasVector<T>` stores values as `(stored_value + bias) = actual_value`, using
a narrower integer type for `stored_value`. Useful when values cluster in a
small numeric range.

### Creating with VectorMaker

```cpp
// Values 1000, 1001, 1002 — bias = 1000, stored deltas = 0, 1, 2
auto bias = vectorMaker.biasVector<int64_t>({1000, 1001, 1002});
// bias->valueAt(0) == 1000
```

---

## RowVector

`RowVector` is a struct: it holds a list of child vectors (one per field) and
an optional null bitmap for the rows themselves.

### Creating

```cpp
auto row = std::make_shared<RowVector>(
    pool,
    ROW({"id", "name", "score"}, {BIGINT(), VARCHAR(), DOUBLE()}),
    /*nulls=*/nullptr,
    /*length=*/100,
    std::vector<VectorPtr>{idVec, nameVec, scoreVec});
```

### Accessing children

```cpp
// By index.
VectorPtr idCol   = row->childAt(0);
VectorPtr nameCol = row->childAt(1);

// By name (requires a RowType).
VectorPtr scoreCol = row->childAt("score");

// Number of fields.
int numFields = row->childrenSize();
```

### Null rows

A null row means the whole struct is null, not any individual field.

```cpp
row->setNull(5, true);   // row 5 is a null struct
row->isNullAt(5);        // true
```

### Resizing

```cpp
// Resize to 200, recursively resizing all children.
row->resize(200);

// Resize the wrapper only (leave children unchanged).
row->unsafeResize(200);
```

---

## ArrayVector

`ArrayVector` stores variable-length arrays. It holds:
- a null bitmap (per top-level row)
- an **offsets** buffer: `offsets[i]` is where array `i` starts in `elements`
- a **sizes** buffer: `sizes[i]` is how many elements array `i` contains
- an **elements** child vector (flat or nested)

### Creating

```cpp
// Three arrays: [1,2], [3,4,5], [6]
auto elements = makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6});

auto offsetsBuf = AlignedBuffer::allocate<int32_t>(3, pool);
auto sizesBuf   = AlignedBuffer::allocate<int32_t>(3, pool);
auto* offsets = offsetsBuf->asMutable<int32_t>();
auto* sizes   = sizesBuf->asMutable<int32_t>();
offsets[0] = 0; sizes[0] = 2;   // [1, 2]
offsets[1] = 2; sizes[1] = 3;   // [3, 4, 5]
offsets[2] = 5; sizes[2] = 1;   // [6]

auto arr = std::make_shared<ArrayVector>(
    pool,
    ARRAY(INTEGER()),
    /*nulls=*/nullptr,
    /*length=*/3,
    offsetsBuf,
    sizesBuf,
    elements);
```

### Reading

```cpp
int32_t off  = arr->offsetAt(1);  // 2
int32_t len  = arr->sizeAt(1);    // 3
// Elements at arr->elements(), rows [off, off+len)

VectorPtr elems = arr->elements();
for (int e = off; e < off + len; ++e) {
  int32_t v = elems->as<FlatVector<int32_t>>()->valueAt(e);  // 3,4,5
}
```

### Accessing raw buffers

```cpp
const int32_t* rawOffsets = arr->rawOffsets();
const int32_t* rawSizes   = arr->rawSizes();
```

---

## MapVector

`MapVector` stores variable-length key-value maps. Layout is identical to
`ArrayVector` — offsets and sizes — but it has two child vectors: `keys` and
`values`.

### Creating

```cpp
// Two maps: {1→"a", 2→"b"}, {3→"c"}
auto keys   = makeFlatVector<int32_t>({1, 2, 3});
auto values = makeFlatVector<StringView>({"a", "b", "c"});

auto offsetsBuf = AlignedBuffer::allocate<int32_t>(2, pool);
auto sizesBuf   = AlignedBuffer::allocate<int32_t>(2, pool);
offsetsBuf->asMutable<int32_t>()[0] = 0;
offsetsBuf->asMutable<int32_t>()[1] = 2;
sizesBuf->asMutable<int32_t>()[0]   = 2;
sizesBuf->asMutable<int32_t>()[1]   = 1;

auto mapVec = std::make_shared<MapVector>(
    pool,
    MAP(INTEGER(), VARCHAR()),
    /*nulls=*/nullptr,
    /*length=*/2,
    offsetsBuf,
    sizesBuf,
    keys,
    values);
```

### Reading

```cpp
int32_t off = mapVec->offsetAt(0);  // 0
int32_t len = mapVec->sizeAt(0);    // 2

auto k = mapVec->mapKeys()->as<FlatVector<int32_t>>();
auto v = mapVec->mapValues()->as<FlatVector<StringView>>();

for (int e = off; e < off + len; ++e) {
  int32_t key   = k->valueAt(e);   // 1, 2
  StringView val = v->valueAt(e);  // "a", "b"
}
```

### Canonicalize (sort keys)

```cpp
// Sort keys in each map for deterministic output.
MapVector::canonicalize(mapVec);
// mapVec->hasSortedKeys() == true
```

---

## VectorMaker — Convenient Construction

`VectorMaker` (`velox/vector/tests/utils/VectorMaker.h`) is the most ergonomic
API for constructing vectors in tests and development. In test fixtures that
inherit from `FunctionBaseTest`, the shorthand helpers (`makeFlatVector`,
`makeArrayVector`, etc.) call through to a `VectorMaker` instance.

```cpp
#include "velox/vector/tests/utils/VectorMaker.h"

memory::MemoryPool* pool = /* ... */;
VectorMaker maker(pool);
```

### Flat scalars

```cpp
// From an initializer list (type inferred from T).
auto v1 = maker.flatVector<int64_t>({1, 2, 3, 4, 5});

// From a std::vector.
std::vector<double> data = {1.5, 2.5, 3.5};
auto v2 = maker.flatVector<double>(data);

// From nullable data (std::nullopt = null row).
auto v3 = maker.flatVectorNullable<int32_t>({1, std::nullopt, 3});
// v3->isNullAt(1) == true

// From generator functions.
auto v4 = maker.flatVector<int64_t>(
    /*size=*/10,
    /*valueAt=*/[](auto row) { return row * row; },
    /*isNullAt=*/[](auto row) { return row % 3 == 0; });

// All-null flat vector.
auto v5 = maker.allNullFlatVector<float>(50);

// With an explicit type (for Timestamp, Date, etc.)
auto v6 = maker.flatVector<int64_t>(
    {1000, 2000},
    TIMESTAMP());
```

### Booleans and strings

```cpp
auto bools  = maker.flatVector<bool>({true, false, true, false});
auto strs   = maker.flatVector<std::string>({"hello", "world"});
auto svs    = maker.flatVector<StringView>({"foo", "bar", "baz"});
```

### Encoded scalar vectors

```cpp
// All rows have the same value (ConstantVector).
auto constVec = maker.constantVector<int32_t>({{42}});

// Run-length encoded (SequenceVector).
auto seqVec = maker.sequenceVector<int32_t>({1, 1, 2, 2, 3});

// Dictionary encoded (DictionaryVector).
auto dictVec = maker.dictionaryVector<int32_t>({10, 20, 10, 30, 10, 20});

// Bias encoded (BiasVector).
auto biasVec = maker.biasVector<int64_t>({1000, 1001, 1002, 1003});

// Choose encoding at runtime.
auto enc = maker.encodedVector<int32_t>(
    VectorEncoding::Simple::DICTIONARY,
    {1, 2, 1, 3});
```

### Arrays

```cpp
// From a nested std::vector.
auto arr1 = maker.arrayVector<int32_t>(
    {{1, 2, 3}, {4, 5}, {6, 7, 8, 9}});

// With null elements.
auto arr2 = maker.arrayVectorNullable<int32_t>(
    {{{1, std::nullopt, 3}},     // array 0: [1, null, 3]
     std::nullopt,               // array 1: null array
     {{4, 5}}});                 // array 2: [4, 5]

// Generator-based (flexible).
auto arr3 = maker.arrayVector<int64_t>(
    /*size=*/5,
    /*sizeAt=*/[](auto row) { return row + 1; },  // array i has i+1 elements
    /*valueAt=*/[](auto row) { return row * 10; },
    /*isNullAt=*/[](auto /*row*/) { return false; },
    /*valueIsNullAt=*/[](auto /*elem*/) { return false; });

// Array of ROW children.
auto rowType = ROW({"x", "y"}, {INTEGER(), DOUBLE()});
auto arr4 = maker.arrayOfRowVector(
    rowType,
    {{{Variant(1), Variant(1.1)}, {Variant(2), Variant(2.2)}},
     {{Variant(3), Variant(3.3)}}});

// All-null array vector.
auto arr5 = maker.allNullArrayVector(10, INTEGER());

// From offsets and a pre-built elements vector.
auto elems = maker.flatVector<int32_t>({10, 20, 30, 40});
auto arr6  = maker.arrayVector(
    /*offsets=*/{0, 2},       // array 0 starts at 0, array 1 at 2
    elems,
    /*nulls=*/{});            // no null arrays
```

### Maps

```cpp
// From a vector of pairs.
auto map1 = maker.mapVector<int32_t, double>(
    {{{1, 1.1}, {2, 2.2}},   // map 0
     {{3, 3.3}}});            // map 1

// With nullable values.
using Pair = std::pair<int32_t, std::optional<double>>;
auto map2 = maker.mapVector<int32_t, double>(
    std::vector<std::vector<Pair>>{
        {{1, 1.1}, {2, std::nullopt}},
        {{3, 3.3}}});

// Nullable maps.
auto map3 = maker.mapVector<int32_t, double>(
    std::vector<std::optional<std::vector<Pair>>>{
        {{{1, 1.1}}},
        std::nullopt,          // null map
        {{{2, 2.2}}}});

// Generator-based.
auto map4 = maker.mapVector<int32_t, int64_t>(
    /*size=*/4,
    /*sizeAt=*/[](auto row) { return row + 1; },
    /*keyAt=*/[](auto entry) { return entry; },
    /*valueAt=*/[](auto entry) { return entry * 100LL; },
    /*isNullAt=*/[](auto /*row*/) { return false; });

// From offsets + pre-built key/value vectors.
auto keys   = maker.flatVector<int32_t>({1, 2, 3, 4});
auto values = maker.flatVector<double>({1.1, 2.2, 3.3, 4.4});
auto map5   = maker.mapVector(
    /*offsets=*/{0, 2},
    keys, values,
    /*nulls=*/{});

// All-null map vector.
auto map6 = maker.allNullMapVector(5, DOUBLE());
```

### RowVectors

```cpp
// From child vectors — field names come from the children types.
auto row1 = maker.rowVector({
    maker.flatVector<int64_t>({1, 2, 3}),
    maker.flatVector<double>({1.1, 2.2, 3.3})});

// With explicit field names.
auto row2 = maker.rowVector(
    {"id", "score"},
    {maker.flatVector<int64_t>({1, 2, 3}),
     maker.flatVector<double>({0.5, 0.6, 0.7})});

// Empty RowVector of a specific type.
auto row3 = maker.rowVector(ROW({"x"}, {INTEGER()}), /*size=*/10);

// A constant row (wraps a single Variant value into a ConstantVector).
auto constRow = maker.constantRow(
    ROW({"a", "b"}, {INTEGER(), VARCHAR()}),
    Variant::row({Variant(1), Variant("hello")}),
    /*size=*/100);
```

### Lazy vectors

```cpp
// Values are only loaded when first accessed.
auto lazy = maker.lazyFlatVector<int32_t>(
    /*size=*/100,
    /*valueAt=*/[](auto row) { return row; },
    /*isNullAt=*/[](auto row) { return row % 5 == 0; });

// Force load.
BaseVector* loaded = lazy->loadedVector();
```

---

## Reading Vectors

### Value access

```cpp
// All vector types: isNullAt, valueAt.
for (int i = 0; i < vec->size(); ++i) {
  if (!vec->isNullAt(i)) {
    // For typed access, cast first:
    auto* flat = vec->as<FlatVector<int64_t>>();
    int64_t v = flat->valueAt(i);
  }
}

// containsNullAt: also checks inside complex types.
bool hasNull = vec->containsNullAt(3);
```

### Null bitmap

```cpp
// Check if the vector might have any nulls at all (fast).
if (vec->mayHaveNulls()) {
  const uint64_t* rawNulls = vec->rawNulls();
  // rawNulls is a bit-packed array; bit i = 0 means null.
  bool isNull = bits::isBitNull(rawNulls, 5);
}

// Count nulls.
int32_t nullCount = BaseVector::countNulls(vec->nulls(), vec->size());
```

### Peeling encodings

```cpp
// Get the innermost non-wrapper vector.
const BaseVector* inner = vec->wrappedVector();

// Translate an index through all wrapping layers.
vector_size_t innerIdx = vec->wrappedIndex(7);

int64_t val = inner->as<FlatVector<int64_t>>()->valueAt(innerIdx);
```

### DecodedVector — uniform access regardless of encoding

`DecodedVector` decodes any encoding combination into a flat view. Use it in
expression evaluation when you don't want to handle every encoding separately.

```cpp
#include "velox/vector/DecodedVector.h"

SelectivityVector rows(vec->size());
DecodedVector decoded;
decoded.decode(*vec, rows);

for (int i = 0; i < vec->size(); ++i) {
  if (rows.isValid(i) && !decoded.isNullAt(i)) {
    int64_t v = decoded.valueAt<int64_t>(i);
  }
}

// Access the flat base and index translation.
const int64_t* base     = decoded.data<int64_t>();
const int32_t* indices  = decoded.indices();
// base[indices[i]] is the value for row i (when not null).
```

### Variant access

```cpp
// Get a single value as a type-erased Variant (useful for debugging).
Variant v = vec->variantAt(3);

// Get all values as Variants.
std::vector<Variant> all = vec->toVariants();
```

### String representation

```cpp
// Summary: encoding, type, size, null count.
std::string summary = vec->toString();

// Value at a specific row.
std::string rowStr = vec->toString(5);

// Range of values.
std::string range = vec->toString(0, 10, ", ", true);
```

---

## Modifying Vectors

### Ensuring writability

Before writing into a vector that may be shared or wrapped, call
`ensureWritable`. It flattens wrappers and copies multiply-referenced buffers
so the caller has exclusive mutable access.

```cpp
// Instance method: make all rows in 'rows' writable.
SelectivityVector rows(vec->size());
vec->ensureWritable(rows);

// Static method: also handles the case where 'result' is nullptr.
VectorPtr result;
BaseVector::ensureWritable(rows, BIGINT(), pool, result);

// Now safe to write.
auto* flat = result->as<FlatVector<int64_t>>();
flat->set(0, 999);
```

### Setting individual values

```cpp
auto* flat = vec->as<FlatVector<int32_t>>();
flat->set(3, 42);          // set value
flat->setNull(3, true);    // mark as null
flat->setNull(3, false);   // clear null
```

### Bulk null operations

```cpp
SelectivityVector activeRows(vec->size());
activeRows.deselect(5);    // don't touch row 5

// Mark selected rows as null.
vec->addNulls(activeRows);

// Clear nulls for selected rows.
vec->clearNulls(activeRows);

// Clear all nulls.
vec->clearAllNulls();

// Set nulls from a raw bitmap.
const uint64_t* nullsBitmap = /* ... */;
vec->addNulls(nullsBitmap, activeRows);
```

### Resizing

```cpp
// Resize to 200 rows; new rows are set to not-null.
vec->resize(200);

// Resize and mark new rows as null.
vec->resize(200, /*setNotNull=*/false);
```

### Appending

```cpp
// Append all rows from 'other' to 'vec'.
vec->append(other.get());
```

---

## Copying

### Deep copy

```cpp
// Static: full deep copy into the same pool.
VectorPtr copy = BaseVector::copy(*original, pool);
```

### Range copy (point-to-point)

```cpp
// Copy rows 10..19 from source into target starting at row 0.
target->copy(source.get(), /*targetIndex=*/0, /*sourceIndex=*/10, /*count=*/10);
```

### SelectivityVector-driven copy

```cpp
// Copy active rows from source into target.
// If toSourceRow is non-null, target[row] ← source[toSourceRow[row]].
SelectivityVector rows(source->size());
target->copy(source.get(), rows, /*toSourceRow=*/nullptr);
```

### CopyRange bulk copy

`CopyRange` batches multiple (sourceIndex, targetIndex, count) triples for
efficient Array/Map/VARCHAR copying:

```cpp
std::vector<BaseVector::CopyRange> ranges = {
    {/*sourceIndex=*/0, /*targetIndex=*/0, /*count=*/5},
    {/*sourceIndex=*/10, /*targetIndex=*/5, /*count=*/3},
};
target->copyRanges(source.get(), folly::Range(ranges.data(), ranges.size()));

// Convert a SelectivityVector to CopyRange (sourceIndex == targetIndex).
auto ranges2 = BaseVector::toCopyRanges(activeRows);
target->copyRanges(source.get(), folly::Range(ranges2.data(), ranges2.size()));
```

### Slicing (zero-copy view)

```cpp
// Create a sub-vector for rows [10, 25).
VectorPtr slice = vec->slice(/*offset=*/10, /*length=*/15);
// slice->size() == 15
// slice->valueAt(0) == vec->valueAt(10)
```

---

## Encoding Manipulation

### wrapInDictionary

Apply a dictionary encoding on top of any vector. `indices[i]` selects which
row of `base` to present at position `i`.

```cpp
// Reverse a vector using dictionary encoding.
auto indicesBuf = AlignedBuffer::allocate<int32_t>(vec->size(), pool);
auto* idx = indicesBuf->asMutable<int32_t>();
for (int i = 0; i < vec->size(); ++i) idx[i] = vec->size() - 1 - i;

auto reversed = BaseVector::wrapInDictionary(
    /*nulls=*/nullptr, indicesBuf, vec->size(), vec);
```

### wrapInConstant

Make a constant view of a single row of an existing vector:

```cpp
// Present row 7 of flatVec as a 1000-row constant.
auto c = BaseVector::wrapInConstant(/*length=*/1000, /*index=*/7, flatVec);
```

### wrapInSequence

Apply run-length encoding on top of an existing vector:

```cpp
// lengths[i] = how many times row i of base is repeated.
auto lengthsBuf = AlignedBuffer::allocate<int32_t>(base->size(), pool);
auto* lens = lengthsBuf->asMutable<int32_t>();
lens[0] = 3; lens[1] = 2; lens[2] = 1;  // 3+2+1 = 6 output rows

auto seq = BaseVector::wrapInSequence(lengthsBuf, /*size=*/6, base);
// seq->valueAt(0..2) == base->valueAt(0)
// seq->valueAt(3..4) == base->valueAt(1)
// seq->valueAt(5)    == base->valueAt(2)
```

### flattenVector — remove all wrappers

```cpp
VectorPtr v = /* dictionary or constant vector */;
BaseVector::flattenVector(v);  // v is now FLAT
```

### constantify — collapse to a constant if possible

```cpp
// If all rows have the same value, returns a ConstantVector; else nullptr.
VectorPtr maybeConst = BaseVector::constantify(v);
if (maybeConst) {
  // All rows were equal; use the constant representation.
}
```

### transpose — permute without copying

```cpp
// result[i] = source[indices[i]], reusing source if singly referenced.
auto permuted = BaseVector::transpose(indicesBuf, std::move(source));
```

---

## Vector Metadata

```cpp
// Number of rows.
vector_size_t n = vec->size();

// Type.
const TypePtr& t = vec->type();
TypeKind      k  = vec->typeKind();  // TypeKind::BIGINT, etc.

// Encoding.
VectorEncoding::Simple enc = vec->encoding();
bool isFlat     = enc == VectorEncoding::Simple::FLAT;
bool isConst    = vec->isConstantEncoding();

// Null information.
bool mightHaveNulls = vec->mayHaveNulls();            // conservative
bool mightHaveNullsRecursive = vec->mayHaveNullsRecursive();
std::optional<vector_size_t> nc = vec->getNullCount(); // may be nullopt

// Memory usage.
uint64_t retained  = vec->retainedSize();    // all held buffers
uint64_t estimated = vec->estimateFlatSize(); // if fully flattened

// Check if the vector can be written into safely.
bool writable = BaseVector::isVectorWritable(vec);
```

---

## Comparison and Ordering

### Equality

```cpp
// Returns true if row i of 'a' equals row j of 'b' (null == null is false by default).
bool eq = a->equalValueAt(b.get(), /*index=*/i, /*otherIndex=*/j);
```

### Three-way compare

```cpp
// Returns negative/zero/positive, or nullopt if either side is null
// and flags say to return null.
CompareFlags flags;
flags.nullHandlingMode = CompareFlags::NullHandlingMode::kNullAsValue;
std::optional<int32_t> cmp = a->compare(b.get(), i, j, flags);
```

### Sorting

```cpp
// Sort a permutation vector 'indices' so that
// vec->valueAt(indices[0]) ≤ vec->valueAt(indices[1]) ≤ …
std::vector<vector_size_t> indices(vec->size());
std::iota(indices.begin(), indices.end(), 0);
vec->sortIndices(indices, CompareFlags{});

// Sort with an additional indirection mapping.
vec->sortIndices(indices, mapping, CompareFlags{});
```

### Hashing

```cpp
// Hash for a single row.
uint64_t h = vec->hashValueAt(5);

// Hash all rows, returning a FlatVector<uint64_t>.
auto hashes = vec->hashAll();
```

### Find duplicates

```cpp
// Scan rows [start, start+size) and return the index of the first
// duplicate, or nullopt if all are distinct.
std::optional<vector_size_t> dup =
    vec->findDuplicateValue(/*start=*/0, vec->size(), CompareFlags{});
```

---

## Reuse and Performance

### prepareForReuse

Resets a vector in-place for a new batch, avoiding allocation:

```cpp
// Static: release shared children, reset sizes, keep allocations.
BaseVector::prepareForReuse(result, newSize);

// Instance method: resets stats and clears data-dependent flags.
result->prepareForReuse();
```

### VectorPool

`VectorPool` is a thread-local cache that recycles allocated vectors. Pass it
to `ensureWritable` and expression evaluation to amortize allocation cost:

```cpp
VectorPool vectorPool(pool);
BaseVector::ensureWritable(rows, BIGINT(), pool, result, &vectorPool);
```

### resetDataDependentFlags

After modifying values, clear cached statistics so downstream code does not
use stale min/max/distinctCount:

```cpp
flat->resetDataDependentFlags(/*rows=*/nullptr);
```

---

## Null Copy Helpers

These static helpers operate on raw null bitmaps for bulk null propagation:

```cpp
// Set null flags for all rows in 'ranges' to 'isNull'.
uint64_t* targetNulls = /* mutable null bitmap */;
BaseVector::setNulls(targetNulls, ranges, /*isNull=*/true);

// Copy null flags from source to target for rows in 'ranges'.
BaseVector::copyNulls(targetNulls, sourceNulls, ranges);

// Count nulls in a range.
int32_t n = BaseVector::countNulls(nullsBuffer, /*begin=*/0, /*end=*/100);
```

---

## Serialization (VectorSaver)

`VectorSaver` (`velox/vector/VectorSaver.h`) writes vectors to streams or
files, preserving encoding. Useful for debugging and reproducing failures.

```cpp
#include "velox/vector/VectorSaver.h"

// Save.
{
  std::ofstream out("vec.bin", std::ios::binary);
  saveVector(*vec, out);
}

// Load.
{
  std::ifstream in("vec.bin", std::ios::binary);
  VectorPtr restored = restoreVector(in, pool);
}

// Convenience file API.
saveVectorToFile(vec.get(), "/tmp/vec.bin");
VectorPtr v2 = restoreVectorFromFile("/tmp/vec.bin", pool);

// Type only.
{
  std::ofstream out("type.bin", std::ios::binary);
  saveType(vec->type(), out);
}
TypePtr t = restoreType(in);

// SelectivityVector.
saveSelectivityVectorToFile(rows, "/tmp/rows.bin");
SelectivityVector r2 = restoreSelectivityVectorFromFile("/tmp/rows.bin");
```

---

## Complete Example: Building and Querying a Nested Structure

The following builds an `ARRAY(MAP(VARCHAR, BIGINT))` vector and inspects its
contents:

```cpp
// Pool setup.
auto pool = memory::addDefaultLeafMemoryPool();
VectorMaker maker(pool.get());

// Keys: "a", "b", "c", "d"
auto keys   = maker.flatVector<StringView>({"a", "b", "c", "d"});
// Values: 1, 2, 3, 4
auto values = maker.flatVector<int64_t>({1, 2, 3, 4});

// Two maps: {"a"→1, "b"→2} and {"c"→3, "d"→4}
auto maps = maker.mapVector(
    /*offsets=*/{0, 2},
    keys, values,
    /*nulls=*/{});

// Wrap in an ArrayVector: one array containing both maps.
auto array = maker.arrayVector(
    /*offsets=*/{0},
    maps,
    /*nulls=*/{});
// array->size() == 1, array->sizeAt(0) == 2

// Read back.
int32_t arrOffset = array->offsetAt(0);  // 0
int32_t arrLen    = array->sizeAt(0);    // 2
auto* innerMaps   = array->elements()->as<MapVector>();

for (int m = arrOffset; m < arrOffset + arrLen; ++m) {
  int32_t mapOffset = innerMaps->offsetAt(m);
  int32_t mapLen    = innerMaps->sizeAt(m);
  auto* k = innerMaps->mapKeys()->as<FlatVector<StringView>>();
  auto* v = innerMaps->mapValues()->as<FlatVector<int64_t>>();
  for (int e = mapOffset; e < mapOffset + mapLen; ++e) {
    StringView key = k->valueAt(e);
    int64_t   val  = v->valueAt(e);
    // Map 0: "a"→1, "b"→2.  Map 1: "c"→3, "d"→4.
  }
}

// Ensure writable and update a value.
SelectivityVector rows(maps->mapValues()->size());
maps->mapValues()->ensureWritable(rows);
maps->mapValues()->as<FlatVector<int64_t>>()->set(0, 99);  // "a"→99

// Serialize for debugging.
saveVectorToFile(array.get(), "/tmp/array_map.bin");
```

---

## Quick Reference

| Goal | Function / Method |
|------|------------------|
| Create flat vector (generic) | `BaseVector::create<FlatVector<T>>(type, size, pool)` |
| Create flat vector (convenient) | `maker.flatVector<T>(data)` |
| Create nullable flat vector | `maker.flatVectorNullable<T>(optionalData)` |
| Create all-null constant | `BaseVector::createNullConstant(type, size, pool)` |
| Create scalar constant | `BaseVector::createConstant(type, value, size, pool)` |
| Create from Variants | `BaseVector::createFromVariants(type, variants, pool)` |
| Wrap in dictionary | `BaseVector::wrapInDictionary(nulls, indices, size, base)` |
| Wrap in constant | `BaseVector::wrapInConstant(length, index, vector)` |
| Wrap in sequence (RLE) | `BaseVector::wrapInSequence(lengths, size, base)` |
| Flatten all encodings | `BaseVector::flattenVector(vec)` |
| Collapse to constant | `BaseVector::constantify(vec)` |
| Permute rows | `BaseVector::transpose(indices, source)` |
| Deep copy | `BaseVector::copy(*vec, pool)` |
| Zero-copy slice | `vec->slice(offset, length)` |
| Append | `vec->append(other.get())` |
| Copy ranges | `target->copyRanges(source, ranges)` |
| Make writable | `BaseVector::ensureWritable(rows, type, pool, result)` |
| Read value | `flat->valueAt(i)` / `flat->valueAtFast(i)` |
| Check null | `vec->isNullAt(i)` |
| Set value | `flat->set(i, value)` |
| Set null | `vec->setNull(i, true)` |
| Bulk set null | `vec->addNulls(rows)` |
| Bulk clear null | `vec->clearNulls(rows)` |
| Resize | `vec->resize(newSize)` |
| Uniform decode | `DecodedVector::decode(*vec, rows)` |
| Peel wrappers | `vec->wrappedVector()` + `vec->wrappedIndex(i)` |
| Compare rows | `a->compare(b, i, j, flags)` |
| Check equality | `a->equalValueAt(b, i, j)` |
| Hash row | `vec->hashValueAt(i)` |
| Hash all rows | `vec->hashAll()` |
| Sort row indices | `vec->sortIndices(indices, flags)` |
| Serialize to file | `saveVectorToFile(vec, path)` |
| Deserialize from file | `restoreVectorFromFile(path, pool)` |
| Reuse vector | `BaseVector::prepareForReuse(vec, newSize)` |
