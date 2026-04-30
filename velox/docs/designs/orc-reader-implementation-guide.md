# ORC Reader Implementation Guide

This document is a self-contained reference for implementing an ORC file reader
in C++. It draws heavily on the Velox DWRF implementation
(`velox/dwio/dwrf/reader/`) but strips away Velox-specific infrastructure (Folly,
arrow-compatible vectors, Presto connector) so you can build a minimal but
production-quality reader from scratch.

---

## Part 1 — ORC File Format

### 1.1 Physical Layout

An ORC file is laid out as:

```
[ stripe 0 data ]
[ stripe 1 data ]
...
[ stripe N-1 data ]
[ file footer     ]  ← protobuf-encoded
[ postscript      ]  ← protobuf-encoded (uncompressed, tiny)
[ 1-byte psLen    ]  ← length of postscript, written as the last byte
```

Reading starts at the end of the file:

1. Read the last byte → `psLen`.
2. Read the `psLen` bytes before it → deserialise as `PostScript`.

> **Implementation note:** A production reader typically reads the last 16 KB of the
> file in a single I/O operation (enough to cover postscript + footer for almost all
> files), then extracts `psLen` from the final byte of that buffer. A second read is
> only needed for unusually large footers.
3. `PostScript` tells you: compression codec, compression block size, footer length, and metadata length (file-level stripe statistics).
4. Read `footerLength` bytes just before the postscript → decompress → deserialise as `Footer`.
5. `Footer` contains:
   - `types[]` — the schema tree.
   - `stripes[]` — one `StripeInformation` per stripe (offset, lengths, row count).
   - `statistics[]` — per-column file-level statistics.
   - `rowIndexStride` — row-group size (default 10 000).

### 1.2 Stripe Layout

Each stripe is:

```
[ index section  ]   ROW_INDEX streams for each column
[ data section   ]   DATA, LENGTH, DICTIONARY_DATA, PRESENT, ... streams
[ stripe footer  ]   StripeFooter protobuf (uncompressed)
```

`StripeInformation` gives `offset`, `indexLength`, `dataLength`, `footerLength`.
The stripe footer lists every stream: its column id, kind, and byte length.

### 1.3 Stream Kinds

Each column is split into one or more streams identified by `(columnId, kind)`:

| Kind              | Purpose                                        |
|-------------------|------------------------------------------------|
| `PRESENT`         | Null bitmap (ByteRLE, one bit per row)         |
| `DATA`            | Main encoded values                            |
| `LENGTH`          | Lengths for strings/arrays/maps (RLE integers) |
| `DICTIONARY_DATA` | String dictionary bytes                        |
| `DICTIONARY_COUNT`| String dictionary lengths (RLE integers)       |
| `SECONDARY`       | Nanosecond component of timestamps (ORC)       |
| `ROW_INDEX`       | Per-row-group statistics + seek positions      |
| `BLOOM_FILTER`    | Deprecated bloom filter (non-UTF8 encoding); superseded by `BLOOM_FILTER_UTF8` |
| `BLOOM_FILTER_UTF8` | Bloom filter for string columns (the active variant; ORC only supports this form) |

For DWRF (Facebook's ORC variant) there are extra kinds:
`NANO_DATA`, `IN_DICTIONARY`, `STRIDE_DICTIONARY`, `STRIDE_DICTIONARY_LENGTH`,
`IN_MAP` (flat maps).

### 1.4 Column Encodings

`StripeFooter.columns[i].kind` tells you how column `i` is encoded:

| Encoding Kind  | Meaning                                |
|----------------|----------------------------------------|
| `DIRECT`       | Raw values, RLEv1 integers             |
| `DIRECT_V2`    | Raw values, RLEv2 integers             |
| `DICTIONARY`   | Dictionary indices, RLEv1              |
| `DICTIONARY_V2`| Dictionary indices, RLEv2              |

String columns in `DICTIONARY` encoding store the dictionary in
`DICTIONARY_DATA` (concatenated bytes) and `DICTIONARY_COUNT` (lengths); each row
stores an integer index into the dictionary via `DATA`.

### 1.5 Integer Encoding (RLE)

ORC uses two integer encoding schemes:

**RLEv1** — each block starts with a control byte:
- Positive control byte `N` (0–127): the next value repeats `N + 3` times; the
  delta is stored in the following byte as a signed value.
- Negative control byte `N` (-1 to -128): the following `|N|` values are stored
  literally as varints.

**RLEv2** — each block starts with a 2-bit header that selects one of four
sub-encodings:
- `SHORT_REPEAT` (00): a short run of 3–10 identical values.
- `DIRECT` (01): literal values stored in fixed-bit-width packing.
- `PATCHED_BASE` (10): base value + delta + patches for outliers.
- `DELTA` (11): monotone or near-monotone delta encoding.

Use RLEv1 for columns with encoding kind `DIRECT` or `DICTIONARY`.
Use RLEv2 for `DIRECT_V2` or `DICTIONARY_V2`.

### 1.6 Null Encoding (ByteRLE)

The `PRESENT` stream uses ByteRLE — each block starts with a control byte:
- Positive `N`: the following byte repeats `N + 3` times.
- Negative `N`: the following `|N|` bytes are stored literally.

Each byte encodes 8 rows: bit 0 of byte 0 is the first row, etc.
**ORC convention: bit = 1 means the value is present (non-null), bit = 0 means null.**

### 1.7 Compression

Individual streams are compressed in blocks. Each block is preceded by a 3-byte
header:
- Bit 0: `0` = compressed, `1` = original (uncompressed).
- Bits 1-23: block length.

Supported codecs: `ZLIB`, `SNAPPY`, `LZO`, `LZ4`, `ZSTD`, `NONE`.

### 1.8 Row Groups (Strides)

Each column stream contains a `ROW_INDEX` stream — a sequence of `RowIndexEntry`
protobuf messages, one per row group. Each entry contains:
- `positions[]` — byte offsets and decoder state required to seek to the first
  row of that group.
- `statistics` — column stats (min, max, nullCount) used for pruning.

Number of row groups = `ceil(stripeRows / rowIndexStride)`.

### 1.9 Type System

The ORC schema is a flat list of `Type` protobuf messages, numbered from 0.
Type 0 is always the root struct. Child types are referenced by index in
`subtypes[]`. Field names (for structs) are in `fieldNames[]`.

ORC type kinds: `BOOLEAN`, `BYTE`, `SHORT`, `INT`, `LONG`, `FLOAT`, `DOUBLE`,
`STRING`, `BINARY`, `TIMESTAMP`, `LIST`, `MAP`, `STRUCT`, `UNION`, `DECIMAL`,
`DATE`, `VARCHAR`, `CHAR`, `TIMESTAMP_INSTANT`, `GEOMETRY`, `GEOGRAPHY`.

`GEOMETRY` and `GEOGRAPHY` are WKB-encoded geospatial types added in ORC 3.x.
Both are encoded identically to `STRING` (DIRECT or DICTIONARY encoding).
`GEOGRAPHY` additionally carries Coordinate Reference System (CRS) metadata and
an edge interpolation algorithm specifier (`SPHERICAL`, `VINCENTY`, `THOMAS`,
`ANDOYER`, `KARNEY`).

---

## Part 2 — Class Architecture

This section defines the minimal class set for a clean reader implementation.
The names mirror Velox's structure but are stripped of Velox-specific types.

```
FileMetadata        — parsed PostScript + Footer
StripeMetadata      — parsed StripeFooter for one stripe
InputStream         — raw file I/O (read bytes at offset)
SeekableStream      — decompressed, seekable byte view of one stream
ByteRleDecoder      — PRESENT stream decoder
IntDecoder<T>       — abstract integer decoder (RLEv1 or RLEv2)
DirectDecoder<T>    — direct-encoded integer decoder
RleDecoderV1<T>     — RLEv1 decoder
RleDecoderV2<T>     — RLEv2 decoder
TypeWithId          — schema node annotated with column ids
StripeStreams        — stripe-scoped stream factory
ColumnReader        — non-selective base column reader
  IntegerColumnReader
  StringDirectColumnReader
  StringDictionaryColumnReader
  StructColumnReader
  ListColumnReader
  MapColumnReader
SelectiveColumnReader  — filter-integrated base column reader
  SelectiveIntegerColumnReader
  SelectiveStringDirectColumnReader
  SelectiveStringDictionaryColumnReader
  SelectiveStructColumnReader
  SelectiveListColumnReader
  SelectiveMapColumnReader
OrcReader           — file-level entry point
OrcRowReader        — stripe/row iterator
```

---

## Part 3 — Key Classes and APIs

### 3.1 `InputStream`

Abstracts raw file I/O. Implement once for local files and once for remote
storage (S3, HDFS).

```cpp
class InputStream {
public:
    virtual ~InputStream() = default;
    virtual uint64_t getLength() const = 0;
    // Read 'length' bytes starting at 'offset' into 'buf'.
    virtual void read(void* buf, uint64_t length, uint64_t offset) = 0;
    virtual std::string getName() const = 0;
};
```

### 3.2 `SeekableStream`

A decompressed, seekable view of one stripe stream (one `(columnId, kind)` pair).
The key design point: the stream returns pointers into its internal decompression
buffer — callers do not own the memory.

```cpp
class SeekableStream {
public:
    virtual ~SeekableStream() = default;
    // Advance to byte offset 'pos' within the stream.
    // Used to implement seekToRowGroup().
    virtual void seekToPosition(const std::vector<uint64_t>& positions,
                                size_t posIndex) = 0;
    // Standard ZeroCopyInputStream interface:
    virtual bool next(const void** data, int* size) = 0;  // get next buffer chunk
    virtual void backUp(int count) = 0;                   // return 'count' bytes
    virtual int64_t byteCount() const = 0;
};
```

**Construction** — create one per stream:

```cpp
// 1. Locate stream bytes in the stripe via StripeStreams.
// 2. Wrap in a PagedInputStream that reads compressed 3-byte-header blocks.
// 3. The PagedInputStream decompresses each block on demand.
std::unique_ptr<SeekableStream> makeStream(
    InputStream& file,
    uint64_t offset, uint64_t length,
    CompressionKind codec, uint64_t blockSize);
```

### 3.3 `ByteRleDecoder`

Decodes the `PRESENT` stream into a null bitmap.

```cpp
class ByteRleDecoder {
public:
    ByteRleDecoder(std::unique_ptr<SeekableStream> input);

    // Decode 'numValues' bits into 'data'. Each byte of 'data' represents 8 rows.
    // Bit = 1 → present (non-null). Bit = 0 → null.
    void next(char* data, uint64_t numValues, const char* notNull);

    // Skip 'numValues' bits.
    uint64_t skip(uint64_t numValues);

    // Seek to row group 'index' using the positions from ROW_INDEX.
    void seekToRowGroup(uint32_t index, const std::vector<uint64_t>& positions);

private:
    std::unique_ptr<SeekableStream> input_;
    int64_t remainingValues_{0}; // values left in current run
    char value_{0};              // current run value
    bool repeating_{false};      // true = run, false = literals
};
```

### 3.4 `IntDecoder<T>`

Abstract base for integer stream decoders.

```cpp
template <bool isSigned>
class IntDecoder {
public:
    using SignedType = typename std::conditional<isSigned, int64_t, uint64_t>::type;
    virtual ~IntDecoder() = default;

    // Decode 'numValues' integers into 'data'. 'nulls' is a null bitmap;
    // if a bit is 0 (null), the slot in 'data' receives 0.
    virtual void next(SignedType* data, uint64_t numValues,
                      const uint64_t* nulls) = 0;

    // Skip 'numValues' non-null values.
    virtual void skip(uint64_t numValues) = 0;

    // Seek to row group using positions from ROW_INDEX.
    virtual void seekToRowGroup(uint32_t index,
                                const std::vector<uint64_t>& positions) = 0;

    // Used by the selective path: decode while calling visitor per value.
    template <typename Visitor>
    void readWithVisitor(const uint64_t* nulls, Visitor& visitor);
};
```

Factory function:

```cpp
// version: RleVersion_1 or RleVersion_2
// useVInts: true for most DWRF streams, false for some binary columns
template <bool isSigned>
std::unique_ptr<IntDecoder<isSigned>> createRleDecoder(
    std::unique_ptr<SeekableStream> input,
    RleVersion version,
    bool useVInts,
    uint32_t numBytes = sizeof(int64_t));
```

### 3.5 `TypeWithId`

Annotates each schema node with its column id, which is the index in the flat
`Footer.types[]` array. Every stream is identified by `(columnId, streamKind)`.

```cpp
struct TypeWithId {
    using NodePtr = std::shared_ptr<const TypeWithId>;

    TypeKind kind;            // STRUCT, INT, STRING, LIST, MAP, ...
    uint32_t id;              // column id (0 = root struct)
    uint32_t maxId;           // highest id in this subtree (for range checks)
    std::vector<std::string> names;    // struct field names
    std::vector<NodePtr> children;     // child types

    static NodePtr build(const Footer& footer, uint32_t index = 0);
};
```

### 3.6 `StripeStreams`

Provides access to streams and encoding metadata for one stripe. Readers call
this to get decoders; the implementation handles offset arithmetic and
decompression.

```cpp
class StripeStreams {
public:
    virtual ~StripeStreams() = default;

    // The encoding kind for a given column id.
    virtual EncodingKind getEncoding(uint32_t columnId) const = 0;

    // The size of the string/integer dictionary for a given column.
    virtual uint32_t getDictionarySize(uint32_t columnId) const = 0;

    // Open a stream. Returns nullptr if not found and 'required' is false.
    virtual std::unique_ptr<SeekableStream> getStream(
        uint32_t columnId, StreamKind kind, bool required) const = 0;

    // Convenience: open and wrap in a ByteRleDecoder.
    std::unique_ptr<ByteRleDecoder> getPresent(uint32_t columnId) {
        auto s = getStream(columnId, StreamKind::PRESENT, false);
        return s ? std::make_unique<ByteRleDecoder>(std::move(s)) : nullptr;
    }

    // Memory pool for allocating decode buffers.
    virtual MemoryPool& pool() const = 0;

    // Number of rows in this stripe.
    virtual int64_t stripeRows() const = 0;

    // Number of rows per row group (rowIndexStride from footer).
    virtual uint32_t rowsPerRowGroup() const = 0;
};
```

**Implementation sketch** (`StripeStreamsImpl`):

```cpp
class StripeStreamsImpl : public StripeStreams {
    // Constructed from file-level metadata and stripe index.
    // On construction: parse the stripe footer (already in memory as
    // StripeMetadata), build a map from (columnId, kind) → (fileOffset, length).
    // loadStreams() populates streams_.

    std::unordered_map<StreamKey, StreamInfo> streams_;
    InputStream& file_;
    CompressionKind codec_;
    uint64_t blockSize_;
    MemoryPool& pool_;
    // ...
};
```

### 3.7 `ColumnReader` (Non-Selective Path)

The non-selective path decodes every row unconditionally and returns a dense
`FlatVector`.

```cpp
class ColumnReader {
public:
    ColumnReader(uint32_t columnId, StripeStreams& stripe);
    virtual ~ColumnReader() = default;

    // Decode 'numValues' rows into 'result'.
    // 'incomingNulls' carries parent-level null bits (null struct ⇒ no child value).
    virtual void next(uint64_t numValues, ColumnVector& result,
                      const uint64_t* incomingNulls = nullptr) = 0;

    // Advance stream past 'numValues' rows; return non-null count.
    virtual uint64_t skip(uint64_t numValues);

    // Factory: build the right subclass based on type and encoding.
    static std::unique_ptr<ColumnReader> build(
        const TypeWithId& type, StripeStreams& stripe);

protected:
    // Read the PRESENT stream and merge with incomingNulls into 'nulls'.
    // Returns non-null count.
    uint64_t readNulls(uint64_t numValues, const uint64_t* incomingNulls,
                       std::vector<uint64_t>& nulls);

    std::unique_ptr<ByteRleDecoder> notNullDecoder_; // nullptr if column is never null
    MemoryPool& pool_;
};
```

#### `IntegerColumnReader`

```cpp
class IntegerColumnReader : public ColumnReader {
public:
    IntegerColumnReader(uint32_t columnId, TypeKind requestedType,
                        StripeStreams& stripe);

    void next(uint64_t numValues, ColumnVector& result,
              const uint64_t* incomingNulls) override;

private:
    std::unique_ptr<IntDecoder<true>> decoder_; // decodes DATA stream
    TypeKind requestedType_;
};
```

`next()` algorithm:
1. Read nulls into `nullBuf`.
2. Allocate `values` buffer of `numValues` × `sizeof(RequestedType)`.
3. Call `decoder_->next(values, numValues, nullBuf)` — decoder writes 0 for null slots.
4. Wrap in `FlatVector<RequestedType>` with the null buffer.

#### `StringDirectColumnReader`

```cpp
class StringDirectColumnReader : public ColumnReader {
public:
    void next(uint64_t numValues, ColumnVector& result,
              const uint64_t* incomingNulls) override;

private:
    std::unique_ptr<IntDecoder<false>> lengthDecoder_; // LENGTH stream
    std::unique_ptr<SeekableStream> dataStream_;       // DATA stream (raw bytes)
};
```

`next()` algorithm:
1. Read nulls.
2. Read `numValues` lengths from `lengthDecoder_` (0 for null slots).
3. Read `sum(lengths)` bytes from `dataStream_`.
4. Build an array of `StringView` pointing into the data buffer.

#### `StringDictionaryColumnReader`

```cpp
class StringDictionaryColumnReader : public ColumnReader {
public:
    void next(uint64_t numValues, ColumnVector& result,
              const uint64_t* incomingNulls) override;

private:
    // Dictionary loaded once at construction time:
    std::vector<std::string> dictionary_;   // decoded string values

    // Per-row index into dictionary_:
    std::unique_ptr<IntDecoder<false>> indexDecoder_; // DATA stream
};
```

`next()` algorithm:
1. Read nulls.
2. Read `numValues` indices.
3. For each non-null row, `result[i] = dictionary_[index[i]]`.

#### `StructColumnReader`

```cpp
class StructColumnReader : public ColumnReader {
public:
    void next(uint64_t numValues, ColumnVector& result,
              const uint64_t* incomingNulls) override;

private:
    std::vector<std::unique_ptr<ColumnReader>> children_;
};
```

`next()` algorithm:
1. Read struct-level nulls into `structNulls`.
2. Merge `structNulls` with `incomingNulls` (AND) → `childNulls`.
3. For each child: call `child->next(numValues, childResult, childNulls)`.
4. Assemble as `StructVector`.

#### `ListColumnReader`

```cpp
class ListColumnReader : public ColumnReader {
public:
    void next(uint64_t numValues, ColumnVector& result,
              const uint64_t* incomingNulls) override;

private:
    std::unique_ptr<IntDecoder<false>> lengthDecoder_; // LENGTH stream
    std::unique_ptr<ColumnReader> child_;
};
```

`next()` algorithm:
1. Read list-level nulls.
2. Read `numValues` lengths (0 for null lists).
3. Total child rows = `sum(lengths)`.
4. Call `child_->next(totalChildRows, childResult, nullptr)`.
5. Build offset buffer from lengths; return `ArrayVector(offsets, child)`.

---

### 3.8 `SelectiveColumnReader` (Selective Path)

The selective path fuses filtering into the decode loop. It is the production
hot path and should be preferred when any filter is present.

```cpp
class SelectiveColumnReader {
public:
    SelectiveColumnReader(const TypeWithId& type, FormatParams& params,
                          const ScanSpec& scanSpec);
    virtual ~SelectiveColumnReader() = default;

    // Phase 1: decode + filter rows in 'rows' (indices into the stripe).
    // incomingNulls: null bitmap from parent struct (nullptr = all present).
    virtual void read(int64_t offset, const RowSet& rows,
                      const uint64_t* incomingNulls) = 0;

    // Phase 2: package buffered results into a vector.
    virtual void getValues(const RowSet& rows, ColumnVector* result) = 0;

    // Factory: build the right subclass.
    static std::unique_ptr<SelectiveColumnReader> build(
        const TypeWithId& fileType, FormatParams& params,
        const ScanSpec& scanSpec);

    // Row indices that passed the filter (populated after read()).
    const RowSet& outputRows() const { return outputRows_; }

protected:
    // Seek to 'offset', decode nulls for the full span, reset scratch buffers.
    template <typename T>
    void prepareRead(int64_t offset, const RowSet& rows,
                     const uint64_t* incomingNulls);

    // Called by visitor to record a passing value.
    template <typename T>
    inline void addValue(T value) {
        reinterpret_cast<T*>(rawValues_)[numValues_++] = value;
    }
    inline void addOutputRow(vector_size_t row) {
        outputRows_.push_back(row);
    }
    template <typename T>
    inline void addNull() {
        reinterpret_cast<T*>(rawValues_)[numValues_++] = T{};
        anyNulls_ = true;
    }

    FormatData* formatData_;     // owns notNullDecoder, row-group index
    const ScanSpec* scanSpec_;   // filter + projection spec

    // Scratch buffers (live for one read()+getValues() cycle):
    BufferPtr values_;           // packed decoded values (passing)
    void* rawValues_{nullptr};
    int32_t numValues_{0};
    int8_t valueSize_{0};

    BufferPtr nullsInReadRange_; // full null bitmap for rows.back()+1 rows
    BufferPtr resultNulls_;      // null bitmap for passing values
    uint64_t* rawResultNulls_{nullptr};

    raw_vector<vector_size_t> outputRows_; // row indices that passed filter
    bool anyNulls_{false};
    bool allNull_{false};
    bool returnReaderNulls_{false}; // optimization: reuse nullsInReadRange_ directly

    int64_t readOffset_{0};     // next row to be decoded
};
```

#### `ColumnVisitor` — the filter-decode hot path

Instead of decoding all values and filtering afterwards, the decoder calls a
visitor once per value. All branching (filter type, dense vs sparse rows,
extract vs drop) is resolved at compile time via template parameters.

```cpp
template <typename T,          // decoded value type: int64_t, float, StringView
          typename TFilter,    // AlwaysTrue, BigintRange, BytesRange, ...
          typename ExtractValues, // ExtractToVector, DropValues
          bool isDense>        // true when rows = {0,1,2,...,N-1}
class ColumnVisitor {
public:
    ColumnVisitor(TFilter& filter, SelectiveColumnReader* reader,
                  const RowSet& rows, ExtractValues values);

    // Called by decoder for each non-null decoded value.
    // Returns: how many stream positions to skip before the next call.
    inline int32_t process(T value, bool atEnd) {
        if (applyFilter(filter_, value)) {
            values_.addValue(currentRow(), value);
            reader_->addOutputRow(currentRow());
        }
        return advance();
    }

    // Called by decoder when the PRESENT bitmap marks current row null.
    inline int32_t processNull(bool atEnd) {
        if (filter_.testNull()) {
            values_.addNull<T>(currentRow());
            reader_->addOutputRow(currentRow());
        }
        return advance();
    }

private:
    TFilter& filter_;
    SelectiveColumnReader* reader_;
    const RowSet& rows_;
    ExtractValues values_;
    int32_t rowIndex_{0};
};
```

**`readCommon()`** — selects the right visitor instance at runtime, dispatching
on whether there is a filter and whether rows are dense:

```cpp
template <typename Reader>
void SelectiveIntegerColumnReader::readCommon(const RowSet& rows) {
    bool isDense = (rows.back() == static_cast<int32_t>(rows.size()) - 1);
    if (scanSpec_->keepValues()) {
        processFilter<Reader, /*isDense*/ false>(
            scanSpec_->filter(), ExtractToVector(this), rows);
    } else {
        processFilter<Reader, false>(
            scanSpec_->filter(), DropValues(), rows);
    }
    // processFilter switches on filter->kind() and isDense to call readHelper
    // with the right template instantiation.
}
```

#### `SelectiveIntegerColumnReader`

```cpp
class SelectiveIntegerColumnReader : public SelectiveColumnReader {
public:
    SelectiveIntegerColumnReader(const TypeWithId& fileType, FormatParams& params,
                                 const ScanSpec& scanSpec);

    void read(int64_t offset, const RowSet& rows,
              const uint64_t* incomingNulls) override;

    void getValues(const RowSet& rows, ColumnVector* result) override;

private:
    template <typename ColumnVisitor>
    void readWithVisitor(const RowSet& rows, ColumnVisitor visitor);

    std::unique_ptr<IntDecoder<true>> decoder_; // DATA stream decoder
};
```

`read()` algorithm:
1. `prepareRead<int64_t>(offset, rows, incomingNulls)`:
   - Seek to `offset` if needed.
   - Decode the PRESENT stream for `rows.back()+1` rows into `nullsInReadRange_`.
   - Allocate scratch buffers.
2. `readCommon(rows)`:
   - Choose `ColumnVisitor` based on filter kind and density.
   - Call `decoder_->readWithVisitor(nullsInReadRange_, visitor)`.
3. After `read()`: `outputRows_` lists rows that passed.

`getValues()` algorithm:
1. `compactScalarValues<int64_t, RequestedType>(rows)`:
   - Remove values not in `outputRows_` (filter may have produced gaps).
   - Narrow from `int64_t` to `int32_t`/`int16_t` if needed.
2. Wrap in `FlatVector<RequestedType>`.

#### `SelectiveStructColumnReader`

The struct reader is the orchestrator. Its `next()` method drives the whole tree.

```cpp
class SelectiveStructColumnReader : public SelectiveColumnReader {
public:
    // Entry point — called by OrcRowReader::next().
    void next(uint64_t numValues, ColumnVector& result,
              const Mutation* mutation) override;

    void read(int64_t offset, const RowSet& rows,
              const uint64_t* incomingNulls) override;

    void getValues(const RowSet& rows, ColumnVector* result) override;

private:
    std::vector<std::unique_ptr<SelectiveColumnReader>> children_;
};
```

`next()` drives the per-stripe read cycle:
1. Compute `rows = [0, 1, ..., numValues-1]` (dense).
2. Call `read(offset_, rows, nullptr)`.
3. For each projected child: `child->getValues(child->outputRows(), &fieldVec)`.
4. Assemble `StructVector(fields...)`.

`read()`:
1. Read struct nulls → `structNulls`.
2. Compute child row set: rows where struct is non-null.
3. For each child: `child->read(offset, nonNullRows, structNulls)`.

---

### 3.9 `FormatData`

Bridges between the generic `SelectiveColumnReader` and DWRF/ORC-specific stream
structures. Each reader holds a `FormatData` produced by `FormatParams`.

```cpp
class FormatData {
public:
    virtual ~FormatData() = default;

    // Decode 'numValues' null bits from the PRESENT stream.
    // Merges with 'incomingNulls'. Writes into 'nulls'.
    virtual void readNulls(int32_t numValues, const uint64_t* incomingNulls,
                           BufferPtr& nulls) = 0;

    // Skip 'numValues' null bits.
    virtual uint64_t skipNulls(uint64_t numValues) = 0;

    // Test whether stride 'i' can be skipped based on filter + statistics.
    virtual void filterRowGroups(const ScanSpec& spec,
                                 uint64_t rowsPerRowGroup,
                                 FilterRowGroupsResult& result) = 0;

    // Seek decoders to the first row of row group 'index'.
    // Returns a PositionProvider that column-specific decoders use to
    // seek their own streams.
    virtual PositionProvider seekToRowGroup(int64_t index) = 0;

    virtual bool hasNulls() const = 0;
};

// ORC-specific implementation:
class OrcFormatData : public FormatData {
    std::unique_ptr<ByteRleDecoder> notNullDecoder_;
    std::unique_ptr<SeekableStream> indexStream_; // ROW_INDEX stream
    RowIndex index_;                               // decoded on demand
    // ...
};
```

`FormatParams` is the factory:

```cpp
class FormatParams {
public:
    explicit FormatParams(MemoryPool& pool);
    virtual std::unique_ptr<FormatData> toFormatData(
        const TypeWithId& type, const ScanSpec& spec) = 0;
};
```

### 3.10 `Filter` Classes

Filters are stateless predicates. They are passed to the `ColumnVisitor` as
a template parameter so all virtual dispatch disappears in the hot loop.

```cpp
class Filter {
public:
    virtual FilterKind kind() const = 0;
    virtual bool testNull() const { return nullAllowed_; }
    virtual bool testInt64(int64_t value) const { return false; }
    virtual bool testFloat(float v) const { return false; }
    virtual bool testDouble(double v) const { return false; }
    virtual bool testStringView(std::string_view v) const { return false; }
    // Row-group pruning:
    virtual bool testInt64Range(int64_t min, int64_t max,
                                bool hasNull) const { return true; }
    virtual bool testStringRange(std::string_view min, std::string_view max,
                                 bool hasNull) const { return true; }
protected:
    bool nullAllowed_;
};

class AlwaysTrue : public Filter { /* testInt64 always returns true */ };

class BigintRange : public Filter {
    int64_t lower_, upper_;
public:
    bool testInt64(int64_t value) const override {
        return value >= lower_ && value <= upper_;
    }
    bool testInt64Range(int64_t min, int64_t max, bool) const override {
        return !(min > upper_ || max < lower_);
    }
};

class BytesRange : public Filter {
    std::string lower_, upper_;
    bool singleValue_; // equality check when lower == upper
public:
    bool testStringView(std::string_view v) const override {
        return v >= lower_ && v <= upper_;
    }
};
```

### 3.11 `ScanSpec`

Carries the projection + filters for one query.

```cpp
class ScanSpec {
public:
    // The filter for this column (nullptr = no filter).
    Filter* filter() const;
    bool hasFilter() const;

    // Whether values should be materialized (projected column).
    bool keepValues() const;

    // Child ScanSpecs for nested types (structs, lists, maps).
    const std::vector<ScanSpec*>& children() const;

    // Find the ScanSpec for a struct field by name.
    ScanSpec* getOrCreateChild(std::string_view name);
};
```

### 3.12 `OrcReader` and `OrcRowReader`

```cpp
class OrcReader {
public:
    // Open a file and parse its footer.
    static std::unique_ptr<OrcReader> create(
        std::unique_ptr<InputStream> input);

    const TypeWithId& schema() const;
    uint64_t numberOfRows() const;
    uint32_t numberOfStripes() const;
    std::vector<uint64_t> rowsPerStripe() const;

    // Create a row reader (starts iteration from the first stripe).
    std::unique_ptr<OrcRowReader> createRowReader(
        const RowReaderOptions& options) const;

private:
    FileMetadata metadata_;
    std::unique_ptr<InputStream> input_;
};

class OrcRowReader {
public:
    // Read up to 'numRows' rows into 'result'.
    // Returns actual rows read (< numRows at end of file).
    uint64_t next(uint64_t numRows, ColumnVector& result);

    // Skip rows.
    uint64_t skipRows(uint64_t numRows);

    // Seek to an absolute row number.
    uint64_t seekToRow(uint64_t rowNumber);

private:
    void loadCurrentStripe();           // lazy stripe I/O
    void checkSkipStrides(uint64_t strideSize); // stride pruning
    uint64_t readNext(uint64_t numRows, ColumnVector& result);

    std::shared_ptr<OrcReader> reader_;
    RowReaderOptions options_;
    uint32_t currentStripe_{0};
    uint64_t currentRowInStripe_{0};

    // Either the non-selective or selective column reader tree:
    std::unique_ptr<ColumnReader> columnReader_;                 // non-selective
    std::unique_ptr<SelectiveColumnReader> selectiveReader_;     // selective
};
```

---

## Part 4 — Wiring Everything Together

### 4.1 Opening a File

```cpp
auto input = std::make_unique<LocalInputStream>("/path/to/file.orc");
auto reader = OrcReader::create(std::move(input));
```

Inside `OrcReader::create()`:
1. Read last byte → `psLen`.
2. Read `psLen` bytes from `fileLen - 1 - psLen` → parse as `PostScript`.
3. Read `footerLen` bytes just before PostScript → decompress → parse as `Footer`.
4. Build `TypeWithId` tree from `footer.types`.
5. Cache `StripeMetadata` (stripe footers) if `rowIndexStride > 0`.

### 4.2 Creating a Row Reader

```cpp
RowReaderOptions opts;
opts.setProjectedColumns({"col1", "col2"});   // optional column selection
opts.setScanSpec(myScanSpec);                 // optional filter pushdown

auto rowReader = reader->createRowReader(opts);
```

Inside `createRowReader()`:
1. Build `ColumnSelector` from projected columns.
2. If `scanSpec` present → create `SelectiveColumnReader` tree.
3. Else → create `ColumnReader` tree.

### 4.3 Reading a Stripe

Called lazily on the first `next()` that needs a new stripe:

```cpp
void OrcRowReader::loadCurrentStripe() {
    // 1. Fetch StripeMetadata (parse stripe footer from file).
    auto meta = reader_->loadStripe(currentStripe_);

    // 2. Build StripeStreams (map of stream locations).
    auto streams = std::make_unique<StripeStreamsImpl>(
        reader_->input(), meta, opts_);

    // 3. Re-build column readers for this stripe.
    //    (encoding may differ stripe-to-stripe, especially dict vs direct)
    if (selectiveReader_) {
        DwrfParams params(*streams, streamLabels_, *columnReaderStats_);
        selectiveReader_ = SelectiveDwrfReader::build(
            selectedType, fileType, params, *opts_.scanSpec());
    } else {
        columnReader_ = ColumnReader::build(
            selectedType, fileType, *streams, streamLabels_);
    }
}
```

### 4.4 Non-Selective Read Loop

```cpp
uint64_t OrcRowReader::next(uint64_t numRows, ColumnVector& result) {
    while (numRows > 0) {
        if (currentRowInStripe_ >= rowsInCurrentStripe_) {
            if (++currentStripe_ >= reader_->numberOfStripes()) break;
            loadCurrentStripe();
        }
        uint64_t batchSize = std::min(numRows,
            rowsInCurrentStripe_ - currentRowInStripe_);
        columnReader_->next(batchSize, result, nullptr);
        currentRowInStripe_ += batchSize;
        numRows -= batchSize;
    }
    return /* rows read */;
}
```

### 4.5 Selective Read Loop

```cpp
uint64_t OrcRowReader::next(uint64_t numRows, ColumnVector& result) {
    if (currentRowInStripe_ == 0) checkSkipStrides(strideSize_);
    uint64_t batchSize = computeBatchSize(numRows);
    selectiveReader_->next(batchSize, result, nullptr);
    currentRowInStripe_ += batchSize;
    return batchSize;
}
```

`checkSkipStrides()` calls `filterRowGroups()` on the root selective reader,
which delegates to each column's `FormatData::filterRowGroups()`. The result is a
bit set of strides to skip; when a stride is skipped, all column readers call
`seekToRowGroup(nextNonSkippedStride)`.

### 4.6 Building Column Readers

#### Non-Selective Factory

```cpp
std::unique_ptr<ColumnReader> ColumnReader::build(
    const TypeWithId& type, StripeStreams& stripe) {
    switch (type.kind) {
        case TypeKind::INT:
        case TypeKind::LONG:
            return std::make_unique<IntegerColumnReader>(
                type.id, type.kind, stripe);
        case TypeKind::STRING:
            if (stripe.getEncoding(type.id) == DICTIONARY)
                return std::make_unique<StringDictionaryColumnReader>(type.id, stripe);
            return std::make_unique<StringDirectColumnReader>(type.id, stripe);
        case TypeKind::STRUCT: {
            auto reader = std::make_unique<StructColumnReader>(type.id, stripe);
            for (auto& child : type.children)
                reader->addChild(build(*child, stripe));
            return reader;
        }
        case TypeKind::LIST: {
            auto child = build(*type.children[0], stripe);
            return std::make_unique<ListColumnReader>(type.id, stripe, std::move(child));
        }
        // ... MAP, FLOAT, DOUBLE, BOOLEAN, TIMESTAMP, DECIMAL, DATE
    }
}
```

#### Selective Factory

```cpp
std::unique_ptr<SelectiveColumnReader> SelectiveColumnReader::build(
    const TypeWithId& fileType, FormatParams& params,
    const ScanSpec& scanSpec) {
    switch (fileType.kind) {
        case TypeKind::INT:
        case TypeKind::LONG:
            return std::make_unique<SelectiveIntegerColumnReader>(
                fileType, params, scanSpec);
        case TypeKind::STRING:
            if (isDictEncoded(fileType, params))
                return std::make_unique<SelectiveStringDictionaryColumnReader>(
                    fileType, params, scanSpec);
            return std::make_unique<SelectiveStringDirectColumnReader>(
                fileType, params, scanSpec);
        case TypeKind::STRUCT: {
            auto reader = std::make_unique<SelectiveStructColumnReader>(
                fileType, params, scanSpec);
            for (auto& childSpec : scanSpec.children()) {
                auto* childType = fileType.findChild(childSpec->field());
                reader->addChild(build(*childType, params, *childSpec));
            }
            return reader;
        }
        // ...
    }
}
```

### 4.7 String Dictionary Loading

Dictionary columns load their dictionary once per stripe, at construction time:

```cpp
StringDictionaryColumnReader::StringDictionaryColumnReader(
    uint32_t columnId, StripeStreams& stripe) {
    // 1. Read all dictionary bytes from DICTIONARY_DATA stream.
    auto dataStream = stripe.getStream(columnId, StreamKind::DICTIONARY_DATA, true);
    uint32_t dictSize = stripe.getDictionarySize(columnId);

    // 2. Read dictionary entry lengths from DICTIONARY_COUNT stream.
    auto countDecoder = createRleDecoder<false>(
        stripe.getStream(columnId, StreamKind::DICTIONARY_COUNT, true),
        stripe.getEncoding(columnId) == DICTIONARY ? RleVersion_1 : RleVersion_2,
        /*useVInts=*/true);

    std::vector<uint64_t> lengths(dictSize);
    countDecoder->next(lengths.data(), dictSize, nullptr);

    // 3. Build string table.
    dictionary_.resize(dictSize);
    for (uint32_t i = 0; i < dictSize; ++i) {
        dictionary_[i].resize(lengths[i]);
        dataStream->readFully(dictionary_[i].data(), lengths[i]);
    }

    // 4. Index decoder reads from DATA stream.
    indexDecoder_ = createRleDecoder<false>(
        stripe.getStream(columnId, StreamKind::DATA, true),
        /* version from encoding kind */, /*useVInts=*/true);
}
```

### 4.8 Seeking to a Row Group

When a stride is not skipped, every column reader must seek its decoders to the
first row of that stride. Position information comes from the `ROW_INDEX` stream:

```cpp
void OrcFormatData::seekToRowGroup(int64_t index) {
    // Parse ROW_INDEX on first call (decode the stream once).
    ensureRowGroupIndex();

    // Get the RowIndexEntry for this stride.
    const auto& entry = rowIndex_.entry(index);
    std::vector<uint64_t> positions(entry.positions().begin(),
                                     entry.positions().end());

    // The positions for the PRESENT stream come first.
    size_t posIdx = 0;
    if (notNullDecoder_) {
        notNullDecoder_->seekToRowGroup(index, positions); // uses posIdx
        posIdx += notNullDecoder_->positionSize();
    }

    // Return remaining positions for the data decoder.
    // The caller (column reader) uses these to seek its own decoder.
}
```

The positions array layout is decoder-specific. For a compressed stream:
```
[compressedBlockOffset, blockByteOffset, valuesConsumedInBlock]
```

For an uncompressed (direct) stream:
```
[byteOffset, valuesConsumedInCurrentRun]
```

---

## Part 5 — Test Cases

### 5.1 File Format Tests

```cpp
// Test: ORC file with single integer column, no nulls
TEST(OrcReaderTest, SingleIntColumn) {
    // Write: [1, 2, 3, 4, 5] as INT, no nulls, DIRECT encoding
    auto reader = openTestFile("single_int.orc");
    auto rowReader = reader->createRowReader({});
    ColumnVector result;
    EXPECT_EQ(rowReader->next(5, result), 5);
    auto& vec = result.asFlat<int32_t>();
    EXPECT_THAT(vec.values, ElementsAre(1, 2, 3, 4, 5));
    EXPECT_FALSE(result.hasNulls());
}

// Test: PRESENT stream with nulls
TEST(OrcReaderTest, NullableIntColumn) {
    // Write: [1, null, 3, null, 5]
    auto reader = openTestFile("nullable_int.orc");
    auto rowReader = reader->createRowReader({});
    ColumnVector result;
    rowReader->next(5, result);
    EXPECT_TRUE(result.hasNulls());
    EXPECT_FALSE(result.isNullAt(0));  EXPECT_EQ(result.valueAt<int32_t>(0), 1);
    EXPECT_TRUE(result.isNullAt(1));
    EXPECT_FALSE(result.isNullAt(2));  EXPECT_EQ(result.valueAt<int32_t>(2), 3);
    EXPECT_TRUE(result.isNullAt(3));
    EXPECT_FALSE(result.isNullAt(4));  EXPECT_EQ(result.valueAt<int32_t>(4), 5);
}

// Test: all nulls → ConstantVector
TEST(OrcReaderTest, AllNullColumn) {
    auto reader = openTestFile("all_null.orc");
    auto rowReader = reader->createRowReader({});
    ColumnVector result;
    rowReader->next(5, result);
    EXPECT_TRUE(result.isConstantNull());
}
```

### 5.2 Encoding Tests

```cpp
// Test: RLEv1 runs (same value repeated)
TEST(OrcReaderTest, Rle1Run) {
    // 10 000 rows all equal 42
    auto reader = openTestFile("rle1_run.orc");
    auto rowReader = reader->createRowReader({});
    ColumnVector result;
    rowReader->next(10000, result);
    for (int i = 0; i < 10000; ++i)
        EXPECT_EQ(result.valueAt<int64_t>(i), 42);
}

// Test: RLEv2 delta encoding (arithmetic sequence)
TEST(OrcReaderTest, Rle2Delta) {
    // [0, 1, 2, ..., 9999]
    auto reader = openTestFile("rle2_delta.orc");
    auto rowReader = reader->createRowReader({});
    ColumnVector result;
    rowReader->next(10000, result);
    for (int i = 0; i < 10000; ++i)
        EXPECT_EQ(result.valueAt<int64_t>(i), i);
}

// Test: string dictionary encoding
TEST(OrcReaderTest, StringDictionary) {
    // 1000 rows cycling through ["apple", "banana", "cherry"]
    auto reader = openTestFile("string_dict.orc");
    auto rowReader = reader->createRowReader({});
    ColumnVector result;
    rowReader->next(1000, result);
    static const char* expected[] = {"apple", "banana", "cherry"};
    for (int i = 0; i < 1000; ++i)
        EXPECT_EQ(result.valueAt<std::string_view>(i), expected[i % 3]);
}

// Test: string direct encoding
TEST(OrcReaderTest, StringDirect) {
    // 5 rows: ["hello", "world", "foo", "bar", "baz"]
    auto reader = openTestFile("string_direct.orc");
    auto rowReader = reader->createRowReader({});
    ColumnVector result;
    rowReader->next(5, result);
    EXPECT_THAT(collectStrings(result), ElementsAre("hello","world","foo","bar","baz"));
}
```

### 5.3 Nested Type Tests

```cpp
// Test: struct with two columns
TEST(OrcReaderTest, StructTwoColumns) {
    // rows: {a: int, b: string}
    auto reader = openTestFile("struct_two_cols.orc");
    auto rowReader = reader->createRowReader({});
    ColumnVector result;
    rowReader->next(3, result);
    auto& s = result.asStruct();
    EXPECT_EQ(s.field(0).valueAt<int32_t>(0), 10);
    EXPECT_EQ(s.field(1).valueAt<std::string_view>(0), "ten");
}

// Test: list column
TEST(OrcReaderTest, ListColumn) {
    // rows: [[1,2,3], [], [4,5]]
    auto reader = openTestFile("list_col.orc");
    auto rowReader = reader->createRowReader({});
    ColumnVector result;
    rowReader->next(3, result);
    auto& list = result.asList();
    EXPECT_EQ(list.sizeAt(0), 3);
    EXPECT_EQ(list.sizeAt(1), 0);
    EXPECT_EQ(list.sizeAt(2), 2);
    EXPECT_EQ(list.elementAt<int32_t>(0), 1);
    EXPECT_EQ(list.elementAt<int32_t>(3), 4);
}

// Test: map column
TEST(OrcReaderTest, MapColumn) {
    // rows: [{1:"a"}, {2:"b", 3:"c"}]
    auto reader = openTestFile("map_col.orc");
    // ... validate key/value pairs
}

// Test: nullable struct — child skips rows where parent is null
TEST(OrcReaderTest, NullableStruct) {
    // rows: [{a:1, b:"x"}, null, {a:3, b:"z"}]
    auto reader = openTestFile("nullable_struct.orc");
    auto rowReader = reader->createRowReader({});
    ColumnVector result;
    rowReader->next(3, result);
    auto& s = result.asStruct();
    EXPECT_FALSE(result.isNullAt(0));
    EXPECT_TRUE(result.isNullAt(1));
    EXPECT_FALSE(result.isNullAt(2));
    EXPECT_EQ(s.field(0).valueAt<int32_t>(2), 3);
}
```

### 5.4 Filter Pushdown Tests

```cpp
// Test: BigintRange filter selects a subset of rows
TEST(OrcReaderTest, FilterBigintRange) {
    // 100 rows: [0..99]. Filter: value > 50.
    auto spec = std::make_unique<ScanSpec>("root");
    spec->getOrCreateChild("value")->setFilter(
        std::make_unique<BigintRange>(51, INT64_MAX, false));

    auto reader = openTestFile("int_0_to_99.orc");
    RowReaderOptions opts;
    opts.setScanSpec(spec.get());
    auto rowReader = reader->createRowReader(opts);
    ColumnVector result;
    int total = 0;
    while (rowReader->next(1024, result) > 0) {
        for (int i = 0; i < result.size(); ++i) {
            EXPECT_GT(result.valueAt<int64_t>(i), 50);
            ++total;
        }
    }
    EXPECT_EQ(total, 49); // 51..99
}

// Test: IsNull filter
TEST(OrcReaderTest, FilterIsNull) {
    // 10 rows, rows 3 and 7 are null. Filter: IS NULL
    auto spec = std::make_unique<ScanSpec>("root");
    spec->getOrCreateChild("col")->setFilter(std::make_unique<IsNull>());

    auto reader = openTestFile("nullable_10.orc");
    RowReaderOptions opts;
    opts.setScanSpec(spec.get());
    auto rowReader = reader->createRowReader(opts);
    ColumnVector result;
    rowReader->next(10, result);
    EXPECT_EQ(result.size(), 2); // rows 3 and 7
}

// Test: BytesRange filter on string column
TEST(OrcReaderTest, FilterBytesRange) {
    // strings: ["apple","banana","cherry","date","elderberry"]
    // Filter: value >= "b" AND value < "d"
    auto spec = std::make_unique<ScanSpec>("root");
    spec->getOrCreateChild("name")->setFilter(
        std::make_unique<BytesRange>("b", false, "d", true, false));
    auto reader = openTestFile("fruits.orc");
    RowReaderOptions opts;
    opts.setScanSpec(spec.get());
    auto rowReader = reader->createRowReader(opts);
    ColumnVector result;
    rowReader->next(5, result);
    EXPECT_THAT(collectStrings(result), UnorderedElementsAre("banana","cherry"));
}

// Test: no-filter scan returns all rows
TEST(OrcReaderTest, NoFilter) {
    auto reader = openTestFile("int_0_to_99.orc");
    auto rowReader = reader->createRowReader({});
    ColumnVector result;
    uint64_t total = 0;
    while (uint64_t n = rowReader->next(1024, result))
        total += n;
    EXPECT_EQ(total, 100);
}
```

### 5.5 Row Group / Stride Pruning Tests

```cpp
// Test: stride is skipped entirely when filter can't match stats
TEST(OrcReaderTest, StrideSkipping) {
    // 30 000 rows in 3 strides of 10 000.
    // Stride 0: values 0..9999. Stride 1: values 10000..19999. Stride 2: 20000..29999.
    // Filter: value > 15000.
    // Stride 0 should be skipped entirely.
    auto spec = std::make_unique<ScanSpec>("root");
    spec->getOrCreateChild("val")->setFilter(
        std::make_unique<BigintRange>(15001, INT64_MAX, false));
    auto reader = openTestFile("three_strides.orc");
    RowReaderOptions opts;
    opts.setScanSpec(spec.get());
    auto rowReader = reader->createRowReader(opts);
    ColumnVector result;
    int total = 0;
    while (uint64_t n = rowReader->next(1024, result))
        total += n;
    // Stride 0 skipped. Stride 1: 15001..19999 → 4999. Stride 2: 20000..29999 → 10000.
    EXPECT_EQ(total, 14999);
    // Verify stride 0 was actually skipped (use reader diagnostics or mock).
    EXPECT_EQ(rowReader->skippedStrides(), 1);
}

// Test: all strides skipped → zero rows returned
TEST(OrcReaderTest, AllStridesSkipped) {
    // 10 000 rows [0..9999]. Filter: value > 99999 (impossible).
    auto spec = std::make_unique<ScanSpec>("root");
    spec->getOrCreateChild("val")->setFilter(
        std::make_unique<BigintRange>(100000, INT64_MAX, false));
    auto reader = openTestFile("int_0_to_9999.orc");
    RowReaderOptions opts;
    opts.setScanSpec(spec.get());
    auto rowReader = reader->createRowReader(opts);
    ColumnVector result;
    EXPECT_EQ(rowReader->next(1024, result), 0);
}
```

### 5.6 Multi-Stripe Tests

```cpp
// Test: reading across stripe boundaries
TEST(OrcReaderTest, MultiStripe) {
    // File with 3 stripes of 1000 rows each. Values 0..2999.
    auto reader = openTestFile("multi_stripe.orc");
    auto rowReader = reader->createRowReader({});
    ColumnVector result;
    int64_t expected = 0;
    while (uint64_t n = rowReader->next(512, result)) {
        for (uint64_t i = 0; i < n; ++i, ++expected)
            EXPECT_EQ(result.valueAt<int64_t>(i), expected);
    }
    EXPECT_EQ(expected, 3000);
}

// Test: seekToRow() jumps to correct position
TEST(OrcReaderTest, SeekToRow) {
    auto reader = openTestFile("int_0_to_9999.orc");
    auto rowReader = reader->createRowReader({});
    rowReader->seekToRow(5000);
    ColumnVector result;
    rowReader->next(10, result);
    for (int i = 0; i < 10; ++i)
        EXPECT_EQ(result.valueAt<int64_t>(i), 5000 + i);
}
```

### 5.7 Compression Tests

```cpp
// Parametrize over compression kinds
TEST_P(OrcReaderCompressionTest, RoundTrip) {
    CompressionKind codec = GetParam();
    // Write 10 000 integers with this codec, read back, verify.
    auto reader = openTestFile("compressed_" + codecName(codec) + ".orc");
    auto rowReader = reader->createRowReader({});
    ColumnVector result;
    uint64_t n = rowReader->next(10000, result);
    EXPECT_EQ(n, 10000);
    for (int i = 0; i < 10000; ++i)
        EXPECT_EQ(result.valueAt<int64_t>(i), i);
}
INSTANTIATE_TEST_SUITE_P(Codecs, OrcReaderCompressionTest,
    Values(CompressionKind::NONE, CompressionKind::ZLIB,
           CompressionKind::SNAPPY, CompressionKind::LZ4,
           CompressionKind::ZSTD));
```

### 5.8 Edge Case Tests

```cpp
// Test: empty file (no rows)
TEST(OrcReaderTest, EmptyFile) {
    auto reader = openTestFile("empty.orc");
    EXPECT_EQ(reader->numberOfRows(), 0);
    auto rowReader = reader->createRowReader({});
    ColumnVector result;
    EXPECT_EQ(rowReader->next(1024, result), 0);
}

// Test: single row
TEST(OrcReaderTest, SingleRow) {
    auto reader = openTestFile("single_row.orc");
    auto rowReader = reader->createRowReader({});
    ColumnVector result;
    EXPECT_EQ(rowReader->next(1024, result), 1);
}

// Test: batch size larger than file
TEST(OrcReaderTest, BatchLargerThanFile) {
    auto reader = openTestFile("100_rows.orc");
    auto rowReader = reader->createRowReader({});
    ColumnVector result;
    EXPECT_EQ(rowReader->next(99999, result), 100);
}

// Test: batch size of 1
TEST(OrcReaderTest, BatchSizeOne) {
    auto reader = openTestFile("10_rows.orc");
    auto rowReader = reader->createRowReader({});
    ColumnVector result;
    for (int expected = 0; expected < 10; ++expected) {
        EXPECT_EQ(rowReader->next(1, result), 1);
        EXPECT_EQ(result.valueAt<int64_t>(0), expected);
    }
    EXPECT_EQ(rowReader->next(1, result), 0);
}

// Test: large string values (> dictionary threshold)
TEST(OrcReaderTest, LargeStrings) {
    // Strings 4 KB each — forces DIRECT encoding
    auto reader = openTestFile("large_strings.orc");
    auto rowReader = reader->createRowReader({});
    ColumnVector result;
    rowReader->next(10, result);
    for (int i = 0; i < 10; ++i)
        EXPECT_EQ(result.valueAt<std::string_view>(i).size(), 4096);
}

// Test: column projection (only read selected columns)
TEST(OrcReaderTest, ColumnProjection) {
    // File has 5 columns: a, b, c, d, e. Only read b and d.
    auto reader = openTestFile("five_cols.orc");
    RowReaderOptions opts;
    opts.setProjectedColumns({"b", "d"});
    auto rowReader = reader->createRowReader(opts);
    ColumnVector result;
    rowReader->next(100, result);
    // result should only have fields b and d
    auto& s = result.asStruct();
    EXPECT_EQ(s.numFields(), 2);
}
```

### 5.9 Selective vs Non-Selective Path Equivalence

```cpp
// Both paths should produce identical results for the same data.
TEST(OrcReaderTest, SelectiveNonSelectiveEquivalence) {
    // A mixed file: integers, strings, struct, with some nulls.
    auto testFile = "mixed_types.orc";

    // Non-selective read
    ColumnVector resultNonSelective;
    {
        auto rowReader = OrcReader::create(openFile(testFile))
            ->createRowReader({});  // no ScanSpec → non-selective
        rowReader->next(1000, resultNonSelective);
    }

    // Selective read (AlwaysTrue filter)
    ColumnVector resultSelective;
    {
        auto spec = alwaysTrueScanSpec();
        RowReaderOptions opts;
        opts.setScanSpec(spec.get());
        auto rowReader = OrcReader::create(openFile(testFile))
            ->createRowReader(opts); // with ScanSpec → selective
        rowReader->next(1000, resultSelective);
    }

    EXPECT_TRUE(columnsEqual(resultNonSelective, resultSelective));
}
```

---

## Part 6 — Implementation Checklist

Use this list to track progress when building your reader:

### Phase 1 — I/O and Decompression
- [ ] `InputStream` for local files
- [ ] Footer parsing: PostScript + Footer protobuf
- [ ] Per-stream compression/decompression (`PagedInputStream`)
- [ ] `SeekableStream` interface + in-memory implementation (for testing)

### Phase 2 — Encoders and Decoders
- [ ] `ByteRleDecoder` (PRESENT stream)
- [ ] `RleDecoderV1<T>` (signed + unsigned)
- [ ] `RleDecoderV2<T>` (SHORT_REPEAT, DIRECT, PATCHED_BASE, DELTA)
- [ ] `DirectDecoder<T>` (varint, fixed-width)

### Phase 3 — Schema and Metadata
- [ ] `TypeWithId` tree builder from `Footer.types`
- [ ] `StripeMetadata` (parse StripeFooter)
- [ ] `StripeStreams` (locate streams by `(columnId, kind)`)
- [ ] `StripeMetadataCache` (optional: pre-parse all stripe footers)

### Phase 4 — Non-Selective Column Readers
- [ ] `ColumnReader` base (null handling)
- [ ] `IntegerColumnReader` (DIRECT + DICTIONARY encodings)
- [ ] `StringDirectColumnReader`
- [ ] `StringDictionaryColumnReader` (load dict at construction)
- [ ] `StructColumnReader`
- [ ] `ListColumnReader`
- [ ] `MapColumnReader`
- [ ] `FloatColumnReader`, `DoubleColumnReader`
- [ ] `BooleanColumnReader`
- [ ] `TimestampColumnReader`
- [ ] `DecimalColumnReader`
- [ ] `DateColumnReader`

### Phase 5 — Selective Column Readers (optional, for filter pushdown)
- [ ] `FormatData` + `OrcFormatData` (null decoder + row-group index)
- [ ] `ScanSpec` and `Filter` class hierarchy
- [ ] `ColumnVisitor` template
- [ ] `SelectiveColumnReader` base (scratch buffers, `prepareRead`, `compactScalarValues`)
- [ ] `SelectiveIntegerColumnReader`
- [ ] `SelectiveStringDirectColumnReader`
- [ ] `SelectiveStringDictionaryColumnReader`
- [ ] `SelectiveStructColumnReader` (orchestrates children)
- [ ] `SelectiveListColumnReader`, `SelectiveMapColumnReader`

### Phase 6 — Row and Stride Iteration
- [ ] `OrcRowReader::next()` (stripe iteration, batch splitting)
- [ ] `OrcRowReader::skipRows()`
- [ ] `OrcRowReader::seekToRow()`
- [ ] `checkSkipStrides()` (filter row-group statistics)
- [ ] `seekToRowGroup()` plumbing end-to-end

### Phase 7 — Tests
- [ ] Unit test each decoder in isolation with hand-crafted byte sequences
- [ ] Integration tests reading real ORC files (write with Apache ORC writer, read with your reader)
- [ ] Equivalence tests: selective vs non-selective produce same results
- [ ] Edge cases: empty file, single row, all-null column, very large strings

---

## Part 7 — Recommended References

1. **Apache ORC Specification** — https://orc.apache.org/specification/
   The authoritative source for file format layout, stream kinds, encoding
   algorithms, and protobuf message definitions.

2. **Apache ORC C++ Reader** — `https://github.com/apache/orc` (`c++/src/`)
   A clean reference implementation. Particularly useful:
   - `c++/src/Reader.cc` — `OrcFileImpl::createRowReader()`
   - `c++/src/ColumnReader.cc` — non-selective column reader factory and `next()`
   - `c++/src/RleDecoderV2.cc` — RLEv2 decoding with all four sub-encodings
   - `c++/src/ByteRle.cc` — PRESENT stream decoder

3. **Velox DWRF Reader** — this repository, `velox/dwio/dwrf/reader/`
   Production-grade implementation with filter pushdown, stride skipping, and
   dictionary caching. Best reference for the selective path and visitor pattern.

4. **ORC Protobuf Definitions** — `velox/dwio/dwrf/proto/orc_proto.proto` (in this repo)
   Complete message definitions for `PostScript`, `Footer`, `StripeFooter`,
   `RowIndex`, `ColumnStatistics`, and all stream kinds.
