# F1 — Vectors by hand

**Format:** Extend · **Size:** L (6 checkpoints, one weekday session each)

Next: [F1a — One query end to end](F1a-query-walkthrough.md).

## Goal

Build every vector encoding from raw buffers, so that you can sketch the memory
layout of any vector from memory.

## How each checkpoint works

Every checkpoint is one or two tests in the `VectorsByHandTest` suite and has
the same three parts:

- **Build** — the vector to construct as the *actual* value. Use only
  `AlignedBuffer`, vector constructors, and the `BaseVector::create*` /
  `wrapIn*` factories. No `makeFlatVector`, `makeArrayVector`, or other
  `VectorMaker` helpers on this side.
- **Done when** — the test that must pass. Build the *expected* vector with the
  `make*` helpers from `VectorTestBase` and compare with `assertEqualVectors`.
- **Explain** — a question you answer out loud when you ask for the review.
  Nothing to write down.

## Setup (once)

1. Create `learning/vectors/VectorsByHandTest.cpp` with a `VectorsByHandTest`
   fixture, modelled on `learning/LearningSmokeTest.cpp`.
2. Add the file to the `velox_learning_test` sources in
   `learning/CMakeLists.txt`.

Done when: `ctest -R velox_learning` runs an empty `VectorsByHandTest` test.

## Reading

Read only what the current checkpoint needs.

| Checkpoint | Read |
|---|---|
| 1 | `velox/docs/develop/vectors.rst` (flat vectors), `velox/vector/FlatVector.h`, `velox/buffer/Buffer.h` (`AlignedBuffer::allocate`), `velox/common/base/Nulls.h` |
| 2 | `velox/type/StringView.h`, the string buffer members of `FlatVector.h` |
| 3 | `velox/docs/develop/dictionary-encoding.rst`, `ConstantVector.h`, `DictionaryVector.h`, the `wrapIn*` and `create*Constant` factories in `BaseVector.h` |
| 4, 5 | `velox/vector/ComplexVector.h` |
| 6 | Nothing new |

## Checkpoints

### 1 — Flat BIGINT with nulls

- **Build:** a `FlatVector<int64_t>` holding `{1, null, 3, null, 5}`. Allocate
  the values buffer and the nulls buffer yourself and pass them to the
  constructor.
- **Done when:** it equals `makeNullableFlatVector<int64_t>({1, std::nullopt, 3, std::nullopt, 5})`.
- **Explain:** what does a null bit of 1 mean in Velox? Why is the nulls buffer
  optional?

### 2 — VARCHAR

- **Build:** a `FlatVector<StringView>` with at least two strings of 12 bytes
  or fewer and two longer ones. Put the long strings' bytes in a buffer you
  allocate, and attach that buffer to the vector.
- **Done when:** it equals the same strings built with `makeFlatVector`, and
  the test still passes after the `std::string`s you copied from go out of
  scope.
- **Explain:** who owns the bytes behind a long `StringView`? Why does
  `StringView` keep a 4-byte prefix even for strings it does not inline?

### 3 — Constant and dictionary

- **Build (test A):** a constant vector of size 4 over the value `7`, and a
  constant vector of size 4 over null.
- **Build (test B):** a flat vector with one null, wrapped in a dictionary that
  reorders the rows and adds one null of its own. Then wrap that dictionary in
  a second dictionary.
- **Done when:** each equals a flat vector of the same logical values built
  with `makeNullableFlatVector`.
- **Explain:** for one row of the two-layer dictionary, walk from the outer
  index to the value. Where can a null come from on the way?

### 4 — ARRAY(BIGINT) and MAP(VARCHAR, BIGINT)

- **Build (test A):** an `ArrayVector` from offsets and sizes buffers and an
  elements vector. Include a null array, an empty array, and two arrays whose
  ranges overlap or appear out of order in the elements vector.
- **Build (test B):** a `MapVector` of three maps, one of them empty, the same
  way.
- **Done when:** each equals the same data built with `makeNullableArrayVector`
  / `makeMapVector`.
- **Explain:** is it legal for arrays to overlap or be out of order in the
  elements vector? Why does Velox allow it?

### 5 — ROW

- **Build:** a `RowVector` with three children: one flat, one dictionary, one
  constant. Make one row null, and make a different row non-null with every
  field null.
- **Done when:** it equals the same data built with `makeRowVector`, and the
  test asserts `isNullAt` on both of those rows.
- **Explain:** how is a null row different from a row whose fields are all
  null? Where is each kind of null stored?

### 6 — Sum without help

- **Build:** `int64_t sumNonNull(const VectorPtr& vector)` for BIGINT input. It
  must handle flat, constant, and dictionaries of any depth. Do not use
  `DecodedVector`; F2 rebuilds it.
- **Done when:** tests pass for the flat vector from checkpoint 1, both
  constants and the two-layer dictionary from checkpoint 3 (cast to BIGINT
  versions if needed), and an all-null input, which sums to 0.
- **Explain:** how did you handle a null added by an outer dictionary layer?

## Acceptance

- Every "Done when" holds and you have answered every "Explain" in the review.
- The review passes: style, naming, and no helper shortcuts on the actual side.

## Stretch

- Build a `LazyVector` with a loader you write, and observe when it loads.
- Call `BaseVector::flattenVector` on the two-layer dictionary and explain what
  it did to the buffers.

## Delayed recall

About a week later, from memory, sketch a two-layer dictionary's buffers and
map one row through it, then say who owns the bytes behind a long `StringView`.
This does not delay later assignments.
