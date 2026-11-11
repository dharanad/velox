# F1 — Vectors by hand

**Format:** Extend · **Size:** L overall (6 checkpoints, each targeted at one weekday session)

After completion, continue with
[F1a — One query end to end](F1a-query-walkthrough.md).

## Goal

Build every vector encoding from raw buffers, with no `VectorMaker` helpers on
the "actual" side. Use `make*` helpers only to build the *expected* side and
compare with `assertEqualVectors`. By the end you should be able to sketch the
memory layout of any vector from memory.

## Reading

- `velox/docs/develop/vectors.rst` and `dictionary-encoding.rst`.
- `velox/vector/BaseVector.h`, `FlatVector.h`, `ConstantVector.h`,
  `DictionaryVector.h`, `ComplexVector.h`.
- `velox/buffer/Buffer.h` (`AlignedBuffer::allocate`),
  `velox/common/base/BitUtil.h` (bit access), and
  `velox/common/base/Nulls.h` (null bits).
- `velox/type/StringView.h`.

## Rules

- One test file, `learning/vectors/VectorsByHandTest.cpp`, with a
  `VectorsByHandTest` suite.
- On the actual side, build vectors from `AlignedBuffer`s, constructors, and
  the `BaseVector::create*` / `wrapIn*` factories.

## Checkpoints

1. **Flat BIGINT with nulls.** Allocate the values and null buffers yourself
   and construct the `FlatVector<int64_t>`. Then answer: what does a null bit
   of 1 mean in Velox, and why is the nulls buffer optional?
2. **VARCHAR.** Mix strings of 12 bytes or fewer with longer ones. Store the
   long strings in a string buffer that you attach to the vector. Then answer:
   why does `StringView` keep a 4-byte prefix even for non-inlined strings?
3. **Constant and dictionary.** Build a constant vector over a value and over a
   null. Build a dictionary with its own nulls on top of a flat vector that
   also has nulls. Then wrap that dictionary in a second dictionary.
4. **ARRAY(BIGINT) and MAP(VARCHAR, BIGINT).** Use offsets and sizes buffers.
   Include a null array and an empty array, and make two arrays overlap or
   appear out of order in the elements vector. Is that legal, and why does
   Velox allow it?
5. **ROW.** Build a `RowVector` whose children use different encodings. Show
   the difference between a null row and a row whose fields are all null.
6. **Sum without help.** Write
   `int64_t sumNonNull(const VectorPtr& vector)` for BIGINT input that handles
   flat, constant, and dictionaries of any depth. Do not use `DecodedVector`
   (it is what F2 rebuilds). Test it on every encoding you built above.

## Acceptance

- All checkpoints are asserted with `assertEqualVectors` or gtest matchers.
- Checkpoint 6 handles nulls at every dictionary layer.
- The review passes (style, naming, no helper shortcuts on the actual side).

## Stretch

- Build a `LazyVector` with a loader you write, and observe when it loads.
- Call `BaseVector::flattenVector` on your nested dictionary and explain what
  it did to the buffers.

## Delayed recall

About a week after completion, answer these without source or notes, then
check your answers and record corrections in `PROGRESS.md`:

1. Sketch a dictionary vector's buffers and map a row through two layers.
2. Explain how nulls at each dictionary layer affect the result.
3. Explain who owns the bytes behind a long `StringView` and their lifetime.
4. Outline the sum traversal for nested dictionaries without `DecodedVector`.

This check does not delay F1a or later assignments.
