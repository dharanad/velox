# F3 — SelectivityVector, EvalCtx, and ensure-writable

**Format:** Explain + Extend · **Size:** S (3 checkpoints) · **Prerequisite:** F2

## Goal

Learn the three things every scalar function receives or relies on: which rows
to process (`SelectivityVector`), where errors and shared state go (`EvalCtx`),
and when a result vector may be written in place (`ensureWritable`). Phase S
uses all three in every assignment.

Each checkpoint is one or two tests in a new `learning/expression/EvalBasicsTest.cpp`
(suite `EvalBasicsTest`), added to `velox_learning_test`. As in F1, "Explain"
questions are answered out loud in the review.

## Checkpoints

### 1 — SelectivityVector

- **Read:** `velox/vector/SelectivityVector.h`.
- **Build:** a test that starts from 100 selected rows, deselects every row
  where `row % 3 == 0` with `setValid`, intersects the result with a second
  selection covering rows 10–49, and collects the surviving rows with
  `applyToSelected`.
- **Done when:** the collected rows match the list you worked out by hand
  (`testing::ElementsAreArray`), and the test asserts `begin()`, `end()`, and
  `isAllSelected()` on the result.
- **Explain:** what goes wrong if you forget `updateBounds()` after
  `setValid`? What do `begin()` and `end()` let a function skip?

### 2 — ensureWritable

- **Read:** the comment on `BaseVector::ensureWritable` in
  `velox/vector/BaseVector.h`, and the `flat` and `dictionary` cases in
  `velox/vector/tests/EnsureWritableVectorTest.cpp`.
- **Build:** three tests that call `BaseVector::ensureWritable` on (a) a flat
  vector only you hold, (b) a flat vector whose values buffer a second vector
  also holds, and (c) a dictionary vector. In each, record the vector and
  values-buffer pointers before and after.
- **Done when:** each test asserts whether the vector and its buffer were
  reused or replaced, and the values in the selected rows can be overwritten
  without changing the second vector in (b).
- **Explain:** what are the two conditions for writing in place? Why would a
  function that skips this call corrupt someone else's data?

### 3 — EvalCtx and per-row errors

- **Read:** in `velox/expression/EvalCtx.h`: the constructors, `setError`,
  `applyToSelectedNoThrow`, `errors`, and `throwOnError`. Skim
  `velox/expression/tests/EvalCtxTest.cpp` for how a test builds an `EvalCtx`.
- **Build:** a test that creates an `EvalCtx`, turns off throw-on-error, and
  runs `applyToSelectedNoThrow` over 10 rows with a callback that throws a
  `VELOX_USER_FAIL` for rows 2 and 7.
- **Done when:** the test asserts that no exception escaped, that exactly rows
  2 and 7 have an error recorded, and that with throw-on-error left on the
  first error is thrown instead (`VELOX_ASSERT_THROW` with the message).
- **Explain:** who turns throw-on-error off in a real query, and what happens
  to the recorded errors afterwards?

## Acceptance

- Every "Done when" holds, every "Explain" is answered, and the review passes.

## Delayed recall

About a week later, from memory, state the two conditions for writing a result
vector in place and what `SelectivityVector::begin()`/`end()` are for.
