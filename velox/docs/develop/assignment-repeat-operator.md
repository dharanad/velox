# Assignment: Implement the Repeat Operator

## Overview

Your task is to implement a new Velox execution operator called **`Repeat`**.
It does not exist in Velox today.

`Repeat` takes each input row and emits it a fixed number of times, where the
repeat count comes from a designated `BIGINT` column in the input. This is
conceptually similar to how `UNNEST` expands an array column into multiple
rows, except the expansion factor is an integer rather than an array's length.

Equivalent SQL (not native SQL, but expressed with a lateral join):

```sql
-- Emit each row of t repeated n times
SELECT t.*
FROM   t,
       LATERAL (SELECT * FROM generate_sequence(1, t.n)) AS _
```

This is a real pattern used in data generation, load testing datasets, and
weight-based sampling pipelines.

---

## Learning Goals

By completing this assignment you will learn:

1. How to define a **plan node** (`PlanNode` subclass in `velox/core/`).
2. How to implement an **operator** (`Operator` subclass in `velox/exec/`).
3. How the `addInput` / `getOutput` / `needsInput` / `isFinished` lifecycle
   works, including **resuming output across multiple `getOutput` calls**.
4. How to use **dictionary encoding** (`BaseVector::wrapInDictionary`) to
   produce repeated rows without copying data.
5. How to register a plan node translator so the engine can instantiate your
   operator.
6. How to write operator tests using `PlanBuilder` and `assertQuery`.

---

## Operator Semantics

```
Input schema:   [c0 T0, c1 T1, ..., n BIGINT, ...]
Output schema:  [c0 T0, c1 T1, ...]   ← all columns except the count column
```

- The **repeat count column** is a `BIGINT` column in the input identified
  by its channel index. It is **not** included in the output.
- Every other input column is passed through unchanged except that each row
  appears `n` times.
- If `n == 0` the row is omitted from the output entirely.
- If `n < 0` throw `VELOX_USER_FAIL`.
- If `n` is **null** treat it as `0` (skip the row). This matches the
  behaviour of PostgreSQL's `generate_series`.
- Output respects `outputBatchRows()` — a single input batch may produce
  multiple output batches if the total repeat count is large.
- The operator does **not** require the input to be sorted.

### Example

Input (4 rows):

```
name    n
------  -
alice   3
bob     0
carol   2
dave    1
```

Expected output (6 rows):

```
name
------
alice
alice
alice
carol
carol
dave
```

---

## Files to Create

```
velox/core/RepeatNode.h           ← plan node definition
velox/core/RepeatNode.cpp         ← serialization / factory
velox/exec/Repeat.h               ← operator header
velox/exec/Repeat.cpp             ← operator implementation
velox/exec/tests/RepeatTest.cpp   ← tests
```

You will also need to touch:

```
velox/exec/CMakeLists.txt         ← add Repeat.cpp
velox/exec/tests/CMakeLists.txt   ← add RepeatTest.cpp
```

---

## Reference Files to Study

Read these files **before** you start writing code. They are the closest
existing analogues to what you need to build.

| File | Why it matters |
|------|----------------|
| `velox/exec/Unnest.h` | Output-expansion operator with the same `addInput` / `getOutput` / partial-batch pattern you need |
| `velox/exec/Unnest.cpp` | How `wrapInDictionary` is used to repeat rows cheaply; `nextInputRow_` and `firstInnerRowStart_` pattern |
| `velox/exec/Limit.h/.cpp` | Simplest stateful operator; shows the full lifecycle with minimal noise |
| `velox/core/PlanNode.h` | Find `LimitNode` (~line 4500) and `UnnestNode` (~line 4593) for concrete plan node examples |
| `velox/exec/Operator.h` | All virtual methods you must or can override; `outputBatchRows()`, `identityProjections_`, `noMoreInput_`, `input_` |
| `velox/exec/tests/UnnestTest.cpp` | Test structure, `PlanBuilder`, `assertQuery`, how to parameterise by batch size |
| `velox/exec/OperatorUtils.h` | `allocateIndices`, `allocateSizes` helper functions used in Unnest |

---

## Tasks

Work through these tasks in order. Each task builds on the previous one.

---

### Task 1 — Plan Node (`velox/core/RepeatNode.h`)

Define `RepeatNode` as a subclass of `PlanNode`.

**Required members:**

```cpp
/// Index of the repeat-count column in the source's output type.
column_index_t repeatCountChannel() const;

/// Output type: all source columns except the repeat-count column.
const RowTypePtr& outputType() const override;
```

**Required overrides:** `name()`, `sources()`, `outputType()`, `addDetails()`.

**Hints:**
- Compute `outputType_` in the constructor by iterating
  `source->outputType()->children()` and skipping the column at
  `repeatCountChannel_`.
- Keep `sources_` as `std::vector<PlanNodePtr>` just like `LimitNode`.
- `name()` should return `"Repeat"`.
- Store `repeatCountChannel_` as `const column_index_t`.

**You do not need to implement serialization** (`serialize` /
`PlanNode::create`) for this assignment — skip those for now.

---

### Task 2 — Plan Node Builder Helper

Add a `repeat` method to `PlanBuilder` (`velox/exec/tests/utils/PlanBuilder.h`)
so tests can write:

```cpp
PlanBuilder().values({data}).repeat("n").planNode()
```

The method takes the name of the repeat-count column as a `std::string_view`.
It looks up the channel index in the current plan node's output type and
creates a `RepeatNode`.

---

### Task 3 — Operator Header (`velox/exec/Repeat.h`)

Define the `Repeat` class with:

```cpp
class Repeat : public Operator {
 public:
  Repeat(int32_t operatorId,
         DriverCtx* driverCtx,
         const std::shared_ptr<const core::RepeatNode>& node);

  bool needsInput() const override;
  void addInput(RowVectorPtr input) override;
  RowVectorPtr getOutput() override;
  bool isFinished() override;
  BlockingReason isBlocked(ContinueFuture*) override {
    return BlockingReason::kNotBlocked;
  }

 private:
  // Channel of the repeat-count column in the input.
  const column_index_t repeatCountChannel_;

  // Maximum number of output rows per batch.
  const vector_size_t maxOutputBatchSize_;

  // Decoded repeat counts for the current input batch.
  DecodedVector decodedRepeatCount_;

  // Cumulative output row count for each input row (inclusive prefix sum).
  // outputOffsets_[i] = sum of repeat counts for rows 0..i.
  // outputOffsets_[-1] = 0 (conceptually).
  std::vector<int64_t> outputOffsets_;

  // Index of the next input row to emit.
  vector_size_t nextInputRow_{0};

  // How many copies of nextInputRow_ have already been emitted.
  int64_t emittedForCurrentRow_{0};

  // Total output rows remaining across all rows >= nextInputRow_.
  int64_t remainingOutputRows_{0};
};
```

---

### Task 4 — Operator Implementation (`velox/exec/Repeat.cpp`)

#### 4a. Constructor

```cpp
Repeat::Repeat(int32_t operatorId,
               DriverCtx* driverCtx,
               const std::shared_ptr<const core::RepeatNode>& node)
    : Operator(driverCtx,
               node->outputType(),
               operatorId,
               node->id(),
               "Repeat"),
      repeatCountChannel_(node->repeatCountChannel()),
      maxOutputBatchSize_(outputBatchRows()) {}
```

Note: the second argument to `Operator` is the **output** type (no count
column). The `identityProjections_` mechanism from Unnest is one way to map
input channels to output channels; alternatively compute the mapping manually.

#### 4b. `addInput`

```cpp
void Repeat::addInput(RowVectorPtr input) {
  // 1. Store input in input_.
  // 2. Decode the repeat-count column using decodedRepeatCount_.decode(...).
  // 3. Validate: no negative counts (VELOX_USER_CHECK_GE).
  // 4. Compute outputOffsets_: a prefix sum of repeat counts
  //    (treat null as 0).
  // 5. Set remainingOutputRows_ = outputOffsets_.back() (or 0 if empty).
  // 6. Reset nextInputRow_ = 0 and emittedForCurrentRow_ = 0.
}
```

**Hint for step 4:** Build `outputOffsets_` so that
`outputOffsets_[i] = outputOffsets_[i-1] + n[i]`.  This lets you quickly
check when you have processed the entire batch.

#### 4c. `needsInput`

```cpp
bool Repeat::needsInput() const {
  return input_ == nullptr;
}
```

#### 4d. `getOutput`

This is the most interesting method. It must:

1. Return `nullptr` if `input_` is null.
2. Determine how many output rows to emit this call (up to
   `maxOutputBatchSize_`).
3. Build an **indices buffer** that maps each output row to the corresponding
   input row, then wrap every non-count input column with
   `BaseVector::wrapInDictionary(nullptr, indices, outputSize, inputColumn)`.
4. Advance `nextInputRow_` and `emittedForCurrentRow_` accordingly.
5. Set `input_ = nullptr` when the batch is fully consumed.

**Key loop logic:**

```
outputRow = 0
while outputRow < maxOutputBatchSize_ and nextInputRow_ < numInputRows:
    n = repeatCount[nextInputRow_]  (0 if null)
    remaining = n - emittedForCurrentRow_
    canEmit = min(remaining, maxOutputBatchSize_ - outputRow)
    fill indices[outputRow .. outputRow+canEmit) with nextInputRow_
    outputRow += canEmit
    emittedForCurrentRow_ += canEmit
    if emittedForCurrentRow_ == n:
        nextInputRow_++
        emittedForCurrentRow_ = 0
```

**Hint:** `allocateIndices(size, pool())` from `velox/exec/OperatorUtils.h`
allocates the indices buffer for you.

**Hint:** For the output vector build:

```cpp
std::vector<VectorPtr> outputColumns;
for (auto& proj : identityProjections_) {
    outputColumns.push_back(BaseVector::wrapInDictionary(
        nullptr, indices, outputSize, input_->childAt(proj.inputChannel)));
}
return std::make_shared<RowVector>(
    pool(), outputType_, nullptr, outputSize, std::move(outputColumns));
```

Set up `identityProjections_` in the constructor by iterating the source
output type and pushing an `IdentityProjection` for each column that is not
the repeat-count channel.

#### 4e. `isFinished`

```cpp
bool Repeat::isFinished() {
  return noMoreInput_ && input_ == nullptr;
}
```

#### 4f. Plan Node Translator

At the bottom of `Repeat.cpp` add a static registration:

```cpp
namespace {
class RepeatTranslator : public Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<Operator> toOperator(
      DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) override {
    if (auto repeat = std::dynamic_pointer_cast<const core::RepeatNode>(node)) {
      return std::make_unique<Repeat>(id, ctx, repeat);
    }
    return nullptr;
  }
};
} // namespace

void registerRepeatOperator() {
  Operator::registerOperator(std::make_unique<RepeatTranslator>());
}
```

Call `registerRepeatOperator()` from your test `SetUp`.

---

### Task 5 — Tests (`velox/exec/tests/RepeatTest.cpp`)

Write the following test cases. Use `HiveConnectorTestBase` (same base class
as `UnnestTest`) and `assertQuery` against DuckDB.

#### Test 1 — Basic correctness

```
Input: rows {("alice", 3), ("bob", 0), ("carol", 2), ("dave", 1)}
Expected output: alice, alice, alice, carol, carol, dave
```

Verify with DuckDB:

```sql
SELECT name FROM tmp, generate_series(1, tmp.n) AS _ WHERE n > 0
UNION ALL
-- or use a lateral join form your DB supports
```

Alternatively, materialise the expected output manually and use
`assertEqualResults`.

#### Test 2 — All-zero counts

Input where every `n = 0`. Expected output is empty.

#### Test 3 — Large repeat counts

A single input row with `n = 10000`. Verify that the output has 10 000 rows
and that `getOutput` is called multiple times (each call returns at most
`outputBatchRows()` rows).

**Hint:** Parameterise the test by batch size (like `UnnestTest`) by passing
`kPreferredOutputBatchRows` in `CursorParameters::queryConfigs`.

#### Test 4 — Null counts

Input rows where `n` is null. Verify those rows are absent from the output.

#### Test 5 — Negative count → user error

Input row with `n = -1`. Verify `VELOX_USER_FAIL` is thrown (use
`VELOX_ASSERT_THROW`).

#### Test 6 — Mixed types of pass-through columns

Input with columns `[BIGINT, VARCHAR, ARRAY(DOUBLE), BIGINT(count)]`. Verify
all three non-count columns appear correctly in the output for repeated rows.

#### Test 7 — Multiple input batches

Feed the operator 3 separate input batches (use `values({b1, b2, b3})`).
Verify the total output row count matches the sum of all repeat counts.

#### Test 8 — Empty input

Zero input rows. Expected output: zero rows.

---

## Acceptance Criteria

Your implementation is complete when:

- [ ] All 8 test cases pass.
- [ ] The code follows the style rules in `CODING_STYLE.md` (PascalCase types,
      camelCase variables, `///` for public API in headers, `//` elsewhere).
- [ ] `needsInput()` returns `true` if and only if `input_ == nullptr`.
- [ ] A single input batch whose total repeat count exceeds `outputBatchRows()`
      produces multiple `getOutput` calls without re-calling `addInput`.
- [ ] No unnecessary data copies — use `wrapInDictionary` for the pass-through
      columns.
- [ ] `isFinished()` only returns `true` after `noMoreInput()` has been called
      **and** all buffered rows have been emitted.
- [ ] The code builds cleanly: `make debug` from the repo root.

---

## Hints and Common Pitfalls

**`input_` ownership.** Set `input_ = nullptr` as soon as the batch is fully
consumed. `needsInput()` returns `true` when `input_` is null, which tells the
driver to call `addInput` again.

**Dictionary vector size.** The `wrapSize` argument to `wrapInDictionary` is
the number of output rows, not the number of input rows. Always pass the actual
output batch size here.

**Zero-count rows.** The inner loop advances `nextInputRow_` without emitting
anything for zero-count rows. Make sure the loop does not stall on them.

**Prefix-sum overflow.** Total output rows can be very large. Use `int64_t`
for `outputOffsets_` and `remainingOutputRows_`.

**`noMoreInput_`.** This is a member of the base `Operator` class set
automatically by `noMoreInput()`. You do not need to track it yourself; just
use it in `isFinished()`.

**Dictionary alphabet size.** After calling `wrapInDictionary`, if the input
batch is large but the output slice is small, the dictionary base vector
retains the full input allocation. Call `BaseVector::copy(*result)` on the
output if you want a compact representation (see `Unnest.cpp:490` for why this
matters). For this assignment this is optional but worth understanding.

**Driver loop.** The Driver calls operators in this order each tick:
```
if needsInput(): addInput(upstream.getOutput())
output = getOutput()
if output != nullptr: downstream.addInput(output)
```
This means `getOutput` may be called even when `input_` is null (just return
`nullptr`). It also means that after one `addInput`, the Driver keeps calling
`getOutput` until `nullptr` is returned before fetching the next upstream
batch. Your implementation must honour this contract.

---

## Stretch Goals

Once the basic implementation works, try these extensions:

1. **Ordinality column** — add an optional output column `__ordinality BIGINT`
   that contains `1, 2, ..., n` for each repetition of a row (like
   `UNNEST ... WITH ORDINALITY`).

2. **`startDrain` support** — override `startDrain()` to return `true` when
   `input_ != nullptr`. This allows the operator to participate in barrier
   draining (see `velox/docs/develop/barrier.md`).

3. **Runtime stats** — add a runtime stat `repeatTotalOutputRows` that counts
   total output rows emitted, surfaced through `addRuntimeStat`.
