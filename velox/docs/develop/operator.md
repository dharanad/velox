# Operators in Velox

## Overview

An `Operator` is the fundamental unit of query execution in Velox. Each operator
corresponds to one node in the query plan (filter, project, join, aggregation,
etc.) and transforms `RowVector` batches from its upstream operator into output
batches for its downstream operator.

Operators live in a `Driver`, which is a single thread that owns an ordered list
of operators forming a pipeline. The `Driver` calls each operator's methods in a
tight loop, passing data downstream one batch at a time.

---

## The Operator Lifecycle

The `Driver` drives every operator through the same state machine:

```
construct → initialize() → [needsInput / addInput / getOutput loop] → isFinished()
```

### 1. Constructor

```cpp
Operator(
    DriverCtx* driverCtx,
    RowTypePtr outputType,
    int32_t operatorId,
    std::string planNodeId,
    std::string_view operatorType,
    std::optional<common::SpillConfig> spillConfig = std::nullopt);
```

The constructor receives the `DriverCtx` (which carries the Task, memory pool,
query config, and executor) and the plan node it corresponds to.

**Important:** Do not allocate memory from the memory pool inside the
constructor. Memory allocation can trigger arbitration, which tries to acquire
the task lock — but the task lock is also held during operator construction,
causing a deadlock. Defer all pool allocations to `initialize()`.

### 2. `initialize()`

Called once before the first call to `needsInput()`. Use it for any setup that
requires memory allocation: building hash tables, compiling expression sets,
allocating buffers.

Always call `Operator::initialize()` first:

```cpp
void MyOperator::initialize() {
  Operator::initialize();   // sets initialized_ = true, wires up memory reclaimer
  exprs_ = makeExprSet(..., operatorCtx_->execCtx());
}
```

### 3. `needsInput()` → `addInput()` → `getOutput()` loop

This is the core execution loop driven by `Driver::runInternal()`.

```
for each batch:
  while nextOp->needsInput():
    result = op->getOutput()
    nextOp->addInput(result)

  result = lastOp->getOutput()   // sink or final result
```

The three methods are the heart of the protocol:

```cpp
/// Returns true if this operator can accept another batch.
virtual bool needsInput() const = 0;

/// Stores input for processing. Called only when needsInput() is true.
virtual void addInput(RowVectorPtr input) = 0;

/// Returns the next output batch, or nullptr if more input is needed
/// or if the operator is blocked.
virtual RowVectorPtr getOutput() = 0;
```

A simple stateless operator (e.g., `Limit`) keeps at most one batch at a time:

```cpp
bool needsInput() const override {
  return input_ == nullptr;   // ready only when slot is empty
}

void addInput(RowVectorPtr input) override {
  input_ = std::move(input);
}

RowVectorPtr getOutput() override {
  if (input_ == nullptr) return nullptr;
  auto output = /* transform input_ */;
  input_ = nullptr;           // slot is now free
  return output;
}
```

### 4. `noMoreInput()`

Called by the Driver when the upstream operator has finished producing data.
The base implementation sets `noMoreInput_ = true`. Stateful operators (e.g.,
aggregations, sorts) use this signal to flush their buffered state:

```cpp
void MyOperator::noMoreInput() {
  Operator::noMoreInput();
  // now drain the hash table or sort buffer into output batches
}
```

### 5. `isBlocked(ContinueFuture* future)`

Returns `BlockingReason::kNotBlocked` if the operator can make progress.
Otherwise sets `*future` to a future that will complete when the block is
resolved, and returns a specific reason (e.g., `kWaitForJoinBuild`,
`kWaitForSplit`). The Driver suspends the thread and re-schedules the driver
when the future resolves.

```cpp
BlockingReason isBlocked(ContinueFuture* future) override {
  if (buildSideReady_) return BlockingReason::kNotBlocked;
  *future = joinBridge_->waitForBuild();
  return BlockingReason::kWaitForJoinBuild;
}
```

### 6. `isFinished()`

Returns `true` when the operator will never produce more output — either because
`noMoreInput_` is true and all buffered state has been drained, or because the
operator terminated early (e.g., `Limit` after emitting enough rows).

---

## Anatomy of an Operator: What to Implement

Here is the minimal set of things every non-source operator must implement:

```cpp
class MyOperator : public Operator {
 public:
  MyOperator(int32_t operatorId, DriverCtx* driverCtx,
             const std::shared_ptr<const core::MyPlanNode>& node);

  void initialize() override;          // optional: do deferred setup
  bool needsInput() const override;
  void addInput(RowVectorPtr input) override;
  RowVectorPtr getOutput() override;
  BlockingReason isBlocked(ContinueFuture* future) override;
  bool isFinished() override;
  void noMoreInput() override;         // optional: override to flush state
};
```

For source operators (the first operator in a pipeline, e.g., `TableScan`),
inherit from `SourceOperator` instead:

```cpp
class MySourceOperator : public SourceOperator {
 public:
  // needsInput, addInput, and noMoreInput are already disallowed by SourceOperator.
  RowVectorPtr getOutput() override;
  BlockingReason isBlocked(ContinueFuture* future) override;
  bool isFinished() override;
};
```

---

## Working with Input

Input arrives as a `RowVectorPtr` — a `RowVector` containing one child
`VectorPtr` per column. The number and order of children match the input schema
(`outputType` of the upstream plan node).

### Accessing column data

```cpp
void MyOperator::addInput(RowVectorPtr input) {
  input_ = std::move(input);
}

RowVectorPtr MyOperator::getOutput() {
  if (!input_) return nullptr;

  const vector_size_t numRows = input_->size();

  // Access the first column (channel 0) as a typed flat vector.
  auto* col0 = input_->childAt(0)->asFlatVector<int64_t>();
  for (vector_size_t row = 0; row < numRows; ++row) {
    if (!col0->isNullAt(row)) {
      int64_t value = col0->valueAt(row);
      // ...
    }
  }
  // ...
}
```

### Handling vector encodings

Input columns may be `FlatVector`, `DictionaryVector`, `ConstantVector`, or
`LazyVector`. Use `DecodedVector` to normalize any encoding into a flat view:

```cpp
#include "velox/vector/DecodedVector.h"

DecodedVector decoded;
SelectivityVector allRows(input_->size());
decoded.decode(*input_->childAt(0), allRows);

for (vector_size_t row = 0; row < input_->size(); ++row) {
  if (!decoded.isNullAt(row)) {
    auto value = decoded.valueAt<int64_t>(row);
  }
}
```

`DecodedVector` translates dictionary indices and constant values transparently,
so you never need to branch on the encoding type.

### Loading LazyVectors

`LazyVector` columns defer I/O until the data is actually needed (for column
pushdown). If your operator needs to read the data, call `loadLazyReclaimable`
at the start of `addInput`:

```cpp
void MyOperator::addInput(RowVectorPtr input) {
  loadLazyReclaimable(input);   // materializes lazy children
  input_ = std::move(input);
}
```

### SelectivityVector for sparse row processing

When only some rows are active (e.g., after a filter), use `SelectivityVector`
to avoid touching filtered-out rows:

```cpp
SelectivityVector activeRows(numRows);
// Deselect rows where col < 0
for (vector_size_t row = 0; row < numRows; ++row) {
  if (col->valueAt(row) < 0) activeRows.setValid(row, false);
}
activeRows.updateBounds();
// Now pass activeRows to expression evaluation or hash table operations.
```

---

## Producing Output

### Pattern 1: Identity projection (pass-through)

When your operator only changes the row count but not the column data, set up
`identityProjections_` in the constructor and use `fillOutput`:

```cpp
MyOperator::MyOperator(...) : Operator(...) {
  // All columns pass through at the same channel positions.
  isIdentityProjection_ = true;
  const auto numColumns = outputType_->size();
  identityProjections_.reserve(numColumns);
  for (column_index_t i = 0; i < numColumns; ++i) {
    identityProjections_.emplace_back(i, i);
  }
}

RowVectorPtr MyOperator::getOutput() {
  if (!input_) return nullptr;

  // Build an index buffer selecting which rows to keep.
  BufferPtr indices = allocateIndices(outputSize, pool());
  auto* raw = indices->asMutable<vector_size_t>();
  // ... fill raw[0..outputSize-1] with surviving row indices ...

  auto output = fillOutput(outputSize, indices);
  input_ = nullptr;
  return output;
}
```

`fillOutput` wraps each identity-projected column in a `DictionaryVector` over
the index buffer — no data is copied.

When all rows are kept (no row-count change), return `input_` directly or pass
`nullptr` as the mapping:

```cpp
auto output = fillOutput(input_->size(), nullptr);
```

### Pattern 2: Mixed projection (some pass-through, some computed)

When your operator adds or replaces some columns:

```cpp
MyOperator::MyOperator(...) : Operator(...) {
  const auto& inputType = node->sources()[0]->outputType();
  const auto numOutput = outputType_->size();

  // Columns 0..N-2 come from input at the same channel.
  for (column_index_t i = 0; i < numOutput - 1; ++i) {
    identityProjections_.emplace_back(i, i);
  }

  // The last output column is computed: goes in results_[0].
  resultProjections_.emplace_back(0, numOutput - 1);
  results_.resize(1);
}

RowVectorPtr MyOperator::getOutput() {
  if (!input_) return nullptr;

  // Compute and store the new column into results_[0].
  VectorPtr& result = results_[0];
  if (!result || result.use_count() > 1) {
    result = BaseVector::create(BIGINT(), input_->size(), pool());
  } else {
    BaseVector::prepareForReuse(result, input_->size());
  }
  auto* flat = result->asFlatVector<int64_t>();
  // ... fill flat ...

  auto output = fillOutput(input_->size(), nullptr);
  input_ = nullptr;
  return output;
}
```

### Reusing output vectors

Allocating a new `VectorPtr` on every batch is expensive. Reuse the previous
allocation if it is not referenced by anyone else:

```cpp
VectorPtr& result = results_[0];
if (result && result.use_count() == 1) {
  BaseVector::prepareForReuse(result, numRows);
} else {
  result = BaseVector::create(type, numRows, pool());
}
```

---

## Registering a New Operator

Every operator needs three things:

1. A **PlanNode** subclass in `velox/core/` that carries the operator's
   parameters and is part of the logical query plan.
2. An **Operator** subclass in `velox/exec/` that implements execution.
3. A **PlanNodeTranslator** registered at startup that maps the PlanNode to the
   Operator.

### Step 1 — Define the PlanNode

```cpp
// velox/core/MyPlanNode.h
class MyPlanNode : public PlanNode {
 public:
  MyPlanNode(const PlanNodeId& id, PlanNodePtr source, int32_t myParam)
      : PlanNode(id), sources_({std::move(source)}), myParam_(myParam) {}

  const std::vector<PlanNodePtr>& sources() const override { return sources_; }
  RowTypePtr outputType() const override { return sources_[0]->outputType(); }
  std::string_view name() const override { return "MyNode"; }

  int32_t myParam() const { return myParam_; }

 private:
  std::vector<PlanNodePtr> sources_;
  int32_t myParam_;
};
```

### Step 2 — Implement the Operator

```cpp
// velox/exec/MyOperator.h
class MyOperator : public Operator {
 public:
  MyOperator(int32_t operatorId, DriverCtx* driverCtx,
             const std::shared_ptr<const core::MyPlanNode>& node);

  bool needsInput() const override { return input_ == nullptr; }
  void addInput(RowVectorPtr input) override;
  RowVectorPtr getOutput() override;
  BlockingReason isBlocked(ContinueFuture*) override {
    return BlockingReason::kNotBlocked;
  }
  bool isFinished() override { return noMoreInput_ && input_ == nullptr; }

 private:
  int32_t myParam_;
};
```

```cpp
// velox/exec/MyOperator.cpp
MyOperator::MyOperator(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::MyPlanNode>& node)
    : Operator(
          driverCtx,
          node->outputType(),
          operatorId,
          node->id(),
          "MyOperator"),
      myParam_(node->myParam()) {
  isIdentityProjection_ = true;
  for (column_index_t i = 0; i < outputType_->size(); ++i) {
    identityProjections_.emplace_back(i, i);
  }
}

void MyOperator::addInput(RowVectorPtr input) {
  input_ = std::move(input);
}

RowVectorPtr MyOperator::getOutput() {
  if (!input_) return nullptr;
  auto output = std::move(input_);
  return output;
}
```

### Step 3 — Register the Translator

```cpp
// In your module's registration function (called once at startup):
Operator::registerOperator(std::make_unique<MyTranslator>());

class MyTranslator : public Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<Operator> toOperator(
      DriverCtx* ctx, int32_t id, const core::PlanNodePtr& node) override {
    if (auto myNode = std::dynamic_pointer_cast<const core::MyPlanNode>(node)) {
      return std::make_unique<MyOperator>(id, ctx, myNode);
    }
    return nullptr;
  }
};
```

---

## Things to Consider When Writing a New Operator

### Memory allocation

- Allocate from `pool()` (the operator's memory pool). Use
  `BaseVector::create()`, `AlignedBuffer::allocate<T>()`, or the STL containers
  seeded with a pool allocator.
- Never allocate in the constructor — only in `initialize()` or later.
- Reuse output vectors across calls to avoid excessive allocation pressure.

### Thread safety

Each `Driver` runs on a single thread. You do not need to lock ordinary operator
state. The exception is any state shared across drivers (e.g., a `JoinBridge`
or an atomic counter shared across parallel drivers), which must be thread-safe
independently.

### Blocking correctly

If your operator must wait for an external event (e.g., a remote fetch, a join
build side), it must not spin. Return a `ContinueFuture` from `isBlocked()` that
resolves when the event fires. The Driver will suspend the thread and
re-schedule the driver when the future completes.

### Yielding during long loops

If your operator runs a long in-memory loop (e.g., sorting millions of rows),
call `shouldYield()` periodically. If it returns `true`, save your position,
return `nullptr` from `getOutput()`, and resume on the next call:

```cpp
for (size_t i = start_; i < data_.size(); ++i) {
  processRow(i);
  if (shouldYield()) {
    start_ = i + 1;
    return nullptr;
  }
}
```

### Handling `noMoreInput` for stateful operators

Operators that accumulate state (aggregation, sort, distinct) must flush it
after `noMoreInput()`. A common pattern:

```cpp
bool isFinished() override {
  return noMoreInput_ && !hasBufferedOutput();
}

RowVectorPtr getOutput() override {
  if (!noMoreInput_) return nullptr;   // wait for all input first
  return drainNextBatch();
}
```

### `isFilter()` and `preservesOrder()`

Override these to `true` when applicable. They allow the planner and
downstream operators to make better decisions (e.g., `isFilter() = true` means
output row count ≤ input row count, enabling the Driver to skip certain
checks).

---

## The Driver Loop in Detail

Understanding exactly how the Driver calls your operator helps avoid subtle bugs.

```
operators_ = [Source, Op1, Op2, ..., Sink]
```

The loop works backwards from the last non-source operator:

```
for i = N down to 0:
  if operators_[i+1].needsInput():
    result = operators_[i].getOutput()
    if result:
      operators_[i+1].addInput(result)
      i += 2          // jump forward: let i+1 produce output now
    else:
      if operators_[i].isFinished():
        operators_[i+1].noMoreInput()
```

This means:

- `getOutput()` can be called many times before `addInput()` is called again.
  Return `nullptr` when you have nothing ready.
- `addInput()` will not be called while `needsInput()` returns `false`.
- Once `noMoreInput()` is called, `addInput()` will never be called again.

---

## Worked Example: A Top-N Operator

A `TopN` operator buffers all rows, keeps the smallest N, and emits them after
seeing `noMoreInput()`.

```cpp
class TopN : public Operator {
 public:
  TopN(int32_t operatorId, DriverCtx* driverCtx,
       const std::shared_ptr<const core::TopNNode>& node)
      : Operator(driverCtx, node->outputType(), operatorId,
                 node->id(), "TopN"),
        limit_(node->count()) {
    isIdentityProjection_ = true;
    for (column_index_t i = 0; i < outputType_->size(); ++i) {
      identityProjections_.emplace_back(i, i);
    }
  }

  bool needsInput() const override {
    // Accept input until we have drained; no internal buffer limit here.
    return !noMoreInput_;
  }

  void addInput(RowVectorPtr input) override {
    // Append input rows to our accumulator.
    if (!accumulator_) {
      accumulator_ = input;
    } else {
      accumulator_->append(input.get());   // (illustrative)
    }
  }

  void noMoreInput() override {
    Operator::noMoreInput();
    // Sort accumulator_, keep only top limit_ rows.
    sortAndTruncate();
    outputReady_ = true;
  }

  RowVectorPtr getOutput() override {
    if (!outputReady_ || !accumulator_) return nullptr;
    outputReady_ = false;
    return std::move(accumulator_);
  }

  BlockingReason isBlocked(ContinueFuture*) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_ && !outputReady_;
  }

 private:
  int64_t limit_;
  RowVectorPtr accumulator_;
  bool outputReady_{false};
};
```

Key observations:
- `needsInput()` returns `false` after `noMoreInput_` is set so the Driver
  stops feeding us.
- `getOutput()` returns `nullptr` until `noMoreInput()` is called, signaling
  the Driver to keep feeding us.
- The output is produced exactly once, then the operator is finished.

---

## Reference: Key Files

| File | Purpose |
|---|---|
| `velox/exec/Operator.h` | Base class, `IdentityProjection`, `OperatorCtx`, registration API |
| `velox/exec/Driver.h/cpp` | Execution loop that calls operator methods |
| `velox/exec/Limit.cpp` | Simple stateless operator, row-count reduction |
| `velox/exec/AssignUniqueId.cpp` | Mixed projection: identity columns + one computed column |
| `velox/exec/FilterProject.cpp` | Filter + identity/computed projection |
| `velox/exec/HashAggregation.cpp` | Stateful operator that buffers until `noMoreInput` |
| `velox/exec/MarkDistinct.cpp` | Stateful operator with spill support |
| `velox/exec/EnforceSingleRow.cpp` | `noMoreInput()` override that synthesizes a null row |
