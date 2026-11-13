# Driver: Execution Engine and Task Orchestration

## Overview

A `Driver` is a single-threaded execution engine for a pipeline of operators.
It owns an ordered list of `Operator` objects and runs them in a loop, pulling
data from the source end and pushing it toward the sink end until the pipeline
is exhausted, blocked, or terminated.

A `Task` is the query-scoped owner of all drivers. It owns the plan fragment,
memory pool, split queues, join bridges, and the full set of drivers. The Task
decides how many drivers to create, which thread pool to schedule them on, and
when to terminate them.

---

## Key Types at a Glance

| Type | Role |
|---|---|
| `Task` | Query-scoped manager: owns plan, splits, memory, and all drivers |
| `DriverFactory` | Blueprint for creating drivers from one pipeline in the plan |
| `Driver` | Single-threaded execution engine for one pipeline instance |
| `DriverCtx` | Per-driver context: task reference, driver/pipeline IDs, spill config |
| `ThreadState` | Tracks the live state of the driver thread (on-thread, blocked, suspended, etc.) |
| `BlockingState` | Captures a blocking future and schedules driver re-enqueue when it resolves |
| `StopReason` | Signal returned from `runInternal` to tell the caller what happened |

---

## The Plan → Pipeline → Driver Mapping

### Plans are split into pipelines

Not every plan node can be chained into a single linear pipeline. Nodes that
have multiple sources (e.g., `HashJoin`) or that need inter-pipeline
communication (e.g., `LocalPartition`) cause pipeline breaks. The
`LocalPlanner` traverses the plan top-down and slices it into a set of
`DriverFactory` objects, one per pipeline.

```
Plan tree:                         Pipelines:
  PartitionedOutput                 Pipeline 0:  TableScan → FilterProject → HashProbe → PartitionedOutput
    HashJoin (probe side)
      FilterProject            ──► Pipeline 1:  TableScan → HashBuild
        TableScan (fact)
      TableScan (dim)
```

### `LocalPlanner::plan()`

Called by `Task` during startup, it:
1. Walks the plan tree from the root.
2. Identifies pipeline boundaries (`mustStartNewPipeline()`).
3. Creates a `DriverFactory` per pipeline with the ordered list of `PlanNode`
   objects and the maximum parallelism for that pipeline.

### `DriverFactory::createDriver()`

For each driver instance within a pipeline, `createDriver()` calls
`Operator::fromPlanNode()` on each plan node in order (from source to sink),
producing the operator vector. It then calls `Driver::init()` to wire up the
`DriverCtx` and `operators_` list.

---

## Task: Creating and Starting Drivers

### Task creation

```cpp
auto task = Task::create(
    taskId,
    planFragment,
    destination,
    queryCtx,
    Task::ExecutionMode::kParallel,
    consumer);
```

The Task is created in `TaskState::kRunning` but no drivers exist yet.
Splits and configuration can be added before or after `start()`.

### `Task::start()` — parallel execution

```cpp
task->start(maxDrivers);
```

This is the standard multi-threaded path:

1. **`createDriverFactoriesLocked(maxDrivers)`** — calls `LocalPlanner::plan()`,
   producing one `DriverFactory` per pipeline.
2. **`initializePartitionOutput()`** — sets up the `OutputBufferManager` if
   the plan has a `PartitionedOutputNode`, and creates `ExchangeClient` objects
   for exchange pipelines.
3. **`createAndStartDrivers()`** — for each pipeline, calls
   `createDriversLocked()` to instantiate all driver objects, then calls
   `Driver::enqueue()` on each, which submits the driver to the executor thread
   pool in `QueryCtx`.

```
Task::start()
  └── createDriverFactoriesLocked()    → LocalPlanner::plan() → DriverFactory[]
  └── initializePartitionOutput()      → OutputBufferManager, ExchangeClients
  └── createAndStartDrivers()
        └── createDriversLocked()      → DriverFactory::createDriver() × N
              └── Driver::init()       → operators_[] wired up
        └── Driver::enqueue() × N      → submitted to executor
```

### `Task::next()` — serial execution

Used in single-threaded contexts (e.g., tests, simple query runners):

```cpp
while (auto batch = task->next()) {
  consume(batch);
}
```

`Task::next()` loops over all drivers sequentially, calling `driver->next()`
on each. If a driver produces a result, the result is returned immediately.
If a driver blocks, its future is recorded and the loop skips to the next
driver. The outer loop keeps cycling until a result appears, or all drivers
are blocked or finished.

---

## DriverCtx: Per-Driver Context

Every driver receives a `DriverCtx` at construction time:

```cpp
struct DriverCtx {
  const int driverId;      // index within the pipeline
  const int pipelineId;    // which pipeline this driver belongs to
  const uint32_t splitGroupId;   // for grouped execution
  const uint32_t partitionId;    // for local exchange partitioning
  std::shared_ptr<Task> task;
  Driver* driver;
};
```

Operators access the task, memory pool, query config, and split queues through
`DriverCtx`. Each operator's memory pool is a child of the task's pool,
named after the plan node and operator type:

```cpp
// Driver.cpp
velox::memory::MemoryPool* DriverCtx::addOperatorPool(...) {
  return task->addOperatorPool(planNodeId, splitGroupId, pipelineId, driverId, operatorType);
}
```

---

## ThreadState: Driver Lifecycle

A driver's concurrency state is tracked by `ThreadState`, serialized on the
Task's mutex:

```
Created (not on thread, all flags false)
  │
  ▼
Enqueued (isEnqueued=true, submitted to executor, no thread yet)
  │
  ▼
On Thread (thread field set, running runInternal())
  ├──► Blocked (hasBlockingFuture=true; future re-enqueues when resolved)
  ├──► Suspended (numSuspensions > 0; stack kept, e.g. waiting for memory arbitration)
  ├──► Yielded (re-enqueued at back of executor queue)
  └──► Terminated (isTerminated=true; final state)
```

Key state transitions:
- `Driver::enqueue()` — sets `isEnqueued=true`, submits `Driver::run()` to the
  executor.
- `Driver::run()` — the executor thread calls this, which calls `runInternal()`.
- `BlockingState::setResume()` — when the blocking future resolves, re-calls
  `Driver::enqueue()`.
- `Task::resume()` — after a pause, re-enqueues all off-thread drivers.
- `Task::removeDriver()` — called when a driver finishes; decrements
  `numRunningDrivers_` and checks if the task is complete.

---

## `runInternal`: The Core Execution Loop

`Driver::run()` calls `Driver::runInternal()`, which is the beating heart of
the driver. It returns a `StopReason` that tells the caller what happened:

| StopReason | Meaning |
|---|---|
| `kNone` | Keep running (internal loop continues) |
| `kBlock` | An operator blocked; driver goes off-thread |
| `kYield` | CPU time slice expired; re-enqueue at back of queue |
| `kPause` | Task was paused; driver suspends |
| `kTerminate` | Task was cancelled or errored |
| `kAtEnd` | All operators finished; pipeline is done |

### The loop structure

The loop iterates over operators starting from the consumer end (the last
non-source operator) and walks backwards:

```
operators_ = [Source(0), Op(1), Op(2), Sink(3)]
                                          ▲
                                    start here, walk backward
```

For each operator pair `(i, i+1)`:

```cpp
// Pseudocode of runInternal
for (int i = startingOperator; i >= 0; --i) {
  if (shouldYield()) return kYield;

  Operator* op     = operators_[i];
  Operator* nextOp = operators_[i + 1];  // consumer

  if (op->isBlocked(&future))     return kBlock;
  if (nextOp->isBlocked(&future)) return kBlock;

  if (nextOp->needsInput()) {
    auto result = op->getOutput();
    if (result) {
      nextOp->addInput(result);
      i += 2;    // jump forward: let nextOp try to produce output now
      continue;
    } else {
      if (op->isFinished()) {
        nextOp->noMoreInput();
        break;
      }
    }
  }
}
```

This loop has two key properties:

1. **Data moves eagerly downstream.** When operator `i` produces a batch,
   the index immediately jumps forward (`i += 2`) so operator `i+1` gets to
   run without waiting for another loop iteration.

2. **The loop starts from the consumer side.** This ensures the pipeline
   drains from the bottom up — the most downstream operator that needs input
   gets priority, which minimizes buffer buildup.

For the sink (last operator, `i == operators_.size() - 1`), the loop calls
`getOutput()` and returns any result to the Task (`kBlock`), which gives the
result back to the caller in serial mode.

### `CALL_OPERATOR` macro

Every operator method call is wrapped in this macro:

```cpp
CALL_OPERATOR(op->getOutput(), op, curOperatorId_, kOpMethodGetOutput);
```

It does four things:
1. Marks the operator as in a non-reclaimable section (prevents memory
   arbitration from reclaiming memory mid-call).
2. Installs a `RuntimeStatWriterScopeGuard` so any stats written inside the
   operator are attributed to it.
3. Records which operator method is running in `OpCallStatus` (useful for
   detecting stuck calls).
4. Annotates any exception thrown with the operator's identity.

---

## Blocking: How Drivers Go Off-Thread and Come Back

When an operator returns a non-null `future` from `isBlocked()`, the driver
must go off-thread:

```cpp
// runInternal (simplified)
if (op->isBlocked(&future)) {
  blockingState = std::make_shared<BlockingState>(self, std::move(future), op, reason);
  return StopReason::kBlock;
}
```

Back in `Driver::run()`:

```cpp
case StopReason::kBlock:
  BlockingState::setResume(blockingState);
  return;
```

`BlockingState::setResume()` attaches a callback to the future via Folly's
executor:

```cpp
std::move(state->future_)
    .via(&exec)
    .thenValue([state](auto&&) {
        // future resolved → re-enqueue the driver
        Driver::enqueue(state->driver_);
    });
```

The driver is now off-thread. When the future resolves (e.g., the join build
side finishes, a split becomes available, memory arbitration completes), the
callback runs and `Driver::enqueue()` re-submits the driver to the executor.
The driver then resumes from the beginning of `runInternal()`, not from where
it blocked. It always re-starts from the consumer end to prioritize draining
buffered data.

---

## CPU Yield Slicing

Long-running drivers could monopolize an executor thread, starving other
tasks. To prevent this, each driver tracks how long it has been on-thread
via `ThreadState::startExecTimeMs`. If the driver has run longer than the
configured `driverCpuTimeSliceLimitMs`:

```cpp
bool Driver::shouldYield() const {
  if (cpuSliceMs_ == 0) return false;
  return execTimeMs() >= cpuSliceMs_;
}
```

`runInternal` checks `shouldYield()` at the top of every iteration. If it
returns true, the driver returns `kYield`:

```cpp
case StopReason::kYield:
  Driver::enqueue(self);   // re-enqueue at the back
  return;
```

The driver gives up the thread and goes to the back of the executor queue,
letting other drivers (and tasks) run.

---

## How Splits Reach Source Operators

`TableScan` (and `Exchange`) are source operators that need external data
in the form of splits (file paths, byte ranges, etc.). The external caller
adds splits to the task:

```cpp
task->addSplit(planNodeId, Split(connectorSplit));
task->noMoreSplits(planNodeId);
```

The Task stores splits in a `SplitsStore` per source plan node. When
`TableScan::getOutput()` needs a new split, it calls:

```cpp
// Inside TableScan
BlockingReason reason = operatorCtx_->task()->getSplitOrFuture(
    driverId, splitGroupId, planNodeId_,
    maxPreloadSplits, preload, split_, future_);
```

If a split is available, `getSplitOrFuture` returns `kNotBlocked` and
fills `split_`. If no split is available yet, it returns `kWaitForSplit`
and fills `future_` with a promise that the Task fulfills when a new
split arrives via `addSplit()`. The driver then goes off-thread until
the promise is fulfilled.

---

## Dynamic Filter Pushdown Through the Driver

The Driver mediates dynamic filter pushdown from `HashProbe` back to
`TableScan`. After building the hash table, `HashProbe` knows the set of
join key values and can push a min/max filter upstream to skip I/O.

```cpp
driver_->pushdownFilters(this, channels, makeFilter);
```

`Driver::pushdownFilters()` walks the operator list backwards from
`HashProbe`, using each intermediate operator's `identityProjections()` to
translate the output channel back to the upstream operator's input channel.
When it reaches an operator that returns `canAddDynamicFilter() == true`
(typically `TableScan` or `FilterProject`), it installs the filter there
via `addDynamicFilterLocked()`.

The filter is stored in a `PipelinePushdownFilters` structure shared across
all parallel drivers of the same pipeline, so only the first driver to
generate a particular filter actually pushes it; subsequent drivers see
that it is already applied.

---

## Driver Finalization and Task Completion

When all operators in a driver's pipeline have finished, `runInternal`
returns `kAtEnd`:

```cpp
// runInternal
if (op->isFinished()) {
  close();
  return StopReason::kAtEnd;
}
```

`close()` calls `Operator::close()` on every operator (releasing memory
and stats), then calls `Task::removeDriver()`. `removeDriver()` decrements
the running driver count for the split group, and checks if all drivers
across all pipelines have finished:

```cpp
// Task::removeDriver()
driverPtr = nullptr;
driverClosedLocked();      // → ++numFinishedDrivers_
allFinished = checkIfFinishedLocked();
```

When `allFinished` is true, `Task::terminate(TaskState::kFinished)` is
called, which fulfills the task completion future, notifies listeners,
and releases task-level resources.

---

## Serial vs Parallel Execution: Summary

| Aspect | `kSerial` (`Task::next()`) | `kParallel` (`Task::start()`) |
|---|---|---|
| Threads | Caller's thread only | Thread pool from `QueryCtx::executor()` |
| Driver scheduling | `Task::next()` round-robins all drivers | Each driver self-schedules via `Driver::enqueue()` |
| Result delivery | Returned from `Task::next()` | Pushed to `OutputBufferManager` or a `Consumer` callback |
| Blocking | External blocker; caller waits on returned future | Driver goes off-thread; re-enqueued on future resolution |
| CPU yield | Disabled (`cpuSliceMs_ == 0`) | Enabled if query config sets a slice limit |
| Typical use | Tests, simple local execution | Production distributed execution |

---

## End-to-End Example: Simple Scan + Filter

```
Plan:   TableScan → FilterProject

Execution flow (serial mode):
  Task::next()
    driver->next()
      runInternal()
        i=1 (FilterProject): needsInput()=true
        i=0 (TableScan):     getOutput() → nullptr (no split yet)
                             isBlocked() → kWaitForSplit
        blockDriver() → return kBlock, future set

  caller waits on future
  task->addSplit(...) → promise fulfilled → future ready

  Task::next()  (called again after future)
    driver->next()
      runInternal()
        i=1 (FilterProject): needsInput()=true
        i=0 (TableScan):     getOutput() → Batch{1024 rows}
        FilterProject.addInput(batch)
        i=3 (jump forward)
        i=1 (FilterProject): getOutput() → FilteredBatch{512 rows}
        i=2 (Sink):          ← pipeline ends; result returned to Task::next()
      return kBlock (result ready)
    return FilteredBatch

  Task::next() returns FilteredBatch to caller
```

---

## Reference: Key Files

| File | Purpose |
|---|---|
| `velox/exec/Driver.h/cpp` | Driver, DriverCtx, ThreadState, BlockingState, DriverFactory |
| `velox/exec/Task.h/cpp` | Task lifecycle, driver creation, split management |
| `velox/exec/LocalPlanner.h/cpp` | Plan → pipeline splitting, DriverFactory construction |
| `velox/exec/BlockingReason.h` | Enum of all blocking reasons (kWaitForSplit, kWaitForJoinBuild, etc.) |
| `velox/exec/Operator.h` | Base class and registration API |
| `velox/core/QueryCtx.h` | Executor, memory pool, and session config passed to Task |
