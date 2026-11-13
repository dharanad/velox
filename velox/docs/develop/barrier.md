# Barriers

Barriers are a synchronization mechanism in Velox for coordinating output
draining across all drivers in a task. They are used in single-threaded
(serial) execution mode to ensure that all buffered output is flushed before
the caller proceeds — for example, before checkpointing or before cleanly
shutting down a pipeline.

## Concepts

### Barrier Split

A **barrier split** is a sentinel `Split` value that carries no data. When
a source operator receives a barrier split, it signals the driver to begin
output draining once all real data splits have been consumed.

```
Split::createBarrier(numDrivers)
```

The `numDrivers` field tells the split store how many per-driver copies of
the barrier to produce, so each driver receives exactly one.

Defined in `velox/exec/BarrierSplit.h` and `velox/exec/Split.h`.

### Driver Barrier State

Each driver carries a `BarrierState` (`velox/exec/Driver.h`) that tracks:

- `active` — whether barrier processing is in progress.
- `drainingOpId` — the index of the operator currently being drained.
- `dropInputOpId` — the highest operator index that has declared it needs
  no more input. Operators with a lower index may skip producing output as
  an optimization.

### Task Barrier State

The task (`velox/exec/Task.h`, `velox/exec/TaskStructs.h`) maintains:

- `barrierRequested_` — set while a barrier is in flight.
- `numDriversUnderBarrier_` — number of drivers that have not yet finished
  draining.
- `barrierFinishPromises_` — futures fulfilled when the last driver
  completes draining.

A separate `BarrierState` keyed by plan node ID in `SplitGroupState::barriers`
is used for multi-driver peer synchronization (see
[allPeersFinished](#allpeersfinished) below).

---

## Requesting a Barrier

```cpp
ContinueFuture future = task->requestBarrier();
// Await future to know when all drivers have finished draining.
```

`Task::requestBarrier()` is the public entry point. It:

1. Validates that the task is in serial execution mode and that every node in
   the pipeline declares `supportsBarrier() == true`.
2. Creates one barrier split per driver for every leaf (source) plan node and
   enqueues them in the split stores.
3. Activates barrier processing on every driver via
   `startDriverBarriersLocked()`.
4. Returns a `ContinueFuture` that is fulfilled when all drivers finish
   draining.

**Constraints**

- Only works in `ExecutionMode::kSerial`.
- Cannot be called after "no more splits" has been signalled on any leaf node.
- Cannot add new splits to the task while a barrier is in progress.
- All plan nodes in the fragment must report `supportsBarrier() == true`.

---

## Barrier Lifecycle

```
requestBarrier()
    │
    ├─ Creates barrier splits (one per driver per leaf node)
    ├─ Calls startDriverBarriersLocked()
    │      └─ Driver::startBarrier() → barrier_.active = true
    │
    │   [ Drivers continue executing normally ]
    │   [ Each driver eventually receives its barrier split ]
    │
    └─ When a driver exhausts its real splits it calls drainOutput()
           │
           └─ drainNextOperator()
                  │
                  ├─ operator[i]->startDrain()
                  │      true  → operator has buffered output; driver loops
                  │      false → no buffered output; advance to operator[i+1]
                  │
                  └─ After all operators: finishBarrier()
                         │
                         └─ Task::finishDriverBarrier()
                                │
                                └─ numDriversUnderBarrier_ == 0
                                       └─ endBarrierLocked()
                                              └─ Fulfills barrierFinishPromises_
```

---

## Driver-Level API

### `Driver::startBarrier()`

Activates barrier mode. Called by the task for every driver before any
barrier split reaches a source operator.

### `Driver::drainOutput()`

Begins sequential draining. Sets `drainingOpId` to 0 and immediately calls
`drainNextOperator()`.

### `Driver::drainNextOperator()`

Walks the operator pipeline from `drainingOpId` forward. For each operator it
calls `Operator::startDrain()`:

- If `startDrain()` returns `true`, the operator has buffered output. The
  driver stops advancing and lets the normal execution loop flush that output.
  The operator calls `Driver::finishDrain(operatorId)` when it is done.
- If `startDrain()` returns `false`, the operator has nothing to flush and the
  driver moves to the next one.

Once all operators have been processed, the driver calls `finishBarrier()`.

### `Driver::dropInput(operatorId)` and `Driver::shouldDropOutput(operatorId)`

An operator that no longer needs input (e.g., a `Limit` that has already
produced enough rows) calls `dropInput(id)`. Upstream operators then call
`shouldDropOutput(id)` to decide whether to discard their output rather than
push it forward, avoiding unnecessary work during draining.

---

## Operator Support

Operators opt in to barriers by overriding `PlanNode::supportsBarrier()` in
their corresponding plan node class. The task validates this at initialization
time and refuses to run `requestBarrier()` if any node in the fragment does
not support it.

Operators implement `Operator::startDrain()` to participate in the draining
phase. The base class version throws `VELOX_NYI`, so every participating
operator must provide its own implementation.

Operators with barrier support include:

- Exchange / MergeExchange
- LocalPartition / LocalMerge / MergeSource
- MixedUnion
- Limit
- AssignUniqueId
- MergeJoin / IndexLookupJoin
- CallbackSink
- FilterProject (pass-through)

Operators **without** barrier support (e.g. HashBuild, HashProbe, TableScan,
Aggregation) prevent `requestBarrier()` from being called on any task that
contains them.

---

## Split Store Mechanics

`SplitsStore` (`velox/exec/TaskStructs.h`) holds the queue of splits for a
single plan node. When a barrier split arrives:

- The store records one barrier split entry per driver in `barrierSplits_[driverId]`.
- All promises for drivers that are blocked waiting for the next split are
  immediately fulfilled, waking those drivers.

Each driver retrieves its barrier exactly once via `tryGetBarrier(driverId)`.
This per-driver keying ensures that a single `createBarrier(numDrivers)` call
distributes one barrier to each driver without duplication.

---

## allPeersFinished

`Task::allPeersFinished()` is a separate synchronization primitive used by
multi-driver operators such as `HashBuild` and `NestedLoopJoinBuild`. It is
not part of the output-drain barrier but shares the same `BarrierState`
struct.

When multiple drivers run the same build operator in parallel, each driver
calls `allPeersFinished()` when it has finished building its partition of the
hash table. The method:

- Increments a counter for the plan node.
- Returns `false` for all but the last driver, providing a `ContinueFuture`
  that the caller must await.
- Returns `true` for the last driver, providing the list of peer drivers and
  the promises to fulfill once the combined table is ready.

The last driver merges all partitions, then fulfills the promises so the
other drivers can continue.

```cpp
// Inside HashBuild::finishHashBuild()
std::vector<ContinuePromise> promises;
std::vector<std::shared_ptr<Driver>> peers;
if (!task()->allPeersFinished(planNodeId(), driver, &future_, promises, peers)) {
  setState(State::kWaitForBuild);
  return false;
}
// Merge peer tables ...
for (auto& promise : promises) {
  promise.setValue();
}
```

---

## Key Source Files

| File | Purpose |
|------|---------|
| `velox/exec/BarrierSplit.h` | `BarrierSplit` struct definition |
| `velox/exec/Split.h` | `Split::createBarrier()`, `Split::isBarrier()` |
| `velox/exec/TaskStructs.h` | `SplitsStore`, task-level `BarrierState` |
| `velox/exec/TaskStructs.cpp` | `SplitsStore::addSplit()`, `tryGetBarrier()` |
| `velox/exec/Task.h` | Public barrier API: `requestBarrier()`, `underBarrier()` |
| `velox/exec/Task.cpp` | `startBarrier()`, `finishDriverBarrier()`, `endBarrierLocked()`, `allPeersFinished()` |
| `velox/exec/Driver.h` | `Driver::BarrierState`, driver barrier declarations |
| `velox/exec/Driver.cpp` | `startBarrier()`, `drainOutput()`, `drainNextOperator()`, `finishBarrier()` |
| `velox/exec/Operator.h` | `Operator::startDrain()` interface |
| `velox/exec/BlockingReason.h` | `BlockingReason` enum used alongside barrier waits |
| `velox/exec/tests/TaskTest.cpp` | Barrier unit tests |
