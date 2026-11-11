# Velox Relearning Plan

Relearn Velox from the ground up by writing the code by hand, then become an
expert in **scalar functions, aggregate functions, and the aggregation and join
operators**. This is purely educational: nothing on this branch goes upstream,
and upstream contributions are outside the curriculum.

## Ground rules

1. **You write all implementation code.** Claude writes assignment specs,
   hollows out code for rebuild assignments, and reviews. Claude does not write
   solution code, even when asked casually; ask for a hint instead.
2. **Conceptual questions are answered freely.** "How does peeling work?",
   "Why does RowContainer put the null flags there?" get a full senior-engineer
   answer with file and line references. Only the solution code is held back.
3. **Hint ladder** when stuck on an assignment:
   - `hint 1` — where to look (file, function, concept).
   - `hint 2` — the shape of the approach (data flow, invariants, edge cases).
   - `hint 3` — pseudocode for the tricky part.
   - `show me` — Claude explains the upstream solution. Use as a last resort
     and record it in PROGRESS.md.
4. **Reviews are upstream-grade.** Correctness, encodings and nulls, memory,
   performance, and CODING_STYLE.md / CLAUDE.md rules. Fix review comments
   before marking an assignment done.
5. **Bug-fix discipline applies here too.** A test fails first, then you fix.

## Assignment formats

| Format | What happens |
|---|---|
| **Rebuild** | Claude replaces real implementation bodies with `VELOX_NYI("Rebuild: ...")` in a `learning: hollow out ...` commit. You reimplement until the upstream tests pass, then diff your version against the original. |
| **Extend** | You build something new (function, aggregate, operator) from a spec, with your own tests, in `learning/`. |
| **Debug** | Claude plants one or more bugs in a commit you don't read. You find them using tests, gdb, and reasoning. |
| **Explain** | You read code and answer questions, or whiteboard a flow. No code. Good for short sessions. |
| **Perf** | Write a folly benchmark, measure, optimize, and explain the numbers. |

## Sizing

Every assignment has a size, and larger ones are split into checkpoints so they
can be spread over weekday sessions. Each checkpoint states its required work
and acceptance criteria; extra depth belongs in explicit stretch goals.

- **S** — up to 2 hours. A weekday evening.
- **M** — half a day, or 2–3 weekday sessions by checkpoint.
- **L** — a weekend push.

These are estimates of active work, excluding dependency installation and cold
builds. Rebuild sizes are provisional until the exact methods to remove are
listed. Split work that exceeds its estimate into independently reviewable
checkpoints before starting it.

## Tracking

Each assignment is an issue on the fork (`dharanad/velox`, label `learning`),
tracked on the private project
[Velox Relearning](https://github.com/users/dharanad/projects/7) with the
fields Status (Todo, In Progress, In Review, Done), Phase, Format, and Size.
Issues and the project carry status only. `PROGRESS.md` is the single written
record: review notes, hints used, takeaways, and recall results all go there.

Reference issues with `Closes #N` only in commits on this branch. In a commit
that goes upstream, `#N` would point at an upstream issue.

## Workflow per assignment

1. Claude writes `learning/assignments/<id>-<topic>.md` with the goal, reading
   list, tasks, acceptance criteria, checkpoints, and stretch goals, and links
   it from the issue. Specs are written one phase at a time, when the previous
   phase is nearly done, so that they match the current code. Every checkpoint
   states what to build, a "Done when" test, and a question to explain. Status moves to In Progress.
2. For rebuilds, first agree on the exact methods to remove and revise the
   size if needed. You describe their contract, invariants, ownership rules,
   and expected edge cases before Claude makes the hollow-out commit. Remove
   only those methods, keeping surrounding code as context.
3. You implement and commit (`learning: NN <what>`). For rebuilds, add at
   least one test case of your own, motivated by the contract or an edge case
   rather than copied from the implementation.
4. Ask for a review (`review NN`). Status moves to In Review. Claude records
   the review notes in `PROGRESS.md`. Iterate until clean.
5. **Debrief.** For rebuilds, compare against the original with
   `git diff <hollow-commit>^ -- <file>`. Claude asks 3–5 questions on the
   design choices behind the upstream code, including what your new test
   covers and why.
6. **Restore the original.** After a rebuild's debrief, tag your version
   (`learning/<id>-mine`), then restore the upstream bodies with
   `git checkout <hollow-commit>^ -- <file>` and commit
   (`learning: NN restore upstream`). Keep the test you added if it passes
   against the original. Later assignments then run on known-correct code.
7. Update `PROGRESS.md` with the date, hints used, and key takeaways. The
   final commit closes the issue, and Status moves to Done.
8. **Delayed recall.** About a week later, spend 15–20 minutes answering 3–5
   questions or sketching the data flow without opening the source or notes.
   Then check your answers and record gaps and corrections in `PROGRESS.md`.
   Revisit weak areas at the next check. Focus on structure, ownership, and
   invariants; exact API names and signatures can be looked up afterward.
   This is a follow-up to a completed assignment, not a gate on starting the
   next one.

## Curriculum

The foundations are short on purpose. They are a refresher before the deep
tracks, which are where most of the time goes.

### Phase F — Foundations

| # | Assignment | Format | Size |
|---|---|---|---|
| F0 | [Build, test, and debug loop](assignments/F0-build-and-tooling.md); `learning/` CMake target | Extend | S |
| F1 | [Vectors by hand](assignments/F1-vectors-by-hand.md): every encoding without VectorMaker | Extend | L |
| F1a | [One query end to end](assignments/F1a-query-walkthrough.md): input vectors, filter/project, aggregation, and output | Explain | S |
| F2 | [`DecodedVector`](assignments/F2-decoded-vector.md) | Rebuild | M |
| F3 | [`SelectivityVector`, `EvalCtx`, and the ensure-writable contract](assignments/F3-selectivity-and-evalctx.md) | Explain + Extend | S |

Reading: `velox/docs/develop/vectors.rst`, `dictionary-encoding.rst`,
`types.rst`, `memory.rst` (pools only).

Do F1a immediately after F1. Revisit the same query in X1 to connect the
visible data flow to tasks, drivers, operators, and blocking.

### Phase S — Scalar functions (expert track)

| # | Assignment | Format | Size |
|---|---|---|---|
| S1a | Simple function null behavior (`call`, `callNullable`, `callNullFree`) and `initialize` with constant arguments | Extend | M |
| S1b | String writers, ownership and zero-copy reuse, ASCII paths; a small benchmark baseline | Extend + Perf | M |
| S1c | Complex output types and their view/writer contracts | Extend | S |
| S2 | The S1b function as a `VectorFunction`: signatures, flat and constant fast paths, `applyToSelectedNoThrow`, error handling with `EvalCtx` | Extend + Perf | M |
| S3 | `SimpleFunctionAdapter` internals: a mini-adapter in `learning/` for one or two signatures, then a guided read of the real adapter | Explain + Extend | M |
| S4 | Expression evaluation: default null propagation, peeling, constant folding, CSE, AND/OR, TRY | Explain + Debug | M |
| S5 | Lambda functions and `FunctionVector` (`transform`, `filter`) | Rebuild | M |

Reading: `scalar-functions.rst`, `view-and-writer-types.rst`,
`expression-evaluation.rst`, `lambda-functions.rst`.

S3 is not a Rebuild. Every simple function instantiates
`SimpleFunctionAdapter.h`, so hollowing it would rebuild the whole functions
library on each edit and bury mistakes in template errors. Build the
mini-adapter first (argument decoding, null handling, and writing results for
the chosen signatures), then explain how the real adapter generalizes each
piece.

The S1 checkpoints have separate acceptance criteria:

| Checkpoint | Required evidence |
|---|---|
| S1a — Null behavior | Tests for each call contract, including null inputs, plus an explanation of when each contract is appropriate. |
| S1a — Initialization | Tests with constant and varying arguments, plus an explanation of what can be prepared once and what remains per-row work. |
| S1b — Strings | Tests for empty, short, long, ASCII, and non-ASCII strings; explain output ownership, reuse lifetimes, and ASCII-path selection. |
| S1b — Measurement | Benchmark a fixed workload with flat and constant inputs and at least two null distributions. Record build mode, input sizes, repeated timings, and correctness checks outside the timed region; explain results without requiring a speedup. |
| S1c — Complex outputs | Tests for null and empty containers and null elements, plus an explanation of the writer lifecycle and ownership. |

S1b stretch goals: optimize the measured bottleneck or add a specialized ASCII
path where the chosen function permits one. Keep the baseline for comparison.
S2 reuses that workload to compare the two implementations.

### Phase A — Aggregate functions (expert track)

| # | Assignment | Format | Size |
|---|---|---|---|
| A1 | `SimpleAggregateAdapter`: fixed-width accumulator, partial, intermediate and final, tested with `AggregationTestBase` | Extend | M |
| A2 | Raw `Aggregate` interface: `addRawInput`, `addIntermediateResults`, `extractValues`, `extractAccumulators`, single-group paths | Rebuild | L |
| A3 | Variable-width accumulators: `HashStringAllocator`, `ValueList`, `SingleValueAccumulator`, `destroy` | Rebuild + Debug | M |
| A4 | Companion functions, masks, `DISTINCT` and `ORDER BY` aggregations | Explain + Extend | M |

Reading: `aggregate-functions.rst`, `aggregations.rst`, `arena.rst`.

A2 opens with an Explain checkpoint on the accumulator row layout: where a
`RowContainer` row keeps each accumulator, its null bit, and the
initialized state, and what the offsets passed to an `Aggregate` mean. H2
rebuilds `RowContainer` much later, so this checkpoint covers only what an
aggregate author must know.

### Phase X — Execution core (prerequisite for H and J)

| # | Assignment | Format | Size |
|---|---|---|---|
| X1 | `PlanBuilder`, `Task`, `Driver`, `Operator` lifecycle, and `isBlocked`/futures | Explain | S |
| X2 | A custom operator plugged in through `PlanNodeTranslator` | Extend | M |
| X3 | Memory beyond pools: arbitration, `Operator::reclaim`, and what makes an operator reclaimable | Explain | S |

In X1, revisit F1a's query and annotate the sketch with the task, driver, and
operator lifecycle and any blocking boundaries. Explain any intermediate
vectors that differ from the original sketch.

X3 is the prerequisite for the spilling assignments, H6 and J4.

Reading: `task.rst`, `operators.rst`, `memory.rst` (in full for X3).

### Phase H — Aggregation operator (expert track)

| # | Assignment | Format | Size |
|---|---|---|---|
| H1 | `VectorHasher`: array, normalized-key, and hash modes; value IDs and ranges | Rebuild | M |
| H2 | `RowContainer`: row layout, null and free flags, store and extract | Rebuild | L |
| H3 | `HashTable` for group-by: probing, rehash, `HashBitRange` partitions | Explain + Debug | L |
| H4 | `GroupingSet` and `HashAggregation`: partial flush, abandoning partial aggregation, global aggregation | Debug | M |
| H5 | `StreamingAggregation` | Rebuild | M |
| H6 | Aggregation spilling | Explain + Debug | L |

Reading: `hash-table.rst`, `spilling.rst`.

### Phase J — Join operators (expert track)

| # | Assignment | Format | Size |
|---|---|---|---|
| J1 | `HashBuild` → `HashJoinBridge` → `HashProbe` flow; join type semantics | Explain | S |
| J2 | Probe-side output for semi, anti, and null-aware anti joins, including filters | Rebuild | L |
| J3 | Outer joins: right and full outer, the probed-flag, the last-prober drain | Rebuild | L |
| J4 | Dynamic filter pushdown and join spilling | Explain + Debug | L |
| J5 | `MergeJoin` and `NestedLoopJoin`: when and why | Explain | S |

Reading: `joins.rst`, `anti-join.rst`, `hash-table-caching.rst`.

## Order

F → S → A → X → H → J. S and A can be interleaved once F is done. H and J
depend on X, and H6 and J4 depend on X3. The plan is a living document, so reorder or add assignments as
interests shift.

## Branch mechanics

- `learning` is branched from `main`, pushed to your fork (`origin`) as a
  backup, and never upstreamed.
- Rebase onto `upstream/main` only between phases, so a hollow-out commit is
  never rebased mid-assignment.
- Your own code lives in `learning/`. Rebuild assignments edit real files in
  `velox/` on this branch only, and those files return to their upstream
  contents once the assignment is done, so `velox/` differs from `main` only
  while a rebuild is in progress (plus any tests you added).
