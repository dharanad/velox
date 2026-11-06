# Velox Relearning Plan

Relearn Velox from the ground up by writing the code by hand, then become an
expert in **scalar functions, aggregate functions, and the aggregation and join
operators**, and contribute upstream in those areas.

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
can be spread over weekday sessions.

- **S** — up to 2 hours. A weekday evening.
- **M** — half a day, or 2–3 weekday sessions by checkpoint.
- **L** — a weekend push.

## Tracking

Each assignment is an issue on the fork (`dharanad/velox`, label `learning`),
tracked on the private project
[Velox Relearning](https://github.com/users/dharanad/projects/7) with the
fields Status (Todo, In Progress, In Review, Done), Phase, Format, and Size.
Review notes go in the issue comments.

Reference issues with `Closes #N` only in commits on this branch. In a commit
that goes upstream, `#N` would point at an upstream issue.

## Workflow per assignment

1. Claude writes `learning/assignments/NN-<topic>.md` with the goal, reading
   list, tasks, acceptance criteria, checkpoints, and stretch goals, and links
   it from the issue. Status moves to In Progress.
2. For rebuilds, Claude makes the hollow-out commit.
3. You implement and commit (`learning: NN <what>`).
4. Ask for a review (`review NN`). Status moves to In Review. Iterate until
   clean.
5. **Debrief.** For rebuilds, compare against the original with
   `git diff <hollow-commit>^ -- <file>`. Claude asks 3–5 questions on the
   design choices behind the upstream code.
6. Update `PROGRESS.md` with the date, hints used, and key takeaways. The
   final commit closes the issue, and Status moves to Done.

## Curriculum

The foundations are short on purpose. They are a refresher before the deep
tracks, which are where most of the time goes.

### Phase F — Foundations

| # | Assignment | Format | Size |
|---|---|---|---|
| F0 | Build, test, and debug loop; `learning/` CMake target | Extend | S |
| F1 | Vectors by hand: every encoding without VectorMaker | Extend | M |
| F2 | `DecodedVector` | Rebuild | M |
| F3 | `SelectivityVector`, `EvalCtx`, and the ensure-writable contract | Explain + Extend | S |

Reading: `velox/docs/develop/vectors.rst`, `dictionary-encoding.rst`,
`types.rst`, `memory.rst` (pools only).

### Phase S — Scalar functions (expert track)

| # | Assignment | Format | Size |
|---|---|---|---|
| S1 | Simple function API in depth: null behavior (`call`, `callNullable`, `callNullFree`), `initialize` with constant arguments, ASCII fast paths, string writers and zero-copy reuse, complex output types | Extend | M |
| S2 | The same function as a `VectorFunction`: signatures, flat and constant fast paths, `applyToSelectedNoThrow`, error handling with `EvalCtx` | Extend + Perf | M |
| S3 | `SimpleFunctionAdapter` internals | Rebuild | L |
| S4 | Expression evaluation: default null propagation, peeling, constant folding, CSE, AND/OR, TRY | Explain + Debug | M |
| S5 | Lambda functions and `FunctionVector` (`transform`, `filter`) | Rebuild | M |

Reading: `scalar-functions.rst`, `view-and-writer-types.rst`,
`expression-evaluation.rst`, `lambda-functions.rst`.

### Phase A — Aggregate functions (expert track)

| # | Assignment | Format | Size |
|---|---|---|---|
| A1 | `SimpleAggregateAdapter`: fixed-width accumulator, partial, intermediate and final, tested with `AggregationTestBase` | Extend | M |
| A2 | Raw `Aggregate` interface: `addRawInput`, `addIntermediateResults`, `extractValues`, `extractAccumulators`, single-group paths | Rebuild | L |
| A3 | Variable-width accumulators: `HashStringAllocator`, `ValueList`, `SingleValueAccumulator`, `destroy` | Rebuild + Debug | M |
| A4 | Companion functions, masks, `DISTINCT` and `ORDER BY` aggregations | Explain + Extend | M |

Reading: `aggregate-functions.rst`, `aggregations.rst`, `arena.rst`.

### Phase X — Execution core (prerequisite for H and J)

| # | Assignment | Format | Size |
|---|---|---|---|
| X1 | `PlanBuilder`, `Task`, `Driver`, `Operator` lifecycle, and `isBlocked`/futures | Explain | S |
| X2 | A custom operator plugged in through `PlanNodeTranslator` | Extend | M |

Reading: `task.rst`, `operators.rst`.

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

### Phase C — Contributing upstream

Pick open upstream issues in functions, aggregates, aggregation, or joins.
Claude acts as a pre-review before you open the PR.

## Order

F → S → A → X → H → J → C. S and A can be interleaved once F is done. H and J
depend on X. The plan is a living document, so reorder or add assignments as
interests shift.

## Branch mechanics

- `learning` is branched from `main`, pushed to your fork (`origin`) as a
  backup, and never upstreamed.
- Rebase onto `upstream/main` only between phases, so a hollow-out commit is
  never rebased mid-assignment.
- Your own code lives in `learning/`. Rebuild assignments edit real files in
  `velox/` on this branch only.
