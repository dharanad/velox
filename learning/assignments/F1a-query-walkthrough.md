# F1a — One query end to end

**Format:** Explain · **Size:** S · **Prerequisite:** F1

## Goal

Connect the vectors you built in F1 to a complete query before studying the
individual subsystems in depth. Keep the sketch as a reference for later phases.

## Reading

- `velox/examples/ScanAndSort.cpp`: input vectors and plan execution.
- `velox/exec/tests/utils/PlanBuilder.h`: values, filter, project, and aggregation.
- `velox/exec/tests/utils/AssertQueryBuilder.h`: running a plan and checking output.
- A small values-based case in `velox/exec/tests/AggregationTest.cpp`.

## Checkpoints

1. **Choose a query.** Use a small input with a grouping key and nullable BIGINT
   values. Describe a filter, a computed projection, and a grouped sum. Include
   a filtered-out row, a null value, and repeated keys. Work out the expected
   rows by hand, including the null behavior. Record the input and operations
   precisely so the same query can be revisited.
2. **Trace the data.** Find an existing test with a similar plan and sketch
   your query from input `RowVector` through filter/project and aggregation to
   output. Annotate each stage with column names, types, expected values, and
   source references. Mark predictions about intermediate encodings as
   predictions until verified. At this point a diagram and explanation are
   enough; implementing a new execution harness is optional.
3. **Check the model.** Explain what each stage consumes and produces, how
   nulls affect the filter and sum, and where many input rows become fewer
   output rows. List execution questions you cannot answer yet for X1.

## Acceptance

- Input rows, operations, and expected output are unambiguous.
- The sketch connects all stages and explains their types and null behavior.
- Source references identify where to inspect or execute a similar plan.
- The sketch and open questions are recorded under F1a in `PROGRESS.md`.

## Revisit in X1

Use the same query to explain tasks, drivers, operator lifecycle, and blocking
boundaries. Verify the earlier encoding predictions by inspecting execution
and record corrections. Deeper execution details belong in X1.

## Stretch

Write and run a test for this query in the learning target, comparing the output
with your hand-calculated result. You write the implementation yourself.
