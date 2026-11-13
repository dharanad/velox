# `IdentityProjection` and Input/Output Channels in Velox

## Background: How Velox Represents Columnar Data

A `RowVector` in Velox is a struct-of-arrays — each column is a `VectorPtr` child, addressed by a zero-based **channel** (i.e., column index). When one operator produces output and the next operator consumes it as input, columns are referred to by their channel index in those respective vectors.

The central question every operator must answer is:

> "I received columns at positions {0, 1, 2, …} in my input. Which columns do I compute from scratch, which do I pass through untouched, and where does each column end up in my output?"

`IdentityProjection` and the input/output channel machinery answer exactly this.

---

## `IdentityProjection`

```cpp
// velox/exec/Operator.h:32
struct IdentityProjection {
  IdentityProjection(column_index_t _inputChannel, column_index_t _outputChannel)
      : inputChannel(_inputChannel), outputChannel(_outputChannel) {}

  column_index_t inputChannel;
  column_index_t outputChannel;
};
```

`IdentityProjection` describes a single column that flows from input to output **without any computation** — the column's `VectorPtr` is reused as-is (or wrapped in a dictionary if rows are filtered/reordered). It is a pair `(inputChannel → outputChannel)`.

### The two projection lists on every `Operator`

```cpp
// velox/exec/Operator.h:664-668
std::vector<IdentityProjection> identityProjections_;   // pass-through columns
std::vector<VectorPtr>          results_;               // computed columns
std::vector<IdentityProjection> resultProjections_;     // maps results_ index → output channel
bool                            isIdentityProjection_;  // true when output == input exactly
```

| List | What it contains |
|---|---|
| `identityProjections_` | Columns taken verbatim from `input_`, mapped to their output position |
| `results_` + `resultProjections_` | Columns produced by expressions/computation, mapped to their output position |

Together they tile the output completely: every output channel is covered by exactly one entry from either list.

---

## `fillOutput` — How the Lists Are Used

`fillOutput` assembles the final `RowVector` from those two lists:

```cpp
// velox/exec/Operator.cpp:248
RowVectorPtr Operator::fillOutput(
    vector_size_t size,
    const BufferPtr& mapping,
    const std::vector<VectorPtr>& results) {

  // Fast path: if nothing changed (no filter, no reorder), return input directly.
  if (size == input_->size() && /* mapping is identity */) {
    if (isIdentityProjection_) {
      return std::move(input_);       // zero-copy passthrough
    }
    wrapResults = false;
  }

  std::vector<VectorPtr> projectedChildren(outputType_->size());

  // 1. Fill identity columns from input_
  projectChildren(projectedChildren, input_, identityProjections_, size, mapping);

  // 2. Fill computed columns from results
  projectChildren(projectedChildren, results, resultProjections_, size, mapping);

  return std::make_shared<RowVector>(..., std::move(projectedChildren));
}
```

When `mapping` is provided (e.g., after a filter removed rows), each identity column is wrapped in a `DictionaryVector` pointing to the surviving row indices — **no data is copied**.

---

## Example 1: `FilterProject` — Filter + Projection

Consider this SQL:

```sql
SELECT a, c + 1 AS d
FROM t
WHERE b > 5
```

The `ProjectNode` has output schema `{a, d}` and input schema `{a, b, c}`.

During `FilterProject::initialize()`:

```cpp
// velox/exec/FilterProject.cpp:175-188
for (column_index_t i = 0; i < project_->projections().size(); i++) {
  auto& projection = project_->projections()[i];

  // Is this projection just "input column x"?
  bool identityProjection = checkAddIdentityProjection(
      projection, inputType, i, identityProjections_);

  if (!identityProjection) {
    // It's a real expression — compile it.
    allExprs.push_back(projection);
    resultProjections_.emplace_back(allExprs.size() - 1, i);
  }
}
```

Result for the example:

| Output channel | Column | Kind |
|---|---|---|
| 0 | `a` | `identityProjections_`: `{inputChannel=0, outputChannel=0}` |
| 1 | `d` (= `c+1`) | `resultProjections_`: `{resultsIndex=0, outputChannel=1}` |

When `getOutput()` runs:

1. Filter evaluates `b > 5` → produces a mapping of surviving rows.
2. `project()` evaluates `c + 1` → puts result in `results_[0]`.
3. `fillOutput(numOut, selectedIndices, results)` is called:
   - Column `a` is wrapped in a `DictionaryVector` over `selectedIndices` — no copy.
   - Column `d` (already evaluated only for selected rows) is placed at output channel 1.

### Pure filter (no projection)

If the node is only a `FilterNode` (no project), `identityProjections_` maps every input column to the same output channel, and `isIdentityProjection_ = true`. After filtering, `fillOutput` wraps the entire input in a dictionary — again no column data is copied.

---

## Example 2: `HashProbe` — Join and Dynamic Filter Pushdown

For a hash join:

```sql
SELECT t.a, t.b, s.x
FROM t JOIN s ON t.id = s.id
```

Probe input type: `{id, a, b}`. Output type: `{a, b, x}`.

In `HashProbe::initialize()`:

```cpp
// velox/exec/HashProbe.cpp:163-176
for (auto i = 0; i < probeType_->size(); ++i) {
  auto& name = probeType_->nameOf(i);
  auto outIndex = outputType_->getChildIdxIfExists(name);
  if (!outIndex.has_value()) continue; // column not in output (e.g. 'id')

  identityProjections_.emplace_back(i, *outIndex);
}
```

Result:

```
identityProjections_ = [
  {inputChannel=1, outputChannel=0},   // a: probe channel 1 → output channel 0
  {inputChannel=2, outputChannel=1},   // b: probe channel 2 → output channel 1
]
```

`x` comes from the build-side hash table and is placed into `results_` separately.

### The second role: dynamic filter pushdown

`identityProjections_` has a second, subtler purpose. When `HashProbe` discovers at runtime that the build side is a small set of values, it can generate a filter and push it upstream to `TableScan` to skip reading entire row groups. But to do that, it needs to know whether the column it wants to filter on flows through intermediate operators unmodified.

The `Driver::pushdownFilters` method walks the operator pipeline backwards, using `identityProjections()` to trace each column through intermediate operators:

```cpp
// velox/exec/Driver.cpp:1107-1121
for (j = filterSourceIndex - 1; j >= 0; --j) {
  auto* prevOp = operators_[j].get();

  const auto& identityProjections = prevOp->identityProjections();
  const auto inputChannel =
      getIdentityProjection(identityProjections, channel);

  if (!inputChannel.has_value()) {
    // This operator transforms the column — stop walking.
    break;
  }
  // Column passes through unchanged — keep walking upstream.
  channel = inputChannel.value();
}
```

If the walk reaches a `TableScan` (which implements `canAddDynamicFilter() = true`), the filter is injected there. This is only valid because each intermediate operator declared via `identityProjections_` that it did not transform the column — it just forwarded a `VectorPtr` from input to output.

**Concrete pipeline:**

```
TableScan → FilterProject(filter only) → HashJoin(probe)
  channel: 2           channel: 2              channel: 2
```

`FilterProject` declares `identityProjections_ = [{2,2}]`, so the Driver knows the column at output channel 2 of `FilterProject` is the same as input channel 2 of `FilterProject`. The walk succeeds and the min/max filter lands in `TableScan`, which uses it to skip Parquet/ORC row groups entirely.

---

## `calculateOutputChannels` — Between-operator Column Reordering

```cpp
// velox/exec/Operator.h:695
std::vector<column_index_t> calculateOutputChannels(
    const RowTypePtr& sourceOutputType,
    const RowTypePtr& targetInputType,
    const RowTypePtr& targetOutputType);
```

When two operators are connected by the `Driver`, their column schemas do not have to be in the same order. For example, operator A might output `{b, a, c}` but operator B expects `{a, b, c}`.

`calculateOutputChannels` returns a mapping `outputChannels[i] = sourceChannel` — "to fill input channel `i` of the target, read source channel `outputChannels[i]`." If the schemas already match, it returns an empty vector as a sentinel meaning "no remapping needed."

Used in `PartitionedOutput` to select and reorder columns before serializing them for network shuffle:

```cpp
// velox/exec/PartitionedOutput.cpp:214
outputChannels_(calculateOutputChannels(
    inputType,            // what upstream produces
    outputType_,          // what we want to send
    outputType_))         // reference for identity check
```

---

## Summary

| Concept | Purpose |
|---|---|
| `IdentityProjection{inputChannel, outputChannel}` | Declares that a column flows from input to output unchanged — enabling zero-copy column reuse and dynamic filter tracing |
| `identityProjections_` | List of pass-through columns for this operator |
| `resultProjections_` | Maps computed `results_[]` indices to output channels |
| `isIdentityProjection_` | Shortcut: output == input with no row-count change → return `input_` directly |
| `fillOutput` | Assembles the output `RowVector` from both lists, using a dictionary wrap when rows are removed |
| `calculateOutputChannels` | Computes cross-operator column index remapping when schemas differ in ordering |

The design avoids copying column buffers in the common case. A filter that keeps 10% of rows still hands downstream operators the same `FlatVector` memory — just wrapped in a dictionary that selects the surviving indices.
