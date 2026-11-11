# F2 — DecodedVector

**Format:** Rebuild · **Size:** M (3 checkpoints) · **Prerequisite:** F1

## Goal

Understand how `DecodedVector` turns any encoding into "a base vector, an
index per row, and a nulls mask", by reimplementing the parts that do it. This
is the generalization of the `sumNonNull` you wrote in F1.

## How a Rebuild works

1. You tell Claude, in your own words, what each method below must do (the
   "Before the hollow-out" list).
2. Claude replaces those method bodies in `velox/vector/DecodedVector.cpp`
   with `VELOX_NYI("Rebuild: ...")` in one commit. Everything else stays.
3. You reimplement them until the checkpoint's tests pass.
4. Debrief: you compare your code with the original, then the original is
   restored (see the workflow in `PLAN.md`).

## Reading

- `velox/vector/DecodedVector.h`: the class comment, `decode`, `index`,
  `isNullAt`, `valueAt`, `base`, and the private members.
- `velox/vector/tests/DecodedVectorTest.cpp`: skim the test names to see which
  behaviours are covered.

## Before the hollow-out

Answer these from the header and from using the class, not from the `.cpp`:

1. After `decode`, what do `base()`, `index(row)`, and `isNullAt(row)` return
   for a flat vector, a constant vector, and a dictionary over a flat vector?
2. What do `isIdentityMapping()` and `isConstantMapping()` let a caller skip?
3. For a dictionary over a dictionary, both with nulls, which rows are null?

## Checkpoints

The method lists and test filters are a proposal. Claude confirms both when
making the hollow-out commit, by checking which tests each method breaks, and
updates this file so each "Done when" is an exact command.

### 1 — Flat and constant inputs

- **Reimplement:** `setBaseData`, `setBaseDataForConstant`, `setFlatNulls`.
- **Done when:** these `DecodedVectorTest` cases pass: `flat`, `flatNulls`,
  `constant`, `constantNull`, `constantComplexType`.
- **Explain:** why does a constant need no per-row indices?

### 2 — Dictionary wrappers

- **Reimplement:** `combineWrappers`, `applyDictionaryWrapper`, `copyNulls`,
  `makeIndices`.
- **Done when:** the `dictionary`, `dictionaryOver*`, `nullsOf*`, and
  `emptyRows*` cases of `DecodedVectorTest` pass.
- **Explain:** when two dictionary layers are combined, how is the final index
  for a row computed, and when must the nulls be copied?

### 3 — Lazy fill-in and re-wrapping

- **Reimplement:** `fillInIndices`, `nulls(rows)`, `wrap`,
  `dictionaryWrapping`.
- **Done when:** all of `DecodedVectorTest` passes, plus one test you add for a
  case the existing tests do not cover. Say in the review why you chose it.
- **Explain:** why are the indices for identity and constant mappings built
  only on demand?

## Acceptance

- Every "Done when" holds and the review passes.
- Debrief done: you can name two places where the original differs from yours
  and why it was written that way.
- The original bodies are restored and your version is tagged
  `learning/F2-mine`.

## Delayed recall

About a week later, from memory, list the fields a `DecodedVector` fills in
during `decode` and what each is for.
