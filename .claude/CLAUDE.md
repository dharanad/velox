# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## PR Review

When asked to review a PR (via `/pr-review`), always use the /pr-review skill.

## Queries

When asked a question about the PR or codebase (via `/query`), use the /query skill.

## Module Documentation

When asked to document a module (via `/module-doc <path>`), use the /module-doc skill.

## Assignments

When asked to create a learning assignment from a design document (via `/assignment <doc-path>`), use the /assignment skill.

## Overview

Velox is an open source C++ library for composable data processing and
query execution. Licensed under Apache 2.0. Requires C++20, GCC 11+ or
Clang 15+. It takes a fully optimized query plan as input and executes it;
SQL parsing and query optimization happen outside Velox.

## Architecture

Velox's execution stack, from bottom to top:

- **`velox/type`** — Generic type system (scalar, complex, and nested types). Entry point for any new type.
- **`velox/vector`** — Arrow-compatible columnar memory layout with multiple encodings: Flat, Dictionary, Constant, Sequence/RLE, and Lazy. `BaseVector` is the root abstraction.
- **`velox/expression`** — Fully vectorized expression evaluation over Vectors. `Expr` / `ExprCompiler` compile a plan expression tree; `EvalCtx` carries evaluation state.
- **`velox/functions`** — Vectorized scalar, aggregate, and window function implementations. `prestosql/` and `sparksql/` hold dialect-specific functions.
- **`velox/exec`** — Relational operators (scan, filter, project, hash join, group-by, order-by, exchange). A `Task` drives a pipeline of `Driver`s, each running a chain of `Operator`s.
- **`velox/connectors`** — Extensible data source/sink interface. `hive/` implements the Hive connector for ORC, DWRF, Parquet, and Nimble via `velox/dwio`.
- **`velox/core`** — Query plan nodes (`PlanNode`) and execution context (`QueryCtx`).
- **`velox/common`** — Cross-cutting infrastructure: memory management (`memory/`), caching (`caching/`), file I/O abstraction (`file/`), compression, config, and metrics.
- **`velox/serializers`** — Wire protocols for distributed shuffle: PrestoPage and Spark UnsafeRow.

Extensibility points: custom types, scalar/aggregate/window functions, operators, file formats, storage adapters, and network serializers.

## Build

```bash
make debug    # debug build
make release  # optimized build
```

## Testing

```bash
make unittest                              # run all tests
cd _build/debug && ctest -j 8             # run all tests in parallel
ctest -R ExprTest                          # run tests matching a pattern
./_build/debug/velox/expression/tests/velox_expression_test --gtest_filter=ExprTest.basic  # run a single test
```

Test files live in `tests/` subdirectories alongside source.

## Formatting

```bash
make format  # format all changed files
```

## Coding Style

Read [CODING_STYLE.md](../CODING_STYLE.md) for the complete guide. Key rules
are summarized below.

### Comments

- Use `///` for public API documentation (classes, public methods, public members).
- Use `//` for private/protected members and comments inside code blocks.
- Start comments with active verbs, not "This class…" or "This method…".
  - ❌ `/// This class builds query plans.`
  - ✅ `/// Builds query plans.`
- Comments should be full English sentences starting with a capital letter and ending with a period.
- Comment every class, every non-trivial method, every member variable.
- Do not restate the variable name. Either explain the semantic meaning or omit the comment.
  - ❌ `// A simple counter.` above `size_t count_{0};`
- Avoid redundant comments that repeat what the code already says. Comments should explain *why*, not *what*.
- Use `// TODO: Description.` for future work. Do not include author's username.
- Do not duplicate comments between `.h` and `.cpp`. Document the function in the header; the implementation should not repeat the same comment. Duplicated comments diverge over time.

### Naming Conventions

- **PascalCase** for types and file names.
- **camelCase** for functions, member and local variables.
- **camelCase_** for private and protected member variables.
- **snake_case** for namespace names and build targets.
- **UPPER_SNAKE_CASE** for macros.
- **kPascalCase** for static constants and enumerators.
- Do not abbreviate. Use full, descriptive names. Well-established abbreviations (`id`, `url`, `sql`, `expr`) are acceptable.
- Prefer `numXxx` over `xxxCount` (e.g. `numRows`, `numKeys`).
- Never name a file or class `*Utils`, `*Helpers`, or `*Common`. These generic
  names attract unrelated functions over time and lose cohesion. Name files and
  classes after the concept they represent. Use a class with static methods to
  group related operations, and shorten method names since the class name
  provides context.

### Asserts and CHECKs

- Use `VELOX_CHECK_*` for internal errors, `VELOX_USER_CHECK_*` for user errors.
- Prefer two-argument forms: `VELOX_CHECK_LT(idx, size)` over `VELOX_CHECK(idx < size)`.
- Use `VELOX_FAIL()` / `VELOX_USER_FAIL()` to throw unconditionally.
- Use `VELOX_UNREACHABLE()` for impossible branches, `VELOX_NYI()` for unimplemented paths.
- Put runtime information (names, values, types) at the **end** of error messages, after the static description.
  - ❌ `VELOX_USER_FAIL("Column '{}' is ambiguous", name);`
  - ✅ `VELOX_USER_FAIL("Column is ambiguous: {}", name);`

### Variables

- Prefer value types, then `std::optional`, then `std::unique_ptr`.
- Prefer `std::string_view` over `const std::string&` for function parameters.
- Use uniform initialization: `size_t size{0}` over `size_t size = 0`.
- Declare variables in the smallest scope, as close to usage as possible.
- Use digit separators (`'`) for numeric literals with 4 or more digits: `10'000`, not `10000`.
- Use trailing commas in multi-line initializer lists, enum definitions, and
  function-call argument lists that span multiple lines. This produces cleaner
  diffs when items are added or reordered.

### API Design

- Keep the public API surface small.
- Prefer free functions in `.cpp` (anonymous namespace) over private/static class methods.
- Define free functions close to where they are used, not grouped together at the top or bottom of the file.
- Keep method implementations in `.cpp` except for trivial one-liners.
- Avoid default arguments when all callers can pass values explicitly.
- Never use `friend`, `FRIEND_TEST`, or any friend declarations. If a test needs access to private members, redesign the API or test through public methods instead.

### Tests

- Place new tests next to related existing tests, not at the end of the file. Group tests by topic (e.g., place `tryCast` next to `types`, `notBetween` next to `ifClause` which uses `between`).

Use gtest container matchers (`testing::ElementsAre`, etc.) for verifying collections:

```cpp
// ❌ Avoid - multiple individual assertions
EXPECT_EQ(result.size(), 3);
EXPECT_EQ(result[0], "a");
EXPECT_EQ(result[1], "b");
EXPECT_EQ(result[2], "c");

// ✅ Prefer - single matcher assertion
EXPECT_THAT(result, testing::ElementsAre("a", "b", "c"));
```

Common matchers:
- `ElementsAre(...)` - exact ordered match
- `UnorderedElementsAre(...)` - exact unordered match
- `Contains(...)` - at least one element matches
- `IsEmpty()` - collection is empty
- `SizeIs(n)` - collection has n elements

Requires `#include <gmock/gmock.h>`.

## Common Mistakes

These are frequently violated rules. Check every new or modified line against
this list before finishing.

- **Bug fixes without a failing test first.** Write the test first, confirm it fails, then fix. A test that passes with and without the fix proves nothing.
- **`///` vs `//` wrong comment style.** `///` is only for public API in headers. Everything else uses `//`.
- **One-letter and abbreviated variable names.** Use full, descriptive names. Only loop indices (`i`, `j`) are acceptable.
- **Undocumented APIs in headers.** Every class, method, and member variable in a `.h` file must have a comment.
- **Non-trivial implementations in headers.** If a method body has more than one statement, it belongs in the `.cpp` file.
- **`goto` statements.** Never use `goto`. Use early returns, helper functions, or duplicated code paths.
- **Fitting tests to buggy code.** Never update test expectations to match buggy output without verifying correctness first.
- **Generic file and class names.** Never name a file or class `*Utils`, `*Helpers`, or `*Common`.
- **Verify causation before asserting it.** Do not attribute failures to a commit based on its message alone. Verify empirically.
- **Silently simplifying an approved plan.** If a step is harder than expected, say so and get approval before reducing scope.
- **Working around infrastructure bugs.** Do not silently work around bugs in shared infrastructure. Report and discuss.

## Design Documents

Design (including proposals) live in `docs/designs/`.  When creating new
designs, place them there with a descriptive filename (e.g.,
`column-extraction-pushdown.md`).
