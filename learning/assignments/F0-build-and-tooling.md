# F0 — Build, test, and debug loop

**Format:** Extend · **Size:** S (two sessions if the cold build is slow)

## Goal

A fast edit, build, test, and debug loop, plus a `learning/` CMake target that
every later Extend assignment builds into. Every assignment after this one
depends on how fast this loop is.

## Reading

- `Makefile`: the `cmake`, `build`, and `debug` targets and the `TARGETS`
  variable.
- `CMake/VeloxUtils.cmake`: `velox_add_library`, `velox_link_libraries`, and
  `velox_add_grouped_tests` (also look at why the grouped-tests default differs
  between Linux and macOS).
- `velox/vector/tests/CMakeLists.txt`: a small, ordinary test target to copy
  the pattern from.

## Tasks

### Checkpoint 1 — Configure and cold build

1. Configure a debug build with grouped tests off, so each exec test file gets
   its own binary:
   `make debug TARGETS=velox_vector_test EXTRA_CMAKE_FLAGS="-DVELOX_ENABLE_GROUPED_TESTS=OFF"`
2. Run `DecodedVectorTest` on its own with `--gtest_filter`.

Done when: the filtered run prints only `DecodedVectorTest` cases, all passing.

### Checkpoint 2 — Trim the build (optional but worth it)

Find which `VELOX_ENABLE_*` options in the root `CMakeLists.txt` you can turn
off without losing anything the curriculum needs. Keep scalar functions,
aggregates, exec, and enough of a connector for `HashJoinTest` and
`AggregationTest` to build. Skip this if the build is already fast enough for
you.

### Checkpoint 3 — The `learning/` target

1. Add an option `VELOX_ENABLE_LEARNING` (default `OFF`) to the root
   `CMakeLists.txt`, and call `add_subdirectory(learning)` when it is on.
2. Write `learning/CMakeLists.txt` defining a test binary
   `velox_learning_test`, registered with ctest.
3. Add `learning/LearningSmokeTest.cpp` with one test that builds a small
   vector through `VectorTestBase` and checks it with a gtest matcher.

Done when: configuring with `-DVELOX_ENABLE_LEARNING=ON` and running
`ctest -R velox_learning` passes, and changing one line in the test rebuilds
only that binary.

### Checkpoint 4 — Debugger (optional)

The first Debug assignment introduces gdb where it is needed, so this can wait.

1. Run one `DecodedVectorTest` case under gdb with a breakpoint in
   `DecodedVector::decode`.
2. At the breakpoint, print the input vector's encoding and size, then step
   until you reach the branch that handles it.
3. Make sure your editor (clangd or the VS Code C++ extension) resolves symbols
   through `_build/debug/compile_commands.json`.

Done when: gdb stops in `DecodedVector::decode` and you can say which branch
your input took, and go-to-definition on `DecodedVector` works in your editor.

## Acceptance

- `ctest -R velox_learning` passes.
- The "Done when" of checkpoints 1 and 3 holds. Checkpoints 2 and 4 are
  optional.
- `PROGRESS.md` has the one configure/build line you will reuse. Nothing else
  needs recording.

## Questions for the debrief

1. Which library targets does your test link, and why does each one need to be
   there? What happens at link time if you leave out the one that provides
   `VectorTestBase`?
2. Why does Velox group test sources into shared binaries on Linux CI, and what
   does that cost a developer iterating on a single test?
3. After a one-line change to `velox/vector/DecodedVector.cpp`, what gets
   recompiled and relinked before `velox_vector_test` runs again?
