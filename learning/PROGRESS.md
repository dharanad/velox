# Progress

| # | Started | Done | Hints used | Notes |
|---|---|---|---|---|
| F0 | | | | |

## Takeaways

Record anything that surprised you, design choices worth remembering, and the
questions you got wrong in a debrief.

### F0

Record environment and toolchain versions, exact configure/build/test commands,
configuration time, cold build time, warm rebuild time, test time, ccache
statistics, and debugger/editor setup here. Timings are machine baselines.

Environment
OS = Ubuntu 22.04.5 LTS
Ram = 47G
CPU Arch = x86_64
Byte Order = Little Endian
CPUs = 24
cmake version 4.3.2
g++ 11.4.0
ninja 1.10.1
GNU gdb 12.1

`time` output for `make debug TARGETS=velox_vector_test EXTRA_CMAKE_FLAGS="-DVELOX_ENABLE_GROUPED_TESTS=OFF"`
33.61s user 81.60s system 89% cpu 2:08.38 total
`time` output for `./_build/debug/velox/vector/tests/velox_vector_test   0.12s user 0.01s system 95% cpu 0.142 total`

### F1a

Record the input rows, query operations, expected output, data-flow sketch,
source references, and open execution questions here. Revisit this in X1.

## Delayed recall

About a week after an assignment, answer from memory before checking the source.
Record understanding of structure, ownership, and invariants; API spelling is
not the goal. Follow up only on gaps, without delaying the next assignment.

| Assignment | Completed | Recall due | Checked | Gaps and corrections | Revisit |
|---|---|---|---|---|---|---|
