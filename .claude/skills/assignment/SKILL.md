---
name: assignment
description: Generate a hands-on learning assignment from a Velox design document. Reads the design doc, extracts the core concepts, and writes a progressive series of implementation exercises to velox/docs/assignments/. Use when the user types "/assignment <design-doc-path>" or asks to create an assignment from a document.
---

# Velox Assignment Generator Skill

Generate a self-contained, hands-on assignment from an existing Velox design
document and write it to `velox/docs/assignments/<topic>.md`.

## Argument

The user provides a path to a design document, e.g.:
- `/assignment velox/docs/designs/memory-module.md`
- `/assignment velox/docs/designs/exec-module.md`

Derive the output filename from the design doc name
(e.g. `memory-module.md` → `velox/docs/assignments/memory-module.md`).
If the assignment file already exists, read it first and update rather than
overwrite.

## Steps

1. **Read the design document** in full.

2. **Identify 5–7 core concepts** from the document — the key abstractions,
   data structures, algorithms, or design patterns that a reader must
   internalize. Order them from most foundational to most complex.

3. **Write one exercise per concept** (see structure below).

4. **Write a capstone exercise** that wires all concepts together into a
   single working system.

5. **Write a learning checklist** of bullet points the student should be able
   to explain after finishing.

6. **Save the file** to `velox/docs/assignments/<topic>.md`.

## Exercise Structure

Each exercise must have all of the following sections:

### Goal
One sentence: the single concept this exercise teaches.

### What to build
A concrete implementation task described in terms of class names, method
signatures, and invariants to enforce. Be specific — say "use `std::atomic<int64_t>`
with `fetch_add` + rollback" not "track memory usage". The student should not
need to read the source to understand what to build.

### Constraints
The rules that force the student to use the concept correctly (no using a
global mutex when atomics are required, etc.).

### Test to pass
A self-contained `main()` snippet with `assert()` calls that passes only when
the implementation is correct. All parameters must be explicit (no magic
numbers without explanation). The test must be compilable with:
```bash
g++ -std=c++20 -O0 -g <file>.cpp -o <file> && ./<file>
```

### Questions to answer
2–3 questions the student must answer in a comment at the top of their file.
These should probe *why* the design was made, not just *what* it does. Always
tie one question back to the original Velox source by referencing a specific
file or line range.

## Quality Bar

- Each exercise must be independently compilable — no cross-file dependencies
  between exercises 1–N (only the capstone can depend on earlier exercises).
- Exercises must be ordered so that each one uses concepts from the previous
  one. The student should feel progressive mastery, not random jumps.
- The "what to build" must be realistic: the student builds a stripped-down but
  functionally correct version of the real Velox component, not a toy.
- Quantitative tests are better than qualitative ones. Prefer `assert(x == 8 << 20)`
  over `assert(x > 0)`.
- Tests must be deterministic. Do not rely on timing, random values, or OS-
  specific behavior unless the exercise is explicitly about concurrency (and
  even then, use `std::atomic` and well-defined sync points).
- Questions must reference the actual Velox source file (e.g. `MemoryPool.h:494`)
  and ask the student to reason about a specific design decision, not just
  describe behavior.
- The capstone must integrate all prior exercises. It should be hard enough
  that completing it means the student genuinely understands the full module.

## Tone

- Write for a C++ programmer who understands the language well but has not
  read Velox's source before.
- Do not explain C++ basics. Explain Velox-specific design decisions.
- Prefer concrete numbers and file references over abstract descriptions.
