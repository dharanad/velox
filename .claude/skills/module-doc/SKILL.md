---
name: module-doc
description: Generate a detailed design document for a Velox module. Reads the module's headers, understands its architecture and design, and writes a doc to velox/docs/designs/. Use when the user types "/module-doc <module-path>" or asks to document a module.
---

# Velox Module Documentation Skill

Generate a comprehensive design document for a given Velox module and write it
to `velox/docs/designs/<module-name>.md`.

## Argument

The user provides a module path, e.g. `/module-doc velox/common/memory` or
`/module-doc velox/exec`. Derive the output filename from the last path
component (e.g. `memory-module.md`, `exec-module.md`).

## Steps

1. **List the module directory** to find all `.h` files. Prioritize files that
   match the module name (e.g. `Memory.h`, `MemoryPool.h`) as the main entry
   points.

2. **Read the key headers** in parallel — start with the files named after the
   module itself, then read the most structurally significant ones (interfaces,
   base classes, allocators, managers). Read implementation `.cpp` files only
   when behavior is not clear from headers.

3. **Identify the architecture** from what you read:
   - What are the main abstractions (classes, interfaces)?
   - How do they relate (inheritance, ownership, lifecycle)?
   - What are the allocation / resource management strategies?
   - What concurrency model is used (atomics, mutexes, thread-local)?
   - What are the key data structures and why were they chosen?

4. **Write the document** to `velox/docs/designs/<module>-module.md`. If the
   file already exists, read it first and update rather than overwrite.

## Document Structure

Use this structure (adjust sections as appropriate for the module):

```
# <Module Name> Module

## Overview
One paragraph: what the module does and why it exists.

## Architecture
Layer diagram or component list with one-line descriptions.

## Layer 1: <Top-level Component>
### Responsibilities
### Key Configuration
### How It Works

## Layer 2: <Next Component>
...

## Key Data Structures
Table or narrative of the most important types.

## Design Summary
Table of key design decisions and their rationale.
```

## Quality Bar

- Reference exact file paths and line numbers for non-obvious behaviors.
- Explain *why* design choices were made, not just *what* they are.
- Include ASCII diagrams for layered or hierarchical structures.
- For allocation strategies, show the memory layout or flow explicitly.
- Do not copy-paste comments verbatim; synthesize and explain.
- Keep the document self-contained — a reader who has not seen the code should
  understand the design after reading it.
