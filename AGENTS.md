# AGENTS.md

# 1. Repository Identity

Repository: `stablecore-storage`

Role:

> Core storage infrastructure for the quantity/takeoff system.

Primary responsibilities:

- Undo / Redo
- Transaction semantics
- Journal / Snapshot / Replay
- Persistent storage
- Recoverable editing
- Upgrade / migration infrastructure

---

# 2. Tech Stack

- Language: C++20
- Build: CMake
- Database: SQLite
- Test: GoogleTest
- Compiler: MSVC VS2022

Cross-platform compatibility is a long-term requirement.

---

# 3. Core Architecture (P0)

The repository follows a strict layered architecture:

```text
Core (semantic rules)
    ↓
Adapter (abstraction bridge)
    ↓
Backend (Memory / SQLite)
````

Mandatory constraints:

* Upper layers must NOT access SQLite directly
* Query / Computed modules must NOT bypass interfaces
* Backend modules must NOT contain business semantics
* Memory and SQLite behavior must remain semantically identical

---

# 4. Storage Semantic Invariants (P0)

The following semantics are invariant and must NEVER be violated:

* `open()` must not mutate data
* read operations must not write
* upgrades must be explicit
* rollback must fully restore state
* deleted records must never revive
* Undo / Redo must preserve identity consistency
* transactions must not leave partial success states
* Memory and SQLite backends must behave identically

Correctness is always higher priority than performance.

---

# 5. AI Execution Boundary (P0)

AI operates in:

> Offline Code Generation Mode

AI may:

* modify source code
* modify tests
* modify CMake
* update documentation
* perform static reasoning and analysis

AI must NOT:

* compile
* run executables
* run tests
* execute scripts
* invoke build systems
* modify generated build artifacts
* validate behavior through execution

Code correctness must be determined through:

* static reasoning
* semantic consistency
* architecture constraints
* existing code patterns

---

# 6. Modification Scope Rules (P0)

Changes must remain strictly localized to the requested scope.

AI must NOT:

* refactor unrelated modules
* rename symbols without necessity
* rewrite stable implementations
* move files across modules
* perform repository-wide formatting
* modify unrelated behavior
* introduce speculative abstractions

Prefer:

* extension over rewrite
* local fixes over global refactors
* adapters over duplication
* incremental evolution over redesign

Large-scale redesign is forbidden unless explicitly requested.

---

# 7. Core Design Principles

## 7.1 Explicit State Transitions

All state mutations must happen through explicit APIs:

* BeginEdit
* Commit
* Rollback
* Open
* Upgrade
* Finalize
* Abort

Implicit state mutation is forbidden.

---

## 7.2 Single Source of Truth

Persistent truth sources are limited to:

* Project
* ChangeSet
* Journal

UI state, cache, and session objects are NOT truth sources.

---

## 7.3 Recoverability First

All write paths must support:

* rollback
* recovery
* interruption safety

Must handle:

* interrupted import
* failed migration
* rollback failure
* deleted-record access

---

## 7.4 Backend Consistency

Memory and SQLite implementations must expose identical semantics.

Backend-specific semantic branches are forbidden.

---

## 7.5 Minimal Surprise Principle

The system must preserve intuitive semantics:

* reads do not write
* deleted data does not revive
* readonly mode does not mutate data
* failed operations do not partially commit

---

# 8. Common Failure Patterns

Avoid the following common architectural violations:

* accessing SQLite directly from upper layers
* embedding business rules inside backend code
* bypassing transaction boundaries
* backend-specific behavior divergence
* implicit writes during reads
* hidden upgrade paths
* using cache state as persistence truth

---

# 9. Directory Overview

Key modules:

| Module      | Responsibility                 |
| ----------- | ------------------------------ |
| Include     | Public interfaces              |
| Memory      | Semantic execution / UndoRedo  |
| Sqlite      | Persistent backend             |
| Query       | Query planning and execution   |
| Computed    | Computed columns / expressions |
| Batch       | Batch editing / import         |
| Migration   | Upgrade and compatibility      |
| Diagnostics | Debugging / diagnostics        |

---

# 10. Coding Rules

## Naming

| Type            | Rule   |
| --------------- | ------ |
| Interface       | `ISC*` |
| Implementation  | `SC*`  |
| Member variable | `m_`   |
| Constant        | `k*`   |

Examples:

* `ISCStorageService`
* `SCProjectContext`

---

## General Rules

Avoid:

* unnecessary renaming
* style-only modifications
* unrelated formatting
* speculative cleanup
* cosmetic refactors

Consistency with surrounding code is preferred over style purity.

---

# 11. Testing Rules

* GoogleTest is mandatory
* New behavior requires tests
* Bug fixes require regression tests

Priority test areas:

* rollback recovery
* deleted-record access
* interrupted import
* migration failure
* undo/redo consistency
* transaction recovery

AI may modify tests but must not execute them.

---

# 12. Documentation Rules

Documentation updates are required only when:

* architecture changes
* public APIs change
* semantics change
* workflow behavior changes

Avoid unrelated documentation rewrites.

---

## Important Documents

Important references include:

* Docs/文档索引.md
* Docs/当前实现状态.md
* Docs/CapabilityGapAssessment.md
* Docs/Roadmap.md
* Docs/V1BaselineDecisions.md

---

# 13. Task Workflow

Every task should follow this order:

1. Analyze scope and boundaries
2. Understand semantic constraints
3. Implement minimal localized changes
4. Add or update tests
5. Perform static self-review
6. Summarize risks and remaining gaps

---

# 14. Required Output

Task summaries should include:

## 14.1 Change Summary

* modified modules
* implemented behavior
* semantic impact

## 14.2 Test Coverage

* affected scenarios
* boundary conditions covered
* regression coverage

## 14.3 Risk Assessment

* semantic risks
* migration risks
* rollback risks
* uncovered paths

## 14.4 Suggested Next Steps

* most reasonable follow-up tasks

---

# 15. Engineering Philosophy

This repository prioritizes:

1. Semantic correctness
2. Recoverability
3. Backend consistency
4. Explicit state transitions
5. Incremental evolution
6. Long-term maintainability

AI is expected to behave as:

> A constrained engineering executor,
> not an autonomous redesign agent.

---

# 16. Quick Reference

## Truth Sources

* Project
* ChangeSet
* Journal

---

## Never Allowed

* implicit writes
* implicit upgrades
* backend semantic divergence
* partial commit states
* SQLite access from upper layers

---

## Priority Order

```text
Correctness
    > Recoverability
        > Consistency
            > Maintainability
                > Performance
```


