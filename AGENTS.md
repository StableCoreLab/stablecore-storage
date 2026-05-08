# AGENTS.md

# 1. Repository Identity

Repository: `stablecore-storage`

Role:

> Core SQLite-based storage infrastructure for the quantity/takeoff system.

Primary responsibilities:

- SQLite project persistence
- Undo / Redo
- Transaction semantics
- ChangeSet / Journal / Snapshot / Replay
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
Adapter (public interface bridge)
    ↓
Backend (SQLite)
````

Mandatory constraints:

* Upper layers must NOT access SQLite directly
* Query / Computed modules must NOT bypass public interfaces
* Backend modules must NOT contain business semantics
* Public storage semantics must remain independent from SQLite implementation details

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
* SQLite implementation details must not leak into public APIs

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
* Snapshot

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
* interrupted SQLite transaction
* corrupted or incomplete journal

---

## 7.4 Backend Isolation

SQLite is an implementation detail.

Rules:

* public APIs must remain storage-semantic
* SQL details must not leak into upper layers
* schema changes must go through explicit migration logic
* storage behavior must not depend on UI state
* SQLite schema must not become a public contract

---

## 7.5 Minimal Surprise Principle

The system must preserve intuitive semantics:

* reads do not write
* deleted data does not revive
* readonly mode does not mutate data
* failed operations do not partially commit
* opening a project does not upgrade it implicitly
* closing a project does not silently commit unfinished edits

---

# 8. Common Failure Patterns

Avoid the following common architectural violations:

* accessing SQLite directly from upper layers
* embedding business rules inside backend code
* bypassing transaction boundaries
* implicit writes during reads
* hidden upgrade paths
* using cache state as persistence truth
* leaking SQLite concepts into public interfaces
* mixing project lifecycle with UI workflow

---

# 9. Directory Overview

Key modules:

| Module      | Responsibility                 |
| ----------- | ------------------------------ |
| Include     | Public interfaces              |
| Sqlite      | SQLite storage backend         |
| Query       | Query planning and execution   |
| Computed    | Computed columns / expressions |
| Batch       | Batch editing / import         |
| Migration   | Upgrade and compatibility      |
| Diagnostics | Debugging / diagnostics        |
| Tools       | Auxiliary developer tools      |

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
* `SCSqliteDatabase`

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
* readonly open
* failed upgrade recovery
* journal recovery

AI may modify tests but must not execute them.

---

# 12. Documentation Rules

Documentation updates are required only when:

* architecture changes
* public APIs change
* storage semantics change
* migration workflow changes
* replay / journal behavior changes

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
2. Understand storage semantic constraints
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

* storage semantic risks
* migration risks
* rollback risks
* SQLite persistence risks
* uncovered paths

## 14.4 Suggested Next Steps

* most reasonable follow-up tasks

---

# 15. Engineering Philosophy

This repository prioritizes:

1. Semantic correctness
2. Recoverability
3. Storage semantic stability
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
* Snapshot

---

## Never Allowed

* implicit writes
* implicit upgrades
* partial commit states
* SQLite access from upper layers
* leaking SQLite internals into public APIs
* hidden migration behavior

---

## Priority Order

```text
Correctness
    > Recoverability
        > Storage Semantic Stability
            > Maintainability
                > Performance
```
