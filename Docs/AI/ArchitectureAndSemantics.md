# Architecture And Semantics

## Repository Identity

- Repository: `stablecore-storage`
- Role: Core SQLite-based storage infrastructure for the quantity/takeoff system

## Primary Responsibilities

- SQLite project persistence
- Undo / Redo
- Transaction semantics
- ChangeSet / Journal / Snapshot / Replay
- Recoverable editing
- Upgrade / migration infrastructure

## Core Architecture (P0)

```text
Core (semantic rules)
    -> Adapter (public interface bridge)
    -> Backend (SQLite)
```

Mandatory constraints:

- Upper layers must NOT access SQLite directly
- Query / Computed modules must NOT bypass public interfaces
- Backend modules must NOT contain business semantics
- Public storage semantics must remain independent from SQLite implementation details

## Storage Semantic Invariants (P0)

The following semantics are invariant and must NEVER be violated:

- `open()` must not mutate data
- read operations must not write
- upgrades must be explicit
- rollback must fully restore state
- deleted records must never revive
- Undo / Redo must preserve identity consistency
- transactions must not leave partial success states
- SQLite implementation details must not leak into public APIs

Correctness is always higher priority than performance.

## Core Design Principles

### Explicit State Transitions

All state mutations must happen through explicit APIs:

- BeginEdit
- Commit
- Rollback
- Open
- Upgrade
- Finalize
- Abort

Implicit state mutation is forbidden.

### Single Source of Truth

Persistent truth sources are limited to:

- Project
- ChangeSet
- Journal
- Snapshot

UI state, cache, and session objects are NOT truth sources.

### Recoverability First

All write paths must support:

- rollback
- recovery
- interruption safety

Must handle:

- interrupted import
- failed migration
- rollback failure
- deleted-record access
- interrupted SQLite transaction
- corrupted or incomplete journal

### Backend Isolation

SQLite is an implementation detail.

Rules:

- public APIs must remain storage-semantic
- SQL details must not leak into upper layers
- schema changes must go through explicit migration logic
- storage behavior must not depend on UI state
- SQLite schema must not become a public contract

### Minimal Surprise Principle

The system must preserve intuitive semantics:

- reads do not write
- deleted data does not revive
- readonly mode does not mutate data
- failed operations do not partially commit
- opening a project does not upgrade it implicitly
- closing a project does not silently commit unfinished edits

## Engineering Philosophy

Priority order:

```text
Correctness
    > Recoverability
        > Storage Semantic Stability
            > Maintainability
                > Performance
```

AI is expected to behave as:

> A constrained engineering executor,
> not an autonomous redesign agent.
