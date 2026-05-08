# Module Map And Reference

## Directory Overview

Key modules:

| Module | Responsibility |
| --- | --- |
| Include | Public interfaces |
| Sqlite | SQLite storage backend |
| Query | Query planning and execution |
| Computed | Computed columns / expressions |
| Batch | Batch editing / import |
| Migration | Upgrade and compatibility |
| Diagnostics | Debugging / diagnostics |
| Tools | Auxiliary developer tools |

## Coding Rules

### Naming

| Type | Rule |
| --- | --- |
| Interface | `ISC*` |
| Implementation | `SC*` |
| Member variable | `m_` |
| Constant | `k*` |

Examples:

- `ISCStorageService`
- `SCProjectContext`
- `SCSqliteDatabase`

### General Rules

Avoid:

- unnecessary renaming
- style-only modifications
- unrelated formatting
- speculative cleanup
- cosmetic refactors

Consistency with surrounding code is preferred over style purity.

## Important Documents

- `Docs/文档索引.md`
- `Docs/当前实现状态.md`
- `Docs/CapabilityGapAssessment.md`
- `Docs/Roadmap.md`
- `Docs/V1BaselineDecisions.md`

## Quick Reference

### Truth Sources

- Project
- ChangeSet
- Journal
- Snapshot

### Never Allowed

- implicit writes
- implicit upgrades
- partial commit states
- SQLite access from upper layers
- leaking SQLite internals into public APIs
- hidden migration behavior
