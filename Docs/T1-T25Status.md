# T1-T25 Status

This file audits `Docs/TaskBreakdown.md` tasks `T1` through `T25` against the repository state as of 2026-03-25.

## Summary

Completed tasks:

- `T1` public API baseline
- `T2` `SCValue` typed access baseline
- `T3` string lifetime and `GetStringCopy`
- `T4` consistent core error behavior baseline
- `T5` in-memory transaction lifecycle baseline
- `T6` delete/tombstone/undo baseline
- `T7` journal aggregation baseline
- `T8` `ChangeSet` generation baseline
- `T9` schema validation baseline
- `T10` minimum query baseline
- `T11` unit test project baseline
- `T12` M1 core regression tests baseline
- `T13` minimum example and onboarding docs baseline
- `T14` SQLite physical schema baseline
- `T15` SQLite thin adapter baseline
- `T16` database initialization and metadata loading
- `T17` persisted schema management baseline
- `T18` persisted record write path baseline
- `T19` SQLite transaction alignment baseline
- `T20` SQLite read/query baseline
- `T21` persisted journal baseline
- `T22` SQLite undo/redo baseline
- `T23` SQLite integration test baseline
- `T24` floor relation business baseline
- `T25` computed-column metadata model baseline

## Notes By Task

### T9

This audit found one remaining gap before this update: relation columns did not require `referenceTable`, and fact columns could incorrectly carry `referenceTable`.

This is now enforced in both backends:

- `Src/Memory/MemoryDatabase.cpp`
- `Src/Sqlite/SqliteAdapter.cpp`

### T10 / T12 / T23

This audit also found coverage gaps around:

- empty query result behavior
- transaction-external write rejection
- SQLite schema validation parity

These are now covered by tests in:

- `Tests/M1Tests.cpp`
- `Tests/M2SqliteTests.cpp`

### T24

The business baseline now explicitly covers:

- `Floor` + `Beam` sample tables
- `FloorRef` relation writes
- query by floor
- `RelationUpdated` observation
- delete rejection for referenced targets
- post-rebind delete success

See:

- `Tests/M3Tests.cpp`

### T25

Before this update, the computed-column model only existed in `Docs/V1BaselineDecisions.md`.

It is now promoted into the public type system via:

- `ComputedFieldKind`
- `TableColumnLayer`
- `FieldDependency`
- `ComputedDependencySet`
- `ComputedColumnDef`

These types intentionally stop at metadata modeling. Expression evaluation, `ruleId` registry, and cache invalidation stay in later tasks (`T26` to `T28`).
