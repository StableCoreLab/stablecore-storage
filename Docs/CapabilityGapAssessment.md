# Capability Gap Assessment

This document evaluates the current storage system against the design intent in:

- `Docs/Roadmap.md`
- `Docs/TaskBreakdown.md`
- `Docs/V1BaselineDecisions.md`

Assessment date:

- `2026-03-25`

Assessment method:

- repository artifact review only
- no compile
- no runtime verification in this pass

## Status Model

This assessment uses three states:

- `Implemented`: repository contains a concrete code path with mostly closed semantics
- `Modeled But Not Connected`: public types, helper modules, or partial runtime exist, but they are not connected into the main product-facing storage flow
- `Not Completed`: design intent exists, but repository does not yet contain the required runtime capability

## Executive Conclusion

Current status is not equivalent to "`M3` completed".

The repository is closer to:

- `M1` substantially implemented
- `M2` substantially implemented at code level
- `M3` partially implemented, with several critical gaps still open

Most precise judgment:

- the storage system is a viable technical base
- it is not yet a fully product-ready storage subsystem as defined by the design documents

## M1 Assessment

### Public V1 baseline

- `Implemented`

Covered by:

- `Include/StableCore/Storage/Types.h`
- `Include/StableCore/Storage/Interfaces.h`
- `Include/StableCore/Storage/Storage.h`

### Value system

- `Implemented`

Covered by:

- `Value`
- typed accessors
- `Null` semantics
- `RecordId` / `Enum` access

### Transaction semantics

- `Implemented`

Covered by:

- `Src/Memory/MemoryDatabase.cpp`

### Delete / tombstone / undo-redo baseline

- `Implemented`

Covered by:

- stable `recordId`
- deleted record handles
- undo/redo restore behavior

### ChangeSet baseline

- `Implemented`

Covered by:

- `ChangeSet`
- `ChangeSource`
- `RelationUpdated`

### Minimum query baseline

- `Implemented`

Covered by:

- `GetRecord`
- `EnumerateRecords`
- `FindRecords`

### M1 tests and docs

- `Implemented`

Covered by:

- `Tests/M1Tests.cpp`
- `Examples/MemoryExample.cpp`
- `README.md`

## M2 Assessment

### SQLite schema and adapter

- `Implemented`

Covered by:

- physical metadata tables
- SQLite thin wrapper classes
- metadata load/open path

### Persisted schema management

- `Implemented`

Covered by:

- persisted `schema_columns`
- column load and save

### Persisted writes and reads

- `Implemented`

Covered by:

- record creation
- field writes
- delete
- query
- relation write validation

### SQLite transaction alignment

- `Implemented`

Covered by:

- edit session aligned with SQLite transaction lifecycle

### Journal persistence and undo-redo

- `Implemented`

Covered by:

- persisted journal transaction rows
- persisted journal entry rows
- undo/redo stack restoration

### SQLite tests

- `Implemented`

Covered by:

- `Tests/M2SqliteTests.cpp`

### M2 residual caveat

- runtime verification not performed in this pass

This is a verification gap, not a repository-structure gap.

## M3 Assessment

### 1. Typical business relations

#### Floor / Beam / FloorRef baseline

- `Implemented`

Covered by:

- `Tests/M3Tests.cpp`

Included behaviors:

- relation field validation
- query beams by floor
- `RelationUpdated`
- reject deleting referenced target
- allow delete after references are moved away

#### Assessment

- this portion of `M3` is the strongest completed area

### 2. Computed column system

#### Computed column definition model

- `Implemented`

Covered by:

- `ComputedColumnDef`
- `ComputedFieldKind`
- dependency declarations
- fact/computed layer marker

Files:

- `Include/StableCore/Storage/Types.h`

#### Minimal expression evaluator

- `Implemented`

Covered by:

- field reference
- constants
- arithmetic
- parentheses
- built-in functions

Files:

- `Include/StableCore/Storage/Computed.h`
- `Src/Computed/ComputedRuntime.cpp`

#### `ruleId` registry mechanism

- `Implemented`

Files:

- `Include/StableCore/Storage/Computed.h`
- `Src/Computed/ComputedRuntime.cpp`

#### ChangeSet + Version driven invalidation and recomputation

- `Modeled But Not Connected`

Reason:

- computed cache exists
- dependency matching exists
- invalidation helper exists
- but there is no automatic connection from database observer flow to a product-facing computed-column pipeline
- there is no mainline record/table read path that transparently serves computed columns

#### Fact-column / computed-column table layering

- `Modeled But Not Connected`

Reason:

- type model exists
- example usage exists
- but no formal `TableView`, column collection runtime, or product-facing composition layer exists in the repository

#### Aggregate computed columns

- `Modeled But Not Connected`

Reason:

- design direction is present
- runtime path still reuses generic `ruleId` evaluator pattern
- no dedicated aggregate execution flow is present

#### Assessment

- computed-column work is beyond pure design stage
- but it is not yet integrated enough to satisfy the design documents' product-facing `M3` expectation

### 3. Batch and performance capability

#### Batch create / update / delete / import

- `Implemented`

Files:

- `Include/StableCore/Storage/Batch.h`
- `Src/Batch/BatchOperations.cpp`

#### Import transaction optimization

- `Modeled But Not Connected`

Reason:

- repository now provides one-edit-session import helper
- deeper backend-level optimization, chunking strategy, and high-volume tuning are not present

#### Large-data query baseline

- `Not Completed`

Reason:

- no benchmark suite
- no measurable query performance baseline

#### Relation query performance validation

- `Not Completed`

Reason:

- no performance-oriented relation query validation exists

#### Index strategy review and materialization

- `Not Completed`

Reason:

- SQLite metadata stores `indexed_flag`
- but repository does not materialize real SQLite indexes from schema metadata

#### Assessment

- functional batch wrappers exist
- performance work is still largely open

### 4. Engineering capability

#### Database upgrade / migration solution

- `Modeled But Not Connected`

Files:

- `Include/StableCore/Storage/Migration.h`
- `Src/Migration/Migration.cpp`

Reason:

- migration planning exists
- but there is no persisted schema version contract wired into SQLite startup
- no migration runner is connected to database open

#### Crash recovery strategy

- `Not Completed`

Reason:

- no concrete startup recovery flow
- no crash-state detection pipeline

#### Corruption handling strategy

- `Not Completed`

Reason:

- no corruption scan or repair behavior exists in runtime code

#### Debugging tools and diagnostic logs

- `Modeled But Not Connected`

Files:

- `Include/StableCore/Storage/Diagnostics.h`
- `Src/Diagnostics/Diagnostics.cpp`

Reason:

- health report and changeset description helpers exist
- but no structured logging pipeline, startup diagnostics process, or operator tooling exists

#### Product integration example

- `Implemented`

Files:

- `Examples/ProductIntegrationExample.cpp`

#### Assessment

- engineering support is currently helper-level, not production-level

## M3 Completion Standard Check

This section maps directly to the `M3` completion criteria in `Docs/Roadmap.md`.

### Product can directly integrate the SQLite backend

- `Partially Satisfied`

Why not fully satisfied:

- core storage API exists
- SQLite backend exists
- but migration, recovery, diagnostics, and performance closure are still insufficient for a true product-ready claim

### Data table tool can show both fact columns and computed columns

- `Not Satisfied`

Reason:

- computed column model exists
- evaluator exists
- but there is no actual data-table runtime that composes fact columns and computed columns into one unified product-facing view

### Typical component relations and floor relations run stably

- `Satisfied`

Reason:

- relation baseline exists in code and tests

### Performance and recovery capability reach practical development level

- `Not Satisfied`

Reason:

- no measured performance baseline
- no index materialization strategy in runtime
- no crash recovery and corruption-handling closure

## Strict Gap List

### High-priority gaps

- computed columns are not connected to a real table/view read path
- computed cache invalidation is not automatically driven through the main observer/product flow
- SQLite migration exists only as planning primitives, not as an executed upgrade pipeline
- crash recovery and corruption handling are not implemented as runtime flows
- performance baseline, index materialization, and import/query stress validation are still missing

### Medium-priority gaps

- batch import is an API wrapper, not a backend-optimized pipeline
- aggregate computed columns do not yet have a dedicated execution model
- diagnostics are helper-level and not integrated with startup checks or structured logs
- product example demonstrates concepts but is still a demo, not a reference integration kit

### Low-priority gaps

- repository status documentation should continue distinguishing “modeled” from “fully connected”
- new extension modules were not compile-verified in this assessment pass

## Final Judgment

The current system should be described as:

- a strong storage-core foundation
- with `M1` substantially complete
- with `M2` substantially complete at repository level
- with `M3` partially complete, but not yet complete by the design-document standard

The biggest remaining difference between current state and product-ready state is not basic storage semantics.

The biggest remaining difference is:

- computed-column integration closure
- engineering closure for migration / recovery / diagnostics
- measured performance closure
