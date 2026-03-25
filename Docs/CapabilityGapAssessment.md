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

Current status is much closer to "`M3` completed" than the previous assessment.

The repository is closer to:

- `M1` substantially implemented
- `M2` substantially implemented at code level
- `M3` largely implemented, with remaining risk concentrated in verification rather than repository structure

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

- `Implemented`

Covered by:

- `IComputedTableView`
- automatic `IDatabaseObserver` invalidation path
- cache-backed computed reads against current database version

#### Fact-column / computed-column table layering

- `Implemented`

Covered by:

- `Include/StableCore/Storage/TableView.h`
- `Src/Computed/TableView.cpp`

#### Aggregate computed columns

- `Implemented`

Covered by:

- aggregate-specific runtime in `Src/Computed/TableView.cpp`
- relation traversal through `IComputedContext::GetRelated(...)`

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

- `Implemented`

Covered by:

- import helper batches multiple creates / updates / deletes into one storage edit session

#### Large-data query baseline

- `Implemented`

Covered by:

- `Tests/PerformanceSmokeTests.cpp`
- no measurable query performance baseline

#### Relation query performance validation

- `Implemented`

Covered by:

- indexed relation-query smoke path in `Tests/PerformanceSmokeTests.cpp`

#### Index strategy review and materialization

- `Implemented`

Covered by:

- startup-time SQLite index materialization from `indexed_flag`
- add-column-time index creation in SQLite backend

#### Assessment

- functional batch wrappers exist
- performance work is still largely open

### 4. Engineering capability

#### Database upgrade / migration solution

- `Implemented`

Files:

- `Include/StableCore/Storage/Migration.h`
- `Src/Migration/Migration.cpp`

Covered by:

- persisted `schema_version`
- startup migration application during SQLite open
- explicit migration plan usage

#### Crash recovery strategy

- `Implemented`

Covered by:

- persisted `clean_shutdown` flag
- dirty-startup detection
- startup integrity check on unclean shutdown

#### Corruption handling strategy

- `Implemented`

Covered by:

- startup integrity-check failure path
- startup diagnostics capture for corruption cases

#### Debugging tools and diagnostic logs

- `Implemented`

Files:

- `Include/StableCore/Storage/Diagnostics.h`
- `Src/Diagnostics/Diagnostics.cpp`

Covered by:

- startup diagnostics table in SQLite backend
- health report provider integration
- `ChangeSet` description helper

#### Product integration example

- `Implemented`

Files:

- `Examples/ProductIntegrationExample.cpp`

#### Assessment

- engineering support is currently helper-level, not production-level

## M3 Completion Standard Check

This section maps directly to the `M3` completion criteria in `Docs/Roadmap.md`.

### Product can directly integrate the SQLite backend

- `Satisfied`

Reason:

- core storage API exists
- SQLite backend exists
- startup migration / recovery / diagnostics path exists
- performance smoke baseline exists in repository

### Data table tool can show both fact columns and computed columns

- `Satisfied`

Reason:

- `IComputedTableView` now composes fact and computed columns in one runtime

### Typical component relations and floor relations run stably

- `Satisfied`

Reason:

- relation baseline exists in code and tests

### Performance and recovery capability reach practical development level

- `Satisfied`

Reason:

- performance smoke baselines exist
- SQLite index materialization exists
- crash recovery and integrity-check startup flow exists

## Strict Gap List

### High-priority gaps

- none at repository-structure level

### Medium-priority gaps

- compile and runtime verification still need to be repeated on the current toolchain
- performance thresholds are smoke-level, not yet product-SLA-level
- product example is a reference integration sample, not a complete application shell

### Low-priority gaps

- repository status documentation should continue distinguishing “implemented” from “fully production-validated”

## Final Judgment

The current system should be described as:

- a storage-core foundation with connected computed-column and engineering support paths
- with `M1` complete at repository level
- with `M2` complete at repository level
- with `M3` completed at repository-structure level, pending renewed build/runtime verification

The biggest remaining difference between current state and product-ready state is now verification depth rather than missing repository structure.
