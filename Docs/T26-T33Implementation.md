# T26-T33 Implementation Notes

This document records the repository-level implementation decisions for tasks `T26` through `T33`.

## T26 Minimal Expression Evaluator

Implemented in:

- `Include/StableCore/Storage/Computed.h`
- `Src/Computed/ComputedRuntime.cpp`

Scope:

- field reference
- numeric constants
- `+ - * /`
- parentheses
- built-in functions: `min`, `max`, `abs`, `if`

Deliberate boundary:

- no scripting language
- no arbitrary cross-table query expressions
- no dynamic code execution

## T27 `ruleId` Registry

Implemented in:

- `Include/StableCore/Storage/Computed.h`
- `Src/Computed/ComputedRuntime.cpp`

Model:

- `IRuleRegistry`
- `IComputedEvaluator`
- `EvaluateComputedColumn(...)`

Usage model:

- simple columns use `ComputedFieldKind::Expression`
- complex rules use `ComputedFieldKind::Rule`
- aggregate rules also reuse registered evaluators in V1

## T28 Computed Cache And Invalidation

Implemented in:

- `Include/StableCore/Storage/Computed.h`
- `Src/Computed/ComputedRuntime.cpp`

Cache key:

- `recordId + columnName + version`

Invalidation source:

- `ChangeSet`
- explicit dependency declarations in `ComputedDependencySet`

V1 decision:

- computed cache stays outside fact storage
- cache invalidation failure must not corrupt fact data

## T29 Batch Write And Import

Implemented in:

- `Include/StableCore/Storage/Batch.h`
- `Src/Batch/BatchOperations.cpp`

Capabilities:

- batch create
- batch update
- batch delete
- import as one edit session

Optimization baseline:

- one storage edit for one import batch
- caller prepares grouped table requests ahead of time
- relation validation still flows through storage semantics

## T30 Performance And Index Strategy

Code baseline:

- batch editing helper reduces edit-session churn
- existing SQLite schema keeps `indexed_flag` metadata for future physical index materialization

Repository decision for this stage:

- keep the performance contract documented instead of hardcoding speculative indexes
- benchmark and index materialization stay data-driven and can be added without changing public APIs

Recommended follow-up:

1. materialize SQLite indexes from `schema_columns.indexed_flag`
2. add import-size-sensitive transaction chunking
3. record query hot paths before widening the query DSL

## T31 Database Upgrade And Migration

Implemented in:

- `Include/StableCore/Storage/Migration.h`
- `Src/Migration/Migration.cpp`

V1 baseline:

- linear migration planning
- explicit `(fromVersion -> toVersion)` steps
- no hidden auto-upgrade behavior

## T32 Recovery And Diagnostics

Implemented in:

- `Include/StableCore/Storage/Diagnostics.h`
- `Src/Diagnostics/Diagnostics.cpp`

Baseline:

- health report model
- diagnostic entries
- `ChangeSet` description helper for logs and tools

Operational expectation:

- SQLite startup path should run health checks before allowing product editing
- crash recovery remains journal-first

## T33 Product Integration Example

Implemented in:

- `Examples/ProductIntegrationExample.cpp`

Demonstrates:

- floor/component relation import
- batch import helper
- computed expression column
- observer + `ChangeSet` logging
- health report generation
