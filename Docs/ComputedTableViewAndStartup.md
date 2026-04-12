# Computed Table View And SQLite Startup

This document records the completion of the previously open repository gaps around:

- computed-column integration closure
- SQLite startup migration / recovery / diagnostics
- index materialization

## Computed Table View

Implemented in:

- `Include/StableCore/Storage/TableView.h`
- `Src/Computed/TableView.cpp`

Capabilities:

- combines fact columns and computed columns in one runtime view
- enumerates underlying storage records
- reads fact cells directly from storage
- evaluates computed cells on demand
- caches computed results by `recordId + columnName + version`
- invalidates computed cache automatically via `IDatabaseObserver`

Supported computed kinds:

- `Expression`
- `Rule`
- `Aggregate`

Aggregate baseline:

- relation traversal via `IComputedContext::GetRelated(...)`
- count / sum / min / max
- relation descriptor format: `TargetTable.RelationField`

## SQLite Startup Flow

Implemented in:

- `Src/Sqlite/SqliteAdapter.cpp`

Startup behavior:

1. open SQLite database
2. initialize physical schema if missing
3. load metadata
4. run migration plan if `schema_version` is behind
5. detect dirty shutdown through `clean_shutdown`
6. run `PRAGMA integrity_check` on dirty startup
7. load tables and journal stacks
8. materialize indexes declared by schema metadata
9. mark current session as unclean until clean destruction

Metadata keys:

- `version`
- `next_record_id`
- `schema_version`
- `clean_shutdown`

Diagnostics persistence:

- SQLite startup diagnostics are inserted into `startup_diagnostics`
- health-report building can now query database-provided diagnostics through `IDatabaseDiagnosticsProvider`

## Index Materialization

Repository baseline now includes:

- default physical lookup indexes
- runtime creation of per-column field-SCValue indexes for columns with `indexed=true`
- add-column-time index creation for persisted SQLite schema changes
