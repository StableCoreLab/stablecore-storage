# Schema Edit Usage

## 1. Purpose

`SCStorage` now exposes high-level schema edit helpers for callers that need to:

- create a table from structured schema data
- apply a field-level patch to an existing table

The public entry points are:

- `CreateTableFromSchema(...)`
- `ApplyTableSchemaPatch(...)`

These APIs are declared in `Include/SCSchemaEdit.h` and re-exported by
`Include/SCStorage.h`.

## 2. Current Scope

Current supported scope is intentionally narrow:

- create table from `SCTableSchemaSnapshot`
- create-path applies `constraints` and `indexes`
- patch one table with:
  - `addColumns / updateColumns / removeColumns`
  - `addConstraints / removeConstraints`
  - `addIndexes / removeIndexes`

Current non-goals:

- no DSL parsing
- no multi-table orchestration
- no constraint/index update-in-place

Low-level schema primitives that are now available on `ISCSchema`:

- `AddConstraint(...)`
- `RemoveConstraint(...)`
- `AddIndex(...)`
- `RemoveIndex(...)`
- `GetConstraintCount(...) / GetConstraint(...) / FindConstraint(...)`
- `GetIndexCount(...) / GetIndex(...) / FindIndex(...)`

## 3. Transaction Contract

Both helpers are explicit write operations.

Behavior contract:

- each helper opens one edit session internally
- all steps must succeed before `Commit`
- any failure triggers rollback of schema journal changes
- create-table failures also delete the newly created table if table creation
  succeeded before a later step failed

This preserves the repository requirement that schema edits must not leave
partial success states visible to callers.

## 4. Minimal Create Example

```cpp
#include "SCStorage.h"

using namespace StableCore::Storage;

SCDbPtr db;
CreateFileDatabase(L"example.sqlite", SCOpenDatabaseOptions{}, db);

SCTableSchemaSnapshot schema;
schema.table.name = L"Beam";

SCColumnDef width;
width.name = L"Width";
width.displayName = L"Width";
width.valueKind = ValueKind::Int64;
width.nullable = false;
width.defaultValue = SCValue::FromInt64(0);
schema.columns.push_back(width);

SCColumnDef name;
name.name = L"Name";
name.displayName = L"Name";
name.valueKind = ValueKind::String;
name.nullable = true;
schema.columns.push_back(name);

SCConstraintDef widthUnique;
widthUnique.kind = SCConstraintKind::Unique;
widthUnique.name = L"uq_Beam_Width";
widthUnique.columns.push_back(L"Width");
schema.constraints.push_back(widthUnique);

SCIndexDef nameIndex;
nameIndex.name = L"idx_Beam_Name";
nameIndex.columns.push_back(SCIndexColumnDef{L"Name", false});
schema.indexes.push_back(nameIndex);

SCSchemaEditResult result;
ErrorCode rc = CreateTableFromSchema(db.Get(), schema, &result);
```

## 5. Minimal Patch Example

```cpp
#include "SCStorage.h"

using namespace StableCore::Storage;

SCTableSchemaPatch patch;
patch.tableName = L"Beam";

SCColumnDef width;
width.name = L"Width";
width.displayName = L"Width Label";
width.valueKind = ValueKind::String;
width.nullable = false;
width.defaultValue = SCValue::FromString(L"0");
patch.updateColumns.push_back(width);

SCColumnDef height;
height.name = L"Height";
height.displayName = L"Height";
height.valueKind = ValueKind::Int64;
height.nullable = false;
height.defaultValue = SCValue::FromInt64(0);
patch.addColumns.push_back(height);

patch.removeIndexes.push_back(L"idx_Beam_Name");

SCConstraintDef widthUnique;
widthUnique.kind = SCConstraintKind::Unique;
widthUnique.name = L"uq_Beam_Width";
widthUnique.columns.push_back(L"Width");
patch.addConstraints.push_back(widthUnique);

SCIndexDef heightIndex;
heightIndex.name = L"idx_Beam_Height";
heightIndex.columns.push_back(SCIndexColumnDef{L"Height", false});
patch.addIndexes.push_back(heightIndex);

SCSchemaEditResult result;
ErrorCode rc = ApplyTableSchemaPatch(db.Get(), patch, &result);
```

## 6. Example Program

See:

- `Examples/SchemaEditExample.cpp`

The example shows:

- creating a database
- creating a table with `CreateTableFromSchema(...)`
- patching the table with `ApplyTableSchemaPatch(...)`
- reading back the final schema

## 7. Validation Notes

The helpers currently validate:

- non-null database pointer
- non-empty table name
- non-empty column names
- no duplicate column names within a request
- no conflicting names across patch sections
- `SCColumnDef` shape compatibility with current backend rules

Important examples:

- relation columns must use `ValueKind::RecordId`
- relation columns must set `referenceTable`
- relation default values must also be `RecordId`
- fact columns must not carry `referenceTable`
- non-null default values must match `valueKind`

## 8. Constraints and Indexes

`CreateTableFromSchema(...)` now applies `constraints` and `indexes` after all
columns are successfully added.

At the storage primitive level, callers can already edit them directly through
`ISCSchema`.

`ApplyTableSchemaPatch(...)` accepts add/remove constraint/index patch sets,
but it still does not support update-in-place for constraints or indexes.

Design direction is documented in:

- `Docs/SCStorageSchemaEdit接口草案.md`

Callers that need to modify an existing constraint or index should currently
express that as `remove + add` within one patch request.
