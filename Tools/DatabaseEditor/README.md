# StableCore Database Editor

`StableCore Database Editor` is the Qt Widgets desktop tool for working with `SCStorage` SQLite databases.

Current capabilities:

- Create and open SQLite databases
- Create tables from a table name or from `SC_SCHEMA_TABLE(...)` schema text
- Delete tables from the object explorer or Table menu
- Add fact columns and relation columns
- Browse, add, delete, and edit records
- Inspect schema, selected records, computed columns, and relation bindings
- Undo and redo schema/data edits
- Export and import CSV
- Export debug packages and backup copies

## Build Requirements

- CMake 3.24+
- Visual Studio 2022 or another C++20 compiler
- Qt 6.8 Widgets development package

The database editor is not built by default. Enable it explicitly:

```powershell
cmake -S Storage -B Build\StorageDbEditor `
  -DSCSTORAGE_BUILD_DB_EDITOR=ON `
  -DSCSTORAGE_BUILD_TESTS=OFF `
  -DCMAKE_PREFIX_PATH=C:\Qt\6.8.0\msvc2022_64
cmake --build Build\StorageDbEditor --config Release --target SCStorageDatabaseEditor
```

If CMake cannot find `Qt6Config.cmake`, verify:

- Qt 6.8 is installed
- `CMAKE_PREFIX_PATH` points to the Qt install prefix
- or `Qt6_DIR` is set explicitly

## Launch

After a successful build:

```powershell
Build\StorageDbEditor\Release\SCStorageDatabaseEditor.exe
```

## Table Creation

### Create by name

Use `Table -> Create Table...` to create an empty table by name.

### Create from schema text

Use `Table -> Create Table From Schema...` and paste text like:

```cpp
SC_SCHEMA_TABLE(ProjectInfo)
{
    Table("ProjectInfo")
        .Column("GUID", SCType::String)
        .Column("ProjectName", SCType::String)
            .Description("工程名称")
        .Column("ProjectCode", SCType::String)
            .Description("工程编号");
}
```

This feature creates the table and its columns from the schema description. Column descriptions are imported as display names, and default values are imported when the DSL provides them. Table descriptions and primary keys are treated as import hints unless the underlying storage model can persist them.

## Table Deletion

`Delete Selected Table` removes the chosen table explicitly.

- The operation is treated as destructive and is not currently undoable.
- Use it only after confirming the table is no longer needed.
- See [Docs/DatabaseEditorTableDeletionSemantics.md](../../Docs/DatabaseEditorTableDeletionSemantics.md) for the current behavior contract.

## Related Files

- `Tools/DatabaseEditor/SCDatabaseEditorMainWindow.cpp`
- `Tools/DatabaseEditor/SCDatabaseSession.cpp`
- `Tools/DatabaseEditor/SCSchemaTableImport.cpp`
- `Tools/DatabaseEditor/SCSchemaTableImportDialog.cpp`
