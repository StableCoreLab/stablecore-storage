#include <filesystem>

#include <gtest/gtest.h>
#include <sqlite3.h>

#include "SCStorage.h"

namespace sc = StableCore::Storage;
namespace fs = std::filesystem;

namespace
{

    fs::path MakeTempDbPath(const wchar_t* fileName)
    {
        fs::path path = fs::temp_directory_path() / fileName;
        std::error_code ec;
        fs::remove(path, ec);
        return path;
    }

    sc::ErrorCode CreateFileDb(const wchar_t* fileName, sc::SCDbPtr& db)
    {
        const fs::path path = MakeTempDbPath(fileName);
        return sc::CreateFileDatabase(path.c_str(), sc::SCOpenDatabaseOptions{}, db);
    }

    bool ExecSqliteScript(const fs::path& dbPath, const char* sql)
    {
        sqlite3* db = nullptr;
        const std::string narrowPath = dbPath.string();
        if (sqlite3_open_v2(narrowPath.c_str(), &db, SQLITE_OPEN_READWRITE, nullptr) != SQLITE_OK)
        {
            if (db != nullptr)
            {
                sqlite3_close(db);
            }
            return false;
        }

        char* error = nullptr;
        const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &error);
        if (error != nullptr)
        {
            sqlite3_free(error);
        }
        sqlite3_close(db);
        return rc == SQLITE_OK;
    }

    sc::SCColumnDef MakeIntColumn(const wchar_t* name, bool nullable = false)
    {
        sc::SCColumnDef column;
        column.name = name;
        column.displayName = name;
        column.valueKind = sc::ValueKind::Int64;
        column.nullable = nullable;
        column.defaultValue = nullable ? sc::SCValue::Null() : sc::SCValue::FromInt64(0);
        return column;
    }

    sc::SCColumnDef MakeStringColumn(const wchar_t* name, bool nullable = false)
    {
        sc::SCColumnDef column;
        column.name = name;
        column.displayName = name;
        column.valueKind = sc::ValueKind::String;
        column.nullable = nullable;
        column.defaultValue = nullable ? sc::SCValue::Null() : sc::SCValue::FromString(L"");
        return column;
    }

    sc::SCConstraintDef MakeUniqueConstraint(const wchar_t* name, const wchar_t* columnName)
    {
        sc::SCConstraintDef constraint;
        constraint.kind = sc::SCConstraintKind::Unique;
        constraint.name = name;
        constraint.columns.push_back(columnName);
        return constraint;
    }

    sc::SCIndexDef MakeIndex(const wchar_t* name, const wchar_t* columnName)
    {
        sc::SCIndexDef index;
        index.name = name;
        index.columns.push_back(sc::SCIndexColumnDef{columnName, false});
        return index;
    }

    sc::SCConstraintDef MakeForeignKeyConstraint(const wchar_t* name,
                                                 const wchar_t* columnName,
                                                 const wchar_t* targetTable,
                                                 const wchar_t* targetColumn)
    {
        sc::SCConstraintDef constraint;
        constraint.kind = sc::SCConstraintKind::ForeignKey;
        constraint.name = name;
        constraint.columns.push_back(columnName);
        constraint.referencedTable = targetTable;
        constraint.referencedColumns.push_back(targetColumn);
        return constraint;
    }

    sc::SCTablePtr CreateBeamTable(sc::SCDbPtr& db)
    {
        sc::SCTablePtr table;
        EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
        EXPECT_EQ(schema->AddColumn(MakeIntColumn(L"Width")), sc::SC_OK);
        EXPECT_EQ(schema->AddColumn(MakeStringColumn(L"Name", true)), sc::SC_OK);
        return table;
    }

}  // namespace

TEST(SchemaEdit, CreateTableFromSchemaBuildsTableAndColumns)
{
    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(L"StableCoreStorage_SchemaEdit_CreateTableSuccess.sqlite", db), sc::SC_OK);

    sc::SCTableSchemaSnapshot schema;
    schema.table.name = L"Beam";
    schema.columns.push_back(MakeIntColumn(L"Width"));
    schema.columns.push_back(MakeStringColumn(L"Name", true));

    sc::SCSchemaEditResult result;
    EXPECT_EQ(sc::CreateTableFromSchema(db.Get(), schema, &result), sc::SC_OK);
    EXPECT_TRUE(result.applied);
    EXPECT_GT(result.committedVersion, 0u);
    EXPECT_EQ(result.tableName, L"Beam");
    ASSERT_EQ(result.addedColumns.size(), 2u);
    EXPECT_EQ(result.addedColumns[0], L"Width");
    EXPECT_EQ(result.addedColumns[1], L"Name");

    sc::SCTablePtr table;
    EXPECT_EQ(db->GetTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr loadedSchema;
    EXPECT_EQ(table->GetSchema(loadedSchema), sc::SC_OK);

    sc::SCColumnDef width;
    EXPECT_EQ(loadedSchema->FindColumn(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width.valueKind, sc::ValueKind::Int64);
    EXPECT_FALSE(width.nullable);

    sc::SCColumnDef name;
    EXPECT_EQ(loadedSchema->FindColumn(L"Name", &name), sc::SC_OK);
    EXPECT_EQ(name.valueKind, sc::ValueKind::String);
    EXPECT_TRUE(name.nullable);
}

TEST(SchemaEdit, CreateTableFromSchemaRollsBackWhenColumnAddFails)
{
    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(L"StableCoreStorage_SchemaEdit_CreateTableRollback.sqlite", db), sc::SC_OK);

    sc::SCTableSchemaSnapshot schema;
    schema.table.name = L"Beam";
    schema.columns.push_back(MakeIntColumn(L"Width"));

    sc::SCColumnDef invalidRelation;
    invalidRelation.name = L"FloorRef";
    invalidRelation.displayName = L"FloorRef";
    invalidRelation.columnKind = sc::ColumnKind::Relation;
    invalidRelation.valueKind = sc::ValueKind::RecordId;
    invalidRelation.referenceTable = L"Floor";
    invalidRelation.defaultValue = sc::SCValue::FromString(L"bad");
    schema.columns.push_back(invalidRelation);

    sc::SCSchemaEditResult result;
    EXPECT_EQ(sc::CreateTableFromSchema(db.Get(), schema, &result), sc::SC_E_SCHEMA_VIOLATION);
    EXPECT_FALSE(result.applied);
    EXPECT_EQ(result.committedVersion, 0u);

    sc::SCTablePtr table;
    EXPECT_EQ(db->GetTable(L"Beam", table), sc::SC_E_TABLE_NOT_FOUND);
}

TEST(SchemaEdit, CreateTableFromSchemaBuildsConstraintsAndIndexes)
{
    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(L"StableCoreStorage_SchemaEdit_CreateTableConstraints.sqlite", db), sc::SC_OK);

    sc::SCTableSchemaSnapshot schema;
    schema.table.name = L"Beam";
    schema.columns.push_back(MakeIntColumn(L"Width"));
    schema.columns.push_back(MakeStringColumn(L"Name", true));

    schema.constraints.push_back(MakeUniqueConstraint(L"uq_Beam_Width", L"Width"));
    schema.indexes.push_back(MakeIndex(L"idx_Beam_Name", L"Name"));

    sc::SCSchemaEditResult result;
    EXPECT_EQ(sc::CreateTableFromSchema(db.Get(), schema, &result), sc::SC_OK);
    EXPECT_TRUE(result.applied);

    sc::SCTablePtr table;
    EXPECT_EQ(db->GetTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr loadedSchema;
    EXPECT_EQ(table->GetSchema(loadedSchema), sc::SC_OK);

    sc::SCConstraintDef constraint;
    EXPECT_EQ(loadedSchema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_OK);
    EXPECT_EQ(constraint.kind, sc::SCConstraintKind::Unique);
    ASSERT_EQ(constraint.columns.size(), 1u);
    EXPECT_EQ(constraint.columns.front(), L"Width");

    sc::SCIndexDef index;
    EXPECT_EQ(loadedSchema->FindIndex(L"idx_Beam_Name", &index), sc::SC_OK);
    ASSERT_EQ(index.columns.size(), 1u);
    EXPECT_EQ(index.columns.front().columnName, L"Name");
}

TEST(SchemaEdit, BackendCreateTableFailureDoesNotLeaveVisibleTable)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_BackendCreateFailureCleanup.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    EXPECT_TRUE(ExecSqliteScript(dbPath, "DROP TABLE schema_tables;"));

    sc::SCTablePtr table;
    EXPECT_NE(db->CreateTable(L"Beam", table), sc::SC_OK);
    EXPECT_EQ(db->GetTable(L"Beam", table), sc::SC_E_TABLE_NOT_FOUND);
}

TEST(SchemaEdit, CreateTableFromSchemaSupportsSelfReferentialForeignKey)
{
    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(L"StableCoreStorage_SchemaEdit_CreateTableSelfForeignKey.sqlite", db), sc::SC_OK);

    sc::SCTableSchemaSnapshot schema;
    schema.table.name = L"Node";
    schema.columns.push_back(MakeIntColumn(L"Id"));
    schema.columns.push_back(MakeIntColumn(L"ParentId", true));
    schema.constraints.push_back(MakeForeignKeyConstraint(L"fk_Node_Parent", L"ParentId", L"Node", L"Id"));

    sc::SCSchemaEditResult result;
    EXPECT_EQ(sc::CreateTableFromSchema(db.Get(), schema, &result), sc::SC_OK);
    EXPECT_TRUE(result.applied);

    sc::SCTablePtr table;
    EXPECT_EQ(db->GetTable(L"Node", table), sc::SC_OK);

    sc::SCSchemaPtr loadedSchema;
    EXPECT_EQ(table->GetSchema(loadedSchema), sc::SC_OK);

    sc::SCConstraintDef constraint;
    EXPECT_EQ(loadedSchema->FindConstraint(L"fk_Node_Parent", &constraint), sc::SC_OK);
    EXPECT_EQ(constraint.kind, sc::SCConstraintKind::ForeignKey);
    EXPECT_EQ(constraint.referencedTable, L"Node");
    ASSERT_EQ(constraint.columns.size(), 1u);
    EXPECT_EQ(constraint.columns.front(), L"ParentId");
    ASSERT_EQ(constraint.referencedColumns.size(), 1u);
    EXPECT_EQ(constraint.referencedColumns.front(), L"Id");
}

TEST(SchemaEdit, CreateTableFromSchemaRollsBackWhenForeignKeyIsInvalid)
{
    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(L"StableCoreStorage_SchemaEdit_CreateTableForeignKeyRollback.sqlite", db), sc::SC_OK);

    sc::SCTableSchemaSnapshot schema;
    schema.table.name = L"Beam";
    schema.columns.push_back(MakeIntColumn(L"Width"));
    schema.columns.push_back(MakeIntColumn(L"FloorId"));
    schema.constraints.push_back(MakeForeignKeyConstraint(L"fk_Beam_Floor", L"FloorId", L"Floor", L"Id"));

    sc::SCSchemaEditResult result;
    EXPECT_EQ(sc::CreateTableFromSchema(db.Get(), schema, &result), sc::SC_E_TABLE_NOT_FOUND);
    EXPECT_FALSE(result.applied);

    sc::SCTablePtr table;
    EXPECT_EQ(db->GetTable(L"Beam", table), sc::SC_E_TABLE_NOT_FOUND);
}

TEST(SchemaEdit, CreateTableFromSchemaRollsBackWhenIndexColumnIsInvalid)
{
    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(L"StableCoreStorage_SchemaEdit_CreateTableIndexRollback.sqlite", db), sc::SC_OK);

    sc::SCTableSchemaSnapshot schema;
    schema.table.name = L"Beam";
    schema.columns.push_back(MakeIntColumn(L"Width"));
    schema.indexes.push_back(MakeIndex(L"idx_Beam_Missing", L"Missing"));

    sc::SCSchemaEditResult result;
    EXPECT_EQ(sc::CreateTableFromSchema(db.Get(), schema, &result), sc::SC_E_COLUMN_NOT_FOUND);
    EXPECT_FALSE(result.applied);

    sc::SCTablePtr table;
    EXPECT_EQ(db->GetTable(L"Beam", table), sc::SC_E_TABLE_NOT_FOUND);
}

TEST(SchemaEdit, ApplyTableSchemaPatchSupportsCommitUndoAndRedo)
{
    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(L"StableCoreStorage_SchemaEdit_PatchUndoRedo.sqlite", db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCTableSchemaPatch patch;
    patch.tableName = L"Beam";
    patch.removeColumns.push_back(L"Name");

    sc::SCColumnDef updatedWidth = MakeStringColumn(L"Width");
    updatedWidth.displayName = L"Width Label";
    updatedWidth.defaultValue = sc::SCValue::FromString(L"0");
    patch.updateColumns.push_back(updatedWidth);
    patch.addColumns.push_back(MakeIntColumn(L"Height"));

    sc::SCSchemaEditResult result;
    EXPECT_EQ(sc::ApplyTableSchemaPatch(db.Get(), patch, &result), sc::SC_OK);
    EXPECT_TRUE(result.applied);
    EXPECT_GT(result.committedVersion, 0u);

    sc::SCColumnDef width;
    EXPECT_EQ(schema->FindColumn(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width.displayName, L"Width Label");
    EXPECT_EQ(width.valueKind, sc::ValueKind::String);

    sc::SCColumnDef height;
    EXPECT_EQ(schema->FindColumn(L"Height", &height), sc::SC_OK);
    EXPECT_EQ(height.valueKind, sc::ValueKind::Int64);

    sc::SCColumnDef name;
    EXPECT_EQ(schema->FindColumn(L"Name", &name), sc::SC_E_COLUMN_NOT_FOUND);

    EXPECT_EQ(db->Undo(), sc::SC_OK);

    EXPECT_EQ(schema->FindColumn(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width.displayName, L"Width");
    EXPECT_EQ(width.valueKind, sc::ValueKind::Int64);
    EXPECT_EQ(schema->FindColumn(L"Height", &height), sc::SC_E_COLUMN_NOT_FOUND);
    EXPECT_EQ(schema->FindColumn(L"Name", &name), sc::SC_OK);
    EXPECT_EQ(name.valueKind, sc::ValueKind::String);

    EXPECT_EQ(db->Redo(), sc::SC_OK);

    EXPECT_EQ(schema->FindColumn(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width.displayName, L"Width Label");
    EXPECT_EQ(width.valueKind, sc::ValueKind::String);
    EXPECT_EQ(schema->FindColumn(L"Height", &height), sc::SC_OK);
    EXPECT_EQ(schema->FindColumn(L"Name", &name), sc::SC_E_COLUMN_NOT_FOUND);
}

TEST(SchemaEdit, ApplyTableSchemaPatchSupportsConstraintIndexCommitUndoAndRedo)
{
    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(L"StableCoreStorage_SchemaEdit_PatchConstraintIndexUndoRedo.sqlite", db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCTableSchemaPatch patch;
    patch.tableName = L"Beam";
    patch.addConstraints.push_back(MakeUniqueConstraint(L"uq_Beam_Width", L"Width"));
    patch.addIndexes.push_back(MakeIndex(L"idx_Beam_Name", L"Name"));

    sc::SCSchemaEditResult result;
    EXPECT_EQ(sc::ApplyTableSchemaPatch(db.Get(), patch, &result), sc::SC_OK);
    EXPECT_TRUE(result.applied);
    EXPECT_GT(result.committedVersion, 0u);

    sc::SCConstraintDef constraint;
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_OK);
    sc::SCIndexDef index;
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_OK);

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_E_CONSTRAINT_NOT_FOUND);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_E_INDEX_NOT_FOUND);

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_OK);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_OK);
}

TEST(SchemaEdit, ApplyTableSchemaPatchRollsBackWhenLaterOperationFails)
{
    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(L"StableCoreStorage_SchemaEdit_PatchRollback.sqlite", db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCTableSchemaPatch patch;
    patch.tableName = L"Beam";
    patch.removeColumns.push_back(L"Name");

    sc::SCColumnDef invalidRelation;
    invalidRelation.name = L"FloorRef";
    invalidRelation.displayName = L"FloorRef";
    invalidRelation.columnKind = sc::ColumnKind::Relation;
    invalidRelation.valueKind = sc::ValueKind::RecordId;
    invalidRelation.referenceTable = L"Floor";
    invalidRelation.defaultValue = sc::SCValue::FromString(L"bad");
    patch.addColumns.push_back(invalidRelation);

    sc::SCSchemaEditResult result;
    EXPECT_EQ(sc::ApplyTableSchemaPatch(db.Get(), patch, &result), sc::SC_E_SCHEMA_VIOLATION);
    EXPECT_FALSE(result.applied);

    sc::SCColumnDef name;
    EXPECT_EQ(schema->FindColumn(L"Name", &name), sc::SC_OK);
    EXPECT_EQ(name.valueKind, sc::ValueKind::String);

    sc::SCColumnDef floorRef;
    EXPECT_EQ(schema->FindColumn(L"FloorRef", &floorRef), sc::SC_E_COLUMN_NOT_FOUND);
}

TEST(SchemaEdit, ApplyTableSchemaPatchRollsBackWhenConstraintOperationFailsAfterIndexRemoval)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_PatchConstraintRollback.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    EXPECT_EQ(schema->AddConstraint(MakeUniqueConstraint(L"uq_Beam_Width", L"Width")), sc::SC_OK);
    EXPECT_EQ(schema->AddIndex(MakeIndex(L"idx_Beam_Name", L"Name")), sc::SC_OK);

    EXPECT_TRUE(ExecSqliteScript(dbPath, "DROP TABLE schema_constraint_columns;"));

    sc::SCTableSchemaPatch patch;
    patch.tableName = L"Beam";
    patch.removeIndexes.push_back(L"idx_Beam_Name");
    patch.removeConstraints.push_back(L"uq_Beam_Width");

    sc::SCSchemaEditResult result;
    EXPECT_NE(sc::ApplyTableSchemaPatch(db.Get(), patch, &result), sc::SC_OK);
    EXPECT_FALSE(result.applied);

    sc::SCConstraintDef constraint;
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_OK);
    sc::SCIndexDef index;
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_OK);
}

TEST(SchemaEdit, SchemaConstraintAndIndexPrimitivesSupportUndoAndRedo)
{
    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(L"StableCoreStorage_SchemaEdit_ConstraintIndexUndoRedo.sqlite", db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"add constraint/index", edit), sc::SC_OK);
    EXPECT_EQ(schema->AddConstraint(MakeUniqueConstraint(L"uq_Beam_Width", L"Width")), sc::SC_OK);
    EXPECT_EQ(schema->AddIndex(MakeIndex(L"idx_Beam_Name", L"Name")), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    std::int32_t constraintCount = 0;
    EXPECT_EQ(schema->GetConstraintCount(&constraintCount), sc::SC_OK);
    EXPECT_EQ(constraintCount, 1);

    std::int32_t indexCount = 0;
    EXPECT_EQ(schema->GetIndexCount(&indexCount), sc::SC_OK);
    EXPECT_EQ(indexCount, 1);

    sc::SCConstraintDef constraint;
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_OK);
    EXPECT_EQ(constraint.kind, sc::SCConstraintKind::Unique);
    ASSERT_EQ(constraint.columns.size(), 1u);
    EXPECT_EQ(constraint.columns.front(), L"Width");

    sc::SCIndexDef index;
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_OK);
    ASSERT_EQ(index.columns.size(), 1u);
    EXPECT_EQ(index.columns.front().columnName, L"Name");

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_E_CONSTRAINT_NOT_FOUND);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_E_INDEX_NOT_FOUND);

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_OK);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_OK);
}

TEST(SchemaEdit, SchemaConstraintAndIndexPrimitivesPersistAcrossReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_ConstraintIndexReopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

        sc::SCTablePtr table = CreateBeamTable(db);
        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"add metadata", edit), sc::SC_OK);
        EXPECT_EQ(schema->AddConstraint(MakeUniqueConstraint(L"uq_Beam_Width", L"Width")), sc::SC_OK);
        EXPECT_EQ(schema->AddIndex(MakeIndex(L"idx_Beam_Name", L"Name")), sc::SC_OK);
        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    }

    sc::SCDbPtr reopened;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, reopened), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(reopened->GetTable(L"Beam", table), sc::SC_OK);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCConstraintDef constraint;
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_OK);
    sc::SCIndexDef index;
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_OK);

    EXPECT_EQ(reopened->Undo(), sc::SC_OK);
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_E_CONSTRAINT_NOT_FOUND);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_E_INDEX_NOT_FOUND);

    EXPECT_EQ(reopened->Redo(), sc::SC_OK);
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_OK);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_OK);
}

TEST(SchemaEdit, SchemaConstraintAndIndexPrimitivesSupportRemovalUndoAndRedo)
{
    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(L"StableCoreStorage_SchemaEdit_ConstraintIndexRemove.sqlite", db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    EXPECT_EQ(schema->AddConstraint(MakeUniqueConstraint(L"uq_Beam_Width", L"Width")), sc::SC_OK);
    EXPECT_EQ(schema->AddIndex(MakeIndex(L"idx_Beam_Name", L"Name")), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"remove constraint/index", edit), sc::SC_OK);
    EXPECT_EQ(schema->RemoveConstraint(L"uq_Beam_Width"), sc::SC_OK);
    EXPECT_EQ(schema->RemoveIndex(L"idx_Beam_Name"), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    sc::SCConstraintDef constraint;
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_E_CONSTRAINT_NOT_FOUND);
    sc::SCIndexDef index;
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_E_INDEX_NOT_FOUND);

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_OK);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_OK);

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_E_CONSTRAINT_NOT_FOUND);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_E_INDEX_NOT_FOUND);
}
