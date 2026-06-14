#include <algorithm>
#include <filesystem>

#include <gtest/gtest.h>
#include <sqlite3.h>

#include "ISCQuery.h"
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

    bool QuerySqliteInt64(const fs::path& dbPath, const char* sql, std::int64_t* outValue)
    {
        if (outValue == nullptr)
        {
            return false;
        }

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

        sqlite3_stmt* stmt = nullptr;
        const int prepareRc = sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
        if (prepareRc != SQLITE_OK)
        {
            if (stmt != nullptr)
            {
                sqlite3_finalize(stmt);
            }
            sqlite3_close(db);
            return false;
        }

        const int stepRc = sqlite3_step(stmt);
        const bool ok = stepRc == SQLITE_ROW;
        if (ok)
        {
            *outValue = sqlite3_column_int64(stmt, 0);
        }

        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return ok;
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

    sc::SCIndexDef MakeCompositeIndex(const wchar_t* name, const wchar_t* firstColumn, const wchar_t* secondColumn)
    {
        sc::SCIndexDef index;
        index.name = name;
        index.columns.push_back(sc::SCIndexColumnDef{firstColumn, false});
        index.columns.push_back(sc::SCIndexColumnDef{secondColumn, false});
        return index;
    }

    sc::ErrorCode ExecuteQueryForBeam(sc::ISCDatabase* db,
                                      std::vector<sc::QueryCondition> conditions,
                                      const sc::QueryConstraints& constraints,
                                      std::vector<std::wstring>* outNames,
                                      sc::QueryExecutionResult* outResult)
    {
        if (db == nullptr || outNames == nullptr || outResult == nullptr)
        {
            return sc::SC_E_POINTER;
        }

        auto planner = sc::CreateDefaultQueryPlanner();
        if (planner == nullptr)
        {
            return sc::SC_E_FAIL;
        }

        sc::QueryPlan plan;
        const sc::ErrorCode planRc = planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                                                        {sc::QueryConditionGroup{sc::QueryLogicOperator::And, std::move(conditions)}},
                                                        sc::QueryLogicOperator::And,
                                                        {sc::SortSpec{L"Name", sc::QueryOrderDirection::Ascending, false}},
                                                        {},
                                                        {},
                                                        constraints,
                                                        &plan);
        if (sc::Failed(planRc))
        {
            return planRc;
        }

        sc::SCRecordCursorPtr cursor;
        sc::QueryExecutionContext context;
        context.backendKind = sc::QueryBackendKind::SQLite;
        context.database = db;
        context.backendHandle = db;
        context.resultCursor = &cursor;

        const sc::ErrorCode execRc = sc::ExecuteQueryPlan(plan, context, outResult);
        if (sc::Failed(execRc))
        {
            return execRc;
        }

        outNames->clear();
        sc::SCRecordPtr record;
        while (cursor->Next(record) == sc::SC_OK && record)
        {
            std::wstring name;
            const sc::ErrorCode nameRc = record->GetStringCopy(L"Name", &name);
            if (nameRc == sc::SC_OK)
            {
                outNames->push_back(name);
            }
            else if (nameRc == sc::SC_E_VALUE_IS_NULL)
            {
                outNames->push_back(L"<NULL>");
            }
            else
            {
                return nameRc;
            }
        }

        return sc::SC_OK;
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

TEST(SchemaEdit, ExplicitCompositeIndexBuildsLogicalQueryIndexStorage)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_CompositeQueryIndex.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
    EXPECT_EQ(schema->AddColumn(MakeIntColumn(L"Width")), sc::SC_OK);
    EXPECT_EQ(schema->AddColumn(MakeStringColumn(L"Name")), sc::SC_OK);
    EXPECT_EQ(schema->AddIndex(MakeCompositeIndex(L"idx_Beam_Width_Name", L"Width", L"Name")), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    std::int64_t value = -1;
    EXPECT_TRUE(QuerySqliteInt64(
        dbPath,
        "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
        "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
        &value));
    EXPECT_EQ(value, 1);

    EXPECT_TRUE(QuerySqliteInt64(
        dbPath,
        "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
        "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
        "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
        &value));
    EXPECT_EQ(value, 1);
}

TEST(SchemaEdit, ExplicitCompositeIndexQueryStorageSurvivesUndoRedoAndReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_CompositeQueryIndexUndoRedoReopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

        sc::SCTablePtr table;
        EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
        EXPECT_EQ(schema->AddColumn(MakeIntColumn(L"Width")), sc::SC_OK);
        EXPECT_EQ(schema->AddColumn(MakeStringColumn(L"Name")), sc::SC_OK);

        sc::SCEditPtr indexEdit;
        EXPECT_EQ(db->BeginEdit(L"add composite index", indexEdit), sc::SC_OK);
        EXPECT_EQ(schema->AddIndex(MakeCompositeIndex(L"idx_Beam_Width_Name", L"Width", L"Name")), sc::SC_OK);
        EXPECT_EQ(db->Commit(indexEdit.Get()), sc::SC_OK);

        sc::SCEditPtr seedEdit;
        EXPECT_EQ(db->BeginEdit(L"seed composite index rows", seedEdit), sc::SC_OK);

        sc::SCRecordPtr row;
        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
        EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

        std::int64_t value = -1;
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 1);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
            "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 1);

        EXPECT_EQ(db->Undo(), sc::SC_OK);
    }

    {
        sc::SCDbPtr reopened;
        EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, reopened), sc::SC_OK);

        sc::SCTablePtr table;
        EXPECT_EQ(reopened->GetTable(L"Beam", table), sc::SC_OK);

        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

        sc::SCIndexDef index;
        EXPECT_EQ(schema->FindIndex(L"idx_Beam_Width_Name", &index), sc::SC_OK);

        std::int64_t value = -1;
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 1);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
            "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 0);

        EXPECT_EQ(reopened->Undo(), sc::SC_OK);
        EXPECT_EQ(schema->FindIndex(L"idx_Beam_Width_Name", &index), sc::SC_E_INDEX_NOT_FOUND);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 0);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
            "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 0);

        EXPECT_EQ(reopened->Redo(), sc::SC_OK);
        EXPECT_EQ(schema->FindIndex(L"idx_Beam_Width_Name", &index), sc::SC_OK);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 1);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
            "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 0);
    }
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

TEST(SchemaEdit, ApplyTableSchemaPatchSupportsExplicitCompositeIndexLogicalStorage)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_PatchCompositeIndexLogicalStorage.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed before patch", seedEdit), sc::SC_OK);
    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCTableSchemaPatch patch;
    patch.tableName = L"Beam";
    patch.addIndexes.push_back(MakeCompositeIndex(L"idx_Beam_Width_Name", L"Width", L"Name"));

    sc::SCSchemaEditResult result;
    EXPECT_EQ(sc::ApplyTableSchemaPatch(db.Get(), patch, &result), sc::SC_OK);
    EXPECT_TRUE(result.applied);

    sc::SCIndexDef index;
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Width_Name", &index), sc::SC_OK);

    std::int64_t value = -1;
    EXPECT_TRUE(QuerySqliteInt64(
        dbPath,
        "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
        "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
        &value));
    EXPECT_EQ(value, 1);
    EXPECT_TRUE(QuerySqliteInt64(
        dbPath,
        "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
        "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
        "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
        &value));
    EXPECT_EQ(value, 1);

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Width_Name", &index), sc::SC_E_INDEX_NOT_FOUND);
    EXPECT_TRUE(QuerySqliteInt64(
        dbPath,
        "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
        "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
        &value));
    EXPECT_EQ(value, 0);
    EXPECT_TRUE(QuerySqliteInt64(
        dbPath,
        "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
        "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
        "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
        &value));
    EXPECT_EQ(value, 0);

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Width_Name", &index), sc::SC_OK);
    EXPECT_TRUE(QuerySqliteInt64(
        dbPath,
        "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
        "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
        &value));
    EXPECT_EQ(value, 1);
    EXPECT_TRUE(QuerySqliteInt64(
        dbPath,
        "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
        "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
        "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
        &value));
    EXPECT_EQ(value, 1);
}

TEST(SchemaEdit, ApplyTableSchemaPatchRemovesExplicitCompositeIndexAcrossReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_PatchRemoveCompositeIndexReopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

        sc::SCTablePtr table = CreateBeamTable(db);
        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
        EXPECT_EQ(schema->AddIndex(MakeCompositeIndex(L"idx_Beam_Width_Name", L"Width", L"Name")), sc::SC_OK);

        sc::SCEditPtr seedEdit;
        EXPECT_EQ(db->BeginEdit(L"seed before remove patch", seedEdit), sc::SC_OK);
        sc::SCRecordPtr row;
        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
        EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

        sc::SCTableSchemaPatch patch;
        patch.tableName = L"Beam";
        patch.removeIndexes.push_back(L"idx_Beam_Width_Name");

        sc::SCSchemaEditResult result;
        EXPECT_EQ(sc::ApplyTableSchemaPatch(db.Get(), patch, &result), sc::SC_OK);
        EXPECT_TRUE(result.applied);

        sc::SCIndexDef index;
        EXPECT_EQ(schema->FindIndex(L"idx_Beam_Width_Name", &index), sc::SC_E_INDEX_NOT_FOUND);

        std::int64_t value = -1;
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 0);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
            "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 0);
    }

    sc::SCDbPtr reopened;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, reopened), sc::SC_OK);

    sc::SCTablePtr reopenedTable;
    EXPECT_EQ(reopened->GetTable(L"Beam", reopenedTable), sc::SC_OK);

    sc::SCSchemaPtr reopenedSchema;
    EXPECT_EQ(reopenedTable->GetSchema(reopenedSchema), sc::SC_OK);

    sc::SCIndexDef reopenedIndex;
    EXPECT_EQ(reopenedSchema->FindIndex(L"idx_Beam_Width_Name", &reopenedIndex), sc::SC_E_INDEX_NOT_FOUND);

    std::int64_t value = -1;
    EXPECT_TRUE(QuerySqliteInt64(
        dbPath,
        "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
        "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
        &value));
    EXPECT_EQ(value, 0);
    EXPECT_TRUE(QuerySqliteInt64(
        dbPath,
        "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
        "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
        "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
        &value));
    EXPECT_EQ(value, 0);
}

TEST(SchemaEdit, ApplyTableSchemaPatchPreservesNullCompositeEntriesThroughUndoRedoAndReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_PatchCompositeNullUndoRedoReopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

        sc::SCTablePtr table = CreateBeamTable(db);
        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

        sc::SCEditPtr seedEdit;
        EXPECT_EQ(db->BeginEdit(L"seed null patch chain", seedEdit), sc::SC_OK);
        sc::SCRecordPtr row;
        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

        sc::SCTableSchemaPatch patch;
        patch.tableName = L"Beam";
        patch.addIndexes.push_back(MakeCompositeIndex(L"idx_Beam_Width_Name", L"Width", L"Name"));

        sc::SCSchemaEditResult result;
        EXPECT_EQ(sc::ApplyTableSchemaPatch(db.Get(), patch, &result), sc::SC_OK);
        EXPECT_TRUE(result.applied);

        std::int64_t value = -1;
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 1);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
            "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 1);

        EXPECT_EQ(db->Undo(), sc::SC_OK);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 0);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
            "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 0);

        EXPECT_EQ(db->Redo(), sc::SC_OK);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 1);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
            "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 1);
    }

    sc::SCDbPtr reopened;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, reopened), sc::SC_OK);
    sc::SCTablePtr reopenedTable;
    EXPECT_EQ(reopened->GetTable(L"Beam", reopenedTable), sc::SC_OK);

    std::int64_t value = -1;
    EXPECT_TRUE(QuerySqliteInt64(
        dbPath,
        "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
        "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
        &value));
    EXPECT_EQ(value, 1);
    EXPECT_TRUE(QuerySqliteInt64(
        dbPath,
        "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
        "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
        "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
        &value));
    EXPECT_EQ(value, 1);
}

TEST(SchemaEdit, ApplyTableSchemaPatchPreservesDefaultCompositeEntriesThroughUndoRedoAndReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_PatchCompositeDefaultUndoRedoReopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

        sc::SCTablePtr table;
        EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
        EXPECT_EQ(schema->AddColumn(MakeIntColumn(L"Width")), sc::SC_OK);
        EXPECT_EQ(schema->AddColumn(MakeStringColumn(L"Name", false)), sc::SC_OK);

        sc::SCEditPtr seedEdit;
        EXPECT_EQ(db->BeginEdit(L"seed default patch chain", seedEdit), sc::SC_OK);
        sc::SCRecordPtr row;
        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

        sc::SCTableSchemaPatch patch;
        patch.tableName = L"Beam";
        patch.addIndexes.push_back(MakeCompositeIndex(L"idx_Beam_Width_Name", L"Width", L"Name"));

        sc::SCSchemaEditResult result;
        EXPECT_EQ(sc::ApplyTableSchemaPatch(db.Get(), patch, &result), sc::SC_OK);
        EXPECT_TRUE(result.applied);

        std::int64_t value = -1;
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 1);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
            "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 1);

        EXPECT_EQ(db->Undo(), sc::SC_OK);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 0);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
            "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 0);

        EXPECT_EQ(db->Redo(), sc::SC_OK);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 1);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
            "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 1);
    }

    sc::SCDbPtr reopened;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, reopened), sc::SC_OK);
    sc::SCTablePtr reopenedTable;
    EXPECT_EQ(reopened->GetTable(L"Beam", reopenedTable), sc::SC_OK);

    std::int64_t value = -1;
    EXPECT_TRUE(QuerySqliteInt64(
        dbPath,
        "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
        "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
        &value));
    EXPECT_EQ(value, 1);
    EXPECT_TRUE(QuerySqliteInt64(
        dbPath,
        "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
        "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
        "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
        &value));
    EXPECT_EQ(value, 1);
}

TEST(SchemaEdit, ApplyTableSchemaPatchAddAndRemoveCompositeIndexChangesActualQueryBehavior)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_PatchCompositeQueryBehavior.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

        sc::SCTablePtr table = CreateBeamTable(db);

        sc::SCEditPtr seedEdit;
        EXPECT_EQ(db->BeginEdit(L"seed query behavior patch", seedEdit), sc::SC_OK);
        sc::SCRecordPtr row;
        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
        EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

        sc::SCTableSchemaPatch addPatch;
        addPatch.tableName = L"Beam";
        addPatch.addIndexes.push_back(MakeCompositeIndex(L"idx_Beam_Width_Name", L"Width", L"Name"));

        sc::SCSchemaEditResult editResult;
        EXPECT_EQ(sc::ApplyTableSchemaPatch(db.Get(), addPatch, &editResult), sc::SC_OK);

        std::vector<std::wstring> names;
        sc::QueryExecutionResult queryResult;
        sc::QueryConstraints constraints;
        constraints.requireIndex = true;
        constraints.allowFallbackScan = false;
        EXPECT_EQ(ExecuteQueryForBeam(db.Get(),
                                      {sc::QueryCondition{L"Width",
                                                          sc::QueryConditionOperator::Equal,
                                                          {sc::SCValue::FromInt64(100)}},
                                       sc::QueryCondition{L"Name",
                                                          sc::QueryConditionOperator::Equal,
                                                          {sc::SCValue::FromString(L"Alpha")}}},
                                      constraints,
                                      &names,
                                      &queryResult),
                  sc::SC_OK);
        EXPECT_EQ(queryResult.mode, sc::QueryExecutionMode::DirectIndex);
        ASSERT_EQ(queryResult.usedIndexIds.size(), 1u);
        EXPECT_EQ(queryResult.usedIndexIds.front(), L"idx_Beam_Width_Name");
        ASSERT_EQ(names.size(), 1u);
        EXPECT_EQ(names.front(), L"Alpha");

        sc::SCTableSchemaPatch removePatch;
        removePatch.tableName = L"Beam";
        removePatch.removeIndexes.push_back(L"idx_Beam_Width_Name");
        EXPECT_EQ(sc::ApplyTableSchemaPatch(db.Get(), removePatch, &editResult), sc::SC_OK);
    }

    sc::SCDbPtr reopened;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, reopened), sc::SC_OK);

    std::vector<std::wstring> names;
    sc::QueryExecutionResult queryResult;
    sc::QueryConstraints constraints;
    constraints.requireIndex = true;
    constraints.allowFallbackScan = false;
    EXPECT_EQ(ExecuteQueryForBeam(reopened.Get(),
                                  {sc::QueryCondition{L"Width",
                                                      sc::QueryConditionOperator::Equal,
                                                      {sc::SCValue::FromInt64(100)}},
                                   sc::QueryCondition{L"Name",
                                                      sc::QueryConditionOperator::Equal,
                                                      {sc::SCValue::FromString(L"Alpha")}}},
                                  constraints,
                                  &names,
                                  &queryResult),
              sc::SC_E_INVALIDARG);
    EXPECT_EQ(queryResult.mode, sc::QueryExecutionMode::Unsupported);
}

TEST(SchemaEdit, ApplyTableSchemaPatchAddedCompositeIndexSupportsActualQueryHitsAfterReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_PatchCompositeQueryHitReopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

        sc::SCTablePtr table = CreateBeamTable(db);

        sc::SCEditPtr seedEdit;
        EXPECT_EQ(db->BeginEdit(L"seed query hit patch", seedEdit), sc::SC_OK);
        sc::SCRecordPtr row;
        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);
        EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

        sc::SCTableSchemaPatch addPatch;
        addPatch.tableName = L"Beam";
        addPatch.addIndexes.push_back(MakeCompositeIndex(L"idx_Beam_Width_Name", L"Width", L"Name"));

        sc::SCSchemaEditResult editResult;
        EXPECT_EQ(sc::ApplyTableSchemaPatch(db.Get(), addPatch, &editResult), sc::SC_OK);
    }

    sc::SCDbPtr reopened;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, reopened), sc::SC_OK);

    std::vector<std::wstring> names;
    sc::QueryExecutionResult queryResult;
    sc::QueryConstraints constraints;
    constraints.requireIndex = true;
    constraints.allowFallbackScan = false;
    EXPECT_EQ(ExecuteQueryForBeam(reopened.Get(),
                                  {sc::QueryCondition{L"Width",
                                                      sc::QueryConditionOperator::Equal,
                                                      {sc::SCValue::FromInt64(100)}}},
                                  constraints,
                                  &names,
                                  &queryResult),
              sc::SC_OK);
    EXPECT_EQ(queryResult.mode, sc::QueryExecutionMode::DirectIndex);
    ASSERT_EQ(queryResult.usedIndexIds.size(), 1u);
    EXPECT_EQ(queryResult.usedIndexIds.front(), L"idx_Beam_Width_Name");
    ASSERT_EQ(names.size(), 2u);
    EXPECT_EQ(names[0], L"Alpha");
    EXPECT_EQ(names[1], L"Bravo");
}

TEST(SchemaEdit, ApplyTableSchemaPatchAddedCompositeIndexSupportsStartsWithAndBetweenAfterReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_PatchCompositeRangeQueriesReopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

        sc::SCTablePtr table = CreateBeamTable(db);

        sc::SCEditPtr seedEdit;
        EXPECT_EQ(db->BeginEdit(L"seed query range patch", seedEdit), sc::SC_OK);
        sc::SCRecordPtr row;
        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Alpine"), sc::SC_OK);
        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);
        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Zulu"), sc::SC_OK);
        EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

        sc::SCTableSchemaPatch addPatch;
        addPatch.tableName = L"Beam";
        addPatch.addIndexes.push_back(MakeCompositeIndex(L"idx_Beam_Width_Name", L"Width", L"Name"));

        sc::SCSchemaEditResult editResult;
        EXPECT_EQ(sc::ApplyTableSchemaPatch(db.Get(), addPatch, &editResult), sc::SC_OK);
    }

    sc::SCDbPtr reopened;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, reopened), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryConstraints constraints;
    constraints.requireIndex = true;
    constraints.allowFallbackScan = false;

    sc::QueryPlan startsWithPlan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::StartsWith,
                                                                        {sc::SCValue::FromString(L"Al")}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Ascending, false}},
                           {},
                           {},
                           constraints,
                           &startsWithPlan),
        sc::SC_OK);

    sc::QueryPlan betweenPlan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::Between,
                                                                        {sc::SCValue::FromString(L"Alpha"),
                                                                         sc::SCValue::FromString(L"Bravo")}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Ascending, false}},
                           {},
                           {},
                           constraints,
                           &betweenPlan),
        sc::SC_OK);

    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = reopened.Get();
    context.backendHandle = reopened.Get();

    sc::SCRecordCursorPtr startsWithCursor;
    context.resultCursor = &startsWithCursor;
    sc::QueryExecutionResult startsWithResult;
    const sc::ErrorCode startsWithRc = sc::ExecuteQueryPlan(startsWithPlan, context, &startsWithResult);
    SCOPED_TRACE(::testing::Message() << "executionRc=" << startsWithRc << " executionNote="
                                      << std::string(startsWithResult.executionNote.begin(),
                                                     startsWithResult.executionNote.end()));
    EXPECT_EQ(startsWithRc, sc::SC_OK);
    EXPECT_EQ(startsWithResult.mode, sc::QueryExecutionMode::PartialIndex);
    ASSERT_EQ(startsWithResult.usedIndexIds.size(), 1u);
    EXPECT_EQ(startsWithResult.usedIndexIds.front(), L"idx_Beam_Width_Name");

    std::vector<std::wstring> startsWithNames;
    sc::SCRecordPtr record;
    while (startsWithCursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        startsWithNames.push_back(name);
    }

    ASSERT_EQ(startsWithNames.size(), 2u);
    EXPECT_EQ(startsWithNames[0], L"Alpha");
    EXPECT_EQ(startsWithNames[1], L"Alpine");

    sc::SCRecordCursorPtr betweenCursor;
    context.resultCursor = &betweenCursor;
    sc::QueryExecutionResult betweenResult;
    EXPECT_EQ(sc::ExecuteQueryPlan(betweenPlan, context, &betweenResult), sc::SC_OK);
    EXPECT_EQ(betweenResult.mode, sc::QueryExecutionMode::PartialIndex);
    ASSERT_EQ(betweenResult.usedIndexIds.size(), 1u);
    EXPECT_EQ(betweenResult.usedIndexIds.front(), L"idx_Beam_Width_Name");

    std::vector<std::wstring> betweenNames;
    while (betweenCursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        betweenNames.push_back(name);
    }

    ASSERT_EQ(betweenNames.size(), 3u);
    EXPECT_EQ(betweenNames[0], L"Alpha");
    EXPECT_EQ(betweenNames[1], L"Alpine");
    EXPECT_EQ(betweenNames[2], L"Bravo");
}

TEST(SchemaEdit, QueryIndexProviderDetectsDriftAndMaintainerRepairsIt)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_QueryIndexRepair.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
    EXPECT_EQ(schema->AddIndex(MakeCompositeIndex(L"idx_Beam_Width_Name", L"Width", L"Name")), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed query index drift", seedEdit), sc::SC_OK);
    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    const auto* provider = dynamic_cast<const sc::IQueryIndexProvider*>(db.Get());
    ASSERT_NE(provider, nullptr);
    auto* maintainer = dynamic_cast<sc::IQueryIndexMaintainer*>(db.Get());
    ASSERT_NE(maintainer, nullptr);

    sc::QueryIndexCheckResult check;
    EXPECT_EQ(provider->CheckQueryIndex(&check), sc::SC_OK);
    EXPECT_EQ(check.state, sc::QueryIndexHealthState::Healthy);

    EXPECT_TRUE(ExecSqliteScript(dbPath, "DELETE FROM query_index_entries;"));

    EXPECT_EQ(provider->CheckQueryIndex(&check), sc::SC_OK);
    EXPECT_EQ(check.state, sc::QueryIndexHealthState::Corrupted);
    EXPECT_NE(check.message.find(L"query-index-entry-missing"), std::wstring::npos);

    EXPECT_EQ(maintainer->RebuildQueryIndex(), sc::SC_OK);
    EXPECT_EQ(provider->CheckQueryIndex(&check), sc::SC_OK);
    EXPECT_EQ(check.state, sc::QueryIndexHealthState::Healthy);

    std::vector<std::wstring> names;
    sc::QueryExecutionResult queryResult;
    sc::QueryConstraints constraints;
    constraints.requireIndex = true;
    constraints.allowFallbackScan = false;
    EXPECT_EQ(ExecuteQueryForBeam(db.Get(),
                                  {sc::QueryCondition{L"Width",
                                                      sc::QueryConditionOperator::Equal,
                                                      {sc::SCValue::FromInt64(100)}},
                                   sc::QueryCondition{L"Name",
                                                      sc::QueryConditionOperator::Equal,
                                                      {sc::SCValue::FromString(L"Alpha")}}},
                                  constraints,
                                  &names,
                                  &queryResult),
              sc::SC_OK);
    EXPECT_EQ(queryResult.mode, sc::QueryExecutionMode::DirectIndex);
    ASSERT_EQ(names.size(), 1u);
    EXPECT_EQ(names.front(), L"Alpha");
}

TEST(SchemaEdit, StorageHealthReportIncludesQueryIndexDiagnosticsWhenEntriesDrift)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_QueryIndexDiagnostics.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
    EXPECT_EQ(schema->AddIndex(MakeCompositeIndex(L"idx_Beam_Width_Name", L"Width", L"Name")), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed query index diagnostics", seedEdit), sc::SC_OK);
    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    EXPECT_TRUE(ExecSqliteScript(dbPath, "DELETE FROM query_index_entries;"));

    sc::SCStorageHealthReport report;
    EXPECT_EQ(sc::BuildStorageHealthReport(db.Get(), L"sqlite", &report), sc::SC_OK);

    const auto it = std::find_if(report.diagnostics.begin(),
                                 report.diagnostics.end(),
                                 [](const sc::SCDiagnosticEntry& entry) {
                                     return entry.category == L"query-index" &&
                                            entry.severity == sc::SCDiagnosticSeverity::Error &&
                                            entry.message.find(L"query-index-entry-missing") != std::wstring::npos;
                                 });
    EXPECT_NE(it, report.diagnostics.end());
}

TEST(SchemaEdit, QueryIndexProviderReportsOutOfDateDuringActiveEditAndRejectsRepair)
{
    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(L"StableCoreStorage_SchemaEdit_QueryIndexActiveEdit.sqlite", db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
    EXPECT_EQ(schema->AddIndex(MakeCompositeIndex(L"idx_Beam_Width_Name", L"Width", L"Name")), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed active edit query index", seedEdit), sc::SC_OK);
    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    const auto* provider = dynamic_cast<const sc::IQueryIndexProvider*>(db.Get());
    ASSERT_NE(provider, nullptr);
    auto* maintainer = dynamic_cast<sc::IQueryIndexMaintainer*>(db.Get());
    ASSERT_NE(maintainer, nullptr);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"dirty query index", edit), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);

    sc::QueryIndexCheckResult check;
    EXPECT_EQ(provider->CheckQueryIndex(&check), sc::SC_OK);
    EXPECT_EQ(check.state, sc::QueryIndexHealthState::OutOfDate);
    EXPECT_EQ(check.message, L"query-index-rebuild-required");
    EXPECT_EQ(maintainer->RebuildQueryIndex(), sc::SC_E_WRITE_CONFLICT);

    EXPECT_EQ(db->Rollback(edit.Get()), sc::SC_OK);
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

TEST(SchemaEdit, AddCompositeIndexFailureDoesNotLeaveSchemaIndexLoaded)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_AddCompositeIndexRollback.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    EXPECT_TRUE(ExecSqliteScript(dbPath, "DROP TABLE query_index_entries;"));

    EXPECT_NE(schema->AddIndex(MakeCompositeIndex(L"idx_Beam_Width_Name", L"Width", L"Name")), sc::SC_OK);

    sc::SCIndexDef index;
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Width_Name", &index), sc::SC_E_INDEX_NOT_FOUND);
}

TEST(SchemaEdit, RemoveCompositeIndexFailurePreservesQueryIndexExecution)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_RemoveCompositeIndexRollback.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
    EXPECT_EQ(schema->AddIndex(MakeCompositeIndex(L"idx_Beam_Width_Name", L"Width", L"Name")), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed composite index rollback", seedEdit), sc::SC_OK);
    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    EXPECT_TRUE(ExecSqliteScript(dbPath, "DROP TABLE schema_index_columns;"));

    EXPECT_NE(schema->RemoveIndex(L"idx_Beam_Width_Name"), sc::SC_OK);

    sc::SCIndexDef index;
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Width_Name", &index), sc::SC_OK);

    std::vector<std::wstring> names;
    sc::QueryExecutionResult queryResult;
    sc::QueryConstraints constraints;
    constraints.requireIndex = true;
    constraints.allowFallbackScan = false;
    EXPECT_EQ(ExecuteQueryForBeam(db.Get(),
                                  {sc::QueryCondition{L"Width",
                                                      sc::QueryConditionOperator::Equal,
                                                      {sc::SCValue::FromInt64(100)}},
                                   sc::QueryCondition{L"Name",
                                                      sc::QueryConditionOperator::Equal,
                                                      {sc::SCValue::FromString(L"Alpha")}}},
                                  constraints,
                                  &names,
                                  &queryResult),
              sc::SC_OK);
    EXPECT_EQ(queryResult.mode, sc::QueryExecutionMode::DirectIndex);
    ASSERT_EQ(queryResult.usedIndexIds.size(), 1u);
    EXPECT_EQ(queryResult.usedIndexIds.front(), L"idx_Beam_Width_Name");
    ASSERT_EQ(names.size(), 1u);
    EXPECT_EQ(names.front(), L"Alpha");
}

TEST(SchemaEdit, DISABLED_RemoveCompositeIndexFailurePreservesQueryIndexExecutionAfterReopen)
{
    GTEST_SKIP() << "The current fault injection drops schema_index_columns, which permanently corrupts on-disk "
                    "schema metadata. That can validate in-process rollback behavior, but it cannot validate "
                    "reopen consistency without a non-destructive failure injection path.";

    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_RemoveCompositeIndexRollbackReopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

        sc::SCTablePtr table = CreateBeamTable(db);
        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
        EXPECT_EQ(schema->AddIndex(MakeCompositeIndex(L"idx_Beam_Width_Name", L"Width", L"Name")), sc::SC_OK);

        sc::SCEditPtr seedEdit;
        EXPECT_EQ(db->BeginEdit(L"seed composite index rollback reopen", seedEdit), sc::SC_OK);
        sc::SCRecordPtr row;
        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
        EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

        EXPECT_TRUE(ExecSqliteScript(dbPath, "DROP TABLE schema_index_columns;"));

        EXPECT_NE(schema->RemoveIndex(L"idx_Beam_Width_Name"), sc::SC_OK);

        sc::SCIndexDef index;
        EXPECT_EQ(schema->FindIndex(L"idx_Beam_Width_Name", &index), sc::SC_OK);
    }

    sc::SCDbPtr reopened;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, reopened), sc::SC_OK);

    sc::SCTablePtr reopenedTable;
    EXPECT_EQ(reopened->GetTable(L"Beam", reopenedTable), sc::SC_OK);
    sc::SCSchemaPtr reopenedSchema;
    EXPECT_EQ(reopenedTable->GetSchema(reopenedSchema), sc::SC_OK);

    sc::SCIndexDef reopenedIndex;
    EXPECT_EQ(reopenedSchema->FindIndex(L"idx_Beam_Width_Name", &reopenedIndex), sc::SC_OK);

    std::vector<std::wstring> names;
    sc::QueryExecutionResult queryResult;
    sc::QueryConstraints constraints;
    constraints.requireIndex = true;
    constraints.allowFallbackScan = false;
    EXPECT_EQ(ExecuteQueryForBeam(reopened.Get(),
                                  {sc::QueryCondition{L"Width",
                                                      sc::QueryConditionOperator::Equal,
                                                      {sc::SCValue::FromInt64(100)}},
                                   sc::QueryCondition{L"Name",
                                                      sc::QueryConditionOperator::Equal,
                                                      {sc::SCValue::FromString(L"Alpha")}}},
                                  constraints,
                                  &names,
                                  &queryResult),
              sc::SC_OK);
    EXPECT_EQ(queryResult.mode, sc::QueryExecutionMode::DirectIndex);
    ASSERT_EQ(queryResult.usedIndexIds.size(), 1u);
    EXPECT_EQ(queryResult.usedIndexIds.front(), L"idx_Beam_Width_Name");
    ASSERT_EQ(names.size(), 1u);
    EXPECT_EQ(names.front(), L"Alpha");
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
