#include <algorithm>
#include <filesystem>

#include <gtest/gtest.h>
#include <sqlite3.h>

#include "ISCQuery.h"
#include "SCStorage.h"

#include "Support/TestPaths.h"
#include "Support/TestSqliteHelpers.h"
#include "Support/TestSchemaBuilders.h"
#include "Support/TestQueryHelpers.h"

namespace sc = StableCore::Storage;
namespace fs = std::filesystem;

TEST(SchemaEdit, ApplyTableSchemaPatchSupportsCommitUndoAndRedo)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_PatchUndoRedo.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

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
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_PatchConstraintIndexUndoRedo.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

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
                                      << std::wstring(startsWithResult.executionNote.begin(),
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