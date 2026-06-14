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

TEST(SchemaEdit, ApplyTableSchemaPatchRollsBackWhenLaterOperationFails)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_PatchRollback.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

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