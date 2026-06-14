#include <filesystem>

#include <gtest/gtest.h>

#include "ISCQuery.h"
#include "../Src/Query/SCQuerySqliteIndexAccess.h"
#include "SCStorage.h"

#include "Support/TestPaths.h"
#include "Support/TestSqliteHelpers.h"
#include "Support/TestSchemaBuilders.h"
#include "Support/TestSeedData.h"

namespace sc = StableCore::Storage;
namespace fs = std::filesystem;

TEST(QueryExecutorCompositeNullDefaultTests, ExplicitCompositeIndexTreatsMissingValueAsDefaultInLogicalEntries)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeMissingDefault.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed missing default", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromString(L"")}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(result.matchedRows, 1u);
}

TEST(QueryExecutorCompositeNullDefaultTests, ExplicitCompositeIndexSupportsNullValuesInLogicalEntries)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeNullValue.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateNullableCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed null composite", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::Null()}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(result.matchedRows, 1u);
}

TEST(QueryExecutorCompositeNullDefaultTests, ExplicitCompositeIndexUpdatesValueToNullWithoutResidualEntry)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeNullUpdate.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateNullableCompositeIndexedBeamTable(db);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed null update", seedEdit), sc::SC_OK);

    sc::SCRecordPtr alpha;
    EXPECT_EQ(table->CreateRecord(alpha), sc::SC_OK);
    EXPECT_EQ(alpha->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(alpha->SetString(L"Name", L"Alpha"), sc::SC_OK);

    sc::SCRecordPtr bravo;
    EXPECT_EQ(table->CreateRecord(bravo), sc::SC_OK);
    EXPECT_EQ(bravo->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(bravo->SetString(L"Name", L"Bravo"), sc::SC_OK);

    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr nullEdit;
    EXPECT_EQ(db->BeginEdit(L"set alpha to null", nullEdit), sc::SC_OK);
    EXPECT_EQ(alpha->SetValue(L"Name", sc::SCValue::Null()), sc::SC_OK);
    EXPECT_EQ(db->Commit(nullEdit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan nullPlan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::Null()}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           {},
                           &nullPlan),
        sc::SC_OK);

    sc::SCRecordCursorPtr nullCursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &nullCursor;

    sc::QueryExecutionResult nullResult;
    EXPECT_EQ(sc::ExecuteQueryPlan(nullPlan, context, &nullResult), sc::SC_OK);
    EXPECT_EQ(nullResult.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(nullResult.matchedRows, 1u);

    sc::SCRecordPtr nullRecord;
    EXPECT_EQ(nullCursor->Next(nullRecord), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(nullRecord));
    std::wstring unusedName;
    EXPECT_EQ(nullRecord->GetStringCopy(L"Name", &unusedName), sc::SC_E_VALUE_IS_NULL);

    sc::QueryPlan alphaPlan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromString(L"Alpha")}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           {},
                           &alphaPlan),
        sc::SC_OK);

    sc::SCRecordCursorPtr alphaCursor;
    context.resultCursor = &alphaCursor;
    sc::QueryExecutionResult alphaResult;
    EXPECT_EQ(sc::ExecuteQueryPlan(alphaPlan, context, &alphaResult), sc::SC_OK);
    EXPECT_EQ(alphaResult.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(alphaResult.matchedRows, 0u);
}

TEST(QueryExecutorCompositeNullDefaultTests, ExplicitCompositeIndexPreservesNullAndDefaultEntriesAfterReopen)
{
    const fs::path nullDbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeNullReopen.sqlite");
    const fs::path defaultDbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeMissingDefaultReopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(nullDbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr nullableTable = CreateNullableCompositeIndexedBeamTable(db);

        sc::SCEditPtr nullableEdit;
        EXPECT_EQ(db->BeginEdit(L"seed null reopen", nullableEdit), sc::SC_OK);

        sc::SCRecordPtr nullableRow;
        EXPECT_EQ(nullableTable->CreateRecord(nullableRow), sc::SC_OK);
        EXPECT_EQ(nullableRow->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(nullableRow->SetString(L"Name", L"Alpha"), sc::SC_OK);
        EXPECT_EQ(db->Commit(nullableEdit.Get()), sc::SC_OK);

        sc::SCEditPtr nullEdit;
        EXPECT_EQ(db->BeginEdit(L"update to null before reopen", nullEdit), sc::SC_OK);
        EXPECT_EQ(nullableRow->SetValue(L"Name", sc::SCValue::Null()), sc::SC_OK);
        EXPECT_EQ(db->Commit(nullEdit.Get()), sc::SC_OK);
    }

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(defaultDbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr defaultTable = CreateCompositeIndexedBeamTable(db);

        sc::SCEditPtr defaultEdit;
        EXPECT_EQ(db->BeginEdit(L"seed default reopen", defaultEdit), sc::SC_OK);
        sc::SCRecordPtr defaultRow;
        EXPECT_EQ(defaultTable->CreateRecord(defaultRow), sc::SC_OK);
        EXPECT_EQ(defaultRow->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(defaultTable->CreateRecord(defaultRow), sc::SC_OK);
        EXPECT_EQ(defaultRow->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(defaultRow->SetString(L"Name", L"Zulu"), sc::SC_OK);
        EXPECT_EQ(db->Commit(defaultEdit.Get()), sc::SC_OK);
    }

    sc::SCDbPtr reopenedNullDb;
    EXPECT_EQ(CreateFileDb(nullDbPath.c_str(), reopenedNullDb), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan nullPlan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::Null()}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           {},
                           &nullPlan),
        sc::SC_OK);

    sc::SCRecordCursorPtr nullCursor;
    sc::QueryExecutionContext nullContext;
    nullContext.backendKind = sc::QueryBackendKind::SQLite;
    nullContext.database = reopenedNullDb.Get();
    nullContext.backendHandle = reopenedNullDb.Get();
    nullContext.resultCursor = &nullCursor;

    sc::QueryExecutionResult nullResult;
    EXPECT_EQ(sc::ExecuteQueryPlan(nullPlan, nullContext, &nullResult), sc::SC_OK);
    EXPECT_EQ(nullResult.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(nullResult.matchedRows, 1u);

    sc::SCDbPtr reopenedDefaultDb;
    EXPECT_EQ(CreateFileDb(defaultDbPath.c_str(), reopenedDefaultDb), sc::SC_OK);

    sc::QueryPlan defaultPlan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromString(L"")}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           {},
                           &defaultPlan),
        sc::SC_OK);

    sc::SCRecordCursorPtr defaultCursor;
    sc::QueryExecutionContext defaultContext;
    defaultContext.backendKind = sc::QueryBackendKind::SQLite;
    defaultContext.database = reopenedDefaultDb.Get();
    defaultContext.backendHandle = reopenedDefaultDb.Get();
    defaultContext.resultCursor = &defaultCursor;

    sc::QueryExecutionResult defaultResult;
    EXPECT_EQ(sc::ExecuteQueryPlan(defaultPlan, defaultContext, &defaultResult), sc::SC_OK);
    EXPECT_EQ(defaultResult.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(defaultResult.matchedRows, 1u);
}
