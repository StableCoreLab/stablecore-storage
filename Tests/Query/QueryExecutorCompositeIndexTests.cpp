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

TEST(QueryExecutorCompositeIndexTests, ExplicitCompositeIndexSupportsPrefixLookupWithoutLegacyIndexedFlag)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositePrefix.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);
    SeedQueryableBeamRows(table, db);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(
            sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
            {sc::QueryConditionGroup{
                sc::QueryLogicOperator::And,
                {sc::QueryCondition{L"Width", sc::QueryConditionOperator::Equal, {sc::SCValue::FromInt64(100)}}}}},
            sc::QueryLogicOperator::And,
            {},
            {},
            {},
            {},
            &plan),
        sc::SC_OK);

    auto* indexAccess = dynamic_cast<sc::ISqliteQueryIndexAccess*>(db.Get());
    ASSERT_NE(indexAccess, nullptr);

    sc::QueryPlan analyzedPlan;
    EXPECT_EQ(indexAccess->AnalyzeCompositeIndexPlan(plan, &analyzedPlan), sc::SC_OK);
    ASSERT_TRUE(analyzedPlan.matchedIndex.has_value());
    EXPECT_EQ(analyzedPlan.matchedIndex->indexName, L"idx_Beam_Width_Name");
    EXPECT_EQ(analyzedPlan.state, sc::QueryPlanState::DirectIndex);
    EXPECT_TRUE(analyzedPlan.pushdown.residualConditions.empty());
}

TEST(QueryExecutorCompositeIndexTests, ExplicitCompositeIndexSupportsThreeColumnHappyPath)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeThreeColumn.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateTripleCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed three column", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 30), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 10), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 20), sc::SC_OK);

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
                                                                        {sc::SCValue::FromString(L"Alpha")}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Height", sc::QueryOrderDirection::Ascending, false}},
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
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name_Height");
    EXPECT_EQ(result.matchedRows, 2u);

    std::vector<std::int64_t> heights;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::int64_t height = 0;
        EXPECT_EQ(record->GetInt64(L"Height", &height), sc::SC_OK);
        heights.push_back(height);
    }

    ASSERT_EQ(heights.size(), 2u);
    EXPECT_EQ(heights[0], 10);
    EXPECT_EQ(heights[1], 30);
}

TEST(QueryExecutorCompositeIndexTests, ExplicitCompositeIndexSupportsThreeColumnLeadingPrefixLookup)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeThreeColumnPrefix.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateTripleCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed three column prefix", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 30), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 10), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 20), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 5), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Ascending, false},
                            sc::SortSpec{L"Height", sc::QueryOrderDirection::Ascending, false}},
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
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name_Height");
    EXPECT_EQ(result.matchedRows, 3u);

    std::vector<std::wstring> orderedKeys;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        std::int64_t height = 0;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        EXPECT_EQ(record->GetInt64(L"Height", &height), sc::SC_OK);
        orderedKeys.push_back(name + L":" + std::to_wstring(height));
    }

    ASSERT_EQ(orderedKeys.size(), 3u);
    EXPECT_EQ(orderedKeys[0], L"Alpha:10");
    EXPECT_EQ(orderedKeys[1], L"Alpha:30");
    EXPECT_EQ(orderedKeys[2], L"Bravo:20");
}

TEST(QueryExecutorCompositeIndexTests, ExplicitCompositeIndexSupportsThreeColumnExactLookup)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeThreeColumnExact.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateTripleCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed three column exact", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 30), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 10), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 10), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{
                               sc::QueryLogicOperator::And,
                               {sc::QueryCondition{L"Width",
                                                   sc::QueryConditionOperator::Equal,
                                                   {sc::SCValue::FromInt64(100)}},
                                sc::QueryCondition{L"Name",
                                                   sc::QueryConditionOperator::Equal,
                                                   {sc::SCValue::FromString(L"Alpha")}},
                                sc::QueryCondition{L"Height",
                                                   sc::QueryConditionOperator::Equal,
                                                   {sc::SCValue::FromInt64(10)}}}}},
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
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name_Height");
    EXPECT_EQ(result.matchedRows, 1u);

    sc::SCRecordPtr record;
    EXPECT_EQ(cursor->Next(record), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(record));

    std::int64_t height = 0;
    EXPECT_EQ(record->GetInt64(L"Height", &height), sc::SC_OK);
    EXPECT_EQ(height, 10);
}

TEST(QueryExecutorCompositeIndexTests, ExplicitCompositeIndexDoesNotReviveDeletedRecords)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeDelete.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);
    SeedQueryableBeamRows(table, db);

    sc::SCRecordPtr record;
    EXPECT_EQ(table->GetRecord(1, record), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(record));

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"delete", edit), sc::SC_OK);
    EXPECT_EQ(table->DeleteRecord(1), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(
            sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
            {sc::QueryConditionGroup{
                sc::QueryLogicOperator::And,
                {sc::QueryCondition{L"Width", sc::QueryConditionOperator::Equal, {sc::SCValue::FromInt64(100)}}}}},
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
    EXPECT_EQ(result.matchedRows, 0u);

    sc::SCRecordPtr deleted;
    EXPECT_EQ(cursor->Next(deleted), sc::SC_OK);
    EXPECT_FALSE(static_cast<bool>(deleted));
}
