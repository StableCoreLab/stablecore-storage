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

TEST(QueryExecutorCompositeOrderingTests, ExplicitCompositeIndexSupportsThreeColumnOrderCoverageAfterLeadingPrefix)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeThreeColumnOrderCoverage.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateTripleCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed three column order coverage", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Charlie"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 40), sc::SC_OK);

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
    EXPECT_FALSE(result.fallbackTriggered);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name_Height");
    EXPECT_EQ(result.matchedRows, 4u);

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

    ASSERT_EQ(orderedKeys.size(), 4u);
    EXPECT_EQ(orderedKeys[0], L"Alpha:10");
    EXPECT_EQ(orderedKeys[1], L"Alpha:30");
    EXPECT_EQ(orderedKeys[2], L"Bravo:20");
    EXPECT_EQ(orderedKeys[3], L"Charlie:40");
}

TEST(QueryExecutorCompositeOrderingTests, DescendingCompositeIndexSupportsThreeColumnOrderCoverageAfterLeadingPrefix)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeThreeColumnOrderCoverageDesc.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateDescendingTripleCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed three column order coverage desc", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Charlie"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 40), sc::SC_OK);

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
                                                                        {sc::SCValue::FromInt64(100)}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Descending, false},
                            sc::SortSpec{L"Height", sc::QueryOrderDirection::Descending, false}},
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
    EXPECT_FALSE(result.fallbackTriggered);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name_Height_Desc");
    EXPECT_EQ(result.matchedRows, 4u);

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

    ASSERT_EQ(orderedKeys.size(), 4u);
    EXPECT_EQ(orderedKeys[0], L"Charlie:40");
    EXPECT_EQ(orderedKeys[1], L"Bravo:20");
    EXPECT_EQ(orderedKeys[2], L"Alpha:30");
    EXPECT_EQ(orderedKeys[3], L"Alpha:10");
}

TEST(QueryExecutorCompositeOrderingTests, ExplicitCompositeIndexSupportsOrderedPrefixScan)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeOrder.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed composite", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Zulu"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                                 {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                          {sc::QueryCondition{L"Width",
                                                                              sc::QueryConditionOperator::Equal,
                                                                              {sc::SCValue::FromInt64(100)}}}}},
                                 sc::QueryLogicOperator::And,
                                 {sc::SortSpec{L"Name", sc::QueryOrderDirection::Ascending, false}},
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
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name");

    std::vector<std::wstring> names;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        names.push_back(name);
    }

    ASSERT_EQ(names.size(), 3u);
    EXPECT_EQ(names[0], L"Alpha");
    EXPECT_EQ(names[1], L"Bravo");
    EXPECT_EQ(names[2], L"Zulu");
}

TEST(QueryExecutorCompositeOrderingTests, ExplicitCompositeIndexUpdatesPrefixBucketWithoutResidualEntries)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositePrefixBucketMove.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed prefix bucket move", seedEdit), sc::SC_OK);

    sc::SCRecordPtr moved;
    EXPECT_EQ(table->CreateRecord(moved), sc::SC_OK);
    EXPECT_EQ(moved->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(moved->SetString(L"Name", L"Alpha"), sc::SC_OK);
    const sc::RecordId movedId = moved->GetId();

    sc::SCRecordPtr stable;
    EXPECT_EQ(table->CreateRecord(stable), sc::SC_OK);
    EXPECT_EQ(stable->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(stable->SetString(L"Name", L"Bravo"), sc::SC_OK);

    sc::SCRecordPtr targetBucket;
    EXPECT_EQ(table->CreateRecord(targetBucket), sc::SC_OK);
    EXPECT_EQ(targetBucket->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(targetBucket->SetString(L"Name", L"Zulu"), sc::SC_OK);

    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr moveEdit;
    EXPECT_EQ(db->BeginEdit(L"move width bucket", moveEdit), sc::SC_OK);
    EXPECT_EQ(moved->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(db->Commit(moveEdit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan width100Plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Ascending, false}},
                           {},
                           {},
                           {},
                           &width100Plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr width100Cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &width100Cursor;

    sc::QueryExecutionResult width100Result;
    EXPECT_EQ(sc::ExecuteQueryPlan(width100Plan, context, &width100Result), sc::SC_OK);
    EXPECT_EQ(width100Result.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(width100Result.matchedRows, 1u);

    std::vector<std::wstring> width100Names;
    sc::SCRecordPtr record;
    while (width100Cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        width100Names.push_back(name);
    }

    ASSERT_EQ(width100Names.size(), 1u);
    EXPECT_EQ(width100Names[0], L"Bravo");

    sc::QueryPlan width200Plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(200)}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Ascending, false}},
                           {},
                           {},
                           {},
                           &width200Plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr width200Cursor;
    context.resultCursor = &width200Cursor;
    sc::QueryExecutionResult width200Result;
    EXPECT_EQ(sc::ExecuteQueryPlan(width200Plan, context, &width200Result), sc::SC_OK);
    EXPECT_EQ(width200Result.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(width200Result.matchedRows, 2u);

    std::vector<std::wstring> width200Names;
    while (width200Cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        width200Names.push_back(name);
    }

    ASSERT_EQ(width200Names.size(), 2u);
    EXPECT_EQ(width200Names[0], L"Alpha");
    EXPECT_EQ(width200Names[1], L"Zulu");

    sc::SCRecordPtr movedRecord;
    EXPECT_EQ(table->GetRecord(movedId, movedRecord), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(movedRecord));

    std::int64_t movedWidth = 0;
    EXPECT_EQ(movedRecord->GetInt64(L"Width", &movedWidth), sc::SC_OK);
    EXPECT_EQ(movedWidth, 200);
}

TEST(QueryExecutorCompositeOrderingTests, DescendingCompositeIndexUpdatesPrefixBucketWithoutResidualEntries)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeDescPrefixBucketMove.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateDescendingCompositeIndexedBeamTable(db);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed desc prefix bucket move", seedEdit), sc::SC_OK);

    sc::SCRecordPtr moved;
    EXPECT_EQ(table->CreateRecord(moved), sc::SC_OK);
    EXPECT_EQ(moved->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(moved->SetString(L"Name", L"Alpha"), sc::SC_OK);
    const sc::RecordId movedId = moved->GetId();

    sc::SCRecordPtr stable;
    EXPECT_EQ(table->CreateRecord(stable), sc::SC_OK);
    EXPECT_EQ(stable->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(stable->SetString(L"Name", L"Bravo"), sc::SC_OK);

    sc::SCRecordPtr targetBucket;
    EXPECT_EQ(table->CreateRecord(targetBucket), sc::SC_OK);
    EXPECT_EQ(targetBucket->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(targetBucket->SetString(L"Name", L"Zulu"), sc::SC_OK);

    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr moveEdit;
    EXPECT_EQ(db->BeginEdit(L"move desc width bucket", moveEdit), sc::SC_OK);
    EXPECT_EQ(moved->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(db->Commit(moveEdit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan width100Plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Descending, false}},
                           {},
                           {},
                           {},
                           &width100Plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr width100Cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &width100Cursor;

    sc::QueryExecutionResult width100Result;
    EXPECT_EQ(sc::ExecuteQueryPlan(width100Plan, context, &width100Result), sc::SC_OK);
    EXPECT_EQ(width100Result.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(width100Result.matchedRows, 1u);

    std::vector<std::wstring> width100Names;
    sc::SCRecordPtr record;
    while (width100Cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        width100Names.push_back(name);
    }

    ASSERT_EQ(width100Names.size(), 1u);
    EXPECT_EQ(width100Names[0], L"Bravo");

    sc::QueryPlan width200Plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(200)}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Descending, false}},
                           {},
                           {},
                           {},
                           &width200Plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr width200Cursor;
    context.resultCursor = &width200Cursor;
    sc::QueryExecutionResult width200Result;
    EXPECT_EQ(sc::ExecuteQueryPlan(width200Plan, context, &width200Result), sc::SC_OK);
    EXPECT_EQ(width200Result.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(width200Result.matchedRows, 2u);

    std::vector<std::wstring> width200Names;
    while (width200Cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        width200Names.push_back(name);
    }

    ASSERT_EQ(width200Names.size(), 2u);
    EXPECT_EQ(width200Names[0], L"Zulu");
    EXPECT_EQ(width200Names[1], L"Alpha");

    sc::SCRecordPtr movedRecord;
    EXPECT_EQ(table->GetRecord(movedId, movedRecord), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(movedRecord));

    std::int64_t movedWidth = 0;
    EXPECT_EQ(movedRecord->GetInt64(L"Width", &movedWidth), sc::SC_OK);
    EXPECT_EQ(movedWidth, 200);
}

TEST(QueryExecutorCompositeOrderingTests, DescendingCompositeIndexSupportsOrderedPrefixScan)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeDescOrder.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateDescendingCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed desc order", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Zulu"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                                 {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                          {sc::QueryCondition{L"Width",
                                                                              sc::QueryConditionOperator::Equal,
                                                                              {sc::SCValue::FromInt64(100)}}}}},
                                 sc::QueryLogicOperator::And,
                                 {sc::SortSpec{L"Name", sc::QueryOrderDirection::Descending, false}},
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
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name_Desc");

    std::vector<std::wstring> names;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        names.push_back(name);
    }

    ASSERT_EQ(names.size(), 3u);
    EXPECT_EQ(names[0], L"Zulu");
    EXPECT_EQ(names[1], L"Bravo");
    EXPECT_EQ(names[2], L"Alpha");
}
