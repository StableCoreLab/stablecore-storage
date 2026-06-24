#include <filesystem>
#include <vector>

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

TEST(QueryExecutorRequireIndexTests, LegacyFindRecordsRoutesThroughExecutor)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_Legacy.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateQueryableBeamTable(db);
    SeedQueryableBeamRows(table, db);

    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(table->FindRecords({L"Width", sc::SCValue::FromInt64(200)}, cursor), sc::SC_OK);

    sc::SCRecordPtr record;
    EXPECT_EQ(cursor->Next(record), sc::SC_OK);
    EXPECT_TRUE(static_cast<bool>(record));

    std::int64_t width = 0;
    EXPECT_EQ(record->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 200);
}

TEST(QueryExecutorRequireIndexTests, SqliteExecutorReportsDirectAndPartialModes)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_Result.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateQueryableBeamTable(db);
    SeedQueryableBeamRows(table, db);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan directPlan;
    EXPECT_EQ(
        planner->BuildPlan(
            sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
            {sc::QueryConditionGroup{
                sc::QueryLogicOperator::And,
                {sc::QueryCondition{L"Width", sc::QueryConditionOperator::Equal, {sc::SCValue::FromInt64(200)}}}}},
            sc::QueryLogicOperator::And,
            {},
            {},
            {},
            {},
            &directPlan),
        sc::SC_OK);

    sc::SCRecordCursorPtr directCursor;
    sc::QueryExecutionResult directResult;
    sc::QueryExecutionContext directContext;
    directContext.backendKind = sc::QueryBackendKind::SQLite;
    directContext.database = db.Get();
    directContext.backendHandle = db.Get();
    directContext.resultCursor = &directCursor;
    EXPECT_EQ(sc::ExecuteQueryPlan(directPlan, directContext, &directResult), sc::SC_OK);
    EXPECT_EQ(directResult.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_FALSE(directResult.fallbackTriggered);
    EXPECT_EQ(directResult.matchedRows, 1u);
    EXPECT_EQ(directResult.returnedRows, 1u);
    EXPECT_FALSE(directResult.usedIndexIds.empty());
    EXPECT_NE(directResult.executionNote.find(L"SELECT record_id"), std::wstring::npos);

    sc::QueryPlan partialPlan;
    EXPECT_EQ(planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                                 {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                          {sc::QueryCondition{L"Name",
                                                                              sc::QueryConditionOperator::StartsWith,
                                                                              {sc::SCValue::FromString(L"Al")}}}}},
                                 sc::QueryLogicOperator::And,
                                 {},
                                 {},
                                 {},
                                 {},
                                 &partialPlan),
              sc::SC_OK);

    sc::SCRecordCursorPtr partialCursor;
    sc::QueryExecutionResult partialResult;
    sc::QueryExecutionContext partialContext;
    partialContext.backendKind = sc::QueryBackendKind::SQLite;
    partialContext.database = db.Get();
    partialContext.backendHandle = db.Get();
    partialContext.resultCursor = &partialCursor;
    EXPECT_EQ(sc::ExecuteQueryPlan(partialPlan, partialContext, &partialResult), sc::SC_OK);
    EXPECT_EQ(partialResult.mode, sc::QueryExecutionMode::PartialIndex);
    EXPECT_FALSE(partialResult.fallbackTriggered);
    EXPECT_EQ(partialResult.matchedRows, 2u);
    EXPECT_EQ(partialResult.returnedRows, 2u);
    EXPECT_FALSE(partialResult.usedIndexIds.empty());
}

TEST(QueryExecutorRequireIndexTests, PlannerRejectsBinaryRangeAndPrefixConditions)
{
    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_BinaryRejected.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr table = CreateQueryableBeamTable(db);
    SeedQueryableBeamRows(table, db);

    sc::QueryPlan rangePlan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Attachment",
                                                                        sc::QueryConditionOperator::GreaterThan,
                                                                        {sc::SCValue::FromBinary(std::vector<std::uint8_t>{0x01, 0x02})}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           {},
                           &rangePlan),
        sc::SC_OK);
    EXPECT_EQ(rangePlan.state, sc::QueryPlanState::Unsupported);
    EXPECT_EQ(rangePlan.fallbackReason, L"binary-condition-unsupported");

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;
    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(rangePlan, context, &result), sc::SC_E_INVALIDARG);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::Unsupported);

    sc::QueryPlan prefixPlan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Attachment",
                                                                        sc::QueryConditionOperator::StartsWith,
                                                                        {sc::SCValue::FromBinary(std::vector<std::uint8_t>{0x01})}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           {},
                           &prefixPlan),
        sc::SC_OK);
    EXPECT_EQ(prefixPlan.state, sc::QueryPlanState::Unsupported);
    EXPECT_EQ(prefixPlan.fallbackReason, L"binary-condition-unsupported");
}

TEST(QueryExecutorRequireIndexTests, BinaryEqualityFallsBackToScanSafely)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_BinaryEqualityScan.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef attachment;
    attachment.name = L"Attachment";
    attachment.displayName = L"Attachment";
    attachment.valueKind = sc::ValueKind::Binary;
    attachment.defaultValue = sc::SCValue::FromBinary(std::vector<std::uint8_t>{});
    EXPECT_EQ(schema->AddColumn(attachment), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed binary", edit), sc::SC_OK);

    sc::SCRecordPtr record;
    EXPECT_EQ(table->CreateRecord(record), sc::SC_OK);
    const std::vector<std::uint8_t> payload{0x01, 0x03};
    EXPECT_EQ(record->SetBinary(L"Attachment", payload.data(), payload.size()), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Attachment",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromBinary(payload)}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);
    EXPECT_EQ(plan.state, sc::QueryPlanState::ScanFallback);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::FallbackScan);
    EXPECT_TRUE(result.fallbackTriggered);
    EXPECT_EQ(result.matchedRows, 1u);
    EXPECT_EQ(result.returnedRows, 1u);
    EXPECT_TRUE(result.usedIndexIds.empty());
}

TEST(QueryExecutorRequireIndexTests, ExecutorRejectsRequireIndexOnPartialPlans)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_Unsupported.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateQueryableBeamTable(db);
    SeedQueryableBeamRows(table, db);

    sc::QueryPlan plan;
    plan.target = sc::QueryTarget{L"Beam", sc::QueryTargetType::Table};
    plan.conditionGroups = {sc::QueryConditionGroup{
        sc::QueryLogicOperator::And,
        {sc::QueryCondition{L"Name", sc::QueryConditionOperator::StartsWith, {sc::SCValue::FromString(L"Al")}}}}};
    plan.conditionGroupLogic = sc::QueryLogicOperator::And;
    plan.state = sc::QueryPlanState::PartialIndex;
    plan.constraints.requireIndex = true;
    plan.constraints.allowPartial = true;
    plan.constraints.allowFallbackScan = false;

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionResult result;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_E_INVALIDARG);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::Unsupported);
    EXPECT_NE(result.executionNote.find(L"executor-unsupported:index-required"), std::wstring::npos);
}

TEST(QueryExecutorRequireIndexTests, ExecutorRejectsRequireIndexOnDirectPlansWithoutExplicitMatchedIndex)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_RequireIndexDirectWithoutMatch.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateQueryableBeamTable(db);
    SeedQueryableBeamRows(table, db);

    sc::QueryPlan plan;
    plan.target = sc::QueryTarget{L"Beam", sc::QueryTargetType::Table};
    plan.conditionGroups = {sc::QueryConditionGroup{
        sc::QueryLogicOperator::And,
        {sc::QueryCondition{L"Width", sc::QueryConditionOperator::Equal, {sc::SCValue::FromInt64(100)}}}}};
    plan.conditionGroupLogic = sc::QueryLogicOperator::And;
    plan.state = sc::QueryPlanState::DirectIndex;
    plan.constraints.requireIndex = true;
    plan.constraints.allowFallbackScan = false;

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionResult result;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_E_INVALIDARG);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::Unsupported);
    EXPECT_NE(result.executionNote.find(L"executor-unsupported:index-required"), std::wstring::npos);
}

TEST(QueryExecutorRequireIndexTests, RequireIndexRejectsLegacySingleColumnIndexesWithoutExplicitSchemaIndex)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_RequireIndexLegacy.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateQueryableBeamTable(db);
    SeedQueryableBeamRows(table, db);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    sc::QueryConstraints constraints;
    constraints.requireIndex = true;
    constraints.allowFallbackScan = false;
    constraints.allowPartial = true;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(200)}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           constraints,
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_E_INVALIDARG);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::Unsupported);
    EXPECT_NE(result.executionNote.find(L"executor-unsupported:index-required"), std::wstring::npos);
}

TEST(QueryExecutorRequireIndexTests, RequireIndexAcceptsExplicitCompositeSchemaIndex)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_RequireIndexExplicitOnly.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);
    SeedQueryableBeamRows(table, db);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    sc::QueryConstraints constraints;
    constraints.requireIndex = true;
    constraints.allowFallbackScan = false;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           constraints,
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    const sc::ErrorCode executeRc = sc::ExecuteQueryPlan(plan, context, &result);
    SCOPED_TRACE(::testing::Message() << "executionRc=" << executeRc << " executionNote="
                                      << std::wstring(result.executionNote.begin(), result.executionNote.end()));
    EXPECT_EQ(executeRc, sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::DirectIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name");
}

TEST(QueryExecutorRequireIndexTests, RequireIndexPrefersExplicitCompositeIndexWhenLegacyHintAlsoExists)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_RequireIndexExplicitWins.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompositeIndexedBeamTableWithLegacyWidth(db);
    SeedQueryableBeamRows(table, db);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    sc::QueryConstraints constraints;
    constraints.requireIndex = true;
    constraints.allowFallbackScan = false;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           constraints,
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
}

TEST(QueryExecutorRequireIndexTests, RequireIndexRejectsNonPrefixCompositeLookupWithoutLegacyIndex)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_RequireIndexCompositeMiss.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);
    SeedQueryableBeamRows(table, db);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    sc::QueryConstraints constraints;
    constraints.requireIndex = true;
    constraints.allowFallbackScan = false;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromString(L"Alpha")}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           constraints,
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_E_INVALIDARG);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::Unsupported);
    EXPECT_NE(result.executionNote.find(L"executor-unsupported:index-required"), std::wstring::npos);
}

TEST(QueryExecutorRequireIndexTests, SqliteExecutorRunsControlledFallbackScan)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_Fallback.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateQueryableBeamTable(db);
    SeedQueryableBeamRows(table, db);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(
            sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
            {sc::QueryConditionGroup{
                sc::QueryLogicOperator::And,
                {sc::QueryCondition{L"Name", sc::QueryConditionOperator::Contains, {sc::SCValue::FromString(L"Al")}}}}},
            sc::QueryLogicOperator::And,
            {},
            {},
            {},
            {},
            &plan),
        sc::SC_OK);
    EXPECT_EQ(plan.state, sc::QueryPlanState::ScanFallback);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::FallbackScan);
    EXPECT_TRUE(result.fallbackTriggered);
    EXPECT_EQ(result.fallbackSource, sc::QueryFallbackSource::Planner);
    EXPECT_EQ(result.scannedRows, 3u);
    EXPECT_EQ(result.matchedRows, 2u);
    EXPECT_EQ(result.returnedRows, 2u);
    EXPECT_NE(result.executionNote.find(L"planner-fallback"), std::wstring::npos);
}

TEST(QueryExecutorRequireIndexTests, RequireIndexSupportsExplicitNullCompositeLookup)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_RequireIndexNullExplicit.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateNullableCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed require index null", edit), sc::SC_OK);

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
    sc::QueryConstraints constraints;
    constraints.requireIndex = true;
    constraints.allowFallbackScan = false;
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
                           constraints,
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
    EXPECT_EQ(result.matchedRows, 1u);
}

TEST(QueryExecutorRequireIndexTests, RequireIndexSupportsExplicitDefaultCompositeLookup)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_RequireIndexDefaultExplicit.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed require index default", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Zulu"), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    sc::QueryConstraints constraints;
    constraints.requireIndex = true;
    constraints.allowFallbackScan = false;
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
                           constraints,
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
    EXPECT_EQ(result.matchedRows, 1u);
}
