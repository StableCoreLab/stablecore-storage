#include <filesystem>

#include <gtest/gtest.h>

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

    sc::ErrorCode CreateFileDb(const wchar_t* path, sc::SCDbPtr& db)
    {
        return sc::CreateFileDatabase(path, sc::SCOpenDatabaseOptions{}, db);
    }

    sc::SCTablePtr CreateQueryableBeamTable(sc::SCDbPtr& db)
    {
        sc::SCTablePtr table;
        EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

        sc::SCColumnDef width;
        width.name = L"Width";
        width.displayName = L"Width";
        width.valueKind = sc::ValueKind::Int64;
        width.indexed = true;
        width.defaultValue = sc::SCValue::FromInt64(0);
        EXPECT_EQ(schema->AddColumn(width), sc::SC_OK);

        sc::SCColumnDef name;
        name.name = L"Name";
        name.displayName = L"Name";
        name.valueKind = sc::ValueKind::String;
        name.indexed = true;
        name.defaultValue = sc::SCValue::FromString(L"");
        EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);

        return table;
    }

    void SeedQueryableBeamRows(const sc::SCTablePtr& table, sc::SCDbPtr& db)
    {
        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

        sc::SCRecordPtr row;
        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 200), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Beta"), sc::SC_OK);

        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 300), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Alpine"), sc::SC_OK);

        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    }

}  // namespace

TEST(QuerySqliteExecutorTests, LegacyFindRecordsRoutesThroughExecutor)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_QuerySqlite_Legacy.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateQueryableBeamTable(db);
    SeedQueryableBeamRows(table, db);

    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(
        table->FindRecords({L"Width", sc::SCValue::FromInt64(200)}, cursor),
        sc::SC_OK);

    bool hasRow = false;
    EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
    EXPECT_TRUE(hasRow);

    sc::SCRecordPtr record;
    EXPECT_EQ(cursor->GetCurrent(record), sc::SC_OK);

    std::int64_t width = 0;
    EXPECT_EQ(record->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 200);
}

TEST(QuerySqliteExecutorTests, SqliteExecutorReportsDirectAndPartialModes)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_QuerySqlite_Result.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateQueryableBeamTable(db);
    SeedQueryableBeamRows(table, db);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan directPlan;
    EXPECT_EQ(planner->BuildPlan(
                  sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                  {sc::QueryConditionGroup{
                      sc::QueryLogicOperator::And,
                      {sc::QueryCondition{L"Width",
                                          sc::QueryConditionOperator::Equal,
                                          {sc::SCValue::FromInt64(200)}}}}},
                  sc::QueryLogicOperator::And, {}, {}, {}, {}, &directPlan),
              sc::SC_OK);

    sc::SCRecordCursorPtr directCursor;
    sc::QueryExecutionResult directResult;
    sc::QueryExecutionContext directContext;
    directContext.backendKind = sc::QueryBackendKind::SQLite;
    directContext.database = db.Get();
    directContext.backendHandle = db.Get();
    directContext.resultCursor = &directCursor;
    EXPECT_EQ(sc::ExecuteQueryPlan(directPlan, directContext, &directResult),
              sc::SC_OK);
    EXPECT_EQ(directResult.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_FALSE(directResult.fallbackTriggered);
    EXPECT_EQ(directResult.matchedRows, 1u);
    EXPECT_EQ(directResult.returnedRows, 1u);
    EXPECT_FALSE(directResult.usedIndexIds.empty());
    EXPECT_NE(directResult.executionNote.find(L"SELECT record_id"),
              std::wstring::npos);

    sc::QueryPlan partialPlan;
    EXPECT_EQ(
        planner->BuildPlan(
            sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
            {sc::QueryConditionGroup{
                sc::QueryLogicOperator::And,
                {sc::QueryCondition{L"Name",
                                    sc::QueryConditionOperator::StartsWith,
                                    {sc::SCValue::FromString(L"Al")}}}}},
            sc::QueryLogicOperator::And, {}, {}, {}, {}, &partialPlan),
        sc::SC_OK);

    sc::SCRecordCursorPtr partialCursor;
    sc::QueryExecutionResult partialResult;
    sc::QueryExecutionContext partialContext;
    partialContext.backendKind = sc::QueryBackendKind::SQLite;
    partialContext.database = db.Get();
    partialContext.backendHandle = db.Get();
    partialContext.resultCursor = &partialCursor;
    EXPECT_EQ(sc::ExecuteQueryPlan(partialPlan, partialContext, &partialResult),
              sc::SC_OK);
    EXPECT_EQ(partialResult.mode, sc::QueryExecutionMode::PartialIndex);
    EXPECT_FALSE(partialResult.fallbackTriggered);
    EXPECT_EQ(partialResult.matchedRows, 2u);
    EXPECT_EQ(partialResult.returnedRows, 2u);
    EXPECT_FALSE(partialResult.usedIndexIds.empty());
}

TEST(QuerySqliteExecutorTests, ExecutorRejectsRequireIndexOnPartialPlans)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_QuerySqlite_Unsupported.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateQueryableBeamTable(db);
    SeedQueryableBeamRows(table, db);

    sc::QueryPlan plan;
    plan.target = sc::QueryTarget{L"Beam", sc::QueryTargetType::Table};
    plan.conditionGroups = {sc::QueryConditionGroup{
        sc::QueryLogicOperator::And,
        {sc::QueryCondition{L"Name",
                            sc::QueryConditionOperator::StartsWith,
                            {sc::SCValue::FromString(L"Al")}}}}};
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
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result),
              sc::SC_E_INVALIDARG);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::Unsupported);
    EXPECT_NE(result.executionNote.find(L"executor-unsupported:index-required"),
              std::wstring::npos);
}

TEST(QuerySqliteExecutorTests, SqliteExecutorRunsControlledFallbackScan)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_QuerySqlite_Fallback.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateQueryableBeamTable(db);
    SeedQueryableBeamRows(table, db);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(planner->BuildPlan(
                  sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                  {sc::QueryConditionGroup{
                      sc::QueryLogicOperator::And,
                      {sc::QueryCondition{L"Name",
                                          sc::QueryConditionOperator::Contains,
                                          {sc::SCValue::FromString(L"Al")}}}}},
                  sc::QueryLogicOperator::And, {}, {}, {}, {}, &plan),
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
    EXPECT_NE(result.executionNote.find(L"planner-fallback"),
              std::wstring::npos);
}
