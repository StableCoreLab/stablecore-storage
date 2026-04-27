#include <gtest/gtest.h>

#include "ISCQuery.h"
#include "SCStorage.h"

namespace sc = StableCore::Storage;

namespace
{

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

    sc::QueryExecutionContext MakeContext(sc::SCDbPtr& db)
    {
        sc::QueryExecutionContext context;
        context.database = db.Get();
        context.backendHandle = db.Get();
        context.backendKind = sc::QueryBackendKind::Memory;
        return context;
    }

}  // namespace

TEST(QueryMemoryExecutorTests, ContextDispatchWorksForMemory)
{
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

    sc::SCTablePtr table = CreateQueryableBeamTable(db);
    SeedQueryableBeamRows(table, db);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(planner->BuildPlan(
                  sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                  {sc::QueryConditionGroup{
                      sc::QueryLogicOperator::And,
                      {sc::QueryCondition{L"Width",
                                          sc::QueryConditionOperator::Equal,
                                          {sc::SCValue::FromInt64(200)}}}}},
                  sc::QueryLogicOperator::And, {}, {}, {}, {}, &plan),
              sc::SC_OK);

    sc::QueryExecutionContext context = MakeContext(db);
    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionResult result;
    context.resultCursor = &cursor;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(result.unsupportedSource, sc::QueryUnsupportedSource::None);
    EXPECT_EQ(result.matchedRows, 1u);
    EXPECT_EQ(result.returnedRows, 1u);
    EXPECT_FALSE(result.usedIndexIds.empty());
}

TEST(QueryMemoryExecutorTests,
     MemoryExecutorReportsExecutorUnsupportedForRequireIndex)
{
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

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
    sc::QueryExecutionContext context = MakeContext(db);
    context.resultCursor = &cursor;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result),
              sc::SC_E_INVALIDARG);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::Unsupported);
    EXPECT_EQ(result.unsupportedSource, sc::QueryUnsupportedSource::Executor);
    EXPECT_NE(result.executionNote.find(L"executor-unsupported:index-required"),
              std::wstring::npos);
}

TEST(QueryMemoryExecutorTests, MemoryExecutorRunsControlledFallbackScan)
{
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

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
    sc::QueryExecutionContext context = MakeContext(db);
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
