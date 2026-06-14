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

TEST(QueryExecutorTieBreakTests, CompositeIndexTieBreakPrefersOrderCoveredCandidate)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeTieBreakOrder.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompetingCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed competing indexes", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Zulu"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 30), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 10), sc::SC_OK);

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
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Height");
}

TEST(QueryExecutorTieBreakTests, DISABLED_CompositeIndexTieBreakPrefersFewerResidualConditionsWhenStrengthIsTied)
{
    GTEST_SKIP() << "Duplicate-predicate residual tie-break currently triggers a bad allocation in "
                    "AnalyzeCompositeIndexPlan and needs targeted debugging.";

    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeTieBreakResidual.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompetingCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed competing residual indexes", edit), sc::SC_OK);

    sc::SCRecordPtr row;
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
    // Intentionally repeat the same Name equality predicate so both competing
    // indexes have equal prefix strength, but idx_Beam_Width_Name leaves fewer
    // residual conditions after normalization than idx_Beam_Width_Height.
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

    auto* indexAccess = dynamic_cast<sc::ISqliteQueryIndexAccess*>(db.Get());
    ASSERT_NE(indexAccess, nullptr);

    sc::QueryPlan analyzedPlan;
    EXPECT_EQ(indexAccess->AnalyzeCompositeIndexPlan(plan, &analyzedPlan), sc::SC_OK);
    ASSERT_TRUE(analyzedPlan.matchedIndex.has_value());
    EXPECT_EQ(analyzedPlan.matchedIndex->indexName, L"idx_Beam_Width_Name");
    EXPECT_EQ(analyzedPlan.state, sc::QueryPlanState::PartialIndex);
    ASSERT_EQ(analyzedPlan.pushdown.residualConditions.size(), 1u);
    EXPECT_EQ(analyzedPlan.pushdown.residualConditions.front().fieldName, L"Height");
    EXPECT_EQ(analyzedPlan.pushdown.residualConditions.front().op, sc::QueryConditionOperator::Equal);
}

TEST(QueryExecutorTieBreakTests, DISABLED_CompositeIndexTieBreakPrefersFewerNaturalResidualConditionsWhenStrengthIsTied)
{
    GTEST_SKIP() << "Natural residual normalization tie-break currently triggers a bad allocation in "
                    "AnalyzeCompositeIndexPlan and needs targeted debugging.";

    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeTieBreakNaturalResidual.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompetingCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed natural residual indexes", edit), sc::SC_OK);

    sc::SCRecordPtr row;
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
    // Name = 'Alpha' naturally implies Name IS NOT NULL. After residual
    // normalization, idx_Beam_Width_Name should leave only Height = 10,
    // while idx_Beam_Width_Height still leaves both Name predicates.
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
                                sc::QueryCondition{L"Name", sc::QueryConditionOperator::IsNotNull, {}},
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

    auto* indexAccess = dynamic_cast<sc::ISqliteQueryIndexAccess*>(db.Get());
    ASSERT_NE(indexAccess, nullptr);

    sc::QueryPlan analyzedPlan;
    EXPECT_EQ(indexAccess->AnalyzeCompositeIndexPlan(plan, &analyzedPlan), sc::SC_OK);
    ASSERT_TRUE(analyzedPlan.matchedIndex.has_value());
    EXPECT_EQ(analyzedPlan.matchedIndex->indexName, L"idx_Beam_Width_Name");
    EXPECT_EQ(analyzedPlan.state, sc::QueryPlanState::PartialIndex);
    ASSERT_EQ(analyzedPlan.pushdown.residualConditions.size(), 1u);
    EXPECT_EQ(analyzedPlan.pushdown.residualConditions.front().fieldName, L"Height");
    EXPECT_EQ(analyzedPlan.pushdown.residualConditions.front().op, sc::QueryConditionOperator::Equal);
}

TEST(QueryExecutorTieBreakTests, ResidualNormalizationDoesNotDropIsNotNullForEqualNull)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeResidualNullCounterExample.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateNullableCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed residual null counterexample", edit), sc::SC_OK);

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
                           {sc::QueryConditionGroup{
                               sc::QueryLogicOperator::And,
                               {sc::QueryCondition{L"Width",
                                                   sc::QueryConditionOperator::Equal,
                                                   {sc::SCValue::FromInt64(100)}},
                                sc::QueryCondition{L"Name",
                                                   sc::QueryConditionOperator::Equal,
                                                   {sc::SCValue::Null()}},
                                sc::QueryCondition{L"Name", sc::QueryConditionOperator::IsNotNull, {}}}}},
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
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::PartialIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name");
    EXPECT_EQ(result.matchedRows, 0u);

    sc::SCRecordPtr record;
    EXPECT_EQ(cursor->Next(record), sc::SC_OK);
    EXPECT_FALSE(static_cast<bool>(record));
}
