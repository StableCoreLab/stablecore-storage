#include <gtest/gtest.h>

#include "ISCQuery.h"

using namespace StableCore::Storage;

TEST(QueryPlannerBridgeTests, LegacyFindRecordsPlanIsNormalized)
{
    QueryPlan plan;
    const ErrorCode rc = SCQueryBridge::BuildPlanFromLegacyFindRecords(
        L"Beam",
        SCQueryCondition{L"Width", SCValue::FromInt64(300)},
        &plan);

    EXPECT_EQ(rc, SC_OK);
    EXPECT_EQ(plan.target.name, L"Beam");
    EXPECT_EQ(plan.target.type, QueryTargetType::Table);
    EXPECT_EQ(plan.conditionGroupLogic, QueryLogicOperator::And);
    ASSERT_EQ(plan.conditionGroups.size(), 1u);
    EXPECT_EQ(plan.conditionGroups.front().logic, QueryLogicOperator::And);
    ASSERT_EQ(plan.conditionGroups.front().conditions.size(), 1u);
    EXPECT_EQ(plan.conditionGroups.front().conditions.front().fieldName, L"Width");
    EXPECT_EQ(plan.conditionGroups.front().conditions.front().op, QueryConditionOperator::Equal);
    EXPECT_EQ(plan.state, QueryPlanState::Unsupported);
}

TEST(QueryPlannerBridgeTests, PlannerClassifiesDirectIndex)
{
    auto planner = CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    QueryPlan plan;
    const ErrorCode rc = planner->BuildPlan(
        QueryTarget{L"Beam", QueryTargetType::Table},
        {QueryConditionGroup{QueryLogicOperator::And, {QueryCondition{L"Width", QueryConditionOperator::Equal, {SCValue::FromInt64(300)}}}}},
        QueryLogicOperator::And,
        {},
        {},
        {},
        {},
        &plan);

    EXPECT_EQ(rc, SC_OK);
    EXPECT_EQ(plan.state, QueryPlanState::DirectIndex);
    EXPECT_EQ(plan.pushdownConditionCount, 1u);
    EXPECT_EQ(plan.fallbackConditionCount, 0u);
}

TEST(QueryPlannerBridgeTests, PlannerClassifiesPartialIndex)
{
    auto planner = CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    QueryPlan plan;
    const ErrorCode rc = planner->BuildPlan(
        QueryTarget{L"Beam", QueryTargetType::Table},
        {QueryConditionGroup{QueryLogicOperator::And, {QueryCondition{L"Name", QueryConditionOperator::StartsWith, {SCValue::FromString(L"AB")}}}}},
        QueryLogicOperator::And,
        {},
        {},
        {},
        {},
        &plan);

    EXPECT_EQ(rc, SC_OK);
    EXPECT_EQ(plan.state, QueryPlanState::PartialIndex);
    EXPECT_EQ(plan.fallbackConditionCount, 1u);
}

TEST(QueryPlannerBridgeTests, PlannerClassifiesFallbackScanForOrGroups)
{
    auto planner = CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    QueryPlan plan;
    const ErrorCode rc = planner->BuildPlan(
        QueryTarget{L"Beam", QueryTargetType::Table},
        {
            QueryConditionGroup{QueryLogicOperator::And, {QueryCondition{L"Width", QueryConditionOperator::Equal, {SCValue::FromInt64(300)}}}},
            QueryConditionGroup{QueryLogicOperator::And, {QueryCondition{L"Height", QueryConditionOperator::Equal, {SCValue::FromInt64(200)}}}},
        },
        QueryLogicOperator::Or,
        {},
        {},
        {},
        {},
        &plan);

    EXPECT_EQ(rc, SC_OK);
    EXPECT_EQ(plan.state, QueryPlanState::ScanFallback);
    EXPECT_TRUE(plan.fallbackReason.find(L"multi-group-or") != std::wstring::npos);
}

TEST(QueryPlannerBridgeTests, PlannerCombinesMultipleGroupsWithAndAsDirectIndex)
{
    auto planner = CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    QueryPlan plan;
    const ErrorCode rc = planner->BuildPlan(
        QueryTarget{L"Beam", QueryTargetType::Table},
        {
            QueryConditionGroup{QueryLogicOperator::And, {QueryCondition{L"Width", QueryConditionOperator::Equal, {SCValue::FromInt64(300)}}}},
            QueryConditionGroup{QueryLogicOperator::And, {QueryCondition{L"Height", QueryConditionOperator::GreaterThan, {SCValue::FromInt64(100)}}}},
        },
        QueryLogicOperator::And,
        {},
        {},
        {},
        {},
        &plan);

    EXPECT_EQ(rc, SC_OK);
    EXPECT_EQ(plan.state, QueryPlanState::DirectIndex);
    EXPECT_EQ(plan.pushdownConditionCount, 2u);
    EXPECT_EQ(plan.fallbackConditionCount, 0u);
}

TEST(QueryPlannerBridgeTests, PlannerRejectsUnsupportedWhenFallbackDisallowed)
{
    auto planner = CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    QueryPlan plan;
    QueryConstraints constraints;
    constraints.allowFallbackScan = false;

    const ErrorCode rc = planner->BuildPlan(
        QueryTarget{L"Beam", QueryTargetType::Table},
        {QueryConditionGroup{QueryLogicOperator::And, {QueryCondition{L"Name", QueryConditionOperator::Contains, {SCValue::FromString(L"AB")}}}}},
        QueryLogicOperator::And,
        {},
        {},
        {},
        constraints,
        &plan);

    EXPECT_EQ(rc, SC_OK);
    EXPECT_EQ(plan.state, QueryPlanState::Unsupported);
    EXPECT_TRUE(plan.fallbackReason.find(L"fallback-disallowed") != std::wstring::npos);
}

TEST(QueryPlannerBridgeTests, ExecutionResultCanBeAdaptedToLegacyStatus)
{
    QueryExecutionResult executionResult;
    executionResult.rc = SC_OK;
    executionResult.mode = QueryExecutionMode::Unsupported;

    ErrorCode legacyRc = SC_OK;
    const ErrorCode rc = SCQueryBridge::AdaptExecutionResultToLegacyStatus(executionResult, &legacyRc);

    EXPECT_EQ(rc, SC_OK);
    EXPECT_EQ(legacyRc, SC_E_NOTIMPL);
}
