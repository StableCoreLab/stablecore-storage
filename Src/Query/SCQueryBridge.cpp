#include "SCQuery.h"

#include <limits>
#include <utility>

namespace StableCore::Storage
{

    ErrorCode SCQueryBridge::BuildPlanFromLegacyFindRecords(
        const std::wstring& targetName, const SCQueryCondition& condition,
        QueryPlan* outPlan)
    {
        if (outPlan == nullptr)
        {
            return SC_E_POINTER;
        }

        if (targetName.empty() || condition.fieldName.empty())
        {
            return SC_E_INVALIDARG;
        }

        QueryPlan plan;
        plan.target.name = targetName;
        plan.target.type = QueryTargetType::Table;
        plan.conditionGroupLogic = QueryLogicOperator::And;

        QueryConditionGroup group;
        group.logic = QueryLogicOperator::And;
        group.conditions.push_back(QueryCondition{
            condition.fieldName,
            QueryConditionOperator::Equal,
            {condition.expectedValue},
        });
        plan.conditionGroups.push_back(std::move(group));

        plan.planId = std::wstring(L"legacy.findrecords:") + targetName;
        plan.hints.preferIndex = true;
        plan.hints.needReferenceInfo = false;
        plan.hints.requireSummaryOnly = false;
        plan.constraints.allowFallbackScan = true;
        plan.constraints.requireIndex = false;
        plan.constraints.allowPartial = true;
        plan.page.offset = 0;
        plan.page.limit = std::numeric_limits<std::uint64_t>::max();
        plan.state = QueryPlanState::Unsupported;
        plan.fallbackReason = L"legacy-bridge-normalized-only";
        plan.pushdownConditionCount = 1;

        *outPlan = std::move(plan);
        return SC_OK;
    }

    ErrorCode SCQueryBridge::AdaptExecutionResultToLegacyStatus(
        const QueryExecutionResult& executionResult, ErrorCode* outLegacyRc)
    {
        if (outLegacyRc == nullptr)
        {
            return SC_E_POINTER;
        }

        if (executionResult.mode == QueryExecutionMode::Unsupported &&
            executionResult.rc == SC_OK)
        {
            *outLegacyRc = SC_E_NOTIMPL;
            return SC_OK;
        }

        *outLegacyRc = executionResult.rc;
        return SC_OK;
    }

}  // namespace StableCore::Storage
