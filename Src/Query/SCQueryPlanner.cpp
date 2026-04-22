#include "ISCQuery.h"

#include <memory>
#include <sstream>
#include <utility>

namespace StableCore::Storage
{
namespace
{

enum class PlanStrength
{
    Direct,
    Partial,
    Fallback,
    Unsupported,
};

PlanStrength MaxStrength(PlanStrength lhs, PlanStrength rhs)
{
    if (lhs == PlanStrength::Unsupported || rhs == PlanStrength::Unsupported)
    {
        return PlanStrength::Unsupported;
    }
    if (lhs == PlanStrength::Fallback || rhs == PlanStrength::Fallback)
    {
        return PlanStrength::Fallback;
    }
    if (lhs == PlanStrength::Partial || rhs == PlanStrength::Partial)
    {
        return PlanStrength::Partial;
    }
    return PlanStrength::Direct;
}

bool HasRequiredArity(const QueryCondition& condition)
{
    const std::size_t count = condition.values.size();
    switch (condition.op)
    {
    case QueryConditionOperator::Equal:
    case QueryConditionOperator::NotEqual:
    case QueryConditionOperator::LessThan:
    case QueryConditionOperator::LessThanOrEqual:
    case QueryConditionOperator::GreaterThan:
    case QueryConditionOperator::GreaterThanOrEqual:
    case QueryConditionOperator::StartsWith:
    case QueryConditionOperator::Contains:
    case QueryConditionOperator::EndsWith:
        return count == 1;
    case QueryConditionOperator::Between:
        return count == 2;
    case QueryConditionOperator::In:
        return count >= 1;
    case QueryConditionOperator::IsNull:
    case QueryConditionOperator::IsNotNull:
        return count == 0;
    default:
        return false;
    }
}

PlanStrength ClassifyCondition(const QueryCondition& condition, std::wstring* outReason)
{
    if (condition.fieldName.empty())
    {
        if (outReason) *outReason = L"condition-field-empty";
        return PlanStrength::Unsupported;
    }

    if (!HasRequiredArity(condition))
    {
        if (outReason) *outReason = L"condition-arity-invalid";
        return PlanStrength::Unsupported;
    }

    switch (condition.op)
    {
    case QueryConditionOperator::Equal:
    case QueryConditionOperator::NotEqual:
    case QueryConditionOperator::LessThan:
    case QueryConditionOperator::LessThanOrEqual:
    case QueryConditionOperator::GreaterThan:
    case QueryConditionOperator::GreaterThanOrEqual:
    case QueryConditionOperator::IsNull:
    case QueryConditionOperator::IsNotNull:
        return PlanStrength::Direct;
    case QueryConditionOperator::In:
    case QueryConditionOperator::Between:
    case QueryConditionOperator::StartsWith:
        return PlanStrength::Partial;
    case QueryConditionOperator::Contains:
    case QueryConditionOperator::EndsWith:
        return PlanStrength::Fallback;
    default:
        if (outReason) *outReason = L"condition-operator-unsupported";
        return PlanStrength::Unsupported;
    }
}

PlanStrength ClassifyGroup(const QueryConditionGroup& group, std::uint32_t* pushdownCount, std::uint32_t* fallbackCount, std::wstring* outReason)
{
    if (group.conditions.empty())
    {
        if (outReason) *outReason = L"group-empty";
        return PlanStrength::Unsupported;
    }

    if (group.logic == QueryLogicOperator::Or && group.conditions.size() > 1)
    {
        for (const auto& condition : group.conditions)
        {
            std::wstring reason;
            const PlanStrength strength = ClassifyCondition(condition, &reason);
            if (strength == PlanStrength::Unsupported)
            {
                if (outReason) *outReason = reason;
                return PlanStrength::Unsupported;
            }
        }
        if (fallbackCount)
        {
            *fallbackCount += static_cast<std::uint32_t>(group.conditions.size());
        }
        if (outReason) *outReason = L"group-or-uses-fallback";
        return PlanStrength::Fallback;
    }

    PlanStrength strength = PlanStrength::Direct;
    for (const auto& condition : group.conditions)
    {
        std::wstring reason;
        const PlanStrength current = ClassifyCondition(condition, &reason);
        if (current == PlanStrength::Unsupported)
        {
            if (outReason) *outReason = reason;
            return PlanStrength::Unsupported;
        }
        if (current == PlanStrength::Direct && pushdownCount)
        {
            ++(*pushdownCount);
        }
        if (current != PlanStrength::Direct && fallbackCount)
        {
            ++(*fallbackCount);
        }
        strength = MaxStrength(strength, current);
    }

    return strength;
}

std::wstring MakePlanId(const QueryTarget& target, std::size_t groupCount, std::size_t orderCount)
{
    std::wstringstream stream;
    stream << L"plan:" << target.name << L":" << static_cast<unsigned long long>(groupCount)
           << L":" << static_cast<unsigned long long>(orderCount);
    return stream.str();
}

class DefaultQueryPlanner final : public IQueryPlanner
{
public:
    ErrorCode BuildPlan(const QueryTarget& target,
                        const std::vector<QueryConditionGroup>& conditionGroups,
                        QueryLogicOperator conditionGroupLogic,
                        const std::vector<SortSpec>& orderBy,
                        const QueryPage& page,
                        const QueryHints& hints,
                        const QueryConstraints& constraints,
                        QueryPlan* outPlan) const override
    {
        if (outPlan == nullptr)
        {
            return SC_E_POINTER;
        }

        QueryPlan plan;
        plan.target = target;
        plan.conditionGroups = conditionGroups;
        plan.conditionGroupLogic = conditionGroupLogic;
        plan.orderBy = orderBy;
        plan.page = page;
        plan.hints = hints;
        plan.constraints = constraints;
        plan.planId = MakePlanId(target, conditionGroups.size(), orderBy.size());

        if (target.name.empty())
        {
            plan.state = QueryPlanState::Unsupported;
            plan.fallbackReason = L"target-name-empty";
            *outPlan = std::move(plan);
            return SC_E_INVALIDARG;
        }

        for (const auto& sort : orderBy)
        {
            if (sort.fieldName.empty())
            {
                plan.state = QueryPlanState::Unsupported;
                plan.fallbackReason = L"sort-field-empty";
                *outPlan = std::move(plan);
                return SC_E_INVALIDARG;
            }
        }

        std::uint32_t pushdownCount = 0;
        std::uint32_t fallbackCount = 0;
        PlanStrength strength = PlanStrength::Direct;

        if (conditionGroups.empty())
        {
            if (!constraints.allowFallbackScan || constraints.requireIndex)
            {
                plan.state = QueryPlanState::Unsupported;
                plan.fallbackReason = L"no-conditions-and-fallback-disallowed";
                *outPlan = std::move(plan);
                return SC_OK;
            }
            strength = PlanStrength::Fallback;
            plan.fallbackReason = L"no-conditions";
        }
        else if (conditionGroupLogic == QueryLogicOperator::Or && conditionGroups.size() > 1)
        {
            for (const auto& group : conditionGroups)
            {
                std::wstring reason;
                const PlanStrength groupStrength = ClassifyGroup(group, &pushdownCount, &fallbackCount, &reason);
                if (groupStrength == PlanStrength::Unsupported)
                {
                    plan.state = QueryPlanState::Unsupported;
                    plan.fallbackReason = reason;
                    *outPlan = std::move(plan);
                    return SC_OK;
                }
            }

            if (!constraints.allowFallbackScan || constraints.requireIndex)
            {
                plan.state = QueryPlanState::Unsupported;
                plan.fallbackReason = L"multi-group-or-requires-fallback";
                *outPlan = std::move(plan);
                return SC_OK;
            }

            strength = PlanStrength::Fallback;
            plan.fallbackReason = L"multi-group-or";
        }
        else
        {
            for (const auto& group : conditionGroups)
            {
                std::wstring reason;
                const PlanStrength groupStrength = ClassifyGroup(group, &pushdownCount, &fallbackCount, &reason);
                if (groupStrength == PlanStrength::Unsupported)
                {
                    plan.state = QueryPlanState::Unsupported;
                    plan.fallbackReason = reason;
                    *outPlan = std::move(plan);
                    return SC_OK;
                }
                strength = MaxStrength(strength, groupStrength);
            }
        }

        if (strength == PlanStrength::Partial && !constraints.allowPartial)
        {
            plan.state = QueryPlanState::Unsupported;
            plan.fallbackReason = L"partial-disallowed";
            *outPlan = std::move(plan);
            return SC_OK;
        }

        if (strength == PlanStrength::Fallback && !constraints.allowFallbackScan)
        {
            plan.state = QueryPlanState::Unsupported;
            plan.fallbackReason = L"fallback-disallowed";
            *outPlan = std::move(plan);
            return SC_OK;
        }

        if (constraints.requireIndex && strength != PlanStrength::Direct)
        {
            plan.state = QueryPlanState::Unsupported;
            plan.fallbackReason = L"index-required";
            *outPlan = std::move(plan);
            return SC_OK;
        }

        switch (strength)
        {
        case PlanStrength::Direct:
            plan.state = QueryPlanState::DirectIndex;
            plan.fallbackReason.clear();
            break;
        case PlanStrength::Partial:
            plan.state = QueryPlanState::PartialIndex;
            break;
        case PlanStrength::Fallback:
            plan.state = QueryPlanState::ScanFallback;
            break;
        case PlanStrength::Unsupported:
        default:
            plan.state = QueryPlanState::Unsupported;
            break;
        }

        plan.pushdownConditionCount = pushdownCount;
        plan.fallbackConditionCount = fallbackCount;
        plan.estimatedRows = conditionGroups.empty() ? 0 : 1;
        plan.estimatedCost = pushdownCount + (fallbackCount * 10);

        *outPlan = std::move(plan);
        return SC_OK;
    }
};

}  // namespace

std::unique_ptr<IQueryPlanner> CreateDefaultQueryPlanner()
{
    return std::make_unique<DefaultQueryPlanner>();
}

}  // namespace StableCore::Storage
