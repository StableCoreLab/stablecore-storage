#include "SCQueryMemoryExecutor.h"

#include <algorithm>
#include <limits>
#include <sstream>
#include <unordered_set>
#include <utility>

#include "SCRefCounted.h"

namespace StableCore::Storage
{
namespace
{

constexpr std::uint64_t kDefaultQueryPageLimit = 1000;

class QueryResultCursor final : public ISCRecordCursor, public SCRefCountedObject
{
public:
    explicit QueryResultCursor(std::vector<SCRecordPtr> records)
        : records_(std::move(records))
    {
    }

    ErrorCode MoveNext(bool* outHasValue) override
    {
        if (outHasValue == nullptr)
        {
            return SC_E_POINTER;
        }

        if (index_ < records_.size())
        {
            current_ = records_[index_++];
            *outHasValue = true;
            return SC_OK;
        }

        current_.Reset();
        *outHasValue = false;
        return SC_OK;
    }

    ErrorCode GetCurrent(SCRecordPtr& outRecord) override
    {
        if (!current_)
        {
            return SC_FALSE_RESULT;
        }

        outRecord = current_;
        return SC_OK;
    }

private:
    std::vector<SCRecordPtr> records_;
    std::size_t index_{0};
    SCRecordPtr current_;
};

bool IsResidualOnlyOperator(QueryConditionOperator op)
{
    return op == QueryConditionOperator::Contains || op == QueryConditionOperator::EndsWith;
}

bool IsPushdownFriendlyOperator(QueryConditionOperator op)
{
    switch (op)
    {
    case QueryConditionOperator::Equal:
    case QueryConditionOperator::NotEqual:
    case QueryConditionOperator::LessThan:
    case QueryConditionOperator::LessThanOrEqual:
    case QueryConditionOperator::GreaterThan:
    case QueryConditionOperator::GreaterThanOrEqual:
    case QueryConditionOperator::IsNull:
    case QueryConditionOperator::IsNotNull:
    case QueryConditionOperator::In:
    case QueryConditionOperator::Between:
    case QueryConditionOperator::StartsWith:
        return true;
    case QueryConditionOperator::Contains:
    case QueryConditionOperator::EndsWith:
    default:
        return false;
    }
}

std::optional<long double> ToNumeric(const SCValue& value)
{
    std::int64_t intValue = 0;
    if (value.AsInt64(&intValue) == SC_OK)
    {
        return static_cast<long double>(intValue);
    }

    double doubleValue = 0.0;
    if (value.AsDouble(&doubleValue) == SC_OK)
    {
        return static_cast<long double>(doubleValue);
    }

    RecordId recordId = 0;
    if (value.AsRecordId(&recordId) == SC_OK)
    {
        return static_cast<long double>(recordId);
    }

    return std::nullopt;
}

std::wstring ToStringValue(const SCValue& value, bool* outOk)
{
    std::wstring text;
    if (value.AsStringCopy(&text) == SC_OK)
    {
        if (outOk) *outOk = true;
        return text;
    }

    if (const auto* enumValue = value.TryGet<SCEnumValue>())
    {
        if (outOk) *outOk = true;
        return enumValue->value;
    }

    if (outOk) *outOk = false;
    return {};
}

bool CompareValues(const SCValue& lhs, const SCValue& rhs, QueryConditionOperator op)
{
    if (lhs.IsNull() || rhs.IsNull())
    {
        switch (op)
        {
        case QueryConditionOperator::Equal:
            return lhs.IsNull() && rhs.IsNull();
        case QueryConditionOperator::NotEqual:
            return !(lhs.IsNull() && rhs.IsNull());
        default:
            return false;
        }
    }

    const std::optional<long double> lhsNumeric = ToNumeric(lhs);
    const std::optional<long double> rhsNumeric = ToNumeric(rhs);
    if (lhsNumeric.has_value() && rhsNumeric.has_value())
    {
        switch (op)
        {
        case QueryConditionOperator::Equal: return *lhsNumeric == *rhsNumeric;
        case QueryConditionOperator::NotEqual: return *lhsNumeric != *rhsNumeric;
        case QueryConditionOperator::LessThan: return *lhsNumeric < *rhsNumeric;
        case QueryConditionOperator::LessThanOrEqual: return *lhsNumeric <= *rhsNumeric;
        case QueryConditionOperator::GreaterThan: return *lhsNumeric > *rhsNumeric;
        case QueryConditionOperator::GreaterThanOrEqual: return *lhsNumeric >= *rhsNumeric;
        default: break;
        }
    }

    bool lhsOk = false;
    bool rhsOk = false;
    const std::wstring lhsText = ToStringValue(lhs, &lhsOk);
    const std::wstring rhsText = ToStringValue(rhs, &rhsOk);
    if (lhsOk && rhsOk)
    {
        switch (op)
        {
        case QueryConditionOperator::Equal: return lhsText == rhsText;
        case QueryConditionOperator::NotEqual: return lhsText != rhsText;
        case QueryConditionOperator::LessThan: return lhsText < rhsText;
        case QueryConditionOperator::LessThanOrEqual: return lhsText <= rhsText;
        case QueryConditionOperator::GreaterThan: return lhsText > rhsText;
        case QueryConditionOperator::GreaterThanOrEqual: return lhsText >= rhsText;
        default: break;
        }
    }

    switch (op)
    {
    case QueryConditionOperator::Equal:
        return lhs == rhs;
    case QueryConditionOperator::NotEqual:
        return !(lhs == rhs);
    default:
        return false;
    }
}

bool EvaluateCondition(const SCValue& actual, const QueryCondition& condition)
{
    switch (condition.op)
    {
    case QueryConditionOperator::Equal:
    case QueryConditionOperator::NotEqual:
    case QueryConditionOperator::LessThan:
    case QueryConditionOperator::LessThanOrEqual:
    case QueryConditionOperator::GreaterThan:
    case QueryConditionOperator::GreaterThanOrEqual:
        return !condition.values.empty() && CompareValues(actual, condition.values.front(), condition.op);

    case QueryConditionOperator::In:
        for (const auto& candidate : condition.values)
        {
            if (CompareValues(actual, candidate, QueryConditionOperator::Equal))
            {
                return true;
            }
        }
        return false;

    case QueryConditionOperator::Between:
        if (condition.values.size() < 2)
        {
            return false;
        }
        return CompareValues(actual, condition.values[0], QueryConditionOperator::GreaterThanOrEqual)
            && CompareValues(actual, condition.values[1], QueryConditionOperator::LessThanOrEqual);

    case QueryConditionOperator::IsNull:
        return actual.IsNull();

    case QueryConditionOperator::IsNotNull:
        return !actual.IsNull();

    case QueryConditionOperator::StartsWith:
    case QueryConditionOperator::Contains:
    case QueryConditionOperator::EndsWith:
    {
        bool ok = false;
        const std::wstring actualText = ToStringValue(actual, &ok);
        if (!ok || condition.values.empty())
        {
            return false;
        }

        bool expectedOk = false;
        const std::wstring expectedText = ToStringValue(condition.values.front(), &expectedOk);
        if (!expectedOk)
        {
            return false;
        }

        if (condition.op == QueryConditionOperator::StartsWith)
        {
            return actualText.size() >= expectedText.size()
                && actualText.compare(0, expectedText.size(), expectedText) == 0;
        }
        if (condition.op == QueryConditionOperator::Contains)
        {
            return actualText.find(expectedText) != std::wstring::npos;
        }
        return actualText.size() >= expectedText.size()
            && actualText.compare(actualText.size() - expectedText.size(), expectedText.size(), expectedText) == 0;
    }
    }

    return false;
}

bool EvaluateConditionOnRecord(const SCRecordPtr& record, const QueryCondition& condition)
{
    SCValue actual = SCValue::Null();
    const ErrorCode rc = record->GetValue(condition.fieldName.c_str(), &actual);
    if (rc == SC_E_VALUE_IS_NULL)
    {
        actual = SCValue::Null();
    }
    else if (Failed(rc))
    {
        return false;
    }

    return EvaluateCondition(actual, condition);
}

bool EvaluateGroupPhase(
    const SCRecordPtr& record,
    const QueryConditionGroup& group,
    bool residualPhase)
{
    const auto matchesPhase = [residualPhase](QueryConditionOperator op)
    {
        return residualPhase ? IsResidualOnlyOperator(op) : IsPushdownFriendlyOperator(op);
    };

    bool hasRelevantCondition = false;
    bool result = (group.logic == QueryLogicOperator::And);
    for (const auto& condition : group.conditions)
    {
        if (!matchesPhase(condition.op))
        {
            continue;
        }

        hasRelevantCondition = true;
        const bool matched = EvaluateConditionOnRecord(record, condition);
        if (group.logic == QueryLogicOperator::And)
        {
            result = result && matched;
            if (!result)
            {
                return false;
            }
        }
        else
        {
            result = result || matched;
            if (result)
            {
                return true;
            }
        }
    }

    if (!hasRelevantCondition)
    {
        return true;
    }
    return result;
}

bool EvaluatePlanForRecord(const SCRecordPtr& record, const QueryPlan& plan)
{
    if (plan.conditionGroups.empty())
    {
        return true;
    }

    auto evaluateGroup = [&plan, &record](const QueryConditionGroup& group) -> bool
    {
        const bool pushdown = EvaluateGroupPhase(record, group, false);
        if (!pushdown)
        {
            return false;
        }

        if (plan.state == QueryPlanState::ScanFallback || plan.state == QueryPlanState::PartialIndex)
        {
            return EvaluateGroupPhase(record, group, true);
        }

        for (const auto& condition : group.conditions)
        {
            if (IsResidualOnlyOperator(condition.op))
            {
                return false;
            }
        }
        return true;
    };

    if (plan.conditionGroupLogic == QueryLogicOperator::And)
    {
        for (const auto& group : plan.conditionGroups)
        {
            if (!evaluateGroup(group))
            {
                return false;
            }
        }
        return true;
    }

    for (const auto& group : plan.conditionGroups)
    {
        if (evaluateGroup(group))
        {
            return true;
        }
    }
    return false;
}

std::wstring BuildPlanDescriptor(const QueryPlan& plan)
{
    std::wstringstream text;
    text << L"memory-executor:" << plan.target.name;
    text << L":groups=" << static_cast<unsigned long long>(plan.conditionGroups.size());
    text << L":mode=" << static_cast<int>(plan.state);
    return text.str();
}

std::wstring BuildFallbackNote(const QueryPlan& plan, QueryFallbackSource source)
{
    const std::wstring prefix = (source == QueryFallbackSource::Planner)
        ? L"planner-fallback:"
        : L"executor-fallback:";
    const std::wstring reason = plan.fallbackReason.empty() ? L"scan-fallback" : plan.fallbackReason;
    return prefix + reason;
}

std::wstring BuildIndexId(const std::wstring& tableName, const std::wstring& fieldName)
{
    std::wstringstream text;
    text << L"mem-idx_" << tableName << L"_" << fieldName;
    return text.str();
}

void AppendUnique(std::vector<std::wstring>* ids, const std::wstring& id)
{
    if (ids == nullptr || id.empty())
    {
        return;
    }

    if (std::find(ids->begin(), ids->end(), id) == ids->end())
    {
        ids->push_back(id);
    }
}

ErrorCode CollectCandidateRecords(
    ISCDatabase* database,
    const QueryPlan& plan,
    std::vector<SCRecordPtr>* outMatchedRecords,
    QueryExecutionResult* outResult)
{
    if (database == nullptr || outMatchedRecords == nullptr || outResult == nullptr)
    {
        return SC_E_POINTER;
    }

    SCTablePtr table;
    const ErrorCode tableRc = database->GetTable(plan.target.name.c_str(), table);
    if (Failed(tableRc))
    {
        return tableRc;
    }

    SCSchemaPtr schema;
    const ErrorCode schemaRc = table->GetSchema(schema);
    if (Failed(schemaRc))
    {
        return schemaRc;
    }

    SCRecordCursorPtr cursor;
    const ErrorCode enumerateRc = table->EnumerateRecords(cursor);
    if (Failed(enumerateRc))
    {
        return enumerateRc;
    }

    std::unordered_set<std::wstring> usedIndexSet;
    for (const auto& group : plan.conditionGroups)
    {
        for (const auto& condition : group.conditions)
        {
            if (!IsPushdownFriendlyOperator(condition.op))
            {
                continue;
            }

            SCColumnDef column;
            if (schema->FindColumn(condition.fieldName.c_str(), &column) == SC_OK && column.indexed)
            {
                usedIndexSet.insert(BuildIndexId(plan.target.name, condition.fieldName));
            }
        }
    }

    std::vector<SCRecordPtr> matched;
    bool hasRow = false;
    std::uint64_t scannedRows = 0;
    while (cursor->MoveNext(&hasRow) == SC_OK && hasRow)
    {
        ++scannedRows;
        SCRecordPtr record;
        const ErrorCode currentRc = cursor->GetCurrent(record);
        if (Failed(currentRc))
        {
            return currentRc;
        }

        if (EvaluatePlanForRecord(record, plan))
        {
            matched.push_back(record);
        }
    }

    outResult->scannedRows = scannedRows;
    outResult->matchedRows = matched.size();
    outResult->usedIndexIds.assign(usedIndexSet.begin(), usedIndexSet.end());
    *outMatchedRecords = std::move(matched);
    return SC_OK;
}

void SortMatchedRecords(std::vector<SCRecordPtr>* records, const QueryPlan& plan)
{
    if (records == nullptr || records->empty() || plan.orderBy.empty())
    {
        return;
    }

    std::stable_sort(
        records->begin(),
        records->end(),
        [&plan](const SCRecordPtr& lhs, const SCRecordPtr& rhs)
        {
            for (const auto& sort : plan.orderBy)
            {
                SCValue left = SCValue::Null();
                SCValue right = SCValue::Null();
                lhs->GetValue(sort.fieldName.c_str(), &left);
                rhs->GetValue(sort.fieldName.c_str(), &right);

                if (left == right)
                {
                    continue;
                }

                const bool lhsLessThanRhs = CompareValues(left, right, QueryConditionOperator::LessThan);
                const bool rhsLessThanLhs = CompareValues(right, left, QueryConditionOperator::LessThan);

                if (sort.direction == QueryOrderDirection::Ascending && lhsLessThanRhs)
                {
                    return true;
                }
                if (sort.direction == QueryOrderDirection::Ascending && rhsLessThanLhs)
                {
                    return false;
                }
                if (sort.direction == QueryOrderDirection::Descending && lhsLessThanRhs)
                {
                    return false;
                }
                if (sort.direction == QueryOrderDirection::Descending && rhsLessThanLhs)
                {
                    return true;
                }
            }
            return lhs->GetId() < rhs->GetId();
        });
}

ErrorCode MaterializeCursor(
    std::vector<SCRecordPtr> matchedRecords,
    const QueryPlan& plan,
    SCRecordCursorPtr* outCursor,
    QueryExecutionResult* outResult)
{
    SortMatchedRecords(&matchedRecords, plan);

    std::uint64_t limit = plan.page.limit;
    if (limit == 0)
    {
        limit = kDefaultQueryPageLimit;
    }
    const bool unlimited = plan.page.limit == std::numeric_limits<std::uint64_t>::max();
    const std::uint64_t offset = plan.page.offset;

    std::vector<SCRecordPtr> pageRecords;
    pageRecords.reserve(matchedRecords.size());
    for (std::uint64_t index = 0; index < matchedRecords.size(); ++index)
    {
        if (index < offset)
        {
            continue;
        }
        if (!unlimited && pageRecords.size() >= limit)
        {
            break;
        }
        pageRecords.push_back(matchedRecords[static_cast<std::size_t>(index)]);
    }

    outResult->returnedRows = pageRecords.size();
    if (outCursor != nullptr)
    {
        *outCursor = SCMakeRef<QueryResultCursor>(std::move(pageRecords));
    }
    return SC_OK;
}

ErrorCode ExecuteControlledFallbackScan(
    ISCDatabase* database,
    const QueryPlan& plan,
    QueryExecutionContext context,
    QueryExecutionResult* outResult)
{
    if (database == nullptr || outResult == nullptr)
    {
        return SC_E_POINTER;
    }

    SCTablePtr table;
    const ErrorCode tableRc = database->GetTable(plan.target.name.c_str(), table);
    if (Failed(tableRc))
    {
        return tableRc;
    }

    SCRecordCursorPtr cursor;
    const ErrorCode enumerateRc = table->EnumerateRecords(cursor);
    if (Failed(enumerateRc))
    {
        return enumerateRc;
    }

    SCSchemaPtr schema;
    const ErrorCode schemaRc = table->GetSchema(schema);
    if (Failed(schemaRc))
    {
        return schemaRc;
    }

    std::unordered_set<std::wstring> usedIndexSet;
    for (const auto& group : plan.conditionGroups)
    {
        for (const auto& condition : group.conditions)
        {
            if (!IsPushdownFriendlyOperator(condition.op))
            {
                continue;
            }

            SCColumnDef column;
            if (schema->FindColumn(condition.fieldName.c_str(), &column) == SC_OK && column.indexed)
            {
                usedIndexSet.insert(BuildIndexId(plan.target.name, condition.fieldName));
            }
        }
    }

    const bool needsSort = !plan.orderBy.empty();
    const bool unlimited = plan.page.limit == std::numeric_limits<std::uint64_t>::max();
    const std::uint64_t pageLimit = (plan.page.limit == 0) ? kDefaultQueryPageLimit : plan.page.limit;
    const std::uint64_t pageOffset = plan.page.offset;

    std::vector<SCRecordPtr> matchedRecords;
    std::vector<SCRecordPtr> pageRecords;
    if (needsSort)
    {
        matchedRecords.reserve(64);
    }
    else
    {
        pageRecords.reserve(static_cast<std::size_t>(pageLimit == 0 ? kDefaultQueryPageLimit : pageLimit));
    }

    bool hasRow = false;
    std::uint64_t scannedRows = 0;
    std::uint64_t matchedRows = 0;
    while (cursor->MoveNext(&hasRow) == SC_OK && hasRow)
    {
        ++scannedRows;
        SCRecordPtr record;
        const ErrorCode currentRc = cursor->GetCurrent(record);
        if (Failed(currentRc))
        {
            return currentRc;
        }

        if (!EvaluatePlanForRecord(record, plan))
        {
            continue;
        }

        ++matchedRows;
        if (needsSort)
        {
            matchedRecords.push_back(record);
            continue;
        }

        if (matchedRows <= pageOffset)
        {
            continue;
        }

        if (!unlimited && pageRecords.size() >= pageLimit)
        {
            continue;
        }

        pageRecords.push_back(record);
    }

    outResult->scannedRows = scannedRows;
    outResult->matchedRows = matchedRows;
    outResult->usedIndexIds.assign(usedIndexSet.begin(), usedIndexSet.end());
    outResult->mode = QueryExecutionMode::FallbackScan;
    outResult->fallbackTriggered = true;
    outResult->fallbackSource = plan.state == QueryPlanState::ScanFallback
        ? QueryFallbackSource::Planner
        : QueryFallbackSource::Executor;
    outResult->fallbackReason = plan.fallbackReason;

    if (needsSort)
    {
        SortMatchedRecords(&matchedRecords, plan);
        std::vector<SCRecordPtr> paged;
        paged.reserve(matchedRecords.size());
        for (std::uint64_t index = 0; index < matchedRecords.size(); ++index)
        {
            if (index < pageOffset)
            {
                continue;
            }
            if (!unlimited && paged.size() >= pageLimit)
            {
                break;
            }
            paged.push_back(matchedRecords[static_cast<std::size_t>(index)]);
        }

        outResult->returnedRows = paged.size();
        if (context.resultCursor != nullptr)
        {
            *static_cast<SCRecordCursorPtr*>(context.resultCursor) = SCMakeRef<QueryResultCursor>(std::move(paged));
        }
    }
    else
    {
        outResult->returnedRows = pageRecords.size();
        if (context.resultCursor != nullptr)
        {
            *static_cast<SCRecordCursorPtr*>(context.resultCursor) = SCMakeRef<QueryResultCursor>(std::move(pageRecords));
        }
    }

    outResult->executionNote = BuildFallbackNote(plan, outResult->fallbackSource);
    outResult->executionNote += L" | " + BuildPlanDescriptor(plan);
    return SC_OK;
}

ErrorCode DispatchMemoryQuery(
    const QueryPlan& plan,
    const QueryExecutionContext& context,
    QueryExecutionResult* outResult)
{
    if (outResult == nullptr)
    {
        return SC_E_POINTER;
    }

    QueryExecutionResult result;
    result.rc = SC_OK;
    result.mode = QueryExecutionMode::Unsupported;

    if (context.database == nullptr)
    {
        result.unsupportedSource = QueryUnsupportedSource::Executor;
        result.executionNote = L"executor-unsupported:database-null";
        *outResult = std::move(result);
        return SC_E_POINTER;
    }

    if (plan.target.type != QueryTargetType::Table)
    {
        result.unsupportedSource = QueryUnsupportedSource::Executor;
        result.executionNote = L"executor-unsupported:view-target-not-enabled";
        *outResult = std::move(result);
        return SC_E_NOTIMPL;
    }

    if (plan.constraints.requireIndex && plan.state != QueryPlanState::DirectIndex)
    {
        result.unsupportedSource = QueryUnsupportedSource::Executor;
        result.executionNote = L"executor-unsupported:index-required";
        *outResult = std::move(result);
        return SC_E_INVALIDARG;
    }

    if (plan.state == QueryPlanState::Unsupported)
    {
        result.unsupportedSource = QueryUnsupportedSource::Planner;
        result.executionNote = L"planner-unsupported";
        result.rc = SC_E_INVALIDARG;
        *outResult = std::move(result);
        return SC_E_INVALIDARG;
    }

    if (plan.state == QueryPlanState::ScanFallback && !plan.constraints.allowFallbackScan)
    {
        result.unsupportedSource = QueryUnsupportedSource::Executor;
        result.executionNote = L"executor-unsupported:fallback-disallowed";
        *outResult = std::move(result);
        return SC_E_INVALIDARG;
    }

    if (plan.state == QueryPlanState::ScanFallback)
    {
        const ErrorCode fallbackRc = ExecuteControlledFallbackScan(context.database, plan, context, &result);
        if (Failed(fallbackRc))
        {
            result.unsupportedSource = QueryUnsupportedSource::Executor;
            result.mode = QueryExecutionMode::Unsupported;
            result.executionNote = L"executor-unsupported:fallback-execution-failed";
            result.rc = fallbackRc;
            *outResult = std::move(result);
            return fallbackRc;
        }

        result.rc = SC_OK;
        *outResult = std::move(result);
        return SC_OK;
    }

    std::vector<SCRecordPtr> matchedRecords;
    const ErrorCode collectRc = CollectCandidateRecords(context.database, plan, &matchedRecords, &result);
    if (Failed(collectRc))
    {
        result.unsupportedSource = QueryUnsupportedSource::Executor;
        result.mode = QueryExecutionMode::Unsupported;
        result.executionNote = L"executor-unsupported:collect-failed";
        result.rc = collectRc;
        *outResult = std::move(result);
        return collectRc;
    }

    switch (plan.state)
    {
    case QueryPlanState::DirectIndex:
        result.mode = QueryExecutionMode::DirectIndex;
        result.fallbackTriggered = false;
        result.fallbackSource = QueryFallbackSource::None;
        result.fallbackReason.clear();
        break;
    case QueryPlanState::PartialIndex:
        result.mode = QueryExecutionMode::PartialIndex;
        result.fallbackTriggered = false;
        result.fallbackSource = QueryFallbackSource::None;
        result.fallbackReason.clear();
        break;
    case QueryPlanState::Unsupported:
        result.mode = QueryExecutionMode::Unsupported;
        result.unsupportedSource = QueryUnsupportedSource::Planner;
        result.executionNote = L"planner-unsupported";
        result.rc = SC_E_INVALIDARG;
        *outResult = std::move(result);
        return SC_E_INVALIDARG;
    }

    const ErrorCode cursorRc = MaterializeCursor(
        std::move(matchedRecords),
        plan,
        static_cast<SCRecordCursorPtr*>(context.resultCursor),
        &result);
    if (Failed(cursorRc))
    {
        result.unsupportedSource = QueryUnsupportedSource::Executor;
        result.executionNote = L"executor-unsupported:cursor-materialization-failed";
        result.mode = QueryExecutionMode::Unsupported;
        result.rc = cursorRc;
        *outResult = std::move(result);
        return cursorRc;
    }

    result.executionNote = BuildPlanDescriptor(plan);
    result.rc = SC_OK;
    *outResult = std::move(result);
    return SC_OK;
}

}  // namespace

ErrorCode ExecuteMemoryQueryDispatch(
    const QueryPlan& plan,
    const QueryExecutionContext& context,
    QueryExecutionResult* outResult)
{
    return DispatchMemoryQuery(plan, context, outResult);
}

}  // namespace StableCore::Storage
