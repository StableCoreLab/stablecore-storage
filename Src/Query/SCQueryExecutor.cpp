#include "ISCQuery.h"

#include <functional>
#include <mutex>
#include <unordered_map>
#include <utility>

namespace StableCore::Storage
{
namespace
{

using DispatchFn = std::function<ErrorCode(const QueryPlan&, const QueryExecutionContext&, QueryExecutionResult*)>;

class QueryExecutionRegistry
{
public:
    static QueryExecutionRegistry& Instance()
    {
        static QueryExecutionRegistry registry;
        return registry;
    }

    void Register(QueryBackendKind backendKind, DispatchFn dispatch)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        dispatchByKind_[backendKind] = std::move(dispatch);
    }

    DispatchFn Find(QueryBackendKind backendKind) const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto it = dispatchByKind_.find(backendKind);
        return it == dispatchByKind_.end() ? DispatchFn{} : it->second;
    }

private:
    mutable std::mutex mutex_;
    std::unordered_map<QueryBackendKind, DispatchFn> dispatchByKind_;
};

void InitializeExecutionResult(QueryExecutionResult* result)
{
    if (result == nullptr)
    {
        return;
    }

    result->rc = SC_OK;
    result->mode = QueryExecutionMode::Unsupported;
    result->unsupportedSource = QueryUnsupportedSource::None;
    result->fallbackSource = QueryFallbackSource::None;
    result->fallbackReason.clear();
    result->scannedRows = 0;
    result->matchedRows = 0;
    result->returnedRows = 0;
    result->usedIndexIds.clear();
    result->fallbackTriggered = false;
    result->executionNote.clear();
}

void FinalizeExecutionResult(const QueryPlan& plan, ErrorCode rc, QueryExecutionResult* result)
{
    if (result == nullptr)
    {
        return;
    }

    if (Succeeded(rc))
    {
        if (result->mode == QueryExecutionMode::Unsupported)
        {
            switch (plan.state)
            {
            case QueryPlanState::DirectIndex:
                result->mode = QueryExecutionMode::DirectIndex;
                break;
            case QueryPlanState::PartialIndex:
                result->mode = QueryExecutionMode::PartialIndex;
                break;
            case QueryPlanState::ScanFallback:
                result->mode = QueryExecutionMode::FallbackScan;
                break;
            case QueryPlanState::Unsupported:
            default:
                result->mode = QueryExecutionMode::Unsupported;
                break;
            }
        }

        if (result->mode == QueryExecutionMode::FallbackScan)
        {
            result->fallbackTriggered = true;
            if (result->fallbackReason.empty())
            {
                result->fallbackReason = plan.fallbackReason;
            }
            if (result->fallbackSource == QueryFallbackSource::None)
            {
                result->fallbackSource = (plan.state == QueryPlanState::ScanFallback)
                    ? QueryFallbackSource::Planner
                    : QueryFallbackSource::Executor;
            }
        }
        else if (result->mode == QueryExecutionMode::DirectIndex || result->mode == QueryExecutionMode::PartialIndex)
        {
            result->fallbackTriggered = false;
            result->fallbackSource = QueryFallbackSource::None;
        }
        return;
    }

    result->mode = QueryExecutionMode::Unsupported;
    if (result->unsupportedSource == QueryUnsupportedSource::None)
    {
        result->unsupportedSource = (plan.state == QueryPlanState::Unsupported)
            ? QueryUnsupportedSource::Planner
            : QueryUnsupportedSource::Executor;
    }
    if (result->executionNote.empty())
    {
        result->executionNote = (result->unsupportedSource == QueryUnsupportedSource::Planner)
            ? L"planner-unsupported:execution-failed"
            : L"executor-unsupported:execution-failed";
    }
}

ErrorCode ExecuteQueryPlanImpl(
    const QueryPlan& plan,
    const QueryExecutionContext& context,
    QueryExecutionResult* outResult)
{
    if (outResult == nullptr)
    {
        return SC_E_POINTER;
    }

    QueryExecutionResult result;
    InitializeExecutionResult(&result);

    if (context.backendKind == QueryBackendKind::Unknown)
    {
        result.unsupportedSource = QueryUnsupportedSource::Executor;
        result.executionNote = L"executor-unsupported:backend-kind-unknown";
        result.rc = SC_E_INVALIDARG;
        *outResult = std::move(result);
        return SC_E_INVALIDARG;
    }

    if (context.database == nullptr)
    {
        result.unsupportedSource = QueryUnsupportedSource::Executor;
        result.executionNote = L"executor-unsupported:database-null";
        result.rc = SC_E_POINTER;
        *outResult = std::move(result);
        return SC_E_POINTER;
    }

    if (plan.target.name.empty())
    {
        result.unsupportedSource = QueryUnsupportedSource::Planner;
        result.executionNote = L"planner-unsupported:target-name-empty";
        result.rc = SC_E_INVALIDARG;
        *outResult = std::move(result);
        return SC_E_INVALIDARG;
    }

    if (plan.state == QueryPlanState::Unsupported)
    {
        result.unsupportedSource = QueryUnsupportedSource::Planner;
        result.executionNote = L"planner-unsupported:" + (plan.fallbackReason.empty() ? std::wstring(L"unsupported-plan") : plan.fallbackReason);
        result.fallbackReason = plan.fallbackReason;
        result.rc = SC_E_INVALIDARG;
        *outResult = std::move(result);
        return SC_E_INVALIDARG;
    }

    if (plan.state == QueryPlanState::ScanFallback && !plan.constraints.allowFallbackScan)
    {
        result.unsupportedSource = QueryUnsupportedSource::Executor;
        result.executionNote = L"executor-unsupported:fallback-disallowed";
        result.fallbackReason = plan.fallbackReason;
        result.rc = SC_E_INVALIDARG;
        *outResult = std::move(result);
        return SC_E_INVALIDARG;
    }

    const auto dispatch = QueryExecutionRegistry::Instance().Find(context.backendKind);
    if (!dispatch)
    {
        result.unsupportedSource = QueryUnsupportedSource::Executor;
        result.executionNote = L"executor-unsupported:no-dispatch-registered";
        result.fallbackReason = plan.fallbackReason;
        result.rc = SC_E_NOTIMPL;
        *outResult = std::move(result);
        return SC_E_NOTIMPL;
    }

    const ErrorCode rc = dispatch(plan, context, &result);
    if (Failed(rc) && result.unsupportedSource == QueryUnsupportedSource::None)
    {
        result.unsupportedSource = QueryUnsupportedSource::Executor;
    }
    if (result.fallbackReason.empty())
    {
        result.fallbackReason = plan.fallbackReason;
    }

    FinalizeExecutionResult(plan, rc, &result);
    result.rc = rc;
    *outResult = std::move(result);
    return rc;
}

}  // namespace

void RegisterQueryExecutionContextDispatch(QueryBackendKind backendKind, QueryExecutionContextDispatch dispatch)
{
    if (dispatch == nullptr)
    {
        return;
    }

    QueryExecutionRegistry::Instance().Register(
        backendKind,
        [dispatch](const QueryPlan& plan, const QueryExecutionContext& context, QueryExecutionResult* outResult)
        {
            return dispatch(plan, context, outResult);
        });
}

ErrorCode ExecuteQueryPlan(
    const QueryPlan& plan,
    const QueryExecutionContext& context,
    QueryExecutionResult* outResult)
{
    return ExecuteQueryPlanImpl(plan, context, outResult);
}

}  // namespace StableCore::Storage
