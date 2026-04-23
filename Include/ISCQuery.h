#pragma once

#include <memory>

#include "ISCInterfaces.h"
#include "SCQuery.h"

namespace StableCore::Storage
{

// Planner converts normalized query requests into executable plans.
class IQueryPlanner
{
public:
    virtual ~IQueryPlanner() = default;
    virtual ErrorCode BuildPlan(const QueryTarget& target,
                                const std::vector<QueryConditionGroup>& conditionGroups,
                                QueryLogicOperator conditionGroupLogic,
                                const std::vector<SortSpec>& orderBy,
                                const QueryPage& page,
                                const QueryHints& hints,
                                const QueryConstraints& constraints,
                                QueryPlan* outPlan) const = 0;
};

// Executor is a unified entry point only; concrete backends may provide their own executor implementations.
class IQueryExecutor
{
public:
    virtual ~IQueryExecutor() = default;
    virtual ErrorCode Execute(
        const QueryPlan& plan,
        const QueryExecutionContext& context,
        QueryExecutionResult* outResult) = 0;
};

// Query index inspection/rebuild is for query execution observability and maintenance.
class IQueryIndexProvider
{
public:
    virtual ~IQueryIndexProvider() = default;
    virtual ErrorCode CheckQueryIndex(QueryIndexCheckResult* outResult) const = 0;
};

class IQueryIndexMaintainer
{
public:
    virtual ~IQueryIndexMaintainer() = default;
    virtual ErrorCode RebuildQueryIndex() = 0;
};

// Reference index provider is intentionally source/target oriented; full dumps are diagnostic only.
class IReferenceIndexProvider
{
public:
    virtual ~IReferenceIndexProvider() = default;
    virtual ErrorCode GetReferencesBySource(
        const std::wstring& sourceTable,
        RecordId sourceRecordId,
        std::vector<ReferenceRecord>* outRecords) const = 0;
    virtual ErrorCode GetReferencesByTarget(
        const std::wstring& targetTable,
        RecordId targetRecordId,
        std::vector<ReverseReferenceRecord>* outRecords) const = 0;
    virtual ErrorCode CheckReferenceIndex(ReferenceIndexCheckResult* outResult) const = 0;
    virtual ErrorCode GetAllReferencesDiagnosticOnly(ReferenceIndex* outIndex) const = 0;
};

class IReferenceIndexMaintainer
{
public:
    virtual ~IReferenceIndexMaintainer() = default;
    virtual ErrorCode RebuildReferenceIndexes() = 0;
    virtual ErrorCode CommitReferenceDelta(const ReferenceIndex& forwardDelta,
                                           const ReverseReferenceIndex& reverseDelta) = 0;
};

using QueryExecutionContextDispatch = ErrorCode (*)(const QueryPlan& plan,
                                                     const QueryExecutionContext& context,
                                                     QueryExecutionResult* outResult);

void RegisterQueryExecutionContextDispatch(QueryBackendKind backendKind, QueryExecutionContextDispatch dispatch);

// Executes a normalized QueryPlan through the registered backend-specific dispatcher.
ErrorCode ExecuteQueryPlan(
    const QueryPlan& plan,
    const QueryExecutionContext& context,
    QueryExecutionResult* outResult);

// Factory used by tests and bridge-facing code to obtain the default planner implementation.
std::unique_ptr<IQueryPlanner> CreateDefaultQueryPlanner();

}  // namespace StableCore::Storage
