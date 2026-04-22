#pragma once

#include "SCErrors.h"
#include "SCTypes.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace StableCore::Storage
{

class IQueryPlanner;
class ISCDatabase;

enum class QueryBackendKind
{
    Unknown,
    SQLite,
    Memory,
};

struct QueryExecutionOptions
{
    bool collectObservability{true};
    bool preserveBackendNote{true};
};

struct QueryExecutionContext
{
    QueryBackendKind backendKind{QueryBackendKind::Unknown};
    ISCDatabase* database{nullptr};
    void* backendHandle{nullptr};
    void* resultCursor{nullptr};
    QueryExecutionOptions options;
    const void* cancellationToken{nullptr};
};

enum class QueryTargetType
{
    Table,
    View,
};

struct QueryTarget
{
    std::wstring name;
    QueryTargetType type{QueryTargetType::Table};
};

enum class QueryConditionOperator
{
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    Between,
    IsNull,
    IsNotNull,
    StartsWith,
    Contains,
    EndsWith,
};

enum class QueryLogicOperator
{
    And,
    Or,
};

struct QueryCondition
{
    std::wstring fieldName;
    QueryConditionOperator op{QueryConditionOperator::Equal};
    std::vector<SCValue> values;
};

struct QueryConditionGroup
{
    // First-stage query plans allow exactly one level of groups; nesting is not modeled here.
    QueryLogicOperator logic{QueryLogicOperator::And};
    std::vector<QueryCondition> conditions;
};

enum class QueryOrderDirection
{
    Ascending,
    Descending,
};

struct SortSpec
{
    std::wstring fieldName;
    QueryOrderDirection direction{QueryOrderDirection::Ascending};
    bool requireStableSort{false};
};

struct QueryPage
{
    std::uint64_t offset{0};
    std::uint64_t limit{0};
};

struct QueryHints
{
    // Hints are advisory only and may be ignored by the executor.
    bool preferIndex{true};
    bool needReferenceInfo{false};
    bool requireSummaryOnly{false};
};

struct QueryConstraints
{
    // Constraints are hard requirements and must be honored or rejected.
    bool allowFallbackScan{true};
    bool requireIndex{false};
    bool allowPartial{true};
};

enum class QueryPlanState
{
    DirectIndex,
    PartialIndex,
    ScanFallback,
    Unsupported,
};

struct QueryPlan
{
    QueryTarget target;
    std::vector<QueryConditionGroup> conditionGroups;
    // Multiple groups are combined using this operator; first-stage plans do not model nesting.
    QueryLogicOperator conditionGroupLogic{QueryLogicOperator::Or};
    std::vector<SortSpec> orderBy;
    QueryPage page;
    QueryHints hints;
    QueryConstraints constraints;
    QueryPlanState state{QueryPlanState::Unsupported};
    std::wstring planId;
    std::wstring fallbackReason;
    std::uint64_t estimatedRows{0};
    std::uint64_t estimatedCost{0};
    std::uint32_t pushdownConditionCount{0};
    std::uint32_t fallbackConditionCount{0};
};

// Query execution result is observability-first: it must expose the path actually taken.
enum class QueryExecutionMode
{
    DirectIndex,
    PartialIndex,
    FallbackScan,
    Unsupported,
};

enum class QueryFallbackSource
{
    None,
    Planner,
    Executor,
};

enum class QueryUnsupportedSource
{
    None,
    Planner,
    Executor,
};

struct QueryExecutionResult
{
    ErrorCode rc{SC_OK};
    QueryExecutionMode mode{QueryExecutionMode::Unsupported};
    QueryUnsupportedSource unsupportedSource{QueryUnsupportedSource::None};
    QueryFallbackSource fallbackSource{QueryFallbackSource::None};
    std::wstring fallbackReason;
    std::uint64_t scannedRows{0};
    std::uint64_t matchedRows{0};
    std::uint64_t returnedRows{0};
    std::vector<std::wstring> usedIndexIds;
    bool fallbackTriggered{false};
    std::wstring executionNote;
};

// Query index and reference index health are checked independently.
enum class QueryIndexHealthState
{
    Healthy,
    Missing,
    OutOfDate,
    Corrupted,
};

struct QueryIndexCheckResult
{
    QueryIndexHealthState state{QueryIndexHealthState::Healthy};
    std::int32_t indexVersion{0};
    std::int32_t expectedVersion{0};
    std::wstring message;
};

enum class ReferenceDirection
{
    Forward,
    Reverse,
};

enum class ReferenceIndexHealthState
{
    Healthy,
    Missing,
    OutOfDate,
    Corrupted,
};

struct ReferenceIndexCheckResult
{
    ReferenceIndexHealthState state{ReferenceIndexHealthState::Healthy};
    std::int32_t indexVersion{0};
    std::int32_t expectedVersion{0};
    std::wstring message;
};

struct ReferenceRecord
{
    std::wstring sourceTable;
    RecordId sourceRecordId{0};
    std::wstring sourceColumn;
    std::wstring targetTable;
    RecordId targetRecordId{0};
    CommitId commitId{0};
    SessionId sessionId{0};
    std::optional<SnapshotId> snapshotId;
};

struct ReverseReferenceRecord
{
    std::wstring targetTable;
    RecordId targetRecordId{0};
    std::wstring sourceTable;
    RecordId sourceRecordId{0};
    std::wstring sourceColumn;
    CommitId commitId{0};
    SessionId sessionId{0};
    std::optional<SnapshotId> snapshotId;
};

struct ReferenceIndex
{
    std::vector<ReferenceRecord> records;
};

struct ReverseReferenceIndex
{
    std::vector<ReverseReferenceRecord> records;
};

// SCQueryBridge is a declaration-only compatibility seam:
// it may translate legacy parameters <-> QueryPlan / QueryExecutionResult,
// but it must not own planning, execution, or index maintenance logic.
class SCQueryBridge
{
public:
    static ErrorCode BuildPlanFromLegacyFindRecords(
        const std::wstring& targetName,
        const SCQueryCondition& condition,
        QueryPlan* outPlan);

    static ErrorCode AdaptExecutionResultToLegacyStatus(
        const QueryExecutionResult& executionResult,
        ErrorCode* outLegacyRc);
};

// Default planner factory used by tests and bridge-facing compatibility seams.
std::unique_ptr<IQueryPlanner> CreateDefaultQueryPlanner();

}  // namespace StableCore::Storage
