#pragma once

#include <vector>

#include "ISCInterfaces.h"

namespace StableCore::Storage
{

using SCImportSessionId = std::uint64_t;
using SCImportChunkId = std::uint64_t;

enum class SCImportSessionState
{
    Idle,
    Staging,
    Checkpointed,
    ReadyToFinalize,
    Finalized,
    Aborted,
    Failed,
};

struct SCFieldValueAssignment
{
    std::wstring fieldName;
    SCValue SCValue;
};

struct SCBatchCreateRecordRequest
{
    std::vector<SCFieldValueAssignment> values;
};

struct SCBatchUpdateRecordRequest
{
    RecordId recordId{0};
    std::vector<SCFieldValueAssignment> values;
};

struct SCBatchTableRequest
{
    std::wstring tableName;
    std::vector<SCBatchCreateRecordRequest> creates;
    std::vector<SCBatchUpdateRecordRequest> updates;
    std::vector<RecordId> deletes;
};

struct SCBatchExecutionOptions
{
    std::wstring editName;
    std::size_t chunkSize{128};
    bool persistCheckpoints{true};
    bool rollbackOnError{true};
};

struct SCImportSessionOptions
{
    std::wstring sessionName;
    std::size_t chunkSize{128};
    bool persistCheckpoints{true};
    bool rollbackOnError{true};
};

struct SCImportChunk
{
    SCImportChunkId chunkId{0};
    std::vector<SCBatchTableRequest> requests;
};

struct SCImportCheckpoint
{
    SCImportSessionId sessionId{0};
    SCImportChunkId lastChunkId{0};
    std::size_t chunkCount{0};
    VersionId baseVersion{0};
    bool persisted{false};
};

struct SCImportStagingArea
{
    SCImportSessionId sessionId{0};
    std::wstring sessionName;
    VersionId baseVersion{0};
    std::size_t chunkSize{128};
    std::vector<SCImportChunk> chunks;
    SCImportSessionState state{SCImportSessionState::Idle};
};

struct SCImportFinalizeCommit
{
    SCImportSessionId sessionId{0};
    bool confirmed{false};
    std::wstring commitName;
};

struct SCImportRecoveryState
{
    SCImportSessionId sessionId{0};
    SCImportSessionState state{SCImportSessionState::Idle};
    SCImportCheckpoint checkpoint;
    SCImportStagingArea stagingArea;
    bool checkpointPersisted{false};
    bool canResume{false};
    bool canFinalize{false};
    std::wstring reason;
};

struct SCBatchExecutionResult
{
    VersionId committedVersion{0};
    std::size_t createdCount{0};
    std::size_t updatedFieldCount{0};
    std::size_t deletedCount{0};
    SCImportSessionId importSessionId{0};
    std::size_t chunkCount{0};
    std::size_t checkpointCount{0};
};

using ISCmportOptions = SCBatchExecutionOptions;
using ISCmportResult = SCBatchExecutionResult;

ErrorCode ExecuteBatchEdit(
    ISCDatabase* database,
    const std::vector<SCBatchTableRequest>& requests,
    const SCBatchExecutionOptions& options,
    SCBatchExecutionResult* outResult);

ErrorCode ExecuteImport(
    ISCDatabase* database,
    const std::vector<SCBatchTableRequest>& requests,
    const ISCmportOptions& options,
    ISCmportResult* outResult);

ErrorCode BeginImportSession(
    ISCDatabase* database,
    const SCImportSessionOptions& options,
    SCImportStagingArea* outSession);

ErrorCode AppendImportChunk(
    ISCDatabase* database,
    SCImportStagingArea* session,
    const SCImportChunk& chunk,
    SCImportCheckpoint* outCheckpoint);

ErrorCode FinalizeImportSession(
    ISCDatabase* database,
    const SCImportStagingArea& session,
    const SCImportFinalizeCommit& commit,
    SCBatchExecutionResult* outResult);

ErrorCode GetImportRecoveryState(
    ISCDatabase* database,
    SCImportSessionId sessionId,
    SCImportRecoveryState* outState);

ErrorCode AbortImportSession(ISCDatabase* database, SCImportSessionId sessionId);

}  // namespace StableCore::Storage
