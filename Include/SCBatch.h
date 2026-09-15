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

    enum class SCBatchOperationKind
    {
        Create,
        Update,
        Delete,
    };

    struct SCBatchFailureLocation
    {
        bool valid{false};
        SCImportChunkId chunkId{0};
        std::size_t tableRequestIndex{0};
        // false means a table request level failure (for example ResolveTable):
        // operationKind / operationIndex / recordId are not available.
        bool hasOperation{false};
        SCBatchOperationKind operationKind{SCBatchOperationKind::Create};
        std::size_t operationIndex{0};
        RecordId recordId{0};
        // Field assignment position of Create / Update failures. Stays at the
        // default value with an empty name when there is no field context.
        std::size_t assignmentIndex{0};
        std::wstring fieldName;
        // Only true when the storage layer actually provided constraint
        // violation details. Immediate errors such as a type mismatch must not
        // present a default constructed value as a PrimaryKey violation.
        bool hasConstraintViolation{false};
        SCConstraintViolationInfo constraintViolation;
    };

    struct SCBatchExecutionOptions
    {
        std::wstring editName;
        std::size_t chunkSize{128};
        bool persistCheckpoints{true};
    };

    // rollbackOnError only belongs to the import session flow: it controls
    // whether a failing chunk append aborts and cleans up the session. Plain
    // batch edits are always atomic, so SCBatchExecutionOptions does not carry
    // the flag any more.
    struct SCImportExecutionOptions
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
        // Attribution for failures that only surface inside Commit(). Empty
        // (valid == false) when the batch succeeded.
        SCBatchFailureLocation failureLocation;
    };

    using ISCmportOptions = SCImportExecutionOptions;
    using ISCmportResult = SCBatchExecutionResult;

    ErrorCode ExecuteBatchEdit(ISCDatabase* database,
                               const std::vector<SCBatchTableRequest>& requests,
                               const SCBatchExecutionOptions& options,
                               SCBatchExecutionResult* outResult);

    ErrorCode ExecuteImport(ISCDatabase* database,
                            const std::vector<SCBatchTableRequest>& requests,
                            const ISCmportOptions& options,
                            ISCmportResult* outResult);

    ErrorCode BeginImportSession(ISCDatabase* database,
                                 const SCImportSessionOptions& options,
                                 SCImportStagingArea* outSession);

    ErrorCode AppendImportChunk(ISCDatabase* database,
                                SCImportStagingArea* session,
                                const SCImportChunk& chunk,
                                SCImportCheckpoint* outCheckpoint);

    ErrorCode FinalizeImportSession(ISCDatabase* database,
                                    const SCImportStagingArea& session,
                                    const SCImportFinalizeCommit& commit,
                                    SCBatchExecutionResult* outResult);

    ErrorCode GetImportRecoveryState(ISCDatabase* database,
                                     SCImportSessionId sessionId,
                                     SCImportRecoveryState* outState);

    ErrorCode AbortImportSession(ISCDatabase* database, SCImportSessionId sessionId);

}  // namespace StableCore::Storage
