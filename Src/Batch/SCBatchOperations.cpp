#include "SCBatch.h"

#include <algorithm>

namespace StableCore::Storage
{
    namespace
    {

        // Internal only: SCBatchApplyContext / SCBatchApplyResult and the
        // operation mapping stay inside the implementation.
        struct SCBatchOperationMapping
        {
            SCImportChunkId chunkId{0};
            std::size_t tableRequestIndex{0};
            SCBatchOperationKind kind{SCBatchOperationKind::Create};
            std::size_t operationIndex{0};
            RecordId recordId{0};
        };

        struct SCBatchApplyContext
        {
            SCImportChunkId chunkId{0};
            std::vector<SCBatchOperationMapping>* operationMappings{nullptr};
        };

        struct SCBatchApplyResult
        {
            SCBatchExecutionResult counters;
            SCBatchFailureLocation immediateFailure;
        };

        ErrorCode CaptureConstraintViolation(ISCDatabase* database, SCBatchFailureLocation* location)
        {
            if (database == nullptr || location == nullptr)
            {
                return SC_E_POINTER;
            }

            SCConstraintViolationInfo info{};
            const ErrorCode rc = database->GetLastConstraintViolationInfo(&info);
            if (Failed(rc))
            {
                return rc;
            }

            location->hasConstraintViolation = true;
            location->constraintViolation = info;
            return SC_OK;
        }

        ErrorCode ApplyAssignments(ISCRecord* record,
                                   const std::vector<SCFieldValueAssignment>& values,
                                   std::size_t* outUpdatedFieldCount,
                                   SCBatchFailureLocation* failureLocation)
        {
            if (record == nullptr)
            {
                return SC_E_POINTER;
            }

            std::size_t assignmentIndex = 0;
            for (const auto& assignment : values)
            {
                if (failureLocation != nullptr)
                {
                    failureLocation->assignmentIndex = assignmentIndex;
                    failureLocation->fieldName = assignment.fieldName;
                }

                const ErrorCode rc = record->SetValue(assignment.fieldName.c_str(), assignment.SCValue);
                if (Failed(rc))
                {
                    return rc;
                }
                ++assignmentIndex;
                if (outUpdatedFieldCount != nullptr)
                {
                    ++(*outUpdatedFieldCount);
                }
            }

            return SC_OK;
        }

        ErrorCode ResolveTable(ISCDatabase* database, const std::wstring& tableName, SCTablePtr& outTable)
        {
            if (database == nullptr || tableName.empty())
            {
                return SC_E_INVALIDARG;
            }

            ErrorCode rc = database->GetTable(tableName.c_str(), outTable);
            if (rc == SC_E_TABLE_NOT_FOUND)
            {
                rc = database->CreateTable(tableName.c_str(), outTable);
            }
            return rc;
        }

        // Only errors that carry real storage layer constraint details may
        // publish SCConstraintViolationInfo. Type mismatches, missing tables or
        // missing records must keep hasConstraintViolation == false instead of
        // exposing a leftover default constructed value.
        bool IsConstraintFailure(ErrorCode rc)
        {
            return rc == SC_E_CONSTRAINT_VIOLATION || rc == SC_E_SCHEMA_VIOLATION;
        }

        const SCBatchOperationMapping* FindOperationMapping(const std::vector<SCBatchOperationMapping>& mappings,
                                                            RecordId recordId)
        {
            if (recordId == 0)
            {
                return nullptr;
            }
            for (const auto& mapping : mappings)
            {
                if (mapping.recordId == recordId)
                {
                    return &mapping;
                }
            }
            return nullptr;
        }

        void StoreOperationMapping(const SCBatchApplyContext& context,
                                   std::size_t tableRequestIndex,
                                   SCBatchOperationKind kind,
                                   std::size_t operationIndex,
                                   RecordId recordId)
        {
            if (context.operationMappings == nullptr)
            {
                return;
            }

            SCBatchOperationMapping mapping;
            mapping.chunkId = context.chunkId;
            mapping.tableRequestIndex = tableRequestIndex;
            mapping.kind = kind;
            mapping.operationIndex = operationIndex;
            mapping.recordId = recordId;
            context.operationMappings->push_back(mapping);
        }

        SCBatchFailureLocation MakeRequestFailureLocation(const SCBatchApplyContext& context,
                                                         std::size_t tableRequestIndex)
        {
            SCBatchFailureLocation location{};
            location.valid = true;
            location.chunkId = context.chunkId;
            location.tableRequestIndex = tableRequestIndex;
            location.hasOperation = false;
            return location;
        }

        SCBatchFailureLocation MakeOperationFailureLocation(const SCBatchApplyContext& context,
                                                           std::size_t tableRequestIndex,
                                                           SCBatchOperationKind kind,
                                                           std::size_t operationIndex)
        {
            SCBatchFailureLocation location = MakeRequestFailureLocation(context, tableRequestIndex);
            location.hasOperation = true;
            location.operationKind = kind;
            location.operationIndex = operationIndex;
            return location;
        }

        ErrorCode ApplyBatchRequests(ISCDatabase* database,
                                     const std::vector<SCBatchTableRequest>& requests,
                                     const SCBatchApplyContext& context,
                                     SCBatchApplyResult* outResult)
        {
            if (database == nullptr || outResult == nullptr)
            {
                return SC_E_POINTER;
            }

            SCBatchExecutionResult& counters = outResult->counters;
            SCBatchFailureLocation& location = outResult->immediateFailure;

            for (std::size_t tableRequestIndex = 0; tableRequestIndex < requests.size(); ++tableRequestIndex)
            {
                const SCBatchTableRequest& request = requests[tableRequestIndex];

                // Request level position: ResolveTable() failures must be
                // attributable without faking an operation, record or field.
                location = MakeRequestFailureLocation(context, tableRequestIndex);

                SCTablePtr table;
                ErrorCode rc = ResolveTable(database, request.tableName, table);
                if (Failed(rc))
                {
                    return rc;
                }

                std::size_t operationIndex = 0;

                for (const auto& create : request.creates)
                {
                    location = MakeOperationFailureLocation(context,
                                                            tableRequestIndex,
                                                            SCBatchOperationKind::Create,
                                                            operationIndex);

                    SCRecordPtr record;
                    rc = table->CreateRecord(record);
                    if (Failed(rc))
                    {
                        return rc;
                    }
                    if (record == nullptr)
                    {
                        return SC_E_POINTER;
                    }

                    const RecordId createdRecordId = record->GetId();
                    location.recordId = createdRecordId;

                    rc = ApplyAssignments(record.Get(), create.values, &counters.updatedFieldCount, &location);
                    if (Failed(rc))
                    {
                        return rc;
                    }

                    StoreOperationMapping(context,
                                          tableRequestIndex,
                                          SCBatchOperationKind::Create,
                                          operationIndex,
                                          createdRecordId);
                    ++counters.createdCount;
                    ++operationIndex;
                }

                for (const auto& update : request.updates)
                {
                    location = MakeOperationFailureLocation(context,
                                                            tableRequestIndex,
                                                            SCBatchOperationKind::Update,
                                                            operationIndex);
                    // Pre-filled so a failing GetRecord() still reports the
                    // RecordId requested by the caller.
                    location.recordId = update.recordId;

                    SCRecordPtr record;
                    rc = table->GetRecord(update.recordId, record);
                    if (Failed(rc))
                    {
                        return rc;
                    }
                    if (record == nullptr)
                    {
                        return SC_E_POINTER;
                    }

                    rc = ApplyAssignments(record.Get(), update.values, &counters.updatedFieldCount, &location);
                    if (Failed(rc))
                    {
                        return rc;
                    }

                    StoreOperationMapping(context,
                                          tableRequestIndex,
                                          SCBatchOperationKind::Update,
                                          operationIndex,
                                          update.recordId);
                    ++operationIndex;
                }

                for (RecordId recordId : request.deletes)
                {
                    location = MakeOperationFailureLocation(context,
                                                            tableRequestIndex,
                                                            SCBatchOperationKind::Delete,
                                                            operationIndex);
                    location.recordId = recordId;

                    rc = table->DeleteRecord(recordId);
                    if (Failed(rc))
                    {
                        return rc;
                    }

                    StoreOperationMapping(context,
                                          tableRequestIndex,
                                          SCBatchOperationKind::Delete,
                                          operationIndex,
                                          recordId);
                    ++counters.deletedCount;
                    ++operationIndex;
                }
            }

            return SC_OK;
        }

        // Guarantees that the accumulated counters and the failure location
        // reach the caller on every return path, success or failure.
        class ResultPublisher
        {
        public:
            ResultPublisher(SCBatchExecutionResult* outResult, const SCBatchExecutionResult& result)
                : outResult_(outResult)
                , result_(result)
            {
            }

            ResultPublisher(const ResultPublisher&) = delete;
            ResultPublisher& operator=(const ResultPublisher&) = delete;

            ~ResultPublisher()
            {
                if (outResult_ != nullptr)
                {
                    *outResult_ = result_;
                }
            }

        private:
            SCBatchExecutionResult* outResult_;
            const SCBatchExecutionResult& result_;
        };

        // Immediate failure inside ApplyBatchRequests(): keep the recorded
        // operation position, only attach constraint details when the storage
        // layer really provided them, then let the transaction owner roll back.
        ErrorCode FinalizeBatchApplyFailure(ISCDatabase* database,
                                            ISCEditSession* edit,
                                            ErrorCode applyRc,
                                            const SCBatchFailureLocation& immediateFailure,
                                            SCBatchExecutionResult* result)
        {
            result->failureLocation = immediateFailure;
            if (IsConstraintFailure(applyRc))
            {
                (void)CaptureConstraintViolation(database, &result->failureLocation);
            }

            const ErrorCode rollbackRc = database->Rollback(edit);
            return Failed(rollbackRc) ? rollbackRc : applyRc;
        }

        // Commit() is where deferred constraints surface, so the violation
        // details must be captured before Rollback() and mapped back onto the
        // originating chunk / table request / operation.
        ErrorCode FinalizeBatchCommitFailure(ISCDatabase* database,
                                             ISCEditSession* edit,
                                             const std::vector<SCBatchOperationMapping>& mappings,
                                             ErrorCode commitRc,
                                             SCBatchExecutionResult* result)
        {
            SCConstraintViolationInfo info{};
            const bool hasViolation = IsConstraintFailure(commitRc) &&
                Succeeded(database->GetLastConstraintViolationInfo(&info));

            if (hasViolation)
            {
                SCBatchFailureLocation location{};
                location.hasConstraintViolation = true;
                location.constraintViolation = info;

                const SCBatchOperationMapping* mapping = FindOperationMapping(mappings, info.recordId);
                if (mapping == nullptr)
                {
                    mapping = FindOperationMapping(mappings, info.referencedRecordId);
                }
                if (mapping != nullptr)
                {
                    location.valid = true;
                    location.chunkId = mapping->chunkId;
                    location.tableRequestIndex = mapping->tableRequestIndex;
                    location.hasOperation = true;
                    location.operationKind = mapping->kind;
                    location.operationIndex = mapping->operationIndex;
                    location.recordId = mapping->recordId;
                }

                result->failureLocation = location;
            }

            const ErrorCode rollbackRc = database->Rollback(edit);
            return Failed(rollbackRc) ? rollbackRc : commitRc;
        }

        std::vector<SCImportChunk> BuildImportChunks(const std::vector<SCBatchTableRequest>& requests,
                                                     std::size_t chunkSize)
        {
            const std::size_t effectiveChunkSize = chunkSize == 0 ? 1 : chunkSize;
            std::vector<SCImportChunk> chunks;
            SCImportChunk currentChunk;
            currentChunk.chunkId = 1;

            for (const auto& request : requests)
            {
                currentChunk.requests.push_back(request);
                if (currentChunk.requests.size() >= effectiveChunkSize)
                {
                    chunks.push_back(currentChunk);
                    currentChunk = SCImportChunk{};
                    currentChunk.chunkId = static_cast<SCImportChunkId>(chunks.size()) + 1;
                }
            }

            if (!currentChunk.requests.empty())
            {
                chunks.push_back(currentChunk);
            }

            for (std::size_t index = 0; index < chunks.size(); ++index)
            {
                chunks[index].chunkId = static_cast<SCImportChunkId>(index + 1);
            }

            return chunks;
        }

    }  // namespace

    ErrorCode ExecuteBatchEdit(ISCDatabase* database,
                               const std::vector<SCBatchTableRequest>& requests,
                               const SCBatchExecutionOptions& options,
                               SCBatchExecutionResult* outResult)
    {
        if (database == nullptr)
        {
            return SC_E_POINTER;
        }

        std::vector<SCBatchOperationMapping> operationMappings;
        SCBatchApplyResult applyResult;
        SCBatchExecutionResult& result = applyResult.counters;
        ResultPublisher publisher(outResult, result);

        SCEditPtr edit;
        const std::wstring editName = options.editName.empty() ? L"BatchEdit" : options.editName;
        ErrorCode rc = database->BeginEdit(editName.c_str(), edit);
        if (Failed(rc))
        {
            return rc;
        }

        SCBatchApplyContext context;
        context.chunkId = 0;
        context.operationMappings = &operationMappings;

        rc = ApplyBatchRequests(database, requests, context, &applyResult);
        if (Failed(rc))
        {
            return FinalizeBatchApplyFailure(database,
                                             edit.Get(),
                                             rc,
                                             applyResult.immediateFailure,
                                             &result);
        }

        rc = database->Commit(edit.Get());
        if (Failed(rc))
        {
            return FinalizeBatchCommitFailure(database, edit.Get(), operationMappings, rc, &result);
        }

        result.committedVersion = database->GetCurrentVersion();
        return SC_OK;
    }

    ErrorCode ExecuteImport(ISCDatabase* database,
                            const std::vector<SCBatchTableRequest>& requests,
                            const ISCmportOptions& options,
                            ISCmportResult* outResult)
    {
        SCImportExecutionOptions effective = options;
        if (effective.editName.empty())
        {
            effective.editName = L"Import";
        }

        SCImportSessionOptions sessionOptions;
        sessionOptions.sessionName = effective.editName;
        sessionOptions.chunkSize = effective.chunkSize;
        sessionOptions.persistCheckpoints = effective.persistCheckpoints;
        sessionOptions.rollbackOnError = effective.rollbackOnError;

        SCImportStagingArea session;
        ErrorCode rc = BeginImportSession(database, sessionOptions, &session);
        if (Failed(rc))
        {
            return rc;
        }

        const std::vector<SCImportChunk> chunks = BuildImportChunks(requests, session.chunkSize);
        SCImportCheckpoint checkpoint;
        for (const auto& chunk : chunks)
        {
            rc = AppendImportChunk(database, &session, chunk, &checkpoint);
            if (Failed(rc))
            {
                // rollbackOnError only decides whether the import session is
                // aborted and cleaned up; the batch edit itself is always
                // rolled back atomically by its transaction owner.
                if (effective.rollbackOnError)
                {
                    AbortImportSession(database, session.sessionId);
                }
                return rc;
            }
        }

        SCImportFinalizeCommit commit;
        commit.sessionId = session.sessionId;
        commit.confirmed = true;
        commit.commitName = effective.editName;
        return FinalizeImportSession(database, session, commit, outResult);
    }

    ErrorCode BeginImportSession(ISCDatabase* database,
                                 const SCImportSessionOptions& options,
                                 SCImportStagingArea* outSession)
    {
        if (database == nullptr)
        {
            return SC_E_POINTER;
        }
        return database->BeginImportSession(options, outSession);
    }

    ErrorCode AppendImportChunk(ISCDatabase* database,
                                SCImportStagingArea* session,
                                const SCImportChunk& chunk,
                                SCImportCheckpoint* outCheckpoint)
    {
        if (database == nullptr)
        {
            return SC_E_POINTER;
        }
        if (session == nullptr)
        {
            return SC_E_POINTER;
        }
        return database->AppendImportChunk(session, chunk, outCheckpoint);
    }

    ErrorCode FinalizeImportSession(ISCDatabase* database,
                                    const SCImportStagingArea& session,
                                    const SCImportFinalizeCommit& commit,
                                    SCBatchExecutionResult* outResult)
    {
        if (database == nullptr)
        {
            return SC_E_POINTER;
        }
        if (!commit.confirmed)
        {
            return SC_E_INVALIDARG;
        }
        if (session.sessionId != commit.sessionId)
        {
            return SC_E_INVALIDARG;
        }

        std::vector<SCBatchOperationMapping> operationMappings;
        SCBatchApplyResult applyResult;
        SCBatchExecutionResult& result = applyResult.counters;
        ResultPublisher publisher(outResult, result);

        ErrorCode rc = SC_OK;
        SCImportRecoveryState recoveryState;
        rc = database->LoadImportRecoveryState(session.sessionId, &recoveryState);
        if (Failed(rc))
        {
            return rc;
        }
        if (!recoveryState.canFinalize)
        {
            return SC_E_INVALIDARG;
        }

        SCEditPtr edit;
        const std::wstring editName = commit.commitName.empty() ? session.sessionName : commit.commitName;
        rc = database->BeginEdit(editName.c_str(), edit);
        if (Failed(rc))
        {
            return rc;
        }

        for (const auto& chunk : session.chunks)
        {
            SCBatchApplyContext context;
            context.chunkId = chunk.chunkId;
            context.operationMappings = &operationMappings;

            rc = ApplyBatchRequests(database, chunk.requests, context, &applyResult);
            if (Failed(rc))
            {
                return FinalizeBatchApplyFailure(database,
                                                 edit.Get(),
                                                 rc,
                                                 applyResult.immediateFailure,
                                                 &result);
            }
        }

        rc = database->Commit(edit.Get());
        if (Failed(rc))
        {
            return FinalizeBatchCommitFailure(database, edit.Get(), operationMappings, rc, &result);
        }

        rc = database->FinalizeImportSession(commit, &recoveryState);
        if (Failed(rc))
        {
            // The imported records are already committed; only clearing the
            // staging checkpoint failed. Report the durable result and flag the
            // partial success instead of pretending the whole import failed.
            result.committedVersion = database->GetCurrentVersion();
            result.importSessionId = session.sessionId;
            result.chunkCount = session.chunks.size();
            result.checkpointCount = session.chunks.size();
            return SC_E_IMPORT_CLEANUP_FAILED;
        }

        result.committedVersion = database->GetCurrentVersion();
        result.importSessionId = session.sessionId;
        result.chunkCount = session.chunks.size();
        result.checkpointCount = session.chunks.size();
        return SC_OK;
    }

    ErrorCode GetImportRecoveryState(ISCDatabase* database,
                                     SCImportSessionId sessionId,
                                     SCImportRecoveryState* outState)
    {
        if (database == nullptr)
        {
            return SC_E_POINTER;
        }
        return database->LoadImportRecoveryState(sessionId, outState);
    }

    ErrorCode AbortImportSession(ISCDatabase* database, SCImportSessionId sessionId)
    {
        if (database == nullptr)
        {
            return SC_E_POINTER;
        }
        return database->AbortImportSession(sessionId);
    }

}  // namespace StableCore::Storage
