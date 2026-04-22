#include "SCBatch.h"

#include <algorithm>

namespace StableCore::Storage
{
namespace
{

ErrorCode ApplyAssignments(ISCRecord* record, const std::vector<SCFieldValueAssignment>& values, std::size_t* outUpdatedFieldCount)
{
    if (record == nullptr)
    {
        return SC_E_POINTER;
    }

    for (const auto& assignment : values)
    {
        const ErrorCode rc = record->SetValue(assignment.fieldName.c_str(), assignment.SCValue);
        if (Failed(rc))
        {
            return rc;
        }
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

ErrorCode ApplyBatchRequests(
    ISCDatabase* database,
    const std::vector<SCBatchTableRequest>& requests,
    SCBatchExecutionResult* outResult)
{
    if (database == nullptr)
    {
        return SC_E_POINTER;
    }

    SCBatchExecutionResult result;
    if (outResult != nullptr)
    {
        result = *outResult;
    }
    for (const auto& request : requests)
    {
        SCTablePtr table;
        ErrorCode rc = ResolveTable(database, request.tableName, table);
        if (Failed(rc))
        {
            return rc;
        }

        for (const auto& create : request.creates)
        {
            SCRecordPtr record;
            rc = table->CreateRecord(record);
            if (Failed(rc))
            {
                return rc;
            }
            rc = ApplyAssignments(record.Get(), create.values, &result.updatedFieldCount);
            if (Failed(rc))
            {
                return rc;
            }
            ++result.createdCount;
        }

        for (const auto& update : request.updates)
        {
            SCRecordPtr record;
            rc = table->GetRecord(update.recordId, record);
            if (Failed(rc))
            {
                return rc;
            }
            rc = ApplyAssignments(record.Get(), update.values, &result.updatedFieldCount);
            if (Failed(rc))
            {
                return rc;
            }
        }

        for (RecordId recordId : request.deletes)
        {
            rc = table->DeleteRecord(recordId);
            if (Failed(rc))
            {
                return rc;
            }
            ++result.deletedCount;
        }
    }

    if (outResult != nullptr)
    {
        *outResult = result;
    }
    return SC_OK;
}

std::vector<SCImportChunk> BuildImportChunks(
    const std::vector<SCBatchTableRequest>& requests,
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

ErrorCode ExecuteBatchEdit(
    ISCDatabase* database,
    const std::vector<SCBatchTableRequest>& requests,
    const SCBatchExecutionOptions& options,
    SCBatchExecutionResult* outResult)
{
    if (database == nullptr)
    {
        return SC_E_POINTER;
    }

    SCEditPtr edit;
    const std::wstring editName = options.editName.empty() ? L"BatchEdit" : options.editName;
    ErrorCode rc = database->BeginEdit(editName.c_str(), edit);
    if (Failed(rc))
    {
        return rc;
    }

    SCBatchExecutionResult result;
    rc = ApplyBatchRequests(database, requests, &result);

    if (Failed(rc))
    {
        if (options.rollbackOnError)
        {
            database->Rollback(edit.Get());
        }
        return rc;
    }

    rc = database->Commit(edit.Get());
    if (Failed(rc))
    {
        if (options.rollbackOnError)
        {
            database->Rollback(edit.Get());
        }
        return rc;
    }

    result.committedVersion = database->GetCurrentVersion();
    if (outResult != nullptr)
    {
        *outResult = result;
    }
    return SC_OK;
}

ErrorCode ExecuteImport(
    ISCDatabase* database,
    const std::vector<SCBatchTableRequest>& requests,
    const ISCmportOptions& options,
    ISCmportResult* outResult)
{
    SCBatchExecutionOptions effective = options;
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

ErrorCode BeginImportSession(
    ISCDatabase* database,
    const SCImportSessionOptions& options,
    SCImportStagingArea* outSession)
{
    if (database == nullptr)
    {
        return SC_E_POINTER;
    }
    return database->BeginImportSession(options, outSession);
}

ErrorCode AppendImportChunk(
    ISCDatabase* database,
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

ErrorCode FinalizeImportSession(
    ISCDatabase* database,
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

    SCEditPtr edit;
    const std::wstring editName = commit.commitName.empty() ? session.sessionName : commit.commitName;
    ErrorCode rc = database->BeginEdit(editName.c_str(), edit);
    if (Failed(rc))
    {
        return rc;
    }

    SCBatchExecutionResult result;
    for (const auto& chunk : session.chunks)
    {
        rc = ApplyBatchRequests(database, chunk.requests, &result);
        if (Failed(rc))
        {
            database->Rollback(edit.Get());
            return rc;
        }
    }

    rc = database->Commit(edit.Get());
    if (Failed(rc))
    {
        database->Rollback(edit.Get());
        return rc;
    }

    SCImportRecoveryState recoveryState;
    rc = database->FinalizeImportSession(commit, &recoveryState);
    if (Failed(rc))
    {
        return rc;
    }

    result.committedVersion = database->GetCurrentVersion();
    result.importSessionId = session.sessionId;
    result.chunkCount = session.chunks.size();
    result.checkpointCount = session.chunks.size();
    if (outResult != nullptr)
    {
        *outResult = result;
    }
    return SC_OK;
}

ErrorCode GetImportRecoveryState(
    ISCDatabase* database,
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
