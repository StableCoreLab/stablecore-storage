#include "SCBatch.h"

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

    for (const auto& request : requests)
    {
        SCTablePtr table;
        rc = ResolveTable(database, request.tableName, table);
        if (Failed(rc))
        {
            break;
        }

        for (const auto& create : request.creates)
        {
            SCRecordPtr record;
            rc = table->CreateRecord(record);
            if (Failed(rc))
            {
                break;
            }
            rc = ApplyAssignments(record.Get(), create.values, &result.updatedFieldCount);
            if (Failed(rc))
            {
                break;
            }
            ++result.createdCount;
        }
        if (Failed(rc))
        {
            break;
        }

        for (const auto& update : request.updates)
        {
            SCRecordPtr record;
            rc = table->GetRecord(update.recordId, record);
            if (Failed(rc))
            {
                break;
            }
            rc = ApplyAssignments(record.Get(), update.values, &result.updatedFieldCount);
            if (Failed(rc))
            {
                break;
            }
        }
        if (Failed(rc))
        {
            break;
        }

        for (RecordId recordId : request.deletes)
        {
            rc = table->DeleteRecord(recordId);
            if (Failed(rc))
            {
                break;
            }
            ++result.deletedCount;
        }
        if (Failed(rc))
        {
            break;
        }
    }

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
    return ExecuteBatchEdit(database, requests, effective, outResult);
}

}  // namespace StableCore::Storage
