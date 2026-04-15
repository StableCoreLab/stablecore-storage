#pragma once

#include <vector>

#include "ISCInterfaces.h"

namespace StableCore::Storage
{

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
    bool rollbackOnError{true};
};

struct SCBatchExecutionResult
{
    VersionId committedVersion{0};
    std::size_t createdCount{0};
    std::size_t updatedFieldCount{0};
    std::size_t deletedCount{0};
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

}  // namespace StableCore::Storage
