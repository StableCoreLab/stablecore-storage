#pragma once

#include <vector>

#include "StableCore/Storage/Interfaces.h"

namespace stablecore::storage
{

struct FieldValueAssignment
{
    std::wstring fieldName;
    Value value;
};

struct BatchCreateRecordRequest
{
    std::vector<FieldValueAssignment> values;
};

struct BatchUpdateRecordRequest
{
    RecordId recordId{0};
    std::vector<FieldValueAssignment> values;
};

struct BatchTableRequest
{
    std::wstring tableName;
    std::vector<BatchCreateRecordRequest> creates;
    std::vector<BatchUpdateRecordRequest> updates;
    std::vector<RecordId> deletes;
};

struct BatchExecutionOptions
{
    std::wstring editName;
    bool rollbackOnError{true};
};

struct BatchExecutionResult
{
    VersionId committedVersion{0};
    std::size_t createdCount{0};
    std::size_t updatedFieldCount{0};
    std::size_t deletedCount{0};
};

using ImportOptions = BatchExecutionOptions;
using ImportResult = BatchExecutionResult;

ErrorCode ExecuteBatchEdit(
    IDatabase* database,
    const std::vector<BatchTableRequest>& requests,
    const BatchExecutionOptions& options,
    BatchExecutionResult* outResult);

ErrorCode ExecuteImport(
    IDatabase* database,
    const std::vector<BatchTableRequest>& requests,
    const ImportOptions& options,
    ImportResult* outResult);

}  // namespace stablecore::storage
