#pragma once

#include <string>
#include <vector>

#include "ISCInterfaces.h"
#include "SCTypes.h"

namespace StableCore::Storage
{

    struct SCTableSchemaPatch
    {
        std::wstring tableName;

        std::vector<SCColumnDef> addColumns;
        std::vector<SCColumnDef> updateColumns;
        std::vector<std::wstring> removeColumns;
        std::vector<SCConstraintDef> addConstraints;
        std::vector<std::wstring> removeConstraints;
        std::vector<SCIndexDef> addIndexes;
        std::vector<std::wstring> removeIndexes;
    };

    struct SCSchemaEditResult
    {
        bool applied{false};
        VersionId committedVersion{0};

        std::wstring tableName;
        std::vector<std::wstring> addedColumns;
        std::vector<std::wstring> updatedColumns;
        std::vector<std::wstring> removedColumns;
        std::vector<std::wstring> addedConstraints;
        std::vector<std::wstring> removedConstraints;
        std::vector<std::wstring> addedIndexes;
        std::vector<std::wstring> removedIndexes;
    };

    ErrorCode CreateTableFromSchema(ISCDatabase* database,
                                    const SCTableSchemaSnapshot& schema,
                                    SCSchemaEditResult* outResult);

    ErrorCode ApplyTableSchemaPatch(ISCDatabase* database,
                                    const SCTableSchemaPatch& patch,
                                    SCSchemaEditResult* outResult);

}  // namespace StableCore::Storage
