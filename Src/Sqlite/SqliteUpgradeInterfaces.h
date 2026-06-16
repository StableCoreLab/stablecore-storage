#pragma once

#include "SCMigration.h"

namespace StableCore::Storage
{
    class SqliteUpgradeContext
    {
    public:
        virtual ~SqliteUpgradeContext() = default;

        virtual ErrorCode ExecuteSql(const char* sql) = 0;
        virtual bool HasTableColumn(const char* tableName, const char* columnName) const = 0;
        virtual ErrorCode BackfillSchemaMetadataV3() = 0;
        virtual void InitializeQueryIndexStorage() = 0;
    };

    using SqliteUpgradeStepHandler = ErrorCode (*)(SqliteUpgradeContext& context, SCUpgradeResult* outResult);

    struct SqliteUpgradeStepRegistration
    {
        SCMigrationStep step;
        SqliteUpgradeStepHandler handler{nullptr};
    };
}
