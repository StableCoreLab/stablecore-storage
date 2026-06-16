#include "SqliteUpgradeRegistry.h"

namespace StableCore::Storage
{
    ErrorCode ApplySqliteUpgradeStep_5_6(SqliteUpgradeContext& context, SCUpgradeResult* outResult)
    {
        if (outResult != nullptr)
        {
            outResult->failureReason.clear();
        }

        if (!context.HasTableColumn("schema_columns", "reference_storage_column"))
        {
            const ErrorCode schemaColumnsRc = context.ExecuteSql(
                "ALTER TABLE schema_columns ADD COLUMN reference_storage_column TEXT NOT NULL DEFAULT '';");
            if (Failed(schemaColumnsRc))
            {
                if (outResult != nullptr)
                {
                    outResult->failureReason = L"Failed to add relation storage column metadata.";
                }
                return schemaColumnsRc;
            }
        }

        if (!context.HasTableColumn("schema_columns", "reference_display_column"))
        {
            const ErrorCode schemaColumnsRc = context.ExecuteSql(
                "ALTER TABLE schema_columns ADD COLUMN reference_display_column TEXT NOT NULL DEFAULT '';");
            if (Failed(schemaColumnsRc))
            {
                if (outResult != nullptr)
                {
                    outResult->failureReason = L"Failed to add relation display column metadata.";
                }
                return schemaColumnsRc;
            }
        }

        return SC_OK;
    }

    SC_SQLITE_UPGRADE_REGISTER_STEP(5,
                                    6,
                                    L"5 -> 6",
                                    L"Add relation storage and display column metadata.",
                                    &ApplySqliteUpgradeStep_5_6)
}  // namespace StableCore::Storage
