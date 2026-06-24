#include "SqliteUpgradeRegistry.h"

namespace StableCore::Storage
{
    ErrorCode ApplySqliteUpgradeStep_6_7(SqliteUpgradeContext& context, SCUpgradeResult* outResult)
    {
        if (outResult != nullptr)
        {
            outResult->failureReason.clear();
        }

        if (!context.HasTableColumn("schema_constraints", "on_delete_action"))
        {
            const ErrorCode rc = context.ExecuteSql(
                "ALTER TABLE schema_constraints ADD COLUMN on_delete_action INTEGER NOT NULL DEFAULT 0;");
            if (Failed(rc))
            {
                if (outResult != nullptr)
                {
                    outResult->failureReason = L"Failed to add foreign key delete action metadata.";
                }
                return rc;
            }
        }

        if (!context.HasTableColumn("schema_constraints", "on_update_action"))
        {
            const ErrorCode rc = context.ExecuteSql(
                "ALTER TABLE schema_constraints ADD COLUMN on_update_action INTEGER NOT NULL DEFAULT 0;");
            if (Failed(rc))
            {
                if (outResult != nullptr)
                {
                    outResult->failureReason = L"Failed to add foreign key update action metadata.";
                }
                return rc;
            }
        }

        return SC_OK;
    }

    SC_SQLITE_UPGRADE_REGISTER_STEP(6,
                                    7,
                                    L"6 -> 7",
                                    L"Add foreign key action metadata.",
                                    &ApplySqliteUpgradeStep_6_7)
}  // namespace StableCore::Storage
