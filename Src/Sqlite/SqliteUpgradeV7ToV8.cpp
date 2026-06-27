#include "SqliteUpgradeRegistry.h"

namespace StableCore::Storage
{
    ErrorCode ApplySqliteUpgradeStep_7_8(SqliteUpgradeContext& context, SCUpgradeResult* outResult)
    {
        if (outResult != nullptr)
        {
            outResult->failureReason.clear();
        }

        const ErrorCode rc = context.ExecuteSql(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_schema_constraints_table_name "
            "ON schema_constraints(table_id, name);");
        if (Failed(rc))
        {
            if (outResult != nullptr)
            {
                outResult->failureReason =
                    L"Failed to enforce unique constraint names within each table.";
            }
            return rc;
        }

        return SC_OK;
    }

    SC_SQLITE_UPGRADE_REGISTER_STEP(7,
                                    8,
                                    L"7 -> 8",
                                    L"Enforce per-table uniqueness for constraint names.",
                                    &ApplySqliteUpgradeStep_7_8)
}  // namespace StableCore::Storage
