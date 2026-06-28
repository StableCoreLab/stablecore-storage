#include "SCFactory.h"
#include "SCQuerySqliteExecutor.h"
#include "SCSchemaEdit.h"

#include "Sqlite/SqliteImplCore.h"
#include "Sqlite/SqliteImplDatabase.h"

namespace StableCore::Storage
{

    ErrorCode CreateFileDatabase(const wchar_t* path, const SCOpenDatabaseOptions& options, SCDbPtr& outDatabase)
    {
        if (path == nullptr)
        {
            return SC_E_POINTER;
        }

        outDatabase.Reset();
        try
        {
            const auto openDatabase = [path](const SCOpenDatabaseOptions& openOptions, SCDbPtr* database) -> ErrorCode {
                if (database == nullptr)
                {
                    return SC_E_POINTER;
                }

                try
                {
                    *database = SCMakeRef<SqliteDatabase>(std::wstring{path}, openOptions);
                    EnsureSqliteQueryDispatchRegistered(database->Get());
                    return SC_OK;
                } catch (...)
                {
                    if (database->Get() != nullptr)
                    {
                        if (auto* sqliteDb = dynamic_cast<SqliteDatabase*>(database->Get()))
                        {
                            sqliteDb->SuppressCleanShutdownOnDestroy();
                        }
                    }
                    database->Reset();
                    return SC_E_FAIL;
                }
            };

            const std::filesystem::path filePath(path);

            if (options.openMode == SCDatabaseOpenMode::ReadOnly)
            {
                std::int32_t currentVersion = 0;
                if (IsInspectableFile(filePath))
                {
                    const ErrorCode versionRc = ReadSchemaVersionFromFile(filePath, &currentVersion);
                    if (Failed(versionRc))
                    {
                        return versionRc;
                    }

                    if (currentVersion != GetLatestSupportedSchemaVersion())
                    {
                        return SC_E_NOTIMPL;
                    }
                }
                SCOpenDatabaseOptions readOnlyOptions = options;
                readOnlyOptions.openMode = SCDatabaseOpenMode::ReadOnly;

                SCDbPtr readOnlyDatabase;
                ErrorCode rc = openDatabase(readOnlyOptions, &readOnlyDatabase);
                if (Failed(rc))
                {
                    return rc;
                }

                outDatabase = std::move(readOnlyDatabase);
                return SC_OK;
            }

            if (IsInspectableFile(filePath))
            {
                UpgradeOpenInspection inspection;
                const ErrorCode inspectRc = InspectDatabaseForOpenOrUpgrade(filePath, &inspection);
                if (Failed(inspectRc))
                {
                    return inspectRc;
                }

                if (!inspection.hasMetadataTable)
                {
                    return SC_E_INVALIDARG;
                }

                if (!inspection.hasJournalTransactionsTable || !inspection.hasJournalEntriesTable ||
                    !inspection.hasJournalSchemaEntriesTable)
                {
                    return SC_E_JOURNAL_TABLE_MISSING;
                }
            }

            SCDbPtr database;
            ErrorCode rc = openDatabase(options, &database);
            if (Failed(rc))
            {
                return rc;
            }

            const std::int32_t currentVersion = database->GetSchemaVersion();
            const std::int32_t targetVersion = GetLatestSupportedSchemaVersion();
            if (currentVersion == targetVersion)
            {
                outDatabase = std::move(database);
                return SC_OK;
            }

            SCUpgradeResult upgradeResult;
            const ErrorCode upgradeRc = UpgradeOpenedSqliteDatabase(database.Get(), &upgradeResult);
            if (Failed(upgradeRc))
            {
                return upgradeRc;
            }

            outDatabase = std::move(database);
            return SC_OK;
        } catch (...)
        {
            outDatabase.Reset();
            return SC_E_FAIL;
        }
    }

    ErrorCode UpgradeFileDatabase(const wchar_t* path, SCDbPtr& outDatabase, SCUpgradeResult* outResult)
    {
        if (path == nullptr)
        {
            return SC_E_POINTER;
        }

        outDatabase.Reset();
        if (outResult != nullptr)
        {
            *outResult = SCUpgradeResult{};
        }

        try
        {
            const std::filesystem::path filePath(path);
            if (!std::filesystem::exists(filePath))
            {
                if (outResult != nullptr)
                {
                    outResult->status = SCUpgradeStatus::Unsupported;
                    outResult->failureReason = L"Database file does not exist.";
                }
                return SC_E_INVALIDARG;
            }

            UpgradeOpenInspection inspection;
            const ErrorCode inspectRc = InspectDatabaseForOpenOrUpgrade(filePath, &inspection);
            if (Failed(inspectRc))
            {
                if (outResult != nullptr)
                {
                    outResult->status = SCUpgradeStatus::Failed;
                    outResult->failureReason = L"Failed to inspect database for upgrade.";
                }
                return inspectRc;
            }

            if (!inspection.hasMetadataTable)
            {
                if (outResult != nullptr)
                {
                    outResult->status = SCUpgradeStatus::Unsupported;
                    outResult->failureReason = L"Database file does not contain the metadata table.";
                }
                return SC_E_INVALIDARG;
            }

            if (!inspection.hasJournalTransactionsTable || !inspection.hasJournalEntriesTable ||
                !inspection.hasJournalSchemaEntriesTable)
            {
                if (outResult != nullptr)
                {
                    outResult->status = SCUpgradeStatus::Unsupported;
                    outResult->sourceVersion = inspection.schemaVersion;
                    outResult->targetVersion = GetLatestSupportedSchemaVersion();
                    outResult->failureReason = L"Database is missing required journal tables.";
                }
                return SC_E_JOURNAL_TABLE_MISSING;
            }

            const std::int32_t targetVersion = GetLatestSupportedSchemaVersion();
            if (inspection.schemaVersion != targetVersion)
            {
                SCUpgradePlan upgradePlan;
                const ErrorCode planRc =
                    BuildRegisteredUpgradePlan(inspection.schemaVersion, targetVersion, &upgradePlan);
                if (Failed(planRc))
                {
                    if (outResult != nullptr)
                    {
                        outResult->status = SCUpgradeStatus::Unsupported;
                        outResult->sourceVersion = inspection.schemaVersion;
                        outResult->targetVersion = targetVersion;
                        outResult->failureReason = L"No registered upgrade path was found.";
                    }
                    return SC_E_UPGRADE_PATH_NOT_FOUND;
                }
            }

            SCOpenDatabaseOptions openOptions;
            openOptions.openMode = SCDatabaseOpenMode::Normal;

            SCDbPtr database;
            const ErrorCode openRc = [&]() -> ErrorCode {
                try
                {
                    database = SCMakeRef<SqliteDatabase>(std::wstring{path}, openOptions);
                    EnsureSqliteQueryDispatchRegistered(database.Get());
                    return SC_OK;
                } catch (...)
                {
                    if (database.Get() != nullptr)
                    {
                        if (auto* sqliteDb = dynamic_cast<SqliteDatabase*>(database.Get()))
                        {
                            sqliteDb->SuppressCleanShutdownOnDestroy();
                        }
                    }
                    database.Reset();
                    return SC_E_FAIL;
                }
            }();
            if (Failed(openRc))
            {
                if (outResult != nullptr)
                {
                    outResult->status = SCUpgradeStatus::Failed;
                    outResult->failureReason = L"Failed to open database for upgrade.";
                }
                return openRc;
            }

            auto* sqliteDb = dynamic_cast<SqliteDatabase*>(database.Get());
            if (sqliteDb == nullptr)
            {
                if (outResult != nullptr)
                {
                    outResult->status = SCUpgradeStatus::Unsupported;
                    outResult->failureReason = L"Database backend does not support upgrade.";
                }
                return SC_E_NOTIMPL;
            }

            const std::int32_t currentVersion = sqliteDb->GetSchemaVersion();
            if (currentVersion == targetVersion)
            {
                if (outResult != nullptr)
                {
                    outResult->status = SCUpgradeStatus::NotRequired;
                    outResult->sourceVersion = currentVersion;
                    outResult->targetVersion = targetVersion;
                    outResult->failureReason = L"Database schema is already up to date.";
                }
                outDatabase = std::move(database);
                return SC_OK;
            }

            SCUpgradeResult upgradeResult;
            const ErrorCode upgradeRc = UpgradeOpenedSqliteDatabase(database.Get(), &upgradeResult);
            if (Failed(upgradeRc))
            {
                if (outResult != nullptr)
                {
                    *outResult = upgradeResult;
                }
                outDatabase.Reset();
                return upgradeRc;
            }

            if (outResult != nullptr)
            {
                *outResult = upgradeResult;
            }
            outDatabase = std::move(database);
            return SC_OK;
        } catch (...)
        {
            outDatabase.Reset();
            if (outResult != nullptr)
            {
                outResult->status = SCUpgradeStatus::Failed;
                outResult->failureReason = L"Unexpected failure while upgrading database.";
            }
            return SC_E_FAIL;
        }
    }

}  // namespace StableCore::Storage
