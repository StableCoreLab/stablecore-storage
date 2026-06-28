#include "Sqlite/SqliteImplCore.h"

#include <mutex>

#include "SCQuerySqliteExecutor.h"

namespace StableCore::Storage
{

JournalOp FromSqliteJournalOp(int op) noexcept
{
    return static_cast<JournalOp>(op);
}

void EnsureSqliteQueryDispatchRegistered(ISCDatabase* database)
{
    (void)database;
    static std::once_flag once;
    std::call_once(once, []() {
        RegisterQueryExecutionContextDispatch(QueryBackendKind::SQLite, &ExecuteSqliteQueryDispatch);
    });
}

ErrorCode BuildRegisteredUpgradePlan(std::int32_t currentVersion,
                                     std::int32_t targetVersion,
                                     SCUpgradePlan* outPlan)
{
    if (outPlan == nullptr)
    {
        return SC_E_POINTER;
    }

    std::vector<SCMigrationStep> availableSteps;
    const auto& registeredSteps = GetRegisteredSqliteUpgradeSteps();
    availableSteps.reserve(registeredSteps.size());
    for (const auto& registration : registeredSteps)
    {
        availableSteps.push_back(registration.step);
    }

    SCMigrationPlan migrationPlan;
    const ErrorCode planRc = BuildMigrationPlan(currentVersion, targetVersion, availableSteps, &migrationPlan);
    if (Failed(planRc))
    {
        return planRc;
    }

    SCUpgradePlan upgradePlan;
    upgradePlan.currentVersion = currentVersion;
    upgradePlan.targetVersion = targetVersion;
    upgradePlan.requiresConfirmation = true;
    upgradePlan.upgradeRequired = true;
    upgradePlan.compatibilityWindow.minReadableVersion = currentVersion;
    upgradePlan.compatibilityWindow.maxReadableVersion = targetVersion;
    upgradePlan.compatibilityWindow.minWritableVersion = targetVersion;
    upgradePlan.compatibilityWindow.maxWritableVersion = targetVersion;
    upgradePlan.compatibilityWindow.readOnlyAllowed = false;
    upgradePlan.compatibilityWindow.upgradeAllowed = true;
    upgradePlan.orderedSteps = std::move(migrationPlan.orderedSteps);
    upgradePlan.reason = L"Explicit upgrade requested.";

    *outPlan = std::move(upgradePlan);
    return SC_OK;
}

ErrorCode UpgradeOpenedSqliteDatabase(ISCDatabase* database, SCUpgradeResult* outResult)
{
    if (database == nullptr)
    {
        return SC_E_POINTER;
    }

    SCUpgradeResult localResult;
    const std::int32_t currentVersion = database->GetSchemaVersion();
    const std::int32_t targetVersion = GetLatestSupportedSchemaVersion();
    localResult.sourceVersion = currentVersion;
    localResult.targetVersion = targetVersion;

    if (currentVersion == targetVersion)
    {
        localResult.status = SCUpgradeStatus::NotRequired;
        localResult.failureReason = L"Database schema is already up to date.";
        if (outResult != nullptr)
        {
            *outResult = localResult;
        }
        return SC_OK;
    }

    SCUpgradePlan upgradePlan;
    const ErrorCode planRc = BuildRegisteredUpgradePlan(currentVersion, targetVersion, &upgradePlan);
    if (Failed(planRc))
    {
        localResult.status = SCUpgradeStatus::Unsupported;
        localResult.failureReason = L"No registered upgrade path was found.";
        if (outResult != nullptr)
        {
            *outResult = localResult;
        }
        return SC_E_UPGRADE_PATH_NOT_FOUND;
    }

    localResult.status = SCUpgradeStatus::Failed;
    const ErrorCode upgradeRc = database->ExecuteUpgradePlan(upgradePlan, true, &localResult);
    if (outResult != nullptr)
    {
        *outResult = localResult;
    }
    return upgradeRc;
}

ErrorCode InspectDatabaseForOpenOrUpgrade(const std::filesystem::path& filePath,
                                          UpgradeOpenInspection* outInspection)
{
    if (outInspection == nullptr)
    {
        return SC_E_POINTER;
    }

    *outInspection = UpgradeOpenInspection{};

    sqlite3* probeDb = nullptr;
    const std::string probePath = SCCommon::ToUtf8(filePath.wstring());
    if (sqlite3_open_v2(probePath.c_str(), &probeDb, SQLITE_OPEN_READONLY, nullptr) != SQLITE_OK)
    {
        if (probeDb != nullptr)
        {
            sqlite3_close(probeDb);
        }
        return SC_E_INVALIDARG;
    }

    UpgradeOpenInspection inspection;
    inspection.hasMetadataTable = HasTableRaw(probeDb, "metadata");
    inspection.hasJournalTransactionsTable = HasTableRaw(probeDb, "journal_transactions");
    inspection.hasJournalEntriesTable = HasTableRaw(probeDb, "journal_entries");
    inspection.hasJournalSchemaEntriesTable = HasTableRaw(probeDb, "journal_schema_entries");

    if (inspection.hasMetadataTable)
    {
        const ErrorCode versionRc = ReadSchemaVersionRaw(probeDb, &inspection.schemaVersion);
        if (Failed(versionRc))
        {
            sqlite3_close(probeDb);
            return versionRc;
        }
    }

    sqlite3_close(probeDb);
    *outInspection = inspection;
    return SC_OK;
}

ErrorCode ReadSchemaVersionFromFile(const std::filesystem::path& filePath, std::int32_t* outVersion)
{
    if (outVersion == nullptr)
    {
        return SC_E_POINTER;
    }
    if (!IsInspectableFile(filePath))
    {
        return SC_E_INVALIDARG;
    }

    sqlite3* probeDb = nullptr;
    const std::string probePath = SCCommon::ToUtf8(filePath.wstring());
    if (sqlite3_open_v2(probePath.c_str(), &probeDb, SQLITE_OPEN_READONLY, nullptr) != SQLITE_OK)
    {
        if (probeDb != nullptr)
        {
            sqlite3_close(probeDb);
        }
        return SC_E_INVALIDARG;
    }

    const ErrorCode rc = ReadSchemaVersionRaw(probeDb, outVersion);
    sqlite3_close(probeDb);
    return rc;
}

bool IsInspectableFile(const std::filesystem::path& filePath)
{
    std::error_code ec;
    return std::filesystem::exists(filePath, ec) && !ec && std::filesystem::is_regular_file(filePath, ec) &&
           !ec;
}

bool HasTableRaw(sqlite3* db, const char* tableName)
{
    if (db == nullptr || tableName == nullptr || *tableName == '\0')
    {
        return false;
    }

    sqlite3_stmt* stmt = nullptr;
    const int prepareRc = sqlite3_prepare_v2(
        db,
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1;",
        -1,
        &stmt,
        nullptr);
    if (prepareRc != SQLITE_OK)
    {
        if (stmt != nullptr)
        {
            sqlite3_finalize(stmt);
        }
        return false;
    }

    const std::string utf8Name = tableName;
    sqlite3_bind_text(stmt, 1, utf8Name.c_str(), static_cast<int>(utf8Name.size()), SQLITE_TRANSIENT);
    bool hasRow = false;
    const int stepRc = sqlite3_step(stmt);
    hasRow = (stepRc == SQLITE_ROW);
    sqlite3_finalize(stmt);
    return hasRow;
}

ErrorCode ReadSchemaVersionRaw(sqlite3* db, std::int32_t* outVersion)
{
    if (db == nullptr)
    {
        return SC_E_POINTER;
    }
    if (outVersion == nullptr)
    {
        return SC_E_POINTER;
    }

    if (!HasTableRaw(db, "metadata"))
    {
        return SC_E_TABLE_NOT_FOUND;
    }

    sqlite3_stmt* stmt = nullptr;
    const int prepareRc = sqlite3_prepare_v2(
        db,
        "SELECT value FROM metadata WHERE key = 'schema_version' LIMIT 1;",
        -1,
        &stmt,
        nullptr);
    if (prepareRc != SQLITE_OK)
    {
        if (stmt != nullptr)
        {
            sqlite3_finalize(stmt);
        }
        return MapSqliteError(prepareRc);
    }

    bool hasRow = false;
    const int stepRc = sqlite3_step(stmt);
    hasRow = (stepRc == SQLITE_ROW);
    if (stepRc != SQLITE_ROW && stepRc != SQLITE_DONE)
    {
        sqlite3_finalize(stmt);
        return MapSqliteError(stepRc);
    }
    if (!hasRow)
    {
        sqlite3_finalize(stmt);
        return SC_E_RECORD_NOT_FOUND;
    }

    try
    {
        const unsigned char* text = sqlite3_column_text(stmt, 0);
        if (text == nullptr)
        {
            sqlite3_finalize(stmt);
            return SC_E_INVALIDARG;
        }
        *outVersion = std::stoi(reinterpret_cast<const char*>(text));
        sqlite3_finalize(stmt);
        return SC_OK;
    } catch (...)
    {
        sqlite3_finalize(stmt);
        return SC_E_INVALIDARG;
    }
}

ErrorCode ValueComparator::Convert(const SCValue& source, ValueKind targetKind, SCValue* outValue)
{
    if (outValue == nullptr)
    {
        return SC_E_POINTER;
    }
    if (source.IsNull())
    {
        *outValue = SCValue::Null();
        return SC_OK;
    }
    if (source.GetKind() == ValueKind::Binary || targetKind == ValueKind::Binary)
    {
        if (source.GetKind() == ValueKind::Binary && targetKind == ValueKind::Binary)
        {
            std::vector<std::uint8_t> bytes;
            const ErrorCode rc = source.AsBinaryCopy(&bytes);
            if (Failed(rc))
            {
                return rc;
            }
            *outValue = SCValue::FromBinary(std::move(bytes));
            return SC_OK;
        }

        *outValue = SCValue::Null();
        return SC_OK;
    }

    switch (source.GetKind())
    {
        case ValueKind::Int64: {
            std::int64_t intValue = 0;
            const ErrorCode rc = source.AsInt64(&intValue);
            if (Failed(rc))
            {
                return rc;
            }
            switch (targetKind)
            {
                case ValueKind::Int64:
                    *outValue = SCValue::FromInt64(intValue);
                    return SC_OK;
                case ValueKind::Double:
                    *outValue = SCValue::FromDouble(static_cast<double>(intValue));
                    return SC_OK;
                case ValueKind::String:
                    *outValue = SCValue::FromString(std::to_wstring(intValue));
                    return SC_OK;
                default:
                    return SC_E_TYPE_MISMATCH;
            }
        }
        case ValueKind::Double: {
            double doubleValue = 0.0;
            const ErrorCode rc = source.AsDouble(&doubleValue);
            if (Failed(rc))
            {
                return rc;
            }
            if (targetKind == ValueKind::Double)
            {
                *outValue = SCValue::FromDouble(doubleValue);
                return SC_OK;
            }
            return SC_E_TYPE_MISMATCH;
        }
        case ValueKind::Bool: {
            bool boolValue = false;
            const ErrorCode rc = source.AsBool(&boolValue);
            if (Failed(rc))
            {
                return rc;
            }
            if (targetKind == ValueKind::Bool)
            {
                *outValue = SCValue::FromBool(boolValue);
                return SC_OK;
            }
            if (targetKind == ValueKind::String)
            {
                *outValue = SCValue::FromString(boolValue ? L"true" : L"false");
                return SC_OK;
            }
            return SC_E_TYPE_MISMATCH;
        }
        case ValueKind::String: {
            std::wstring text;
            const ErrorCode rc = source.AsStringCopy(&text);
            if (Failed(rc))
            {
                return rc;
            }
            const std::wstring trimmed = SCCommon::Trim(text);
            switch (targetKind)
            {
                case ValueKind::String:
                    *outValue = SCValue::FromString(text);
                    return SC_OK;
                case ValueKind::Int64: {
                    std::int64_t intValue = 0;
                    if (!SCCommon::TryParseInt64(trimmed, &intValue))
                    {
                        return SC_E_TYPE_MISMATCH;
                    }
                    *outValue = SCValue::FromInt64(intValue);
                    return SC_OK;
                }
                case ValueKind::Double: {
                    double doubleValue = 0.0;
                    if (!SCCommon::TryParseDouble(trimmed, &doubleValue))
                    {
                        return SC_E_TYPE_MISMATCH;
                    }
                    *outValue = SCValue::FromDouble(doubleValue);
                    return SC_OK;
                }
                case ValueKind::Bool: {
                    bool boolValue = false;
                    if (!SCCommon::TryParseBool(trimmed, &boolValue))
                    {
                        return SC_E_TYPE_MISMATCH;
                    }
                    *outValue = SCValue::FromBool(boolValue);
                    return SC_OK;
                }
                case ValueKind::RecordId: {
                    std::int64_t recordId = 0;
                    if (!SCCommon::TryParseInt64(trimmed, &recordId))
                    {
                        return SC_E_TYPE_MISMATCH;
                    }
                    *outValue = SCValue::FromRecordId(recordId);
                    return SC_OK;
                }
                default:
                    return SC_E_TYPE_MISMATCH;
            }
        }
        case ValueKind::RecordId: {
            RecordId recordId = 0;
            const ErrorCode rc = source.AsRecordId(&recordId);
            if (Failed(rc))
            {
                return rc;
            }
            if (targetKind == ValueKind::RecordId)
            {
                *outValue = SCValue::FromRecordId(recordId);
                return SC_OK;
            }
            if (targetKind == ValueKind::String)
            {
                *outValue = SCValue::FromString(std::to_wstring(recordId));
                return SC_OK;
            }
            return SC_E_TYPE_MISMATCH;
        }
        case ValueKind::Enum:
            if (targetKind == ValueKind::Enum)
            {
                std::wstring text;
                const ErrorCode rc = source.AsEnumCopy(&text);
                if (Failed(rc))
                {
                    return rc;
                }
                *outValue = SCValue::FromEnum(std::move(text));
                return SC_OK;
            }
            return SC_E_TYPE_MISMATCH;
        case ValueKind::Null:
        default:
            return SC_E_TYPE_MISMATCH;
    }
}

#if defined(_WIN32)
std::wstring GetBackupTempDirectory(const std::wstring& targetPath)
{
    const std::size_t slash = targetPath.find_last_of(L"\\/");
    std::wstring directory;
    if (slash == std::wstring::npos)
    {
        wchar_t currentDirectory[MAX_PATH] = {};
        const DWORD len = GetCurrentDirectoryW(MAX_PATH, currentDirectory);
        if (len == 0 || len >= MAX_PATH)
        {
            return {};
        }
        directory.assign(currentDirectory, currentDirectory + len);
    } else
    {
        directory = targetPath.substr(0, slash);
        if (directory.empty())
        {
            wchar_t currentDirectory[MAX_PATH] = {};
            const DWORD len = GetCurrentDirectoryW(MAX_PATH, currentDirectory);
            if (len == 0 || len >= MAX_PATH)
            {
                return {};
            }
            directory.assign(currentDirectory, currentDirectory + len);
        }
    }

    if (!directory.empty() && directory.back() != L'\\' && directory.back() != L'/')
    {
        directory.push_back(L'\\');
    }
    return directory;
}

bool CreateSiblingTempFile(const std::wstring& targetPath, std::wstring* outTempPath)
{
    if (outTempPath == nullptr)
    {
        return false;
    }

    const std::wstring directory = GetBackupTempDirectory(targetPath);
    if (directory.empty())
    {
        return false;
    }

    wchar_t tempPath[MAX_PATH] = {};
    if (GetTempFileNameW(directory.c_str(), L"bkp", 0, tempPath) == 0)
    {
        return false;
    }

    *outTempPath = tempPath;
    return true;
}
#endif

ErrorCode GetSingleCount(sqlite3* db, const char* sql, std::size_t* outCount)
{
    if (outCount == nullptr)
    {
        return SC_E_POINTER;
    }

    sqlite3_stmt* stmt = nullptr;
    const int prepareRc = sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
    if (prepareRc != SQLITE_OK)
    {
        if (stmt != nullptr)
        {
            sqlite3_finalize(stmt);
        }
        return MapSqliteError(prepareRc);
    }

    const int stepRc = sqlite3_step(stmt);
    ErrorCode rc = SC_OK;
    if (stepRc == SQLITE_ROW)
    {
        *outCount = static_cast<std::size_t>(sqlite3_column_int64(stmt, 0));
    } else if (stepRc == SQLITE_DONE)
    {
        *outCount = 0;
    } else
    {
        rc = MapSqliteError(stepRc);
    }

    sqlite3_finalize(stmt);
    return rc;
}

ErrorCode UpdateMetadataBaselineVersion(sqlite3* db, VersionId version)
{
    sqlite3_stmt* stmt = nullptr;
    const int prepareRc = sqlite3_prepare_v2(
        db, "UPDATE metadata SET value = ? WHERE key = 'baseline_version';", -1, &stmt, nullptr);
    if (prepareRc != SQLITE_OK)
    {
        if (stmt != nullptr)
        {
            sqlite3_finalize(stmt);
        }
        return MapSqliteError(prepareRc);
    }

    const int bindRc = sqlite3_bind_text(
        stmt, 1, std::to_string(static_cast<std::uint64_t>(version)).c_str(), -1, SQLITE_TRANSIENT);
    if (bindRc != SQLITE_OK)
    {
        sqlite3_finalize(stmt);
        return MapSqliteError(bindRc);
    }

    const int stepRc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return stepRc == SQLITE_DONE ? SC_OK : MapSqliteError(stepRc);
}

ErrorCode RunSqliteExec(sqlite3* db, const char* sql)
{
    char* error = nullptr;
    const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &error);
    if (error != nullptr)
    {
        sqlite3_free(error);
    }
    return MapSqliteError(rc);
}

ErrorCode ClearJournalHistoryForBackup(sqlite3* db,
                                       VersionId currentVersion,
                                       std::size_t* removedTransactionCount,
                                       std::size_t* removedEntryCount)
{
    const ErrorCode txCountRc =
        GetSingleCount(db, "SELECT COUNT(*) FROM journal_transactions;", removedTransactionCount);
    if (Failed(txCountRc))
    {
        return txCountRc;
    }

    const ErrorCode entryCountRc =
        GetSingleCount(db, "SELECT COUNT(*) FROM journal_entries;", removedEntryCount);
    if (Failed(entryCountRc))
    {
        return entryCountRc;
    }

    std::size_t removedSchemaEntryCount = 0;
    const ErrorCode schemaEntryCountRc =
        GetSingleCount(db, "SELECT COUNT(*) FROM journal_schema_entries;", &removedSchemaEntryCount);
    if (Failed(schemaEntryCountRc))
    {
        return schemaEntryCountRc;
    }
    *removedEntryCount += removedSchemaEntryCount;

    const ErrorCode deleteEntriesRc = RunSqliteExec(db, "DELETE FROM journal_entries;");
    if (Failed(deleteEntriesRc))
    {
        return deleteEntriesRc;
    }

    const ErrorCode deleteSchemaEntriesRc = RunSqliteExec(db, "DELETE FROM journal_schema_entries;");
    if (Failed(deleteSchemaEntriesRc))
    {
        return deleteSchemaEntriesRc;
    }

    const ErrorCode deleteTransactionsRc = RunSqliteExec(db, "DELETE FROM journal_transactions;");
    if (Failed(deleteTransactionsRc))
    {
        return deleteTransactionsRc;
    }

    return UpdateMetadataBaselineVersion(db, currentVersion);
}

ErrorCode VacuumTargetDatabase(sqlite3* db)
{
    return RunSqliteExec(db, "VACUUM;");
}

ErrorCode ValidateTargetDatabase(sqlite3* db)
{
    sqlite3_stmt* stmt = nullptr;
    int prepareRc = sqlite3_prepare_v2(db, "PRAGMA integrity_check;", -1, &stmt, nullptr);
    if (prepareRc != SQLITE_OK)
    {
        if (stmt != nullptr)
        {
            sqlite3_finalize(stmt);
        }
        return MapSqliteError(prepareRc);
    }

    int stepRc = sqlite3_step(stmt);
    if (stepRc != SQLITE_ROW)
    {
        sqlite3_finalize(stmt);
        return MapSqliteError(stepRc == SQLITE_DONE ? SQLITE_OK : stepRc);
    }

    const std::wstring integrity = SCCommon::FromUtf8(std::string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0))));
    sqlite3_finalize(stmt);
    if (integrity != L"ok")
    {
        return SC_E_VALIDATION_FAILED;
    }

    prepareRc = sqlite3_prepare_v2(db, "PRAGMA foreign_key_check;", -1, &stmt, nullptr);
    if (prepareRc != SQLITE_OK)
    {
        if (stmt != nullptr)
        {
            sqlite3_finalize(stmt);
        }
        return MapSqliteError(prepareRc);
    }

    stepRc = sqlite3_step(stmt);
    if (stepRc != SQLITE_DONE)
    {
        sqlite3_finalize(stmt);
        return SC_E_VALIDATION_FAILED;
    }

    sqlite3_finalize(stmt);
    return SC_OK;
}

ErrorCode GetFileSizeBytes(const std::wstring& path, std::uint64_t* outSize)
{
    if (outSize == nullptr)
    {
        return SC_E_POINTER;
    }

#if defined(_WIN32)
    WIN32_FILE_ATTRIBUTE_DATA data{};
    if (!GetFileAttributesExW(path.c_str(), GetFileExInfoStandard, &data))
    {
        return SC_E_IO_ERROR;
    }
    ULARGE_INTEGER size{};
    size.HighPart = data.nFileSizeHigh;
    size.LowPart = data.nFileSizeLow;
    *outSize = size.QuadPart;
    return SC_OK;
#else
    (void)path;
    *outSize = 0;
    return SC_E_NOTIMPL;
#endif
}

std::wstring ImportSerializer::SerializeValue(const SCValue& value)
{
    std::wstringstream ss;
    ss << static_cast<int>(value.GetKind()) << L'\n';
    switch (value.GetKind())
    {
        case ValueKind::Null:
            break;
        case ValueKind::Int64: {
            std::int64_t v = 0;
            value.AsInt64(&v);
            ss << v;
            break;
        }
        case ValueKind::Double: {
            double v = 0.0;
            value.AsDouble(&v);
            ss << std::setprecision(17) << v;
            break;
        }
        case ValueKind::Bool: {
            bool v = false;
            value.AsBool(&v);
            ss << (v ? L'1' : L'0');
            break;
        }
        case ValueKind::String: {
            std::wstring v;
            value.AsStringCopy(&v);
            ss << v;
            break;
        }
        case ValueKind::RecordId: {
            RecordId v = 0;
            value.AsRecordId(&v);
            ss << v;
            break;
        }
        case ValueKind::Enum: {
            std::wstring v;
            value.AsEnumCopy(&v);
            ss << v;
            break;
        }
        case ValueKind::Binary: {
            std::vector<std::uint8_t> v;
            value.AsBinaryCopy(&v);
            ss << SCCommon::BytesToHex(v);
            break;
        }
    }
    return ss.str();
}

std::wstring ImportSerializer::SerializeConstraintKeyValue(const SCValue& value)
{
    std::wstring payload;
    switch (value.GetKind())
    {
        case ValueKind::Null:
            SCCommon::AppendToken(&payload, L"null");
            break;
        case ValueKind::Int64: {
            std::int64_t v = 0;
            value.AsInt64(&v);
            SCCommon::AppendToken(&payload, L"i64");
            SCCommon::AppendToken(&payload, std::to_wstring(v));
            break;
        }
        case ValueKind::Double: {
            double v = 0.0;
            value.AsDouble(&v);
            std::wstringstream ss;
            ss << std::setprecision(17) << v;
            SCCommon::AppendToken(&payload, L"f64");
            SCCommon::AppendToken(&payload, ss.str());
            break;
        }
        case ValueKind::Bool: {
            bool v = false;
            value.AsBool(&v);
            SCCommon::AppendToken(&payload, L"bool");
            SCCommon::AppendToken(&payload, v ? L"1" : L"0");
            break;
        }
        case ValueKind::String: {
            std::wstring v;
            value.AsStringCopy(&v);
            SCCommon::AppendToken(&payload, L"str");
            SCCommon::AppendToken(&payload, v);
            break;
        }
        case ValueKind::RecordId: {
            RecordId v = 0;
            value.AsRecordId(&v);
            SCCommon::AppendToken(&payload, L"rid");
            SCCommon::AppendToken(&payload, std::to_wstring(v));
            break;
        }
        case ValueKind::Enum: {
            std::wstring v;
            value.AsEnumCopy(&v);
            SCCommon::AppendToken(&payload, L"enum");
            SCCommon::AppendToken(&payload, v);
            break;
        }
        case ValueKind::Binary: {
            std::vector<std::uint8_t> v;
            value.AsBinaryCopy(&v);
            SCCommon::AppendToken(&payload, L"bin");
            SCCommon::AppendToken(&payload, SCCommon::BytesToHex(v));
            break;
        }
    }
    return payload;
}

std::wstring ImportSerializer::SerializeImportSessionPayload(const SCImportStagingArea& session)
{
    std::wstring payload;
    SCCommon::AppendToken(&payload, L"SCIMPORT1");
    SCCommon::AppendToken(&payload, std::to_wstring(session.sessionId));
    SCCommon::AppendToken(&payload, session.sessionName);
    SCCommon::AppendToken(&payload, std::to_wstring(session.baseVersion));
    SCCommon::AppendToken(&payload, std::to_wstring(session.chunkSize));
    SCCommon::AppendToken(&payload, std::to_wstring(static_cast<int>(session.state)));
    SCCommon::AppendToken(&payload, std::to_wstring(static_cast<unsigned long long>(session.chunks.size())));
    for (const auto& chunk : session.chunks)
    {
        SCCommon::AppendToken(&payload, std::to_wstring(chunk.chunkId));
        SCCommon::AppendToken(&payload, std::to_wstring(static_cast<unsigned long long>(chunk.requests.size())));
        for (const auto& request : chunk.requests)
        {
            SCCommon::AppendToken(&payload, request.tableName);
            SCCommon::AppendToken(&payload, std::to_wstring(static_cast<unsigned long long>(request.creates.size())));
            for (const auto& create : request.creates)
            {
                SCCommon::AppendToken(&payload, std::to_wstring(static_cast<unsigned long long>(create.values.size())));
                for (const auto& assignment : create.values)
                {
                    SCCommon::AppendToken(&payload, assignment.fieldName);
                    SCCommon::AppendToken(&payload, ImportSerializer::SerializeValue(assignment.SCValue));
                }
            }

            SCCommon::AppendToken(&payload, std::to_wstring(static_cast<unsigned long long>(request.updates.size())));
            for (const auto& update : request.updates)
            {
                SCCommon::AppendToken(&payload, std::to_wstring(update.recordId));
                SCCommon::AppendToken(&payload, std::to_wstring(static_cast<unsigned long long>(update.values.size())));
                for (const auto& assignment : update.values)
                {
                    SCCommon::AppendToken(&payload, assignment.fieldName);
                    SCCommon::AppendToken(&payload, ImportSerializer::SerializeValue(assignment.SCValue));
                }
            }

            SCCommon::AppendToken(&payload, std::to_wstring(static_cast<unsigned long long>(request.deletes.size())));
            for (RecordId recordId : request.deletes)
            {
                SCCommon::AppendToken(&payload, std::to_wstring(recordId));
            }
        }
    }
    return payload;
}

bool ImportSerializer::DeserializeValue(const std::wstring& token, SCValue* outValue)
{
    if (outValue == nullptr)
    {
        return false;
    }

    const std::size_t separator = token.find(L'\n');
    const std::wstring kindText = (separator == std::wstring::npos) ? token : token.substr(0, separator);
    const std::wstring payload =
        (separator == std::wstring::npos) ? std::wstring{} : token.substr(separator + 1);

    int kind = 0;
    try
    {
        kind = std::stoi(kindText);
    } catch (...)
    {
        return false;
    }

    switch (static_cast<ValueKind>(kind))
    {
        case ValueKind::Null:
            *outValue = SCValue::Null();
            return true;
        case ValueKind::Int64:
            try
            {
                *outValue = SCValue::FromInt64(std::stoll(payload));
                return true;
            } catch (...)
            {
                return false;
            }
        case ValueKind::Double:
            try
            {
                *outValue = SCValue::FromDouble(std::stod(payload));
                return true;
            } catch (...)
            {
                return false;
            }
        case ValueKind::Bool:
            *outValue = SCValue::FromBool(payload == L"1");
            return true;
        case ValueKind::String:
            *outValue = SCValue::FromString(payload);
            return true;
        case ValueKind::RecordId:
            try
            {
                *outValue = SCValue::FromRecordId(static_cast<RecordId>(std::stoll(payload)));
                return true;
            } catch (...)
            {
                return false;
            }
        case ValueKind::Enum:
            *outValue = SCValue::FromEnum(payload);
            return true;
        case ValueKind::Binary: {
            std::vector<std::uint8_t> bytes;
            if (!SCCommon::HexToBytes(payload, &bytes))
            {
                return false;
            }
            *outValue = SCValue::FromBinary(std::move(bytes));
            return true;
        }
        default:
            return false;
    }
}

std::wstring ImportSerializer::SerializeConstraintDef(const SCConstraintDef& def)
{
    std::wstring payload;
    SCCommon::AppendToken(&payload, std::to_wstring(static_cast<int>(def.kind)));
    SCCommon::AppendToken(&payload, def.name);
    SCCommon::AppendToken(&payload, std::to_wstring(static_cast<int>(def.sourceKind)));
    SCCommon::AppendToken(&payload, def.referencedTable);
    SCCommon::AppendToken(&payload, def.checkExpression);
    SCCommon::AppendToken(&payload, std::to_wstring(static_cast<unsigned long long>(def.columns.size())));
    for (const std::wstring& column : def.columns)
    {
        SCCommon::AppendToken(&payload, column);
    }
    SCCommon::AppendToken(&payload, std::to_wstring(static_cast<unsigned long long>(def.referencedColumns.size())));
    for (const std::wstring& column : def.referencedColumns)
    {
        SCCommon::AppendToken(&payload, column);
    }
    SCCommon::AppendToken(&payload, std::to_wstring(static_cast<int>(ToSqliteForeignKeyAction(def.onDelete))));
    SCCommon::AppendToken(&payload, std::to_wstring(static_cast<int>(ToSqliteForeignKeyAction(def.onUpdate))));
    return payload;
}

bool ImportSerializer::TryParseSerializedConstraintBody(const std::vector<std::wstring>& tokens,
                                                         bool actionsBeforeCheckExpression,
                                                         SCConstraintDef* outDef)
{
    if (outDef == nullptr)
    {
        return false;
    }

    auto parseAction = [](const std::wstring& token, SCForeignKeyAction* outAction) -> bool {
        if (outAction == nullptr)
        {
            return false;
        }

        int action = 0;
        try
        {
            action = std::stoi(token);
        } catch (...)
        {
            return false;
        }

        *outAction = FromSqliteForeignKeyAction(action);
        return IsForeignKeyActionValid(*outAction);
    };

    auto parseCount = [](const std::wstring& token, std::size_t* outCount) -> bool {
        if (outCount == nullptr)
        {
            return false;
        }

        try
        {
            *outCount = static_cast<std::size_t>(std::stoull(token));
            return true;
        } catch (...)
        {
            return false;
        }
    };

    SCConstraintDef def = *outDef;
    def.columns.clear();
    def.referencedColumns.clear();
    def.onDelete = SCForeignKeyAction::Restrict;
    def.onUpdate = SCForeignKeyAction::Restrict;

    std::size_t index = 0;
    if (actionsBeforeCheckExpression)
    {
        if (tokens.size() < 3 || !parseAction(tokens[index++], &def.onDelete) ||
            !parseAction(tokens[index++], &def.onUpdate))
        {
            return false;
        }
    }

    if (index >= tokens.size())
    {
        return false;
    }
    def.checkExpression = tokens[index++];

    if (index >= tokens.size())
    {
        return false;
    }

    std::size_t columnCount = 0;
    if (!parseCount(tokens[index++], &columnCount))
    {
        return false;
    }

    if (tokens.size() < index + columnCount + 1)
    {
        return false;
    }

    for (std::size_t columnIndex = 0; columnIndex < columnCount; ++columnIndex)
    {
        def.columns.push_back(tokens[index++]);
    }

    std::size_t referencedColumnCount = 0;
    if (index >= tokens.size() || !parseCount(tokens[index++], &referencedColumnCount))
    {
        return false;
    }

    if (tokens.size() < index + referencedColumnCount)
    {
        return false;
    }

    for (std::size_t columnIndex = 0; columnIndex < referencedColumnCount; ++columnIndex)
    {
        def.referencedColumns.push_back(tokens[index++]);
    }

    if (!actionsBeforeCheckExpression)
    {
        if (index == tokens.size())
        {
            *outDef = std::move(def);
            return true;
        }
        if (tokens.size() != index + 2 || !parseAction(tokens[index++], &def.onDelete) ||
            !parseAction(tokens[index++], &def.onUpdate))
        {
            return false;
        }
    }

    if (index != tokens.size())
    {
        return false;
    }

    *outDef = std::move(def);
    return true;
}

bool ImportSerializer::DeserializeConstraintDef(const std::wstring& payload, SCConstraintDef* outDef)
{
    if (outDef == nullptr)
    {
        return false;
    }

    std::size_t cursor = 0;
    std::wstring token;
    if (!SCCommon::ReadToken(payload, &cursor, &token))
    {
        return false;
    }

    int kind = 0;
    try
    {
        kind = std::stoi(token);
    } catch (...)
    {
        return false;
    }

    SCConstraintDef def;
    def.kind = static_cast<SCConstraintKind>(kind);
    if (!SCCommon::ReadToken(payload, &cursor, &def.name) || !SCCommon::ReadToken(payload, &cursor, &token))
    {
        return false;
    }

    try
    {
        def.sourceKind = static_cast<SCSchemaSourceKind>(std::stoi(token));
    } catch (...)
    {
        return false;
    }

    if (!SCCommon::ReadToken(payload, &cursor, &def.referencedTable) ||
        !SCCommon::ReadToken(payload, &cursor, &token))
    {
        return false;
    }

    std::vector<std::wstring> bodyTokens;
    bodyTokens.push_back(token);
    while (cursor < payload.size())
    {
        if (!SCCommon::ReadToken(payload, &cursor, &token))
        {
            return false;
        }
        bodyTokens.push_back(token);
    }

    if (!ImportSerializer::TryParseSerializedConstraintBody(bodyTokens, false, &def) &&
        (def.kind != SCConstraintKind::ForeignKey ||
         !ImportSerializer::TryParseSerializedConstraintBody(bodyTokens, true, &def)))
    {
        return false;
    }

    *outDef = std::move(def);
    return true;
}

std::wstring ImportSerializer::SerializeIndexDef(const SCIndexDef& def)
{
    std::wstring payload;
    SCCommon::AppendToken(&payload, def.name);
    SCCommon::AppendToken(&payload, std::to_wstring(static_cast<int>(def.sourceKind)));
    SCCommon::AppendToken(&payload, std::to_wstring(static_cast<unsigned long long>(def.columns.size())));
    for (const SCIndexColumnDef& column : def.columns)
    {
        SCCommon::AppendToken(&payload, column.columnName);
        SCCommon::AppendToken(&payload, column.descending ? L"1" : L"0");
    }
    return payload;
}

bool ImportSerializer::DeserializeIndexDef(const std::wstring& payload, SCIndexDef* outDef)
{
    if (outDef == nullptr)
    {
        return false;
    }

    std::size_t cursor = 0;
    std::wstring token;
    SCIndexDef def;
    if (!SCCommon::ReadToken(payload, &cursor, &def.name) || !SCCommon::ReadToken(payload, &cursor, &token))
    {
        return false;
    }

    try
    {
        def.sourceKind = static_cast<SCSchemaSourceKind>(std::stoi(token));
    } catch (...)
    {
        return false;
    }

    if (!SCCommon::ReadToken(payload, &cursor, &token))
    {
        return false;
    }

    std::size_t columnCount = 0;
    try
    {
        columnCount = static_cast<std::size_t>(std::stoull(token));
    } catch (...)
    {
        return false;
    }

    for (std::size_t index = 0; index < columnCount; ++index)
    {
        SCIndexColumnDef column;
        if (!SCCommon::ReadToken(payload, &cursor, &column.columnName) || !SCCommon::ReadToken(payload, &cursor, &token))
        {
            return false;
        }
        column.descending = (token == L"1");
        def.columns.push_back(std::move(column));
    }

    *outDef = std::move(def);
    return cursor == payload.size();
}

bool ImportSerializer::DeserializeImportSessionPayload(const std::wstring& payload, SCImportStagingArea* outSession)
{
    if (outSession == nullptr)
    {
        return false;
    }

    std::size_t cursor = 0;
    std::wstring token;
    if (!SCCommon::ReadToken(payload, &cursor, &token) || token != L"SCIMPORT1")
    {
        return false;
    }

    SCImportStagingArea session;
    if (!SCCommon::ReadToken(payload, &cursor, &token))
    {
        return false;
    }
    try
    {
        session.sessionId = static_cast<SCImportSessionId>(std::stoull(token));
    } catch (...)
    {
        return false;
    }

    if (!SCCommon::ReadToken(payload, &cursor, &session.sessionName))
    {
        return false;
    }
    if (!SCCommon::ReadToken(payload, &cursor, &token))
    {
        return false;
    }
    try
    {
        session.baseVersion = static_cast<VersionId>(std::stoull(token));
    } catch (...)
    {
        return false;
    }
    if (!SCCommon::ReadToken(payload, &cursor, &token))
    {
        return false;
    }
    try
    {
        session.chunkSize = static_cast<std::size_t>(std::stoull(token));
    } catch (...)
    {
        return false;
    }
    if (!SCCommon::ReadToken(payload, &cursor, &token))
    {
        return false;
    }
    try
    {
        session.state = static_cast<SCImportSessionState>(std::stoi(token));
    } catch (...)
    {
        return false;
    }
    if (!SCCommon::ReadToken(payload, &cursor, &token))
    {
        return false;
    }
    std::size_t chunkCount = 0;
    try
    {
        chunkCount = static_cast<std::size_t>(std::stoull(token));
    } catch (...)
    {
        return false;
    }

    for (std::size_t chunkIndex = 0; chunkIndex < chunkCount; ++chunkIndex)
    {
        SCImportChunk chunk;
        if (!SCCommon::ReadToken(payload, &cursor, &token))
        {
            return false;
        }
        try
        {
            chunk.chunkId = static_cast<SCImportChunkId>(std::stoull(token));
        } catch (...)
        {
            return false;
        }

        if (!SCCommon::ReadToken(payload, &cursor, &token))
        {
            return false;
        }
        std::size_t requestCount = 0;
        try
        {
            requestCount = static_cast<std::size_t>(std::stoull(token));
        } catch (...)
        {
            return false;
        }

        for (std::size_t requestIndex = 0; requestIndex < requestCount; ++requestIndex)
        {
            SCBatchTableRequest request;
            if (!SCCommon::ReadToken(payload, &cursor, &request.tableName))
            {
                return false;
            }

            if (!SCCommon::ReadToken(payload, &cursor, &token))
            {
                return false;
            }
            std::size_t createCount = 0;
            try
            {
                createCount = static_cast<std::size_t>(std::stoull(token));
            } catch (...)
            {
                return false;
            }
            for (std::size_t createIndex = 0; createIndex < createCount; ++createIndex)
            {
                SCBatchCreateRecordRequest create;
                if (!SCCommon::ReadToken(payload, &cursor, &token))
                {
                    return false;
                }
                std::size_t valueCount = 0;
                try
                {
                    valueCount = static_cast<std::size_t>(std::stoull(token));
                } catch (...)
                {
                    return false;
                }
                for (std::size_t valueIndex = 0; valueIndex < valueCount; ++valueIndex)
                {
                    SCFieldValueAssignment assignment;
                    std::wstring valueToken;
                    if (!SCCommon::ReadToken(payload, &cursor, &assignment.fieldName) ||
                        !SCCommon::ReadToken(payload, &cursor, &valueToken))
                    {
                        return false;
                    }
                    if (!ImportSerializer::DeserializeValue(valueToken, &assignment.SCValue))
                    {
                        return false;
                    }
                    create.values.push_back(std::move(assignment));
                }
                request.creates.push_back(std::move(create));
            }

            if (!SCCommon::ReadToken(payload, &cursor, &token))
            {
                return false;
            }
            std::size_t updateCount = 0;
            try
            {
                updateCount = static_cast<std::size_t>(std::stoull(token));
            } catch (...)
            {
                return false;
            }
            for (std::size_t updateIndex = 0; updateIndex < updateCount; ++updateIndex)
            {
                SCBatchUpdateRecordRequest update;
                if (!SCCommon::ReadToken(payload, &cursor, &token))
                {
                    return false;
                }
                try
                {
                    update.recordId = static_cast<RecordId>(std::stoll(token));
                } catch (...)
                {
                    return false;
                }
                if (!SCCommon::ReadToken(payload, &cursor, &token))
                {
                    return false;
                }
                std::size_t valueCount = 0;
                try
                {
                    valueCount = static_cast<std::size_t>(std::stoull(token));
                } catch (...)
                {
                    return false;
                }
                for (std::size_t valueIndex = 0; valueIndex < valueCount; ++valueIndex)
                {
                    SCFieldValueAssignment assignment;
                    std::wstring valueToken;
                    if (!SCCommon::ReadToken(payload, &cursor, &assignment.fieldName) ||
                        !SCCommon::ReadToken(payload, &cursor, &valueToken))
                    {
                        return false;
                    }
                    if (!ImportSerializer::DeserializeValue(valueToken, &assignment.SCValue))
                    {
                        return false;
                    }
                    update.values.push_back(std::move(assignment));
                }
                request.updates.push_back(std::move(update));
            }

            if (!SCCommon::ReadToken(payload, &cursor, &token))
            {
                return false;
            }
            std::size_t deleteCount = 0;
            try
            {
                deleteCount = static_cast<std::size_t>(std::stoull(token));
            } catch (...)
            {
                return false;
            }
            for (std::size_t deleteIndex = 0; deleteIndex < deleteCount; ++deleteIndex)
            {
                if (!SCCommon::ReadToken(payload, &cursor, &token))
                {
                    return false;
                }
                try
                {
                    request.deletes.push_back(static_cast<RecordId>(std::stoll(token)));
                } catch (...)
                {
                    return false;
                }
            }

            chunk.requests.push_back(std::move(request));
        }

        session.chunks.push_back(std::move(chunk));
    }

    *outSession = std::move(session);
    return true;
}

ErrorCode MapSqliteError(int sqliteCode, int /*sqliteExtendedCode*/)
{
    switch (sqliteCode)
    {
        case SQLITE_OK:
        case SQLITE_DONE:
        case SQLITE_ROW:
            return SC_OK;
        case SQLITE_CONSTRAINT:
        case SQLITE_CONSTRAINT_PRIMARYKEY:
        case SQLITE_CONSTRAINT_UNIQUE:
        case SQLITE_CONSTRAINT_FOREIGNKEY:
            return SC_E_CONSTRAINT_VIOLATION;
        case SQLITE_BUSY:
        case SQLITE_LOCKED:
            return SC_E_WRITE_CONFLICT;
        case SQLITE_MISMATCH:
            return SC_E_TYPE_MISMATCH;
        default:
            return SC_E_FAIL;
    }
}

// === Composite index helpers ===

bool IsCompositeIndexExplicit(const SCIndexDef& def)
{
    return def.sourceKind == SCSchemaSourceKind::Explicit && !def.columns.empty() &&
           def.columns.size() <= kCompositeIndexMaxColumns;
}

bool IsEqualityIndexOperator(QueryConditionOperator op) noexcept
{
    return op == QueryConditionOperator::Equal;
}

bool IsRangeIndexOperator(QueryConditionOperator op) noexcept
{
    switch (op)
    {
        case QueryConditionOperator::LessThan:
        case QueryConditionOperator::LessThanOrEqual:
        case QueryConditionOperator::GreaterThan:
        case QueryConditionOperator::GreaterThanOrEqual:
        case QueryConditionOperator::Between:
        case QueryConditionOperator::StartsWith:
            return true;
        default:
            return false;
    }
}

std::wstring BuildQueryIndexStorageKey(std::int64_t tableRowId, const std::wstring& indexName)
{
    return std::to_wstring(tableRowId) + L"|" + indexName;
}

// === Validation helpers ===

ErrorCode ValidateColumnDefShape(const SCColumnDef& def)
{
    if (def.name.empty())
    {
        return SC_E_INVALIDARG;
    }
    if (def.columnKind == ColumnKind::Relation)
    {
        if (def.referenceTable.empty())
        {
            return SC_E_SCHEMA_VIOLATION;
        }
        if (def.referenceStorageColumn.empty() && !def.referenceDisplayColumn.empty())
        {
            return SC_E_SCHEMA_VIOLATION;
        }
        if (def.referenceStorageColumn.empty())
        {
            if (def.valueKind != ValueKind::RecordId)
            {
                return SC_E_SCHEMA_VIOLATION;
            }
            if (!def.defaultValue.IsNull() && def.defaultValue.GetKind() != ValueKind::RecordId)
            {
                return SC_E_SCHEMA_VIOLATION;
            }
        }
    } else
    {
        if (!def.referenceTable.empty())
        {
            return SC_E_SCHEMA_VIOLATION;
        }
        if (!def.referenceStorageColumn.empty() || !def.referenceDisplayColumn.empty())
        {
            return SC_E_SCHEMA_VIOLATION;
        }
        if (!def.defaultValue.IsNull() && def.defaultValue.GetKind() != def.valueKind)
        {
            return SC_E_SCHEMA_VIOLATION;
        }
    }
    return SC_OK;
}

ErrorCode ValidateConstraintDefShape(const SCConstraintDef& def)
{
    if (def.name.empty() || def.columns.empty())
    {
        return SC_E_INVALIDARG;
    }
    switch (def.kind)
    {
        case SCConstraintKind::PrimaryKey:
        case SCConstraintKind::Unique:
            if (!def.referencedTable.empty() || !def.referencedColumns.empty() || !def.checkExpression.empty())
            {
                return SC_E_SCHEMA_VIOLATION;
            }
            if (def.onDelete != SCForeignKeyAction::Restrict || def.onUpdate != SCForeignKeyAction::Restrict)
            {
                return SC_E_SCHEMA_VIOLATION;
            }
            break;
        case SCConstraintKind::ForeignKey:
            if (def.referencedTable.empty())
            {
                return SC_E_SCHEMA_VIOLATION;
            }
            if (!IsForeignKeyActionValid(def.onDelete) || !IsForeignKeyActionValid(def.onUpdate))
            {
                return SC_E_SCHEMA_VIOLATION;
            }
            if (!def.checkExpression.empty())
            {
                return SC_E_SCHEMA_VIOLATION;
            }
            if (!def.referencedColumns.empty() && def.referencedColumns.size() != def.columns.size())
            {
                return SC_E_SCHEMA_VIOLATION;
            }
            break;
        case SCConstraintKind::Check:
            if (def.checkExpression.empty())
            {
                return SC_E_SCHEMA_VIOLATION;
            }
            if (!def.referencedTable.empty() || !def.referencedColumns.empty())
            {
                return SC_E_SCHEMA_VIOLATION;
            }
            break;
        default:
            return SC_E_INVALIDARG;
    }
    return SC_OK;
}

// === Column def binding ===

void BindColumnDefForStorage(SqliteStmt& stmt,
                             int displayNameIndex,
                             int valueKindIndex,
                             int columnKindIndex,
                             int nullableIndex,
                             int editableIndex,
                             int userDefinedIndex,
                             int indexedIndex,
                             int participatesIndex,
                             int unitIndex,
                             int referenceTableIndex,
                             int referenceStorageColumnIndex,
                             int referenceDisplayColumnIndex,
                             int defaultKindIndex,
                             int defaultInt64Index,
                             int defaultDoubleIndex,
                             int defaultBoolIndex,
                             int defaultTextIndex,
                             int defaultBlobIndex,
                             const SCColumnDef& def)
{
    stmt.BindText(displayNameIndex, def.displayName);
    stmt.BindInt(valueKindIndex, ToSqliteValueKind(def.valueKind));
    stmt.BindInt(columnKindIndex, ToSqliteColumnKind(def.columnKind));
    stmt.BindInt(nullableIndex, def.nullable ? 1 : 0);
    stmt.BindInt(editableIndex, def.editable ? 1 : 0);
    stmt.BindInt(userDefinedIndex, def.userDefined ? 1 : 0);
    stmt.BindInt(indexedIndex, def.indexed ? 1 : 0);
    stmt.BindInt(participatesIndex, def.participatesInCalc ? 1 : 0);
    stmt.BindText(unitIndex, def.unit);
    stmt.BindText(referenceTableIndex, def.referenceTable);
    stmt.BindText(referenceStorageColumnIndex, def.referenceStorageColumn);
    stmt.BindText(referenceDisplayColumnIndex, def.referenceDisplayColumn);
    StorageCodec::BindValue(stmt,
                            defaultKindIndex,
                            defaultInt64Index,
                            defaultDoubleIndex,
                            defaultBoolIndex,
                            defaultTextIndex,
                            defaultBlobIndex,
                            def.defaultValue);
}

// ── Foreign key helpers ────────────────────────────────────────────

ErrorCode ValidateValueKind(ValueKind expected, const SCValue& value, bool nullable)
{
    if (value.IsNull())
    {
        return nullable ? SC_OK : SC_E_SCHEMA_VIOLATION;
    }
    return value.GetKind() == expected ? SC_OK : SC_E_TYPE_MISMATCH;
}

int ToSqliteForeignKeyAction(SCForeignKeyAction action) noexcept
{
    return static_cast<int>(action);
}

SCForeignKeyAction FromSqliteForeignKeyAction(int action) noexcept
{
    switch (action)
    {
        case static_cast<int>(SCForeignKeyAction::NoAction):
            return SCForeignKeyAction::NoAction;
        case static_cast<int>(SCForeignKeyAction::Restrict):
            return SCForeignKeyAction::Restrict;
        case static_cast<int>(SCForeignKeyAction::SetNull):
            return SCForeignKeyAction::SetNull;
        case static_cast<int>(SCForeignKeyAction::SetDefault):
            return SCForeignKeyAction::SetDefault;
        case static_cast<int>(SCForeignKeyAction::Cascade):
            return SCForeignKeyAction::Cascade;
        default:
            return SCForeignKeyAction::NoAction;
    }
}

bool IsForeignKeyActionValid(SCForeignKeyAction action) noexcept
{
    switch (action)
    {
        case SCForeignKeyAction::Restrict:
        case SCForeignKeyAction::NoAction:
        case SCForeignKeyAction::SetNull:
        case SCForeignKeyAction::SetDefault:
        case SCForeignKeyAction::Cascade:
            return true;
        default:
            return false;
    }
}

std::wstring ResolveForeignKeyReferencedColumn(const SCConstraintDef& constraint, std::size_t index)
{
    if (index < constraint.referencedColumns.size() && !constraint.referencedColumns[index].empty())
    {
        return constraint.referencedColumns[index];
    }
    return index < constraint.columns.size() ? constraint.columns[index] : std::wstring{};
}

ErrorCode ValidateIndexDefShape(const SCIndexDef& def)
{
    if (def.name.empty() || def.columns.empty())
    {
        return SC_E_INVALIDARG;
    }

    for (const SCIndexColumnDef& column : def.columns)
    {
        if (column.columnName.empty())
        {
            return SC_E_INVALIDARG;
        }
    }

    return SC_OK;
}

std::wstring MakeForeignKeyReferenceCacheKey(const std::wstring& tableName, const std::wstring& columnName)
{
    return SCCommon::ToUpper(tableName) + L"|" + SCCommon::ToUpper(columnName);
}

} // namespace StableCore::Storage
