#include "SCFactory.h"
#include "ISCQuery.h"
#include "SCQuerySqliteExecutor.h"
#include "SCSchemaEdit.h"

#include <algorithm>
#include <atomic>
#include <iomanip>
#include <cstring>
#include <cwctype>
#include <filesystem>
#include <mutex>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <sqlite3.h>

#include "ISCDiagnostics.h"
#include "SCBatch.h"
#include "SCMigration.h"
#include "SCQuerySqliteIndexAccess.h"
#include "SqliteUpgradeRegistry.h"
#include "SCRefCounted.h"

#if defined(_WIN32)
#include <windows.h>
#endif

namespace StableCore::Storage
{
    namespace
    {
        bool AppendToken(std::wstring* out, const std::wstring& token);
        int ToSqliteForeignKeyAction(SCForeignKeyAction action) noexcept;
        SCForeignKeyAction FromSqliteForeignKeyAction(int action) noexcept;
        bool IsForeignKeyActionValid(SCForeignKeyAction action) noexcept;

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

        // Shared upgrade executor used by the factory open path and the
        // explicit upgrade entry point.
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

        ErrorCode MapSqliteError(int code);
        std::string ToUtf8(const std::wstring& text);
        bool HasTableRaw(sqlite3* db, const char* tableName);
        bool HasTableColumnRaw(sqlite3* db, const char* tableName, const char* columnName);
        ErrorCode ReadSchemaVersionRaw(sqlite3* db, std::int32_t* outVersion);
        bool IsInspectableFile(const std::filesystem::path& filePath);
        ErrorCode ReadSchemaVersionFromFile(const std::filesystem::path& filePath, std::int32_t* outVersion);

        struct UpgradeOpenInspection
        {
            std::int32_t schemaVersion{0};
            bool hasMetadataTable{false};
            bool hasJournalTransactionsTable{false};
            bool hasJournalEntriesTable{false};
            bool hasJournalSchemaEntriesTable{false};
        };

        ErrorCode InspectDatabaseForOpenOrUpgrade(const std::filesystem::path& filePath,
                                                  UpgradeOpenInspection* outInspection)
        {
            if (outInspection == nullptr)
            {
                return SC_E_POINTER;
            }

            *outInspection = UpgradeOpenInspection{};

            sqlite3* probeDb = nullptr;
            const std::string probePath = ToUtf8(filePath.wstring());
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
            const std::string probePath = ToUtf8(filePath.wstring());
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

        constexpr int kStackUndo = 0;
        constexpr int kStackRedo = 1;

        std::string ToUtf8(const std::wstring& text)
        {
#if defined(_WIN32)
            if (text.empty())
            {
                return {};
            }

            const int bytes = WideCharToMultiByte(
                CP_UTF8, 0, text.c_str(), static_cast<int>(text.size()), nullptr, 0, nullptr, nullptr);
            std::string utf8(static_cast<std::size_t>(bytes), '\0');
            WideCharToMultiByte(
                CP_UTF8, 0, text.c_str(), static_cast<int>(text.size()), utf8.data(), bytes, nullptr, nullptr);
            return utf8;
#else
            return std::string(text.begin(), text.end());
#endif
        }

        std::wstring FromUtf8(const char* text)
        {
            if (text == nullptr)
            {
                return {};
            }

#if defined(_WIN32)
            const int chars = MultiByteToWideChar(CP_UTF8, 0, text, -1, nullptr, 0);
            if (chars <= 1)
            {
                return {};
            }

            std::wstring wide(static_cast<std::size_t>(chars - 1), L'\0');
            MultiByteToWideChar(CP_UTF8, 0, text, -1, wide.data(), chars);
            return wide;
#else
            std::string narrow(text);
            return std::wstring(narrow.begin(), narrow.end());
#endif
        }

        std::wstring TrimCopy(const std::wstring& text)
        {
            const auto first =
                std::find_if_not(text.begin(), text.end(), [](wchar_t ch) { return std::iswspace(ch) != 0; });
            const auto last =
                std::find_if_not(text.rbegin(), text.rend(), [](wchar_t ch) { return std::iswspace(ch) != 0; }).base();
            if (first >= last)
            {
                return {};
            }
            return std::wstring(first, last);
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

        bool TryParseInt64Strict(const std::wstring& text, std::int64_t* outValue)
        {
            if (outValue == nullptr)
            {
                return false;
            }

            try
            {
                std::size_t parsed = 0;
                const long long value = std::stoll(text, &parsed, 10);
                if (parsed != text.size())
                {
                    return false;
                }
                *outValue = static_cast<std::int64_t>(value);
                return true;
            } catch (...)
            {
                return false;
            }
        }

        bool TryParseDoubleStrict(const std::wstring& text, double* outValue)
        {
            if (outValue == nullptr)
            {
                return false;
            }

            try
            {
                std::size_t parsed = 0;
                const double value = std::stod(text, &parsed);
                if (parsed != text.size())
                {
                    return false;
                }
                *outValue = value;
                return true;
            } catch (...)
            {
                return false;
            }
        }

        bool TryParseBoolStrict(const std::wstring& text, bool* outValue)
        {
            if (outValue == nullptr)
            {
                return false;
            }

            std::wstring normalized;
            normalized.reserve(text.size());
            for (wchar_t ch : text)
            {
                normalized.push_back(static_cast<wchar_t>(std::towlower(ch)));
            }

            if (normalized == L"true" || normalized == L"1")
            {
                *outValue = true;
                return true;
            }
            if (normalized == L"false" || normalized == L"0")
            {
                *outValue = false;
                return true;
            }
            return false;
        }

        std::wstring BytesToHex(const std::vector<std::uint8_t>& bytes)
        {
            static constexpr wchar_t kHexDigits[] = L"0123456789ABCDEF";
            std::wstring hex;
            hex.reserve(bytes.size() * 2);
            for (std::uint8_t byte : bytes)
            {
                hex.push_back(kHexDigits[(byte >> 4) & 0x0F]);
                hex.push_back(kHexDigits[byte & 0x0F]);
            }
            return hex;
        }

        bool HexValue(wchar_t ch, std::uint8_t* outValue)
        {
            if (outValue == nullptr)
            {
                return false;
            }

            if (ch >= L'0' && ch <= L'9')
            {
                *outValue = static_cast<std::uint8_t>(ch - L'0');
                return true;
            }
            if (ch >= L'a' && ch <= L'f')
            {
                *outValue = static_cast<std::uint8_t>(10 + ch - L'a');
                return true;
            }
            if (ch >= L'A' && ch <= L'F')
            {
                *outValue = static_cast<std::uint8_t>(10 + ch - L'A');
                return true;
            }
            return false;
        }

        bool HexToBytes(const std::wstring& hex, std::vector<std::uint8_t>* outBytes)
        {
            if (outBytes == nullptr || (hex.size() % 2) != 0)
            {
                return false;
            }

            std::vector<std::uint8_t> bytes;
            bytes.reserve(hex.size() / 2);
            for (std::size_t index = 0; index < hex.size(); index += 2)
            {
                std::uint8_t high = 0;
                std::uint8_t low = 0;
                if (!HexValue(hex[index], &high) || !HexValue(hex[index + 1], &low))
                {
                    return false;
                }
                bytes.push_back(static_cast<std::uint8_t>((high << 4) | low));
            }

            *outBytes = std::move(bytes);
            return true;
        }

        ErrorCode ConvertColumnValue(const SCValue& source, ValueKind targetKind, SCValue* outValue)
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
                    const std::wstring trimmed = TrimCopy(text);
                    switch (targetKind)
                    {
                        case ValueKind::String:
                            *outValue = SCValue::FromString(text);
                            return SC_OK;
                        case ValueKind::Int64: {
                            std::int64_t intValue = 0;
                            if (!TryParseInt64Strict(trimmed, &intValue))
                            {
                                return SC_E_TYPE_MISMATCH;
                            }
                            *outValue = SCValue::FromInt64(intValue);
                            return SC_OK;
                        }
                        case ValueKind::Double: {
                            double doubleValue = 0.0;
                            if (!TryParseDoubleStrict(trimmed, &doubleValue))
                            {
                                return SC_E_TYPE_MISMATCH;
                            }
                            *outValue = SCValue::FromDouble(doubleValue);
                            return SC_OK;
                        }
                        case ValueKind::Bool: {
                            bool boolValue = false;
                            if (!TryParseBoolStrict(trimmed, &boolValue))
                            {
                                return SC_E_TYPE_MISMATCH;
                            }
                            *outValue = SCValue::FromBool(boolValue);
                            return SC_OK;
                        }
                        case ValueKind::RecordId: {
                            std::int64_t recordId = 0;
                            if (!TryParseInt64Strict(trimmed, &recordId))
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

        struct ScopedDeleteFile
        {
            explicit ScopedDeleteFile(std::wstring path) : path_(std::move(path))
            {
            }
            ~ScopedDeleteFile()
            {
                if (!path_.empty())
                {
                    DeleteFileW(path_.c_str());
                }
            }

            void Release() noexcept
            {
                path_.clear();
            }

            const std::wstring& Path() const noexcept
            {
                return path_;
            }

        private:
            std::wstring path_;
        };

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

            const std::wstring integrity = FromUtf8(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0)));
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

        std::wstring SerializeImportValue(const SCValue& value)
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
                    ss << BytesToHex(v);
                    break;
                }
            }
            return ss.str();
        }

        std::wstring SerializeConstraintKeyValue(const SCValue& value)
        {
            std::wstring payload;
            switch (value.GetKind())
            {
                case ValueKind::Null:
                    AppendToken(&payload, L"null");
                    break;
                case ValueKind::Int64: {
                    std::int64_t v = 0;
                    value.AsInt64(&v);
                    AppendToken(&payload, L"i64");
                    AppendToken(&payload, std::to_wstring(v));
                    break;
                }
                case ValueKind::Double: {
                    double v = 0.0;
                    value.AsDouble(&v);
                    std::wstringstream ss;
                    ss << std::setprecision(17) << v;
                    AppendToken(&payload, L"f64");
                    AppendToken(&payload, ss.str());
                    break;
                }
                case ValueKind::Bool: {
                    bool v = false;
                    value.AsBool(&v);
                    AppendToken(&payload, L"bool");
                    AppendToken(&payload, v ? L"1" : L"0");
                    break;
                }
                case ValueKind::String: {
                    std::wstring v;
                    value.AsStringCopy(&v);
                    AppendToken(&payload, L"str");
                    AppendToken(&payload, v);
                    break;
                }
                case ValueKind::RecordId: {
                    RecordId v = 0;
                    value.AsRecordId(&v);
                    AppendToken(&payload, L"rid");
                    AppendToken(&payload, std::to_wstring(v));
                    break;
                }
                case ValueKind::Enum: {
                    std::wstring v;
                    value.AsEnumCopy(&v);
                    AppendToken(&payload, L"enum");
                    AppendToken(&payload, v);
                    break;
                }
                case ValueKind::Binary: {
                    std::vector<std::uint8_t> v;
                    value.AsBinaryCopy(&v);
                    AppendToken(&payload, L"bin");
                    AppendToken(&payload, BytesToHex(v));
                    break;
                }
            }
            return payload;
        }

        bool AppendToken(std::wstring* out, const std::wstring& token)
        {
            if (out == nullptr)
            {
                return false;
            }

            *out += std::to_wstring(static_cast<unsigned long long>(token.size()));
            *out += L':';
            *out += token;
            return true;
        }

        bool ReadToken(const std::wstring& payload, std::size_t* cursor, std::wstring* outToken)
        {
            if (cursor == nullptr || outToken == nullptr)
            {
                return false;
            }
            if (*cursor >= payload.size())
            {
                return false;
            }

            const std::size_t colon = payload.find(L':', *cursor);
            if (colon == std::wstring::npos)
            {
                return false;
            }

            const std::wstring lengthText = payload.substr(*cursor, colon - *cursor);
            std::size_t length = 0;
            try
            {
                length = static_cast<std::size_t>(std::stoull(lengthText));
            } catch (...)
            {
                return false;
            }

            const std::size_t start = colon + 1;
            if (start + length > payload.size())
            {
                return false;
            }

            *outToken = payload.substr(start, length);
            *cursor = start + length;
            return true;
        }

        std::wstring SerializeImportSessionPayload(const SCImportStagingArea& session)
        {
            std::wstring payload;
            AppendToken(&payload, L"SCIMPORT1");
            AppendToken(&payload, std::to_wstring(session.sessionId));
            AppendToken(&payload, session.sessionName);
            AppendToken(&payload, std::to_wstring(session.baseVersion));
            AppendToken(&payload, std::to_wstring(session.chunkSize));
            AppendToken(&payload, std::to_wstring(static_cast<int>(session.state)));
            AppendToken(&payload, std::to_wstring(static_cast<unsigned long long>(session.chunks.size())));
            for (const auto& chunk : session.chunks)
            {
                AppendToken(&payload, std::to_wstring(chunk.chunkId));
                AppendToken(&payload, std::to_wstring(static_cast<unsigned long long>(chunk.requests.size())));
                for (const auto& request : chunk.requests)
                {
                    AppendToken(&payload, request.tableName);
                    AppendToken(&payload, std::to_wstring(static_cast<unsigned long long>(request.creates.size())));
                    for (const auto& create : request.creates)
                    {
                    AppendToken(&payload, std::to_wstring(static_cast<unsigned long long>(create.values.size())));
                        for (const auto& assignment : create.values)
                        {
                            AppendToken(&payload, assignment.fieldName);
                            AppendToken(&payload, SerializeImportValue(assignment.SCValue));
                        }
                    }

                    AppendToken(&payload, std::to_wstring(static_cast<unsigned long long>(request.updates.size())));
                    for (const auto& update : request.updates)
                    {
                        AppendToken(&payload, std::to_wstring(update.recordId));
                        AppendToken(&payload, std::to_wstring(static_cast<unsigned long long>(update.values.size())));
                        for (const auto& assignment : update.values)
                        {
                            AppendToken(&payload, assignment.fieldName);
                            AppendToken(&payload, SerializeImportValue(assignment.SCValue));
                        }
                    }

                    AppendToken(&payload, std::to_wstring(static_cast<unsigned long long>(request.deletes.size())));
                    for (RecordId recordId : request.deletes)
                    {
                        AppendToken(&payload, std::to_wstring(recordId));
                    }
                }
            }
            return payload;
        }

        bool DeserializeImportValue(const std::wstring& token, SCValue* outValue)
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
                    if (!HexToBytes(payload, &bytes))
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

        std::wstring SerializeConstraintDef(const SCConstraintDef& def)
        {
            std::wstring payload;
            AppendToken(&payload, std::to_wstring(static_cast<int>(def.kind)));
            AppendToken(&payload, def.name);
            AppendToken(&payload, std::to_wstring(static_cast<int>(def.sourceKind)));
            AppendToken(&payload, def.referencedTable);
            AppendToken(&payload, def.checkExpression);
            AppendToken(&payload, std::to_wstring(static_cast<unsigned long long>(def.columns.size())));
            for (const std::wstring& column : def.columns)
            {
                AppendToken(&payload, column);
            }
            AppendToken(&payload, std::to_wstring(static_cast<unsigned long long>(def.referencedColumns.size())));
            for (const std::wstring& column : def.referencedColumns)
            {
                AppendToken(&payload, column);
            }
            AppendToken(&payload, std::to_wstring(static_cast<int>(ToSqliteForeignKeyAction(def.onDelete))));
            AppendToken(&payload, std::to_wstring(static_cast<int>(ToSqliteForeignKeyAction(def.onUpdate))));
            return payload;
        }

        bool TryParseSerializedConstraintBody(const std::vector<std::wstring>& tokens,
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

        bool DeserializeConstraintDef(const std::wstring& payload, SCConstraintDef* outDef)
        {
            if (outDef == nullptr)
            {
                return false;
            }

            std::size_t cursor = 0;
            std::wstring token;
            if (!ReadToken(payload, &cursor, &token))
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
            if (!ReadToken(payload, &cursor, &def.name) || !ReadToken(payload, &cursor, &token))
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

            if (!ReadToken(payload, &cursor, &def.referencedTable) ||
                !ReadToken(payload, &cursor, &token))
            {
                return false;
            }

            std::vector<std::wstring> bodyTokens;
            bodyTokens.push_back(token);
            while (cursor < payload.size())
            {
                if (!ReadToken(payload, &cursor, &token))
                {
                    return false;
                }
                bodyTokens.push_back(token);
            }

            if (!TryParseSerializedConstraintBody(bodyTokens, false, &def) &&
                (def.kind != SCConstraintKind::ForeignKey ||
                 !TryParseSerializedConstraintBody(bodyTokens, true, &def)))
            {
                return false;
            }

            *outDef = std::move(def);
            return true;
        }

        std::wstring SerializeIndexDef(const SCIndexDef& def)
        {
            std::wstring payload;
            AppendToken(&payload, def.name);
            AppendToken(&payload, std::to_wstring(static_cast<int>(def.sourceKind)));
            AppendToken(&payload, std::to_wstring(static_cast<unsigned long long>(def.columns.size())));
            for (const SCIndexColumnDef& column : def.columns)
            {
                AppendToken(&payload, column.columnName);
                AppendToken(&payload, column.descending ? L"1" : L"0");
            }
            return payload;
        }

        bool DeserializeIndexDef(const std::wstring& payload, SCIndexDef* outDef)
        {
            if (outDef == nullptr)
            {
                return false;
            }

            std::size_t cursor = 0;
            std::wstring token;
            SCIndexDef def;
            if (!ReadToken(payload, &cursor, &def.name) || !ReadToken(payload, &cursor, &token))
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

            if (!ReadToken(payload, &cursor, &token))
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
                if (!ReadToken(payload, &cursor, &column.columnName) || !ReadToken(payload, &cursor, &token))
                {
                    return false;
                }
                column.descending = (token == L"1");
                def.columns.push_back(std::move(column));
            }

            *outDef = std::move(def);
            return cursor == payload.size();
        }

        bool DeserializeImportSessionPayload(const std::wstring& payload, SCImportStagingArea* outSession)
        {
            if (outSession == nullptr)
            {
                return false;
            }

            std::size_t cursor = 0;
            std::wstring token;
            if (!ReadToken(payload, &cursor, &token) || token != L"SCIMPORT1")
            {
                return false;
            }

            SCImportStagingArea session;
            if (!ReadToken(payload, &cursor, &token))
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

            if (!ReadToken(payload, &cursor, &session.sessionName))
            {
                return false;
            }
            if (!ReadToken(payload, &cursor, &token))
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
            if (!ReadToken(payload, &cursor, &token))
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
            if (!ReadToken(payload, &cursor, &token))
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
            if (!ReadToken(payload, &cursor, &token))
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
                if (!ReadToken(payload, &cursor, &token))
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

                if (!ReadToken(payload, &cursor, &token))
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
                    if (!ReadToken(payload, &cursor, &request.tableName))
                    {
                        return false;
                    }

                    if (!ReadToken(payload, &cursor, &token))
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
                        if (!ReadToken(payload, &cursor, &token))
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
                            if (!ReadToken(payload, &cursor, &assignment.fieldName) ||
                                !ReadToken(payload, &cursor, &valueToken))
                            {
                                return false;
                            }
                            if (!DeserializeImportValue(valueToken, &assignment.SCValue))
                            {
                                return false;
                            }
                            create.values.push_back(std::move(assignment));
                        }
                        request.creates.push_back(std::move(create));
                    }

                    if (!ReadToken(payload, &cursor, &token))
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
                        if (!ReadToken(payload, &cursor, &token))
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
                        if (!ReadToken(payload, &cursor, &token))
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
                            if (!ReadToken(payload, &cursor, &assignment.fieldName) ||
                                !ReadToken(payload, &cursor, &valueToken))
                            {
                                return false;
                            }
                            if (!DeserializeImportValue(valueToken, &assignment.SCValue))
                            {
                                return false;
                            }
                            update.values.push_back(std::move(assignment));
                        }
                        request.updates.push_back(std::move(update));
                    }

                    if (!ReadToken(payload, &cursor, &token))
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
                        if (!ReadToken(payload, &cursor, &token))
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

        ErrorCode MapSqliteError(int code)
        {
            switch (code)
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

        class SqliteStmt
        {
        public:
            SqliteStmt() = default;

            SqliteStmt(sqlite3* db, const char* sql)
            {
                const int rc = sqlite3_prepare_v2(db, sql, -1, &stmt_, nullptr);
                if (rc != SQLITE_OK)
                {
                    throw std::runtime_error(sqlite3_errmsg(db));
                }
            }

            ~SqliteStmt()
            {
                if (stmt_ != nullptr)
                {
                    sqlite3_finalize(stmt_);
                }
            }

            SqliteStmt(const SqliteStmt&) = delete;
            SqliteStmt& operator=(const SqliteStmt&) = delete;

            SqliteStmt(SqliteStmt&& other) noexcept : stmt_(other.stmt_)
            {
                other.stmt_ = nullptr;
            }

            SqliteStmt& operator=(SqliteStmt&& other) noexcept
            {
                if (this != &other)
                {
                    if (stmt_ != nullptr)
                    {
                        sqlite3_finalize(stmt_);
                    }
                    stmt_ = other.stmt_;
                    other.stmt_ = nullptr;
                }
                return *this;
            }

            ErrorCode BindInt(int index, int SCValue)
            {
                return MapSqliteError(sqlite3_bind_int(stmt_, index, SCValue));
            }
            ErrorCode BindInt64(int index, std::int64_t SCValue)
            {
                return MapSqliteError(sqlite3_bind_int64(stmt_, index, SCValue));
            }
            ErrorCode BindDouble(int index, double SCValue)
            {
                return MapSqliteError(sqlite3_bind_double(stmt_, index, SCValue));
            }
            ErrorCode BindNull(int index)
            {
                return MapSqliteError(sqlite3_bind_null(stmt_, index));
            }

            ErrorCode BindText(int index, const std::wstring& SCValue)
            {
                const std::string utf8 = ToUtf8(SCValue);
                return MapSqliteError(
                    sqlite3_bind_text(stmt_, index, utf8.c_str(), static_cast<int>(utf8.size()), SQLITE_TRANSIENT));
            }

            ErrorCode BindBlob(int index, const std::vector<std::uint8_t>& SCValue)
            {
                return MapSqliteError(sqlite3_bind_blob(stmt_,
                                                        index,
                                                        SCValue.empty() ? nullptr : SCValue.data(),
                                                        static_cast<int>(SCValue.size()),
                                                        SQLITE_TRANSIENT));
            }

            ErrorCode Step(bool* outHasRow = nullptr)
            {
                const int rc = sqlite3_step(stmt_);
                if (outHasRow != nullptr)
                {
                    *outHasRow = (rc == SQLITE_ROW);
                }
                return MapSqliteError(rc);
            }

            ErrorCode Reset()
            {
                const int rc = sqlite3_reset(stmt_);
                sqlite3_clear_bindings(stmt_);
                return MapSqliteError(rc);
            }

            int ColumnInt(int index) const
            {
                return sqlite3_column_int(stmt_, index);
            }
            std::int64_t ColumnInt64(int index) const
            {
                return sqlite3_column_int64(stmt_, index);
            }
            double ColumnDouble(int index) const
            {
                return sqlite3_column_double(stmt_, index);
            }
            bool ColumnBool(int index) const
            {
                return sqlite3_column_int(stmt_, index) != 0;
            }
            bool ColumnIsNull(int index) const
            {
                return sqlite3_column_type(stmt_, index) == SQLITE_NULL;
            }
            std::wstring ColumnText(int index) const
            {
                return FromUtf8(reinterpret_cast<const char*>(sqlite3_column_text(stmt_, index)));
            }
            std::vector<std::uint8_t> ColumnBlob(int index) const
            {
                const auto* data = static_cast<const std::uint8_t*>(sqlite3_column_blob(stmt_, index));
                const int size = sqlite3_column_bytes(stmt_, index);
                if (data == nullptr || size <= 0)
                {
                    return {};
                }
                return std::vector<std::uint8_t>(data, data + static_cast<std::size_t>(size));
            }

        private:
            sqlite3_stmt* stmt_{nullptr};
        };

        class SqliteDb
        {
        public:
            explicit SqliteDb(const std::wstring& path, bool readOnly)
            {
                const std::string utf8 = ToUtf8(path);
                const int rc =
                    sqlite3_open_v2(utf8.c_str(),
                                    &db_,
                                    (readOnly ? SQLITE_OPEN_READONLY : (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE)) |
                                        SQLITE_OPEN_FULLMUTEX,
                                    nullptr);
                if (rc != SQLITE_OK)
                {
                    const std::string message = (db_ != nullptr) ? sqlite3_errmsg(db_) : "sqlite open failed";
                    if (db_ != nullptr)
                    {
                        sqlite3_close(db_);
                        db_ = nullptr;
                    }
                    throw std::runtime_error(message);
                }

                sqlite3_exec(db_, "PRAGMA foreign_keys = ON;", nullptr, nullptr, nullptr);
            }

            ~SqliteDb()
            {
                if (db_ != nullptr)
                {
                    sqlite3_close(db_);
                }
            }

            sqlite3* Raw() const noexcept
            {
                return db_;
            }

            ErrorCode Execute(const char* sql)
            {
                char* error = nullptr;
                const int rc = sqlite3_exec(db_, sql, nullptr, nullptr, &error);
                if (error != nullptr)
                {
                    sqlite3_free(error);
                }
                return MapSqliteError(rc);
            }

            SqliteStmt Prepare(const char* sql)
            {
                return SqliteStmt(db_, sql);
            }

            std::int64_t LastInsertRowId() const noexcept
            {
                return sqlite3_last_insert_rowid(db_);
            }

        private:
            sqlite3* db_{nullptr};
        };

        class SqliteTxn
        {
        public:
            explicit SqliteTxn(SqliteDb& db) : db_(db)
            {
                if (Failed(db_.Execute("BEGIN IMMEDIATE TRANSACTION;")))
                {
                    throw std::runtime_error("failed to begin sqlite transaction");
                }
            }

            ~SqliteTxn()
            {
                if (!completed_)
                {
                    db_.Execute("ROLLBACK;");
                }
            }

            ErrorCode Commit()
            {
                if (completed_)
                {
                    return SC_OK;
                }
                completed_ = true;
                return db_.Execute("COMMIT;");
            }

        private:
            SqliteDb& db_;
            bool completed_{false};
        };

        class SqliteSavepoint
        {
        public:
            explicit SqliteSavepoint(SqliteDb& db, const char* baseName)
                : db_(db)
            {
                const std::uint64_t savepointId =
                    nextId_.fetch_add(1, std::memory_order_relaxed);
                name_ = std::string(baseName) + "_" + std::to_string(savepointId);
                if (Failed(db_.Execute(("SAVEPOINT " + name_ + ";").c_str())))
                {
                    throw std::runtime_error("failed to create sqlite savepoint");
                }
            }

            ~SqliteSavepoint()
            {
                if (!completed_)
                {
                    db_.Execute(("ROLLBACK TO SAVEPOINT " + name_ + ";").c_str());
                    db_.Execute(("RELEASE SAVEPOINT " + name_ + ";").c_str());
                }
            }

            ErrorCode Commit()
            {
                if (completed_)
                {
                    return SC_OK;
                }
                completed_ = true;
                return db_.Execute(("RELEASE SAVEPOINT " + name_ + ";").c_str());
            }

        private:
            inline static std::atomic<std::uint64_t> nextId_{1};

            SqliteDb& db_;
            std::string name_;
            bool completed_{false};
        };

        class SqliteSchema;

        struct SqliteRecordData
        {
            explicit SqliteRecordData(RecordId newId) : id(newId)
            {
            }

            RecordId id{0};
            RecordState state{RecordState::Alive};
            VersionId lastModifiedVersion{0};
            std::unordered_map<std::wstring, SCValue> values;
        };

        struct SqlitePersistedJournalTransaction
        {
            std::int64_t txId{0};
            JournalTransaction tx;
        };

        struct SqlitePersistedJournalEntry
        {
            int sequenceIndex{0};
            JournalEntry entry;
        };

        struct DeferredRenameColumnSnapshot
        {
            SqliteSchema* schema{nullptr};
            SCColumnDef column;
        };

        struct DeferredRenameConstraintSnapshot
        {
            SqliteSchema* schema{nullptr};
            SCConstraintDef constraint;
        };

        struct DeferredRenameState
        {
            SCTablePtr tableRef;
            std::wstring oldName;
            std::wstring newName;
            std::vector<DeferredRenameColumnSnapshot> relationColumns;
            std::vector<DeferredRenameConstraintSnapshot> foreignKeyConstraints;
        };

        struct DeferredSchemaOp
        {
            enum class Kind
            {
                RenameTable,
            };

            Kind kind{Kind::RenameTable};
            DeferredRenameState rename;
        };

        struct CompositeIndexEncodedKey
        {
            std::vector<std::uint8_t> prefix1;
            std::vector<std::uint8_t> prefix2;
            std::vector<std::uint8_t> prefix3;
            std::vector<std::uint8_t> full;
        };

        struct CompositeIndexLookupBounds
        {
            std::uint32_t equalityPrefixLength{0};
            std::vector<std::uint8_t> equalityPrefixKey;
            std::uint32_t exactPrefixLength{0};
            std::vector<std::uint8_t> exactPrefixKey;
            bool exactMatch{false};
            bool hasLowerBound{false};
            bool includeLowerBound{true};
            std::vector<std::uint8_t> lowerBound;
            bool hasUpperBound{false};
            bool includeUpperBound{true};
            std::vector<std::uint8_t> upperBound;
        };

        class SqliteDatabase;
        class SqliteTable;

        constexpr std::size_t kCompositeIndexMaxColumns = 3;

        bool IsCompositeIndexExplicit(const SCIndexDef& def)
        {
            return def.sourceKind == SCSchemaSourceKind::Explicit && !def.columns.empty() &&
                   def.columns.size() <= kCompositeIndexMaxColumns;
        }

        bool IsEqualityIndexOperator(QueryConditionOperator op)
        {
            return op == QueryConditionOperator::Equal;
        }

        bool IsRangeIndexOperator(QueryConditionOperator op)
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
                case QueryConditionOperator::Equal:
                case QueryConditionOperator::NotEqual:
                case QueryConditionOperator::In:
                case QueryConditionOperator::IsNull:
                case QueryConditionOperator::IsNotNull:
                case QueryConditionOperator::Contains:
                case QueryConditionOperator::EndsWith:
                default:
                    return false;
            }
        }

        std::wstring BuildQueryIndexStorageKey(std::int64_t tableRowId, const std::wstring& indexName)
        {
            return std::to_wstring(tableRowId) + L"|" + indexName;
        }

        ErrorCode ValidateValueKind(ValueKind expected, const SCValue& value, bool nullable)
        {
            if (value.IsNull())
            {
                return nullable ? SC_OK : SC_E_SCHEMA_VIOLATION;
            }
            return value.GetKind() == expected ? SC_OK : SC_E_TYPE_MISMATCH;
        }

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
                } else
                {
                    if (!def.defaultValue.IsNull() && def.defaultValue.GetKind() != def.valueKind)
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
                    if (def.onDelete != SCForeignKeyAction::Restrict || def.onUpdate != SCForeignKeyAction::Restrict)
                    {
                        return SC_E_SCHEMA_VIOLATION;
                    }
                    break;
                default:
                    return SC_E_SCHEMA_VIOLATION;
            }

            return SC_OK;
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

        int ToSqliteValueKind(ValueKind kind) noexcept
        {
            return static_cast<int>(kind);
        }
        ValueKind FromSqliteValueKind(int kind) noexcept
        {
            return static_cast<ValueKind>(kind);
        }
        int ToSqliteColumnKind(ColumnKind kind) noexcept
        {
            return static_cast<int>(kind);
        }
        ColumnKind FromSqliteColumnKind(int kind) noexcept
        {
            return static_cast<ColumnKind>(kind);
        }
        int ToSqliteRecordState(RecordState state) noexcept
        {
            return static_cast<int>(state);
        }
        RecordState FromSqliteRecordState(int state) noexcept
        {
            return static_cast<RecordState>(state);
        }
        int ToSqliteSchemaSourceKind(SCSchemaSourceKind kind) noexcept
        {
            return static_cast<int>(kind);
        }
        SCSchemaSourceKind FromSqliteSchemaSourceKind(int kind) noexcept
        {
            return static_cast<SCSchemaSourceKind>(kind);
        }
        int ToSqliteConstraintKind(SCConstraintKind kind) noexcept
        {
            return static_cast<int>(kind);
        }
        SCConstraintKind FromSqliteConstraintKind(int kind) noexcept
        {
            return static_cast<SCConstraintKind>(kind);
        }
        int ToSqliteJournalOp(JournalOp op) noexcept
        {
            return static_cast<int>(op);
        }
        JournalOp FromSqliteJournalOp(int op) noexcept
        {
            return static_cast<JournalOp>(op);
        }

        bool EqualsIgnoreCase(const std::wstring& left, const std::wstring& right) noexcept
        {
            if (left.size() != right.size())
            {
                return false;
            }

            for (std::size_t index = 0; index < left.size(); ++index)
            {
                if (std::towlower(left[index]) != std::towlower(right[index]))
                {
                    return false;
                }
            }
            return true;
        }

        std::wstring ToUpperCopy(std::wstring text)
        {
            std::transform(text.begin(), text.end(), text.begin(), [](wchar_t ch) {
                return static_cast<wchar_t>(std::towupper(ch));
            });
            return text;
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
                case static_cast<int>(SCForeignKeyAction::Cascade):
                    return SCForeignKeyAction::Cascade;
                case static_cast<int>(SCForeignKeyAction::SetNull):
                    return SCForeignKeyAction::SetNull;
                case static_cast<int>(SCForeignKeyAction::SetDefault):
                    return SCForeignKeyAction::SetDefault;
                case static_cast<int>(SCForeignKeyAction::Restrict):
                default:
                    return SCForeignKeyAction::Restrict;
            }
        }

        bool IsForeignKeyActionValid(SCForeignKeyAction action) noexcept
        {
            switch (action)
            {
                case SCForeignKeyAction::Restrict:
                case SCForeignKeyAction::NoAction:
                case SCForeignKeyAction::Cascade:
                case SCForeignKeyAction::SetNull:
                case SCForeignKeyAction::SetDefault:
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

        bool ForeignKeyConstraintReferencesColumn(const SCConstraintDef& constraint, const std::wstring& columnName)
        {
            for (std::size_t index = 0; index < constraint.columns.size(); ++index)
            {
                if (EqualsIgnoreCase(ResolveForeignKeyReferencedColumn(constraint, index), columnName))
                {
                    return true;
                }
            }
            return false;
        }

        struct ForeignKeyReferenceEntry
        {
            std::wstring sourceTableName;
            SCConstraintDef constraint;
        };

        std::wstring MakeForeignKeyReferenceCacheKey(const std::wstring& tableName, const std::wstring& columnName)
        {
            return ToUpperCopy(tableName) + L"|" + ToUpperCopy(columnName);
        }

        struct ConstraintExpressionNode
        {
            enum class Kind
            {
                Literal,
                Column,
                Unary,
                Binary,
            };

            enum class UnaryOp
            {
                Negate,
                Not,
            };

            enum class BinaryOp
            {
                Add,
                Subtract,
                Multiply,
                Divide,
                Equal,
                NotEqual,
                Less,
                LessEqual,
                Greater,
                GreaterEqual,
                And,
                Or,
            };

            Kind kind{Kind::Literal};
            SCValue literal;
            std::wstring identifier;
            UnaryOp unaryOp{UnaryOp::Negate};
            BinaryOp binaryOp{BinaryOp::Add};
            std::unique_ptr<ConstraintExpressionNode> left;
            std::unique_ptr<ConstraintExpressionNode> right;
        };

        struct ConstraintExpressionAst
        {
            std::unique_ptr<ConstraintExpressionNode> root;
            std::vector<std::wstring> identifiers;
        };

        class ConstraintExpressionParser
        {
        public:
            ConstraintExpressionParser(std::wstring source, std::set<std::wstring> allowedColumns)
                : source_(std::move(source)), allowedColumns_(std::move(allowedColumns))
            {
                token_ = NextToken();
            }

            ErrorCode Parse(ConstraintExpressionAst* outAst)
            {
                if (outAst == nullptr)
                {
                    return SC_E_POINTER;
                }

                auto root = ParseOr();
                if (Failed(lastError_))
                {
                    return lastError_;
                }
                if (token_.kind != TokenKind::End)
                {
                    return SC_E_VALIDATION_FAILED;
                }

                outAst->root = std::move(root);
                outAst->identifiers = std::move(identifiers_);
                return SC_OK;
            }

        private:
            enum class TokenKind
            {
                End,
                Identifier,
                Number,
                String,
                LParen,
                RParen,
                Comma,
                Plus,
                Minus,
                Star,
                Slash,
                Equal,
                NotEqual,
                Less,
                LessEqual,
                Greater,
                GreaterEqual,
                And,
                Or,
                Not,
                Null,
                True,
                False,
            };

            struct Token
            {
                TokenKind kind{TokenKind::End};
                std::wstring text;
            };

            Token NextToken()
            {
                SkipWhitespace();
                if (index_ >= source_.size())
                {
                    return {};
                }

                const wchar_t ch = source_[index_];
                if (std::iswdigit(ch) != 0 || ch == L'.')
                {
                    return ReadNumber();
                }
                if (std::iswalpha(ch) != 0 || ch == L'_')
                {
                    return ReadIdentifier();
                }
                if (ch == L'\'')
                {
                    return ReadString();
                }

                ++index_;
                switch (ch)
                {
                    case L'(':
                        return {TokenKind::LParen, L"("};
                    case L')':
                        return {TokenKind::RParen, L")"};
                    case L',':
                        return {TokenKind::Comma, L","};
                    case L'+':
                        return {TokenKind::Plus, L"+"};
                    case L'-':
                        return {TokenKind::Minus, L"-"};
                    case L'*':
                        return {TokenKind::Star, L"*"};
                    case L'/':
                        return {TokenKind::Slash, L"/"};
                    case L'=':
                        return {TokenKind::Equal, L"="};
                    case L'!':
                        if (index_ < source_.size() && source_[index_] == L'=')
                        {
                            ++index_;
                            return {TokenKind::NotEqual, L"!="};
                        }
                        return {TokenKind::Not, L"!"};
                    case L'<':
                        if (index_ < source_.size() && source_[index_] == L'>')
                        {
                            ++index_;
                            return {TokenKind::NotEqual, L"<>",};
                        }
                        if (index_ < source_.size() && source_[index_] == L'=')
                        {
                            ++index_;
                            return {TokenKind::LessEqual, L"<="};
                        }
                        return {TokenKind::Less, L"<"};
                    case L'>':
                        if (index_ < source_.size() && source_[index_] == L'=')
                        {
                            ++index_;
                            return {TokenKind::GreaterEqual, L">="};
                        }
                        return {TokenKind::Greater, L">"};
                    default:
                        return {};
                }
            }

            void SkipWhitespace()
            {
                while (index_ < source_.size() && std::iswspace(source_[index_]) != 0)
                {
                    ++index_;
                }
            }

            Token ReadNumber()
            {
                const std::size_t start = index_;
                bool sawDot = false;
                while (index_ < source_.size())
                {
                    const wchar_t current = source_[index_];
                    if (std::iswdigit(current) != 0)
                    {
                        ++index_;
                        continue;
                    }
                    if (current == L'.' && !sawDot)
                    {
                        sawDot = true;
                        ++index_;
                        continue;
                    }
                    break;
                }
                return {TokenKind::Number, source_.substr(start, index_ - start)};
            }

            Token ReadIdentifier()
            {
                const std::size_t start = index_;
                while (index_ < source_.size())
                {
                    const wchar_t current = source_[index_];
                    if (std::iswalnum(current) != 0 || current == L'_' || current == L'.')
                    {
                        ++index_;
                        continue;
                    }
                    break;
                }

                const std::wstring text = source_.substr(start, index_ - start);
                const std::wstring upper = ToUpperCopy(text);
                if (upper == L"AND")
                {
                    return {TokenKind::And, text};
                }
                if (upper == L"OR")
                {
                    return {TokenKind::Or, text};
                }
                if (upper == L"NOT")
                {
                    return {TokenKind::Not, text};
                }
                if (upper == L"NULL")
                {
                    return {TokenKind::Null, text};
                }
                if (upper == L"TRUE")
                {
                    return {TokenKind::True, text};
                }
                if (upper == L"FALSE")
                {
                    return {TokenKind::False, text};
                }
                return {TokenKind::Identifier, text};
            }

            Token ReadString()
            {
                ++index_;
                std::wstring text;
                while (index_ < source_.size())
                {
                    const wchar_t current = source_[index_++];
                    if (current == L'\'')
                    {
                        if (index_ < source_.size() && source_[index_] == L'\'')
                        {
                            text.push_back(L'\'');
                            ++index_;
                            continue;
                        }
                        return {TokenKind::String, text};
                    }
                    text.push_back(current);
                }
                lastError_ = SC_E_VALIDATION_FAILED;
                return {};
            }

            std::unique_ptr<ConstraintExpressionNode> ParseOr()
            {
                auto node = ParseAnd();
                if (Failed(lastError_))
                {
                    return nullptr;
                }
                while (token_.kind == TokenKind::Or)
                {
                    Advance();
                    auto right = ParseAnd();
                    if (Failed(lastError_))
                    {
                        return nullptr;
                    }
                    node = MakeBinaryNode(ConstraintExpressionNode::BinaryOp::Or, std::move(node), std::move(right));
                }
                return node;
            }

            std::unique_ptr<ConstraintExpressionNode> ParseAnd()
            {
                auto node = ParseEquality();
                if (Failed(lastError_))
                {
                    return nullptr;
                }
                while (token_.kind == TokenKind::And)
                {
                    Advance();
                    auto right = ParseEquality();
                    if (Failed(lastError_))
                    {
                        return nullptr;
                    }
                    node = MakeBinaryNode(ConstraintExpressionNode::BinaryOp::And, std::move(node), std::move(right));
                }
                return node;
            }

            std::unique_ptr<ConstraintExpressionNode> ParseEquality()
            {
                auto node = ParseComparison();
                if (Failed(lastError_))
                {
                    return nullptr;
                }
                while (token_.kind == TokenKind::Equal || token_.kind == TokenKind::NotEqual)
                {
                    const TokenKind op = token_.kind;
                    Advance();
                    auto right = ParseComparison();
                    if (Failed(lastError_))
                    {
                        return nullptr;
                    }
                    node = MakeBinaryNode(op == TokenKind::Equal ? ConstraintExpressionNode::BinaryOp::Equal
                                                                 : ConstraintExpressionNode::BinaryOp::NotEqual,
                                          std::move(node),
                                          std::move(right));
                }
                return node;
            }

            std::unique_ptr<ConstraintExpressionNode> ParseComparison()
            {
                auto node = ParseTerm();
                if (Failed(lastError_))
                {
                    return nullptr;
                }
                while (token_.kind == TokenKind::Less || token_.kind == TokenKind::LessEqual ||
                       token_.kind == TokenKind::Greater || token_.kind == TokenKind::GreaterEqual)
                {
                    const TokenKind op = token_.kind;
                    Advance();
                    auto right = ParseTerm();
                    if (Failed(lastError_))
                    {
                        return nullptr;
                    }
                    ConstraintExpressionNode::BinaryOp binaryOp = ConstraintExpressionNode::BinaryOp::Less;
                    switch (op)
                    {
                        case TokenKind::Less:
                            binaryOp = ConstraintExpressionNode::BinaryOp::Less;
                            break;
                        case TokenKind::LessEqual:
                            binaryOp = ConstraintExpressionNode::BinaryOp::LessEqual;
                            break;
                        case TokenKind::Greater:
                            binaryOp = ConstraintExpressionNode::BinaryOp::Greater;
                            break;
                        case TokenKind::GreaterEqual:
                            binaryOp = ConstraintExpressionNode::BinaryOp::GreaterEqual;
                            break;
                        default:
                            break;
                    }
                    node = MakeBinaryNode(binaryOp, std::move(node), std::move(right));
                }
                return node;
            }

            std::unique_ptr<ConstraintExpressionNode> ParseTerm()
            {
                auto node = ParseFactor();
                if (Failed(lastError_))
                {
                    return nullptr;
                }
                while (token_.kind == TokenKind::Plus || token_.kind == TokenKind::Minus)
                {
                    const TokenKind op = token_.kind;
                    Advance();
                    auto right = ParseFactor();
                    if (Failed(lastError_))
                    {
                        return nullptr;
                    }
                    node = MakeBinaryNode(op == TokenKind::Plus ? ConstraintExpressionNode::BinaryOp::Add
                                                                 : ConstraintExpressionNode::BinaryOp::Subtract,
                                          std::move(node),
                                          std::move(right));
                }
                return node;
            }

            std::unique_ptr<ConstraintExpressionNode> ParseFactor()
            {
                auto node = ParseUnary();
                if (Failed(lastError_))
                {
                    return nullptr;
                }
                while (token_.kind == TokenKind::Star || token_.kind == TokenKind::Slash)
                {
                    const TokenKind op = token_.kind;
                    Advance();
                    auto right = ParseUnary();
                    if (Failed(lastError_))
                    {
                        return nullptr;
                    }
                    node = MakeBinaryNode(op == TokenKind::Star ? ConstraintExpressionNode::BinaryOp::Multiply
                                                                 : ConstraintExpressionNode::BinaryOp::Divide,
                                          std::move(node),
                                          std::move(right));
                }
                return node;
            }

            std::unique_ptr<ConstraintExpressionNode> ParseUnary()
            {
                if (token_.kind == TokenKind::Minus)
                {
                    Advance();
                    auto node = ParseUnary();
                    if (Failed(lastError_))
                    {
                        return nullptr;
                    }
                    return MakeUnaryNode(ConstraintExpressionNode::UnaryOp::Negate, std::move(node));
                }
                if (token_.kind == TokenKind::Not)
                {
                    Advance();
                    auto node = ParseUnary();
                    if (Failed(lastError_))
                    {
                        return nullptr;
                    }
                    return MakeUnaryNode(ConstraintExpressionNode::UnaryOp::Not, std::move(node));
                }
                return ParsePrimary();
            }

            std::unique_ptr<ConstraintExpressionNode> ParsePrimary()
            {
                switch (token_.kind)
                {
                    case TokenKind::Number: {
                        const std::wstring number = token_.text;
                        Advance();
                        auto node = std::make_unique<ConstraintExpressionNode>();
                        node->kind = ConstraintExpressionNode::Kind::Literal;
                        try
                        {
                            node->literal = number.find(L'.') == std::wstring::npos
                                                ? SCValue::FromInt64(std::stoll(number))
                                                : SCValue::FromDouble(std::stod(number));
                        } catch (...)
                        {
                            lastError_ = SC_E_VALIDATION_FAILED;
                            return {};
                        }
                        return node;
                    }
                    case TokenKind::String: {
                        auto node = std::make_unique<ConstraintExpressionNode>();
                        node->kind = ConstraintExpressionNode::Kind::Literal;
                        node->literal = SCValue::FromString(token_.text);
                        Advance();
                        return node;
                    }
                    case TokenKind::True: {
                        auto node = std::make_unique<ConstraintExpressionNode>();
                        node->kind = ConstraintExpressionNode::Kind::Literal;
                        node->literal = SCValue::FromBool(true);
                        Advance();
                        return node;
                    }
                    case TokenKind::False: {
                        auto node = std::make_unique<ConstraintExpressionNode>();
                        node->kind = ConstraintExpressionNode::Kind::Literal;
                        node->literal = SCValue::FromBool(false);
                        Advance();
                        return node;
                    }
                    case TokenKind::Null: {
                        auto node = std::make_unique<ConstraintExpressionNode>();
                        node->kind = ConstraintExpressionNode::Kind::Literal;
                        node->literal = SCValue::Null();
                        Advance();
                        return node;
                    }
                    case TokenKind::Identifier: {
                        const std::wstring identifier = token_.text;
                        if (!allowedColumns_.empty() &&
                            std::none_of(allowedColumns_.begin(),
                                         allowedColumns_.end(),
                                         [&identifier](const std::wstring& candidate) {
                                             return EqualsIgnoreCase(candidate, identifier);
                                         }))
                        {
                            lastError_ = SC_E_COLUMN_NOT_FOUND;
                            return {};
                        }
                        identifiers_.push_back(identifier);
                        auto node = std::make_unique<ConstraintExpressionNode>();
                        node->kind = ConstraintExpressionNode::Kind::Column;
                        node->identifier = identifier;
                        Advance();
                        return node;
                    }
                    case TokenKind::LParen: {
                        Advance();
                        auto node = ParseOr();
                        if (Failed(lastError_))
                        {
                            return nullptr;
                        }
                        if (token_.kind != TokenKind::RParen)
                        {
                            lastError_ = SC_E_VALIDATION_FAILED;
                            return {};
                        }
                        Advance();
                        return node;
                    }
                    default:
                        lastError_ = SC_E_VALIDATION_FAILED;
                        return {};
                }
            }

            static std::unique_ptr<ConstraintExpressionNode> MakeUnaryNode(
                ConstraintExpressionNode::UnaryOp op,
                std::unique_ptr<ConstraintExpressionNode> child)
            {
                auto node = std::make_unique<ConstraintExpressionNode>();
                node->kind = ConstraintExpressionNode::Kind::Unary;
                node->unaryOp = op;
                node->left = std::move(child);
                return node;
            }

            static std::unique_ptr<ConstraintExpressionNode> MakeBinaryNode(
                ConstraintExpressionNode::BinaryOp op,
                std::unique_ptr<ConstraintExpressionNode> left,
                std::unique_ptr<ConstraintExpressionNode> right)
            {
                auto node = std::make_unique<ConstraintExpressionNode>();
                node->kind = ConstraintExpressionNode::Kind::Binary;
                node->binaryOp = op;
                node->left = std::move(left);
                node->right = std::move(right);
                return node;
            }

            void Advance()
            {
                token_ = NextToken();
            }

            std::wstring source_;
            std::set<std::wstring> allowedColumns_;
            std::size_t index_{0};
            Token token_;
            ErrorCode lastError_{SC_OK};
            std::vector<std::wstring> identifiers_;
        };

        bool IsNumericValue(const SCValue& value) noexcept
        {
            switch (value.GetKind())
            {
                case ValueKind::Int64:
                case ValueKind::Double:
                case ValueKind::Bool:
                case ValueKind::RecordId:
                    return true;
                default:
                    return false;
            }
        }

        bool TryAsDouble(const SCValue& value, double* outValue) noexcept
        {
            if (outValue == nullptr)
            {
                return false;
            }

            switch (value.GetKind())
            {
                case ValueKind::Int64: {
                    std::int64_t typed = 0;
                    if (Failed(value.AsInt64(&typed)))
                    {
                        return false;
                    }
                    *outValue = static_cast<double>(typed);
                    return true;
                }
                case ValueKind::Double:
                    return Succeeded(value.AsDouble(outValue));
                case ValueKind::Bool: {
                    bool flag = false;
                    if (Failed(value.AsBool(&flag)))
                    {
                        return false;
                    }
                    *outValue = flag ? 1.0 : 0.0;
                    return true;
                }
                case ValueKind::RecordId: {
                    RecordId id = 0;
                    if (Failed(value.AsRecordId(&id)))
                    {
                        return false;
                    }
                    *outValue = static_cast<double>(id);
                    return true;
                }
                default:
                    return false;
            }
        }

        bool TryAsText(const SCValue& value, std::wstring* outValue) noexcept
        {
            if (outValue == nullptr)
            {
                return false;
            }

            switch (value.GetKind())
            {
                case ValueKind::String:
                case ValueKind::Enum:
                    return Succeeded(value.AsStringCopy(outValue)) || Succeeded(value.AsEnumCopy(outValue));
                default:
                    return false;
            }
        }

        bool IsTruthyValue(const SCValue& value) noexcept
        {
            if (value.IsNull())
            {
                return false;
            }

            switch (value.GetKind())
            {
                case ValueKind::Bool: {
                    bool flag = false;
                    return Succeeded(value.AsBool(&flag)) && flag;
                }
                case ValueKind::Int64: {
                    std::int64_t typed = 0;
                    return Succeeded(value.AsInt64(&typed)) && typed != 0;
                }
                case ValueKind::Double: {
                    double typed = 0.0;
                    return Succeeded(value.AsDouble(&typed)) && typed != 0.0;
                }
                case ValueKind::RecordId: {
                    RecordId typed = 0;
                    return Succeeded(value.AsRecordId(&typed)) && typed != 0;
                }
                case ValueKind::String:
                case ValueKind::Enum: {
                    std::wstring text;
                    return Succeeded(value.AsStringCopy(&text)) && !text.empty();
                }
                default:
                    return false;
            }
        }

        ErrorCode EvaluateConstraintExpressionNode(const ConstraintExpressionNode& node,
                                                   const std::unordered_map<std::wstring, SCValue>& values,
                                                   SCValue* outValue)
        {
            if (outValue == nullptr)
            {
                return SC_E_POINTER;
            }

            switch (node.kind)
            {
                case ConstraintExpressionNode::Kind::Literal:
                    *outValue = node.literal;
                    return SC_OK;
                case ConstraintExpressionNode::Kind::Column: {
                    const auto it = std::find_if(values.begin(),
                                                 values.end(),
                                                 [&node](const auto& entry) {
                                                     return EqualsIgnoreCase(entry.first, node.identifier);
                                                 });
                    if (it == values.end())
                    {
                        return SC_E_COLUMN_NOT_FOUND;
                    }
                    *outValue = it->second;
                    return SC_OK;
                }
                case ConstraintExpressionNode::Kind::Unary: {
                    SCValue child;
                    const ErrorCode rc = EvaluateConstraintExpressionNode(*node.left, values, &child);
                    if (Failed(rc))
                    {
                        return rc;
                    }
                    if (node.unaryOp == ConstraintExpressionNode::UnaryOp::Not)
                    {
                        *outValue = SCValue::FromBool(!IsTruthyValue(child));
                        return SC_OK;
                    }
                    double numeric = 0.0;
                    if (!TryAsDouble(child, &numeric))
                    {
                        return SC_E_TYPE_MISMATCH;
                    }
                    *outValue = SCValue::FromDouble(-numeric);
                    return SC_OK;
                }
                case ConstraintExpressionNode::Kind::Binary: {
                    SCValue left;
                    SCValue right;
                    const ErrorCode leftRc = EvaluateConstraintExpressionNode(*node.left, values, &left);
                    if (Failed(leftRc))
                    {
                        return leftRc;
                    }
                    const ErrorCode rightRc = EvaluateConstraintExpressionNode(*node.right, values, &right);
                    if (Failed(rightRc))
                    {
                        return rightRc;
                    }

                    switch (node.binaryOp)
                    {
                        case ConstraintExpressionNode::BinaryOp::Add:
                        case ConstraintExpressionNode::BinaryOp::Subtract:
                        case ConstraintExpressionNode::BinaryOp::Multiply:
                        case ConstraintExpressionNode::BinaryOp::Divide: {
                            double leftNumber = 0.0;
                            double rightNumber = 0.0;
                            if (!TryAsDouble(left, &leftNumber) || !TryAsDouble(right, &rightNumber))
                            {
                                return SC_E_TYPE_MISMATCH;
                            }
                            if (node.binaryOp == ConstraintExpressionNode::BinaryOp::Divide && rightNumber == 0.0)
                            {
                                return SC_E_INVALIDARG;
                            }
                            switch (node.binaryOp)
                            {
                                case ConstraintExpressionNode::BinaryOp::Add:
                                    *outValue = SCValue::FromDouble(leftNumber + rightNumber);
                                    break;
                                case ConstraintExpressionNode::BinaryOp::Subtract:
                                    *outValue = SCValue::FromDouble(leftNumber - rightNumber);
                                    break;
                                case ConstraintExpressionNode::BinaryOp::Multiply:
                                    *outValue = SCValue::FromDouble(leftNumber * rightNumber);
                                    break;
                                case ConstraintExpressionNode::BinaryOp::Divide:
                                    *outValue = SCValue::FromDouble(leftNumber / rightNumber);
                                    break;
                                default:
                                    break;
                            }
                            return SC_OK;
                        }
                        case ConstraintExpressionNode::BinaryOp::Equal:
                            if (IsNumericValue(left) && IsNumericValue(right))
                            {
                                double leftNumber = 0.0;
                                double rightNumber = 0.0;
                                if (!TryAsDouble(left, &leftNumber) || !TryAsDouble(right, &rightNumber))
                                {
                                    return SC_E_TYPE_MISMATCH;
                                }
                                *outValue = SCValue::FromBool(leftNumber == rightNumber);
                            } else
                            {
                                *outValue = SCValue::FromBool(left == right);
                            }
                            return SC_OK;
                        case ConstraintExpressionNode::BinaryOp::NotEqual:
                            if (IsNumericValue(left) && IsNumericValue(right))
                            {
                                double leftNumber = 0.0;
                                double rightNumber = 0.0;
                                if (!TryAsDouble(left, &leftNumber) || !TryAsDouble(right, &rightNumber))
                                {
                                    return SC_E_TYPE_MISMATCH;
                                }
                                *outValue = SCValue::FromBool(leftNumber != rightNumber);
                            } else
                            {
                                *outValue = SCValue::FromBool(left != right);
                            }
                            return SC_OK;
                        case ConstraintExpressionNode::BinaryOp::Less:
                        case ConstraintExpressionNode::BinaryOp::LessEqual:
                        case ConstraintExpressionNode::BinaryOp::Greater:
                        case ConstraintExpressionNode::BinaryOp::GreaterEqual: {
                            if (IsNumericValue(left) && IsNumericValue(right))
                            {
                                double leftNumber = 0.0;
                                double rightNumber = 0.0;
                                if (!TryAsDouble(left, &leftNumber) || !TryAsDouble(right, &rightNumber))
                                {
                                    return SC_E_TYPE_MISMATCH;
                                }
                                bool result = false;
                                switch (node.binaryOp)
                                {
                                    case ConstraintExpressionNode::BinaryOp::Less:
                                        result = leftNumber < rightNumber;
                                        break;
                                    case ConstraintExpressionNode::BinaryOp::LessEqual:
                                        result = leftNumber <= rightNumber;
                                        break;
                                    case ConstraintExpressionNode::BinaryOp::Greater:
                                        result = leftNumber > rightNumber;
                                        break;
                                    case ConstraintExpressionNode::BinaryOp::GreaterEqual:
                                        result = leftNumber >= rightNumber;
                                        break;
                                    default:
                                        break;
                                }
                                *outValue = SCValue::FromBool(result);
                                return SC_OK;
                            }

                            std::wstring leftText;
                            std::wstring rightText;
                            if (!TryAsText(left, &leftText) || !TryAsText(right, &rightText))
                            {
                                return SC_E_TYPE_MISMATCH;
                            }

                            bool result = false;
                            switch (node.binaryOp)
                            {
                                case ConstraintExpressionNode::BinaryOp::Less:
                                    result = leftText < rightText;
                                    break;
                                case ConstraintExpressionNode::BinaryOp::LessEqual:
                                    result = leftText <= rightText;
                                    break;
                                case ConstraintExpressionNode::BinaryOp::Greater:
                                    result = leftText > rightText;
                                    break;
                                case ConstraintExpressionNode::BinaryOp::GreaterEqual:
                                    result = leftText >= rightText;
                                    break;
                                default:
                                    break;
                            }
                            *outValue = SCValue::FromBool(result);
                            return SC_OK;
                        }
                        case ConstraintExpressionNode::BinaryOp::And:
                            *outValue = SCValue::FromBool(IsTruthyValue(left) && IsTruthyValue(right));
                            return SC_OK;
                        case ConstraintExpressionNode::BinaryOp::Or:
                            *outValue = SCValue::FromBool(IsTruthyValue(left) || IsTruthyValue(right));
                            return SC_OK;
                        default:
                            return SC_E_FAIL;
                    }
                }
            }

            return SC_E_FAIL;
        }

        std::wstring SanitizeIdentifier(const std::wstring& input)
        {
            std::wstring result;
            result.reserve(input.size());
            for (wchar_t ch : input)
            {
                if (::iswalnum(ch) != 0)
                {
                    result.push_back(ch);
                } else
                {
                    result.push_back(L'_');
                }
            }
            return result;
        }

        void BindValueForStorage(SqliteStmt& stmt,
                                 int kindIndex,
                                 int intIndex,
                                 int doubleIndex,
                                 int boolIndex,
                                 int textIndex,
                                 int blobIndex,
                                 const SCValue& value)
        {
            stmt.BindInt(kindIndex, ToSqliteValueKind(value.GetKind()));

            switch (value.GetKind())
            {
                case ValueKind::Null:
                    stmt.BindNull(intIndex);
                    stmt.BindNull(doubleIndex);
                    stmt.BindNull(boolIndex);
                    stmt.BindNull(textIndex);
                    stmt.BindNull(blobIndex);
                    break;
                case ValueKind::Int64: {
                    std::int64_t v = 0;
                    value.AsInt64(&v);
                    stmt.BindInt64(intIndex, v);
                    stmt.BindNull(doubleIndex);
                    stmt.BindNull(boolIndex);
                    stmt.BindNull(textIndex);
                    stmt.BindNull(blobIndex);
                    break;
                }
                case ValueKind::Double: {
                    double v = 0.0;
                    value.AsDouble(&v);
                    stmt.BindNull(intIndex);
                    stmt.BindDouble(doubleIndex, v);
                    stmt.BindNull(boolIndex);
                    stmt.BindNull(textIndex);
                    stmt.BindNull(blobIndex);
                    break;
                }
                case ValueKind::Bool: {
                    bool v = false;
                    value.AsBool(&v);
                    stmt.BindNull(intIndex);
                    stmt.BindNull(doubleIndex);
                    stmt.BindInt(boolIndex, v ? 1 : 0);
                    stmt.BindNull(textIndex);
                    stmt.BindNull(blobIndex);
                    break;
                }
                case ValueKind::String: {
                    std::wstring text;
                    value.AsStringCopy(&text);
                    stmt.BindNull(intIndex);
                    stmt.BindNull(doubleIndex);
                    stmt.BindNull(boolIndex);
                    stmt.BindText(textIndex, text);
                    stmt.BindNull(blobIndex);
                    break;
                }
                case ValueKind::RecordId: {
                    RecordId id = 0;
                    value.AsRecordId(&id);
                    stmt.BindInt64(intIndex, id);
                    stmt.BindNull(doubleIndex);
                    stmt.BindNull(boolIndex);
                    stmt.BindNull(textIndex);
                    stmt.BindNull(blobIndex);
                    break;
                }
                case ValueKind::Enum: {
                    std::wstring text;
                    value.AsEnumCopy(&text);
                    stmt.BindNull(intIndex);
                    stmt.BindNull(doubleIndex);
                    stmt.BindNull(boolIndex);
                    stmt.BindText(textIndex, text);
                    stmt.BindNull(blobIndex);
                    break;
                }
                case ValueKind::Binary: {
                    std::vector<std::uint8_t> bytes;
                    value.AsBinaryCopy(&bytes);
                    stmt.BindNull(intIndex);
                    stmt.BindNull(doubleIndex);
                    stmt.BindNull(boolIndex);
                    stmt.BindNull(textIndex);
                    stmt.BindBlob(blobIndex, bytes);
                    break;
                }
            }
        }

        ErrorCode BindValueToStatement(SqliteStmt& stmt, int index, const SCValue& value)
        {
            switch (value.GetKind())
            {
                case ValueKind::Null:
                    return stmt.BindNull(index);
                case ValueKind::Int64: {
                    std::int64_t v = 0;
                    const ErrorCode rc = value.AsInt64(&v);
                    return Failed(rc) ? rc : stmt.BindInt64(index, v);
                }
                case ValueKind::Double: {
                    double v = 0.0;
                    const ErrorCode rc = value.AsDouble(&v);
                    return Failed(rc) ? rc : stmt.BindDouble(index, v);
                }
                case ValueKind::Bool: {
                    bool v = false;
                    const ErrorCode rc = value.AsBool(&v);
                    return Failed(rc) ? rc : stmt.BindInt(index, v ? 1 : 0);
                }
                case ValueKind::String: {
                    std::wstring v;
                    const ErrorCode rc = value.AsStringCopy(&v);
                    return Failed(rc) ? rc : stmt.BindText(index, v);
                }
                case ValueKind::RecordId: {
                    RecordId v = 0;
                    const ErrorCode rc = value.AsRecordId(&v);
                    return Failed(rc) ? rc : stmt.BindInt64(index, static_cast<std::int64_t>(v));
                }
                case ValueKind::Enum: {
                    std::wstring v;
                    const ErrorCode rc = value.AsEnumCopy(&v);
                    return Failed(rc) ? rc : stmt.BindText(index, v);
                }
                case ValueKind::Binary: {
                    std::vector<std::uint8_t> v;
                    const ErrorCode rc = value.AsBinaryCopy(&v);
                    return Failed(rc) ? rc : stmt.BindBlob(index, v);
                }
                default:
                    return SC_E_TYPE_MISMATCH;
            }
        }

        std::wstring QuoteSqlIdentifier(const std::wstring& identifier)
        {
            std::wstring quoted;
            quoted.push_back(L'"');
            for (wchar_t ch : identifier)
            {
                if (ch == L'"')
                {
                    quoted.push_back(L'"');
                    quoted.push_back(L'"');
                } else
                {
                    quoted.push_back(ch);
                }
            }
            quoted.push_back(L'"');
            return quoted;
        }

        SCValue ReadValueFromStorage(const SqliteStmt& stmt,
                                     int kindIndex,
                                     int intIndex,
                                     int doubleIndex,
                                     int boolIndex,
                                     int textIndex,
                                     int blobIndex)
        {
            switch (FromSqliteValueKind(stmt.ColumnInt(kindIndex)))
            {
                case ValueKind::Null:
                    return SCValue::Null();
                case ValueKind::Int64:
                    return SCValue::FromInt64(stmt.ColumnInt64(intIndex));
                case ValueKind::Double:
                    return SCValue::FromDouble(stmt.ColumnDouble(doubleIndex));
                case ValueKind::Bool:
                    return SCValue::FromBool(stmt.ColumnBool(boolIndex));
                case ValueKind::String:
                    return SCValue::FromString(stmt.ColumnText(textIndex));
                case ValueKind::RecordId:
                    return SCValue::FromRecordId(stmt.ColumnInt64(intIndex));
                case ValueKind::Enum:
                    return SCValue::FromEnum(stmt.ColumnText(textIndex));
                case ValueKind::Binary:
                    return SCValue::FromBinary(stmt.ColumnBlob(blobIndex));
                default:
                    return SCValue::Null();
            }
        }

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
            BindValueForStorage(stmt,
                                defaultKindIndex,
                                defaultInt64Index,
                                defaultDoubleIndex,
                                defaultBoolIndex,
                                defaultTextIndex,
                                defaultBlobIndex,
                                def.defaultValue);
        }

        SCColumnDef ReadColumnDefFromStorage(const SqliteStmt& stmt,
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
                                             int defaultBlobIndex)
        {
            SCColumnDef def;
            def.displayName = stmt.ColumnText(displayNameIndex);
            def.valueKind = FromSqliteValueKind(stmt.ColumnInt(valueKindIndex));
            def.columnKind = FromSqliteColumnKind(stmt.ColumnInt(columnKindIndex));
            def.nullable = stmt.ColumnBool(nullableIndex);
            def.editable = stmt.ColumnBool(editableIndex);
            def.userDefined = stmt.ColumnBool(userDefinedIndex);
            def.indexed = stmt.ColumnBool(indexedIndex);
            def.participatesInCalc = stmt.ColumnBool(participatesIndex);
            def.unit = stmt.ColumnText(unitIndex);
            def.referenceTable = stmt.ColumnText(referenceTableIndex);
            def.referenceStorageColumn = stmt.ColumnText(referenceStorageColumnIndex);
            def.referenceDisplayColumn = stmt.ColumnText(referenceDisplayColumnIndex);
            def.defaultValue = ReadValueFromStorage(stmt,
                                                    defaultKindIndex,
                                                    defaultInt64Index,
                                                    defaultDoubleIndex,
                                                    defaultBoolIndex,
                                                    defaultTextIndex,
                                                    defaultBlobIndex);
            return def;
        }

        class SqliteSchema final : public ISCSchema, public SCRefCountedObject
        {
        public:
            struct SchemaColumnEntry
            {
                std::int64_t rowId{0};
                SCColumnDef def;
            };

            struct SchemaConstraintEntry
            {
                std::int64_t rowId{0};
                SCConstraintDef def;
            };

            struct SchemaIndexEntry
            {
                std::int64_t rowId{0};
                SCIndexDef def;
            };

            SqliteSchema(SqliteDatabase* db, std::wstring tableName, std::int64_t tableRowId)
                : db_(db), tableName_(std::move(tableName)), tableRowId_(tableRowId)
            {
            }

            ErrorCode GetColumnCount(std::int32_t* outCount) override;
            ErrorCode GetColumn(std::int32_t index, SCColumnDef* outDef) override;
            ErrorCode FindColumn(const wchar_t* name, SCColumnDef* outDef) override;
            ErrorCode GetSchemaSnapshot(SCTableSchemaSnapshot* outSnapshot) override;
            ErrorCode GetConstraintCount(std::int32_t* outCount) override;
            ErrorCode GetConstraint(std::int32_t index, SCConstraintDef* outDef) override;
            ErrorCode FindConstraint(const wchar_t* name, SCConstraintDef* outDef) override;
            ErrorCode GetIndexCount(std::int32_t* outCount) override;
            ErrorCode GetIndex(std::int32_t index, SCIndexDef* outDef) override;
            ErrorCode FindIndex(const wchar_t* name, SCIndexDef* outDef) override;
            ErrorCode AddColumn(const SCColumnDef& def) override;
            ErrorCode UpdateColumn(const SCColumnDef& def) override;
            ErrorCode RemoveColumn(const wchar_t* name) override;
            ErrorCode AddConstraint(const SCConstraintDef& def) override;
            ErrorCode RemoveConstraint(const wchar_t* name) override;
            ErrorCode AddIndex(const SCIndexDef& def) override;
            ErrorCode RemoveIndex(const wchar_t* name) override;

            void SetTableName(std::wstring tableName)
            {
                tableName_ = std::move(tableName);
            }

            void LoadTableDescription(const std::wstring& description)
            {
                description_ = description;
            }

            void LoadConstraint(const SCConstraintDef& def)
            {
                LoadConstraint(def, nextConstraintRowId_++);
            }

            void LoadConstraint(const SCConstraintDef& def, std::int64_t rowId)
            {
                const std::int64_t normalizedRowId = rowId > 0 ? rowId : nextConstraintRowId_++;
                if (normalizedRowId >= nextConstraintRowId_)
                {
                    nextConstraintRowId_ = normalizedRowId + 1;
                }

                const auto insertIt = std::find_if(
                    constraints_.begin(), constraints_.end(), [normalizedRowId](const SchemaConstraintEntry& existing) {
                        return existing.rowId > normalizedRowId;
                    });
                constraints_.insert(insertIt, SchemaConstraintEntry{normalizedRowId, def});
                constraintsByName_[def.name] = SchemaConstraintEntry{normalizedRowId, def};
                constraintRowIdsByName_[def.name] = normalizedRowId;
            }

            void ReplaceConstraint(const SCConstraintDef& def)
            {
                const auto vecIt = std::find_if(
                    constraints_.begin(), constraints_.end(), [&def](const SchemaConstraintEntry& existing) {
                        return existing.def.name == def.name;
                    });
                if (vecIt != constraints_.end())
                {
                    vecIt->def = def;
                }
                const std::int64_t rowId = vecIt != constraints_.end() ? vecIt->rowId : -1;
                constraintsByName_[def.name] = SchemaConstraintEntry{rowId, def};
                if (rowId >= 0)
                {
                    constraintRowIdsByName_[def.name] = rowId;
                }
            }

            void UnloadConstraint(const wchar_t* name)
            {
                if (name == nullptr)
                {
                    return;
                }

                constraintsByName_.erase(name);
                constraintRowIdsByName_.erase(name);

                const auto vecIt =
                    std::find_if(constraints_.begin(), constraints_.end(), [name](const SchemaConstraintEntry& def) {
                        return def.def.name == name;
                    });
                if (vecIt != constraints_.end())
                {
                    constraints_.erase(vecIt);
                }
            }

            void LoadIndex(const SCIndexDef& def)
            {
                LoadIndex(def, nextIndexRowId_++);
            }

            void LoadIndex(const SCIndexDef& def, std::int64_t rowId)
            {
                const std::int64_t normalizedRowId = rowId > 0 ? rowId : nextIndexRowId_++;
                if (normalizedRowId >= nextIndexRowId_)
                {
                    nextIndexRowId_ = normalizedRowId + 1;
                }

                const auto insertIt =
                    std::find_if(indexes_.begin(), indexes_.end(), [normalizedRowId](const SchemaIndexEntry& existing) {
                        return existing.rowId > normalizedRowId;
                    });
                indexes_.insert(insertIt, SchemaIndexEntry{normalizedRowId, def});
                indexesByName_[def.name] = SchemaIndexEntry{normalizedRowId, def};
                indexRowIdsByName_[def.name] = normalizedRowId;
            }

            void ReplaceIndex(const SCIndexDef& def)
            {
                const auto vecIt =
                    std::find_if(indexes_.begin(), indexes_.end(), [&def](const SchemaIndexEntry& existing) {
                        return existing.def.name == def.name;
                    });
                if (vecIt != indexes_.end())
                {
                    vecIt->def = def;
                }
                const std::int64_t rowId = vecIt != indexes_.end() ? vecIt->rowId : -1;
                indexesByName_[def.name] = SchemaIndexEntry{rowId, def};
                if (rowId >= 0)
                {
                    indexRowIdsByName_[def.name] = rowId;
                }
            }

            void UnloadIndex(const wchar_t* name)
            {
                if (name == nullptr)
                {
                    return;
                }

                indexesByName_.erase(name);
                indexRowIdsByName_.erase(name);

                const auto vecIt = std::find_if(indexes_.begin(), indexes_.end(), [name](const SchemaIndexEntry& def) {
                    return def.def.name == name;
                });
                if (vecIt != indexes_.end())
                {
                    indexes_.erase(vecIt);
                }
            }

            std::wstring LegacyPrimaryKeyName() const
            {
                return L"pk_" + SanitizeIdentifier(tableName_);
            }

            std::wstring LegacyIndexName(const std::wstring& columnName) const
            {
                return L"idx_" + SanitizeIdentifier(tableName_) + L"_" + SanitizeIdentifier(columnName);
            }

            void SetLegacyIndexState(const std::wstring& columnName, bool indexed)
            {
                const std::wstring indexName = LegacyIndexName(columnName);
                const auto indexIt =
                    std::find_if(indexes_.begin(), indexes_.end(), [&indexName](const SchemaIndexEntry& index) {
                        return index.def.name == indexName;
                    });

                if (!indexed)
                {
                    if (indexIt != indexes_.end())
                    {
                        indexesByName_.erase(indexName);
                        indexRowIdsByName_.erase(indexName);
                        indexes_.erase(indexIt);
                    }
                    return;
                }

                SCIndexDef index;
                index.name = indexName;
                index.sourceKind = SCSchemaSourceKind::Explicit;
                index.columns.push_back(SCIndexColumnDef{columnName, false});
                if (indexIt != indexes_.end())
                {
                    indexIt->def = index;
                    indexesByName_[indexName] = *indexIt;
                } else
                {
                    LoadIndex(std::move(index));
                }
            }

            void RemovePrimaryKeyIfColumnMatches(const std::wstring& columnName)
            {
                const auto it = std::remove_if(
                    constraints_.begin(), constraints_.end(), [&columnName](const SchemaConstraintEntry& constraint) {
                        return constraint.def.kind == SCConstraintKind::PrimaryKey &&
                               constraint.def.columns.size() == 1 &&
                               EqualsIgnoreCase(constraint.def.columns.front(), columnName);
                    });
                for (auto iter = it; iter != constraints_.end(); ++iter)
                {
                    constraintsByName_.erase(iter->def.name);
                    constraintRowIdsByName_.erase(iter->def.name);
                }
                constraints_.erase(it, constraints_.end());
            }

            const SCColumnDef* FindColumnDef(const std::wstring& name) const noexcept
            {
                const auto it = columnsByName_.find(name);
                return it == columnsByName_.end() ? nullptr : &it->second.def;
            }

            const SCConstraintDef* FindConstraintDef(const std::wstring& name) const noexcept
            {
                const auto it = constraintsByName_.find(name);
                return it == constraintsByName_.end() ? nullptr : &it->second.def;
            }

            const SCIndexDef* FindIndexDef(const std::wstring& name) const noexcept
            {
                const auto it = indexesByName_.find(name);
                return it == indexesByName_.end() ? nullptr : &it->second.def;
            }

            std::int64_t FindColumnRowId(const wchar_t* name) const noexcept
            {
                if (name == nullptr)
                {
                    return -1;
                }

                const auto it = columnRowIdsByName_.find(name);
                return it == columnRowIdsByName_.end() ? -1 : it->second;
            }

            std::int64_t FindConstraintRowId(const wchar_t* name) const noexcept
            {
                if (name == nullptr)
                {
                    return -1;
                }

                const auto it = constraintRowIdsByName_.find(name);
                return it == constraintRowIdsByName_.end() ? -1 : it->second;
            }

            std::int64_t FindIndexRowId(const wchar_t* name) const noexcept
            {
                if (name == nullptr)
                {
                    return -1;
                }

                const auto it = indexRowIdsByName_.find(name);
                return it == indexRowIdsByName_.end() ? -1 : it->second;
            }

            void LoadColumn(const SCColumnDef& def)
            {
                LoadColumn(def, nextColumnRowId_++);
            }

            void LoadColumn(const SCColumnDef& def, std::int64_t rowId)
            {
                const std::int64_t normalizedRowId = rowId > 0 ? rowId : nextColumnRowId_++;
                if (normalizedRowId >= nextColumnRowId_)
                {
                    nextColumnRowId_ = normalizedRowId + 1;
                }

                const auto insertIt = std::find_if(
                    columns_.begin(), columns_.end(), [normalizedRowId](const SchemaColumnEntry& existing) {
                        return existing.rowId > normalizedRowId;
                    });
                columns_.insert(insertIt, SchemaColumnEntry{normalizedRowId, def});
                columnsByName_[def.name] = SchemaColumnEntry{normalizedRowId, def};
                columnRowIdsByName_[def.name] = normalizedRowId;
            }

            void ReplaceColumn(const SCColumnDef& def)
            {
                const auto vecIt =
                    std::find_if(columns_.begin(), columns_.end(), [&def](const SchemaColumnEntry& existing) {
                        return existing.def.name == def.name;
                    });
                if (vecIt != columns_.end())
                {
                    vecIt->def = def;
                }
                const std::int64_t rowId = vecIt != columns_.end() ? vecIt->rowId : -1;
                columnsByName_[def.name] = SchemaColumnEntry{rowId, def};
                if (rowId >= 0)
                {
                    columnRowIdsByName_[def.name] = rowId;
                }
            }

            void UnloadColumn(const wchar_t* name)
            {
                if (name == nullptr)
                {
                    return;
                }

                const auto mapIt = columnsByName_.find(name);
                if (mapIt != columnsByName_.end())
                {
                    columnsByName_.erase(mapIt);
                }

                const auto rowIdIt = columnRowIdsByName_.find(name);
                if (rowIdIt != columnRowIdsByName_.end())
                {
                    columnRowIdsByName_.erase(rowIdIt);
                }

                const auto vecIt = std::find_if(columns_.begin(), columns_.end(), [name](const SchemaColumnEntry& def) {
                    return def.def.name == name;
                });
                if (vecIt != columns_.end())
                {
                    columns_.erase(vecIt);
                }
            }

            std::int64_t TableRowId() const noexcept
            {
                return tableRowId_;
            }

        private:
            SqliteDatabase* db_{nullptr};
            std::wstring tableName_;
            std::wstring description_;
            std::int64_t tableRowId_{0};
            std::vector<SchemaColumnEntry> columns_;
            std::vector<SchemaConstraintEntry> constraints_;
            std::vector<SchemaIndexEntry> indexes_;
            std::unordered_map<std::wstring, SchemaColumnEntry> columnsByName_;
            std::unordered_map<std::wstring, SchemaConstraintEntry> constraintsByName_;
            std::unordered_map<std::wstring, SchemaIndexEntry> indexesByName_;
            std::unordered_map<std::wstring, std::int64_t> columnRowIdsByName_;
            std::unordered_map<std::wstring, std::int64_t> constraintRowIdsByName_;
            std::unordered_map<std::wstring, std::int64_t> indexRowIdsByName_;
            std::int64_t nextColumnRowId_{1};
            std::int64_t nextConstraintRowId_{1};
            std::int64_t nextIndexRowId_{1};
        };

        class SqliteEditSession final : public ISCEditSession, public SCRefCountedObject
        {
        public:
            SqliteEditSession(std::wstring name, VersionId openedVersion)
                : name_(std::move(name)), openedVersion_(openedVersion)
            {
            }

            const wchar_t* GetName() const override
            {
                return name_.c_str();
            }
            EditState GetState() const noexcept override
            {
                return state_;
            }
            VersionId GetOpenedVersion() const noexcept override
            {
                return openedVersion_;
            }

            void SetState(EditState state) noexcept
            {
                state_ = state;
            }

        private:
            std::wstring name_;
            VersionId openedVersion_{0};
            EditState state_{EditState::Active};
        };

        class SqliteRecord final : public ISCRecord, public SCRefCountedObject
        {
        public:
            SqliteRecord(SqliteDatabase* db, SqliteTable* table, std::shared_ptr<SqliteRecordData> data)
                : db_(db), table_(table), data_(std::move(data))
            {
            }

            RecordId GetId() const noexcept override;
            bool IsDeleted() const noexcept override;
            VersionId GetLastModifiedVersion() const noexcept override;

            ErrorCode GetValue(const wchar_t* name, SCValue* outValue) override;
            ErrorCode SetValue(const wchar_t* name, const SCValue& value) override;

            ErrorCode GetInt64(const wchar_t* name, std::int64_t* outValue) override;
            ErrorCode SetInt64(const wchar_t* name, std::int64_t value) override;

            ErrorCode GetDouble(const wchar_t* name, double* outValue) override;
            ErrorCode SetDouble(const wchar_t* name, double value) override;

            ErrorCode GetBool(const wchar_t* name, bool* outValue) override;
            ErrorCode SetBool(const wchar_t* name, bool value) override;

            ErrorCode GetString(const wchar_t* name, const wchar_t** outValue) override;
            ErrorCode GetStringCopy(const wchar_t* name, std::wstring* outValue) override;
            ErrorCode SetString(const wchar_t* name, const wchar_t* value) override;

            ErrorCode GetBinary(const wchar_t* name, const std::uint8_t** outValue, std::size_t* outSize) override;
            ErrorCode GetBinaryCopy(const wchar_t* name, std::vector<std::uint8_t>* outValue) override;
            ErrorCode SetBinary(const wchar_t* name, const std::uint8_t* value, std::size_t size) override;

            ErrorCode GetRef(const wchar_t* name, RecordId* outValue) override;
            ErrorCode SetRef(const wchar_t* name, RecordId value) override;

        private:
            ErrorCode ReadTypedValue(const wchar_t* name, SCValue* outValue);
            ErrorCode ResolveValueStorage(const wchar_t* name, const SCValue** outValue) const;

            SqliteDatabase* db_{nullptr};
            SqliteTable* table_{nullptr};
            std::shared_ptr<SqliteRecordData> data_;
        };

        class SqliteRecordCursor final : public ISCRecordCursor, public SCRefCountedObject
        {
        public:
            explicit SqliteRecordCursor(std::vector<SCRecordPtr> records) : records_(std::move(records))
            {
            }

            ErrorCode Next(SCRecordPtr& outRecord) override
            {
                if (index_ < records_.size())
                {
                    outRecord = records_[index_++];
                    return SC_OK;
                }

                outRecord.Reset();
                return SC_OK;
            }

        private:
            std::vector<SCRecordPtr> records_;
            std::size_t index_{0};
        };

        class SqliteTable final : public ISCTable, public SCRefCountedObject
        {
        public:
            SqliteTable(SqliteDatabase* db, std::wstring name, std::int64_t tableRowId)
                : db_(db), name_(std::move(name)), schema_(SCMakeRef<SqliteSchema>(db, name_, tableRowId))
            {
            }

            ErrorCode GetRecord(RecordId id, SCRecordPtr& outRecord) override;
            ErrorCode CreateRecord(SCRecordPtr& outRecord) override;
            ErrorCode DeleteRecord(RecordId id) override;
            ErrorCode GetSchema(SCSchemaPtr& outSchema) override
            {
                outSchema = schema_;
                return SC_OK;
            }
            ErrorCode EnumerateRecords(SCRecordCursorPtr& outCursor) override;
            ErrorCode FindRecords(const SCQueryCondition& condition, SCRecordCursorPtr& outCursor) override;

            const std::wstring& Name() const noexcept
            {
                return name_;
            }
            void SetName(std::wstring name)
            {
                name_ = std::move(name);
            }
            SqliteSchema* Schema() const noexcept
            {
                return schema_.Get();
            }
            std::int64_t TableRowId() const noexcept
            {
                return schema_->TableRowId();
            }

            std::shared_ptr<SqliteRecordData> FindRecordData(RecordId id) const
            {
                const auto it = records_.find(id);
                return it == records_.end() ? nullptr : it->second;
            }

            std::unordered_map<RecordId, std::shared_ptr<SqliteRecordData>>& Records() noexcept
            {
                return records_;
            }

        private:
            SCRecordPtr MakeRecord(const std::shared_ptr<SqliteRecordData>& data)
            {
                return SCMakeRef<SqliteRecord>(db_, this, data);
            }

            SqliteDatabase* db_{nullptr};
            std::wstring name_;
            SCRefPtr<SqliteSchema> schema_;
            std::unordered_map<RecordId, std::shared_ptr<SqliteRecordData>> records_;
        };

        class SqliteDatabase final : public ISCDatabase,
                                     public ISCDatabaseDiagnosticsProvider,
                                     public IQueryIndexProvider,
                                     public IQueryIndexMaintainer,
                                     public IReferenceIndexProvider,
                                     public IReferenceIndexMaintainer,
                                     public ISqliteQueryIndexAccess,
                                     public SqliteUpgradeContext,
                                     public SCRefCountedObject
        {
        public:
            explicit SqliteDatabase(const std::wstring& path);
            explicit SqliteDatabase(const std::wstring& path, bool readOnly);
            explicit SqliteDatabase(const std::wstring& path, const SCOpenDatabaseOptions& options);
            ~SqliteDatabase() override;

            ErrorCode BeginEdit(const wchar_t* name, SCEditPtr& outEdit) override;
            ErrorCode Commit(ISCEditSession* edit) override;
            ErrorCode Rollback(ISCEditSession* edit) override;
            ErrorCode Undo() override;
            ErrorCode Redo() override;
            ErrorCode GetTableCount(std::int32_t* outCount) override;
            ErrorCode GetTableName(std::int32_t index, std::wstring* outName) override;
            ErrorCode GetTable(const wchar_t* name, SCTablePtr& outTable) override;
            ErrorCode CreateTable(const wchar_t* name, SCTablePtr& outTable) override;
            ErrorCode DeleteTable(const wchar_t* name) override;
            ErrorCode RenameTable(const wchar_t* originalName,
                                  const wchar_t* newName) override;
            ErrorCode AddObserver(ISCDatabaseObserver* observer) override;
            ErrorCode RemoveObserver(ISCDatabaseObserver* observer) override;
            ErrorCode GetLastConstraintViolationInfo(SCConstraintViolationInfo* outInfo) const override;
            ErrorCode ExecuteUpgradePlan(const SCUpgradePlan& plan,
                                         bool confirmed,
                                         SCUpgradeResult* outResult) override;
            ErrorCode BeginImportSession(const SCImportSessionOptions& options,
                                         SCImportStagingArea* outSession) override;
            ErrorCode AppendImportChunk(SCImportStagingArea* session,
                                        const SCImportChunk& chunk,
                                        SCImportCheckpoint* outCheckpoint) override;
            ErrorCode LoadImportRecoveryState(std::uint64_t sessionId, SCImportRecoveryState* outState) override;
            ErrorCode FinalizeImportSession(const SCImportFinalizeCommit& commit,
                                            SCImportRecoveryState* outState) override;
            ErrorCode AbortImportSession(std::uint64_t sessionId) override;
            ErrorCode CreateBackupCopy(const wchar_t* targetPath,
                                       const SCBackupOptions& options,
                                       SCBackupResult* outResult) override;
            ErrorCode ClearColumnValues(ISCTable* table, const wchar_t* name) override;
            ErrorCode ResetHistoryBaseline(SCBackupResult* outResult = nullptr) override;
            ErrorCode GetEditLogState(SCEditLogState* outState) const override;
            ErrorCode GetEditingState(SCEditingDatabaseState* outState) const override;
            VersionId GetCurrentVersion() const noexcept override
            {
                return version_;
            }
            std::int32_t GetSchemaVersion() const noexcept override
            {
                return schemaVersion_;
            }
            ErrorCode CollectDiagnostics(SCStorageHealthReport* outReport) const override;
            ErrorCode CheckQueryIndex(QueryIndexCheckResult* outResult) const override;
            ErrorCode RebuildQueryIndex() override;
            ErrorCode GetReferencesBySource(const std::wstring& sourceTable,
                                            RecordId sourceRecordId,
                                            std::vector<ReferenceRecord>* outRecords) const override;
            ErrorCode GetReferencesByTarget(const std::wstring& targetTable,
                                            RecordId targetRecordId,
                                            std::vector<ReverseReferenceRecord>* outRecords) const override;
            ErrorCode CheckReferenceIndex(ReferenceIndexCheckResult* outResult) const override;
            ErrorCode GetAllReferencesDiagnosticOnly(ReferenceIndex* outIndex) const override;
            ErrorCode RebuildReferenceIndexes() override;
            ErrorCode CommitReferenceDelta(const ReferenceIndex& forwardDelta,
                                           const ReverseReferenceIndex& reverseDelta) override;
            ErrorCode AnalyzeCompositeIndexPlan(const QueryPlan& inputPlan, QueryPlan* outPlan) override;
            ErrorCode CollectCompositeIndexRecordIds(const QueryPlan& analyzedPlan,
                                                     std::vector<RecordId>* outRecordIds,
                                                     std::uint64_t* outScannedEntries) override;
            ErrorCode ExecuteSql(const char* sql) override
            {
                return db_.Execute(sql);
            }
            bool HasTableColumn(const char* tableName, const char* columnName) const override
            {
                return HasTableColumnRaw(db_.Raw(), tableName, columnName);
            }
            ErrorCode BackfillSchemaMetadataV3() override;
            void InitializeQueryIndexStorage() override;

            friend class SqliteSchema;
            friend class SqliteTable;

            bool HasActiveEdit() const noexcept
            {
                return static_cast<bool>(activeEdit_);
            }
            bool IsCleanShutdown() const noexcept
            {
                return cleanShutdown_;
            }
            void SuppressCleanShutdownOnDestroy() noexcept
            {
                persistCleanShutdownOnDestroy_ = false;
            }
            RecordId AllocateRecordId() noexcept
            {
                return nextRecordId_++;
            }

            ErrorCode WriteValue(SqliteTable* table,
                                 const std::shared_ptr<SqliteRecordData>& data,
                                 const std::wstring& fieldName,
                                 const SCValue& value);
            ErrorCode ResolveRelationStoredValue(const SCColumnDef& relationColumn,
                                                 RecordId targetRecordId,
                                                 SCValue* outValue) const;
            ErrorCode ResolveRelationTargetRecordId(const SCColumnDef& relationColumn,
                                                    const SCValue& storedValue,
                                                    RecordId* outRecordId) const;
            ErrorCode DeleteRecord(SqliteTable* table, const std::shared_ptr<SqliteRecordData>& data);
            void RecordCreate(SqliteTable* table, const std::shared_ptr<SqliteRecordData>& data);
            ErrorCode PersistAddedColumn(SqliteSchema* schema, const SCColumnDef& def);
            ErrorCode PersistUpdatedColumn(SqliteSchema* schema, const SCColumnDef& def);
            ErrorCode PersistRemovedColumn(SqliteSchema* schema, const wchar_t* columnName);

        private:
            struct JournalLookup
            {
                bool createdInActiveEdit{false};
                bool deletedInActiveEdit{false};
            };

            void InitializeSchema();
            void LoadMetadata();
            void SaveMetadata(VersionId version, VersionId baselineVersion);
            void SaveMetadata(VersionId version);
            void SaveMetadata();
            void SaveMetadataKey(const wchar_t* key, const std::wstring& value);
            void LoadTables();
            bool HasTable(const wchar_t* name);
            void EnsureSchemaMetadataTables();
            ErrorCode LoadSchemaMetadata(SqliteTable* table);
            void LoadJournalStacks();
            // Explicit writable setup/repair path only. Must not be called from load/open.
            ErrorCode EnsureLegacyColumnIndexes();
            // Explicit repair/write path only. Must not be called from load/open.
            ErrorCode RebuildCompositeQueryIndexes();
            ErrorCode EnsureImportSessionStore();
            void EnsureColumnIndex(std::int64_t tableRowId, const std::wstring& columnName);
            void RunStartupIntegrityCheck();
            void LogStartupDiagnostic(SCDiagnosticSeverity severity,
                                      const std::wstring& category,
                                      const std::wstring& message);
            void ClearConstraintViolation() const;
            void SetConstraintViolation(const SCConstraintViolationInfo& info) const;
            void SetCleanShutdownFlag(bool cleanShutdown);
            ErrorCode EnsureWritable() const;
            ErrorCode ValidateActiveEdit(ISCEditSession* edit) const;
            ErrorCode ValidateWrite(SqliteTable* table,
                                    const std::shared_ptr<SqliteRecordData>& data,
                                    const std::wstring& fieldName,
                                    const SCValue& value);
            ErrorCode ValidateColumnDefForSchema(SqliteSchema* schema, const SCColumnDef& def) const;
            ErrorCode ValidateColumnDefForUpdate(SqliteSchema* schema, const SCColumnDef& def) const;
            ErrorCode ValidateConstraintDefForSchema(SqliteSchema* schema, const SCConstraintDef& def) const;
            ErrorCode ValidateIndexDefForSchema(SqliteSchema* schema, const SCIndexDef& def) const;
            ErrorCode ValidateConstraintUniqueness(SqliteTable* table,
                                                   const SCConstraintDef& constraint,
                                                   const std::shared_ptr<SqliteRecordData>& candidateData,
                                                   const std::wstring* overrideFieldName = nullptr,
                                                   const SCValue* overrideValue = nullptr) const;
            ErrorCode ValidateCheckConstraint(SqliteTable* table,
                                              const SCConstraintDef& constraint,
                                              const std::shared_ptr<SqliteRecordData>& candidateData,
                                              const std::wstring* overrideFieldName = nullptr,
                                              const SCValue* overrideValue = nullptr) const;
            ErrorCode ValidateForeignKeyConstraint(SqliteTable* table,
                                                   const SCConstraintDef& constraint,
                                                   const std::shared_ptr<SqliteRecordData>& candidateData,
                                                   const std::wstring* overrideFieldName = nullptr,
                                                   const SCValue* overrideValue = nullptr) const;
            ErrorCode ApplyForeignKeyActionsForTableDelete(SqliteTable* table,
                                                           const std::shared_ptr<SqliteRecordData>& candidateData);
            ErrorCode ApplyForeignKeyActionsForColumnUpdate(SqliteTable* table,
                                                            const std::shared_ptr<SqliteRecordData>& candidateData,
                                                            const std::wstring& fieldName,
                                                            const SCValue& oldValue,
                                                            const SCValue& newValue);
            ErrorCode ValidateTableConstraints(SqliteTable* table,
                                               const std::shared_ptr<SqliteRecordData>& candidateData,
                                               const std::wstring* overrideFieldName = nullptr,
                                               const SCValue* overrideValue = nullptr,
                                               const SCConstraintDef* specificConstraint = nullptr) const;
            ErrorCode ValidateUniqueAndPrimaryKeyConstraints(SqliteTable* table,
                                                             const std::shared_ptr<SqliteRecordData>& candidateData,
                                                             const std::wstring* overrideFieldName = nullptr,
                                                             const SCValue* overrideValue = nullptr,
                                                             const SCConstraintDef* specificConstraint = nullptr) const;
            ErrorCode ValidateForeignKeyReferencesToTable(SqliteTable* table,
                                                          const std::shared_ptr<SqliteRecordData>& candidateData,
                                                          const std::wstring* overrideFieldName = nullptr,
                                                          const SCValue* overrideValue = nullptr) const;
            ErrorCode ValidateForeignKeyReferencesForTouchedTables(
                const JournalTransaction& tx,
                bool reverseRenameResolution = false) const;
            bool HasForeignKeyReferencesToTable(const std::wstring& tableName) const;
            bool HasForeignKeyReferencesToColumn(const std::wstring& tableName, const std::wstring& columnName) const;
            void MarkForeignKeyReferenceCacheDirty() noexcept;
            ErrorCode RefreshForeignKeyReferenceCache() const;
            const std::vector<ForeignKeyReferenceEntry>* GetForeignKeyReferenceEntries(const std::wstring& tableName) const;
            const std::vector<ForeignKeyReferenceEntry>*
            GetForeignKeyReferenceEntries(const std::wstring& tableName, const std::wstring& columnName) const;
            ErrorCode ReadConstraintValue(SqliteTable* table,
                                          const SqliteRecordData& recordData,
                                          const std::wstring& columnName,
                                          const std::wstring* overrideFieldName,
                                          const SCValue* overrideValue,
                                          SCValue* outValue,
                                          bool* outIsNull) const;
            const SCColumnDef* FindRelationStorageColumn(const SCColumnDef& relationColumn) const;
            const SCColumnDef* FindRelationDisplayColumn(const SCColumnDef& relationColumn) const;
            ErrorCode ResolveRelationWriteValue(const SCColumnDef& relationColumn,
                                                const SCValue& inputValue,
                                                SCValue* outValue) const;
            ErrorCode ResolveRelationDisplayValue(const SCColumnDef& relationColumn,
                                                  const SCValue& storedValue,
                                                  std::wstring* outValue) const;
            ErrorCode EncodeIndexColumnValue(const SCValue& value,
                                             ValueKind valueKind,
                                             bool descending,
                                             std::vector<std::uint8_t>* outBytes) const;
            ErrorCode EncodeIndexColumnPrefixValue(const SCValue& value,
                                                   ValueKind valueKind,
                                                   bool descending,
                                                   std::vector<std::uint8_t>* outBytes) const;
            ErrorCode BuildCompositeIndexKey(const SqliteSchema* schema,
                                             const SCIndexDef& indexDef,
                                             const SqliteRecordData& recordData,
                                             CompositeIndexEncodedKey* outKey) const;
            ErrorCode BuildCompositeLookupBounds(const SqliteSchema* schema,
                                                 const QueryPlan& analyzedPlan,
                                                 CompositeIndexLookupBounds* outBounds) const;
            ErrorCode BuildCompositeEqualityPrefixBounds(const SqliteSchema* schema,
                                                         const QueryPlan& analyzedPlan,
                                                         CompositeIndexLookupBounds* outBounds) const;
            ErrorCode ValidateRequiredValuesForCommit() const;
            ErrorCode ValidateUniqueAndPrimaryKeyConstraintsForTouchedTables(
                const JournalTransaction& tx,
                bool reverseRenameResolution = false) const;
            bool HasAliveRecords(SqliteSchema* schema) const;
            bool IsRecordReferenced(const std::wstring& tableName, RecordId recordId) const;
            SqliteTable* FindTableByRowId(std::int64_t tableRowId) const;
            void MarkReferenceIndexDirty() noexcept;
            void RefreshReferenceIndexState();
            void CollectTouchedTableNames(const JournalTransaction& tx,
                                          std::vector<std::wstring>* outTableNames,
                                          bool reverseRenameResolution = false) const;
            ErrorCode CaptureDeferredRenameState(const std::wstring& oldName,
                                                 const std::wstring& newName,
                                                 DeferredRenameState* outState) const;
            void ApplyDeferredRenameWorkingState(const DeferredRenameState& state);
            void RollbackDeferredRenameWorkingState(const DeferredRenameState& state);
            DeferredRenameState* FindDeferredRenameState(const std::wstring& oldName,
                                                         const std::wstring& newName);
            const DeferredRenameState* FindDeferredRenameState(const std::wstring& oldName,
                                                               const std::wstring& newName) const;
            ErrorCode RecordDeferredRenameTable(const std::wstring& oldName,
                                                const std::wstring& newName);
            bool JournalTransactionContainsRenameTable(
                const JournalTransaction& tx) const;
            std::wstring ResolveJournalTableNameToReplayState(
                const JournalTransaction& tx,
                const std::wstring& tableName,
                bool reverseRenameResolution) const;
            bool JournalEntryMatchesCurrentTableName(const JournalEntry& entry,
                                                     const std::wstring& tableName) const;
            JournalLookup LookupRecordJournalState(const std::wstring& tableName, RecordId recordId) const;
            void RemoveFieldJournalEntries(const std::wstring& tableName, RecordId recordId);
            void RemoveAllJournalEntriesForRecord(const std::wstring& tableName, RecordId recordId);
            void RecordSchemaJournal(const std::wstring& tableName,
                                     const SCColumnDef& oldColumn,
                                     const SCColumnDef& newColumn,
                                     JournalOp op,
                                     std::int64_t columnRowId = -1);
            void RecordConstraintJournal(const std::wstring& tableName,
                                         const SCConstraintDef& oldConstraint,
                                         const SCConstraintDef& newConstraint,
                                         JournalOp op,
                                         std::int64_t constraintRowId = -1);
            void RecordIndexJournal(const std::wstring& tableName,
                                    const SCIndexDef& oldIndex,
                                    const SCIndexDef& newIndex,
                                    JournalOp op,
                                    std::int64_t indexRowId = -1);
            void RecordJournal(const std::wstring& tableName,
                               RecordId recordId,
                               const std::wstring& fieldName,
                               const SCValue& oldValue,
                               const SCValue& newValue,
                               bool oldDeleted,
                               bool newDeleted,
                               JournalOp op);
            void RecordTableRenameJournal(const std::wstring& originalName,
                                          const std::wstring& newName);
            ErrorCode PersistTableRename(const std::wstring& originalName,
                                         const std::wstring& newName,
                                         bool recordJournal,
                                         bool manageTransaction);
            ErrorCode PersistDeferredRenameToStorage(const DeferredRenameState& state);
            ErrorCode PersistDeferredSchemaOps();
            ErrorCode PersistSchemaAddColumn(SqliteSchema* schema, const SCColumnDef& def, std::int64_t rowId = -1);
            ErrorCode PersistSchemaUpdateColumn(SqliteSchema* schema, const SCColumnDef& def);
            ErrorCode PersistSchemaRemoveColumn(SqliteSchema* schema, const wchar_t* columnName);
            ErrorCode PersistSchemaAddConstraint(SqliteSchema* schema,
                                                 const SCConstraintDef& def,
                                                 std::int64_t rowId = -1);
            ErrorCode PersistSchemaRemoveConstraint(SqliteSchema* schema, const wchar_t* name);
            ErrorCode PersistSchemaAddIndex(SqliteSchema* schema, const SCIndexDef& def, std::int64_t rowId = -1);
            ErrorCode PersistSchemaRemoveIndex(SqliteSchema* schema, const wchar_t* name);
            ErrorCode PersistQueryIndexDefinition(SqliteSchema* schema,
                                                  const SCIndexDef& def,
                                                  std::int64_t schemaIndexRowId,
                                                  bool updateCache = true);
            ErrorCode RemoveQueryIndexDefinition(SqliteSchema* schema, const wchar_t* name, bool updateCache = true);
            ErrorCode RebuildCompositeIndexEntriesForTable(SqliteTable* table,
                                                           const SCIndexDef& indexDef,
                                                           std::int64_t schemaIndexRowId = -1);
            ErrorCode RebuildCompositeIndexEntriesForRecord(SqliteTable* table,
                                                            const SCIndexDef& indexDef,
                                                            const SqliteRecordData& recordData,
                                                            std::int64_t schemaIndexRowId = -1);
            ErrorCode RebuildCompositeIndexEntriesForRecord(SqliteTable* table, const SqliteRecordData& recordData);
            ErrorCode RebuildCompositeIndexesForTable(SqliteTable* table);
            std::int64_t FindQueryIndexStorageRowId(std::int64_t tableRowId, const std::wstring& indexName) const;
            ErrorCode CheckCompositeQueryIndexConsistency(QueryIndexCheckResult* outResult) const;
            ErrorCode SyncLegacyIndexMetadata(SqliteSchema* schema, const std::wstring& columnName, bool indexed);
            ErrorCode RemoveLegacyPrimaryKeyMetadata(SqliteSchema* schema, const std::wstring& columnName);
            ErrorCode PersistAddedConstraint(SqliteSchema* schema, const SCConstraintDef& def);
            ErrorCode PersistRemovedConstraint(SqliteSchema* schema, const wchar_t* name);
            ErrorCode PersistAddedIndex(SqliteSchema* schema, const SCIndexDef& def);
            ErrorCode PersistRemovedIndex(SqliteSchema* schema, const wchar_t* name);
            ErrorCode ApplyJournalReverse(const JournalTransaction& tx);
            ErrorCode ApplyJournalForward(const JournalTransaction& tx);
            ErrorCode ApplyJournalReverse(const JournalTransaction& tx,
                                          std::size_t beginIndex,
                                          std::size_t endIndex,
                                          std::size_t* outAppliedCount = nullptr);
            ErrorCode ApplyJournalForward(const JournalTransaction& tx,
                                          std::size_t beginIndex,
                                          std::size_t endIndex,
                                          std::size_t* outAppliedCount = nullptr);
            ErrorCode ApplyEntry(const JournalEntry& entry, bool reverse);
            ErrorCode ApplySchemaEntry(const JournalEntry& entry, bool reverse);
            void UpdateTouchedVersions(const JournalTransaction& tx,
                                       VersionId version,
                                       bool reverseRenameResolution = false);
            SCChangeSet BuildChangeSet(const JournalTransaction& tx, ChangeSource source, VersionId version) const;
            void NotifyObservers(const SCChangeSet& SCChangeSet);
            std::vector<std::pair<std::wstring, RecordId>> GetTouchedRecordKeys(
                const JournalTransaction& tx,
                bool reverseRenameResolution = false) const;
            void PersistTouchedRecords(const JournalTransaction& tx,
                                       VersionId version,
                                       bool reverseRenameResolution = false);
            std::int64_t InsertJournalTransaction(const JournalTransaction& tx, int stackKind, int stackOrder);
            void PersistJournalEntries(std::int64_t txId, const JournalTransaction& tx);
            void PersistSchemaJournalEntries(std::int64_t txId, const JournalTransaction& tx, int* sequence);
            void DeleteRedoJournalRows();
            void UpdateJournalTransactionStack(std::int64_t txId, int stackKind, int stackOrder);
            void DeleteJournalTransaction(std::int64_t txId);
            void ReloadColumnValuesFromStorage(SqliteTable* table, const wchar_t* columnName);
            std::wstring SerializeImportSession(const SCImportStagingArea& session) const;
            ErrorCode DeserializeImportSession(const std::wstring& payload, SCImportStagingArea* outSession) const;

            std::wstring path_;
            SqliteDb db_;
            SCDatabaseOpenMode openMode_{SCDatabaseOpenMode::Normal};
            VersionId baselineVersion_{0};
            bool readOnly_{false};
            VersionId version_{0};
            RecordId nextRecordId_{1};
            std::int32_t schemaVersion_{0};
            bool referenceIndexDirty_{true};
            bool referenceIndexBuilt_{false};
            VersionId referenceIndexVersion_{0};
            mutable bool foreignKeyReferenceCacheDirty_{true};
            mutable std::unordered_map<std::wstring, std::vector<ForeignKeyReferenceEntry>>
                foreignKeyReferenceCacheByTable_;
            mutable std::unordered_map<std::wstring, std::vector<ForeignKeyReferenceEntry>>
                foreignKeyReferenceCacheByTableAndColumn_;
            bool cleanShutdown_{true};
            bool persistCleanShutdownOnDestroy_{true};
            bool dirtyStartupDetected_{false};
            bool corruptionDetected_{false};
            bool importSessionStoreReady_{false};
            mutable std::optional<SCConstraintViolationInfo> lastConstraintViolation_;
            mutable std::unordered_set<std::wstring> activeConstraintPropagationKeys_;
            std::vector<SCDiagnosticEntry> startupDiagnostics_;
            std::map<std::wstring, SCTablePtr> tables_;
            std::vector<ISCDatabaseObserver*> observers_;
            SCRefPtr<SqliteEditSession> activeEdit_;
            JournalTransaction activeJournal_;
            std::vector<DeferredSchemaOp> activeSchemaOps_;
            std::vector<SqlitePersistedJournalTransaction> undoStack_;
            std::vector<SqlitePersistedJournalTransaction> redoStack_;
            std::unordered_map<std::wstring, std::int64_t> queryIndexRowIdsByTableAndName_;
        };

        ErrorCode SqliteSchema::GetColumnCount(std::int32_t* outCount)
        {
            if (outCount == nullptr)
            {
                return SC_E_POINTER;
            }
            *outCount = static_cast<std::int32_t>(columns_.size());
            return SC_OK;
        }

        ErrorCode SqliteSchema::GetColumn(std::int32_t index, SCColumnDef* outDef)
        {
            if (outDef == nullptr)
            {
                return SC_E_POINTER;
            }
            if (index < 0 || static_cast<std::size_t>(index) >= columns_.size())
            {
                return SC_E_INVALIDARG;
            }
            *outDef = columns_[static_cast<std::size_t>(index)].def;
            return SC_OK;
        }

        ErrorCode SqliteSchema::FindColumn(const wchar_t* name, SCColumnDef* outDef)
        {
            if (name == nullptr)
            {
                return SC_E_INVALIDARG;
            }
            if (outDef == nullptr)
            {
                return SC_E_POINTER;
            }
            const auto it = columnsByName_.find(name);
            if (it == columnsByName_.end())
            {
                return SC_E_COLUMN_NOT_FOUND;
            }
            *outDef = it->second.def;
            return SC_OK;
        }

        ErrorCode SqliteSchema::GetSchemaSnapshot(SCTableSchemaSnapshot* outSnapshot)
        {
            if (outSnapshot == nullptr)
            {
                return SC_E_POINTER;
            }

            outSnapshot->table.name = tableName_;
            outSnapshot->table.description = description_;
            outSnapshot->columns.clear();
            outSnapshot->constraints.clear();
            outSnapshot->indexes.clear();

            outSnapshot->columns.reserve(columns_.size());
            for (const auto& column : columns_)
            {
                outSnapshot->columns.push_back(column.def);
            }

            outSnapshot->constraints.reserve(constraints_.size());
            for (const auto& constraint : constraints_)
            {
                outSnapshot->constraints.push_back(constraint.def);
            }

            outSnapshot->indexes.reserve(indexes_.size());
            for (const auto& index : indexes_)
            {
                outSnapshot->indexes.push_back(index.def);
            }

            return SC_OK;
        }

        ErrorCode SqliteSchema::GetConstraintCount(std::int32_t* outCount)
        {
            if (outCount == nullptr)
            {
                return SC_E_POINTER;
            }
            *outCount = static_cast<std::int32_t>(constraints_.size());
            return SC_OK;
        }

        ErrorCode SqliteSchema::GetConstraint(std::int32_t index, SCConstraintDef* outDef)
        {
            if (outDef == nullptr)
            {
                return SC_E_POINTER;
            }
            if (index < 0 || static_cast<std::size_t>(index) >= constraints_.size())
            {
                return SC_E_INVALIDARG;
            }

            *outDef = constraints_[static_cast<std::size_t>(index)].def;
            return SC_OK;
        }

        ErrorCode SqliteSchema::FindConstraint(const wchar_t* name, SCConstraintDef* outDef)
        {
            if (name == nullptr)
            {
                return SC_E_INVALIDARG;
            }
            if (outDef == nullptr)
            {
                return SC_E_POINTER;
            }

            const auto it = constraintsByName_.find(name);
            if (it == constraintsByName_.end())
            {
                return SC_E_CONSTRAINT_NOT_FOUND;
            }

            *outDef = it->second.def;
            return SC_OK;
        }

        ErrorCode SqliteSchema::GetIndexCount(std::int32_t* outCount)
        {
            if (outCount == nullptr)
            {
                return SC_E_POINTER;
            }
            *outCount = static_cast<std::int32_t>(indexes_.size());
            return SC_OK;
        }

        ErrorCode SqliteSchema::GetIndex(std::int32_t index, SCIndexDef* outDef)
        {
            if (outDef == nullptr)
            {
                return SC_E_POINTER;
            }
            if (index < 0 || static_cast<std::size_t>(index) >= indexes_.size())
            {
                return SC_E_INVALIDARG;
            }

            *outDef = indexes_[static_cast<std::size_t>(index)].def;
            return SC_OK;
        }

        ErrorCode SqliteSchema::FindIndex(const wchar_t* name, SCIndexDef* outDef)
        {
            if (name == nullptr)
            {
                return SC_E_INVALIDARG;
            }
            if (outDef == nullptr)
            {
                return SC_E_POINTER;
            }

            const auto it = indexesByName_.find(name);
            if (it == indexesByName_.end())
            {
                return SC_E_INDEX_NOT_FOUND;
            }

            *outDef = it->second.def;
            return SC_OK;
        }

        ErrorCode SqliteSchema::AddColumn(const SCColumnDef& def)
        {
            const ErrorCode validate = db_->ValidateColumnDefForSchema(this, def);
            if (Failed(validate))
            {
                return validate;
            }
            if (columnsByName_.contains(def.name))
            {
                return SC_E_COLUMN_EXISTS;
            }
            const ErrorCode persist = db_->PersistAddedColumn(this, def);
            return persist;
        }

        ErrorCode SqliteSchema::UpdateColumn(const SCColumnDef& def)
        {
            const ErrorCode validate = db_->ValidateColumnDefForUpdate(this, def);
            if (Failed(validate))
            {
                return validate;
            }
            if (FindColumnDef(def.name) == nullptr)
            {
                return SC_E_COLUMN_NOT_FOUND;
            }

            const ErrorCode persist = db_->PersistUpdatedColumn(this, def);
            return persist;
        }

        ErrorCode SqliteSchema::RemoveColumn(const wchar_t* name)
        {
            if (name == nullptr)
            {
                return SC_E_INVALIDARG;
            }
            if (FindColumnDef(name) == nullptr)
            {
                return SC_E_COLUMN_NOT_FOUND;
            }
            return db_->PersistRemovedColumn(this, name);
        }

        ErrorCode SqliteSchema::AddConstraint(const SCConstraintDef& def)
        {
            const ErrorCode validate = db_->ValidateConstraintDefForSchema(this, def);
            if (Failed(validate))
            {
                return validate;
            }
            if (constraintsByName_.contains(def.name))
            {
                return SC_E_INVALIDARG;
            }
            return db_->PersistAddedConstraint(this, def);
        }

        ErrorCode SqliteSchema::RemoveConstraint(const wchar_t* name)
        {
            if (name == nullptr)
            {
                return SC_E_INVALIDARG;
            }
            if (FindConstraintDef(name) == nullptr)
            {
                return SC_E_CONSTRAINT_NOT_FOUND;
            }
            return db_->PersistRemovedConstraint(this, name);
        }

        ErrorCode SqliteSchema::AddIndex(const SCIndexDef& def)
        {
            const ErrorCode validate = db_->ValidateIndexDefForSchema(this, def);
            if (Failed(validate))
            {
                return validate;
            }
            if (indexesByName_.contains(def.name))
            {
                return SC_E_CONSTRAINT_VIOLATION;
            }
            return db_->PersistAddedIndex(this, def);
        }

        ErrorCode SqliteSchema::RemoveIndex(const wchar_t* name)
        {
            if (name == nullptr)
            {
                return SC_E_INVALIDARG;
            }
            if (FindIndexDef(name) == nullptr)
            {
                return SC_E_INDEX_NOT_FOUND;
            }
            return db_->PersistRemovedIndex(this, name);
        }

        RecordId SqliteRecord::GetId() const noexcept
        {
            return data_->id;
        }

        bool SqliteRecord::IsDeleted() const noexcept
        {
            return data_->state == RecordState::Deleted;
        }

        VersionId SqliteRecord::GetLastModifiedVersion() const noexcept
        {
            return data_->lastModifiedVersion;
        }

        ErrorCode SqliteRecord::ReadTypedValue(const wchar_t* name, SCValue* outValue)
        {
            return GetValue(name, outValue);
        }

        ErrorCode SqliteRecord::ResolveValueStorage(const wchar_t* name, const SCValue** outValue) const
        {
            if (name == nullptr)
            {
                return SC_E_INVALIDARG;
            }
            if (outValue == nullptr)
            {
                return SC_E_POINTER;
            }
            if (IsDeleted())
            {
                return SC_E_RECORD_DELETED;
            }

            const SCColumnDef* column = table_->Schema()->FindColumnDef(name);
            if (column == nullptr)
            {
                return SC_E_COLUMN_NOT_FOUND;
            }

            const auto it = data_->values.find(name);
            *outValue = (it != data_->values.end()) ? &it->second : &column->defaultValue;
            return (*outValue)->IsNull() ? SC_E_VALUE_IS_NULL : SC_OK;
        }

        ErrorCode SqliteRecord::GetValue(const wchar_t* name, SCValue* outValue)
        {
            if (outValue == nullptr)
            {
                return SC_E_POINTER;
            }
            const SCValue* storage = nullptr;
            const ErrorCode rc = ResolveValueStorage(name, &storage);
            if (rc == SC_E_VALUE_IS_NULL)
            {
                *outValue = SCValue::Null();
                return rc;
            }
            if (Failed(rc))
            {
                return rc;
            }
            *outValue = *storage;
            return SC_OK;
        }

        ErrorCode SqliteRecord::SetValue(const wchar_t* name, const SCValue& value)
        {
            if (name == nullptr)
            {
                return SC_E_INVALIDARG;
            }
            return db_->WriteValue(table_, data_, name, value);
        }

        ErrorCode SqliteRecord::GetInt64(const wchar_t* name, std::int64_t* outValue)
        {
            SCValue value;
            const ErrorCode rc = ReadTypedValue(name, &value);
            if (Failed(rc))
            {
                return rc;
            }
            return value.AsInt64(outValue);
        }

        ErrorCode SqliteRecord::SetInt64(const wchar_t* name, std::int64_t value)
        {
            return SetValue(name, SCValue::FromInt64(value));
        }

        ErrorCode SqliteRecord::GetDouble(const wchar_t* name, double* outValue)
        {
            SCValue value;
            const ErrorCode rc = ReadTypedValue(name, &value);
            if (Failed(rc))
            {
                return rc;
            }
            return value.AsDouble(outValue);
        }

        ErrorCode SqliteRecord::SetDouble(const wchar_t* name, double value)
        {
            return SetValue(name, SCValue::FromDouble(value));
        }

        ErrorCode SqliteRecord::GetBool(const wchar_t* name, bool* outValue)
        {
            SCValue value;
            const ErrorCode rc = ReadTypedValue(name, &value);
            if (Failed(rc))
            {
                return rc;
            }
            return value.AsBool(outValue);
        }

        ErrorCode SqliteRecord::SetBool(const wchar_t* name, bool value)
        {
            return SetValue(name, SCValue::FromBool(value));
        }

        ErrorCode SqliteRecord::GetString(const wchar_t* name, const wchar_t** outValue)
        {
            const SCValue* storage = nullptr;
            const ErrorCode rc = ResolveValueStorage(name, &storage);
            if (Failed(rc))
            {
                return rc;
            }
            return storage->AsString(outValue);
        }

        ErrorCode SqliteRecord::GetStringCopy(const wchar_t* name, std::wstring* outValue)
        {
            SCValue value;
            const ErrorCode rc = ReadTypedValue(name, &value);
            if (Failed(rc))
            {
                return rc;
            }
            return value.AsStringCopy(outValue);
        }

        ErrorCode SqliteRecord::SetString(const wchar_t* name, const wchar_t* value)
        {
            return SetValue(name, value == nullptr ? SCValue::Null() : SCValue::FromString(value));
        }

        ErrorCode SqliteRecord::GetBinary(const wchar_t* name, const std::uint8_t** outValue, std::size_t* outSize)
        {
            const SCValue* storage = nullptr;
            const ErrorCode rc = ResolveValueStorage(name, &storage);
            if (Failed(rc))
            {
                return rc;
            }
            return storage->AsBinary(outValue, outSize);
        }

        ErrorCode SqliteRecord::GetBinaryCopy(const wchar_t* name, std::vector<std::uint8_t>* outValue)
        {
            SCValue value;
            const ErrorCode rc = ReadTypedValue(name, &value);
            if (Failed(rc))
            {
                return rc;
            }
            return value.AsBinaryCopy(outValue);
        }

        ErrorCode SqliteRecord::SetBinary(const wchar_t* name, const std::uint8_t* value, std::size_t size)
        {
            if (value == nullptr && size > 0)
            {
                return SC_E_POINTER;
            }
            std::vector<std::uint8_t> bytes;
            if (size > 0)
            {
                bytes.assign(value, value + size);
            }
            return SetValue(name, SCValue::FromBinary(std::move(bytes)));
        }

        ErrorCode SqliteRecord::GetRef(const wchar_t* name, RecordId* outValue)
        {
            if (outValue == nullptr)
            {
                return SC_E_POINTER;
            }

            const SCColumnDef* column = table_->Schema()->FindColumnDef(name);
            if (column == nullptr)
            {
                return SC_E_COLUMN_NOT_FOUND;
            }

            SCValue value;
            const ErrorCode rc = ReadTypedValue(name, &value);
            if (Failed(rc))
            {
                return rc;
            }

            return db_->ResolveRelationTargetRecordId(*column, value, outValue);
        }

        ErrorCode SqliteRecord::SetRef(const wchar_t* name, RecordId value)
        {
            if (name == nullptr)
            {
                return SC_E_INVALIDARG;
            }

            const SCColumnDef* column = table_->Schema()->FindColumnDef(name);
            if (column == nullptr)
            {
                return SC_E_COLUMN_NOT_FOUND;
            }

            SCValue storedValue;
            const ErrorCode rc = db_->ResolveRelationStoredValue(*column, value, &storedValue);
            if (Failed(rc))
            {
                return rc;
            }
            return SetValue(name, storedValue);
        }

        ErrorCode SqliteTable::GetRecord(RecordId id, SCRecordPtr& outRecord)
        {
            auto data = FindRecordData(id);
            if (!data)
            {
                return SC_E_RECORD_NOT_FOUND;
            }
            outRecord = MakeRecord(data);
            return SC_OK;
        }

        ErrorCode SqliteTable::CreateRecord(SCRecordPtr& outRecord)
        {
            if (!db_->HasActiveEdit())
            {
                return SC_E_NO_ACTIVE_EDIT;
            }

            std::int32_t columnCount = 0;
            const ErrorCode columnCountRc = schema_->GetColumnCount(&columnCount);
            if (Failed(columnCountRc))
            {
                return columnCountRc;
            }
            if (columnCount <= 0)
            {
                return SC_E_SCHEMA_VIOLATION;
            }

            auto data = std::make_shared<SqliteRecordData>(db_->AllocateRecordId());
            records_.emplace(data->id, data);
            db_->RecordCreate(this, data);
            const ErrorCode constraintRc = db_->ValidateTableConstraints(this, data);
            if (Failed(constraintRc))
            {
                db_->RemoveAllJournalEntriesForRecord(name_, data->id);
                records_.erase(data->id);
                return constraintRc;
            }
            db_->MarkReferenceIndexDirty();
            outRecord = MakeRecord(data);
            return SC_OK;
        }

        ErrorCode SqliteTable::DeleteRecord(RecordId id)
        {
            auto data = FindRecordData(id);
            if (!data)
            {
                return SC_E_RECORD_NOT_FOUND;
            }
            return db_->DeleteRecord(this, data);
        }

        ErrorCode SqliteTable::EnumerateRecords(SCRecordCursorPtr& outCursor)
        {
            std::vector<SCRecordPtr> records;
            for (const auto& [_, data] : records_)
            {
                if (data->state == RecordState::Alive)
                {
                    records.push_back(MakeRecord(data));
                }
            }
            outCursor = SCMakeRef<SqliteRecordCursor>(std::move(records));
            return SC_OK;
        }

        ErrorCode SqliteTable::FindRecords(const SCQueryCondition& condition, SCRecordCursorPtr& outCursor)
        {
            QueryPlan legacyPlan;
            const ErrorCode bridgeRc = SCQueryBridge::BuildPlanFromLegacyFindRecords(name_, condition, &legacyPlan);
            if (Failed(bridgeRc))
            {
                return bridgeRc;
            }

            auto planner = CreateDefaultQueryPlanner();
            if (!planner)
            {
                return SC_E_NOTIMPL;
            }

            QueryPlan executablePlan;
            const ErrorCode planRc = planner->BuildPlan(legacyPlan.target,
                                                        legacyPlan.conditionGroups,
                                                        legacyPlan.conditionGroupLogic,
                                                        legacyPlan.orderBy,
                                                        legacyPlan.page,
                                                        legacyPlan.hints,
                                                        legacyPlan.constraints,
                                                        &executablePlan);
            if (Failed(planRc))
            {
                return planRc;
            }

            QueryExecutionContext context;
            context.backendKind = QueryBackendKind::SQLite;
            context.database = db_;
            context.backendHandle = db_;
            context.resultCursor = &outCursor;

            QueryExecutionResult executionResult;
            return ExecuteQueryPlan(executablePlan, context, &executionResult);
        }

        SqliteDatabase::SqliteDatabase(const std::wstring& path) : SqliteDatabase(path, false)
        {
        }

        SqliteDatabase::SqliteDatabase(const std::wstring& path, bool readOnly)
            : path_(path),
              db_(path, readOnly),
              openMode_(readOnly ? SCDatabaseOpenMode::ReadOnly : SCDatabaseOpenMode::Normal),
              readOnly_(readOnly)
        {
            const bool hasMetadataTable = HasTable(L"metadata");
            if (!hasMetadataTable)
            {
                if (readOnly_)
                {
                    throw std::runtime_error("metadata table is missing in read-only open");
                }

                InitializeSchema();
            }

            LoadMetadata();
            LoadTables();

            if (!readOnly_)
            {
                if (schemaVersion_ <= 0)
                {
                    schemaVersion_ = GetLatestSupportedSchemaVersion();
                    SaveMetadata();
                }
            }

            LoadJournalStacks();
        }

        SqliteDatabase::SqliteDatabase(const std::wstring& path, const SCOpenDatabaseOptions& options)
            : path_(path),
              db_(path, options.openMode == SCDatabaseOpenMode::ReadOnly),
              openMode_(options.openMode),
              readOnly_(options.openMode == SCDatabaseOpenMode::ReadOnly)
        {
            const bool hasMetadataTable = HasTable(L"metadata");
            if (!hasMetadataTable)
            {
                if (readOnly_)
                {
                    throw std::runtime_error("metadata table is missing in read-only open");
                }

                InitializeSchema();
            }

            LoadMetadata();
            LoadTables();

            if (!readOnly_ && schemaVersion_ <= 0)
            {
                schemaVersion_ = GetLatestSupportedSchemaVersion();
                SaveMetadata();
            }

            LoadJournalStacks();
        }

        SqliteDatabase::~SqliteDatabase()
        {
            try
            {
                if (!readOnly_ && persistCleanShutdownOnDestroy_)
                {
                    SetCleanShutdownFlag(true);
                }
            } catch (...)
            {
            }
        }

        void SqliteDatabase::InitializeSchema()
        {
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, "
                "value TEXT NOT NULL);");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS startup_diagnostics ("
                "diag_id INTEGER PRIMARY KEY AUTOINCREMENT, severity INTEGER "
                "NOT NULL, category TEXT NOT NULL, message TEXT NOT NULL);");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS tables (table_id INTEGER PRIMARY "
                "KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE);");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS schema_columns ("
                "table_id INTEGER NOT NULL, column_name TEXT NOT NULL, "
                "display_name TEXT NOT NULL, value_kind INTEGER NOT NULL,"
                "column_kind INTEGER NOT NULL, nullable_flag INTEGER NOT NULL, "
                "editable_flag INTEGER NOT NULL,"
                "user_defined_flag INTEGER NOT NULL, indexed_flag INTEGER NOT "
                "NULL, participates_in_calc_flag INTEGER NOT NULL,"
                "unit TEXT NOT NULL, reference_table TEXT NOT NULL, "
                "reference_storage_column TEXT NOT NULL, "
                "reference_display_column TEXT NOT NULL, "
                "default_kind INTEGER NOT NULL, default_int64 INTEGER,"
                "default_double REAL, default_bool INTEGER, default_text TEXT, "
                "default_blob BLOB, "
                "PRIMARY KEY(table_id, column_name));");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS records ("
                "table_id INTEGER NOT NULL, record_id INTEGER NOT NULL, state "
                "INTEGER NOT NULL, last_modified_version INTEGER NOT NULL,"
                "PRIMARY KEY(table_id, record_id));");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS field_values ("
                "table_id INTEGER NOT NULL, record_id INTEGER NOT NULL, "
                "column_name TEXT NOT NULL, value_kind INTEGER NOT NULL,"
                "int64_value INTEGER, double_value REAL, bool_value INTEGER, "
                "text_value TEXT, blob_value BLOB,"
                "PRIMARY KEY(table_id, record_id, column_name));");
            db_.Execute(
                "CREATE INDEX IF NOT EXISTS idx_records_table_state ON "
                "records(table_id, state);");
            db_.Execute(
                "CREATE INDEX IF NOT EXISTS idx_field_values_lookup ON "
                "field_values(table_id, column_name, value_kind, int64_value, "
                "text_value);");
            db_.Execute(
                "CREATE INDEX IF NOT EXISTS idx_field_values_record ON "
                "field_values(table_id, record_id, column_name);");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS journal_transactions ("
                "tx_id INTEGER PRIMARY KEY AUTOINCREMENT, action_name TEXT NOT "
                "NULL, committed_version INTEGER NOT NULL,"
                " stack_kind INTEGER NOT NULL, stack_order INTEGER NOT NULL);");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS journal_entries ("
                "tx_id INTEGER NOT NULL, sequence_index INTEGER NOT NULL, op "
                "INTEGER NOT NULL, table_name TEXT NOT NULL,"
                "record_id INTEGER NOT NULL, field_name TEXT NOT NULL, "
                "old_kind INTEGER NOT NULL, old_int64 INTEGER,"
                "old_double REAL, old_bool INTEGER, old_text TEXT, old_blob BLOB, new_kind "
                "INTEGER NOT NULL, new_int64 INTEGER,"
                "new_double REAL, new_bool INTEGER, new_text TEXT, new_blob BLOB, old_deleted "
                "INTEGER NOT NULL, new_deleted INTEGER NOT NULL,"
                "PRIMARY KEY(tx_id, sequence_index));");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS journal_schema_entries ("
                "tx_id INTEGER NOT NULL, sequence_index INTEGER NOT NULL, op "
                "INTEGER NOT NULL, table_name TEXT NOT NULL, column_name TEXT "
                "NOT NULL, column_rowid INTEGER NOT NULL, old_display_name TEXT, old_value_kind INTEGER, "
                "old_column_kind INTEGER, old_nullable INTEGER, old_editable "
                "INTEGER, old_user_defined INTEGER, old_indexed INTEGER, "
                "old_participates_in_calc INTEGER, old_unit TEXT, "
                "old_reference_table TEXT, old_reference_storage_column TEXT, "
                "old_reference_display_column TEXT, old_default_kind INTEGER, "
                "old_default_int64 INTEGER, old_default_double REAL, "
                "old_default_bool INTEGER, old_default_text TEXT, "
                "old_default_blob BLOB, "
                "new_display_name TEXT, new_value_kind INTEGER, "
                "new_column_kind INTEGER, new_nullable INTEGER, "
                "new_editable INTEGER, new_user_defined INTEGER, "
                "new_indexed INTEGER, new_participates_in_calc INTEGER, "
                "new_unit TEXT, new_reference_table TEXT, "
                "new_reference_storage_column TEXT, new_reference_display_column TEXT, "
                "new_default_kind INTEGER, new_default_int64 INTEGER, new_default_double REAL, "
                "new_default_bool INTEGER, new_default_text TEXT, "
                "new_default_blob BLOB, "
                "PRIMARY KEY(tx_id, sequence_index));");
            EnsureSchemaMetadataTables();
        }

        void SqliteDatabase::LoadMetadata()
        {
            version_ = 0;
            baselineVersion_ = 0;
            nextRecordId_ = 1;
            schemaVersion_ = 0;
            cleanShutdown_ = true;
            bool hasBaselineVersion = false;

            SqliteStmt stmt = db_.Prepare("SELECT key, value FROM metadata;");
            bool hasRow = false;
            while (stmt.Step(&hasRow) == SC_OK && hasRow)
            {
                const std::wstring key = stmt.ColumnText(0);
                const std::wstring SCValue = stmt.ColumnText(1);
                if (key == L"version")
                {
                    version_ = static_cast<VersionId>(std::stoull(SCValue));
                } else if (key == L"baseline_version")
                {
                    baselineVersion_ = static_cast<VersionId>(std::stoull(SCValue));
                    hasBaselineVersion = true;
                } else if (key == L"schema_version")
                {
                    schemaVersion_ = static_cast<std::int32_t>(std::stoi(SCValue));
                } else if (key == L"clean_shutdown")
                {
                    cleanShutdown_ = (SCValue == L"1");
                } else if (key == L"next_record_id")
                {
                    nextRecordId_ = static_cast<RecordId>(std::stoll(SCValue));
                }
            }

            if (!hasBaselineVersion)
            {
                baselineVersion_ = version_;
            }
        }

        void SqliteDatabase::SaveMetadata(VersionId version, VersionId baselineVersion)
        {
            SaveMetadataKey(L"version", std::to_wstring(version));
            SaveMetadataKey(L"baseline_version", std::to_wstring(baselineVersion));
            SaveMetadataKey(L"next_record_id", std::to_wstring(nextRecordId_));
            SaveMetadataKey(L"schema_version", std::to_wstring(schemaVersion_));
            SaveMetadataKey(L"clean_shutdown", cleanShutdown_ ? L"1" : L"0");
        }

        void SqliteDatabase::SaveMetadata(VersionId version)
        {
            SaveMetadata(version, baselineVersion_);
        }

        void SqliteDatabase::SaveMetadata()
        {
            SaveMetadata(version_);
        }

        void SqliteDatabase::SaveMetadataKey(const wchar_t* key, const std::wstring& value)
        {
            SqliteStmt stmt = db_.Prepare(
                "INSERT INTO metadata(key, value) VALUES(?, ?)"
                " ON CONFLICT(key) DO UPDATE SET value=excluded.value;");
            stmt.BindText(1, key != nullptr ? key : L"");
            stmt.BindText(2, value);
            stmt.Step();
        }

        bool SqliteDatabase::HasTable(const wchar_t* name)
        {
            if (name == nullptr || *name == L'\0')
            {
                return false;
            }

            SqliteStmt stmt = db_.Prepare(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? "
                "LIMIT 1;");
            stmt.BindText(1, name);
            bool hasRow = false;
            return stmt.Step(&hasRow) == SC_OK && hasRow;
        }

        bool HasTableColumnRaw(sqlite3* db, const char* tableName, const char* columnName)
        {
            if (db == nullptr || tableName == nullptr || columnName == nullptr)
            {
                return false;
            }

            std::string sql = std::string("PRAGMA table_info(") + tableName + ");";
            sqlite3_stmt* stmt = nullptr;
            if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
            {
                if (stmt != nullptr)
                {
                    sqlite3_finalize(stmt);
                }
                return false;
            }

            bool hasColumn = false;
            while (sqlite3_step(stmt) == SQLITE_ROW)
            {
                const auto* name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
                if (name != nullptr && std::strcmp(name, columnName) == 0)
                {
                    hasColumn = true;
                    break;
                }
            }

            sqlite3_finalize(stmt);
            return hasColumn;
        }

        void SqliteDatabase::EnsureSchemaMetadataTables()
        {
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS schema_tables ("
                "table_id INTEGER PRIMARY KEY, description TEXT NOT NULL "
                "DEFAULT '');");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS schema_constraints ("
                "constraint_id INTEGER PRIMARY KEY AUTOINCREMENT, table_id "
                "INTEGER NOT NULL, kind INTEGER NOT NULL, name TEXT NOT NULL, "
                "source_kind INTEGER NOT NULL, referenced_table TEXT NOT NULL, "
                "on_delete_action INTEGER NOT NULL DEFAULT 0, "
                "on_update_action INTEGER NOT NULL DEFAULT 0, "
                "check_expression TEXT NOT NULL DEFAULT '');");
            db_.Execute(
                "CREATE INDEX IF NOT EXISTS idx_schema_constraints_table ON "
                "schema_constraints(table_id);");
            db_.Execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_schema_constraints_table_name ON "
                "schema_constraints(table_id, name);");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS schema_constraint_columns ("
                "constraint_id INTEGER NOT NULL, column_ordinal INTEGER NOT "
                "NULL, column_name TEXT NOT NULL, referenced_column_name TEXT "
                "NOT NULL DEFAULT '', PRIMARY KEY(constraint_id, "
                "column_ordinal));");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS schema_indexes ("
                "index_id INTEGER PRIMARY KEY AUTOINCREMENT, table_id INTEGER "
                "NOT NULL, name TEXT NOT NULL, source_kind INTEGER NOT NULL);");
            db_.Execute(
                "CREATE INDEX IF NOT EXISTS idx_schema_indexes_table ON "
                "schema_indexes(table_id);");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS schema_index_columns ("
                "index_id INTEGER NOT NULL, column_ordinal INTEGER NOT NULL, "
                "column_name TEXT NOT NULL, descending_flag INTEGER NOT NULL, "
                "PRIMARY KEY(index_id, column_ordinal));");
            InitializeQueryIndexStorage();
        }

        void SqliteDatabase::LoadTables()
        {
            const bool supportsBinaryStorage = schemaVersion_ >= 4;
            const bool supportsRelationReferenceColumns = schemaVersion_ >= 6;
            SqliteStmt tablesStmt = db_.Prepare("SELECT table_id, name FROM tables ORDER BY name;");
            bool hasRow = false;
            while (tablesStmt.Step(&hasRow) == SC_OK && hasRow)
            {
                const std::int64_t tableRowId = tablesStmt.ColumnInt64(0);
                const std::wstring tableName = tablesStmt.ColumnText(1);
                SCTablePtr table = SCMakeRef<SqliteTable>(this, tableName, tableRowId);
                tables_.emplace(tableName, table);
                auto* sqliteTable = static_cast<SqliteTable*>(table.Get());

                SqliteStmt columnsStmt =
                    db_.Prepare(
                        supportsBinaryStorage
                            ? (supportsRelationReferenceColumns
                                   ? "SELECT rowid, column_name, display_name, "
                                     "value_kind, column_kind, nullable_flag, "
                                     "editable_flag, user_defined_flag, indexed_flag, "
                                     "participates_in_calc_flag, unit, reference_table, "
                                     "reference_storage_column, reference_display_column, "
                                     "default_kind, default_int64, default_double, "
                                     "default_bool, default_text, default_blob FROM "
                                     "schema_columns WHERE table_id = ? ORDER BY rowid;"
                                   : "SELECT rowid, column_name, display_name, "
                                     "value_kind, column_kind, nullable_flag, "
                                     "editable_flag, user_defined_flag, indexed_flag, "
                                     "participates_in_calc_flag, unit, reference_table, "
                                     "'' AS reference_storage_column, '' AS reference_display_column, "
                                     "default_kind, default_int64, default_double, "
                                     "default_bool, default_text, default_blob FROM "
                                     "schema_columns WHERE table_id = ? ORDER BY rowid;")
                            : (supportsRelationReferenceColumns
                                   ? "SELECT rowid, column_name, display_name, "
                                     "value_kind, column_kind, nullable_flag, "
                                     "editable_flag, user_defined_flag, indexed_flag, "
                                     "participates_in_calc_flag, unit, reference_table, "
                                     "reference_storage_column, reference_display_column, "
                                     "default_kind, default_int64, default_double, "
                                     "default_bool, default_text FROM schema_columns "
                                     "WHERE table_id = ? ORDER BY rowid;"
                                   : "SELECT rowid, column_name, display_name, "
                                     "value_kind, column_kind, nullable_flag, "
                                     "editable_flag, user_defined_flag, indexed_flag, "
                                     "participates_in_calc_flag, unit, reference_table, "
                                     "'' AS reference_storage_column, '' AS reference_display_column, "
                                     "default_kind, default_int64, default_double, "
                                     "default_bool, default_text FROM schema_columns "
                                     "WHERE table_id = ? ORDER BY rowid;"));
                columnsStmt.BindInt64(1, tableRowId);
                bool hasColumn = false;
                while (columnsStmt.Step(&hasColumn) == SC_OK && hasColumn)
                {
                    SCColumnDef def;
                    const std::int64_t rowId = columnsStmt.ColumnInt64(0);
                    def.name = columnsStmt.ColumnText(1);
                    def.displayName = columnsStmt.ColumnText(2);
                    def.valueKind = FromSqliteValueKind(columnsStmt.ColumnInt(3));
                    def.columnKind = FromSqliteColumnKind(columnsStmt.ColumnInt(4));
                    def.nullable = columnsStmt.ColumnBool(5);
                    def.editable = columnsStmt.ColumnBool(6);
                    def.userDefined = columnsStmt.ColumnBool(7);
                    def.indexed = columnsStmt.ColumnBool(8);
                    def.participatesInCalc = columnsStmt.ColumnBool(9);
                    def.unit = columnsStmt.ColumnText(10);
                    def.referenceTable = columnsStmt.ColumnText(11);
                    def.referenceStorageColumn = columnsStmt.ColumnText(12);
                    def.referenceDisplayColumn = columnsStmt.ColumnText(13);
                    def.defaultValue = supportsBinaryStorage
                                           ? ReadValueFromStorage(columnsStmt, 14, 15, 16, 17, 18, 19)
                                           : ReadValueFromStorage(columnsStmt, 14, 15, 16, 17, 18, 18);
                    sqliteTable->Schema()->LoadColumn(def, rowId);
                }
                const ErrorCode metadataRc = LoadSchemaMetadata(sqliteTable);
                if (Failed(metadataRc))
                {
                    throw std::runtime_error("failed to load schema metadata");
                }

                SqliteStmt recordsStmt = db_.Prepare(
                    "SELECT record_id, state, last_modified_version FROM "
                    "records WHERE table_id = ?;");
                recordsStmt.BindInt64(1, tableRowId);
                bool hasRecord = false;
                while (recordsStmt.Step(&hasRecord) == SC_OK && hasRecord)
                {
                    auto record = std::make_shared<SqliteRecordData>(recordsStmt.ColumnInt64(0));
                    record->state = FromSqliteRecordState(recordsStmt.ColumnInt(1));
                    record->lastModifiedVersion = static_cast<VersionId>(recordsStmt.ColumnInt64(2));
                    sqliteTable->Records().emplace(record->id, record);
                    if (record->id >= nextRecordId_)
                    {
                        nextRecordId_ = record->id + 1;
                    }
                }

                SqliteStmt valuesStmt =
                    db_.Prepare(supportsBinaryStorage ? "SELECT record_id, column_name, value_kind, "
                                                        "int64_value, double_value, bool_value, text_value, "
                                                        "blob_value FROM field_values WHERE table_id = ?;"
                                                      : "SELECT record_id, column_name, value_kind, "
                                                        "int64_value, double_value, bool_value, text_value "
                                                        "FROM field_values WHERE table_id = ?;");
                valuesStmt.BindInt64(1, tableRowId);
                bool hasValue = false;
                while (valuesStmt.Step(&hasValue) == SC_OK && hasValue)
                {
                    auto record = sqliteTable->FindRecordData(valuesStmt.ColumnInt64(0));
                    if (!record)
                    {
                        continue;
                    }
                    record->values[valuesStmt.ColumnText(1)] = supportsBinaryStorage
                                                                   ? ReadValueFromStorage(valuesStmt, 2, 3, 4, 5, 6, 7)
                                                                   : ReadValueFromStorage(valuesStmt, 2, 3, 4, 5, 6, 6);
                }
            }
        }

        ErrorCode SqliteDatabase::LoadSchemaMetadata(SqliteTable* table)
        {
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }

            auto* schema = table->Schema();
            if (schema == nullptr)
            {
                return SC_E_FAIL;
            }

            const std::int64_t tableRowId = table->TableRowId();

            if (HasTable(L"schema_tables"))
            {
                SqliteStmt tableMetaStmt = db_.Prepare("SELECT description FROM schema_tables WHERE table_id = ?;");
                tableMetaStmt.BindInt64(1, tableRowId);
                bool hasMetaRow = false;
                if (tableMetaStmt.Step(&hasMetaRow) == SC_OK && hasMetaRow)
                {
                    schema->LoadTableDescription(tableMetaStmt.ColumnText(0));
                }
            }

            if (HasTable(L"schema_constraints") && HasTable(L"schema_constraint_columns"))
            {
                const bool supportsForeignKeyActions = schemaVersion_ >= 7;
                SqliteStmt constraintsStmt = db_.Prepare(
                    supportsForeignKeyActions
                        ? "SELECT constraint_id, kind, name, source_kind, "
                          "referenced_table, on_delete_action, on_update_action, "
                          "check_expression FROM schema_constraints WHERE table_id = ? ORDER BY "
                          "constraint_id;"
                        : "SELECT constraint_id, kind, name, source_kind, "
                          "referenced_table, 0 AS on_delete_action, 0 AS on_update_action, "
                          "check_expression FROM schema_constraints WHERE table_id = ? ORDER BY "
                          "constraint_id;");
                constraintsStmt.BindInt64(1, tableRowId);
                bool hasConstraint = false;
                while (constraintsStmt.Step(&hasConstraint) == SC_OK && hasConstraint)
                {
                    SCConstraintDef constraint;
                    const std::int64_t constraintId = constraintsStmt.ColumnInt64(0);
                    constraint.kind = FromSqliteConstraintKind(constraintsStmt.ColumnInt(1));
                    constraint.name = constraintsStmt.ColumnText(2);
                    constraint.sourceKind = FromSqliteSchemaSourceKind(constraintsStmt.ColumnInt(3));
                    constraint.referencedTable = constraintsStmt.ColumnText(4);
                    constraint.onDelete = FromSqliteForeignKeyAction(constraintsStmt.ColumnInt(5));
                    constraint.onUpdate = FromSqliteForeignKeyAction(constraintsStmt.ColumnInt(6));
                    constraint.checkExpression = constraintsStmt.ColumnText(7);

                    SqliteStmt constraintColumnsStmt = db_.Prepare(
                        "SELECT column_name, referenced_column_name FROM "
                        "schema_constraint_columns WHERE constraint_id = ? "
                        "ORDER BY column_ordinal;");
                    constraintColumnsStmt.BindInt64(1, constraintId);
                    bool hasConstraintColumn = false;
                    while (constraintColumnsStmt.Step(&hasConstraintColumn) == SC_OK && hasConstraintColumn)
                    {
                        constraint.columns.push_back(constraintColumnsStmt.ColumnText(0));
                        const std::wstring referencedColumn = constraintColumnsStmt.ColumnText(1);
                        if (!referencedColumn.empty())
                        {
                            constraint.referencedColumns.push_back(referencedColumn);
                        }
                    }

                    schema->LoadConstraint(constraint, constraintId);
                }
            }

            if (HasTable(L"schema_indexes") && HasTable(L"schema_index_columns"))
            {
                SqliteStmt indexesStmt = db_.Prepare(
                    "SELECT index_id, name, source_kind FROM schema_indexes "
                    "WHERE table_id = ? ORDER BY index_id;");
                indexesStmt.BindInt64(1, tableRowId);
                bool hasIndex = false;
                while (indexesStmt.Step(&hasIndex) == SC_OK && hasIndex)
                {
                    SCIndexDef index;
                    const std::int64_t indexId = indexesStmt.ColumnInt64(0);
                    index.name = indexesStmt.ColumnText(1);
                    index.sourceKind = FromSqliteSchemaSourceKind(indexesStmt.ColumnInt(2));

                    SqliteStmt indexColumnsStmt = db_.Prepare(
                        "SELECT column_name, descending_flag FROM "
                        "schema_index_columns WHERE index_id = ? ORDER BY "
                        "column_ordinal;");
                    indexColumnsStmt.BindInt64(1, indexId);
                    bool hasIndexColumn = false;
                    while (indexColumnsStmt.Step(&hasIndexColumn) == SC_OK && hasIndexColumn)
                    {
                        index.columns.push_back(
                            SCIndexColumnDef{indexColumnsStmt.ColumnText(0), indexColumnsStmt.ColumnBool(1)});
                    }

                    schema->LoadIndex(index, indexId);
                    if (HasTable(L"query_indexes") && IsCompositeIndexExplicit(index))
                    {
                        queryIndexRowIdsByTableAndName_[BuildQueryIndexStorageKey(tableRowId, index.name)] = indexId;
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::BackfillSchemaMetadataV3()
        {
            EnsureSchemaMetadataTables();

            for (const auto& [_, tableRef] : tables_)
            {
                auto* table = static_cast<SqliteTable*>(tableRef.Get());
                if (table == nullptr)
                {
                    continue;
                }

                SCSchemaPtr schema;
                const ErrorCode schemaRc = tableRef->GetSchema(schema);
                if (Failed(schemaRc) || !schema)
                {
                    return Failed(schemaRc) ? schemaRc : SC_E_FAIL;
                }

                SCTableSchemaSnapshot snapshot;
                const ErrorCode snapshotRc = schema->GetSchemaSnapshot(&snapshot);
                if (Failed(snapshotRc))
                {
                    return snapshotRc;
                }

                const auto toUpperCopy = [](std::wstring text) {
                    std::transform(text.begin(), text.end(), text.begin(), [](wchar_t ch) {
                        return static_cast<wchar_t>(std::towupper(ch));
                    });
                    return text;
                };

                if (snapshot.constraints.empty())
                {
                    const auto idColumn = std::find_if(
                        snapshot.columns.begin(), snapshot.columns.end(), [&toUpperCopy](const SCColumnDef& column) {
                            return toUpperCopy(column.name) == L"ID";
                        });
                    if (idColumn != snapshot.columns.end())
                    {
                        SCConstraintDef constraint;
                        constraint.kind = SCConstraintKind::PrimaryKey;
                        constraint.name = L"pk_legacy";
                        constraint.columns.push_back(idColumn->name);
                        constraint.sourceKind = SCSchemaSourceKind::MigratedConvention;
                        snapshot.constraints.push_back(std::move(constraint));
                    }
                }

                if (snapshot.indexes.empty())
                {
                    for (const SCColumnDef& column : snapshot.columns)
                    {
                        if (!column.indexed)
                        {
                            continue;
                        }

                        SCIndexDef index;
                        index.name = L"idx_legacy_" + column.name;
                        index.sourceKind = SCSchemaSourceKind::LegacyHint;
                        index.columns.push_back(SCIndexColumnDef{column.name, false});
                        snapshot.indexes.push_back(std::move(index));
                    }
                }

                {
                    SqliteStmt stmt = db_.Prepare(
                        "INSERT INTO schema_tables(table_id, description) "
                        "VALUES(?, ?) ON CONFLICT(table_id) DO UPDATE SET "
                        "description=excluded.description;");
                    stmt.BindInt64(1, table->TableRowId());
                    stmt.BindText(2, snapshot.table.description);
                    const ErrorCode rc = stmt.Step();
                    if (Failed(rc))
                    {
                        return rc;
                    }
                }

                {
                    SqliteStmt stmt = db_.Prepare(
                        "DELETE FROM schema_constraint_columns WHERE "
                        "constraint_id IN (SELECT constraint_id FROM "
                        "schema_constraints WHERE table_id = ?);");
                    stmt.BindInt64(1, table->TableRowId());
                    const ErrorCode rc = stmt.Step();
                    if (Failed(rc))
                    {
                        return rc;
                    }
                }

                {
                    SqliteStmt stmt = db_.Prepare("DELETE FROM schema_constraints WHERE table_id = ?;");
                    stmt.BindInt64(1, table->TableRowId());
                    const ErrorCode rc = stmt.Step();
                    if (Failed(rc))
                    {
                        return rc;
                    }
                }

                {
                    SqliteStmt stmt = db_.Prepare(
                        "DELETE FROM schema_index_columns WHERE index_id IN "
                        "(SELECT index_id FROM schema_indexes WHERE table_id "
                        "= ?);");
                    stmt.BindInt64(1, table->TableRowId());
                    const ErrorCode rc = stmt.Step();
                    if (Failed(rc))
                    {
                        return rc;
                    }
                }

                {
                    SqliteStmt stmt = db_.Prepare("DELETE FROM schema_indexes WHERE table_id = ?;");
                    stmt.BindInt64(1, table->TableRowId());
                    const ErrorCode rc = stmt.Step();
                    if (Failed(rc))
                    {
                        return rc;
                    }
                }

                for (const SCConstraintDef& constraint : snapshot.constraints)
                {
                    SqliteStmt stmt = db_.Prepare(
                        "INSERT INTO schema_constraints(table_id, kind, "
                        "name, source_kind, referenced_table, "
                        "on_delete_action, on_update_action, "
                        "check_expression) VALUES(?, ?, ?, ?, ?, ?, ?, ?);");
                    stmt.BindInt64(1, table->TableRowId());
                    stmt.BindInt(2, ToSqliteConstraintKind(constraint.kind));
                    stmt.BindText(3, constraint.name);
                    stmt.BindInt(4, ToSqliteSchemaSourceKind(constraint.sourceKind));
                    stmt.BindText(5, constraint.referencedTable);
                    stmt.BindInt(6, ToSqliteForeignKeyAction(constraint.onDelete));
                    stmt.BindInt(7, ToSqliteForeignKeyAction(constraint.onUpdate));
                    stmt.BindText(8, constraint.checkExpression);
                    const ErrorCode rc = stmt.Step();
                    if (Failed(rc))
                    {
                        return rc;
                    }

                    const std::int64_t constraintId = db_.LastInsertRowId();
                    for (std::size_t index = 0; index < constraint.columns.size(); ++index)
                    {
                        SqliteStmt columnStmt = db_.Prepare(
                            "INSERT INTO schema_constraint_columns("
                            "constraint_id, column_ordinal, column_name, "
                            "referenced_column_name) VALUES(?, ?, ?, ?);");
                        columnStmt.BindInt64(1, constraintId);
                        columnStmt.BindInt64(2, static_cast<std::int64_t>(index));
                        columnStmt.BindText(3, constraint.columns[index]);
                        const std::wstring referencedColumn = index < constraint.referencedColumns.size()
                                                                  ? constraint.referencedColumns[index]
                                                                  : std::wstring{};
                        columnStmt.BindText(4, referencedColumn);
                        const ErrorCode columnRc = columnStmt.Step();
                        if (Failed(columnRc))
                        {
                            return columnRc;
                        }
                    }
                }

                for (const SCIndexDef& index : snapshot.indexes)
                {
                    SqliteStmt stmt = db_.Prepare(
                        "INSERT INTO schema_indexes(table_id, name, "
                        "source_kind) VALUES(?, ?, ?);");
                    stmt.BindInt64(1, table->TableRowId());
                    stmt.BindText(2, index.name);
                    stmt.BindInt(3, ToSqliteSchemaSourceKind(index.sourceKind));
                    const ErrorCode rc = stmt.Step();
                    if (Failed(rc))
                    {
                        return rc;
                    }

                    const std::int64_t indexId = db_.LastInsertRowId();
                    for (std::size_t columnIndex = 0; columnIndex < index.columns.size(); ++columnIndex)
                    {
                        SqliteStmt columnStmt = db_.Prepare(
                            "INSERT INTO schema_index_columns(index_id, "
                            "column_ordinal, column_name, descending_flag) "
                            "VALUES(?, ?, ?, ?);");
                        columnStmt.BindInt64(1, indexId);
                        columnStmt.BindInt64(2, static_cast<std::int64_t>(columnIndex));
                        columnStmt.BindText(3, index.columns[columnIndex].columnName);
                        columnStmt.BindInt(4, index.columns[columnIndex].descending ? 1 : 0);
                        const ErrorCode columnRc = columnStmt.Step();
                        if (Failed(columnRc))
                        {
                            return columnRc;
                        }
                    }
                }

                std::wstringstream message;
                message << L"Backfilled schema metadata for table " << snapshot.table.name << L" ("
                        << snapshot.constraints.size() << L" constraints, " << snapshot.indexes.size() << L" indexes).";
                LogStartupDiagnostic(SCDiagnosticSeverity::Info, L"upgrade", message.str());
            }

            return SC_OK;
        }

        void SqliteDatabase::LoadJournalStacks()
        {
            if (!HasTable(L"journal_transactions") || !HasTable(L"journal_entries") ||
                !HasTable(L"journal_schema_entries"))
            {
                return;
            }

            const bool supportsBinaryStorage = schemaVersion_ >= 4;
            const bool supportsExtendedSchemaJournalColumns =
                HasTableColumn("journal_schema_entries", "old_reference_storage_column") &&
                HasTableColumn("journal_schema_entries", "old_reference_display_column") &&
                HasTableColumn("journal_schema_entries", "new_reference_storage_column") &&
                HasTableColumn("journal_schema_entries", "new_reference_display_column");
            constexpr int kJournalSchemaSequenceIndex = 0;
            constexpr int kJournalSchemaOpIndex = 1;
            constexpr int kJournalSchemaTableNameIndex = 2;
            constexpr int kJournalSchemaColumnNameIndex = 3;
            constexpr int kJournalSchemaColumnRowIdIndex = 4;
            constexpr int kJournalSchemaOldDisplayNameIndex = 5;
            constexpr int kJournalSchemaOldValueKindIndex = 6;
            constexpr int kJournalSchemaOldColumnKindIndex = 7;
            constexpr int kJournalSchemaOldNullableIndex = 8;
            constexpr int kJournalSchemaOldEditableIndex = 9;
            constexpr int kJournalSchemaOldUserDefinedIndex = 10;
            constexpr int kJournalSchemaOldIndexedIndex = 11;
            constexpr int kJournalSchemaOldParticipatesIndex = 12;
            constexpr int kJournalSchemaOldUnitIndex = 13;
            constexpr int kJournalSchemaOldReferenceTableIndex = 14;
            constexpr int kJournalSchemaOldReferenceStorageColumnIndex = 15;
            constexpr int kJournalSchemaOldReferenceDisplayColumnIndex = 16;
            constexpr int kJournalSchemaOldDefaultKindIndex = 17;
            constexpr int kJournalSchemaOldDefaultInt64Index = 18;
            constexpr int kJournalSchemaOldDefaultDoubleIndex = 19;
            constexpr int kJournalSchemaOldDefaultBoolIndex = 20;
            constexpr int kJournalSchemaOldDefaultTextIndex = 21;
            constexpr int kJournalSchemaOldDefaultBlobIndex = 22;
            constexpr int kJournalSchemaNewDisplayNameIndex = 23;
            constexpr int kJournalSchemaNewValueKindIndex = 24;
            constexpr int kJournalSchemaNewColumnKindIndex = 25;
            constexpr int kJournalSchemaNewNullableIndex = 26;
            constexpr int kJournalSchemaNewEditableIndex = 27;
            constexpr int kJournalSchemaNewUserDefinedIndex = 28;
            constexpr int kJournalSchemaNewIndexedIndex = 29;
            constexpr int kJournalSchemaNewParticipatesIndex = 30;
            constexpr int kJournalSchemaNewUnitIndex = 31;
            constexpr int kJournalSchemaNewReferenceTableIndex = 32;
            constexpr int kJournalSchemaNewReferenceStorageColumnIndex = 33;
            constexpr int kJournalSchemaNewReferenceDisplayColumnIndex = 34;
            constexpr int kJournalSchemaNewDefaultKindIndex = 35;
            constexpr int kJournalSchemaNewDefaultInt64Index = 36;
            constexpr int kJournalSchemaNewDefaultDoubleIndex = 37;
            constexpr int kJournalSchemaNewDefaultBoolIndex = 38;
            constexpr int kJournalSchemaNewDefaultTextIndex = 39;
            constexpr int kJournalSchemaNewDefaultBlobIndex = 40;
            SqliteStmt txStmt = db_.Prepare(
                "SELECT tx_id, action_name, committed_version, stack_kind FROM "
                "journal_transactions ORDER BY stack_kind, stack_order;");
            bool hasTx = false;
            while (txStmt.Step(&hasTx) == SC_OK && hasTx)
            {
                SqlitePersistedJournalTransaction persisted;
                persisted.txId = txStmt.ColumnInt64(0);
                persisted.tx.actionName = txStmt.ColumnText(1);
                persisted.tx.commitId = static_cast<CommitId>(persisted.txId);
                persisted.tx.committedVersion = static_cast<VersionId>(txStmt.ColumnInt64(2));
                const int stackKind = txStmt.ColumnInt(3);

                std::vector<SqlitePersistedJournalEntry> entries;

                SqliteStmt entryStmt =
                    db_.Prepare(supportsBinaryStorage ? "SELECT sequence_index, op, table_name, record_id, "
                                                        "field_name, old_kind, old_int64, old_double, "
                                                        "old_bool, old_text, old_blob, new_kind, "
                                                        "new_int64, new_double, new_bool, new_text, "
                                                        "new_blob, old_deleted, new_deleted FROM "
                                                        "journal_entries WHERE tx_id = ?;"
                                                      : "SELECT sequence_index, op, table_name, record_id, "
                                                        "field_name, old_kind, old_int64, old_double, "
                                                        "old_bool, old_text, new_kind, new_int64, "
                                                        "new_double, new_bool, new_text, old_deleted, "
                                                        "new_deleted FROM journal_entries WHERE tx_id = ?;");
                entryStmt.BindInt64(1, persisted.txId);
                bool hasEntry = false;
                while (entryStmt.Step(&hasEntry) == SC_OK && hasEntry)
                {
                    SqlitePersistedJournalEntry persistedEntry;
                    persistedEntry.sequenceIndex = entryStmt.ColumnInt(0);
                    JournalEntry entry;
                    entry.op = FromSqliteJournalOp(entryStmt.ColumnInt(1));
                    entry.tableName = entryStmt.ColumnText(2);
                    entry.recordId = entryStmt.ColumnInt64(3);
                    entry.fieldName = entryStmt.ColumnText(4);
                    entry.oldValue = supportsBinaryStorage ? ReadValueFromStorage(entryStmt, 5, 6, 7, 8, 9, 10)
                                                           : ReadValueFromStorage(entryStmt, 5, 6, 7, 8, 9, 9);
                    entry.newValue = supportsBinaryStorage ? ReadValueFromStorage(entryStmt, 11, 12, 13, 14, 15, 16)
                                                           : ReadValueFromStorage(entryStmt, 10, 11, 12, 13, 14, 14);
                    entry.oldDeleted = supportsBinaryStorage ? entryStmt.ColumnBool(17) : entryStmt.ColumnBool(15);
                    entry.newDeleted = supportsBinaryStorage ? entryStmt.ColumnBool(18) : entryStmt.ColumnBool(16);

                    switch (entry.op)
                    {
                        case JournalOp::AddConstraint:
                        case JournalOp::RemoveConstraint:
                            entry.constraintRowId = entry.recordId;
                            if (!entry.oldValue.IsNull())
                            {
                                std::wstring payload;
                                entry.oldValue.AsStringCopy(&payload);
                                if (!DeserializeConstraintDef(payload, &entry.oldConstraint))
                                {
                                    throw std::runtime_error("failed to deserialize constraint journal entry");
                                }
                            }
                            if (!entry.newValue.IsNull())
                            {
                                std::wstring payload;
                                entry.newValue.AsStringCopy(&payload);
                                if (!DeserializeConstraintDef(payload, &entry.newConstraint))
                                {
                                    throw std::runtime_error("failed to deserialize constraint journal entry");
                                }
                            }
                            entry.oldValue = SCValue::Null();
                            entry.newValue = SCValue::Null();
                            entry.recordId = 0;
                            break;
                        case JournalOp::AddIndex:
                        case JournalOp::RemoveIndex:
                            entry.indexRowId = entry.recordId;
                            if (!entry.oldValue.IsNull())
                            {
                                std::wstring payload;
                                entry.oldValue.AsStringCopy(&payload);
                                if (!DeserializeIndexDef(payload, &entry.oldIndex))
                                {
                                    throw std::runtime_error("failed to deserialize index journal entry");
                                }
                            }
                            if (!entry.newValue.IsNull())
                            {
                                std::wstring payload;
                                entry.newValue.AsStringCopy(&payload);
                                if (!DeserializeIndexDef(payload, &entry.newIndex))
                                {
                                    throw std::runtime_error("failed to deserialize index journal entry");
                                }
                            }
                            entry.oldValue = SCValue::Null();
                            entry.newValue = SCValue::Null();
                            entry.recordId = 0;
                            break;
                        default:
                            break;
                    }
                    persistedEntry.entry = std::move(entry);
                    entries.push_back(std::move(persistedEntry));
                }

                SqliteStmt schemaStmt = db_.Prepare(
                    supportsBinaryStorage
                        ? (supportsExtendedSchemaJournalColumns
                               ? "SELECT sequence_index, op, table_name, column_name, "
                                 "column_rowid, old_display_name, old_value_kind, "
                                 "old_column_kind, old_nullable, old_editable, "
                                 "old_user_defined, old_indexed, "
                                 "old_participates_in_calc, old_unit, "
                                 "old_reference_table, old_reference_storage_column, "
                                 "old_reference_display_column, old_default_kind, "
                                 "old_default_int64, old_default_double, "
                                 "old_default_bool, old_default_text, "
                                 "old_default_blob, new_display_name, "
                                 "new_value_kind, new_column_kind, new_nullable, "
                                 "new_editable, new_user_defined, new_indexed, "
                                 "new_participates_in_calc, new_unit, "
                                 "new_reference_table, new_reference_storage_column, "
                                 "new_reference_display_column, new_default_kind, "
                                 "new_default_int64, new_default_double, "
                                 "new_default_bool, new_default_text, "
                                 "new_default_blob FROM journal_schema_entries WHERE tx_id = ?;"
                               : "SELECT sequence_index, op, table_name, column_name, "
                                 "column_rowid, old_display_name, old_value_kind, "
                                 "old_column_kind, old_nullable, old_editable, "
                                 "old_user_defined, old_indexed, "
                                 "old_participates_in_calc, old_unit, "
                                 "old_reference_table, '' AS old_reference_storage_column, "
                                 "'' AS old_reference_display_column, old_default_kind, "
                                 "old_default_int64, old_default_double, "
                                 "old_default_bool, old_default_text, "
                                 "old_default_blob, new_display_name, "
                                 "new_value_kind, new_column_kind, new_nullable, "
                                 "new_editable, new_user_defined, new_indexed, "
                                 "new_participates_in_calc, new_unit, "
                                 "new_reference_table, '' AS new_reference_storage_column, "
                                 "'' AS new_reference_display_column, new_default_kind, "
                                 "new_default_int64, new_default_double, "
                                 "new_default_bool, new_default_text, "
                                 "new_default_blob FROM journal_schema_entries WHERE tx_id = ?;")
                        : (supportsExtendedSchemaJournalColumns
                               ? "SELECT sequence_index, op, table_name, column_name, "
                                 "column_rowid, old_display_name, old_value_kind, "
                                 "old_column_kind, old_nullable, old_editable, "
                                 "old_user_defined, old_indexed, "
                                 "old_participates_in_calc, old_unit, "
                                 "old_reference_table, old_reference_storage_column, "
                                 "old_reference_display_column, old_default_kind, "
                                 "old_default_int64, old_default_double, "
                                 "old_default_bool, old_default_text, "
                                 "new_display_name, new_value_kind, new_column_kind, "
                                 "new_nullable, new_editable, new_user_defined, "
                                 "new_indexed, new_participates_in_calc, new_unit, "
                                 "new_reference_table, new_reference_storage_column, "
                                 "new_reference_display_column, new_default_kind, "
                                 "new_default_int64, new_default_double, "
                                 "new_default_bool, new_default_text FROM "
                                 "journal_schema_entries WHERE tx_id = ?;"
                               : "SELECT sequence_index, op, table_name, column_name, "
                                 "column_rowid, old_display_name, old_value_kind, "
                                 "old_column_kind, old_nullable, old_editable, "
                                 "old_user_defined, old_indexed, "
                                 "old_participates_in_calc, old_unit, "
                                 "old_reference_table, '' AS old_reference_storage_column, "
                                 "'' AS old_reference_display_column, old_default_kind, "
                                 "old_default_int64, old_default_double, "
                                 "old_default_bool, old_default_text, "
                                 "new_display_name, new_value_kind, new_column_kind, "
                                 "new_nullable, new_editable, new_user_defined, "
                                 "new_indexed, new_participates_in_calc, new_unit, "
                                 "new_reference_table, '' AS new_reference_storage_column, "
                                 "'' AS new_reference_display_column, new_default_kind, "
                                 "new_default_int64, new_default_double, "
                                 "new_default_bool, new_default_text FROM "
                                 "journal_schema_entries WHERE tx_id = ?;"));
                schemaStmt.BindInt64(1, persisted.txId);
                hasEntry = false;
                while (schemaStmt.Step(&hasEntry) == SC_OK && hasEntry)
                {
                    SqlitePersistedJournalEntry persistedEntry;
                    persistedEntry.sequenceIndex = schemaStmt.ColumnInt(kJournalSchemaSequenceIndex);
                    JournalEntry entry;
                    entry.op = FromSqliteJournalOp(schemaStmt.ColumnInt(kJournalSchemaOpIndex));
                    entry.tableName = schemaStmt.ColumnText(kJournalSchemaTableNameIndex);
                    entry.fieldName = schemaStmt.ColumnText(kJournalSchemaColumnNameIndex);
                    entry.columnRowId = schemaStmt.ColumnInt64(kJournalSchemaColumnRowIdIndex);
                    entry.oldColumn = supportsBinaryStorage
                                          ? ReadColumnDefFromStorage(
                                                schemaStmt,
                                                kJournalSchemaOldDisplayNameIndex,
                                                kJournalSchemaOldValueKindIndex,
                                                kJournalSchemaOldColumnKindIndex,
                                                kJournalSchemaOldNullableIndex,
                                                kJournalSchemaOldEditableIndex,
                                                kJournalSchemaOldUserDefinedIndex,
                                                kJournalSchemaOldIndexedIndex,
                                                kJournalSchemaOldParticipatesIndex,
                                                kJournalSchemaOldUnitIndex,
                                                kJournalSchemaOldReferenceTableIndex,
                                                kJournalSchemaOldReferenceStorageColumnIndex,
                                                kJournalSchemaOldReferenceDisplayColumnIndex,
                                                kJournalSchemaOldDefaultKindIndex,
                                                kJournalSchemaOldDefaultInt64Index,
                                                kJournalSchemaOldDefaultDoubleIndex,
                                                kJournalSchemaOldDefaultBoolIndex,
                                                kJournalSchemaOldDefaultTextIndex,
                                                kJournalSchemaOldDefaultBlobIndex)
                                          : ReadColumnDefFromStorage(
                                                schemaStmt,
                                                kJournalSchemaOldDisplayNameIndex,
                                                kJournalSchemaOldValueKindIndex,
                                                kJournalSchemaOldColumnKindIndex,
                                                kJournalSchemaOldNullableIndex,
                                                kJournalSchemaOldEditableIndex,
                                                kJournalSchemaOldUserDefinedIndex,
                                                kJournalSchemaOldIndexedIndex,
                                                kJournalSchemaOldParticipatesIndex,
                                                kJournalSchemaOldUnitIndex,
                                                kJournalSchemaOldReferenceTableIndex,
                                                kJournalSchemaOldReferenceStorageColumnIndex,
                                                kJournalSchemaOldReferenceDisplayColumnIndex,
                                                kJournalSchemaOldDefaultKindIndex,
                                                kJournalSchemaOldDefaultInt64Index,
                                                kJournalSchemaOldDefaultDoubleIndex,
                                                kJournalSchemaOldDefaultBoolIndex,
                                                kJournalSchemaOldDefaultTextIndex,
                                                kJournalSchemaOldDefaultTextIndex);
                    entry.newColumn =
                        supportsBinaryStorage
                            ? ReadColumnDefFromStorage(
                                  schemaStmt,
                                  kJournalSchemaNewDisplayNameIndex,
                                  kJournalSchemaNewValueKindIndex,
                                  kJournalSchemaNewColumnKindIndex,
                                  kJournalSchemaNewNullableIndex,
                                  kJournalSchemaNewEditableIndex,
                                  kJournalSchemaNewUserDefinedIndex,
                                  kJournalSchemaNewIndexedIndex,
                                  kJournalSchemaNewParticipatesIndex,
                                  kJournalSchemaNewUnitIndex,
                                  kJournalSchemaNewReferenceTableIndex,
                                  kJournalSchemaNewReferenceStorageColumnIndex,
                                  kJournalSchemaNewReferenceDisplayColumnIndex,
                                  kJournalSchemaNewDefaultKindIndex,
                                  kJournalSchemaNewDefaultInt64Index,
                                  kJournalSchemaNewDefaultDoubleIndex,
                                  kJournalSchemaNewDefaultBoolIndex,
                                  kJournalSchemaNewDefaultTextIndex,
                                  kJournalSchemaNewDefaultBlobIndex)
                            : ReadColumnDefFromStorage(
                                  schemaStmt,
                                  kJournalSchemaNewDisplayNameIndex,
                                  kJournalSchemaNewValueKindIndex,
                                  kJournalSchemaNewColumnKindIndex,
                                  kJournalSchemaNewNullableIndex,
                                  kJournalSchemaNewEditableIndex,
                                  kJournalSchemaNewUserDefinedIndex,
                                  kJournalSchemaNewIndexedIndex,
                                  kJournalSchemaNewParticipatesIndex,
                                  kJournalSchemaNewUnitIndex,
                                  kJournalSchemaNewReferenceTableIndex,
                                  kJournalSchemaNewReferenceStorageColumnIndex,
                                  kJournalSchemaNewReferenceDisplayColumnIndex,
                                  kJournalSchemaNewDefaultKindIndex,
                                  kJournalSchemaNewDefaultInt64Index,
                                  kJournalSchemaNewDefaultDoubleIndex,
                                  kJournalSchemaNewDefaultBoolIndex,
                                  kJournalSchemaNewDefaultTextIndex,
                                  kJournalSchemaNewDefaultTextIndex);
                    persistedEntry.entry = std::move(entry);
                    entries.push_back(std::move(persistedEntry));
                }

                std::sort(entries.begin(),
                          entries.end(),
                          [](const SqlitePersistedJournalEntry& left, const SqlitePersistedJournalEntry& right) {
                              return left.sequenceIndex < right.sequenceIndex;
                          });
                for (auto& entry : entries)
                {
                    persisted.tx.entries.push_back(std::move(entry.entry));
                }

                if (stackKind == kStackUndo)
                {
                    undoStack_.push_back(std::move(persisted));
                } else
                {
                    redoStack_.push_back(std::move(persisted));
                }
            }
        }

        ErrorCode SqliteDatabase::ExecuteUpgradePlan(const SCUpgradePlan& plan,
                                                     bool confirmed,
                                                     SCUpgradeResult* outResult)
        {
            if (outResult == nullptr)
            {
                return SC_E_POINTER;
            }

            SCUpgradeResult result;
            result.sourceVersion = schemaVersion_;
            result.targetVersion = plan.targetVersion;

            const auto finish = [&](ErrorCode rc) {
                *outResult = result;
                return rc;
            };

            if (readOnly_)
            {
                result.status = SCUpgradeStatus::Unsupported;
                result.failureReason = L"Database is opened read-only.";
                return finish(SC_E_READ_ONLY_DATABASE);
            }

            if (!cleanShutdown_)
            {
                result.status = SCUpgradeStatus::Unsupported;
                result.failureReason = L"Upgrade is not allowed after an unclean shutdown.";
                return finish(SC_E_WRITE_CONFLICT);
            }

            if (!confirmed)
            {
                result.status = SCUpgradeStatus::Failed;
                result.failureReason = L"Upgrade confirmation was not provided.";
                return finish(SC_E_INVALIDARG);
            }

            if (plan.currentVersion != schemaVersion_)
            {
                result.status = SCUpgradeStatus::Failed;
                result.failureReason = L"Upgrade plan does not match the current schema version.";
                return finish(SC_E_INVALIDARG);
            }

            if (plan.targetVersion <= plan.currentVersion)
            {
                result.status = SCUpgradeStatus::NotRequired;
                result.failureReason = L"Upgrade is not required for the current schema version.";
                return finish(SC_OK);
            }

            if (plan.orderedSteps.empty())
            {
                result.status = SCUpgradeStatus::Unsupported;
                result.failureReason = L"Upgrade plan does not contain executable steps.";
                return finish(SC_E_NOTIMPL);
            }

            const std::int32_t originalSchemaVersion = schemaVersion_;

            try
            {
                SqliteTxn txn(db_);
                for (const SCMigrationStep& step : plan.orderedSteps)
                {
                    if (step.fromVersion != schemaVersion_)
                    {
                        schemaVersion_ = originalSchemaVersion;
                        result.status = SCUpgradeStatus::Failed;
                        result.failureReason =
                            L"Upgrade step chain does not match the current "
                            L"schema version.";
                        return finish(SC_E_INVALIDARG);
                    }

                    const SqliteUpgradeStepRegistration* registration =
                        FindRegisteredSqliteUpgradeStep(step.fromVersion, step.toVersion);
                    if (registration == nullptr || registration->handler == nullptr)
                    {
                        schemaVersion_ = originalSchemaVersion;
                        result.status = SCUpgradeStatus::Unsupported;
                        result.failureReason = L"Unsupported upgrade step.";
                        return finish(SC_E_NOTIMPL);
                    }

                    const ErrorCode stepRc = registration->handler(*this, &result);
                    if (Failed(stepRc))
                    {
                        schemaVersion_ = originalSchemaVersion;
                        result.rolledBack = true;
                        result.status = stepRc == SC_E_NOTIMPL ? SCUpgradeStatus::Unsupported
                                                                : SCUpgradeStatus::RolledBack;
                        if (result.failureReason.empty())
                        {
                            result.failureReason = L"Upgrade step failed.";
                        }
                        return finish(stepRc);
                    }

                    schemaVersion_ = step.toVersion;
                    std::wstringstream message;
                    message << L"Applied upgrade step " << step.name << L" to schema version " << schemaVersion_
                            << L".";
                    LogStartupDiagnostic(SCDiagnosticSeverity::Info, L"upgrade", message.str());
                }

                SaveMetadata();
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    schemaVersion_ = originalSchemaVersion;
                    result.status = SCUpgradeStatus::RolledBack;
                    result.rolledBack = true;
                    result.failureReason = L"Failed to commit the upgrade transaction.";
                    return finish(commitRc);
                }
            } catch (...)
            {
                schemaVersion_ = originalSchemaVersion;
                result.status = SCUpgradeStatus::RolledBack;
                result.rolledBack = true;
                result.failureReason = L"Upgrade transaction failed and was rolled back.";
                return finish(SC_E_FAIL);
            }

            result.status = SCUpgradeStatus::Success;
            result.rolledBack = false;
            result.sourceVersion = originalSchemaVersion;
            result.targetVersion = schemaVersion_;
            result.failureReason.clear();
            return finish(SC_OK);
        }

        void SqliteDatabase::InitializeQueryIndexStorage()
        {
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS query_indexes ("
                "schema_index_id INTEGER PRIMARY KEY, table_id INTEGER NOT "
                "NULL, index_name TEXT NOT NULL, key_arity INTEGER NOT NULL, "
                "UNIQUE(table_id, index_name));");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS query_index_entries ("
                "schema_index_id INTEGER NOT NULL, record_id INTEGER NOT "
                "NULL, alive_flag INTEGER NOT NULL, key_prefix_1 BLOB, "
                "key_prefix_2 BLOB, key_prefix_3 BLOB, full_key BLOB NOT "
                "NULL, PRIMARY KEY(schema_index_id, record_id));");
            db_.Execute(
                "CREATE INDEX IF NOT EXISTS idx_query_index_entries_prefix1 "
                "ON query_index_entries(schema_index_id, alive_flag, "
                "key_prefix_1, record_id);");
            db_.Execute(
                "CREATE INDEX IF NOT EXISTS idx_query_index_entries_prefix2 "
                "ON query_index_entries(schema_index_id, alive_flag, "
                "key_prefix_2, record_id);");
            db_.Execute(
                "CREATE INDEX IF NOT EXISTS idx_query_index_entries_prefix3 "
                "ON query_index_entries(schema_index_id, alive_flag, "
                "key_prefix_3, record_id);");
            db_.Execute(
                "CREATE INDEX IF NOT EXISTS idx_query_index_entries_full ON "
                "query_index_entries(schema_index_id, alive_flag, full_key, "
                "record_id);");
        }

        ErrorCode SqliteDatabase::EnsureLegacyColumnIndexes()
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }

            for (const auto& [_, tableRef] : tables_)
            {
                const auto* table = static_cast<SqliteTable*>(tableRef.Get());
                if (table == nullptr)
                {
                    continue;
                }

                SCSchemaPtr schema;
                if (Failed(tableRef->GetSchema(schema)) || !schema)
                {
                    continue;
                }

                std::int32_t count = 0;
                if (Failed(schema->GetColumnCount(&count)))
                {
                    continue;
                }

                for (std::int32_t index = 0; index < count; ++index)
                {
                    SCColumnDef column;
                    if (Failed(schema->GetColumn(index, &column)))
                    {
                        continue;
                    }
                    if (column.indexed)
                    {
                        EnsureColumnIndex(table->TableRowId(), column.name);
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::RebuildCompositeQueryIndexes()
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }

            queryIndexRowIdsByTableAndName_.clear();
            for (const auto& [_, tableRef] : tables_)
            {
                auto* sqliteTable = static_cast<SqliteTable*>(tableRef.Get());
                if (sqliteTable == nullptr)
                {
                    continue;
                }

                const ErrorCode rebuildRc = RebuildCompositeIndexesForTable(sqliteTable);
                if (Failed(rebuildRc))
                {
                    return rebuildRc;
                }
            }

            return SC_OK;
        }

        void SqliteDatabase::EnsureColumnIndex(std::int64_t tableRowId, const std::wstring& columnName)
        {
            const std::wstring indexName =
                L"idx_fv_" + std::to_wstring(tableRowId) + L"_" + SanitizeIdentifier(columnName);
            const std::string sql = ToUtf8(L"CREATE INDEX IF NOT EXISTS " + indexName +
                                           L" ON field_values(table_id, column_name, int64_value, "
                                           L"double_value, bool_value, text_value);");
            db_.Execute(sql.c_str());
        }

        void SqliteDatabase::RunStartupIntegrityCheck()
        {
            SqliteStmt stmt = db_.Prepare("PRAGMA integrity_check;");
            bool hasRow = false;
            while (stmt.Step(&hasRow) == SC_OK && hasRow)
            {
                const std::wstring result = stmt.ColumnText(0);
                if (result != L"ok")
                {
                    corruptionDetected_ = true;
                    LogStartupDiagnostic(SCDiagnosticSeverity::Error,
                                         L"integrity",
                                         std::wstring(L"SQLite integrity check failed: ") + result);
                    throw std::runtime_error("sqlite integrity check failed");
                }
            }

            LogStartupDiagnostic(SCDiagnosticSeverity::Info, L"integrity", L"SQLite integrity check passed.");
        }

        void SqliteDatabase::LogStartupDiagnostic(SCDiagnosticSeverity severity,
                                                  const std::wstring& category,
                                                  const std::wstring& message)
        {
            startupDiagnostics_.push_back(SCDiagnosticEntry{severity, category, message});
            SqliteStmt stmt = db_.Prepare(
                "INSERT INTO startup_diagnostics(severity, category, message) "
                "VALUES(?, ?, ?);");
            stmt.BindInt(1, static_cast<int>(severity));
            stmt.BindText(2, category);
            stmt.BindText(3, message);
            stmt.Step();
        }

        void SqliteDatabase::SetCleanShutdownFlag(bool cleanShutdown)
        {
            cleanShutdown_ = cleanShutdown;
            SaveMetadataKey(L"clean_shutdown", cleanShutdown_ ? L"1" : L"0");
        }

        ErrorCode SqliteDatabase::EnsureImportSessionStore()
        {
            if (importSessionStoreReady_ || readOnly_)
            {
                return SC_OK;
            }

            const ErrorCode rc = db_.Execute(
                "CREATE TABLE IF NOT EXISTS import_sessions ("
                "session_id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "session_name TEXT NOT NULL, state INTEGER NOT NULL, "
                "base_version INTEGER NOT NULL, chunk_size INTEGER NOT NULL, "
                "checkpoint_chunk_id INTEGER NOT NULL, checkpoint_count "
                "INTEGER NOT NULL, payload TEXT NOT NULL);");
            if (Succeeded(rc))
            {
                importSessionStoreReady_ = true;
            }
            return rc;
        }

        std::wstring SqliteDatabase::SerializeImportSession(const SCImportStagingArea& session) const
        {
            return SerializeImportSessionPayload(session);
        }

        ErrorCode SqliteDatabase::DeserializeImportSession(const std::wstring& payload,
                                                           SCImportStagingArea* outSession) const
        {
            if (outSession == nullptr)
            {
                return SC_E_POINTER;
            }
            return DeserializeImportSessionPayload(payload, outSession) ? SC_OK : SC_E_FAIL;
        }

        ErrorCode SqliteDatabase::BeginImportSession(const SCImportSessionOptions& options,
                                                     SCImportStagingArea* outSession)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (outSession == nullptr)
            {
                return SC_E_POINTER;
            }

            const ErrorCode storeRc = EnsureImportSessionStore();
            if (Failed(storeRc))
            {
                return storeRc;
            }

            SCImportStagingArea session;
            session.sessionName = options.sessionName.empty() ? L"Import" : options.sessionName;
            session.baseVersion = version_;
            session.chunkSize = options.chunkSize == 0 ? 1 : options.chunkSize;
            session.state = SCImportSessionState::Staging;

            SqliteStmt stmt = db_.Prepare(
                "INSERT INTO import_sessions(session_name, state, "
                "base_version, chunk_size, checkpoint_chunk_id, "
                "checkpoint_count, payload)"
                " VALUES(?, ?, ?, ?, ?, ?, ?);");
            stmt.BindText(1, session.sessionName);
            stmt.BindInt(2, static_cast<int>(SCImportSessionState::Staging));
            stmt.BindInt64(3, static_cast<std::int64_t>(version_));
            stmt.BindInt64(4, static_cast<std::int64_t>(session.chunkSize));
            stmt.BindInt64(5, 0);
            stmt.BindInt64(6, 0);
            stmt.BindText(7, L"");
            const ErrorCode rc = stmt.Step();
            if (Failed(rc))
            {
                return rc;
            }

            session.sessionId = static_cast<SCImportSessionId>(db_.LastInsertRowId());
            const std::wstring payload = SerializeImportSessionPayload(session);
            SqliteStmt updateStmt = db_.Prepare(
                "UPDATE import_sessions SET payload = ?, checkpoint_chunk_id = "
                "?, checkpoint_count = ?, state = ? WHERE session_id = ?;");
            updateStmt.BindText(1, payload);
            updateStmt.BindInt64(2, 0);
            updateStmt.BindInt64(3, 0);
            updateStmt.BindInt(4, static_cast<int>(SCImportSessionState::Staging));
            updateStmt.BindInt64(5, static_cast<std::int64_t>(session.sessionId));
            const ErrorCode updateRc = updateStmt.Step();
            if (Failed(updateRc))
            {
                return updateRc;
            }
            *outSession = session;
            return SC_OK;
        }

        ErrorCode SqliteDatabase::AppendImportChunk(SCImportStagingArea* session,
                                                    const SCImportChunk& chunk,
                                                    SCImportCheckpoint* outCheckpoint)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (session == nullptr)
            {
                return SC_E_POINTER;
            }

            const ErrorCode storeRc = EnsureImportSessionStore();
            if (Failed(storeRc))
            {
                return storeRc;
            }

            session->chunks.push_back(chunk);
            session->state = SCImportSessionState::Checkpointed;

            SqliteStmt loadStmt = db_.Prepare("SELECT session_id FROM import_sessions WHERE session_id = ?;");
            loadStmt.BindInt64(1, static_cast<std::int64_t>(session->sessionId));
            bool hasRow = false;
            if (Failed(loadStmt.Step(&hasRow)) || !hasRow)
            {
                return SC_E_RECORD_NOT_FOUND;
            }

            const std::wstring payload = SerializeImportSessionPayload(*session);
            SqliteStmt updateStmt = db_.Prepare(
                "UPDATE import_sessions SET state = ?, checkpoint_chunk_id = "
                "?, checkpoint_count = ?, payload = ? WHERE session_id = ?;");
            updateStmt.BindInt(1, static_cast<int>(SCImportSessionState::Checkpointed));
            updateStmt.BindInt64(2, static_cast<std::int64_t>(chunk.chunkId));
            updateStmt.BindInt64(3, static_cast<std::int64_t>(session->chunks.size()));
            updateStmt.BindText(4, payload);
            updateStmt.BindInt64(5, static_cast<std::int64_t>(session->sessionId));
            const ErrorCode rc = updateStmt.Step();
            if (Failed(rc))
            {
                return rc;
            }

            if (outCheckpoint != nullptr)
            {
                outCheckpoint->sessionId = session->sessionId;
                outCheckpoint->lastChunkId = chunk.chunkId;
                outCheckpoint->chunkCount = session->chunks.size();
                outCheckpoint->baseVersion = session->baseVersion;
                outCheckpoint->persisted = true;
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::LoadImportRecoveryState(std::uint64_t sessionId, SCImportRecoveryState* outState)
        {
            if (outState == nullptr)
            {
                return SC_E_POINTER;
            }

            const ErrorCode storeRc = EnsureImportSessionStore();
            if (Failed(storeRc))
            {
                return storeRc;
            }

            SqliteStmt stmt = db_.Prepare(
                "SELECT session_name, state, base_version, chunk_size, "
                "checkpoint_chunk_id, checkpoint_count, payload"
                " FROM import_sessions WHERE session_id = ?;");
            stmt.BindInt64(1, static_cast<std::int64_t>(sessionId));

            bool hasRow = false;
            const ErrorCode stepRc = stmt.Step(&hasRow);
            if (Failed(stepRc))
            {
                return stepRc;
            }
            if (!hasRow)
            {
                return SC_E_RECORD_NOT_FOUND;
            }

            SCImportRecoveryState state;
            state.sessionId = sessionId;
            state.state = static_cast<SCImportSessionState>(stmt.ColumnInt(1));
            state.checkpoint.sessionId = sessionId;
            state.checkpoint.lastChunkId = static_cast<SCImportChunkId>(stmt.ColumnInt64(4));
            state.checkpoint.chunkCount = static_cast<std::size_t>(stmt.ColumnInt64(5));
            state.checkpoint.baseVersion = static_cast<VersionId>(stmt.ColumnInt64(2));
            state.checkpoint.persisted = true;
            state.checkpointPersisted = true;

            const std::wstring payload = stmt.ColumnText(6);
            if (!payload.empty())
            {
                if (!DeserializeImportSessionPayload(payload, &state.stagingArea))
                {
                    return SC_E_FAIL;
                }
            } else
            {
                state.stagingArea.sessionId = sessionId;
                state.stagingArea.sessionName = stmt.ColumnText(0);
                state.stagingArea.baseVersion = static_cast<VersionId>(stmt.ColumnInt64(2));
                state.stagingArea.chunkSize = static_cast<std::size_t>(stmt.ColumnInt64(3));
                state.stagingArea.state = state.state;
            }

            state.stagingArea.sessionId = sessionId;
            state.stagingArea.sessionName = stmt.ColumnText(0);
            state.stagingArea.baseVersion = static_cast<VersionId>(stmt.ColumnInt64(2));
            state.stagingArea.chunkSize = static_cast<std::size_t>(stmt.ColumnInt64(3));
            state.stagingArea.state = state.state;
            state.canResume =
                state.state != SCImportSessionState::Finalized && state.state != SCImportSessionState::Aborted;
            state.canFinalize = state.state == SCImportSessionState::Checkpointed ||
                                state.state == SCImportSessionState::ReadyToFinalize;
            state.reason = state.canResume ? L"Import session recoverable from checkpoint."
                                           : L"Import session is no longer recoverable.";

            *outState = std::move(state);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::FinalizeImportSession(const SCImportFinalizeCommit& commit,
                                                        SCImportRecoveryState* outState)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (!commit.confirmed)
            {
                return SC_E_INVALIDARG;
            }

            const ErrorCode storeRc = EnsureImportSessionStore();
            if (Failed(storeRc))
            {
                return storeRc;
            }

            SCImportRecoveryState recoveryState;
            const ErrorCode loadRc = LoadImportRecoveryState(commit.sessionId, &recoveryState);
            if (Failed(loadRc))
            {
                return loadRc;
            }

            SqliteStmt deleteStmt = db_.Prepare("DELETE FROM import_sessions WHERE session_id = ?;");
            deleteStmt.BindInt64(1, static_cast<std::int64_t>(commit.sessionId));
            const ErrorCode deleteRc = deleteStmt.Step();
            if (Failed(deleteRc))
            {
                return deleteRc;
            }

            recoveryState.state = SCImportSessionState::Finalized;
            recoveryState.canResume = false;
            recoveryState.canFinalize = false;
            recoveryState.reason = L"Import session finalized and checkpoint cleared.";
            if (outState != nullptr)
            {
                *outState = std::move(recoveryState);
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::AbortImportSession(std::uint64_t sessionId)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }

            const ErrorCode storeRc = EnsureImportSessionStore();
            if (Failed(storeRc))
            {
                return storeRc;
            }

            SqliteStmt deleteStmt = db_.Prepare("DELETE FROM import_sessions WHERE session_id = ?;");
            deleteStmt.BindInt64(1, static_cast<std::int64_t>(sessionId));
            return deleteStmt.Step();
        }

        ErrorCode SqliteDatabase::EnsureWritable() const
        {
            return readOnly_ ? SC_E_READ_ONLY_DATABASE : SC_OK;
        }

        ErrorCode SqliteDatabase::CollectDiagnostics(SCStorageHealthReport* outReport) const
        {
            if (outReport == nullptr)
            {
                return SC_E_POINTER;
            }

            outReport->diagnostics.insert(
                outReport->diagnostics.end(), startupDiagnostics_.begin(), startupDiagnostics_.end());
            if (dirtyStartupDetected_)
            {
                outReport->diagnostics.push_back(SCDiagnosticEntry{
                    SCDiagnosticSeverity::Warning,
                    L"startup",
                    L"Current session followed an unclean shutdown.",
                });
            }
            if (corruptionDetected_)
            {
                outReport->diagnostics.push_back(SCDiagnosticEntry{
                    SCDiagnosticSeverity::Error,
                    L"integrity",
                    L"Corruption was detected during startup integrity checks.",
                });
            }

            QueryIndexCheckResult queryIndexCheck;
            if (Succeeded(CheckQueryIndex(&queryIndexCheck)))
            {
                if (queryIndexCheck.state == QueryIndexHealthState::Missing ||
                    queryIndexCheck.state == QueryIndexHealthState::OutOfDate)
                {
                    outReport->diagnostics.push_back(SCDiagnosticEntry{
                        SCDiagnosticSeverity::Warning,
                        L"query-index",
                        queryIndexCheck.message,
                    });
                } else if (queryIndexCheck.state == QueryIndexHealthState::Corrupted)
                {
                    outReport->diagnostics.push_back(SCDiagnosticEntry{
                        SCDiagnosticSeverity::Error,
                        L"query-index",
                        queryIndexCheck.message,
                    });
                }
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::CheckQueryIndex(QueryIndexCheckResult* outResult) const
        {
            if (outResult == nullptr)
            {
                return SC_E_POINTER;
            }

            return CheckCompositeQueryIndexConsistency(outResult);
        }

        ErrorCode SqliteDatabase::RebuildQueryIndex()
        {
            if (activeEdit_)
            {
                return SC_E_WRITE_CONFLICT;
            }

            const ErrorCode legacyRc = EnsureLegacyColumnIndexes();
            if (Failed(legacyRc))
            {
                return legacyRc;
            }

            return RebuildCompositeQueryIndexes();
        }

        ErrorCode SqliteDatabase::BeginEdit(const wchar_t* name, SCEditPtr& outEdit)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (activeEdit_)
            {
                return SC_E_WRITE_CONFLICT;
            }
            activeJournal_ = JournalTransaction{};
            activeSchemaOps_.clear();
            activeJournal_.actionName = (name != nullptr && *name != L'\0') ? name : L"Edit";
            activeEdit_ = SCMakeRef<SqliteEditSession>(activeJournal_.actionName, version_);
            outEdit = activeEdit_;
            return SC_OK;
        }

        ErrorCode SqliteDatabase::Commit(ISCEditSession* edit)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            ClearConstraintViolation();
            const ErrorCode validate = ValidateActiveEdit(edit);
            if (Failed(validate))
            {
                return validate;
            }

            if (activeJournal_.entries.empty())
            {
                activeEdit_->SetState(EditState::Committed);
                activeEdit_.Reset();
                activeJournal_ = JournalTransaction{};
                activeSchemaOps_.clear();
                return SC_OK;
            }

            const ErrorCode requiredValueRc = ValidateRequiredValuesForCommit();
            if (Failed(requiredValueRc))
            {
                return requiredValueRc;
            }

            try
            {
                const VersionId committedVersion = version_ + 1;
                JournalTransaction committedJournal = activeJournal_;
                SqliteTxn txn(db_);
                const ErrorCode schemaRc = PersistDeferredSchemaOps();
                if (Failed(schemaRc))
                {
                    return schemaRc;
                }

                const ErrorCode uniqueConstraintRc =
                    ValidateUniqueAndPrimaryKeyConstraintsForTouchedTables(
                        activeJournal_, false);
                if (Failed(uniqueConstraintRc))
                {
                    return uniqueConstraintRc;
                }
                const ErrorCode foreignKeyReferenceRc =
                    ValidateForeignKeyReferencesForTouchedTables(
                        activeJournal_, false);
                if (Failed(foreignKeyReferenceRc))
                {
                    return foreignKeyReferenceRc;
                }
                PersistTouchedRecords(committedJournal,
                                      committedVersion,
                                      false);
                DeleteRedoJournalRows();
                committedJournal.committedVersion = committedVersion;
                const std::int64_t txId =
                    InsertJournalTransaction(committedJournal, kStackUndo, static_cast<int>(undoStack_.size()));
                committedJournal.commitId = static_cast<CommitId>(txId);
                PersistJournalEntries(txId, committedJournal);
                SaveMetadata(committedVersion);
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    return commitRc;
                }

                version_ = committedVersion;
                UpdateTouchedVersions(committedJournal, version_, false);
                undoStack_.push_back(SqlitePersistedJournalTransaction{txId, committedJournal});
                redoStack_.clear();
                activeEdit_->SetState(EditState::Committed);
                activeJournal_ = committedJournal;
                activeSchemaOps_.clear();
            } catch (...)
            {
                return SC_E_FAIL;
            }

            RefreshReferenceIndexState();
            const SCChangeSet SCChangeSet = BuildChangeSet(activeJournal_, ChangeSource::UserEdit, version_);
            activeEdit_.Reset();
            activeJournal_ = JournalTransaction{};
            NotifyObservers(SCChangeSet);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::Rollback(ISCEditSession* edit)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            const ErrorCode validate = ValidateActiveEdit(edit);
            if (Failed(validate))
            {
                return validate;
            }

            std::size_t reversedCount = 0;
            try
            {
                if (!activeJournal_.entries.empty())
                {
                    SqliteTxn txn(db_);
                    const ErrorCode applyRc = ApplyJournalReverse(
                        activeJournal_,
                        0,
                        activeJournal_.entries.size(),
                        &reversedCount);
                    if (Failed(applyRc))
                    {
                        const std::size_t beginIndex =
                            activeJournal_.entries.size() - reversedCount;
                        (void)ApplyJournalForward(activeJournal_,
                                                  beginIndex,
                                                  activeJournal_.entries.size(),
                                                  nullptr);
                        return applyRc;
                    }
                    RefreshReferenceIndexState();
                    const ErrorCode commitRc = txn.Commit();
                    if (Failed(commitRc))
                    {
                        const std::size_t beginIndex =
                            activeJournal_.entries.size() - reversedCount;
                        (void)ApplyJournalForward(activeJournal_,
                                                  beginIndex,
                                                  activeJournal_.entries.size(),
                                                  nullptr);
                        return commitRc;
                    }
                } else
                {
                    RefreshReferenceIndexState();
                }
            } catch (...)
            {
                if (reversedCount > 0)
                {
                    const std::size_t beginIndex =
                        activeJournal_.entries.size() - reversedCount;
                    (void)ApplyJournalForward(activeJournal_,
                                              beginIndex,
                                              activeJournal_.entries.size(),
                                              nullptr);
                }
                return SC_E_FAIL;
            }

            activeEdit_->SetState(EditState::RolledBack);
            activeEdit_.Reset();
            activeJournal_ = JournalTransaction{};
            activeSchemaOps_.clear();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::Undo()
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            ClearConstraintViolation();
            if (activeEdit_)
            {
                return SC_E_WRITE_CONFLICT;
            }
            if (undoStack_.empty())
            {
                return SC_E_UNDO_STACK_EMPTY;
            }

            SqlitePersistedJournalTransaction tx = undoStack_.back();
            undoStack_.pop_back();

            std::size_t reversedCount = 0;
            try
            {
                SqliteTxn txn(db_);
                const ErrorCode applyRc = ApplyJournalReverse(
                    tx.tx, 0, tx.tx.entries.size(), &reversedCount);
                if (Failed(applyRc))
                {
                    const std::size_t beginIndex =
                        tx.tx.entries.size() - reversedCount;
                    (void)ApplyJournalForward(tx.tx,
                                              beginIndex,
                                              tx.tx.entries.size(),
                                              nullptr);
                    undoStack_.push_back(tx);
                    return applyRc;
                }
                const ErrorCode constraintRc =
                    ValidateUniqueAndPrimaryKeyConstraintsForTouchedTables(
                        tx.tx, true);
                if (Failed(constraintRc))
                {
                    const std::size_t beginIndex =
                        tx.tx.entries.size() - reversedCount;
                    (void)ApplyJournalForward(tx.tx,
                                              beginIndex,
                                              tx.tx.entries.size(),
                                              nullptr);
                    undoStack_.push_back(tx);
                    return constraintRc;
                }
                const ErrorCode foreignKeyReferenceRc =
                    ValidateForeignKeyReferencesForTouchedTables(
                        tx.tx, true);
                if (Failed(foreignKeyReferenceRc))
                {
                    const std::size_t beginIndex =
                        tx.tx.entries.size() - reversedCount;
                    (void)ApplyJournalForward(tx.tx,
                                              beginIndex,
                                              tx.tx.entries.size(),
                                              nullptr);
                    undoStack_.push_back(tx);
                    return foreignKeyReferenceRc;
                }
                const VersionId nextVersion = version_ + 1;
                PersistTouchedRecords(tx.tx, nextVersion, true);
                UpdateJournalTransactionStack(tx.txId, kStackRedo, static_cast<int>(redoStack_.size()));
                SaveMetadata(nextVersion);
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    const std::size_t beginIndex =
                        tx.tx.entries.size() - reversedCount;
                    (void)ApplyJournalForward(tx.tx,
                                              beginIndex,
                                              tx.tx.entries.size(),
                                              nullptr);
                    undoStack_.push_back(tx);
                    return commitRc;
                }

                version_ = nextVersion;
                UpdateTouchedVersions(tx.tx, version_, true);
                redoStack_.push_back(tx);
            } catch (...)
            {
                if (reversedCount > 0)
                {
                    const std::size_t beginIndex =
                        tx.tx.entries.size() - reversedCount;
                    (void)ApplyJournalForward(tx.tx,
                                              beginIndex,
                                              tx.tx.entries.size(),
                                              nullptr);
                }
                undoStack_.push_back(tx);
                return SC_E_FAIL;
            }

            RefreshReferenceIndexState();
            NotifyObservers(BuildChangeSet(tx.tx, ChangeSource::Undo, version_));
            return SC_OK;
        }

        ErrorCode SqliteDatabase::Redo()
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            ClearConstraintViolation();
            if (activeEdit_)
            {
                return SC_E_WRITE_CONFLICT;
            }
            if (redoStack_.empty())
            {
                return SC_E_REDO_STACK_EMPTY;
            }

            SqlitePersistedJournalTransaction tx = redoStack_.back();
            redoStack_.pop_back();

            std::size_t forwardedCount = 0;
            try
            {
                SqliteTxn txn(db_);
                const ErrorCode applyRc = ApplyJournalForward(
                    tx.tx, 0, tx.tx.entries.size(), &forwardedCount);
                if (Failed(applyRc))
                {
                    (void)ApplyJournalReverse(tx.tx, 0, forwardedCount, nullptr);
                    redoStack_.push_back(tx);
                    return applyRc;
                }
                const ErrorCode constraintRc =
                    ValidateUniqueAndPrimaryKeyConstraintsForTouchedTables(
                        tx.tx, false);
                if (Failed(constraintRc))
                {
                    (void)ApplyJournalReverse(tx.tx, 0, forwardedCount, nullptr);
                    redoStack_.push_back(tx);
                    return constraintRc;
                }
                const ErrorCode foreignKeyReferenceRc =
                    ValidateForeignKeyReferencesForTouchedTables(
                        tx.tx, false);
                if (Failed(foreignKeyReferenceRc))
                {
                    (void)ApplyJournalReverse(tx.tx, 0, forwardedCount, nullptr);
                    redoStack_.push_back(tx);
                    return foreignKeyReferenceRc;
                }
                const VersionId nextVersion = version_ + 1;
                PersistTouchedRecords(tx.tx, nextVersion, false);
                UpdateJournalTransactionStack(tx.txId, kStackUndo, static_cast<int>(undoStack_.size()));
                SaveMetadata(nextVersion);
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    (void)ApplyJournalReverse(tx.tx, 0, forwardedCount, nullptr);
                    redoStack_.push_back(tx);
                    return commitRc;
                }

                version_ = nextVersion;
                UpdateTouchedVersions(tx.tx, version_, false);
                undoStack_.push_back(tx);
            } catch (...)
            {
                if (forwardedCount > 0)
                {
                    (void)ApplyJournalReverse(tx.tx, 0, forwardedCount, nullptr);
                }
                redoStack_.push_back(tx);
                return SC_E_FAIL;
            }

            RefreshReferenceIndexState();
            NotifyObservers(BuildChangeSet(tx.tx, ChangeSource::Redo, version_));
            return SC_OK;
        }

        ErrorCode SqliteDatabase::GetTable(const wchar_t* name, SCTablePtr& outTable)
        {
            if (name == nullptr)
            {
                return SC_E_INVALIDARG;
            }
            const auto it = tables_.find(name);
            if (it == tables_.end())
            {
                return SC_E_TABLE_NOT_FOUND;
            }
            outTable = it->second;
            return SC_OK;
        }

        ErrorCode SqliteDatabase::GetTableCount(std::int32_t* outCount)
        {
            if (outCount == nullptr)
            {
                return SC_E_POINTER;
            }

            *outCount = static_cast<std::int32_t>(tables_.size());
            return SC_OK;
        }

        ErrorCode SqliteDatabase::GetTableName(std::int32_t index, std::wstring* outName)
        {
            if (outName == nullptr)
            {
                return SC_E_POINTER;
            }
            if (index < 0 || static_cast<std::size_t>(index) >= tables_.size())
            {
                return SC_E_INVALIDARG;
            }

            auto it = tables_.begin();
            std::advance(it, index);
            *outName = it->first;
            return SC_OK;
        }

        ErrorCode SqliteDatabase::CreateTable(const wchar_t* name, SCTablePtr& outTable)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (name == nullptr || *name == L'\0')
            {
                return SC_E_INVALIDARG;
            }

            const auto existing = tables_.find(name);
            if (existing != tables_.end())
            {
                outTable = existing->second;
                return SC_OK;
            }

            try
            {
                SqliteTxn txn(db_);
                SqliteStmt stmt = db_.Prepare("INSERT INTO tables(name) VALUES(?);");
                stmt.BindText(1, name);
                const ErrorCode rc = stmt.Step();
                if (Failed(rc))
                {
                    return rc;
                }

                const std::int64_t tableRowId = db_.LastInsertRowId();
                SCTablePtr table = SCMakeRef<SqliteTable>(this, std::wstring{name}, tableRowId);

                SqliteStmt metaStmt = db_.Prepare(
                    "INSERT INTO schema_tables(table_id, description) "
                    "VALUES(?, '') ON CONFLICT(table_id) DO UPDATE SET "
                    "description=excluded.description;");
                metaStmt.BindInt64(1, tableRowId);
                const ErrorCode metaRc = metaStmt.Step();
                if (Failed(metaRc))
                {
                    return metaRc;
                }

                SaveMetadata();
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    return commitRc;
                }

                tables_.emplace(name, table);
                MarkReferenceIndexDirty();
                MarkForeignKeyReferenceCacheDirty();
                outTable = std::move(table);
                return SC_OK;
            } catch (...)
            {
                return SC_E_SCHEMA_VIOLATION;
            }
        }

        ErrorCode SqliteDatabase::DeleteTable(const wchar_t* name)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (name == nullptr || *name == L'\0')
            {
                return SC_E_INVALIDARG;
            }

            const std::wstring tableName{name};

            const auto tableIt = tables_.find(tableName);
            if (tableIt == tables_.end())
            {
                return SC_E_TABLE_NOT_FOUND;
            }

            auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
            if (table == nullptr)
            {
                return SC_E_FAIL;
            }

            for (const auto& [otherName, otherTableRef] : tables_)
            {
                if (EqualsIgnoreCase(otherName, tableName))
                {
                    continue;
                }

                auto* otherTable = static_cast<SqliteTable*>(otherTableRef.Get());
                if (otherTable == nullptr)
                {
                    continue;
                }

                SCSchemaPtr otherSchema;
                const ErrorCode schemaRc = otherTable->GetSchema(otherSchema);
                if (Failed(schemaRc) || !otherSchema)
                {
                    continue;
                }

                std::int32_t columnCount = 0;
                const ErrorCode columnCountRc = otherSchema->GetColumnCount(&columnCount);
                if (Failed(columnCountRc))
                {
                    return columnCountRc;
                }

                for (std::int32_t columnIndex = 0; columnIndex < columnCount; ++columnIndex)
                {
                    SCColumnDef column;
                    const ErrorCode columnRc = otherSchema->GetColumn(columnIndex, &column);
                    if (Failed(columnRc))
                    {
                        return columnRc;
                    }
                    if (column.columnKind == ColumnKind::Relation && EqualsIgnoreCase(column.referenceTable, tableName))
                    {
                        return SC_E_CONSTRAINT_VIOLATION;
                    }
                }
            }

            SCSchemaPtr schema;
            if (Failed(table->GetSchema(schema)) || !schema)
            {
                return SC_E_FAIL;
            }

            std::vector<std::wstring> indexedColumns;
            std::int32_t schemaColumnCount = 0;
            const ErrorCode schemaCountRc = schema->GetColumnCount(&schemaColumnCount);
            if (Failed(schemaCountRc))
            {
                return schemaCountRc;
            }

            for (std::int32_t columnIndex = 0; columnIndex < schemaColumnCount; ++columnIndex)
            {
                SCColumnDef column;
                const ErrorCode columnRc = schema->GetColumn(columnIndex, &column);
                if (Failed(columnRc))
                {
                    return columnRc;
                }
                if (column.indexed)
                {
                    indexedColumns.push_back(column.name);
                }
            }

            try
            {
                SqliteTxn txn(db_);

                if (HasTable(L"schema_constraint_columns"))
                {
                    SqliteStmt deleteConstraintColumnsStmt = db_.Prepare(
                        "DELETE FROM schema_constraint_columns WHERE "
                        "constraint_id IN (SELECT constraint_id FROM "
                        "schema_constraints WHERE table_id = ?);");
                    deleteConstraintColumnsStmt.BindInt64(1, table->TableRowId());
                    const ErrorCode rc = deleteConstraintColumnsStmt.Step();
                    if (Failed(rc))
                    {
                        return rc;
                    }
                }

                if (HasTable(L"schema_constraints"))
                {
                    SqliteStmt deleteConstraintsStmt =
                        db_.Prepare("DELETE FROM schema_constraints WHERE table_id = ?;");
                    deleteConstraintsStmt.BindInt64(1, table->TableRowId());
                    const ErrorCode rc = deleteConstraintsStmt.Step();
                    if (Failed(rc))
                    {
                        return rc;
                    }
                }

                if (HasTable(L"schema_index_columns"))
                {
                    SqliteStmt deleteIndexColumnsStmt = db_.Prepare(
                        "DELETE FROM schema_index_columns WHERE index_id IN "
                        "(SELECT index_id FROM schema_indexes WHERE "
                        "table_id = ?);");
                    deleteIndexColumnsStmt.BindInt64(1, table->TableRowId());
                    const ErrorCode rc = deleteIndexColumnsStmt.Step();
                    if (Failed(rc))
                    {
                        return rc;
                    }
                }

                if (HasTable(L"schema_indexes"))
                {
                    SqliteStmt deleteIndexesStmt = db_.Prepare("DELETE FROM schema_indexes WHERE table_id = ?;");
                    deleteIndexesStmt.BindInt64(1, table->TableRowId());
                    const ErrorCode rc = deleteIndexesStmt.Step();
                    if (Failed(rc))
                    {
                        return rc;
                    }
                }

                if (HasTable(L"schema_tables"))
                {
                    SqliteStmt deleteTableMetaStmt = db_.Prepare("DELETE FROM schema_tables WHERE table_id = ?;");
                    deleteTableMetaStmt.BindInt64(1, table->TableRowId());
                    const ErrorCode rc = deleteTableMetaStmt.Step();
                    if (Failed(rc))
                    {
                        return rc;
                    }
                }

                for (const std::wstring& columnName : indexedColumns)
                {
                    const std::wstring indexName =
                        L"idx_fv_" + std::to_wstring(table->TableRowId()) + L"_" + SanitizeIdentifier(columnName);
                    const std::string dropIndexSql = "DROP INDEX IF EXISTS " + ToUtf8(indexName) + ";";
                    SqliteStmt dropIndexStmt = db_.Prepare(dropIndexSql.c_str());
                    const ErrorCode rc = dropIndexStmt.Step();
                    if (Failed(rc))
                    {
                        return rc;
                    }
                }

                SqliteStmt deleteFieldValuesStmt = db_.Prepare("DELETE FROM field_values WHERE table_id = ?;");
                deleteFieldValuesStmt.BindInt64(1, table->TableRowId());
                const ErrorCode deleteFieldValuesRc = deleteFieldValuesStmt.Step();
                if (Failed(deleteFieldValuesRc))
                {
                    return deleteFieldValuesRc;
                }

                SqliteStmt deleteRecordsStmt = db_.Prepare("DELETE FROM records WHERE table_id = ?;");
                deleteRecordsStmt.BindInt64(1, table->TableRowId());
                const ErrorCode deleteRecordsRc = deleteRecordsStmt.Step();
                if (Failed(deleteRecordsRc))
                {
                    return deleteRecordsRc;
                }

                SqliteStmt deleteColumnsStmt = db_.Prepare("DELETE FROM schema_columns WHERE table_id = ?;");
                deleteColumnsStmt.BindInt64(1, table->TableRowId());
                const ErrorCode deleteColumnsRc = deleteColumnsStmt.Step();
                if (Failed(deleteColumnsRc))
                {
                    return deleteColumnsRc;
                }

                SqliteStmt deleteTableStmt = db_.Prepare("DELETE FROM tables WHERE table_id = ?;");
                deleteTableStmt.BindInt64(1, table->TableRowId());
                const ErrorCode deleteRc = deleteTableStmt.Step();
                if (Failed(deleteRc))
                {
                    return deleteRc;
                }

                SaveMetadata();
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    return commitRc;
                }
            } catch (...)
            {
                return SC_E_FAIL;
            }

            tables_.erase(tableIt);
            MarkReferenceIndexDirty();
            MarkForeignKeyReferenceCacheDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::RenameTable(const wchar_t* originalName,
                                              const wchar_t* newName)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (originalName == nullptr || newName == nullptr ||
                *originalName == L'\0' || *newName == L'\0')
            {
                return SC_E_INVALIDARG;
            }
            if (!HasActiveEdit())
            {
                return PersistTableRename(originalName,
                                          newName,
                                          false,
                                          true);
            }

            const ErrorCode renameRc =
                RecordDeferredRenameTable(originalName, newName);
            if (Failed(renameRc))
            {
                return renameRc;
            }

            const DeferredRenameState* state =
                FindDeferredRenameState(originalName, newName);
            if (state == nullptr)
            {
                return SC_E_FAIL;
            }

            RecordTableRenameJournal(state->oldName, state->newName);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::AddObserver(ISCDatabaseObserver* observer)
        {
            if (observer == nullptr)
            {
                return SC_E_POINTER;
            }
            observers_.push_back(observer);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::RemoveObserver(ISCDatabaseObserver* observer)
        {
            observers_.erase(std::remove(observers_.begin(), observers_.end(), observer), observers_.end());
            return SC_OK;
        }

        ErrorCode SqliteDatabase::GetLastConstraintViolationInfo(SCConstraintViolationInfo* outInfo) const
        {
            if (outInfo == nullptr)
            {
                return SC_E_POINTER;
            }
            if (!lastConstraintViolation_)
            {
                *outInfo = SCConstraintViolationInfo{};
                return SC_FALSE_RESULT;
            }
            *outInfo = *lastConstraintViolation_;
            return SC_OK;
        }

        void SqliteDatabase::ClearConstraintViolation() const
        {
            lastConstraintViolation_.reset();
        }

        void SqliteDatabase::SetConstraintViolation(const SCConstraintViolationInfo& info) const
        {
            lastConstraintViolation_ = info;
        }

        ErrorCode SqliteDatabase::CreateBackupCopy(const wchar_t* targetPath,
                                                   const SCBackupOptions& options,
                                                   SCBackupResult* outResult)
        {
            if (targetPath == nullptr || *targetPath == L'\0')
            {
                return SC_E_INVALIDARG;
            }

#if !defined(_WIN32)
            (void)options;
            (void)outResult;
            return SC_E_NOTIMPL;
#else
            if (!options.overwriteExisting)
            {
                const DWORD attrs = GetFileAttributesW(targetPath);
                if (attrs != INVALID_FILE_ATTRIBUTES)
                {
                    return SC_E_FILE_EXISTS;
                }
            }

            std::wstring tempPath;
            if (!CreateSiblingTempFile(targetPath, &tempPath))
            {
                return SC_E_IO_ERROR;
            }

            ScopedDeleteFile cleanup(std::move(tempPath));
            const std::wstring& tempFilePath = cleanup.Path();
            const std::string tempUtf8 = ToUtf8(tempFilePath);

            sqlite3* targetDb = nullptr;
            const int openRc = sqlite3_open_v2(tempUtf8.c_str(),
                                               &targetDb,
                                               SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
                                               nullptr);
            if (openRc != SQLITE_OK)
            {
                if (targetDb != nullptr)
                {
                    sqlite3_close(targetDb);
                }
                return SC_E_IO_ERROR;
            }

            ErrorCode resultRc = SC_OK;
            sqlite3_backup* backup = sqlite3_backup_init(targetDb, "main", db_.Raw(), "main");
            if (backup == nullptr)
            {
                sqlite3_close(targetDb);
                return SC_E_FAIL;
            }

            const int stepRc = sqlite3_backup_step(backup, -1);
            if (stepRc != SQLITE_DONE)
            {
                resultRc = MapSqliteError(stepRc);
            }
            sqlite3_backup_finish(backup);

            std::size_t removedTransactionCount = 0;
            std::size_t removedEntryCount = 0;
            if (resultRc == SC_OK && !options.preserveJournalHistory)
            {
                resultRc =
                    ClearJournalHistoryForBackup(targetDb, version_, &removedTransactionCount, &removedEntryCount);
            }

            if (resultRc == SC_OK && options.vacuumTarget)
            {
                resultRc = VacuumTargetDatabase(targetDb);
            }

            if (resultRc == SC_OK && options.validateTarget)
            {
                resultRc = ValidateTargetDatabase(targetDb);
            }

            std::uint64_t outputFileSizeBytes = 0;
            if (resultRc == SC_OK)
            {
                const ErrorCode sizeRc = GetFileSizeBytes(tempFilePath, &outputFileSizeBytes);
                if (Failed(sizeRc))
                {
                    resultRc = sizeRc;
                }
            }

            sqlite3_close(targetDb);

            if (resultRc == SC_OK)
            {
                if (!MoveFileExW(tempFilePath.c_str(), targetPath, MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH))
                {
                    return SC_E_IO_ERROR;
                }

                cleanup.Release();
                if (outResult != nullptr)
                {
                    outResult->removedJournalTransactionCount = static_cast<std::uint64_t>(removedTransactionCount);
                    outResult->removedJournalEntryCount = static_cast<std::uint64_t>(removedEntryCount);
                    outResult->outputFileSizeBytes = outputFileSizeBytes;
                }
            }

            return resultRc;
#endif
        }

        ErrorCode SqliteDatabase::ResetHistoryBaseline(SCBackupResult* outResult)
        {
            if (readOnly_)
            {
                return SC_E_READ_ONLY_DATABASE;
            }
            if (activeEdit_)
            {
                return SC_E_WRITE_CONFLICT;
            }

            std::size_t removedTransactionCount = 0;
            std::size_t removedEntryCount = 0;

            try
            {
                SqliteTxn txn(db_);
                const ErrorCode clearRc =
                    ClearJournalHistoryForBackup(db_.Raw(), version_, &removedTransactionCount, &removedEntryCount);
                if (Failed(clearRc))
                {
                    return clearRc;
                }
                SaveMetadata(version_, version_);
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    return commitRc;
                }

                baselineVersion_ = version_;
                undoStack_.clear();
                redoStack_.clear();
            } catch (...)
            {
                return SC_E_FAIL;
            }

            if (outResult != nullptr)
            {
                *outResult = SCBackupResult{};
                outResult->removedJournalTransactionCount = static_cast<std::uint64_t>(removedTransactionCount);
                outResult->removedJournalEntryCount = static_cast<std::uint64_t>(removedEntryCount);
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::GetEditLogState(SCEditLogState* outState) const
        {
            if (outState == nullptr)
            {
                return SC_E_POINTER;
            }

            outState->baselineVersion = baselineVersion_;
            outState->undoItems.clear();
            outState->redoItems.clear();

            if (openMode_ == SCDatabaseOpenMode::NoHistory)
            {
                return SC_OK;
            }

            outState->undoItems.reserve(undoStack_.size());
            for (const auto& tx : undoStack_)
            {
                outState->undoItems.push_back(SCEditLogEntry{
                    tx.tx.commitId, tx.tx.committedVersion, SCEditLogActionKind::Commit, tx.tx.actionName, L"", 0});
            }

            outState->redoItems.reserve(redoStack_.size());
            for (const auto& tx : redoStack_)
            {
                outState->redoItems.push_back(SCEditLogEntry{
                    tx.tx.commitId, tx.tx.committedVersion, SCEditLogActionKind::Commit, tx.tx.actionName, L"", 0});
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::GetEditingState(SCEditingDatabaseState* outState) const
        {
            if (outState == nullptr)
            {
                return SC_E_POINTER;
            }

            outState->open = true;
            outState->dirty = static_cast<bool>(activeEdit_) || !undoStack_.empty();
            outState->openMode = openMode_;
            outState->currentVersion = version_;
            outState->baselineVersion = baselineVersion_;
            outState->undoCount = openMode_ == SCDatabaseOpenMode::NoHistory ? 0 : undoStack_.size();
            outState->redoCount = openMode_ == SCDatabaseOpenMode::NoHistory ? 0 : redoStack_.size();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::ValidateActiveEdit(ISCEditSession* edit) const
        {
            if (!activeEdit_)
            {
                return SC_E_NO_ACTIVE_EDIT;
            }
            if (edit == nullptr)
            {
                return SC_E_POINTER;
            }
            if (edit != activeEdit_.Get())
            {
                return SC_E_EDIT_MISMATCH;
            }
            if (activeEdit_->GetState() != EditState::Active)
            {
                return SC_E_EDIT_ALREADY_CLOSED;
            }
            return SC_OK;
        }

        const SCColumnDef* SqliteDatabase::FindRelationStorageColumn(const SCColumnDef& relationColumn) const
        {
            if (relationColumn.referenceTable.empty() ||
                relationColumn.referenceStorageColumn.empty())
            {
                return nullptr;
            }

            const auto tableIt = tables_.find(relationColumn.referenceTable);
            if (tableIt == tables_.end())
            {
                return nullptr;
            }

            auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
            if (table == nullptr)
            {
                return nullptr;
            }

            return table->Schema()->FindColumnDef(relationColumn.referenceStorageColumn);
        }

        const SCColumnDef* SqliteDatabase::FindRelationDisplayColumn(const SCColumnDef& relationColumn) const
        {
            if (relationColumn.referenceTable.empty())
            {
                return nullptr;
            }

            const std::wstring& displayColumnName = relationColumn.referenceDisplayColumn.empty()
                                                        ? relationColumn.referenceStorageColumn
                                                        : relationColumn.referenceDisplayColumn;
            if (displayColumnName.empty())
            {
                return nullptr;
            }

            const auto tableIt = tables_.find(relationColumn.referenceTable);
            if (tableIt == tables_.end())
            {
                return nullptr;
            }

            auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
            if (table == nullptr)
            {
                return nullptr;
            }

            return table->Schema()->FindColumnDef(displayColumnName);
        }

        ErrorCode SqliteDatabase::ResolveRelationStoredValue(const SCColumnDef& relationColumn,
                                                             RecordId targetRecordId,
                                                             SCValue* outValue) const
        {
            if (outValue == nullptr)
            {
                return SC_E_POINTER;
            }
            if (relationColumn.referenceStorageColumn.empty())
            {
                *outValue = SCValue::FromRecordId(targetRecordId);
                return SC_OK;
            }

            const auto tableIt = tables_.find(relationColumn.referenceTable);
            if (tableIt == tables_.end())
            {
                return SC_E_REFERENCE_INVALID;
            }

            auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
            if (table == nullptr)
            {
                return SC_E_REFERENCE_INVALID;
            }

            SCRecordPtr record;
            const ErrorCode recordRc = table->GetRecord(targetRecordId, record);
            if (Failed(recordRc))
            {
                return recordRc;
            }
            if (!record || record->IsDeleted())
            {
                return SC_E_REFERENCE_INVALID;
            }

            return record->GetValue(relationColumn.referenceStorageColumn.c_str(), outValue);
        }

        ErrorCode SqliteDatabase::ResolveRelationWriteValue(const SCColumnDef& relationColumn,
                                                            const SCValue& inputValue,
                                                            SCValue* outValue) const
        {
            if (outValue == nullptr)
            {
                return SC_E_POINTER;
            }
            if (relationColumn.referenceStorageColumn.empty())
            {
                *outValue = inputValue;
                return SC_OK;
            }

            RecordId targetRecordId = 0;
            if (Succeeded(ResolveRelationTargetRecordId(relationColumn, inputValue, &targetRecordId)))
            {
                return ResolveRelationStoredValue(relationColumn, targetRecordId, outValue);
            }

            const SCColumnDef* displayColumn = FindRelationDisplayColumn(relationColumn);
            if (displayColumn == nullptr)
            {
                return SC_E_REFERENCE_INVALID;
            }

            const auto tableIt = tables_.find(relationColumn.referenceTable);
            if (tableIt == tables_.end())
            {
                return SC_E_REFERENCE_INVALID;
            }

            auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
            if (table == nullptr)
            {
                return SC_E_REFERENCE_INVALID;
            }

            bool found = false;
            RecordId foundId = 0;
            for (const auto& [candidateId, candidateData] : table->Records())
            {
                if (candidateData == nullptr || candidateData->state == RecordState::Deleted)
                {
                    continue;
                }

                SCRecordPtr record;
                if (Failed(table->GetRecord(candidateId, record)) || !record)
                {
                    continue;
                }

                SCValue displayValue;
                if (Failed(record->GetValue(displayColumn->name.c_str(), &displayValue)))
                {
                    continue;
                }

                if (displayValue != inputValue)
                {
                    continue;
                }

                if (found)
                {
                    return SC_E_REFERENCE_INVALID;
                }
                found = true;
                foundId = candidateId;
            }

            if (!found)
            {
                return SC_E_REFERENCE_INVALID;
            }

            return ResolveRelationStoredValue(relationColumn, foundId, outValue);
        }

        ErrorCode SqliteDatabase::ResolveRelationTargetRecordId(const SCColumnDef& relationColumn,
                                                                const SCValue& storedValue,
                                                                RecordId* outRecordId) const
        {
            if (outRecordId == nullptr)
            {
                return SC_E_POINTER;
            }
            if (relationColumn.referenceStorageColumn.empty())
            {
                return storedValue.AsRecordId(outRecordId);
            }

            const auto tableIt = tables_.find(relationColumn.referenceTable);
            if (tableIt == tables_.end())
            {
                return SC_E_REFERENCE_INVALID;
            }

            auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
            if (table == nullptr)
            {
                return SC_E_REFERENCE_INVALID;
            }

            bool found = false;
            RecordId foundId = 0;
            for (const auto& [candidateId, candidateData] : table->Records())
            {
                if (candidateData == nullptr || candidateData->state == RecordState::Deleted)
                {
                    continue;
                }

                SCRecordPtr record;
                if (Failed(table->GetRecord(candidateId, record)) || !record)
                {
                    continue;
                }

                SCValue candidateStoredValue;
                if (Failed(record->GetValue(relationColumn.referenceStorageColumn.c_str(), &candidateStoredValue)))
                {
                    continue;
                }

                if (candidateStoredValue != storedValue)
                {
                    continue;
                }
                if (found)
                {
                    return SC_E_REFERENCE_INVALID;
                }
                found = true;
                foundId = candidateId;
            }

            if (!found)
            {
                return SC_E_REFERENCE_INVALID;
            }

            *outRecordId = foundId;
            return SC_OK;
        }

        ErrorCode SqliteDatabase::ResolveRelationDisplayValue(const SCColumnDef& relationColumn,
                                                              const SCValue& storedValue,
                                                              std::wstring* outValue) const
        {
            if (outValue == nullptr)
            {
                return SC_E_POINTER;
            }
            if (relationColumn.referenceTable.empty())
            {
                return SC_E_REFERENCE_INVALID;
            }

            const std::wstring& displayColumnName = relationColumn.referenceDisplayColumn.empty()
                                                        ? relationColumn.referenceStorageColumn
                                                        : relationColumn.referenceDisplayColumn;
            if (displayColumnName.empty())
            {
                return storedValue.AsStringCopy(outValue);
            }

            RecordId targetRecordId = 0;
            const ErrorCode targetRc = ResolveRelationTargetRecordId(relationColumn, storedValue, &targetRecordId);
            if (Failed(targetRc))
            {
                return targetRc;
            }

            const auto tableIt = tables_.find(relationColumn.referenceTable);
            if (tableIt == tables_.end())
            {
                return SC_E_REFERENCE_INVALID;
            }

            auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
            if (table == nullptr)
            {
                return SC_E_REFERENCE_INVALID;
            }

            SCRecordPtr record;
            const ErrorCode recordRc = table->GetRecord(targetRecordId, record);
            if (Failed(recordRc))
            {
                return recordRc;
            }

            SCValue displayValue;
            const ErrorCode valueRc = record->GetValue(displayColumnName.c_str(), &displayValue);
            if (valueRc == SC_E_VALUE_IS_NULL)
            {
                outValue->clear();
                return SC_OK;
            }
            if (Failed(valueRc))
            {
                return valueRc;
            }

            switch (displayValue.GetKind())
            {
                case ValueKind::Int64: {
                    std::int64_t v = 0;
                    if (displayValue.AsInt64(&v) == SC_OK)
                    {
                        *outValue = std::to_wstring(v);
                        return SC_OK;
                    }
                    break;
                }
                case ValueKind::Double: {
                    double v = 0.0;
                    if (displayValue.AsDouble(&v) == SC_OK)
                    {
                        std::wstringstream stream;
                        stream << v;
                        *outValue = stream.str();
                        return SC_OK;
                    }
                    break;
                }
                case ValueKind::Bool: {
                    bool v = false;
                    if (displayValue.AsBool(&v) == SC_OK)
                    {
                        *outValue = v ? L"true" : L"false";
                        return SC_OK;
                    }
                    break;
                }
                case ValueKind::String:
                    return displayValue.AsStringCopy(outValue);
                case ValueKind::RecordId: {
                    RecordId v = 0;
                    if (displayValue.AsRecordId(&v) == SC_OK)
                    {
                        *outValue = std::to_wstring(v);
                        return SC_OK;
                    }
                    break;
                }
                case ValueKind::Enum:
                    return displayValue.AsEnumCopy(outValue);
                case ValueKind::Binary:
                case ValueKind::Null:
                default:
                    break;
            }
            *outValue = L"";
            return SC_OK;
        }

        ErrorCode SqliteDatabase::ValidateWrite(SqliteTable* table,
                                                const std::shared_ptr<SqliteRecordData>& data,
                                                const std::wstring& fieldName,
                                                const SCValue& value)
        {
            if (!activeEdit_)
            {
                return SC_E_NO_ACTIVE_EDIT;
            }
            if (data->state == RecordState::Deleted)
            {
                return SC_E_RECORD_DELETED;
            }

            const SCColumnDef* column = table->Schema()->FindColumnDef(fieldName);
            if (column == nullptr)
            {
                return SC_E_COLUMN_NOT_FOUND;
            }
            if (!column->editable)
            {
                return SC_E_READ_ONLY_COLUMN;
            }

            if (column->columnKind == ColumnKind::Relation &&
                !column->referenceStorageColumn.empty())
            {
                if (value.IsNull())
                {
                    return column->nullable ? SC_OK : SC_E_VALUE_IS_NULL;
                }
                SCValue normalized;
                const ErrorCode refRc = ResolveRelationWriteValue(*column, value, &normalized);
                if (Failed(refRc))
                {
                    return refRc;
                }
                return SC_OK;
            }

            const ErrorCode validate = ValidateValueKind(column->valueKind, value, column->nullable);
            if (Failed(validate))
            {
                return validate;
            }

            if (column->columnKind == ColumnKind::Relation && !value.IsNull())
            {
                RecordId refId = 0;
                const ErrorCode refRc = value.AsRecordId(&refId);
                if (Failed(refRc))
                {
                    return refRc;
                }
                if (!column->referenceTable.empty())
                {
                    const auto targetIt = tables_.find(column->referenceTable);
                    if (targetIt == tables_.end())
                    {
                        return SC_E_REFERENCE_INVALID;
                    }
                    auto target = static_cast<SqliteTable*>(targetIt->second.Get())->FindRecordData(refId);
                    if (!target || target->state == RecordState::Deleted)
                    {
                        return SC_E_REFERENCE_INVALID;
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::ValidateColumnDefForSchema(SqliteSchema* schema, const SCColumnDef& def) const
        {
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            const ErrorCode shapeRc = ValidateColumnDefShape(def);
            if (Failed(shapeRc))
            {
                return shapeRc;
            }

            const ErrorCode relationRc = ValidateRelationColumnDef(this, def);
            if (Failed(relationRc))
            {
                return relationRc;
            }

            if (!def.nullable && def.defaultValue.IsNull() && HasAliveRecords(schema))
            {
                return SC_E_SCHEMA_VIOLATION;
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::ValidateColumnDefForUpdate(SqliteSchema* schema, const SCColumnDef& def) const
        {
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            const ErrorCode shapeRc = ValidateColumnDefShape(def);
            if (Failed(shapeRc))
            {
                return shapeRc;
            }

            const ErrorCode relationRc = ValidateRelationColumnDef(this, def);
            if (Failed(relationRc))
            {
                return relationRc;
            }

            const SCColumnDef* previousDef = schema->FindColumnDef(def.name);
            if (previousDef == nullptr)
            {
                return SC_E_COLUMN_NOT_FOUND;
            }

            if (!def.nullable)
            {
                auto* table = FindTableByRowId(schema->TableRowId());
                if (table == nullptr)
                {
                    return SC_E_FAIL;
                }

                for (const auto& [_, data] : table->Records())
                {
                    if (data == nullptr || data->state == RecordState::Deleted)
                    {
                        continue;
                    }

                    SCRecordPtr record;
                    const ErrorCode recordRc = table->GetRecord(data->id, record);
                    if (Failed(recordRc) || !record)
                    {
                        return Failed(recordRc) ? recordRc : SC_E_FAIL;
                    }

                    SCValue value;
                    const ErrorCode valueRc = record->GetValue(def.name.c_str(), &value);
                    if (valueRc == SC_E_VALUE_IS_NULL)
                    {
                        return SC_E_SCHEMA_VIOLATION;
                    }
                    if (Failed(valueRc))
                    {
                        return valueRc;
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::ValidateConstraintDefForSchema(SqliteSchema* schema, const SCConstraintDef& def) const
        {
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            const ErrorCode shapeRc = ValidateConstraintDefShape(def);
            if (Failed(shapeRc))
            {
                return shapeRc;
            }

            std::set<std::wstring> seenColumns;
            for (const std::wstring& columnName : def.columns)
            {
                if (columnName.empty())
                {
                    return SC_E_INVALIDARG;
                }
                if (schema->FindColumnDef(columnName) == nullptr)
                {
                    return SC_E_COLUMN_NOT_FOUND;
                }
                if (!seenColumns.insert(columnName).second)
                {
                    return SC_E_SCHEMA_VIOLATION;
                }
            }

            for (const std::wstring& columnName : def.referencedColumns)
            {
                if (columnName.empty())
                {
                    return SC_E_INVALIDARG;
                }
            }

            if (def.kind == SCConstraintKind::ForeignKey)
            {
                const auto tableIt = tables_.find(def.referencedTable);
                if (tableIt == tables_.end())
                {
                    return SC_E_TABLE_NOT_FOUND;
                }

                if (!IsForeignKeyActionValid(def.onDelete) || !IsForeignKeyActionValid(def.onUpdate))
                {
                    return SC_E_SCHEMA_VIOLATION;
                }

                if (!def.referencedColumns.empty())
                {
                    SCSchemaPtr referencedSchema;
                    const ErrorCode schemaRc = tableIt->second->GetSchema(referencedSchema);
                    if (Failed(schemaRc) || !referencedSchema)
                    {
                        return Failed(schemaRc) ? schemaRc : SC_E_FAIL;
                    }

                    for (const std::wstring& columnName : def.referencedColumns)
                    {
                        SCColumnDef ignored;
                        const ErrorCode columnRc = referencedSchema->FindColumn(columnName.c_str(), &ignored);
                        if (Failed(columnRc))
                        {
                            return columnRc;
                        }
                    }
                }

                if (def.onDelete == SCForeignKeyAction::SetNull || def.onUpdate == SCForeignKeyAction::SetNull)
                {
                    for (const std::wstring& columnName : def.columns)
                    {
                        const SCColumnDef* sourceColumn = schema->FindColumnDef(columnName);
                        if (sourceColumn == nullptr || !sourceColumn->nullable)
                        {
                            return SC_E_SCHEMA_VIOLATION;
                        }
                    }
                }

                if (def.onDelete == SCForeignKeyAction::SetDefault ||
                    def.onUpdate == SCForeignKeyAction::SetDefault)
                {
                    for (const std::wstring& columnName : def.columns)
                    {
                        const SCColumnDef* sourceColumn = schema->FindColumnDef(columnName);
                        if (sourceColumn == nullptr)
                        {
                            return SC_E_COLUMN_NOT_FOUND;
                        }
                        if (sourceColumn->defaultValue.IsNull() && !sourceColumn->nullable)
                        {
                            return SC_E_SCHEMA_VIOLATION;
                        }
                        if (!sourceColumn->defaultValue.IsNull() &&
                            sourceColumn->defaultValue.GetKind() != sourceColumn->valueKind)
                        {
                            return SC_E_SCHEMA_VIOLATION;
                        }
                    }
                }
            } else if (def.kind == SCConstraintKind::Check)
            {
                std::set<std::wstring> allowedColumns;
                for (const std::wstring& columnName : def.columns)
                {
                    allowedColumns.insert(columnName);
                }
                ConstraintExpressionParser parser(def.checkExpression, std::move(allowedColumns));
                ConstraintExpressionAst ast;
                const ErrorCode parseRc = parser.Parse(&ast);
                if (Failed(parseRc))
                {
                    return parseRc;
                }
            }

            if (def.kind == SCConstraintKind::PrimaryKey || def.kind == SCConstraintKind::Unique ||
                def.kind == SCConstraintKind::ForeignKey || def.kind == SCConstraintKind::Check)
            {
                auto* table = FindTableByRowId(schema->TableRowId());
                if (table != nullptr)
                {
                    ErrorCode validationRc = SC_OK;
                    switch (def.kind)
                    {
                        case SCConstraintKind::PrimaryKey:
                        case SCConstraintKind::Unique:
                            validationRc = ValidateConstraintUniqueness(table, def, std::shared_ptr<SqliteRecordData>{});
                            break;
                        case SCConstraintKind::Check:
                            validationRc = ValidateCheckConstraint(table, def, std::shared_ptr<SqliteRecordData>{});
                            break;
                        case SCConstraintKind::ForeignKey:
                            validationRc = ValidateForeignKeyConstraint(table, def, std::shared_ptr<SqliteRecordData>{});
                            break;
                        default:
                            break;
                    }
                    if (Failed(validationRc))
                    {
                        return validationRc;
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::ValidateIndexDefForSchema(SqliteSchema* schema, const SCIndexDef& def) const
        {
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            const ErrorCode shapeRc = ValidateIndexDefShape(def);
            if (Failed(shapeRc))
            {
                return shapeRc;
            }

            if (def.columns.size() > kCompositeIndexMaxColumns)
            {
                return SC_E_NOTIMPL;
            }

            std::set<std::wstring> seenColumns;
            for (const SCIndexColumnDef& column : def.columns)
            {
                if (schema->FindColumnDef(column.columnName) == nullptr)
                {
                    return SC_E_COLUMN_NOT_FOUND;
                }
                if (!seenColumns.insert(column.columnName).second)
                {
                    return SC_E_SCHEMA_VIOLATION;
                }
            }

            return SC_OK;
        }

        SqliteTable* SqliteDatabase::FindTableByRowId(std::int64_t tableRowId) const
        {
            for (const auto& [_, tableRef] : tables_)
            {
                auto* table = static_cast<SqliteTable*>(tableRef.Get());
                if (table != nullptr && table->TableRowId() == tableRowId)
                {
                    return table;
                }
            }
            return nullptr;
        }

        void SqliteDatabase::CollectTouchedTableNames(
            const JournalTransaction& tx,
            std::vector<std::wstring>* outTableNames,
            bool reverseRenameResolution) const
        {
            if (outTableNames == nullptr)
            {
                return;
            }

            outTableNames->clear();
            const bool hasRenameTable =
                JournalTransactionContainsRenameTable(tx);
            for (const auto& entry : tx.entries)
            {
                const std::wstring resolvedName =
                    hasRenameTable
                        ? ResolveJournalTableNameToReplayState(
                              tx,
                              entry.tableName,
                              reverseRenameResolution)
                        : entry.tableName;
                if (resolvedName.empty())
                {
                    continue;
                }
                if (std::find(outTableNames->begin(),
                              outTableNames->end(),
                              resolvedName) == outTableNames->end())
                {
                    outTableNames->push_back(resolvedName);
                }
            }
        }

        ErrorCode SqliteDatabase::CaptureDeferredRenameState(
            const std::wstring& oldName,
            const std::wstring& newName,
            DeferredRenameState* outState) const
        {
            if (outState == nullptr)
            {
                return SC_E_POINTER;
            }
            if (EqualsIgnoreCase(oldName, newName))
            {
                return SC_E_INVALIDARG;
            }

            auto tableIt = tables_.find(oldName);
            if (tableIt == tables_.end())
            {
                for (auto it = tables_.begin(); it != tables_.end(); ++it)
                {
                    if (EqualsIgnoreCase(it->first, oldName))
                    {
                        tableIt = it;
                        break;
                    }
                }
            }
            if (tableIt == tables_.end())
            {
                return SC_E_TABLE_NOT_FOUND;
            }

            for (const auto& [existingName, existingTable] : tables_)
            {
                (void)existingTable;
                if (EqualsIgnoreCase(existingName, newName) &&
                    !EqualsIgnoreCase(existingName, tableIt->first))
                {
                    return SC_E_SCHEMA_VIOLATION;
                }
            }

            DeferredRenameState capturedState;
            capturedState.tableRef = tableIt->second;
            capturedState.oldName = tableIt->first;
            capturedState.newName = newName;

            for (const auto& [otherName, otherTableRef] : tables_)
            {
                (void)otherName;
                auto* otherTable = static_cast<SqliteTable*>(otherTableRef.Get());
                if (otherTable == nullptr)
                {
                    continue;
                }

                SCSchemaPtr otherSchema;
                const ErrorCode schemaRc = otherTable->GetSchema(otherSchema);
                if (Failed(schemaRc) || !otherSchema)
                {
                    return Failed(schemaRc) ? schemaRc : SC_E_FAIL;
                }

                auto* sqliteOtherSchema =
                    static_cast<SqliteSchema*>(otherSchema.Get());
                if (sqliteOtherSchema == nullptr)
                {
                    return SC_E_FAIL;
                }

                std::int32_t columnCount = 0;
                const ErrorCode columnCountRc =
                    otherSchema->GetColumnCount(&columnCount);
                if (Failed(columnCountRc))
                {
                    return columnCountRc;
                }

                for (std::int32_t columnIndex = 0; columnIndex < columnCount;
                     ++columnIndex)
                {
                    SCColumnDef column;
                    const ErrorCode columnRc =
                        otherSchema->GetColumn(columnIndex, &column);
                    if (Failed(columnRc))
                    {
                        return columnRc;
                    }

                    if (column.columnKind == ColumnKind::Relation &&
                        EqualsIgnoreCase(column.referenceTable,
                                         capturedState.oldName))
                    {
                        capturedState.relationColumns.push_back(
                            DeferredRenameColumnSnapshot{sqliteOtherSchema, column});
                    }
                }

                std::int32_t constraintCount = 0;
                const ErrorCode constraintCountRc =
                    otherSchema->GetConstraintCount(&constraintCount);
                if (Failed(constraintCountRc))
                {
                    return constraintCountRc;
                }

                for (std::int32_t constraintIndex = 0;
                     constraintIndex < constraintCount; ++constraintIndex)
                {
                    SCConstraintDef constraint;
                    const ErrorCode constraintRc =
                        otherSchema->GetConstraint(constraintIndex, &constraint);
                    if (Failed(constraintRc))
                    {
                        return constraintRc;
                    }

                    if (constraint.kind == SCConstraintKind::ForeignKey &&
                        EqualsIgnoreCase(constraint.referencedTable,
                                         capturedState.oldName))
                    {
                        capturedState.foreignKeyConstraints.push_back(
                            DeferredRenameConstraintSnapshot{
                                sqliteOtherSchema, constraint});
                    }
                }
            }

            *outState = std::move(capturedState);
            return SC_OK;
        }

        void SqliteDatabase::ApplyDeferredRenameWorkingState(
            const DeferredRenameState& state)
        {
            auto eraseCurrentEntry = [&]() {
                for (auto it = tables_.begin(); it != tables_.end(); ++it)
                {
                    if (it->second.Get() == state.tableRef.Get())
                    {
                        tables_.erase(it);
                        return;
                    }
                }
            };

            eraseCurrentEntry();
            tables_.emplace(state.newName, state.tableRef);

            auto* table = static_cast<SqliteTable*>(state.tableRef.Get());
            if (table != nullptr)
            {
                table->SetName(state.newName);
                if (auto* schema = table->Schema())
                {
                    schema->SetTableName(state.newName);
                }
            }

            for (const auto& snapshot : state.relationColumns)
            {
                if (snapshot.schema == nullptr)
                {
                    continue;
                }
                SCColumnDef updatedColumn = snapshot.column;
                updatedColumn.referenceTable = state.newName;
                snapshot.schema->ReplaceColumn(updatedColumn);
            }

            for (const auto& snapshot : state.foreignKeyConstraints)
            {
                if (snapshot.schema == nullptr)
                {
                    continue;
                }
                SCConstraintDef updatedConstraint = snapshot.constraint;
                updatedConstraint.referencedTable = state.newName;
                snapshot.schema->ReplaceConstraint(updatedConstraint);
            }

            MarkReferenceIndexDirty();
            MarkForeignKeyReferenceCacheDirty();
        }

        void SqliteDatabase::RollbackDeferredRenameWorkingState(
            const DeferredRenameState& state)
        {
            auto eraseCurrentEntry = [&]() {
                for (auto it = tables_.begin(); it != tables_.end(); ++it)
                {
                    if (it->second.Get() == state.tableRef.Get())
                    {
                        tables_.erase(it);
                        return;
                    }
                }
            };

            eraseCurrentEntry();
            tables_.emplace(state.oldName, state.tableRef);

            auto* table = static_cast<SqliteTable*>(state.tableRef.Get());
            if (table != nullptr)
            {
                table->SetName(state.oldName);
                if (auto* schema = table->Schema())
                {
                    schema->SetTableName(state.oldName);
                }
            }

            for (const auto& snapshot : state.relationColumns)
            {
                if (snapshot.schema != nullptr)
                {
                    snapshot.schema->ReplaceColumn(snapshot.column);
                }
            }

            for (const auto& snapshot : state.foreignKeyConstraints)
            {
                if (snapshot.schema != nullptr)
                {
                    snapshot.schema->ReplaceConstraint(snapshot.constraint);
                }
            }

            MarkReferenceIndexDirty();
            MarkForeignKeyReferenceCacheDirty();
        }

        DeferredRenameState* SqliteDatabase::FindDeferredRenameState(
            const std::wstring& oldName,
            const std::wstring& newName)
        {
            for (auto it = activeSchemaOps_.rbegin();
                 it != activeSchemaOps_.rend(); ++it)
            {
                if (it->kind != DeferredSchemaOp::Kind::RenameTable)
                {
                    continue;
                }
                if (EqualsIgnoreCase(it->rename.oldName, oldName) &&
                    EqualsIgnoreCase(it->rename.newName, newName))
                {
                    return &it->rename;
                }
            }
            return nullptr;
        }

        const DeferredRenameState* SqliteDatabase::FindDeferredRenameState(
            const std::wstring& oldName,
            const std::wstring& newName) const
        {
            for (auto it = activeSchemaOps_.rbegin();
                 it != activeSchemaOps_.rend(); ++it)
            {
                if (it->kind != DeferredSchemaOp::Kind::RenameTable)
                {
                    continue;
                }
                if (EqualsIgnoreCase(it->rename.oldName, oldName) &&
                    EqualsIgnoreCase(it->rename.newName, newName))
                {
                    return &it->rename;
                }
            }
            return nullptr;
        }

        ErrorCode SqliteDatabase::RecordDeferredRenameTable(
            const std::wstring& oldName,
            const std::wstring& newName)
        {
            DeferredRenameState capturedState;
            const ErrorCode captureRc =
                CaptureDeferredRenameState(oldName, newName, &capturedState);
            if (Failed(captureRc))
            {
                return captureRc;
            }

            ApplyDeferredRenameWorkingState(capturedState);

            DeferredSchemaOp op;
            op.kind = DeferredSchemaOp::Kind::RenameTable;
            op.rename = std::move(capturedState);
            activeSchemaOps_.push_back(std::move(op));
            return SC_OK;
        }

        bool SqliteDatabase::JournalTransactionContainsRenameTable(
            const JournalTransaction& tx) const
        {
            return std::any_of(
                tx.entries.begin(),
                tx.entries.end(),
                [](const JournalEntry& entry) {
                    return entry.op == JournalOp::RenameTable;
                });
        }

        std::wstring SqliteDatabase::ResolveJournalTableNameToReplayState(
            const JournalTransaction& tx,
            const std::wstring& tableName,
            bool reverseRenameResolution) const
        {
            std::wstring resolvedName = tableName;
            if (resolvedName.empty())
            {
                return resolvedName;
            }

            auto applyRename = [&resolvedName](const JournalEntry& entry,
                                               bool reverseDirection) {
                if (entry.op != JournalOp::RenameTable)
                {
                    return;
                }

                std::wstring oldName;
                std::wstring newName;
                if (entry.oldValue.AsStringCopy(&oldName) != SC_OK ||
                    entry.newValue.AsStringCopy(&newName) != SC_OK)
                {
                    return;
                }

                const std::wstring& fromName = reverseDirection ? newName : oldName;
                const std::wstring& toName = reverseDirection ? oldName : newName;
                if (EqualsIgnoreCase(resolvedName, fromName))
                {
                    resolvedName = toName;
                }
            };

            if (reverseRenameResolution)
            {
                for (auto it = tx.entries.rbegin(); it != tx.entries.rend(); ++it)
                {
                    applyRename(*it, true);
                }
            }
            else
            {
                for (const auto& entry : tx.entries)
                {
                    applyRename(entry, false);
                }
            }

            return resolvedName;
        }

        bool SqliteDatabase::JournalEntryMatchesCurrentTableName(
            const JournalEntry& entry,
            const std::wstring& tableName) const
        {
            if (EqualsIgnoreCase(entry.tableName, tableName))
            {
                return true;
            }
            if (!JournalTransactionContainsRenameTable(activeJournal_))
            {
                return false;
            }
            return EqualsIgnoreCase(
                ResolveJournalTableNameToReplayState(activeJournal_,
                                                     entry.tableName,
                                                     false),
                tableName);
        }

        ErrorCode SqliteDatabase::ReadConstraintValue(SqliteTable* table,
                                                      const SqliteRecordData& recordData,
                                                      const std::wstring& columnName,
                                                      const std::wstring* overrideFieldName,
                                                      const SCValue* overrideValue,
                                                      SCValue* outValue,
                                                      bool* outIsNull) const
        {
            if (table == nullptr || outValue == nullptr || outIsNull == nullptr)
            {
                return SC_E_POINTER;
            }

            SCSchemaPtr schema;
            const ErrorCode schemaRc = table->GetSchema(schema);
            if (Failed(schemaRc) || !schema)
            {
                return Failed(schemaRc) ? schemaRc : SC_E_FAIL;
            }

            SCColumnDef column;
            const ErrorCode columnRc = schema->FindColumn(columnName.c_str(), &column);
            if (Failed(columnRc))
            {
                return columnRc;
            }

            if (overrideFieldName != nullptr && overrideValue != nullptr && EqualsIgnoreCase(*overrideFieldName, column.name))
            {
                *outValue = *overrideValue;
            } else
            {
                const auto valueIt = recordData.values.find(column.name);
                *outValue = (valueIt != recordData.values.end()) ? valueIt->second : column.defaultValue;
            }

            *outIsNull = outValue->IsNull();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::ValidateConstraintUniqueness(SqliteTable* table,
                                                               const SCConstraintDef& constraint,
                                                               const std::shared_ptr<SqliteRecordData>& candidateData,
                                                               const std::wstring* overrideFieldName,
                                                               const SCValue* overrideValue) const
        {
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }
            if (constraint.kind != SCConstraintKind::PrimaryKey && constraint.kind != SCConstraintKind::Unique)
            {
                return SC_OK;
            }

            auto buildKeySignature = [&](const std::shared_ptr<SqliteRecordData>& recordData,
                                         const std::wstring* fieldOverrideName,
                                         const SCValue* fieldOverrideValue,
                                         std::wstring* outSignature,
                                         bool* outHasNull) -> ErrorCode {
                if (recordData == nullptr || outSignature == nullptr || outHasNull == nullptr)
                {
                    return SC_E_POINTER;
                }

                outSignature->clear();
                *outHasNull = false;

                for (const std::wstring& columnName : constraint.columns)
                {
                    SCValue value;
                    bool columnIsNull = false;
                    const ErrorCode valueRc = ReadConstraintValue(table,
                                                                  *recordData,
                                                                  columnName,
                                                                  fieldOverrideName,
                                                                  fieldOverrideValue,
                                                                  &value,
                                                                  &columnIsNull);
                    if (Failed(valueRc))
                    {
                        return valueRc;
                    }
                    if (columnIsNull)
                    {
                        *outHasNull = true;
                    }
                    AppendToken(outSignature, SerializeConstraintKeyValue(value));
                }

                return SC_OK;
            };

            if (candidateData != nullptr)
            {
                std::wstring candidateSignature;
                bool candidateHasNull = false;
                const ErrorCode candidateKeyRc =
                    buildKeySignature(candidateData, overrideFieldName, overrideValue, &candidateSignature, &candidateHasNull);
                if (Failed(candidateKeyRc))
                {
                    return candidateKeyRc;
                }

                if (candidateHasNull)
                {
                    if (constraint.kind == SCConstraintKind::PrimaryKey)
                    {
                        SetConstraintViolation(SCConstraintViolationInfo{
                            table->Name(),
                            constraint.name,
                            constraint.kind,
                            constraint.columns,
                            candidateData->id,
                            0,
                            L"Primary key columns cannot be null.",
                        });
                        return SC_E_CONSTRAINT_VIOLATION;
                    }
                    return SC_OK;
                }

                for (const auto& [recordId, recordData] : table->Records())
                {
                    if (recordData == nullptr || recordData->state == RecordState::Deleted ||
                        recordData.get() == candidateData.get())
                    {
                        continue;
                    }

                    std::wstring otherSignature;
                    bool otherHasNull = false;
                    const ErrorCode otherKeyRc =
                        buildKeySignature(recordData, nullptr, nullptr, &otherSignature, &otherHasNull);
                    if (Failed(otherKeyRc))
                    {
                        return otherKeyRc;
                    }
                    if (otherHasNull || otherSignature != candidateSignature)
                    {
                        continue;
                    }

                    SetConstraintViolation(SCConstraintViolationInfo{
                        table->Name(),
                        constraint.name,
                        constraint.kind,
                        constraint.columns,
                        candidateData->id,
                        recordData->id,
                        L"Unique key already exists.",
                    });
                    return SC_E_CONSTRAINT_VIOLATION;
                }
                return SC_OK;
            }

            std::unordered_set<std::wstring> seenKeySignatures;
            seenKeySignatures.reserve(table->Records().size());

            for (const auto& [recordId, recordData] : table->Records())
            {
                if (recordData == nullptr || recordData->state == RecordState::Deleted)
                {
                    continue;
                }

                std::wstring keySignature;
                bool hasNull = false;
                const ErrorCode keyRc = buildKeySignature(recordData, nullptr, nullptr, &keySignature, &hasNull);
                if (Failed(keyRc))
                {
                    return keyRc;
                }

                if (hasNull)
                {
                    if (constraint.kind == SCConstraintKind::PrimaryKey)
                    {
                        SetConstraintViolation(SCConstraintViolationInfo{
                            table->Name(),
                            constraint.name,
                            constraint.kind,
                            constraint.columns,
                            recordData->id,
                            0,
                            L"Primary key columns cannot be null.",
                        });
                        return SC_E_CONSTRAINT_VIOLATION;
                    }
                    continue;
                }

                if (!seenKeySignatures.insert(keySignature).second)
                {
                    SetConstraintViolation(SCConstraintViolationInfo{
                        table->Name(),
                        constraint.name,
                        constraint.kind,
                        constraint.columns,
                        recordData->id,
                        0,
                        L"Unique key already exists.",
                    });
                    return SC_E_CONSTRAINT_VIOLATION;
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::ValidateCheckConstraint(SqliteTable* table,
                                                          const SCConstraintDef& constraint,
                                                          const std::shared_ptr<SqliteRecordData>& candidateData,
                                                          const std::wstring* overrideFieldName,
                                                          const SCValue* overrideValue) const
        {
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }
            if (constraint.kind != SCConstraintKind::Check)
            {
                return SC_OK;
            }
            if (overrideFieldName != nullptr)
            {
                const auto usesField = std::any_of(constraint.columns.begin(),
                                                   constraint.columns.end(),
                                                   [overrideFieldName](const std::wstring& columnName) {
                                                       return EqualsIgnoreCase(columnName, *overrideFieldName);
                                                   });
                if (!usesField)
                {
                    return SC_OK;
                }
            }

            if (candidateData == nullptr)
            {
                for (const auto& [_, recordData] : table->Records())
                {
                    if (recordData == nullptr || recordData->state == RecordState::Deleted)
                    {
                        continue;
                    }
                    const ErrorCode rc =
                        ValidateCheckConstraint(table, constraint, recordData, overrideFieldName, overrideValue);
                    if (Failed(rc))
                    {
                        return rc;
                    }
                }
                return SC_OK;
            }

            SCSchemaPtr schema;
            const ErrorCode schemaRc = table->GetSchema(schema);
            if (Failed(schemaRc) || !schema)
            {
                return Failed(schemaRc) ? schemaRc : SC_E_FAIL;
            }

            std::set<std::wstring> allowedColumns(constraint.columns.begin(), constraint.columns.end());
            ConstraintExpressionParser parser(constraint.checkExpression, std::move(allowedColumns));
            ConstraintExpressionAst ast;
            const ErrorCode parseRc = parser.Parse(&ast);
            if (Failed(parseRc))
            {
                return parseRc;
            }

            std::unordered_map<std::wstring, SCValue> values;
            values.reserve(constraint.columns.size());
            for (const std::wstring& columnName : constraint.columns)
            {
                SCValue value;
                bool isNull = false;
                const ErrorCode valueRc = ReadConstraintValue(table,
                                                              *candidateData,
                                                              columnName,
                                                              overrideFieldName,
                                                              overrideValue,
                                                              &value,
                                                              &isNull);
                if (Failed(valueRc))
                {
                    return valueRc;
                }
                values[columnName] = value;
            }

            SCValue result;
            const ErrorCode evalRc = EvaluateConstraintExpressionNode(*ast.root, values, &result);
            if (Failed(evalRc))
            {
                return evalRc;
            }

            if (result.IsNull())
            {
                return SC_OK;
            }
            if (IsTruthyValue(result))
            {
                return SC_OK;
            }

            SetConstraintViolation(SCConstraintViolationInfo{
                table->Name(),
                constraint.name,
                constraint.kind,
                constraint.columns,
                candidateData != nullptr ? candidateData->id : 0,
                0,
                L"Check constraint evaluated to false.",
            });
            return SC_E_CONSTRAINT_VIOLATION;
        }

        ErrorCode SqliteDatabase::ValidateForeignKeyConstraint(SqliteTable* table,
                                                               const SCConstraintDef& constraint,
                                                               const std::shared_ptr<SqliteRecordData>& candidateData,
                                                               const std::wstring* overrideFieldName,
                                                               const SCValue* overrideValue) const
        {
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }
            if (constraint.kind != SCConstraintKind::ForeignKey)
            {
                return SC_OK;
            }
            if (constraint.referencedTable.empty())
            {
                return SC_E_SCHEMA_VIOLATION;
            }
            if (overrideFieldName != nullptr)
            {
                const auto usesField = std::any_of(constraint.columns.begin(),
                                                   constraint.columns.end(),
                                                   [overrideFieldName](const std::wstring& columnName) {
                                                       return EqualsIgnoreCase(columnName, *overrideFieldName);
                                                   });
                if (!usesField)
                {
                    return SC_OK;
                }
            }

            if (candidateData == nullptr)
            {
                for (const auto& [_, recordData] : table->Records())
                {
                    if (recordData == nullptr || recordData->state == RecordState::Deleted)
                    {
                        continue;
                    }
                    const ErrorCode rc =
                        ValidateForeignKeyConstraint(table, constraint, recordData, overrideFieldName, overrideValue);
                    if (Failed(rc))
                    {
                        return rc;
                    }
                }
                return SC_OK;
            }

            SCSchemaPtr schema;
            const ErrorCode schemaRc = table->GetSchema(schema);
            if (Failed(schemaRc) || !schema)
            {
                return Failed(schemaRc) ? schemaRc : SC_E_FAIL;
            }

            auto targetIt = tables_.find(constraint.referencedTable);
            if (targetIt == tables_.end())
            {
                return SC_E_TABLE_NOT_FOUND;
            }

            auto* targetTable = static_cast<SqliteTable*>(targetIt->second.Get());
            if (targetTable == nullptr)
            {
                return SC_E_FAIL;
            }

            SCSchemaPtr targetSchema;
            const ErrorCode targetSchemaRc = targetTable->GetSchema(targetSchema);
            if (Failed(targetSchemaRc) || !targetSchema)
            {
                return Failed(targetSchemaRc) ? targetSchemaRc : SC_E_FAIL;
            }

            std::vector<SCValue> sourceValues;
            sourceValues.reserve(constraint.columns.size());
            for (std::size_t i = 0; i < constraint.columns.size(); ++i)
            {
                SCValue value;
                bool isNull = false;
                const ErrorCode valueRc = ReadConstraintValue(table,
                                                              *candidateData,
                                                              constraint.columns[i],
                                                              overrideFieldName,
                                                              overrideValue,
                                                              &value,
                                                              &isNull);
                if (Failed(valueRc))
                {
                    return valueRc;
                }
                if (isNull)
                {
                    return SC_OK;
                }
                sourceValues.push_back(value);
            }

            bool matched = false;
            for (const auto& [candidateId, candidateDataIt] : targetTable->Records())
            {
                if (candidateDataIt == nullptr || candidateDataIt->state == RecordState::Deleted)
                {
                    continue;
                }

                bool rowMatches = true;
                for (std::size_t i = 0; i < constraint.columns.size(); ++i)
                {
                    const std::wstring targetColumnName =
                        (i < constraint.referencedColumns.size() && !constraint.referencedColumns[i].empty())
                            ? constraint.referencedColumns[i]
                            : constraint.columns[i];

                    SCValue targetValue;
                    bool isNull = false;
                    const ErrorCode valueRc = ReadConstraintValue(targetTable,
                                                                  *candidateDataIt,
                                                                  targetColumnName,
                                                                  nullptr,
                                                                  nullptr,
                                                                  &targetValue,
                                                                  &isNull);
                    if (Failed(valueRc))
                    {
                        return valueRc;
                    }
                    if (isNull || targetValue != sourceValues[i])
                    {
                        rowMatches = false;
                        break;
                    }
                }

                if (!rowMatches)
                {
                    continue;
                }

                if (matched)
                {
                    SetConstraintViolation(SCConstraintViolationInfo{
                        table->Name(),
                        constraint.name,
                        constraint.kind,
                        constraint.columns,
                        candidateData != nullptr ? candidateData->id : 0,
                        candidateId,
                        L"Referenced key is not unique.",
                    });
                    return SC_E_CONSTRAINT_VIOLATION;
                }
                matched = true;
            }

            if (!matched)
            {
                SetConstraintViolation(SCConstraintViolationInfo{
                    table->Name(),
                    constraint.name,
                    constraint.kind,
                    constraint.columns,
                    candidateData != nullptr ? candidateData->id : 0,
                    0,
                    L"Referenced key does not exist.",
                });
                return SC_E_CONSTRAINT_VIOLATION;
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::ApplyForeignKeyActionsForTableDelete(
            SqliteTable* table, const std::shared_ptr<SqliteRecordData>& candidateData)
        {
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }
            if (candidateData == nullptr)
            {
                return SC_E_POINTER;
            }

            SCSchemaPtr schema;
            const ErrorCode schemaRc = table->GetSchema(schema);
            if (Failed(schemaRc) || !schema)
            {
                return Failed(schemaRc) ? schemaRc : SC_E_FAIL;
            }
            auto* sourceSchema = table->Schema();
            if (sourceSchema == nullptr)
            {
                return SC_E_FAIL;
            }

            struct Scope
            {
                std::unordered_set<std::wstring>* set{nullptr};
                std::wstring key;
                ~Scope()
                {
                    if (set != nullptr)
                    {
                        set->erase(key);
                    }
                }
            };

            const std::wstring parentScopeKey = table->Name() + L"|" + std::to_wstring(candidateData->id);
            if (!activeConstraintPropagationKeys_.insert(parentScopeKey).second)
            {
                return SC_OK;
            }
            Scope parentScope{&activeConstraintPropagationKeys_, parentScopeKey};

            for (const auto& [_, tableRef] : tables_)
            {
                auto* sourceTable = static_cast<SqliteTable*>(tableRef.Get());
                if (sourceTable == nullptr)
                {
                    continue;
                }

                SCSchemaPtr sourceSchema;
                if (Failed(sourceTable->GetSchema(sourceSchema)) || !sourceSchema)
                {
                    continue;
                }
                auto* sourceSchemaImpl = sourceTable->Schema();
                if (sourceSchemaImpl == nullptr)
                {
                    continue;
                }

                std::int32_t constraintCount = 0;
                if (Failed(sourceSchema->GetConstraintCount(&constraintCount)))
                {
                    continue;
                }

                for (std::int32_t constraintIndex = 0; constraintIndex < constraintCount; ++constraintIndex)
                {
                    SCConstraintDef constraint;
                    if (Failed(sourceSchema->GetConstraint(constraintIndex, &constraint)))
                    {
                        continue;
                    }
                    if (constraint.kind != SCConstraintKind::ForeignKey ||
                        !EqualsIgnoreCase(constraint.referencedTable, table->Name()))
                    {
                        continue;
                    }

                    std::vector<SCValue> parentValues;
                    parentValues.reserve(constraint.columns.size());
                    bool anyNull = false;
                    for (std::size_t i = 0; i < constraint.columns.size(); ++i)
                    {
                        SCValue value;
                        bool isNull = false;
                        const ErrorCode valueRc = ReadConstraintValue(table,
                                                                      *candidateData,
                                                                      ResolveForeignKeyReferencedColumn(constraint, i),
                                                                      nullptr,
                                                                      nullptr,
                                                                      &value,
                                                                      &isNull);
                        if (Failed(valueRc))
                        {
                            return valueRc;
                        }
                        if (isNull)
                        {
                            anyNull = true;
                        }
                        parentValues.push_back(value);
                    }
                    if (anyNull)
                    {
                        continue;
                    }

                    for (const auto& [childId, childData] : sourceTable->Records())
                    {
                        if (childData == nullptr || childData->state == RecordState::Deleted)
                        {
                            continue;
                        }

                        bool matches = true;
                        for (std::size_t i = 0; i < constraint.columns.size(); ++i)
                        {
                            SCValue childValue;
                            bool isNull = false;
                            const ErrorCode childRc = ReadConstraintValue(sourceTable,
                                                                           *childData,
                                                                           constraint.columns[i],
                                                                           nullptr,
                                                                           nullptr,
                                                                           &childValue,
                                                                           &isNull);
                            if (Failed(childRc))
                            {
                                return childRc;
                            }
                            if (isNull || childValue != parentValues[i])
                            {
                                matches = false;
                                break;
                            }
                        }

                        if (!matches)
                        {
                            continue;
                        }

                        const std::wstring scopeKey = sourceTable->Name() + L"|" + std::to_wstring(childId);
                        if (!activeConstraintPropagationKeys_.insert(scopeKey).second)
                        {
                            continue;
                        }
                        Scope scope{&activeConstraintPropagationKeys_, scopeKey};

                        switch (constraint.onDelete)
                        {
                            case SCForeignKeyAction::Restrict:
                            case SCForeignKeyAction::NoAction:
                                SetConstraintViolation(SCConstraintViolationInfo{
                                    sourceTable->Name(),
                                    constraint.name,
                                    constraint.kind,
                                    constraint.columns,
                                    childId,
                                    candidateData->id,
                                    L"Foreign key delete/update would leave dependent rows.",
                                });
                                return SC_E_CONSTRAINT_VIOLATION;
                            case SCForeignKeyAction::Cascade:
                                if (const ErrorCode deleteRc = sourceTable->DeleteRecord(childId);
                                    Failed(deleteRc) && deleteRc != SC_E_RECORD_DELETED)
                                {
                                    return deleteRc;
                                }
                                break;
                            case SCForeignKeyAction::SetNull:
                            case SCForeignKeyAction::SetDefault: {
                                for (std::size_t i = 0; i < constraint.columns.size(); ++i)
                                {
                                    const std::wstring childColumnName = constraint.columns[i];
                                    SCValue nextValue = SCValue::Null();
                                    if (constraint.onDelete == SCForeignKeyAction::SetDefault)
                                    {
                                        const SCColumnDef* childColumn = sourceSchemaImpl->FindColumnDef(childColumnName);
                                        if (childColumn == nullptr)
                                        {
                                            return SC_E_COLUMN_NOT_FOUND;
                                        }
                                        nextValue = childColumn->defaultValue;
                                    }
                                    const ErrorCode writeRc = WriteValue(sourceTable, childData, childColumnName, nextValue);
                                    if (Failed(writeRc) && writeRc != SC_E_RECORD_DELETED)
                                    {
                                        return writeRc;
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::ApplyForeignKeyActionsForColumnUpdate(
            SqliteTable* table,
            const std::shared_ptr<SqliteRecordData>& candidateData,
            const std::wstring& fieldName,
            const SCValue& oldValue,
            const SCValue& newValue)
        {
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }
            if (candidateData == nullptr)
            {
                return SC_E_POINTER;
            }

            if (oldValue == newValue)
            {
                return SC_OK;
            }

            struct Scope
            {
                std::unordered_set<std::wstring>* set{nullptr};
                std::wstring key;
                ~Scope()
                {
                    if (set != nullptr)
                    {
                        set->erase(key);
                    }
                }
            };

            const std::wstring parentScopeKey = table->Name() + L"|" + std::to_wstring(candidateData->id);
            if (!activeConstraintPropagationKeys_.insert(parentScopeKey).second)
            {
                return SC_OK;
            }
            Scope parentScope{&activeConstraintPropagationKeys_, parentScopeKey};

            const auto* referenceEntries = GetForeignKeyReferenceEntries(table->Name(), fieldName);
            if (referenceEntries == nullptr || referenceEntries->empty())
            {
                return SC_OK;
            }

            for (const auto& entry : *referenceEntries)
            {
                auto sourceIt = tables_.find(entry.sourceTableName);
                if (sourceIt == tables_.end())
                {
                    continue;
                }

                auto* sourceTable = static_cast<SqliteTable*>(sourceIt->second.Get());
                if (sourceTable == nullptr)
                {
                    continue;
                }

                SCSchemaPtr sourceSchema;
                if (Failed(sourceTable->GetSchema(sourceSchema)) || !sourceSchema)
                {
                    continue;
                }
                auto* sourceSchemaImpl = sourceTable->Schema();
                if (sourceSchemaImpl == nullptr)
                {
                    continue;
                }

                std::vector<SCValue> parentOldValues;
                std::vector<SCValue> parentNewValues;
                parentOldValues.reserve(entry.constraint.columns.size());
                parentNewValues.reserve(entry.constraint.columns.size());
                bool anyNull = false;
                for (std::size_t i = 0; i < entry.constraint.columns.size(); ++i)
                {
                    const std::wstring referencedColumn = ResolveForeignKeyReferencedColumn(entry.constraint, i);

                    SCValue oldTupleValue;
                    bool oldIsNull = false;
                    const ErrorCode oldRc = ReadConstraintValue(table,
                                                                 *candidateData,
                                                                 referencedColumn,
                                                                 nullptr,
                                                                 nullptr,
                                                                 &oldTupleValue,
                                                                 &oldIsNull);
                    if (Failed(oldRc))
                    {
                        return oldRc;
                    }
                    parentOldValues.push_back(oldTupleValue);
                    anyNull = anyNull || oldIsNull;

                    SCValue newTupleValue;
                    bool newIsNull = false;
                    const ErrorCode newRc = ReadConstraintValue(table,
                                                                 *candidateData,
                                                                 referencedColumn,
                                                                 &fieldName,
                                                                 &newValue,
                                                                 &newTupleValue,
                                                                 &newIsNull);
                    if (Failed(newRc))
                    {
                        return newRc;
                    }
                    parentNewValues.push_back(newTupleValue);
                }

                if (anyNull)
                {
                    continue;
                }

                for (const auto& [childId, childData] : sourceTable->Records())
                {
                    if (childData == nullptr || childData->state == RecordState::Deleted)
                    {
                        continue;
                    }

                    bool matches = true;
                    for (std::size_t i = 0; i < entry.constraint.columns.size(); ++i)
                    {
                        SCValue childValue;
                        bool isNull = false;
                        const ErrorCode childRc = ReadConstraintValue(sourceTable,
                                                                       *childData,
                                                                       entry.constraint.columns[i],
                                                                       nullptr,
                                                                       nullptr,
                                                                       &childValue,
                                                                       &isNull);
                        if (Failed(childRc))
                        {
                            return childRc;
                        }
                        if (isNull || childValue != parentOldValues[i])
                        {
                            matches = false;
                            break;
                        }
                    }

                    if (!matches)
                    {
                        continue;
                    }

                    const std::wstring scopeKey = sourceTable->Name() + L"|" + std::to_wstring(childId);
                    if (!activeConstraintPropagationKeys_.insert(scopeKey).second)
                    {
                        continue;
                    }
                    Scope scope{&activeConstraintPropagationKeys_, scopeKey};

                    switch (entry.constraint.onUpdate)
                    {
                        case SCForeignKeyAction::Restrict:
                        case SCForeignKeyAction::NoAction:
                            SetConstraintViolation(SCConstraintViolationInfo{
                                sourceTable->Name(),
                                entry.constraint.name,
                                entry.constraint.kind,
                                entry.constraint.columns,
                                childId,
                                candidateData->id,
                                L"Foreign key update would leave dependent rows.",
                            });
                            return SC_E_CONSTRAINT_VIOLATION;
                        case SCForeignKeyAction::Cascade:
                            for (std::size_t i = 0; i < entry.constraint.columns.size(); ++i)
                            {
                                const ErrorCode writeRc = WriteValue(sourceTable,
                                                                     childData,
                                                                     entry.constraint.columns[i],
                                                                     parentNewValues[i]);
                                if (Failed(writeRc) && writeRc != SC_E_RECORD_DELETED)
                                {
                                    return writeRc;
                                }
                            }
                            break;
                        case SCForeignKeyAction::SetNull:
                        case SCForeignKeyAction::SetDefault: {
                            for (std::size_t i = 0; i < entry.constraint.columns.size(); ++i)
                            {
                                SCValue nextValue = SCValue::Null();
                                if (entry.constraint.onUpdate == SCForeignKeyAction::SetDefault)
                                {
                                    const SCColumnDef* childColumn = sourceSchemaImpl->FindColumnDef(entry.constraint.columns[i]);
                                    if (childColumn == nullptr)
                                    {
                                        return SC_E_COLUMN_NOT_FOUND;
                                    }
                                    nextValue = childColumn->defaultValue;
                                }
                                const ErrorCode writeRc =
                                    WriteValue(sourceTable, childData, entry.constraint.columns[i], nextValue);
                                if (Failed(writeRc) && writeRc != SC_E_RECORD_DELETED)
                                {
                                    return writeRc;
                                }
                            }
                            break;
                        }
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::ValidateTableConstraints(SqliteTable* table,
                                                           const std::shared_ptr<SqliteRecordData>& candidateData,
                                                           const std::wstring* overrideFieldName,
                                                           const SCValue* overrideValue,
                                                           const SCConstraintDef* specificConstraint) const
        {
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }

            SCSchemaPtr schema;
            const ErrorCode schemaRc = table->GetSchema(schema);
            if (Failed(schemaRc) || !schema)
            {
                return Failed(schemaRc) ? schemaRc : SC_E_FAIL;
            }

            std::int32_t constraintCount = 0;
            const ErrorCode countRc = schema->GetConstraintCount(&constraintCount);
            if (Failed(countRc))
            {
                return countRc;
            }

            auto validateRecord = [&](const std::shared_ptr<SqliteRecordData>& recordData) -> ErrorCode {
                for (std::int32_t index = 0; index < constraintCount; ++index)
                {
                    SCConstraintDef constraint;
                    const ErrorCode constraintRc = schema->GetConstraint(index, &constraint);
                    if (Failed(constraintRc))
                    {
                        return constraintRc;
                    }

                    if (constraint.kind != SCConstraintKind::PrimaryKey && constraint.kind != SCConstraintKind::Unique &&
                        constraint.kind != SCConstraintKind::Check && constraint.kind != SCConstraintKind::ForeignKey)
                    {
                        continue;
                    }

                    if (specificConstraint != nullptr && !EqualsIgnoreCase(constraint.name, specificConstraint->name))
                    {
                        continue;
                    }

                    if (overrideFieldName != nullptr)
                    {
                        const auto usesField = std::any_of(constraint.columns.begin(),
                                                           constraint.columns.end(),
                                                           [overrideFieldName](const std::wstring& columnName) {
                                                               return EqualsIgnoreCase(columnName, *overrideFieldName);
                                                           });
                        if (!usesField && constraint.kind != SCConstraintKind::ForeignKey)
                        {
                            continue;
                        }
                    }

                    ErrorCode validateRc = SC_OK;
                    switch (constraint.kind)
                    {
                        case SCConstraintKind::PrimaryKey:
                        case SCConstraintKind::Unique:
                            validateRc = ValidateConstraintUniqueness(
                                table, constraint, recordData, overrideFieldName, overrideValue);
                            break;
                        case SCConstraintKind::Check:
                            validateRc =
                                ValidateCheckConstraint(table, constraint, recordData, overrideFieldName, overrideValue);
                            break;
                        case SCConstraintKind::ForeignKey:
                            validateRc = ValidateForeignKeyConstraint(
                                table, constraint, recordData, overrideFieldName, overrideValue);
                            break;
                        default:
                            break;
                    }

                    if (Failed(validateRc))
                    {
                        return validateRc;
                    }
                }
                return SC_OK;
            };

            if (candidateData != nullptr)
            {
                return validateRecord(candidateData);
            }

            for (const auto& [_, recordData] : table->Records())
            {
                if (recordData == nullptr || recordData->state == RecordState::Deleted)
                {
                    continue;
                }
                const ErrorCode validateRc = validateRecord(recordData);
                if (Failed(validateRc))
                {
                    return validateRc;
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::ValidateForeignKeyReferencesToTable(SqliteTable* table,
                                                                       const std::shared_ptr<SqliteRecordData>& candidateData,
                                                                       const std::wstring* overrideFieldName,
                                                                       const SCValue* overrideValue) const
        {
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }

            SCSchemaPtr schema;
            const ErrorCode schemaRc = table->GetSchema(schema);
            if (Failed(schemaRc) || !schema)
            {
                return Failed(schemaRc) ? schemaRc : SC_E_FAIL;
            }

            const auto* referenceEntries = GetForeignKeyReferenceEntries(table->Name());
            if (referenceEntries == nullptr || referenceEntries->empty())
            {
                return SC_OK;
            }

            std::int32_t columnCount = 0;
            const ErrorCode countRc = schema->GetColumnCount(&columnCount);
            if (Failed(countRc))
            {
                return countRc;
            }

            for (std::int32_t columnIndex = 0; columnIndex < columnCount; ++columnIndex)
            {
                SCColumnDef column;
                if (Failed(schema->GetColumn(columnIndex, &column)))
                {
                    continue;
                }

                const auto* columnEntries = GetForeignKeyReferenceEntries(table->Name(), column.name);
                if (columnEntries == nullptr || columnEntries->empty())
                {
                    continue;
                }

                for (const auto& entry : *columnEntries)
                {
                    auto sourceIt = tables_.find(entry.sourceTableName);
                    if (sourceIt == tables_.end())
                    {
                        continue;
                    }

                    auto* sourceTable = static_cast<SqliteTable*>(sourceIt->second.Get());
                    if (sourceTable == nullptr)
                    {
                        continue;
                    }

                    SCSchemaPtr sourceSchema;
                    if (Failed(sourceTable->GetSchema(sourceSchema)) || !sourceSchema)
                    {
                        continue;
                    }

                    std::vector<SCValue> targetValues;
                    targetValues.reserve(entry.constraint.columns.size());
                    bool anyNull = false;
                    for (std::size_t i = 0; i < entry.constraint.columns.size(); ++i)
                    {
                        const std::wstring referencedColumnName = ResolveForeignKeyReferencedColumn(entry.constraint, i);
                        SCValue value;
                        bool isNull = false;
                        const ErrorCode valueRc = ReadConstraintValue(table,
                                                                      *candidateData,
                                                                      referencedColumnName,
                                                                      overrideFieldName,
                                                                      overrideValue,
                                                                      &value,
                                                                      &isNull);
                        if (Failed(valueRc))
                        {
                            return valueRc;
                        }
                        if (isNull)
                        {
                            anyNull = true;
                        }
                        targetValues.push_back(value);
                    }

                    if (anyNull)
                    {
                        continue;
                    }

                    for (const auto& [candidateId, candidateDataIt] : sourceTable->Records())
                    {
                        if (candidateDataIt == nullptr || candidateDataIt->state == RecordState::Deleted)
                        {
                            continue;
                        }

                        bool match = true;
                        for (std::size_t i = 0; i < entry.constraint.columns.size(); ++i)
                        {
                            SCValue sourceValue;
                            bool isNull = false;
                            const ErrorCode valueRc = ReadConstraintValue(sourceTable,
                                                                          *candidateDataIt,
                                                                          entry.constraint.columns[i],
                                                                          nullptr,
                                                                          nullptr,
                                                                          &sourceValue,
                                                                          &isNull);
                            if (Failed(valueRc))
                            {
                                return valueRc;
                            }
                            if (isNull || sourceValue != targetValues[i])
                            {
                                match = false;
                                break;
                            }
                        }

                        if (match)
                        {
                            return SC_E_CONSTRAINT_VIOLATION;
                        }
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::ValidateUniqueAndPrimaryKeyConstraints(
            SqliteTable* table,
            const std::shared_ptr<SqliteRecordData>& candidateData,
            const std::wstring* overrideFieldName,
            const SCValue* overrideValue,
            const SCConstraintDef* specificConstraint) const
        {
            return ValidateTableConstraints(table, candidateData, overrideFieldName, overrideValue, specificConstraint);
        }

        ErrorCode SqliteDatabase::ValidateForeignKeyReferencesForTouchedTables(
            const JournalTransaction& tx,
            bool reverseRenameResolution) const
        {
            std::vector<std::wstring> touchedTableNames;
            CollectTouchedTableNames(
                tx,
                &touchedTableNames,
                reverseRenameResolution);

            for (const std::wstring& tableName : touchedTableNames)
            {
                const auto* referenceEntries = GetForeignKeyReferenceEntries(tableName);
                if (referenceEntries == nullptr || referenceEntries->empty())
                {
                    continue;
                }

                for (const auto& entry : *referenceEntries)
                {
                    auto sourceIt = tables_.find(entry.sourceTableName);
                    if (sourceIt == tables_.end())
                    {
                        continue;
                    }

                    auto* sourceTable = static_cast<SqliteTable*>(sourceIt->second.Get());
                    if (sourceTable == nullptr)
                    {
                        continue;
                    }

                    const ErrorCode validateRc =
                        ValidateForeignKeyConstraint(sourceTable, entry.constraint, std::shared_ptr<SqliteRecordData>{});
                    if (Failed(validateRc))
                    {
                        return validateRc;
                    }
                }
            }

            return SC_OK;
        }

        bool SqliteDatabase::HasForeignKeyReferencesToTable(const std::wstring& tableName) const
        {
            const auto* entries = GetForeignKeyReferenceEntries(tableName);
            return entries != nullptr && !entries->empty();
        }

        bool SqliteDatabase::HasForeignKeyReferencesToColumn(const std::wstring& tableName,
                                                             const std::wstring& columnName) const
        {
            const auto* entries = GetForeignKeyReferenceEntries(tableName, columnName);
            if (entries == nullptr)
            {
                return false;
            }
            return !entries->empty();
        }

        ErrorCode SqliteDatabase::ValidateUniqueAndPrimaryKeyConstraintsForTouchedTables(
            const JournalTransaction& tx,
            bool reverseRenameResolution) const
        {
            std::vector<std::wstring> touchedTableNames;
            CollectTouchedTableNames(
                tx,
                &touchedTableNames,
                reverseRenameResolution);

            for (const std::wstring& tableName : touchedTableNames)
            {
                auto tableIt = tables_.find(tableName);
                if (tableIt == tables_.end())
                {
                    continue;
                }

                auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
                if (table == nullptr)
                {
                    continue;
                }

                const ErrorCode validateRc = ValidateTableConstraints(table, std::shared_ptr<SqliteRecordData>{});
                if (Failed(validateRc))
                {
                    return validateRc;
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::EncodeIndexColumnValue(const SCValue& value,
                                                         ValueKind valueKind,
                                                         bool descending,
                                                         std::vector<std::uint8_t>* outBytes) const
        {
            if (outBytes == nullptr)
            {
                return SC_E_POINTER;
            }

            outBytes->clear();
            auto appendByte = [outBytes](std::uint8_t byte) { outBytes->push_back(byte); };
            auto appendU64 = [outBytes](std::uint64_t value) {
                for (int shift = 56; shift >= 0; shift -= 8)
                {
                    outBytes->push_back(static_cast<std::uint8_t>((value >> shift) & 0xffu));
                }
            };

            if (value.IsNull())
            {
                appendByte(0x00);
            } else
            {
                appendByte(0x01);
                switch (valueKind)
                {
                    case ValueKind::Int64:
                    case ValueKind::RecordId: {
                        std::int64_t typed = 0;
                        ErrorCode rc = SC_OK;
                        if (valueKind == ValueKind::RecordId)
                        {
                            RecordId recordId = 0;
                            rc = value.AsRecordId(&recordId);
                            typed = static_cast<std::int64_t>(recordId);
                        } else
                        {
                            rc = value.AsInt64(&typed);
                        }
                        if (Failed(rc))
                        {
                            return rc;
                        }
                        appendByte(valueKind == ValueKind::RecordId ? 0x14 : 0x11);
                        appendU64(static_cast<std::uint64_t>(typed) ^ 0x8000000000000000ull);
                        break;
                    }
                    case ValueKind::Double: {
                        double typed = 0.0;
                        const ErrorCode rc = value.AsDouble(&typed);
                        if (Failed(rc))
                        {
                            return rc;
                        }
                        appendByte(0x12);
                        std::uint64_t bits = 0;
                        static_assert(sizeof(bits) == sizeof(typed));
                        std::memcpy(&bits, &typed, sizeof(bits));
                        bits = (bits & 0x8000000000000000ull) != 0 ? ~bits : (bits ^ 0x8000000000000000ull);
                        appendU64(bits);
                        break;
                    }
                    case ValueKind::Bool: {
                        bool typed = false;
                        const ErrorCode rc = value.AsBool(&typed);
                        if (Failed(rc))
                        {
                            return rc;
                        }
                        appendByte(0x13);
                        appendByte(typed ? 1 : 0);
                        break;
                    }
                    case ValueKind::String:
                    case ValueKind::Enum: {
                        std::wstring text;
                        const ErrorCode rc = value.AsStringCopy(&text);
                        if (Failed(rc))
                        {
                            return rc;
                        }
                        appendByte(valueKind == ValueKind::Enum ? 0x16 : 0x15);
                        const std::string utf8 = ToUtf8(text);
                        for (unsigned char byte : utf8)
                        {
                            if (byte == 0x00u)
                            {
                                outBytes->push_back(0x00u);
                                outBytes->push_back(0xffu);
                            } else
                            {
                                outBytes->push_back(static_cast<std::uint8_t>(byte));
                            }
                        }
                        outBytes->push_back(0x00u);
                        outBytes->push_back(0x00u);
                        break;
                    }
                    case ValueKind::Binary: {
                        const auto* bytes = value.TryGet<std::vector<std::uint8_t>>();
                        if (bytes == nullptr)
                        {
                            return SC_E_TYPE_MISMATCH;
                        }
                        appendByte(0x17);
                        appendU64(static_cast<std::uint64_t>(bytes->size()));
                        outBytes->insert(outBytes->end(), bytes->begin(), bytes->end());
                        break;
                    }
                    case ValueKind::Null:
                    default:
                        appendByte(0x10);
                        break;
                }
            }

            if (descending)
            {
                for (std::uint8_t& byte : *outBytes)
                {
                    byte = static_cast<std::uint8_t>(0xffu - byte);
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::EncodeIndexColumnPrefixValue(const SCValue& value,
                                                               ValueKind valueKind,
                                                               bool descending,
                                                               std::vector<std::uint8_t>* outBytes) const
        {
            if (outBytes == nullptr)
            {
                return SC_E_POINTER;
            }
            outBytes->clear();

            if (valueKind != ValueKind::String && valueKind != ValueKind::Enum)
            {
                return EncodeIndexColumnValue(value, valueKind, descending, outBytes);
            }

            std::wstring text;
            const ErrorCode rc = value.AsStringCopy(&text);
            if (Failed(rc))
            {
                return rc;
            }

            // Keep the prefix shape aligned with EncodeIndexColumnValue() for
            // non-null strings, but omit the trailing terminator so the caller
            // can build a prefix range over the composite full key.
            outBytes->push_back(0x01u);
            outBytes->push_back(valueKind == ValueKind::Enum ? 0x16 : 0x15);
            const std::string utf8 = ToUtf8(text);
            for (unsigned char byte : utf8)
            {
                if (byte == 0x00u)
                {
                    outBytes->push_back(0x00u);
                    outBytes->push_back(0xffu);
                } else
                {
                    outBytes->push_back(static_cast<std::uint8_t>(byte));
                }
            }

            if (descending)
            {
                for (std::uint8_t& byte : *outBytes)
                {
                    byte = static_cast<std::uint8_t>(0xffu - byte);
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::BuildCompositeIndexKey(const SqliteSchema* schema,
                                                         const SCIndexDef& indexDef,
                                                         const SqliteRecordData& recordData,
                                                         CompositeIndexEncodedKey* outKey) const
        {
            if (schema == nullptr || outKey == nullptr)
            {
                return schema == nullptr ? SC_E_POINTER : SC_E_POINTER;
            }

            outKey->prefix1.clear();
            outKey->prefix2.clear();
            outKey->prefix3.clear();
            outKey->full.clear();

            std::vector<std::uint8_t> encoded;
            for (std::size_t index = 0; index < indexDef.columns.size(); ++index)
            {
                const SCIndexColumnDef& indexColumn = indexDef.columns[index];
                const SCColumnDef* columnDef = schema->FindColumnDef(indexColumn.columnName);
                if (columnDef == nullptr)
                {
                    return SC_E_COLUMN_NOT_FOUND;
                }

                const auto valueIt = recordData.values.find(indexColumn.columnName);
                const SCValue& value = valueIt != recordData.values.end() ? valueIt->second : columnDef->defaultValue;
                const ErrorCode encodeRc =
                    EncodeIndexColumnValue(value, columnDef->valueKind, indexColumn.descending, &encoded);
                if (Failed(encodeRc))
                {
                    return encodeRc;
                }

                outKey->full.push_back(0xfeu);
                outKey->full.insert(outKey->full.end(), encoded.begin(), encoded.end());
                if (index == 0)
                {
                    outKey->prefix1 = outKey->full;
                } else if (index == 1)
                {
                    outKey->prefix2 = outKey->full;
                } else if (index == 2)
                {
                    outKey->prefix3 = outKey->full;
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::BuildCompositeLookupBounds(const SqliteSchema* schema,
                                                             const QueryPlan& analyzedPlan,
                                                             CompositeIndexLookupBounds* outBounds) const
        {
            if (schema == nullptr || outBounds == nullptr || !analyzedPlan.matchedIndex.has_value())
            {
                return SC_E_POINTER;
            }

            *outBounds = CompositeIndexLookupBounds{};

            const QueryMatchedIndexSpec& matchedIndex = analyzedPlan.matchedIndex.value();
            const SCIndexDef* indexDef = schema->FindIndexDef(matchedIndex.indexName);
            if (indexDef == nullptr)
            {
                return SC_E_INDEX_NOT_FOUND;
            }

            auto appendExactSegment =
                [this](const SCValue& value,
                       const SCColumnDef& column,
                       bool descending,
                       std::vector<std::uint8_t>* outBytes) -> ErrorCode {
                std::vector<std::uint8_t> encoded;
                const ErrorCode encodeRc = EncodeIndexColumnValue(value, column.valueKind, descending, &encoded);
                if (Failed(encodeRc))
                {
                    return encodeRc;
                }
                outBytes->push_back(0xfeu);
                outBytes->insert(outBytes->end(), encoded.begin(), encoded.end());
                return SC_OK;
            };

            for (std::size_t index = 0; index < matchedIndex.equalityPrefixLength; ++index)
            {
                const std::wstring& columnName = matchedIndex.keyColumns[index];
                const auto it =
                    std::find_if(analyzedPlan.pushdown.pushdownConditions.begin(),
                                 analyzedPlan.pushdown.pushdownConditions.end(),
                                 [&columnName](const QueryCondition& condition) {
                                     return condition.fieldName == columnName &&
                                            IsEqualityIndexOperator(condition.op) && condition.values.size() == 1;
                                 });
                if (it == analyzedPlan.pushdown.pushdownConditions.end())
                {
                    break;
                }

                SCColumnDef column;
                const ErrorCode columnRc = const_cast<SqliteSchema*>(schema)->FindColumn(columnName.c_str(), &column);
                if (Failed(columnRc))
                {
                    return columnRc;
                }
                const ErrorCode appendRc = appendExactSegment(
                    it->values.front(), column, (*indexDef).columns[index].descending, &outBounds->equalityPrefixKey);
                if (Failed(appendRc))
                {
                    return appendRc;
                }
                outBounds->equalityPrefixLength = static_cast<std::uint32_t>(index + 1);
            }

            if (!matchedIndex.hasRangeCondition)
            {
                outBounds->exactMatch = true;
                outBounds->exactPrefixLength = matchedIndex.equalityPrefixLength;
                outBounds->exactPrefixKey = outBounds->equalityPrefixKey;
                return SC_OK;
            }

            const std::size_t rangeColumnIndex = matchedIndex.equalityPrefixLength;
            if (rangeColumnIndex >= matchedIndex.keyColumns.size())
            {
                return SC_E_INVALIDARG;
            }

            const std::wstring& rangeColumnName = matchedIndex.keyColumns[rangeColumnIndex];
            const auto rangeIt =
                std::find_if(analyzedPlan.pushdown.pushdownConditions.begin(),
                             analyzedPlan.pushdown.pushdownConditions.end(),
                             [&rangeColumnName](const QueryCondition& condition) {
                                 return condition.fieldName == rangeColumnName && IsRangeIndexOperator(condition.op);
                             });
            if (rangeIt == analyzedPlan.pushdown.pushdownConditions.end())
            {
                return SC_E_INVALIDARG;
            }

            SCColumnDef rangeColumn;
            const ErrorCode rangeColumnRc =
                const_cast<SqliteSchema*>(schema)->FindColumn(rangeColumnName.c_str(), &rangeColumn);
            if (Failed(rangeColumnRc))
            {
                return rangeColumnRc;
            }

            const bool descending = (*indexDef).columns[rangeColumnIndex].descending;
            std::vector<std::uint8_t> baseKey = outBounds->equalityPrefixKey;
            switch (rangeIt->op)
            {
                case QueryConditionOperator::GreaterThan:
                case QueryConditionOperator::GreaterThanOrEqual: {
                    if (rangeIt->values.size() != 1)
                    {
                        return SC_E_INVALIDARG;
                    }
                    std::vector<std::uint8_t> bound = baseKey;
                    const ErrorCode appendRc =
                        appendExactSegment(rangeIt->values.front(), rangeColumn, descending, &bound);
                    if (Failed(appendRc))
                    {
                        return appendRc;
                    }
                    if (descending)
                    {
                        outBounds->hasUpperBound = true;
                        outBounds->includeUpperBound = rangeIt->op == QueryConditionOperator::GreaterThanOrEqual;
                        outBounds->upperBound = std::move(bound);
                        if (outBounds->includeUpperBound)
                        {
                            outBounds->upperBound.push_back(0xffu);
                        }
                    } else
                    {
                        outBounds->hasLowerBound = true;
                        outBounds->includeLowerBound = rangeIt->op == QueryConditionOperator::GreaterThanOrEqual;
                        outBounds->lowerBound = std::move(bound);
                        if (!outBounds->includeLowerBound)
                        {
                            outBounds->lowerBound.push_back(0xffu);
                        }
                    }
                    break;
                }
                case QueryConditionOperator::LessThan:
                case QueryConditionOperator::LessThanOrEqual: {
                    if (rangeIt->values.size() != 1)
                    {
                        return SC_E_INVALIDARG;
                    }
                    std::vector<std::uint8_t> bound = baseKey;
                    const ErrorCode appendRc =
                        appendExactSegment(rangeIt->values.front(), rangeColumn, descending, &bound);
                    if (Failed(appendRc))
                    {
                        return appendRc;
                    }
                    if (descending)
                    {
                        outBounds->hasLowerBound = true;
                        outBounds->includeLowerBound = rangeIt->op == QueryConditionOperator::LessThanOrEqual;
                        outBounds->lowerBound = std::move(bound);
                        if (!outBounds->includeLowerBound)
                        {
                            outBounds->lowerBound.push_back(0xffu);
                        }
                    } else
                    {
                        outBounds->hasUpperBound = true;
                        outBounds->includeUpperBound = rangeIt->op == QueryConditionOperator::LessThanOrEqual;
                        outBounds->upperBound = std::move(bound);
                        if (outBounds->includeUpperBound)
                        {
                            outBounds->upperBound.push_back(0xffu);
                        }
                    }
                    break;
                }
                case QueryConditionOperator::Between: {
                    if (rangeIt->values.size() != 2)
                    {
                        return SC_E_INVALIDARG;
                    }
                    std::vector<std::uint8_t> firstBound = baseKey;
                    {
                        const ErrorCode appendRc =
                            appendExactSegment(rangeIt->values[0], rangeColumn, descending, &firstBound);
                        if (Failed(appendRc))
                        {
                            return appendRc;
                        }
                    }
                    std::vector<std::uint8_t> secondBound = baseKey;
                    {
                        const ErrorCode appendRc =
                            appendExactSegment(rangeIt->values[1], rangeColumn, descending, &secondBound);
                        if (Failed(appendRc))
                        {
                            return appendRc;
                        }
                    }
                    if (descending)
                    {
                        outBounds->hasLowerBound = true;
                        outBounds->includeLowerBound = true;
                        outBounds->lowerBound = std::move(secondBound);
                        outBounds->hasUpperBound = true;
                        outBounds->includeUpperBound = true;
                        outBounds->upperBound = std::move(firstBound);
                        outBounds->upperBound.push_back(0xffu);
                    } else
                    {
                        outBounds->hasLowerBound = true;
                        outBounds->includeLowerBound = true;
                        outBounds->lowerBound = std::move(firstBound);
                        outBounds->hasUpperBound = true;
                        outBounds->includeUpperBound = true;
                        outBounds->upperBound = std::move(secondBound);
                        outBounds->upperBound.push_back(0xffu);
                    }
                    break;
                }
                case QueryConditionOperator::StartsWith: {
                    if (rangeIt->values.size() != 1)
                    {
                        return SC_E_INVALIDARG;
                    }
                    if (rangeColumn.valueKind != ValueKind::String && rangeColumn.valueKind != ValueKind::Enum)
                    {
                        return SC_E_NOTIMPL;
                    }

                    std::vector<std::uint8_t> encodedPrefix;
                    const ErrorCode encodePrefixRc = EncodeIndexColumnPrefixValue(
                        rangeIt->values.front(), rangeColumn.valueKind, descending, &encodedPrefix);
                    if (Failed(encodePrefixRc))
                    {
                        return encodePrefixRc;
                    }

                    outBounds->hasLowerBound = true;
                    outBounds->includeLowerBound = true;
                    outBounds->lowerBound = baseKey;
                    outBounds->lowerBound.push_back(0xfeu);
                    outBounds->lowerBound.insert(
                        outBounds->lowerBound.end(), encodedPrefix.begin(), encodedPrefix.end());

                    outBounds->hasUpperBound = true;
                    outBounds->includeUpperBound = false;
                    outBounds->upperBound = outBounds->lowerBound;
                    outBounds->upperBound.push_back(0xffu);
                    break;
                }
                default:
                    return SC_E_NOTIMPL;
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::BuildCompositeEqualityPrefixBounds(const SqliteSchema* schema,
                                                                     const QueryPlan& analyzedPlan,
                                                                     CompositeIndexLookupBounds* outBounds) const
        {
            if (schema == nullptr || outBounds == nullptr || !analyzedPlan.matchedIndex.has_value())
            {
                return SC_E_POINTER;
            }

            *outBounds = CompositeIndexLookupBounds{};

            const QueryMatchedIndexSpec& matchedIndex = analyzedPlan.matchedIndex.value();
            const SCIndexDef* indexDef = schema->FindIndexDef(matchedIndex.indexName);
            if (indexDef == nullptr)
            {
                return SC_E_INDEX_NOT_FOUND;
            }

            auto appendExactSegment =
                [this](const SCValue& value,
                       const SCColumnDef& column,
                       bool descending,
                       std::vector<std::uint8_t>* outBytes) -> ErrorCode {
                std::vector<std::uint8_t> encoded;
                const ErrorCode encodeRc = EncodeIndexColumnValue(value, column.valueKind, descending, &encoded);
                if (Failed(encodeRc))
                {
                    return encodeRc;
                }
                outBytes->push_back(0xfeu);
                outBytes->insert(outBytes->end(), encoded.begin(), encoded.end());
                return SC_OK;
            };

            for (std::size_t index = 0; index < matchedIndex.equalityPrefixLength; ++index)
            {
                const std::wstring& columnName = matchedIndex.keyColumns[index];
                const auto it =
                    std::find_if(analyzedPlan.pushdown.pushdownConditions.begin(),
                                 analyzedPlan.pushdown.pushdownConditions.end(),
                                 [&columnName](const QueryCondition& condition) {
                                     return condition.fieldName == columnName &&
                                            IsEqualityIndexOperator(condition.op) && condition.values.size() == 1;
                                 });
                if (it == analyzedPlan.pushdown.pushdownConditions.end())
                {
                    break;
                }

                SCColumnDef column;
                const ErrorCode columnRc = const_cast<SqliteSchema*>(schema)->FindColumn(columnName.c_str(), &column);
                if (Failed(columnRc))
                {
                    return columnRc;
                }
                const ErrorCode appendRc = appendExactSegment(
                    it->values.front(), column, indexDef->columns[index].descending, &outBounds->equalityPrefixKey);
                if (Failed(appendRc))
                {
                    return appendRc;
                }
                outBounds->equalityPrefixLength = static_cast<std::uint32_t>(index + 1);
            }

            if (outBounds->equalityPrefixLength == 0)
            {
                return SC_E_INVALIDARG;
            }

            outBounds->exactMatch = true;
            outBounds->exactPrefixLength = outBounds->equalityPrefixLength;
            outBounds->exactPrefixKey = outBounds->equalityPrefixKey;
            return SC_OK;
        }

        std::int64_t SqliteDatabase::FindQueryIndexStorageRowId(std::int64_t tableRowId,
                                                                const std::wstring& indexName) const
        {
            const auto it = queryIndexRowIdsByTableAndName_.find(BuildQueryIndexStorageKey(tableRowId, indexName));
            return it == queryIndexRowIdsByTableAndName_.end() ? -1 : it->second;
        }

        ErrorCode SqliteDatabase::CheckCompositeQueryIndexConsistency(QueryIndexCheckResult* outResult) const
        {
            if (outResult == nullptr)
            {
                return SC_E_POINTER;
            }

            outResult->state = QueryIndexHealthState::Healthy;
            outResult->indexVersion = static_cast<std::int32_t>(version_);
            outResult->expectedVersion = static_cast<std::int32_t>(version_);
            outResult->message = L"query-index-current";

            if (activeEdit_ && !activeJournal_.entries.empty())
            {
                outResult->state = QueryIndexHealthState::OutOfDate;
                outResult->message = L"query-index-rebuild-required";
                return SC_OK;
            }

            auto countEntriesForIndex = [this](std::int64_t schemaIndexRowId, std::int64_t* outCount) -> ErrorCode {
                if (outCount == nullptr)
                {
                    return SC_E_POINTER;
                }

                sqlite3_stmt* stmt = nullptr;
                const int prepareRc = sqlite3_prepare_v2(
                    db_.Raw(),
                    "SELECT COUNT(*) FROM query_index_entries WHERE schema_index_id = ? AND alive_flag = 1;",
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

                sqlite3_bind_int64(stmt, 1, schemaIndexRowId);
                const int stepRc = sqlite3_step(stmt);
                if (stepRc != SQLITE_ROW)
                {
                    sqlite3_finalize(stmt);
                    return MapSqliteError(stepRc);
                }

                *outCount = sqlite3_column_int64(stmt, 0);
                sqlite3_finalize(stmt);
                return SC_OK;
            };

            auto hasExactEntry = [this](std::int64_t schemaIndexRowId,
                                        RecordId recordId,
                                        const CompositeIndexEncodedKey& key,
                                        bool* outFound) -> ErrorCode {
                if (outFound == nullptr)
                {
                    return SC_E_POINTER;
                }

                sqlite3_stmt* stmt = nullptr;
                const int prepareRc = sqlite3_prepare_v2(
                    db_.Raw(),
                    "SELECT 1 FROM query_index_entries WHERE schema_index_id = ? AND record_id = ? AND alive_flag = 1 "
                    "AND full_key = ? LIMIT 1;",
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

                sqlite3_bind_int64(stmt, 1, schemaIndexRowId);
                sqlite3_bind_int64(stmt, 2, recordId);
                sqlite3_bind_blob(stmt,
                                  3,
                                  key.full.data(),
                                  static_cast<int>(key.full.size()),
                                  SQLITE_TRANSIENT);
                const int stepRc = sqlite3_step(stmt);
                *outFound = stepRc == SQLITE_ROW;
                sqlite3_finalize(stmt);
                return (stepRc == SQLITE_ROW || stepRc == SQLITE_DONE) ? SC_OK : MapSqliteError(stepRc);
            };

            for (const auto& [_, tableRef] : tables_)
            {
                auto* table = static_cast<SqliteTable*>(tableRef.Get());
                if (table == nullptr)
                {
                    continue;
                }

                SCTableSchemaSnapshot snapshot;
                const ErrorCode snapshotRc = table->Schema()->GetSchemaSnapshot(&snapshot);
                if (Failed(snapshotRc))
                {
                    return snapshotRc;
                }

                for (const SCIndexDef& indexDef : snapshot.indexes)
                {
                    if (!IsCompositeIndexExplicit(indexDef))
                    {
                        continue;
                    }

                    const std::int64_t schemaIndexRowId = FindQueryIndexStorageRowId(table->TableRowId(), indexDef.name);
                    if (schemaIndexRowId <= 0)
                    {
                        outResult->state = QueryIndexHealthState::Missing;
                        outResult->message = L"query-index-definition-missing:" + indexDef.name;
                        return SC_OK;
                    }

                    std::int64_t expectedAliveCount = 0;
                    for (const auto& [recordId, recordData] : table->Records())
                    {
                        (void)recordId;
                        if (recordData == nullptr || recordData->state != RecordState::Alive)
                        {
                            continue;
                        }

                        ++expectedAliveCount;
                        CompositeIndexEncodedKey key;
                        const ErrorCode keyRc = BuildCompositeIndexKey(table->Schema(), indexDef, *recordData, &key);
                        if (Failed(keyRc))
                        {
                            outResult->state = QueryIndexHealthState::Corrupted;
                            outResult->message = L"query-index-key-build-failed:" + indexDef.name;
                            return SC_OK;
                        }

                        bool found = false;
                        const ErrorCode entryRc = hasExactEntry(schemaIndexRowId, recordData->id, key, &found);
                        if (Failed(entryRc))
                        {
                            return entryRc;
                        }
                        if (!found)
                        {
                            outResult->state = QueryIndexHealthState::Corrupted;
                            outResult->message = L"query-index-entry-missing:" + indexDef.name;
                            return SC_OK;
                        }
                    }

                    std::int64_t actualAliveCount = 0;
                    const ErrorCode countRc = countEntriesForIndex(schemaIndexRowId, &actualAliveCount);
                    if (Failed(countRc))
                    {
                        return countRc;
                    }

                    if (actualAliveCount != expectedAliveCount)
                    {
                        outResult->state = QueryIndexHealthState::Corrupted;
                        outResult->message = L"query-index-entry-count-mismatch:" + indexDef.name;
                        return SC_OK;
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistQueryIndexDefinition(SqliteSchema* schema,
                                                              const SCIndexDef& def,
                                                              std::int64_t schemaIndexRowId,
                                                              bool updateCache)
        {
            if (schema == nullptr || schemaIndexRowId <= 0)
            {
                return SC_E_INVALIDARG;
            }
            if (!IsCompositeIndexExplicit(def))
            {
                return SC_OK;
            }

            SqliteStmt stmt = db_.Prepare(
                "INSERT OR REPLACE INTO query_indexes(schema_index_id, table_id, index_name, key_arity) "
                "VALUES(?, ?, ?, ?);");
            stmt.BindInt64(1, schemaIndexRowId);
            stmt.BindInt64(2, schema->TableRowId());
            stmt.BindText(3, def.name);
            stmt.BindInt64(4, static_cast<std::int64_t>(def.columns.size()));
            const ErrorCode rc = stmt.Step();
            if (Failed(rc))
            {
                return rc;
            }

            if (updateCache)
            {
                queryIndexRowIdsByTableAndName_[BuildQueryIndexStorageKey(schema->TableRowId(), def.name)] =
                    schemaIndexRowId;
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::RemoveQueryIndexDefinition(SqliteSchema* schema, const wchar_t* name, bool updateCache)
        {
            if (schema == nullptr || name == nullptr)
            {
                return SC_E_POINTER;
            }

            {
                SqliteStmt deleteEntries = db_.Prepare("DELETE FROM query_index_entries WHERE schema_index_id = ?;");
                deleteEntries.BindInt64(1, schema->FindIndexRowId(name));
                const ErrorCode rc = deleteEntries.Step();
                if (Failed(rc))
                {
                    return rc;
                }
            }

            SqliteStmt deleteDef = db_.Prepare("DELETE FROM query_indexes WHERE schema_index_id = ?;");
            deleteDef.BindInt64(1, schema->FindIndexRowId(name));
            const ErrorCode rc = deleteDef.Step();
            if (Failed(rc))
            {
                return rc;
            }

            if (updateCache)
            {
                queryIndexRowIdsByTableAndName_.erase(BuildQueryIndexStorageKey(schema->TableRowId(), name));
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::RebuildCompositeIndexEntriesForRecord(SqliteTable* table,
                                                                        const SCIndexDef& indexDef,
                                                                        const SqliteRecordData& recordData,
                                                                        std::int64_t schemaIndexRowId)
        {
            if (table == nullptr || !IsCompositeIndexExplicit(indexDef))
            {
                return table == nullptr ? SC_E_POINTER : SC_OK;
            }

            const std::int64_t effectiveSchemaIndexRowId =
                schemaIndexRowId > 0 ? schemaIndexRowId : table->Schema()->FindIndexRowId(indexDef.name.c_str());
            if (effectiveSchemaIndexRowId <= 0)
            {
                return SC_E_INDEX_NOT_FOUND;
            }

            {
                SqliteStmt deleteStmt =
                    db_.Prepare("DELETE FROM query_index_entries WHERE schema_index_id = ? AND record_id = ?;");
                deleteStmt.BindInt64(1, effectiveSchemaIndexRowId);
                deleteStmt.BindInt64(2, recordData.id);
                const ErrorCode deleteRc = deleteStmt.Step();
                if (Failed(deleteRc))
                {
                    return deleteRc;
                }
            }

            if (recordData.state != RecordState::Alive)
            {
                return SC_OK;
            }

            CompositeIndexEncodedKey key;
            const ErrorCode keyRc = BuildCompositeIndexKey(table->Schema(), indexDef, recordData, &key);
            if (Failed(keyRc))
            {
                return keyRc;
            }

            SqliteStmt insertStmt = db_.Prepare(
                "INSERT INTO query_index_entries(schema_index_id, record_id, alive_flag, key_prefix_1, "
                "key_prefix_2, key_prefix_3, full_key) VALUES(?, ?, 1, ?, ?, ?, ?);");
            insertStmt.BindInt64(1, effectiveSchemaIndexRowId);
            insertStmt.BindInt64(2, recordData.id);
            key.prefix1.empty() ? insertStmt.BindNull(3) : insertStmt.BindBlob(3, key.prefix1);
            key.prefix2.empty() ? insertStmt.BindNull(4) : insertStmt.BindBlob(4, key.prefix2);
            key.prefix3.empty() ? insertStmt.BindNull(5) : insertStmt.BindBlob(5, key.prefix3);
            insertStmt.BindBlob(6, key.full);
            return insertStmt.Step();
        }

        ErrorCode SqliteDatabase::RebuildCompositeIndexEntriesForRecord(SqliteTable* table,
                                                                        const SqliteRecordData& recordData)
        {
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }

            SCTableSchemaSnapshot snapshot;
            const ErrorCode snapshotRc = table->Schema()->GetSchemaSnapshot(&snapshot);
            if (Failed(snapshotRc))
            {
                return snapshotRc;
            }

            for (const SCIndexDef& indexDef : snapshot.indexes)
            {
                const ErrorCode rc = RebuildCompositeIndexEntriesForRecord(table, indexDef, recordData);
                if (Failed(rc))
                {
                    return rc;
                }
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::RebuildCompositeIndexEntriesForTable(SqliteTable* table,
                                                                       const SCIndexDef& indexDef,
                                                                       std::int64_t schemaIndexRowId)
        {
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }
            if (!IsCompositeIndexExplicit(indexDef))
            {
                return SC_OK;
            }

            const std::int64_t effectiveSchemaIndexRowId =
                schemaIndexRowId > 0 ? schemaIndexRowId : table->Schema()->FindIndexRowId(indexDef.name.c_str());
            if (effectiveSchemaIndexRowId <= 0)
            {
                return SC_E_INDEX_NOT_FOUND;
            }

            {
                SqliteStmt deleteStmt = db_.Prepare("DELETE FROM query_index_entries WHERE schema_index_id = ?;");
                deleteStmt.BindInt64(1, effectiveSchemaIndexRowId);
                const ErrorCode deleteRc = deleteStmt.Step();
                if (Failed(deleteRc))
                {
                    return deleteRc;
                }
            }

            for (const auto& [_, recordData] : table->Records())
            {
                if (!recordData)
                {
                    continue;
                }
                const ErrorCode rc =
                    RebuildCompositeIndexEntriesForRecord(table, indexDef, *recordData, effectiveSchemaIndexRowId);
                if (Failed(rc))
                {
                    return rc;
                }
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::RebuildCompositeIndexesForTable(SqliteTable* table)
        {
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }

            SCTableSchemaSnapshot snapshot;
            const ErrorCode snapshotRc = table->Schema()->GetSchemaSnapshot(&snapshot);
            if (Failed(snapshotRc))
            {
                return snapshotRc;
            }

            for (const SCIndexDef& indexDef : snapshot.indexes)
            {
                const std::int64_t schemaIndexRowId = table->Schema()->FindIndexRowId(indexDef.name.c_str());
                const ErrorCode persistRc = PersistQueryIndexDefinition(table->Schema(), indexDef, schemaIndexRowId);
                if (Failed(persistRc))
                {
                    return persistRc;
                }

                const ErrorCode rebuildRc = RebuildCompositeIndexEntriesForTable(table, indexDef);
                if (Failed(rebuildRc))
                {
                    return rebuildRc;
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::AnalyzeCompositeIndexPlan(const QueryPlan& inputPlan, QueryPlan* outPlan)
        {
            if (outPlan == nullptr)
            {
                return SC_E_POINTER;
            }

            QueryPlan analyzed = inputPlan;
            analyzed.matchedIndex.reset();
            analyzed.pushdown.pushdownConditions.clear();
            analyzed.pushdown.residualConditions.clear();

            if (inputPlan.target.type != QueryTargetType::Table || inputPlan.conditionGroups.size() != 1 ||
                inputPlan.conditionGroupLogic != QueryLogicOperator::And || inputPlan.conditionGroups.front().logic != QueryLogicOperator::And)
            {
                *outPlan = std::move(analyzed);
                return SC_OK;
            }

            const auto tableIt = tables_.find(inputPlan.target.name);
            if (tableIt == tables_.end())
            {
                return SC_E_TABLE_NOT_FOUND;
            }
            auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
            auto* schema = table->Schema();

            SCTableSchemaSnapshot snapshot;
            const ErrorCode snapshotRc = schema->GetSchemaSnapshot(&snapshot);
            if (Failed(snapshotRc))
            {
                return snapshotRc;
            }

            const QueryConditionGroup& group = inputPlan.conditionGroups.front();
            std::optional<QueryMatchedIndexSpec> bestMatch;
            std::vector<QueryCondition> bestPushdown;
            std::vector<QueryCondition> bestResidual;
            auto isConditionConsumedByPushdown = [](const QueryCondition& condition,
                                                    const std::vector<QueryCondition>& usedPushdown) {
                return std::find_if(usedPushdown.begin(),
                                    usedPushdown.end(),
                                    [&condition](const QueryCondition& used) {
                                        return used.fieldName == condition.fieldName && used.op == condition.op &&
                                               used.values == condition.values;
                                    }) != usedPushdown.end();
            };
            auto isConditionImpliedByPushdown = [](const QueryCondition& condition,
                                                   const std::vector<QueryCondition>& usedPushdown) {
                if (condition.op != QueryConditionOperator::IsNotNull)
                {
                    return false;
                }

                return std::find_if(
                           usedPushdown.begin(),
                           usedPushdown.end(),
                           [&condition](const QueryCondition& used) {
                               return used.fieldName == condition.fieldName &&
                                      used.op == QueryConditionOperator::Equal && used.values.size() == 1 &&
                                      !used.values.front().IsNull();
                           }) != usedPushdown.end();
            };
            auto isBetterCandidate = [](const QueryMatchedIndexSpec& candidate,
                                        const std::vector<QueryCondition>& candidateResidual,
                                        const QueryMatchedIndexSpec& best,
                                        const std::vector<QueryCondition>& bestResidual) {
                if (candidate.matchedPrefixLength != best.matchedPrefixLength)
                {
                    return candidate.matchedPrefixLength > best.matchedPrefixLength;
                }
                if (candidate.equalityPrefixLength != best.equalityPrefixLength)
                {
                    return candidate.equalityPrefixLength > best.equalityPrefixLength;
                }
                if (candidate.exactOrderCovered != best.exactOrderCovered)
                {
                    return candidate.exactOrderCovered && !best.exactOrderCovered;
                }
                if (candidate.orderCovered != best.orderCovered)
                {
                    return candidate.orderCovered && !best.orderCovered;
                }
                if (candidateResidual.size() != bestResidual.size())
                {
                    return candidateResidual.size() < bestResidual.size();
                }
                if (candidate.hasRangeCondition != best.hasRangeCondition)
                {
                    return !candidate.hasRangeCondition && best.hasRangeCondition;
                }
                return candidate.indexName < best.indexName;
            };

            for (const SCIndexDef& indexDef : snapshot.indexes)
            {
                if (!IsCompositeIndexExplicit(indexDef))
                {
                    continue;
                }

                bool hasBinaryColumn = false;
                for (const SCIndexColumnDef& indexColumn : indexDef.columns)
                {
                    const SCColumnDef* columnDef = schema->FindColumnDef(indexColumn.columnName);
                    if (columnDef == nullptr)
                    {
                        return SC_E_COLUMN_NOT_FOUND;
                    }
                    if (columnDef->valueKind == ValueKind::Binary)
                    {
                        hasBinaryColumn = true;
                        break;
                    }
                }
                if (hasBinaryColumn)
                {
                    continue;
                }

                QueryMatchedIndexSpec candidate;
                candidate.indexName = indexDef.name;
                for (const SCIndexColumnDef& column : indexDef.columns)
                {
                    candidate.keyColumns.push_back(column.columnName);
                }

                std::vector<QueryCondition> usedPushdown;
                std::vector<QueryCondition> residual = group.conditions;
                bool encounteredRange = false;
                for (const SCIndexColumnDef& column : indexDef.columns)
                {
                    if (encounteredRange)
                    {
                        break;
                    }

                    const auto equalityIt =
                        std::find_if(group.conditions.begin(), group.conditions.end(), [&column](const QueryCondition& condition) {
                            return condition.fieldName == column.columnName && IsEqualityIndexOperator(condition.op) &&
                                   condition.values.size() == 1;
                        });
                    if (equalityIt != group.conditions.end())
                    {
                        ++candidate.matchedPrefixLength;
                        ++candidate.equalityPrefixLength;
                        usedPushdown.push_back(*equalityIt);
                        continue;
                    }

                    const auto rangeIt =
                        std::find_if(group.conditions.begin(), group.conditions.end(), [&column](const QueryCondition& condition) {
                            return condition.fieldName == column.columnName && IsRangeIndexOperator(condition.op);
                        });
                    if (rangeIt == group.conditions.end())
                    {
                        break;
                    }

                    ++candidate.matchedPrefixLength;
                    candidate.hasRangeCondition = true;
                    usedPushdown.push_back(*rangeIt);
                    encounteredRange = true;
                    break;
                }

                if (candidate.matchedPrefixLength == 0)
                {
                    continue;
                }

                residual.erase(std::remove_if(residual.begin(),
                                              residual.end(),
                                              [&usedPushdown, &isConditionConsumedByPushdown, &isConditionImpliedByPushdown](
                                                  const QueryCondition& condition) {
                                                  return isConditionConsumedByPushdown(condition, usedPushdown) ||
                                                         isConditionImpliedByPushdown(condition, usedPushdown);
                                              }),
                               residual.end());

                candidate.orderCovered = !inputPlan.orderBy.empty() && !candidate.hasRangeCondition;
                candidate.exactOrderCovered = candidate.orderCovered;
                if (candidate.orderCovered)
                {
                    std::size_t orderColumnIndex = candidate.equalityPrefixLength;
                    for (const SortSpec& sort : inputPlan.orderBy)
                    {
                        if (orderColumnIndex >= indexDef.columns.size() ||
                            indexDef.columns[orderColumnIndex].columnName != sort.fieldName)
                        {
                            candidate.orderCovered = false;
                            candidate.exactOrderCovered = false;
                            break;
                        }

                        const bool indexDescending = indexDef.columns[orderColumnIndex].descending;
                        const bool sortDescending = sort.direction == QueryOrderDirection::Descending;
                        if (indexDescending != sortDescending)
                        {
                            candidate.orderCovered = false;
                            candidate.exactOrderCovered = false;
                            break;
                        }
                        ++orderColumnIndex;
                    }
                }

                if (!bestMatch.has_value() || isBetterCandidate(candidate, residual, *bestMatch, bestResidual))
                {
                    bestMatch = candidate;
                    bestPushdown = std::move(usedPushdown);
                    bestResidual = std::move(residual);
                }
            }

            if (bestMatch.has_value())
            {
                analyzed.matchedIndex = bestMatch;
                analyzed.pushdown.pushdownConditions = std::move(bestPushdown);
                analyzed.pushdown.residualConditions = std::move(bestResidual);
                analyzed.pushdownConditionCount =
                    static_cast<std::uint32_t>(analyzed.pushdown.pushdownConditions.size());
                analyzed.fallbackConditionCount =
                    static_cast<std::uint32_t>(analyzed.pushdown.residualConditions.size());
                analyzed.state =
                    analyzed.pushdown.residualConditions.empty() ? QueryPlanState::DirectIndex : QueryPlanState::PartialIndex;
                analyzed.fallbackReason.clear();
            } else if (analyzed.constraints.requireIndex)
            {
                analyzed.state = QueryPlanState::Unsupported;
                analyzed.fallbackReason = L"index-required";
            }

            *outPlan = std::move(analyzed);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::CollectCompositeIndexRecordIds(const QueryPlan& analyzedPlan,
                                                                 std::vector<RecordId>* outRecordIds,
                                                                 std::uint64_t* outScannedEntries)
        {
            if (outRecordIds == nullptr)
            {
                return SC_E_POINTER;
            }
            outRecordIds->clear();
            if (outScannedEntries != nullptr)
            {
                *outScannedEntries = 0;
            }
            if (!analyzedPlan.matchedIndex.has_value())
            {
                return SC_E_INVALIDARG;
            }

            const auto tableIt = tables_.find(analyzedPlan.target.name);
            if (tableIt == tables_.end())
            {
                return SC_E_TABLE_NOT_FOUND;
            }

            auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
            const std::int64_t schemaIndexRowId =
                FindQueryIndexStorageRowId(table->TableRowId(), analyzedPlan.matchedIndex->indexName);
            if (schemaIndexRowId <= 0)
            {
                return SC_E_INDEX_NOT_FOUND;
            }

            std::string sql = "SELECT record_id FROM query_index_entries WHERE schema_index_id = ? AND alive_flag = 1";
            CompositeIndexLookupBounds bounds;
            bool useRangeFullVerificationScan = analyzedPlan.matchedIndex->hasRangeCondition;
            if (!useRangeFullVerificationScan)
            {
                const ErrorCode boundsRc = BuildCompositeLookupBounds(table->Schema(), analyzedPlan, &bounds);
                if (Failed(boundsRc))
                {
                    return boundsRc;
                }
                if (bounds.exactMatch && bounds.exactPrefixLength == 0)
                {
                    return SC_E_INVALIDARG;
                }
                if (!bounds.exactMatch && !bounds.hasLowerBound && !bounds.hasUpperBound)
                {
                    return SC_E_INVALIDARG;
                }

                if (bounds.exactMatch)
                {
                    const char* keyColumn = bounds.exactPrefixLength == 1
                                                ? "key_prefix_1"
                                                : (bounds.exactPrefixLength == 2 ? "key_prefix_2" : "key_prefix_3");
                    sql += " AND ";
                    sql += keyColumn;
                    sql += " = ?";
                } else
                {
                    if (bounds.equalityPrefixLength > 0)
                    {
                        const char* keyColumn = bounds.equalityPrefixLength == 1
                                                    ? "key_prefix_1"
                                                    : (bounds.equalityPrefixLength == 2 ? "key_prefix_2" : "key_prefix_3");
                        sql += " AND ";
                        sql += keyColumn;
                        sql += " = ?";
                    }
                    if (bounds.hasLowerBound)
                    {
                        sql += bounds.includeLowerBound ? " AND full_key >= ?" : " AND full_key > ?";
                    }
                    if (bounds.hasUpperBound)
                    {
                        sql += bounds.includeUpperBound ? " AND full_key <= ?" : " AND full_key < ?";
                    }
                }
            }
            else if (analyzedPlan.matchedIndex->equalityPrefixLength > 0)
            {
                const ErrorCode boundsRc = BuildCompositeEqualityPrefixBounds(table->Schema(), analyzedPlan, &bounds);
                if (Succeeded(boundsRc) && bounds.exactPrefixLength > 0)
                {
                    const char* keyColumn = bounds.exactPrefixLength == 1
                                                ? "key_prefix_1"
                                                : (bounds.exactPrefixLength == 2 ? "key_prefix_2" : "key_prefix_3");
                    sql += " AND ";
                    sql += keyColumn;
                    sql += " = ?";
                }
            }
            sql += " ORDER BY full_key, record_id;";
            SqliteStmt stmt = db_.Prepare(sql.c_str());
            stmt.BindInt64(1, schemaIndexRowId);
            int bindIndex = 2;
            if (!useRangeFullVerificationScan && bounds.exactMatch)
            {
                stmt.BindBlob(bindIndex++, bounds.exactPrefixKey);
            } else if (!useRangeFullVerificationScan)
            {
                if (bounds.equalityPrefixLength > 0)
                {
                    stmt.BindBlob(bindIndex++, bounds.equalityPrefixKey);
                }
                if (bounds.hasLowerBound)
                {
                    stmt.BindBlob(bindIndex++, bounds.lowerBound);
                }
                if (bounds.hasUpperBound)
                {
                    stmt.BindBlob(bindIndex++, bounds.upperBound);
                }
            } else if (bounds.exactPrefixLength > 0)
            {
                stmt.BindBlob(bindIndex++, bounds.exactPrefixKey);
            }
            bool hasRow = false;
            while (stmt.Step(&hasRow) == SC_OK && hasRow)
            {
                outRecordIds->push_back(stmt.ColumnInt64(0));
                if (outScannedEntries != nullptr)
                {
                    ++(*outScannedEntries);
                }
            }
            return SC_OK;
        }

        bool SqliteDatabase::HasAliveRecords(SqliteSchema* schema) const
        {
            if (schema == nullptr)
            {
                return false;
            }

            const auto tableIt = std::find_if(
                tables_.begin(),
                tables_.end(),
                [tableRowId = schema->TableRowId()](const std::pair<const std::wstring, SCTablePtr>& entry) {
                    const auto* table = static_cast<SqliteTable*>(entry.second.Get());
                    return table != nullptr && table->TableRowId() == tableRowId;
                });
            if (tableIt == tables_.end())
            {
                return false;
            }

            auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
            if (table == nullptr)
            {
                return false;
            }

            for (const auto& [_, data] : table->Records())
            {
                if (data != nullptr && data->state == RecordState::Alive)
                {
                    return true;
                }
            }
            return false;
        }

        ErrorCode SqliteDatabase::ValidateRequiredValuesForCommit() const
        {
            for (const auto& [_, tableRef] : tables_)
            {
                auto* table = static_cast<SqliteTable*>(tableRef.Get());
                if (table == nullptr)
                {
                    continue;
                }

                SCSchemaPtr schema;
                const ErrorCode schemaRc = table->GetSchema(schema);
                if (Failed(schemaRc) || !schema)
                {
                    return Failed(schemaRc) ? schemaRc : SC_E_FAIL;
                }

                std::int32_t columnCount = 0;
                const ErrorCode countRc = schema->GetColumnCount(&columnCount);
                if (Failed(countRc))
                {
                    return countRc;
                }

                for (std::int32_t columnIndex = 0; columnIndex < columnCount; ++columnIndex)
                {
                    SCColumnDef column;
                    const ErrorCode columnRc = schema->GetColumn(columnIndex, &column);
                    if (Failed(columnRc))
                    {
                        return columnRc;
                    }

                    if (column.nullable || !column.defaultValue.IsNull())
                    {
                        continue;
                    }

                    for (const auto& [recordId, data] : table->Records())
                    {
                        (void)recordId;
                        if (data == nullptr || data->state == RecordState::Deleted)
                        {
                            continue;
                        }

                        const auto valueIt = data->values.find(column.name);
                        if (valueIt == data->values.end() || valueIt->second.IsNull())
                        {
                            return SC_E_SCHEMA_VIOLATION;
                        }
                    }
                }
            }

            return SC_OK;
        }

        SqliteDatabase::JournalLookup SqliteDatabase::LookupRecordJournalState(const std::wstring& tableName,
                                                                               RecordId recordId) const
        {
            JournalLookup lookup;
            for (const auto& entry : activeJournal_.entries)
            {
                if (!JournalEntryMatchesCurrentTableName(entry, tableName) ||
                    entry.recordId != recordId)
                {
                    continue;
                }
                if (entry.op == JournalOp::CreateRecord)
                {
                    lookup.createdInActiveEdit = true;
                } else if (entry.op == JournalOp::DeleteRecord)
                {
                    lookup.deletedInActiveEdit = true;
                }
            }
            return lookup;
        }

        void SqliteDatabase::RemoveFieldJournalEntries(const std::wstring& tableName, RecordId recordId)
        {
            activeJournal_.entries.erase(
                std::remove_if(activeJournal_.entries.begin(),
                               activeJournal_.entries.end(),
                               [&](const JournalEntry& entry) {
                                   return JournalEntryMatchesCurrentTableName(entry, tableName) &&
                                          entry.recordId == recordId &&
                                          (entry.op == JournalOp::SetValue || entry.op == JournalOp::SetRelation);
                               }),
                activeJournal_.entries.end());
        }

        void SqliteDatabase::RemoveAllJournalEntriesForRecord(const std::wstring& tableName, RecordId recordId)
        {
            activeJournal_.entries.erase(std::remove_if(activeJournal_.entries.begin(),
                                                        activeJournal_.entries.end(),
                                                        [&](const JournalEntry& entry) {
                                                            return JournalEntryMatchesCurrentTableName(entry, tableName) &&
                                                                   entry.recordId == recordId;
                                                        }),
                                         activeJournal_.entries.end());
        }

        ErrorCode SqliteDatabase::WriteValue(SqliteTable* table,
                                             const std::shared_ptr<SqliteRecordData>& data,
                                             const std::wstring& fieldName,
                                             const SCValue& value)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            ClearConstraintViolation();
            const ErrorCode validate = ValidateWrite(table, data, fieldName, value);
            if (Failed(validate))
            {
                return validate;
            }

            const JournalLookup lookup = LookupRecordJournalState(table->Name(), data->id);
            if (lookup.deletedInActiveEdit)
            {
                return SC_E_RECORD_DELETED;
            }

            const SCColumnDef* column = table->Schema()->FindColumnDef(fieldName);
            SCValue storedValue = value;
            if (column != nullptr && column->columnKind == ColumnKind::Relation &&
                !column->referenceStorageColumn.empty() && !value.IsNull())
            {
                const ErrorCode relationRc = ResolveRelationWriteValue(*column, value, &storedValue);
                if (Failed(relationRc))
                {
                    return relationRc;
                }
            }

            SCValue oldValue = column->defaultValue;
            const auto existing = data->values.find(fieldName);
            if (existing != data->values.end())
            {
                oldValue = existing->second;
            }

            if (oldValue == storedValue)
            {
                return SC_OK;
            }

            const ErrorCode constraintRc =
                ValidateTableConstraints(table, data, &fieldName, &storedValue);
            if (Failed(constraintRc))
            {
                return constraintRc;
            }

            const bool hasForeignKeyReferences = HasForeignKeyReferencesToColumn(table->Name(), fieldName);
            if (hasForeignKeyReferences)
            {
                const ErrorCode foreignKeyActionRc =
                    ApplyForeignKeyActionsForColumnUpdate(table, data, fieldName, oldValue, storedValue);
                if (Failed(foreignKeyActionRc))
                {
                    return foreignKeyActionRc;
                }
            }

            data->values[fieldName] = storedValue;
            const JournalOp op = (column != nullptr && column->columnKind == ColumnKind::Relation)
                                     ? JournalOp::SetRelation
                                     : JournalOp::SetValue;
            RecordJournal(table->Name(), data->id, fieldName, oldValue, storedValue, false, false, op);
            MarkReferenceIndexDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::DeleteRecord(SqliteTable* table, const std::shared_ptr<SqliteRecordData>& data)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            ClearConstraintViolation();
            if (!activeEdit_)
            {
                return SC_E_NO_ACTIVE_EDIT;
            }
            if (data->state == RecordState::Deleted)
            {
                return SC_E_RECORD_DELETED;
            }
            if (HasForeignKeyReferencesToTable(table->Name()))
            {
                const ErrorCode foreignKeyActionRc = ApplyForeignKeyActionsForTableDelete(table, data);
                if (Failed(foreignKeyActionRc))
                {
                    return foreignKeyActionRc;
                }
            }
            if (IsRecordReferenced(table->Name(), data->id))
            {
                return SC_E_CONSTRAINT_VIOLATION;
            }

            const JournalLookup lookup = LookupRecordJournalState(table->Name(), data->id);
            if (lookup.deletedInActiveEdit)
            {
                return SC_E_RECORD_DELETED;
            }

            data->state = RecordState::Deleted;
            if (lookup.createdInActiveEdit)
            {
                RemoveAllJournalEntriesForRecord(table->Name(), data->id);
                data->values.clear();
                MarkReferenceIndexDirty();
                return SC_OK;
            }

            RemoveFieldJournalEntries(table->Name(), data->id);
            RecordJournal(
                table->Name(), data->id, L"", SCValue::Null(), SCValue::Null(), false, true, JournalOp::DeleteRecord);
            MarkReferenceIndexDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::GetReferencesBySource(const std::wstring& sourceTable,
                                                        RecordId sourceRecordId,
                                                        std::vector<ReferenceRecord>* outRecords) const
        {
            if (outRecords == nullptr)
            {
                return SC_E_POINTER;
            }
            outRecords->clear();

            const auto tableIt = tables_.find(sourceTable);
            if (tableIt == tables_.end())
            {
                return SC_OK;
            }

            auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
            if (table == nullptr)
            {
                return SC_OK;
            }

            const auto recordIt = table->Records().find(sourceRecordId);
            if (recordIt == table->Records().end() || recordIt->second == nullptr ||
                recordIt->second->state == RecordState::Deleted)
            {
                return SC_OK;
            }

            SCSchemaPtr schema;
            const ErrorCode schemaRc = table->GetSchema(schema);
            if (Failed(schemaRc) || !schema)
            {
                return schemaRc;
            }

            std::int32_t columnCount = 0;
            const ErrorCode countRc = schema->GetColumnCount(&columnCount);
            if (Failed(countRc))
            {
                return countRc;
            }

            for (std::int32_t columnIndex = 0; columnIndex < columnCount; ++columnIndex)
            {
                SCColumnDef column;
                if (Failed(schema->GetColumn(columnIndex, &column)))
                {
                    continue;
                }
                if (column.columnKind != ColumnKind::Relation)
                {
                    continue;
                }

                SCRecordPtr record;
                if (Failed(table->GetRecord(sourceRecordId, record)) || !record)
                {
                    continue;
                }

                SCValue relationValue;
                if (Failed(record->GetValue(column.name.c_str(), &relationValue)))
                {
                    continue;
                }

                RecordId targetRecordId = 0;
                if (Failed(ResolveRelationTargetRecordId(column, relationValue, &targetRecordId)))
                {
                    continue;
                }

                outRecords->push_back(ReferenceRecord{sourceTable,
                                                      sourceRecordId,
                                                      column.name,
                                                      column.referenceTable,
                                                      targetRecordId,
                                                      version_,
                                                      0,
                                                      std::nullopt});
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::GetReferencesByTarget(const std::wstring& targetTable,
                                                        RecordId targetRecordId,
                                                        std::vector<ReverseReferenceRecord>* outRecords) const
        {
            if (outRecords == nullptr)
            {
                return SC_E_POINTER;
            }
            outRecords->clear();

            for (const auto& [_, tableRef] : tables_)
            {
                auto* table = static_cast<SqliteTable*>(tableRef.Get());
                if (table == nullptr)
                {
                    continue;
                }

                SCSchemaPtr schema;
                if (Failed(table->GetSchema(schema)) || !schema)
                {
                    continue;
                }

                std::int32_t columnCount = 0;
                if (Failed(schema->GetColumnCount(&columnCount)))
                {
                    continue;
                }

                for (std::int32_t columnIndex = 0; columnIndex < columnCount; ++columnIndex)
                {
                    SCColumnDef column;
                    if (Failed(schema->GetColumn(columnIndex, &column)))
                    {
                        continue;
                    }
                    if (column.columnKind != ColumnKind::Relation || column.referenceTable != targetTable)
                    {
                        continue;
                    }

                    for (const auto& [candidateId, candidateData] : table->Records())
                    {
                        if (candidateData == nullptr || candidateData->state == RecordState::Deleted)
                        {
                            continue;
                        }

                        SCRecordPtr record;
                        if (Failed(table->GetRecord(candidateId, record)) || !record)
                        {
                            continue;
                        }

                        SCValue relationValue;
                        if (Failed(record->GetValue(column.name.c_str(), &relationValue)))
                        {
                            continue;
                        }

                        RecordId referencedId = 0;
                        if (Succeeded(ResolveRelationTargetRecordId(column, relationValue, &referencedId)) &&
                            referencedId == targetRecordId)
                        {
                            outRecords->push_back(ReverseReferenceRecord{targetTable,
                                                                         targetRecordId,
                                                                         table->Name(),
                                                                         candidateId,
                                                                         column.name,
                                                                         version_,
                                                                         0,
                                                                         std::nullopt});
                        }
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::CheckReferenceIndex(ReferenceIndexCheckResult* outResult) const
        {
            if (outResult == nullptr)
            {
                return SC_E_POINTER;
            }

            if (!referenceIndexBuilt_)
            {
                outResult->state = ReferenceIndexHealthState::Missing;
                outResult->message = L"reference-index-not-built";
            } else if (referenceIndexDirty_)
            {
                outResult->state = ReferenceIndexHealthState::OutOfDate;
                outResult->message = L"reference-index-rebuild-required";
            } else
            {
                outResult->state = ReferenceIndexHealthState::Healthy;
                outResult->message = L"reference-index-current";
            }
            outResult->indexVersion = static_cast<std::int32_t>(referenceIndexVersion_);
            outResult->expectedVersion = static_cast<std::int32_t>(version_);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::GetAllReferencesDiagnosticOnly(ReferenceIndex* outIndex) const
        {
            if (outIndex == nullptr)
            {
                return SC_E_POINTER;
            }

            outIndex->records.clear();
            for (const auto& [_, tableRef] : tables_)
            {
                auto* table = static_cast<SqliteTable*>(tableRef.Get());
                if (table == nullptr)
                {
                    continue;
                }

                SCSchemaPtr schema;
                if (Failed(table->GetSchema(schema)) || !schema)
                {
                    continue;
                }

                std::int32_t columnCount = 0;
                if (Failed(schema->GetColumnCount(&columnCount)))
                {
                    continue;
                }

                for (const auto& [recordId, recordData] : table->Records())
                {
                    if (recordData == nullptr || recordData->state == RecordState::Deleted)
                    {
                        continue;
                    }

                    for (std::int32_t columnIndex = 0; columnIndex < columnCount; ++columnIndex)
                    {
                        SCColumnDef column;
                        if (Failed(schema->GetColumn(columnIndex, &column)))
                        {
                            continue;
                        }
                        if (column.columnKind != ColumnKind::Relation)
                        {
                            continue;
                        }

                        SCRecordPtr record;
                        if (Failed(table->GetRecord(recordId, record)) || !record)
                        {
                            continue;
                        }

                        SCValue relationValue;
                        if (Failed(record->GetValue(column.name.c_str(), &relationValue)))
                        {
                            continue;
                        }

                        RecordId targetId = 0;
                        if (Failed(ResolveRelationTargetRecordId(column, relationValue, &targetId)))
                        {
                            continue;
                        }

                        outIndex->records.push_back(ReferenceRecord{table->Name(),
                                                                    recordId,
                                                                    column.name,
                                                                    column.referenceTable,
                                                                    targetId,
                                                                    version_,
                                                                    0,
                                                                    std::nullopt});
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::RebuildReferenceIndexes()
        {
            if (readOnly_)
            {
                return SC_E_READ_ONLY_DATABASE;
            }

            ReferenceIndex index;
            const ErrorCode rc = GetAllReferencesDiagnosticOnly(&index);
            if (Failed(rc))
            {
                return rc;
            }

            referenceIndexBuilt_ = true;
            referenceIndexDirty_ = false;
            referenceIndexVersion_ = version_;
            return SC_OK;
        }

        ErrorCode SqliteDatabase::CommitReferenceDelta(const ReferenceIndex& forwardDelta,
                                                       const ReverseReferenceIndex& reverseDelta)
        {
            if (forwardDelta.records.size() != reverseDelta.records.size())
            {
                return SC_E_INVALIDARG;
            }

            return RebuildReferenceIndexes();
        }

        void SqliteDatabase::MarkReferenceIndexDirty() noexcept
        {
            referenceIndexDirty_ = true;
        }

        void SqliteDatabase::MarkForeignKeyReferenceCacheDirty() noexcept
        {
            foreignKeyReferenceCacheDirty_ = true;
        }

        ErrorCode SqliteDatabase::RefreshForeignKeyReferenceCache() const
        {
            foreignKeyReferenceCacheByTable_.clear();
            foreignKeyReferenceCacheByTableAndColumn_.clear();

            for (const auto& [_, tableRef] : tables_)
            {
                auto* sourceTable = static_cast<SqliteTable*>(tableRef.Get());
                if (sourceTable == nullptr)
                {
                    continue;
                }

                SCSchemaPtr sourceSchema;
                if (Failed(sourceTable->GetSchema(sourceSchema)) || !sourceSchema)
                {
                    continue;
                }

                std::int32_t constraintCount = 0;
                const ErrorCode countRc = sourceSchema->GetConstraintCount(&constraintCount);
                if (Failed(countRc))
                {
                    return countRc;
                }

                for (std::int32_t index = 0; index < constraintCount; ++index)
                {
                    SCConstraintDef constraint;
                    const ErrorCode constraintRc = sourceSchema->GetConstraint(index, &constraint);
                    if (Failed(constraintRc))
                    {
                        return constraintRc;
                    }

                    if (constraint.kind != SCConstraintKind::ForeignKey || constraint.referencedTable.empty())
                    {
                        continue;
                    }

                    foreignKeyReferenceCacheByTable_[ToUpperCopy(constraint.referencedTable)].push_back(
                        ForeignKeyReferenceEntry{sourceTable->Name(), constraint});

                    for (std::size_t index = 0; index < constraint.columns.size(); ++index)
                    {
                        const std::wstring resolvedReferencedColumn = ResolveForeignKeyReferencedColumn(constraint, index);
                        if (resolvedReferencedColumn.empty())
                        {
                            continue;
                        }
                        foreignKeyReferenceCacheByTableAndColumn_[MakeForeignKeyReferenceCacheKey(constraint.referencedTable,
                                                                                                  resolvedReferencedColumn)]
                            .push_back(ForeignKeyReferenceEntry{sourceTable->Name(), constraint});
                    }
                }
            }

            foreignKeyReferenceCacheDirty_ = false;
            return SC_OK;
        }

        const std::vector<ForeignKeyReferenceEntry>*
        SqliteDatabase::GetForeignKeyReferenceEntries(const std::wstring& tableName) const
        {
            if (foreignKeyReferenceCacheDirty_)
            {
                const ErrorCode rc = RefreshForeignKeyReferenceCache();
                if (Failed(rc))
                {
                    return nullptr;
                }
            }

            const auto it = foreignKeyReferenceCacheByTable_.find(ToUpperCopy(tableName));
            if (it == foreignKeyReferenceCacheByTable_.end())
            {
                return nullptr;
            }
            return &it->second;
        }

        const std::vector<ForeignKeyReferenceEntry>*
        SqliteDatabase::GetForeignKeyReferenceEntries(const std::wstring& tableName, const std::wstring& columnName) const
        {
            if (foreignKeyReferenceCacheDirty_)
            {
                const ErrorCode rc = RefreshForeignKeyReferenceCache();
                if (Failed(rc))
                {
                    return nullptr;
                }
            }

            const auto it = foreignKeyReferenceCacheByTableAndColumn_.find(
                MakeForeignKeyReferenceCacheKey(tableName, columnName));
            if (it == foreignKeyReferenceCacheByTableAndColumn_.end())
            {
                return nullptr;
            }
            return &it->second;
        }

        ErrorCode SqliteDatabase::SyncLegacyIndexMetadata(SqliteSchema* schema,
                                                          const std::wstring& columnName,
                                                          bool indexed)
        {
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            const std::wstring indexName = schema->LegacyIndexName(columnName);
            SCIndexDef index;
            index.name = indexName;
            index.sourceKind = SCSchemaSourceKind::Explicit;
            index.columns.push_back(SCIndexColumnDef{columnName, false});

            {
                SqliteStmt deleteColumnsStmt = db_.Prepare(
                    "DELETE FROM schema_index_columns WHERE index_id IN ("
                    "SELECT index_id FROM schema_indexes WHERE table_id = ? "
                    "AND name = ?);");
                deleteColumnsStmt.BindInt64(1, schema->TableRowId());
                deleteColumnsStmt.BindText(2, indexName);
                const ErrorCode rc = deleteColumnsStmt.Step();
                if (Failed(rc))
                {
                    return rc;
                }
            }

            {
                SqliteStmt deleteIndexStmt = db_.Prepare(
                    "DELETE FROM schema_indexes WHERE table_id = ? AND "
                    "name = ?;");
                deleteIndexStmt.BindInt64(1, schema->TableRowId());
                deleteIndexStmt.BindText(2, indexName);
                const ErrorCode rc = deleteIndexStmt.Step();
                if (Failed(rc))
                {
                    return rc;
                }
            }

            {
                const ErrorCode removeQueryIndexRc =
                    RemoveQueryIndexDefinition(schema, indexName.c_str(), false);
                if (Failed(removeQueryIndexRc))
                {
                    return removeQueryIndexRc;
                }
            }

            if (!indexed)
            {
                schema->UnloadIndex(indexName.c_str());
                queryIndexRowIdsByTableAndName_.erase(
                    BuildQueryIndexStorageKey(schema->TableRowId(), indexName));
                return SC_OK;
            }

            SqliteStmt insertIndexStmt = db_.Prepare(
                "INSERT INTO schema_indexes(table_id, name, source_kind) "
                "VALUES(?, ?, ?);");
            insertIndexStmt.BindInt64(1, schema->TableRowId());
            insertIndexStmt.BindText(2, indexName);
            insertIndexStmt.BindInt(3, ToSqliteSchemaSourceKind(SCSchemaSourceKind::Explicit));
            const ErrorCode insertRc = insertIndexStmt.Step();
            if (Failed(insertRc))
            {
                return insertRc;
            }

            const std::int64_t indexId = db_.LastInsertRowId();
            SqliteStmt insertColumnStmt = db_.Prepare(
                "INSERT INTO schema_index_columns(index_id, column_ordinal, "
                "column_name, descending_flag) VALUES(?, 0, ?, 0);");
            insertColumnStmt.BindInt64(1, indexId);
            insertColumnStmt.BindText(2, columnName);
            const ErrorCode columnRc = insertColumnStmt.Step();
            if (Failed(columnRc))
            {
                return columnRc;
            }

            const ErrorCode queryIndexRc = PersistQueryIndexDefinition(schema, index, indexId);
            if (Failed(queryIndexRc))
            {
                return queryIndexRc;
            }

            // Keep the legacy indexed-column path aligned with query-index storage so the executor can resolve it.
            const auto tableIt = std::find_if(
                tables_.begin(),
                tables_.end(),
                [tableRowId = schema->TableRowId()](const std::pair<const std::wstring, SCTablePtr>& entry) {
                    const auto* table = static_cast<const SqliteTable*>(entry.second.Get());
                    return table != nullptr && table->TableRowId() == tableRowId;
                });
            if (tableIt != tables_.end())
            {
                auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
                const ErrorCode rebuildRc = RebuildCompositeIndexEntriesForTable(table, index, indexId);
                if (Failed(rebuildRc))
                {
                    return rebuildRc;
                }
            }

            schema->UnloadIndex(indexName.c_str());
            schema->LoadIndex(index, indexId);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::RemoveLegacyPrimaryKeyMetadata(SqliteSchema* schema, const std::wstring& columnName)
        {
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            if (!EqualsIgnoreCase(columnName, L"Id"))
            {
                return SC_OK;
            }

            const std::wstring pkName = schema->LegacyPrimaryKeyName();

            {
                SqliteStmt deleteColumnsStmt = db_.Prepare(
                    "DELETE FROM schema_constraint_columns WHERE constraint_id "
                    "IN (SELECT constraint_id FROM schema_constraints WHERE "
                    "table_id = ? AND kind = ? AND name = ?);");
                deleteColumnsStmt.BindInt64(1, schema->TableRowId());
                deleteColumnsStmt.BindInt(2, ToSqliteConstraintKind(SCConstraintKind::PrimaryKey));
                deleteColumnsStmt.BindText(3, pkName);
                const ErrorCode rc = deleteColumnsStmt.Step();
                if (Failed(rc))
                {
                    return rc;
                }
            }

            SqliteStmt deleteConstraintStmt = db_.Prepare(
                "DELETE FROM schema_constraints WHERE table_id = ? AND kind = ? "
                "AND name = ?;");
            deleteConstraintStmt.BindInt64(1, schema->TableRowId());
            deleteConstraintStmt.BindInt(2, ToSqliteConstraintKind(SCConstraintKind::PrimaryKey));
            deleteConstraintStmt.BindText(3, pkName);
            const ErrorCode deleteRc = deleteConstraintStmt.Step();
            if (Failed(deleteRc))
            {
                return deleteRc;
            }

            schema->RemovePrimaryKeyIfColumnMatches(columnName);
            return SC_OK;
        }

        void SqliteDatabase::RefreshReferenceIndexState()
        {
            const ErrorCode rc = RebuildReferenceIndexes();
            if (Failed(rc))
            {
                referenceIndexBuilt_ = false;
                referenceIndexVersion_ = 0;
            }
        }

        bool SqliteDatabase::IsRecordReferenced(const std::wstring& tableName, RecordId recordId) const
        {
            ReferenceIndexCheckResult check;
            if (Succeeded(CheckReferenceIndex(&check)) && check.state == ReferenceIndexHealthState::Healthy)
            {
                std::vector<ReverseReferenceRecord> refs;
                if (Succeeded(GetReferencesByTarget(tableName, recordId, &refs)))
                {
                    return !refs.empty();
                }
            }

            for (const auto& [_, tableRef] : tables_)
            {
                auto* table = static_cast<SqliteTable*>(tableRef.Get());
                if (table == nullptr)
                {
                    continue;
                }

                SCSchemaPtr schema;
                if (Failed(table->GetSchema(schema)) || !schema)
                {
                    continue;
                }

                std::int32_t columnCount = 0;
                if (Failed(schema->GetColumnCount(&columnCount)))
                {
                    continue;
                }

                for (std::int32_t columnIndex = 0; columnIndex < columnCount; ++columnIndex)
                {
                    SCColumnDef column;
                    if (Failed(schema->GetColumn(columnIndex, &column)))
                    {
                        continue;
                    }
                    if (column.columnKind != ColumnKind::Relation || column.referenceTable != tableName)
                    {
                        continue;
                    }

                    SCValue targetStoredValue;
                    if (Failed(ResolveRelationStoredValue(column, recordId, &targetStoredValue)))
                    {
                        continue;
                    }

                    for (const auto& [candidateId, candidateData] : table->Records())
                    {
                        if (candidateData == nullptr || candidateData->state == RecordState::Deleted)
                        {
                            continue;
                        }

                        SCRecordPtr record;
                        if (Failed(table->GetRecord(candidateId, record)) || !record)
                        {
                            continue;
                        }

                        SCValue relationValue;
                        if (Failed(record->GetValue(column.name.c_str(), &relationValue)))
                        {
                            continue;
                        }

                        if (relationValue == targetStoredValue)
                        {
                            return true;
                        }
                    }
                }
            }

            return false;
        }

        void SqliteDatabase::RecordCreate(SqliteTable* table, const std::shared_ptr<SqliteRecordData>& data)
        {
            RecordJournal(
                table->Name(), data->id, L"", SCValue::Null(), SCValue::Null(), true, false, JournalOp::CreateRecord);
        }

        ErrorCode SqliteDatabase::PersistSchemaAddColumn(SqliteSchema* schema,
                                                         const SCColumnDef& def,
                                                         std::int64_t rowId)
        {
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            SqliteStmt stmt = db_.Prepare(
                "INSERT INTO schema_columns("
                " rowid, table_id, column_name, display_name, value_kind, "
                "column_kind, nullable_flag, editable_flag,"
                " user_defined_flag, indexed_flag, "
                "participates_in_calc_flag, unit, reference_table,"
                " reference_storage_column, reference_display_column,"
                " default_kind, default_int64, default_double, "
                "default_bool, default_text, default_blob)"
                " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "?, ?, ?);");
            if (rowId > 0)
            {
                stmt.BindInt64(1, rowId);
            } else
            {
                stmt.BindNull(1);
            }
            stmt.BindInt64(2, schema->TableRowId());
            stmt.BindText(3, def.name);
            BindColumnDefForStorage(stmt,
                                    4,
                                    5,
                                    6,
                                    7,
                                    8,
                                    9,
                                    10,
                                    11,
                                    12,
                                    13,
                                    14,
                                    15,
                                    16,
                                    17,
                                    18,
                                    19,
                                    20,
                                    21,
                                    def);
            const ErrorCode rc = stmt.Step();
            if (Failed(rc))
            {
                return rc;
            }

            if (def.indexed)
            {
                const std::wstring indexName =
                    L"idx_fv_" + std::to_wstring(schema->TableRowId()) + L"_" + SanitizeIdentifier(def.name);
                SqliteStmt createIndexStmt =
                    db_.Prepare((std::string("CREATE INDEX IF NOT EXISTS ") + ToUtf8(indexName) +
                                 " ON field_values(table_id, column_name, "
                                 "int64_value, double_value, bool_value, text_value);")
                                    .c_str());
                const ErrorCode createIndexRc = createIndexStmt.Step();
                if (Failed(createIndexRc))
                {
                    return createIndexRc;
                }
            }

            const std::int64_t insertedRowId = rowId > 0 ? rowId : db_.LastInsertRowId();
            const ErrorCode metadataRc = SyncLegacyIndexMetadata(schema, def.name, def.indexed);
            if (Failed(metadataRc))
            {
                return metadataRc;
            }
            schema->LoadColumn(def, insertedRowId);
            MarkReferenceIndexDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistSchemaUpdateColumn(SqliteSchema* schema, const SCColumnDef& def)
        {
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            const SCColumnDef* previousDef = schema->FindColumnDef(def.name);
            if (previousDef == nullptr)
            {
                return SC_E_COLUMN_NOT_FOUND;
            }

            SqliteStmt stmt = db_.Prepare(
                "UPDATE schema_columns SET "
                " display_name = ?, value_kind = ?, column_kind = ?, "
                "nullable_flag = ?, editable_flag = ?, user_defined_flag = ?, "
                "indexed_flag = ?, participates_in_calc_flag = ?, unit = ?, "
                "reference_table = ?, reference_storage_column = ?, "
                "reference_display_column = ?, default_kind = ?, "
                "default_int64 = ?, default_double = ?, default_bool = ?, "
                "default_text = ?, default_blob = ? "
                "WHERE table_id = ? AND column_name = ?;");
            stmt.BindText(1, def.displayName);
            stmt.BindInt(2, ToSqliteValueKind(def.valueKind));
            stmt.BindInt(3, ToSqliteColumnKind(def.columnKind));
            stmt.BindInt(4, def.nullable ? 1 : 0);
            stmt.BindInt(5, def.editable ? 1 : 0);
            stmt.BindInt(6, def.userDefined ? 1 : 0);
            stmt.BindInt(7, def.indexed ? 1 : 0);
            stmt.BindInt(8, def.participatesInCalc ? 1 : 0);
            stmt.BindText(9, def.unit);
            stmt.BindText(10, def.referenceTable);
            stmt.BindText(11, def.referenceStorageColumn);
            stmt.BindText(12, def.referenceDisplayColumn);
            BindValueForStorage(stmt, 13, 14, 15, 16, 17, 18, def.defaultValue);
            stmt.BindInt64(19, schema->TableRowId());
            stmt.BindText(20, def.name);
            const ErrorCode rc = stmt.Step();
            if (Failed(rc))
            {
                return rc;
            }

            const std::wstring indexName =
                L"idx_fv_" + std::to_wstring(schema->TableRowId()) + L"_" + SanitizeIdentifier(def.name);
            if (previousDef->indexed != def.indexed)
            {
                if (def.indexed)
                {
                    SqliteStmt createIndexStmt =
                        db_.Prepare((std::string("CREATE INDEX IF NOT EXISTS ") + ToUtf8(indexName) +
                                     " ON field_values(table_id, column_name, "
                                     "int64_value, double_value, bool_value, text_value);")
                                        .c_str());
                    const ErrorCode createIndexRc = createIndexStmt.Step();
                    if (Failed(createIndexRc))
                    {
                        return createIndexRc;
                    }
                } else
                {
                    SqliteStmt dropIndexStmt =
                        db_.Prepare((std::string("DROP INDEX IF EXISTS ") + ToUtf8(indexName) + ";").c_str());
                    const ErrorCode dropIndexRc = dropIndexStmt.Step();
                    if (Failed(dropIndexRc))
                    {
                        return dropIndexRc;
                    }
                }
            }

            const ErrorCode metadataRc = SyncLegacyIndexMetadata(schema, def.name, def.indexed);
            if (Failed(metadataRc))
            {
                return metadataRc;
            }
            schema->ReplaceColumn(def);
            MarkReferenceIndexDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistSchemaRemoveColumn(SqliteSchema* schema, const wchar_t* columnName)
        {
            if (schema == nullptr || columnName == nullptr)
            {
                return SC_E_POINTER;
            }

            const SCColumnDef* previousDef = schema->FindColumnDef(columnName);
            if (previousDef == nullptr)
            {
                return SC_E_COLUMN_NOT_FOUND;
            }

            auto tableIt = std::find_if(
                tables_.begin(),
                tables_.end(),
                [tableRowId = schema->TableRowId()](const std::pair<const std::wstring, SCTablePtr>& entry) {
                    const auto* table = static_cast<SqliteTable*>(entry.second.Get());
                    return table != nullptr && table->TableRowId() == tableRowId;
                });
            if (tableIt == tables_.end())
            {
                return SC_E_FAIL;
            }
            auto* sqliteTable = static_cast<SqliteTable*>(tableIt->second.Get());

            const std::wstring indexName =
                L"idx_fv_" + std::to_wstring(schema->TableRowId()) + L"_" + SanitizeIdentifier(columnName);
            const std::string dropIndexSql = "DROP INDEX IF EXISTS " + ToUtf8(indexName) + ";";
            SqliteStmt dropIndexStmt = db_.Prepare(dropIndexSql.c_str());
            const ErrorCode dropIndexRc = dropIndexStmt.Step();
            if (Failed(dropIndexRc))
            {
                return dropIndexRc;
            }

            SqliteStmt deleteValuesStmt = db_.Prepare(
                "DELETE FROM field_values WHERE table_id = ? AND "
                "column_name = ?;");
            deleteValuesStmt.BindInt64(1, schema->TableRowId());
            deleteValuesStmt.BindText(2, columnName);
            const ErrorCode deleteValuesRc = deleteValuesStmt.Step();
            if (Failed(deleteValuesRc))
            {
                return deleteValuesRc;
            }

            SqliteStmt stmt = db_.Prepare(
                "DELETE FROM schema_columns WHERE table_id = ? AND "
                "column_name = ?;");
            stmt.BindInt64(1, schema->TableRowId());
            stmt.BindText(2, columnName);
            const ErrorCode rc = stmt.Step();
            if (Failed(rc))
            {
                return rc;
            }

            const ErrorCode metadataRc = SyncLegacyIndexMetadata(schema, columnName, false);
            if (Failed(metadataRc))
            {
                return metadataRc;
            }
            const ErrorCode pkMetadataRc = RemoveLegacyPrimaryKeyMetadata(schema, columnName);
            if (Failed(pkMetadataRc))
            {
                return pkMetadataRc;
            }

            for (const auto& [recordId, data] : sqliteTable->Records())
            {
                (void)recordId;
                if (data != nullptr)
                {
                    data->values.erase(columnName);
                }
            }

            schema->UnloadColumn(columnName);
            MarkReferenceIndexDirty();
            MarkForeignKeyReferenceCacheDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistSchemaAddConstraint(SqliteSchema* schema,
                                                             const SCConstraintDef& def,
                                                             std::int64_t rowId)
        {
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            SqliteStmt stmt = db_.Prepare(
                "INSERT INTO schema_constraints("
                " constraint_id, table_id, kind, name, source_kind, "
                "referenced_table, on_delete_action, on_update_action, "
                "check_expression)"
                " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);");
            if (rowId > 0)
            {
                stmt.BindInt64(1, rowId);
            } else
            {
                stmt.BindNull(1);
            }
            stmt.BindInt64(2, schema->TableRowId());
            stmt.BindInt(3, ToSqliteConstraintKind(def.kind));
            stmt.BindText(4, def.name);
            stmt.BindInt(5, ToSqliteSchemaSourceKind(def.sourceKind));
            stmt.BindText(6, def.referencedTable);
            stmt.BindInt(7, ToSqliteForeignKeyAction(def.onDelete));
            stmt.BindInt(8, ToSqliteForeignKeyAction(def.onUpdate));
            stmt.BindText(9, def.checkExpression);
            const ErrorCode rc = stmt.Step();
            if (Failed(rc))
            {
                return rc;
            }

            const std::int64_t constraintId = rowId > 0 ? rowId : db_.LastInsertRowId();
            for (std::size_t index = 0; index < def.columns.size(); ++index)
            {
                SqliteStmt columnStmt = db_.Prepare(
                    "INSERT INTO schema_constraint_columns("
                    " constraint_id, column_ordinal, column_name, "
                    "referenced_column_name) VALUES(?, ?, ?, ?);");
                columnStmt.BindInt64(1, constraintId);
                columnStmt.BindInt64(2, static_cast<std::int64_t>(index));
                columnStmt.BindText(3, def.columns[index]);
                columnStmt.BindText(
                    4, index < def.referencedColumns.size() ? def.referencedColumns[index] : std::wstring{});
                const ErrorCode columnRc = columnStmt.Step();
                if (Failed(columnRc))
                {
                    return columnRc;
                }
            }

            schema->LoadConstraint(def, constraintId);
            MarkForeignKeyReferenceCacheDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistSchemaRemoveConstraint(SqliteSchema* schema, const wchar_t* name)
        {
            if (schema == nullptr || name == nullptr)
            {
                return SC_E_POINTER;
            }

            if (schema->FindConstraintDef(name) == nullptr)
            {
                return SC_E_CONSTRAINT_NOT_FOUND;
            }

            {
                SqliteStmt deleteColumnsStmt = db_.Prepare(
                    "DELETE FROM schema_constraint_columns WHERE constraint_id "
                    "IN (SELECT constraint_id FROM schema_constraints WHERE "
                    "table_id = ? AND name = ?);");
                deleteColumnsStmt.BindInt64(1, schema->TableRowId());
                deleteColumnsStmt.BindText(2, name);
                const ErrorCode rc = deleteColumnsStmt.Step();
                if (Failed(rc))
                {
                    return rc;
                }
            }

            SqliteStmt deleteConstraintStmt =
                db_.Prepare("DELETE FROM schema_constraints WHERE table_id = ? AND name = ?;");
            deleteConstraintStmt.BindInt64(1, schema->TableRowId());
            deleteConstraintStmt.BindText(2, name);
            const ErrorCode deleteRc = deleteConstraintStmt.Step();
            if (Failed(deleteRc))
            {
                return deleteRc;
            }

            schema->UnloadConstraint(name);
            MarkForeignKeyReferenceCacheDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistSchemaAddIndex(SqliteSchema* schema, const SCIndexDef& def, std::int64_t rowId)
        {
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            SqliteStmt stmt = db_.Prepare(
                "INSERT INTO schema_indexes(index_id, table_id, name, "
                "source_kind) VALUES(?, ?, ?, ?);");
            if (rowId > 0)
            {
                stmt.BindInt64(1, rowId);
            } else
            {
                stmt.BindNull(1);
            }
            stmt.BindInt64(2, schema->TableRowId());
            stmt.BindText(3, def.name);
            stmt.BindInt(4, ToSqliteSchemaSourceKind(def.sourceKind));
            const ErrorCode rc = stmt.Step();
            if (Failed(rc))
            {
                return rc;
            }

            const std::int64_t indexId = rowId > 0 ? rowId : db_.LastInsertRowId();
            const ErrorCode queryIndexDefRc = PersistQueryIndexDefinition(schema, def, indexId, false);
            if (Failed(queryIndexDefRc))
            {
                return queryIndexDefRc;
            }

            for (std::size_t columnIndex = 0; columnIndex < def.columns.size(); ++columnIndex)
            {
                SqliteStmt columnStmt = db_.Prepare(
                    "INSERT INTO schema_index_columns(index_id, "
                    "column_ordinal, column_name, descending_flag) "
                    "VALUES(?, ?, ?, ?);");
                columnStmt.BindInt64(1, indexId);
                columnStmt.BindInt64(2, static_cast<std::int64_t>(columnIndex));
                columnStmt.BindText(3, def.columns[columnIndex].columnName);
                columnStmt.BindInt(4, def.columns[columnIndex].descending ? 1 : 0);
                const ErrorCode columnRc = columnStmt.Step();
                if (Failed(columnRc))
                {
                    return columnRc;
                }
            }

            const bool compositeIndex = IsCompositeIndexExplicit(def);
            const auto tableIt = std::find_if(
                tables_.begin(),
                tables_.end(),
                [tableRowId = schema->TableRowId()](const std::pair<const std::wstring, SCTablePtr>& entry) {
                    const auto* table = static_cast<const SqliteTable*>(entry.second.Get());
                    return table != nullptr && table->TableRowId() == tableRowId;
                });
            if (tableIt != tables_.end())
            {
                auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
                const ErrorCode rebuildRc = RebuildCompositeIndexEntriesForTable(table, def, indexId);
                if (Failed(rebuildRc))
                {
                    return rebuildRc;
                }
            }

            schema->LoadIndex(def, indexId);
            if (compositeIndex)
            {
                queryIndexRowIdsByTableAndName_[BuildQueryIndexStorageKey(schema->TableRowId(), def.name)] = indexId;
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistSchemaRemoveIndex(SqliteSchema* schema, const wchar_t* name)
        {
            if (schema == nullptr || name == nullptr)
            {
                return SC_E_POINTER;
            }

            if (schema->FindIndexDef(name) == nullptr)
            {
                return SC_E_INDEX_NOT_FOUND;
            }

            const SCIndexDef* previousDef = schema->FindIndexDef(name);
            const bool compositeIndex = previousDef != nullptr && IsCompositeIndexExplicit(*previousDef);
            if (compositeIndex)
            {
                const ErrorCode removeDefRc = RemoveQueryIndexDefinition(schema, name, false);
                if (Failed(removeDefRc))
                {
                    return removeDefRc;
                }
            }

            {
                SqliteStmt deleteColumnsStmt = db_.Prepare(
                    "DELETE FROM schema_index_columns WHERE index_id IN "
                    "(SELECT index_id FROM schema_indexes WHERE table_id = ? "
                    "AND name = ?);");
                deleteColumnsStmt.BindInt64(1, schema->TableRowId());
                deleteColumnsStmt.BindText(2, name);
                const ErrorCode rc = deleteColumnsStmt.Step();
                if (Failed(rc))
                {
                    return rc;
                }
            }

            SqliteStmt deleteIndexStmt = db_.Prepare("DELETE FROM schema_indexes WHERE table_id = ? AND name = ?;");
            deleteIndexStmt.BindInt64(1, schema->TableRowId());
            deleteIndexStmt.BindText(2, name);
            const ErrorCode deleteRc = deleteIndexStmt.Step();
            if (Failed(deleteRc))
            {
                return deleteRc;
            }

            schema->UnloadIndex(name);
            if (compositeIndex)
            {
                queryIndexRowIdsByTableAndName_.erase(BuildQueryIndexStorageKey(schema->TableRowId(), name));
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistAddedColumn(SqliteSchema* schema, const SCColumnDef& def)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            auto tableIt = std::find_if(
                tables_.begin(),
                tables_.end(),
                [tableRowId = schema->TableRowId()](const std::pair<const std::wstring, SCTablePtr>& entry) {
                    const auto* table = static_cast<SqliteTable*>(entry.second.Get());
                    return table != nullptr && table->TableRowId() == tableRowId;
                });
            if (tableIt == tables_.end())
            {
                return SC_E_FAIL;
            }
            const std::wstring tableName = tableIt->first;

            try
            {
                SqliteTxn txn(db_);
                const ErrorCode rc = PersistSchemaAddColumn(schema, def);
                if (Failed(rc))
                {
                    return rc;
                }
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    schema->UnloadColumn(def.name.c_str());
                    return commitRc;
                }
            } catch (...)
            {
                schema->UnloadColumn(def.name.c_str());
                return SC_E_FAIL;
            }

            const std::int64_t rowId = schema->FindColumnRowId(def.name.c_str());
            RecordSchemaJournal(tableName, SCColumnDef{}, def, JournalOp::AddColumn, rowId);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistUpdatedColumn(SqliteSchema* schema, const SCColumnDef& def)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            auto tableIt = std::find_if(
                tables_.begin(),
                tables_.end(),
                [tableRowId = schema->TableRowId()](const std::pair<const std::wstring, SCTablePtr>& entry) {
                    const auto* table = static_cast<SqliteTable*>(entry.second.Get());
                    return table != nullptr && table->TableRowId() == tableRowId;
                });
            if (tableIt == tables_.end())
            {
                return SC_E_FAIL;
            }
            auto* sqliteTable = static_cast<SqliteTable*>(tableIt->second.Get());

            struct ColumnValueUpdate
            {
                std::shared_ptr<SqliteRecordData> data;
                SCValue newValue;
                SCValue oldValue;
            };

            std::vector<ColumnValueUpdate> updates;
            const SCColumnDef* previousDef = schema->FindColumnDef(def.name);
            if (previousDef == nullptr)
            {
                return SC_E_COLUMN_NOT_FOUND;
            }
            const SCColumnDef previousColumn = *previousDef;
            const std::size_t journalCheckpoint = activeJournal_.entries.size();
            const std::int64_t rowId = schema->FindColumnRowId(def.name.c_str());

            const auto restoreUpdatedValues = [&updates, &def]() {
                for (const auto& update : updates)
                {
                    if (update.oldValue.IsNull())
                    {
                        update.data->values.erase(def.name);
                    } else
                    {
                        update.data->values[def.name] = update.oldValue;
                    }
                }
            };
            const auto cleanupFailedUpdate = [&]() {
                restoreUpdatedValues();
                activeJournal_.entries.resize(journalCheckpoint);
                schema->ReplaceColumn(previousColumn);
            };

            if (previousDef->valueKind != def.valueKind)
            {
                updates.reserve(sqliteTable->Records().size());
                for (const auto& [recordId, data] : sqliteTable->Records())
                {
                    if (data == nullptr)
                    {
                        continue;
                    }

                    const auto valueIt = data->values.find(def.name);
                    if (valueIt == data->values.end())
                    {
                        continue;
                    }

                    SCValue converted;
                    const ErrorCode convertRc = ConvertColumnValue(valueIt->second, def.valueKind, &converted);
                    if (Failed(convertRc))
                    {
                        return convertRc;
                    }

                    updates.push_back(ColumnValueUpdate{data, std::move(converted), valueIt->second});
                }
            }

            try
            {
                SqliteTxn txn(db_);
                const ErrorCode schemaRc = PersistSchemaUpdateColumn(schema, def);
                if (Failed(schemaRc))
                {
                    cleanupFailedUpdate();
                    return schemaRc;
                }

                if (!updates.empty())
                {
                    SqliteStmt deleteValueStmt = db_.Prepare(
                        "DELETE FROM field_values WHERE table_id = ? AND "
                        "record_id = ? AND column_name = ?;");
                    SqliteStmt insertValueStmt = db_.Prepare(
                        "INSERT INTO field_values(table_id, record_id, "
                        "column_name, value_kind, int64_value, double_value, "
                        "bool_value, text_value, blob_value) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);");
                    for (auto& update : updates)
                    {
                        deleteValueStmt.BindInt64(1, schema->TableRowId());
                        deleteValueStmt.BindInt64(2, update.data->id);
                        deleteValueStmt.BindText(3, def.name);
                        const ErrorCode deleteRc = deleteValueStmt.Step();
                        deleteValueStmt.Reset();
                        if (Failed(deleteRc))
                        {
                            cleanupFailedUpdate();
                            return deleteRc;
                        }

                        if (!update.newValue.IsNull())
                        {
                            update.data->values[def.name] = update.newValue;
                            insertValueStmt.BindInt64(1, schema->TableRowId());
                            insertValueStmt.BindInt64(2, update.data->id);
                            insertValueStmt.BindText(3, def.name);
                            BindValueForStorage(insertValueStmt, 4, 5, 6, 7, 8, 9, update.newValue);
                            const ErrorCode insertRc = insertValueStmt.Step();
                            insertValueStmt.Reset();
                            if (Failed(insertRc))
                            {
                                cleanupFailedUpdate();
                                return insertRc;
                            }
                        } else
                        {
                            update.data->values.erase(def.name);
                        }

                        if (HasActiveEdit())
                        {
                            RecordJournal(
                                sqliteTable->Name(),
                                update.data->id,
                                def.name,
                                update.oldValue,
                                update.newValue,
                                false,
                                false,
                                def.columnKind == ColumnKind::Relation ? JournalOp::SetRelation : JournalOp::SetValue);
                        }
                    }
                }

                const ErrorCode finalCommitRc = txn.Commit();
                if (Failed(finalCommitRc))
                {
                    cleanupFailedUpdate();
                    return finalCommitRc;
                }
            } catch (...)
            {
                cleanupFailedUpdate();
                return SC_E_FAIL;
            }

            RecordSchemaJournal(tableIt->first, previousColumn, def, JournalOp::UpdateColumn, rowId);

            MarkReferenceIndexDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::ClearColumnValues(ISCTable* table, const wchar_t* name)
        {
            if (name == nullptr)
            {
                return SC_E_INVALIDARG;
            }
            if (!activeEdit_)
            {
                return SC_E_NO_ACTIVE_EDIT;
            }
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }

            auto* sqliteTable = static_cast<SqliteTable*>(table);
            SCSchemaPtr schema;
            const ErrorCode schemaRc = sqliteTable->GetSchema(schema);
            if (Failed(schemaRc) || !schema)
            {
                return schemaRc;
            }

            SCColumnDef column;
            const ErrorCode columnRc = schema->FindColumn(name, &column);
            if (Failed(columnRc))
            {
                return columnRc;
            }

            const bool relationColumn = column.columnKind == ColumnKind::Relation;
            for (const auto& [recordId, data] : sqliteTable->Records())
            {
                if (data == nullptr)
                {
                    continue;
                }

                const auto valueIt = data->values.find(name);
                if (valueIt == data->values.end())
                {
                    continue;
                }

                const SCValue oldValue = valueIt->second;
                data->values.erase(valueIt);
                RecordJournal(sqliteTable->Name(),
                              recordId,
                              name,
                              oldValue,
                              SCValue::Null(),
                              false,
                              false,
                              relationColumn ? JournalOp::SetRelation : JournalOp::SetValue);
            }

            MarkReferenceIndexDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistRemovedColumn(SqliteSchema* schema, const wchar_t* columnName)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (schema == nullptr || columnName == nullptr)
            {
                return SC_E_POINTER;
            }

            const SCColumnDef* previousDef = schema->FindColumnDef(columnName);
            if (previousDef == nullptr)
            {
                return SC_E_COLUMN_NOT_FOUND;
            }
            const SCColumnDef previousColumn = *previousDef;
            const std::int64_t rowId = schema->FindColumnRowId(columnName);
            auto tableIt = std::find_if(
                tables_.begin(),
                tables_.end(),
                [tableRowId = schema->TableRowId()](const std::pair<const std::wstring, SCTablePtr>& entry) {
                    const auto* table = static_cast<SqliteTable*>(entry.second.Get());
                    return table != nullptr && table->TableRowId() == tableRowId;
                });
            if (tableIt == tables_.end())
            {
                return SC_E_FAIL;
            }
            const std::wstring tableName = tableIt->first;
            auto* sqliteTable = static_cast<SqliteTable*>(tableIt->second.Get());
            if (sqliteTable == nullptr)
            {
                return SC_E_FAIL;
            }
            const std::wstring columnKey(columnName);
            struct ColumnRemovalValueSnapshot
            {
                RecordId recordId{0};
                bool hadValue{false};
                SCValue value;
            };
            std::vector<ColumnRemovalValueSnapshot> valueSnapshots;
            valueSnapshots.reserve(sqliteTable->Records().size());
            for (const auto& [recordId, data] : sqliteTable->Records())
            {
                ColumnRemovalValueSnapshot snapshot;
                snapshot.recordId = recordId;
                if (data != nullptr)
                {
                    const auto valueIt = data->values.find(columnKey);
                    if (valueIt != data->values.end())
                    {
                        snapshot.hadValue = true;
                        snapshot.value = valueIt->second;
                    }
                }
                valueSnapshots.push_back(std::move(snapshot));
            }

            const auto restorePreImage = [&]() {
                try
                {
                    const auto schemaColumn = schema->FindColumnDef(columnName);
                    if (schemaColumn != nullptr)
                    {
                        schema->ReplaceColumn(previousColumn);
                    } else
                    {
                        schema->LoadColumn(previousColumn);
                    }

                    for (const auto& snapshot : valueSnapshots)
                    {
                        const auto recordIt = sqliteTable->Records().find(snapshot.recordId);
                        if (recordIt == sqliteTable->Records().end() || recordIt->second == nullptr)
                        {
                            continue;
                        }

                        if (snapshot.hadValue)
                        {
                            recordIt->second->values[columnKey] = snapshot.value;
                        } else
                        {
                            recordIt->second->values.erase(columnKey);
                        }
                    }
                } catch (...)
                {
                }
            };

            try
            {
                SqliteTxn txn(db_);
                const ErrorCode rc = PersistSchemaRemoveColumn(schema, columnName);
                if (Failed(rc))
                {
                    restorePreImage();
                    return rc;
                }
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    restorePreImage();
                    return commitRc;
                }
            } catch (...)
            {
                restorePreImage();
                return SC_E_FAIL;
            }

            RecordSchemaJournal(tableName, previousColumn, SCColumnDef{}, JournalOp::RemoveColumn, rowId);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistAddedConstraint(SqliteSchema* schema, const SCConstraintDef& def)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            ClearConstraintViolation();
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            auto* table = FindTableByRowId(schema->TableRowId());
            if (table == nullptr)
            {
                return SC_E_FAIL;
            }

            if (def.kind == SCConstraintKind::PrimaryKey || def.kind == SCConstraintKind::Unique ||
                def.kind == SCConstraintKind::ForeignKey || def.kind == SCConstraintKind::Check)
            {
                ErrorCode constraintValidationRc = SC_OK;
                switch (def.kind)
                {
                    case SCConstraintKind::PrimaryKey:
                    case SCConstraintKind::Unique:
                        constraintValidationRc =
                            ValidateConstraintUniqueness(table, def, std::shared_ptr<SqliteRecordData>{});
                        break;
                    case SCConstraintKind::Check:
                        constraintValidationRc = ValidateCheckConstraint(table, def, std::shared_ptr<SqliteRecordData>{});
                        break;
                    case SCConstraintKind::ForeignKey:
                        constraintValidationRc =
                            ValidateForeignKeyConstraint(table, def, std::shared_ptr<SqliteRecordData>{});
                        break;
                    default:
                        break;
                }
                if (Failed(constraintValidationRc))
                {
                    return constraintValidationRc;
                }
            }

            try
            {
                SqliteTxn txn(db_);
                const ErrorCode rc = PersistSchemaAddConstraint(schema, def);
                if (Failed(rc))
                {
                    return rc;
                }
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    schema->UnloadConstraint(def.name.c_str());
                    return commitRc;
                }
            } catch (...)
            {
                schema->UnloadConstraint(def.name.c_str());
                return SC_E_FAIL;
            }

            const std::int64_t rowId = schema->FindConstraintRowId(def.name.c_str());
            RecordConstraintJournal(table->Name(), SCConstraintDef{}, def, JournalOp::AddConstraint, rowId);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistRemovedConstraint(SqliteSchema* schema, const wchar_t* name)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (schema == nullptr || name == nullptr)
            {
                return SC_E_POINTER;
            }

            const SCConstraintDef* previousDef = schema->FindConstraintDef(name);
            if (previousDef == nullptr)
            {
                return SC_E_CONSTRAINT_NOT_FOUND;
            }
            const SCConstraintDef previousConstraint = *previousDef;
            const std::int64_t rowId = schema->FindConstraintRowId(name);

            try
            {
                SqliteTxn txn(db_);
                const ErrorCode rc = PersistSchemaRemoveConstraint(schema, name);
                if (Failed(rc))
                {
                    if (schema->FindConstraintDef(name) == nullptr)
                    {
                        schema->LoadConstraint(previousConstraint, rowId);
                    }
                    return rc;
                }
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    if (schema->FindConstraintDef(name) == nullptr)
                    {
                        schema->LoadConstraint(previousConstraint, rowId);
                    }
                    return commitRc;
                }
            } catch (...)
            {
                if (schema->FindConstraintDef(name) == nullptr)
                {
                    schema->LoadConstraint(previousConstraint, rowId);
                }
                return SC_E_FAIL;
            }

            auto tableIt = std::find_if(
                tables_.begin(),
                tables_.end(),
                [tableRowId = schema->TableRowId()](const std::pair<const std::wstring, SCTablePtr>& entry) {
                    const auto* table = static_cast<SqliteTable*>(entry.second.Get());
                    return table != nullptr && table->TableRowId() == tableRowId;
                });
            if (tableIt == tables_.end())
            {
                return SC_E_FAIL;
            }

            RecordConstraintJournal(
                tableIt->first, previousConstraint, SCConstraintDef{}, JournalOp::RemoveConstraint, rowId);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistAddedIndex(SqliteSchema* schema, const SCIndexDef& def)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            auto tableIt = std::find_if(
                tables_.begin(),
                tables_.end(),
                [tableRowId = schema->TableRowId()](const std::pair<const std::wstring, SCTablePtr>& entry) {
                    const auto* table = static_cast<SqliteTable*>(entry.second.Get());
                    return table != nullptr && table->TableRowId() == tableRowId;
                });
            if (tableIt == tables_.end())
            {
                return SC_E_FAIL;
            }

            try
            {
                SqliteTxn txn(db_);
                const ErrorCode rc = PersistSchemaAddIndex(schema, def);
                if (Failed(rc))
                {
                    return rc;
                }
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    schema->UnloadIndex(def.name.c_str());
                    return commitRc;
                }
            } catch (...)
            {
                schema->UnloadIndex(def.name.c_str());
                return SC_E_FAIL;
            }

            const std::int64_t rowId = schema->FindIndexRowId(def.name.c_str());
            RecordIndexJournal(tableIt->first, SCIndexDef{}, def, JournalOp::AddIndex, rowId);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistRemovedIndex(SqliteSchema* schema, const wchar_t* name)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (schema == nullptr || name == nullptr)
            {
                return SC_E_POINTER;
            }

            const SCIndexDef* previousDef = schema->FindIndexDef(name);
            if (previousDef == nullptr)
            {
                return SC_E_INDEX_NOT_FOUND;
            }
            const SCIndexDef previousIndex = *previousDef;
            const std::int64_t rowId = schema->FindIndexRowId(name);

            try
            {
                SqliteTxn txn(db_);
                const ErrorCode rc = PersistSchemaRemoveIndex(schema, name);
                if (Failed(rc))
                {
                    if (schema->FindIndexDef(name) == nullptr)
                    {
                        schema->LoadIndex(previousIndex, rowId);
                    }
                    return rc;
                }
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    if (schema->FindIndexDef(name) == nullptr)
                    {
                        schema->LoadIndex(previousIndex, rowId);
                    }
                    return commitRc;
                }
            } catch (...)
            {
                if (schema->FindIndexDef(name) == nullptr)
                {
                    schema->LoadIndex(previousIndex, rowId);
                }
                return SC_E_FAIL;
            }

            auto tableIt = std::find_if(
                tables_.begin(),
                tables_.end(),
                [tableRowId = schema->TableRowId()](const std::pair<const std::wstring, SCTablePtr>& entry) {
                    const auto* table = static_cast<SqliteTable*>(entry.second.Get());
                    return table != nullptr && table->TableRowId() == tableRowId;
                });
            if (tableIt == tables_.end())
            {
                return SC_E_FAIL;
            }

            RecordIndexJournal(tableIt->first, previousIndex, SCIndexDef{}, JournalOp::RemoveIndex, rowId);
            return SC_OK;
        }

        void SqliteDatabase::ReloadColumnValuesFromStorage(SqliteTable* table, const wchar_t* columnName)
        {
            if (table == nullptr || columnName == nullptr)
            {
                return;
            }

            try
            {
                const bool supportsBinaryStorage = schemaVersion_ >= 4;
                SqliteStmt valuesStmt =
                    db_.Prepare(supportsBinaryStorage ? "SELECT record_id, value_kind, int64_value, "
                                                        "double_value, bool_value, text_value, blob_value "
                                                        "FROM field_values WHERE table_id = ? AND "
                                                        "column_name = ?;"
                                                      : "SELECT record_id, value_kind, int64_value, "
                                                        "double_value, bool_value, text_value FROM "
                                                        "field_values WHERE table_id = ? AND column_name = "
                                                        "?;");
                valuesStmt.BindInt64(1, table->TableRowId());
                valuesStmt.BindText(2, columnName);
                bool hasValue = false;
                while (valuesStmt.Step(&hasValue) == SC_OK && hasValue)
                {
                    const RecordId recordId = valuesStmt.ColumnInt64(0);
                    const auto recordIt = table->Records().find(recordId);
                    if (recordIt == table->Records().end() || recordIt->second == nullptr)
                    {
                        continue;
                    }

                    recordIt->second->values[std::wstring(columnName)] =
                        supportsBinaryStorage ? ReadValueFromStorage(valuesStmt, 1, 2, 3, 4, 5, 6)
                                              : ReadValueFromStorage(valuesStmt, 1, 2, 3, 4, 5, 5);
                }
            } catch (...)
            {
            }
        }

        void SqliteDatabase::RecordJournal(const std::wstring& tableName,
                                           RecordId recordId,
                                           const std::wstring& fieldName,
                                           const SCValue& oldValue,
                                           const SCValue& newValue,
                                           bool oldDeleted,
                                           bool newDeleted,
                                           JournalOp op)
        {
            for (auto& entry : activeJournal_.entries)
            {
                if (entry.op == op &&
                    JournalEntryMatchesCurrentTableName(entry, tableName) &&
                    entry.recordId == recordId &&
                    entry.fieldName == fieldName)
                {
                    entry.newValue = newValue;
                    entry.newDeleted = newDeleted;
                    return;
                }
            }

            activeJournal_.entries.push_back(JournalEntry{
                op,
                tableName,
                recordId,
                fieldName,
                oldValue,
                newValue,
                oldDeleted,
                newDeleted,
            });
        }

        void SqliteDatabase::RecordTableRenameJournal(const std::wstring& originalName,
                                                      const std::wstring& newName)
        {
            if (!HasActiveEdit())
            {
                return;
            }

            JournalEntry entry;
            entry.op = JournalOp::RenameTable;
            entry.tableName = originalName;
            entry.oldValue = SCValue::FromString(originalName);
            entry.newValue = SCValue::FromString(newName);
            activeJournal_.entries.push_back(std::move(entry));
        }

        ErrorCode SqliteDatabase::PersistTableRename(const std::wstring& originalName,
                                                     const std::wstring& newName,
                                                     bool recordJournal,
                                                     bool manageTransaction)
        {
            if (EqualsIgnoreCase(originalName, newName))
            {
                return SC_E_INVALIDARG;
            }

            auto tableIt = tables_.find(originalName);
            if (tableIt == tables_.end())
            {
                for (auto it = tables_.begin(); it != tables_.end(); ++it)
                {
                    if (EqualsIgnoreCase(it->first, originalName))
                    {
                        tableIt = it;
                        break;
                    }
                }
            }
            if (tableIt == tables_.end())
            {
                return SC_E_TABLE_NOT_FOUND;
            }

            for (const auto& [existingName, _] : tables_)
            {
                if (EqualsIgnoreCase(existingName, newName))
                {
                    return SC_E_SCHEMA_VIOLATION;
                }
            }

            auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
            if (table == nullptr)
            {
                return SC_E_FAIL;
            }

            struct ColumnSnapshot
            {
                SqliteSchema* schema{nullptr};
                SCColumnDef column;
            };

            struct ConstraintSnapshot
            {
                SqliteSchema* schema{nullptr};
                SCConstraintDef constraint;
            };

            std::vector<ColumnSnapshot> originalColumns;
            std::vector<ConstraintSnapshot> originalConstraints;
            const auto restoreSchemaState = [&]() {
                for (auto it = originalColumns.rbegin(); it != originalColumns.rend();
                     ++it)
                {
                    if (it->schema != nullptr)
                    {
                        it->schema->ReplaceColumn(it->column);
                    }
                }
                for (auto it = originalConstraints.rbegin();
                     it != originalConstraints.rend(); ++it)
                {
                    if (it->schema != nullptr)
                    {
                        it->schema->ReplaceConstraint(it->constraint);
                    }
                }
            };

            std::optional<SqliteTxn> txn;
            std::optional<SqliteSavepoint> savepoint;
            try
            {
                if (manageTransaction)
                {
                    txn.emplace(db_);
                }
                else
                {
                    savepoint.emplace(db_, "rename_table");
                }

                SqliteStmt updateTableStmt =
                    db_.Prepare("UPDATE tables SET name = ? WHERE table_id = ?;");
                updateTableStmt.BindText(1, newName);
                updateTableStmt.BindInt64(2, table->TableRowId());
                const ErrorCode updateRc = updateTableStmt.Step();
                if (Failed(updateRc))
                {
                    return updateRc;
                }

                for (const auto& [otherName, otherTableRef] : tables_)
                {
                    (void)otherName;
                    auto* otherTable = static_cast<SqliteTable*>(otherTableRef.Get());
                    if (otherTable == nullptr)
                    {
                        continue;
                    }

                    SCSchemaPtr otherSchema;
                    const ErrorCode schemaRc = otherTable->GetSchema(otherSchema);
                    if (Failed(schemaRc) || !otherSchema)
                    {
                        restoreSchemaState();
                        return schemaRc;
                    }

                    auto* sqliteOtherSchema =
                        static_cast<SqliteSchema*>(otherSchema.Get());
                    if (sqliteOtherSchema == nullptr)
                    {
                        restoreSchemaState();
                        return SC_E_FAIL;
                    }

                    std::int32_t columnCount = 0;
                    const ErrorCode columnCountRc =
                        otherSchema->GetColumnCount(&columnCount);
                    if (Failed(columnCountRc))
                    {
                        restoreSchemaState();
                        return columnCountRc;
                    }

                    for (std::int32_t columnIndex = 0; columnIndex < columnCount;
                         ++columnIndex)
                    {
                        SCColumnDef column;
                        const ErrorCode columnRc =
                            otherSchema->GetColumn(columnIndex, &column);
                        if (Failed(columnRc))
                        {
                            restoreSchemaState();
                            return columnRc;
                        }

                        if (column.columnKind != ColumnKind::Relation ||
                            !EqualsIgnoreCase(column.referenceTable, originalName))
                        {
                            continue;
                        }

                        originalColumns.push_back(ColumnSnapshot{sqliteOtherSchema, column});
                        column.referenceTable = newName;
                        const ErrorCode updateColumnRc =
                            PersistSchemaUpdateColumn(sqliteOtherSchema, column);
                        if (Failed(updateColumnRc))
                        {
                            restoreSchemaState();
                            return updateColumnRc;
                        }
                    }

                    std::int32_t constraintCount = 0;
                    const ErrorCode constraintCountRc =
                        otherSchema->GetConstraintCount(&constraintCount);
                    if (Failed(constraintCountRc))
                    {
                        restoreSchemaState();
                        return constraintCountRc;
                    }

                    for (std::int32_t constraintIndex = 0;
                         constraintIndex < constraintCount; ++constraintIndex)
                    {
                        SCConstraintDef constraint;
                        const ErrorCode constraintRc =
                            otherSchema->GetConstraint(constraintIndex, &constraint);
                        if (Failed(constraintRc))
                        {
                            restoreSchemaState();
                            return constraintRc;
                        }
                        if (constraint.kind != SCConstraintKind::ForeignKey ||
                            !EqualsIgnoreCase(constraint.referencedTable, originalName))
                        {
                            continue;
                        }

                        originalConstraints.push_back(
                            ConstraintSnapshot{sqliteOtherSchema, constraint});
                        constraint.referencedTable = newName;

                        SqliteStmt updateConstraintStmt = db_.Prepare(
                            "UPDATE schema_constraints SET referenced_table = ? "
                            "WHERE table_id = ? AND name = ?;");
                        updateConstraintStmt.BindText(1, constraint.referencedTable);
                        updateConstraintStmt.BindInt64(2, sqliteOtherSchema->TableRowId());
                        updateConstraintStmt.BindText(3, constraint.name);
                        const ErrorCode updateConstraintRc =
                            updateConstraintStmt.Step();
                        if (Failed(updateConstraintRc))
                        {
                            restoreSchemaState();
                            return updateConstraintRc;
                        }

                        sqliteOtherSchema->ReplaceConstraint(constraint);
                    }
                }

                SaveMetadata();
                if (txn.has_value())
                {
                    const ErrorCode commitRc = txn->Commit();
                    if (Failed(commitRc))
                    {
                        restoreSchemaState();
                        return commitRc;
                    }
                }
                if (savepoint.has_value())
                {
                    const ErrorCode commitRc = savepoint->Commit();
                    if (Failed(commitRc))
                    {
                        restoreSchemaState();
                        return commitRc;
                    }
                }
            } catch (...)
            {
                restoreSchemaState();
                return SC_E_FAIL;
            }

            const std::wstring canonicalOriginalName = tableIt->first;
            SCTablePtr tableRef = tableIt->second;
            tables_.erase(tableIt);
            tables_.emplace(newName, tableRef);
            table->SetName(newName);
            if (auto* schema = table->Schema())
            {
                schema->SetTableName(newName);
            }
            if (recordJournal)
            {
                RecordTableRenameJournal(canonicalOriginalName, newName);
            }
            MarkReferenceIndexDirty();
            MarkForeignKeyReferenceCacheDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistDeferredRenameToStorage(
            const DeferredRenameState& state)
        {
            auto* table = static_cast<SqliteTable*>(state.tableRef.Get());
            if (table == nullptr)
            {
                return SC_E_FAIL;
            }

            SqliteStmt updateTableStmt =
                db_.Prepare("UPDATE tables SET name = ? WHERE table_id = ?;");
            updateTableStmt.BindText(1, state.newName);
            updateTableStmt.BindInt64(2, table->TableRowId());
            const ErrorCode updateRc = updateTableStmt.Step();
            if (Failed(updateRc))
            {
                return updateRc;
            }

            for (const auto& snapshot : state.relationColumns)
            {
                if (snapshot.schema == nullptr)
                {
                    continue;
                }

                SqliteStmt updateColumnStmt = db_.Prepare(
                    "UPDATE schema_columns SET reference_table = ? "
                    "WHERE table_id = ? AND column_name = ?;");
                updateColumnStmt.BindText(1, state.newName);
                updateColumnStmt.BindInt64(2, snapshot.schema->TableRowId());
                updateColumnStmt.BindText(3, snapshot.column.name);
                const ErrorCode updateColumnRc = updateColumnStmt.Step();
                if (Failed(updateColumnRc))
                {
                    return updateColumnRc;
                }
            }

            for (const auto& snapshot : state.foreignKeyConstraints)
            {
                if (snapshot.schema == nullptr)
                {
                    continue;
                }

                SqliteStmt updateConstraintStmt = db_.Prepare(
                    "UPDATE schema_constraints SET referenced_table = ? "
                    "WHERE table_id = ? AND name = ?;");
                updateConstraintStmt.BindText(1, state.newName);
                updateConstraintStmt.BindInt64(2, snapshot.schema->TableRowId());
                updateConstraintStmt.BindText(3, snapshot.constraint.name);
                const ErrorCode updateConstraintRc =
                    updateConstraintStmt.Step();
                if (Failed(updateConstraintRc))
                {
                    return updateConstraintRc;
                }
            }

            MarkReferenceIndexDirty();
            MarkForeignKeyReferenceCacheDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistDeferredSchemaOps()
        {
            for (const DeferredSchemaOp& op : activeSchemaOps_)
            {
                switch (op.kind)
                {
                    case DeferredSchemaOp::Kind::RenameTable: {
                        const ErrorCode renameRc =
                            PersistDeferredRenameToStorage(op.rename);
                        if (Failed(renameRc))
                        {
                            return renameRc;
                        }
                        break;
                    }
                }
            }
            return SC_OK;
        }

        void SqliteDatabase::RecordSchemaJournal(const std::wstring& tableName,
                                                 const SCColumnDef& oldColumn,
                                                 const SCColumnDef& newColumn,
                                                 JournalOp op,
                                                 std::int64_t columnRowId)
        {
            if (!HasActiveEdit())
            {
                return;
            }

            const std::wstring& columnName = !newColumn.name.empty() ? newColumn.name : oldColumn.name;
            for (auto& entry : activeJournal_.entries)
            {
                if (entry.op == op &&
                    JournalEntryMatchesCurrentTableName(entry, tableName) &&
                    entry.fieldName == columnName)
                {
                    entry.oldColumn = oldColumn;
                    entry.newColumn = newColumn;
                    entry.columnRowId = columnRowId;
                    return;
                }
            }

            activeJournal_.entries.push_back(JournalEntry{
                op,
                tableName,
                0,
                columnName,
                SCValue::Null(),
                SCValue::Null(),
                false,
                false,
                oldColumn,
                newColumn,
                columnRowId,
            });
        }

        void SqliteDatabase::RecordConstraintJournal(const std::wstring& tableName,
                                                     const SCConstraintDef& oldConstraint,
                                                     const SCConstraintDef& newConstraint,
                                                     JournalOp op,
                                                     std::int64_t constraintRowId)
        {
            if (!HasActiveEdit())
            {
                return;
            }

            const std::wstring& constraintName = !newConstraint.name.empty() ? newConstraint.name : oldConstraint.name;
            for (auto& entry : activeJournal_.entries)
            {
                if (entry.op == op &&
                    JournalEntryMatchesCurrentTableName(entry, tableName) &&
                    entry.fieldName == constraintName)
                {
                    entry.oldConstraint = oldConstraint;
                    entry.newConstraint = newConstraint;
                    entry.constraintRowId = constraintRowId;
                    return;
                }
            }

            JournalEntry entry;
            entry.op = op;
            entry.tableName = tableName;
            entry.fieldName = constraintName;
            entry.oldConstraint = oldConstraint;
            entry.newConstraint = newConstraint;
            entry.constraintRowId = constraintRowId;
            activeJournal_.entries.push_back(std::move(entry));
        }

        void SqliteDatabase::RecordIndexJournal(const std::wstring& tableName,
                                                const SCIndexDef& oldIndex,
                                                const SCIndexDef& newIndex,
                                                JournalOp op,
                                                std::int64_t indexRowId)
        {
            if (!HasActiveEdit())
            {
                return;
            }

            const std::wstring& indexName = !newIndex.name.empty() ? newIndex.name : oldIndex.name;
            for (auto& entry : activeJournal_.entries)
            {
                if (entry.op == op &&
                    JournalEntryMatchesCurrentTableName(entry, tableName) &&
                    entry.fieldName == indexName)
                {
                    entry.oldIndex = oldIndex;
                    entry.newIndex = newIndex;
                    entry.indexRowId = indexRowId;
                    return;
                }
            }

            JournalEntry entry;
            entry.op = op;
            entry.tableName = tableName;
            entry.fieldName = indexName;
            entry.oldIndex = oldIndex;
            entry.newIndex = newIndex;
            entry.indexRowId = indexRowId;
            activeJournal_.entries.push_back(std::move(entry));
        }

        ErrorCode SqliteDatabase::ApplyJournalReverse(const JournalTransaction& tx)
        {
            return ApplyJournalReverse(tx, 0, tx.entries.size(), nullptr);
        }

        ErrorCode SqliteDatabase::ApplyJournalForward(const JournalTransaction& tx)
        {
            return ApplyJournalForward(tx, 0, tx.entries.size(), nullptr);
        }

        ErrorCode SqliteDatabase::ApplyJournalReverse(const JournalTransaction& tx,
                                                      std::size_t beginIndex,
                                                      std::size_t endIndex,
                                                      std::size_t* outAppliedCount)
        {
            if (outAppliedCount != nullptr)
            {
                *outAppliedCount = 0;
            }
            if (beginIndex > endIndex || endIndex > tx.entries.size())
            {
                return SC_E_INVALIDARG;
            }

            for (std::size_t index = endIndex; index > beginIndex; --index)
            {
                const ErrorCode rc = ApplyEntry(tx.entries[index - 1], true);
                if (Failed(rc))
                {
                    return rc;
                }
                if (outAppliedCount != nullptr)
                {
                    ++(*outAppliedCount);
                }
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::ApplyJournalForward(const JournalTransaction& tx,
                                                      std::size_t beginIndex,
                                                      std::size_t endIndex,
                                                      std::size_t* outAppliedCount)
        {
            if (outAppliedCount != nullptr)
            {
                *outAppliedCount = 0;
            }
            if (beginIndex > endIndex || endIndex > tx.entries.size())
            {
                return SC_E_INVALIDARG;
            }

            for (std::size_t index = beginIndex; index < endIndex; ++index)
            {
                const ErrorCode rc = ApplyEntry(tx.entries[index], false);
                if (Failed(rc))
                {
                    return rc;
                }
                if (outAppliedCount != nullptr)
                {
                    ++(*outAppliedCount);
                }
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::ApplyEntry(const JournalEntry& entry, bool reverse)
        {
            if (entry.op == JournalOp::RenameTable)
            {
                std::wstring oldName;
                std::wstring newName;
                if (entry.oldValue.AsStringCopy(&oldName) != SC_OK ||
                    entry.newValue.AsStringCopy(&newName) != SC_OK ||
                    oldName.empty() || newName.empty())
                {
                    return SC_E_FAIL;
                }

                if (HasActiveEdit())
                {
                    DeferredRenameState* state =
                        FindDeferredRenameState(oldName, newName);
                    if (state == nullptr)
                    {
                        return SC_E_FAIL;
                    }

                    if (reverse)
                    {
                        RollbackDeferredRenameWorkingState(*state);
                    }
                    else
                    {
                        ApplyDeferredRenameWorkingState(*state);
                    }
                    return SC_OK;
                }

                const std::wstring& fromName = reverse ? newName : oldName;
                const std::wstring& toName = reverse ? oldName : newName;
                return PersistTableRename(fromName, toName, false, false);
            }

            const auto tableIt = tables_.find(entry.tableName);
            if (tableIt == tables_.end())
            {
                return SC_OK;
            }

            auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
            switch (entry.op)
            {
                case JournalOp::AddColumn:
                case JournalOp::UpdateColumn:
                case JournalOp::RemoveColumn:
                case JournalOp::AddConstraint:
                case JournalOp::RemoveConstraint:
                case JournalOp::AddIndex:
                case JournalOp::RemoveIndex:
                    return ApplySchemaEntry(entry, reverse);
                case JournalOp::CreateRecord:
                case JournalOp::DeleteRecord:
                case JournalOp::SetRelation:
                case JournalOp::SetValue: {
                    auto data = table->FindRecordData(entry.recordId);
                    if (!data)
                    {
                        data = std::make_shared<SqliteRecordData>(entry.recordId);
                        table->Records().emplace(entry.recordId, data);
                    }

                    switch (entry.op)
                    {
                        case JournalOp::CreateRecord:
                        case JournalOp::DeleteRecord:
                            data->state = reverse ? (entry.oldDeleted ? RecordState::Deleted : RecordState::Alive)
                                                  : (entry.newDeleted ? RecordState::Deleted : RecordState::Alive);
                            if (reverse && entry.op == JournalOp::CreateRecord)
                            {
                                data->values.clear();
                            }
                            break;
                        case JournalOp::SetRelation:
                        case JournalOp::SetValue:
                            if (reverse)
                            {
                                if (entry.oldValue.IsNull())
                                {
                                    data->values.erase(entry.fieldName);
                                } else
                                {
                                    data->values[entry.fieldName] = entry.oldValue;
                                }
                            } else if (entry.newValue.IsNull())
                            {
                                data->values.erase(entry.fieldName);
                            } else
                            {
                                data->values[entry.fieldName] = entry.newValue;
                            }
                            break;
                        default:
                            break;
                    }
                    break;
                }
                default:
                    break;
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::ApplySchemaEntry(const JournalEntry& entry, bool reverse)
        {
            const auto tableIt = tables_.find(entry.tableName);
            if (tableIt == tables_.end())
            {
                return SC_OK;
            }

            auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
            if (table == nullptr)
            {
                return SC_OK;
            }

            auto* schema = table->Schema();
            if (schema == nullptr)
            {
                return SC_OK;
            }

            const SCColumnDef& def = reverse ? entry.oldColumn : entry.newColumn;
            const auto makeReplayColumn = [&]() {
                SCColumnDef replayColumn = def;
                if (replayColumn.name.empty())
                {
                    replayColumn.name = entry.fieldName;
                }
                return replayColumn;
            };

            switch (entry.op)
            {
                case JournalOp::AddColumn:
                    if (reverse)
                    {
                        return PersistSchemaRemoveColumn(schema, entry.fieldName.c_str());
                    } else
                    {
                        return PersistSchemaAddColumn(schema, makeReplayColumn(), entry.columnRowId);
                    }
                case JournalOp::UpdateColumn:
                    return PersistSchemaUpdateColumn(schema, makeReplayColumn());
                case JournalOp::RemoveColumn:
                    if (reverse)
                    {
                        return PersistSchemaAddColumn(schema, makeReplayColumn(), entry.columnRowId);
                    } else
                    {
                        return PersistSchemaRemoveColumn(schema, entry.fieldName.c_str());
                    }
                case JournalOp::AddConstraint:
                    if (reverse)
                    {
                        return PersistSchemaRemoveConstraint(schema, entry.fieldName.c_str());
                    }
                    return PersistSchemaAddConstraint(schema, entry.newConstraint, entry.constraintRowId);
                case JournalOp::RemoveConstraint:
                    if (reverse)
                    {
                        return PersistSchemaAddConstraint(schema, entry.oldConstraint, entry.constraintRowId);
                    }
                    return PersistSchemaRemoveConstraint(schema, entry.fieldName.c_str());
                case JournalOp::AddIndex:
                    if (reverse)
                    {
                        return PersistSchemaRemoveIndex(schema, entry.fieldName.c_str());
                    }
                    return PersistSchemaAddIndex(schema, entry.newIndex, entry.indexRowId);
                case JournalOp::RemoveIndex:
                    if (reverse)
                    {
                        return PersistSchemaAddIndex(schema, entry.oldIndex, entry.indexRowId);
                    }
                    return PersistSchemaRemoveIndex(schema, entry.fieldName.c_str());
                default:
                    break;
            }
            return SC_OK;
        }

        void SqliteDatabase::UpdateTouchedVersions(
            const JournalTransaction& tx,
            VersionId version,
            bool reverseRenameResolution)
        {
            for (const auto& [tableName, recordId] :
                 GetTouchedRecordKeys(tx, reverseRenameResolution))
            {
                const auto tableIt = tables_.find(tableName);
                if (tableIt == tables_.end())
                {
                    continue;
                }
                auto record = static_cast<SqliteTable*>(tableIt->second.Get())->FindRecordData(recordId);
                if (record)
                {
                    record->lastModifiedVersion = version;
                }
            }
        }

        SCChangeSet SqliteDatabase::BuildChangeSet(const JournalTransaction& tx,
                                                   ChangeSource source,
                                                   VersionId version) const
        {
            SCChangeSet SCChangeSet;
            SCChangeSet.actionName = tx.actionName;
            SCChangeSet.source = source;
            SCChangeSet.version = version;

            for (const auto& entry : tx.entries)
            {
                SCDataChange change;
                change.tableName = entry.tableName;
                change.recordId = entry.recordId;
                change.fieldName = entry.fieldName;
                change.oldValue = (source == ChangeSource::Undo) ? entry.newValue : entry.oldValue;
                change.newValue = (source == ChangeSource::Undo) ? entry.oldValue : entry.newValue;
                change.structuralChange =
                    (entry.op == JournalOp::CreateRecord || entry.op == JournalOp::DeleteRecord ||
                     entry.op == JournalOp::AddColumn || entry.op == JournalOp::UpdateColumn ||
                     entry.op == JournalOp::RemoveColumn || entry.op == JournalOp::AddConstraint ||
                     entry.op == JournalOp::RemoveConstraint || entry.op == JournalOp::AddIndex ||
                     entry.op == JournalOp::RemoveIndex || entry.op == JournalOp::RenameTable);
                change.relationChange = (entry.op == JournalOp::SetRelation);

                switch (entry.op)
                {
                    case JournalOp::CreateRecord:
                        change.kind =
                            (source == ChangeSource::Undo) ? ChangeKind::RecordDeleted : ChangeKind::RecordCreated;
                        break;
                    case JournalOp::DeleteRecord:
                        change.kind =
                            (source == ChangeSource::Undo) ? ChangeKind::RecordCreated : ChangeKind::RecordDeleted;
                        break;
                    case JournalOp::SetRelation:
                        change.kind = ChangeKind::RelationUpdated;
                        break;
                    case JournalOp::AddColumn:
                    case JournalOp::UpdateColumn:
                    case JournalOp::RemoveColumn:
                    case JournalOp::AddConstraint:
                    case JournalOp::RemoveConstraint:
                    case JournalOp::AddIndex:
                    case JournalOp::RemoveIndex:
                    case JournalOp::SetValue:
                        change.kind = ChangeKind::FieldUpdated;
                        break;
                    case JournalOp::RenameTable:
                        change.kind = ChangeKind::TableRenamed;
                        break;
                    default:
                        change.kind = ChangeKind::FieldUpdated;
                        break;
                }

                SCChangeSet.changes.push_back(std::move(change));
            }
            return SCChangeSet;
        }

        void SqliteDatabase::NotifyObservers(const SCChangeSet& SCChangeSet)
        {
            std::vector<ISCDatabaseObserver*> snapshot = observers_;
            for (auto* observer : snapshot)
            {
                if (observer != nullptr)
                {
                    observer->OnDatabaseChanged(SCChangeSet);
                }
            }
        }

        std::vector<std::pair<std::wstring, RecordId>>
        SqliteDatabase::GetTouchedRecordKeys(
            const JournalTransaction& tx,
            bool reverseRenameResolution) const
        {
            std::set<std::pair<std::wstring, RecordId>> unique;
            const bool hasRenameTable =
                JournalTransactionContainsRenameTable(tx);
            for (const auto& entry : tx.entries)
            {
                unique.emplace(
                    hasRenameTable
                        ? ResolveJournalTableNameToReplayState(
                              tx,
                              entry.tableName,
                              reverseRenameResolution)
                        : entry.tableName,
                    entry.recordId);
            }
            return std::vector<std::pair<std::wstring, RecordId>>(unique.begin(), unique.end());
        }

        void SqliteDatabase::PersistTouchedRecords(
            const JournalTransaction& tx,
            VersionId version,
            bool reverseRenameResolution)
        {
            SqliteStmt upsertRecord = db_.Prepare(
                "INSERT INTO records(table_id, record_id, state, "
                "last_modified_version) VALUES(?, ?, ?, ?)"
                " ON CONFLICT(table_id, record_id) DO UPDATE SET "
                "state=excluded.state, "
                "last_modified_version=excluded.last_modified_version;");
            SqliteStmt deleteValues = db_.Prepare(
                "DELETE FROM field_values WHERE table_id = ? AND record_id = "
                "?;");
            SqliteStmt insertValue = db_.Prepare(
                "INSERT INTO field_values(table_id, record_id, column_name, "
                "value_kind, int64_value, double_value, bool_value, text_value, blob_value)"
                " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);");

            for (const auto& [tableName, recordId] :
                 GetTouchedRecordKeys(tx, reverseRenameResolution))
            {
                const auto tableIt = tables_.find(tableName);
                if (tableIt == tables_.end())
                {
                    continue;
                }

                auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
                auto data = table->FindRecordData(recordId);
                if (!data)
                {
                    continue;
                }

                upsertRecord.BindInt64(1, table->TableRowId());
                upsertRecord.BindInt64(2, data->id);
                upsertRecord.BindInt(3, ToSqliteRecordState(data->state));
                upsertRecord.BindInt64(4, static_cast<std::int64_t>(version));
                upsertRecord.Step();
                upsertRecord.Reset();

                deleteValues.BindInt64(1, table->TableRowId());
                deleteValues.BindInt64(2, data->id);
                deleteValues.Step();
                deleteValues.Reset();

                if (data->state == RecordState::Deleted)
                {
                    const ErrorCode rebuildDeletedRc = RebuildCompositeIndexEntriesForRecord(table, *data);
                    if (Failed(rebuildDeletedRc))
                    {
                        throw std::runtime_error("failed to rebuild deleted composite index entries");
                    }
                    continue;
                }

                for (const auto& [fieldName, SCValue] : data->values)
                {
                    insertValue.BindInt64(1, table->TableRowId());
                    insertValue.BindInt64(2, data->id);
                    insertValue.BindText(3, fieldName);
                    BindValueForStorage(insertValue, 4, 5, 6, 7, 8, 9, SCValue);
                    insertValue.Step();
                    insertValue.Reset();
                }

                const ErrorCode rebuildRc = RebuildCompositeIndexEntriesForRecord(table, *data);
                if (Failed(rebuildRc))
                {
                    throw std::runtime_error("failed to rebuild composite index entries");
                }
            }
        }

        std::int64_t SqliteDatabase::InsertJournalTransaction(const JournalTransaction& tx,
                                                              int stackKind,
                                                              int stackOrder)
        {
            SqliteStmt stmt = db_.Prepare(
                "INSERT INTO journal_transactions(action_name, "
                "committed_version, stack_kind, stack_order) VALUES(?, ?, ?, "
                "?);");
            stmt.BindText(1, tx.actionName);
            stmt.BindInt64(2, static_cast<std::int64_t>(tx.committedVersion));
            stmt.BindInt(3, stackKind);
            stmt.BindInt(4, stackOrder);
            stmt.Step();
            return db_.LastInsertRowId();
        }

        void SqliteDatabase::PersistJournalEntries(std::int64_t txId, const JournalTransaction& tx)
        {
            SqliteStmt valueStmt = db_.Prepare(
                "INSERT INTO journal_entries("
                " tx_id, sequence_index, op, table_name, record_id, "
                "field_name, old_kind, old_int64, old_double, old_bool, "
                "old_text, old_blob,"
                " new_kind, new_int64, new_double, new_bool, new_text, new_blob, "
                "old_deleted, new_deleted)"
                " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

            int sequence = 0;
            for (const auto& entry : tx.entries)
            {
                if (entry.op == JournalOp::AddColumn || entry.op == JournalOp::UpdateColumn ||
                    entry.op == JournalOp::RemoveColumn)
                {
                    continue;
                }

                valueStmt.BindInt64(1, txId);
                valueStmt.BindInt(2, sequence++);
                valueStmt.BindInt(3, ToSqliteJournalOp(entry.op));
                valueStmt.BindText(4, entry.tableName);
                const bool constraintEntry =
                    entry.op == JournalOp::AddConstraint || entry.op == JournalOp::RemoveConstraint;
                const bool indexEntry = entry.op == JournalOp::AddIndex || entry.op == JournalOp::RemoveIndex;
                valueStmt.BindInt64(
                    5, constraintEntry ? entry.constraintRowId : (indexEntry ? entry.indexRowId : entry.recordId));
                valueStmt.BindText(6, entry.fieldName);
                const SCValue oldPersistedValue =
                    constraintEntry ? (entry.oldConstraint.name.empty()
                                           ? SCValue::Null()
                                           : SCValue::FromString(SerializeConstraintDef(entry.oldConstraint)))
                                    : (indexEntry ? (entry.oldIndex.name.empty()
                                                         ? SCValue::Null()
                                                         : SCValue::FromString(SerializeIndexDef(entry.oldIndex)))
                                                  : entry.oldValue);
                const SCValue newPersistedValue =
                    constraintEntry ? (entry.newConstraint.name.empty()
                                           ? SCValue::Null()
                                           : SCValue::FromString(SerializeConstraintDef(entry.newConstraint)))
                                    : (indexEntry ? (entry.newIndex.name.empty()
                                                         ? SCValue::Null()
                                                         : SCValue::FromString(SerializeIndexDef(entry.newIndex)))
                                                  : entry.newValue);
                BindValueForStorage(valueStmt, 7, 8, 9, 10, 11, 12, oldPersistedValue);
                BindValueForStorage(valueStmt, 13, 14, 15, 16, 17, 18, newPersistedValue);
                valueStmt.BindInt(19, entry.oldDeleted ? 1 : 0);
                valueStmt.BindInt(20, entry.newDeleted ? 1 : 0);
                valueStmt.Step();
                valueStmt.Reset();
            }

            PersistSchemaJournalEntries(txId, tx, &sequence);
        }

        void SqliteDatabase::PersistSchemaJournalEntries(std::int64_t txId, const JournalTransaction& tx, int* sequence)
        {
            if (sequence == nullptr)
            {
                return;
            }

            SqliteStmt stmt = db_.Prepare(
                "INSERT INTO journal_schema_entries("
                " tx_id, sequence_index, op, table_name, column_name, "
                "column_rowid, old_display_name, old_value_kind, "
                "old_column_kind, old_nullable, old_editable, "
                "old_user_defined, old_indexed, old_participates_in_calc, "
                "old_unit, old_reference_table, old_reference_storage_column, "
                "old_reference_display_column, old_default_kind, "
                "old_default_int64, old_default_double, old_default_bool, "
                "old_default_text, old_default_blob, new_display_name, new_value_kind, "
                "new_column_kind, new_nullable, new_editable, "
                "new_user_defined, new_indexed, new_participates_in_calc, "
                "new_unit, new_reference_table, new_reference_storage_column, "
                "new_reference_display_column, new_default_kind, "
                "new_default_int64, new_default_double, new_default_bool, "
                "new_default_text, new_default_blob)"
                " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

            for (const auto& entry : tx.entries)
            {
                if (entry.op != JournalOp::AddColumn && entry.op != JournalOp::UpdateColumn &&
                    entry.op != JournalOp::RemoveColumn)
                {
                    continue;
                }

                stmt.BindInt64(1, txId);
                stmt.BindInt(2, (*sequence)++);
                stmt.BindInt(3, ToSqliteJournalOp(entry.op));
                stmt.BindText(4, entry.tableName);
                stmt.BindText(5, entry.fieldName);
                stmt.BindInt64(6, entry.columnRowId);
                BindColumnDefForStorage(
                    stmt, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
                    entry.oldColumn);
                BindColumnDefForStorage(
                    stmt, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42,
                    entry.newColumn);
                stmt.Step();
                stmt.Reset();
            }
        }

        void SqliteDatabase::DeleteRedoJournalRows()
        {
            for (const auto& tx : redoStack_)
            {
                DeleteJournalTransaction(tx.txId);
            }
            redoStack_.clear();
        }

        void SqliteDatabase::UpdateJournalTransactionStack(std::int64_t txId, int stackKind, int stackOrder)
        {
            SqliteStmt stmt = db_.Prepare(
                "UPDATE journal_transactions SET stack_kind = ?, stack_order = "
                "? WHERE tx_id = ?;");
            stmt.BindInt(1, stackKind);
            stmt.BindInt(2, stackOrder);
            stmt.BindInt64(3, txId);
            stmt.Step();
        }

        void SqliteDatabase::DeleteJournalTransaction(std::int64_t txId)
        {
            SqliteStmt valueStmt = db_.Prepare("DELETE FROM journal_entries WHERE tx_id = ?;");
            valueStmt.BindInt64(1, txId);
            valueStmt.Step();

            SqliteStmt schemaStmt = db_.Prepare("DELETE FROM journal_schema_entries WHERE tx_id = ?;");
            schemaStmt.BindInt64(1, txId);
            schemaStmt.Step();

            SqliteStmt stmt = db_.Prepare("DELETE FROM journal_transactions WHERE tx_id = ?;");
            stmt.BindInt64(1, txId);
            stmt.Step();
        }

    }  // namespace

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
