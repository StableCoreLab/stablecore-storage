#include "SCFactory.h"
#include "ISCQuery.h"
#include "SCQuerySqliteExecutor.h"

#include <algorithm>
#include <iomanip>
#include <mutex>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <sqlite3.h>

#include "ISCDiagnostics.h"
#include "SCBatch.h"
#include "SCMigration.h"
#include "SCRefCounted.h"

#if defined(_WIN32)
#include <windows.h>
#endif

namespace StableCore::Storage
{
    namespace
    {

        void EnsureSqliteQueryDispatchRegistered(ISCDatabase* database)
        {
            (void)database;
            static std::once_flag once;
            std::call_once(once, []() {
                RegisterQueryExecutionContextDispatch(
                    QueryBackendKind::SQLite, &ExecuteSqliteQueryDispatch);
            });
        }

        constexpr int kStackUndo = 0;
        constexpr int kStackRedo = 1;

        ErrorCode MapSqliteError(int code);

        std::string ToUtf8(const std::wstring& text)
        {
#if defined(_WIN32)
            if (text.empty())
            {
                return {};
            }

            const int bytes = WideCharToMultiByte(CP_UTF8, 0, text.c_str(),
                                                  static_cast<int>(text.size()),
                                                  nullptr, 0, nullptr, nullptr);
            std::string utf8(static_cast<std::size_t>(bytes), '\0');
            WideCharToMultiByte(CP_UTF8, 0, text.c_str(),
                                static_cast<int>(text.size()), utf8.data(),
                                bytes, nullptr, nullptr);
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
            const int chars =
                MultiByteToWideChar(CP_UTF8, 0, text, -1, nullptr, 0);
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

#if defined(_WIN32)
        std::wstring GetBackupTempDirectory(const std::wstring& targetPath)
        {
            const std::size_t slash = targetPath.find_last_of(L"\\/");
            std::wstring directory;
            if (slash == std::wstring::npos)
            {
                wchar_t currentDirectory[MAX_PATH] = {};
                const DWORD len =
                    GetCurrentDirectoryW(MAX_PATH, currentDirectory);
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
                    const DWORD len =
                        GetCurrentDirectoryW(MAX_PATH, currentDirectory);
                    if (len == 0 || len >= MAX_PATH)
                    {
                        return {};
                    }
                    directory.assign(currentDirectory, currentDirectory + len);
                }
            }

            if (!directory.empty() && directory.back() != L'\\' &&
                directory.back() != L'/')
            {
                directory.push_back(L'\\');
            }
            return directory;
        }

        bool CreateSiblingTempFile(const std::wstring& targetPath,
                                   std::wstring* outTempPath)
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
            explicit ScopedDeleteFile(std::wstring path)
                : path_(std::move(path))
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

        ErrorCode GetSingleCount(sqlite3* db, const char* sql,
                                 std::size_t* outCount)
        {
            if (outCount == nullptr)
            {
                return SC_E_POINTER;
            }

            sqlite3_stmt* stmt = nullptr;
            const int prepareRc =
                sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
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
                *outCount =
                    static_cast<std::size_t>(sqlite3_column_int64(stmt, 0));
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
                db,
                "UPDATE metadata SET value = ? WHERE key = 'baseline_version';",
                -1, &stmt, nullptr);
            if (prepareRc != SQLITE_OK)
            {
                if (stmt != nullptr)
                {
                    sqlite3_finalize(stmt);
                }
                return MapSqliteError(prepareRc);
            }

            const int bindRc = sqlite3_bind_text(
                stmt, 1,
                std::to_string(static_cast<std::uint64_t>(version)).c_str(), -1,
                SQLITE_TRANSIENT);
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

        ErrorCode ClearJournalHistoryForBackup(
            sqlite3* db, VersionId currentVersion,
            std::size_t* removedTransactionCount,
            std::size_t* removedEntryCount)
        {
            const ErrorCode txCountRc =
                GetSingleCount(db, "SELECT COUNT(*) FROM journal_transactions;",
                               removedTransactionCount);
            if (Failed(txCountRc))
            {
                return txCountRc;
            }

            const ErrorCode entryCountRc = GetSingleCount(
                db, "SELECT COUNT(*) FROM journal_entries;", removedEntryCount);
            if (Failed(entryCountRc))
            {
                return entryCountRc;
            }

            const ErrorCode deleteEntriesRc =
                RunSqliteExec(db, "DELETE FROM journal_entries;");
            if (Failed(deleteEntriesRc))
            {
                return deleteEntriesRc;
            }

            const ErrorCode deleteTransactionsRc =
                RunSqliteExec(db, "DELETE FROM journal_transactions;");
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
            int prepareRc = sqlite3_prepare_v2(db, "PRAGMA integrity_check;",
                                               -1, &stmt, nullptr);
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
                return MapSqliteError(stepRc == SQLITE_DONE ? SQLITE_OK
                                                            : stepRc);
            }

            const std::wstring integrity = FromUtf8(
                reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0)));
            sqlite3_finalize(stmt);
            if (integrity != L"ok")
            {
                return SC_E_VALIDATION_FAILED;
            }

            prepareRc = sqlite3_prepare_v2(db, "PRAGMA foreign_key_check;", -1,
                                           &stmt, nullptr);
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

        ErrorCode GetFileSizeBytes(const std::wstring& path,
                                   std::uint64_t* outSize)
        {
            if (outSize == nullptr)
            {
                return SC_E_POINTER;
            }

#if defined(_WIN32)
            WIN32_FILE_ATTRIBUTE_DATA data{};
            if (!GetFileAttributesExW(path.c_str(), GetFileExInfoStandard,
                                      &data))
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
            }
            return ss.str();
        }

        bool AppendToken(std::wstring* out, const std::wstring& token)
        {
            if (out == nullptr)
            {
                return false;
            }

            *out += std::to_wstring(token.size());
            *out += L':';
            *out += token;
            return true;
        }

        bool ReadToken(const std::wstring& payload, std::size_t* cursor,
                       std::wstring* outToken)
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

            const std::wstring lengthText =
                payload.substr(*cursor, colon - *cursor);
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

        std::wstring SerializeImportSessionPayload(
            const SCImportStagingArea& session)
        {
            std::wstring payload;
            AppendToken(&payload, L"SCIMPORT1");
            AppendToken(&payload, std::to_wstring(session.sessionId));
            AppendToken(&payload, session.sessionName);
            AppendToken(&payload, std::to_wstring(session.baseVersion));
            AppendToken(&payload, std::to_wstring(session.chunkSize));
            AppendToken(&payload,
                        std::to_wstring(static_cast<int>(session.state)));
            AppendToken(&payload, std::to_wstring(session.chunks.size()));
            for (const auto& chunk : session.chunks)
            {
                AppendToken(&payload, std::to_wstring(chunk.chunkId));
                AppendToken(&payload, std::to_wstring(chunk.requests.size()));
                for (const auto& request : chunk.requests)
                {
                    AppendToken(&payload, request.tableName);
                    AppendToken(&payload,
                                std::to_wstring(request.creates.size()));
                    for (const auto& create : request.creates)
                    {
                        AppendToken(&payload,
                                    std::to_wstring(create.values.size()));
                        for (const auto& assignment : create.values)
                        {
                            AppendToken(&payload, assignment.fieldName);
                            AppendToken(&payload, SerializeImportValue(
                                                      assignment.SCValue));
                        }
                    }

                    AppendToken(&payload,
                                std::to_wstring(request.updates.size()));
                    for (const auto& update : request.updates)
                    {
                        AppendToken(&payload, std::to_wstring(update.recordId));
                        AppendToken(&payload,
                                    std::to_wstring(update.values.size()));
                        for (const auto& assignment : update.values)
                        {
                            AppendToken(&payload, assignment.fieldName);
                            AppendToken(&payload, SerializeImportValue(
                                                      assignment.SCValue));
                        }
                    }

                    AppendToken(&payload,
                                std::to_wstring(request.deletes.size()));
                    for (RecordId recordId : request.deletes)
                    {
                        AppendToken(&payload, std::to_wstring(recordId));
                    }
                }
            }
            return payload;
        }

        bool DeserializeImportValue(const std::wstring& token,
                                    SCValue* outValue)
        {
            if (outValue == nullptr)
            {
                return false;
            }

            const std::size_t separator = token.find(L'\n');
            const std::wstring kindText = (separator == std::wstring::npos)
                                              ? token
                                              : token.substr(0, separator);
            const std::wstring payload = (separator == std::wstring::npos)
                                             ? std::wstring{}
                                             : token.substr(separator + 1);

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
                        *outValue = SCValue::FromRecordId(
                            static_cast<RecordId>(std::stoll(payload)));
                        return true;
                    } catch (...)
                    {
                        return false;
                    }
                case ValueKind::Enum:
                    *outValue = SCValue::FromEnum(payload);
                    return true;
                default:
                    return false;
            }
        }

        bool DeserializeImportSessionPayload(const std::wstring& payload,
                                             SCImportStagingArea* outSession)
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
                session.sessionId =
                    static_cast<SCImportSessionId>(std::stoull(token));
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
                session.baseVersion =
                    static_cast<VersionId>(std::stoull(token));
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
                session.chunkSize =
                    static_cast<std::size_t>(std::stoull(token));
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
                session.state =
                    static_cast<SCImportSessionState>(std::stoi(token));
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

            for (std::size_t chunkIndex = 0; chunkIndex < chunkCount;
                 ++chunkIndex)
            {
                SCImportChunk chunk;
                if (!ReadToken(payload, &cursor, &token))
                {
                    return false;
                }
                try
                {
                    chunk.chunkId =
                        static_cast<SCImportChunkId>(std::stoull(token));
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

                for (std::size_t requestIndex = 0; requestIndex < requestCount;
                     ++requestIndex)
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
                        createCount =
                            static_cast<std::size_t>(std::stoull(token));
                    } catch (...)
                    {
                        return false;
                    }
                    for (std::size_t createIndex = 0; createIndex < createCount;
                         ++createIndex)
                    {
                        SCBatchCreateRecordRequest create;
                        if (!ReadToken(payload, &cursor, &token))
                        {
                            return false;
                        }
                        std::size_t valueCount = 0;
                        try
                        {
                            valueCount =
                                static_cast<std::size_t>(std::stoull(token));
                        } catch (...)
                        {
                            return false;
                        }
                        for (std::size_t valueIndex = 0;
                             valueIndex < valueCount; ++valueIndex)
                        {
                            SCFieldValueAssignment assignment;
                            std::wstring valueToken;
                            if (!ReadToken(payload, &cursor,
                                           &assignment.fieldName) ||
                                !ReadToken(payload, &cursor, &valueToken))
                            {
                                return false;
                            }
                            if (!DeserializeImportValue(valueToken,
                                                        &assignment.SCValue))
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
                        updateCount =
                            static_cast<std::size_t>(std::stoull(token));
                    } catch (...)
                    {
                        return false;
                    }
                    for (std::size_t updateIndex = 0; updateIndex < updateCount;
                         ++updateIndex)
                    {
                        SCBatchUpdateRecordRequest update;
                        if (!ReadToken(payload, &cursor, &token))
                        {
                            return false;
                        }
                        try
                        {
                            update.recordId =
                                static_cast<RecordId>(std::stoll(token));
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
                            valueCount =
                                static_cast<std::size_t>(std::stoull(token));
                        } catch (...)
                        {
                            return false;
                        }
                        for (std::size_t valueIndex = 0;
                             valueIndex < valueCount; ++valueIndex)
                        {
                            SCFieldValueAssignment assignment;
                            std::wstring valueToken;
                            if (!ReadToken(payload, &cursor,
                                           &assignment.fieldName) ||
                                !ReadToken(payload, &cursor, &valueToken))
                            {
                                return false;
                            }
                            if (!DeserializeImportValue(valueToken,
                                                        &assignment.SCValue))
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
                        deleteCount =
                            static_cast<std::size_t>(std::stoull(token));
                    } catch (...)
                    {
                        return false;
                    }
                    for (std::size_t deleteIndex = 0; deleteIndex < deleteCount;
                         ++deleteIndex)
                    {
                        if (!ReadToken(payload, &cursor, &token))
                        {
                            return false;
                        }
                        try
                        {
                            request.deletes.push_back(
                                static_cast<RecordId>(std::stoll(token)));
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
                return MapSqliteError(
                    sqlite3_bind_int64(stmt_, index, SCValue));
            }
            ErrorCode BindDouble(int index, double SCValue)
            {
                return MapSqliteError(
                    sqlite3_bind_double(stmt_, index, SCValue));
            }
            ErrorCode BindNull(int index)
            {
                return MapSqliteError(sqlite3_bind_null(stmt_, index));
            }

            ErrorCode BindText(int index, const std::wstring& SCValue)
            {
                const std::string utf8 = ToUtf8(SCValue);
                return MapSqliteError(sqlite3_bind_text(
                    stmt_, index, utf8.c_str(), static_cast<int>(utf8.size()),
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
                return FromUtf8(reinterpret_cast<const char*>(
                    sqlite3_column_text(stmt_, index)));
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
                const int rc = sqlite3_open_v2(
                    utf8.c_str(), &db_,
                    (readOnly ? SQLITE_OPEN_READONLY
                              : (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE)) |
                        SQLITE_OPEN_FULLMUTEX,
                    nullptr);
                if (rc != SQLITE_OK)
                {
                    const std::string message = (db_ != nullptr)
                                                    ? sqlite3_errmsg(db_)
                                                    : "sqlite open failed";
                    if (db_ != nullptr)
                    {
                        sqlite3_close(db_);
                        db_ = nullptr;
                    }
                    throw std::runtime_error(message);
                }

                sqlite3_exec(db_, "PRAGMA foreign_keys = ON;", nullptr, nullptr,
                             nullptr);
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
                    throw std::runtime_error(
                        "failed to begin sqlite transaction");
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

        class SqliteDatabase;
        class SqliteTable;

        ErrorCode ValidateValueKind(ValueKind expected, const SCValue& value,
                                    bool nullable)
        {
            if (value.IsNull())
            {
                return nullable ? SC_OK : SC_E_SCHEMA_VIOLATION;
            }
            return value.GetKind() == expected ? SC_OK : SC_E_TYPE_MISMATCH;
        }

        ErrorCode ValidateColumnDef(const SCColumnDef& def)
        {
            if (def.name.empty())
            {
                return SC_E_INVALIDARG;
            }
            if (def.columnKind == ColumnKind::Relation)
            {
                if (def.valueKind != ValueKind::RecordId)
                {
                    return SC_E_SCHEMA_VIOLATION;
                }
                if (def.referenceTable.empty())
                {
                    return SC_E_SCHEMA_VIOLATION;
                }
                if (!def.defaultValue.IsNull() &&
                    def.defaultValue.GetKind() != ValueKind::RecordId)
                {
                    return SC_E_SCHEMA_VIOLATION;
                }
            } else
            {
                if (!def.referenceTable.empty())
                {
                    return SC_E_SCHEMA_VIOLATION;
                }
                if (!def.defaultValue.IsNull() &&
                    def.defaultValue.GetKind() != def.valueKind)
                {
                    return SC_E_SCHEMA_VIOLATION;
                }
            }
            if (!def.nullable && def.defaultValue.IsNull())
            {
                return SC_E_SCHEMA_VIOLATION;
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
        int ToSqliteJournalOp(JournalOp op) noexcept
        {
            return static_cast<int>(op);
        }
        JournalOp FromSqliteJournalOp(int op) noexcept
        {
            return static_cast<JournalOp>(op);
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

        void BindValueForStorage(SqliteStmt& stmt, int kindIndex, int intIndex,
                                 int doubleIndex, int boolIndex, int textIndex,
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
                    break;
                case ValueKind::Int64: {
                    std::int64_t v = 0;
                    value.AsInt64(&v);
                    stmt.BindInt64(intIndex, v);
                    stmt.BindNull(doubleIndex);
                    stmt.BindNull(boolIndex);
                    stmt.BindNull(textIndex);
                    break;
                }
                case ValueKind::Double: {
                    double v = 0.0;
                    value.AsDouble(&v);
                    stmt.BindNull(intIndex);
                    stmt.BindDouble(doubleIndex, v);
                    stmt.BindNull(boolIndex);
                    stmt.BindNull(textIndex);
                    break;
                }
                case ValueKind::Bool: {
                    bool v = false;
                    value.AsBool(&v);
                    stmt.BindNull(intIndex);
                    stmt.BindNull(doubleIndex);
                    stmt.BindInt(boolIndex, v ? 1 : 0);
                    stmt.BindNull(textIndex);
                    break;
                }
                case ValueKind::String: {
                    std::wstring text;
                    value.AsStringCopy(&text);
                    stmt.BindNull(intIndex);
                    stmt.BindNull(doubleIndex);
                    stmt.BindNull(boolIndex);
                    stmt.BindText(textIndex, text);
                    break;
                }
                case ValueKind::RecordId: {
                    RecordId id = 0;
                    value.AsRecordId(&id);
                    stmt.BindInt64(intIndex, id);
                    stmt.BindNull(doubleIndex);
                    stmt.BindNull(boolIndex);
                    stmt.BindNull(textIndex);
                    break;
                }
                case ValueKind::Enum: {
                    std::wstring text;
                    value.AsEnumCopy(&text);
                    stmt.BindNull(intIndex);
                    stmt.BindNull(doubleIndex);
                    stmt.BindNull(boolIndex);
                    stmt.BindText(textIndex, text);
                    break;
                }
            }
        }

        SCValue ReadValueFromStorage(const SqliteStmt& stmt, int kindIndex,
                                     int intIndex, int doubleIndex,
                                     int boolIndex, int textIndex)
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
                default:
                    return SCValue::Null();
            }
        }

        class SqliteSchema final : public ISCSchema, public SCRefCountedObject
        {
        public:
            SqliteSchema(SqliteDatabase* db, std::wstring tableName,
                         std::int64_t tableRowId)
                : db_(db),
                  tableName_(std::move(tableName)),
                  tableRowId_(tableRowId)
            {
            }

            ErrorCode GetColumnCount(std::int32_t* outCount) override;
            ErrorCode GetColumn(std::int32_t index,
                                SCColumnDef* outDef) override;
            ErrorCode FindColumn(const wchar_t* name,
                                 SCColumnDef* outDef) override;
            ErrorCode AddColumn(const SCColumnDef& def) override;
            ErrorCode UpdateColumn(const SCColumnDef& def) override;
            ErrorCode RemoveColumn(const wchar_t* name) override;

            const SCColumnDef* FindColumnDef(
                const std::wstring& name) const noexcept
            {
                const auto it = columnsByName_.find(name);
                return it == columnsByName_.end() ? nullptr : &it->second;
            }

            void LoadColumn(const SCColumnDef& def)
            {
                columns_.push_back(def);
                columnsByName_[def.name] = def;
            }

            void ReplaceColumn(const SCColumnDef& def)
            {
                const auto vecIt =
                    std::find_if(columns_.begin(), columns_.end(),
                                 [&def](const SCColumnDef& existing) {
                                     return existing.name == def.name;
                                 });
                if (vecIt != columns_.end())
                {
                    *vecIt = def;
                }
                columnsByName_[def.name] = def;
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

                const auto vecIt =
                    std::find_if(columns_.begin(), columns_.end(),
                                 [name](const SCColumnDef& def) {
                                     return def.name == name;
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
            std::int64_t tableRowId_{0};
            std::vector<SCColumnDef> columns_;
            std::unordered_map<std::wstring, SCColumnDef> columnsByName_;
        };

        class SqliteEditSession final : public ISCEditSession,
                                        public SCRefCountedObject
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
            SqliteRecord(SqliteDatabase* db, SqliteTable* table,
                         std::shared_ptr<SqliteRecordData> data)
                : db_(db), table_(table), data_(std::move(data))
            {
            }

            RecordId GetId() const noexcept override;
            bool IsDeleted() const noexcept override;
            VersionId GetLastModifiedVersion() const noexcept override;

            ErrorCode GetValue(const wchar_t* name, SCValue* outValue) override;
            ErrorCode SetValue(const wchar_t* name,
                               const SCValue& value) override;

            ErrorCode GetInt64(const wchar_t* name,
                               std::int64_t* outValue) override;
            ErrorCode SetInt64(const wchar_t* name,
                               std::int64_t value) override;

            ErrorCode GetDouble(const wchar_t* name, double* outValue) override;
            ErrorCode SetDouble(const wchar_t* name, double value) override;

            ErrorCode GetBool(const wchar_t* name, bool* outValue) override;
            ErrorCode SetBool(const wchar_t* name, bool value) override;

            ErrorCode GetString(const wchar_t* name,
                                const wchar_t** outValue) override;
            ErrorCode GetStringCopy(const wchar_t* name,
                                    std::wstring* outValue) override;
            ErrorCode SetString(const wchar_t* name,
                                const wchar_t* value) override;

            ErrorCode GetRef(const wchar_t* name, RecordId* outValue) override;
            ErrorCode SetRef(const wchar_t* name, RecordId value) override;

        private:
            ErrorCode ReadTypedValue(const wchar_t* name, SCValue* outValue);

            SqliteDatabase* db_{nullptr};
            SqliteTable* table_{nullptr};
            std::shared_ptr<SqliteRecordData> data_;
        };

        class SqliteRecordCursor final : public ISCRecordCursor,
                                         public SCRefCountedObject
        {
        public:
            explicit SqliteRecordCursor(std::vector<SCRecordPtr> records)
                : records_(std::move(records))
            {
            }

            ErrorCode MoveNext(bool* outHasValue) override
            {
                if (outHasValue == nullptr)
                {
                    return SC_E_POINTER;
                }

                if (index_ < records_.size())
                {
                    current_ = records_[index_++];
                    *outHasValue = true;
                    return SC_OK;
                }

                current_.Reset();
                *outHasValue = false;
                return SC_OK;
            }

            ErrorCode GetCurrent(SCRecordPtr& outRecord) override
            {
                if (!current_)
                {
                    return SC_FALSE_RESULT;
                }
                outRecord = current_;
                return SC_OK;
            }

        private:
            std::vector<SCRecordPtr> records_;
            std::size_t index_{0};
            SCRecordPtr current_;
        };

        class SqliteTable final : public ISCTable, public SCRefCountedObject
        {
        public:
            SqliteTable(SqliteDatabase* db, std::wstring name,
                        std::int64_t tableRowId)
                : db_(db),
                  name_(std::move(name)),
                  schema_(SCMakeRef<SqliteSchema>(db, name_, tableRowId))
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
            ErrorCode FindRecords(const SCQueryCondition& condition,
                                  SCRecordCursorPtr& outCursor) override;

            const std::wstring& Name() const noexcept
            {
                return name_;
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

            std::unordered_map<RecordId, std::shared_ptr<SqliteRecordData>>&
            Records() noexcept
            {
                return records_;
            }

        private:
            SCRecordPtr MakeRecord(
                const std::shared_ptr<SqliteRecordData>& data)
            {
                return SCMakeRef<SqliteRecord>(db_, this, data);
            }

            SqliteDatabase* db_{nullptr};
            std::wstring name_;
            SCRefPtr<SqliteSchema> schema_;
            std::unordered_map<RecordId, std::shared_ptr<SqliteRecordData>>
                records_;
        };

        class SqliteDatabase final : public ISCDatabase,
                                     public ISCDatabaseDiagnosticsProvider,
                                     public IReferenceIndexProvider,
                                     public IReferenceIndexMaintainer,
                                     public SCRefCountedObject
        {
        public:
            explicit SqliteDatabase(const std::wstring& path);
            explicit SqliteDatabase(const std::wstring& path, bool readOnly);
            explicit SqliteDatabase(const std::wstring& path,
                                    const SCOpenDatabaseOptions& options);
            ~SqliteDatabase() override;

            ErrorCode BeginEdit(const wchar_t* name,
                                SCEditPtr& outEdit) override;
            ErrorCode Commit(ISCEditSession* edit) override;
            ErrorCode Rollback(ISCEditSession* edit) override;
            ErrorCode Undo() override;
            ErrorCode Redo() override;
            ErrorCode GetTableCount(std::int32_t* outCount) override;
            ErrorCode GetTableName(std::int32_t index,
                                   std::wstring* outName) override;
            ErrorCode GetTable(const wchar_t* name,
                               SCTablePtr& outTable) override;
            ErrorCode CreateTable(const wchar_t* name,
                                  SCTablePtr& outTable) override;
            ErrorCode AddObserver(ISCDatabaseObserver* observer) override;
            ErrorCode RemoveObserver(ISCDatabaseObserver* observer) override;
            ErrorCode ExecuteUpgradePlan(const SCUpgradePlan& plan,
                                         bool confirmed,
                                         SCUpgradeResult* outResult) override;
            ErrorCode BeginImportSession(
                const SCImportSessionOptions& options,
                SCImportStagingArea* outSession) override;
            ErrorCode AppendImportChunk(
                SCImportStagingArea* session, const SCImportChunk& chunk,
                SCImportCheckpoint* outCheckpoint) override;
            ErrorCode LoadImportRecoveryState(
                std::uint64_t sessionId,
                SCImportRecoveryState* outState) override;
            ErrorCode FinalizeImportSession(
                const SCImportFinalizeCommit& commit,
                SCImportRecoveryState* outState) override;
            ErrorCode AbortImportSession(std::uint64_t sessionId) override;
            ErrorCode CreateBackupCopy(const wchar_t* targetPath,
                                       const SCBackupOptions& options,
                                       SCBackupResult* outResult) override;
            ErrorCode ClearColumnValues(ISCTable* table,
                                        const wchar_t* name) override;
            ErrorCode ResetHistoryBaseline(
                SCBackupResult* outResult = nullptr) override;
            ErrorCode GetEditLogState(SCEditLogState* outState) const override;
            ErrorCode GetEditingState(
                SCEditingDatabaseState* outState) const override;
            VersionId GetCurrentVersion() const noexcept override
            {
                return version_;
            }
            std::int32_t GetSchemaVersion() const noexcept override
            {
                return schemaVersion_;
            }
            ErrorCode CollectDiagnostics(
                SCStorageHealthReport* outReport) const override;
            ErrorCode GetReferencesBySource(
                const std::wstring& sourceTable, RecordId sourceRecordId,
                std::vector<ReferenceRecord>* outRecords) const override;
            ErrorCode GetReferencesByTarget(
                const std::wstring& targetTable, RecordId targetRecordId,
                std::vector<ReverseReferenceRecord>* outRecords) const override;
            ErrorCode CheckReferenceIndex(
                ReferenceIndexCheckResult* outResult) const override;
            ErrorCode GetAllReferencesDiagnosticOnly(
                ReferenceIndex* outIndex) const override;
            ErrorCode RebuildReferenceIndexes() override;
            ErrorCode CommitReferenceDelta(
                const ReferenceIndex& forwardDelta,
                const ReverseReferenceIndex& reverseDelta) override;

            bool HasActiveEdit() const noexcept
            {
                return static_cast<bool>(activeEdit_);
            }
            RecordId AllocateRecordId() noexcept
            {
                return nextRecordId_++;
            }

            ErrorCode WriteValue(SqliteTable* table,
                                 const std::shared_ptr<SqliteRecordData>& data,
                                 const std::wstring& fieldName,
                                 const SCValue& value);
            ErrorCode DeleteRecord(
                SqliteTable* table,
                const std::shared_ptr<SqliteRecordData>& data);
            void RecordCreate(SqliteTable* table,
                              const std::shared_ptr<SqliteRecordData>& data);
            ErrorCode PersistAddedColumn(SqliteSchema* schema,
                                         const SCColumnDef& def);
            ErrorCode PersistUpdatedColumn(SqliteSchema* schema,
                                           const SCColumnDef& def);
            ErrorCode PersistRemovedColumn(SqliteSchema* schema,
                                           const wchar_t* columnName);

        private:
            struct JournalLookup
            {
                bool createdInActiveEdit{false};
                bool deletedInActiveEdit{false};
            };

            void InitializeSchema();
            void LoadMetadata();
            void SaveMetadata();
            void SaveMetadataKey(const wchar_t* key, const std::wstring& value);
            void LoadTables();
            void LoadJournalStacks();
            void MaterializeIndexes();
            ErrorCode EnsureImportSessionStore();
            void EnsureColumnIndex(std::int64_t tableRowId,
                                   const std::wstring& columnName);
            void RunStartupIntegrityCheck();
            void LogStartupDiagnostic(SCDiagnosticSeverity severity,
                                      const std::wstring& category,
                                      const std::wstring& message);
            void SetCleanShutdownFlag(bool cleanShutdown);
            ErrorCode EnsureWritable() const;
            ErrorCode ValidateActiveEdit(ISCEditSession* edit) const;
            ErrorCode ValidateWrite(
                SqliteTable* table,
                const std::shared_ptr<SqliteRecordData>& data,
                const std::wstring& fieldName, const SCValue& value);
            bool IsRecordReferenced(const std::wstring& tableName,
                                    RecordId recordId) const;
            void MarkReferenceIndexDirty() noexcept;
            void RefreshReferenceIndexState();
            JournalLookup LookupRecordJournalState(
                const std::wstring& tableName, RecordId recordId) const;
            void RemoveFieldJournalEntries(const std::wstring& tableName,
                                           RecordId recordId);
            void RemoveAllJournalEntriesForRecord(const std::wstring& tableName,
                                                  RecordId recordId);
            void RecordJournal(const std::wstring& tableName, RecordId recordId,
                               const std::wstring& fieldName,
                               const SCValue& oldValue, const SCValue& newValue,
                               bool oldDeleted, bool newDeleted, JournalOp op);
            void ApplyJournalReverse(const JournalTransaction& tx);
            void ApplyJournalForward(const JournalTransaction& tx);
            void ApplyEntry(const JournalEntry& entry, bool reverse);
            void UpdateTouchedVersions(const JournalTransaction& tx,
                                       VersionId version);
            SCChangeSet BuildChangeSet(const JournalTransaction& tx,
                                       ChangeSource source,
                                       VersionId version) const;
            void NotifyObservers(const SCChangeSet& SCChangeSet);
            std::vector<std::pair<std::wstring, RecordId>> GetTouchedRecordKeys(
                const JournalTransaction& tx) const;
            void PersistTouchedRecords(const JournalTransaction& tx);
            std::int64_t InsertJournalTransaction(const JournalTransaction& tx,
                                                  int stackKind,
                                                  int stackOrder);
            void PersistJournalEntries(std::int64_t txId,
                                       const JournalTransaction& tx);
            void DeleteRedoJournalRows();
            void UpdateJournalTransactionStack(std::int64_t txId, int stackKind,
                                               int stackOrder);
            void DeleteJournalTransaction(std::int64_t txId);
            std::wstring SerializeImportSession(
                const SCImportStagingArea& session) const;
            ErrorCode DeserializeImportSession(
                const std::wstring& payload,
                SCImportStagingArea* outSession) const;

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
            bool cleanShutdown_{true};
            bool dirtyStartupDetected_{false};
            bool corruptionDetected_{false};
            bool importSessionStoreReady_{false};
            std::vector<SCDiagnosticEntry> startupDiagnostics_;
            std::map<std::wstring, SCTablePtr> tables_;
            std::vector<ISCDatabaseObserver*> observers_;
            SCRefPtr<SqliteEditSession> activeEdit_;
            JournalTransaction activeJournal_;
            std::vector<SqlitePersistedJournalTransaction> undoStack_;
            std::vector<SqlitePersistedJournalTransaction> redoStack_;
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

        ErrorCode SqliteSchema::GetColumn(std::int32_t index,
                                          SCColumnDef* outDef)
        {
            if (outDef == nullptr)
            {
                return SC_E_POINTER;
            }
            if (index < 0 || static_cast<std::size_t>(index) >= columns_.size())
            {
                return SC_E_INVALIDARG;
            }
            *outDef = columns_[static_cast<std::size_t>(index)];
            return SC_OK;
        }

        ErrorCode SqliteSchema::FindColumn(const wchar_t* name,
                                           SCColumnDef* outDef)
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
            *outDef = it->second;
            return SC_OK;
        }

        ErrorCode SqliteSchema::AddColumn(const SCColumnDef& def)
        {
            const ErrorCode validate = ValidateColumnDef(def);
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
            const ErrorCode validate = ValidateColumnDef(def);
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

        ErrorCode SqliteRecord::ReadTypedValue(const wchar_t* name,
                                               SCValue* outValue)
        {
            return GetValue(name, outValue);
        }

        ErrorCode SqliteRecord::GetValue(const wchar_t* name, SCValue* outValue)
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
            *outValue =
                (it != data_->values.end()) ? it->second : column->defaultValue;
            return outValue->IsNull() ? SC_E_VALUE_IS_NULL : SC_OK;
        }

        ErrorCode SqliteRecord::SetValue(const wchar_t* name,
                                         const SCValue& value)
        {
            if (name == nullptr)
            {
                return SC_E_INVALIDARG;
            }
            return db_->WriteValue(table_, data_, name, value);
        }

        ErrorCode SqliteRecord::GetInt64(const wchar_t* name,
                                         std::int64_t* outValue)
        {
            SCValue value;
            const ErrorCode rc = ReadTypedValue(name, &value);
            if (Failed(rc))
            {
                return rc;
            }
            return value.AsInt64(outValue);
        }

        ErrorCode SqliteRecord::SetInt64(const wchar_t* name,
                                         std::int64_t value)
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

        ErrorCode SqliteRecord::GetString(const wchar_t* name,
                                          const wchar_t** outValue)
        {
            SCValue value;
            const ErrorCode rc = ReadTypedValue(name, &value);
            if (Failed(rc))
            {
                return rc;
            }
            return value.AsString(outValue);
        }

        ErrorCode SqliteRecord::GetStringCopy(const wchar_t* name,
                                              std::wstring* outValue)
        {
            SCValue value;
            const ErrorCode rc = ReadTypedValue(name, &value);
            if (Failed(rc))
            {
                return rc;
            }
            return value.AsStringCopy(outValue);
        }

        ErrorCode SqliteRecord::SetString(const wchar_t* name,
                                          const wchar_t* value)
        {
            return SetValue(name, value == nullptr
                                      ? SCValue::Null()
                                      : SCValue::FromString(value));
        }

        ErrorCode SqliteRecord::GetRef(const wchar_t* name, RecordId* outValue)
        {
            SCValue value;
            const ErrorCode rc = ReadTypedValue(name, &value);
            if (Failed(rc))
            {
                return rc;
            }
            return value.AsRecordId(outValue);
        }

        ErrorCode SqliteRecord::SetRef(const wchar_t* name, RecordId value)
        {
            return SetValue(name, SCValue::FromRecordId(value));
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

            auto data =
                std::make_shared<SqliteRecordData>(db_->AllocateRecordId());
            records_.emplace(data->id, data);
            db_->RecordCreate(this, data);
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

        ErrorCode SqliteTable::FindRecords(const SCQueryCondition& condition,
                                           SCRecordCursorPtr& outCursor)
        {
            QueryPlan legacyPlan;
            const ErrorCode bridgeRc =
                SCQueryBridge::BuildPlanFromLegacyFindRecords(name_, condition,
                                                              &legacyPlan);
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
            const ErrorCode planRc = planner->BuildPlan(
                legacyPlan.target, legacyPlan.conditionGroups,
                legacyPlan.conditionGroupLogic, legacyPlan.orderBy,
                legacyPlan.page, legacyPlan.hints, legacyPlan.constraints,
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

        SqliteDatabase::SqliteDatabase(const std::wstring& path)
            : SqliteDatabase(path, false)
        {
        }

        SqliteDatabase::SqliteDatabase(const std::wstring& path, bool readOnly)
            : path_(path),
              db_(path, readOnly),
              openMode_(readOnly ? SCDatabaseOpenMode::ReadOnly
                                 : SCDatabaseOpenMode::Normal),
              readOnly_(readOnly)
        {
            if (!readOnly_)
            {
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

        SqliteDatabase::SqliteDatabase(const std::wstring& path,
                                       const SCOpenDatabaseOptions& options)
            : path_(path),
              db_(path, options.openMode == SCDatabaseOpenMode::ReadOnly),
              openMode_(options.openMode),
              readOnly_(options.openMode == SCDatabaseOpenMode::ReadOnly)
        {
            if (!readOnly_)
            {
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
                if (!readOnly_)
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
                "default_kind INTEGER NOT NULL, default_int64 INTEGER,"
                "default_double REAL, default_bool INTEGER, default_text TEXT, "
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
                "text_value TEXT,"
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
                "old_double REAL, old_bool INTEGER, old_text TEXT, new_kind "
                "INTEGER NOT NULL, new_int64 INTEGER,"
                "new_double REAL, new_bool INTEGER, new_text TEXT, old_deleted "
                "INTEGER NOT NULL, new_deleted INTEGER NOT NULL,"
                "PRIMARY KEY(tx_id, sequence_index));");
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
                    baselineVersion_ =
                        static_cast<VersionId>(std::stoull(SCValue));
                    hasBaselineVersion = true;
                } else if (key == L"schema_version")
                {
                    schemaVersion_ =
                        static_cast<std::int32_t>(std::stoi(SCValue));
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

        void SqliteDatabase::SaveMetadata()
        {
            SaveMetadataKey(L"version", std::to_wstring(version_));
            SaveMetadataKey(L"baseline_version",
                            std::to_wstring(baselineVersion_));
            SaveMetadataKey(L"next_record_id", std::to_wstring(nextRecordId_));
            SaveMetadataKey(L"schema_version", std::to_wstring(schemaVersion_));
            SaveMetadataKey(L"clean_shutdown", cleanShutdown_ ? L"1" : L"0");
        }

        void SqliteDatabase::SaveMetadataKey(const wchar_t* key,
                                             const std::wstring& value)
        {
            SqliteStmt stmt = db_.Prepare(
                "INSERT INTO metadata(key, value) VALUES(?, ?)"
                " ON CONFLICT(key) DO UPDATE SET value=excluded.value;");
            stmt.BindText(1, key != nullptr ? key : L"");
            stmt.BindText(2, value);
            stmt.Step();
        }

        void SqliteDatabase::LoadTables()
        {
            SqliteStmt tablesStmt =
                db_.Prepare("SELECT table_id, name FROM tables ORDER BY name;");
            bool hasRow = false;
            while (tablesStmt.Step(&hasRow) == SC_OK && hasRow)
            {
                const std::int64_t tableRowId = tablesStmt.ColumnInt64(0);
                const std::wstring tableName = tablesStmt.ColumnText(1);
                SCTablePtr table =
                    SCMakeRef<SqliteTable>(this, tableName, tableRowId);
                tables_.emplace(tableName, table);
                auto* sqliteTable = static_cast<SqliteTable*>(table.Get());

                SqliteStmt columnsStmt = db_.Prepare(
                    "SELECT column_name, display_name, value_kind, "
                    "column_kind, nullable_flag, editable_flag, "
                    "user_defined_flag,"
                    " indexed_flag, participates_in_calc_flag, unit, "
                    "reference_table, default_kind, default_int64, "
                    "default_double,"
                    " default_bool, default_text FROM schema_columns WHERE "
                    "table_id = ? ORDER BY rowid;");
                columnsStmt.BindInt64(1, tableRowId);
                bool hasColumn = false;
                while (columnsStmt.Step(&hasColumn) == SC_OK && hasColumn)
                {
                    SCColumnDef def;
                    def.name = columnsStmt.ColumnText(0);
                    def.displayName = columnsStmt.ColumnText(1);
                    def.valueKind =
                        FromSqliteValueKind(columnsStmt.ColumnInt(2));
                    def.columnKind =
                        FromSqliteColumnKind(columnsStmt.ColumnInt(3));
                    def.nullable = columnsStmt.ColumnBool(4);
                    def.editable = columnsStmt.ColumnBool(5);
                    def.userDefined = columnsStmt.ColumnBool(6);
                    def.indexed = columnsStmt.ColumnBool(7);
                    def.participatesInCalc = columnsStmt.ColumnBool(8);
                    def.unit = columnsStmt.ColumnText(9);
                    def.referenceTable = columnsStmt.ColumnText(10);
                    def.defaultValue =
                        ReadValueFromStorage(columnsStmt, 11, 12, 13, 14, 15);
                    sqliteTable->Schema()->LoadColumn(def);
                }

                SqliteStmt recordsStmt = db_.Prepare(
                    "SELECT record_id, state, last_modified_version FROM "
                    "records WHERE table_id = ?;");
                recordsStmt.BindInt64(1, tableRowId);
                bool hasRecord = false;
                while (recordsStmt.Step(&hasRecord) == SC_OK && hasRecord)
                {
                    auto record = std::make_shared<SqliteRecordData>(
                        recordsStmt.ColumnInt64(0));
                    record->state =
                        FromSqliteRecordState(recordsStmt.ColumnInt(1));
                    record->lastModifiedVersion =
                        static_cast<VersionId>(recordsStmt.ColumnInt64(2));
                    sqliteTable->Records().emplace(record->id, record);
                    if (record->id >= nextRecordId_)
                    {
                        nextRecordId_ = record->id + 1;
                    }
                }

                SqliteStmt valuesStmt = db_.Prepare(
                    "SELECT record_id, column_name, value_kind, int64_value, "
                    "double_value, bool_value, text_value"
                    " FROM field_values WHERE table_id = ?;");
                valuesStmt.BindInt64(1, tableRowId);
                bool hasValue = false;
                while (valuesStmt.Step(&hasValue) == SC_OK && hasValue)
                {
                    auto record =
                        sqliteTable->FindRecordData(valuesStmt.ColumnInt64(0));
                    if (!record)
                    {
                        continue;
                    }
                    record->values[valuesStmt.ColumnText(1)] =
                        ReadValueFromStorage(valuesStmt, 2, 3, 4, 5, 6);
                }
            }
        }

        void SqliteDatabase::LoadJournalStacks()
        {
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
                persisted.tx.committedVersion =
                    static_cast<VersionId>(txStmt.ColumnInt64(2));
                const int stackKind = txStmt.ColumnInt(3);

                SqliteStmt entryStmt = db_.Prepare(
                    "SELECT op, table_name, record_id, field_name, old_kind, "
                    "old_int64, old_double, old_bool, old_text,"
                    " new_kind, new_int64, new_double, new_bool, new_text, "
                    "old_deleted, new_deleted"
                    " FROM journal_entries WHERE tx_id = ? ORDER BY "
                    "sequence_index;");
                entryStmt.BindInt64(1, persisted.txId);
                bool hasEntry = false;
                while (entryStmt.Step(&hasEntry) == SC_OK && hasEntry)
                {
                    JournalEntry entry;
                    entry.op = FromSqliteJournalOp(entryStmt.ColumnInt(0));
                    entry.tableName = entryStmt.ColumnText(1);
                    entry.recordId = entryStmt.ColumnInt64(2);
                    entry.fieldName = entryStmt.ColumnText(3);
                    entry.oldValue =
                        ReadValueFromStorage(entryStmt, 4, 5, 6, 7, 8);
                    entry.newValue =
                        ReadValueFromStorage(entryStmt, 9, 10, 11, 12, 13);
                    entry.oldDeleted = entryStmt.ColumnBool(14);
                    entry.newDeleted = entryStmt.ColumnBool(15);
                    persisted.tx.entries.push_back(std::move(entry));
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
                result.failureReason =
                    L"Upgrade is not allowed after an unclean shutdown.";
                return finish(SC_E_WRITE_CONFLICT);
            }

            if (!confirmed)
            {
                result.status = SCUpgradeStatus::Failed;
                result.failureReason =
                    L"Upgrade confirmation was not provided.";
                return finish(SC_E_INVALIDARG);
            }

            if (plan.currentVersion != schemaVersion_)
            {
                result.status = SCUpgradeStatus::Failed;
                result.failureReason =
                    L"Upgrade plan does not match the current schema version.";
                return finish(SC_E_INVALIDARG);
            }

            if (plan.targetVersion <= plan.currentVersion)
            {
                result.status = SCUpgradeStatus::NotRequired;
                result.failureReason =
                    L"Upgrade is not required for the current schema version.";
                return finish(SC_OK);
            }

            if (plan.orderedSteps.empty())
            {
                result.status = SCUpgradeStatus::Unsupported;
                result.failureReason =
                    L"Upgrade plan does not contain executable steps.";
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

                    if (step.fromVersion == 1 && step.toVersion == 2)
                    {
                        const ErrorCode diagnosticsRc = db_.Execute(
                            "CREATE TABLE IF NOT EXISTS startup_diagnostics ("
                            "diag_id INTEGER PRIMARY KEY AUTOINCREMENT, "
                            "severity INTEGER NOT NULL, category TEXT NOT "
                            "NULL, message TEXT NOT NULL);");
                        if (Failed(diagnosticsRc))
                        {
                            schemaVersion_ = originalSchemaVersion;
                            result.status = SCUpgradeStatus::RolledBack;
                            result.rolledBack = true;
                            result.failureReason =
                                L"Failed to create upgrade-required diagnostic "
                                L"storage.";
                            return finish(diagnosticsRc);
                        }

                        const ErrorCode indexRc = db_.Execute(
                            "CREATE INDEX IF NOT EXISTS "
                            "idx_field_values_record ON field_values(table_id, "
                            "record_id, column_name);");
                        if (Failed(indexRc))
                        {
                            schemaVersion_ = originalSchemaVersion;
                            result.status = SCUpgradeStatus::RolledBack;
                            result.rolledBack = true;
                            result.failureReason =
                                L"Failed to create upgrade-required lookup "
                                L"index.";
                            return finish(indexRc);
                        }
                    } else
                    {
                        schemaVersion_ = originalSchemaVersion;
                        result.status = SCUpgradeStatus::Unsupported;
                        result.failureReason = L"Unsupported upgrade step.";
                        return finish(SC_E_NOTIMPL);
                    }

                    schemaVersion_ = step.toVersion;
                    std::wstringstream message;
                    message << L"Applied upgrade step " << step.name
                            << L" to schema version " << schemaVersion_ << L".";
                    LogStartupDiagnostic(SCDiagnosticSeverity::Info, L"upgrade",
                                         message.str());
                }

                SaveMetadata();
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    schemaVersion_ = originalSchemaVersion;
                    result.status = SCUpgradeStatus::RolledBack;
                    result.rolledBack = true;
                    result.failureReason =
                        L"Failed to commit the upgrade transaction.";
                    return finish(commitRc);
                }
            } catch (...)
            {
                schemaVersion_ = originalSchemaVersion;
                result.status = SCUpgradeStatus::RolledBack;
                result.rolledBack = true;
                result.failureReason =
                    L"Upgrade transaction failed and was rolled back.";
                return finish(SC_E_FAIL);
            }

            result.status = SCUpgradeStatus::Success;
            result.rolledBack = false;
            result.sourceVersion = originalSchemaVersion;
            result.targetVersion = schemaVersion_;
            result.failureReason.clear();
            return finish(SC_OK);
        }

        void SqliteDatabase::MaterializeIndexes()
        {
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
        }

        void SqliteDatabase::EnsureColumnIndex(std::int64_t tableRowId,
                                               const std::wstring& columnName)
        {
            const std::wstring indexName = L"idx_fv_" +
                                           std::to_wstring(tableRowId) + L"_" +
                                           SanitizeIdentifier(columnName);
            const std::string sql =
                ToUtf8(L"CREATE INDEX IF NOT EXISTS " + indexName +
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
                    LogStartupDiagnostic(
                        SCDiagnosticSeverity::Error, L"integrity",
                        std::wstring(L"SQLite integrity check failed: ") +
                            result);
                    throw std::runtime_error("sqlite integrity check failed");
                }
            }

            LogStartupDiagnostic(SCDiagnosticSeverity::Info, L"integrity",
                                 L"SQLite integrity check passed.");
        }

        void SqliteDatabase::LogStartupDiagnostic(SCDiagnosticSeverity severity,
                                                  const std::wstring& category,
                                                  const std::wstring& message)
        {
            startupDiagnostics_.push_back(
                SCDiagnosticEntry{severity, category, message});
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

        std::wstring SqliteDatabase::SerializeImportSession(
            const SCImportStagingArea& session) const
        {
            return SerializeImportSessionPayload(session);
        }

        ErrorCode SqliteDatabase::DeserializeImportSession(
            const std::wstring& payload, SCImportStagingArea* outSession) const
        {
            if (outSession == nullptr)
            {
                return SC_E_POINTER;
            }
            return DeserializeImportSessionPayload(payload, outSession)
                       ? SC_OK
                       : SC_E_FAIL;
        }

        ErrorCode SqliteDatabase::BeginImportSession(
            const SCImportSessionOptions& options,
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
            session.sessionName =
                options.sessionName.empty() ? L"Import" : options.sessionName;
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

            session.sessionId =
                static_cast<SCImportSessionId>(db_.LastInsertRowId());
            const std::wstring payload = SerializeImportSessionPayload(session);
            SqliteStmt updateStmt = db_.Prepare(
                "UPDATE import_sessions SET payload = ?, checkpoint_chunk_id = "
                "?, checkpoint_count = ?, state = ? WHERE session_id = ?;");
            updateStmt.BindText(1, payload);
            updateStmt.BindInt64(2, 0);
            updateStmt.BindInt64(3, 0);
            updateStmt.BindInt(4,
                               static_cast<int>(SCImportSessionState::Staging));
            updateStmt.BindInt64(5,
                                 static_cast<std::int64_t>(session.sessionId));
            const ErrorCode updateRc = updateStmt.Step();
            if (Failed(updateRc))
            {
                return updateRc;
            }
            *outSession = session;
            return SC_OK;
        }

        ErrorCode SqliteDatabase::AppendImportChunk(
            SCImportStagingArea* session, const SCImportChunk& chunk,
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

            SqliteStmt loadStmt = db_.Prepare(
                "SELECT session_id FROM import_sessions WHERE session_id = ?;");
            loadStmt.BindInt64(1,
                               static_cast<std::int64_t>(session->sessionId));
            bool hasRow = false;
            if (Failed(loadStmt.Step(&hasRow)) || !hasRow)
            {
                return SC_E_RECORD_NOT_FOUND;
            }

            const std::wstring payload =
                SerializeImportSessionPayload(*session);
            SqliteStmt updateStmt = db_.Prepare(
                "UPDATE import_sessions SET state = ?, checkpoint_chunk_id = "
                "?, checkpoint_count = ?, payload = ? WHERE session_id = ?;");
            updateStmt.BindInt(
                1, static_cast<int>(SCImportSessionState::Checkpointed));
            updateStmt.BindInt64(2, static_cast<std::int64_t>(chunk.chunkId));
            updateStmt.BindInt64(
                3, static_cast<std::int64_t>(session->chunks.size()));
            updateStmt.BindText(4, payload);
            updateStmt.BindInt64(5,
                                 static_cast<std::int64_t>(session->sessionId));
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

        ErrorCode SqliteDatabase::LoadImportRecoveryState(
            std::uint64_t sessionId, SCImportRecoveryState* outState)
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
            state.checkpoint.lastChunkId =
                static_cast<SCImportChunkId>(stmt.ColumnInt64(4));
            state.checkpoint.chunkCount =
                static_cast<std::size_t>(stmt.ColumnInt64(5));
            state.checkpoint.baseVersion =
                static_cast<VersionId>(stmt.ColumnInt64(2));
            state.checkpoint.persisted = true;
            state.checkpointPersisted = true;

            const std::wstring payload = stmt.ColumnText(6);
            if (!payload.empty())
            {
                if (!DeserializeImportSessionPayload(payload,
                                                     &state.stagingArea))
                {
                    return SC_E_FAIL;
                }
            } else
            {
                state.stagingArea.sessionId = sessionId;
                state.stagingArea.sessionName = stmt.ColumnText(0);
                state.stagingArea.baseVersion =
                    static_cast<VersionId>(stmt.ColumnInt64(2));
                state.stagingArea.chunkSize =
                    static_cast<std::size_t>(stmt.ColumnInt64(3));
                state.stagingArea.state = state.state;
            }

            state.stagingArea.sessionId = sessionId;
            state.stagingArea.sessionName = stmt.ColumnText(0);
            state.stagingArea.baseVersion =
                static_cast<VersionId>(stmt.ColumnInt64(2));
            state.stagingArea.chunkSize =
                static_cast<std::size_t>(stmt.ColumnInt64(3));
            state.stagingArea.state = state.state;
            state.canResume = state.state != SCImportSessionState::Finalized &&
                              state.state != SCImportSessionState::Aborted;
            state.canFinalize =
                state.state == SCImportSessionState::Checkpointed ||
                state.state == SCImportSessionState::ReadyToFinalize;
            state.reason = state.canResume
                               ? L"Import session recoverable from checkpoint."
                               : L"Import session is no longer recoverable.";

            *outState = std::move(state);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::FinalizeImportSession(
            const SCImportFinalizeCommit& commit,
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
            const ErrorCode loadRc =
                LoadImportRecoveryState(commit.sessionId, &recoveryState);
            if (Failed(loadRc))
            {
                return loadRc;
            }

            SqliteStmt deleteStmt = db_.Prepare(
                "DELETE FROM import_sessions WHERE session_id = ?;");
            deleteStmt.BindInt64(1,
                                 static_cast<std::int64_t>(commit.sessionId));
            const ErrorCode deleteRc = deleteStmt.Step();
            if (Failed(deleteRc))
            {
                return deleteRc;
            }

            recoveryState.state = SCImportSessionState::Finalized;
            recoveryState.canResume = false;
            recoveryState.canFinalize = false;
            recoveryState.reason =
                L"Import session finalized and checkpoint cleared.";
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

            SqliteStmt deleteStmt = db_.Prepare(
                "DELETE FROM import_sessions WHERE session_id = ?;");
            deleteStmt.BindInt64(1, static_cast<std::int64_t>(sessionId));
            return deleteStmt.Step();
        }

        ErrorCode SqliteDatabase::EnsureWritable() const
        {
            return readOnly_ ? SC_E_READ_ONLY_DATABASE : SC_OK;
        }

        ErrorCode SqliteDatabase::CollectDiagnostics(
            SCStorageHealthReport* outReport) const
        {
            if (outReport == nullptr)
            {
                return SC_E_POINTER;
            }

            outReport->diagnostics.insert(outReport->diagnostics.end(),
                                          startupDiagnostics_.begin(),
                                          startupDiagnostics_.end());
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
            return SC_OK;
        }

        ErrorCode SqliteDatabase::BeginEdit(const wchar_t* name,
                                            SCEditPtr& outEdit)
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
            activeJournal_.actionName =
                (name != nullptr && *name != L'\0') ? name : L"Edit";
            activeEdit_ = SCMakeRef<SqliteEditSession>(
                activeJournal_.actionName, version_);
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
            const ErrorCode validate = ValidateActiveEdit(edit);
            if (Failed(validate))
            {
                return validate;
            }

            activeEdit_->SetState(EditState::Committed);
            if (activeJournal_.entries.empty())
            {
                activeEdit_.Reset();
                activeJournal_ = JournalTransaction{};
                return SC_OK;
            }

            try
            {
                SqliteTxn txn(db_);
                ++version_;
                UpdateTouchedVersions(activeJournal_, version_);
                PersistTouchedRecords(activeJournal_);
                DeleteRedoJournalRows();
                activeJournal_.committedVersion = version_;
                const std::int64_t txId = InsertJournalTransaction(
                    activeJournal_, kStackUndo,
                    static_cast<int>(undoStack_.size()));
                activeJournal_.commitId = static_cast<CommitId>(txId);
                PersistJournalEntries(txId, activeJournal_);
                undoStack_.push_back(
                    SqlitePersistedJournalTransaction{txId, activeJournal_});
                redoStack_.clear();
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

            RefreshReferenceIndexState();
            const SCChangeSet SCChangeSet = BuildChangeSet(
                activeJournal_, ChangeSource::UserEdit, version_);
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

            if (!activeJournal_.entries.empty())
            {
                ApplyJournalReverse(activeJournal_);
            }
            RefreshReferenceIndexState();
            activeEdit_->SetState(EditState::RolledBack);
            activeEdit_.Reset();
            activeJournal_ = JournalTransaction{};
            return SC_OK;
        }

        ErrorCode SqliteDatabase::Undo()
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
            if (undoStack_.empty())
            {
                return SC_E_UNDO_STACK_EMPTY;
            }

            SqlitePersistedJournalTransaction tx = undoStack_.back();
            undoStack_.pop_back();
            ApplyJournalReverse(tx.tx);

            try
            {
                SqliteTxn txn(db_);
                ++version_;
                UpdateTouchedVersions(tx.tx, version_);
                PersistTouchedRecords(tx.tx);
                UpdateJournalTransactionStack(
                    tx.txId, kStackRedo, static_cast<int>(redoStack_.size()));
                redoStack_.push_back(tx);
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

            RefreshReferenceIndexState();
            NotifyObservers(
                BuildChangeSet(tx.tx, ChangeSource::Undo, version_));
            return SC_OK;
        }

        ErrorCode SqliteDatabase::Redo()
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
            if (redoStack_.empty())
            {
                return SC_E_REDO_STACK_EMPTY;
            }

            SqlitePersistedJournalTransaction tx = redoStack_.back();
            redoStack_.pop_back();
            ApplyJournalForward(tx.tx);

            try
            {
                SqliteTxn txn(db_);
                ++version_;
                UpdateTouchedVersions(tx.tx, version_);
                PersistTouchedRecords(tx.tx);
                UpdateJournalTransactionStack(
                    tx.txId, kStackUndo, static_cast<int>(undoStack_.size()));
                undoStack_.push_back(tx);
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

            RefreshReferenceIndexState();
            NotifyObservers(
                BuildChangeSet(tx.tx, ChangeSource::Redo, version_));
            return SC_OK;
        }

        ErrorCode SqliteDatabase::GetTable(const wchar_t* name,
                                           SCTablePtr& outTable)
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

        ErrorCode SqliteDatabase::GetTableName(std::int32_t index,
                                               std::wstring* outName)
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

        ErrorCode SqliteDatabase::CreateTable(const wchar_t* name,
                                              SCTablePtr& outTable)
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
                SqliteStmt stmt =
                    db_.Prepare("INSERT INTO tables(name) VALUES(?);");
                stmt.BindText(1, name);
                const ErrorCode rc = stmt.Step();
                if (Failed(rc))
                {
                    return rc;
                }

                SCTablePtr table = SCMakeRef<SqliteTable>(
                    this, std::wstring{name}, db_.LastInsertRowId());
                tables_.emplace(name, table);
                SaveMetadata();
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    return commitRc;
                }

                MarkReferenceIndexDirty();
                outTable = std::move(table);
                return SC_OK;
            } catch (...)
            {
                return SC_E_FAIL;
            }
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
            observers_.erase(
                std::remove(observers_.begin(), observers_.end(), observer),
                observers_.end());
            return SC_OK;
        }

        ErrorCode SqliteDatabase::CreateBackupCopy(
            const wchar_t* targetPath, const SCBackupOptions& options,
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
            const int openRc =
                sqlite3_open_v2(tempUtf8.c_str(), &targetDb,
                                SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
                                    SQLITE_OPEN_FULLMUTEX,
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
            sqlite3_backup* backup =
                sqlite3_backup_init(targetDb, "main", db_.Raw(), "main");
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
                resultRc = ClearJournalHistoryForBackup(
                    targetDb, version_, &removedTransactionCount,
                    &removedEntryCount);
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
                const ErrorCode sizeRc =
                    GetFileSizeBytes(tempFilePath, &outputFileSizeBytes);
                if (Failed(sizeRc))
                {
                    resultRc = sizeRc;
                }
            }

            sqlite3_close(targetDb);

            if (resultRc == SC_OK)
            {
                if (!MoveFileExW(
                        tempFilePath.c_str(), targetPath,
                        MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH))
                {
                    return SC_E_IO_ERROR;
                }

                cleanup.Release();
                if (outResult != nullptr)
                {
                    outResult->removedJournalTransactionCount =
                        static_cast<std::uint64_t>(removedTransactionCount);
                    outResult->removedJournalEntryCount =
                        static_cast<std::uint64_t>(removedEntryCount);
                    outResult->outputFileSizeBytes = outputFileSizeBytes;
                }
            }

            return resultRc;
#endif
        }

        ErrorCode SqliteDatabase::ResetHistoryBaseline(
            SCBackupResult* outResult)
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
                const ErrorCode clearRc = ClearJournalHistoryForBackup(
                    db_.Raw(), version_, &removedTransactionCount,
                    &removedEntryCount);
                if (Failed(clearRc))
                {
                    return clearRc;
                }
                baselineVersion_ = version_;
                undoStack_.clear();
                redoStack_.clear();
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

            if (outResult != nullptr)
            {
                *outResult = SCBackupResult{};
                outResult->removedJournalTransactionCount =
                    static_cast<std::uint64_t>(removedTransactionCount);
                outResult->removedJournalEntryCount =
                    static_cast<std::uint64_t>(removedEntryCount);
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::GetEditLogState(
            SCEditLogState* outState) const
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
                    tx.tx.commitId, tx.tx.committedVersion,
                    SCEditLogActionKind::Commit, tx.tx.actionName, L"", 0});
            }

            outState->redoItems.reserve(redoStack_.size());
            for (const auto& tx : redoStack_)
            {
                outState->redoItems.push_back(SCEditLogEntry{
                    tx.tx.commitId, tx.tx.committedVersion,
                    SCEditLogActionKind::Commit, tx.tx.actionName, L"", 0});
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::GetEditingState(
            SCEditingDatabaseState* outState) const
        {
            if (outState == nullptr)
            {
                return SC_E_POINTER;
            }

            outState->open = true;
            outState->dirty =
                static_cast<bool>(activeEdit_) || !undoStack_.empty();
            outState->openMode = openMode_;
            outState->currentVersion = version_;
            outState->baselineVersion = baselineVersion_;
            outState->undoCount = openMode_ == SCDatabaseOpenMode::NoHistory
                                      ? 0
                                      : undoStack_.size();
            outState->redoCount = openMode_ == SCDatabaseOpenMode::NoHistory
                                      ? 0
                                      : redoStack_.size();
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

        ErrorCode SqliteDatabase::ValidateWrite(
            SqliteTable* table, const std::shared_ptr<SqliteRecordData>& data,
            const std::wstring& fieldName, const SCValue& value)
        {
            if (!activeEdit_)
            {
                return SC_E_NO_ACTIVE_EDIT;
            }
            if (data->state == RecordState::Deleted)
            {
                return SC_E_RECORD_DELETED;
            }

            const SCColumnDef* column =
                table->Schema()->FindColumnDef(fieldName);
            if (column == nullptr)
            {
                return SC_E_COLUMN_NOT_FOUND;
            }
            if (!column->editable)
            {
                return SC_E_READ_ONLY_COLUMN;
            }

            const ErrorCode validate =
                ValidateValueKind(column->valueKind, value, column->nullable);
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
                    auto target =
                        static_cast<SqliteTable*>(targetIt->second.Get())
                            ->FindRecordData(refId);
                    if (!target || target->state == RecordState::Deleted)
                    {
                        return SC_E_REFERENCE_INVALID;
                    }
                }
            }

            return SC_OK;
        }

        SqliteDatabase::JournalLookup SqliteDatabase::LookupRecordJournalState(
            const std::wstring& tableName, RecordId recordId) const
        {
            JournalLookup lookup;
            for (const auto& entry : activeJournal_.entries)
            {
                if (entry.tableName != tableName || entry.recordId != recordId)
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

        void SqliteDatabase::RemoveFieldJournalEntries(
            const std::wstring& tableName, RecordId recordId)
        {
            activeJournal_.entries.erase(
                std::remove_if(activeJournal_.entries.begin(),
                               activeJournal_.entries.end(),
                               [&](const JournalEntry& entry) {
                                   return entry.tableName == tableName &&
                                          entry.recordId == recordId &&
                                          (entry.op == JournalOp::SetValue ||
                                           entry.op == JournalOp::SetRelation);
                               }),
                activeJournal_.entries.end());
        }

        void SqliteDatabase::RemoveAllJournalEntriesForRecord(
            const std::wstring& tableName, RecordId recordId)
        {
            activeJournal_.entries.erase(
                std::remove_if(activeJournal_.entries.begin(),
                               activeJournal_.entries.end(),
                               [&](const JournalEntry& entry) {
                                   return entry.tableName == tableName &&
                                          entry.recordId == recordId;
                               }),
                activeJournal_.entries.end());
        }

        ErrorCode SqliteDatabase::WriteValue(
            SqliteTable* table, const std::shared_ptr<SqliteRecordData>& data,
            const std::wstring& fieldName, const SCValue& value)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            const ErrorCode validate =
                ValidateWrite(table, data, fieldName, value);
            if (Failed(validate))
            {
                return validate;
            }

            const JournalLookup lookup =
                LookupRecordJournalState(table->Name(), data->id);
            if (lookup.deletedInActiveEdit)
            {
                return SC_E_RECORD_DELETED;
            }

            const SCColumnDef* column =
                table->Schema()->FindColumnDef(fieldName);
            SCValue oldValue = column->defaultValue;
            const auto existing = data->values.find(fieldName);
            if (existing != data->values.end())
            {
                oldValue = existing->second;
            }

            if (oldValue == value)
            {
                return SC_OK;
            }

            data->values[fieldName] = value;
            const JournalOp op = (column != nullptr &&
                                  column->columnKind == ColumnKind::Relation)
                                     ? JournalOp::SetRelation
                                     : JournalOp::SetValue;
            RecordJournal(table->Name(), data->id, fieldName, oldValue, value,
                          false, false, op);
            MarkReferenceIndexDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::DeleteRecord(
            SqliteTable* table, const std::shared_ptr<SqliteRecordData>& data)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (!activeEdit_)
            {
                return SC_E_NO_ACTIVE_EDIT;
            }
            if (data->state == RecordState::Deleted)
            {
                return SC_E_RECORD_DELETED;
            }
            if (IsRecordReferenced(table->Name(), data->id))
            {
                return SC_E_CONSTRAINT_VIOLATION;
            }

            const JournalLookup lookup =
                LookupRecordJournalState(table->Name(), data->id);
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
            RecordJournal(table->Name(), data->id, L"", SCValue::Null(),
                          SCValue::Null(), false, true,
                          JournalOp::DeleteRecord);
            MarkReferenceIndexDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::GetReferencesBySource(
            const std::wstring& sourceTable, RecordId sourceRecordId,
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
            if (recordIt == table->Records().end() ||
                recordIt->second == nullptr ||
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

            for (std::int32_t columnIndex = 0; columnIndex < columnCount;
                 ++columnIndex)
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

                const auto valueIt = recordIt->second->values.find(column.name);
                if (valueIt == recordIt->second->values.end())
                {
                    continue;
                }

                RecordId targetRecordId = 0;
                if (Failed(valueIt->second.AsRecordId(&targetRecordId)))
                {
                    continue;
                }

                outRecords->push_back(
                    ReferenceRecord{sourceTable, sourceRecordId, column.name,
                                    column.referenceTable, targetRecordId,
                                    version_, 0, std::nullopt});
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::GetReferencesByTarget(
            const std::wstring& targetTable, RecordId targetRecordId,
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

                for (std::int32_t columnIndex = 0; columnIndex < columnCount;
                     ++columnIndex)
                {
                    SCColumnDef column;
                    if (Failed(schema->GetColumn(columnIndex, &column)))
                    {
                        continue;
                    }
                    if (column.columnKind != ColumnKind::Relation ||
                        column.referenceTable != targetTable)
                    {
                        continue;
                    }

                    for (const auto& [candidateId, candidateData] :
                         table->Records())
                    {
                        if (candidateData == nullptr ||
                            candidateData->state == RecordState::Deleted)
                        {
                            continue;
                        }

                        const auto valueIt =
                            candidateData->values.find(column.name);
                        if (valueIt == candidateData->values.end())
                        {
                            continue;
                        }

                        RecordId referencedId = 0;
                        if (Succeeded(
                                valueIt->second.AsRecordId(&referencedId)) &&
                            referencedId == targetRecordId)
                        {
                            outRecords->push_back(ReverseReferenceRecord{
                                targetTable, targetRecordId, table->Name(),
                                candidateId, column.name, version_, 0,
                                std::nullopt});
                        }
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::CheckReferenceIndex(
            ReferenceIndexCheckResult* outResult) const
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
            outResult->indexVersion =
                static_cast<std::int32_t>(referenceIndexVersion_);
            outResult->expectedVersion = static_cast<std::int32_t>(version_);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::GetAllReferencesDiagnosticOnly(
            ReferenceIndex* outIndex) const
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
                    if (recordData == nullptr ||
                        recordData->state == RecordState::Deleted)
                    {
                        continue;
                    }

                    for (std::int32_t columnIndex = 0;
                         columnIndex < columnCount; ++columnIndex)
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

                        const auto valueIt =
                            recordData->values.find(column.name);
                        if (valueIt == recordData->values.end())
                        {
                            continue;
                        }

                        RecordId targetId = 0;
                        if (Failed(valueIt->second.AsRecordId(&targetId)))
                        {
                            continue;
                        }

                        outIndex->records.push_back(ReferenceRecord{
                            table->Name(), recordId, column.name,
                            column.referenceTable, targetId, version_, 0,
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

        ErrorCode SqliteDatabase::CommitReferenceDelta(
            const ReferenceIndex& forwardDelta,
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

        void SqliteDatabase::RefreshReferenceIndexState()
        {
            const ErrorCode rc = RebuildReferenceIndexes();
            if (Failed(rc))
            {
                referenceIndexBuilt_ = false;
                referenceIndexVersion_ = 0;
            }
        }

        bool SqliteDatabase::IsRecordReferenced(const std::wstring& tableName,
                                                RecordId recordId) const
        {
            ReferenceIndexCheckResult check;
            if (Succeeded(CheckReferenceIndex(&check)) &&
                check.state == ReferenceIndexHealthState::Healthy)
            {
                std::vector<ReverseReferenceRecord> refs;
                if (Succeeded(
                        GetReferencesByTarget(tableName, recordId, &refs)))
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

                for (std::int32_t columnIndex = 0; columnIndex < columnCount;
                     ++columnIndex)
                {
                    SCColumnDef column;
                    if (Failed(schema->GetColumn(columnIndex, &column)))
                    {
                        continue;
                    }
                    if (column.columnKind != ColumnKind::Relation ||
                        column.referenceTable != tableName)
                    {
                        continue;
                    }

                    for (const auto& [candidateId, candidateData] :
                         table->Records())
                    {
                        if (candidateData == nullptr ||
                            candidateData->state == RecordState::Deleted)
                        {
                            continue;
                        }

                        const auto valueIt =
                            candidateData->values.find(column.name);
                        if (valueIt == candidateData->values.end())
                        {
                            continue;
                        }

                        RecordId referencedId = 0;
                        if (Succeeded(
                                valueIt->second.AsRecordId(&referencedId)) &&
                            referencedId == recordId)
                        {
                            return true;
                        }
                    }
                }
            }

            return false;
        }

        void SqliteDatabase::RecordCreate(
            SqliteTable* table, const std::shared_ptr<SqliteRecordData>& data)
        {
            RecordJournal(table->Name(), data->id, L"", SCValue::Null(),
                          SCValue::Null(), true, false,
                          JournalOp::CreateRecord);
            MarkReferenceIndexDirty();
        }

        ErrorCode SqliteDatabase::PersistAddedColumn(SqliteSchema* schema,
                                                     const SCColumnDef& def)
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

            try
            {
                SqliteTxn txn(db_);
                SqliteStmt stmt = db_.Prepare(
                    "INSERT INTO schema_columns("
                    " table_id, column_name, display_name, value_kind, "
                    "column_kind, nullable_flag, editable_flag,"
                    " user_defined_flag, indexed_flag, "
                    "participates_in_calc_flag, unit, reference_table,"
                    " default_kind, default_int64, default_double, "
                    "default_bool, default_text)"
                    " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                    "?);");
                stmt.BindInt64(1, schema->TableRowId());
                stmt.BindText(2, def.name);
                stmt.BindText(3, def.displayName);
                stmt.BindInt(4, ToSqliteValueKind(def.valueKind));
                stmt.BindInt(5, ToSqliteColumnKind(def.columnKind));
                stmt.BindInt(6, def.nullable ? 1 : 0);
                stmt.BindInt(7, def.editable ? 1 : 0);
                stmt.BindInt(8, def.userDefined ? 1 : 0);
                stmt.BindInt(9, def.indexed ? 1 : 0);
                stmt.BindInt(10, def.participatesInCalc ? 1 : 0);
                stmt.BindText(11, def.unit);
                stmt.BindText(12, def.referenceTable);
                BindValueForStorage(stmt, 13, 14, 15, 16, 17, def.defaultValue);
                const ErrorCode rc = stmt.Step();
                if (Failed(rc))
                {
                    return rc;
                }
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    return commitRc;
                }
            } catch (...)
            {
                return SC_E_FAIL;
            }

            schema->LoadColumn(def);
            if (def.indexed)
            {
                EnsureColumnIndex(schema->TableRowId(), def.name);
            }
            MarkReferenceIndexDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistUpdatedColumn(SqliteSchema* schema,
                                                       const SCColumnDef& def)
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

            try
            {
                SqliteTxn txn(db_);
                SqliteStmt stmt = db_.Prepare(
                    "UPDATE schema_columns SET "
                    " display_name = ?, value_kind = ?, column_kind = ?, "
                    "nullable_flag = ?, editable_flag = ?, user_defined_flag = ?, "
                    "indexed_flag = ?, participates_in_calc_flag = ?, unit = ?, "
                    "reference_table = ?, default_kind = ?, default_int64 = ?, "
                    "default_double = ?, default_bool = ?, default_text = ? "
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
                BindValueForStorage(stmt, 11, 12, 13, 14, 15, def.defaultValue);
                stmt.BindInt64(16, schema->TableRowId());
                stmt.BindText(17, def.name);
                const ErrorCode rc = stmt.Step();
                if (Failed(rc))
                {
                    return rc;
                }
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    return commitRc;
                }
            } catch (...)
            {
                return SC_E_FAIL;
            }

            schema->ReplaceColumn(def);

            const std::wstring indexName =
                L"idx_fv_" + std::to_wstring(schema->TableRowId()) + L"_" +
                SanitizeIdentifier(def.name);
            if (def.indexed)
            {
                EnsureColumnIndex(schema->TableRowId(), def.name);
            } else
            {
                db_.Execute((std::string("DROP INDEX IF EXISTS ") +
                             ToUtf8(indexName) + ";")
                                .c_str());
            }

            MarkReferenceIndexDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::ClearColumnValues(ISCTable* table,
                                                    const wchar_t* name)
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
                RecordJournal(sqliteTable->Name(), recordId, name, oldValue,
                              SCValue::Null(), false, false,
                              relationColumn ? JournalOp::SetRelation
                                             : JournalOp::SetValue);
            }

            MarkReferenceIndexDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistRemovedColumn(
            SqliteSchema* schema, const wchar_t* columnName)
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

            try
            {
                SqliteTxn txn(db_);
                const std::wstring indexName =
                    L"idx_fv_" + std::to_wstring(schema->TableRowId()) + L"_" +
                    SanitizeIdentifier(columnName);
                const std::string dropIndexSql =
                    "DROP INDEX IF EXISTS " + ToUtf8(indexName) + ";";
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
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    return commitRc;
                }
            } catch (...)
            {
                return SC_E_FAIL;
            }

            schema->UnloadColumn(columnName);
            MarkReferenceIndexDirty();
            return SC_OK;
        }

        void SqliteDatabase::RecordJournal(const std::wstring& tableName,
                                           RecordId recordId,
                                           const std::wstring& fieldName,
                                           const SCValue& oldValue,
                                           const SCValue& newValue,
                                           bool oldDeleted, bool newDeleted,
                                           JournalOp op)
        {
            for (auto& entry : activeJournal_.entries)
            {
                if (entry.op == op && entry.tableName == tableName &&
                    entry.recordId == recordId && entry.fieldName == fieldName)
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

        void SqliteDatabase::ApplyJournalReverse(const JournalTransaction& tx)
        {
            for (auto it = tx.entries.rbegin(); it != tx.entries.rend(); ++it)
            {
                ApplyEntry(*it, true);
            }
        }

        void SqliteDatabase::ApplyJournalForward(const JournalTransaction& tx)
        {
            for (const auto& entry : tx.entries)
            {
                ApplyEntry(entry, false);
            }
        }

        void SqliteDatabase::ApplyEntry(const JournalEntry& entry, bool reverse)
        {
            const auto tableIt = tables_.find(entry.tableName);
            if (tableIt == tables_.end())
            {
                return;
            }

            auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
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
                    data->state = reverse
                                      ? (entry.oldDeleted ? RecordState::Deleted
                                                          : RecordState::Alive)
                                      : (entry.newDeleted ? RecordState::Deleted
                                                          : RecordState::Alive);
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
            }
        }

        void SqliteDatabase::UpdateTouchedVersions(const JournalTransaction& tx,
                                                   VersionId version)
        {
            for (const auto& [tableName, recordId] : GetTouchedRecordKeys(tx))
            {
                const auto tableIt = tables_.find(tableName);
                if (tableIt == tables_.end())
                {
                    continue;
                }
                auto record = static_cast<SqliteTable*>(tableIt->second.Get())
                                  ->FindRecordData(recordId);
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
                change.oldValue = (source == ChangeSource::Undo)
                                      ? entry.newValue
                                      : entry.oldValue;
                change.newValue = (source == ChangeSource::Undo)
                                      ? entry.oldValue
                                      : entry.newValue;
                change.structuralChange =
                    (entry.op == JournalOp::CreateRecord ||
                     entry.op == JournalOp::DeleteRecord);
                change.relationChange = (entry.op == JournalOp::SetRelation);

                switch (entry.op)
                {
                    case JournalOp::CreateRecord:
                        change.kind = (source == ChangeSource::Undo)
                                          ? ChangeKind::RecordDeleted
                                          : ChangeKind::RecordCreated;
                        break;
                    case JournalOp::DeleteRecord:
                        change.kind = (source == ChangeSource::Undo)
                                          ? ChangeKind::RecordCreated
                                          : ChangeKind::RecordDeleted;
                        break;
                    case JournalOp::SetRelation:
                        change.kind = ChangeKind::RelationUpdated;
                        break;
                    case JournalOp::SetValue:
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
        SqliteDatabase::GetTouchedRecordKeys(const JournalTransaction& tx) const
        {
            std::set<std::pair<std::wstring, RecordId>> unique;
            for (const auto& entry : tx.entries)
            {
                unique.emplace(entry.tableName, entry.recordId);
            }
            return std::vector<std::pair<std::wstring, RecordId>>(
                unique.begin(), unique.end());
        }

        void SqliteDatabase::PersistTouchedRecords(const JournalTransaction& tx)
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
                "value_kind, int64_value, double_value, bool_value, text_value)"
                " VALUES(?, ?, ?, ?, ?, ?, ?, ?);");

            for (const auto& [tableName, recordId] : GetTouchedRecordKeys(tx))
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
                upsertRecord.BindInt64(
                    4, static_cast<std::int64_t>(data->lastModifiedVersion));
                upsertRecord.Step();
                upsertRecord.Reset();

                deleteValues.BindInt64(1, table->TableRowId());
                deleteValues.BindInt64(2, data->id);
                deleteValues.Step();
                deleteValues.Reset();

                if (data->state == RecordState::Deleted)
                {
                    continue;
                }

                for (const auto& [fieldName, SCValue] : data->values)
                {
                    insertValue.BindInt64(1, table->TableRowId());
                    insertValue.BindInt64(2, data->id);
                    insertValue.BindText(3, fieldName);
                    BindValueForStorage(insertValue, 4, 5, 6, 7, 8, SCValue);
                    insertValue.Step();
                    insertValue.Reset();
                }
            }
        }

        std::int64_t SqliteDatabase::InsertJournalTransaction(
            const JournalTransaction& tx, int stackKind, int stackOrder)
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

        void SqliteDatabase::PersistJournalEntries(std::int64_t txId,
                                                   const JournalTransaction& tx)
        {
            SqliteStmt stmt = db_.Prepare(
                "INSERT INTO journal_entries("
                " tx_id, sequence_index, op, table_name, record_id, "
                "field_name, old_kind, old_int64, old_double, old_bool, "
                "old_text,"
                " new_kind, new_int64, new_double, new_bool, new_text, "
                "old_deleted, new_deleted)"
                " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "?);");

            int sequence = 0;
            for (const auto& entry : tx.entries)
            {
                stmt.BindInt64(1, txId);
                stmt.BindInt(2, sequence++);
                stmt.BindInt(3, ToSqliteJournalOp(entry.op));
                stmt.BindText(4, entry.tableName);
                stmt.BindInt64(5, entry.recordId);
                stmt.BindText(6, entry.fieldName);
                BindValueForStorage(stmt, 7, 8, 9, 10, 11, entry.oldValue);
                BindValueForStorage(stmt, 12, 13, 14, 15, 16, entry.newValue);
                stmt.BindInt(17, entry.oldDeleted ? 1 : 0);
                stmt.BindInt(18, entry.newDeleted ? 1 : 0);
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

        void SqliteDatabase::UpdateJournalTransactionStack(std::int64_t txId,
                                                           int stackKind,
                                                           int stackOrder)
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
            SqliteStmt stmt = db_.Prepare(
                "DELETE FROM journal_transactions WHERE tx_id = ?;");
            stmt.BindInt64(1, txId);
            stmt.Step();
        }

    }  // namespace

    ErrorCode CreateFileDatabase(const wchar_t* path,
                                 const SCOpenDatabaseOptions& options,
                                 SCDbPtr& outDatabase)
    {
        if (path == nullptr)
        {
            return SC_E_POINTER;
        }

        try
        {
            outDatabase =
                SCMakeRef<SqliteDatabase>(std::wstring{path}, options);
            EnsureSqliteQueryDispatchRegistered(outDatabase.Get());
            return SC_OK;
        } catch (...)
        {
            outDatabase.Reset();
            return SC_E_FAIL;
        }
    }

}  // namespace StableCore::Storage
