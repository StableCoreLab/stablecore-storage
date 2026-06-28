#pragma once

#include <algorithm>
#include <atomic>
#include <cstring>
#include <cwctype>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <sqlite3.h>

#include <SCCommon/SCCommon.h>

#include "ISCDiagnostics.h"
#include "ISCQuery.h"
#include "SCBatch.h"
#include "SCMigration.h"
#include "SCQuery.h"
#include "SCQuerySqliteIndexAccess.h"
#include "SqliteUpgradeRegistry.h"
#include "SCRefCounted.h"

#if defined(_WIN32)
#include <windows.h>
#endif

namespace StableCore::Storage
{

// === Forward declarations ===
class SqliteSchema;
class SqliteDatabase;
class SqliteTable;
class SqliteEditSession;
class SqliteRecord;

// === Error / utility ===

int ToSqliteForeignKeyAction(SCForeignKeyAction action) noexcept;
SCForeignKeyAction FromSqliteForeignKeyAction(int action) noexcept;
bool IsForeignKeyActionValid(SCForeignKeyAction action) noexcept;
ErrorCode MapSqliteError(int sqliteCode, int sqliteExtendedCode = 0);

// === Upgrade infrastructure ===
struct UpgradeOpenInspection
{
    std::int32_t schemaVersion{0};
    bool hasMetadataTable{false};
    bool hasJournalTransactionsTable{false};
    bool hasJournalEntriesTable{false};
    bool hasJournalSchemaEntriesTable{false};
};

void EnsureSqliteQueryDispatchRegistered(ISCDatabase* db);
ErrorCode BuildRegisteredUpgradePlan(std::int32_t currentVersion,
                                     std::int32_t targetVersion,
                                     SCUpgradePlan* outPlan);
ErrorCode UpgradeOpenedSqliteDatabase(ISCDatabase* database, SCUpgradeResult* outResult);
ErrorCode InspectDatabaseForOpenOrUpgrade(const std::filesystem::path& filePath,
                                          UpgradeOpenInspection* outInspection);
ErrorCode ReadSchemaVersionFromFile(const std::filesystem::path& filePath, std::int32_t* outVersion);
bool IsInspectableFile(const std::filesystem::path& filePath);
bool HasTableRaw(sqlite3* db, const char* tableName);
bool HasTableColumnRaw(sqlite3* db, const char* tableName, const char* columnName);
ErrorCode ReadSchemaVersionRaw(sqlite3* db, std::int32_t* outVersion);

constexpr int kStackUndo = 0;
constexpr int kStackRedo = 1;

// === Value conversion (Storage-specific) ===
// ConvertColumnValue moved to ValueComparator internal class

// === File system helpers (Win32) ===
#if defined(_WIN32)
std::wstring GetBackupTempDirectory(const std::wstring& targetPath);
bool CreateSiblingTempFile(const std::wstring& targetPath, std::wstring* outTempPath);

struct ScopedDeleteFile
{
    explicit ScopedDeleteFile(std::wstring path) : path_(std::move(path)) {}
    ~ScopedDeleteFile()
    {
        if (!path_.empty())
        {
            DeleteFileW(path_.c_str());
        }
    }

    void Release() noexcept { path_.clear(); }
    const std::wstring& Path() const noexcept { return path_; }

private:
    std::wstring path_;
};
#endif

// === Raw SQLite tools ===
ErrorCode GetSingleCount(sqlite3* db, const char* sql, std::size_t* outCount);
ErrorCode UpdateMetadataBaselineVersion(sqlite3* db, VersionId version);
ErrorCode RunSqliteExec(sqlite3* db, const char* sql);
ErrorCode ClearJournalHistoryForBackup(sqlite3* db,
                                       VersionId currentVersion,
                                       std::size_t* removedTransactionCount,
                                       std::size_t* removedEntryCount);
ErrorCode VacuumTargetDatabase(sqlite3* db);
ErrorCode ValidateTargetDatabase(sqlite3* db);
ErrorCode GetFileSizeBytes(const std::wstring& path, std::uint64_t* outSize);

// === SQLite wrapper classes (all inline) ===
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

    ErrorCode BindInt(int index, int value)
    {
        return MapSqliteError(sqlite3_bind_int(stmt_, index, value));
    }
    ErrorCode BindInt64(int index, std::int64_t value)
    {
        return MapSqliteError(sqlite3_bind_int64(stmt_, index, value));
    }
    ErrorCode BindDouble(int index, double value)
    {
        return MapSqliteError(sqlite3_bind_double(stmt_, index, value));
    }
    ErrorCode BindNull(int index)
    {
        return MapSqliteError(sqlite3_bind_null(stmt_, index));
    }

    ErrorCode BindText(int index, const std::wstring& value)
    {
        const std::string utf8 = SCCommon::ToUtf8(value);
        return MapSqliteError(
            sqlite3_bind_text(stmt_, index, utf8.c_str(), static_cast<int>(utf8.size()), SQLITE_TRANSIENT));
    }

    ErrorCode BindBlob(int index, const std::vector<std::uint8_t>& value)
    {
        return MapSqliteError(sqlite3_bind_blob(stmt_,
                                                index,
                                                value.empty() ? nullptr : value.data(),
                                                static_cast<int>(value.size()),
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

    int ColumnInt(int index) const { return sqlite3_column_int(stmt_, index); }
    std::int64_t ColumnInt64(int index) const { return sqlite3_column_int64(stmt_, index); }
    double ColumnDouble(int index) const { return sqlite3_column_double(stmt_, index); }
    bool ColumnBool(int index) const { return sqlite3_column_int(stmt_, index) != 0; }
    bool ColumnIsNull(int index) const { return sqlite3_column_type(stmt_, index) == SQLITE_NULL; }
    std::wstring ColumnText(int index) const
    {
        const char* text = reinterpret_cast<const char*>(sqlite3_column_text(stmt_, index));
        if (text == nullptr)
        {
            return {};
        }
        return SCCommon::FromUtf8(std::string(text));
    }
    std::vector<std::uint8_t> ColumnBlob(int index) const
    {
        const auto* data = static_cast<const std::uint8_t*>(sqlite3_column_blob(stmt_, index));
        const int size = sqlite3_column_bytes(stmt_, index);
        if (data == nullptr || size <= 0) { return {}; }
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
        const std::string utf8 = SCCommon::ToUtf8(path);
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
        if (db_ != nullptr) { sqlite3_close(db_); }
    }

    SqliteDb(const SqliteDb&) = delete;
    SqliteDb& operator=(const SqliteDb&) = delete;
    SqliteDb(SqliteDb&&) = delete;
    SqliteDb& operator=(SqliteDb&&) = delete;

    sqlite3* Raw() const noexcept { return db_; }

    ErrorCode Execute(const char* sql)
    {
        char* error = nullptr;
        const int rc = sqlite3_exec(db_, sql, nullptr, nullptr, &error);
        if (error != nullptr) { sqlite3_free(error); }
        return MapSqliteError(rc);
    }

    SqliteStmt Prepare(const char* sql) { return SqliteStmt(db_, sql); }
    std::int64_t LastInsertRowId() const noexcept { return sqlite3_last_insert_rowid(db_); }

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
        if (!completed_) { db_.Execute("ROLLBACK;"); }
    }

    SqliteTxn(const SqliteTxn&) = delete;
    SqliteTxn& operator=(const SqliteTxn&) = delete;
    SqliteTxn(SqliteTxn&&) = delete;
    SqliteTxn& operator=(SqliteTxn&&) = delete;

    ErrorCode Commit()
    {
        if (completed_) { return SC_OK; }
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
    explicit SqliteSavepoint(SqliteDb& db, const char* baseName) : db_(db)
    {
        const std::uint64_t savepointId = nextId_.fetch_add(1, std::memory_order_relaxed);
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

    SqliteSavepoint(const SqliteSavepoint&) = delete;
    SqliteSavepoint& operator=(const SqliteSavepoint&) = delete;

    ErrorCode Commit()
    {
        if (completed_) { return SC_OK; }
        completed_ = true;
        return db_.Execute(("RELEASE SAVEPOINT " + name_ + ";").c_str());
    }

private:
    inline static std::atomic<std::uint64_t> nextId_{1};
    SqliteDb& db_;
    std::string name_;
    bool completed_{false};
};

// === Core data structures ===
struct SqliteRecordData
{
    explicit SqliteRecordData(RecordId newId) : id(newId) {}

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
    enum class Kind { RenameTable };
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

constexpr std::size_t kCompositeIndexMaxColumns = 3;

// === Constraint expression AST ===
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

// === Composite index / constraint expression utilities ===
bool IsCompositeIndexExplicit(const SCIndexDef& def);
bool IsEqualityIndexOperator(QueryConditionOperator op) noexcept;
bool IsRangeIndexOperator(QueryConditionOperator op) noexcept;
std::wstring BuildQueryIndexStorageKey(std::int64_t tableRowId, const std::wstring& indexName);

// === Journal helpers ===
JournalOp FromSqliteJournalOp(int op) noexcept;

// === Value kind / column kind / record state / schema source conversion ===
inline ValueKind FromSqliteValueKind(int kind) noexcept { return static_cast<ValueKind>(kind); }
inline ColumnKind FromSqliteColumnKind(int kind) noexcept { return static_cast<ColumnKind>(kind); }
inline RecordState FromSqliteRecordState(int state) noexcept { return static_cast<RecordState>(state); }
inline SCConstraintKind FromSqliteConstraintKind(int kind) noexcept { return static_cast<SCConstraintKind>(kind); }
inline SCSchemaSourceKind FromSqliteSchemaSourceKind(int kind) noexcept { return static_cast<SCSchemaSourceKind>(kind); }
inline int ToSqliteValueKind(ValueKind kind) noexcept { return static_cast<int>(kind); }
inline int ToSqliteColumnKind(ColumnKind kind) noexcept { return static_cast<int>(kind); }
inline int ToSqliteRecordState(RecordState state) noexcept { return static_cast<int>(state); }
inline int ToSqliteSchemaSourceKind(SCSchemaSourceKind kind) noexcept { return static_cast<int>(kind); }
inline int ToSqliteConstraintKind(SCConstraintKind kind) noexcept { return static_cast<int>(kind); }
inline int ToSqliteJournalOp(JournalOp op) noexcept { return static_cast<int>(op); }

// === Constraint expression evaluation ===
ErrorCode EvaluateConstraintExpressionNode(const ConstraintExpressionNode& node,
                                           const std::unordered_map<std::wstring, SCValue>& values,
                                           SCValue* outValue);

// === Column def / constraint def / index def validation ===
ErrorCode ValidateColumnDefShape(const SCColumnDef& def);
ErrorCode ValidateConstraintDefShape(const SCConstraintDef& def);
ErrorCode ValidateIndexDefShape(const SCIndexDef& def);
ErrorCode ValidateValueKind(ValueKind expected, const SCValue& value, bool nullable);

// === Foreign key helpers ===
std::wstring ResolveForeignKeyReferencedColumn(const SCConstraintDef& constraint, std::size_t index);
std::wstring MakeForeignKeyReferenceCacheKey(const std::wstring& tableName, const std::wstring& columnName);

// === Column def storage binding ===
void BindColumnDefForStorage(SqliteStmt& stmt,
                             int displayNameIndex, int valueKindIndex, int columnKindIndex,
                             int nullableIndex, int editableIndex, int userDefinedIndex,
                             int indexedIndex, int participatesIndex, int unitIndex,
                             int referenceTableIndex, int referenceStorageColumnIndex,
                             int referenceDisplayColumnIndex, int defaultKindIndex,
                             int defaultInt64Index, int defaultDoubleIndex, int defaultBoolIndex,
                             int defaultTextIndex, int defaultBlobIndex,
                             const SCColumnDef& def);

// ── Import serializer ──────────────────────────────────────────────
class ImportSerializer final {
public:
    ImportSerializer() = delete;
    static std::wstring SerializeValue(const SCValue& value);
    static bool DeserializeValue(const std::wstring& token, SCValue* outValue);
    static std::wstring SerializeConstraintDef(const SCConstraintDef& def);
    static bool DeserializeConstraintDef(const std::wstring& payload, SCConstraintDef* outDef);
    static std::wstring SerializeIndexDef(const SCIndexDef& def);
    static bool DeserializeIndexDef(const std::wstring& payload, SCIndexDef* outDef);
    static std::wstring SerializeConstraintKeyValue(const SCValue& value);
    static std::wstring SerializeImportSessionPayload(const SCImportStagingArea& session);
    static bool DeserializeImportSessionPayload(const std::wstring& payload, SCImportStagingArea* outSession);
    static bool TryParseSerializedConstraintBody(const std::vector<std::wstring>& tokens,
                                                  bool actionsBeforeCheckExpression,
                                                  SCConstraintDef* outDef);
};

// ── Value comparator ───────────────────────────────────────────────
class ValueComparator final {
public:
    ValueComparator() = delete;
    static bool IsNumeric(ValueKind kind) noexcept;
    static bool TryAsDouble(const SCValue& value, double* out) noexcept;
    static bool TryAsText(const SCValue& value, std::wstring* out) noexcept;
    static bool IsTruthy(const SCValue& value) noexcept;
    static ErrorCode Convert(const SCValue& source, ValueKind targetKind, SCValue* outValue);
};

// ── Storage codec ──────────────────────────────────────────────────
class StorageCodec final {
public:
    StorageCodec() = delete;

    // Simplified single-index methods
    static ErrorCode BindValue(SqliteStmt& stmt, int columnIndex, ValueKind valueKind, const SCValue& value);
    static ErrorCode ReadValue(SqliteStmt& stmt, int columnIndex, ValueKind valueKind, SCValue* outValue);
    static ErrorCode ReadColumnDef(SqliteStmt& stmt, int columnIndex, SCColumnDef* outDef);

    // Multi-index overloads (original per-column-index style)
    static void BindValue(SqliteStmt& stmt,
                          int kindIndex, int intIndex, int doubleIndex,
                          int boolIndex, int textIndex, int blobIndex,
                          const SCValue& value);
    static SCValue ReadValue(SqliteStmt& stmt,
                             int kindIndex, int intIndex, int doubleIndex,
                             int boolIndex, int textIndex, int blobIndex);
    static SCColumnDef ReadColumnDef(SqliteStmt& stmt,
                                     int displayNameIndex, int valueKindIndex, int columnKindIndex,
                                     int nullableIndex, int editableIndex, int userDefinedIndex,
                                     int indexedIndex, int participatesInCalcIndex, int unitIndex,
                                     int referenceTableIndex, int referenceStorageColumnIndex,
                                     int referenceDisplayColumnIndex, int defaultKindIndex,
                                     int defaultInt64Index, int defaultDoubleIndex,
                                     int defaultBoolIndex, int defaultTextIndex,
                                     int defaultBlobIndex);
};

// === Constraint expression parser ===
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
                    return {TokenKind::NotEqual, L"<>"};
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
        const std::wstring upper = SCCommon::ToUpper(text);
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
                                     return SCCommon::EqualsIgnoreCase(candidate, identifier);
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

// === SqliteSchema class ===
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
        : db_(db), tableName_(std::move(tableName)), tableRowId_(tableRowId) {}

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

    void SetTableName(std::wstring tableName) { tableName_ = std::move(tableName); }
    void LoadTableDescription(const std::wstring& description) { description_ = description; }

    void LoadConstraint(const SCConstraintDef& def) { LoadConstraint(def, nextConstraintRowId_++); }
    void LoadConstraint(const SCConstraintDef& def, std::int64_t rowId)
    {
        const std::int64_t normalizedRowId = rowId > 0 ? rowId : nextConstraintRowId_++;
        if (normalizedRowId >= nextConstraintRowId_) { nextConstraintRowId_ = normalizedRowId + 1; }
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
        if (vecIt != constraints_.end()) { vecIt->def = def; }
        const std::int64_t rowId = vecIt != constraints_.end() ? vecIt->rowId : -1;
        constraintsByName_[def.name] = SchemaConstraintEntry{rowId, def};
        if (rowId >= 0) { constraintRowIdsByName_[def.name] = rowId; }
    }

    void UnloadConstraint(const wchar_t* name)
    {
        if (name == nullptr) { return; }
        constraintsByName_.erase(name);
        constraintRowIdsByName_.erase(name);
        const auto vecIt = std::find_if(
            constraints_.begin(), constraints_.end(), [name](const SchemaConstraintEntry& def) {
                return def.def.name == name;
            });
        if (vecIt != constraints_.end()) { constraints_.erase(vecIt); }
    }

    void LoadIndex(const SCIndexDef& def) { LoadIndex(def, nextIndexRowId_++); }
    void LoadIndex(const SCIndexDef& def, std::int64_t rowId)
    {
        const std::int64_t normalizedRowId = rowId > 0 ? rowId : nextIndexRowId_++;
        if (normalizedRowId >= nextIndexRowId_) { nextIndexRowId_ = normalizedRowId + 1; }
        const auto insertIt = std::find_if(
            indexes_.begin(), indexes_.end(), [normalizedRowId](const SchemaIndexEntry& existing) {
                return existing.rowId > normalizedRowId;
            });
        indexes_.insert(insertIt, SchemaIndexEntry{normalizedRowId, def});
        indexesByName_[def.name] = SchemaIndexEntry{normalizedRowId, def};
        indexRowIdsByName_[def.name] = normalizedRowId;
    }

    void ReplaceIndex(const SCIndexDef& def)
    {
        const auto vecIt = std::find_if(
            indexes_.begin(), indexes_.end(), [&def](const SchemaIndexEntry& existing) {
                return existing.def.name == def.name;
            });
        if (vecIt != indexes_.end()) { vecIt->def = def; }
        const std::int64_t rowId = vecIt != indexes_.end() ? vecIt->rowId : -1;
        indexesByName_[def.name] = SchemaIndexEntry{rowId, def};
        if (rowId >= 0) { indexRowIdsByName_[def.name] = rowId; }
    }

    void UnloadIndex(const wchar_t* name)
    {
        if (name == nullptr) { return; }
        indexesByName_.erase(name);
        indexRowIdsByName_.erase(name);
        const auto vecIt = std::find_if(
            indexes_.begin(), indexes_.end(), [name](const SchemaIndexEntry& def) {
                return def.def.name == name;
            });
        if (vecIt != indexes_.end()) { indexes_.erase(vecIt); }
    }

    std::wstring LegacyPrimaryKeyName() const
    {
        return L"pk_" + SCCommon::SanitizeFileName(tableName_);
    }

    std::wstring LegacyIndexName(const std::wstring& columnName) const
    {
        return L"idx_" + SCCommon::SanitizeFileName(tableName_) + L"_" + SCCommon::SanitizeFileName(columnName);
    }

    void SetLegacyIndexState(const std::wstring& columnName, bool indexed)
    {
        const std::wstring indexName = LegacyIndexName(columnName);
        const auto indexIt = std::find_if(
            indexes_.begin(), indexes_.end(), [&indexName](const SchemaIndexEntry& index) {
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
        }
        else
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
                       SCCommon::EqualsIgnoreCase(constraint.def.columns.front(), columnName);
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
        if (name == nullptr) { return -1; }
        const auto it = columnRowIdsByName_.find(name);
        return it == columnRowIdsByName_.end() ? -1 : it->second;
    }

    std::int64_t FindConstraintRowId(const wchar_t* name) const noexcept
    {
        if (name == nullptr) { return -1; }
        const auto it = constraintRowIdsByName_.find(name);
        return it == constraintRowIdsByName_.end() ? -1 : it->second;
    }

    std::int64_t FindIndexRowId(const wchar_t* name) const noexcept
    {
        if (name == nullptr) { return -1; }
        const auto it = indexRowIdsByName_.find(name);
        return it == indexRowIdsByName_.end() ? -1 : it->second;
    }

    void LoadColumn(const SCColumnDef& def) { LoadColumn(def, nextColumnRowId_++); }
    void LoadColumn(const SCColumnDef& def, std::int64_t rowId)
    {
        const std::int64_t normalizedRowId = rowId > 0 ? rowId : nextColumnRowId_++;
        if (normalizedRowId >= nextColumnRowId_) { nextColumnRowId_ = normalizedRowId + 1; }
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
        const auto vecIt = std::find_if(
            columns_.begin(), columns_.end(), [&def](const SchemaColumnEntry& existing) {
                return existing.def.name == def.name;
            });
        if (vecIt != columns_.end()) { vecIt->def = def; }
        const std::int64_t rowId = vecIt != columns_.end() ? vecIt->rowId : -1;
        columnsByName_[def.name] = SchemaColumnEntry{rowId, def};
        if (rowId >= 0) { columnRowIdsByName_[def.name] = rowId; }
    }

    void UnloadColumn(const wchar_t* name)
    {
        if (name == nullptr) { return; }
        const auto mapIt = columnsByName_.find(name);
        if (mapIt != columnsByName_.end()) { columnsByName_.erase(mapIt); }
        const auto rowIdIt = columnRowIdsByName_.find(name);
        if (rowIdIt != columnRowIdsByName_.end()) { columnRowIdsByName_.erase(rowIdIt); }
        const auto vecIt = std::find_if(
            columns_.begin(), columns_.end(), [name](const SchemaColumnEntry& def) {
                return def.def.name == name;
            });
        if (vecIt != columns_.end()) { columns_.erase(vecIt); }
    }

    std::int64_t TableRowId() const noexcept { return tableRowId_; }

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

} // namespace StableCore::Storage
