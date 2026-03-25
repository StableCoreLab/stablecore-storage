#include "StableCore/Storage/Factory.h"

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <sqlite3.h>

#include "StableCore/Storage/RefCounted.h"

#if defined(_WIN32)
#include <windows.h>
#endif

namespace stablecore::storage
{
namespace
{

constexpr int kStackUndo = 0;
constexpr int kStackRedo = 1;

std::string ToUtf8(const std::wstring& text)
{
#if defined(_WIN32)
    if (text.empty())
    {
        return {};
    }

    const int bytes = WideCharToMultiByte(CP_UTF8, 0, text.c_str(), static_cast<int>(text.size()), nullptr, 0, nullptr, nullptr);
    std::string utf8(static_cast<std::size_t>(bytes), '\0');
    WideCharToMultiByte(CP_UTF8, 0, text.c_str(), static_cast<int>(text.size()), utf8.data(), bytes, nullptr, nullptr);
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

    SqliteStmt(SqliteStmt&& other) noexcept
        : stmt_(other.stmt_)
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

    ErrorCode BindInt(int index, int value) { return MapSqliteError(sqlite3_bind_int(stmt_, index, value)); }
    ErrorCode BindInt64(int index, std::int64_t value) { return MapSqliteError(sqlite3_bind_int64(stmt_, index, value)); }
    ErrorCode BindDouble(int index, double value) { return MapSqliteError(sqlite3_bind_double(stmt_, index, value)); }
    ErrorCode BindNull(int index) { return MapSqliteError(sqlite3_bind_null(stmt_, index)); }

    ErrorCode BindText(int index, const std::wstring& value)
    {
        const std::string utf8 = ToUtf8(value);
        return MapSqliteError(sqlite3_bind_text(stmt_, index, utf8.c_str(), static_cast<int>(utf8.size()), SQLITE_TRANSIENT));
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
    std::wstring ColumnText(int index) const { return FromUtf8(reinterpret_cast<const char*>(sqlite3_column_text(stmt_, index))); }

private:
    sqlite3_stmt* stmt_{nullptr};
};

class SqliteDb
{
public:
    explicit SqliteDb(const std::wstring& path)
    {
        const std::string utf8 = ToUtf8(path);
        const int rc = sqlite3_open_v2(
            utf8.c_str(),
            &db_,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
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

    sqlite3* Raw() const noexcept { return db_; }

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
    explicit SqliteTxn(SqliteDb& db)
        : db_(db)
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

struct SqliteRecordData
{
    explicit SqliteRecordData(RecordId newId)
        : id(newId)
    {
    }

    RecordId id{0};
    RecordState state{RecordState::Alive};
    VersionId lastModifiedVersion{0};
    std::unordered_map<std::wstring, Value> values;
};

struct SqlitePersistedJournalTransaction
{
    std::int64_t txId{0};
    JournalTransaction tx;
};

class SqliteDatabase;
class SqliteTable;

ErrorCode ValidateValueKind(ValueKind expected, const Value& value, bool nullable)
{
    if (value.IsNull())
    {
        return nullable ? SC_OK : SC_E_SCHEMA_VIOLATION;
    }
    return value.GetKind() == expected ? SC_OK : SC_E_TYPE_MISMATCH;
}

ErrorCode ValidateColumnDef(const ColumnDef& def)
{
    if (def.name.empty())
    {
        return SC_E_INVALIDARG;
    }
    if (def.columnKind == ColumnKind::Relation && def.valueKind != ValueKind::RecordId)
    {
        return SC_E_SCHEMA_VIOLATION;
    }
    if (!def.defaultValue.IsNull() && def.defaultValue.GetKind() != def.valueKind)
    {
        return SC_E_SCHEMA_VIOLATION;
    }
    if (!def.nullable && def.defaultValue.IsNull())
    {
        return SC_E_SCHEMA_VIOLATION;
    }
    return SC_OK;
}

int ToSqliteValueKind(ValueKind kind) noexcept { return static_cast<int>(kind); }
ValueKind FromSqliteValueKind(int kind) noexcept { return static_cast<ValueKind>(kind); }
int ToSqliteColumnKind(ColumnKind kind) noexcept { return static_cast<int>(kind); }
ColumnKind FromSqliteColumnKind(int kind) noexcept { return static_cast<ColumnKind>(kind); }
int ToSqliteRecordState(RecordState state) noexcept { return static_cast<int>(state); }
RecordState FromSqliteRecordState(int state) noexcept { return static_cast<RecordState>(state); }
int ToSqliteJournalOp(JournalOp op) noexcept { return static_cast<int>(op); }
JournalOp FromSqliteJournalOp(int op) noexcept { return static_cast<JournalOp>(op); }

void BindValueForStorage(SqliteStmt& stmt, int kindIndex, int intIndex, int doubleIndex, int boolIndex, int textIndex, const Value& value)
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
    case ValueKind::Int64:
    {
        std::int64_t v = 0;
        value.AsInt64(&v);
        stmt.BindInt64(intIndex, v);
        stmt.BindNull(doubleIndex);
        stmt.BindNull(boolIndex);
        stmt.BindNull(textIndex);
        break;
    }
    case ValueKind::Double:
    {
        double v = 0.0;
        value.AsDouble(&v);
        stmt.BindNull(intIndex);
        stmt.BindDouble(doubleIndex, v);
        stmt.BindNull(boolIndex);
        stmt.BindNull(textIndex);
        break;
    }
    case ValueKind::Bool:
    {
        bool v = false;
        value.AsBool(&v);
        stmt.BindNull(intIndex);
        stmt.BindNull(doubleIndex);
        stmt.BindInt(boolIndex, v ? 1 : 0);
        stmt.BindNull(textIndex);
        break;
    }
    case ValueKind::String:
    {
        std::wstring text;
        value.AsStringCopy(&text);
        stmt.BindNull(intIndex);
        stmt.BindNull(doubleIndex);
        stmt.BindNull(boolIndex);
        stmt.BindText(textIndex, text);
        break;
    }
    case ValueKind::RecordId:
    {
        RecordId id = 0;
        value.AsRecordId(&id);
        stmt.BindInt64(intIndex, id);
        stmt.BindNull(doubleIndex);
        stmt.BindNull(boolIndex);
        stmt.BindNull(textIndex);
        break;
    }
    case ValueKind::Enum:
    {
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

Value ReadValueFromStorage(const SqliteStmt& stmt, int kindIndex, int intIndex, int doubleIndex, int boolIndex, int textIndex)
{
    switch (FromSqliteValueKind(stmt.ColumnInt(kindIndex)))
    {
    case ValueKind::Null: return Value::Null();
    case ValueKind::Int64: return Value::FromInt64(stmt.ColumnInt64(intIndex));
    case ValueKind::Double: return Value::FromDouble(stmt.ColumnDouble(doubleIndex));
    case ValueKind::Bool: return Value::FromBool(stmt.ColumnBool(boolIndex));
    case ValueKind::String: return Value::FromString(stmt.ColumnText(textIndex));
    case ValueKind::RecordId: return Value::FromRecordId(stmt.ColumnInt64(intIndex));
    case ValueKind::Enum: return Value::FromEnum(stmt.ColumnText(textIndex));
    default: return Value::Null();
    }
}

class SqliteSchema final : public ISchema, public RefCountedObject
{
public:
    SqliteSchema(SqliteDatabase* db, std::wstring tableName, std::int64_t tableRowId)
        : db_(db)
        , tableName_(std::move(tableName))
        , tableRowId_(tableRowId)
    {
    }

    ErrorCode GetColumnCount(std::int32_t* outCount) override;
    ErrorCode GetColumn(std::int32_t index, ColumnDef* outDef) override;
    ErrorCode FindColumn(const wchar_t* name, ColumnDef* outDef) override;
    ErrorCode AddColumn(const ColumnDef& def) override;

    const ColumnDef* FindColumnDef(const std::wstring& name) const noexcept
    {
        const auto it = columnsByName_.find(name);
        return it == columnsByName_.end() ? nullptr : &it->second;
    }

    void LoadColumn(const ColumnDef& def)
    {
        columns_.push_back(def);
        columnsByName_[def.name] = def;
    }

    std::int64_t TableRowId() const noexcept
    {
        return tableRowId_;
    }

private:
    SqliteDatabase* db_{nullptr};
    std::wstring tableName_;
    std::int64_t tableRowId_{0};
    std::vector<ColumnDef> columns_;
    std::unordered_map<std::wstring, ColumnDef> columnsByName_;
};

class SqliteEditSession final : public IEditSession, public RefCountedObject
{
public:
    SqliteEditSession(std::wstring name, VersionId openedVersion)
        : name_(std::move(name))
        , openedVersion_(openedVersion)
    {
    }

    const wchar_t* GetName() const override { return name_.c_str(); }
    EditState GetState() const noexcept override { return state_; }
    VersionId GetOpenedVersion() const noexcept override { return openedVersion_; }

    void SetState(EditState state) noexcept
    {
        state_ = state;
    }

private:
    std::wstring name_;
    VersionId openedVersion_{0};
    EditState state_{EditState::Active};
};

class SqliteRecord final : public IRecord, public RefCountedObject
{
public:
    SqliteRecord(SqliteDatabase* db, SqliteTable* table, std::shared_ptr<SqliteRecordData> data)
        : db_(db)
        , table_(table)
        , data_(std::move(data))
    {
    }

    RecordId GetId() const noexcept override;
    bool IsDeleted() const noexcept override;
    VersionId GetLastModifiedVersion() const noexcept override;

    ErrorCode GetValue(const wchar_t* name, Value* outValue) override;
    ErrorCode SetValue(const wchar_t* name, const Value& value) override;

    ErrorCode GetInt64(const wchar_t* name, std::int64_t* outValue) override;
    ErrorCode SetInt64(const wchar_t* name, std::int64_t value) override;

    ErrorCode GetDouble(const wchar_t* name, double* outValue) override;
    ErrorCode SetDouble(const wchar_t* name, double value) override;

    ErrorCode GetBool(const wchar_t* name, bool* outValue) override;
    ErrorCode SetBool(const wchar_t* name, bool value) override;

    ErrorCode GetString(const wchar_t* name, const wchar_t** outValue) override;
    ErrorCode GetStringCopy(const wchar_t* name, std::wstring* outValue) override;
    ErrorCode SetString(const wchar_t* name, const wchar_t* value) override;

    ErrorCode GetRef(const wchar_t* name, RecordId* outValue) override;
    ErrorCode SetRef(const wchar_t* name, RecordId value) override;

private:
    ErrorCode ReadTypedValue(const wchar_t* name, Value* outValue);

    SqliteDatabase* db_{nullptr};
    SqliteTable* table_{nullptr};
    std::shared_ptr<SqliteRecordData> data_;
};

class SqliteRecordCursor final : public IRecordCursor, public RefCountedObject
{
public:
    explicit SqliteRecordCursor(std::vector<RecordPtr> records)
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

    ErrorCode GetCurrent(RecordPtr& outRecord) override
    {
        if (!current_)
        {
            return SC_FALSE_RESULT;
        }
        outRecord = current_;
        return SC_OK;
    }

private:
    std::vector<RecordPtr> records_;
    std::size_t index_{0};
    RecordPtr current_;
};

class SqliteTable final : public ITable, public RefCountedObject
{
public:
    SqliteTable(SqliteDatabase* db, std::wstring name, std::int64_t tableRowId)
        : db_(db)
        , name_(std::move(name))
        , schema_(MakeRef<SqliteSchema>(db, name_, tableRowId))
    {
    }

    ErrorCode GetRecord(RecordId id, RecordPtr& outRecord) override;
    ErrorCode CreateRecord(RecordPtr& outRecord) override;
    ErrorCode DeleteRecord(RecordId id) override;
    ErrorCode GetSchema(SchemaPtr& outSchema) override
    {
        outSchema = schema_;
        return SC_OK;
    }
    ErrorCode EnumerateRecords(RecordCursorPtr& outCursor) override;
    ErrorCode FindRecords(const QueryCondition& condition, RecordCursorPtr& outCursor) override;

    const std::wstring& Name() const noexcept { return name_; }
    SqliteSchema* Schema() const noexcept { return schema_.Get(); }
    std::int64_t TableRowId() const noexcept { return schema_->TableRowId(); }

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
    RecordPtr MakeRecord(const std::shared_ptr<SqliteRecordData>& data)
    {
        return MakeRef<SqliteRecord>(db_, this, data);
    }

    SqliteDatabase* db_{nullptr};
    std::wstring name_;
    RefPtr<SqliteSchema> schema_;
    std::unordered_map<RecordId, std::shared_ptr<SqliteRecordData>> records_;
};

class SqliteDatabase final : public IDatabase, public RefCountedObject
{
public:
    explicit SqliteDatabase(const std::wstring& path);

    ErrorCode BeginEdit(const wchar_t* name, EditPtr& outEdit) override;
    ErrorCode Commit(IEditSession* edit) override;
    ErrorCode Rollback(IEditSession* edit) override;
    ErrorCode Undo() override;
    ErrorCode Redo() override;
    ErrorCode GetTable(const wchar_t* name, TablePtr& outTable) override;
    ErrorCode CreateTable(const wchar_t* name, TablePtr& outTable) override;
    ErrorCode AddObserver(IDatabaseObserver* observer) override;
    ErrorCode RemoveObserver(IDatabaseObserver* observer) override;
    VersionId GetCurrentVersion() const noexcept override { return version_; }

    bool HasActiveEdit() const noexcept { return static_cast<bool>(activeEdit_); }
    RecordId AllocateRecordId() noexcept { return nextRecordId_++; }

    ErrorCode WriteValue(SqliteTable* table, const std::shared_ptr<SqliteRecordData>& data, const std::wstring& fieldName, const Value& value);
    ErrorCode DeleteRecord(SqliteTable* table, const std::shared_ptr<SqliteRecordData>& data);
    void RecordCreate(SqliteTable* table, const std::shared_ptr<SqliteRecordData>& data);
    ErrorCode PersistAddedColumn(SqliteSchema* schema, const ColumnDef& def);

private:
    struct JournalLookup
    {
        bool createdInActiveEdit{false};
        bool deletedInActiveEdit{false};
    };

    void InitializeSchema();
    void LoadMetadata();
    void SaveMetadata();
    void LoadTables();
    void LoadJournalStacks();
    ErrorCode ValidateActiveEdit(IEditSession* edit) const;
    ErrorCode ValidateWrite(SqliteTable* table, const std::shared_ptr<SqliteRecordData>& data, const std::wstring& fieldName, const Value& value);
    JournalLookup LookupRecordJournalState(const std::wstring& tableName, RecordId recordId) const;
    void RemoveFieldJournalEntries(const std::wstring& tableName, RecordId recordId);
    void RemoveAllJournalEntriesForRecord(const std::wstring& tableName, RecordId recordId);
    void RecordJournal(const std::wstring& tableName, RecordId recordId, const std::wstring& fieldName, const Value& oldValue, const Value& newValue, bool oldDeleted, bool newDeleted, JournalOp op);
    void ApplyJournalReverse(const JournalTransaction& tx);
    void ApplyJournalForward(const JournalTransaction& tx);
    void ApplyEntry(const JournalEntry& entry, bool reverse);
    void UpdateTouchedVersions(const JournalTransaction& tx, VersionId version);
    ChangeSet BuildChangeSet(const JournalTransaction& tx, ChangeSource source, VersionId version) const;
    void NotifyObservers(const ChangeSet& changeSet);
    std::vector<std::pair<std::wstring, RecordId>> GetTouchedRecordKeys(const JournalTransaction& tx) const;
    void PersistTouchedRecords(const JournalTransaction& tx);
    std::int64_t InsertJournalTransaction(const JournalTransaction& tx, int stackKind, int stackOrder);
    void PersistJournalEntries(std::int64_t txId, const JournalTransaction& tx);
    void DeleteRedoJournalRows();
    void UpdateJournalTransactionStack(std::int64_t txId, int stackKind, int stackOrder);
    void DeleteJournalTransaction(std::int64_t txId);

    std::wstring path_;
    SqliteDb db_;
    VersionId version_{0};
    RecordId nextRecordId_{1};
    std::map<std::wstring, TablePtr> tables_;
    std::vector<IDatabaseObserver*> observers_;
    RefPtr<SqliteEditSession> activeEdit_;
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

ErrorCode SqliteSchema::GetColumn(std::int32_t index, ColumnDef* outDef)
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

ErrorCode SqliteSchema::FindColumn(const wchar_t* name, ColumnDef* outDef)
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

ErrorCode SqliteSchema::AddColumn(const ColumnDef& def)
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

ErrorCode SqliteRecord::ReadTypedValue(const wchar_t* name, Value* outValue)
{
    return GetValue(name, outValue);
}

ErrorCode SqliteRecord::GetValue(const wchar_t* name, Value* outValue)
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

    const ColumnDef* column = table_->Schema()->FindColumnDef(name);
    if (column == nullptr)
    {
        return SC_E_COLUMN_NOT_FOUND;
    }

    const auto it = data_->values.find(name);
    *outValue = (it != data_->values.end()) ? it->second : column->defaultValue;
    return outValue->IsNull() ? SC_E_VALUE_IS_NULL : SC_OK;
}

ErrorCode SqliteRecord::SetValue(const wchar_t* name, const Value& value)
{
    if (name == nullptr)
    {
        return SC_E_INVALIDARG;
    }
    return db_->WriteValue(table_, data_, name, value);
}

ErrorCode SqliteRecord::GetInt64(const wchar_t* name, std::int64_t* outValue)
{
    Value value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsInt64(outValue);
}

ErrorCode SqliteRecord::SetInt64(const wchar_t* name, std::int64_t value)
{
    return SetValue(name, Value::FromInt64(value));
}

ErrorCode SqliteRecord::GetDouble(const wchar_t* name, double* outValue)
{
    Value value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsDouble(outValue);
}

ErrorCode SqliteRecord::SetDouble(const wchar_t* name, double value)
{
    return SetValue(name, Value::FromDouble(value));
}

ErrorCode SqliteRecord::GetBool(const wchar_t* name, bool* outValue)
{
    Value value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsBool(outValue);
}

ErrorCode SqliteRecord::SetBool(const wchar_t* name, bool value)
{
    return SetValue(name, Value::FromBool(value));
}

ErrorCode SqliteRecord::GetString(const wchar_t* name, const wchar_t** outValue)
{
    Value value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsString(outValue);
}

ErrorCode SqliteRecord::GetStringCopy(const wchar_t* name, std::wstring* outValue)
{
    Value value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsStringCopy(outValue);
}

ErrorCode SqliteRecord::SetString(const wchar_t* name, const wchar_t* value)
{
    return SetValue(name, value == nullptr ? Value::Null() : Value::FromString(value));
}

ErrorCode SqliteRecord::GetRef(const wchar_t* name, RecordId* outValue)
{
    Value value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsRecordId(outValue);
}

ErrorCode SqliteRecord::SetRef(const wchar_t* name, RecordId value)
{
    return SetValue(name, Value::FromRecordId(value));
}

ErrorCode SqliteTable::GetRecord(RecordId id, RecordPtr& outRecord)
{
    auto data = FindRecordData(id);
    if (!data)
    {
        return SC_E_RECORD_NOT_FOUND;
    }
    outRecord = MakeRecord(data);
    return SC_OK;
}

ErrorCode SqliteTable::CreateRecord(RecordPtr& outRecord)
{
    if (!db_->HasActiveEdit())
    {
        return SC_E_NO_ACTIVE_EDIT;
    }

    auto data = std::make_shared<SqliteRecordData>(db_->AllocateRecordId());
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

ErrorCode SqliteTable::EnumerateRecords(RecordCursorPtr& outCursor)
{
    std::vector<RecordPtr> records;
    for (const auto& [_, data] : records_)
    {
        if (data->state == RecordState::Alive)
        {
            records.push_back(MakeRecord(data));
        }
    }
    outCursor = MakeRef<SqliteRecordCursor>(std::move(records));
    return SC_OK;
}

ErrorCode SqliteTable::FindRecords(const QueryCondition& condition, RecordCursorPtr& outCursor)
{
    const ColumnDef* column = Schema()->FindColumnDef(condition.fieldName);
    if (column == nullptr)
    {
        return SC_E_COLUMN_NOT_FOUND;
    }

    std::vector<RecordPtr> matched;
    for (const auto& [_, data] : records_)
    {
        if (data->state == RecordState::Deleted)
        {
            continue;
        }

        Value actual = column->defaultValue;
        const auto it = data->values.find(condition.fieldName);
        if (it != data->values.end())
        {
            actual = it->second;
        }
        if (actual == condition.expectedValue)
        {
            matched.push_back(MakeRecord(data));
        }
    }

    outCursor = MakeRef<SqliteRecordCursor>(std::move(matched));
    return SC_OK;
}

SqliteDatabase::SqliteDatabase(const std::wstring& path)
    : path_(path)
    , db_(path)
{
    InitializeSchema();
    LoadMetadata();
    LoadTables();
    LoadJournalStacks();
}

void SqliteDatabase::InitializeSchema()
{
    db_.Execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);");
    db_.Execute("CREATE TABLE IF NOT EXISTS tables (table_id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE);");
    db_.Execute(
        "CREATE TABLE IF NOT EXISTS schema_columns ("
        "table_id INTEGER NOT NULL, column_name TEXT NOT NULL, display_name TEXT NOT NULL, value_kind INTEGER NOT NULL,"
        "column_kind INTEGER NOT NULL, nullable_flag INTEGER NOT NULL, editable_flag INTEGER NOT NULL,"
        "user_defined_flag INTEGER NOT NULL, indexed_flag INTEGER NOT NULL, participates_in_calc_flag INTEGER NOT NULL,"
        "unit TEXT NOT NULL, reference_table TEXT NOT NULL, default_kind INTEGER NOT NULL, default_int64 INTEGER,"
        "default_double REAL, default_bool INTEGER, default_text TEXT, PRIMARY KEY(table_id, column_name));");
    db_.Execute(
        "CREATE TABLE IF NOT EXISTS records ("
        "table_id INTEGER NOT NULL, record_id INTEGER NOT NULL, state INTEGER NOT NULL, last_modified_version INTEGER NOT NULL,"
        "PRIMARY KEY(table_id, record_id));");
    db_.Execute(
        "CREATE TABLE IF NOT EXISTS field_values ("
        "table_id INTEGER NOT NULL, record_id INTEGER NOT NULL, column_name TEXT NOT NULL, value_kind INTEGER NOT NULL,"
        "int64_value INTEGER, double_value REAL, bool_value INTEGER, text_value TEXT,"
        "PRIMARY KEY(table_id, record_id, column_name));");
    db_.Execute("CREATE INDEX IF NOT EXISTS idx_records_table_state ON records(table_id, state);");
    db_.Execute("CREATE INDEX IF NOT EXISTS idx_field_values_lookup ON field_values(table_id, column_name, value_kind, int64_value, text_value);");
    db_.Execute(
        "CREATE TABLE IF NOT EXISTS journal_transactions ("
        "tx_id INTEGER PRIMARY KEY AUTOINCREMENT, action_name TEXT NOT NULL, stack_kind INTEGER NOT NULL, stack_order INTEGER NOT NULL);");
    db_.Execute(
        "CREATE TABLE IF NOT EXISTS journal_entries ("
        "tx_id INTEGER NOT NULL, sequence_index INTEGER NOT NULL, op INTEGER NOT NULL, table_name TEXT NOT NULL,"
        "record_id INTEGER NOT NULL, field_name TEXT NOT NULL, old_kind INTEGER NOT NULL, old_int64 INTEGER,"
        "old_double REAL, old_bool INTEGER, old_text TEXT, new_kind INTEGER NOT NULL, new_int64 INTEGER,"
        "new_double REAL, new_bool INTEGER, new_text TEXT, old_deleted INTEGER NOT NULL, new_deleted INTEGER NOT NULL,"
        "PRIMARY KEY(tx_id, sequence_index));");
}

void SqliteDatabase::LoadMetadata()
{
    version_ = 0;
    nextRecordId_ = 1;

    SqliteStmt stmt = db_.Prepare("SELECT key, value FROM metadata;");
    bool hasRow = false;
    while (stmt.Step(&hasRow) == SC_OK && hasRow)
    {
        const std::wstring key = stmt.ColumnText(0);
        const std::wstring value = stmt.ColumnText(1);
        if (key == L"version")
        {
            version_ = static_cast<VersionId>(std::stoull(value));
        }
        else if (key == L"next_record_id")
        {
            nextRecordId_ = static_cast<RecordId>(std::stoll(value));
        }
    }
}

void SqliteDatabase::SaveMetadata()
{
    SqliteStmt stmt = db_.Prepare(
        "INSERT INTO metadata(key, value) VALUES(?, ?)"
        " ON CONFLICT(key) DO UPDATE SET value=excluded.value;");

    stmt.BindText(1, L"version");
    stmt.BindText(2, std::to_wstring(version_));
    stmt.Step();
    stmt.Reset();

    stmt.BindText(1, L"next_record_id");
    stmt.BindText(2, std::to_wstring(nextRecordId_));
    stmt.Step();
}

void SqliteDatabase::LoadTables()
{
    SqliteStmt tablesStmt = db_.Prepare("SELECT table_id, name FROM tables ORDER BY name;");
    bool hasRow = false;
    while (tablesStmt.Step(&hasRow) == SC_OK && hasRow)
    {
        const std::int64_t tableRowId = tablesStmt.ColumnInt64(0);
        const std::wstring tableName = tablesStmt.ColumnText(1);
        TablePtr table = MakeRef<SqliteTable>(this, tableName, tableRowId);
        tables_.emplace(tableName, table);
        auto* sqliteTable = static_cast<SqliteTable*>(table.Get());

        SqliteStmt columnsStmt = db_.Prepare(
            "SELECT column_name, display_name, value_kind, column_kind, nullable_flag, editable_flag, user_defined_flag,"
            " indexed_flag, participates_in_calc_flag, unit, reference_table, default_kind, default_int64, default_double,"
            " default_bool, default_text FROM schema_columns WHERE table_id = ? ORDER BY rowid;");
        columnsStmt.BindInt64(1, tableRowId);
        bool hasColumn = false;
        while (columnsStmt.Step(&hasColumn) == SC_OK && hasColumn)
        {
            ColumnDef def;
            def.name = columnsStmt.ColumnText(0);
            def.displayName = columnsStmt.ColumnText(1);
            def.valueKind = FromSqliteValueKind(columnsStmt.ColumnInt(2));
            def.columnKind = FromSqliteColumnKind(columnsStmt.ColumnInt(3));
            def.nullable = columnsStmt.ColumnBool(4);
            def.editable = columnsStmt.ColumnBool(5);
            def.userDefined = columnsStmt.ColumnBool(6);
            def.indexed = columnsStmt.ColumnBool(7);
            def.participatesInCalc = columnsStmt.ColumnBool(8);
            def.unit = columnsStmt.ColumnText(9);
            def.referenceTable = columnsStmt.ColumnText(10);
            def.defaultValue = ReadValueFromStorage(columnsStmt, 11, 12, 13, 14, 15);
            sqliteTable->Schema()->LoadColumn(def);
        }

        SqliteStmt recordsStmt = db_.Prepare("SELECT record_id, state, last_modified_version FROM records WHERE table_id = ?;");
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

        SqliteStmt valuesStmt = db_.Prepare(
            "SELECT record_id, column_name, value_kind, int64_value, double_value, bool_value, text_value"
            " FROM field_values WHERE table_id = ?;");
        valuesStmt.BindInt64(1, tableRowId);
        bool hasValue = false;
        while (valuesStmt.Step(&hasValue) == SC_OK && hasValue)
        {
            auto record = sqliteTable->FindRecordData(valuesStmt.ColumnInt64(0));
            if (!record)
            {
                continue;
            }
            record->values[valuesStmt.ColumnText(1)] = ReadValueFromStorage(valuesStmt, 2, 3, 4, 5, 6);
        }
    }
}

void SqliteDatabase::LoadJournalStacks()
{
    SqliteStmt txStmt = db_.Prepare("SELECT tx_id, action_name, stack_kind FROM journal_transactions ORDER BY stack_kind, stack_order;");
    bool hasTx = false;
    while (txStmt.Step(&hasTx) == SC_OK && hasTx)
    {
        SqlitePersistedJournalTransaction persisted;
        persisted.txId = txStmt.ColumnInt64(0);
        persisted.tx.actionName = txStmt.ColumnText(1);
        const int stackKind = txStmt.ColumnInt(2);

        SqliteStmt entryStmt = db_.Prepare(
            "SELECT op, table_name, record_id, field_name, old_kind, old_int64, old_double, old_bool, old_text,"
            " new_kind, new_int64, new_double, new_bool, new_text, old_deleted, new_deleted"
            " FROM journal_entries WHERE tx_id = ? ORDER BY sequence_index;");
        entryStmt.BindInt64(1, persisted.txId);
        bool hasEntry = false;
        while (entryStmt.Step(&hasEntry) == SC_OK && hasEntry)
        {
            JournalEntry entry;
            entry.op = FromSqliteJournalOp(entryStmt.ColumnInt(0));
            entry.tableName = entryStmt.ColumnText(1);
            entry.recordId = entryStmt.ColumnInt64(2);
            entry.fieldName = entryStmt.ColumnText(3);
            entry.oldValue = ReadValueFromStorage(entryStmt, 4, 5, 6, 7, 8);
            entry.newValue = ReadValueFromStorage(entryStmt, 9, 10, 11, 12, 13);
            entry.oldDeleted = entryStmt.ColumnBool(14);
            entry.newDeleted = entryStmt.ColumnBool(15);
            persisted.tx.entries.push_back(std::move(entry));
        }

        if (stackKind == kStackUndo)
        {
            undoStack_.push_back(std::move(persisted));
        }
        else
        {
            redoStack_.push_back(std::move(persisted));
        }
    }
}

ErrorCode SqliteDatabase::BeginEdit(const wchar_t* name, EditPtr& outEdit)
{
    if (activeEdit_)
    {
        return SC_E_WRITE_CONFLICT;
    }
    activeJournal_ = JournalTransaction{};
    activeJournal_.actionName = (name != nullptr && *name != L'\0') ? name : L"Edit";
    activeEdit_ = MakeRef<SqliteEditSession>(activeJournal_.actionName, version_);
    outEdit = activeEdit_;
    return SC_OK;
}

ErrorCode SqliteDatabase::Commit(IEditSession* edit)
{
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
        const std::int64_t txId = InsertJournalTransaction(activeJournal_, kStackUndo, static_cast<int>(undoStack_.size()));
        PersistJournalEntries(txId, activeJournal_);
        undoStack_.push_back(SqlitePersistedJournalTransaction{txId, activeJournal_});
        redoStack_.clear();
        SaveMetadata();
        const ErrorCode commitRc = txn.Commit();
        if (Failed(commitRc))
        {
            return commitRc;
        }
    }
    catch (...)
    {
        return SC_E_FAIL;
    }

    const ChangeSet changeSet = BuildChangeSet(activeJournal_, ChangeSource::UserEdit, version_);
    activeEdit_.Reset();
    activeJournal_ = JournalTransaction{};
    NotifyObservers(changeSet);
    return SC_OK;
}

ErrorCode SqliteDatabase::Rollback(IEditSession* edit)
{
    const ErrorCode validate = ValidateActiveEdit(edit);
    if (Failed(validate))
    {
        return validate;
    }

    if (!activeJournal_.entries.empty())
    {
        ApplyJournalReverse(activeJournal_);
    }
    activeEdit_->SetState(EditState::RolledBack);
    activeEdit_.Reset();
    activeJournal_ = JournalTransaction{};
    return SC_OK;
}

ErrorCode SqliteDatabase::Undo()
{
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
        UpdateJournalTransactionStack(tx.txId, kStackRedo, static_cast<int>(redoStack_.size()));
        redoStack_.push_back(tx);
        SaveMetadata();
        const ErrorCode commitRc = txn.Commit();
        if (Failed(commitRc))
        {
            return commitRc;
        }
    }
    catch (...)
    {
        return SC_E_FAIL;
    }

    NotifyObservers(BuildChangeSet(tx.tx, ChangeSource::Undo, version_));
    return SC_OK;
}

ErrorCode SqliteDatabase::Redo()
{
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
        UpdateJournalTransactionStack(tx.txId, kStackUndo, static_cast<int>(undoStack_.size()));
        undoStack_.push_back(tx);
        SaveMetadata();
        const ErrorCode commitRc = txn.Commit();
        if (Failed(commitRc))
        {
            return commitRc;
        }
    }
    catch (...)
    {
        return SC_E_FAIL;
    }

    NotifyObservers(BuildChangeSet(tx.tx, ChangeSource::Redo, version_));
    return SC_OK;
}

ErrorCode SqliteDatabase::GetTable(const wchar_t* name, TablePtr& outTable)
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

ErrorCode SqliteDatabase::CreateTable(const wchar_t* name, TablePtr& outTable)
{
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

        TablePtr table = MakeRef<SqliteTable>(this, std::wstring{name}, db_.LastInsertRowId());
        tables_.emplace(name, table);
        SaveMetadata();
        const ErrorCode commitRc = txn.Commit();
        if (Failed(commitRc))
        {
            return commitRc;
        }

        outTable = std::move(table);
        return SC_OK;
    }
    catch (...)
    {
        return SC_E_FAIL;
    }
}

ErrorCode SqliteDatabase::AddObserver(IDatabaseObserver* observer)
{
    if (observer == nullptr)
    {
        return SC_E_POINTER;
    }
    observers_.push_back(observer);
    return SC_OK;
}

ErrorCode SqliteDatabase::RemoveObserver(IDatabaseObserver* observer)
{
    observers_.erase(std::remove(observers_.begin(), observers_.end(), observer), observers_.end());
    return SC_OK;
}

ErrorCode SqliteDatabase::ValidateActiveEdit(IEditSession* edit) const
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

ErrorCode SqliteDatabase::ValidateWrite(SqliteTable* table, const std::shared_ptr<SqliteRecordData>& data, const std::wstring& fieldName, const Value& value)
{
    if (!activeEdit_)
    {
        return SC_E_NO_ACTIVE_EDIT;
    }
    if (data->state == RecordState::Deleted)
    {
        return SC_E_RECORD_DELETED;
    }

    const ColumnDef* column = table->Schema()->FindColumnDef(fieldName);
    if (column == nullptr)
    {
        return SC_E_COLUMN_NOT_FOUND;
    }
    if (!column->editable)
    {
        return SC_E_READ_ONLY_COLUMN;
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

SqliteDatabase::JournalLookup SqliteDatabase::LookupRecordJournalState(const std::wstring& tableName, RecordId recordId) const
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
        }
        else if (entry.op == JournalOp::DeleteRecord)
        {
            lookup.deletedInActiveEdit = true;
        }
    }
    return lookup;
}

void SqliteDatabase::RemoveFieldJournalEntries(const std::wstring& tableName, RecordId recordId)
{
    activeJournal_.entries.erase(
        std::remove_if(
            activeJournal_.entries.begin(),
            activeJournal_.entries.end(),
            [&](const JournalEntry& entry)
            {
                return entry.tableName == tableName
                    && entry.recordId == recordId
                    && (entry.op == JournalOp::SetValue || entry.op == JournalOp::SetRelation);
            }),
        activeJournal_.entries.end());
}

void SqliteDatabase::RemoveAllJournalEntriesForRecord(const std::wstring& tableName, RecordId recordId)
{
    activeJournal_.entries.erase(
        std::remove_if(
            activeJournal_.entries.begin(),
            activeJournal_.entries.end(),
            [&](const JournalEntry& entry)
            {
                return entry.tableName == tableName && entry.recordId == recordId;
            }),
        activeJournal_.entries.end());
}

ErrorCode SqliteDatabase::WriteValue(SqliteTable* table, const std::shared_ptr<SqliteRecordData>& data, const std::wstring& fieldName, const Value& value)
{
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

    const ColumnDef* column = table->Schema()->FindColumnDef(fieldName);
    Value oldValue = column->defaultValue;
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
    const JournalOp op = (column != nullptr && column->columnKind == ColumnKind::Relation)
        ? JournalOp::SetRelation
        : JournalOp::SetValue;
    RecordJournal(table->Name(), data->id, fieldName, oldValue, value, false, false, op);
    return SC_OK;
}

ErrorCode SqliteDatabase::DeleteRecord(SqliteTable* table, const std::shared_ptr<SqliteRecordData>& data)
{
    if (!activeEdit_)
    {
        return SC_E_NO_ACTIVE_EDIT;
    }
    if (data->state == RecordState::Deleted)
    {
        return SC_E_RECORD_DELETED;
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
        return SC_OK;
    }

    RemoveFieldJournalEntries(table->Name(), data->id);
    RecordJournal(table->Name(), data->id, L"", Value::Null(), Value::Null(), false, true, JournalOp::DeleteRecord);
    return SC_OK;
}

void SqliteDatabase::RecordCreate(SqliteTable* table, const std::shared_ptr<SqliteRecordData>& data)
{
    RecordJournal(table->Name(), data->id, L"", Value::Null(), Value::Null(), true, false, JournalOp::CreateRecord);
}

ErrorCode SqliteDatabase::PersistAddedColumn(SqliteSchema* schema, const ColumnDef& def)
{
    if (schema == nullptr)
    {
        return SC_E_POINTER;
    }

    try
    {
        SqliteTxn txn(db_);
        SqliteStmt stmt = db_.Prepare(
            "INSERT INTO schema_columns("
            " table_id, column_name, display_name, value_kind, column_kind, nullable_flag, editable_flag,"
            " user_defined_flag, indexed_flag, participates_in_calc_flag, unit, reference_table,"
            " default_kind, default_int64, default_double, default_bool, default_text)"
            " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
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
    }
    catch (...)
    {
        return SC_E_FAIL;
    }

    schema->LoadColumn(def);
    return SC_OK;
}

void SqliteDatabase::RecordJournal(
    const std::wstring& tableName,
    RecordId recordId,
    const std::wstring& fieldName,
    const Value& oldValue,
    const Value& newValue,
    bool oldDeleted,
    bool newDeleted,
    JournalOp op)
{
    for (auto& entry : activeJournal_.entries)
    {
        if (entry.op == op && entry.tableName == tableName && entry.recordId == recordId && entry.fieldName == fieldName)
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
            ? (entry.oldDeleted ? RecordState::Deleted : RecordState::Alive)
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
            }
            else
            {
                data->values[entry.fieldName] = entry.oldValue;
            }
        }
        else if (entry.newValue.IsNull())
        {
            data->values.erase(entry.fieldName);
        }
        else
        {
            data->values[entry.fieldName] = entry.newValue;
        }
        break;
    }
}

void SqliteDatabase::UpdateTouchedVersions(const JournalTransaction& tx, VersionId version)
{
    for (const auto& [tableName, recordId] : GetTouchedRecordKeys(tx))
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

ChangeSet SqliteDatabase::BuildChangeSet(const JournalTransaction& tx, ChangeSource source, VersionId version) const
{
    ChangeSet changeSet;
    changeSet.actionName = tx.actionName;
    changeSet.source = source;
    changeSet.version = version;

    for (const auto& entry : tx.entries)
    {
        DataChange change;
        change.tableName = entry.tableName;
        change.recordId = entry.recordId;
        change.fieldName = entry.fieldName;
        change.oldValue = (source == ChangeSource::Undo) ? entry.newValue : entry.oldValue;
        change.newValue = (source == ChangeSource::Undo) ? entry.oldValue : entry.newValue;
        change.structuralChange = (entry.op == JournalOp::CreateRecord || entry.op == JournalOp::DeleteRecord);
        change.relationChange = (entry.op == JournalOp::SetRelation);

        switch (entry.op)
        {
        case JournalOp::CreateRecord:
            change.kind = (source == ChangeSource::Undo) ? ChangeKind::RecordDeleted : ChangeKind::RecordCreated;
            break;
        case JournalOp::DeleteRecord:
            change.kind = (source == ChangeSource::Undo) ? ChangeKind::RecordCreated : ChangeKind::RecordDeleted;
            break;
        case JournalOp::SetRelation:
            change.kind = ChangeKind::RelationUpdated;
            break;
        case JournalOp::SetValue:
        default:
            change.kind = ChangeKind::FieldUpdated;
            break;
        }

        changeSet.changes.push_back(std::move(change));
    }
    return changeSet;
}

void SqliteDatabase::NotifyObservers(const ChangeSet& changeSet)
{
    std::vector<IDatabaseObserver*> snapshot = observers_;
    for (auto* observer : snapshot)
    {
        if (observer != nullptr)
        {
            observer->OnDatabaseChanged(changeSet);
        }
    }
}

std::vector<std::pair<std::wstring, RecordId>> SqliteDatabase::GetTouchedRecordKeys(const JournalTransaction& tx) const
{
    std::set<std::pair<std::wstring, RecordId>> unique;
    for (const auto& entry : tx.entries)
    {
        unique.emplace(entry.tableName, entry.recordId);
    }
    return std::vector<std::pair<std::wstring, RecordId>>(unique.begin(), unique.end());
}

void SqliteDatabase::PersistTouchedRecords(const JournalTransaction& tx)
{
    SqliteStmt upsertRecord = db_.Prepare(
        "INSERT INTO records(table_id, record_id, state, last_modified_version) VALUES(?, ?, ?, ?)"
        " ON CONFLICT(table_id, record_id) DO UPDATE SET state=excluded.state, last_modified_version=excluded.last_modified_version;");
    SqliteStmt deleteValues = db_.Prepare("DELETE FROM field_values WHERE table_id = ? AND record_id = ?;");
    SqliteStmt insertValue = db_.Prepare(
        "INSERT INTO field_values(table_id, record_id, column_name, value_kind, int64_value, double_value, bool_value, text_value)"
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
        upsertRecord.BindInt64(4, static_cast<std::int64_t>(data->lastModifiedVersion));
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

        for (const auto& [fieldName, value] : data->values)
        {
            insertValue.BindInt64(1, table->TableRowId());
            insertValue.BindInt64(2, data->id);
            insertValue.BindText(3, fieldName);
            BindValueForStorage(insertValue, 4, 5, 6, 7, 8, value);
            insertValue.Step();
            insertValue.Reset();
        }
    }
}

std::int64_t SqliteDatabase::InsertJournalTransaction(const JournalTransaction& tx, int stackKind, int stackOrder)
{
    SqliteStmt stmt = db_.Prepare("INSERT INTO journal_transactions(action_name, stack_kind, stack_order) VALUES(?, ?, ?);");
    stmt.BindText(1, tx.actionName);
    stmt.BindInt(2, stackKind);
    stmt.BindInt(3, stackOrder);
    stmt.Step();
    return db_.LastInsertRowId();
}

void SqliteDatabase::PersistJournalEntries(std::int64_t txId, const JournalTransaction& tx)
{
    SqliteStmt stmt = db_.Prepare(
        "INSERT INTO journal_entries("
        " tx_id, sequence_index, op, table_name, record_id, field_name, old_kind, old_int64, old_double, old_bool, old_text,"
        " new_kind, new_int64, new_double, new_bool, new_text, old_deleted, new_deleted)"
        " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

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

void SqliteDatabase::UpdateJournalTransactionStack(std::int64_t txId, int stackKind, int stackOrder)
{
    SqliteStmt stmt = db_.Prepare("UPDATE journal_transactions SET stack_kind = ?, stack_order = ? WHERE tx_id = ?;");
    stmt.BindInt(1, stackKind);
    stmt.BindInt(2, stackOrder);
    stmt.BindInt64(3, txId);
    stmt.Step();
}

void SqliteDatabase::DeleteJournalTransaction(std::int64_t txId)
{
    SqliteStmt stmt = db_.Prepare("DELETE FROM journal_transactions WHERE tx_id = ?;");
    stmt.BindInt64(1, txId);
    stmt.Step();
}

ErrorCode CreateSqliteDatabase(const wchar_t* path, DbPtr& outDatabase)
{
    if (path == nullptr)
    {
        return SC_E_POINTER;
    }

    try
    {
        outDatabase = MakeRef<SqliteDatabase>(std::wstring{path});
        return SC_OK;
    }
    catch (...)
    {
        outDatabase.Reset();
        return SC_E_FAIL;
    }
}

}  // namespace
}  // namespace stablecore::storage
