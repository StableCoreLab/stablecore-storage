#pragma once

#include "StableCore/Storage/Errors.h"
#include "StableCore/Storage/RefPtr.h"
#include "StableCore/Storage/Types.h"

namespace stablecore::storage
{

class ISchema;
class IRecord;
class IRecordCursor;
class ITable;
class IEditSession;
class IDatabase;
class IDatabaseObserver;

using SchemaPtr = RefPtr<ISchema>;
using RecordPtr = RefPtr<IRecord>;
using RecordCursorPtr = RefPtr<IRecordCursor>;
using TablePtr = RefPtr<ITable>;
using EditPtr = RefPtr<IEditSession>;
using DbPtr = RefPtr<IDatabase>;

class ISchema : public virtual IRefObject
{
public:
    // Returns the number of registered columns in the table schema.
    virtual ErrorCode GetColumnCount(std::int32_t* outCount) = 0;
    // Reads a column definition by index.
    virtual ErrorCode GetColumn(std::int32_t index, ColumnDef* outDef) = 0;
    // Looks up a column by name.
    virtual ErrorCode FindColumn(const wchar_t* name, ColumnDef* outDef) = 0;
    // Registers a new schema column. Unknown columns cannot be written before registration.
    virtual ErrorCode AddColumn(const ColumnDef& def) = 0;
};

class IRecord : public virtual IRefObject
{
public:
    // Stable logical identity of the record. The id is preserved across undo/redo restore.
    virtual RecordId GetId() const noexcept = 0;
    // Deleted records remain addressable by existing handles but reject writes and typed reads.
    virtual bool IsDeleted() const noexcept = 0;
    virtual VersionId GetLastModifiedVersion() const noexcept = 0;

    // Returns SC_OK when a value exists, SC_E_VALUE_IS_NULL when the column resolves to Null.
    virtual ErrorCode GetValue(const wchar_t* name, Value* outValue) = 0;
    // All writes must happen inside an active edit session owned by the database.
    virtual ErrorCode SetValue(const wchar_t* name, const Value& value) = 0;

    virtual ErrorCode GetInt64(const wchar_t* name, std::int64_t* outValue) = 0;
    virtual ErrorCode SetInt64(const wchar_t* name, std::int64_t value) = 0;

    virtual ErrorCode GetDouble(const wchar_t* name, double* outValue) = 0;
    virtual ErrorCode SetDouble(const wchar_t* name, double value) = 0;

    virtual ErrorCode GetBool(const wchar_t* name, bool* outValue) = 0;
    virtual ErrorCode SetBool(const wchar_t* name, bool value) = 0;

    // The returned pointer is owned by the record value storage and stays valid until the record value changes.
    virtual ErrorCode GetString(const wchar_t* name, const wchar_t** outValue) = 0;
    // Copies the current string value into caller-owned storage.
    virtual ErrorCode GetStringCopy(const wchar_t* name, std::wstring* outValue) = 0;
    virtual ErrorCode SetString(const wchar_t* name, const wchar_t* value) = 0;

    virtual ErrorCode GetRef(const wchar_t* name, RecordId* outValue) = 0;
    virtual ErrorCode SetRef(const wchar_t* name, RecordId value) = 0;
};

class IRecordCursor : public virtual IRefObject
{
public:
    // Moves the cursor to the next record. Returns SC_OK and outHasValue=false on exhaustion.
    virtual ErrorCode MoveNext(bool* outHasValue) = 0;
    virtual ErrorCode GetCurrent(RecordPtr& outRecord) = 0;
};

class ITable : public virtual IRefObject
{
public:
    // Returns both alive and deleted records when addressed by id. Deleted state must be checked on the record.
    virtual ErrorCode GetRecord(RecordId id, RecordPtr& outRecord) = 0;
    // Creates a new record in the current edit session.
    virtual ErrorCode CreateRecord(RecordPtr& outRecord) = 0;
    virtual ErrorCode DeleteRecord(RecordId id) = 0;

    virtual ErrorCode GetSchema(SchemaPtr& outSchema) = 0;
    // Enumerates alive records only.
    virtual ErrorCode EnumerateRecords(RecordCursorPtr& outCursor) = 0;
    // Performs a single-column equality match over alive records only.
    virtual ErrorCode FindRecords(const QueryCondition& condition, RecordCursorPtr& outCursor) = 0;
};

class IEditSession : public virtual IRefObject
{
public:
    virtual const wchar_t* GetName() const = 0;
    virtual EditState GetState() const noexcept = 0;
    virtual VersionId GetOpenedVersion() const noexcept = 0;
};

class IDatabaseObserver
{
public:
    virtual ~IDatabaseObserver() = default;
    virtual void OnDatabaseChanged(const ChangeSet& changeSet) = 0;
};

class IDatabase : public virtual IRefObject
{
public:
    // Starts the single active edit session for this database.
    virtual ErrorCode BeginEdit(const wchar_t* name, EditPtr& outEdit) = 0;
    virtual ErrorCode Commit(IEditSession* edit) = 0;
    virtual ErrorCode Rollback(IEditSession* edit) = 0;

    virtual ErrorCode Undo() = 0;
    virtual ErrorCode Redo() = 0;

    virtual ErrorCode GetTableCount(std::int32_t* outCount) = 0;
    virtual ErrorCode GetTableName(std::int32_t index, std::wstring* outName) = 0;
    virtual ErrorCode GetTable(const wchar_t* name, TablePtr& outTable) = 0;
    virtual ErrorCode CreateTable(const wchar_t* name, TablePtr& outTable) = 0;

    virtual ErrorCode AddObserver(IDatabaseObserver* observer) = 0;
    virtual ErrorCode RemoveObserver(IDatabaseObserver* observer) = 0;

    virtual VersionId GetCurrentVersion() const noexcept = 0;
};

}  // namespace stablecore::storage
