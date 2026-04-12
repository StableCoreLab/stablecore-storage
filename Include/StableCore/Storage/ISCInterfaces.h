#pragma once

#include "StableCore/Storage/SCErrors.h"
#include "StableCore/Storage/ISCRefPtr.h"
#include "StableCore/Storage/SCTypes.h"

namespace stablecore::storage
{

class ISCSchema;
class ISCRecord;
class ISCRecordCursor;
class ISCTable;
class ISCEditSession;
class ISCDatabase;
class ISCDatabaseObserver;

using SCSchemaPtr = SCRefPtr<ISCSchema>;
using SCRecordPtr = SCRefPtr<ISCRecord>;
using SCRecordCursorPtr = SCRefPtr<ISCRecordCursor>;
using SCTablePtr = SCRefPtr<ISCTable>;
using SCEditPtr = SCRefPtr<ISCEditSession>;
using SCDbPtr = SCRefPtr<ISCDatabase>;

class ISCSchema : public virtual ISCRefObject
{
public:
    // Returns the number of registered columns in the table schema.
    virtual ErrorCode GetColumnCount(std::int32_t* outCount) = 0;
    // Reads a column definition by index.
    virtual ErrorCode GetColumn(std::int32_t index, SCColumnDef* outDef) = 0;
    // Looks up a column by name.
    virtual ErrorCode FindColumn(const wchar_t* name, SCColumnDef* outDef) = 0;
    // Registers a new schema column. Unknown columns cannot be written before registration.
    virtual ErrorCode AddColumn(const SCColumnDef& def) = 0;
};

class ISCRecord : public virtual ISCRefObject
{
public:
    // Stable logical identity of the record. The id is preserved across undo/redo restore.
    virtual RecordId GetId() const noexcept = 0;
    // Deleted records remain addressable by existing handles but reject writes and typed reads.
    virtual bool IsDeleted() const noexcept = 0;
    virtual VersionId GetLastModifiedVersion() const noexcept = 0;

    // Returns SC_OK when a SCValue exists, SC_E_VALUE_IS_NULL when the column resolves to Null.
    virtual ErrorCode GetValue(const wchar_t* name, SCValue* outValue) = 0;
    // All writes must happen inside an active edit session owned by the database.
    virtual ErrorCode SetValue(const wchar_t* name, const SCValue& value) = 0;

    virtual ErrorCode GetInt64(const wchar_t* name, std::int64_t* outValue) = 0;
    virtual ErrorCode SetInt64(const wchar_t* name, std::int64_t value) = 0;

    virtual ErrorCode GetDouble(const wchar_t* name, double* outValue) = 0;
    virtual ErrorCode SetDouble(const wchar_t* name, double value) = 0;

    virtual ErrorCode GetBool(const wchar_t* name, bool* outValue) = 0;
    virtual ErrorCode SetBool(const wchar_t* name, bool value) = 0;

    // The returned pointer is owned by the record SCValue storage and stays valid until the record SCValue changes.
    virtual ErrorCode GetString(const wchar_t* name, const wchar_t** outValue) = 0;
    // Copies the current string SCValue into caller-owned storage.
    virtual ErrorCode GetStringCopy(const wchar_t* name, std::wstring* outValue) = 0;
    virtual ErrorCode SetString(const wchar_t* name, const wchar_t* value) = 0;

    virtual ErrorCode GetRef(const wchar_t* name, RecordId* outValue) = 0;
    virtual ErrorCode SetRef(const wchar_t* name, RecordId value) = 0;
};

class ISCRecordCursor : public virtual ISCRefObject
{
public:
    // Moves the cursor to the next record. Returns SC_OK and outHasValue=false on exhaustion.
    virtual ErrorCode MoveNext(bool* outHasValue) = 0;
    virtual ErrorCode GetCurrent(SCRecordPtr& outRecord) = 0;
};

class ISCTable : public virtual ISCRefObject
{
public:
    // Returns both alive and deleted records when addressed by id. Deleted state must be checked on the record.
    virtual ErrorCode GetRecord(RecordId id, SCRecordPtr& outRecord) = 0;
    // Creates a new record in the current edit session.
    virtual ErrorCode CreateRecord(SCRecordPtr& outRecord) = 0;
    virtual ErrorCode DeleteRecord(RecordId id) = 0;

    virtual ErrorCode GetSchema(SCSchemaPtr& outSchema) = 0;
    // Enumerates alive records only.
    virtual ErrorCode EnumerateRecords(SCRecordCursorPtr& outCursor) = 0;
    // Performs a single-column equality match over alive records only.
    virtual ErrorCode FindRecords(const SCQueryCondition& condition, SCRecordCursorPtr& outCursor) = 0;
};

class ISCEditSession : public virtual ISCRefObject
{
public:
    virtual const wchar_t* GetName() const = 0;
    virtual EditState GetState() const noexcept = 0;
    virtual VersionId GetOpenedVersion() const noexcept = 0;
};

class ISCDatabaseObserver
{
public:
    virtual ~ISCDatabaseObserver() = default;
    virtual void OnDatabaseChanged(const SCChangeSet& changeSet) = 0;
};

class ISCDatabase : public virtual ISCRefObject
{
public:
    // Starts the single active edit session for this database.
    virtual ErrorCode BeginEdit(const wchar_t* name, SCEditPtr& outEdit) = 0;
    virtual ErrorCode Commit(ISCEditSession* edit) = 0;
    virtual ErrorCode Rollback(ISCEditSession* edit) = 0;

    virtual ErrorCode Undo() = 0;
    virtual ErrorCode Redo() = 0;

    virtual ErrorCode GetTableCount(std::int32_t* outCount) = 0;
    virtual ErrorCode GetTableName(std::int32_t index, std::wstring* outName) = 0;
    virtual ErrorCode GetTable(const wchar_t* name, SCTablePtr& outTable) = 0;
    virtual ErrorCode CreateTable(const wchar_t* name, SCTablePtr& outTable) = 0;

    virtual ErrorCode AddObserver(ISCDatabaseObserver* observer) = 0;
    virtual ErrorCode RemoveObserver(ISCDatabaseObserver* observer) = 0;

    virtual VersionId GetCurrentVersion() const noexcept = 0;
};

}  // namespace stablecore::storage
