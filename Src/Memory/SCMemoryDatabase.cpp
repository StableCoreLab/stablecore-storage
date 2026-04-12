#include "StableCore/Storage/SCFactory.h"

#include <algorithm>
#include <map>
#include <memory>
#include <unordered_map>

#include "StableCore/Storage/SCRefCounted.h"

namespace StableCore::Storage
{
namespace
{

class MemoryDatabase;
class MemoryTable;

struct MemoryRecordData
{
    explicit MemoryRecordData(RecordId newId)
        : id(newId)
    {
    }

    RecordId id{0};
    RecordState state{RecordState::Alive};
    VersionId lastModifiedVersion{0};
    std::unordered_map<std::wstring, SCValue> values;
};

ErrorCode ValidateValueKind(ValueKind expected, const SCValue& value, bool nullable)
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
        if (!def.defaultValue.IsNull() && def.defaultValue.GetKind() != ValueKind::RecordId)
        {
            return SC_E_SCHEMA_VIOLATION;
        }
    }
    else
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

    if (!def.nullable && def.defaultValue.IsNull())
    {
        return SC_E_SCHEMA_VIOLATION;
    }

    return SC_OK;
}

class MemorySchema final : public ISCSchema, public SCRefCountedObject
{
public:
    ErrorCode GetColumnCount(std::int32_t* outCount) override
    {
        if (outCount == nullptr)
        {
            return SC_E_POINTER;
        }

        *outCount = static_cast<std::int32_t>(columns_.size());
        return SC_OK;
    }

    ErrorCode GetColumn(std::int32_t index, SCColumnDef* outDef) override
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

    ErrorCode FindColumn(const wchar_t* name, SCColumnDef* outDef) override
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

    ErrorCode AddColumn(const SCColumnDef& def) override
    {
        const ErrorCode validate = ValidateColumnDef(def);
        if (Failed(validate))
        {
            return validate;
        }
        if (columnsByName_.find(def.name) != columnsByName_.end())
        {
            return SC_E_COLUMN_EXISTS;
        }

        columns_.push_back(def);
        columnsByName_.emplace(def.name, def);
        return SC_OK;
    }

    const SCColumnDef* FindColumnDef(const std::wstring& name) const noexcept
    {
        const auto it = columnsByName_.find(name);
        return it == columnsByName_.end() ? nullptr : &it->second;
    }

private:
    std::vector<SCColumnDef> columns_;
    std::unordered_map<std::wstring, SCColumnDef> columnsByName_;
};

class MemoryEditSession final : public ISCEditSession, public SCRefCountedObject
{
public:
    MemoryEditSession(std::wstring name, VersionId version)
        : name_(std::move(name))
        , openedVersion_(version)
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

class MemoryRecord final : public ISCRecord, public SCRefCountedObject
{
public:
    MemoryRecord(MemoryDatabase* db, MemoryTable* table, std::shared_ptr<MemoryRecordData> data)
        : db_(db)
        , table_(table)
        , data_(std::move(data))
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

    ErrorCode GetRef(const wchar_t* name, RecordId* outValue) override;
    ErrorCode SetRef(const wchar_t* name, RecordId value) override;

private:
    ErrorCode ReadTypedValue(const wchar_t* name, SCValue* outValue);

    MemoryDatabase* db_{nullptr};
    MemoryTable* table_{nullptr};
    std::shared_ptr<MemoryRecordData> data_;
};

class MemoryRecordCursor final : public ISCRecordCursor, public SCRefCountedObject
{
public:
    explicit MemoryRecordCursor(std::vector<SCRecordPtr> records)
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

class MemoryTable final : public ISCTable, public SCRefCountedObject
{
public:
    MemoryTable(MemoryDatabase* db, std::wstring name)
        : db_(db)
        , name_(std::move(name))
        , schema_(SCMakeRef<MemorySchema>())
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

    MemorySchema* Schema() const noexcept
    {
        return schema_.Get();
    }

    std::shared_ptr<MemoryRecordData> FindRecordData(RecordId id) const
    {
        const auto it = records_.find(id);
        return it == records_.end() ? nullptr : it->second;
    }

    std::unordered_map<RecordId, std::shared_ptr<MemoryRecordData>>& Records() noexcept
    {
        return records_;
    }

private:
    SCRecordPtr MakeRecord(const std::shared_ptr<MemoryRecordData>& data)
    {
        return SCMakeRef<MemoryRecord>(db_, this, data);
    }

    MemoryDatabase* db_{nullptr};
    std::wstring name_;
    SCRefPtr<MemorySchema> schema_;
    std::unordered_map<RecordId, std::shared_ptr<MemoryRecordData>> records_;
};

class MemoryDatabase final : public ISCDatabase, public SCRefCountedObject
{
public:
    ErrorCode BeginEdit(const wchar_t* name, SCEditPtr& outEdit) override;
    ErrorCode Commit(ISCEditSession* edit) override;
    ErrorCode Rollback(ISCEditSession* edit) override;

    ErrorCode Undo() override;
    ErrorCode Redo() override;

    ErrorCode GetTableCount(std::int32_t* outCount) override;
    ErrorCode GetTableName(std::int32_t index, std::wstring* outName) override;
    ErrorCode GetTable(const wchar_t* name, SCTablePtr& outTable) override;
    ErrorCode CreateTable(const wchar_t* name, SCTablePtr& outTable) override;

    ErrorCode AddObserver(ISCDatabaseObserver* observer) override;
    ErrorCode RemoveObserver(ISCDatabaseObserver* observer) override;

    VersionId GetCurrentVersion() const noexcept override
    {
        return version_;
    }

    bool HasActiveEdit() const noexcept
    {
        return static_cast<bool>(activeEdit_);
    }

    RecordId AllocateRecordId() noexcept
    {
        return nextRecordId_++;
    }

    ErrorCode WriteValue(MemoryTable* table, const std::shared_ptr<MemoryRecordData>& data, const std::wstring& fieldName, const SCValue& value);
    ErrorCode DeleteRecord(MemoryTable* table, const std::shared_ptr<MemoryRecordData>& data);
    void RecordCreate(MemoryTable* table, const std::shared_ptr<MemoryRecordData>& data);

private:
    struct JournalLookup
    {
        bool createdInActiveEdit{false};
        bool deletedInActiveEdit{false};
    };

    ErrorCode ValidateActiveEdit(ISCEditSession* edit) const;
    ErrorCode ValidateWrite(MemoryTable* table, const std::shared_ptr<MemoryRecordData>& data, const std::wstring& fieldName, const SCValue& value);
    bool IsRecordReferenced(const std::wstring& tableName, RecordId recordId) const;
    JournalLookup LookupRecordJournalState(const std::wstring& tableName, RecordId recordId) const;
    void RemoveFieldJournalEntries(const std::wstring& tableName, RecordId recordId);
    void RemoveAllJournalEntriesForRecord(const std::wstring& tableName, RecordId recordId);
    void RecordJournal(
        const std::wstring& tableName,
        RecordId recordId,
        const std::wstring& fieldName,
        const SCValue& oldValue,
        const SCValue& newValue,
        bool oldDeleted,
        bool newDeleted,
        JournalOp forcedOp);
    void ApplyJournalReverse(const JournalTransaction& tx);
    void ApplyJournalForward(const JournalTransaction& tx);
    void ApplyEntry(const JournalEntry& entry, bool reverse);
    void UpdateTouchedVersions(const JournalTransaction& tx, VersionId version);
    SCChangeSet BuildChangeSet(const JournalTransaction& tx, ChangeSource source, VersionId version) const;
    void NotifyObservers(const SCChangeSet& SCChangeSet);

    VersionId version_{0};
    RecordId nextRecordId_{1};
    std::map<std::wstring, SCTablePtr> tables_;
    std::vector<ISCDatabaseObserver*> observers_;
    SCRefPtr<MemoryEditSession> activeEdit_;
    JournalTransaction activeJournal_;
    std::vector<JournalTransaction> undoStack_;
    std::vector<JournalTransaction> redoStack_;
};

ErrorCode MemoryDatabase::BeginEdit(const wchar_t* name, SCEditPtr& outEdit)
{
    if (activeEdit_)
    {
        return SC_E_WRITE_CONFLICT;
    }

    activeJournal_ = JournalTransaction{};
    activeJournal_.actionName = (name != nullptr && *name != L'\0') ? name : L"Edit";
    activeEdit_ = SCMakeRef<MemoryEditSession>(activeJournal_.actionName, version_);
    outEdit = activeEdit_;
    return SC_OK;
}

ErrorCode MemoryDatabase::Commit(ISCEditSession* edit)
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

    ++version_;
    UpdateTouchedVersions(activeJournal_, version_);
    undoStack_.push_back(activeJournal_);
    redoStack_.clear();

    SCChangeSet SCChangeSet = BuildChangeSet(activeJournal_, ChangeSource::UserEdit, version_);
    activeEdit_.Reset();
    activeJournal_ = JournalTransaction{};
    NotifyObservers(SCChangeSet);
    return SC_OK;
}

ErrorCode MemoryDatabase::Rollback(ISCEditSession* edit)
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

ErrorCode MemoryDatabase::Undo()
{
    if (activeEdit_)
    {
        return SC_E_WRITE_CONFLICT;
    }
    if (undoStack_.empty())
    {
        return SC_E_UNDO_STACK_EMPTY;
    }

    JournalTransaction tx = undoStack_.back();
    undoStack_.pop_back();
    ApplyJournalReverse(tx);
    ++version_;
    UpdateTouchedVersions(tx, version_);
    redoStack_.push_back(tx);
    NotifyObservers(BuildChangeSet(tx, ChangeSource::Undo, version_));
    return SC_OK;
}

ErrorCode MemoryDatabase::Redo()
{
    if (activeEdit_)
    {
        return SC_E_WRITE_CONFLICT;
    }
    if (redoStack_.empty())
    {
        return SC_E_REDO_STACK_EMPTY;
    }

    JournalTransaction tx = redoStack_.back();
    redoStack_.pop_back();
    ApplyJournalForward(tx);
    ++version_;
    UpdateTouchedVersions(tx, version_);
    undoStack_.push_back(tx);
    NotifyObservers(BuildChangeSet(tx, ChangeSource::Redo, version_));
    return SC_OK;
}

ErrorCode MemoryDatabase::GetTable(const wchar_t* name, SCTablePtr& outTable)
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

ErrorCode MemoryDatabase::GetTableCount(std::int32_t* outCount)
{
    if (outCount == nullptr)
    {
        return SC_E_POINTER;
    }

    *outCount = static_cast<std::int32_t>(tables_.size());
    return SC_OK;
}

ErrorCode MemoryDatabase::GetTableName(std::int32_t index, std::wstring* outName)
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

ErrorCode MemoryDatabase::CreateTable(const wchar_t* name, SCTablePtr& outTable)
{
    if (name == nullptr || *name == L'\0')
    {
        return SC_E_INVALIDARG;
    }

    const auto found = tables_.find(name);
    if (found != tables_.end())
    {
        outTable = found->second;
        return SC_OK;
    }

    SCTablePtr table = SCMakeRef<MemoryTable>(this, std::wstring{name});
    tables_.emplace(name, table);
    outTable = std::move(table);
    return SC_OK;
}

ErrorCode MemoryDatabase::AddObserver(ISCDatabaseObserver* observer)
{
    if (observer == nullptr)
    {
        return SC_E_POINTER;
    }

    observers_.push_back(observer);
    return SC_OK;
}

ErrorCode MemoryDatabase::RemoveObserver(ISCDatabaseObserver* observer)
{
    observers_.erase(std::remove(observers_.begin(), observers_.end(), observer), observers_.end());
    return SC_OK;
}

ErrorCode MemoryDatabase::ValidateActiveEdit(ISCEditSession* edit) const
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

ErrorCode MemoryDatabase::ValidateWrite(
    MemoryTable* table,
    const std::shared_ptr<MemoryRecordData>& data,
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

    const ErrorCode validate = ValidateValueKind(column->valueKind, value, column->nullable);
    if (Failed(validate))
    {
        return validate;
    }

    if (column->columnKind == ColumnKind::Relation && !value.IsNull())
    {
        RecordId refId = 0;
        const ErrorCode refRead = value.AsRecordId(&refId);
        if (Failed(refRead))
        {
            return refRead;
        }

        if (!column->referenceTable.empty())
        {
            const auto targetIt = tables_.find(column->referenceTable);
            if (targetIt == tables_.end())
            {
                return SC_E_REFERENCE_INVALID;
            }

            auto targetRecord = static_cast<MemoryTable*>(targetIt->second.Get())->FindRecordData(refId);
            if (!targetRecord || targetRecord->state == RecordState::Deleted)
            {
                return SC_E_REFERENCE_INVALID;
            }
        }
    }

    return SC_OK;
}

MemoryDatabase::JournalLookup MemoryDatabase::LookupRecordJournalState(const std::wstring& tableName, RecordId recordId) const
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

void MemoryDatabase::RemoveFieldJournalEntries(const std::wstring& tableName, RecordId recordId)
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

void MemoryDatabase::RemoveAllJournalEntriesForRecord(const std::wstring& tableName, RecordId recordId)
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

ErrorCode MemoryDatabase::WriteValue(
    MemoryTable* table,
    const std::shared_ptr<MemoryRecordData>& data,
    const std::wstring& fieldName,
    const SCValue& value)
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

    const SCColumnDef* column = table->Schema()->FindColumnDef(fieldName);
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
    const JournalOp op = (column != nullptr && column->columnKind == ColumnKind::Relation)
        ? JournalOp::SetRelation
        : JournalOp::SetValue;
    RecordJournal(table->Name(), data->id, fieldName, oldValue, value, false, false, op);
    return SC_OK;
}

ErrorCode MemoryDatabase::DeleteRecord(MemoryTable* table, const std::shared_ptr<MemoryRecordData>& data)
{
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
    RecordJournal(table->Name(), data->id, L"", SCValue::Null(), SCValue::Null(), false, true, JournalOp::DeleteRecord);
    return SC_OK;
}

bool MemoryDatabase::IsRecordReferenced(const std::wstring& tableName, RecordId recordId) const
{
    for (const auto& [_, tableRef] : tables_)
    {
        auto* table = static_cast<MemoryTable*>(tableRef.Get());
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

            for (const auto& [candidateId, candidateData] : table->Records())
            {
                if (candidateData == nullptr || candidateData->state == RecordState::Deleted)
                {
                    continue;
                }
                const auto valueIt = candidateData->values.find(column.name);
                if (valueIt == candidateData->values.end())
                {
                    continue;
                }

                RecordId referencedId = 0;
                if (Succeeded(valueIt->second.AsRecordId(&referencedId)) && referencedId == recordId)
                {
                    return true;
                }
            }
        }
    }

    return false;
}

void MemoryDatabase::RecordCreate(MemoryTable* table, const std::shared_ptr<MemoryRecordData>& data)
{
    RecordJournal(table->Name(), data->id, L"", SCValue::Null(), SCValue::Null(), true, false, JournalOp::CreateRecord);
}

void MemoryDatabase::RecordJournal(
    const std::wstring& tableName,
    RecordId recordId,
    const std::wstring& fieldName,
    const SCValue& oldValue,
    const SCValue& newValue,
    bool oldDeleted,
    bool newDeleted,
    JournalOp forcedOp)
{
    for (auto& entry : activeJournal_.entries)
    {
        if (entry.op == forcedOp && entry.tableName == tableName && entry.recordId == recordId && entry.fieldName == fieldName)
        {
            entry.newValue = newValue;
            entry.newDeleted = newDeleted;
            return;
        }
    }

    activeJournal_.entries.push_back(JournalEntry{
        forcedOp,
        tableName,
        recordId,
        fieldName,
        oldValue,
        newValue,
        oldDeleted,
        newDeleted,
    });
}

void MemoryDatabase::ApplyJournalReverse(const JournalTransaction& tx)
{
    for (auto it = tx.entries.rbegin(); it != tx.entries.rend(); ++it)
    {
        ApplyEntry(*it, true);
    }
}

void MemoryDatabase::ApplyJournalForward(const JournalTransaction& tx)
{
    for (const auto& entry : tx.entries)
    {
        ApplyEntry(entry, false);
    }
}

void MemoryDatabase::ApplyEntry(const JournalEntry& entry, bool reverse)
{
    const auto tableIt = tables_.find(entry.tableName);
    if (tableIt == tables_.end())
    {
        return;
    }

    auto* table = static_cast<MemoryTable*>(tableIt->second.Get());
    auto data = table->FindRecordData(entry.recordId);
    if (!data)
    {
        data = std::make_shared<MemoryRecordData>(entry.recordId);
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

void MemoryDatabase::UpdateTouchedVersions(const JournalTransaction& tx, VersionId version)
{
    for (const auto& entry : tx.entries)
    {
        const auto tableIt = tables_.find(entry.tableName);
        if (tableIt == tables_.end())
        {
            continue;
        }

        auto record = static_cast<MemoryTable*>(tableIt->second.Get())->FindRecordData(entry.recordId);
        if (record != nullptr)
        {
            record->lastModifiedVersion = version;
        }
    }
}

SCChangeSet MemoryDatabase::BuildChangeSet(const JournalTransaction& tx, ChangeSource source, VersionId version) const
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

        SCChangeSet.changes.push_back(std::move(change));
    }

    return SCChangeSet;
}

void MemoryDatabase::NotifyObservers(const SCChangeSet& SCChangeSet)
{
    std::vector<ISCDatabaseObserver*> observers = observers_;
    for (auto* observer : observers)
    {
        if (observer != nullptr)
        {
            observer->OnDatabaseChanged(SCChangeSet);
        }
    }
}

ErrorCode MemoryRecord::ReadTypedValue(const wchar_t* name, SCValue* outValue)
{
    const ErrorCode rc = GetValue(name, outValue);
    return rc;
}

RecordId MemoryRecord::GetId() const noexcept
{
    return data_->id;
}

bool MemoryRecord::IsDeleted() const noexcept
{
    return data_->state == RecordState::Deleted;
}

VersionId MemoryRecord::GetLastModifiedVersion() const noexcept
{
    return data_->lastModifiedVersion;
}

ErrorCode MemoryRecord::GetValue(const wchar_t* name, SCValue* outValue)
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
    *outValue = (it != data_->values.end()) ? it->second : column->defaultValue;
    return outValue->IsNull() ? SC_E_VALUE_IS_NULL : SC_OK;
}

ErrorCode MemoryRecord::SetValue(const wchar_t* name, const SCValue& value)
{
    if (name == nullptr)
    {
        return SC_E_INVALIDARG;
    }
    return db_->WriteValue(table_, data_, name, value);
}

ErrorCode MemoryRecord::GetInt64(const wchar_t* name, std::int64_t* outValue)
{
    SCValue value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsInt64(outValue);
}

ErrorCode MemoryRecord::SetInt64(const wchar_t* name, std::int64_t value)
{
    return SetValue(name, SCValue::FromInt64(value));
}

ErrorCode MemoryRecord::GetDouble(const wchar_t* name, double* outValue)
{
    SCValue value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsDouble(outValue);
}

ErrorCode MemoryRecord::SetDouble(const wchar_t* name, double value)
{
    return SetValue(name, SCValue::FromDouble(value));
}

ErrorCode MemoryRecord::GetBool(const wchar_t* name, bool* outValue)
{
    SCValue value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsBool(outValue);
}

ErrorCode MemoryRecord::SetBool(const wchar_t* name, bool value)
{
    return SetValue(name, SCValue::FromBool(value));
}

ErrorCode MemoryRecord::GetString(const wchar_t* name, const wchar_t** outValue)
{
    SCValue value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsString(outValue);
}

ErrorCode MemoryRecord::GetStringCopy(const wchar_t* name, std::wstring* outValue)
{
    SCValue value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsStringCopy(outValue);
}

ErrorCode MemoryRecord::SetString(const wchar_t* name, const wchar_t* value)
{
    return SetValue(name, value == nullptr ? SCValue::Null() : SCValue::FromString(value));
}

ErrorCode MemoryRecord::GetRef(const wchar_t* name, RecordId* outValue)
{
    SCValue value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsRecordId(outValue);
}

ErrorCode MemoryRecord::SetRef(const wchar_t* name, RecordId value)
{
    return SetValue(name, SCValue::FromRecordId(value));
}

ErrorCode MemoryTable::GetRecord(RecordId id, SCRecordPtr& outRecord)
{
    auto data = FindRecordData(id);
    if (data == nullptr)
    {
        return SC_E_RECORD_NOT_FOUND;
    }

    outRecord = MakeRecord(data);
    return SC_OK;
}

ErrorCode MemoryTable::CreateRecord(SCRecordPtr& outRecord)
{
    if (!db_->HasActiveEdit())
    {
        return SC_E_NO_ACTIVE_EDIT;
    }

    auto data = std::make_shared<MemoryRecordData>(db_->AllocateRecordId());
    records_.emplace(data->id, data);
    db_->RecordCreate(this, data);
    outRecord = MakeRecord(data);
    return SC_OK;
}

ErrorCode MemoryTable::DeleteRecord(RecordId id)
{
    auto data = FindRecordData(id);
    if (data == nullptr)
    {
        return SC_E_RECORD_NOT_FOUND;
    }

    return db_->DeleteRecord(this, data);
}

ErrorCode MemoryTable::EnumerateRecords(SCRecordCursorPtr& outCursor)
{
    std::vector<SCRecordPtr> records;
    records.reserve(records_.size());
    for (const auto& [_, data] : records_)
    {
        if (data->state == RecordState::Alive)
        {
            records.push_back(MakeRecord(data));
        }
    }

    outCursor = SCMakeRef<MemoryRecordCursor>(std::move(records));
    return SC_OK;
}

ErrorCode MemoryTable::FindRecords(const SCQueryCondition& condition, SCRecordCursorPtr& outCursor)
{
    const SCColumnDef* column = Schema()->FindColumnDef(condition.fieldName);
    if (column == nullptr)
    {
        return SC_E_COLUMN_NOT_FOUND;
    }

    std::vector<SCRecordPtr> matched;
    for (const auto& [_, data] : records_)
    {
        if (data->state == RecordState::Deleted)
        {
            continue;
        }

        SCValue actual = column->defaultValue;
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

    outCursor = SCMakeRef<MemoryRecordCursor>(std::move(matched));
    return SC_OK;
}

}  // namespace

ErrorCode CreateInMemoryDatabase(SCDbPtr& outDatabase)
{
    outDatabase = SCMakeRef<MemoryDatabase>();
    return SC_OK;
}

}  // namespace StableCore::Storage
