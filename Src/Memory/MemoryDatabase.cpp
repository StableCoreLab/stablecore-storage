#include "StableCore/Storage/Factory.h"

#include <algorithm>
#include <map>
#include <memory>
#include <unordered_map>

#include "StableCore/Storage/RefCounted.h"

namespace stablecore::storage
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
    std::unordered_map<std::wstring, Value> values;
};

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

    if (def.columnKind == ColumnKind::Relation)
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
    else if (!def.defaultValue.IsNull() && def.defaultValue.GetKind() != def.valueKind)
    {
        return SC_E_SCHEMA_VIOLATION;
    }

    if (!def.nullable && def.defaultValue.IsNull())
    {
        return SC_E_SCHEMA_VIOLATION;
    }

    return SC_OK;
}

class MemorySchema final : public ISchema, public RefCountedObject
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

    ErrorCode GetColumn(std::int32_t index, ColumnDef* outDef) override
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

    ErrorCode FindColumn(const wchar_t* name, ColumnDef* outDef) override
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

    ErrorCode AddColumn(const ColumnDef& def) override
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

        columns_.push_back(def);
        columnsByName_.emplace(def.name, def);
        return SC_OK;
    }

    const ColumnDef* FindColumnDef(const std::wstring& name) const noexcept
    {
        const auto it = columnsByName_.find(name);
        return it == columnsByName_.end() ? nullptr : &it->second;
    }

private:
    std::vector<ColumnDef> columns_;
    std::unordered_map<std::wstring, ColumnDef> columnsByName_;
};

class MemoryEditSession final : public IEditSession, public RefCountedObject
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

class MemoryRecord final : public IRecord, public RefCountedObject
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

    MemoryDatabase* db_{nullptr};
    MemoryTable* table_{nullptr};
    std::shared_ptr<MemoryRecordData> data_;
};

class MemoryRecordCursor final : public IRecordCursor, public RefCountedObject
{
public:
    explicit MemoryRecordCursor(std::vector<RecordPtr> records)
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

class MemoryTable final : public ITable, public RefCountedObject
{
public:
    MemoryTable(MemoryDatabase* db, std::wstring name)
        : db_(db)
        , name_(std::move(name))
        , schema_(MakeRef<MemorySchema>())
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
    RecordPtr MakeRecord(const std::shared_ptr<MemoryRecordData>& data)
    {
        return MakeRef<MemoryRecord>(db_, this, data);
    }

    MemoryDatabase* db_{nullptr};
    std::wstring name_;
    RefPtr<MemorySchema> schema_;
    std::unordered_map<RecordId, std::shared_ptr<MemoryRecordData>> records_;
};

class MemoryDatabase final : public IDatabase, public RefCountedObject
{
public:
    ErrorCode BeginEdit(const wchar_t* name, EditPtr& outEdit) override;
    ErrorCode Commit(IEditSession* edit) override;
    ErrorCode Rollback(IEditSession* edit) override;

    ErrorCode Undo() override;
    ErrorCode Redo() override;

    ErrorCode GetTable(const wchar_t* name, TablePtr& outTable) override;
    ErrorCode CreateTable(const wchar_t* name, TablePtr& outTable) override;

    ErrorCode AddObserver(IDatabaseObserver* observer) override;
    ErrorCode RemoveObserver(IDatabaseObserver* observer) override;

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

    ErrorCode WriteValue(MemoryTable* table, const std::shared_ptr<MemoryRecordData>& data, const std::wstring& fieldName, const Value& value);
    ErrorCode DeleteRecord(MemoryTable* table, const std::shared_ptr<MemoryRecordData>& data);
    void RecordCreate(MemoryTable* table, const std::shared_ptr<MemoryRecordData>& data);

private:
    struct JournalLookup
    {
        bool createdInActiveEdit{false};
        bool deletedInActiveEdit{false};
    };

    ErrorCode ValidateActiveEdit(IEditSession* edit) const;
    ErrorCode ValidateWrite(MemoryTable* table, const std::shared_ptr<MemoryRecordData>& data, const std::wstring& fieldName, const Value& value);
    JournalLookup LookupRecordJournalState(const std::wstring& tableName, RecordId recordId) const;
    void RemoveFieldJournalEntries(const std::wstring& tableName, RecordId recordId);
    void RemoveAllJournalEntriesForRecord(const std::wstring& tableName, RecordId recordId);
    void RecordJournal(
        const std::wstring& tableName,
        RecordId recordId,
        const std::wstring& fieldName,
        const Value& oldValue,
        const Value& newValue,
        bool oldDeleted,
        bool newDeleted,
        JournalOp forcedOp);
    void ApplyJournalReverse(const JournalTransaction& tx);
    void ApplyJournalForward(const JournalTransaction& tx);
    void ApplyEntry(const JournalEntry& entry, bool reverse);
    void UpdateTouchedVersions(const JournalTransaction& tx, VersionId version);
    ChangeSet BuildChangeSet(const JournalTransaction& tx, ChangeSource source, VersionId version) const;
    void NotifyObservers(const ChangeSet& changeSet);

    VersionId version_{0};
    RecordId nextRecordId_{1};
    std::map<std::wstring, TablePtr> tables_;
    std::vector<IDatabaseObserver*> observers_;
    RefPtr<MemoryEditSession> activeEdit_;
    JournalTransaction activeJournal_;
    std::vector<JournalTransaction> undoStack_;
    std::vector<JournalTransaction> redoStack_;
};

ErrorCode MemoryDatabase::BeginEdit(const wchar_t* name, EditPtr& outEdit)
{
    if (activeEdit_)
    {
        return SC_E_WRITE_CONFLICT;
    }

    activeJournal_ = JournalTransaction{};
    activeJournal_.actionName = (name != nullptr && *name != L'\0') ? name : L"Edit";
    activeEdit_ = MakeRef<MemoryEditSession>(activeJournal_.actionName, version_);
    outEdit = activeEdit_;
    return SC_OK;
}

ErrorCode MemoryDatabase::Commit(IEditSession* edit)
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

    ChangeSet changeSet = BuildChangeSet(activeJournal_, ChangeSource::UserEdit, version_);
    activeEdit_.Reset();
    activeJournal_ = JournalTransaction{};
    NotifyObservers(changeSet);
    return SC_OK;
}

ErrorCode MemoryDatabase::Rollback(IEditSession* edit)
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

ErrorCode MemoryDatabase::GetTable(const wchar_t* name, TablePtr& outTable)
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

ErrorCode MemoryDatabase::CreateTable(const wchar_t* name, TablePtr& outTable)
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

    TablePtr table = MakeRef<MemoryTable>(this, std::wstring{name});
    tables_.emplace(name, table);
    outTable = std::move(table);
    return SC_OK;
}

ErrorCode MemoryDatabase::AddObserver(IDatabaseObserver* observer)
{
    if (observer == nullptr)
    {
        return SC_E_POINTER;
    }

    observers_.push_back(observer);
    return SC_OK;
}

ErrorCode MemoryDatabase::RemoveObserver(IDatabaseObserver* observer)
{
    observers_.erase(std::remove(observers_.begin(), observers_.end(), observer), observers_.end());
    return SC_OK;
}

ErrorCode MemoryDatabase::ValidateActiveEdit(IEditSession* edit) const
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
    const Value& value)
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
    const Value& value)
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

void MemoryDatabase::RecordCreate(MemoryTable* table, const std::shared_ptr<MemoryRecordData>& data)
{
    RecordJournal(table->Name(), data->id, L"", Value::Null(), Value::Null(), true, false, JournalOp::CreateRecord);
}

void MemoryDatabase::RecordJournal(
    const std::wstring& tableName,
    RecordId recordId,
    const std::wstring& fieldName,
    const Value& oldValue,
    const Value& newValue,
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

ChangeSet MemoryDatabase::BuildChangeSet(const JournalTransaction& tx, ChangeSource source, VersionId version) const
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

void MemoryDatabase::NotifyObservers(const ChangeSet& changeSet)
{
    std::vector<IDatabaseObserver*> observers = observers_;
    for (auto* observer : observers)
    {
        if (observer != nullptr)
        {
            observer->OnDatabaseChanged(changeSet);
        }
    }
}

ErrorCode MemoryRecord::ReadTypedValue(const wchar_t* name, Value* outValue)
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

ErrorCode MemoryRecord::GetValue(const wchar_t* name, Value* outValue)
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

ErrorCode MemoryRecord::SetValue(const wchar_t* name, const Value& value)
{
    if (name == nullptr)
    {
        return SC_E_INVALIDARG;
    }
    return db_->WriteValue(table_, data_, name, value);
}

ErrorCode MemoryRecord::GetInt64(const wchar_t* name, std::int64_t* outValue)
{
    Value value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsInt64(outValue);
}

ErrorCode MemoryRecord::SetInt64(const wchar_t* name, std::int64_t value)
{
    return SetValue(name, Value::FromInt64(value));
}

ErrorCode MemoryRecord::GetDouble(const wchar_t* name, double* outValue)
{
    Value value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsDouble(outValue);
}

ErrorCode MemoryRecord::SetDouble(const wchar_t* name, double value)
{
    return SetValue(name, Value::FromDouble(value));
}

ErrorCode MemoryRecord::GetBool(const wchar_t* name, bool* outValue)
{
    Value value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsBool(outValue);
}

ErrorCode MemoryRecord::SetBool(const wchar_t* name, bool value)
{
    return SetValue(name, Value::FromBool(value));
}

ErrorCode MemoryRecord::GetString(const wchar_t* name, const wchar_t** outValue)
{
    Value value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsString(outValue);
}

ErrorCode MemoryRecord::GetStringCopy(const wchar_t* name, std::wstring* outValue)
{
    Value value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsStringCopy(outValue);
}

ErrorCode MemoryRecord::SetString(const wchar_t* name, const wchar_t* value)
{
    return SetValue(name, value == nullptr ? Value::Null() : Value::FromString(value));
}

ErrorCode MemoryRecord::GetRef(const wchar_t* name, RecordId* outValue)
{
    Value value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsRecordId(outValue);
}

ErrorCode MemoryRecord::SetRef(const wchar_t* name, RecordId value)
{
    return SetValue(name, Value::FromRecordId(value));
}

ErrorCode MemoryTable::GetRecord(RecordId id, RecordPtr& outRecord)
{
    auto data = FindRecordData(id);
    if (data == nullptr)
    {
        return SC_E_RECORD_NOT_FOUND;
    }

    outRecord = MakeRecord(data);
    return SC_OK;
}

ErrorCode MemoryTable::CreateRecord(RecordPtr& outRecord)
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

ErrorCode MemoryTable::EnumerateRecords(RecordCursorPtr& outCursor)
{
    std::vector<RecordPtr> records;
    records.reserve(records_.size());
    for (const auto& [_, data] : records_)
    {
        if (data->state == RecordState::Alive)
        {
            records.push_back(MakeRecord(data));
        }
    }

    outCursor = MakeRef<MemoryRecordCursor>(std::move(records));
    return SC_OK;
}

ErrorCode MemoryTable::FindRecords(const QueryCondition& condition, RecordCursorPtr& outCursor)
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

    outCursor = MakeRef<MemoryRecordCursor>(std::move(matched));
    return SC_OK;
}

}  // namespace

ErrorCode CreateInMemoryDatabase(DbPtr& outDatabase)
{
    outDatabase = MakeRef<MemoryDatabase>();
    return SC_OK;
}

}  // namespace stablecore::storage
