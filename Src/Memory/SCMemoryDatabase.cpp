#include "SCBatch.h"
#include "SCFactory.h"
#include "ISCQuery.h"
#include "SCQueryMemoryExecutor.h"

#include <algorithm>
#include <map>
#include <memory>
#include <cwctype>
#include <unordered_map>
#include <typeindex>

#include "SCRefCounted.h"

namespace StableCore::Storage
{
    namespace
    {

        class MemoryDatabase;
        class MemoryTable;

        struct MemoryRecordData
        {
            explicit MemoryRecordData(RecordId newId) : id(newId)
            {
            }

            RecordId id{0};
            RecordState state{RecordState::Alive};
            VersionId lastModifiedVersion{0};
            std::unordered_map<std::wstring, SCValue> values;
        };

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

        std::wstring TrimCopy(const std::wstring& text)
        {
            const auto first = std::find_if_not(
                text.begin(), text.end(),
                [](wchar_t ch) { return std::iswspace(ch) != 0; });
            const auto last = std::find_if_not(
                text.rbegin(), text.rend(),
                [](wchar_t ch) { return std::iswspace(ch) != 0; })
                                  .base();
            if (first >= last)
            {
                return {};
            }
            return std::wstring(first, last);
        }

        bool TryParseInt64Strict(const std::wstring& text,
                                std::int64_t* outValue)
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
                normalized.push_back(
                    static_cast<wchar_t>(std::towlower(ch)));
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

        ErrorCode ConvertColumnValue(const SCValue& source,
                                     ValueKind targetKind, SCValue* outValue)
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
                            *outValue = SCValue::FromDouble(
                                static_cast<double>(intValue));
                            return SC_OK;
                        case ValueKind::String:
                            *outValue =
                                SCValue::FromString(std::to_wstring(intValue));
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
                        *outValue = SCValue::FromString(
                            boolValue ? L"true" : L"false");
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

        class MemorySchema final : public ISCSchema, public SCRefCountedObject
        {
        public:
            explicit MemorySchema(MemoryTable* table) : table_(table) {}

            ErrorCode GetColumnCount(std::int32_t* outCount) override
            {
                if (outCount == nullptr)
                {
                    return SC_E_POINTER;
                }

                *outCount = static_cast<std::int32_t>(columns_.size());
                return SC_OK;
            }

            ErrorCode GetColumn(std::int32_t index,
                                SCColumnDef* outDef) override
            {
                if (outDef == nullptr)
                {
                    return SC_E_POINTER;
                }
                if (index < 0 ||
                    static_cast<std::size_t>(index) >= columns_.size())
                {
                    return SC_E_INVALIDARG;
                }

                *outDef = columns_[static_cast<std::size_t>(index)];
                return SC_OK;
            }

            ErrorCode FindColumn(const wchar_t* name,
                                 SCColumnDef* outDef) override
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

            ErrorCode AddColumn(const SCColumnDef& def) override;

            ErrorCode UpdateColumn(const SCColumnDef& def) override;

            ErrorCode RemoveColumn(const wchar_t* name) override;

            const SCColumnDef* FindColumnDef(
                const std::wstring& name) const noexcept
            {
                const auto it = columnsByName_.find(name);
                return it == columnsByName_.end() ? nullptr : &it->second;
            }

            MemoryTable* Table() const noexcept
            {
                return table_;
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

        private:
            MemoryTable* table_{nullptr};
            std::vector<SCColumnDef> columns_;
            std::unordered_map<std::wstring, SCColumnDef> columnsByName_;
        };

        class MemoryEditSession final : public ISCEditSession,
                                        public SCRefCountedObject
        {
        public:
            MemoryEditSession(std::wstring name, VersionId version)
                : name_(std::move(name)), openedVersion_(version)
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
            MemoryRecord(MemoryDatabase* db, MemoryTable* table,
                         std::shared_ptr<MemoryRecordData> data)
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

            MemoryDatabase* db_{nullptr};
            MemoryTable* table_{nullptr};
            std::shared_ptr<MemoryRecordData> data_;
        };

        class MemoryRecordCursor final : public ISCRecordCursor,
                                         public SCRefCountedObject
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
                : db_(db),
                  name_(std::move(name)),
                  schema_(SCMakeRef<MemorySchema>(this))
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

            MemoryDatabase* Database() const noexcept
            {
                return db_;
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

            std::unordered_map<RecordId, std::shared_ptr<MemoryRecordData>>&
            Records() noexcept
            {
                return records_;
            }

        private:
            SCRecordPtr MakeRecord(
                const std::shared_ptr<MemoryRecordData>& data)
            {
                return SCMakeRef<MemoryRecord>(db_, this, data);
            }

            MemoryDatabase* db_{nullptr};
            std::wstring name_;
            SCRefPtr<MemorySchema> schema_;
            std::unordered_map<RecordId, std::shared_ptr<MemoryRecordData>>
                records_;
        };

        class MemoryDatabase final : public ISCDatabase,
                                     public IReferenceIndexProvider,
                                     public IReferenceIndexMaintainer,
                                     public SCRefCountedObject
        {
        public:
            explicit MemoryDatabase(
                SCDatabaseOpenMode openMode = SCDatabaseOpenMode::Normal)
                : openMode_(openMode)
            {
            }

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

            ErrorCode AddObserver(ISCDatabaseObserver* observer) override;
            ErrorCode RemoveObserver(ISCDatabaseObserver* observer) override;

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
            ErrorCode GetEditLogState(SCEditLogState* outState) const override;
            ErrorCode GetEditingState(
                SCEditingDatabaseState* outState) const override;
            ErrorCode ResetHistoryBaseline(
                SCBackupResult* outResult = nullptr) override;

            VersionId GetCurrentVersion() const noexcept override
            {
                return version_;
            }

            std::int32_t GetSchemaVersion() const noexcept override
            {
                return schemaVersion_;
            }

            bool HasActiveEdit() const noexcept
            {
                return static_cast<bool>(activeEdit_);
            }

            ErrorCode EnsureWritable() const;

            RecordId AllocateRecordId() noexcept
            {
                return nextRecordId_++;
            }

            ErrorCode WriteValue(MemoryTable* table,
                                 const std::shared_ptr<MemoryRecordData>& data,
                                 const std::wstring& fieldName,
                                 const SCValue& value);
            ErrorCode DeleteRecord(
                MemoryTable* table,
                const std::shared_ptr<MemoryRecordData>& data);
            void RecordCreate(MemoryTable* table,
                              const std::shared_ptr<MemoryRecordData>& data);

        private:
            friend class MemorySchema;

            struct JournalLookup
            {
                bool createdInActiveEdit{false};
                bool deletedInActiveEdit{false};
            };

            ErrorCode ValidateActiveEdit(ISCEditSession* edit) const;
            ErrorCode ValidateWrite(
                MemoryTable* table,
                const std::shared_ptr<MemoryRecordData>& data,
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
            bool RecordColumnValueMigration(
                MemoryTable* table, RecordId recordId,
                const std::wstring& fieldName, const SCValue& oldValue,
                const SCValue& newValue, ColumnKind columnKind);
            void RecordSchemaJournal(const std::wstring& tableName,
                                     const SCColumnDef& oldColumn,
                                     const SCColumnDef& newColumn,
                                     JournalOp op);
            void RecordJournal(const std::wstring& tableName, RecordId recordId,
                               const std::wstring& fieldName,
                               const SCValue& oldValue, const SCValue& newValue,
                               bool oldDeleted, bool newDeleted,
                               JournalOp forcedOp);
            void ApplyJournalReverse(const JournalTransaction& tx);
            void ApplyJournalForward(const JournalTransaction& tx);
            void ApplyEntry(const JournalEntry& entry, bool reverse);
            void UpdateTouchedVersions(const JournalTransaction& tx,
                                       VersionId version);
            SCChangeSet BuildChangeSet(const JournalTransaction& tx,
                                       ChangeSource source,
                                       VersionId version) const;
            void NotifyObservers(const SCChangeSet& SCChangeSet);

            VersionId version_{0};
            RecordId nextRecordId_{1};
            std::int32_t schemaVersion_{2};
            SCImportSessionId nextImportSessionId_{1};
            std::map<SCImportSessionId, SCImportRecoveryState> importSessions_;
            std::map<std::wstring, SCTablePtr> tables_;
            std::vector<ISCDatabaseObserver*> observers_;
            SCRefPtr<MemoryEditSession> activeEdit_;
            JournalTransaction activeJournal_;
            std::vector<JournalTransaction> undoStack_;
            std::vector<JournalTransaction> redoStack_;
            SCDatabaseOpenMode openMode_{SCDatabaseOpenMode::Normal};
            VersionId baselineVersion_{0};
            CommitId nextJournalTransactionId_{1};
            bool referenceIndexDirty_{true};
            bool referenceIndexBuilt_{false};
            VersionId referenceIndexVersion_{0};
        };

        ErrorCode MemoryDatabase::BeginEdit(const wchar_t* name,
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
            activeEdit_ = SCMakeRef<MemoryEditSession>(
                activeJournal_.actionName, version_);
            outEdit = activeEdit_;
            return SC_OK;
        }

        ErrorCode MemoryDatabase::Commit(ISCEditSession* edit)
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

            ++version_;
            UpdateTouchedVersions(activeJournal_, version_);
            activeJournal_.commitId = nextJournalTransactionId_++;
            activeJournal_.committedVersion = version_;
            undoStack_.push_back(activeJournal_);
            redoStack_.clear();
            RefreshReferenceIndexState();

            SCChangeSet SCChangeSet = BuildChangeSet(
                activeJournal_, ChangeSource::UserEdit, version_);
            activeEdit_.Reset();
            activeJournal_ = JournalTransaction{};
            NotifyObservers(SCChangeSet);
            return SC_OK;
        }

        ErrorCode MemoryDatabase::Rollback(ISCEditSession* edit)
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

        ErrorCode MemoryDatabase::Undo()
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

            JournalTransaction tx = undoStack_.back();
            undoStack_.pop_back();
            ApplyJournalReverse(tx);
            ++version_;
            UpdateTouchedVersions(tx, version_);
            redoStack_.push_back(tx);
            RefreshReferenceIndexState();
            NotifyObservers(BuildChangeSet(tx, ChangeSource::Undo, version_));
            return SC_OK;
        }

        ErrorCode MemoryDatabase::Redo()
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

            JournalTransaction tx = redoStack_.back();
            redoStack_.pop_back();
            ApplyJournalForward(tx);
            ++version_;
            UpdateTouchedVersions(tx, version_);
            undoStack_.push_back(tx);
            RefreshReferenceIndexState();
            NotifyObservers(BuildChangeSet(tx, ChangeSource::Redo, version_));
            return SC_OK;
        }

        ErrorCode MemoryDatabase::GetTable(const wchar_t* name,
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

        ErrorCode MemoryDatabase::GetTableCount(std::int32_t* outCount)
        {
            if (outCount == nullptr)
            {
                return SC_E_POINTER;
            }

            *outCount = static_cast<std::int32_t>(tables_.size());
            return SC_OK;
        }

        ErrorCode MemoryDatabase::GetTableName(std::int32_t index,
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

        ErrorCode MemoryDatabase::CreateTable(const wchar_t* name,
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

            const auto found = tables_.find(name);
            if (found != tables_.end())
            {
                outTable = found->second;
                return SC_OK;
            }

            SCTablePtr table = SCMakeRef<MemoryTable>(this, std::wstring{name});
            tables_.emplace(name, table);
            outTable = std::move(table);
            MarkReferenceIndexDirty();
            return SC_OK;
        }

        ErrorCode MemoryDatabase::ExecuteUpgradePlan(const SCUpgradePlan&, bool,
                                                     SCUpgradeResult*)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }

            return SC_E_NOTIMPL;
        }

        ErrorCode MemoryDatabase::BeginImportSession(
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

            SCImportStagingArea session;
            session.sessionId = nextImportSessionId_++;
            session.sessionName =
                options.sessionName.empty() ? L"Import" : options.sessionName;
            session.baseVersion = version_;
            session.chunkSize = options.chunkSize == 0 ? 1 : options.chunkSize;
            session.state = SCImportSessionState::Staging;

            SCImportRecoveryState recoveryState;
            recoveryState.sessionId = session.sessionId;
            recoveryState.state = SCImportSessionState::Staging;
            recoveryState.stagingArea = session;
            recoveryState.checkpoint.sessionId = session.sessionId;
            recoveryState.checkpoint.baseVersion = version_;
            recoveryState.canResume = true;
            recoveryState.canFinalize = false;
            recoveryState.reason = L"Import session staged in memory.";

            importSessions_.emplace(session.sessionId, recoveryState);
            *outSession = std::move(session);
            return SC_OK;
        }

        ErrorCode MemoryDatabase::AppendImportChunk(
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

            session->chunks.push_back(chunk);
            session->state = SCImportSessionState::Checkpointed;

            const auto it = importSessions_.find(session->sessionId);
            if (it == importSessions_.end())
            {
                return SC_E_RECORD_NOT_FOUND;
            }

            it->second.stagingArea = *session;
            it->second.state = SCImportSessionState::Checkpointed;
            it->second.checkpoint.sessionId = session->sessionId;
            it->second.checkpoint.lastChunkId = chunk.chunkId;
            it->second.checkpoint.chunkCount = session->chunks.size();
            it->second.checkpoint.baseVersion = session->baseVersion;
            it->second.checkpoint.persisted = true;
            it->second.checkpointPersisted = true;
            it->second.canResume = true;
            it->second.canFinalize = true;
            it->second.reason = L"Import checkpoint stored in memory.";

            if (outCheckpoint != nullptr)
            {
                *outCheckpoint = it->second.checkpoint;
            }
            return SC_OK;
        }

        ErrorCode MemoryDatabase::LoadImportRecoveryState(
            std::uint64_t sessionId, SCImportRecoveryState* outState)
        {
            if (outState == nullptr)
            {
                return SC_E_POINTER;
            }

            const auto it = importSessions_.find(sessionId);
            if (it == importSessions_.end())
            {
                return SC_E_RECORD_NOT_FOUND;
            }

            *outState = it->second;
            return SC_OK;
        }

        ErrorCode MemoryDatabase::FinalizeImportSession(
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

            const auto it = importSessions_.find(commit.sessionId);
            if (it == importSessions_.end())
            {
                return SC_E_RECORD_NOT_FOUND;
            }

            it->second.state = SCImportSessionState::Finalized;
            it->second.canResume = false;
            it->second.canFinalize = false;
            it->second.reason = L"Import session finalized.";
            if (outState != nullptr)
            {
                *outState = it->second;
            }
            importSessions_.erase(it);
            return SC_OK;
        }

        ErrorCode MemoryDatabase::AbortImportSession(std::uint64_t sessionId)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }

            const auto it = importSessions_.find(sessionId);
            if (it == importSessions_.end())
            {
                return SC_E_RECORD_NOT_FOUND;
            }

            importSessions_.erase(it);
            return SC_OK;
        }

        ErrorCode MemoryDatabase::CreateBackupCopy(
            const wchar_t* targetPath, const SCBackupOptions& options,
            SCBackupResult* outResult)
        {
            (void)targetPath;
            (void)options;
            (void)outResult;
            return SC_E_NOTIMPL;
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
            observers_.erase(
                std::remove(observers_.begin(), observers_.end(), observer),
                observers_.end());
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

        ErrorCode MemoryDatabase::EnsureWritable() const
        {
            return openMode_ == SCDatabaseOpenMode::ReadOnly
                       ? SC_E_READ_ONLY_DATABASE
                       : SC_OK;
        }

        ErrorCode MemoryDatabase::GetEditLogState(
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
                    tx.commitId, tx.committedVersion,
                    SCEditLogActionKind::Commit, tx.actionName, L"", 0});
            }

            outState->redoItems.reserve(redoStack_.size());
            for (const auto& tx : redoStack_)
            {
                outState->redoItems.push_back(SCEditLogEntry{
                    tx.commitId, tx.committedVersion,
                    SCEditLogActionKind::Commit, tx.actionName, L"", 0});
            }

            return SC_OK;
        }

        ErrorCode MemoryDatabase::GetEditingState(
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

        ErrorCode MemoryDatabase::ResetHistoryBaseline(
            SCBackupResult* outResult)
        {
            if (openMode_ == SCDatabaseOpenMode::ReadOnly)
            {
                return SC_E_READ_ONLY_DATABASE;
            }
            if (activeEdit_)
            {
                return SC_E_WRITE_CONFLICT;
            }

            const std::size_t removedTransactionCount = undoStack_.size();
            const std::size_t removedEntryCount = redoStack_.size();
            if (outResult != nullptr)
            {
                *outResult = SCBackupResult{};
                outResult->removedJournalTransactionCount =
                    static_cast<std::uint64_t>(removedTransactionCount);
                outResult->removedJournalEntryCount =
                    static_cast<std::uint64_t>(removedEntryCount);
            }

            baselineVersion_ = version_;
            undoStack_.clear();
            redoStack_.clear();
            return SC_OK;
        }

        ErrorCode MemoryDatabase::ValidateWrite(
            MemoryTable* table, const std::shared_ptr<MemoryRecordData>& data,
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

                    auto targetRecord =
                        static_cast<MemoryTable*>(targetIt->second.Get())
                            ->FindRecordData(refId);
                    if (!targetRecord ||
                        targetRecord->state == RecordState::Deleted)
                    {
                        return SC_E_REFERENCE_INVALID;
                    }
                }
            }

            return SC_OK;
        }

        MemoryDatabase::JournalLookup MemoryDatabase::LookupRecordJournalState(
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

        void MemoryDatabase::RemoveFieldJournalEntries(
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

        void MemoryDatabase::RemoveAllJournalEntriesForRecord(
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

        ErrorCode MemorySchema::AddColumn(const SCColumnDef& def)
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

            LoadColumn(def);
            if (table_ != nullptr && table_->Database() != nullptr &&
                table_->Database()->HasActiveEdit())
            {
                table_->Database()->RecordSchemaJournal(
                    table_->Name(), SCColumnDef{}, def, JournalOp::AddColumn);
            }
            return SC_OK;
        }

        ErrorCode MemorySchema::RemoveColumn(const wchar_t* name)
        {
            if (name == nullptr)
            {
                return SC_E_INVALIDARG;
            }

            const auto it = columnsByName_.find(name);
            if (it == columnsByName_.end())
            {
                return SC_E_COLUMN_NOT_FOUND;
            }

            const SCColumnDef removed = it->second;
            if (table_ != nullptr)
            {
                for (const auto& [_, data] : table_->Records())
                {
                    if (data != nullptr)
                    {
                        data->values.erase(name);
                    }
                }
            }
            UnloadColumn(name);
            if (table_ != nullptr && table_->Database() != nullptr &&
                table_->Database()->HasActiveEdit())
            {
                table_->Database()->RecordSchemaJournal(
                    table_->Name(), removed, SCColumnDef{},
                    JournalOp::RemoveColumn);
            }
            return SC_OK;
        }

        ErrorCode MemorySchema::UpdateColumn(const SCColumnDef& def)
        {
            const ErrorCode validate = ValidateColumnDef(def);
            if (Failed(validate))
            {
                return validate;
            }

            const auto existingIt = columnsByName_.find(def.name);
            if (existingIt == columnsByName_.end())
            {
                return SC_E_COLUMN_NOT_FOUND;
            }

            const auto vecIt =
                std::find_if(columns_.begin(), columns_.end(),
                             [&def](const SCColumnDef& existing) {
                                 return existing.name == def.name;
                             });
            if (vecIt == columns_.end())
            {
                return SC_E_COLUMN_NOT_FOUND;
            }

            const SCColumnDef previous = existingIt->second;
            if (previous.valueKind != def.valueKind)
            {
                if (table_ == nullptr)
                {
                    return SC_E_FAIL;
                }

                struct ColumnValueUpdate
                {
                    RecordId recordId{0};
                    SCValue oldValue;
                    SCValue newValue;
                };

                std::vector<ColumnValueUpdate> updates;
                updates.reserve(table_->Records().size());
                for (const auto& [recordId, data] : table_->Records())
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
                    const ErrorCode convertRc =
                        ConvertColumnValue(valueIt->second, def.valueKind,
                                           &converted);
                    if (Failed(convertRc))
                    {
                        return convertRc;
                    }

                    updates.push_back(ColumnValueUpdate{
                        recordId, valueIt->second, std::move(converted)});
                }

                for (const auto& update : updates)
                {
                    auto data = table_->FindRecordData(update.recordId);
                    if (data == nullptr)
                    {
                        continue;
                    }

                    const auto valueIt = data->values.find(def.name);
                    if (valueIt == data->values.end())
                    {
                        continue;
                    }

                    if (update.newValue.IsNull())
                    {
                        data->values.erase(valueIt);
                    } else
                    {
                        valueIt->second = update.newValue;
                    }

                    if (table_->Database() != nullptr)
                    {
                        table_->Database()->RecordColumnValueMigration(
                            table_, update.recordId, def.name, update.oldValue,
                            update.newValue, def.columnKind);
                    }
                }
            }

            ReplaceColumn(def);
            if (table_ != nullptr && table_->Database() != nullptr &&
                table_->Database()->HasActiveEdit())
            {
                table_->Database()->RecordSchemaJournal(
                    table_->Name(), previous, def, JournalOp::UpdateColumn);
            }
            return SC_OK;
        }

        bool MemoryDatabase::RecordColumnValueMigration(
            MemoryTable* table, RecordId recordId,
            const std::wstring& fieldName, const SCValue& oldValue,
            const SCValue& newValue, ColumnKind columnKind)
        {
            if (table == nullptr || !HasActiveEdit())
            {
                return false;
            }

            RecordJournal(table->Name(), recordId, fieldName, oldValue, newValue,
                          false, false,
                          columnKind == ColumnKind::Relation
                              ? JournalOp::SetRelation
                              : JournalOp::SetValue);
            return true;
        }

        void MemoryDatabase::RecordSchemaJournal(
            const std::wstring& tableName, const SCColumnDef& oldColumn,
            const SCColumnDef& newColumn, JournalOp op)
        {
            if (!HasActiveEdit())
            {
                return;
            }

            const std::wstring& fieldName =
                !newColumn.name.empty() ? newColumn.name : oldColumn.name;
            for (auto& entry : activeJournal_.entries)
            {
                if (entry.op == op && entry.tableName == tableName &&
                    entry.fieldName == fieldName)
                {
                    entry.oldColumn = oldColumn;
                    entry.newColumn = newColumn;
                    return;
                }
            }

            activeJournal_.entries.push_back(JournalEntry{
                op,
                tableName,
                0,
                fieldName,
                SCValue::Null(),
                SCValue::Null(),
                false,
                false,
                oldColumn,
                newColumn,
            });
        }

        ErrorCode MemoryDatabase::WriteValue(
            MemoryTable* table, const std::shared_ptr<MemoryRecordData>& data,
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

        ErrorCode MemoryDatabase::DeleteRecord(
            MemoryTable* table, const std::shared_ptr<MemoryRecordData>& data)
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

        ErrorCode MemoryDatabase::ClearColumnValues(ISCTable* table,
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

            auto* memoryTable = static_cast<MemoryTable*>(table);
            SCSchemaPtr schema;
            const ErrorCode schemaRc = memoryTable->GetSchema(schema);
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
            for (const auto& [recordId, data] : memoryTable->Records())
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
                RecordJournal(memoryTable->Name(), recordId, name, oldValue,
                              SCValue::Null(), false, false,
                              relationColumn ? JournalOp::SetRelation
                                             : JournalOp::SetValue);
            }

            MarkReferenceIndexDirty();
            return SC_OK;
        }

        ErrorCode MemoryDatabase::GetReferencesBySource(
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

            auto* table = static_cast<MemoryTable*>(tableIt->second.Get());
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

        ErrorCode MemoryDatabase::GetReferencesByTarget(
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

        ErrorCode MemoryDatabase::CheckReferenceIndex(
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

        ErrorCode MemoryDatabase::GetAllReferencesDiagnosticOnly(
            ReferenceIndex* outIndex) const
        {
            if (outIndex == nullptr)
            {
                return SC_E_POINTER;
            }

            outIndex->records.clear();
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

        bool MemoryDatabase::IsRecordReferenced(const std::wstring& tableName,
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

        void MemoryDatabase::RecordCreate(
            MemoryTable* table, const std::shared_ptr<MemoryRecordData>& data)
        {
            RecordJournal(table->Name(), data->id, L"", SCValue::Null(),
                          SCValue::Null(), true, false,
                          JournalOp::CreateRecord);
            MarkReferenceIndexDirty();
        }

        void MemoryDatabase::RecordJournal(const std::wstring& tableName,
                                           RecordId recordId,
                                           const std::wstring& fieldName,
                                           const SCValue& oldValue,
                                           const SCValue& newValue,
                                           bool oldDeleted, bool newDeleted,
                                           JournalOp forcedOp)
        {
            for (auto& entry : activeJournal_.entries)
            {
                if (entry.op == forcedOp && entry.tableName == tableName &&
                    entry.recordId == recordId && entry.fieldName == fieldName)
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
            switch (entry.op)
            {
                case JournalOp::AddColumn: {
                    auto* schema = table->Schema();
                    if (schema == nullptr)
                    {
                        break;
                    }
                    if (reverse)
                    {
                        schema->UnloadColumn(entry.fieldName.c_str());
                    } else
                    {
                        SCColumnDef def = entry.newColumn;
                        if (def.name.empty())
                        {
                            def.name = entry.fieldName;
                        }
                        schema->LoadColumn(def);
                    }
                    break;
                }
                case JournalOp::CreateRecord:
                case JournalOp::DeleteRecord:
                case JournalOp::SetRelation:
                case JournalOp::SetValue: {
                    auto data = table->FindRecordData(entry.recordId);
                    if (!data)
                    {
                        data = std::make_shared<MemoryRecordData>(
                            entry.recordId);
                        table->Records().emplace(entry.recordId, data);
                    }

                    switch (entry.op)
                    {
                        case JournalOp::CreateRecord:
                        case JournalOp::DeleteRecord:
                            data->state = reverse
                                              ? (entry.oldDeleted
                                                     ? RecordState::Deleted
                                                     : RecordState::Alive)
                                              : (entry.newDeleted
                                                     ? RecordState::Deleted
                                                     : RecordState::Alive);
                            if (reverse &&
                                entry.op == JournalOp::CreateRecord)
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
                                    data->values[entry.fieldName] =
                                        entry.oldValue;
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
                case JournalOp::UpdateColumn: {
                    auto* schema = table->Schema();
                    if (schema == nullptr)
                    {
                        break;
                    }
                    SCColumnDef def = reverse ? entry.oldColumn
                                              : entry.newColumn;
                    if (def.name.empty())
                    {
                        def.name = entry.fieldName;
                    }
                    if (reverse)
                    {
                        schema->ReplaceColumn(def);
                    } else
                    {
                        schema->ReplaceColumn(def);
                    }
                    break;
                }
                case JournalOp::RemoveColumn: {
                    auto* schema = table->Schema();
                    if (schema == nullptr)
                    {
                        break;
                    }
                    if (reverse)
                    {
                        SCColumnDef def = entry.oldColumn;
                        if (def.name.empty())
                        {
                            def.name = entry.fieldName;
                        }
                        schema->LoadColumn(def);
                    } else
                    {
                        for (const auto& [_, data] : table->Records())
                        {
                            if (data != nullptr)
                            {
                                data->values.erase(entry.fieldName);
                            }
                        }
                        schema->UnloadColumn(entry.fieldName.c_str());
                    }
                    break;
                }
                default:
                    break;
            }
        }

        void MemoryDatabase::UpdateTouchedVersions(const JournalTransaction& tx,
                                                   VersionId version)
        {
            for (const auto& entry : tx.entries)
            {
                const auto tableIt = tables_.find(entry.tableName);
                if (tableIt == tables_.end())
                {
                    continue;
                }

                auto record = static_cast<MemoryTable*>(tableIt->second.Get())
                                  ->FindRecordData(entry.recordId);
                if (record != nullptr)
                {
                    record->lastModifiedVersion = version;
                }
            }
        }

        SCChangeSet MemoryDatabase::BuildChangeSet(const JournalTransaction& tx,
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
                     entry.op == JournalOp::DeleteRecord ||
                     entry.op == JournalOp::AddColumn ||
                     entry.op == JournalOp::UpdateColumn ||
                     entry.op == JournalOp::RemoveColumn);
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
                    case JournalOp::AddColumn:
                    case JournalOp::UpdateColumn:
                    case JournalOp::RemoveColumn:
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

        ErrorCode MemoryRecord::ReadTypedValue(const wchar_t* name,
                                               SCValue* outValue)
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
            *outValue =
                (it != data_->values.end()) ? it->second : column->defaultValue;
            return outValue->IsNull() ? SC_E_VALUE_IS_NULL : SC_OK;
        }

        ErrorCode MemoryRecord::SetValue(const wchar_t* name,
                                         const SCValue& value)
        {
            if (name == nullptr)
            {
                return SC_E_INVALIDARG;
            }
            return db_->WriteValue(table_, data_, name, value);
        }

        ErrorCode MemoryRecord::GetInt64(const wchar_t* name,
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

        ErrorCode MemoryRecord::SetInt64(const wchar_t* name,
                                         std::int64_t value)
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

        ErrorCode MemoryRecord::GetString(const wchar_t* name,
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

        ErrorCode MemoryRecord::GetStringCopy(const wchar_t* name,
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

        ErrorCode MemoryRecord::SetString(const wchar_t* name,
                                          const wchar_t* value)
        {
            return SetValue(name, value == nullptr
                                      ? SCValue::Null()
                                      : SCValue::FromString(value));
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

            auto data =
                std::make_shared<MemoryRecordData>(db_->AllocateRecordId());
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

        ErrorCode MemoryTable::FindRecords(const SCQueryCondition& condition,
                                           SCRecordCursorPtr& outCursor)
        {
            QueryPlan legacyPlan;
            const ErrorCode bridgeRc =
                SCQueryBridge::BuildPlanFromLegacyFindRecords(Name(), condition,
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
            context.backendKind = QueryBackendKind::Memory;
            context.database = db_;
            context.backendHandle = db_;
            context.resultCursor = &outCursor;

            QueryExecutionResult executionResult;
            return ExecuteQueryPlan(executablePlan, context, &executionResult);
        }

    }  // namespace

    ErrorCode CreateInMemoryDatabase(const SCOpenDatabaseOptions& options,
                                     SCDbPtr& outDatabase)
    {
        outDatabase = SCMakeRef<MemoryDatabase>(options.openMode);
        RegisterQueryExecutionContextDispatch(QueryBackendKind::Memory,
                                              &ExecuteMemoryQueryDispatch);
        return SC_OK;
    }

    ErrorCode CreateInMemoryDatabase(SCDbPtr& outDatabase)
    {
        return CreateInMemoryDatabase(SCOpenDatabaseOptions{}, outDatabase);
    }

    ErrorCode MemoryDatabase::RebuildReferenceIndexes()
    {
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

    ErrorCode MemoryDatabase::CommitReferenceDelta(
        const ReferenceIndex& forwardDelta,
        const ReverseReferenceIndex& reverseDelta)
    {
        if (forwardDelta.records.size() != reverseDelta.records.size())
        {
            return SC_E_INVALIDARG;
        }

        return RebuildReferenceIndexes();
    }

    void MemoryDatabase::MarkReferenceIndexDirty() noexcept
    {
        referenceIndexDirty_ = true;
    }

    void MemoryDatabase::RefreshReferenceIndexState()
    {
        const ErrorCode rc = RebuildReferenceIndexes();
        if (Failed(rc))
        {
            referenceIndexBuilt_ = false;
            referenceIndexVersion_ = 0;
        }
    }

}  // namespace StableCore::Storage
