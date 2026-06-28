#pragma once

#include "Sqlite/SqliteImplCore.h"

namespace StableCore::Storage
{

// === SqliteEditSession ===
class SqliteEditSession final : public ISCEditSession, public SCRefCountedObject
{
public:
    SqliteEditSession(std::wstring name, VersionId openedVersion)
        : name_(std::move(name)), openedVersion_(openedVersion) {}

    const wchar_t* GetName() const override { return name_.c_str(); }
    EditState GetState() const noexcept override { return state_; }
    VersionId GetOpenedVersion() const noexcept override { return openedVersion_; }

    void SetState(EditState state) noexcept { state_ = state; }

private:
    std::wstring name_;
    VersionId openedVersion_{0};
    EditState state_{EditState::Active};
};

// === SqliteRecord ===
class SqliteRecord final : public ISCRecord, public SCRefCountedObject
{
public:
    SqliteRecord(SqliteDatabase* db, SqliteTable* table, std::shared_ptr<SqliteRecordData> data)
        : db_(db), table_(table), data_(std::move(data)) {}

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

// === SqliteRecordCursor ===
class SqliteRecordCursor final : public ISCRecordCursor, public SCRefCountedObject
{
public:
    explicit SqliteRecordCursor(std::vector<SCRecordPtr> records) : records_(std::move(records)) {}

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

// === SqliteTable ===
class SqliteTable final : public ISCTable, public SCRefCountedObject
{
public:
    SqliteTable(SqliteDatabase* db, std::wstring name, std::int64_t tableRowId)
        : db_(db), name_(std::move(name)), schema_(SCMakeRef<SqliteSchema>(db, name_, tableRowId)) {}

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

    const std::wstring& Name() const noexcept { return name_; }
    void SetName(std::wstring name) { name_ = std::move(name); }
    SqliteSchema* Schema() const noexcept { return schema_.Get(); }
    std::int64_t TableRowId() const noexcept { return schema_->TableRowId(); }

    std::shared_ptr<SqliteRecordData> FindRecordData(RecordId id) const
    {
        const auto it = records_.find(id);
        return it == records_.end() ? nullptr : it->second;
    }

    std::unordered_map<RecordId, std::shared_ptr<SqliteRecordData>>& Records() noexcept { return records_; }

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

// === Foreign key reference entry ===
struct ForeignKeyReferenceEntry
{
    std::wstring sourceTableName;
    SCConstraintDef constraint;
};

// === SqliteDatabase ===
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
    ErrorCode RenameTable(const wchar_t* originalName, const wchar_t* newName) override;
    ErrorCode AddObserver(ISCDatabaseObserver* observer) override;
    ErrorCode RemoveObserver(ISCDatabaseObserver* observer) override;
    ErrorCode GetLastConstraintViolationInfo(SCConstraintViolationInfo* outInfo) const override;
    ErrorCode ExecuteUpgradePlan(const SCUpgradePlan& plan, bool confirmed, SCUpgradeResult* outResult) override;
    ErrorCode BeginImportSession(const SCImportSessionOptions& options, SCImportStagingArea* outSession) override;
    ErrorCode AppendImportChunk(SCImportStagingArea* session,
                                const SCImportChunk& chunk,
                                SCImportCheckpoint* outCheckpoint) override;
    ErrorCode LoadImportRecoveryState(std::uint64_t sessionId, SCImportRecoveryState* outState) override;
    ErrorCode FinalizeImportSession(const SCImportFinalizeCommit& commit, SCImportRecoveryState* outState) override;
    ErrorCode AbortImportSession(std::uint64_t sessionId) override;
    ErrorCode CreateBackupCopy(const wchar_t* targetPath,
                               const SCBackupOptions& options,
                               SCBackupResult* outResult) override;
    ErrorCode ClearColumnValues(ISCTable* table, const wchar_t* name) override;
    ErrorCode ResetHistoryBaseline(SCBackupResult* outResult = nullptr) override;
    ErrorCode GetEditLogState(SCEditLogState* outState) const override;
    ErrorCode GetEditingState(SCEditingDatabaseState* outState) const override;
    VersionId GetCurrentVersion() const noexcept override { return version_; }
    std::int32_t GetSchemaVersion() const noexcept override { return schemaVersion_; }
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
    ErrorCode ExecuteSql(const char* sql) override { return db_.Execute(sql); }
    bool HasTableColumn(const char* tableName, const char* columnName) const override
    {
        return HasTableColumnRaw(db_.Raw(), tableName, columnName);
    }
    ErrorCode BackfillSchemaMetadataV3() override;
    void InitializeQueryIndexStorage() override;

    friend class SqliteSchema;
    friend class SqliteTable;

    bool HasActiveEdit() const noexcept { return static_cast<bool>(activeEdit_); }
    bool IsCleanShutdown() const noexcept { return cleanShutdown_; }
    void SuppressCleanShutdownOnDestroy() noexcept { persistCleanShutdownOnDestroy_ = false; }
    RecordId AllocateRecordId() noexcept { return nextRecordId_++; }

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
    ErrorCode EnsureLegacyColumnIndexes();
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
        const JournalTransaction& tx, bool reverseRenameResolution = false) const;
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
        const JournalTransaction& tx, bool reverseRenameResolution = false) const;
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
    DeferredRenameState* FindDeferredRenameState(const std::wstring& oldName, const std::wstring& newName);
    const DeferredRenameState* FindDeferredRenameState(const std::wstring& oldName,
                                                       const std::wstring& newName) const;
    ErrorCode RecordDeferredRenameTable(const std::wstring& oldName, const std::wstring& newName);
    bool JournalTransactionContainsRenameTable(const JournalTransaction& tx) const;
    std::wstring ResolveJournalTableNameToReplayState(const JournalTransaction& tx,
                                                      const std::wstring& tableName,
                                                      bool reverseRenameResolution) const;
    bool JournalEntryMatchesCurrentTableName(const JournalEntry& entry, const std::wstring& tableName) const;
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
    void RecordTableRenameJournal(const std::wstring& originalName, const std::wstring& newName);
    ErrorCode PersistTableRename(const std::wstring& originalName,
                                 const std::wstring& newName,
                                 bool recordJournal,
                                 bool manageTransaction);
    ErrorCode PersistDeferredRenameToStorage(const DeferredRenameState& state);
    ErrorCode PersistDeferredSchemaOps();
    ErrorCode PersistSchemaAddColumn(SqliteSchema* schema, const SCColumnDef& def, std::int64_t rowId = -1);
    ErrorCode PersistSchemaUpdateColumn(SqliteSchema* schema, const SCColumnDef& def);
    ErrorCode PersistSchemaRemoveColumn(SqliteSchema* schema, const wchar_t* columnName);
    ErrorCode PersistSchemaAddConstraint(SqliteSchema* schema, const SCConstraintDef& def, std::int64_t rowId = -1);
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
    ErrorCode FinalizeReplayFailure(ErrorCode primaryRc, ErrorCode compensationRc);
    void ClearReplayCompensationFailure() noexcept;
    ErrorCode ApplyEntry(const JournalEntry& entry, bool reverse);
    ErrorCode ApplySchemaEntry(const JournalEntry& entry, bool reverse);
    void UpdateTouchedVersions(const JournalTransaction& tx,
                               VersionId version,
                               bool reverseRenameResolution = false);
    SCChangeSet BuildChangeSet(const JournalTransaction& tx, ChangeSource source, VersionId version) const;
    void NotifyObservers(const SCChangeSet& changeSet);
    std::vector<std::pair<std::wstring, RecordId>> GetTouchedRecordKeys(
        const JournalTransaction& tx, bool reverseRenameResolution = false) const;
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
    bool replayCompensationFailureDetected_{false};
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

} // namespace StableCore::Storage
