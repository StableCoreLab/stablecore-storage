#include "Sqlite/SqliteImplCore.h"
#include "Sqlite/SqliteImplDatabase.h"
#include "SCSchemaEdit.h"

namespace StableCore::Storage
{
        ErrorCode SqliteDatabase::BeginEdit(const wchar_t* name, SCEditPtr& outEdit)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            // A poisoned edit must be closed by Rollback() before a new edit
            // can be started.
            if (activeEditPoisoned_)
            {
                return SC_E_WRITE_CONFLICT;
            }
            const ErrorCode transitionRc = EnsureWriteTransitionAllowed();
            if (Failed(transitionRc))
            {
                return transitionRc;
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
            // A poisoned edit may only be closed through Rollback().
            if (activeEditPoisoned_)
            {
                return SC_E_WRITE_CONFLICT;
            }
            const ErrorCode transitionRc = EnsureWriteTransitionAllowed();
            if (Failed(transitionRc))
            {
                return transitionRc;
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

            // Phase 1: final constraint boundary. It runs entirely on the
            // in-memory candidate state and never touches SQLite, so a rejected
            // candidate leaves the edit Active and the caller can fix the
            // values and Commit() again.
            const ErrorCode candidateRc = ValidateCommitCandidateState();
            if (Failed(candidateRc))
            {
                return candidateRc;
            }

            // Phase 2: persistence attempt. Every failure - error code,
            // exception or txn.Commit() - leaves through the single
            // FinalizeCommitFailure() exit so memory is never left in a state
            // that disagrees with storage.
            const ErrorCode persistRc = CommitPersistencePhase();
            if (Failed(persistRc))
            {
                return persistRc;
            }

            RefreshReferenceIndexState();
            const SCChangeSet SCChangeSet = BuildChangeSet(activeJournal_, ChangeSource::UserEdit, version_);
            activeEdit_.Reset();
            activeJournal_ = JournalTransaction{};
            publicStateTransitionActive_ = true;
            struct Guard
            {
                SqliteDatabase* db;
                ~Guard() { db->publicStateTransitionActive_ = false; }
            } guard{this};
            NotifyObservers(SCChangeSet);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::CommitPersistencePhase()
        {
            const VersionId committedVersion = version_ + 1;
            JournalTransaction pendingJournal = activeJournal_;

            try
            {
                SqliteTxn txn(db_);
                // Fault injection: simulate a persistence failure after the
                // transaction has already been opened. FinalizeCommitFailure()
                // decides whether the edit stays usable or becomes Poisoned.
                if (journalFailureInjection_.operation == JournalFailureOperation::Commit)
                {
                    return FinalizeCommitFailure(SC_E_IO_ERROR, pendingJournal);
                }
                const ErrorCode schemaRc = PersistDeferredSchemaOps();
                if (Failed(schemaRc))
                {
                    return FinalizeCommitFailure(schemaRc, pendingJournal);
                }

                PersistTouchedRecords(pendingJournal,
                                      committedVersion,
                                      false);
                DeleteRedoJournalRows();
                pendingJournal.committedVersion = committedVersion;
                const std::int64_t txId =
                    InsertJournalTransaction(pendingJournal, kStackUndo, static_cast<int>(undoStack_.size()));
                pendingJournal.commitId = static_cast<CommitId>(txId);
                PersistJournalEntries(txId, pendingJournal);
                SaveMetadata(committedVersion);
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    return FinalizeCommitFailure(commitRc, pendingJournal);
                }

                version_ = committedVersion;
                UpdateTouchedVersions(pendingJournal, version_, false);
                undoStack_.push_back(SqlitePersistedJournalTransaction{txId, pendingJournal});
                redoStack_.clear();
                activeEdit_->SetState(EditState::Committed);
                activeJournal_ = pendingJournal;
                activeSchemaOps_.clear();
                return SC_OK;
            } catch (...)
            {
                return FinalizeCommitFailure(SC_E_FAIL, pendingJournal);
            }
        }

        ErrorCode SqliteDatabase::FinalizeCommitFailure(ErrorCode primaryRc,
                                                        const JournalTransaction& pendingJournal)
        {
            // Nothing in the persistence phase is allowed to change the
            // in-memory model before txn.Commit() succeeds: version, undo stack
            // and the active journal are only touched afterwards. The only
            // thing left to prove is that SQLite really left the transaction,
            // because a still open transaction would mean storage may hold part
            // of the candidate while memory does not.
            bool restored = false;
            if (db_.Raw() != nullptr)
            {
                if (sqlite3_get_autocommit(db_.Raw()) != 0)
                {
                    // No transaction is active: SQLite already ended it (a
                    // failed COMMIT is atomic, it never leaves a partial write).
                    restored = true;
                } else
                {
                    // The transaction is still open (statement error before
                    // COMMIT). Roll it back right here instead of relying on the
                    // SqliteTxn destructor, so the outcome can be verified.
                    const ErrorCode rollbackRc = db_.Execute("ROLLBACK;");
                    restored = Succeeded(rollbackRc) && sqlite3_get_autocommit(db_.Raw()) != 0;
                    // Fault injection: force the recovery ROLLBACK to be
                    // considered failed, which drives the edit into Poisoned.
                    if (journalFailureInjection_.failCommitRollback)
                    {
                        restored = false;
                    }
                }
            }

            if (restored)
            {
                try
                {
                    // Rebuild the derived state (reference index) from the
                    // unchanged candidate records and verify the journal is
                    // still the one this attempt started from.
                    RefreshReferenceIndexState();
                    restored = !replayCompensationFailureDetected_ &&
                               pendingJournal.entries.size() == activeJournal_.entries.size();
                } catch (...)
                {
                    restored = false;
                }
            }

            if (!restored)
            {
                // Recovery cannot be proven: the edit becomes Poisoned. Normal
                // reads, writes, Commit(), Undo, Redo and BeginEdit() are
                // rejected; only Rollback() may still close it.
                activeEditPoisoned_ = true;
                return primaryRc;
            }

            // Verified: memory matches storage again, so the edit stays usable.
            // A later Rollback() can close it normally.
            activeEditPoisoned_ = false;
            return primaryRc;
        }

        ErrorCode SqliteDatabase::Rollback(ISCEditSession* edit)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            const ErrorCode transitionRc = EnsureWriteTransitionAllowed();
            if (Failed(transitionRc))
            {
                return transitionRc;
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
                        const ErrorCode compensationRc =
                            ApplyJournalForward(activeJournal_,
                                                beginIndex,
                                                activeJournal_.entries.size(),
                                                nullptr);
                        return FinalizeReplayFailure(applyRc, compensationRc);
                    }
                    RefreshReferenceIndexState();
                    const ErrorCode commitRc = txn.Commit();
                    if (Failed(commitRc))
                    {
                        const std::size_t beginIndex =
                            activeJournal_.entries.size() - reversedCount;
                        const ErrorCode compensationRc =
                            ApplyJournalForward(activeJournal_,
                                                beginIndex,
                                                activeJournal_.entries.size(),
                                                nullptr);
                        return FinalizeReplayFailure(commitRc, compensationRc);
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
                    const ErrorCode compensationRc =
                        ApplyJournalForward(activeJournal_,
                                            beginIndex,
                                            activeJournal_.entries.size(),
                                            nullptr);
                    return FinalizeReplayFailure(SC_E_FAIL, compensationRc);
                }
                return SC_E_FAIL;
            }

            activeEdit_->SetState(EditState::RolledBack);
            activeEdit_.Reset();
            activeJournal_ = JournalTransaction{};
            activeSchemaOps_.clear();
            // Rollback() is the only way out of a poisoned edit: the active
            // journal, the in-memory records and the reference index are all
            // back to the pre-edit state, so the connection is usable again.
            activeEditPoisoned_ = false;
            return SC_OK;
        }

        ErrorCode SqliteDatabase::Undo()
        {
            SCHistoryMoveRequest request;
            request.direction = SCHistoryMoveDirection::Undo;
            SCHistoryMoveResult result;
            return MoveHistory(request, &result);
        }

        ErrorCode SqliteDatabase::Redo()
        {
            SCHistoryMoveRequest request;
            request.direction = SCHistoryMoveDirection::Redo;
            SCHistoryMoveResult result;
            return MoveHistory(request, &result);
        }

        ErrorCode SqliteDatabase::GetTable(const wchar_t* name, SCTablePtr& outTable)
        {
            const ErrorCode usableRc = EnsureUsable();
            if (Failed(usableRc))
            {
                return usableRc;
            }
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
            const ErrorCode usableRc = EnsureUsable();
            if (Failed(usableRc))
            {
                return usableRc;
            }
            if (outCount == nullptr)
            {
                return SC_E_POINTER;
            }

            *outCount = static_cast<std::int32_t>(tables_.size());
            return SC_OK;
        }

        ErrorCode SqliteDatabase::GetTableName(std::int32_t index, std::wstring* outName)
        {
            const ErrorCode usableRc = EnsureUsable();
            if (Failed(usableRc))
            {
                return usableRc;
            }
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
            const ErrorCode editableRc = EnsureEditableState();
            if (Failed(editableRc))
            {
                return editableRc;
            }
            const ErrorCode transitionRc = EnsureWriteTransitionAllowed();
            if (Failed(transitionRc))
            {
                return transitionRc;
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
            const ErrorCode editableRc = EnsureEditableState();
            if (Failed(editableRc))
            {
                return editableRc;
            }
            const ErrorCode transitionRc = EnsureWriteTransitionAllowed();
            if (Failed(transitionRc))
            {
                return transitionRc;
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
                if (SCCommon::EqualsIgnoreCase(otherName, tableName))
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
                    if (column.columnKind == ColumnKind::Relation && SCCommon::EqualsIgnoreCase(column.referenceTable, tableName))
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
                        L"idx_fv_" + std::to_wstring(table->TableRowId()) + L"_" + SCCommon::SanitizeFileName(columnName);
                    const std::string dropIndexSql = "DROP INDEX IF EXISTS " + SCCommon::ToUtf8(indexName) + ";";
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
            const ErrorCode editableRc = EnsureEditableState();
            if (Failed(editableRc))
            {
                return editableRc;
            }
            const ErrorCode transitionRc = EnsureWriteTransitionAllowed();
            if (Failed(transitionRc))
            {
                return transitionRc;
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
            const ErrorCode usableRc = EnsureUsable();
            if (Failed(usableRc))
            {
                return usableRc;
            }
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
            const ErrorCode usableRc = EnsureUsable();
            if (Failed(usableRc))
            {
                return usableRc;
            }
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
            const std::string tempUtf8 = SCCommon::ToUtf8(tempFilePath);

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
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (readOnly_)
            {
                return SC_E_READ_ONLY_DATABASE;
            }
            if (activeEdit_)
            {
                return SC_E_WRITE_CONFLICT;
            }
            const ErrorCode transitionRc = EnsureWriteTransitionAllowed();
            if (Failed(transitionRc))
            {
                return transitionRc;
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
            outState->dirty = static_cast<bool>(activeEdit_) || !undoStack_.empty() ||
                              replayCompensationFailureDetected_;
            outState->openMode = openMode_;
            outState->currentVersion = version_;
            outState->baselineVersion = baselineVersion_;
            outState->undoCount = openMode_ == SCDatabaseOpenMode::NoHistory ? 0 : undoStack_.size();
            outState->redoCount = openMode_ == SCDatabaseOpenMode::NoHistory ? 0 : redoStack_.size();
            outState->consistencyLost = replayCompensationFailureDetected_;
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

        // ==== History Move helpers ====

        SCHistoryPosition SqliteDatabase::CurrentHistoryPosition() const noexcept
        {
            SCHistoryPosition p;
            if (!undoStack_.empty())
            {
                const auto& tx = undoStack_.back().tx;
                p.commitId = tx.commitId;
                p.committedVersion = tx.committedVersion;
            }
            else
            {
                p.commitId = 0;
                p.committedVersion = baselineVersion_;
            }
            return p;
        }

        SCHistoryPosition SqliteDatabase::TargetHistoryPosition(
            SCHistoryMoveDirection direction) const noexcept
        {
            if (direction == SCHistoryMoveDirection::Undo)
            {
                if (undoStack_.size() >= 2)
                {
                    const auto& tx = undoStack_[undoStack_.size() - 2].tx;
                    return SCHistoryPosition{tx.commitId, tx.committedVersion};
                }
                return SCHistoryPosition{0, baselineVersion_};
            }
            else
            {
                if (!redoStack_.empty())
                {
                    const auto& tx = redoStack_.back().tx;
                    return SCHistoryPosition{tx.commitId, tx.committedVersion};
                }
                return CurrentHistoryPosition();
            }
        }

        void SqliteDatabase::FillHistoryMoveResultState(
            SCHistoryMoveResult* outResult) const noexcept
        {
            outResult->previousPosition = CurrentHistoryPosition();
            outResult->currentPosition = outResult->previousPosition;
            outResult->databaseVersion = version_;
            outResult->undoCount = undoStack_.size();
            outResult->redoCount = redoStack_.size();
        }

        ErrorCode SqliteDatabase::EnsureWriteTransitionAllowed() const noexcept
        {
            if (publicStateTransitionActive_)
            {
                return SC_E_WRITE_CONFLICT;
            }
            return SC_OK;
        }

        // ==== History Move core ====

        ErrorCode SqliteDatabase::ExecuteUndoCore(HistoryMoveCoreResult* outCoreResult)
        {
            outCoreResult->outcome = SCHistoryMoveOutcome::NotMoved;
            outCoreResult->hasChangeSet = false;

            SqlitePersistedJournalTransaction tx = undoStack_.back();
            undoStack_.pop_back();
            const auto restoreUndoStack = [&]() {
                undoStack_.push_back(tx);
            };

            std::size_t reversedCount = 0;
            try
            {
                SqliteTxn txn(db_);
                const ErrorCode applyRc = ApplyJournalReverse(
                    tx.tx, 0, tx.tx.entries.size(), &reversedCount);

                // Fault injection: primary failure
                if (journalFailureInjection_.operation == JournalFailureOperation::Undo &&
                    reversedCount >= journalFailureInjection_.failPrimaryAfterAppliedEntries)
                {
                    const std::size_t beginIndex =
                        tx.tx.entries.size() - reversedCount;
                    std::size_t compensationApplied = 0;
                    ErrorCode compensationRc =
                        ApplyJournalForward(tx.tx,
                                            beginIndex,
                                            tx.tx.entries.size(),
                                            &compensationApplied);
                    // Fault injection: compensation failure
                    if (compensationRc == SC_OK &&
                        compensationApplied >= journalFailureInjection_.failCompensationAfterAppliedEntries)
                    {
                        compensationRc = SC_E_FAIL;
                    }
                    restoreUndoStack();
                    const ErrorCode finalized = FinalizeReplayFailure(SC_E_FAIL, compensationRc);
                    if (Failed(compensationRc))
                    {
                        outCoreResult->outcome = SCHistoryMoveOutcome::ConsistencyLost;
                    }
                    return finalized;
                }

                if (Failed(applyRc))
                {
                    const std::size_t beginIndex =
                        tx.tx.entries.size() - reversedCount;
                    const ErrorCode compensationRc =
                        ApplyJournalForward(tx.tx,
                                            beginIndex,
                                            tx.tx.entries.size(),
                                            nullptr);
                    restoreUndoStack();
                    const ErrorCode finalized = FinalizeReplayFailure(applyRc, compensationRc);
                    if (Failed(compensationRc))
                    {
                        outCoreResult->outcome = SCHistoryMoveOutcome::ConsistencyLost;
                    }
                    return finalized;
                }
                const ErrorCode constraintRc =
                    ValidateUniqueAndPrimaryKeyConstraintsForTouchedTables(
                        tx.tx, true);
                if (Failed(constraintRc))
                {
                    const std::size_t beginIndex =
                        tx.tx.entries.size() - reversedCount;
                    const ErrorCode compensationRc =
                        ApplyJournalForward(tx.tx,
                                            beginIndex,
                                            tx.tx.entries.size(),
                                            nullptr);
                    restoreUndoStack();
                    const ErrorCode finalized = FinalizeReplayFailure(constraintRc, compensationRc);
                    if (Failed(compensationRc))
                    {
                        outCoreResult->outcome = SCHistoryMoveOutcome::ConsistencyLost;
                    }
                    return finalized;
                }
                const ErrorCode foreignKeyReferenceRc =
                    ValidateForeignKeyReferencesForTouchedTables(
                        tx.tx, true);
                if (Failed(foreignKeyReferenceRc))
                {
                    const std::size_t beginIndex =
                        tx.tx.entries.size() - reversedCount;
                    const ErrorCode compensationRc =
                        ApplyJournalForward(tx.tx,
                                            beginIndex,
                                            tx.tx.entries.size(),
                                            nullptr);
                    restoreUndoStack();
                    const ErrorCode finalized = FinalizeReplayFailure(foreignKeyReferenceRc,
                                                 compensationRc);
                    if (Failed(compensationRc))
                    {
                        outCoreResult->outcome = SCHistoryMoveOutcome::ConsistencyLost;
                    }
                    return finalized;
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
                    const ErrorCode compensationRc =
                        ApplyJournalForward(tx.tx,
                                            beginIndex,
                                            tx.tx.entries.size(),
                                            nullptr);
                    restoreUndoStack();
                    const ErrorCode finalized = FinalizeReplayFailure(commitRc, compensationRc);
                    if (Failed(compensationRc))
                    {
                        outCoreResult->outcome = SCHistoryMoveOutcome::ConsistencyLost;
                    }
                    return finalized;
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
                    const ErrorCode compensationRc =
                        ApplyJournalForward(tx.tx,
                                            beginIndex,
                                            tx.tx.entries.size(),
                                            nullptr);
                    restoreUndoStack();
                    const ErrorCode finalized = FinalizeReplayFailure(SC_E_FAIL, compensationRc);
                    if (Failed(compensationRc))
                    {
                        outCoreResult->outcome = SCHistoryMoveOutcome::ConsistencyLost;
                    }
                    return finalized;
                }
                restoreUndoStack();
                return SC_E_FAIL;
            }

            RefreshReferenceIndexState();

            outCoreResult->outcome = SCHistoryMoveOutcome::Moved;
            outCoreResult->hasChangeSet = true;
            outCoreResult->changeSet = BuildChangeSet(tx.tx, ChangeSource::Undo, version_);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::ExecuteRedoCore(HistoryMoveCoreResult* outCoreResult)
        {
            outCoreResult->outcome = SCHistoryMoveOutcome::NotMoved;
            outCoreResult->hasChangeSet = false;

            SqlitePersistedJournalTransaction tx = redoStack_.back();
            redoStack_.pop_back();
            const auto restoreRedoStack = [&]() {
                redoStack_.push_back(tx);
            };

            std::size_t forwardedCount = 0;
            try
            {
                SqliteTxn txn(db_);
                const ErrorCode applyRc = ApplyJournalForward(
                    tx.tx, 0, tx.tx.entries.size(), &forwardedCount);

                // Fault injection: primary failure
                if (journalFailureInjection_.operation == JournalFailureOperation::Redo &&
                    forwardedCount >= journalFailureInjection_.failPrimaryAfterAppliedEntries)
                {
                    std::size_t compensationApplied = 0;
                    ErrorCode compensationRc =
                        ApplyJournalReverse(tx.tx, 0, forwardedCount, &compensationApplied);
                    // Fault injection: compensation failure
                    if (compensationRc == SC_OK &&
                        compensationApplied >= journalFailureInjection_.failCompensationAfterAppliedEntries)
                    {
                        compensationRc = SC_E_FAIL;
                    }
                    restoreRedoStack();
                    const ErrorCode finalized = FinalizeReplayFailure(SC_E_FAIL, compensationRc);
                    if (Failed(compensationRc))
                    {
                        outCoreResult->outcome = SCHistoryMoveOutcome::ConsistencyLost;
                    }
                    return finalized;
                }

                if (Failed(applyRc))
                {
                    const ErrorCode compensationRc =
                        ApplyJournalReverse(tx.tx, 0, forwardedCount, nullptr);
                    restoreRedoStack();
                    const ErrorCode finalized = FinalizeReplayFailure(applyRc, compensationRc);
                    if (Failed(compensationRc))
                    {
                        outCoreResult->outcome = SCHistoryMoveOutcome::ConsistencyLost;
                    }
                    return finalized;
                }
                const ErrorCode constraintRc =
                    ValidateUniqueAndPrimaryKeyConstraintsForTouchedTables(
                        tx.tx, false);
                if (Failed(constraintRc))
                {
                    const ErrorCode compensationRc =
                        ApplyJournalReverse(tx.tx, 0, forwardedCount, nullptr);
                    restoreRedoStack();
                    const ErrorCode finalized = FinalizeReplayFailure(constraintRc, compensationRc);
                    if (Failed(compensationRc))
                    {
                        outCoreResult->outcome = SCHistoryMoveOutcome::ConsistencyLost;
                    }
                    return finalized;
                }
                const ErrorCode foreignKeyReferenceRc =
                    ValidateForeignKeyReferencesForTouchedTables(
                        tx.tx, false);
                if (Failed(foreignKeyReferenceRc))
                {
                    const ErrorCode compensationRc =
                        ApplyJournalReverse(tx.tx, 0, forwardedCount, nullptr);
                    restoreRedoStack();
                    const ErrorCode finalized = FinalizeReplayFailure(foreignKeyReferenceRc,
                                                 compensationRc);
                    if (Failed(compensationRc))
                    {
                        outCoreResult->outcome = SCHistoryMoveOutcome::ConsistencyLost;
                    }
                    return finalized;
                }
                const VersionId nextVersion = version_ + 1;
                PersistTouchedRecords(tx.tx, nextVersion, false);
                UpdateJournalTransactionStack(tx.txId, kStackUndo, static_cast<int>(undoStack_.size()));
                SaveMetadata(nextVersion);
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    const ErrorCode compensationRc =
                        ApplyJournalReverse(tx.tx, 0, forwardedCount, nullptr);
                    restoreRedoStack();
                    const ErrorCode finalized = FinalizeReplayFailure(commitRc, compensationRc);
                    if (Failed(compensationRc))
                    {
                        outCoreResult->outcome = SCHistoryMoveOutcome::ConsistencyLost;
                    }
                    return finalized;
                }

                version_ = nextVersion;
                UpdateTouchedVersions(tx.tx, version_, false);
                undoStack_.push_back(tx);
            } catch (...)
            {
                if (forwardedCount > 0)
                {
                    const ErrorCode compensationRc =
                        ApplyJournalReverse(tx.tx, 0, forwardedCount, nullptr);
                    restoreRedoStack();
                    const ErrorCode finalized = FinalizeReplayFailure(SC_E_FAIL, compensationRc);
                    if (Failed(compensationRc))
                    {
                        outCoreResult->outcome = SCHistoryMoveOutcome::ConsistencyLost;
                    }
                    return finalized;
                }
                restoreRedoStack();
                return SC_E_FAIL;
            }

            RefreshReferenceIndexState();

            outCoreResult->outcome = SCHistoryMoveOutcome::Moved;
            outCoreResult->hasChangeSet = true;
            outCoreResult->changeSet = BuildChangeSet(tx.tx, ChangeSource::Redo, version_);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::MoveHistory(const SCHistoryMoveRequest& request,
                                              SCHistoryMoveResult* outResult)
        {
            if (outResult == nullptr)
            {
                return SC_E_POINTER;
            }
            *outResult = SCHistoryMoveResult{};

            // Validate direction
            if (request.direction != SCHistoryMoveDirection::Undo &&
                request.direction != SCHistoryMoveDirection::Redo)
            {
                return SC_E_INVALIDARG;
            }

            // Ensure writable + no reentry
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            // Undo / Redo are rejected while an edit is poisoned: history moves
            // would replay on top of a candidate whose durability is unknown.
            if (activeEditPoisoned_)
            {
                return SC_E_WRITE_CONFLICT;
            }
            ClearConstraintViolation();
            const ErrorCode transitionRc = EnsureWriteTransitionAllowed();
            if (Failed(transitionRc))
            {
                return transitionRc;
            }
            if (activeEdit_)
            {
                return SC_E_WRITE_CONFLICT;
            }

            // Compute positions
            FillHistoryMoveResultState(outResult);
            const SCHistoryPosition expectedTarget =
                TargetHistoryPosition(request.direction);
            outResult->targetPosition = expectedTarget;

            // Check stack empty
            if (request.direction == SCHistoryMoveDirection::Undo && undoStack_.empty())
            {
                return SC_E_UNDO_STACK_EMPTY;
            }
            if (request.direction == SCHistoryMoveDirection::Redo && redoStack_.empty())
            {
                return SC_E_REDO_STACK_EMPTY;
            }

            // Match check: expectedCurrent
            const auto& expectedCurrent = request.expectedCurrent;
            if (expectedCurrent.commitIdEnabled)
            {
                if (expectedCurrent.position.commitId != outResult->previousPosition.commitId)
                {
                    return SC_E_HISTORY_POSITION_MISMATCH;
                }
            }
            if (expectedCurrent.committedVersionEnabled)
            {
                if (expectedCurrent.position.committedVersion != outResult->previousPosition.committedVersion)
                {
                    return SC_E_HISTORY_POSITION_MISMATCH;
                }
            }

            // Match check: expectedTarget
            const auto& expectedTargetReq = request.expectedTarget;
            if (expectedTargetReq.commitIdEnabled)
            {
                if (expectedTargetReq.position.commitId != expectedTarget.commitId)
                {
                    return SC_E_HISTORY_POSITION_MISMATCH;
                }
            }
            if (expectedTargetReq.committedVersionEnabled)
            {
                if (expectedTargetReq.position.committedVersion != expectedTarget.committedVersion)
                {
                    return SC_E_HISTORY_POSITION_MISMATCH;
                }
            }

            // Save candidate item before moving
            SCEditLogEntry candidateItem;
            if (request.direction == SCHistoryMoveDirection::Undo)
            {
                const auto& backTx = undoStack_.back().tx;
                candidateItem = SCEditLogEntry{backTx.commitId, backTx.committedVersion,
                                                SCEditLogActionKind::Commit, backTx.actionName, L"", 0};
            }
            else
            {
                const auto& backTx = redoStack_.back().tx;
                candidateItem = SCEditLogEntry{backTx.commitId, backTx.committedVersion,
                                                SCEditLogActionKind::Commit, backTx.actionName, L"", 0};
            }

            // Activate reentry protection
            publicStateTransitionActive_ = true;
            struct Guard {
                SqliteDatabase* db;
                ~Guard() { db->publicStateTransitionActive_ = false; }
            } guard{this};

            // Execute core
            HistoryMoveCoreResult coreResult;
            ErrorCode coreRc;
            if (request.direction == SCHistoryMoveDirection::Undo)
            {
                coreRc = ExecuteUndoCore(&coreResult);
            }
            else
            {
                coreRc = ExecuteRedoCore(&coreResult);
            }

            // Update result state
            outResult->outcome = coreResult.outcome;
            outResult->undoCount = undoStack_.size();
            outResult->redoCount = redoStack_.size();
            outResult->databaseVersion = version_;

            if (coreResult.outcome == SCHistoryMoveOutcome::Moved ||
                coreResult.outcome == SCHistoryMoveOutcome::ConsistencyLost)
            {
                outResult->movedItem = candidateItem;
                outResult->currentPosition = CurrentHistoryPosition();
            }

            if (coreResult.outcome == SCHistoryMoveOutcome::Moved)
            {
                if (coreResult.hasChangeSet)
                {
                    NotifyObservers(coreResult.changeSet);
                }
                else
                {
                    // Internal inconsistency: Moved without ChangeSet
                    replayCompensationFailureDetected_ = true;
                    outResult->outcome = SCHistoryMoveOutcome::ConsistencyLost;
                    outResult->currentPosition = CurrentHistoryPosition();
                    return SC_E_FAIL;
                }
                return coreRc;
            }

            return coreRc;
        }

        // ==== Fault injection for testing ====

        void SqliteDatabase::SetJournalFailureInjectionForTesting(
            const JournalFailureInjection& injection)
        {
            journalFailureInjection_ = injection;
        }

        void SqliteDatabase::ClearJournalFailureInjectionForTesting() noexcept
        {
            journalFailureInjection_ = JournalFailureInjection{};
        }

} // namespace StableCore::Storage
