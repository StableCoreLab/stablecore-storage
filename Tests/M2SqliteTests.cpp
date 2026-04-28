#include <filesystem>
#include <string>
#include <utility>

#include <gtest/gtest.h>
#include <sqlite3.h>

#include "SCStorage.h"

namespace sc = StableCore::Storage;
namespace fs = std::filesystem;

namespace
{

    fs::path MakeTempDbPath(const wchar_t* fileName)
    {
        fs::path path = fs::temp_directory_path() / fileName;
        std::error_code ec;
        fs::remove(path, ec);
        return path;
    }

    sc::ErrorCode CreateFileDb(const wchar_t* path, sc::SCDbPtr& db)
    {
        return sc::CreateFileDatabase(path, sc::SCOpenDatabaseOptions{}, db);
    }

    sc::ErrorCode CreateReadOnlyFileDb(const wchar_t* path, sc::SCDbPtr& db)
    {
        sc::SCOpenDatabaseOptions options;
        options.openMode = sc::SCDatabaseOpenMode::ReadOnly;
        return sc::CreateFileDatabase(path, options, db);
    }

    bool ExecSqliteScript(const fs::path& dbPath, const char* sql)
    {
        sqlite3* db = nullptr;
        const std::string narrowPath = dbPath.string();
        if (sqlite3_open_v2(narrowPath.c_str(), &db, SQLITE_OPEN_READWRITE,
                            nullptr) != SQLITE_OK)
        {
            if (db != nullptr)
            {
                sqlite3_close(db);
            }
            return false;
        }

        char* error = nullptr;
        const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &error);
        if (error != nullptr)
        {
            sqlite3_free(error);
        }
        sqlite3_close(db);
        return rc == SQLITE_OK;
    }

    bool QuerySqliteInt64(const fs::path& dbPath, const char* sql,
                          std::int64_t* outValue)
    {
        if (outValue == nullptr)
        {
            return false;
        }

        sqlite3* db = nullptr;
        const std::string narrowPath = dbPath.string();
        if (sqlite3_open_v2(narrowPath.c_str(), &db, SQLITE_OPEN_READWRITE,
                            nullptr) != SQLITE_OK)
        {
            if (db != nullptr)
            {
                sqlite3_close(db);
            }
            return false;
        }

        sqlite3_stmt* stmt = nullptr;
        const int prepareRc = sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
        if (prepareRc != SQLITE_OK)
        {
            sqlite3_close(db);
            return false;
        }

        const int stepRc = sqlite3_step(stmt);
        bool ok = false;
        if (stepRc == SQLITE_ROW)
        {
            *outValue = sqlite3_column_int64(stmt, 0);
            ok = true;
        }

        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return ok;
    }

    bool QuerySqliteExists(const fs::path& dbPath, const char* sql)
    {
        std::int64_t value = 0;
        return QuerySqliteInt64(dbPath, sql, &value) && value != 0;
    }

    bool SetMetadataValue(const fs::path& dbPath, const char* key,
                          const char* value)
    {
        const std::string sql = std::string("UPDATE metadata SET value='") +
                                value + "' WHERE key='" + key + "';";
        return ExecSqliteScript(dbPath, sql.c_str());
    }

    class SqliteReadTransactionLock
    {
    public:
        explicit SqliteReadTransactionLock(const fs::path& dbPath)
        {
            const std::string narrowPath = dbPath.string();
            if (sqlite3_open_v2(narrowPath.c_str(), &db_, SQLITE_OPEN_READWRITE,
                                nullptr) != SQLITE_OK)
            {
                if (db_ != nullptr)
                {
                    sqlite3_close(db_);
                    db_ = nullptr;
                }
                return;
            }

            if (sqlite3_exec(db_, "BEGIN;", nullptr, nullptr, nullptr) !=
                SQLITE_OK)
            {
                sqlite3_close(db_);
                db_ = nullptr;
                return;
            }

            if (sqlite3_exec(db_, "SELECT COUNT(*) FROM metadata;", nullptr,
                             nullptr, nullptr) != SQLITE_OK)
            {
                sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
                sqlite3_close(db_);
                db_ = nullptr;
                return;
            }

            held_ = true;
        }

        ~SqliteReadTransactionLock()
        {
            if (db_ != nullptr)
            {
                sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
                sqlite3_close(db_);
            }
        }

        bool IsHeld() const
        {
            return held_;
        }

    private:
        sqlite3* db_{nullptr};
        bool held_{false};
    };

    sc::SCTablePtr CreateBeamTable(sc::SCDbPtr& db)
    {
        sc::SCTablePtr table;
        EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

        sc::SCColumnDef width;
        width.name = L"Width";
        width.displayName = L"Width";
        width.valueKind = sc::ValueKind::Int64;
        width.defaultValue = sc::SCValue::FromInt64(0);
        EXPECT_EQ(schema->AddColumn(width), sc::SC_OK);

        return table;
    }

    class FinalizeFailingDatabase final : public sc::ISCDatabase,
                                          public sc::SCRefCountedObject
    {
    public:
        explicit FinalizeFailingDatabase(sc::SCDbPtr inner)
            : inner_(std::move(inner))
        {
        }

        sc::ErrorCode BeginEdit(const wchar_t* name,
                                sc::SCEditPtr& outEdit) override
        {
            return inner_->BeginEdit(name, outEdit);
        }
        sc::ErrorCode Commit(sc::ISCEditSession* edit) override
        {
            return inner_->Commit(edit);
        }
        sc::ErrorCode Rollback(sc::ISCEditSession* edit) override
        {
            return inner_->Rollback(edit);
        }
        sc::ErrorCode Undo() override
        {
            return inner_->Undo();
        }
        sc::ErrorCode Redo() override
        {
            return inner_->Redo();
        }
        sc::ErrorCode GetTableCount(std::int32_t* outCount) override
        {
            return inner_->GetTableCount(outCount);
        }
        sc::ErrorCode GetTableName(std::int32_t index,
                                   std::wstring* outName) override
        {
            return inner_->GetTableName(index, outName);
        }
        sc::ErrorCode GetTable(const wchar_t* name,
                               sc::SCTablePtr& outTable) override
        {
            return inner_->GetTable(name, outTable);
        }
        sc::ErrorCode CreateTable(const wchar_t* name,
                                  sc::SCTablePtr& outTable) override
        {
            return inner_->CreateTable(name, outTable);
        }
        sc::ErrorCode ClearColumnValues(sc::ISCTable* table,
                                        const wchar_t* name) override
        {
            return inner_->ClearColumnValues(table, name);
        }
        sc::ErrorCode ExecuteUpgradePlan(
            const sc::SCUpgradePlan& plan, bool confirmed,
            sc::SCUpgradeResult* outResult) override
        {
            return inner_->ExecuteUpgradePlan(plan, confirmed, outResult);
        }
        sc::ErrorCode BeginImportSession(
            const sc::SCImportSessionOptions& options,
            sc::SCImportStagingArea* outSession) override
        {
            return inner_->BeginImportSession(options, outSession);
        }
        sc::ErrorCode AppendImportChunk(
            sc::SCImportStagingArea* session, const sc::SCImportChunk& chunk,
            sc::SCImportCheckpoint* outCheckpoint) override
        {
            return inner_->AppendImportChunk(session, chunk, outCheckpoint);
        }
        sc::ErrorCode LoadImportRecoveryState(
            std::uint64_t sessionId,
            sc::SCImportRecoveryState* outState) override
        {
            return inner_->LoadImportRecoveryState(sessionId, outState);
        }
        sc::ErrorCode FinalizeImportSession(
            const sc::SCImportFinalizeCommit&,
            sc::SCImportRecoveryState* outState) override
        {
            if (outState != nullptr)
            {
                *outState = sc::SCImportRecoveryState{};
            }
            return sc::SC_E_FAIL;
        }
        sc::ErrorCode AbortImportSession(std::uint64_t sessionId) override
        {
            return inner_->AbortImportSession(sessionId);
        }
        sc::ErrorCode AddObserver(sc::ISCDatabaseObserver* observer) override
        {
            return inner_->AddObserver(observer);
        }
        sc::ErrorCode RemoveObserver(sc::ISCDatabaseObserver* observer) override
        {
            return inner_->RemoveObserver(observer);
        }
        sc::VersionId GetCurrentVersion() const noexcept override
        {
            return inner_->GetCurrentVersion();
        }
        std::int32_t GetSchemaVersion() const noexcept override
        {
            return inner_->GetSchemaVersion();
        }

    private:
        sc::SCDbPtr inner_;
    };

    class CommitFailingDatabase final : public sc::ISCDatabase,
                                        public sc::SCRefCountedObject
    {
    public:
        explicit CommitFailingDatabase(sc::SCDbPtr inner)
            : inner_(std::move(inner))
        {
        }

        sc::ErrorCode BeginEdit(const wchar_t* name,
                                sc::SCEditPtr& outEdit) override
        {
            return inner_->BeginEdit(name, outEdit);
        }
        sc::ErrorCode Commit(sc::ISCEditSession*) override
        {
            return sc::SC_E_FAIL;
        }
        sc::ErrorCode Rollback(sc::ISCEditSession* edit) override
        {
            return inner_->Rollback(edit);
        }
        sc::ErrorCode Undo() override
        {
            return inner_->Undo();
        }
        sc::ErrorCode Redo() override
        {
            return inner_->Redo();
        }
        sc::ErrorCode GetTableCount(std::int32_t* outCount) override
        {
            return inner_->GetTableCount(outCount);
        }
        sc::ErrorCode GetTableName(std::int32_t index,
                                   std::wstring* outName) override
        {
            return inner_->GetTableName(index, outName);
        }
        sc::ErrorCode GetTable(const wchar_t* name,
                               sc::SCTablePtr& outTable) override
        {
            return inner_->GetTable(name, outTable);
        }
        sc::ErrorCode CreateTable(const wchar_t* name,
                                  sc::SCTablePtr& outTable) override
        {
            return inner_->CreateTable(name, outTable);
        }
        sc::ErrorCode ClearColumnValues(sc::ISCTable* table,
                                        const wchar_t* name) override
        {
            return inner_->ClearColumnValues(table, name);
        }
        sc::ErrorCode ExecuteUpgradePlan(
            const sc::SCUpgradePlan& plan, bool confirmed,
            sc::SCUpgradeResult* outResult) override
        {
            return inner_->ExecuteUpgradePlan(plan, confirmed, outResult);
        }
        sc::ErrorCode BeginImportSession(
            const sc::SCImportSessionOptions& options,
            sc::SCImportStagingArea* outSession) override
        {
            return inner_->BeginImportSession(options, outSession);
        }
        sc::ErrorCode AppendImportChunk(
            sc::SCImportStagingArea* session, const sc::SCImportChunk& chunk,
            sc::SCImportCheckpoint* outCheckpoint) override
        {
            return inner_->AppendImportChunk(session, chunk, outCheckpoint);
        }
        sc::ErrorCode LoadImportRecoveryState(
            std::uint64_t sessionId,
            sc::SCImportRecoveryState* outState) override
        {
            return inner_->LoadImportRecoveryState(sessionId, outState);
        }
        sc::ErrorCode FinalizeImportSession(
            const sc::SCImportFinalizeCommit& commit,
            sc::SCImportRecoveryState* outState) override
        {
            return inner_->FinalizeImportSession(commit, outState);
        }
        sc::ErrorCode AbortImportSession(std::uint64_t sessionId) override
        {
            return inner_->AbortImportSession(sessionId);
        }
        sc::ErrorCode AddObserver(sc::ISCDatabaseObserver* observer) override
        {
            return inner_->AddObserver(observer);
        }
        sc::ErrorCode RemoveObserver(sc::ISCDatabaseObserver* observer) override
        {
            return inner_->RemoveObserver(observer);
        }
        sc::VersionId GetCurrentVersion() const noexcept override
        {
            return inner_->GetCurrentVersion();
        }
        std::int32_t GetSchemaVersion() const noexcept override
        {
            return inner_->GetSchemaVersion();
        }

    private:
        sc::SCDbPtr inner_;
    };

}  // namespace

TEST(StorageM2Sqlite, PersistedRecordSurvivesReopen)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_Reopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr beamTable = CreateBeamTable(db);

        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

        sc::SCRecordPtr beam;
        EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
        EXPECT_EQ(beam->SetInt64(L"Width", 320), sc::SC_OK);
        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    }

    {
        sc::SCDbPtr reopened;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);

        sc::SCTablePtr beamTable;
        EXPECT_EQ(reopened->GetTable(L"Beam", beamTable), sc::SC_OK);

        sc::SCRecordCursorPtr cursor;
        EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);

        bool hasRow = false;
        EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
        EXPECT_TRUE(hasRow);

        sc::SCRecordPtr beam;
        EXPECT_EQ(cursor->GetCurrent(beam), sc::SC_OK);

        std::int64_t width = 0;
        EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
        EXPECT_EQ(width, 320);
    }
}

TEST(StorageM2Sqlite, UndoRedoSurvivesReopen)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_UndoRedo.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
        sc::SCTablePtr beamTable = CreateBeamTable(db);

        sc::SCEditPtr createEdit;
        EXPECT_EQ(db->BeginEdit(L"create", createEdit), sc::SC_OK);
        sc::SCRecordPtr beam;
        EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
        EXPECT_EQ(beam->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(db->Commit(createEdit.Get()), sc::SC_OK);

        sc::SCEditPtr modifyEdit;
        EXPECT_EQ(db->BeginEdit(L"modify", modifyEdit), sc::SC_OK);
        EXPECT_EQ(beam->SetInt64(L"Width", 450), sc::SC_OK);
        EXPECT_EQ(db->Commit(modifyEdit.Get()), sc::SC_OK);

        EXPECT_EQ(db->Undo(), sc::SC_OK);
    }

    {
        sc::SCDbPtr reopened;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);

        sc::SCTablePtr beamTable;
        EXPECT_EQ(reopened->GetTable(L"Beam", beamTable), sc::SC_OK);

        sc::SCRecordCursorPtr cursor;
        EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
        bool hasRow = false;
        EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
        EXPECT_TRUE(hasRow);

        sc::SCRecordPtr beam;
        EXPECT_EQ(cursor->GetCurrent(beam), sc::SC_OK);

        std::int64_t width = 0;
        EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
        EXPECT_EQ(width, 100);

        EXPECT_EQ(reopened->Redo(), sc::SC_OK);
        EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
        EXPECT_EQ(width, 450);
    }
}

TEST(StorageM2Sqlite, OpenModeAndEditLogStateAreQueryable)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_EditLogState.sqlite");

    sc::SCOpenDatabaseOptions options;
    options.openMode = sc::SCDatabaseOpenMode::Normal;

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), options, db), sc::SC_OK);

    sc::SCEditingDatabaseState editingState;
    EXPECT_EQ(db->GetEditingState(&editingState), sc::SC_OK);
    EXPECT_TRUE(editingState.open);
    EXPECT_EQ(editingState.openMode, sc::SCDatabaseOpenMode::Normal);
    EXPECT_EQ(editingState.undoCount, 0u);
    EXPECT_EQ(editingState.redoCount, 0u);

    sc::SCEditLogState logState;
    EXPECT_EQ(db->GetEditLogState(&logState), sc::SC_OK);
    EXPECT_TRUE(logState.undoItems.empty());
    EXPECT_TRUE(logState.redoItems.empty());

    sc::SCTablePtr beamTable = CreateBeamTable(db);
    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 320), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    EXPECT_EQ(db->GetEditLogState(&logState), sc::SC_OK);
    EXPECT_EQ(logState.undoItems.size(), 1u);
    EXPECT_TRUE(logState.redoItems.empty());
    ASSERT_EQ(logState.undoItems.size(), 1u);
    const sc::CommitId firstCommitId = logState.undoItems.front().commitId;
    const sc::VersionId firstCommittedVersion =
        logState.undoItems.front().version;
    EXPECT_NE(firstCommitId, 0u);
    EXPECT_EQ(firstCommittedVersion, 1u);
    EXPECT_EQ(logState.undoItems.front().kind, sc::SCEditLogActionKind::Commit);

    sc::SCBackupResult resetResult;
    EXPECT_EQ(db->ResetHistoryBaseline(&resetResult), sc::SC_OK);
    EXPECT_EQ(resetResult.removedJournalTransactionCount, 1u);
    EXPECT_GT(resetResult.removedJournalEntryCount, 0u);

    sc::SCEditingDatabaseState postResetState;
    EXPECT_EQ(db->GetEditingState(&postResetState), sc::SC_OK);
    const sc::VersionId savedBaselineVersion = postResetState.baselineVersion;
    EXPECT_EQ(savedBaselineVersion, postResetState.currentVersion);

    sc::SCEditPtr secondEdit;
    EXPECT_EQ(db->BeginEdit(L"second", secondEdit), sc::SC_OK);
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 480), sc::SC_OK);
    EXPECT_EQ(db->Commit(secondEdit.Get()), sc::SC_OK);

    EXPECT_EQ(db->GetEditingState(&postResetState), sc::SC_OK);
    EXPECT_EQ(postResetState.baselineVersion, savedBaselineVersion);
    EXPECT_GT(postResetState.currentVersion, savedBaselineVersion);

    EXPECT_EQ(db->GetEditLogState(&logState), sc::SC_OK);
    EXPECT_EQ(logState.baselineVersion, savedBaselineVersion);
    EXPECT_EQ(logState.undoItems.size(), 1u);
    EXPECT_TRUE(logState.redoItems.empty());
    const sc::CommitId secondCommitId = logState.undoItems.front().commitId;
    const sc::VersionId secondCommittedVersion =
        logState.undoItems.front().version;
    EXPECT_NE(secondCommitId, 0u);
    EXPECT_NE(secondCommitId, firstCommitId);
    EXPECT_EQ(secondCommittedVersion, 2u);
    EXPECT_EQ(logState.undoItems.front().kind, sc::SCEditLogActionKind::Commit);

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_EQ(db->GetEditLogState(&logState), sc::SC_OK);
    ASSERT_EQ(logState.redoItems.size(), 1u);
    EXPECT_EQ(logState.redoItems.front().commitId, secondCommitId);
    EXPECT_EQ(logState.redoItems.front().version, secondCommittedVersion);
    EXPECT_EQ(logState.redoItems.front().kind, sc::SCEditLogActionKind::Commit);

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_EQ(db->GetEditLogState(&logState), sc::SC_OK);
    ASSERT_EQ(logState.undoItems.size(), 1u);
    EXPECT_EQ(logState.undoItems.front().commitId, secondCommitId);
    EXPECT_EQ(logState.undoItems.front().version, secondCommittedVersion);
    EXPECT_EQ(logState.undoItems.front().kind, sc::SCEditLogActionKind::Commit);

    sc::SCDbPtr reopened;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);

    sc::SCEditingDatabaseState reopenedState;
    EXPECT_EQ(reopened->GetEditingState(&reopenedState), sc::SC_OK);
    EXPECT_EQ(reopenedState.baselineVersion, savedBaselineVersion);
    EXPECT_EQ(reopenedState.currentVersion, 4u);

    sc::SCEditLogState reopenedLog;
    EXPECT_EQ(reopened->GetEditLogState(&reopenedLog), sc::SC_OK);
    EXPECT_EQ(reopenedLog.baselineVersion, savedBaselineVersion);
    EXPECT_EQ(reopenedLog.undoItems.size(), 1u);
    EXPECT_TRUE(reopenedLog.redoItems.empty());
    EXPECT_EQ(reopenedLog.undoItems.front().commitId, secondCommitId);
    EXPECT_EQ(reopenedLog.undoItems.front().version, secondCommittedVersion);
    EXPECT_EQ(reopenedLog.undoItems.front().kind,
              sc::SCEditLogActionKind::Commit);
}

TEST(StorageM2Sqlite,
     ResetHistoryBaselineCommitFailureKeepsEditLogStateUnchanged)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_ResetHistoryBaselineBusy.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCEditPtr createEdit;
    EXPECT_EQ(db->BeginEdit(L"create", createEdit), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(db->Commit(createEdit.Get()), sc::SC_OK);

    sc::SCEditPtr modifyEdit;
    EXPECT_EQ(db->BeginEdit(L"modify", modifyEdit), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 250), sc::SC_OK);
    EXPECT_EQ(db->Commit(modifyEdit.Get()), sc::SC_OK);

    EXPECT_EQ(db->Undo(), sc::SC_OK);

    sc::SCEditingDatabaseState editingStateBefore;
    EXPECT_EQ(db->GetEditingState(&editingStateBefore), sc::SC_OK);
    sc::SCEditLogState logStateBefore;
    EXPECT_EQ(db->GetEditLogState(&logStateBefore), sc::SC_OK);
    ASSERT_EQ(logStateBefore.undoItems.size(), 1u);
    ASSERT_EQ(logStateBefore.redoItems.size(), 1u);

    const sc::CommitId undoCommitIdBefore =
        logStateBefore.undoItems.front().commitId;
    const sc::CommitId redoCommitIdBefore =
        logStateBefore.redoItems.front().commitId;

    SqliteReadTransactionLock readLock(dbPath);
    ASSERT_TRUE(readLock.IsHeld());

    sc::SCBackupResult resetResult;
    EXPECT_NE(db->ResetHistoryBaseline(&resetResult), sc::SC_OK);

    sc::SCEditingDatabaseState editingStateAfter;
    EXPECT_EQ(db->GetEditingState(&editingStateAfter), sc::SC_OK);
    EXPECT_EQ(editingStateAfter.currentVersion,
              editingStateBefore.currentVersion);
    EXPECT_EQ(editingStateAfter.baselineVersion,
              editingStateBefore.baselineVersion);
    EXPECT_EQ(editingStateAfter.undoCount, editingStateBefore.undoCount);
    EXPECT_EQ(editingStateAfter.redoCount, editingStateBefore.redoCount);

    sc::SCEditLogState logStateAfter;
    EXPECT_EQ(db->GetEditLogState(&logStateAfter), sc::SC_OK);
    EXPECT_EQ(logStateAfter.baselineVersion, logStateBefore.baselineVersion);
    ASSERT_EQ(logStateAfter.undoItems.size(), logStateBefore.undoItems.size());
    ASSERT_EQ(logStateAfter.redoItems.size(), logStateBefore.redoItems.size());
    EXPECT_EQ(logStateAfter.undoItems.front().commitId, undoCommitIdBefore);
    EXPECT_EQ(logStateAfter.redoItems.front().commitId, redoCommitIdBefore);
}

TEST(StorageM2Sqlite, CreateBackupCopyPreservesOrTrimsJournalHistory)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_BackupLevel2.sqlite");
    const fs::path trimmedBackupPath =
        MakeTempDbPath(L"StableCoreStorage_M2_BackupLevel2_Trimmed.sqlite");
    const fs::path preservedBackupPath =
        MakeTempDbPath(L"StableCoreStorage_M2_BackupLevel2_Preserved.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(
        sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db),
        sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);
    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 320), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    sc::SCEditingDatabaseState sourceState;
    EXPECT_EQ(db->GetEditingState(&sourceState), sc::SC_OK);

    sc::SCBackupOptions trimmedOptions;
    sc::SCBackupResult trimmedResult;
    EXPECT_EQ(db->CreateBackupCopy(trimmedBackupPath.c_str(), trimmedOptions,
                                   &trimmedResult),
              sc::SC_OK);
    EXPECT_EQ(trimmedResult.removedJournalTransactionCount, 1u);
    EXPECT_GT(trimmedResult.removedJournalEntryCount, 0u);
    EXPECT_GT(trimmedResult.outputFileSizeBytes, 0u);

    sc::SCDbPtr trimmedDb;
    EXPECT_EQ(CreateFileDb(trimmedBackupPath.c_str(), trimmedDb), sc::SC_OK);

    sc::SCEditLogState trimmedLog;
    EXPECT_EQ(trimmedDb->GetEditLogState(&trimmedLog), sc::SC_OK);
    EXPECT_TRUE(trimmedLog.undoItems.empty());
    EXPECT_TRUE(trimmedLog.redoItems.empty());

    sc::SCEditingDatabaseState trimmedState;
    EXPECT_EQ(trimmedDb->GetEditingState(&trimmedState), sc::SC_OK);
    EXPECT_EQ(trimmedState.currentVersion, sourceState.currentVersion);
    EXPECT_EQ(trimmedState.baselineVersion, sourceState.currentVersion);

    sc::SCBackupOptions preservedOptions;
    preservedOptions.preserveJournalHistory = true;
    preservedOptions.overwriteExisting = true;
    sc::SCBackupResult preservedResult;
    EXPECT_EQ(db->CreateBackupCopy(preservedBackupPath.c_str(),
                                   preservedOptions, &preservedResult),
              sc::SC_OK);
    EXPECT_EQ(preservedResult.removedJournalTransactionCount, 0u);
    EXPECT_EQ(preservedResult.removedJournalEntryCount, 0u);
    EXPECT_GT(preservedResult.outputFileSizeBytes, 0u);

    sc::SCDbPtr preservedDb;
    EXPECT_EQ(CreateFileDb(preservedBackupPath.c_str(), preservedDb),
              sc::SC_OK);

    sc::SCEditLogState preservedLog;
    EXPECT_EQ(preservedDb->GetEditLogState(&preservedLog), sc::SC_OK);
    EXPECT_EQ(preservedLog.baselineVersion, sourceState.baselineVersion);
    ASSERT_EQ(preservedLog.undoItems.size(), 1u);
    EXPECT_TRUE(preservedLog.redoItems.empty());
    EXPECT_EQ(preservedLog.undoItems.front().kind,
              sc::SCEditLogActionKind::Commit);

    sc::SCEditingDatabaseState preservedState;
    EXPECT_EQ(preservedDb->GetEditingState(&preservedState), sc::SC_OK);
    EXPECT_EQ(preservedState.currentVersion, sourceState.currentVersion);
    EXPECT_EQ(preservedState.baselineVersion, sourceState.baselineVersion);

    sc::SCBackupResult overwriteCheckResult;
    EXPECT_EQ(db->CreateBackupCopy(trimmedBackupPath.c_str(), trimmedOptions,
                                   &overwriteCheckResult),
              sc::SC_E_FILE_EXISTS);
}

TEST(StorageM2Sqlite, NoHistoryOpenModeHidesEditLog)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_NoHistory.sqlite");

    sc::SCOpenDatabaseOptions options;
    options.openMode = sc::SCDatabaseOpenMode::NoHistory;

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), options, db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);
    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 320), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    sc::SCEditLogState logState;
    EXPECT_EQ(db->GetEditLogState(&logState), sc::SC_OK);
    EXPECT_TRUE(logState.undoItems.empty());
    EXPECT_TRUE(logState.redoItems.empty());
}

TEST(StorageM2Sqlite, PersistedQueryAndDelete)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_Query.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
        sc::SCTablePtr beamTable = CreateBeamTable(db);

        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

        sc::SCRecordPtr beamA;
        EXPECT_EQ(beamTable->CreateRecord(beamA), sc::SC_OK);
        EXPECT_EQ(beamA->SetInt64(L"Width", 200), sc::SC_OK);

        sc::SCRecordPtr beamB;
        EXPECT_EQ(beamTable->CreateRecord(beamB), sc::SC_OK);
        EXPECT_EQ(beamB->SetInt64(L"Width", 500), sc::SC_OK);

        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

        sc::SCEditPtr deleteEdit;
        EXPECT_EQ(db->BeginEdit(L"delete", deleteEdit), sc::SC_OK);
        EXPECT_EQ(beamTable->DeleteRecord(beamA->GetId()), sc::SC_OK);
        EXPECT_EQ(db->Commit(deleteEdit.Get()), sc::SC_OK);
    }

    {
        sc::SCDbPtr reopened;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);
        sc::SCTablePtr beamTable;
        EXPECT_EQ(reopened->GetTable(L"Beam", beamTable), sc::SC_OK);

        sc::SCRecordCursorPtr cursor;
        sc::SCQueryCondition condition{L"Width", sc::SCValue::FromInt64(500)};
        EXPECT_EQ(beamTable->FindRecords(condition, cursor), sc::SC_OK);

        bool hasRow = false;
        EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
        EXPECT_TRUE(hasRow);

        sc::SCRecordPtr beam;
        EXPECT_EQ(cursor->GetCurrent(beam), sc::SC_OK);

        std::int64_t width = 0;
        EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
        EXPECT_EQ(width, 500);
    }
}

TEST(StorageM2Sqlite, PersistedSchemaRejectsInvalidReferenceTableUsage)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_SchemaValidation.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr beamTable;
    EXPECT_EQ(db->CreateTable(L"Beam", beamTable), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(beamTable->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef factWithRef;
    factWithRef.name = L"Width";
    factWithRef.valueKind = sc::ValueKind::Int64;
    factWithRef.referenceTable = L"Floor";
    EXPECT_EQ(schema->AddColumn(factWithRef), sc::SC_E_SCHEMA_VIOLATION);

    sc::SCColumnDef relationWithoutRef;
    relationWithoutRef.name = L"FloorRef";
    relationWithoutRef.valueKind = sc::ValueKind::RecordId;
    relationWithoutRef.columnKind = sc::ColumnKind::Relation;
    EXPECT_EQ(schema->AddColumn(relationWithoutRef), sc::SC_E_SCHEMA_VIOLATION);
}

TEST(StorageM2Sqlite, PersistedEmptyQueryIsNotError)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_EmptyQuery.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(beamTable->FindRecords({L"Width", sc::SCValue::FromInt64(12345)},
                                     cursor),
              sc::SC_OK);

    bool hasRow = true;
    EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
    EXPECT_FALSE(hasRow);
}

TEST(StorageM2Sqlite, VersionGraphReportsUpgradeWindow)
{
    sc::SCVersionGraph graph;
    EXPECT_EQ(sc::BuildDefaultVersionGraph(&graph), sc::SC_OK);
    EXPECT_GE(graph.latestSupportedVersion, 2);
    EXPECT_FALSE(graph.nodes.empty());
    EXPECT_FALSE(graph.edges.empty());

    sc::SCOpenDecision decision;
    EXPECT_EQ(sc::EvaluateOpenDecision(graph, 1, true, &decision), sc::SC_OK);
    EXPECT_EQ(decision.mode, sc::SCOpenMode::UpgradeRequired);
    EXPECT_TRUE(decision.needsUpgrade);
    EXPECT_TRUE(decision.readOnlyOnly);
    EXPECT_FALSE(decision.writable);

    EXPECT_EQ(sc::EvaluateOpenDecision(graph, graph.latestSupportedVersion,
                                       true, &decision),
              sc::SC_OK);
    EXPECT_EQ(decision.mode, sc::SCOpenMode::ReadWrite);
    EXPECT_FALSE(decision.needsUpgrade);
    EXPECT_FALSE(decision.readOnlyOnly);
    EXPECT_TRUE(decision.writable);
}

TEST(StorageM2Sqlite, ReadOnlySqliteOpenRejectsWrites)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_ReadOnly.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr beamTable = CreateBeamTable(db);

        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

        sc::SCRecordPtr beam;
        EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
        EXPECT_EQ(beam->SetInt64(L"Width", 320), sc::SC_OK);
        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    }

    sc::SCDbPtr readOnlyDb;
    EXPECT_EQ(CreateReadOnlyFileDb(dbPath.c_str(), readOnlyDb), sc::SC_OK);
    EXPECT_EQ(readOnlyDb->GetSchemaVersion(), 2);

    sc::SCEditPtr edit;
    EXPECT_EQ(readOnlyDb->BeginEdit(L"read-only", edit),
              sc::SC_E_READ_ONLY_DATABASE);
}

TEST(StorageM2Sqlite, UpgradeExecutionIsExplicit)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_ExplicitUpgrade.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
        sc::SCTablePtr beamTable = CreateBeamTable(db);

        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

        sc::SCRecordPtr beam;
        EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
        EXPECT_EQ(beam->SetInt64(L"Width", 320), sc::SC_OK);
        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    }

    EXPECT_TRUE(SetMetadataValue(dbPath, "schema_version", "1"));

    sc::SCDbPtr reopened;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);
    EXPECT_EQ(reopened->GetSchemaVersion(), 1);

    sc::SCVersionGraph graph;
    EXPECT_EQ(sc::BuildDefaultVersionGraph(&graph), sc::SC_OK);

    sc::SCUpgradePlan plan;
    EXPECT_EQ(
        sc::BuildUpgradePlan(1, graph.latestSupportedVersion, graph, &plan),
        sc::SC_OK);
    EXPECT_TRUE(plan.requiresConfirmation);
    EXPECT_TRUE(plan.upgradeRequired);

    sc::SCUpgradeResult result;
    EXPECT_EQ(reopened->ExecuteUpgradePlan(plan, false, &result),
              sc::SC_E_INVALIDARG);
    EXPECT_EQ(result.status, sc::SCUpgradeStatus::Failed);
    EXPECT_EQ(reopened->GetSchemaVersion(), 1);

    EXPECT_EQ(reopened->ExecuteUpgradePlan(plan, true, &result), sc::SC_OK);
    EXPECT_EQ(result.status, sc::SCUpgradeStatus::Success);
    EXPECT_EQ(result.rolledBack, false);
    EXPECT_EQ(reopened->GetSchemaVersion(), graph.latestSupportedVersion);
}

TEST(StorageM2Sqlite, UpgradeFailureRollsBackOnObjectNameConflict)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_UpgradeRollback.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
        sc::SCTablePtr beamTable = CreateBeamTable(db);

        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

        sc::SCRecordPtr beam;
        EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
        EXPECT_EQ(beam->SetInt64(L"Width", 160), sc::SC_OK);
        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    }

    EXPECT_TRUE(ExecSqliteScript(
        dbPath,
        "DROP TABLE IF EXISTS startup_diagnostics;"
        "DROP VIEW IF EXISTS startup_diagnostics;"
        "CREATE VIEW startup_diagnostics AS SELECT 1 AS diag_id, 0 AS "
        "severity, 'upgrade' AS category, 'blocked' AS message;"
        "UPDATE metadata SET value='1' WHERE key='schema_version';"
        "UPDATE metadata SET value='1' WHERE key='clean_shutdown';"));

    sc::SCDbPtr reopened;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);
    EXPECT_EQ(reopened->GetSchemaVersion(), 1);

    sc::SCVersionGraph graph;
    EXPECT_EQ(sc::BuildDefaultVersionGraph(&graph), sc::SC_OK);

    sc::SCUpgradePlan plan;
    EXPECT_EQ(
        sc::BuildUpgradePlan(1, graph.latestSupportedVersion, graph, &plan),
        sc::SC_OK);

    sc::SCUpgradeResult result;
    EXPECT_NE(reopened->ExecuteUpgradePlan(plan, true, &result), sc::SC_OK);
    EXPECT_EQ(result.status, sc::SCUpgradeStatus::RolledBack);
    EXPECT_TRUE(result.rolledBack);
    EXPECT_EQ(reopened->GetSchemaVersion(), 1);

    sc::SCTablePtr beamTable;
    EXPECT_EQ(reopened->GetTable(L"Beam", beamTable), sc::SC_OK);
    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
    bool hasRow = false;
    EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
    EXPECT_TRUE(hasRow);
}

TEST(StorageM2Sqlite, UncleanShutdownBlocksUpgradeExecution)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_UncleanUpgrade.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
        sc::SCTablePtr beamTable = CreateBeamTable(db);

        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

        sc::SCRecordPtr beam;
        EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
        EXPECT_EQ(beam->SetInt64(L"Width", 160), sc::SC_OK);
        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    }

    EXPECT_TRUE(SetMetadataValue(dbPath, "schema_version", "1"));
    EXPECT_TRUE(SetMetadataValue(dbPath, "clean_shutdown", "0"));

    sc::SCDbPtr reopened;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);
    EXPECT_EQ(reopened->GetSchemaVersion(), 1);

    sc::SCVersionGraph graph;
    EXPECT_EQ(sc::BuildDefaultVersionGraph(&graph), sc::SC_OK);

    sc::SCOpenDecision decision;
    EXPECT_EQ(sc::EvaluateOpenDecision(graph, reopened->GetSchemaVersion(),
                                       false, &decision),
              sc::SC_OK);
    EXPECT_EQ(decision.mode, sc::SCOpenMode::ReadOnly);

    sc::SCUpgradePlan plan;
    EXPECT_EQ(
        sc::BuildUpgradePlan(1, graph.latestSupportedVersion, graph, &plan),
        sc::SC_OK);

    sc::SCUpgradeResult result;
    EXPECT_EQ(reopened->ExecuteUpgradePlan(plan, true, &result),
              sc::SC_E_WRITE_CONFLICT);
    EXPECT_EQ(result.status, sc::SCUpgradeStatus::Unsupported);
    EXPECT_EQ(result.rolledBack, false);
    EXPECT_EQ(reopened->GetSchemaVersion(), 1);
}

TEST(StorageM2Sqlite, ImportSessionCheckpointIsNotLiveState)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_ImportCheckpoint.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCImportSessionOptions sessionOptions;
    sessionOptions.sessionName = L"chunked import";
    sessionOptions.chunkSize = 1;

    sc::SCImportStagingArea session;
    EXPECT_EQ(sc::BeginImportSession(db.Get(), sessionOptions, &session),
              sc::SC_OK);

    sc::SCImportChunk chunk;
    chunk.chunkId = 1;
    sc::SCBatchTableRequest request;
    request.tableName = L"Beam";
    request.creates.push_back(sc::SCBatchCreateRecordRequest{
        {{L"Width", sc::SCValue::FromInt64(140)}}});
    chunk.requests.push_back(request);

    sc::SCImportCheckpoint checkpoint;
    EXPECT_EQ(sc::AppendImportChunk(db.Get(), &session, chunk, &checkpoint),
              sc::SC_OK);
    EXPECT_EQ(checkpoint.sessionId, session.sessionId);
    EXPECT_EQ(checkpoint.chunkCount, 1u);
    EXPECT_TRUE(checkpoint.persisted);

    sc::SCImportRecoveryState recoveryState;
    EXPECT_EQ(
        sc::GetImportRecoveryState(db.Get(), session.sessionId, &recoveryState),
        sc::SC_OK);
    EXPECT_TRUE(recoveryState.canResume);
    EXPECT_TRUE(recoveryState.canFinalize);
    EXPECT_EQ(recoveryState.stagingArea.chunks.size(), 1u);

    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
    bool hasRow = true;
    EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
    EXPECT_FALSE(hasRow);

    sc::SCImportFinalizeCommit commit;
    commit.sessionId = session.sessionId;
    commit.confirmed = true;
    commit.commitName = L"finalize import";
    sc::SCBatchExecutionResult result;
    EXPECT_EQ(sc::FinalizeImportSession(db.Get(), session, commit, &result),
              sc::SC_OK);
    EXPECT_EQ(result.importSessionId, session.sessionId);
    EXPECT_EQ(result.chunkCount, 1u);

    EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
    EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
    EXPECT_TRUE(hasRow);
}

TEST(StorageM2Sqlite, RestartedDatabaseCanFinalizeImportRecoveryState)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_RestartImportRecovery.sqlite");
    sc::SCImportSessionId sessionId = 0;

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
        sc::SCTablePtr beamTable = CreateBeamTable(db);

        sc::SCImportSessionOptions sessionOptions;
        sessionOptions.sessionName = L"restart recovery import";
        sessionOptions.chunkSize = 1;

        sc::SCImportStagingArea session;
        EXPECT_EQ(sc::BeginImportSession(db.Get(), sessionOptions, &session),
                  sc::SC_OK);
        sessionId = session.sessionId;

        sc::SCImportChunk chunk;
        chunk.chunkId = 1;
        sc::SCBatchTableRequest request;
        request.tableName = L"Beam";
        request.creates.push_back(sc::SCBatchCreateRecordRequest{
            {{L"Width", sc::SCValue::FromInt64(210)}}});
        chunk.requests.push_back(request);

        sc::SCImportCheckpoint checkpoint;
        EXPECT_EQ(sc::AppendImportChunk(db.Get(), &session, chunk, &checkpoint),
                  sc::SC_OK);

        sc::SCImportRecoveryState recoveryState;
        EXPECT_EQ(
            sc::GetImportRecoveryState(db.Get(), sessionId, &recoveryState),
            sc::SC_OK);
        EXPECT_TRUE(recoveryState.canResume);
        EXPECT_TRUE(recoveryState.canFinalize);

        sc::SCRecordCursorPtr cursor;
        EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
        bool hasRow = false;
        EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
        EXPECT_FALSE(hasRow);
    }

    sc::SCDbPtr reopened;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);

    sc::SCImportRecoveryState recoveryState;
    EXPECT_EQ(reopened->LoadImportRecoveryState(sessionId, &recoveryState),
              sc::SC_OK);
    EXPECT_EQ(recoveryState.sessionId, sessionId);
    EXPECT_TRUE(recoveryState.canResume);
    EXPECT_TRUE(recoveryState.canFinalize);
    EXPECT_EQ(recoveryState.stagingArea.chunks.size(), 1u);

    sc::SCImportFinalizeCommit commit;
    commit.sessionId = sessionId;
    commit.confirmed = true;
    commit.commitName = L"restart recovery import";
    sc::SCBatchExecutionResult result;
    EXPECT_EQ(sc::FinalizeImportSession(
                  reopened.Get(), recoveryState.stagingArea, commit, &result),
              sc::SC_OK);
    EXPECT_EQ(result.importSessionId, sessionId);
    EXPECT_EQ(result.createdCount, 1u);

    sc::SCTablePtr beamTable;
    EXPECT_EQ(reopened->GetTable(L"Beam", beamTable), sc::SC_OK);
    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
    bool hasRow = false;
    EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
    EXPECT_TRUE(hasRow);

    EXPECT_EQ(reopened->LoadImportRecoveryState(sessionId, &recoveryState),
              sc::SC_E_RECORD_NOT_FOUND);
}

TEST(StorageM2Sqlite, AbortImportSessionClearsRecoveryState)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_AbortImport.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    CreateBeamTable(db);

    sc::SCImportSessionOptions sessionOptions;
    sessionOptions.sessionName = L"abort import";
    sessionOptions.chunkSize = 1;

    sc::SCImportStagingArea session;
    EXPECT_EQ(sc::BeginImportSession(db.Get(), sessionOptions, &session),
              sc::SC_OK);

    sc::SCImportChunk chunk;
    chunk.chunkId = 1;
    sc::SCBatchTableRequest request;
    request.tableName = L"Beam";
    request.creates.push_back(sc::SCBatchCreateRecordRequest{
        {{L"Width", sc::SCValue::FromInt64(180)}}});
    chunk.requests.push_back(request);

    sc::SCImportCheckpoint checkpoint;
    EXPECT_EQ(sc::AppendImportChunk(db.Get(), &session, chunk, &checkpoint),
              sc::SC_OK);

    sc::SCImportRecoveryState recoveryState;
    EXPECT_EQ(
        sc::GetImportRecoveryState(db.Get(), session.sessionId, &recoveryState),
        sc::SC_OK);
    EXPECT_TRUE(recoveryState.canResume);
    EXPECT_TRUE(recoveryState.canFinalize);

    EXPECT_EQ(sc::AbortImportSession(db.Get(), session.sessionId), sc::SC_OK);
    EXPECT_EQ(
        sc::GetImportRecoveryState(db.Get(), session.sessionId, &recoveryState),
        sc::SC_E_RECORD_NOT_FOUND);

    sc::SCImportFinalizeCommit commit;
    commit.sessionId = session.sessionId;
    commit.confirmed = true;
    commit.commitName = L"abort import";
    sc::SCBatchExecutionResult result;
    EXPECT_EQ(sc::FinalizeImportSession(db.Get(), session, commit, &result),
              sc::SC_E_RECORD_NOT_FOUND);
}

TEST(StorageM2Sqlite, ExecuteImportUsesChunkedSessionModel)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_ChunkedImport.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    std::vector<sc::SCBatchTableRequest> requests;
    for (int i = 0; i < 4; ++i)
    {
        sc::SCBatchTableRequest request;
        request.tableName = L"Beam";
        request.creates.push_back(sc::SCBatchCreateRecordRequest{
            {{L"Width", sc::SCValue::FromInt64(100 + i)}}});
        requests.push_back(request);
    }

    sc::SCBatchExecutionOptions options;
    options.editName = L"chunked import";
    options.chunkSize = 2;

    sc::SCBatchExecutionResult result;
    EXPECT_EQ(sc::ExecuteImport(db.Get(), requests, options, &result),
              sc::SC_OK);
    EXPECT_EQ(result.chunkCount, 2u);
    EXPECT_EQ(result.checkpointCount, 2u);
    EXPECT_EQ(result.createdCount, 4u);

    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
    bool hasRow = false;
    std::size_t count = 0;
    while (cursor->MoveNext(&hasRow) == sc::SC_OK && hasRow)
    {
        ++count;
    }
    EXPECT_EQ(count, 4u);
}

TEST(StorageM2Sqlite, BatchFailureDoesNotLeaveActiveEdit)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_BatchFailureCleanup.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCBatchExecutionOptions options;
    options.editName = L"broken batch";
    options.rollbackOnError = false;

    std::vector<sc::SCBatchTableRequest> requests;
    sc::SCBatchTableRequest request;
    request.tableName = L"Beam";
    request.updates.push_back(sc::SCBatchUpdateRecordRequest{
        9999, {{L"Width", sc::SCValue::FromInt64(123)}}});
    requests.push_back(request);

    sc::SCBatchExecutionResult result;
    EXPECT_EQ(sc::ExecuteBatchEdit(db.Get(), requests, options, &result),
              sc::SC_E_RECORD_NOT_FOUND);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"after failed batch", edit), sc::SC_OK);
    EXPECT_EQ(db->Rollback(edit.Get()), sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
    bool hasRow = false;
    EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
    EXPECT_FALSE(hasRow);
}

TEST(StorageM2Sqlite, ExecuteImportKeepsRecoveryStateWhenCommitFails)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_ImportCommitFailure.sqlite");

    sc::SCDbPtr realDb;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), realDb), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(realDb);

    sc::SCRefPtr<CommitFailingDatabase> proxy =
        sc::SCMakeRef<CommitFailingDatabase>(realDb);

    std::vector<sc::SCBatchTableRequest> requests;
    sc::SCBatchTableRequest request;
    request.tableName = L"Beam";
    request.creates.push_back(sc::SCBatchCreateRecordRequest{
        {{L"Width", sc::SCValue::FromInt64(256)}}});
    requests.push_back(request);

    sc::SCBatchExecutionOptions options;
    options.editName = L"import commit failure";
    options.chunkSize = 1;

    sc::SCBatchExecutionResult result;
    EXPECT_EQ(sc::ExecuteImport(proxy.Get(), requests, options, &result),
              sc::SC_E_FAIL);

    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
    bool hasRow = false;
    EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
    EXPECT_FALSE(hasRow);

    sc::SCImportRecoveryState recoveryState;
    EXPECT_EQ(proxy->LoadImportRecoveryState(1, &recoveryState), sc::SC_OK);
    EXPECT_EQ(recoveryState.sessionId, 1u);
    EXPECT_EQ(recoveryState.state, sc::SCImportSessionState::Checkpointed);
    EXPECT_TRUE(recoveryState.canResume);
    EXPECT_TRUE(recoveryState.canFinalize);
    EXPECT_TRUE(recoveryState.checkpointPersisted);
}

TEST(StorageM2Sqlite, ExecuteImportClearsRecoveryStateOnSuccess)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_ImportFinalizeSuccess.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    std::vector<sc::SCBatchTableRequest> requests;
    sc::SCBatchTableRequest request;
    request.tableName = L"Beam";
    request.creates.push_back(sc::SCBatchCreateRecordRequest{
        {{L"Width", sc::SCValue::FromInt64(320)}}});
    requests.push_back(request);

    sc::SCBatchExecutionOptions options;
    options.editName = L"import finalize success";
    options.chunkSize = 1;

    sc::SCBatchExecutionResult result;
    EXPECT_EQ(sc::ExecuteImport(db.Get(), requests, options, &result),
              sc::SC_OK);
    EXPECT_GT(result.importSessionId, 0u);
    EXPECT_EQ(result.createdCount, 1u);

    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
    bool hasRow = false;
    EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
    EXPECT_TRUE(hasRow);

    sc::SCImportRecoveryState recoveryState;
    EXPECT_EQ(
        db->LoadImportRecoveryState(result.importSessionId, &recoveryState),
        sc::SC_E_RECORD_NOT_FOUND);
}

TEST(StorageM2Sqlite, RemoveColumnCleansSqliteFootprint)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_RemoveColumnFootprint.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed beam", edit), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 128), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(beamTable->GetSchema(schema), sc::SC_OK);
    std::int64_t tableId = -1;
    EXPECT_TRUE(QuerySqliteInt64(
        dbPath, "SELECT table_id FROM tables WHERE name = 'Beam' LIMIT 1;",
        &tableId));
    EXPECT_EQ(schema->RemoveColumn(L"Width"), sc::SC_OK);

    std::int64_t schemaColumnCount = -1;
    EXPECT_TRUE(
        QuerySqliteInt64(dbPath,
                         "SELECT COUNT(*) FROM schema_columns sc JOIN tables t "
                         "ON t.table_id = sc.table_id "
                         "WHERE t.name = 'Beam' AND sc.column_name = 'Width';",
                         &schemaColumnCount));
    EXPECT_EQ(schemaColumnCount, 0);

    std::int64_t fieldValueCount = -1;
    EXPECT_TRUE(
        QuerySqliteInt64(dbPath,
                         "SELECT COUNT(*) FROM field_values fv JOIN tables t "
                         "ON t.table_id = fv.table_id "
                         "WHERE t.name = 'Beam' AND fv.column_name = 'Width';",
                         &fieldValueCount));
    EXPECT_EQ(fieldValueCount, 0);

    const std::string indexSql =
        "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'idx_fv_" +
        std::to_string(tableId) + "_Width' LIMIT 1;";
    EXPECT_FALSE(QuerySqliteExists(dbPath, indexSql.c_str()));
}

TEST(StorageM2Sqlite,
     UpdateColumnFailureDuringValueRewriteRestoresSchemaAndRecordValues)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_UpdateColumnRewriteFail.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed", seedEdit), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    const sc::RecordId beamId = beam->GetId();
    EXPECT_EQ(beam->SetInt64(L"Width", 128), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    EXPECT_TRUE(ExecSqliteScript(
        dbPath,
        "CREATE TRIGGER fail_width_value_rewrite "
        "BEFORE DELETE ON field_values "
        "WHEN OLD.column_name = 'Width' "
        "BEGIN "
        "  SELECT RAISE(ABORT, 'forced field value rewrite failure'); "
        "END;"));

    sc::SCSchemaPtr schema;
    EXPECT_EQ(beamTable->GetSchema(schema), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"update width", edit), sc::SC_OK);

    sc::SCColumnDef updated;
    updated.name = L"Width";
    updated.displayName = L"Width Label";
    updated.valueKind = sc::ValueKind::String;
    updated.defaultValue = sc::SCValue::FromString(L"0");
    EXPECT_NE(schema->UpdateColumn(updated), sc::SC_OK);

    sc::SCColumnDef loaded;
    EXPECT_EQ(schema->FindColumn(L"Width", &loaded), sc::SC_OK);
    EXPECT_EQ(loaded.displayName, L"Width");
    EXPECT_EQ(loaded.valueKind, sc::ValueKind::Int64);

    sc::SCRecordPtr reloaded;
    EXPECT_EQ(beamTable->GetRecord(beamId, reloaded), sc::SC_OK);
    std::int64_t width = 0;
    EXPECT_EQ(reloaded->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 128);

    EXPECT_EQ(db->Rollback(edit.Get()), sc::SC_OK);

    EXPECT_EQ(schema->FindColumn(L"Width", &loaded), sc::SC_OK);
    EXPECT_EQ(loaded.displayName, L"Width");
    EXPECT_EQ(loaded.valueKind, sc::ValueKind::Int64);
    EXPECT_EQ(beamTable->GetRecord(beamId, reloaded), sc::SC_OK);
    EXPECT_EQ(reloaded->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 128);
}

TEST(StorageM2Sqlite, SchemaColumnJournalSurvivesReopenAndUndoRedo)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_M2_SchemaJournal.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr table = CreateBeamTable(db);
        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

        sc::SCColumnDef height;
        height.name = L"Height";
        height.displayName = L"Height";
        height.valueKind = sc::ValueKind::Int64;
        height.defaultValue = sc::SCValue::FromInt64(0);

        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"add height", edit), sc::SC_OK);
        EXPECT_EQ(schema->AddColumn(height), sc::SC_OK);
        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    }

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr table;
        EXPECT_EQ(db->GetTable(L"Beam", table), sc::SC_OK);
        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

        sc::SCColumnDef loaded;
        EXPECT_EQ(schema->FindColumn(L"Height", &loaded), sc::SC_OK);
        EXPECT_EQ(loaded.displayName, L"Height");

        EXPECT_EQ(db->Undo(), sc::SC_OK);
        EXPECT_EQ(schema->FindColumn(L"Height", &loaded),
                  sc::SC_E_COLUMN_NOT_FOUND);

        EXPECT_EQ(db->Redo(), sc::SC_OK);
        EXPECT_EQ(schema->FindColumn(L"Height", &loaded), sc::SC_OK);
        EXPECT_EQ(loaded.displayName, L"Height");
    }
}
