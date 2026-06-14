#include <filesystem>

#include <gtest/gtest.h>
#include <sqlite3.h>

#include "SCStorage.h"

#include "Support/TestPaths.h"

namespace sc = StableCore::Storage;
namespace fs = std::filesystem;

namespace
{
    sc::ErrorCode CreateFileDb(const wchar_t* path, sc::SCDbPtr& db)
    {
        return sc::CreateFileDatabase(path, sc::SCOpenDatabaseOptions{}, db);
    }

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

    class SqliteReadTransactionLock
    {
    public:
        explicit SqliteReadTransactionLock(const fs::path& dbPath)
        {
            const std::string narrowPath = dbPath.string();
            if (sqlite3_open_v2(narrowPath.c_str(), &db_, SQLITE_OPEN_READWRITE, nullptr) != SQLITE_OK)
            {
                if (db_ != nullptr)
                {
                    sqlite3_close(db_);
                    db_ = nullptr;
                }
                return;
            }

            if (sqlite3_exec(db_, "BEGIN;", nullptr, nullptr, nullptr) != SQLITE_OK)
            {
                sqlite3_close(db_);
                db_ = nullptr;
                return;
            }

            if (sqlite3_exec(db_, "SELECT COUNT(*) FROM metadata;", nullptr, nullptr, nullptr) != SQLITE_OK)
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
}

TEST(SqliteEditLog, OpenModeAndEditLogStateAreQueryable)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_EditLog_State.sqlite");

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
    const sc::VersionId firstCommittedVersion = logState.undoItems.front().version;
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
    const sc::VersionId secondCommittedVersion = logState.undoItems.front().version;
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
    EXPECT_EQ(reopenedLog.undoItems.front().kind, sc::SCEditLogActionKind::Commit);
}

TEST(SqliteEditLog, ResetHistoryBaselineCommitFailureKeepsEditLogStateUnchanged)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_EditLog_ResetBaselineBusy.sqlite");

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

    const sc::CommitId undoCommitIdBefore = logStateBefore.undoItems.front().commitId;
    const sc::CommitId redoCommitIdBefore = logStateBefore.redoItems.front().commitId;

    SqliteReadTransactionLock readLock(dbPath);
    ASSERT_TRUE(readLock.IsHeld());

    sc::SCBackupResult resetResult;
    EXPECT_NE(db->ResetHistoryBaseline(&resetResult), sc::SC_OK);

    sc::SCEditingDatabaseState editingStateAfter;
    EXPECT_EQ(db->GetEditingState(&editingStateAfter), sc::SC_OK);
    EXPECT_EQ(editingStateAfter.currentVersion, editingStateBefore.currentVersion);
    EXPECT_EQ(editingStateAfter.baselineVersion, editingStateBefore.baselineVersion);
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

TEST(SqliteEditLog, NoHistoryOpenModeHidesEditLog)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_EditLog_NoHistory.sqlite");

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