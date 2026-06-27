#include <filesystem>
#include <utility>

#include <gtest/gtest.h>
#include <sqlite3.h>

#include "SCStorage.h"

#include "Support/TestPaths.h"
#include "Support/TestSqliteHelpers.h"

namespace sc = StableCore::Storage;
namespace fs = std::filesystem;

namespace
{
    struct RecordingObserver final : sc::ISCDatabaseObserver
    {
        void OnDatabaseChanged(const sc::SCChangeSet& changeSet) override
        {
            seen.push_back(changeSet);
        }

        std::vector<sc::SCChangeSet> seen;
    };

    sc::ErrorCode OpenTestFileDb(const wchar_t* path, sc::SCDbPtr& db)
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

    std::vector<std::pair<std::wstring, std::wstring>> CollectRenamePairs(
        const sc::SCChangeSet& changeSet)
    {
        std::vector<std::pair<std::wstring, std::wstring>> pairs;
        for (const sc::SCDataChange& change : changeSet.changes)
        {
            if (!change.structuralChange ||
                change.kind != sc::ChangeKind::TableRenamed)
            {
                continue;
            }

            std::wstring fromName;
            std::wstring toName;
            if (change.oldValue.AsStringCopy(&fromName) != sc::SC_OK ||
                change.newValue.AsStringCopy(&toName) != sc::SC_OK)
            {
                continue;
            }
            pairs.emplace_back(std::move(fromName), std::move(toName));
        }
        return pairs;
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
    (void)beamTable;
    (void)beamTable;
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
    EXPECT_EQ(OpenTestFileDb(dbPath.c_str(), reopened), sc::SC_OK);

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
    EXPECT_EQ(OpenTestFileDb(dbPath.c_str(), db), sc::SC_OK);
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
    EXPECT_EQ(editingStateAfter.open, editingStateBefore.open);
    EXPECT_EQ(editingStateAfter.dirty, editingStateBefore.dirty);
    EXPECT_EQ(editingStateAfter.openMode, editingStateBefore.openMode);
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

TEST(SqliteEditLog, RenameTableChangeSetsPreserveEveryRenameInUndoRedo)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_Sqlite_EditLog_RenamePairs.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(OpenTestFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);
    sc::SCTablePtr floorTable;
    EXPECT_EQ(db->CreateTable(L"Floor", floorTable), sc::SC_OK);
    sc::SCSchemaPtr floorSchema;
    EXPECT_EQ(floorTable->GetSchema(floorSchema), sc::SC_OK);
    sc::SCColumnDef floorWidth;
    floorWidth.name = L"Width";
    floorWidth.displayName = L"Width";
    floorWidth.valueKind = sc::ValueKind::Int64;
    floorWidth.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(floorSchema->AddColumn(floorWidth), sc::SC_OK);
    (void)floorTable;

    RecordingObserver observer;
    EXPECT_EQ(db->AddObserver(&observer), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"rename pair", edit), sc::SC_OK);
    EXPECT_EQ(db->RenameTable(L"Beam", L"BeamRenamed"), sc::SC_OK);
    EXPECT_EQ(db->RenameTable(L"Floor", L"FloorRenamed"), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    ASSERT_FALSE(observer.seen.empty());
    const sc::SCChangeSet& commitChangeSet = observer.seen.back();
    EXPECT_EQ(commitChangeSet.source, sc::ChangeSource::UserEdit);
    const auto commitPairs = CollectRenamePairs(commitChangeSet);
    ASSERT_EQ(commitPairs.size(), 2u);
    EXPECT_EQ(commitPairs[0], std::make_pair(std::wstring(L"Beam"), std::wstring(L"BeamRenamed")));
    EXPECT_EQ(commitPairs[1], std::make_pair(std::wstring(L"Floor"), std::wstring(L"FloorRenamed")));

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    ASSERT_FALSE(observer.seen.empty());
    const sc::SCChangeSet& undoChangeSet = observer.seen.back();
    EXPECT_EQ(undoChangeSet.source, sc::ChangeSource::Undo);
    const auto undoPairs = CollectRenamePairs(undoChangeSet);
    ASSERT_EQ(undoPairs.size(), 2u);
    EXPECT_EQ(undoPairs[0], std::make_pair(std::wstring(L"BeamRenamed"), std::wstring(L"Beam")));
    EXPECT_EQ(undoPairs[1], std::make_pair(std::wstring(L"FloorRenamed"), std::wstring(L"Floor")));

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    ASSERT_FALSE(observer.seen.empty());
    const sc::SCChangeSet& redoChangeSet = observer.seen.back();
    EXPECT_EQ(redoChangeSet.source, sc::ChangeSource::Redo);
    const auto redoPairs = CollectRenamePairs(redoChangeSet);
    ASSERT_EQ(redoPairs.size(), 2u);
    EXPECT_EQ(redoPairs[0], std::make_pair(std::wstring(L"Beam"), std::wstring(L"BeamRenamed")));
    EXPECT_EQ(redoPairs[1], std::make_pair(std::wstring(L"Floor"), std::wstring(L"FloorRenamed")));

    EXPECT_EQ(db->RemoveObserver(&observer), sc::SC_OK);
}

TEST(SqliteEditLog, SchemaEditsDoNotEmitRenameTableChanges)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_Sqlite_EditLog_NoFalseRename.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(OpenTestFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);
    (void)beamTable;

    RecordingObserver observer;
    EXPECT_EQ(db->AddObserver(&observer), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"add schema column", edit), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->GetTable(L"Beam", table), sc::SC_OK);
    ASSERT_TRUE(table != nullptr);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef height;
    height.name = L"Height";
    height.displayName = L"Height";
    height.valueKind = sc::ValueKind::Int64;
    height.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(height), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    ASSERT_FALSE(observer.seen.empty());
    const sc::SCChangeSet& changeSet = observer.seen.back();
    EXPECT_TRUE(CollectRenamePairs(changeSet).empty());

    bool sawFieldUpdate = false;
    for (const sc::SCDataChange& change : changeSet.changes)
    {
        if (change.tableName == L"Beam" && change.kind == sc::ChangeKind::FieldUpdated)
        {
            sawFieldUpdate = true;
        }
        EXPECT_NE(change.kind, sc::ChangeKind::TableRenamed);
    }
    EXPECT_TRUE(sawFieldUpdate);

    EXPECT_EQ(db->RemoveObserver(&observer), sc::SC_OK);
}

TEST(SqliteEditLog, RenameTablePreservesEarlierJournaledWritesInSameEdit)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_Sqlite_EditLog_RenameMixedWrites.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(OpenTestFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"rename after writes", edit), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    ASSERT_TRUE(beam != nullptr);
    const sc::RecordId recordId = beam->GetId();
    EXPECT_EQ(beam->SetInt64(L"Width", 11), sc::SC_OK);

    EXPECT_EQ(db->RenameTable(L"Beam", L"BeamRenamed"), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 17), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    sc::SCTablePtr renamedTable;
    EXPECT_EQ(db->GetTable(L"BeamRenamed", renamedTable), sc::SC_OK);
    ASSERT_TRUE(renamedTable != nullptr);

    sc::SCRecordPtr reloaded;
    EXPECT_EQ(renamedTable->GetRecord(recordId, reloaded), sc::SC_OK);
    ASSERT_TRUE(reloaded != nullptr);
    std::int64_t width = 0;
    EXPECT_EQ(reloaded->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 17);

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    sc::SCTablePtr originalTable;
    EXPECT_EQ(db->GetTable(L"Beam", originalTable), sc::SC_OK);
    ASSERT_TRUE(originalTable != nullptr);
    sc::SCRecordCursorPtr originalCursor;
    EXPECT_EQ(originalTable->EnumerateRecords(originalCursor), sc::SC_OK);
    std::size_t originalCount = 0;
    sc::SCRecordPtr originalRecord;
    while (originalCursor->Next(originalRecord) == sc::SC_OK && originalRecord)
    {
        ++originalCount;
    }
    EXPECT_EQ(originalCount, 0u);
    sc::SCRecordPtr missing;
    EXPECT_EQ(originalTable->GetRecord(recordId, missing), sc::SC_OK);
    ASSERT_TRUE(missing != nullptr);
    EXPECT_TRUE(missing->IsDeleted());

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_EQ(db->GetTable(L"BeamRenamed", renamedTable), sc::SC_OK);
    ASSERT_TRUE(renamedTable != nullptr);
    EXPECT_EQ(renamedTable->GetRecord(recordId, reloaded), sc::SC_OK);
    ASSERT_TRUE(reloaded != nullptr);
    width = 0;
    EXPECT_EQ(reloaded->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 17);
}

TEST(SqliteEditLog, RenameTableRollbackRestoresOriginalTableWithinActiveEdit)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_Sqlite_EditLog_RenameRollback.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(OpenTestFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"rename rollback", edit), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    ASSERT_TRUE(beam != nullptr);
    EXPECT_EQ(beam->SetInt64(L"Width", 24), sc::SC_OK);

    EXPECT_EQ(db->RenameTable(L"Beam", L"BeamRenamed"), sc::SC_OK);
    EXPECT_EQ(db->Rollback(edit.Get()), sc::SC_OK);

    sc::SCTablePtr originalTable;
    EXPECT_EQ(db->GetTable(L"Beam", originalTable), sc::SC_OK);
    ASSERT_TRUE(originalTable != nullptr);

    sc::SCTablePtr renamedTable;
    EXPECT_NE(db->GetTable(L"BeamRenamed", renamedTable), sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(originalTable->EnumerateRecords(cursor), sc::SC_OK);
    std::size_t count = 0;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        ++count;
    }
    EXPECT_EQ(count, 0u);
}

TEST(SqliteEditLog, RenameTableIsNotPersistedBeforeActiveEditCommit)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_Sqlite_EditLog_RenameDeferred.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(OpenTestFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);
    (void)beamTable;

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"rename deferred", edit), sc::SC_OK);
    EXPECT_EQ(db->RenameTable(L"Beam", L"BeamRenamed"), sc::SC_OK);

    sc::SCTablePtr renamedInEdit;
    EXPECT_EQ(db->GetTable(L"BeamRenamed", renamedInEdit), sc::SC_OK);
    ASSERT_TRUE(renamedInEdit != nullptr);

    sc::SCDbPtr reopened;
    EXPECT_EQ(OpenTestFileDb(dbPath.c_str(), reopened), sc::SC_OK);

    sc::SCTablePtr persistedBeam;
    EXPECT_EQ(reopened->GetTable(L"Beam", persistedBeam), sc::SC_OK);
    ASSERT_TRUE(persistedBeam != nullptr);

    sc::SCTablePtr persistedRenamed;
    EXPECT_NE(reopened->GetTable(L"BeamRenamed", persistedRenamed), sc::SC_OK);

    EXPECT_EQ(db->Rollback(edit.Get()), sc::SC_OK);
}

TEST(SqliteEditLog, UndoFailureAfterPartialReverseRestoresOnlyAppliedEntries)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_Sqlite_EditLog_PartialUndoCompensation.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(OpenTestFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);
    ASSERT_TRUE(beamTable != nullptr);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed row", seedEdit), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    ASSERT_TRUE(beam != nullptr);
    const sc::RecordId recordId = beam->GetId();
    EXPECT_EQ(beam->SetInt64(L"Width", 10), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(beamTable->GetSchema(schema), sc::SC_OK);
    ASSERT_TRUE(schema != nullptr);

    sc::SCEditPtr compoundEdit;
    EXPECT_EQ(db->BeginEdit(L"add column and update value", compoundEdit), sc::SC_OK);

    sc::SCColumnDef height;
    height.name = L"Height";
    height.displayName = L"Height";
    height.valueKind = sc::ValueKind::Int64;
    height.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(height), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 20), sc::SC_OK);
    EXPECT_EQ(db->Commit(compoundEdit.Get()), sc::SC_OK);

    sc::SCEditingDatabaseState editingStateBefore;
    EXPECT_EQ(db->GetEditingState(&editingStateBefore), sc::SC_OK);
    sc::SCEditLogState logStateBefore;
    EXPECT_EQ(db->GetEditLogState(&logStateBefore), sc::SC_OK);

    RecordingObserver observer;
    EXPECT_EQ(db->AddObserver(&observer), sc::SC_OK);

    EXPECT_TRUE(ExecSqliteScript(dbPath, "DROP TABLE schema_columns;"));

    EXPECT_NE(db->Undo(), sc::SC_OK);

    sc::SCEditingDatabaseState editingStateAfter;
    EXPECT_EQ(db->GetEditingState(&editingStateAfter), sc::SC_OK);
    EXPECT_EQ(editingStateAfter.currentVersion, editingStateBefore.currentVersion);
    EXPECT_EQ(editingStateAfter.baselineVersion, editingStateBefore.baselineVersion);
    EXPECT_EQ(editingStateAfter.undoCount, editingStateBefore.undoCount);
    EXPECT_EQ(editingStateAfter.redoCount, editingStateBefore.redoCount);

    sc::SCEditLogState logStateAfter;
    EXPECT_EQ(db->GetEditLogState(&logStateAfter), sc::SC_OK);
    EXPECT_EQ(logStateAfter.undoItems.size(), logStateBefore.undoItems.size());
    EXPECT_EQ(logStateAfter.redoItems.size(), logStateBefore.redoItems.size());

    EXPECT_TRUE(observer.seen.empty());
    EXPECT_EQ(db->RemoveObserver(&observer), sc::SC_OK);

    sc::SCEditPtr recoveryEdit;
    EXPECT_EQ(db->BeginEdit(L"recovery", recoveryEdit), sc::SC_OK);
    EXPECT_EQ(db->Commit(recoveryEdit.Get()), sc::SC_OK);

    sc::SCEditingDatabaseState editingStateRecovered;
    EXPECT_EQ(db->GetEditingState(&editingStateRecovered), sc::SC_OK);
    EXPECT_EQ(editingStateRecovered.dirty, editingStateBefore.dirty);
    EXPECT_EQ(editingStateRecovered.currentVersion, editingStateBefore.currentVersion);
    EXPECT_EQ(editingStateRecovered.baselineVersion, editingStateBefore.baselineVersion);

    sc::SCRecordPtr reloaded;
    EXPECT_EQ(beamTable->GetRecord(recordId, reloaded), sc::SC_OK);
    ASSERT_TRUE(reloaded != nullptr);

    std::int64_t width = 0;
    EXPECT_EQ(reloaded->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 20);

    sc::SCColumnDef loadedHeight;
    EXPECT_EQ(schema->FindColumn(L"Height", &loadedHeight), sc::SC_OK);
    EXPECT_EQ(loadedHeight.valueKind, sc::ValueKind::Int64);
}
