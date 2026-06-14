#include <filesystem>

#include <gtest/gtest.h>

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
}

TEST(SqliteBackup, CreateBackupCopyPreservesOrTrimsJournalHistory)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Backup_Level2.sqlite");
    const fs::path trimmedBackupPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Backup_Level2_Trimmed.sqlite");
    const fs::path preservedBackupPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Backup_Level2_Preserved.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

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
    EXPECT_EQ(db->CreateBackupCopy(trimmedBackupPath.c_str(), trimmedOptions, &trimmedResult), sc::SC_OK);
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
    EXPECT_EQ(db->CreateBackupCopy(preservedBackupPath.c_str(), preservedOptions, &preservedResult), sc::SC_OK);
    EXPECT_EQ(preservedResult.removedJournalTransactionCount, 0u);
    EXPECT_EQ(preservedResult.removedJournalEntryCount, 0u);
    EXPECT_GT(preservedResult.outputFileSizeBytes, 0u);

    sc::SCDbPtr preservedDb;
    ASSERT_EQ(CreateFileDb(preservedBackupPath.c_str(), preservedDb), sc::SC_OK);

    sc::SCEditLogState preservedLog;
    EXPECT_EQ(preservedDb->GetEditLogState(&preservedLog), sc::SC_OK);
    EXPECT_EQ(preservedLog.baselineVersion, sourceState.baselineVersion);
    ASSERT_EQ(preservedLog.undoItems.size(), 1u);
    EXPECT_TRUE(preservedLog.redoItems.empty());
    EXPECT_EQ(preservedLog.undoItems.front().kind, sc::SCEditLogActionKind::Commit);

    sc::SCEditingDatabaseState preservedState;
    EXPECT_EQ(preservedDb->GetEditingState(&preservedState), sc::SC_OK);
    EXPECT_EQ(preservedState.currentVersion, sourceState.currentVersion);
    EXPECT_EQ(preservedState.baselineVersion, sourceState.baselineVersion);

    sc::SCBackupResult overwriteCheckResult;
    EXPECT_EQ(db->CreateBackupCopy(trimmedBackupPath.c_str(), trimmedOptions, &overwriteCheckResult),
              sc::SC_E_FILE_EXISTS);
}