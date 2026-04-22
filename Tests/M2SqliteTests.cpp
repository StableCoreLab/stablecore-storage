#include <filesystem>

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

bool ExecSqliteScript(const fs::path& dbPath, const char* sql)
{
    sqlite3* db = nullptr;
    const std::string narrowPath = dbPath.string();
    if (sqlite3_open_v2(narrowPath.c_str(), &db, SQLITE_OPEN_READWRITE, nullptr) != SQLITE_OK)
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

bool SetMetadataValue(const fs::path& dbPath, const char* key, const char* value)
{
    const std::string sql =
        std::string("UPDATE metadata SET value='") + value + "' WHERE key='" + key + "';";
    return ExecSqliteScript(dbPath, sql.c_str());
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

}  // namespace

TEST(StorageM2Sqlite, PersistedRecordSurvivesReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_M2_Reopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), db), sc::SC_OK);

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
        EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), reopened), sc::SC_OK);

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
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_M2_UndoRedo.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), db), sc::SC_OK);
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
        EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), reopened), sc::SC_OK);

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

TEST(StorageM2Sqlite, PersistedQueryAndDelete)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_M2_Query.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), db), sc::SC_OK);
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
        EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), reopened), sc::SC_OK);
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
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_M2_SchemaValidation.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), db), sc::SC_OK);

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
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_M2_EmptyQuery.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(beamTable->FindRecords({L"Width", sc::SCValue::FromInt64(12345)}, cursor), sc::SC_OK);

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

    EXPECT_EQ(sc::EvaluateOpenDecision(graph, graph.latestSupportedVersion, true, &decision), sc::SC_OK);
    EXPECT_EQ(decision.mode, sc::SCOpenMode::ReadWrite);
    EXPECT_FALSE(decision.needsUpgrade);
    EXPECT_FALSE(decision.readOnlyOnly);
    EXPECT_TRUE(decision.writable);
}

TEST(StorageM2Sqlite, ReadOnlySqliteOpenRejectsWrites)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_M2_ReadOnly.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr beamTable = CreateBeamTable(db);

        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

        sc::SCRecordPtr beam;
        EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
        EXPECT_EQ(beam->SetInt64(L"Width", 320), sc::SC_OK);
        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    }

    sc::SCDbPtr readOnlyDb;
    EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), true, readOnlyDb), sc::SC_OK);
    EXPECT_EQ(readOnlyDb->GetSchemaVersion(), 2);

    sc::SCEditPtr edit;
    EXPECT_EQ(readOnlyDb->BeginEdit(L"read-only", edit), sc::SC_E_READ_ONLY_DATABASE);
}

TEST(StorageM2Sqlite, UpgradeExecutionIsExplicit)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_M2_ExplicitUpgrade.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), db), sc::SC_OK);
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
    EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), reopened), sc::SC_OK);
    EXPECT_EQ(reopened->GetSchemaVersion(), 1);

    sc::SCVersionGraph graph;
    EXPECT_EQ(sc::BuildDefaultVersionGraph(&graph), sc::SC_OK);

    sc::SCUpgradePlan plan;
    EXPECT_EQ(sc::BuildUpgradePlan(1, graph.latestSupportedVersion, graph, &plan), sc::SC_OK);
    EXPECT_TRUE(plan.requiresConfirmation);
    EXPECT_TRUE(plan.upgradeRequired);

    sc::SCUpgradeResult result;
    EXPECT_EQ(reopened->ExecuteUpgradePlan(plan, false, &result), sc::SC_E_INVALIDARG);
    EXPECT_EQ(result.status, sc::SCUpgradeStatus::Failed);
    EXPECT_EQ(reopened->GetSchemaVersion(), 1);

    EXPECT_EQ(reopened->ExecuteUpgradePlan(plan, true, &result), sc::SC_OK);
    EXPECT_EQ(result.status, sc::SCUpgradeStatus::Success);
    EXPECT_EQ(result.rolledBack, false);
    EXPECT_EQ(reopened->GetSchemaVersion(), graph.latestSupportedVersion);
}

TEST(StorageM2Sqlite, UncleanShutdownBlocksUpgradeExecution)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_M2_UncleanUpgrade.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), db), sc::SC_OK);
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
    EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), reopened), sc::SC_OK);
    EXPECT_EQ(reopened->GetSchemaVersion(), 1);

    sc::SCVersionGraph graph;
    EXPECT_EQ(sc::BuildDefaultVersionGraph(&graph), sc::SC_OK);

    sc::SCOpenDecision decision;
    EXPECT_EQ(sc::EvaluateOpenDecision(graph, reopened->GetSchemaVersion(), false, &decision), sc::SC_OK);
    EXPECT_EQ(decision.mode, sc::SCOpenMode::ReadOnly);

    sc::SCUpgradePlan plan;
    EXPECT_EQ(sc::BuildUpgradePlan(1, graph.latestSupportedVersion, graph, &plan), sc::SC_OK);

    sc::SCUpgradeResult result;
    EXPECT_EQ(reopened->ExecuteUpgradePlan(plan, true, &result), sc::SC_E_WRITE_CONFLICT);
    EXPECT_EQ(result.status, sc::SCUpgradeStatus::Unsupported);
    EXPECT_EQ(result.rolledBack, false);
    EXPECT_EQ(reopened->GetSchemaVersion(), 1);
}

TEST(StorageM2Sqlite, ImportSessionCheckpointIsNotLiveState)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_M2_ImportCheckpoint.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCImportSessionOptions sessionOptions;
    sessionOptions.sessionName = L"chunked import";
    sessionOptions.chunkSize = 1;

    sc::SCImportStagingArea session;
    EXPECT_EQ(sc::BeginImportSession(db.Get(), sessionOptions, &session), sc::SC_OK);

    sc::SCImportChunk chunk;
    chunk.chunkId = 1;
    sc::SCBatchTableRequest request;
    request.tableName = L"Beam";
    request.creates.push_back(sc::SCBatchCreateRecordRequest{{{L"Width", sc::SCValue::FromInt64(140)}}});
    chunk.requests.push_back(request);

    sc::SCImportCheckpoint checkpoint;
    EXPECT_EQ(sc::AppendImportChunk(db.Get(), &session, chunk, &checkpoint), sc::SC_OK);
    EXPECT_EQ(checkpoint.sessionId, session.sessionId);
    EXPECT_EQ(checkpoint.chunkCount, 1u);
    EXPECT_TRUE(checkpoint.persisted);

    sc::SCImportRecoveryState recoveryState;
    EXPECT_EQ(sc::GetImportRecoveryState(db.Get(), session.sessionId, &recoveryState), sc::SC_OK);
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
    EXPECT_EQ(sc::FinalizeImportSession(db.Get(), session, commit, &result), sc::SC_OK);
    EXPECT_EQ(result.importSessionId, session.sessionId);
    EXPECT_EQ(result.chunkCount, 1u);

    EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
    EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
    EXPECT_TRUE(hasRow);
}

TEST(StorageM2Sqlite, ExecuteImportUsesChunkedSessionModel)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_M2_ChunkedImport.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    std::vector<sc::SCBatchTableRequest> requests;
    for (int i = 0; i < 4; ++i)
    {
        sc::SCBatchTableRequest request;
        request.tableName = L"Beam";
        request.creates.push_back(sc::SCBatchCreateRecordRequest{{{L"Width", sc::SCValue::FromInt64(100 + i)}}});
        requests.push_back(request);
    }

    sc::SCBatchExecutionOptions options;
    options.editName = L"chunked import";
    options.chunkSize = 2;

    sc::SCBatchExecutionResult result;
    EXPECT_EQ(sc::ExecuteImport(db.Get(), requests, options, &result), sc::SC_OK);
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
