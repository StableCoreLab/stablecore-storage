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

    class CommitFailingDatabase final : public sc::ISCDatabase, public sc::SCRefCountedObject
    {
    public:
        explicit CommitFailingDatabase(sc::SCDbPtr inner) : inner_(std::move(inner))
        {
        }

        sc::ErrorCode BeginEdit(const wchar_t* name, sc::SCEditPtr& outEdit) override
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
        sc::ErrorCode GetTableName(std::int32_t index, std::wstring* outName) override
        {
            return inner_->GetTableName(index, outName);
        }
        sc::ErrorCode GetTable(const wchar_t* name, sc::SCTablePtr& outTable) override
        {
            return inner_->GetTable(name, outTable);
        }
        sc::ErrorCode CreateTable(const wchar_t* name, sc::SCTablePtr& outTable) override
        {
            return inner_->CreateTable(name, outTable);
        }
        sc::ErrorCode DeleteTable(const wchar_t* name) override
        {
            return inner_->DeleteTable(name);
        }
        sc::ErrorCode ClearColumnValues(sc::ISCTable* table, const wchar_t* name) override
        {
            return inner_->ClearColumnValues(table, name);
        }
        sc::ErrorCode ExecuteUpgradePlan(const sc::SCUpgradePlan& plan,
                                         bool confirmed,
                                         sc::SCUpgradeResult* outResult) override
        {
            return inner_->ExecuteUpgradePlan(plan, confirmed, outResult);
        }
        sc::ErrorCode BeginImportSession(const sc::SCImportSessionOptions& options,
                                         sc::SCImportStagingArea* outSession) override
        {
            return inner_->BeginImportSession(options, outSession);
        }
        sc::ErrorCode AppendImportChunk(sc::SCImportStagingArea* session,
                                        const sc::SCImportChunk& chunk,
                                        sc::SCImportCheckpoint* outCheckpoint) override
        {
            return inner_->AppendImportChunk(session, chunk, outCheckpoint);
        }
        sc::ErrorCode LoadImportRecoveryState(std::uint64_t sessionId, sc::SCImportRecoveryState* outState) override
        {
            return inner_->LoadImportRecoveryState(sessionId, outState);
        }
        sc::ErrorCode FinalizeImportSession(const sc::SCImportFinalizeCommit& commit,
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
}

TEST(SqliteImport, ImportSessionCheckpointIsNotLiveState)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Import_Checkpoint.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

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
    sc::SCRecordPtr beam;
    EXPECT_EQ(cursor->Next(beam), sc::SC_OK);
    EXPECT_FALSE(static_cast<bool>(beam));

    sc::SCImportFinalizeCommit commit;
    commit.sessionId = session.sessionId;
    commit.confirmed = true;
    commit.commitName = L"finalize import";
    sc::SCBatchExecutionResult result;
    EXPECT_EQ(sc::FinalizeImportSession(db.Get(), session, commit, &result), sc::SC_OK);
    EXPECT_EQ(result.importSessionId, session.sessionId);
    EXPECT_EQ(result.chunkCount, 1u);

    EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
    sc::SCRecordPtr finalizedBeam;
    EXPECT_EQ(cursor->Next(finalizedBeam), sc::SC_OK);
    EXPECT_TRUE(static_cast<bool>(finalizedBeam));
}

TEST(SqliteImport, RestartedDatabaseCanFinalizeImportRecoveryState)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Import_RestartRecovery.sqlite");
    sc::SCImportSessionId sessionId = 0;

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
        sc::SCTablePtr beamTable = CreateBeamTable(db);

        sc::SCImportSessionOptions sessionOptions;
        sessionOptions.sessionName = L"restart recovery import";
        sessionOptions.chunkSize = 1;

        sc::SCImportStagingArea session;
        EXPECT_EQ(sc::BeginImportSession(db.Get(), sessionOptions, &session), sc::SC_OK);
        sessionId = session.sessionId;

        sc::SCImportChunk chunk;
        chunk.chunkId = 1;
        sc::SCBatchTableRequest request;
        request.tableName = L"Beam";
        request.creates.push_back(sc::SCBatchCreateRecordRequest{{{L"Width", sc::SCValue::FromInt64(210)}}});
        chunk.requests.push_back(request);

        sc::SCImportCheckpoint checkpoint;
        EXPECT_EQ(sc::AppendImportChunk(db.Get(), &session, chunk, &checkpoint), sc::SC_OK);

        sc::SCImportRecoveryState recoveryState;
        EXPECT_EQ(sc::GetImportRecoveryState(db.Get(), sessionId, &recoveryState), sc::SC_OK);
        EXPECT_TRUE(recoveryState.canResume);
        EXPECT_TRUE(recoveryState.canFinalize);

        sc::SCRecordCursorPtr cursor;
        EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
        sc::SCRecordPtr beam;
        EXPECT_EQ(cursor->Next(beam), sc::SC_OK);
        EXPECT_FALSE(static_cast<bool>(beam));
    }

    sc::SCDbPtr reopened;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);

    sc::SCImportRecoveryState recoveryState;
    EXPECT_EQ(reopened->LoadImportRecoveryState(sessionId, &recoveryState), sc::SC_OK);
    EXPECT_EQ(recoveryState.sessionId, sessionId);
    EXPECT_TRUE(recoveryState.canResume);
    EXPECT_TRUE(recoveryState.canFinalize);
    EXPECT_EQ(recoveryState.stagingArea.chunks.size(), 1u);

    sc::SCImportFinalizeCommit commit;
    commit.sessionId = sessionId;
    commit.confirmed = true;
    commit.commitName = L"restart recovery import";
    sc::SCBatchExecutionResult result;
    EXPECT_EQ(sc::FinalizeImportSession(reopened.Get(), recoveryState.stagingArea, commit, &result), sc::SC_OK);
    EXPECT_EQ(result.importSessionId, sessionId);
    EXPECT_EQ(result.createdCount, 1u);

    sc::SCTablePtr beamTable;
    EXPECT_EQ(reopened->GetTable(L"Beam", beamTable), sc::SC_OK);
    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(cursor->Next(beam), sc::SC_OK);
    EXPECT_TRUE(static_cast<bool>(beam));

    EXPECT_EQ(reopened->LoadImportRecoveryState(sessionId, &recoveryState), sc::SC_E_RECORD_NOT_FOUND);
}

TEST(SqliteImport, AbortImportSessionClearsRecoveryState)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Import_Abort.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    CreateBeamTable(db);

    sc::SCImportSessionOptions sessionOptions;
    sessionOptions.sessionName = L"abort import";
    sessionOptions.chunkSize = 1;

    sc::SCImportStagingArea session;
    EXPECT_EQ(sc::BeginImportSession(db.Get(), sessionOptions, &session), sc::SC_OK);

    sc::SCImportChunk chunk;
    chunk.chunkId = 1;
    sc::SCBatchTableRequest request;
    request.tableName = L"Beam";
    request.creates.push_back(sc::SCBatchCreateRecordRequest{{{L"Width", sc::SCValue::FromInt64(180)}}});
    chunk.requests.push_back(request);

    sc::SCImportCheckpoint checkpoint;
    EXPECT_EQ(sc::AppendImportChunk(db.Get(), &session, chunk, &checkpoint), sc::SC_OK);

    sc::SCImportRecoveryState recoveryState;
    EXPECT_EQ(sc::GetImportRecoveryState(db.Get(), session.sessionId, &recoveryState), sc::SC_OK);
    EXPECT_TRUE(recoveryState.canResume);
    EXPECT_TRUE(recoveryState.canFinalize);

    EXPECT_EQ(sc::AbortImportSession(db.Get(), session.sessionId), sc::SC_OK);
    EXPECT_EQ(sc::GetImportRecoveryState(db.Get(), session.sessionId, &recoveryState), sc::SC_E_RECORD_NOT_FOUND);

    sc::SCImportFinalizeCommit commit;
    commit.sessionId = session.sessionId;
    commit.confirmed = true;
    commit.commitName = L"abort import";
    sc::SCBatchExecutionResult result;
    EXPECT_EQ(sc::FinalizeImportSession(db.Get(), session, commit, &result), sc::SC_E_RECORD_NOT_FOUND);
}

TEST(SqliteImport, ExecuteImportUsesChunkedSessionModel)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Import_Chunked.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
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
    sc::SCRecordPtr beam;
    std::size_t count = 0;
    while (cursor->Next(beam) == sc::SC_OK && beam)
    {
        ++count;
    }
    EXPECT_EQ(count, 4u);
}

TEST(SqliteImport, ExecuteImportKeepsRecoveryStateWhenCommitFails)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Import_CommitFailure.sqlite");

    sc::SCDbPtr realDb;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), realDb), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(realDb);

    sc::SCRefPtr<CommitFailingDatabase> proxy = sc::SCMakeRef<CommitFailingDatabase>(realDb);

    std::vector<sc::SCBatchTableRequest> requests;
    sc::SCBatchTableRequest request;
    request.tableName = L"Beam";
    request.creates.push_back(sc::SCBatchCreateRecordRequest{{{L"Width", sc::SCValue::FromInt64(256)}}});
    requests.push_back(request);

    sc::SCBatchExecutionOptions options;
    options.editName = L"import commit failure";
    options.chunkSize = 1;

    sc::SCBatchExecutionResult result;
    EXPECT_EQ(sc::ExecuteImport(proxy.Get(), requests, options, &result), sc::SC_E_FAIL);

    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(cursor->Next(beam), sc::SC_OK);
    EXPECT_FALSE(static_cast<bool>(beam));

    sc::SCImportRecoveryState recoveryState;
    EXPECT_EQ(proxy->LoadImportRecoveryState(1, &recoveryState), sc::SC_OK);
    EXPECT_EQ(recoveryState.sessionId, 1u);
    EXPECT_EQ(recoveryState.state, sc::SCImportSessionState::Checkpointed);
    EXPECT_TRUE(recoveryState.canResume);
    EXPECT_TRUE(recoveryState.canFinalize);
    EXPECT_TRUE(recoveryState.checkpointPersisted);
}

TEST(SqliteImport, ExecuteImportClearsRecoveryStateOnSuccess)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Import_FinalizeSuccess.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    std::vector<sc::SCBatchTableRequest> requests;
    sc::SCBatchTableRequest request;
    request.tableName = L"Beam";
    request.creates.push_back(sc::SCBatchCreateRecordRequest{{{L"Width", sc::SCValue::FromInt64(320)}}});
    requests.push_back(request);

    sc::SCBatchExecutionOptions options;
    options.editName = L"import finalize success";
    options.chunkSize = 1;

    sc::SCBatchExecutionResult result;
    EXPECT_EQ(sc::ExecuteImport(db.Get(), requests, options, &result), sc::SC_OK);
    EXPECT_GT(result.importSessionId, 0u);
    EXPECT_EQ(result.createdCount, 1u);

    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(cursor->Next(beam), sc::SC_OK);
    EXPECT_TRUE(static_cast<bool>(beam));

    sc::SCImportRecoveryState recoveryState;
    EXPECT_EQ(db->LoadImportRecoveryState(result.importSessionId, &recoveryState), sc::SC_E_RECORD_NOT_FOUND);
}