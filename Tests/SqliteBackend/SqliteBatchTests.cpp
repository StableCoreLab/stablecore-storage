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

TEST(SqliteBatch, BatchFailureDoesNotLeaveActiveEdit)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Batch_BatchFailureCleanup.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCBatchExecutionOptions options;
    options.editName = L"broken batch";
    options.rollbackOnError = false;

    std::vector<sc::SCBatchTableRequest> requests;
    sc::SCBatchTableRequest request;
    request.tableName = L"Beam";
    request.updates.push_back(sc::SCBatchUpdateRecordRequest{9999, {{L"Width", sc::SCValue::FromInt64(123)}}});
    requests.push_back(request);

    sc::SCBatchExecutionResult result;
    EXPECT_EQ(sc::ExecuteBatchEdit(db.Get(), requests, options, &result), sc::SC_E_RECORD_NOT_FOUND);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"after failed batch", edit), sc::SC_OK);
    EXPECT_EQ(db->Rollback(edit.Get()), sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(cursor->Next(beam), sc::SC_OK);
    EXPECT_FALSE(static_cast<bool>(beam));
}