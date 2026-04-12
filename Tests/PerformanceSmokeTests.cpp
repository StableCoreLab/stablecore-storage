#include <chrono>
#include <filesystem>

#include <gtest/gtest.h>

#include "StableCore/Storage/SCStorage.h"

namespace sc = StableCore::Storage;
namespace fs = std::filesystem;

namespace
{

sc::SCTablePtr CreateIndexedBeamTable(sc::SCDbPtr& db)
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
    width.indexed = true;
    EXPECT_EQ(schema->AddColumn(width), sc::SC_OK);

    sc::SCColumnDef floorRef;
    floorRef.name = L"FloorRef";
    floorRef.displayName = L"FloorRef";
    floorRef.valueKind = sc::ValueKind::RecordId;
    floorRef.columnKind = sc::ColumnKind::Relation;
    floorRef.referenceTable = L"Floor";
    floorRef.indexed = true;
    EXPECT_EQ(schema->AddColumn(floorRef), sc::SC_OK);

    return table;
}

sc::SCTablePtr CreateFloorTable(sc::SCDbPtr& db)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Floor", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef name;
    name.name = L"Name";
    name.displayName = L"Name";
    name.valueKind = sc::ValueKind::String;
    name.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);
    return table;
}

fs::path MakeTempDbPath(const wchar_t* fileName)
{
    fs::path path = fs::temp_directory_path() / fileName;
    std::error_code ec;
    fs::remove(path, ec);
    return path;
}

}  // namespace

TEST(StoragePerformance, SqliteBulkImportAndRelationQuerySmoke)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_PerfSmoke.sqlite");

    sc::SCDbPtr db;
    ASSERT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr floorTable = CreateFloorTable(db);
    sc::SCTablePtr beamTable = CreateIndexedBeamTable(db);

    sc::SCEditPtr seedEdit;
    ASSERT_EQ(db->BeginEdit(L"seed floors", seedEdit), sc::SC_OK);
    sc::SCRecordPtr floor;
    ASSERT_EQ(floorTable->CreateRecord(floor), sc::SC_OK);
    ASSERT_EQ(floor->SetString(L"Name", L"2F"), sc::SC_OK);
    ASSERT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    std::vector<sc::SCBatchTableRequest> requests;
    sc::SCBatchTableRequest beamImport;
    beamImport.tableName = L"Beam";
    for (int i = 0; i < 500; ++i)
    {
        beamImport.creates.push_back(sc::SCBatchCreateRecordRequest{{
            {L"Width", sc::SCValue::FromInt64((i % 5) * 100)},
            {L"FloorRef", sc::SCValue::FromRecordId(floor->GetId())},
        }});
    }
    requests.push_back(beamImport);

    const auto importStart = std::chrono::steady_clock::now();
    sc::SCBatchExecutionResult ISCmportResult;
    ASSERT_EQ(sc::ExecuteImport(db.Get(), requests, sc::ISCmportOptions{L"perf import"}, &ISCmportResult), sc::SC_OK);
    const auto importElapsed = std::chrono::steady_clock::now() - importStart;

    sc::SCRecordCursorPtr floorCursor;
    const auto queryStart = std::chrono::steady_clock::now();
    ASSERT_EQ(beamTable->FindRecords({L"FloorRef", sc::SCValue::FromRecordId(floor->GetId())}, floorCursor), sc::SC_OK);
    const auto queryElapsed = std::chrono::steady_clock::now() - queryStart;

    bool hasRow = false;
    std::int64_t count = 0;
    while (floorCursor->MoveNext(&hasRow) == sc::SC_OK && hasRow)
    {
        ++count;
    }

    EXPECT_EQ(ISCmportResult.createdCount, 500u);
    EXPECT_EQ(count, 500);
    EXPECT_GE(importElapsed.count(), 0);
    EXPECT_GE(queryElapsed.count(), 0);
}
