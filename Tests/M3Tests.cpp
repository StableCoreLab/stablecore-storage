#include <algorithm>
#include <filesystem>
#include <functional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "StableCore/Storage/SCStorage.h"

namespace sc = stablecore::storage;
namespace fs = std::filesystem;

namespace
{

struct RecordingObserver final : sc::ISCDatabaseObserver
{
    void OnDatabaseChanged(const sc::SCChangeSet& SCChangeSet) override
    {
        seen.push_back(SCChangeSet);
    }

    std::vector<sc::SCChangeSet> seen;
};

using CreateDbFn = std::function<sc::SCDbPtr()>;

fs::path MakeTempDbPath(const wchar_t* fileName)
{
    fs::path path = fs::temp_directory_path() / fileName;
    std::error_code ec;
    fs::remove(path, ec);
    return path;
}

sc::SCTablePtr CreateFloorTable(sc::SCDbPtr& db)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Floor", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef title;
    title.name = L"Title";
    title.displayName = L"Title";
    title.valueKind = sc::ValueKind::String;
    title.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(schema->AddColumn(title), sc::SC_OK);

    return table;
}

sc::SCTablePtr CreateBeamTable(sc::SCDbPtr& db)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef name;
    name.name = L"Name";
    name.displayName = L"Name";
    name.valueKind = sc::ValueKind::String;
    name.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);

    sc::SCColumnDef floorRef;
    floorRef.name = L"FloorRef";
    floorRef.displayName = L"FloorRef";
    floorRef.valueKind = sc::ValueKind::RecordId;
    floorRef.columnKind = sc::ColumnKind::Relation;
    floorRef.referenceTable = L"Floor";
    floorRef.nullable = true;
    EXPECT_EQ(schema->AddColumn(floorRef), sc::SC_OK);

    return table;
}

std::vector<std::wstring> CollectBeamNames(sc::SCTablePtr& beamTable, sc::RecordId floorId)
{
    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(beamTable->FindRecords({L"FloorRef", sc::SCValue::FromRecordId(floorId)}, cursor), sc::SC_OK);

    std::vector<std::wstring> names;
    bool hasRow = false;
    while (cursor->MoveNext(&hasRow) == sc::SC_OK && hasRow)
    {
        sc::SCRecordPtr beam;
        EXPECT_EQ(cursor->GetCurrent(beam), sc::SC_OK);

        std::wstring name;
        EXPECT_EQ(beam->GetStringCopy(L"Name", &name), sc::SC_OK);
        names.push_back(std::move(name));
    }

    return names;
}

void RunM3RelationBaseline(const CreateDbFn& createDb)
{
    sc::SCDbPtr db = createDb();
    ASSERT_TRUE(static_cast<bool>(db));

    sc::SCTablePtr floorTable = CreateFloorTable(db);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    RecordingObserver observer;
    EXPECT_EQ(db->AddObserver(&observer), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    ASSERT_EQ(db->BeginEdit(L"seed relation baseline", seedEdit), sc::SC_OK);

    sc::SCRecordPtr floor2;
    ASSERT_EQ(floorTable->CreateRecord(floor2), sc::SC_OK);
    ASSERT_EQ(floor2->SetString(L"Title", L"2F"), sc::SC_OK);

    sc::SCRecordPtr floor3;
    ASSERT_EQ(floorTable->CreateRecord(floor3), sc::SC_OK);
    ASSERT_EQ(floor3->SetString(L"Title", L"3F"), sc::SC_OK);

    sc::SCRecordPtr beamA;
    ASSERT_EQ(beamTable->CreateRecord(beamA), sc::SC_OK);
    ASSERT_EQ(beamA->SetString(L"Name", L"B-A"), sc::SC_OK);
    ASSERT_EQ(beamA->SetRef(L"FloorRef", floor2->GetId()), sc::SC_OK);

    sc::SCRecordPtr beamB;
    ASSERT_EQ(beamTable->CreateRecord(beamB), sc::SC_OK);
    ASSERT_EQ(beamB->SetString(L"Name", L"B-B"), sc::SC_OK);
    ASSERT_EQ(beamB->SetRef(L"FloorRef", floor2->GetId()), sc::SC_OK);

    sc::SCRecordPtr beamC;
    ASSERT_EQ(beamTable->CreateRecord(beamC), sc::SC_OK);
    ASSERT_EQ(beamC->SetString(L"Name", L"B-C"), sc::SC_OK);
    ASSERT_EQ(beamC->SetRef(L"FloorRef", floor3->GetId()), sc::SC_OK);

    ASSERT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    const std::vector<std::wstring> floor2Beams = CollectBeamNames(beamTable, floor2->GetId());
    EXPECT_EQ(floor2Beams.size(), 2u);
    EXPECT_NE(std::find(floor2Beams.begin(), floor2Beams.end(), L"B-A"), floor2Beams.end());
    EXPECT_NE(std::find(floor2Beams.begin(), floor2Beams.end(), L"B-B"), floor2Beams.end());

    bool sawRelationUpdated = false;
    for (const auto& SCChangeSet : observer.seen)
    {
        for (const auto& change : SCChangeSet.changes)
        {
            if (change.kind == sc::ChangeKind::RelationUpdated
                && change.tableName == L"Beam"
                && change.fieldName == L"FloorRef")
            {
                sawRelationUpdated = true;
            }
        }
    }
    EXPECT_TRUE(sawRelationUpdated);

    sc::SCEditPtr deleteReferencedFloor;
    ASSERT_EQ(db->BeginEdit(L"delete referenced floor", deleteReferencedFloor), sc::SC_OK);
    EXPECT_EQ(floorTable->DeleteRecord(floor2->GetId()), sc::SC_E_CONSTRAINT_VIOLATION);
    EXPECT_EQ(db->Rollback(deleteReferencedFloor.Get()), sc::SC_OK);

    sc::SCEditPtr moveBeam;
    ASSERT_EQ(db->BeginEdit(L"move beam to another floor", moveBeam), sc::SC_OK);
    ASSERT_EQ(beamA->SetRef(L"FloorRef", floor3->GetId()), sc::SC_OK);
    ASSERT_EQ(beamB->SetRef(L"FloorRef", floor3->GetId()), sc::SC_OK);
    ASSERT_EQ(db->Commit(moveBeam.Get()), sc::SC_OK);

    sc::SCEditPtr deleteNowFreeFloor;
    ASSERT_EQ(db->BeginEdit(L"delete unreferenced floor", deleteNowFreeFloor), sc::SC_OK);
    EXPECT_EQ(floorTable->DeleteRecord(floor2->GetId()), sc::SC_OK);
    EXPECT_EQ(db->Commit(deleteNowFreeFloor.Get()), sc::SC_OK);

    EXPECT_EQ(db->RemoveObserver(&observer), sc::SC_OK);
}

}  // namespace

TEST(StorageM3, MemoryRelationBaseline)
{
    RunM3RelationBaseline([]()
    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);
        return db;
    });
}

TEST(StorageM3, SqliteRelationBaseline)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_M3_RelationBaseline.sqlite");

    RunM3RelationBaseline([&dbPath]()
    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), db), sc::SC_OK);
        return db;
    });

    sc::SCDbPtr reopened;
    ASSERT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), reopened), sc::SC_OK);

    sc::SCTablePtr floorTable;
    ASSERT_EQ(reopened->GetTable(L"Floor", floorTable), sc::SC_OK);

    sc::SCTablePtr beamTable;
    ASSERT_EQ(reopened->GetTable(L"Beam", beamTable), sc::SC_OK);

    sc::SCRecordCursorPtr floorCursor;
    ASSERT_EQ(floorTable->EnumerateRecords(floorCursor), sc::SC_OK);

    bool hasRow = false;
    std::vector<sc::RecordId> aliveFloorIds;
    while (floorCursor->MoveNext(&hasRow) == sc::SC_OK && hasRow)
    {
        sc::SCRecordPtr floor;
        ASSERT_EQ(floorCursor->GetCurrent(floor), sc::SC_OK);
        aliveFloorIds.push_back(floor->GetId());
    }

    ASSERT_EQ(aliveFloorIds.size(), 1u);

    const std::vector<std::wstring> remainingBeams = CollectBeamNames(beamTable, aliveFloorIds.front());
    EXPECT_EQ(remainingBeams.size(), 3u);
}
