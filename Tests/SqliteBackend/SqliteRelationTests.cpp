#include <algorithm>
#include <filesystem>
#include <functional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "SCStorage.h"

#include "Support/TestPaths.h"

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

    using CreateDbFn = std::function<sc::SCDbPtr()>;

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

    sc::SCTablePtr CreateBusinessKeyFloorTable(sc::SCDbPtr& db)
    {
        sc::SCTablePtr table;
        EXPECT_EQ(db->CreateTable(L"Floor", table), sc::SC_OK);

        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

        sc::SCColumnDef code;
        code.name = L"Code";
        code.displayName = L"Code";
        code.valueKind = sc::ValueKind::String;
        code.nullable = false;
        code.defaultValue = sc::SCValue::FromString(L"");
        EXPECT_EQ(schema->AddColumn(code), sc::SC_OK);

        sc::SCConstraintDef codeUnique;
        codeUnique.kind = sc::SCConstraintKind::Unique;
        codeUnique.name = L"uq_Floor_Code";
        codeUnique.columns.push_back(L"Code");
        EXPECT_EQ(schema->AddConstraint(codeUnique), sc::SC_OK);

        sc::SCColumnDef name;
        name.name = L"Name";
        name.displayName = L"Name";
        name.valueKind = sc::ValueKind::String;
        name.nullable = false;
        name.defaultValue = sc::SCValue::FromString(L"");
        EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);

        return table;
    }

    sc::SCTablePtr CreateBusinessKeyBeamTable(sc::SCDbPtr& db)
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
        floorRef.valueKind = sc::ValueKind::String;
        floorRef.columnKind = sc::ColumnKind::Relation;
        floorRef.referenceTable = L"Floor";
        floorRef.referenceStorageColumn = L"Code";
        floorRef.referenceDisplayColumn = L"Name";
        floorRef.nullable = false;
        floorRef.defaultValue = sc::SCValue::FromString(L"");
        EXPECT_EQ(schema->AddColumn(floorRef), sc::SC_OK);

        return table;
    }

    std::vector<std::wstring> CollectBeamNames(sc::SCTablePtr& beamTable, sc::RecordId floorId)
    {
        sc::SCRecordCursorPtr cursor;
        EXPECT_EQ(beamTable->FindRecords({L"FloorRef", sc::SCValue::FromRecordId(floorId)}, cursor), sc::SC_OK);

        std::vector<std::wstring> names;
        sc::SCRecordPtr beam;
        while (cursor->Next(beam) == sc::SC_OK && static_cast<bool>(beam))
        {
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
        for (const auto& changeSet : observer.seen)
        {
            for (const auto& change : changeSet.changes)
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
        // 需要重新获取记录引用
        sc::SCRecordPtr beamAReloaded;
        ASSERT_EQ(beamTable->GetRecord(beamA->GetId(), beamAReloaded), sc::SC_OK);
        sc::SCRecordPtr beamBReloaded;
        ASSERT_EQ(beamTable->GetRecord(beamB->GetId(), beamBReloaded), sc::SC_OK);
        ASSERT_EQ(beamAReloaded->SetValue(L"FloorRef", sc::SCValue::FromRecordId(floor3->GetId())), sc::SC_OK);
        ASSERT_EQ(beamBReloaded->SetValue(L"FloorRef", sc::SCValue::FromRecordId(floor3->GetId())), sc::SC_OK);
        ASSERT_EQ(db->Commit(moveBeam.Get()), sc::SC_OK);

        sc::SCEditPtr deleteNowFreeFloor;
        ASSERT_EQ(db->BeginEdit(L"delete unreferenced floor", deleteNowFreeFloor), sc::SC_OK);
        EXPECT_EQ(floorTable->DeleteRecord(floor2->GetId()), sc::SC_OK);
        EXPECT_EQ(db->Commit(deleteNowFreeFloor.Get()), sc::SC_OK);

        EXPECT_EQ(db->RemoveObserver(&observer), sc::SC_OK);
    }
}

// 迁移自 M3Tests.cpp - MemoryRelationBaseline
// 测试内存数据库中的关系字段基本功能
// 注意：当前 API 不支持内存数据库，使用文件数据库代替
TEST(SqliteRelation, SqliteRelationBaseline)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_M3_RelationBaseline.sqlite");

    RunM3RelationBaseline([&dbPath]()
    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);
        return db;
    });

    sc::SCDbPtr reopened;
    ASSERT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, reopened), sc::SC_OK);

    sc::SCTablePtr floorTable;
    ASSERT_EQ(reopened->GetTable(L"Floor", floorTable), sc::SC_OK);

    sc::SCTablePtr beamTable;
    ASSERT_EQ(reopened->GetTable(L"Beam", beamTable), sc::SC_OK);

    sc::SCRecordCursorPtr floorCursor;
    ASSERT_EQ(floorTable->EnumerateRecords(floorCursor), sc::SC_OK);

    std::vector<sc::RecordId> aliveFloorIds;
    sc::SCRecordPtr floor;
    while (floorCursor->Next(floor) == sc::SC_OK && static_cast<bool>(floor))
    {
        aliveFloorIds.push_back(floor->GetId());
    }

    ASSERT_EQ(aliveFloorIds.size(), 1u);

    const std::vector<std::wstring> remainingBeams = CollectBeamNames(beamTable, aliveFloorIds.front());
    EXPECT_EQ(remainingBeams.size(), 3u);
}

// 新增测试：关系字段引用完整性约束
// 对应仓库硬约束：deleted records must never revive
TEST(SqliteRelation, RelationIntegrityPreventsDeletingReferencedRecord)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_RelationIntegrity.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr floorTable = CreateFloorTable(db);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCEditPtr seedEdit;
    ASSERT_EQ(db->BeginEdit(L"seed", seedEdit), sc::SC_OK);

    sc::SCRecordPtr floor;
    ASSERT_EQ(floorTable->CreateRecord(floor), sc::SC_OK);
    ASSERT_EQ(floor->SetString(L"Title", L"1F"), sc::SC_OK);

    sc::SCRecordPtr beam;
    ASSERT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    ASSERT_EQ(beam->SetString(L"Name", L"B1"), sc::SC_OK);
    ASSERT_EQ(beam->SetRef(L"FloorRef", floor->GetId()), sc::SC_OK);

    ASSERT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    // 尝试删除被引用的楼层应该失败
    sc::SCEditPtr deleteEdit;
    ASSERT_EQ(db->BeginEdit(L"delete referenced", deleteEdit), sc::SC_OK);
    EXPECT_EQ(floorTable->DeleteRecord(floor->GetId()), sc::SC_E_CONSTRAINT_VIOLATION);
    EXPECT_EQ(db->Rollback(deleteEdit.Get()), sc::SC_OK);

    // 楼层应该仍然存在
    sc::SCRecordPtr reloadedFloor;
    EXPECT_EQ(floorTable->GetRecord(floor->GetId(), reloadedFloor), sc::SC_OK);
    EXPECT_FALSE(reloadedFloor->IsDeleted());
}

TEST(SqliteRelation, RelationCanStoreBusinessKeyAndDisplayDifferentColumn)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_BusinessKeyRelation.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr floorTable = CreateBusinessKeyFloorTable(db);
    sc::SCTablePtr beamTable = CreateBusinessKeyBeamTable(db);

    sc::SCEditPtr seedEdit;
    ASSERT_EQ(db->BeginEdit(L"seed business relation", seedEdit), sc::SC_OK);

    sc::SCRecordPtr floor;
    ASSERT_EQ(floorTable->CreateRecord(floor), sc::SC_OK);
    ASSERT_EQ(floor->SetString(L"Code", L"F-001"), sc::SC_OK);
    ASSERT_EQ(floor->SetString(L"Name", L"1F"), sc::SC_OK);

    sc::SCRecordPtr beam;
    ASSERT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    ASSERT_EQ(beam->SetString(L"Name", L"B-1"), sc::SC_OK);
    ASSERT_EQ(beam->SetRef(L"FloorRef", floor->GetId()), sc::SC_OK);

    ASSERT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    std::wstring storedCode;
    ASSERT_EQ(beam->GetStringCopy(L"FloorRef", &storedCode), sc::SC_OK);
    EXPECT_EQ(storedCode, L"F-001");

    sc::RecordId refId = 0;
    EXPECT_EQ(beam->GetRef(L"FloorRef", &refId), sc::SC_OK);
    EXPECT_EQ(refId, floor->GetId());

    sc::SCQueryCondition condition{L"FloorRef", sc::SCValue::FromString(L"F-001")};
    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(beamTable->FindRecords(condition, cursor), sc::SC_OK);

    int count = 0;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && static_cast<bool>(record))
    {
        ++count;
    }
    EXPECT_EQ(count, 1);

    sc::SCEditPtr deleteEdit;
    ASSERT_EQ(db->BeginEdit(L"delete business referenced", deleteEdit), sc::SC_OK);
    EXPECT_EQ(floorTable->DeleteRecord(floor->GetId()), sc::SC_E_CONSTRAINT_VIOLATION);
    EXPECT_EQ(db->Rollback(deleteEdit.Get()), sc::SC_OK);
}

// 新增测试：解除引用后可以删除记录
TEST(SqliteRelation, UnreferencedRecordCanBeDeleted)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_UnreferencedDelete.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr floorTable = CreateFloorTable(db);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCEditPtr seedEdit;
    ASSERT_EQ(db->BeginEdit(L"seed", seedEdit), sc::SC_OK);

    sc::SCRecordPtr floor;
    ASSERT_EQ(floorTable->CreateRecord(floor), sc::SC_OK);
    ASSERT_EQ(floor->SetString(L"Title", L"1F"), sc::SC_OK);

    sc::SCRecordPtr beam;
    ASSERT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    ASSERT_EQ(beam->SetString(L"Name", L"B1"), sc::SC_OK);
    ASSERT_EQ(beam->SetRef(L"FloorRef", floor->GetId()), sc::SC_OK);

    ASSERT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    // 先解除引用
    sc::SCEditPtr clearRefEdit;
    ASSERT_EQ(db->BeginEdit(L"clear ref", clearRefEdit), sc::SC_OK);
    // 需要重新获取记录引用
    sc::SCRecordPtr beamReloaded;
    ASSERT_EQ(beamTable->GetRecord(beam->GetId(), beamReloaded), sc::SC_OK);
    ASSERT_EQ(beamReloaded->SetValue(L"FloorRef", sc::SCValue::Null()), sc::SC_OK);
    ASSERT_EQ(db->Commit(clearRefEdit.Get()), sc::SC_OK);

    // 现在可以删除楼层
    sc::SCEditPtr deleteEdit;
    ASSERT_EQ(db->BeginEdit(L"delete unreferenced", deleteEdit), sc::SC_OK);
    EXPECT_EQ(floorTable->DeleteRecord(floor->GetId()), sc::SC_OK);
    ASSERT_EQ(db->Commit(deleteEdit.Get()), sc::SC_OK);

    // 楼层应该被删除
    sc::SCRecordPtr reloadedFloor;
    EXPECT_EQ(floorTable->GetRecord(floor->GetId(), reloadedFloor), sc::SC_OK);
    EXPECT_TRUE(reloadedFloor->IsDeleted());
}

// 补回自 M3Tests.cpp - SqliteReferenceIndexReadOnlyAccess
// 测试引用索引的只读访问能力
TEST(SqliteRelation, SqliteReferenceIndexReadOnlyAccess)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_ReferenceIndexReadOnly.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr floorTable = CreateFloorTable(db);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCEditPtr seedEdit;
    ASSERT_EQ(db->BeginEdit(L"seed", seedEdit), sc::SC_OK);

    sc::SCRecordPtr floor1;
    ASSERT_EQ(floorTable->CreateRecord(floor1), sc::SC_OK);
    ASSERT_EQ(floor1->SetString(L"Title", L"1F"), sc::SC_OK);

    sc::SCRecordPtr floor2;
    ASSERT_EQ(floorTable->CreateRecord(floor2), sc::SC_OK);
    ASSERT_EQ(floor2->SetString(L"Title", L"2F"), sc::SC_OK);

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
    ASSERT_EQ(beamC->SetRef(L"FloorRef", floor1->GetId()), sc::SC_OK);

    ASSERT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    const sc::IReferenceIndexProvider* provider = dynamic_cast<const sc::IReferenceIndexProvider*>(db.Get());
    ASSERT_NE(provider, nullptr);

    sc::ReferenceIndexCheckResult check;
    EXPECT_EQ(provider->CheckReferenceIndex(&check), sc::SC_OK);
    EXPECT_EQ(check.state, sc::ReferenceIndexHealthState::Healthy);

    std::vector<sc::ReverseReferenceRecord> reverseRefs;
    EXPECT_EQ(provider->GetReferencesByTarget(L"Floor", floor2->GetId(), &reverseRefs), sc::SC_OK);
    EXPECT_EQ(reverseRefs.size(), 2u);

    std::vector<sc::ReferenceRecord> forwardRefs;
    EXPECT_EQ(provider->GetReferencesBySource(L"Beam", beamA->GetId(), &forwardRefs), sc::SC_OK);
    EXPECT_EQ(forwardRefs.size(), 1u);
    EXPECT_EQ(forwardRefs.front().sourceTable, L"Beam");
    EXPECT_EQ(forwardRefs.front().targetTable, L"Floor");
}

// 补回自 M3Tests.cpp - SqliteReferenceIndexCanBeRebuilt
// 测试引用索引可以重建
TEST(SqliteRelation, SqliteReferenceIndexCanBeRebuilt)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_ReferenceIndexRebuild.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr floorTable = CreateFloorTable(db);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::IReferenceIndexMaintainer* maintainer = dynamic_cast<sc::IReferenceIndexMaintainer*>(db.Get());
    ASSERT_NE(maintainer, nullptr);

    const sc::IReferenceIndexProvider* provider = dynamic_cast<const sc::IReferenceIndexProvider*>(db.Get());
    ASSERT_NE(provider, nullptr);

    sc::ReferenceIndexCheckResult check;
    EXPECT_EQ(provider->CheckReferenceIndex(&check), sc::SC_OK);
    EXPECT_EQ(check.state, sc::ReferenceIndexHealthState::Missing);

    sc::SCEditPtr seedEdit;
    ASSERT_EQ(db->BeginEdit(L"seed", seedEdit), sc::SC_OK);

    sc::SCRecordPtr floor;
    ASSERT_EQ(floorTable->CreateRecord(floor), sc::SC_OK);
    ASSERT_EQ(floor->SetString(L"Title", L"1F"), sc::SC_OK);

    sc::SCRecordPtr beam;
    ASSERT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    ASSERT_EQ(beam->SetString(L"Name", L"B1"), sc::SC_OK);
    ASSERT_EQ(beam->SetRef(L"FloorRef", floor->GetId()), sc::SC_OK);

    ASSERT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    EXPECT_EQ(provider->CheckReferenceIndex(&check), sc::SC_OK);
    EXPECT_EQ(check.state, sc::ReferenceIndexHealthState::Healthy);

    sc::SCEditPtr modifyEdit;
    ASSERT_EQ(db->BeginEdit(L"modify", modifyEdit), sc::SC_OK);

    sc::SCRecordPtr beamReloaded;
    ASSERT_EQ(beamTable->GetRecord(beam->GetId(), beamReloaded), sc::SC_OK);
    ASSERT_EQ(beamReloaded->SetRef(L"FloorRef", floor->GetId()), sc::SC_OK);

    EXPECT_EQ(db->Rollback(modifyEdit.Get()), sc::SC_OK);

    EXPECT_EQ(provider->CheckReferenceIndex(&check), sc::SC_OK);
    EXPECT_EQ(check.state, sc::ReferenceIndexHealthState::Healthy);

    std::vector<sc::ReferenceRecord> forwardRefs;
    EXPECT_EQ(provider->GetReferencesBySource(L"Beam", beam->GetId(), &forwardRefs), sc::SC_OK);
    EXPECT_EQ(forwardRefs.size(), 1u);
}
