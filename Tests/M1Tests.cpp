#include <vector>

#include <gtest/gtest.h>

#include "StableCore/Storage/Storage.h"

namespace sc = stablecore::storage;

namespace
{

struct RecordingObserver final : sc::IDatabaseObserver
{
    void OnDatabaseChanged(const sc::ChangeSet& changeSet) override
    {
        seen.push_back(changeSet);
    }

    std::vector<sc::ChangeSet> seen;
};

sc::TablePtr CreateBeamTable(sc::DbPtr& db)
{
    sc::TablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::ColumnDef width;
    width.name = L"Width";
    width.displayName = L"Width";
    width.valueKind = sc::ValueKind::Int64;
    width.defaultValue = sc::Value::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(width), sc::SC_OK);

    sc::ColumnDef name;
    name.name = L"Name";
    name.displayName = L"Name";
    name.valueKind = sc::ValueKind::String;
    name.nullable = true;
    EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);

    return table;
}

sc::TablePtr CreateFloorTable(sc::DbPtr& db)
{
    sc::TablePtr table;
    EXPECT_EQ(db->CreateTable(L"Floor", table), sc::SC_OK);

    sc::SchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::ColumnDef title;
    title.name = L"Title";
    title.displayName = L"Title";
    title.valueKind = sc::ValueKind::String;
    title.defaultValue = sc::Value::FromString(L"");
    EXPECT_EQ(schema->AddColumn(title), sc::SC_OK);

    return table;
}

}  // namespace

TEST(StorageM1, ValueTypedAccess)
{
    sc::Value value = sc::Value::FromRecordId(42);
    sc::RecordId id = 0;
    EXPECT_EQ(value.AsRecordId(&id), sc::SC_OK);
    EXPECT_EQ(id, 42);

    std::wstring text;
    EXPECT_EQ(sc::Value::Null().AsStringCopy(&text), sc::SC_E_VALUE_IS_NULL);
}

TEST(StorageM1, SchemaRejectsInvalidRelationDefault)
{
    sc::DbPtr db;
    EXPECT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

    sc::TablePtr table;
    EXPECT_EQ(db->CreateTable(L"RelationHolder", table), sc::SC_OK);

    sc::SchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::ColumnDef relation;
    relation.name = L"FloorRef";
    relation.columnKind = sc::ColumnKind::Relation;
    relation.valueKind = sc::ValueKind::RecordId;
    relation.defaultValue = sc::Value::FromString(L"bad");

    EXPECT_EQ(schema->AddColumn(relation), sc::SC_E_SCHEMA_VIOLATION);
}

TEST(StorageM1, TransactionCommitAndQuery)
{
    sc::DbPtr db;
    EXPECT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

    sc::TablePtr beamTable = CreateBeamTable(db);

    sc::EditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"create beam", edit), sc::SC_OK);

    sc::RecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 300), sc::SC_OK);
    EXPECT_EQ(beam->SetString(L"Name", L"B1"), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    sc::RecordCursorPtr cursor;
    sc::QueryCondition condition{L"Width", sc::Value::FromInt64(300)};
    EXPECT_EQ(beamTable->FindRecords(condition, cursor), sc::SC_OK);

    bool hasRow = false;
    EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
    EXPECT_TRUE(hasRow);

    sc::RecordPtr current;
    EXPECT_EQ(cursor->GetCurrent(current), sc::SC_OK);

    std::int64_t width = 0;
    EXPECT_EQ(current->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 300);
}

TEST(StorageM1, RollbackRestoresRecordValues)
{
    sc::DbPtr db;
    EXPECT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

    sc::TablePtr beamTable = CreateBeamTable(db);

    sc::EditPtr createEdit;
    EXPECT_EQ(db->BeginEdit(L"seed", createEdit), sc::SC_OK);

    sc::RecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    const sc::RecordId beamId = beam->GetId();
    EXPECT_EQ(beam->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(db->Commit(createEdit.Get()), sc::SC_OK);

    sc::EditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"modify", edit), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 500), sc::SC_OK);
    EXPECT_EQ(db->Rollback(edit.Get()), sc::SC_OK);

    sc::RecordPtr reloaded;
    EXPECT_EQ(beamTable->GetRecord(beamId, reloaded), sc::SC_OK);
    std::int64_t width = 0;
    EXPECT_EQ(reloaded->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 200);
}

TEST(StorageM1, DeleteAndUndoRedo)
{
    sc::DbPtr db;
    EXPECT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

    sc::TablePtr beamTable = CreateBeamTable(db);

    sc::EditPtr createEdit;
    EXPECT_EQ(db->BeginEdit(L"seed", createEdit), sc::SC_OK);
    sc::RecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    const sc::RecordId beamId = beam->GetId();
    EXPECT_EQ(beam->SetInt64(L"Width", 250), sc::SC_OK);
    EXPECT_EQ(db->Commit(createEdit.Get()), sc::SC_OK);

    sc::EditPtr deleteEdit;
    EXPECT_EQ(db->BeginEdit(L"delete", deleteEdit), sc::SC_OK);
    EXPECT_EQ(beamTable->DeleteRecord(beamId), sc::SC_OK);
    EXPECT_EQ(db->Commit(deleteEdit.Get()), sc::SC_OK);
    EXPECT_TRUE(beam->IsDeleted());
    std::int64_t deletedWidth = 0;
    EXPECT_EQ(beam->GetInt64(L"Width", &deletedWidth), sc::SC_E_RECORD_DELETED);

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_FALSE(beam->IsDeleted());

    std::int64_t width = 0;
    EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 250);

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_TRUE(beam->IsDeleted());
}

TEST(StorageM1, RelationFieldValidationAndChangeSet)
{
    sc::DbPtr db;
    EXPECT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

    sc::TablePtr floorTable = CreateFloorTable(db);
    sc::TablePtr beamTable = CreateBeamTable(db);

    sc::SchemaPtr beamSchema;
    EXPECT_EQ(beamTable->GetSchema(beamSchema), sc::SC_OK);

    sc::ColumnDef floorRef;
    floorRef.name = L"FloorRef";
    floorRef.displayName = L"FloorRef";
    floorRef.valueKind = sc::ValueKind::RecordId;
    floorRef.columnKind = sc::ColumnKind::Relation;
    floorRef.referenceTable = L"Floor";
    floorRef.nullable = true;
    EXPECT_EQ(beamSchema->AddColumn(floorRef), sc::SC_OK);

    RecordingObserver observer;
    EXPECT_EQ(db->AddObserver(&observer), sc::SC_OK);

    sc::EditPtr createEdit;
    EXPECT_EQ(db->BeginEdit(L"seed", createEdit), sc::SC_OK);
    sc::RecordPtr floor;
    EXPECT_EQ(floorTable->CreateRecord(floor), sc::SC_OK);
    EXPECT_EQ(floor->SetString(L"Title", L"2F"), sc::SC_OK);

    sc::RecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetRef(L"FloorRef", floor->GetId()), sc::SC_OK);
    EXPECT_EQ(db->Commit(createEdit.Get()), sc::SC_OK);

    EXPECT_FALSE(observer.seen.empty());
    const sc::ChangeSet& changeSet = observer.seen.back();

    bool sawRelationUpdate = false;
    for (const auto& change : changeSet.changes)
    {
        if (change.kind == sc::ChangeKind::RelationUpdated && change.fieldName == L"FloorRef")
        {
            sawRelationUpdate = true;
        }
    }
    EXPECT_TRUE(sawRelationUpdate);

    EXPECT_EQ(db->RemoveObserver(&observer), sc::SC_OK);
}

TEST(StorageM1, GetStringCopyAndDefaultValue)
{
    sc::DbPtr db;
    EXPECT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

    sc::TablePtr beamTable = CreateBeamTable(db);

    sc::EditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);
    sc::RecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    std::int64_t width = 0;
    EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 0);

    std::wstring name;
    EXPECT_EQ(beam->GetStringCopy(L"Name", &name), sc::SC_E_VALUE_IS_NULL);
}
