#include <vector>

#include <gtest/gtest.h>

#include "SCStorage.h"

namespace sc = StableCore::Storage;

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

        sc::SCColumnDef name;
        name.name = L"Name";
        name.displayName = L"Name";
        name.valueKind = sc::ValueKind::String;
        name.nullable = true;
        EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);

        return table;
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

}  // namespace

TEST(StorageM1, ValueTypedAccess)
{
    sc::SCValue SCValue = sc::SCValue::FromRecordId(42);
    sc::RecordId id = 0;
    EXPECT_EQ(SCValue.AsRecordId(&id), sc::SC_OK);
    EXPECT_EQ(id, 42);

    std::wstring text;
    EXPECT_EQ(sc::SCValue::Null().AsStringCopy(&text), sc::SC_E_VALUE_IS_NULL);
}

TEST(StorageM1, SchemaRejectsInvalidRelationDefault)
{
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"RelationHolder", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef relation;
    relation.name = L"FloorRef";
    relation.columnKind = sc::ColumnKind::Relation;
    relation.valueKind = sc::ValueKind::RecordId;
    relation.defaultValue = sc::SCValue::FromString(L"bad");

    EXPECT_EQ(schema->AddColumn(relation), sc::SC_E_SCHEMA_VIOLATION);
}

TEST(StorageM1, SchemaRejectsInvalidReferenceTableUsage)
{
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

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

TEST(StorageM1, TransactionCommitAndQuery)
{
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"create beam", edit), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 300), sc::SC_OK);
    EXPECT_EQ(beam->SetString(L"Name", L"B1"), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::SCQueryCondition condition{L"Width", sc::SCValue::FromInt64(300)};
    EXPECT_EQ(beamTable->FindRecords(condition, cursor), sc::SC_OK);

    bool hasRow = false;
    EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
    EXPECT_TRUE(hasRow);

    sc::SCRecordPtr current;
    EXPECT_EQ(cursor->GetCurrent(current), sc::SC_OK);

    std::int64_t width = 0;
    EXPECT_EQ(current->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 300);
}

TEST(StorageM1, RollbackRestoresRecordValues)
{
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCEditPtr createEdit;
    EXPECT_EQ(db->BeginEdit(L"seed", createEdit), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    const sc::RecordId beamId = beam->GetId();
    EXPECT_EQ(beam->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(db->Commit(createEdit.Get()), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"modify", edit), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 500), sc::SC_OK);
    EXPECT_EQ(db->Rollback(edit.Get()), sc::SC_OK);

    sc::SCRecordPtr reloaded;
    EXPECT_EQ(beamTable->GetRecord(beamId, reloaded), sc::SC_OK);
    std::int64_t width = 0;
    EXPECT_EQ(reloaded->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 200);
}

TEST(StorageM1, DeleteAndUndoRedo)
{
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCEditPtr createEdit;
    EXPECT_EQ(db->BeginEdit(L"seed", createEdit), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    const sc::RecordId beamId = beam->GetId();
    EXPECT_EQ(beam->SetInt64(L"Width", 250), sc::SC_OK);
    EXPECT_EQ(db->Commit(createEdit.Get()), sc::SC_OK);

    sc::SCEditPtr deleteEdit;
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
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

    sc::SCTablePtr floorTable = CreateFloorTable(db);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCSchemaPtr beamSchema;
    EXPECT_EQ(beamTable->GetSchema(beamSchema), sc::SC_OK);

    sc::SCColumnDef floorRef;
    floorRef.name = L"FloorRef";
    floorRef.displayName = L"FloorRef";
    floorRef.valueKind = sc::ValueKind::RecordId;
    floorRef.columnKind = sc::ColumnKind::Relation;
    floorRef.referenceTable = L"Floor";
    floorRef.nullable = true;
    EXPECT_EQ(beamSchema->AddColumn(floorRef), sc::SC_OK);

    RecordingObserver observer;
    EXPECT_EQ(db->AddObserver(&observer), sc::SC_OK);

    sc::SCEditPtr createEdit;
    EXPECT_EQ(db->BeginEdit(L"seed", createEdit), sc::SC_OK);
    sc::SCRecordPtr floor;
    EXPECT_EQ(floorTable->CreateRecord(floor), sc::SC_OK);
    EXPECT_EQ(floor->SetString(L"Title", L"2F"), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetRef(L"FloorRef", floor->GetId()), sc::SC_OK);
    EXPECT_EQ(db->Commit(createEdit.Get()), sc::SC_OK);

    EXPECT_FALSE(observer.seen.empty());
    const sc::SCChangeSet& SCChangeSet = observer.seen.back();

    bool sawRelationUpdate = false;
    for (const auto& change : SCChangeSet.changes)
    {
        if (change.kind == sc::ChangeKind::RelationUpdated &&
            change.fieldName == L"FloorRef")
        {
            sawRelationUpdate = true;
        }
    }
    EXPECT_TRUE(sawRelationUpdate);

    EXPECT_EQ(db->RemoveObserver(&observer), sc::SC_OK);
}

TEST(StorageM1, GetStringCopyAndDefaultValue)
{
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    std::int64_t width = 0;
    EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 0);

    std::wstring name;
    EXPECT_EQ(beam->GetStringCopy(L"Name", &name), sc::SC_E_VALUE_IS_NULL);
}

TEST(StorageM1, WriteRequiresActiveEditAndEmptyQueryIsNotError)
{
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed", seedEdit), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    EXPECT_EQ(beam->SetInt64(L"Width", 200), sc::SC_E_NO_ACTIVE_EDIT);

    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(
        beamTable->FindRecords({L"Width", sc::SCValue::FromInt64(999)}, cursor),
        sc::SC_OK);

    bool hasRow = true;
    EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
    EXPECT_FALSE(hasRow);
    sc::SCRecordPtr current;
    EXPECT_EQ(cursor->GetCurrent(current), sc::SC_FALSE_RESULT);
}

TEST(StorageM1, ReadOnlyOpenModeRejectsMemoryEdits)
{
    sc::SCOpenDatabaseOptions options;
    options.openMode = sc::SCDatabaseOpenMode::ReadOnly;

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateInMemoryDatabase(options, db), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"readonly", edit), sc::SC_E_READ_ONLY_DATABASE);
}

TEST(StorageM1, EditLogCommitIdentityAndVersionStayStableAcrossUndoRedo)
{
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 300), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    sc::SCEditLogState logState;
    EXPECT_EQ(db->GetEditLogState(&logState), sc::SC_OK);
    ASSERT_EQ(logState.undoItems.size(), 1u);
    const sc::CommitId commitId = logState.undoItems.front().commitId;
    const sc::VersionId committedVersion = logState.undoItems.front().version;
    EXPECT_NE(commitId, 0u);
    EXPECT_EQ(committedVersion, 1u);

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_EQ(db->GetEditLogState(&logState), sc::SC_OK);
    ASSERT_EQ(logState.redoItems.size(), 1u);
    EXPECT_EQ(logState.redoItems.front().commitId, commitId);
    EXPECT_EQ(logState.redoItems.front().version, committedVersion);
    EXPECT_EQ(logState.redoItems.front().kind, sc::SCEditLogActionKind::Commit);

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_EQ(db->GetEditLogState(&logState), sc::SC_OK);
    ASSERT_EQ(logState.undoItems.size(), 1u);
    EXPECT_EQ(logState.undoItems.front().commitId, commitId);
    EXPECT_EQ(logState.undoItems.front().version, committedVersion);
    EXPECT_EQ(logState.undoItems.front().kind, sc::SCEditLogActionKind::Commit);
}

TEST(StorageM1, CreateBackupCopyIsNotSupportedOnMemoryDatabase)
{
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

    sc::SCBackupOptions options;
    sc::SCBackupResult result;

    EXPECT_EQ(db->CreateBackupCopy(L"StableCoreStorage_M1_BackupCopy.sqlite",
                                   options, &result),
              sc::SC_E_NOTIMPL);
}
