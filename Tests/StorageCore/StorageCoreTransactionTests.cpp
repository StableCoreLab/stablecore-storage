#include <gtest/gtest.h>

#include "SCStorage.h"

#include "Support/TestFixtures.h"

namespace sc = StableCore::Storage;

TEST_F(CompositeBeamQueryTest, TransactionCommitAndQuery)
{
    sc::SCEditPtr edit;
    EXPECT_EQ(db()->BeginEdit(L"create beam", edit), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable()->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 300), sc::SC_OK);
    EXPECT_EQ(beam->SetString(L"Name", L"B1"), sc::SC_OK);
    EXPECT_EQ(db()->Commit(edit.Get()), sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::SCQueryCondition condition{L"Width", sc::SCValue::FromInt64(300)};
    EXPECT_EQ(beamTable()->FindRecords(condition, cursor), sc::SC_OK);

    sc::SCRecordPtr result;
    EXPECT_EQ(cursor->Next(result), sc::SC_OK);
    EXPECT_TRUE(static_cast<bool>(result));

    std::int64_t width = 0;
    EXPECT_EQ(result->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 300);
}

TEST_F(BeamTableTest, RollbackRestoresRecordValues)
{
    sc::SCEditPtr createEdit;
    EXPECT_EQ(db()->BeginEdit(L"seed", createEdit), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable()->CreateRecord(beam), sc::SC_OK);
    const sc::RecordId beamId = beam->GetId();
    EXPECT_EQ(beam->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(db()->Commit(createEdit.Get()), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db()->BeginEdit(L"modify", edit), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 500), sc::SC_OK);
    EXPECT_EQ(db()->Rollback(edit.Get()), sc::SC_OK);

    sc::SCRecordPtr reloaded;
    EXPECT_EQ(beamTable()->GetRecord(beamId, reloaded), sc::SC_OK);
    std::int64_t width = 0;
    EXPECT_EQ(reloaded->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 200);
}

TEST_F(BeamTableTest, DeleteAndUndoRedo)
{
    sc::SCEditPtr createEdit;
    EXPECT_EQ(db()->BeginEdit(L"seed", createEdit), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable()->CreateRecord(beam), sc::SC_OK);
    const sc::RecordId beamId = beam->GetId();
    EXPECT_EQ(beam->SetInt64(L"Width", 250), sc::SC_OK);
    EXPECT_EQ(db()->Commit(createEdit.Get()), sc::SC_OK);

    sc::SCEditPtr deleteEdit;
    EXPECT_EQ(db()->BeginEdit(L"delete", deleteEdit), sc::SC_OK);
    EXPECT_EQ(beamTable()->DeleteRecord(beamId), sc::SC_OK);
    EXPECT_EQ(db()->Commit(deleteEdit.Get()), sc::SC_OK);
    EXPECT_TRUE(beam->IsDeleted());

    std::int64_t deletedWidth = 0;
    EXPECT_EQ(beam->GetInt64(L"Width", &deletedWidth), sc::SC_E_RECORD_DELETED);

    EXPECT_EQ(db()->Undo(), sc::SC_OK);
    EXPECT_FALSE(beam->IsDeleted());

    std::int64_t width = 0;
    EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 250);

    EXPECT_EQ(db()->Redo(), sc::SC_OK);
    EXPECT_TRUE(beam->IsDeleted());
}

TEST_F(BeamTableTest, CreateAndDeleteWithinEditRollbackRemovesTransientRecord)
{
    sc::SCEditPtr edit;
    EXPECT_EQ(db()->BeginEdit(L"create and delete", edit), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable()->CreateRecord(beam), sc::SC_OK);
    const sc::RecordId beamId = beam->GetId();
    EXPECT_EQ(beam->SetInt64(L"Width", 275), sc::SC_OK);
    EXPECT_EQ(beamTable()->DeleteRecord(beamId), sc::SC_OK);
    EXPECT_EQ(db()->Rollback(edit.Get()), sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(beamTable()->EnumerateRecords(cursor), sc::SC_OK);

    sc::SCRecordPtr result;
    EXPECT_EQ(cursor->Next(result), sc::SC_OK);
    EXPECT_FALSE(static_cast<bool>(result));

    sc::SCRecordPtr reloaded;
    EXPECT_EQ(beamTable()->GetRecord(beamId, reloaded), sc::SC_E_RECORD_NOT_FOUND);
}

TEST_F(BeamTableTest, CreateAndDeleteWithinEditCommitDoesNotLeaveDeletedStub)
{
    sc::SCEditPtr edit;
    EXPECT_EQ(db()->BeginEdit(L"create and delete", edit), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable()->CreateRecord(beam), sc::SC_OK);
    const sc::RecordId beamId = beam->GetId();
    EXPECT_EQ(beam->SetInt64(L"Width", 275), sc::SC_OK);
    EXPECT_EQ(beamTable()->DeleteRecord(beamId), sc::SC_OK);
    EXPECT_EQ(db()->Commit(edit.Get()), sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(beamTable()->EnumerateRecords(cursor), sc::SC_OK);

    sc::SCRecordPtr result;
    EXPECT_EQ(cursor->Next(result), sc::SC_OK);
    EXPECT_FALSE(static_cast<bool>(result));

    sc::SCRecordPtr reloaded;
    EXPECT_EQ(beamTable()->GetRecord(beamId, reloaded), sc::SC_E_RECORD_NOT_FOUND);
}

TEST_F(BeamTableTest, UncommittedEditDoesNotPersist)
{
    sc::SCEditPtr createEdit;
    EXPECT_EQ(db()->BeginEdit(L"seed", createEdit), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable()->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 300), sc::SC_OK);

    EXPECT_EQ(db()->Rollback(createEdit.Get()), sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(beamTable()->EnumerateRecords(cursor), sc::SC_OK);

    sc::SCRecordPtr result;
    EXPECT_EQ(cursor->Next(result), sc::SC_OK);
    EXPECT_FALSE(static_cast<bool>(result));
}

TEST_F(BeamTableTest, MultipleSequentialTransactions)
{
    sc::SCEditPtr edit1;
    EXPECT_EQ(db()->BeginEdit(L"first", edit1), sc::SC_OK);
    sc::SCRecordPtr beam1;
    EXPECT_EQ(beamTable()->CreateRecord(beam1), sc::SC_OK);
    EXPECT_EQ(beam1->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(db()->Commit(edit1.Get()), sc::SC_OK);

    sc::SCEditPtr edit2;
    EXPECT_EQ(db()->BeginEdit(L"second", edit2), sc::SC_OK);
    sc::SCRecordPtr beam2;
    EXPECT_EQ(beamTable()->CreateRecord(beam2), sc::SC_OK);
    EXPECT_EQ(beam2->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(db()->Commit(edit2.Get()), sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(beamTable()->EnumerateRecords(cursor), sc::SC_OK);

    int count = 0;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && static_cast<bool>(record))
    {
        count++;
    }
    EXPECT_EQ(count, 2);
}

TEST_F(BeamTableTest, UndoRedoPreservesRecordIdentity)
{
    sc::SCEditPtr createEdit;
    EXPECT_EQ(db()->BeginEdit(L"seed", createEdit), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable()->CreateRecord(beam), sc::SC_OK);
    const sc::RecordId originalId = beam->GetId();
    EXPECT_EQ(beam->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(db()->Commit(createEdit.Get()), sc::SC_OK);

    sc::SCEditPtr modifyEdit;
    EXPECT_EQ(db()->BeginEdit(L"modify", modifyEdit), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(db()->Commit(modifyEdit.Get()), sc::SC_OK);

    EXPECT_EQ(db()->Undo(), sc::SC_OK);
    EXPECT_EQ(beam->GetId(), originalId);

    EXPECT_EQ(db()->Redo(), sc::SC_OK);
    EXPECT_EQ(beam->GetId(), originalId);
}

TEST_F(BeamTableTest, WriteRequiresActiveEdit)
{
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable()->CreateRecord(beam), sc::SC_E_NO_ACTIVE_EDIT);

    sc::SCEditPtr edit;
    EXPECT_EQ(db()->BeginEdit(L"create", edit), sc::SC_OK);
    EXPECT_EQ(beamTable()->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(db()->Rollback(edit.Get()), sc::SC_OK);

    EXPECT_EQ(beam->SetInt64(L"Width", 100), sc::SC_E_NO_ACTIVE_EDIT);
}

TEST_F(BeamTableTest, EmptyQueryIsNotError)
{
    sc::SCRecordCursorPtr cursor;
    sc::SCQueryCondition condition{L"Width", sc::SCValue::FromInt64(999)};
    EXPECT_EQ(beamTable()->FindRecords(condition, cursor), sc::SC_OK);

    sc::SCRecordPtr result;
    EXPECT_EQ(cursor->Next(result), sc::SC_OK);
    EXPECT_FALSE(static_cast<bool>(result));

    sc::SCTablePtr emptyTable;
    EXPECT_EQ(db()->CreateTable(L"EmptyTable", emptyTable), sc::SC_OK);

    sc::SCRecordCursorPtr emptyCursor;
    EXPECT_EQ(emptyTable->EnumerateRecords(emptyCursor), sc::SC_OK);
    EXPECT_EQ(emptyCursor->Next(result), sc::SC_OK);
    EXPECT_FALSE(static_cast<bool>(result));
}

TEST_F(FileDbTest, CreateRecordRejectsEmptySchemaTable)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db()->CreateTable(L"EmptyTable", table), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db()->BeginEdit(L"create empty record", edit), sc::SC_OK);

    sc::SCRecordPtr record;
    EXPECT_EQ(table->CreateRecord(record), sc::SC_E_SCHEMA_VIOLATION);

    EXPECT_EQ(db()->Rollback(edit.Get()), sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(table->EnumerateRecords(cursor), sc::SC_OK);

    sc::SCRecordPtr result;
    EXPECT_EQ(cursor->Next(result), sc::SC_OK);
    EXPECT_FALSE(static_cast<bool>(result));
}

TEST_F(BeamTableTest, EditLogCommitIdentityAndVersionStayStableAcrossUndoRedo)
{
    sc::SCEditPtr createEdit;
    EXPECT_EQ(db()->BeginEdit(L"seed", createEdit), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable()->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 100), sc::SC_OK);

    EXPECT_EQ(db()->Commit(createEdit.Get()), sc::SC_OK);

    sc::SCEditLogState logState;
    EXPECT_EQ(db()->GetEditLogState(&logState), sc::SC_OK);
    ASSERT_EQ(logState.undoItems.size(), 1u);
    const sc::CommitId commitId = logState.undoItems.front().commitId;
    const sc::VersionId committedVersion = logState.undoItems.front().version;
    EXPECT_NE(commitId, 0u);
    EXPECT_EQ(committedVersion, 1u);

    EXPECT_EQ(db()->Undo(), sc::SC_OK);
    EXPECT_EQ(db()->GetEditLogState(&logState), sc::SC_OK);
    ASSERT_EQ(logState.redoItems.size(), 1u);
    EXPECT_EQ(logState.redoItems.front().commitId, commitId);
    EXPECT_EQ(logState.redoItems.front().version, committedVersion);
    EXPECT_EQ(logState.redoItems.front().kind, sc::SCEditLogActionKind::Commit);

    EXPECT_EQ(db()->Redo(), sc::SC_OK);
    EXPECT_EQ(db()->GetEditLogState(&logState), sc::SC_OK);
    ASSERT_EQ(logState.undoItems.size(), 1u);
    EXPECT_EQ(logState.undoItems.front().commitId, commitId);
    EXPECT_EQ(logState.undoItems.front().version, committedVersion);
    EXPECT_EQ(logState.undoItems.front().kind, sc::SCEditLogActionKind::Commit);
}

TEST_F(FileDbTest, SchemaColumnJournalSupportsAddUpdateRemoveUndoRedo)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db()->CreateTable(L"SchemaJournalTable", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef col1;
    col1.name = L"Col1";
    col1.displayName = L"Col1";
    col1.valueKind = sc::ValueKind::Int64;
    col1.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(col1), sc::SC_OK);

    sc::SCColumnDef found;
    EXPECT_EQ(schema->FindColumn(L"Col1", &found), sc::SC_OK);

    sc::SCEditPtr edit;

    sc::SCColumnDef col2;
    col2.name = L"Col2";
    col2.displayName = L"Col2";
    col2.valueKind = sc::ValueKind::String;
    col2.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(db()->BeginEdit(L"add col2", edit), sc::SC_OK);
    EXPECT_EQ(schema->AddColumn(col2), sc::SC_OK);
    EXPECT_EQ(db()->Commit(edit.Get()), sc::SC_OK);
    EXPECT_EQ(schema->FindColumn(L"Col2", &found), sc::SC_OK);

    EXPECT_EQ(db()->Undo(), sc::SC_OK);
    EXPECT_EQ(schema->FindColumn(L"Col2", &found), sc::SC_E_COLUMN_NOT_FOUND);

    EXPECT_EQ(db()->Redo(), sc::SC_OK);
    EXPECT_EQ(schema->FindColumn(L"Col2", &found), sc::SC_OK);

    sc::SCColumnDef updatedCol1;
    updatedCol1.name = L"Col1";
    updatedCol1.displayName = L"Updated Col1";
    updatedCol1.valueKind = sc::ValueKind::Int64;
    updatedCol1.defaultValue = sc::SCValue::FromInt64(0);
    updatedCol1.indexed = true;
    EXPECT_EQ(db()->BeginEdit(L"update col1", edit), sc::SC_OK);
    EXPECT_EQ(schema->UpdateColumn(updatedCol1), sc::SC_OK);
    EXPECT_EQ(db()->Commit(edit.Get()), sc::SC_OK);
    EXPECT_EQ(schema->FindColumn(L"Col1", &found), sc::SC_OK);
    EXPECT_EQ(found.displayName, L"Updated Col1");
    EXPECT_TRUE(found.indexed);

    EXPECT_EQ(db()->Undo(), sc::SC_OK);
    EXPECT_EQ(schema->FindColumn(L"Col1", &found), sc::SC_OK);
    EXPECT_EQ(found.displayName, L"Col1");
    EXPECT_FALSE(found.indexed);

    EXPECT_EQ(db()->Redo(), sc::SC_OK);
    EXPECT_EQ(schema->FindColumn(L"Col1", &found), sc::SC_OK);
    EXPECT_EQ(found.displayName, L"Updated Col1");
    EXPECT_TRUE(found.indexed);

    EXPECT_EQ(db()->BeginEdit(L"remove col1", edit), sc::SC_OK);
    EXPECT_EQ(schema->RemoveColumn(L"Col1"), sc::SC_OK);
    EXPECT_EQ(db()->Commit(edit.Get()), sc::SC_OK);
    EXPECT_EQ(schema->FindColumn(L"Col1", &found), sc::SC_E_COLUMN_NOT_FOUND);

    EXPECT_EQ(db()->Undo(), sc::SC_OK);
    EXPECT_EQ(schema->FindColumn(L"Col1", &found), sc::SC_OK);
    EXPECT_EQ(found.displayName, L"Updated Col1");

    EXPECT_EQ(db()->Redo(), sc::SC_OK);
    EXPECT_EQ(schema->FindColumn(L"Col1", &found), sc::SC_E_COLUMN_NOT_FOUND);
}
