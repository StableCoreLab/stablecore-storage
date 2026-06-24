#include <algorithm>
#include <filesystem>

#include <gtest/gtest.h>
#include <sqlite3.h>

#include "ISCQuery.h"
#include "SCStorage.h"

#include "Support/TestPaths.h"
#include "Support/TestSqliteHelpers.h"
#include "Support/TestSchemaBuilders.h"
#include "Support/TestQueryHelpers.h"

namespace sc = StableCore::Storage;
namespace fs = std::filesystem;

TEST(SchemaEdit, SchemaConstraintAndIndexPrimitivesSupportUndoAndRedo)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_ConstraintIndexUndoRedo.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"add constraint/index", edit), sc::SC_OK);
    EXPECT_EQ(schema->AddConstraint(MakeUniqueConstraint(L"uq_Beam_Width", L"Width")), sc::SC_OK);
    EXPECT_EQ(schema->AddIndex(MakeIndex(L"idx_Beam_Name", L"Name")), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    std::int32_t constraintCount = 0;
    EXPECT_EQ(schema->GetConstraintCount(&constraintCount), sc::SC_OK);
    EXPECT_EQ(constraintCount, 1);

    std::int32_t indexCount = 0;
    EXPECT_EQ(schema->GetIndexCount(&indexCount), sc::SC_OK);
    EXPECT_EQ(indexCount, 1);

    sc::SCConstraintDef constraint;
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_OK);
    EXPECT_EQ(constraint.kind, sc::SCConstraintKind::Unique);
    ASSERT_EQ(constraint.columns.size(), 1u);
    EXPECT_EQ(constraint.columns.front(), L"Width");

    sc::SCIndexDef index;
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_OK);
    ASSERT_EQ(index.columns.size(), 1u);
    EXPECT_EQ(index.columns.front().columnName, L"Name");

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_E_CONSTRAINT_NOT_FOUND);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_E_INDEX_NOT_FOUND);

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_OK);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_OK);
}

TEST(SchemaEdit, SchemaConstraintAndIndexPrimitivesPersistAcrossReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_ConstraintIndexReopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

        sc::SCTablePtr table = CreateBeamTable(db);
        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"add metadata", edit), sc::SC_OK);
        EXPECT_EQ(schema->AddConstraint(MakeUniqueConstraint(L"uq_Beam_Width", L"Width")), sc::SC_OK);
        EXPECT_EQ(schema->AddIndex(MakeIndex(L"idx_Beam_Name", L"Name")), sc::SC_OK);
        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    }

    sc::SCDbPtr reopened;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, reopened), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(reopened->GetTable(L"Beam", table), sc::SC_OK);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCConstraintDef constraint;
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_OK);
    sc::SCIndexDef index;
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_OK);

    EXPECT_EQ(reopened->Undo(), sc::SC_OK);
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_E_CONSTRAINT_NOT_FOUND);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_E_INDEX_NOT_FOUND);

    EXPECT_EQ(reopened->Redo(), sc::SC_OK);
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_OK);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_OK);
}

TEST(SchemaEdit, SchemaConstraintAndIndexPrimitivesSupportRemovalUndoAndRedo)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_ConstraintIndexRemove.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    EXPECT_EQ(schema->AddConstraint(MakeUniqueConstraint(L"uq_Beam_Width", L"Width")), sc::SC_OK);
    EXPECT_EQ(schema->AddIndex(MakeIndex(L"idx_Beam_Name", L"Name")), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"remove constraint/index", edit), sc::SC_OK);
    EXPECT_EQ(schema->RemoveConstraint(L"uq_Beam_Width"), sc::SC_OK);
    EXPECT_EQ(schema->RemoveIndex(L"idx_Beam_Name"), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    sc::SCConstraintDef constraint;
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_E_CONSTRAINT_NOT_FOUND);
    sc::SCIndexDef index;
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_E_INDEX_NOT_FOUND);

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_OK);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_OK);

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_E_CONSTRAINT_NOT_FOUND);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_E_INDEX_NOT_FOUND);
}

TEST(SchemaEdit, UniqueConstraintRejectsDuplicateValueWrites)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_UniqueWriteEnforcement.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table = CreateWidthOnlyBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    EXPECT_EQ(schema->AddConstraint(MakeUniqueConstraint(L"uq_Beam_Width", L"Width")), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed unique row", seedEdit), sc::SC_OK);
    sc::SCRecordPtr first;
    EXPECT_EQ(table->CreateRecord(first), sc::SC_OK);
    EXPECT_EQ(first->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr duplicateEdit;
    EXPECT_EQ(db->BeginEdit(L"duplicate width", duplicateEdit), sc::SC_OK);
    sc::SCRecordPtr second;
    EXPECT_EQ(table->CreateRecord(second), sc::SC_OK);
    EXPECT_EQ(second->SetInt64(L"Width", 100), sc::SC_E_CONSTRAINT_VIOLATION);
    EXPECT_EQ(db->Rollback(duplicateEdit.Get()), sc::SC_OK);
}

TEST(SchemaEdit, UniqueConstraintSupportsUndoAndRedoForValidWrites)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_UniqueUndoRedo.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table = CreateWidthOnlyBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
    EXPECT_EQ(schema->AddConstraint(MakeUniqueConstraint(L"uq_Beam_Width", L"Width")), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed unique row", seedEdit), sc::SC_OK);
    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    const sc::RecordId rowId = row->GetId();
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr updateEdit;
    EXPECT_EQ(db->BeginEdit(L"update unique row", updateEdit), sc::SC_OK);
    sc::SCRecordPtr reloaded;
    EXPECT_EQ(table->GetRecord(rowId, reloaded), sc::SC_OK);
    EXPECT_EQ(reloaded->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(db->Commit(updateEdit.Get()), sc::SC_OK);

    sc::SCRecordPtr committed;
    EXPECT_EQ(table->GetRecord(rowId, committed), sc::SC_OK);
    sc::SCValue widthValue;
    EXPECT_EQ(committed->GetValue(L"Width", &widthValue), sc::SC_OK);
    std::int64_t width = 0;
    EXPECT_EQ(widthValue.AsInt64(&width), sc::SC_OK);
    EXPECT_EQ(width, 200);

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_EQ(table->GetRecord(rowId, committed), sc::SC_OK);
    EXPECT_EQ(committed->GetValue(L"Width", &widthValue), sc::SC_OK);
    EXPECT_EQ(widthValue.AsInt64(&width), sc::SC_OK);
    EXPECT_EQ(width, 100);

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_EQ(table->GetRecord(rowId, committed), sc::SC_OK);
    EXPECT_EQ(committed->GetValue(L"Width", &widthValue), sc::SC_OK);
    EXPECT_EQ(widthValue.AsInt64(&width), sc::SC_OK);
    EXPECT_EQ(width, 200);
}

TEST(SchemaEdit, UniqueConstraintRejectsExistingDuplicateDataWhenAdded)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_UniqueAddEnforcement.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table = CreateWidthOnlyBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed duplicate widths", seedEdit), sc::SC_OK);
    sc::SCRecordPtr first;
    EXPECT_EQ(table->CreateRecord(first), sc::SC_OK);
    EXPECT_EQ(first->SetInt64(L"Width", 100), sc::SC_OK);
    sc::SCRecordPtr second;
    EXPECT_EQ(table->CreateRecord(second), sc::SC_OK);
    EXPECT_EQ(second->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr addEdit;
    EXPECT_EQ(db->BeginEdit(L"add duplicate unique constraint", addEdit), sc::SC_OK);
    EXPECT_EQ(schema->AddConstraint(MakeUniqueConstraint(L"uq_Beam_Width", L"Width")), sc::SC_E_CONSTRAINT_VIOLATION);
    EXPECT_EQ(db->Rollback(addEdit.Get()), sc::SC_OK);
}

TEST(SchemaEdit, CompositeUniqueConstraintRejectsDuplicateValueWrites)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_CompositeUniqueWriteEnforcement.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCConstraintDef compositeUnique;
    compositeUnique.kind = sc::SCConstraintKind::Unique;
    compositeUnique.name = L"uq_Beam_Width_Name";
    compositeUnique.columns.push_back(L"Width");
    compositeUnique.columns.push_back(L"Name");
    EXPECT_EQ(schema->AddConstraint(compositeUnique), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed composite unique row", seedEdit), sc::SC_OK);
    sc::SCRecordPtr first;
    EXPECT_EQ(table->CreateRecord(first), sc::SC_OK);
    EXPECT_EQ(first->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(first->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr duplicateEdit;
    EXPECT_EQ(db->BeginEdit(L"duplicate composite unique", duplicateEdit), sc::SC_OK);
    sc::SCRecordPtr second;
    EXPECT_EQ(table->CreateRecord(second), sc::SC_OK);
    EXPECT_EQ(second->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(second->SetString(L"Name", L"Alpha"), sc::SC_E_CONSTRAINT_VIOLATION);
    EXPECT_EQ(db->Rollback(duplicateEdit.Get()), sc::SC_OK);
}

TEST(SchemaEdit, CompositeUniqueConstraintRejectsExistingDuplicateDataWhenAdded)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_CompositeUniqueAddEnforcement.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed duplicate composite rows", seedEdit), sc::SC_OK);
    sc::SCRecordPtr first;
    EXPECT_EQ(table->CreateRecord(first), sc::SC_OK);
    EXPECT_EQ(first->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(first->SetString(L"Name", L"Alpha"), sc::SC_OK);
    sc::SCRecordPtr second;
    EXPECT_EQ(table->CreateRecord(second), sc::SC_OK);
    EXPECT_EQ(second->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(second->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr addEdit;
    EXPECT_EQ(db->BeginEdit(L"add duplicate composite unique", addEdit), sc::SC_OK);
    sc::SCConstraintDef compositeUnique;
    compositeUnique.kind = sc::SCConstraintKind::Unique;
    compositeUnique.name = L"uq_Beam_Width_Name";
    compositeUnique.columns.push_back(L"Width");
    compositeUnique.columns.push_back(L"Name");
    EXPECT_EQ(schema->AddConstraint(compositeUnique), sc::SC_E_CONSTRAINT_VIOLATION);
    EXPECT_EQ(db->Rollback(addEdit.Get()), sc::SC_OK);
}

TEST(SchemaEdit, CompositeUniqueConstraintRejectsConflictingWritesWithinOneTransaction)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_SchemaEdit_CompositeUniqueTransactionConflict.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCConstraintDef compositeUnique;
    compositeUnique.kind = sc::SCConstraintKind::Unique;
    compositeUnique.name = L"uq_Beam_Width_Name";
    compositeUnique.columns.push_back(L"Width");
    compositeUnique.columns.push_back(L"Name");
    EXPECT_EQ(schema->AddConstraint(compositeUnique), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed composite unique rows", seedEdit), sc::SC_OK);
    sc::SCRecordPtr first;
    EXPECT_EQ(table->CreateRecord(first), sc::SC_OK);
    EXPECT_EQ(first->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(first->SetString(L"Name", L"Alpha"), sc::SC_OK);
    sc::SCRecordPtr second;
    EXPECT_EQ(table->CreateRecord(second), sc::SC_OK);
    EXPECT_EQ(second->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(second->SetString(L"Name", L"Beta"), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr updateEdit;
    EXPECT_EQ(db->BeginEdit(L"conflicting composite unique update", updateEdit), sc::SC_OK);
    sc::SCRecordPtr firstLoaded;
    EXPECT_EQ(table->GetRecord(first->GetId(), firstLoaded), sc::SC_OK);
    EXPECT_EQ(firstLoaded->SetInt64(L"Width", 300), sc::SC_OK);
    EXPECT_EQ(firstLoaded->SetString(L"Name", L"Gamma"), sc::SC_OK);

    sc::SCRecordPtr secondLoaded;
    EXPECT_EQ(table->GetRecord(second->GetId(), secondLoaded), sc::SC_OK);
    EXPECT_EQ(secondLoaded->SetInt64(L"Width", 300), sc::SC_OK);
    EXPECT_EQ(secondLoaded->SetString(L"Name", L"Gamma"), sc::SC_E_CONSTRAINT_VIOLATION);
    EXPECT_EQ(db->Rollback(updateEdit.Get()), sc::SC_OK);
}

TEST(SchemaEdit, CompositeUniqueConstraintSurvivesDeleteUpdateUndoRedoMix)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_CompositeUniqueDeleteUpdateUndoRedo.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCConstraintDef compositeUnique;
    compositeUnique.kind = sc::SCConstraintKind::Unique;
    compositeUnique.name = L"uq_Beam_Width_Name";
    compositeUnique.columns.push_back(L"Width");
    compositeUnique.columns.push_back(L"Name");
    EXPECT_EQ(schema->AddConstraint(compositeUnique), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed composite unique rows", seedEdit), sc::SC_OK);
    sc::SCRecordPtr first;
    EXPECT_EQ(table->CreateRecord(first), sc::SC_OK);
    EXPECT_EQ(first->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(first->SetString(L"Name", L"Alpha"), sc::SC_OK);
    const sc::RecordId firstId = first->GetId();

    sc::SCRecordPtr second;
    EXPECT_EQ(table->CreateRecord(second), sc::SC_OK);
    EXPECT_EQ(second->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(second->SetString(L"Name", L"Beta"), sc::SC_OK);
    const sc::RecordId secondId = second->GetId();
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr mixEdit;
    EXPECT_EQ(db->BeginEdit(L"delete and retarget composite unique", mixEdit), sc::SC_OK);
    EXPECT_EQ(table->DeleteRecord(firstId), sc::SC_OK);
    sc::SCRecordPtr survivingRow;
    EXPECT_EQ(table->GetRecord(secondId, survivingRow), sc::SC_OK);
    EXPECT_EQ(survivingRow->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(survivingRow->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(db->Commit(mixEdit.Get()), sc::SC_OK);

    sc::SCRecordPtr deletedRow;
    EXPECT_EQ(table->GetRecord(firstId, deletedRow), sc::SC_OK);
    EXPECT_TRUE(deletedRow->IsDeleted());
    sc::SCValue widthValue;
    sc::SCValue nameValue;
    EXPECT_EQ(survivingRow->GetValue(L"Width", &widthValue), sc::SC_OK);
    EXPECT_EQ(survivingRow->GetValue(L"Name", &nameValue), sc::SC_OK);
    std::int64_t widthInt = 0;
    std::wstring nameText;
    EXPECT_EQ(widthValue.AsInt64(&widthInt), sc::SC_OK);
    EXPECT_EQ(nameValue.AsStringCopy(&nameText), sc::SC_OK);
    EXPECT_EQ(widthInt, 100);
    EXPECT_EQ(nameText, L"Alpha");

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_FALSE(deletedRow->IsDeleted());
    EXPECT_EQ(deletedRow->GetValue(L"Width", &widthValue), sc::SC_OK);
    EXPECT_EQ(deletedRow->GetValue(L"Name", &nameValue), sc::SC_OK);
    EXPECT_EQ(widthValue.AsInt64(&widthInt), sc::SC_OK);
    EXPECT_EQ(nameValue.AsStringCopy(&nameText), sc::SC_OK);
    EXPECT_EQ(widthInt, 100);
    EXPECT_EQ(nameText, L"Alpha");
    EXPECT_EQ(table->GetRecord(secondId, survivingRow), sc::SC_OK);
    EXPECT_EQ(survivingRow->GetValue(L"Width", &widthValue), sc::SC_OK);
    EXPECT_EQ(survivingRow->GetValue(L"Name", &nameValue), sc::SC_OK);
    EXPECT_EQ(widthValue.AsInt64(&widthInt), sc::SC_OK);
    EXPECT_EQ(nameValue.AsStringCopy(&nameText), sc::SC_OK);
    EXPECT_EQ(widthInt, 200);
    EXPECT_EQ(nameText, L"Beta");

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_TRUE(deletedRow->IsDeleted());
    EXPECT_EQ(table->GetRecord(secondId, survivingRow), sc::SC_OK);
    EXPECT_EQ(survivingRow->GetValue(L"Width", &widthValue), sc::SC_OK);
    EXPECT_EQ(survivingRow->GetValue(L"Name", &nameValue), sc::SC_OK);
    EXPECT_EQ(widthValue.AsInt64(&widthInt), sc::SC_OK);
    EXPECT_EQ(nameValue.AsStringCopy(&nameText), sc::SC_OK);
    EXPECT_EQ(widthInt, 100);
    EXPECT_EQ(nameText, L"Alpha");
}

TEST(SchemaEdit, CompositePrimaryKeyConstraintRejectsDuplicateValueWrites)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_CompositePrimaryKeyWriteEnforcement.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef width;
    width.name = L"Width";
    width.displayName = L"Width";
    width.valueKind = sc::ValueKind::Int64;
    width.nullable = false;
    width.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(width), sc::SC_OK);

    sc::SCColumnDef name;
    name.name = L"Name";
    name.displayName = L"Name";
    name.valueKind = sc::ValueKind::String;
    name.nullable = false;
    name.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);

    EXPECT_EQ(schema->AddConstraint(MakeCompositePrimaryKeyConstraint(L"pk_Beam_Width_Name", L"Width", L"Name")),
              sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed composite primary key row", seedEdit), sc::SC_OK);
    sc::SCRecordPtr first;
    EXPECT_EQ(table->CreateRecord(first), sc::SC_OK);
    EXPECT_EQ(first->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(first->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr duplicateEdit;
    EXPECT_EQ(db->BeginEdit(L"duplicate composite primary key", duplicateEdit), sc::SC_OK);
    sc::SCRecordPtr second;
    EXPECT_EQ(table->CreateRecord(second), sc::SC_OK);
    EXPECT_EQ(second->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(second->SetString(L"Name", L"Alpha"), sc::SC_E_CONSTRAINT_VIOLATION);
    EXPECT_EQ(db->Rollback(duplicateEdit.Get()), sc::SC_OK);
}

TEST(SchemaEdit, CompositePrimaryKeyConstraintSupportsUndoAndRedoAcrossMultipleFields)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_SchemaEdit_CompositePrimaryKeyUndoRedo.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef width;
    width.name = L"Width";
    width.displayName = L"Width";
    width.valueKind = sc::ValueKind::Int64;
    width.nullable = false;
    width.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(width), sc::SC_OK);

    sc::SCColumnDef name;
    name.name = L"Name";
    name.displayName = L"Name";
    name.valueKind = sc::ValueKind::String;
    name.nullable = false;
    name.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);

    EXPECT_EQ(schema->AddConstraint(MakeCompositePrimaryKeyConstraint(L"pk_Beam_Width_Name", L"Width", L"Name")),
              sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed composite primary key row", seedEdit), sc::SC_OK);
    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    const sc::RecordId rowId = row->GetId();
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr updateEdit;
    EXPECT_EQ(db->BeginEdit(L"update composite primary key", updateEdit), sc::SC_OK);
    sc::SCRecordPtr reloaded;
    EXPECT_EQ(table->GetRecord(rowId, reloaded), sc::SC_OK);
    EXPECT_EQ(reloaded->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(reloaded->SetString(L"Name", L"Beta"), sc::SC_OK);
    EXPECT_EQ(db->Commit(updateEdit.Get()), sc::SC_OK);

    sc::SCRecordPtr committed;
    EXPECT_EQ(table->GetRecord(rowId, committed), sc::SC_OK);
    sc::SCValue widthValue;
    sc::SCValue nameValue;
    EXPECT_EQ(committed->GetValue(L"Width", &widthValue), sc::SC_OK);
    EXPECT_EQ(committed->GetValue(L"Name", &nameValue), sc::SC_OK);
    std::int64_t widthInt = 0;
    std::wstring nameText;
    EXPECT_EQ(widthValue.AsInt64(&widthInt), sc::SC_OK);
    EXPECT_EQ(nameValue.AsStringCopy(&nameText), sc::SC_OK);
    EXPECT_EQ(widthInt, 200);
    EXPECT_EQ(nameText, L"Beta");

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_EQ(table->GetRecord(rowId, committed), sc::SC_OK);
    EXPECT_EQ(committed->GetValue(L"Width", &widthValue), sc::SC_OK);
    EXPECT_EQ(committed->GetValue(L"Name", &nameValue), sc::SC_OK);
    EXPECT_EQ(widthValue.AsInt64(&widthInt), sc::SC_OK);
    EXPECT_EQ(nameValue.AsStringCopy(&nameText), sc::SC_OK);
    EXPECT_EQ(widthInt, 100);
    EXPECT_EQ(nameText, L"Alpha");

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_EQ(table->GetRecord(rowId, committed), sc::SC_OK);
    EXPECT_EQ(committed->GetValue(L"Width", &widthValue), sc::SC_OK);
    EXPECT_EQ(committed->GetValue(L"Name", &nameValue), sc::SC_OK);
    EXPECT_EQ(widthValue.AsInt64(&widthInt), sc::SC_OK);
    EXPECT_EQ(nameValue.AsStringCopy(&nameText), sc::SC_OK);
    EXPECT_EQ(widthInt, 200);
    EXPECT_EQ(nameText, L"Beta");
}

TEST(SchemaEdit, CompositePrimaryKeyConstraintSurvivesDeleteUpdateUndoRedoMix)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_SchemaEdit_CompositePrimaryKeyDeleteUpdateUndoRedo.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef width;
    width.name = L"Width";
    width.displayName = L"Width";
    width.valueKind = sc::ValueKind::Int64;
    width.nullable = false;
    width.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(width), sc::SC_OK);

    sc::SCColumnDef name;
    name.name = L"Name";
    name.displayName = L"Name";
    name.valueKind = sc::ValueKind::String;
    name.nullable = false;
    name.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);

    EXPECT_EQ(schema->AddConstraint(MakeCompositePrimaryKeyConstraint(L"pk_Beam_Width_Name", L"Width", L"Name")),
              sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed composite primary key rows", seedEdit), sc::SC_OK);
    sc::SCRecordPtr first;
    EXPECT_EQ(table->CreateRecord(first), sc::SC_OK);
    EXPECT_EQ(first->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(first->SetString(L"Name", L"Alpha"), sc::SC_OK);
    const sc::RecordId firstId = first->GetId();

    sc::SCRecordPtr second;
    EXPECT_EQ(table->CreateRecord(second), sc::SC_OK);
    EXPECT_EQ(second->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(second->SetString(L"Name", L"Beta"), sc::SC_OK);
    const sc::RecordId secondId = second->GetId();
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr mixEdit;
    EXPECT_EQ(db->BeginEdit(L"delete and retarget composite primary key", mixEdit), sc::SC_OK);
    EXPECT_EQ(table->DeleteRecord(firstId), sc::SC_OK);
    sc::SCRecordPtr survivingRow;
    EXPECT_EQ(table->GetRecord(secondId, survivingRow), sc::SC_OK);
    EXPECT_EQ(survivingRow->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(survivingRow->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(db->Commit(mixEdit.Get()), sc::SC_OK);

    sc::SCRecordPtr deletedRow;
    EXPECT_EQ(table->GetRecord(firstId, deletedRow), sc::SC_OK);
    EXPECT_TRUE(deletedRow->IsDeleted());
    sc::SCValue widthValue;
    sc::SCValue nameValue;
    EXPECT_EQ(survivingRow->GetValue(L"Width", &widthValue), sc::SC_OK);
    EXPECT_EQ(survivingRow->GetValue(L"Name", &nameValue), sc::SC_OK);
    std::int64_t widthInt = 0;
    std::wstring nameText;
    EXPECT_EQ(widthValue.AsInt64(&widthInt), sc::SC_OK);
    EXPECT_EQ(nameValue.AsStringCopy(&nameText), sc::SC_OK);
    EXPECT_EQ(widthInt, 100);
    EXPECT_EQ(nameText, L"Alpha");

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_FALSE(deletedRow->IsDeleted());
    EXPECT_EQ(deletedRow->GetValue(L"Width", &widthValue), sc::SC_OK);
    EXPECT_EQ(deletedRow->GetValue(L"Name", &nameValue), sc::SC_OK);
    EXPECT_EQ(widthValue.AsInt64(&widthInt), sc::SC_OK);
    EXPECT_EQ(nameValue.AsStringCopy(&nameText), sc::SC_OK);
    EXPECT_EQ(widthInt, 100);
    EXPECT_EQ(nameText, L"Alpha");
    EXPECT_EQ(table->GetRecord(secondId, survivingRow), sc::SC_OK);
    EXPECT_EQ(survivingRow->GetValue(L"Width", &widthValue), sc::SC_OK);
    EXPECT_EQ(survivingRow->GetValue(L"Name", &nameValue), sc::SC_OK);
    EXPECT_EQ(widthValue.AsInt64(&widthInt), sc::SC_OK);
    EXPECT_EQ(nameValue.AsStringCopy(&nameText), sc::SC_OK);
    EXPECT_EQ(widthInt, 200);
    EXPECT_EQ(nameText, L"Beta");

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_TRUE(deletedRow->IsDeleted());
    EXPECT_EQ(table->GetRecord(secondId, survivingRow), sc::SC_OK);
    EXPECT_EQ(survivingRow->GetValue(L"Width", &widthValue), sc::SC_OK);
    EXPECT_EQ(survivingRow->GetValue(L"Name", &nameValue), sc::SC_OK);
    EXPECT_EQ(widthValue.AsInt64(&widthInt), sc::SC_OK);
    EXPECT_EQ(nameValue.AsStringCopy(&nameText), sc::SC_OK);
    EXPECT_EQ(widthInt, 100);
    EXPECT_EQ(nameText, L"Alpha");
}

TEST(SchemaEdit, CompositePrimaryKeyConstraintRejectsExistingDuplicateDataWhenAdded)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_CompositePrimaryKeyAddEnforcement.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef width;
    width.name = L"Width";
    width.displayName = L"Width";
    width.valueKind = sc::ValueKind::Int64;
    width.nullable = false;
    width.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(width), sc::SC_OK);

    sc::SCColumnDef name;
    name.name = L"Name";
    name.displayName = L"Name";
    name.valueKind = sc::ValueKind::String;
    name.nullable = false;
    name.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed duplicate composite primary key rows", seedEdit), sc::SC_OK);
    sc::SCRecordPtr first;
    EXPECT_EQ(table->CreateRecord(first), sc::SC_OK);
    EXPECT_EQ(first->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(first->SetString(L"Name", L"Alpha"), sc::SC_OK);
    sc::SCRecordPtr second;
    EXPECT_EQ(table->CreateRecord(second), sc::SC_OK);
    EXPECT_EQ(second->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(second->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr addEdit;
    EXPECT_EQ(db->BeginEdit(L"add duplicate composite primary key", addEdit), sc::SC_OK);
    EXPECT_EQ(schema->AddConstraint(MakeCompositePrimaryKeyConstraint(L"pk_Beam_Width_Name", L"Width", L"Name")),
              sc::SC_E_CONSTRAINT_VIOLATION);
    EXPECT_EQ(db->Rollback(addEdit.Get()), sc::SC_OK);
}

TEST(SchemaEdit, PrimaryKeyConstraintRejectsExistingNullDataWhenAdded)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_PrimaryKeyAddEnforcement.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef code;
    code.name = L"Code";
    code.displayName = L"Code";
    code.valueKind = sc::ValueKind::String;
    code.nullable = true;
    EXPECT_EQ(schema->AddColumn(code), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed nullable row", seedEdit), sc::SC_OK);
    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr addEdit;
    EXPECT_EQ(db->BeginEdit(L"add primary key", addEdit), sc::SC_OK);
    EXPECT_EQ(schema->AddConstraint(MakePrimaryKeyConstraint(L"pk_Beam_Code", L"Code")), sc::SC_E_CONSTRAINT_VIOLATION);
    EXPECT_EQ(db->Rollback(addEdit.Get()), sc::SC_OK);
}

TEST(SchemaEdit, PrimaryKeyConstraintRejectsDuplicateValueWrites)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_PrimaryKeyWriteEnforcement.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table = CreateWidthOnlyBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    EXPECT_EQ(schema->AddConstraint(MakePrimaryKeyConstraint(L"pk_Beam_Width", L"Width")), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed primary key row", seedEdit), sc::SC_OK);
    sc::SCRecordPtr first;
    EXPECT_EQ(table->CreateRecord(first), sc::SC_OK);
    EXPECT_EQ(first->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr duplicateEdit;
    EXPECT_EQ(db->BeginEdit(L"duplicate primary key value", duplicateEdit), sc::SC_OK);
    sc::SCRecordPtr second;
    EXPECT_EQ(table->CreateRecord(second), sc::SC_OK);
    EXPECT_EQ(second->SetInt64(L"Width", 100), sc::SC_E_CONSTRAINT_VIOLATION);
    EXPECT_EQ(db->Rollback(duplicateEdit.Get()), sc::SC_OK);
}

TEST(SchemaEdit, PrimaryKeyConstraintRejectsNullValuesOnCreateRecord)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_PrimaryKeyNullCreateRecord.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef code;
    code.name = L"Code";
    code.displayName = L"Code";
    code.valueKind = sc::ValueKind::String;
    code.nullable = true;
    EXPECT_EQ(schema->AddColumn(code), sc::SC_OK);
    EXPECT_EQ(schema->AddConstraint(MakePrimaryKeyConstraint(L"pk_Beam_Code", L"Code")), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"commit missing primary key", edit), sc::SC_OK);
    sc::SCRecordPtr record;
    EXPECT_EQ(table->CreateRecord(record), sc::SC_E_CONSTRAINT_VIOLATION);
    EXPECT_EQ(db->Rollback(edit.Get()), sc::SC_OK);
}

TEST(SchemaEdit, CheckConstraintRejectsInvalidWrites)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_CheckWriteEnforcement.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef width;
    width.name = L"Width";
    width.displayName = L"Width";
    width.valueKind = sc::ValueKind::Int64;
    width.nullable = false;
    width.defaultValue = sc::SCValue::FromInt64(1);
    EXPECT_EQ(schema->AddColumn(width), sc::SC_OK);
    EXPECT_EQ(schema->AddConstraint(MakeCheckConstraint(L"ck_Beam_Width_Positive", L"Width", L"Width > 0")),
              sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed valid width", seedEdit), sc::SC_OK);
    sc::SCRecordPtr record;
    EXPECT_EQ(table->CreateRecord(record), sc::SC_OK);
    EXPECT_EQ(record->SetInt64(L"Width", 10), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr invalidEdit;
    EXPECT_EQ(db->BeginEdit(L"invalid width", invalidEdit), sc::SC_OK);
    sc::SCRecordPtr reloaded;
    EXPECT_EQ(table->GetRecord(record->GetId(), reloaded), sc::SC_OK);
    EXPECT_EQ(reloaded->SetInt64(L"Width", -1), sc::SC_E_CONSTRAINT_VIOLATION);
    EXPECT_EQ(db->Rollback(invalidEdit.Get()), sc::SC_OK);
}

TEST(SchemaEdit, CheckConstraintRejectsExistingInvalidDataWhenAdded)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_CheckAddEnforcement.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef width;
    width.name = L"Width";
    width.displayName = L"Width";
    width.valueKind = sc::ValueKind::Int64;
    width.nullable = false;
    width.defaultValue = sc::SCValue::FromInt64(1);
    EXPECT_EQ(schema->AddColumn(width), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed invalid width", seedEdit), sc::SC_OK);
    sc::SCRecordPtr record;
    EXPECT_EQ(table->CreateRecord(record), sc::SC_OK);
    EXPECT_EQ(record->SetInt64(L"Width", -1), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr addEdit;
    EXPECT_EQ(db->BeginEdit(L"add check", addEdit), sc::SC_OK);
    EXPECT_EQ(schema->AddConstraint(MakeCheckConstraint(L"ck_Beam_Width_Positive", L"Width", L"Width > 0")),
              sc::SC_E_CONSTRAINT_VIOLATION);
    EXPECT_EQ(db->Rollback(addEdit.Get()), sc::SC_OK);
}

TEST(SchemaEdit, CheckConstraintSupportsUndoAndRedoForValidWrites)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_CheckUndoRedo.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef width;
    width.name = L"Width";
    width.displayName = L"Width";
    width.valueKind = sc::ValueKind::Int64;
    width.nullable = false;
    width.defaultValue = sc::SCValue::FromInt64(1);
    EXPECT_EQ(schema->AddColumn(width), sc::SC_OK);
    EXPECT_EQ(schema->AddConstraint(MakeCheckConstraint(L"ck_Beam_Width_Positive", L"Width", L"Width > 0")),
              sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed width", seedEdit), sc::SC_OK);
    sc::SCRecordPtr record;
    EXPECT_EQ(table->CreateRecord(record), sc::SC_OK);
    EXPECT_EQ(record->SetInt64(L"Width", 10), sc::SC_OK);
    const sc::RecordId rowId = record->GetId();
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr updateEdit;
    EXPECT_EQ(db->BeginEdit(L"update width", updateEdit), sc::SC_OK);
    sc::SCRecordPtr reloaded;
    EXPECT_EQ(table->GetRecord(rowId, reloaded), sc::SC_OK);
    EXPECT_EQ(reloaded->SetInt64(L"Width", 20), sc::SC_OK);
    EXPECT_EQ(db->Commit(updateEdit.Get()), sc::SC_OK);

    sc::SCValue widthValue;
    EXPECT_EQ(reloaded->GetValue(L"Width", &widthValue), sc::SC_OK);
    std::int64_t widthValueInt = 0;
    EXPECT_EQ(widthValue.AsInt64(&widthValueInt), sc::SC_OK);
    EXPECT_EQ(widthValueInt, 20);

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_EQ(table->GetRecord(rowId, reloaded), sc::SC_OK);
    EXPECT_EQ(reloaded->GetValue(L"Width", &widthValue), sc::SC_OK);
    EXPECT_EQ(widthValue.AsInt64(&widthValueInt), sc::SC_OK);
    EXPECT_EQ(widthValueInt, 10);

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_EQ(table->GetRecord(rowId, reloaded), sc::SC_OK);
    EXPECT_EQ(reloaded->GetValue(L"Width", &widthValue), sc::SC_OK);
    EXPECT_EQ(widthValue.AsInt64(&widthValueInt), sc::SC_OK);
    EXPECT_EQ(widthValueInt, 20);
}

TEST(SchemaEdit, ForeignKeyConstraintRejectsInvalidWritesAndDeletes)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_ForeignKeyWriteDeleteEnforcement.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr floorTable;
    EXPECT_EQ(db->CreateTable(L"Floor", floorTable), sc::SC_OK);
    sc::SCSchemaPtr floorSchema;
    EXPECT_EQ(floorTable->GetSchema(floorSchema), sc::SC_OK);

    sc::SCColumnDef floorCode;
    floorCode.name = L"Code";
    floorCode.displayName = L"Code";
    floorCode.valueKind = sc::ValueKind::String;
    floorCode.nullable = false;
    floorCode.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(floorSchema->AddColumn(floorCode), sc::SC_OK);
    EXPECT_EQ(floorSchema->AddConstraint(MakeUniqueConstraint(L"uq_Floor_Code", L"Code")), sc::SC_OK);

    sc::SCTablePtr beamTable;
    EXPECT_EQ(db->CreateTable(L"Beam", beamTable), sc::SC_OK);
    sc::SCSchemaPtr beamSchema;
    EXPECT_EQ(beamTable->GetSchema(beamSchema), sc::SC_OK);

    sc::SCColumnDef beamCode;
    beamCode.name = L"FloorCode";
    beamCode.displayName = L"FloorCode";
    beamCode.valueKind = sc::ValueKind::String;
    beamCode.nullable = true;
    beamCode.defaultValue = sc::SCValue::Null();
    EXPECT_EQ(beamSchema->AddColumn(beamCode), sc::SC_OK);

    EXPECT_EQ(beamSchema->AddConstraint(MakeForeignKeyConstraint(L"fk_Beam_FloorCode",
                                                                 L"FloorCode",
                                                                 L"Floor",
                                                                 L"Code")),
              sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed foreign key rows", seedEdit), sc::SC_OK);
    sc::SCRecordPtr floor;
    EXPECT_EQ(floorTable->CreateRecord(floor), sc::SC_OK);
    EXPECT_EQ(floor->SetString(L"Code", L"F-001"), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetString(L"FloorCode", L"F-001"), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr invalidWriteEdit;
    EXPECT_EQ(db->BeginEdit(L"invalid foreign key write", invalidWriteEdit), sc::SC_OK);
    sc::SCRecordPtr beamReloaded;
    EXPECT_EQ(beamTable->GetRecord(beam->GetId(), beamReloaded), sc::SC_OK);
    EXPECT_EQ(beamReloaded->SetString(L"FloorCode", L"F-404"), sc::SC_E_CONSTRAINT_VIOLATION);
    EXPECT_EQ(db->Rollback(invalidWriteEdit.Get()), sc::SC_OK);

    sc::SCEditPtr deleteEdit;
    EXPECT_EQ(db->BeginEdit(L"delete referenced floor", deleteEdit), sc::SC_OK);
    EXPECT_EQ(floorTable->DeleteRecord(floor->GetId()), sc::SC_E_CONSTRAINT_VIOLATION);
    EXPECT_EQ(db->Rollback(deleteEdit.Get()), sc::SC_OK);

    sc::SCEditPtr updateEdit;
    EXPECT_EQ(db->BeginEdit(L"update referenced floor", updateEdit), sc::SC_OK);
    sc::SCRecordPtr floorReloaded;
    EXPECT_EQ(floorTable->GetRecord(floor->GetId(), floorReloaded), sc::SC_OK);
    EXPECT_EQ(floorReloaded->SetString(L"Code", L"F-002"), sc::SC_E_CONSTRAINT_VIOLATION);
    EXPECT_EQ(db->Rollback(updateEdit.Get()), sc::SC_OK);
}

TEST(SchemaEdit, ForeignKeyConstraintRejectsExistingInvalidDataWhenAdded)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_ForeignKeyAddEnforcement.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr floorTable;
    EXPECT_EQ(db->CreateTable(L"Floor", floorTable), sc::SC_OK);
    sc::SCSchemaPtr floorSchema;
    EXPECT_EQ(floorTable->GetSchema(floorSchema), sc::SC_OK);

    sc::SCColumnDef floorCode;
    floorCode.name = L"Code";
    floorCode.displayName = L"Code";
    floorCode.valueKind = sc::ValueKind::String;
    floorCode.nullable = false;
    floorCode.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(floorSchema->AddColumn(floorCode), sc::SC_OK);
    EXPECT_EQ(floorSchema->AddConstraint(MakeUniqueConstraint(L"uq_Floor_Code", L"Code")), sc::SC_OK);

    sc::SCTablePtr beamTable;
    EXPECT_EQ(db->CreateTable(L"Beam", beamTable), sc::SC_OK);
    sc::SCSchemaPtr beamSchema;
    EXPECT_EQ(beamTable->GetSchema(beamSchema), sc::SC_OK);

    sc::SCColumnDef beamCode;
    beamCode.name = L"FloorCode";
    beamCode.displayName = L"FloorCode";
    beamCode.valueKind = sc::ValueKind::String;
    beamCode.nullable = true;
    beamCode.defaultValue = sc::SCValue::Null();
    EXPECT_EQ(beamSchema->AddColumn(beamCode), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed invalid foreign key row", seedEdit), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetString(L"FloorCode", L"Missing"), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr addEdit;
    EXPECT_EQ(db->BeginEdit(L"add foreign key", addEdit), sc::SC_OK);
    EXPECT_EQ(beamSchema->AddConstraint(MakeForeignKeyConstraint(L"fk_Beam_FloorCode",
                                                                 L"FloorCode",
                                                                 L"Floor",
                                                                 L"Code")),
              sc::SC_E_CONSTRAINT_VIOLATION);
    EXPECT_EQ(db->Rollback(addEdit.Get()), sc::SC_OK);
}

TEST(SchemaEdit, ForeignKeyConstraintSupportsUndoAndRedoForValidWrites)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_ForeignKeyUndoRedo.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr floorTable;
    EXPECT_EQ(db->CreateTable(L"Floor", floorTable), sc::SC_OK);
    sc::SCSchemaPtr floorSchema;
    EXPECT_EQ(floorTable->GetSchema(floorSchema), sc::SC_OK);

    sc::SCColumnDef floorCode;
    floorCode.name = L"Code";
    floorCode.displayName = L"Code";
    floorCode.valueKind = sc::ValueKind::String;
    floorCode.nullable = false;
    floorCode.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(floorSchema->AddColumn(floorCode), sc::SC_OK);
    EXPECT_EQ(floorSchema->AddConstraint(MakeUniqueConstraint(L"uq_Floor_Code", L"Code")), sc::SC_OK);

    sc::SCTablePtr beamTable;
    EXPECT_EQ(db->CreateTable(L"Beam", beamTable), sc::SC_OK);
    sc::SCSchemaPtr beamSchema;
    EXPECT_EQ(beamTable->GetSchema(beamSchema), sc::SC_OK);

    sc::SCColumnDef beamCode;
    beamCode.name = L"FloorCode";
    beamCode.displayName = L"FloorCode";
    beamCode.valueKind = sc::ValueKind::String;
    beamCode.nullable = true;
    beamCode.defaultValue = sc::SCValue::Null();
    EXPECT_EQ(beamSchema->AddColumn(beamCode), sc::SC_OK);
    EXPECT_EQ(beamSchema->AddConstraint(MakeForeignKeyConstraint(L"fk_Beam_FloorCode",
                                                                 L"FloorCode",
                                                                 L"Floor",
                                                                 L"Code")),
              sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed foreign key rows", seedEdit), sc::SC_OK);
    sc::SCRecordPtr floor1;
    EXPECT_EQ(floorTable->CreateRecord(floor1), sc::SC_OK);
    EXPECT_EQ(floor1->SetString(L"Code", L"F-001"), sc::SC_OK);
    sc::SCRecordPtr floor2;
    EXPECT_EQ(floorTable->CreateRecord(floor2), sc::SC_OK);
    EXPECT_EQ(floor2->SetString(L"Code", L"F-002"), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetString(L"FloorCode", L"F-001"), sc::SC_OK);
    const sc::RecordId beamId = beam->GetId();
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr updateEdit;
    EXPECT_EQ(db->BeginEdit(L"update foreign key value", updateEdit), sc::SC_OK);
    sc::SCRecordPtr beamReloaded;
    EXPECT_EQ(beamTable->GetRecord(beamId, beamReloaded), sc::SC_OK);
    EXPECT_EQ(beamReloaded->SetString(L"FloorCode", L"F-002"), sc::SC_OK);
    EXPECT_EQ(db->Commit(updateEdit.Get()), sc::SC_OK);

    sc::SCValue fkValue;
    EXPECT_EQ(beamReloaded->GetValue(L"FloorCode", &fkValue), sc::SC_OK);
    std::wstring fkText;
    EXPECT_EQ(fkValue.AsStringCopy(&fkText), sc::SC_OK);
    EXPECT_EQ(fkText, L"F-002");

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_EQ(beamTable->GetRecord(beamId, beamReloaded), sc::SC_OK);
    EXPECT_EQ(beamReloaded->GetValue(L"FloorCode", &fkValue), sc::SC_OK);
    EXPECT_EQ(fkValue.AsStringCopy(&fkText), sc::SC_OK);
    EXPECT_EQ(fkText, L"F-001");

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_EQ(beamTable->GetRecord(beamId, beamReloaded), sc::SC_OK);
    EXPECT_EQ(beamReloaded->GetValue(L"FloorCode", &fkValue), sc::SC_OK);
    EXPECT_EQ(fkValue.AsStringCopy(&fkText), sc::SC_OK);
    EXPECT_EQ(fkText, L"F-002");
}

TEST(SchemaEdit, ForeignKeyConstraintSupportsCascadeDelete)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_ForeignKeyCascadeDelete.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr floorTable;
    EXPECT_EQ(db->CreateTable(L"Floor", floorTable), sc::SC_OK);
    sc::SCSchemaPtr floorSchema;
    EXPECT_EQ(floorTable->GetSchema(floorSchema), sc::SC_OK);

    sc::SCColumnDef floorCode;
    floorCode.name = L"Code";
    floorCode.displayName = L"Code";
    floorCode.valueKind = sc::ValueKind::String;
    floorCode.nullable = false;
    floorCode.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(floorSchema->AddColumn(floorCode), sc::SC_OK);
    EXPECT_EQ(floorSchema->AddConstraint(MakeUniqueConstraint(L"uq_Floor_Code", L"Code")), sc::SC_OK);

    sc::SCTablePtr beamTable;
    EXPECT_EQ(db->CreateTable(L"Beam", beamTable), sc::SC_OK);
    sc::SCSchemaPtr beamSchema;
    EXPECT_EQ(beamTable->GetSchema(beamSchema), sc::SC_OK);

    sc::SCColumnDef beamCode;
    beamCode.name = L"FloorCode";
    beamCode.displayName = L"FloorCode";
    beamCode.valueKind = sc::ValueKind::String;
    beamCode.nullable = true;
    beamCode.defaultValue = sc::SCValue::Null();
    EXPECT_EQ(beamSchema->AddColumn(beamCode), sc::SC_OK);
    EXPECT_EQ(beamSchema->AddConstraint(MakeForeignKeyConstraint(L"fk_Beam_FloorCode",
                                                                 L"FloorCode",
                                                                 L"Floor",
                                                                 L"Code",
                                                                 sc::SCForeignKeyAction::Cascade,
                                                                 sc::SCForeignKeyAction::Restrict)),
              sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed cascade delete rows", seedEdit), sc::SC_OK);
    sc::SCRecordPtr floor;
    EXPECT_EQ(floorTable->CreateRecord(floor), sc::SC_OK);
    EXPECT_EQ(floor->SetString(L"Code", L"F-001"), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetString(L"FloorCode", L"F-001"), sc::SC_OK);
    const sc::RecordId beamId = beam->GetId();
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr deleteEdit;
    EXPECT_EQ(db->BeginEdit(L"delete cascade parent", deleteEdit), sc::SC_OK);
    EXPECT_EQ(floorTable->DeleteRecord(floor->GetId()), sc::SC_OK);
    EXPECT_EQ(db->Commit(deleteEdit.Get()), sc::SC_OK);

    sc::SCRecordPtr beamReloaded;
    EXPECT_EQ(beamTable->GetRecord(beamId, beamReloaded), sc::SC_OK);
    EXPECT_TRUE(beamReloaded->IsDeleted());
}

TEST(SchemaEdit, ForeignKeyConstraintSupportsSetNullOnUpdate)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_ForeignKeySetNullUpdate.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr floorTable;
    EXPECT_EQ(db->CreateTable(L"Floor", floorTable), sc::SC_OK);
    sc::SCSchemaPtr floorSchema;
    EXPECT_EQ(floorTable->GetSchema(floorSchema), sc::SC_OK);

    sc::SCColumnDef floorCode;
    floorCode.name = L"Code";
    floorCode.displayName = L"Code";
    floorCode.valueKind = sc::ValueKind::String;
    floorCode.nullable = false;
    floorCode.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(floorSchema->AddColumn(floorCode), sc::SC_OK);
    EXPECT_EQ(floorSchema->AddConstraint(MakeUniqueConstraint(L"uq_Floor_Code", L"Code")), sc::SC_OK);

    sc::SCTablePtr beamTable;
    EXPECT_EQ(db->CreateTable(L"Beam", beamTable), sc::SC_OK);
    sc::SCSchemaPtr beamSchema;
    EXPECT_EQ(beamTable->GetSchema(beamSchema), sc::SC_OK);

    sc::SCColumnDef beamCode;
    beamCode.name = L"FloorCode";
    beamCode.displayName = L"FloorCode";
    beamCode.valueKind = sc::ValueKind::String;
    beamCode.nullable = true;
    beamCode.defaultValue = sc::SCValue::Null();
    EXPECT_EQ(beamSchema->AddColumn(beamCode), sc::SC_OK);
    EXPECT_EQ(beamSchema->AddConstraint(MakeForeignKeyConstraint(L"fk_Beam_FloorCode",
                                                                 L"FloorCode",
                                                                 L"Floor",
                                                                 L"Code",
                                                                 sc::SCForeignKeyAction::Restrict,
                                                                 sc::SCForeignKeyAction::SetNull)),
              sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed set null rows", seedEdit), sc::SC_OK);
    sc::SCRecordPtr floor;
    EXPECT_EQ(floorTable->CreateRecord(floor), sc::SC_OK);
    EXPECT_EQ(floor->SetString(L"Code", L"F-001"), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetString(L"FloorCode", L"F-001"), sc::SC_OK);
    const sc::RecordId beamId = beam->GetId();
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr updateEdit;
    EXPECT_EQ(db->BeginEdit(L"update parent key", updateEdit), sc::SC_OK);
    sc::SCRecordPtr floorReloaded;
    EXPECT_EQ(floorTable->GetRecord(floor->GetId(), floorReloaded), sc::SC_OK);
    EXPECT_EQ(floorReloaded->SetString(L"Code", L"F-002"), sc::SC_OK);
    EXPECT_EQ(db->Commit(updateEdit.Get()), sc::SC_OK);

    sc::SCRecordPtr beamReloaded;
    EXPECT_EQ(beamTable->GetRecord(beamId, beamReloaded), sc::SC_OK);
    sc::SCValue fkValue;
    EXPECT_EQ(beamReloaded->GetValue(L"FloorCode", &fkValue), sc::SC_E_VALUE_IS_NULL);
    EXPECT_TRUE(fkValue.IsNull());
}

TEST(SchemaEdit, ConstraintViolationInfoReportsOffendingConstraint)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_ConstraintViolationInfo.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table = CreateWidthOnlyBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
    EXPECT_EQ(schema->AddConstraint(MakeUniqueConstraint(L"uq_Beam_Width", L"Width")), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed unique violation info", seedEdit), sc::SC_OK);
    sc::SCRecordPtr first;
    EXPECT_EQ(table->CreateRecord(first), sc::SC_OK);
    EXPECT_EQ(first->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr duplicateEdit;
    EXPECT_EQ(db->BeginEdit(L"duplicate unique violation info", duplicateEdit), sc::SC_OK);
    sc::SCRecordPtr second;
    EXPECT_EQ(table->CreateRecord(second), sc::SC_OK);
    EXPECT_EQ(second->SetInt64(L"Width", 100), sc::SC_E_CONSTRAINT_VIOLATION);

    sc::SCConstraintViolationInfo info;
    EXPECT_EQ(db->GetLastConstraintViolationInfo(&info), sc::SC_OK);
    EXPECT_EQ(info.tableName, L"Beam");
    EXPECT_EQ(info.constraintName, L"uq_Beam_Width");
    EXPECT_EQ(info.kind, sc::SCConstraintKind::Unique);
    ASSERT_EQ(info.columns.size(), 1u);
    EXPECT_EQ(info.columns.front(), L"Width");
    EXPECT_EQ(info.recordId, second->GetId());
    EXPECT_EQ(db->Rollback(duplicateEdit.Get()), sc::SC_OK);
}

TEST(SchemaEdit, ForeignKeyConstraintSupportsCascadeCycleDelete)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_ForeignKeyCascadeCycleDelete.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr alphaTable;
    EXPECT_EQ(db->CreateTable(L"Alpha", alphaTable), sc::SC_OK);
    sc::SCSchemaPtr alphaSchema;
    EXPECT_EQ(alphaTable->GetSchema(alphaSchema), sc::SC_OK);

    sc::SCColumnDef alphaCode;
    alphaCode.name = L"Code";
    alphaCode.displayName = L"Code";
    alphaCode.valueKind = sc::ValueKind::String;
    alphaCode.nullable = false;
    alphaCode.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(alphaSchema->AddColumn(alphaCode), sc::SC_OK);
    EXPECT_EQ(alphaSchema->AddConstraint(MakeUniqueConstraint(L"uq_Alpha_Code", L"Code")), sc::SC_OK);

    sc::SCColumnDef alphaRef;
    alphaRef.name = L"BetaCode";
    alphaRef.displayName = L"BetaCode";
    alphaRef.valueKind = sc::ValueKind::String;
    alphaRef.nullable = true;
    alphaRef.defaultValue = sc::SCValue::Null();
    EXPECT_EQ(alphaSchema->AddColumn(alphaRef), sc::SC_OK);

    sc::SCTablePtr betaTable;
    EXPECT_EQ(db->CreateTable(L"Beta", betaTable), sc::SC_OK);
    sc::SCSchemaPtr betaSchema;
    EXPECT_EQ(betaTable->GetSchema(betaSchema), sc::SC_OK);

    sc::SCColumnDef betaCode;
    betaCode.name = L"Code";
    betaCode.displayName = L"Code";
    betaCode.valueKind = sc::ValueKind::String;
    betaCode.nullable = false;
    betaCode.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(betaSchema->AddColumn(betaCode), sc::SC_OK);
    EXPECT_EQ(betaSchema->AddConstraint(MakeUniqueConstraint(L"uq_Beta_Code", L"Code")), sc::SC_OK);

    sc::SCColumnDef betaRef;
    betaRef.name = L"AlphaCode";
    betaRef.displayName = L"AlphaCode";
    betaRef.valueKind = sc::ValueKind::String;
    betaRef.nullable = true;
    betaRef.defaultValue = sc::SCValue::Null();
    EXPECT_EQ(betaSchema->AddColumn(betaRef), sc::SC_OK);

    EXPECT_EQ(alphaSchema->AddConstraint(MakeForeignKeyConstraint(L"fk_Alpha_BetaCode",
                                                                  L"BetaCode",
                                                                  L"Beta",
                                                                  L"Code",
                                                                  sc::SCForeignKeyAction::Cascade,
                                                                  sc::SCForeignKeyAction::Restrict)),
              sc::SC_OK);
    EXPECT_EQ(betaSchema->AddConstraint(MakeForeignKeyConstraint(L"fk_Beta_AlphaCode",
                                                                 L"AlphaCode",
                                                                 L"Alpha",
                                                                 L"Code",
                                                                 sc::SCForeignKeyAction::Cascade,
                                                                 sc::SCForeignKeyAction::Restrict)),
              sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed cycle rows", seedEdit), sc::SC_OK);

    sc::SCRecordPtr alpha;
    EXPECT_EQ(alphaTable->CreateRecord(alpha), sc::SC_OK);
    EXPECT_EQ(alpha->SetString(L"Code", L"A-001"), sc::SC_OK);

    sc::SCRecordPtr beta;
    EXPECT_EQ(betaTable->CreateRecord(beta), sc::SC_OK);
    EXPECT_EQ(beta->SetString(L"Code", L"B-001"), sc::SC_OK);

    EXPECT_EQ(alpha->SetString(L"BetaCode", L"B-001"), sc::SC_OK);
    EXPECT_EQ(beta->SetString(L"AlphaCode", L"A-001"), sc::SC_OK);
    const sc::RecordId alphaId = alpha->GetId();
    const sc::RecordId betaId = beta->GetId();
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr deleteEdit;
    EXPECT_EQ(db->BeginEdit(L"delete cycle parent", deleteEdit), sc::SC_OK);
    EXPECT_EQ(alphaTable->DeleteRecord(alphaId), sc::SC_OK);
    EXPECT_EQ(db->Commit(deleteEdit.Get()), sc::SC_OK);

    sc::SCRecordPtr alphaReloaded;
    EXPECT_EQ(alphaTable->GetRecord(alphaId, alphaReloaded), sc::SC_OK);
    EXPECT_TRUE(alphaReloaded->IsDeleted());

    sc::SCRecordPtr betaReloaded;
    EXPECT_EQ(betaTable->GetRecord(betaId, betaReloaded), sc::SC_OK);
    EXPECT_TRUE(betaReloaded->IsDeleted());

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_EQ(alphaTable->GetRecord(alphaId, alphaReloaded), sc::SC_OK);
    EXPECT_FALSE(alphaReloaded->IsDeleted());
    EXPECT_EQ(betaTable->GetRecord(betaId, betaReloaded), sc::SC_OK);
    EXPECT_FALSE(betaReloaded->IsDeleted());

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_EQ(alphaTable->GetRecord(alphaId, alphaReloaded), sc::SC_OK);
    EXPECT_TRUE(alphaReloaded->IsDeleted());
    EXPECT_EQ(betaTable->GetRecord(betaId, betaReloaded), sc::SC_OK);
    EXPECT_TRUE(betaReloaded->IsDeleted());
}

TEST(SchemaEdit, ForeignKeyConstraintLeavesStateUnchangedOnRestrictFailure)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_ForeignKeyRestrictRollback.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr anchorTable;
    EXPECT_EQ(db->CreateTable(L"Anchor", anchorTable), sc::SC_OK);
    sc::SCSchemaPtr anchorSchema;
    EXPECT_EQ(anchorTable->GetSchema(anchorSchema), sc::SC_OK);

    sc::SCColumnDef anchorCode;
    anchorCode.name = L"Code";
    anchorCode.displayName = L"Code";
    anchorCode.valueKind = sc::ValueKind::String;
    anchorCode.nullable = false;
    anchorCode.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(anchorSchema->AddColumn(anchorCode), sc::SC_OK);
    EXPECT_EQ(anchorSchema->AddConstraint(MakeUniqueConstraint(L"uq_Anchor_Code", L"Code")), sc::SC_OK);

    sc::SCTablePtr blockerTable;
    EXPECT_EQ(db->CreateTable(L"A_Blocker", blockerTable), sc::SC_OK);
    sc::SCSchemaPtr blockerSchema;
    EXPECT_EQ(blockerTable->GetSchema(blockerSchema), sc::SC_OK);

    sc::SCColumnDef blockerCode;
    blockerCode.name = L"AnchorCode";
    blockerCode.displayName = L"AnchorCode";
    blockerCode.valueKind = sc::ValueKind::String;
    blockerCode.nullable = true;
    blockerCode.defaultValue = sc::SCValue::Null();
    EXPECT_EQ(blockerSchema->AddColumn(blockerCode), sc::SC_OK);
    EXPECT_EQ(blockerSchema->AddConstraint(MakeForeignKeyConstraint(L"fk_Blocker_AnchorCode",
                                                                    L"AnchorCode",
                                                                    L"Anchor",
                                                                    L"Code",
                                                                    sc::SCForeignKeyAction::Restrict,
                                                                    sc::SCForeignKeyAction::Restrict)),
              sc::SC_OK);

    sc::SCTablePtr cascadeTable;
    EXPECT_EQ(db->CreateTable(L"Z_Cascade", cascadeTable), sc::SC_OK);
    sc::SCSchemaPtr cascadeSchema;
    EXPECT_EQ(cascadeTable->GetSchema(cascadeSchema), sc::SC_OK);

    sc::SCColumnDef cascadeCode;
    cascadeCode.name = L"AnchorCode";
    cascadeCode.displayName = L"AnchorCode";
    cascadeCode.valueKind = sc::ValueKind::String;
    cascadeCode.nullable = true;
    cascadeCode.defaultValue = sc::SCValue::Null();
    EXPECT_EQ(cascadeSchema->AddColumn(cascadeCode), sc::SC_OK);
    EXPECT_EQ(cascadeSchema->AddConstraint(MakeForeignKeyConstraint(L"fk_Cascade_AnchorCode",
                                                                    L"AnchorCode",
                                                                    L"Anchor",
                                                                    L"Code",
                                                                    sc::SCForeignKeyAction::Cascade,
                                                                    sc::SCForeignKeyAction::Restrict)),
              sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed restrict rollback rows", seedEdit), sc::SC_OK);

    sc::SCRecordPtr anchor;
    EXPECT_EQ(anchorTable->CreateRecord(anchor), sc::SC_OK);
    EXPECT_EQ(anchor->SetString(L"Code", L"A-001"), sc::SC_OK);
    const sc::RecordId anchorId = anchor->GetId();

    sc::SCRecordPtr blocker;
    EXPECT_EQ(blockerTable->CreateRecord(blocker), sc::SC_OK);
    EXPECT_EQ(blocker->SetString(L"AnchorCode", L"A-001"), sc::SC_OK);
    const sc::RecordId blockerId = blocker->GetId();

    sc::SCRecordPtr cascade;
    EXPECT_EQ(cascadeTable->CreateRecord(cascade), sc::SC_OK);
    EXPECT_EQ(cascade->SetString(L"AnchorCode", L"A-001"), sc::SC_OK);
    const sc::RecordId cascadeId = cascade->GetId();

    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr deleteEdit;
    EXPECT_EQ(db->BeginEdit(L"delete blocked parent", deleteEdit), sc::SC_OK);
    EXPECT_EQ(anchorTable->DeleteRecord(anchorId), sc::SC_E_CONSTRAINT_VIOLATION);

    sc::SCConstraintViolationInfo info;
    EXPECT_EQ(db->GetLastConstraintViolationInfo(&info), sc::SC_OK);
    EXPECT_EQ(info.tableName, L"A_Blocker");
    EXPECT_EQ(info.constraintName, L"fk_Blocker_AnchorCode");
    EXPECT_EQ(info.kind, sc::SCConstraintKind::ForeignKey);

    sc::SCRecordPtr anchorReloaded;
    EXPECT_EQ(anchorTable->GetRecord(anchorId, anchorReloaded), sc::SC_OK);
    EXPECT_FALSE(anchorReloaded->IsDeleted());

    sc::SCRecordPtr blockerReloaded;
    EXPECT_EQ(blockerTable->GetRecord(blockerId, blockerReloaded), sc::SC_OK);
    EXPECT_FALSE(blockerReloaded->IsDeleted());

    sc::SCRecordPtr cascadeReloaded;
    EXPECT_EQ(cascadeTable->GetRecord(cascadeId, cascadeReloaded), sc::SC_OK);
    EXPECT_FALSE(cascadeReloaded->IsDeleted());

    EXPECT_EQ(db->Rollback(deleteEdit.Get()), sc::SC_OK);
}

TEST(SchemaEdit, ForeignKeyConstraintSupportsSetNullCycleDeleteUndoRedo)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_ForeignKeySetNullCycleDeleteUndoRedo.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr alphaTable;
    EXPECT_EQ(db->CreateTable(L"Alpha", alphaTable), sc::SC_OK);
    sc::SCSchemaPtr alphaSchema;
    EXPECT_EQ(alphaTable->GetSchema(alphaSchema), sc::SC_OK);

    sc::SCColumnDef alphaCode;
    alphaCode.name = L"Code";
    alphaCode.displayName = L"Code";
    alphaCode.valueKind = sc::ValueKind::String;
    alphaCode.nullable = false;
    alphaCode.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(alphaSchema->AddColumn(alphaCode), sc::SC_OK);
    EXPECT_EQ(alphaSchema->AddConstraint(MakeUniqueConstraint(L"uq_Alpha_Code", L"Code")), sc::SC_OK);

    sc::SCColumnDef alphaBetaCode;
    alphaBetaCode.name = L"BetaCode";
    alphaBetaCode.displayName = L"BetaCode";
    alphaBetaCode.valueKind = sc::ValueKind::String;
    alphaBetaCode.nullable = true;
    alphaBetaCode.defaultValue = sc::SCValue::Null();
    EXPECT_EQ(alphaSchema->AddColumn(alphaBetaCode), sc::SC_OK);

    sc::SCTablePtr betaTable;
    EXPECT_EQ(db->CreateTable(L"Beta", betaTable), sc::SC_OK);
    sc::SCSchemaPtr betaSchema;
    EXPECT_EQ(betaTable->GetSchema(betaSchema), sc::SC_OK);

    sc::SCColumnDef betaCode;
    betaCode.name = L"Code";
    betaCode.displayName = L"Code";
    betaCode.valueKind = sc::ValueKind::String;
    betaCode.nullable = false;
    betaCode.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(betaSchema->AddColumn(betaCode), sc::SC_OK);
    EXPECT_EQ(betaSchema->AddConstraint(MakeUniqueConstraint(L"uq_Beta_Code", L"Code")), sc::SC_OK);

    sc::SCColumnDef betaAlphaCode;
    betaAlphaCode.name = L"AlphaCode";
    betaAlphaCode.displayName = L"AlphaCode";
    betaAlphaCode.valueKind = sc::ValueKind::String;
    betaAlphaCode.nullable = true;
    betaAlphaCode.defaultValue = sc::SCValue::Null();
    EXPECT_EQ(betaSchema->AddColumn(betaAlphaCode), sc::SC_OK);

    EXPECT_EQ(alphaSchema->AddConstraint(MakeForeignKeyConstraint(L"fk_Alpha_BetaCode",
                                                                  L"BetaCode",
                                                                  L"Beta",
                                                                  L"Code",
                                                                  sc::SCForeignKeyAction::SetNull,
                                                                  sc::SCForeignKeyAction::Restrict)),
              sc::SC_OK);
    EXPECT_EQ(betaSchema->AddConstraint(MakeForeignKeyConstraint(L"fk_Beta_AlphaCode",
                                                                 L"AlphaCode",
                                                                 L"Alpha",
                                                                 L"Code",
                                                                 sc::SCForeignKeyAction::SetNull,
                                                                 sc::SCForeignKeyAction::Restrict)),
              sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed set null cycle rows", seedEdit), sc::SC_OK);

    sc::SCRecordPtr alpha;
    EXPECT_EQ(alphaTable->CreateRecord(alpha), sc::SC_OK);
    EXPECT_EQ(alpha->SetString(L"Code", L"A-001"), sc::SC_OK);

    sc::SCRecordPtr beta;
    EXPECT_EQ(betaTable->CreateRecord(beta), sc::SC_OK);
    EXPECT_EQ(beta->SetString(L"Code", L"B-001"), sc::SC_OK);

    EXPECT_EQ(alpha->SetString(L"BetaCode", L"B-001"), sc::SC_OK);
    EXPECT_EQ(beta->SetString(L"AlphaCode", L"A-001"), sc::SC_OK);
    const sc::RecordId alphaId = alpha->GetId();
    const sc::RecordId betaId = beta->GetId();
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr deleteEdit;
    EXPECT_EQ(db->BeginEdit(L"delete set null parent", deleteEdit), sc::SC_OK);
    EXPECT_EQ(alphaTable->DeleteRecord(alphaId), sc::SC_OK);
    EXPECT_EQ(db->Commit(deleteEdit.Get()), sc::SC_OK);

    sc::SCRecordPtr alphaReloaded;
    EXPECT_EQ(alphaTable->GetRecord(alphaId, alphaReloaded), sc::SC_OK);
    EXPECT_TRUE(alphaReloaded->IsDeleted());

    sc::SCRecordPtr betaReloaded;
    EXPECT_EQ(betaTable->GetRecord(betaId, betaReloaded), sc::SC_OK);
    sc::SCValue fkValue;
    EXPECT_EQ(betaReloaded->GetValue(L"AlphaCode", &fkValue), sc::SC_E_VALUE_IS_NULL);
    EXPECT_TRUE(fkValue.IsNull());

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_EQ(alphaTable->GetRecord(alphaId, alphaReloaded), sc::SC_OK);
    EXPECT_FALSE(alphaReloaded->IsDeleted());
    EXPECT_EQ(betaTable->GetRecord(betaId, betaReloaded), sc::SC_OK);
    EXPECT_EQ(betaReloaded->GetValue(L"AlphaCode", &fkValue), sc::SC_OK);
    std::wstring alphaCodeText;
    EXPECT_EQ(fkValue.AsStringCopy(&alphaCodeText), sc::SC_OK);
    EXPECT_EQ(alphaCodeText, L"A-001");

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_EQ(alphaTable->GetRecord(alphaId, alphaReloaded), sc::SC_OK);
    EXPECT_TRUE(alphaReloaded->IsDeleted());
    EXPECT_EQ(betaTable->GetRecord(betaId, betaReloaded), sc::SC_OK);
    EXPECT_EQ(betaReloaded->GetValue(L"AlphaCode", &fkValue), sc::SC_E_VALUE_IS_NULL);
    EXPECT_TRUE(fkValue.IsNull());
}

TEST(SchemaEdit, ForeignKeyConstraintSupportsSetDefaultCycleDeleteUndoRedo)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_ForeignKeySetDefaultCycleDeleteUndoRedo.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr alphaTable;
    EXPECT_EQ(db->CreateTable(L"Alpha", alphaTable), sc::SC_OK);
    sc::SCSchemaPtr alphaSchema;
    EXPECT_EQ(alphaTable->GetSchema(alphaSchema), sc::SC_OK);

    sc::SCColumnDef alphaCode;
    alphaCode.name = L"Code";
    alphaCode.displayName = L"Code";
    alphaCode.valueKind = sc::ValueKind::String;
    alphaCode.nullable = false;
    alphaCode.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(alphaSchema->AddColumn(alphaCode), sc::SC_OK);
    EXPECT_EQ(alphaSchema->AddConstraint(MakeUniqueConstraint(L"uq_Alpha_Code", L"Code")), sc::SC_OK);

    sc::SCColumnDef alphaBetaCode;
    alphaBetaCode.name = L"BetaCode";
    alphaBetaCode.displayName = L"BetaCode";
    alphaBetaCode.valueKind = sc::ValueKind::String;
    alphaBetaCode.nullable = true;
    alphaBetaCode.defaultValue = sc::SCValue::Null();
    EXPECT_EQ(alphaSchema->AddColumn(alphaBetaCode), sc::SC_OK);

    sc::SCTablePtr betaTable;
    EXPECT_EQ(db->CreateTable(L"Beta", betaTable), sc::SC_OK);
    sc::SCSchemaPtr betaSchema;
    EXPECT_EQ(betaTable->GetSchema(betaSchema), sc::SC_OK);

    sc::SCColumnDef betaCode;
    betaCode.name = L"Code";
    betaCode.displayName = L"Code";
    betaCode.valueKind = sc::ValueKind::String;
    betaCode.nullable = false;
    betaCode.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(betaSchema->AddColumn(betaCode), sc::SC_OK);
    EXPECT_EQ(betaSchema->AddConstraint(MakeUniqueConstraint(L"uq_Beta_Code", L"Code")), sc::SC_OK);

    sc::SCColumnDef betaAlphaCode;
    betaAlphaCode.name = L"AlphaCode";
    betaAlphaCode.displayName = L"AlphaCode";
    betaAlphaCode.valueKind = sc::ValueKind::String;
    betaAlphaCode.nullable = true;
    betaAlphaCode.defaultValue = sc::SCValue::FromString(L"A-DEFAULT");
    EXPECT_EQ(betaSchema->AddColumn(betaAlphaCode), sc::SC_OK);

    EXPECT_EQ(alphaSchema->AddConstraint(MakeForeignKeyConstraint(L"fk_Alpha_BetaCode",
                                                                  L"BetaCode",
                                                                  L"Beta",
                                                                  L"Code",
                                                                  sc::SCForeignKeyAction::SetDefault,
                                                                  sc::SCForeignKeyAction::Restrict)),
              sc::SC_OK);
    EXPECT_EQ(betaSchema->AddConstraint(MakeForeignKeyConstraint(L"fk_Beta_AlphaCode",
                                                                 L"AlphaCode",
                                                                 L"Alpha",
                                                                 L"Code",
                                                                 sc::SCForeignKeyAction::SetDefault,
                                                                 sc::SCForeignKeyAction::Restrict)),
              sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed set default cycle rows", seedEdit), sc::SC_OK);

    sc::SCRecordPtr alphaDefault;
    EXPECT_EQ(alphaTable->CreateRecord(alphaDefault), sc::SC_OK);
    EXPECT_EQ(alphaDefault->SetString(L"Code", L"A-DEFAULT"), sc::SC_OK);

    sc::SCRecordPtr alpha;
    EXPECT_EQ(alphaTable->CreateRecord(alpha), sc::SC_OK);
    EXPECT_EQ(alpha->SetString(L"Code", L"A-001"), sc::SC_OK);

    sc::SCRecordPtr beta;
    EXPECT_EQ(betaTable->CreateRecord(beta), sc::SC_OK);
    EXPECT_EQ(beta->SetString(L"Code", L"B-001"), sc::SC_OK);

    EXPECT_EQ(alpha->SetString(L"BetaCode", L"B-001"), sc::SC_OK);
    EXPECT_EQ(beta->SetString(L"AlphaCode", L"A-001"), sc::SC_OK);
    const sc::RecordId alphaDefaultId = alphaDefault->GetId();
    const sc::RecordId alphaId = alpha->GetId();
    const sc::RecordId betaId = beta->GetId();
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr deleteEdit;
    EXPECT_EQ(db->BeginEdit(L"delete set default parent", deleteEdit), sc::SC_OK);
    EXPECT_EQ(alphaTable->DeleteRecord(alphaId), sc::SC_OK);
    EXPECT_EQ(db->Commit(deleteEdit.Get()), sc::SC_OK);

    sc::SCRecordPtr alphaReloaded;
    EXPECT_EQ(alphaTable->GetRecord(alphaId, alphaReloaded), sc::SC_OK);
    EXPECT_TRUE(alphaReloaded->IsDeleted());

    sc::SCRecordPtr alphaDefaultReloaded;
    EXPECT_EQ(alphaTable->GetRecord(alphaDefaultId, alphaDefaultReloaded), sc::SC_OK);
    EXPECT_FALSE(alphaDefaultReloaded->IsDeleted());

    sc::SCRecordPtr betaReloaded;
    EXPECT_EQ(betaTable->GetRecord(betaId, betaReloaded), sc::SC_OK);
    sc::SCValue fkValue;
    EXPECT_EQ(betaReloaded->GetValue(L"AlphaCode", &fkValue), sc::SC_OK);
    std::wstring alphaCodeText;
    EXPECT_EQ(fkValue.AsStringCopy(&alphaCodeText), sc::SC_OK);
    EXPECT_EQ(alphaCodeText, L"A-DEFAULT");

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_EQ(alphaTable->GetRecord(alphaId, alphaReloaded), sc::SC_OK);
    EXPECT_FALSE(alphaReloaded->IsDeleted());
    EXPECT_EQ(betaTable->GetRecord(betaId, betaReloaded), sc::SC_OK);
    EXPECT_EQ(betaReloaded->GetValue(L"AlphaCode", &fkValue), sc::SC_OK);
    EXPECT_EQ(fkValue.AsStringCopy(&alphaCodeText), sc::SC_OK);
    EXPECT_EQ(alphaCodeText, L"A-001");

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_EQ(alphaTable->GetRecord(alphaId, alphaReloaded), sc::SC_OK);
    EXPECT_TRUE(alphaReloaded->IsDeleted());
    EXPECT_EQ(betaTable->GetRecord(betaId, betaReloaded), sc::SC_OK);
    EXPECT_EQ(betaReloaded->GetValue(L"AlphaCode", &fkValue), sc::SC_OK);
    EXPECT_EQ(fkValue.AsStringCopy(&alphaCodeText), sc::SC_OK);
    EXPECT_EQ(alphaCodeText, L"A-DEFAULT");
}
