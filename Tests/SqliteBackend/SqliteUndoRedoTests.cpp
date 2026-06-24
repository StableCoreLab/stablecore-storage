#include <filesystem>
#include <sstream>

#include <gtest/gtest.h>

#include "SCStorage.h"

#include "Support/TestPaths.h"
#include "Support/TestSqliteHelpers.h"

namespace sc = StableCore::Storage;
namespace fs = std::filesystem;

namespace
{
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

TEST(SqliteUndoRedo, UndoRedoSurvivesReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_UndoRedo_Reopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
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
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);

        sc::SCTablePtr beamTable;
        EXPECT_EQ(reopened->GetTable(L"Beam", beamTable), sc::SC_OK);

        sc::SCRecordCursorPtr cursor;
        EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
        sc::SCRecordPtr beam;
        EXPECT_EQ(cursor->Next(beam), sc::SC_OK);
        EXPECT_TRUE(static_cast<bool>(beam));

        std::int64_t width = 0;
        EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
        EXPECT_EQ(width, 100);

        EXPECT_EQ(reopened->Redo(), sc::SC_OK);
        EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
        EXPECT_EQ(width, 450);
    }
}

TEST(SqliteUndoRedo, SchemaColumnJournalSurvivesReopenAndUndoRedo)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_UndoRedo_SchemaJournal.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr table = CreateBeamTable(db);
        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

        sc::SCColumnDef name;
        name.name = L"Name";
        name.displayName = L"Name";
        name.valueKind = sc::ValueKind::String;
        name.defaultValue = sc::SCValue::FromString(L"");
        EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);

        sc::SCColumnDef height;
        height.name = L"Height";
        height.displayName = L"Height";
        height.valueKind = sc::ValueKind::Int64;
        height.defaultValue = sc::SCValue::FromInt64(0);

        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"add height", edit), sc::SC_OK);
        EXPECT_EQ(schema->AddColumn(height), sc::SC_OK);
        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    }

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr table;
        EXPECT_EQ(db->GetTable(L"Beam", table), sc::SC_OK);
        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

        sc::SCColumnDef loaded;
        EXPECT_EQ(schema->FindColumn(L"Height", &loaded), sc::SC_OK);
        EXPECT_EQ(loaded.displayName, L"Height");

        EXPECT_EQ(db->Undo(), sc::SC_OK);
        EXPECT_EQ(schema->FindColumn(L"Height", &loaded), sc::SC_E_COLUMN_NOT_FOUND);

        EXPECT_EQ(db->Redo(), sc::SC_OK);
        EXPECT_EQ(schema->FindColumn(L"Height", &loaded), sc::SC_OK);
        EXPECT_EQ(loaded.displayName, L"Height");
    }
}

TEST(SqliteUndoRedo, SchemaColumnUndoRestoreKeepsOriginalColumnOrder)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_UndoRedo_SchemaOrder.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef name;
    name.name = L"Name";
    name.displayName = L"Name";
    name.valueKind = sc::ValueKind::String;
    name.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);

    sc::SCColumnDef height;
    height.name = L"Height";
    height.displayName = L"Height";
    height.valueKind = sc::ValueKind::Int64;
    height.defaultValue = sc::SCValue::FromInt64(0);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"add height", edit), sc::SC_OK);
    EXPECT_EQ(schema->AddColumn(height), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    std::int32_t columnCount = 0;
    sc::SCColumnDef column;
    EXPECT_EQ(schema->GetColumnCount(&columnCount), sc::SC_OK);
    EXPECT_EQ(columnCount, 3);
    EXPECT_EQ(schema->GetColumn(0, &column), sc::SC_OK);
    EXPECT_EQ(column.name, L"Width");
    EXPECT_EQ(schema->GetColumn(1, &column), sc::SC_OK);
    EXPECT_EQ(column.name, L"Name");
    EXPECT_EQ(schema->GetColumn(2, &column), sc::SC_OK);
    EXPECT_EQ(column.name, L"Height");

    EXPECT_EQ(db->BeginEdit(L"remove width", edit), sc::SC_OK);
    EXPECT_EQ(schema->RemoveColumn(L"Width"), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    EXPECT_EQ(schema->GetColumnCount(&columnCount), sc::SC_OK);
    EXPECT_EQ(columnCount, 2);
    EXPECT_EQ(schema->GetColumn(0, &column), sc::SC_OK);
    EXPECT_EQ(column.name, L"Name");
    EXPECT_EQ(schema->GetColumn(1, &column), sc::SC_OK);
    EXPECT_EQ(column.name, L"Height");

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_EQ(schema->GetColumnCount(&columnCount), sc::SC_OK);
    EXPECT_EQ(columnCount, 3);
    EXPECT_EQ(schema->GetColumn(0, &column), sc::SC_OK);
    EXPECT_EQ(column.name, L"Width");
    EXPECT_EQ(schema->GetColumn(1, &column), sc::SC_OK);
    EXPECT_EQ(column.name, L"Name");
    EXPECT_EQ(schema->GetColumn(2, &column), sc::SC_OK);
    EXPECT_EQ(column.name, L"Height");

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_EQ(schema->GetColumnCount(&columnCount), sc::SC_OK);
    EXPECT_EQ(columnCount, 2);
    EXPECT_EQ(schema->GetColumn(0, &column), sc::SC_OK);
    EXPECT_EQ(column.name, L"Name");
    EXPECT_EQ(schema->GetColumn(1, &column), sc::SC_OK);
    EXPECT_EQ(column.name, L"Height");
}

TEST(SqliteUndoRedo, PersistedRedoRejectsParentUpdateThatViolatesReopenedForeignKey)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_UndoRedo_PersistedRedoForeignKey.sqlite");
    sc::RecordId floorId = 0;
    sc::RecordId beamId = 0;

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

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

        sc::SCConstraintDef floorCodeUnique;
        floorCodeUnique.kind = sc::SCConstraintKind::Unique;
        floorCodeUnique.name = L"uq_Floor_Code";
        floorCodeUnique.columns.push_back(L"Code");
        EXPECT_EQ(floorSchema->AddConstraint(floorCodeUnique), sc::SC_OK);

        sc::SCTablePtr beamTable;
        EXPECT_EQ(db->CreateTable(L"Beam", beamTable), sc::SC_OK);
        sc::SCSchemaPtr beamSchema;
        EXPECT_EQ(beamTable->GetSchema(beamSchema), sc::SC_OK);

        sc::SCColumnDef beamFloorCode;
        beamFloorCode.name = L"FloorCode";
        beamFloorCode.displayName = L"FloorCode";
        beamFloorCode.valueKind = sc::ValueKind::String;
        beamFloorCode.nullable = true;
        beamFloorCode.defaultValue = sc::SCValue::Null();
        EXPECT_EQ(beamSchema->AddColumn(beamFloorCode), sc::SC_OK);

        sc::SCEditPtr seedEdit;
        EXPECT_EQ(db->BeginEdit(L"seed rows", seedEdit), sc::SC_OK);
        sc::SCRecordPtr floor;
        EXPECT_EQ(floorTable->CreateRecord(floor), sc::SC_OK);
        EXPECT_EQ(floor->SetString(L"Code", L"F-001"), sc::SC_OK);
        sc::SCRecordPtr beam;
        EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
        EXPECT_EQ(beam->SetString(L"FloorCode", L"F-001"), sc::SC_OK);
        EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);
        floorId = floor->GetId();
        beamId = beam->GetId();

        sc::SCEditPtr parentUpdateEdit;
        EXPECT_EQ(db->BeginEdit(L"update parent code", parentUpdateEdit), sc::SC_OK);
        EXPECT_EQ(floor->SetString(L"Code", L"F-002"), sc::SC_OK);
        EXPECT_EQ(db->Commit(parentUpdateEdit.Get()), sc::SC_OK);

        sc::SCEditPtr childUpdateEdit;
        EXPECT_EQ(db->BeginEdit(L"update child code", childUpdateEdit), sc::SC_OK);
        EXPECT_EQ(beam->SetString(L"FloorCode", L"F-002"), sc::SC_OK);
        EXPECT_EQ(db->Commit(childUpdateEdit.Get()), sc::SC_OK);

        EXPECT_EQ(db->Undo(), sc::SC_OK);
        EXPECT_EQ(db->Undo(), sc::SC_OK);

        std::int64_t beamTableId = 0;
        ASSERT_TRUE(QuerySqliteInt64(dbPath, "SELECT table_id FROM tables WHERE name = 'Beam';", &beamTableId));

        std::ostringstream sql;
        sql << "BEGIN;"
            << "INSERT INTO schema_constraints(table_id, kind, name, source_kind, referenced_table, "
               "on_delete_action, on_update_action, check_expression) VALUES("
            << beamTableId << ", "
            << static_cast<int>(sc::SCConstraintKind::ForeignKey) << ", 'fk_Beam_FloorCode', "
            << static_cast<int>(sc::SCSchemaSourceKind::Explicit) << ", 'Floor', "
            << static_cast<int>(sc::SCForeignKeyAction::Restrict) << ", "
            << static_cast<int>(sc::SCForeignKeyAction::Restrict) << ", '');"
            << "INSERT INTO schema_constraint_columns(constraint_id, column_ordinal, column_name, "
               "referenced_column_name) VALUES(last_insert_rowid(), 0, 'FloorCode', 'Code');"
            << "COMMIT;";
        ASSERT_TRUE(ExecSqliteScript(dbPath, sql.str().c_str()));
    }

    sc::SCDbPtr reopened;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);

    sc::SCTablePtr floorTable;
    EXPECT_EQ(reopened->GetTable(L"Floor", floorTable), sc::SC_OK);
    sc::SCTablePtr beamTable;
    EXPECT_EQ(reopened->GetTable(L"Beam", beamTable), sc::SC_OK);

    EXPECT_EQ(reopened->Redo(), sc::SC_E_CONSTRAINT_VIOLATION);

    sc::SCRecordPtr floor;
    EXPECT_EQ(floorTable->GetRecord(floorId, floor), sc::SC_OK);
    const wchar_t* floorCode = nullptr;
    EXPECT_EQ(floor->GetString(L"Code", &floorCode), sc::SC_OK);
    EXPECT_STREQ(floorCode, L"F-001");

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->GetRecord(beamId, beam), sc::SC_OK);
    const wchar_t* beamFloorCodeText = nullptr;
    EXPECT_EQ(beam->GetString(L"FloorCode", &beamFloorCodeText), sc::SC_OK);
    EXPECT_STREQ(beamFloorCodeText, L"F-001");
}
