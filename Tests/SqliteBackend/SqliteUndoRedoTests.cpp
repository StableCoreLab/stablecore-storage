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