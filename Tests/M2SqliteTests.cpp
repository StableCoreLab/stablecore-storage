#include <filesystem>

#include <gtest/gtest.h>

#include "StableCore/Storage/Storage.h"

namespace sc = stablecore::storage;
namespace fs = std::filesystem;

namespace
{

fs::path MakeTempDbPath(const wchar_t* fileName)
{
    fs::path path = fs::temp_directory_path() / fileName;
    std::error_code ec;
    fs::remove(path, ec);
    return path;
}

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

    return table;
}

}  // namespace

TEST(StorageM2Sqlite, PersistedRecordSurvivesReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_M2_Reopen.sqlite");

    {
        sc::DbPtr db;
        EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), db), sc::SC_OK);

        sc::TablePtr beamTable = CreateBeamTable(db);

        sc::EditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

        sc::RecordPtr beam;
        EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
        EXPECT_EQ(beam->SetInt64(L"Width", 320), sc::SC_OK);
        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    }

    {
        sc::DbPtr reopened;
        EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), reopened), sc::SC_OK);

        sc::TablePtr beamTable;
        EXPECT_EQ(reopened->GetTable(L"Beam", beamTable), sc::SC_OK);

        sc::RecordCursorPtr cursor;
        EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);

        bool hasRow = false;
        EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
        EXPECT_TRUE(hasRow);

        sc::RecordPtr beam;
        EXPECT_EQ(cursor->GetCurrent(beam), sc::SC_OK);

        std::int64_t width = 0;
        EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
        EXPECT_EQ(width, 320);
    }
}

TEST(StorageM2Sqlite, UndoRedoSurvivesReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_M2_UndoRedo.sqlite");

    {
        sc::DbPtr db;
        EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), db), sc::SC_OK);
        sc::TablePtr beamTable = CreateBeamTable(db);

        sc::EditPtr createEdit;
        EXPECT_EQ(db->BeginEdit(L"create", createEdit), sc::SC_OK);
        sc::RecordPtr beam;
        EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
        EXPECT_EQ(beam->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(db->Commit(createEdit.Get()), sc::SC_OK);

        sc::EditPtr modifyEdit;
        EXPECT_EQ(db->BeginEdit(L"modify", modifyEdit), sc::SC_OK);
        EXPECT_EQ(beam->SetInt64(L"Width", 450), sc::SC_OK);
        EXPECT_EQ(db->Commit(modifyEdit.Get()), sc::SC_OK);

        EXPECT_EQ(db->Undo(), sc::SC_OK);
    }

    {
        sc::DbPtr reopened;
        EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), reopened), sc::SC_OK);

        sc::TablePtr beamTable;
        EXPECT_EQ(reopened->GetTable(L"Beam", beamTable), sc::SC_OK);

        sc::RecordCursorPtr cursor;
        EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
        bool hasRow = false;
        EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
        EXPECT_TRUE(hasRow);

        sc::RecordPtr beam;
        EXPECT_EQ(cursor->GetCurrent(beam), sc::SC_OK);

        std::int64_t width = 0;
        EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
        EXPECT_EQ(width, 100);

        EXPECT_EQ(reopened->Redo(), sc::SC_OK);
        EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
        EXPECT_EQ(width, 450);
    }
}

TEST(StorageM2Sqlite, PersistedQueryAndDelete)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_M2_Query.sqlite");

    {
        sc::DbPtr db;
        EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), db), sc::SC_OK);
        sc::TablePtr beamTable = CreateBeamTable(db);

        sc::EditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

        sc::RecordPtr beamA;
        EXPECT_EQ(beamTable->CreateRecord(beamA), sc::SC_OK);
        EXPECT_EQ(beamA->SetInt64(L"Width", 200), sc::SC_OK);

        sc::RecordPtr beamB;
        EXPECT_EQ(beamTable->CreateRecord(beamB), sc::SC_OK);
        EXPECT_EQ(beamB->SetInt64(L"Width", 500), sc::SC_OK);

        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

        sc::EditPtr deleteEdit;
        EXPECT_EQ(db->BeginEdit(L"delete", deleteEdit), sc::SC_OK);
        EXPECT_EQ(beamTable->DeleteRecord(beamA->GetId()), sc::SC_OK);
        EXPECT_EQ(db->Commit(deleteEdit.Get()), sc::SC_OK);
    }

    {
        sc::DbPtr reopened;
        EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), reopened), sc::SC_OK);
        sc::TablePtr beamTable;
        EXPECT_EQ(reopened->GetTable(L"Beam", beamTable), sc::SC_OK);

        sc::RecordCursorPtr cursor;
        sc::QueryCondition condition{L"Width", sc::Value::FromInt64(500)};
        EXPECT_EQ(beamTable->FindRecords(condition, cursor), sc::SC_OK);

        bool hasRow = false;
        EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
        EXPECT_TRUE(hasRow);

        sc::RecordPtr beam;
        EXPECT_EQ(cursor->GetCurrent(beam), sc::SC_OK);

        std::int64_t width = 0;
        EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
        EXPECT_EQ(width, 500);
    }
}
