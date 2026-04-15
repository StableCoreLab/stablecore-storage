#include <filesystem>

#include <gtest/gtest.h>

#include "SCStorage.h"

namespace sc = StableCore::Storage;
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

}  // namespace

TEST(StorageM2Sqlite, PersistedRecordSurvivesReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_M2_Reopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr beamTable = CreateBeamTable(db);

        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

        sc::SCRecordPtr beam;
        EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
        EXPECT_EQ(beam->SetInt64(L"Width", 320), sc::SC_OK);
        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    }

    {
        sc::SCDbPtr reopened;
        EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), reopened), sc::SC_OK);

        sc::SCTablePtr beamTable;
        EXPECT_EQ(reopened->GetTable(L"Beam", beamTable), sc::SC_OK);

        sc::SCRecordCursorPtr cursor;
        EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);

        bool hasRow = false;
        EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
        EXPECT_TRUE(hasRow);

        sc::SCRecordPtr beam;
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
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), db), sc::SC_OK);
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
        EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), reopened), sc::SC_OK);

        sc::SCTablePtr beamTable;
        EXPECT_EQ(reopened->GetTable(L"Beam", beamTable), sc::SC_OK);

        sc::SCRecordCursorPtr cursor;
        EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
        bool hasRow = false;
        EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
        EXPECT_TRUE(hasRow);

        sc::SCRecordPtr beam;
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
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), db), sc::SC_OK);
        sc::SCTablePtr beamTable = CreateBeamTable(db);

        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

        sc::SCRecordPtr beamA;
        EXPECT_EQ(beamTable->CreateRecord(beamA), sc::SC_OK);
        EXPECT_EQ(beamA->SetInt64(L"Width", 200), sc::SC_OK);

        sc::SCRecordPtr beamB;
        EXPECT_EQ(beamTable->CreateRecord(beamB), sc::SC_OK);
        EXPECT_EQ(beamB->SetInt64(L"Width", 500), sc::SC_OK);

        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

        sc::SCEditPtr deleteEdit;
        EXPECT_EQ(db->BeginEdit(L"delete", deleteEdit), sc::SC_OK);
        EXPECT_EQ(beamTable->DeleteRecord(beamA->GetId()), sc::SC_OK);
        EXPECT_EQ(db->Commit(deleteEdit.Get()), sc::SC_OK);
    }

    {
        sc::SCDbPtr reopened;
        EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), reopened), sc::SC_OK);
        sc::SCTablePtr beamTable;
        EXPECT_EQ(reopened->GetTable(L"Beam", beamTable), sc::SC_OK);

        sc::SCRecordCursorPtr cursor;
        sc::SCQueryCondition condition{L"Width", sc::SCValue::FromInt64(500)};
        EXPECT_EQ(beamTable->FindRecords(condition, cursor), sc::SC_OK);

        bool hasRow = false;
        EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
        EXPECT_TRUE(hasRow);

        sc::SCRecordPtr beam;
        EXPECT_EQ(cursor->GetCurrent(beam), sc::SC_OK);

        std::int64_t width = 0;
        EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
        EXPECT_EQ(width, 500);
    }
}

TEST(StorageM2Sqlite, PersistedSchemaRejectsInvalidReferenceTableUsage)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_M2_SchemaValidation.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr beamTable;
    EXPECT_EQ(db->CreateTable(L"Beam", beamTable), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(beamTable->GetSchema(schema), sc::SC_OK);

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

TEST(StorageM2Sqlite, PersistedEmptyQueryIsNotError)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_M2_EmptyQuery.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateSqliteDatabase(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(beamTable->FindRecords({L"Width", sc::SCValue::FromInt64(12345)}, cursor), sc::SC_OK);

    bool hasRow = true;
    EXPECT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
    EXPECT_FALSE(hasRow);
}
