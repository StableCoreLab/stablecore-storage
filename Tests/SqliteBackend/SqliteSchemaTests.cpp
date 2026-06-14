#include <filesystem>
#include <string>

#include <gtest/gtest.h>
#include <sqlite3.h>

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

    bool ExecSqliteScript(const fs::path& dbPath, const char* sql)
    {
        sqlite3* db = nullptr;
        const std::string narrowPath = dbPath.string();
        if (sqlite3_open_v2(narrowPath.c_str(), &db, SQLITE_OPEN_READWRITE, nullptr) != SQLITE_OK)
        {
            if (db != nullptr)
            {
                sqlite3_close(db);
            }
            return false;
        }

        char* error = nullptr;
        const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &error);
        if (error != nullptr)
        {
            sqlite3_free(error);
        }
        sqlite3_close(db);
        return rc == SQLITE_OK;
    }

    bool QuerySqliteInt64(const fs::path& dbPath, const char* sql, std::int64_t* outValue)
    {
        if (outValue == nullptr)
        {
            return false;
        }

        sqlite3* db = nullptr;
        const std::string narrowPath = dbPath.string();
        if (sqlite3_open_v2(narrowPath.c_str(), &db, SQLITE_OPEN_READWRITE, nullptr) != SQLITE_OK)
        {
            if (db != nullptr)
            {
                sqlite3_close(db);
            }
            return false;
        }

        sqlite3_stmt* stmt = nullptr;
        const int prepareRc = sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
        if (prepareRc != SQLITE_OK)
        {
            sqlite3_close(db);
            return false;
        }

        const int stepRc = sqlite3_step(stmt);
        bool ok = false;
        if (stepRc == SQLITE_ROW)
        {
            *outValue = sqlite3_column_int64(stmt, 0);
            ok = true;
        }

        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return ok;
    }

    bool QuerySqliteExists(const fs::path& dbPath, const char* sql)
    {
        std::int64_t value = 0;
        return QuerySqliteInt64(dbPath, sql, &value) && value != 0;
    }
}

TEST(SqliteSchema, RemoveColumnCleansSqliteFootprint)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Schema_RemoveColumnFootprint.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed beam", edit), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 128), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(beamTable->GetSchema(schema), sc::SC_OK);
    std::int64_t tableId = -1;
    EXPECT_TRUE(QuerySqliteInt64(dbPath, "SELECT table_id FROM tables WHERE name = 'Beam' LIMIT 1;", &tableId));
    EXPECT_EQ(schema->RemoveColumn(L"Width"), sc::SC_OK);

    std::int64_t schemaColumnCount = -1;
    EXPECT_TRUE(QuerySqliteInt64(dbPath,
                                 "SELECT COUNT(*) FROM schema_columns sc JOIN tables t "
                                 "ON t.table_id = sc.table_id "
                                 "WHERE t.name = 'Beam' AND sc.column_name = 'Width';",
                                 &schemaColumnCount));
    EXPECT_EQ(schemaColumnCount, 0);

    std::int64_t fieldValueCount = -1;
    EXPECT_TRUE(QuerySqliteInt64(dbPath,
                                 "SELECT COUNT(*) FROM field_values fv JOIN tables t "
                                 "ON t.table_id = fv.table_id "
                                 "WHERE t.name = 'Beam' AND fv.column_name = 'Width';",
                                 &fieldValueCount));
    EXPECT_EQ(fieldValueCount, 0);

    const std::string indexSql = "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'idx_fv_" +
                                 std::to_string(tableId) + "_Width' LIMIT 1;";
    EXPECT_FALSE(QuerySqliteExists(dbPath, indexSql.c_str()));
}

TEST(SqliteSchema, UpdateColumnFailureDuringValueRewriteRestoresSchemaAndRecordValues)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Schema_UpdateColumnRewriteFail.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed", seedEdit), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    const sc::RecordId beamId = beam->GetId();
    EXPECT_EQ(beam->SetInt64(L"Width", 128), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    EXPECT_TRUE(ExecSqliteScript(dbPath,
                                 "CREATE TRIGGER fail_width_value_rewrite "
                                 "BEFORE DELETE ON field_values "
                                 "WHEN OLD.column_name = 'Width' "
                                 "BEGIN "
                                 "  SELECT RAISE(ABORT, 'forced field value rewrite failure'); "
                                 "END;"));

    sc::SCSchemaPtr schema;
    EXPECT_EQ(beamTable->GetSchema(schema), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"update width", edit), sc::SC_OK);

    sc::SCColumnDef updated;
    updated.name = L"Width";
    updated.displayName = L"Width Label";
    updated.valueKind = sc::ValueKind::String;
    updated.defaultValue = sc::SCValue::FromString(L"0");
    EXPECT_NE(schema->UpdateColumn(updated), sc::SC_OK);

    sc::SCColumnDef loaded;
    EXPECT_EQ(schema->FindColumn(L"Width", &loaded), sc::SC_OK);
    EXPECT_EQ(loaded.displayName, L"Width");
    EXPECT_EQ(loaded.valueKind, sc::ValueKind::Int64);

    sc::SCRecordPtr reloaded;
    EXPECT_EQ(beamTable->GetRecord(beamId, reloaded), sc::SC_OK);
    std::int64_t width = 0;
    EXPECT_EQ(reloaded->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 128);

    EXPECT_EQ(db->Rollback(edit.Get()), sc::SC_OK);

    EXPECT_EQ(schema->FindColumn(L"Width", &loaded), sc::SC_OK);
    EXPECT_EQ(loaded.displayName, L"Width");
    EXPECT_EQ(loaded.valueKind, sc::ValueKind::Int64);
    EXPECT_EQ(beamTable->GetRecord(beamId, reloaded), sc::SC_OK);
    EXPECT_EQ(reloaded->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 128);
}

TEST(SqliteSchema, UpdateColumnCrossingBinaryBoundaryClearsExistingValues)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Schema_UpdateColumnBinaryBoundary.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr beamTable = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(beamTable->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef attachment;
    attachment.name = L"Attachment";
    attachment.displayName = L"Attachment";
    attachment.valueKind = sc::ValueKind::Binary;
    attachment.nullable = true;
    EXPECT_EQ(schema->AddColumn(attachment), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed", seedEdit), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    const sc::RecordId beamId = beam->GetId();
    const std::vector<std::uint8_t> payload{0x01, 0x02, 0x03};
    EXPECT_EQ(beam->SetInt64(L"Width", 128), sc::SC_OK);
    EXPECT_EQ(beam->SetBinary(L"Attachment", payload.data(), payload.size()), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"rewrite across binary boundary", edit), sc::SC_OK);

    sc::SCColumnDef widthAsBinary;
    widthAsBinary.name = L"Width";
    widthAsBinary.displayName = L"Width";
    widthAsBinary.valueKind = sc::ValueKind::Binary;
    widthAsBinary.nullable = true;
    EXPECT_EQ(schema->UpdateColumn(widthAsBinary), sc::SC_OK);

    sc::SCColumnDef attachmentAsString;
    attachmentAsString.name = L"Attachment";
    attachmentAsString.displayName = L"Attachment";
    attachmentAsString.valueKind = sc::ValueKind::String;
    attachmentAsString.nullable = true;
    EXPECT_EQ(schema->UpdateColumn(attachmentAsString), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    sc::SCRecordPtr reloaded;
    EXPECT_EQ(beamTable->GetRecord(beamId, reloaded), sc::SC_OK);

    std::vector<std::uint8_t> loadedBinary;
    EXPECT_EQ(reloaded->GetBinaryCopy(L"Width", &loadedBinary), sc::SC_E_VALUE_IS_NULL);
    EXPECT_TRUE(loadedBinary.empty());

    std::wstring loadedText;
    EXPECT_EQ(reloaded->GetStringCopy(L"Attachment", &loadedText), sc::SC_E_VALUE_IS_NULL);
    EXPECT_TRUE(loadedText.empty());
}