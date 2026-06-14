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

TEST(SchemaEdit, ExplicitCompositeIndexBuildsLogicalQueryIndexStorage)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_CompositeQueryIndex.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
    EXPECT_EQ(schema->AddColumn(MakeIntColumn(L"Width")), sc::SC_OK);
    EXPECT_EQ(schema->AddColumn(MakeStringColumn(L"Name")), sc::SC_OK);
    EXPECT_EQ(schema->AddIndex(MakeCompositeIndex(L"idx_Beam_Width_Name", L"Width", L"Name")), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    std::int64_t value = -1;
    EXPECT_TRUE(QuerySqliteInt64(
        dbPath,
        "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
        "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
        &value));
    EXPECT_EQ(value, 1);

    EXPECT_TRUE(QuerySqliteInt64(
        dbPath,
        "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
        "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
        "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
        &value));
    EXPECT_EQ(value, 1);
}

TEST(SchemaEdit, ExplicitCompositeIndexQueryStorageSurvivesUndoRedoAndReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_CompositeQueryIndexUndoRedoReopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

        sc::SCTablePtr table;
        EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
        EXPECT_EQ(schema->AddColumn(MakeIntColumn(L"Width")), sc::SC_OK);
        EXPECT_EQ(schema->AddColumn(MakeStringColumn(L"Name")), sc::SC_OK);

        sc::SCEditPtr indexEdit;
        EXPECT_EQ(db->BeginEdit(L"add composite index", indexEdit), sc::SC_OK);
        EXPECT_EQ(schema->AddIndex(MakeCompositeIndex(L"idx_Beam_Width_Name", L"Width", L"Name")), sc::SC_OK);
        EXPECT_EQ(db->Commit(indexEdit.Get()), sc::SC_OK);

        sc::SCEditPtr seedEdit;
        EXPECT_EQ(db->BeginEdit(L"seed composite index rows", seedEdit), sc::SC_OK);

        sc::SCRecordPtr row;
        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
        EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

        std::int64_t value = -1;
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 1);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
            "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 1);

        EXPECT_EQ(db->Undo(), sc::SC_OK);
    }

    {
        sc::SCDbPtr reopened;
        EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, reopened), sc::SC_OK);

        sc::SCTablePtr table;
        EXPECT_EQ(reopened->GetTable(L"Beam", table), sc::SC_OK);

        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

        sc::SCIndexDef index;
        EXPECT_EQ(schema->FindIndex(L"idx_Beam_Width_Name", &index), sc::SC_OK);

        std::int64_t value = -1;
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 1);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
            "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 0);

        EXPECT_EQ(reopened->Undo(), sc::SC_OK);
        EXPECT_EQ(schema->FindIndex(L"idx_Beam_Width_Name", &index), sc::SC_E_INDEX_NOT_FOUND);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 0);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
            "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 0);

        EXPECT_EQ(reopened->Redo(), sc::SC_OK);
        EXPECT_EQ(schema->FindIndex(L"idx_Beam_Width_Name", &index), sc::SC_OK);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_indexes qi JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 1);
        EXPECT_TRUE(QuerySqliteInt64(
            dbPath,
            "SELECT COUNT(*) FROM query_index_entries qie JOIN query_indexes qi "
            "ON qi.schema_index_id = qie.schema_index_id JOIN tables t ON t.table_id = qi.table_id "
            "WHERE t.name = 'Beam' AND qi.index_name = 'idx_Beam_Width_Name';",
            &value));
        EXPECT_EQ(value, 0);
    }
}