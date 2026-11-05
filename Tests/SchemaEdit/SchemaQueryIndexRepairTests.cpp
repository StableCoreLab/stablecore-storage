#include <algorithm>
#include <filesystem>
#include <string>

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

TEST(SchemaEdit, QueryIndexProviderDetectsDriftAndMaintainerRepairsIt)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_QueryIndexRepair.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
    EXPECT_EQ(schema->AddIndex(MakeCompositeIndex(L"idx_Beam_Width_Name", L"Width", L"Name")), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed query index drift", seedEdit), sc::SC_OK);
    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    const auto* provider = dynamic_cast<const sc::IQueryIndexProvider*>(db.Get());
    ASSERT_NE(provider, nullptr);
    auto* maintainer = dynamic_cast<sc::IQueryIndexMaintainer*>(db.Get());
    ASSERT_NE(maintainer, nullptr);

    sc::QueryIndexCheckResult check;
    EXPECT_EQ(provider->CheckQueryIndex(&check), sc::SC_OK);
    EXPECT_EQ(check.state, sc::QueryIndexHealthState::Healthy);

    EXPECT_TRUE(ExecSqliteScript(dbPath, "DELETE FROM query_index_entries;"));

    EXPECT_EQ(provider->CheckQueryIndex(&check), sc::SC_OK);
    EXPECT_EQ(check.state, sc::QueryIndexHealthState::Corrupted);
    EXPECT_NE(check.message.find(L"query-index-entry-missing"), std::wstring::npos);

    EXPECT_EQ(maintainer->RebuildQueryIndex(), sc::SC_OK);
    EXPECT_EQ(provider->CheckQueryIndex(&check), sc::SC_OK);
    EXPECT_EQ(check.state, sc::QueryIndexHealthState::Healthy);

    std::vector<std::wstring> names;
    sc::QueryExecutionResult queryResult;
    sc::QueryConstraints constraints;
    constraints.requireIndex = true;
    constraints.allowFallbackScan = false;
    EXPECT_EQ(ExecuteQueryForBeam(db.Get(),
                                  {sc::QueryCondition{L"Width",
                                                      sc::QueryConditionOperator::Equal,
                                                      {sc::SCValue::FromInt64(100)}},
                                   sc::QueryCondition{L"Name",
                                                      sc::QueryConditionOperator::Equal,
                                                      {sc::SCValue::FromString(L"Alpha")}}},
                                  constraints,
                                  &names,
                                  &queryResult),
              sc::SC_OK);
    EXPECT_EQ(queryResult.mode, sc::QueryExecutionMode::DirectIndex);
    ASSERT_EQ(names.size(), 1u);
    EXPECT_EQ(names.front(), L"Alpha");
}

TEST(SchemaEdit, QueryIndexRepairFailureRestoresPersistedEntriesAndRemainsUsable)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QueryIndexRepairRollback.sqlite");
    sc::SCDbPtr db;
    ASSERT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    ASSERT_EQ(table->GetSchema(schema), sc::SC_OK);
    ASSERT_EQ(schema->AddIndex(MakeCompositeIndex(L"idx_Width_Name", L"Width", L"Name")), sc::SC_OK);
    sc::SCEditPtr edit;
    ASSERT_EQ(db->BeginEdit(L"seed repair rollback", edit), sc::SC_OK);
    sc::SCRecordPtr row;
    ASSERT_EQ(table->CreateRecord(row), sc::SC_OK);
    ASSERT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    ASSERT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    ASSERT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    std::int64_t beforeCount = 0;
    ASSERT_TRUE(QuerySqliteInt64(dbPath, "SELECT COUNT(*) FROM query_index_entries;", &beforeCount));
    ASSERT_GT(beforeCount, 0);
    ASSERT_TRUE(ExecSqliteScript(dbPath,
        "CREATE TRIGGER fail_query_index_repair BEFORE INSERT ON query_index_entries "
        "BEGIN SELECT RAISE(ABORT, 'forced query index rebuild failure'); END;"));
    auto* maintainer = dynamic_cast<sc::IQueryIndexMaintainer*>(db.Get());
    const auto* provider = dynamic_cast<const sc::IQueryIndexProvider*>(db.Get());
    ASSERT_NE(maintainer, nullptr);
    ASSERT_NE(provider, nullptr);
    EXPECT_TRUE(sc::Failed(maintainer->RebuildQueryIndex()));
    std::int64_t afterCount = 0;
    ASSERT_TRUE(QuerySqliteInt64(dbPath, "SELECT COUNT(*) FROM query_index_entries;", &afterCount));
    EXPECT_EQ(afterCount, beforeCount);
    sc::QueryIndexCheckResult check;
    ASSERT_EQ(provider->CheckQueryIndex(&check), sc::SC_OK);
    EXPECT_EQ(check.state, sc::QueryIndexHealthState::Healthy);

    ASSERT_TRUE(ExecSqliteScript(dbPath, "DROP TRIGGER fail_query_index_repair;"));
    EXPECT_EQ(maintainer->RebuildQueryIndex(), sc::SC_OK);
    EXPECT_EQ(db->BeginEdit(L"edit after failed repair", edit), sc::SC_OK);
    EXPECT_EQ(db->Rollback(edit.Get()), sc::SC_OK);
}

TEST(SchemaEdit, QueryIndexRepairAcceptsSqliteAutomaticRollback)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QueryIndexAutoRollback.sqlite");
    sc::SCDbPtr db;
    ASSERT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    ASSERT_EQ(table->GetSchema(schema), sc::SC_OK);
    ASSERT_EQ(schema->AddIndex(MakeCompositeIndex(L"idx_Width_Name", L"Width", L"Name")), sc::SC_OK);
    sc::SCEditPtr edit;
    ASSERT_EQ(db->BeginEdit(L"seed automatic rollback", edit), sc::SC_OK);
    sc::SCRecordPtr row;
    ASSERT_EQ(table->CreateRecord(row), sc::SC_OK);
    ASSERT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    ASSERT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    ASSERT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    std::int64_t beforeCount = 0;
    ASSERT_TRUE(QuerySqliteInt64(dbPath, "SELECT COUNT(*) FROM query_index_entries;", &beforeCount));
    ASSERT_GT(beforeCount, 0);
    ASSERT_TRUE(ExecSqliteScript(dbPath,
        "CREATE TRIGGER rollback_query_index_repair BEFORE INSERT ON query_index_entries "
        "BEGIN SELECT RAISE(ROLLBACK, 'forced automatic rollback'); END;"));
    auto* maintainer = dynamic_cast<sc::IQueryIndexMaintainer*>(db.Get());
    const auto* provider = dynamic_cast<const sc::IQueryIndexProvider*>(db.Get());
    ASSERT_NE(maintainer, nullptr);
    ASSERT_NE(provider, nullptr);
    EXPECT_TRUE(sc::Failed(maintainer->RebuildQueryIndex()));
    std::int64_t afterCount = 0;
    ASSERT_TRUE(QuerySqliteInt64(dbPath, "SELECT COUNT(*) FROM query_index_entries;", &afterCount));
    EXPECT_EQ(afterCount, beforeCount);
    sc::QueryIndexCheckResult check;
    ASSERT_EQ(provider->CheckQueryIndex(&check), sc::SC_OK);
    EXPECT_EQ(check.state, sc::QueryIndexHealthState::Healthy);
    sc::SCEditingDatabaseState state;
    ASSERT_EQ(db->GetEditingState(&state), sc::SC_OK);
    EXPECT_FALSE(state.consistencyLost);

    ASSERT_TRUE(ExecSqliteScript(dbPath, "DROP TRIGGER rollback_query_index_repair;"));
    EXPECT_EQ(maintainer->RebuildQueryIndex(), sc::SC_OK);
    EXPECT_EQ(db->BeginEdit(L"edit after automatic rollback", edit), sc::SC_OK);
    EXPECT_EQ(db->Rollback(edit.Get()), sc::SC_OK);
}

TEST(SchemaEdit, QueryIndexRepairRollsBackWhenLegacyIndexCreationFails)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_LegacyIndexRepairRollback.sqlite");
    sc::SCDbPtr db;
    ASSERT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr table = CreateQueryableBeamTable(db);
    sc::SCSchemaPtr schema;
    ASSERT_EQ(table->GetSchema(schema), sc::SC_OK);
    ASSERT_EQ(schema->AddIndex(MakeCompositeIndex(L"idx_Width_Name", L"Width", L"Name")), sc::SC_OK);
    std::int64_t tableId = 0;
    ASSERT_TRUE(QuerySqliteInt64(dbPath, "SELECT table_id FROM tables WHERE name = 'Beam';", &tableId));
    const std::string widthIndex = "idx_fv_" + std::to_string(tableId) + "_Width";
    const std::string nameIndex = "idx_fv_" + std::to_string(tableId) + "_Name";
    const std::string setupSql = "DROP INDEX IF EXISTS " + widthIndex + "; "
                                 "DROP INDEX IF EXISTS " + nameIndex + "; "
                                 "CREATE TABLE " + nameIndex + " (value INTEGER); "
                                 "DELETE FROM query_index_entries;";
    ASSERT_TRUE(ExecSqliteScript(dbPath, setupSql.c_str()));
    auto* maintainer = dynamic_cast<sc::IQueryIndexMaintainer*>(db.Get());
    ASSERT_NE(maintainer, nullptr);
    EXPECT_TRUE(sc::Failed(maintainer->RebuildQueryIndex()));
    EXPECT_FALSE(QuerySqliteExists(dbPath,
        ("SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = '" + widthIndex + "';").c_str()));
    std::int64_t entryCount = -1;
    ASSERT_TRUE(QuerySqliteInt64(dbPath, "SELECT COUNT(*) FROM query_index_entries;", &entryCount));
    EXPECT_EQ(entryCount, 0);

    ASSERT_TRUE(ExecSqliteScript(dbPath, ("DROP TABLE " + nameIndex + ";").c_str()));
    EXPECT_EQ(maintainer->RebuildQueryIndex(), sc::SC_OK);
    EXPECT_TRUE(QuerySqliteExists(dbPath,
        ("SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = '" + widthIndex + "';").c_str()));
    EXPECT_TRUE(QuerySqliteExists(dbPath,
        ("SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = '" + nameIndex + "';").c_str()));
    const auto* provider = dynamic_cast<const sc::IQueryIndexProvider*>(db.Get());
    ASSERT_NE(provider, nullptr);
    sc::QueryIndexCheckResult check;
    ASSERT_EQ(provider->CheckQueryIndex(&check), sc::SC_OK);
    EXPECT_EQ(check.state, sc::QueryIndexHealthState::Healthy);
}

TEST(SchemaEdit, QueryIndexRepairRejectsReadOnlyDatabase)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QueryIndexRepairReadOnly.sqlite");
    {
        sc::SCDbPtr db;
        ASSERT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
        CreateBeamTable(db);
    }
    sc::SCDbPtr readOnly;
    ASSERT_EQ(CreateReadOnlyFileDb(dbPath.c_str(), readOnly), sc::SC_OK);
    auto* maintainer = dynamic_cast<sc::IQueryIndexMaintainer*>(readOnly.Get());
    ASSERT_NE(maintainer, nullptr);
    EXPECT_EQ(maintainer->RebuildQueryIndex(), sc::SC_E_READ_ONLY_DATABASE);
}

TEST(SchemaEdit, StorageHealthReportIncludesQueryIndexDiagnosticsWhenEntriesDrift)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_QueryIndexDiagnostics.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
    EXPECT_EQ(schema->AddIndex(MakeCompositeIndex(L"idx_Beam_Width_Name", L"Width", L"Name")), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed query index diagnostics", seedEdit), sc::SC_OK);
    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    EXPECT_TRUE(ExecSqliteScript(dbPath, "DELETE FROM query_index_entries;"));

    sc::SCStorageHealthReport report;
    EXPECT_EQ(sc::BuildStorageHealthReport(db.Get(), L"sqlite", &report), sc::SC_OK);

    const auto it = std::find_if(report.diagnostics.begin(),
                                 report.diagnostics.end(),
                                 [](const sc::SCDiagnosticEntry& entry) {
                                     return entry.category == L"query-index" &&
                                            entry.severity == sc::SCDiagnosticSeverity::Error &&
                                            entry.message.find(L"query-index-entry-missing") != std::wstring::npos;
                                 });
    EXPECT_NE(it, report.diagnostics.end());
}

TEST(SchemaEdit, QueryIndexProviderReportsOutOfDateDuringActiveEditAndRejectsRepair)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_QueryIndexActiveEdit.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
    EXPECT_EQ(schema->AddIndex(MakeCompositeIndex(L"idx_Beam_Width_Name", L"Width", L"Name")), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed active edit query index", seedEdit), sc::SC_OK);
    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    const auto* provider = dynamic_cast<const sc::IQueryIndexProvider*>(db.Get());
    ASSERT_NE(provider, nullptr);
    auto* maintainer = dynamic_cast<sc::IQueryIndexMaintainer*>(db.Get());
    ASSERT_NE(maintainer, nullptr);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"dirty query index", edit), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);

    sc::QueryIndexCheckResult check;
    EXPECT_EQ(provider->CheckQueryIndex(&check), sc::SC_OK);
    EXPECT_EQ(check.state, sc::QueryIndexHealthState::OutOfDate);
    EXPECT_EQ(check.message, L"query-index-rebuild-required");
    EXPECT_EQ(maintainer->RebuildQueryIndex(), sc::SC_E_WRITE_CONFLICT);

    EXPECT_EQ(db->Rollback(edit.Get()), sc::SC_OK);
}
