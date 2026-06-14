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