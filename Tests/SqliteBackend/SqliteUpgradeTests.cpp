#include <filesystem>
#include <string>

#include <gtest/gtest.h>
#include <sqlite3.h>

#include "SCStorage.h"

#include "Support/TestPaths.h"
#include "Support/TestSqliteHelpers.h"

namespace sc = StableCore::Storage;
namespace fs = std::filesystem;

// 改写自 VersionGraphReportsUpgradeWindow
// 新语义：低于发布基线的版本应被明确拒绝，不进入 upgrade required
TEST(SqliteUpgrade, UnsupportedVersionIsRejectedByOpenPolicy)
{
    sc::SCVersionGraph graph;
    EXPECT_EQ(sc::BuildDefaultVersionGraph(&graph), sc::SC_OK);
    EXPECT_EQ(graph.latestSupportedVersion, 6);
    EXPECT_FALSE(graph.nodes.empty());
    EXPECT_TRUE(graph.edges.empty());

    // 基线版本（最新支持版本）应该可以正常读写打开
    sc::SCOpenDecision baselineDecision;
    EXPECT_EQ(sc::EvaluateOpenDecision(graph, graph.latestSupportedVersion, true, &baselineDecision), sc::SC_OK);
    EXPECT_EQ(baselineDecision.mode, sc::SCOpenMode::ReadWrite);
    EXPECT_FALSE(baselineDecision.needsUpgrade);
    EXPECT_TRUE(baselineDecision.writable);
    EXPECT_FALSE(baselineDecision.readOnlyOnly);

    // 远低于基线的旧版本应该被明确拒绝
    sc::SCOpenDecision oldVersionDecision;
    EXPECT_EQ(sc::EvaluateOpenDecision(graph, 1, true, &oldVersionDecision), sc::SC_OK);
    // 旧版本应被拒绝，不允许 writable open
    EXPECT_EQ(oldVersionDecision.mode, sc::SCOpenMode::UpgradeRequired);
    EXPECT_TRUE(oldVersionDecision.needsUpgrade);
    EXPECT_FALSE(oldVersionDecision.writable);

    // 版本 2 也应该被拒绝（如果低于基线）
    sc::SCOpenDecision v2Decision;
    EXPECT_EQ(sc::EvaluateOpenDecision(graph, 2, true, &v2Decision), sc::SC_OK);
    EXPECT_EQ(v2Decision.mode, sc::SCOpenMode::UpgradeRequired);
    EXPECT_TRUE(v2Decision.needsUpgrade);
    EXPECT_FALSE(v2Decision.writable);
}

// 迁移了 ReadOnlySqliteOpenRejectsWrites 到 SqliteReadOnlyOpenTests.cpp

// 补回真实 open path 的旧库安全拒绝测试
// 测试低于基线版本的数据库在真实打开时被拒绝且不改库
TEST(SqliteUpgrade, RealOpenPathRejectsUnsupportedSchemaVersionWithoutModifyingDatabase)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_OldVersionReject.sqlite");

    // 先创建一个有效的数据库
    sc::SCDbPtr seedDb;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, seedDb), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(seedDb->CreateTable(L"TestTable", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef col;
    col.name = L"Value";
    col.valueKind = sc::ValueKind::Int64;
    col.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(col), sc::SC_OK);

    seedDb.Reset();

    // 直接修改 metadata 表中的版本号为低版本
    EXPECT_TRUE(SetMetadataValue(dbPath, "schema_version", "1"));
    const auto fileSizeBeforeOpen = fs::file_size(dbPath);

    // 记录修改前的版本号
    std::int64_t schemaVersionBeforeOpen = 0;
    EXPECT_TRUE(
        QuerySqliteInt64(dbPath, "SELECT value FROM metadata WHERE key='schema_version'", &schemaVersionBeforeOpen));
    EXPECT_EQ(schemaVersionBeforeOpen, 1);

    // 尝试打开这个低版本数据库
    sc::SCDbPtr reopenDb;
    sc::SCOpenDatabaseOptions options;
    options.openMode = sc::SCDatabaseOpenMode::Normal;
    const sc::ErrorCode openRc = sc::CreateFileDatabase(dbPath.c_str(), options, reopenDb);

    // 应该被拒绝（返回错误码）
    EXPECT_NE(openRc, sc::SC_OK);

    // 验证版本号没有被修改
    std::int64_t schemaVersionAfterOpen = 0;
    EXPECT_TRUE(
        QuerySqliteInt64(dbPath, "SELECT value FROM metadata WHERE key='schema_version'", &schemaVersionAfterOpen));
    EXPECT_EQ(schemaVersionAfterOpen, schemaVersionBeforeOpen);

    // 验证数据库文件仍然存在
    EXPECT_TRUE(fs::exists(dbPath));
    const auto fileSizeAfterOpen = fs::file_size(dbPath);
    EXPECT_EQ(fileSizeAfterOpen, fileSizeBeforeOpen);

    // 验证表结构没有被破坏
    std::int64_t tableCount = 0;
    EXPECT_TRUE(QuerySqliteInt64(dbPath, "SELECT COUNT(*) FROM tables WHERE name='TestTable'", &tableCount));
    EXPECT_GT(tableCount, 0);
}

TEST(SqliteUpgrade, ExplicitUpgradeEntryAppliesRegisteredRelationUpgrade)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_ExplicitUpgrade.sqlite");

    sc::SCDbPtr seedDb;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, seedDb), sc::SC_OK);

    seedDb.Reset();

    EXPECT_TRUE(SetMetadataValue(dbPath, "schema_version", "5"));

    sc::SCDbPtr upgradedDb;
    sc::SCUpgradeResult upgradeResult;
    EXPECT_EQ(sc::UpgradeFileDatabase(dbPath.c_str(), upgradedDb, &upgradeResult), sc::SC_OK);
    EXPECT_EQ(upgradeResult.status, sc::SCUpgradeStatus::Success);
    EXPECT_EQ(upgradeResult.sourceVersion, 5);
    EXPECT_EQ(upgradeResult.targetVersion, 6);
    ASSERT_TRUE(upgradedDb);
    EXPECT_EQ(upgradedDb->GetSchemaVersion(), 6);

    upgradedDb.Reset();

    sc::SCDbPtr reopenedDb;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, reopenedDb), sc::SC_OK);
    ASSERT_TRUE(reopenedDb);
    EXPECT_EQ(reopenedDb->GetSchemaVersion(), 6);
}

TEST(SqliteUpgrade, CreateFileDatabaseAutoUpgradesRegisteredRelationVersion)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_CreateAutoUpgrade.sqlite");

    sc::SCDbPtr seedDb;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, seedDb), sc::SC_OK);
    seedDb.Reset();

    EXPECT_TRUE(SetMetadataValue(dbPath, "schema_version", "5"));

    sc::SCDbPtr reopenedDb;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, reopenedDb), sc::SC_OK);
    ASSERT_TRUE(reopenedDb);
    EXPECT_EQ(reopenedDb->GetSchemaVersion(), 6);

    std::int64_t schemaVersionAfterOpen = 0;
    EXPECT_TRUE(
    QuerySqliteInt64(dbPath, "SELECT value FROM metadata WHERE key='schema_version'", &schemaVersionAfterOpen));
    EXPECT_EQ(schemaVersionAfterOpen, 6);
}

TEST(SqliteUpgrade, MissingJournalTablesAreReportedExplicitly)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_MissingJournalTables.sqlite");

    sc::SCDbPtr seedDb;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, seedDb), sc::SC_OK);
    seedDb.Reset();

    EXPECT_TRUE(ExecSqliteScript(dbPath, "DROP TABLE journal_entries;"));
    EXPECT_TRUE(ExecSqliteScript(dbPath, "DROP TABLE journal_schema_entries;"));
    EXPECT_TRUE(ExecSqliteScript(dbPath, "DROP TABLE journal_transactions;"));
    EXPECT_TRUE(SetMetadataValue(dbPath, "schema_version", "5"));

    sc::SCDbPtr reopenedDb;
    const sc::ErrorCode openRc = sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, reopenedDb);
    EXPECT_EQ(openRc, sc::SC_E_JOURNAL_TABLE_MISSING);
    EXPECT_FALSE(reopenedDb);
}

TEST(SqliteUpgrade, MissingUpgradePathIsReportedExplicitly)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_MissingUpgradePath.sqlite");

    sc::SCDbPtr seedDb;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, seedDb), sc::SC_OK);
    seedDb.Reset();

    EXPECT_TRUE(SetMetadataValue(dbPath, "schema_version", "1"));

    sc::SCDbPtr reopenedDb;
    const sc::ErrorCode openRc = sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, reopenedDb);
    EXPECT_EQ(openRc, sc::SC_E_UPGRADE_PATH_NOT_FOUND);
    EXPECT_FALSE(reopenedDb);
}

TEST(SqliteUpgrade, SchemaVersionFiveWithLegacyJournalSchemaOpensAndUpgrades)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_LegacyJournalSchema.sqlite");

    sc::SCDbPtr seedDb;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, seedDb), sc::SC_OK);
    seedDb.Reset();

    EXPECT_TRUE(ExecSqliteScript(
        dbPath,
        "DROP TABLE journal_schema_entries;"
        "CREATE TABLE journal_schema_entries ("
        "tx_id INTEGER NOT NULL, sequence_index INTEGER NOT NULL, op INTEGER NOT NULL, "
        "table_name TEXT NOT NULL, column_name TEXT NOT NULL, column_rowid INTEGER NOT NULL, "
        "old_display_name TEXT, old_value_kind INTEGER, old_column_kind INTEGER, old_nullable INTEGER, "
        "old_editable INTEGER, old_user_defined INTEGER, old_indexed INTEGER, "
        "old_participates_in_calc INTEGER, old_unit TEXT, old_reference_table TEXT, "
        "old_default_kind INTEGER, old_default_int64 INTEGER, old_default_double REAL, "
        "old_default_bool INTEGER, old_default_text TEXT, old_default_blob BLOB, "
        "new_display_name TEXT, new_value_kind INTEGER, new_column_kind INTEGER, new_nullable INTEGER, "
        "new_editable INTEGER, new_user_defined INTEGER, new_indexed INTEGER, "
        "new_participates_in_calc INTEGER, new_unit TEXT, new_reference_table TEXT, "
        "new_default_kind INTEGER, new_default_int64 INTEGER, new_default_double REAL, "
        "new_default_bool INTEGER, new_default_text TEXT, new_default_blob BLOB, "
        "PRIMARY KEY(tx_id, sequence_index));"));
    EXPECT_TRUE(SetMetadataValue(dbPath, "schema_version", "5"));

    sc::SCDbPtr reopenedDb;
    const sc::ErrorCode openRc = sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, reopenedDb);
    EXPECT_EQ(openRc, sc::SC_OK);
    ASSERT_TRUE(reopenedDb);
    EXPECT_EQ(reopenedDb->GetSchemaVersion(), 6);
    EXPECT_TRUE(SqliteTableHasColumn(dbPath, "journal_schema_entries", "old_reference_storage_column"));
    EXPECT_TRUE(SqliteTableHasColumn(dbPath, "journal_schema_entries", "new_reference_storage_column"));
}
