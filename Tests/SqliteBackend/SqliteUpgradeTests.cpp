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

    sc::ErrorCode CreateReadOnlyFileDb(const wchar_t* path, sc::SCDbPtr& db)
    {
        sc::SCOpenDatabaseOptions options;
        options.openMode = sc::SCDatabaseOpenMode::ReadOnly;
        return sc::CreateFileDatabase(path, options, db);
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

    bool SetMetadataValue(const fs::path& dbPath, const char* key, const char* value)
    {
        const std::string sql = std::string("UPDATE metadata SET value='") + value + "' WHERE key='" + key + "';";
        return ExecSqliteScript(dbPath, sql.c_str());
    }
}

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
