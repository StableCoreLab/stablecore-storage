#include "TestSqliteHelpers.h"
#include "TestPaths.h"

#include <sqlite3.h>
#include <string>

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
        if (stmt != nullptr)
        {
            sqlite3_finalize(stmt);
        }
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

bool SetMetadataValue(const fs::path& dbPath, const char* key, const char* value)
{
    const std::string sql = std::string("UPDATE metadata SET value='") + value + "' WHERE key='" + key + "';";
    return ExecSqliteScript(dbPath, sql.c_str());
}

bool SqliteTableHasColumn(const fs::path& dbPath, const char* tableName, const char* columnName)
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

    const std::string pragmaSql = std::string("PRAGMA table_info(") + tableName + ");";
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, pragmaSql.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
    {
        sqlite3_close(db);
        return false;
    }

    bool found = false;
    while (sqlite3_step(stmt) == SQLITE_ROW)
    {
        const unsigned char* name = sqlite3_column_text(stmt, 1);
        if (name != nullptr && std::strcmp(reinterpret_cast<const char*>(name), columnName) == 0)
        {
            found = true;
            break;
        }
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return found;
}
