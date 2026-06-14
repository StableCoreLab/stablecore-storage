#pragma once

#include <filesystem>
#include <cstring>

#include "SCStorage.h"

namespace sc = StableCore::Storage;
namespace fs = std::filesystem;

// Create a file-based database in read-write mode
sc::ErrorCode CreateFileDb(const wchar_t* path, sc::SCDbPtr& db);

// Create a file-based database in read-only mode
sc::ErrorCode CreateReadOnlyFileDb(const wchar_t* path, sc::SCDbPtr& db);

// Execute raw SQLite SQL script directly via sqlite3 API
bool ExecSqliteScript(const fs::path& dbPath, const char* sql);

// Query a single int64 value from SQLite database
bool QuerySqliteInt64(const fs::path& dbPath, const char* sql, std::int64_t* outValue);

// Check if a query result exists (non-zero value)
bool QuerySqliteExists(const fs::path& dbPath, const char* sql);

// Update a metadata key-value pair in the database
bool SetMetadataValue(const fs::path& dbPath, const char* key, const char* value);

// Check if a table has a specific column
bool SqliteTableHasColumn(const fs::path& dbPath, const char* tableName, const char* columnName);
