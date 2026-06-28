#include "Sqlite/SqliteImplCore.h"
#include "Sqlite/SqliteImplDatabase.h"

namespace StableCore::Storage
{
        SqliteDatabase::SqliteDatabase(const std::wstring& path) : SqliteDatabase(path, false)
        {
        }

        SqliteDatabase::SqliteDatabase(const std::wstring& path, bool readOnly)
            : path_(path),
              db_(path, readOnly),
              openMode_(readOnly ? SCDatabaseOpenMode::ReadOnly : SCDatabaseOpenMode::Normal),
              readOnly_(readOnly)
        {
            const bool hasMetadataTable = HasTable(L"metadata");
            if (!hasMetadataTable)
            {
                if (readOnly_)
                {
                    throw std::runtime_error("metadata table is missing in read-only open");
                }

                InitializeSchema();
            }

            LoadMetadata();
            LoadTables();

            if (!readOnly_)
            {
                if (schemaVersion_ <= 0)
                {
                    schemaVersion_ = GetLatestSupportedSchemaVersion();
                    SaveMetadata();
                }
            }

            LoadJournalStacks();
        }

        SqliteDatabase::SqliteDatabase(const std::wstring& path, const SCOpenDatabaseOptions& options)
            : path_(path),
              db_(path, options.openMode == SCDatabaseOpenMode::ReadOnly),
              openMode_(options.openMode),
              readOnly_(options.openMode == SCDatabaseOpenMode::ReadOnly)
        {
            const bool hasMetadataTable = HasTable(L"metadata");
            if (!hasMetadataTable)
            {
                if (readOnly_)
                {
                    throw std::runtime_error("metadata table is missing in read-only open");
                }

                InitializeSchema();
            }

            LoadMetadata();
            LoadTables();

            if (!readOnly_ && schemaVersion_ <= 0)
            {
                schemaVersion_ = GetLatestSupportedSchemaVersion();
                SaveMetadata();
            }

            LoadJournalStacks();
        }

        SqliteDatabase::~SqliteDatabase()
        {
            try
            {
                if (!readOnly_ && persistCleanShutdownOnDestroy_)
                {
                    SetCleanShutdownFlag(true);
                }
            } catch (...)
            {
            }
        }

        void SqliteDatabase::InitializeSchema()
        {
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, "
                "value TEXT NOT NULL);");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS startup_diagnostics ("
                "diag_id INTEGER PRIMARY KEY AUTOINCREMENT, severity INTEGER "
                "NOT NULL, category TEXT NOT NULL, message TEXT NOT NULL);");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS tables (table_id INTEGER PRIMARY "
                "KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE);");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS schema_columns ("
                "table_id INTEGER NOT NULL, column_name TEXT NOT NULL, "
                "display_name TEXT NOT NULL, value_kind INTEGER NOT NULL,"
                "column_kind INTEGER NOT NULL, nullable_flag INTEGER NOT NULL, "
                "editable_flag INTEGER NOT NULL,"
                "user_defined_flag INTEGER NOT NULL, indexed_flag INTEGER NOT "
                "NULL, participates_in_calc_flag INTEGER NOT NULL,"
                "unit TEXT NOT NULL, reference_table TEXT NOT NULL, "
                "reference_storage_column TEXT NOT NULL, "
                "reference_display_column TEXT NOT NULL, "
                "default_kind INTEGER NOT NULL, default_int64 INTEGER,"
                "default_double REAL, default_bool INTEGER, default_text TEXT, "
                "default_blob BLOB, "
                "PRIMARY KEY(table_id, column_name));");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS records ("
                "table_id INTEGER NOT NULL, record_id INTEGER NOT NULL, state "
                "INTEGER NOT NULL, last_modified_version INTEGER NOT NULL,"
                "PRIMARY KEY(table_id, record_id));");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS field_values ("
                "table_id INTEGER NOT NULL, record_id INTEGER NOT NULL, "
                "column_name TEXT NOT NULL, value_kind INTEGER NOT NULL,"
                "int64_value INTEGER, double_value REAL, bool_value INTEGER, "
                "text_value TEXT, blob_value BLOB,"
                "PRIMARY KEY(table_id, record_id, column_name));");
            db_.Execute(
                "CREATE INDEX IF NOT EXISTS idx_records_table_state ON "
                "records(table_id, state);");
            db_.Execute(
                "CREATE INDEX IF NOT EXISTS idx_field_values_lookup ON "
                "field_values(table_id, column_name, value_kind, int64_value, "
                "text_value);");
            db_.Execute(
                "CREATE INDEX IF NOT EXISTS idx_field_values_record ON "
                "field_values(table_id, record_id, column_name);");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS journal_transactions ("
                "tx_id INTEGER PRIMARY KEY AUTOINCREMENT, action_name TEXT NOT "
                "NULL, committed_version INTEGER NOT NULL,"
                " stack_kind INTEGER NOT NULL, stack_order INTEGER NOT NULL);");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS journal_entries ("
                "tx_id INTEGER NOT NULL, sequence_index INTEGER NOT NULL, op "
                "INTEGER NOT NULL, table_name TEXT NOT NULL,"
                "record_id INTEGER NOT NULL, field_name TEXT NOT NULL, "
                "old_kind INTEGER NOT NULL, old_int64 INTEGER,"
                "old_double REAL, old_bool INTEGER, old_text TEXT, old_blob BLOB, new_kind "
                "INTEGER NOT NULL, new_int64 INTEGER,"
                "new_double REAL, new_bool INTEGER, new_text TEXT, new_blob BLOB, old_deleted "
                "INTEGER NOT NULL, new_deleted INTEGER NOT NULL,"
                "PRIMARY KEY(tx_id, sequence_index));");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS journal_schema_entries ("
                "tx_id INTEGER NOT NULL, sequence_index INTEGER NOT NULL, op "
                "INTEGER NOT NULL, table_name TEXT NOT NULL, column_name TEXT "
                "NOT NULL, column_rowid INTEGER NOT NULL, old_display_name TEXT, old_value_kind INTEGER, "
                "old_column_kind INTEGER, old_nullable INTEGER, old_editable "
                "INTEGER, old_user_defined INTEGER, old_indexed INTEGER, "
                "old_participates_in_calc INTEGER, old_unit TEXT, "
                "old_reference_table TEXT, old_reference_storage_column TEXT, "
                "old_reference_display_column TEXT, old_default_kind INTEGER, "
                "old_default_int64 INTEGER, old_default_double REAL, "
                "old_default_bool INTEGER, old_default_text TEXT, "
                "old_default_blob BLOB, "
                "new_display_name TEXT, new_value_kind INTEGER, "
                "new_column_kind INTEGER, new_nullable INTEGER, "
                "new_editable INTEGER, new_user_defined INTEGER, "
                "new_indexed INTEGER, new_participates_in_calc INTEGER, "
                "new_unit TEXT, new_reference_table TEXT, "
                "new_reference_storage_column TEXT, new_reference_display_column TEXT, "
                "new_default_kind INTEGER, new_default_int64 INTEGER, new_default_double REAL, "
                "new_default_bool INTEGER, new_default_text TEXT, "
                "new_default_blob BLOB, "
                "PRIMARY KEY(tx_id, sequence_index));");
            EnsureSchemaMetadataTables();
        }

        void SqliteDatabase::LoadMetadata()
        {
            version_ = 0;
            baselineVersion_ = 0;
            nextRecordId_ = 1;
            schemaVersion_ = 0;
            cleanShutdown_ = true;
            bool hasBaselineVersion = false;

            SqliteStmt stmt = db_.Prepare("SELECT key, value FROM metadata;");
            bool hasRow = false;
            while (stmt.Step(&hasRow) == SC_OK && hasRow)
            {
                const std::wstring key = stmt.ColumnText(0);
                const std::wstring SCValue = stmt.ColumnText(1);
                if (key == L"version")
                {
                    version_ = static_cast<VersionId>(std::stoull(SCValue));
                } else if (key == L"baseline_version")
                {
                    baselineVersion_ = static_cast<VersionId>(std::stoull(SCValue));
                    hasBaselineVersion = true;
                } else if (key == L"schema_version")
                {
                    schemaVersion_ = static_cast<std::int32_t>(std::stoi(SCValue));
                } else if (key == L"clean_shutdown")
                {
                    cleanShutdown_ = (SCValue == L"1");
                } else if (key == L"next_record_id")
                {
                    nextRecordId_ = static_cast<RecordId>(std::stoll(SCValue));
                }
            }

            if (!hasBaselineVersion)
            {
                baselineVersion_ = version_;
            }
        }

        void SqliteDatabase::SaveMetadata(VersionId version, VersionId baselineVersion)
        {
            SaveMetadataKey(L"version", std::to_wstring(version));
            SaveMetadataKey(L"baseline_version", std::to_wstring(baselineVersion));
            SaveMetadataKey(L"next_record_id", std::to_wstring(nextRecordId_));
            SaveMetadataKey(L"schema_version", std::to_wstring(schemaVersion_));
            SaveMetadataKey(L"clean_shutdown", cleanShutdown_ ? L"1" : L"0");
        }

        void SqliteDatabase::SaveMetadata(VersionId version)
        {
            SaveMetadata(version, baselineVersion_);
        }

        void SqliteDatabase::SaveMetadata()
        {
            SaveMetadata(version_);
        }

        void SqliteDatabase::SaveMetadataKey(const wchar_t* key, const std::wstring& value)
        {
            SqliteStmt stmt = db_.Prepare(
                "INSERT INTO metadata(key, value) VALUES(?, ?)"
                " ON CONFLICT(key) DO UPDATE SET value=excluded.value;");
            stmt.BindText(1, key != nullptr ? key : L"");
            stmt.BindText(2, value);
            stmt.Step();
        }

        bool SqliteDatabase::HasTable(const wchar_t* name)
        {
            if (name == nullptr || *name == L'\0')
            {
                return false;
            }

            SqliteStmt stmt = db_.Prepare(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? "
                "LIMIT 1;");
            stmt.BindText(1, name);
            bool hasRow = false;
            return stmt.Step(&hasRow) == SC_OK && hasRow;
        }

        bool HasTableColumnRaw(sqlite3* db, const char* tableName, const char* columnName)
        {
            if (db == nullptr || tableName == nullptr || columnName == nullptr)
            {
                return false;
            }

            std::string sql = std::string("PRAGMA table_info(") + tableName + ");";
            sqlite3_stmt* stmt = nullptr;
            if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
            {
                if (stmt != nullptr)
                {
                    sqlite3_finalize(stmt);
                }
                return false;
            }

            bool hasColumn = false;
            while (sqlite3_step(stmt) == SQLITE_ROW)
            {
                const auto* name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
                if (name != nullptr && std::strcmp(name, columnName) == 0)
                {
                    hasColumn = true;
                    break;
                }
            }

            sqlite3_finalize(stmt);
            return hasColumn;
        }

        void SqliteDatabase::EnsureSchemaMetadataTables()
        {
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS schema_tables ("
                "table_id INTEGER PRIMARY KEY, description TEXT NOT NULL "
                "DEFAULT '');");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS schema_constraints ("
                "constraint_id INTEGER PRIMARY KEY AUTOINCREMENT, table_id "
                "INTEGER NOT NULL, kind INTEGER NOT NULL, name TEXT NOT NULL, "
                "source_kind INTEGER NOT NULL, referenced_table TEXT NOT NULL, "
                "on_delete_action INTEGER NOT NULL DEFAULT 0, "
                "on_update_action INTEGER NOT NULL DEFAULT 0, "
                "check_expression TEXT NOT NULL DEFAULT '');");
            db_.Execute(
                "CREATE INDEX IF NOT EXISTS idx_schema_constraints_table ON "
                "schema_constraints(table_id);");
            db_.Execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_schema_constraints_table_name ON "
                "schema_constraints(table_id, name);");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS schema_constraint_columns ("
                "constraint_id INTEGER NOT NULL, column_ordinal INTEGER NOT "
                "NULL, column_name TEXT NOT NULL, referenced_column_name TEXT "
                "NOT NULL DEFAULT '', PRIMARY KEY(constraint_id, "
                "column_ordinal));");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS schema_indexes ("
                "index_id INTEGER PRIMARY KEY AUTOINCREMENT, table_id INTEGER "
                "NOT NULL, name TEXT NOT NULL, source_kind INTEGER NOT NULL);");
            db_.Execute(
                "CREATE INDEX IF NOT EXISTS idx_schema_indexes_table ON "
                "schema_indexes(table_id);");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS schema_index_columns ("
                "index_id INTEGER NOT NULL, column_ordinal INTEGER NOT NULL, "
                "column_name TEXT NOT NULL, descending_flag INTEGER NOT NULL, "
                "PRIMARY KEY(index_id, column_ordinal));");
            InitializeQueryIndexStorage();
        }

        void SqliteDatabase::LoadTables()
        {
            const bool supportsBinaryStorage = schemaVersion_ >= 4;
            const bool supportsRelationReferenceColumns = schemaVersion_ >= 6;
            SqliteStmt tablesStmt = db_.Prepare("SELECT table_id, name FROM tables ORDER BY name;");
            bool hasRow = false;
            while (tablesStmt.Step(&hasRow) == SC_OK && hasRow)
            {
                const std::int64_t tableRowId = tablesStmt.ColumnInt64(0);
                const std::wstring tableName = tablesStmt.ColumnText(1);
                SCTablePtr table = SCMakeRef<SqliteTable>(this, tableName, tableRowId);
                tables_.emplace(tableName, table);
                auto* sqliteTable = static_cast<SqliteTable*>(table.Get());

                SqliteStmt columnsStmt =
                    db_.Prepare(
                        supportsBinaryStorage
                            ? (supportsRelationReferenceColumns
                                   ? "SELECT rowid, column_name, display_name, "
                                     "value_kind, column_kind, nullable_flag, "
                                     "editable_flag, user_defined_flag, indexed_flag, "
                                     "participates_in_calc_flag, unit, reference_table, "
                                     "reference_storage_column, reference_display_column, "
                                     "default_kind, default_int64, default_double, "
                                     "default_bool, default_text, default_blob FROM "
                                     "schema_columns WHERE table_id = ? ORDER BY rowid;"
                                   : "SELECT rowid, column_name, display_name, "
                                     "value_kind, column_kind, nullable_flag, "
                                     "editable_flag, user_defined_flag, indexed_flag, "
                                     "participates_in_calc_flag, unit, reference_table, "
                                     "'' AS reference_storage_column, '' AS reference_display_column, "
                                     "default_kind, default_int64, default_double, "
                                     "default_bool, default_text, default_blob FROM "
                                     "schema_columns WHERE table_id = ? ORDER BY rowid;")
                            : (supportsRelationReferenceColumns
                                   ? "SELECT rowid, column_name, display_name, "
                                     "value_kind, column_kind, nullable_flag, "
                                     "editable_flag, user_defined_flag, indexed_flag, "
                                     "participates_in_calc_flag, unit, reference_table, "
                                     "reference_storage_column, reference_display_column, "
                                     "default_kind, default_int64, default_double, "
                                     "default_bool, default_text FROM schema_columns "
                                     "WHERE table_id = ? ORDER BY rowid;"
                                   : "SELECT rowid, column_name, display_name, "
                                     "value_kind, column_kind, nullable_flag, "
                                     "editable_flag, user_defined_flag, indexed_flag, "
                                     "participates_in_calc_flag, unit, reference_table, "
                                     "'' AS reference_storage_column, '' AS reference_display_column, "
                                     "default_kind, default_int64, default_double, "
                                     "default_bool, default_text FROM schema_columns "
                                     "WHERE table_id = ? ORDER BY rowid;"));
                columnsStmt.BindInt64(1, tableRowId);
                bool hasColumn = false;
                while (columnsStmt.Step(&hasColumn) == SC_OK && hasColumn)
                {
                    SCColumnDef def;
                    const std::int64_t rowId = columnsStmt.ColumnInt64(0);
                    def.name = columnsStmt.ColumnText(1);
                    def.displayName = columnsStmt.ColumnText(2);
                    def.valueKind = FromSqliteValueKind(columnsStmt.ColumnInt(3));
                    def.columnKind = FromSqliteColumnKind(columnsStmt.ColumnInt(4));
                    def.nullable = columnsStmt.ColumnBool(5);
                    def.editable = columnsStmt.ColumnBool(6);
                    def.userDefined = columnsStmt.ColumnBool(7);
                    def.indexed = columnsStmt.ColumnBool(8);
                    def.participatesInCalc = columnsStmt.ColumnBool(9);
                    def.unit = columnsStmt.ColumnText(10);
                    def.referenceTable = columnsStmt.ColumnText(11);
                    def.referenceStorageColumn = columnsStmt.ColumnText(12);
                    def.referenceDisplayColumn = columnsStmt.ColumnText(13);
                    def.defaultValue = supportsBinaryStorage
                                           ? StorageCodec::ReadValue(columnsStmt, 14, 15, 16, 17, 18, 19)
                                           : StorageCodec::ReadValue(columnsStmt, 14, 15, 16, 17, 18, 18);
                    sqliteTable->Schema()->LoadColumn(def, rowId);
                }
                const ErrorCode metadataRc = LoadSchemaMetadata(sqliteTable);
                if (Failed(metadataRc))
                {
                    throw std::runtime_error("failed to load schema metadata");
                }

                SqliteStmt recordsStmt = db_.Prepare(
                    "SELECT record_id, state, last_modified_version FROM "
                    "records WHERE table_id = ?;");
                recordsStmt.BindInt64(1, tableRowId);
                bool hasRecord = false;
                while (recordsStmt.Step(&hasRecord) == SC_OK && hasRecord)
                {
                    auto record = std::make_shared<SqliteRecordData>(recordsStmt.ColumnInt64(0));
                    record->state = FromSqliteRecordState(recordsStmt.ColumnInt(1));
                    record->lastModifiedVersion = static_cast<VersionId>(recordsStmt.ColumnInt64(2));
                    sqliteTable->Records().emplace(record->id, record);
                    if (record->id >= nextRecordId_)
                    {
                        nextRecordId_ = record->id + 1;
                    }
                }

                SqliteStmt valuesStmt =
                    db_.Prepare(supportsBinaryStorage ? "SELECT record_id, column_name, value_kind, "
                                                        "int64_value, double_value, bool_value, text_value, "
                                                        "blob_value FROM field_values WHERE table_id = ?;"
                                                      : "SELECT record_id, column_name, value_kind, "
                                                        "int64_value, double_value, bool_value, text_value "
                                                        "FROM field_values WHERE table_id = ?;");
                valuesStmt.BindInt64(1, tableRowId);
                bool hasValue = false;
                while (valuesStmt.Step(&hasValue) == SC_OK && hasValue)
                {
                    auto record = sqliteTable->FindRecordData(valuesStmt.ColumnInt64(0));
                    if (!record)
                    {
                        continue;
                    }
                    record->values[valuesStmt.ColumnText(1)] = supportsBinaryStorage
                                                                   ? StorageCodec::ReadValue(valuesStmt, 2, 3, 4, 5, 6, 7)
                                                                   : StorageCodec::ReadValue(valuesStmt, 2, 3, 4, 5, 6, 6);
                }
            }
        }

        ErrorCode SqliteDatabase::LoadSchemaMetadata(SqliteTable* table)
        {
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }

            auto* schema = table->Schema();
            if (schema == nullptr)
            {
                return SC_E_FAIL;
            }

            const std::int64_t tableRowId = table->TableRowId();

            if (HasTable(L"schema_tables"))
            {
                SqliteStmt tableMetaStmt = db_.Prepare("SELECT description FROM schema_tables WHERE table_id = ?;");
                tableMetaStmt.BindInt64(1, tableRowId);
                bool hasMetaRow = false;
                if (tableMetaStmt.Step(&hasMetaRow) == SC_OK && hasMetaRow)
                {
                    schema->LoadTableDescription(tableMetaStmt.ColumnText(0));
                }
            }

            if (HasTable(L"schema_constraints") && HasTable(L"schema_constraint_columns"))
            {
                const bool supportsForeignKeyActions = schemaVersion_ >= 7;
                SqliteStmt constraintsStmt = db_.Prepare(
                    supportsForeignKeyActions
                        ? "SELECT constraint_id, kind, name, source_kind, "
                          "referenced_table, on_delete_action, on_update_action, "
                          "check_expression FROM schema_constraints WHERE table_id = ? ORDER BY "
                          "constraint_id;"
                        : "SELECT constraint_id, kind, name, source_kind, "
                          "referenced_table, 0 AS on_delete_action, 0 AS on_update_action, "
                          "check_expression FROM schema_constraints WHERE table_id = ? ORDER BY "
                          "constraint_id;");
                constraintsStmt.BindInt64(1, tableRowId);
                bool hasConstraint = false;
                while (constraintsStmt.Step(&hasConstraint) == SC_OK && hasConstraint)
                {
                    SCConstraintDef constraint;
                    const std::int64_t constraintId = constraintsStmt.ColumnInt64(0);
                    constraint.kind = FromSqliteConstraintKind(constraintsStmt.ColumnInt(1));
                    constraint.name = constraintsStmt.ColumnText(2);
                    constraint.sourceKind = FromSqliteSchemaSourceKind(constraintsStmt.ColumnInt(3));
                    constraint.referencedTable = constraintsStmt.ColumnText(4);
                    constraint.onDelete = FromSqliteForeignKeyAction(constraintsStmt.ColumnInt(5));
                    constraint.onUpdate = FromSqliteForeignKeyAction(constraintsStmt.ColumnInt(6));
                    constraint.checkExpression = constraintsStmt.ColumnText(7);

                    SqliteStmt constraintColumnsStmt = db_.Prepare(
                        "SELECT column_name, referenced_column_name FROM "
                        "schema_constraint_columns WHERE constraint_id = ? "
                        "ORDER BY column_ordinal;");
                    constraintColumnsStmt.BindInt64(1, constraintId);
                    bool hasConstraintColumn = false;
                    while (constraintColumnsStmt.Step(&hasConstraintColumn) == SC_OK && hasConstraintColumn)
                    {
                        constraint.columns.push_back(constraintColumnsStmt.ColumnText(0));
                        const std::wstring referencedColumn = constraintColumnsStmt.ColumnText(1);
                        if (!referencedColumn.empty())
                        {
                            constraint.referencedColumns.push_back(referencedColumn);
                        }
                    }

                    schema->LoadConstraint(constraint, constraintId);
                }
            }

            if (HasTable(L"schema_indexes") && HasTable(L"schema_index_columns"))
            {
                SqliteStmt indexesStmt = db_.Prepare(
                    "SELECT index_id, name, source_kind FROM schema_indexes "
                    "WHERE table_id = ? ORDER BY index_id;");
                indexesStmt.BindInt64(1, tableRowId);
                bool hasIndex = false;
                while (indexesStmt.Step(&hasIndex) == SC_OK && hasIndex)
                {
                    SCIndexDef index;
                    const std::int64_t indexId = indexesStmt.ColumnInt64(0);
                    index.name = indexesStmt.ColumnText(1);
                    index.sourceKind = FromSqliteSchemaSourceKind(indexesStmt.ColumnInt(2));

                    SqliteStmt indexColumnsStmt = db_.Prepare(
                        "SELECT column_name, descending_flag FROM "
                        "schema_index_columns WHERE index_id = ? ORDER BY "
                        "column_ordinal;");
                    indexColumnsStmt.BindInt64(1, indexId);
                    bool hasIndexColumn = false;
                    while (indexColumnsStmt.Step(&hasIndexColumn) == SC_OK && hasIndexColumn)
                    {
                        index.columns.push_back(
                            SCIndexColumnDef{indexColumnsStmt.ColumnText(0), indexColumnsStmt.ColumnBool(1)});
                    }

                    schema->LoadIndex(index, indexId);
                    if (HasTable(L"query_indexes") && IsCompositeIndexExplicit(index))
                    {
                        queryIndexRowIdsByTableAndName_[BuildQueryIndexStorageKey(tableRowId, index.name)] = indexId;
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::BackfillSchemaMetadataV3()
        {
            EnsureSchemaMetadataTables();

            for (const auto& [_, tableRef] : tables_)
            {
                auto* table = static_cast<SqliteTable*>(tableRef.Get());
                if (table == nullptr)
                {
                    continue;
                }

                SCSchemaPtr schema;
                const ErrorCode schemaRc = tableRef->GetSchema(schema);
                if (Failed(schemaRc) || !schema)
                {
                    return Failed(schemaRc) ? schemaRc : SC_E_FAIL;
                }

                SCTableSchemaSnapshot snapshot;
                const ErrorCode snapshotRc = schema->GetSchemaSnapshot(&snapshot);
                if (Failed(snapshotRc))
                {
                    return snapshotRc;
                }

                const auto toUpperCopy = [](std::wstring text) {
                    std::transform(text.begin(), text.end(), text.begin(), [](wchar_t ch) {
                        return static_cast<wchar_t>(std::towupper(ch));
                    });
                    return text;
                };

                if (snapshot.constraints.empty())
                {
                    const auto idColumn = std::find_if(
                        snapshot.columns.begin(), snapshot.columns.end(), [&toUpperCopy](const SCColumnDef& column) {
                            return SCCommon::ToUpper(column.name) == L"ID";
                        });
                    if (idColumn != snapshot.columns.end())
                    {
                        SCConstraintDef constraint;
                        constraint.kind = SCConstraintKind::PrimaryKey;
                        constraint.name = L"pk_legacy";
                        constraint.columns.push_back(idColumn->name);
                        constraint.sourceKind = SCSchemaSourceKind::MigratedConvention;
                        snapshot.constraints.push_back(std::move(constraint));
                    }
                }

                if (snapshot.indexes.empty())
                {
                    for (const SCColumnDef& column : snapshot.columns)
                    {
                        if (!column.indexed)
                        {
                            continue;
                        }

                        SCIndexDef index;
                        index.name = L"idx_legacy_" + column.name;
                        index.sourceKind = SCSchemaSourceKind::LegacyHint;
                        index.columns.push_back(SCIndexColumnDef{column.name, false});
                        snapshot.indexes.push_back(std::move(index));
                    }
                }

                {
                    SqliteStmt stmt = db_.Prepare(
                        "INSERT INTO schema_tables(table_id, description) "
                        "VALUES(?, ?) ON CONFLICT(table_id) DO UPDATE SET "
                        "description=excluded.description;");
                    stmt.BindInt64(1, table->TableRowId());
                    stmt.BindText(2, snapshot.table.description);
                    const ErrorCode rc = stmt.Step();
                    if (Failed(rc))
                    {
                        return rc;
                    }
                }

                {
                    SqliteStmt stmt = db_.Prepare(
                        "DELETE FROM schema_constraint_columns WHERE "
                        "constraint_id IN (SELECT constraint_id FROM "
                        "schema_constraints WHERE table_id = ?);");
                    stmt.BindInt64(1, table->TableRowId());
                    const ErrorCode rc = stmt.Step();
                    if (Failed(rc))
                    {
                        return rc;
                    }
                }

                {
                    SqliteStmt stmt = db_.Prepare("DELETE FROM schema_constraints WHERE table_id = ?;");
                    stmt.BindInt64(1, table->TableRowId());
                    const ErrorCode rc = stmt.Step();
                    if (Failed(rc))
                    {
                        return rc;
                    }
                }

                {
                    SqliteStmt stmt = db_.Prepare(
                        "DELETE FROM schema_index_columns WHERE index_id IN "
                        "(SELECT index_id FROM schema_indexes WHERE table_id "
                        "= ?);");
                    stmt.BindInt64(1, table->TableRowId());
                    const ErrorCode rc = stmt.Step();
                    if (Failed(rc))
                    {
                        return rc;
                    }
                }

                {
                    SqliteStmt stmt = db_.Prepare("DELETE FROM schema_indexes WHERE table_id = ?;");
                    stmt.BindInt64(1, table->TableRowId());
                    const ErrorCode rc = stmt.Step();
                    if (Failed(rc))
                    {
                        return rc;
                    }
                }

                for (const SCConstraintDef& constraint : snapshot.constraints)
                {
                    SqliteStmt stmt = db_.Prepare(
                        "INSERT INTO schema_constraints(table_id, kind, "
                        "name, source_kind, referenced_table, "
                        "on_delete_action, on_update_action, "
                        "check_expression) VALUES(?, ?, ?, ?, ?, ?, ?, ?);");
                    stmt.BindInt64(1, table->TableRowId());
                    stmt.BindInt(2, ToSqliteConstraintKind(constraint.kind));
                    stmt.BindText(3, constraint.name);
                    stmt.BindInt(4, ToSqliteSchemaSourceKind(constraint.sourceKind));
                    stmt.BindText(5, constraint.referencedTable);
                    stmt.BindInt(6, ToSqliteForeignKeyAction(constraint.onDelete));
                    stmt.BindInt(7, ToSqliteForeignKeyAction(constraint.onUpdate));
                    stmt.BindText(8, constraint.checkExpression);
                    const ErrorCode rc = stmt.Step();
                    if (Failed(rc))
                    {
                        return rc;
                    }

                    const std::int64_t constraintId = db_.LastInsertRowId();
                    for (std::size_t index = 0; index < constraint.columns.size(); ++index)
                    {
                        SqliteStmt columnStmt = db_.Prepare(
                            "INSERT INTO schema_constraint_columns("
                            "constraint_id, column_ordinal, column_name, "
                            "referenced_column_name) VALUES(?, ?, ?, ?);");
                        columnStmt.BindInt64(1, constraintId);
                        columnStmt.BindInt64(2, static_cast<std::int64_t>(index));
                        columnStmt.BindText(3, constraint.columns[index]);
                        const std::wstring referencedColumn = index < constraint.referencedColumns.size()
                                                                  ? constraint.referencedColumns[index]
                                                                  : std::wstring{};
                        columnStmt.BindText(4, referencedColumn);
                        const ErrorCode columnRc = columnStmt.Step();
                        if (Failed(columnRc))
                        {
                            return columnRc;
                        }
                    }
                }

                for (const SCIndexDef& index : snapshot.indexes)
                {
                    SqliteStmt stmt = db_.Prepare(
                        "INSERT INTO schema_indexes(table_id, name, "
                        "source_kind) VALUES(?, ?, ?);");
                    stmt.BindInt64(1, table->TableRowId());
                    stmt.BindText(2, index.name);
                    stmt.BindInt(3, ToSqliteSchemaSourceKind(index.sourceKind));
                    const ErrorCode rc = stmt.Step();
                    if (Failed(rc))
                    {
                        return rc;
                    }

                    const std::int64_t indexId = db_.LastInsertRowId();
                    for (std::size_t columnIndex = 0; columnIndex < index.columns.size(); ++columnIndex)
                    {
                        SqliteStmt columnStmt = db_.Prepare(
                            "INSERT INTO schema_index_columns(index_id, "
                            "column_ordinal, column_name, descending_flag) "
                            "VALUES(?, ?, ?, ?);");
                        columnStmt.BindInt64(1, indexId);
                        columnStmt.BindInt64(2, static_cast<std::int64_t>(columnIndex));
                        columnStmt.BindText(3, index.columns[columnIndex].columnName);
                        columnStmt.BindInt(4, index.columns[columnIndex].descending ? 1 : 0);
                        const ErrorCode columnRc = columnStmt.Step();
                        if (Failed(columnRc))
                        {
                            return columnRc;
                        }
                    }
                }

                std::wstringstream message;
                message << L"Backfilled schema metadata for table " << snapshot.table.name << L" ("
                        << snapshot.constraints.size() << L" constraints, " << snapshot.indexes.size() << L" indexes).";
                LogStartupDiagnostic(SCDiagnosticSeverity::Info, L"upgrade", message.str());
            }

            return SC_OK;
        }

        void SqliteDatabase::LoadJournalStacks()
        {
            if (!HasTable(L"journal_transactions") || !HasTable(L"journal_entries") ||
                !HasTable(L"journal_schema_entries"))
            {
                return;
            }

            const bool supportsBinaryStorage = schemaVersion_ >= 4;
            const bool supportsExtendedSchemaJournalColumns =
                HasTableColumn("journal_schema_entries", "old_reference_storage_column") &&
                HasTableColumn("journal_schema_entries", "old_reference_display_column") &&
                HasTableColumn("journal_schema_entries", "new_reference_storage_column") &&
                HasTableColumn("journal_schema_entries", "new_reference_display_column");
            constexpr int kJournalSchemaSequenceIndex = 0;
            constexpr int kJournalSchemaOpIndex = 1;
            constexpr int kJournalSchemaTableNameIndex = 2;
            constexpr int kJournalSchemaColumnNameIndex = 3;
            constexpr int kJournalSchemaColumnRowIdIndex = 4;
            constexpr int kJournalSchemaOldDisplayNameIndex = 5;
            constexpr int kJournalSchemaOldValueKindIndex = 6;
            constexpr int kJournalSchemaOldColumnKindIndex = 7;
            constexpr int kJournalSchemaOldNullableIndex = 8;
            constexpr int kJournalSchemaOldEditableIndex = 9;
            constexpr int kJournalSchemaOldUserDefinedIndex = 10;
            constexpr int kJournalSchemaOldIndexedIndex = 11;
            constexpr int kJournalSchemaOldParticipatesIndex = 12;
            constexpr int kJournalSchemaOldUnitIndex = 13;
            constexpr int kJournalSchemaOldReferenceTableIndex = 14;
            constexpr int kJournalSchemaOldReferenceStorageColumnIndex = 15;
            constexpr int kJournalSchemaOldReferenceDisplayColumnIndex = 16;
            constexpr int kJournalSchemaOldDefaultKindIndex = 17;
            constexpr int kJournalSchemaOldDefaultInt64Index = 18;
            constexpr int kJournalSchemaOldDefaultDoubleIndex = 19;
            constexpr int kJournalSchemaOldDefaultBoolIndex = 20;
            constexpr int kJournalSchemaOldDefaultTextIndex = 21;
            constexpr int kJournalSchemaOldDefaultBlobIndex = 22;
            constexpr int kJournalSchemaNewDisplayNameIndex = 23;
            constexpr int kJournalSchemaNewValueKindIndex = 24;
            constexpr int kJournalSchemaNewColumnKindIndex = 25;
            constexpr int kJournalSchemaNewNullableIndex = 26;
            constexpr int kJournalSchemaNewEditableIndex = 27;
            constexpr int kJournalSchemaNewUserDefinedIndex = 28;
            constexpr int kJournalSchemaNewIndexedIndex = 29;
            constexpr int kJournalSchemaNewParticipatesIndex = 30;
            constexpr int kJournalSchemaNewUnitIndex = 31;
            constexpr int kJournalSchemaNewReferenceTableIndex = 32;
            constexpr int kJournalSchemaNewReferenceStorageColumnIndex = 33;
            constexpr int kJournalSchemaNewReferenceDisplayColumnIndex = 34;
            constexpr int kJournalSchemaNewDefaultKindIndex = 35;
            constexpr int kJournalSchemaNewDefaultInt64Index = 36;
            constexpr int kJournalSchemaNewDefaultDoubleIndex = 37;
            constexpr int kJournalSchemaNewDefaultBoolIndex = 38;
            constexpr int kJournalSchemaNewDefaultTextIndex = 39;
            constexpr int kJournalSchemaNewDefaultBlobIndex = 40;
            SqliteStmt txStmt = db_.Prepare(
                "SELECT tx_id, action_name, committed_version, stack_kind FROM "
                "journal_transactions ORDER BY stack_kind, stack_order;");
            bool hasTx = false;
            while (txStmt.Step(&hasTx) == SC_OK && hasTx)
            {
                SqlitePersistedJournalTransaction persisted;
                persisted.txId = txStmt.ColumnInt64(0);
                persisted.tx.actionName = txStmt.ColumnText(1);
                persisted.tx.commitId = static_cast<CommitId>(persisted.txId);
                persisted.tx.committedVersion = static_cast<VersionId>(txStmt.ColumnInt64(2));
                const int stackKind = txStmt.ColumnInt(3);

                std::vector<SqlitePersistedJournalEntry> entries;

                SqliteStmt entryStmt =
                    db_.Prepare(supportsBinaryStorage ? "SELECT sequence_index, op, table_name, record_id, "
                                                        "field_name, old_kind, old_int64, old_double, "
                                                        "old_bool, old_text, old_blob, new_kind, "
                                                        "new_int64, new_double, new_bool, new_text, "
                                                        "new_blob, old_deleted, new_deleted FROM "
                                                        "journal_entries WHERE tx_id = ?;"
                                                      : "SELECT sequence_index, op, table_name, record_id, "
                                                        "field_name, old_kind, old_int64, old_double, "
                                                        "old_bool, old_text, new_kind, new_int64, "
                                                        "new_double, new_bool, new_text, old_deleted, "
                                                        "new_deleted FROM journal_entries WHERE tx_id = ?;");
                entryStmt.BindInt64(1, persisted.txId);
                bool hasEntry = false;
                while (entryStmt.Step(&hasEntry) == SC_OK && hasEntry)
                {
                    SqlitePersistedJournalEntry persistedEntry;
                    persistedEntry.sequenceIndex = entryStmt.ColumnInt(0);
                    JournalEntry entry;
                    entry.op = FromSqliteJournalOp(entryStmt.ColumnInt(1));
                    entry.tableName = entryStmt.ColumnText(2);
                    entry.recordId = entryStmt.ColumnInt64(3);
                    entry.fieldName = entryStmt.ColumnText(4);
                    entry.oldValue = supportsBinaryStorage ? StorageCodec::ReadValue(entryStmt, 5, 6, 7, 8, 9, 10)
                                                           : StorageCodec::ReadValue(entryStmt, 5, 6, 7, 8, 9, 9);
                    entry.newValue = supportsBinaryStorage ? StorageCodec::ReadValue(entryStmt, 11, 12, 13, 14, 15, 16)
                                                           : StorageCodec::ReadValue(entryStmt, 10, 11, 12, 13, 14, 14);
                    entry.oldDeleted = supportsBinaryStorage ? entryStmt.ColumnBool(17) : entryStmt.ColumnBool(15);
                    entry.newDeleted = supportsBinaryStorage ? entryStmt.ColumnBool(18) : entryStmt.ColumnBool(16);

                    switch (entry.op)
                    {
                        case JournalOp::AddConstraint:
                        case JournalOp::RemoveConstraint:
                            entry.constraintRowId = entry.recordId;
                            if (!entry.oldValue.IsNull())
                            {
                                std::wstring payload;
                                entry.oldValue.AsStringCopy(&payload);
                                if (!ImportSerializer::DeserializeConstraintDef(payload, &entry.oldConstraint))
                                {
                                    throw std::runtime_error("failed to deserialize constraint journal entry");
                                }
                            }
                            if (!entry.newValue.IsNull())
                            {
                                std::wstring payload;
                                entry.newValue.AsStringCopy(&payload);
                                if (!ImportSerializer::DeserializeConstraintDef(payload, &entry.newConstraint))
                                {
                                    throw std::runtime_error("failed to deserialize constraint journal entry");
                                }
                            }
                            entry.oldValue = SCValue::Null();
                            entry.newValue = SCValue::Null();
                            entry.recordId = 0;
                            break;
                        case JournalOp::AddIndex:
                        case JournalOp::RemoveIndex:
                            entry.indexRowId = entry.recordId;
                            if (!entry.oldValue.IsNull())
                            {
                                std::wstring payload;
                                entry.oldValue.AsStringCopy(&payload);
                                if (!ImportSerializer::DeserializeIndexDef(payload, &entry.oldIndex))
                                {
                                    throw std::runtime_error("failed to deserialize index journal entry");
                                }
                            }
                            if (!entry.newValue.IsNull())
                            {
                                std::wstring payload;
                                entry.newValue.AsStringCopy(&payload);
                                if (!ImportSerializer::DeserializeIndexDef(payload, &entry.newIndex))
                                {
                                    throw std::runtime_error("failed to deserialize index journal entry");
                                }
                            }
                            entry.oldValue = SCValue::Null();
                            entry.newValue = SCValue::Null();
                            entry.recordId = 0;
                            break;
                        default:
                            break;
                    }
                    persistedEntry.entry = std::move(entry);
                    entries.push_back(std::move(persistedEntry));
                }

                SqliteStmt schemaStmt = db_.Prepare(
                    supportsBinaryStorage
                        ? (supportsExtendedSchemaJournalColumns
                               ? "SELECT sequence_index, op, table_name, column_name, "
                                 "column_rowid, old_display_name, old_value_kind, "
                                 "old_column_kind, old_nullable, old_editable, "
                                 "old_user_defined, old_indexed, "
                                 "old_participates_in_calc, old_unit, "
                                 "old_reference_table, old_reference_storage_column, "
                                 "old_reference_display_column, old_default_kind, "
                                 "old_default_int64, old_default_double, "
                                 "old_default_bool, old_default_text, "
                                 "old_default_blob, new_display_name, "
                                 "new_value_kind, new_column_kind, new_nullable, "
                                 "new_editable, new_user_defined, new_indexed, "
                                 "new_participates_in_calc, new_unit, "
                                 "new_reference_table, new_reference_storage_column, "
                                 "new_reference_display_column, new_default_kind, "
                                 "new_default_int64, new_default_double, "
                                 "new_default_bool, new_default_text, "
                                 "new_default_blob FROM journal_schema_entries WHERE tx_id = ?;"
                               : "SELECT sequence_index, op, table_name, column_name, "
                                 "column_rowid, old_display_name, old_value_kind, "
                                 "old_column_kind, old_nullable, old_editable, "
                                 "old_user_defined, old_indexed, "
                                 "old_participates_in_calc, old_unit, "
                                 "old_reference_table, '' AS old_reference_storage_column, "
                                 "'' AS old_reference_display_column, old_default_kind, "
                                 "old_default_int64, old_default_double, "
                                 "old_default_bool, old_default_text, "
                                 "old_default_blob, new_display_name, "
                                 "new_value_kind, new_column_kind, new_nullable, "
                                 "new_editable, new_user_defined, new_indexed, "
                                 "new_participates_in_calc, new_unit, "
                                 "new_reference_table, '' AS new_reference_storage_column, "
                                 "'' AS new_reference_display_column, new_default_kind, "
                                 "new_default_int64, new_default_double, "
                                 "new_default_bool, new_default_text, "
                                 "new_default_blob FROM journal_schema_entries WHERE tx_id = ?;")
                        : (supportsExtendedSchemaJournalColumns
                               ? "SELECT sequence_index, op, table_name, column_name, "
                                 "column_rowid, old_display_name, old_value_kind, "
                                 "old_column_kind, old_nullable, old_editable, "
                                 "old_user_defined, old_indexed, "
                                 "old_participates_in_calc, old_unit, "
                                 "old_reference_table, old_reference_storage_column, "
                                 "old_reference_display_column, old_default_kind, "
                                 "old_default_int64, old_default_double, "
                                 "old_default_bool, old_default_text, "
                                 "new_display_name, new_value_kind, new_column_kind, "
                                 "new_nullable, new_editable, new_user_defined, "
                                 "new_indexed, new_participates_in_calc, new_unit, "
                                 "new_reference_table, new_reference_storage_column, "
                                 "new_reference_display_column, new_default_kind, "
                                 "new_default_int64, new_default_double, "
                                 "new_default_bool, new_default_text FROM "
                                 "journal_schema_entries WHERE tx_id = ?;"
                               : "SELECT sequence_index, op, table_name, column_name, "
                                 "column_rowid, old_display_name, old_value_kind, "
                                 "old_column_kind, old_nullable, old_editable, "
                                 "old_user_defined, old_indexed, "
                                 "old_participates_in_calc, old_unit, "
                                 "old_reference_table, '' AS old_reference_storage_column, "
                                 "'' AS old_reference_display_column, old_default_kind, "
                                 "old_default_int64, old_default_double, "
                                 "old_default_bool, old_default_text, "
                                 "new_display_name, new_value_kind, new_column_kind, "
                                 "new_nullable, new_editable, new_user_defined, "
                                 "new_indexed, new_participates_in_calc, new_unit, "
                                 "new_reference_table, '' AS new_reference_storage_column, "
                                 "'' AS new_reference_display_column, new_default_kind, "
                                 "new_default_int64, new_default_double, "
                                 "new_default_bool, new_default_text FROM "
                                 "journal_schema_entries WHERE tx_id = ?;"));
                schemaStmt.BindInt64(1, persisted.txId);
                hasEntry = false;
                while (schemaStmt.Step(&hasEntry) == SC_OK && hasEntry)
                {
                    SqlitePersistedJournalEntry persistedEntry;
                    persistedEntry.sequenceIndex = schemaStmt.ColumnInt(kJournalSchemaSequenceIndex);
                    JournalEntry entry;
                    entry.op = FromSqliteJournalOp(schemaStmt.ColumnInt(kJournalSchemaOpIndex));
                    entry.tableName = schemaStmt.ColumnText(kJournalSchemaTableNameIndex);
                    entry.fieldName = schemaStmt.ColumnText(kJournalSchemaColumnNameIndex);
                    entry.columnRowId = schemaStmt.ColumnInt64(kJournalSchemaColumnRowIdIndex);
                    entry.oldColumn = supportsBinaryStorage
                                          ? StorageCodec::ReadColumnDef(
                                                schemaStmt,
                                                kJournalSchemaOldDisplayNameIndex,
                                                kJournalSchemaOldValueKindIndex,
                                                kJournalSchemaOldColumnKindIndex,
                                                kJournalSchemaOldNullableIndex,
                                                kJournalSchemaOldEditableIndex,
                                                kJournalSchemaOldUserDefinedIndex,
                                                kJournalSchemaOldIndexedIndex,
                                                kJournalSchemaOldParticipatesIndex,
                                                kJournalSchemaOldUnitIndex,
                                                kJournalSchemaOldReferenceTableIndex,
                                                kJournalSchemaOldReferenceStorageColumnIndex,
                                                kJournalSchemaOldReferenceDisplayColumnIndex,
                                                kJournalSchemaOldDefaultKindIndex,
                                                kJournalSchemaOldDefaultInt64Index,
                                                kJournalSchemaOldDefaultDoubleIndex,
                                                kJournalSchemaOldDefaultBoolIndex,
                                                kJournalSchemaOldDefaultTextIndex,
                                                kJournalSchemaOldDefaultBlobIndex)
                                          : StorageCodec::ReadColumnDef(
                                                schemaStmt,
                                                kJournalSchemaOldDisplayNameIndex,
                                                kJournalSchemaOldValueKindIndex,
                                                kJournalSchemaOldColumnKindIndex,
                                                kJournalSchemaOldNullableIndex,
                                                kJournalSchemaOldEditableIndex,
                                                kJournalSchemaOldUserDefinedIndex,
                                                kJournalSchemaOldIndexedIndex,
                                                kJournalSchemaOldParticipatesIndex,
                                                kJournalSchemaOldUnitIndex,
                                                kJournalSchemaOldReferenceTableIndex,
                                                kJournalSchemaOldReferenceStorageColumnIndex,
                                                kJournalSchemaOldReferenceDisplayColumnIndex,
                                                kJournalSchemaOldDefaultKindIndex,
                                                kJournalSchemaOldDefaultInt64Index,
                                                kJournalSchemaOldDefaultDoubleIndex,
                                                kJournalSchemaOldDefaultBoolIndex,
                                                kJournalSchemaOldDefaultTextIndex,
                                                kJournalSchemaOldDefaultTextIndex);
                    entry.newColumn =
                        supportsBinaryStorage
                            ? StorageCodec::ReadColumnDef(
                                  schemaStmt,
                                  kJournalSchemaNewDisplayNameIndex,
                                  kJournalSchemaNewValueKindIndex,
                                  kJournalSchemaNewColumnKindIndex,
                                  kJournalSchemaNewNullableIndex,
                                  kJournalSchemaNewEditableIndex,
                                  kJournalSchemaNewUserDefinedIndex,
                                  kJournalSchemaNewIndexedIndex,
                                  kJournalSchemaNewParticipatesIndex,
                                  kJournalSchemaNewUnitIndex,
                                  kJournalSchemaNewReferenceTableIndex,
                                  kJournalSchemaNewReferenceStorageColumnIndex,
                                  kJournalSchemaNewReferenceDisplayColumnIndex,
                                  kJournalSchemaNewDefaultKindIndex,
                                  kJournalSchemaNewDefaultInt64Index,
                                  kJournalSchemaNewDefaultDoubleIndex,
                                  kJournalSchemaNewDefaultBoolIndex,
                                  kJournalSchemaNewDefaultTextIndex,
                                  kJournalSchemaNewDefaultBlobIndex)
                            : StorageCodec::ReadColumnDef(
                                  schemaStmt,
                                  kJournalSchemaNewDisplayNameIndex,
                                  kJournalSchemaNewValueKindIndex,
                                  kJournalSchemaNewColumnKindIndex,
                                  kJournalSchemaNewNullableIndex,
                                  kJournalSchemaNewEditableIndex,
                                  kJournalSchemaNewUserDefinedIndex,
                                  kJournalSchemaNewIndexedIndex,
                                  kJournalSchemaNewParticipatesIndex,
                                  kJournalSchemaNewUnitIndex,
                                  kJournalSchemaNewReferenceTableIndex,
                                  kJournalSchemaNewReferenceStorageColumnIndex,
                                  kJournalSchemaNewReferenceDisplayColumnIndex,
                                  kJournalSchemaNewDefaultKindIndex,
                                  kJournalSchemaNewDefaultInt64Index,
                                  kJournalSchemaNewDefaultDoubleIndex,
                                  kJournalSchemaNewDefaultBoolIndex,
                                  kJournalSchemaNewDefaultTextIndex,
                                  kJournalSchemaNewDefaultTextIndex);
                    persistedEntry.entry = std::move(entry);
                    entries.push_back(std::move(persistedEntry));
                }

                std::sort(entries.begin(),
                          entries.end(),
                          [](const SqlitePersistedJournalEntry& left, const SqlitePersistedJournalEntry& right) {
                              return left.sequenceIndex < right.sequenceIndex;
                          });
                for (auto& entry : entries)
                {
                    persisted.tx.entries.push_back(std::move(entry.entry));
                }

                if (stackKind == kStackUndo)
                {
                    undoStack_.push_back(std::move(persisted));
                } else
                {
                    redoStack_.push_back(std::move(persisted));
                }
            }
        }

        ErrorCode SqliteDatabase::ExecuteUpgradePlan(const SCUpgradePlan& plan,
                                                     bool confirmed,
                                                     SCUpgradeResult* outResult)
        {
            if (outResult == nullptr)
            {
                return SC_E_POINTER;
            }

            SCUpgradeResult result;
            result.sourceVersion = schemaVersion_;
            result.targetVersion = plan.targetVersion;

            const auto finish = [&](ErrorCode rc) {
                *outResult = result;
                return rc;
            };

            if (readOnly_)
            {
                result.status = SCUpgradeStatus::Unsupported;
                result.failureReason = L"Database is opened read-only.";
                return finish(SC_E_READ_ONLY_DATABASE);
            }

            if (!cleanShutdown_)
            {
                result.status = SCUpgradeStatus::Unsupported;
                result.failureReason = L"Upgrade is not allowed after an unclean shutdown.";
                return finish(SC_E_WRITE_CONFLICT);
            }

            if (!confirmed)
            {
                result.status = SCUpgradeStatus::Failed;
                result.failureReason = L"Upgrade confirmation was not provided.";
                return finish(SC_E_INVALIDARG);
            }

            if (plan.currentVersion != schemaVersion_)
            {
                result.status = SCUpgradeStatus::Failed;
                result.failureReason = L"Upgrade plan does not match the current schema version.";
                return finish(SC_E_INVALIDARG);
            }

            if (plan.targetVersion <= plan.currentVersion)
            {
                result.status = SCUpgradeStatus::NotRequired;
                result.failureReason = L"Upgrade is not required for the current schema version.";
                return finish(SC_OK);
            }

            if (plan.orderedSteps.empty())
            {
                result.status = SCUpgradeStatus::Unsupported;
                result.failureReason = L"Upgrade plan does not contain executable steps.";
                return finish(SC_E_NOTIMPL);
            }

            const std::int32_t originalSchemaVersion = schemaVersion_;

            try
            {
                SqliteTxn txn(db_);
                for (const SCMigrationStep& step : plan.orderedSteps)
                {
                    if (step.fromVersion != schemaVersion_)
                    {
                        schemaVersion_ = originalSchemaVersion;
                        result.status = SCUpgradeStatus::Failed;
                        result.failureReason =
                            L"Upgrade step chain does not match the current "
                            L"schema version.";
                        return finish(SC_E_INVALIDARG);
                    }

                    const SqliteUpgradeStepRegistration* registration =
                        FindRegisteredSqliteUpgradeStep(step.fromVersion, step.toVersion);
                    if (registration == nullptr || registration->handler == nullptr)
                    {
                        schemaVersion_ = originalSchemaVersion;
                        result.status = SCUpgradeStatus::Unsupported;
                        result.failureReason = L"Unsupported upgrade step.";
                        return finish(SC_E_NOTIMPL);
                    }

                    const ErrorCode stepRc = registration->handler(*this, &result);
                    if (Failed(stepRc))
                    {
                        schemaVersion_ = originalSchemaVersion;
                        result.rolledBack = true;
                        result.status = stepRc == SC_E_NOTIMPL ? SCUpgradeStatus::Unsupported
                                                                : SCUpgradeStatus::RolledBack;
                        if (result.failureReason.empty())
                        {
                            result.failureReason = L"Upgrade step failed.";
                        }
                        return finish(stepRc);
                    }

                    schemaVersion_ = step.toVersion;
                    std::wstringstream message;
                    message << L"Applied upgrade step " << step.name << L" to schema version " << schemaVersion_
                            << L".";
                    LogStartupDiagnostic(SCDiagnosticSeverity::Info, L"upgrade", message.str());
                }

                SaveMetadata();
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    schemaVersion_ = originalSchemaVersion;
                    result.status = SCUpgradeStatus::RolledBack;
                    result.rolledBack = true;
                    result.failureReason = L"Failed to commit the upgrade transaction.";
                    return finish(commitRc);
                }
            } catch (...)
            {
                schemaVersion_ = originalSchemaVersion;
                result.status = SCUpgradeStatus::RolledBack;
                result.rolledBack = true;
                result.failureReason = L"Upgrade transaction failed and was rolled back.";
                return finish(SC_E_FAIL);
            }

            result.status = SCUpgradeStatus::Success;
            result.rolledBack = false;
            result.sourceVersion = originalSchemaVersion;
            result.targetVersion = schemaVersion_;
            result.failureReason.clear();
            return finish(SC_OK);
        }

        void SqliteDatabase::InitializeQueryIndexStorage()
        {
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS query_indexes ("
                "schema_index_id INTEGER PRIMARY KEY, table_id INTEGER NOT "
                "NULL, index_name TEXT NOT NULL, key_arity INTEGER NOT NULL, "
                "UNIQUE(table_id, index_name));");
            db_.Execute(
                "CREATE TABLE IF NOT EXISTS query_index_entries ("
                "schema_index_id INTEGER NOT NULL, record_id INTEGER NOT "
                "NULL, alive_flag INTEGER NOT NULL, key_prefix_1 BLOB, "
                "key_prefix_2 BLOB, key_prefix_3 BLOB, full_key BLOB NOT "
                "NULL, PRIMARY KEY(schema_index_id, record_id));");
            db_.Execute(
                "CREATE INDEX IF NOT EXISTS idx_query_index_entries_prefix1 "
                "ON query_index_entries(schema_index_id, alive_flag, "
                "key_prefix_1, record_id);");
            db_.Execute(
                "CREATE INDEX IF NOT EXISTS idx_query_index_entries_prefix2 "
                "ON query_index_entries(schema_index_id, alive_flag, "
                "key_prefix_2, record_id);");
            db_.Execute(
                "CREATE INDEX IF NOT EXISTS idx_query_index_entries_prefix3 "
                "ON query_index_entries(schema_index_id, alive_flag, "
                "key_prefix_3, record_id);");
            db_.Execute(
                "CREATE INDEX IF NOT EXISTS idx_query_index_entries_full ON "
                "query_index_entries(schema_index_id, alive_flag, full_key, "
                "record_id);");
        }

        ErrorCode SqliteDatabase::EnsureLegacyColumnIndexes()
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }

            for (const auto& [_, tableRef] : tables_)
            {
                const auto* table = static_cast<SqliteTable*>(tableRef.Get());
                if (table == nullptr)
                {
                    continue;
                }

                SCSchemaPtr schema;
                if (Failed(tableRef->GetSchema(schema)) || !schema)
                {
                    continue;
                }

                std::int32_t count = 0;
                if (Failed(schema->GetColumnCount(&count)))
                {
                    continue;
                }

                for (std::int32_t index = 0; index < count; ++index)
                {
                    SCColumnDef column;
                    if (Failed(schema->GetColumn(index, &column)))
                    {
                        continue;
                    }
                    if (column.indexed)
                    {
                        EnsureColumnIndex(table->TableRowId(), column.name);
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::RebuildCompositeQueryIndexes()
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }

            queryIndexRowIdsByTableAndName_.clear();
            for (const auto& [_, tableRef] : tables_)
            {
                auto* sqliteTable = static_cast<SqliteTable*>(tableRef.Get());
                if (sqliteTable == nullptr)
                {
                    continue;
                }

                const ErrorCode rebuildRc = RebuildCompositeIndexesForTable(sqliteTable);
                if (Failed(rebuildRc))
                {
                    return rebuildRc;
                }
            }

            return SC_OK;
        }

        void SqliteDatabase::EnsureColumnIndex(std::int64_t tableRowId, const std::wstring& columnName)
        {
            const std::wstring indexName =
                L"idx_fv_" + std::to_wstring(tableRowId) + L"_" + SCCommon::SanitizeFileName(columnName);
            const std::string sql = SCCommon::ToUtf8(L"CREATE INDEX IF NOT EXISTS " + indexName +
                                           L" ON field_values(table_id, column_name, int64_value, "
                                           L"double_value, bool_value, text_value);");
            db_.Execute(sql.c_str());
        }

        void SqliteDatabase::RunStartupIntegrityCheck()
        {
            SqliteStmt stmt = db_.Prepare("PRAGMA integrity_check;");
            bool hasRow = false;
            while (stmt.Step(&hasRow) == SC_OK && hasRow)
            {
                const std::wstring result = stmt.ColumnText(0);
                if (result != L"ok")
                {
                    corruptionDetected_ = true;
                    LogStartupDiagnostic(SCDiagnosticSeverity::Error,
                                         L"integrity",
                                         std::wstring(L"SQLite integrity check failed: ") + result);
                    throw std::runtime_error("sqlite integrity check failed");
                }
            }

            LogStartupDiagnostic(SCDiagnosticSeverity::Info, L"integrity", L"SQLite integrity check passed.");
        }

        void SqliteDatabase::LogStartupDiagnostic(SCDiagnosticSeverity severity,
                                                  const std::wstring& category,
                                                  const std::wstring& message)
        {
            startupDiagnostics_.push_back(SCDiagnosticEntry{severity, category, message});
            SqliteStmt stmt = db_.Prepare(
                "INSERT INTO startup_diagnostics(severity, category, message) "
                "VALUES(?, ?, ?);");
            stmt.BindInt(1, static_cast<int>(severity));
            stmt.BindText(2, category);
            stmt.BindText(3, message);
            stmt.Step();
        }

        void SqliteDatabase::SetCleanShutdownFlag(bool cleanShutdown)
        {
            cleanShutdown_ = cleanShutdown;
            SaveMetadataKey(L"clean_shutdown", cleanShutdown_ ? L"1" : L"0");
        }

        ErrorCode SqliteDatabase::EnsureImportSessionStore()
        {
            if (importSessionStoreReady_ || readOnly_)
            {
                return SC_OK;
            }

            const ErrorCode rc = db_.Execute(
                "CREATE TABLE IF NOT EXISTS import_sessions ("
                "session_id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "session_name TEXT NOT NULL, state INTEGER NOT NULL, "
                "base_version INTEGER NOT NULL, chunk_size INTEGER NOT NULL, "
                "checkpoint_chunk_id INTEGER NOT NULL, checkpoint_count "
                "INTEGER NOT NULL, payload TEXT NOT NULL);");
            if (Succeeded(rc))
            {
                importSessionStoreReady_ = true;
            }
            return rc;
        }

        std::wstring SqliteDatabase::SerializeImportSession(const SCImportStagingArea& session) const
        {
            return ImportSerializer::SerializeImportSessionPayload(session);
        }

        ErrorCode SqliteDatabase::DeserializeImportSession(const std::wstring& payload,
                                                           SCImportStagingArea* outSession) const
        {
            if (outSession == nullptr)
            {
                return SC_E_POINTER;
            }
            return ImportSerializer::DeserializeImportSessionPayload(payload, outSession) ? SC_OK : SC_E_FAIL;
        }

        ErrorCode SqliteDatabase::BeginImportSession(const SCImportSessionOptions& options,
                                                     SCImportStagingArea* outSession)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (outSession == nullptr)
            {
                return SC_E_POINTER;
            }

            const ErrorCode storeRc = EnsureImportSessionStore();
            if (Failed(storeRc))
            {
                return storeRc;
            }

            SCImportStagingArea session;
            session.sessionName = options.sessionName.empty() ? L"Import" : options.sessionName;
            session.baseVersion = version_;
            session.chunkSize = options.chunkSize == 0 ? 1 : options.chunkSize;
            session.state = SCImportSessionState::Staging;

            SqliteStmt stmt = db_.Prepare(
                "INSERT INTO import_sessions(session_name, state, "
                "base_version, chunk_size, checkpoint_chunk_id, "
                "checkpoint_count, payload)"
                " VALUES(?, ?, ?, ?, ?, ?, ?);");
            stmt.BindText(1, session.sessionName);
            stmt.BindInt(2, static_cast<int>(SCImportSessionState::Staging));
            stmt.BindInt64(3, static_cast<std::int64_t>(version_));
            stmt.BindInt64(4, static_cast<std::int64_t>(session.chunkSize));
            stmt.BindInt64(5, 0);
            stmt.BindInt64(6, 0);
            stmt.BindText(7, L"");
            const ErrorCode rc = stmt.Step();
            if (Failed(rc))
            {
                return rc;
            }

            session.sessionId = static_cast<SCImportSessionId>(db_.LastInsertRowId());
            const std::wstring payload = ImportSerializer::SerializeImportSessionPayload(session);
            SqliteStmt updateStmt = db_.Prepare(
                "UPDATE import_sessions SET payload = ?, checkpoint_chunk_id = "
                "?, checkpoint_count = ?, state = ? WHERE session_id = ?;");
            updateStmt.BindText(1, payload);
            updateStmt.BindInt64(2, 0);
            updateStmt.BindInt64(3, 0);
            updateStmt.BindInt(4, static_cast<int>(SCImportSessionState::Staging));
            updateStmt.BindInt64(5, static_cast<std::int64_t>(session.sessionId));
            const ErrorCode updateRc = updateStmt.Step();
            if (Failed(updateRc))
            {
                return updateRc;
            }
            *outSession = session;
            return SC_OK;
        }

        ErrorCode SqliteDatabase::AppendImportChunk(SCImportStagingArea* session,
                                                    const SCImportChunk& chunk,
                                                    SCImportCheckpoint* outCheckpoint)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (session == nullptr)
            {
                return SC_E_POINTER;
            }

            const ErrorCode storeRc = EnsureImportSessionStore();
            if (Failed(storeRc))
            {
                return storeRc;
            }

            session->chunks.push_back(chunk);
            session->state = SCImportSessionState::Checkpointed;

            SqliteStmt loadStmt = db_.Prepare("SELECT session_id FROM import_sessions WHERE session_id = ?;");
            loadStmt.BindInt64(1, static_cast<std::int64_t>(session->sessionId));
            bool hasRow = false;
            if (Failed(loadStmt.Step(&hasRow)) || !hasRow)
            {
                return SC_E_RECORD_NOT_FOUND;
            }

            const std::wstring payload = ImportSerializer::SerializeImportSessionPayload(*session);
            SqliteStmt updateStmt = db_.Prepare(
                "UPDATE import_sessions SET state = ?, checkpoint_chunk_id = "
                "?, checkpoint_count = ?, payload = ? WHERE session_id = ?;");
            updateStmt.BindInt(1, static_cast<int>(SCImportSessionState::Checkpointed));
            updateStmt.BindInt64(2, static_cast<std::int64_t>(chunk.chunkId));
            updateStmt.BindInt64(3, static_cast<std::int64_t>(session->chunks.size()));
            updateStmt.BindText(4, payload);
            updateStmt.BindInt64(5, static_cast<std::int64_t>(session->sessionId));
            const ErrorCode rc = updateStmt.Step();
            if (Failed(rc))
            {
                return rc;
            }

            if (outCheckpoint != nullptr)
            {
                outCheckpoint->sessionId = session->sessionId;
                outCheckpoint->lastChunkId = chunk.chunkId;
                outCheckpoint->chunkCount = session->chunks.size();
                outCheckpoint->baseVersion = session->baseVersion;
                outCheckpoint->persisted = true;
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::LoadImportRecoveryState(std::uint64_t sessionId, SCImportRecoveryState* outState)
        {
            if (outState == nullptr)
            {
                return SC_E_POINTER;
            }

            const ErrorCode storeRc = EnsureImportSessionStore();
            if (Failed(storeRc))
            {
                return storeRc;
            }

            SqliteStmt stmt = db_.Prepare(
                "SELECT session_name, state, base_version, chunk_size, "
                "checkpoint_chunk_id, checkpoint_count, payload"
                " FROM import_sessions WHERE session_id = ?;");
            stmt.BindInt64(1, static_cast<std::int64_t>(sessionId));

            bool hasRow = false;
            const ErrorCode stepRc = stmt.Step(&hasRow);
            if (Failed(stepRc))
            {
                return stepRc;
            }
            if (!hasRow)
            {
                return SC_E_RECORD_NOT_FOUND;
            }

            SCImportRecoveryState state;
            state.sessionId = sessionId;
            state.state = static_cast<SCImportSessionState>(stmt.ColumnInt(1));
            state.checkpoint.sessionId = sessionId;
            state.checkpoint.lastChunkId = static_cast<SCImportChunkId>(stmt.ColumnInt64(4));
            state.checkpoint.chunkCount = static_cast<std::size_t>(stmt.ColumnInt64(5));
            state.checkpoint.baseVersion = static_cast<VersionId>(stmt.ColumnInt64(2));
            state.checkpoint.persisted = true;
            state.checkpointPersisted = true;

            const std::wstring payload = stmt.ColumnText(6);
            if (!payload.empty())
            {
                if (!ImportSerializer::DeserializeImportSessionPayload(payload, &state.stagingArea))
                {
                    return SC_E_FAIL;
                }
            } else
            {
                state.stagingArea.sessionId = sessionId;
                state.stagingArea.sessionName = stmt.ColumnText(0);
                state.stagingArea.baseVersion = static_cast<VersionId>(stmt.ColumnInt64(2));
                state.stagingArea.chunkSize = static_cast<std::size_t>(stmt.ColumnInt64(3));
                state.stagingArea.state = state.state;
            }

            state.stagingArea.sessionId = sessionId;
            state.stagingArea.sessionName = stmt.ColumnText(0);
            state.stagingArea.baseVersion = static_cast<VersionId>(stmt.ColumnInt64(2));
            state.stagingArea.chunkSize = static_cast<std::size_t>(stmt.ColumnInt64(3));
            state.stagingArea.state = state.state;
            state.canResume =
                state.state != SCImportSessionState::Finalized && state.state != SCImportSessionState::Aborted;
            state.canFinalize = state.state == SCImportSessionState::Checkpointed ||
                                state.state == SCImportSessionState::ReadyToFinalize;
            state.reason = state.canResume ? L"Import session recoverable from checkpoint."
                                           : L"Import session is no longer recoverable.";

            *outState = std::move(state);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::FinalizeImportSession(const SCImportFinalizeCommit& commit,
                                                        SCImportRecoveryState* outState)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (!commit.confirmed)
            {
                return SC_E_INVALIDARG;
            }

            const ErrorCode storeRc = EnsureImportSessionStore();
            if (Failed(storeRc))
            {
                return storeRc;
            }

            SCImportRecoveryState recoveryState;
            const ErrorCode loadRc = LoadImportRecoveryState(commit.sessionId, &recoveryState);
            if (Failed(loadRc))
            {
                return loadRc;
            }

            SqliteStmt deleteStmt = db_.Prepare("DELETE FROM import_sessions WHERE session_id = ?;");
            deleteStmt.BindInt64(1, static_cast<std::int64_t>(commit.sessionId));
            const ErrorCode deleteRc = deleteStmt.Step();
            if (Failed(deleteRc))
            {
                return deleteRc;
            }

            recoveryState.state = SCImportSessionState::Finalized;
            recoveryState.canResume = false;
            recoveryState.canFinalize = false;
            recoveryState.reason = L"Import session finalized and checkpoint cleared.";
            if (outState != nullptr)
            {
                *outState = std::move(recoveryState);
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::AbortImportSession(std::uint64_t sessionId)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }

            const ErrorCode storeRc = EnsureImportSessionStore();
            if (Failed(storeRc))
            {
                return storeRc;
            }

            SqliteStmt deleteStmt = db_.Prepare("DELETE FROM import_sessions WHERE session_id = ?;");
            deleteStmt.BindInt64(1, static_cast<std::int64_t>(sessionId));
            return deleteStmt.Step();
        }

        ErrorCode SqliteDatabase::EnsureWritable() const
        {
            return readOnly_ ? SC_E_READ_ONLY_DATABASE : SC_OK;
        }

        ErrorCode SqliteDatabase::CollectDiagnostics(SCStorageHealthReport* outReport) const
        {
            if (outReport == nullptr)
            {
                return SC_E_POINTER;
            }

            outReport->diagnostics.insert(
                outReport->diagnostics.end(), startupDiagnostics_.begin(), startupDiagnostics_.end());
            if (dirtyStartupDetected_)
            {
                outReport->diagnostics.push_back(SCDiagnosticEntry{
                    SCDiagnosticSeverity::Warning,
                    L"startup",
                    L"Current session followed an unclean shutdown.",
                });
            }
            if (corruptionDetected_)
            {
                outReport->diagnostics.push_back(SCDiagnosticEntry{
                    SCDiagnosticSeverity::Error,
                    L"integrity",
                    L"Corruption was detected during startup integrity checks.",
                });
            }

            QueryIndexCheckResult queryIndexCheck;
            if (Succeeded(CheckQueryIndex(&queryIndexCheck)))
            {
                if (queryIndexCheck.state == QueryIndexHealthState::Missing ||
                    queryIndexCheck.state == QueryIndexHealthState::OutOfDate)
                {
                    outReport->diagnostics.push_back(SCDiagnosticEntry{
                        SCDiagnosticSeverity::Warning,
                        L"query-index",
                        queryIndexCheck.message,
                    });
                } else if (queryIndexCheck.state == QueryIndexHealthState::Corrupted)
                {
                    outReport->diagnostics.push_back(SCDiagnosticEntry{
                        SCDiagnosticSeverity::Error,
                        L"query-index",
                        queryIndexCheck.message,
                    });
                }
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::CheckQueryIndex(QueryIndexCheckResult* outResult) const
        {
            if (outResult == nullptr)
            {
                return SC_E_POINTER;
            }

            return CheckCompositeQueryIndexConsistency(outResult);
        }

        ErrorCode SqliteDatabase::RebuildQueryIndex()
        {
            if (activeEdit_)
            {
                return SC_E_WRITE_CONFLICT;
            }

            const ErrorCode legacyRc = EnsureLegacyColumnIndexes();
            if (Failed(legacyRc))
            {
                return legacyRc;
            }

            return RebuildCompositeQueryIndexes();
        }

} // namespace StableCore::Storage