#include <filesystem>
#include <cstdint>
#include <vector>

#include <gtest/gtest.h>
#include <QStringList>
#include <QVariant>

#include "SCBatch.h"
#include "SCDatabaseSession.h"
#include "SCSchemaTableImport.h"

namespace sc = StableCore::Storage;
namespace editor = StableCore::Storage::Editor;
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

    sc::SCColumnDef MakeIntColumn(const wchar_t* name)
    {
        sc::SCColumnDef column;
        column.name = name;
        column.displayName = name;
        column.valueKind = sc::ValueKind::Int64;
        column.defaultValue = sc::SCValue::FromInt64(0);
        return column;
    }

    sc::SCColumnDef MakeRequiredIntColumn(const wchar_t* name)
    {
        sc::SCColumnDef column = MakeIntColumn(name);
        column.nullable = false;
        return column;
    }

    sc::SCColumnDef MakeRequiredIntColumnWithoutDefault(const wchar_t* name)
    {
        sc::SCColumnDef column;
        column.name = name;
        column.displayName = name;
        column.valueKind = sc::ValueKind::Int64;
        column.nullable = false;
        return column;
    }

    sc::SCColumnDef MakeStringColumn(const wchar_t* name)
    {
        sc::SCColumnDef column;
        column.name = name;
        column.displayName = name;
        column.valueKind = sc::ValueKind::String;
        column.defaultValue = sc::SCValue::FromString(L"");
        return column;
    }

    sc::SCColumnDef MakeBinaryColumn(const wchar_t* name)
    {
        sc::SCColumnDef column;
        column.name = name;
        column.displayName = name;
        column.valueKind = sc::ValueKind::Binary;
        column.nullable = false;
        return column;
    }

    sc::SCIndexDef MakeIndex(const wchar_t* name, const wchar_t* columnName)
    {
        sc::SCIndexDef index;
        index.name = name;
        index.columns.push_back(sc::SCIndexColumnDef{columnName, false});
        return index;
    }

    sc::SCComputedColumnDef MakeExpressionComputedColumnForTable(
        const wchar_t* name,
        const wchar_t* expression,
        const wchar_t* dependencyTable,
        const wchar_t* dependencyField)
    {
        sc::SCComputedColumnDef column;
        column.name = name;
        column.displayName = name;
        column.valueKind = sc::ValueKind::Int64;
        column.kind = sc::ComputedFieldKind::Expression;
        column.expression = expression;
        column.dependencies.factFields = {{dependencyTable, dependencyField}};
        return column;
    }

    sc::SCComputedColumnDef MakeExpressionComputedColumn(const wchar_t* name,
                                                         const wchar_t* expression,
                                                         const wchar_t* dependencyField)
    {
        return MakeExpressionComputedColumnForTable(name,
                                                    expression,
                                                    L"Beam",
                                                    dependencyField);
    }

    sc::SCColumnDef MakeBusinessKeyRelationColumn(const wchar_t* name,
                                                  const wchar_t* referenceTable,
                                                  const wchar_t* storageColumn,
                                                  const wchar_t* displayColumn)
    {
        sc::SCColumnDef column;
        column.name = name;
        column.displayName = name;
        column.valueKind = sc::ValueKind::String;
        column.columnKind = sc::ColumnKind::Relation;
        column.referenceTable = referenceTable;
        column.referenceStorageColumn = storageColumn;
        column.referenceDisplayColumn = displayColumn;
        column.defaultValue = sc::SCValue::FromString(L"");
        return column;
    }

    sc::SCColumnDef MakeRecordIdRelationColumn(const wchar_t* name,
                                               const wchar_t* referenceTable)
    {
        sc::SCColumnDef column;
        column.name = name;
        column.displayName = name;
        column.valueKind = sc::ValueKind::RecordId;
        column.columnKind = sc::ColumnKind::Relation;
        column.referenceTable = referenceTable;
        column.defaultValue = sc::SCValue::FromRecordId(0);
        return column;
    }

    void AddUniqueConstraint(sc::SCSchemaPtr schema, const wchar_t* name,
                             const wchar_t* columnName)
    {
        sc::SCConstraintDef constraint;
        constraint.kind = sc::SCConstraintKind::Unique;
        constraint.name = name;
        constraint.columns.push_back(columnName);
        EXPECT_EQ(schema->AddConstraint(constraint), sc::SC_OK);
    }

    std::vector<std::wstring> CollectColumnNames(const QVector<sc::SCColumnDef>& columns)
    {
        std::vector<std::wstring> names;
        for (const sc::SCColumnDef& column : columns)
        {
            names.push_back(column.name);
        }
        return names;
    }

    std::wstring FirstFactDependencyTableName(const sc::SCComputedColumnDef& column)
    {
        return column.dependencies.factFields.empty()
                   ? std::wstring()
                   : column.dependencies.factFields.front().tableName;
    }

    bool LoadSessionComputedColumnOnTable(editor::SCDatabaseSession& session,
                                          const QString& tableName,
                                          const QString& columnName,
                                          sc::SCComputedColumnDef* outColumn,
                                          QString* outError)
    {
        if (!session.SelectTable(tableName, outError))
        {
            return false;
        }
        return session.GetSessionComputedColumn(columnName, outColumn, outError);
    }

    bool ReadCurrentComputedInt64(editor::SCDatabaseSession& session,
                                  sc::RecordId recordId,
                                  const wchar_t* columnName,
                                  std::int64_t* outValue)
    {
        if (outValue == nullptr || session.CurrentTableView() == nullptr)
        {
            return false;
        }

        sc::SCValue value;
        if (session.CurrentTableView()->GetCellValue(recordId, columnName, &value) !=
            sc::SC_OK)
        {
            return false;
        }

        return value.AsInt64(outValue) == sc::SC_OK;
    }

}  // namespace

TEST(DatabaseEditorSession, ExecuteBatchEditCreatesRecordsWithValues)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_BatchImport.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Id"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeStringColumn(L"Code"), &error)) << error.toStdString();

    std::vector<sc::SCBatchTableRequest> requests;
    sc::SCBatchTableRequest request;
    request.tableName = L"Beam";
    sc::SCBatchCreateRecordRequest create;
    create.values.push_back({L"Id", sc::SCValue::FromInt64(7)});
    create.values.push_back({L"Code", sc::SCValue::FromString(L"Beam-A")});
    request.creates.push_back(create);
    requests.push_back(request);

    sc::SCBatchExecutionOptions options;
    options.editName = L"Import CSV";
    options.rollbackOnError = true;

    sc::SCBatchExecutionResult result;
    const sc::ErrorCode rc = sc::ExecuteBatchEdit(session.Database(), requests, options, &result);
    ASSERT_EQ(rc, sc::SC_OK);
    EXPECT_EQ(result.createdCount, 1u);

    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordCursorPtr cursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    sc::SCRecordPtr record;
    ASSERT_EQ(cursor->Next(record), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(record));

    std::int64_t id = 0;
    ASSERT_EQ(record->GetInt64(L"Id", &id), sc::SC_OK);
    EXPECT_EQ(id, 7);

    std::wstring code;
    ASSERT_EQ(record->GetStringCopy(L"Code", &code), sc::SC_OK);
    EXPECT_EQ(code, L"Beam-A");
}

TEST(DatabaseEditorSession, AddColumnAllowsNonNullableColumnsWithoutDefaultOnEmptyTable)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_AddColumnRequiresDefault.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();

    EXPECT_TRUE(session.AddColumn(MakeRequiredIntColumnWithoutDefault(L"Width"), &error)) << error.toStdString();
}

TEST(DatabaseEditorSession, AddColumnRejectsNonNullableColumnsWithoutDefaultWhenTableHasRecords)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_AddColumnHasRecords.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Id"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();

    EXPECT_FALSE(session.AddColumn(MakeRequiredIntColumnWithoutDefault(L"Width"), &error));
    EXPECT_EQ(error, QStringLiteral("Storage error: 0xffffffffa0010008"));
}

TEST(DatabaseEditorSession, AddIndexPersistsAcrossReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_AddIndexReopen.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Id"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeStringColumn(L"Code"), &error)) << error.toStdString();

    sc::SCIndexDef index = MakeIndex(L"idx_Beam_Code", L"Code");
    ASSERT_TRUE(session.AddIndex(index, &error)) << error.toStdString();

    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCSchemaPtr schema;
    ASSERT_EQ(session.CurrentTable()->GetSchema(schema), sc::SC_OK);
    sc::SCIndexDef loadedIndex;
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Code", &loadedIndex), sc::SC_OK);
    ASSERT_EQ(loadedIndex.columns.size(), 1u);
    EXPECT_EQ(loadedIndex.columns[0].columnName, L"Code");

    editor::SCDatabaseSession reopened;
    ASSERT_TRUE(reopened.OpenDatabase(QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(reopened.SelectTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(reopened.CurrentTable() != nullptr);
    ASSERT_EQ(reopened.CurrentTable()->GetSchema(schema), sc::SC_OK);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Code", &loadedIndex), sc::SC_OK);
    ASSERT_EQ(loadedIndex.columns.size(), 1u);
    EXPECT_EQ(loadedIndex.columns[0].columnName, L"Code");
}

TEST(DatabaseEditorSession, AddIndexRejectsWhilePendingEditIsActive)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_AddIndexPendingEdit.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Id"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeStringColumn(L"Code"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeRequiredIntColumnWithoutDefault(L"Width"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();

    ASSERT_TRUE(session.HasPendingEdit());

    sc::SCIndexDef index = MakeIndex(L"idx_Beam_Code", L"Code");
    EXPECT_FALSE(session.AddIndex(index, &error));
    EXPECT_EQ(
        error,
        QStringLiteral(
            "A pending edit is active. Save or discard it before starting a new action."));

    ASSERT_TRUE(session.DiscardPendingChanges(&error)) << error.toStdString();
}

TEST(DatabaseEditorSession, AddRecordCreatesPendingEditForRequiredColumnsAndSaveRequiresValues)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_AddRecordNeedsRequiredValue.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeRequiredIntColumnWithoutDefault(L"Width"), &error)) << error.toStdString();

    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();

    EXPECT_TRUE(session.HasPendingEdit());

    EXPECT_FALSE(session.SavePendingChanges(&error));
    EXPECT_EQ(error, QStringLiteral("Storage error: 0xffffffffa0010008"));

    ASSERT_TRUE(session.DiscardPendingChanges(&error)) << error.toStdString();

    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordCursorPtr cursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    sc::SCRecordPtr record;
    ASSERT_EQ(cursor->Next(record), sc::SC_OK);
    EXPECT_FALSE(static_cast<bool>(record));
}

TEST(DatabaseEditorSession, AddRecordAllowsExplicitRequiredValuesBeforeSave)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_AddRecordRequiredSave.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeRequiredIntColumnWithoutDefault(L"Width"), &error)) << error.toStdString();

    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();
    ASSERT_TRUE(session.CurrentTable() != nullptr);

    sc::SCRecordCursorPtr cursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    sc::SCRecordPtr record;
    ASSERT_EQ(cursor->Next(record), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(record));
    ASSERT_TRUE(
        session.SetCellValue(record->GetId(), QStringLiteral("Width"), QVariant::fromValue<qlonglong>(42), &error))
        << error.toStdString();

    ASSERT_TRUE(session.SavePendingChanges(&error)) << error.toStdString();

    EXPECT_FALSE(session.HasPendingEdit());

    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    ASSERT_EQ(cursor->Next(record), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(record));
    std::int64_t width = 0;
    ASSERT_EQ(record->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 42);
}

TEST(DatabaseEditorSession, BinaryCellValuesRoundTripThroughEditorSession)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_BinaryRoundTrip.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeBinaryColumn(L"Attachment"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();

    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordCursorPtr cursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    sc::SCRecordPtr record;
    ASSERT_EQ(cursor->Next(record), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(record));

    const sc::RecordId recordId = record->GetId();
    ASSERT_TRUE(session.SetCellValue(recordId, QStringLiteral("Attachment"),
                                     QStringLiteral("0xAABBCC"), &error))
        << error.toStdString();

    QVariant displayValue;
    ASSERT_TRUE(session.GetCellDisplayValue(recordId, QStringLiteral("Attachment"),
                                            &displayValue, &error))
        << error.toStdString();
    EXPECT_EQ(displayValue.toString(), QStringLiteral("0xAABBCC"));

    QVariant storedValue;
    ASSERT_TRUE(session.GetCellStoredValue(recordId, QStringLiteral("Attachment"),
                                           &storedValue, &error))
        << error.toStdString();
    EXPECT_EQ(storedValue.toString(), QStringLiteral("0xAABBCC"));

    ASSERT_TRUE(session.SavePendingChanges(&error)) << error.toStdString();

    editor::SCDatabaseSession reopened;
    ASSERT_TRUE(reopened.OpenDatabase(QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(reopened.SelectTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(reopened.CurrentTable() != nullptr);
    ASSERT_EQ(reopened.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    ASSERT_EQ(cursor->Next(record), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(record));

    std::vector<std::uint8_t> bytes;
    EXPECT_EQ(record->GetBinaryCopy(L"Attachment", &bytes), sc::SC_OK);
    EXPECT_EQ(bytes, (std::vector<std::uint8_t>{0xAA, 0xBB, 0xCC}));
}

TEST(DatabaseEditorSession, CreateTableFromSchemaSupportsBinaryNotNullWithoutExplicitDefault)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_BinarySchemaImport.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();

    editor::SCSchemaTableImportResult schema;
    schema.tableMacroName = QStringLiteral("Beam");
    schema.tableName = QStringLiteral("Beam");

    sc::SCColumnDef attachment;
    attachment.name = L"Attachment";
    attachment.displayName = L"Attachment";
    attachment.valueKind = sc::ValueKind::Binary;
    attachment.nullable = false;
    schema.columns.push_back(attachment);

    ASSERT_TRUE(session.CreateTableFromSchema(schema, &error)) << error.toStdString();

    QVector<sc::SCColumnDef> columns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error)) << error.toStdString();
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].name, L"Attachment");
    EXPECT_EQ(columns[0].valueKind, sc::ValueKind::Binary);
    EXPECT_FALSE(columns[0].nullable);
}

TEST(DatabaseEditorSession, CreateTableFromSchemaImportsConstraintsAndIndexes)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_ConstraintIndexImport.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();

    ASSERT_TRUE(session.CreateTable(QStringLiteral("Floor"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeStringColumn(L"Code"), &error)) << error.toStdString();
    ASSERT_TRUE(session.CurrentTable() != nullptr);
    {
        sc::SCSchemaPtr floorSchema;
        ASSERT_EQ(session.CurrentTable()->GetSchema(floorSchema), sc::SC_OK);
        sc::SCConstraintDef uniqueConstraint;
        uniqueConstraint.kind = sc::SCConstraintKind::Unique;
        uniqueConstraint.name = L"uq_Floor_Code";
        uniqueConstraint.columns.push_back(L"Code");
        ASSERT_EQ(floorSchema->AddConstraint(uniqueConstraint), sc::SC_OK);
    }

    editor::SCSchemaTableImportResult schema;
    schema.tableMacroName = QStringLiteral("Beam");
    schema.tableName = QStringLiteral("Beam");

    sc::SCColumnDef id = MakeRequiredIntColumnWithoutDefault(L"Id");
    schema.columns.push_back(id);

    sc::SCColumnDef code = MakeStringColumn(L"Code");
    code.nullable = false;
    schema.columns.push_back(code);

    sc::SCColumnDef floorCode = MakeStringColumn(L"FloorCode");
    floorCode.nullable = false;
    schema.columns.push_back(floorCode);

    sc::SCConstraintDef primaryKey;
    primaryKey.kind = sc::SCConstraintKind::PrimaryKey;
    primaryKey.name = L"pk_Beam_Id";
    primaryKey.columns.push_back(L"Id");
    primaryKey.sourceKind = sc::SCSchemaSourceKind::Explicit;
    schema.constraints.push_back(primaryKey);

    sc::SCConstraintDef uniqueConstraint;
    uniqueConstraint.kind = sc::SCConstraintKind::Unique;
    uniqueConstraint.name = L"uq_Beam_Code";
    uniqueConstraint.columns.push_back(L"Code");
    uniqueConstraint.sourceKind = sc::SCSchemaSourceKind::Explicit;
    schema.constraints.push_back(uniqueConstraint);

    sc::SCConstraintDef checkConstraint;
    checkConstraint.kind = sc::SCConstraintKind::Check;
    checkConstraint.name = L"ck_Beam_Code";
    checkConstraint.columns.push_back(L"Code");
    checkConstraint.checkExpression = L"Code <> ''";
    checkConstraint.sourceKind = sc::SCSchemaSourceKind::Explicit;
    schema.constraints.push_back(checkConstraint);

    sc::SCConstraintDef foreignKey;
    foreignKey.kind = sc::SCConstraintKind::ForeignKey;
    foreignKey.name = L"fk_Beam_FloorCode";
    foreignKey.columns.push_back(L"FloorCode");
    foreignKey.referencedTable = L"Floor";
    foreignKey.referencedColumns.push_back(L"Code");
    foreignKey.onDelete = sc::SCForeignKeyAction::Cascade;
    foreignKey.onUpdate = sc::SCForeignKeyAction::Restrict;
    foreignKey.sourceKind = sc::SCSchemaSourceKind::Explicit;
    schema.constraints.push_back(foreignKey);

    editor::SCSchemaTableImportIndex index;
    index.name = QStringLiteral("idx_Beam_Code");
    index.columns.push_back(sc::SCIndexColumnDef{L"Code", false});
    schema.indexes.push_back(index);

    ASSERT_TRUE(session.CreateTableFromSchema(schema, &error)) << error.toStdString();
    ASSERT_TRUE(session.SelectTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.CurrentTable() != nullptr);

    sc::SCSchemaPtr beamSchema;
    ASSERT_EQ(session.CurrentTable()->GetSchema(beamSchema), sc::SC_OK);

    sc::SCConstraintDef loadedConstraint;
    EXPECT_EQ(beamSchema->FindConstraint(L"pk_Beam_Id", &loadedConstraint), sc::SC_OK);
    EXPECT_EQ(loadedConstraint.kind, sc::SCConstraintKind::PrimaryKey);

    EXPECT_EQ(beamSchema->FindConstraint(L"uq_Beam_Code", &loadedConstraint), sc::SC_OK);
    EXPECT_EQ(loadedConstraint.kind, sc::SCConstraintKind::Unique);

    EXPECT_EQ(beamSchema->FindConstraint(L"ck_Beam_Code", &loadedConstraint), sc::SC_OK);
    EXPECT_EQ(loadedConstraint.kind, sc::SCConstraintKind::Check);
    EXPECT_EQ(loadedConstraint.checkExpression, L"Code <> ''");

    EXPECT_EQ(beamSchema->FindConstraint(L"fk_Beam_FloorCode", &loadedConstraint), sc::SC_OK);
    EXPECT_EQ(loadedConstraint.kind, sc::SCConstraintKind::ForeignKey);
    EXPECT_EQ(loadedConstraint.referencedTable, L"Floor");
    EXPECT_EQ(loadedConstraint.referencedColumns,
              (std::vector<std::wstring>{L"Code"}));
    EXPECT_EQ(loadedConstraint.onDelete, sc::SCForeignKeyAction::Cascade);
    EXPECT_EQ(loadedConstraint.onUpdate, sc::SCForeignKeyAction::Restrict);

    sc::SCIndexDef loadedIndex;
    EXPECT_EQ(beamSchema->FindIndex(L"idx_Beam_Code", &loadedIndex), sc::SC_OK);
    ASSERT_EQ(loadedIndex.columns.size(), 1u);
    EXPECT_EQ(loadedIndex.columns[0].columnName, L"Code");
}

TEST(DatabaseEditorSession, CreateTableFromSchemaPreservesDescendingIndexColumns)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_DescendingIndexImport.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();

    editor::SCSchemaTableImportResult schema;
    schema.tableMacroName = QStringLiteral("Beam");
    schema.tableName = QStringLiteral("Beam");
    schema.columns.push_back(MakeStringColumn(L"Code"));
    schema.columns.push_back(MakeStringColumn(L"FloorCode"));

    editor::SCSchemaTableImportIndex index;
    index.name = QStringLiteral("idx_Beam_Code_FloorCode");
    index.columns.push_back(sc::SCIndexColumnDef{L"Code", false});
    index.columns.push_back(sc::SCIndexColumnDef{L"FloorCode", true});
    schema.indexes.push_back(index);

    ASSERT_TRUE(session.CreateTableFromSchema(schema, &error)) << error.toStdString();
    ASSERT_TRUE(session.CurrentTable() != nullptr);

    sc::SCSchemaPtr beamSchema;
    ASSERT_EQ(session.CurrentTable()->GetSchema(beamSchema), sc::SC_OK);

    sc::SCIndexDef loadedIndex;
    ASSERT_EQ(beamSchema->FindIndex(L"idx_Beam_Code_FloorCode", &loadedIndex),
              sc::SC_OK);
    ASSERT_EQ(loadedIndex.columns.size(), 2u);
    EXPECT_EQ(loadedIndex.columns[0].columnName, L"Code");
    EXPECT_FALSE(loadedIndex.columns[0].descending);
    EXPECT_EQ(loadedIndex.columns[1].columnName, L"FloorCode");
    EXPECT_TRUE(loadedIndex.columns[1].descending);
}

TEST(DatabaseEditorSession, CreateTableFromSchemaDeletesImportedTableWhenConstraintCreationFails)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_ConstraintImportRollback.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();

    editor::SCSchemaTableImportResult schema;
    schema.tableMacroName = QStringLiteral("Beam");
    schema.tableName = QStringLiteral("Beam");
    schema.columns.push_back(MakeRequiredIntColumnWithoutDefault(L"Id"));
    schema.columns.push_back(MakeIntColumn(L"FloorId"));

    sc::SCConstraintDef foreignKey;
    foreignKey.kind = sc::SCConstraintKind::ForeignKey;
    foreignKey.name = L"fk_Beam_FloorId";
    foreignKey.columns.push_back(L"FloorId");
    foreignKey.referencedTable = L"Floor";
    foreignKey.referencedColumns.push_back(L"Id");
    foreignKey.sourceKind = sc::SCSchemaSourceKind::Explicit;
    schema.constraints.push_back(foreignKey);

    EXPECT_FALSE(session.CreateTableFromSchema(schema, &error));

    ASSERT_TRUE(session.Database() != nullptr);
    sc::SCTablePtr beamTable;
    EXPECT_EQ(session.Database()->GetTable(L"Beam", beamTable),
              sc::SC_E_TABLE_NOT_FOUND);
}

TEST(DatabaseEditorSession, RelationFieldUsesConfiguredStorageAndDisplayColumns)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_RelationBusinessKey.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Floor"), &error)) << error.toStdString();
    {
        sc::SCColumnDef code;
        code.name = L"Code";
        code.displayName = L"Code";
        code.valueKind = sc::ValueKind::String;
        code.nullable = false;
        code.defaultValue = sc::SCValue::FromString(L"");
        ASSERT_TRUE(session.AddColumn(code, &error)) << error.toStdString();
    }
    ASSERT_TRUE(session.AddColumn(MakeStringColumn(L"Name"), &error)) << error.toStdString();
    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCSchemaPtr floorSchema;
    ASSERT_EQ(session.CurrentTable()->GetSchema(floorSchema), sc::SC_OK);
    AddUniqueConstraint(floorSchema, L"uq_Floor_Code", L"Code");

    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeStringColumn(L"Name"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(
                    MakeBusinessKeyRelationColumn(L"FloorRef", L"Floor", L"Code",
                                                  L"Name"),
                    &error))
        << error.toStdString();

    ASSERT_TRUE(session.SelectTable(QStringLiteral("Floor"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();

    sc::SCRecordCursorPtr cursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    sc::SCRecordPtr floorRecord;
    ASSERT_EQ(cursor->Next(floorRecord), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(floorRecord));
    ASSERT_TRUE(session.SetCellValue(floorRecord->GetId(), QStringLiteral("Code"),
                                     QStringLiteral("F-001"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.SetCellValue(floorRecord->GetId(), QStringLiteral("Name"),
                                     QStringLiteral("1F"), &error))
        << error.toStdString();

    const sc::RecordId floorRecordId = floorRecord->GetId();
    ASSERT_TRUE(session.SavePendingChanges(&error)) << error.toStdString();

    ASSERT_TRUE(session.SelectTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    sc::SCRecordPtr beamRecord;
    ASSERT_EQ(cursor->Next(beamRecord), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(beamRecord));
    ASSERT_TRUE(session.SetCellValue(beamRecord->GetId(), QStringLiteral("Name"),
                                     QStringLiteral("B-1"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.SetCellValue(beamRecord->GetId(), QStringLiteral("FloorRef"),
                                     QStringLiteral("F-001"), &error))
        << error.toStdString();

    ASSERT_TRUE(session.SavePendingChanges(&error)) << error.toStdString();

    QVariant displayValue;
    const sc::RecordId beamRecordId = beamRecord->GetId();
    ASSERT_TRUE(session.GetCellDisplayValue(beamRecordId,
                                            QStringLiteral("FloorRef"),
                                            &displayValue, &error))
        << error.toStdString();
    EXPECT_EQ(displayValue.toString(), QStringLiteral("1F (F-001)"));

    QVariant storedCellValue;
    ASSERT_TRUE(session.GetCellStoredValue(beamRecordId,
                                           QStringLiteral("FloorRef"),
                                           &storedCellValue, &error))
        << error.toStdString();
    EXPECT_EQ(storedCellValue.toString(), QStringLiteral("F-001"));

    sc::SCColumnDef relationColumn = MakeBusinessKeyRelationColumn(
        L"FloorRef", L"Floor", L"Code", L"Name");
    QVariant storedValue;
    ASSERT_TRUE(session.GetRelationStoredValue(floorRecordId,
                                               relationColumn, &storedValue,
                                               &error))
        << error.toStdString();
    EXPECT_EQ(storedValue.toString(), QStringLiteral("F-001"));
}

TEST(DatabaseEditorSession, RelationCandidatesSupportRecordIdBinding)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_RecordIdRelationCandidates.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Floor"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeStringColumn(L"Name"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();

    sc::SCRecordCursorPtr cursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    sc::SCRecordPtr floorRecord;
    ASSERT_EQ(cursor->Next(floorRecord), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(floorRecord));
    ASSERT_TRUE(session.SetCellValue(floorRecord->GetId(), QStringLiteral("Name"),
                                     QStringLiteral("1F"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.SavePendingChanges(&error)) << error.toStdString();

    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeRecordIdRelationColumn(L"FloorRef", L"Floor"), &error)) << error.toStdString();

    sc::SCColumnDef relationColumn = MakeRecordIdRelationColumn(L"FloorRef", L"Floor");
    QVector<editor::SCDatabaseSession::RelationCandidate> candidates;
    ASSERT_TRUE(session.BuildRelationCandidates(QStringLiteral("Floor"), relationColumn,
                                                &candidates, &error))
        << error.toStdString();
    ASSERT_FALSE(candidates.isEmpty());

    const auto& candidate = candidates[0];
    ASSERT_GE(candidate.previewFields.size(), 2);
    EXPECT_EQ(candidate.previewFields[0].first, QStringLiteral("RecordId"));
    EXPECT_EQ(candidate.previewFields[0].second,
              QString::number(static_cast<qlonglong>(candidate.recordId)));
    EXPECT_EQ(candidate.previewFields[1].first, QStringLiteral("StoredValue"));
    EXPECT_EQ(candidate.previewFields[1].second,
              QString::number(static_cast<qlonglong>(candidate.recordId)));
}

TEST(DatabaseEditorSession, RecordSnapshotStartsWithRecordId)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_RecordSnapshotRecordId.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeStringColumn(L"Name"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();

    sc::SCRecordCursorPtr cursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    sc::SCRecordPtr record;
    ASSERT_EQ(cursor->Next(record), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(record));

    QVector<QPair<QString, QString>> fields;
    ASSERT_TRUE(session.BuildRecordSnapshot(record->GetId(), &fields, &error)) << error.toStdString();
    ASSERT_FALSE(fields.isEmpty());
    EXPECT_EQ(fields[0].first, QStringLiteral("RecordId"));
    EXPECT_EQ(fields[0].second,
              QString::number(static_cast<qlonglong>(record->GetId())));
}

TEST(DatabaseEditorSession, PendingRequiredEditBlocksTableSwitchUntilSaved)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_AddRecordBlocksTableSwitch.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeRequiredIntColumnWithoutDefault(L"Width"), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Other"), &error)) << error.toStdString();

    ASSERT_TRUE(session.SelectTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();

    EXPECT_FALSE(session.SelectTable(QStringLiteral("Other"), &error));
    EXPECT_EQ(error,
              QStringLiteral("Save or discard pending changes before switching "
                             "tables."));

    ASSERT_TRUE(session.DiscardPendingChanges(&error)) << error.toStdString();
    ASSERT_TRUE(session.SelectTable(QStringLiteral("Other"), &error)) << error.toStdString();
}

TEST(DatabaseEditorSession, ExecuteBatchEditRollsBackOnInvalidAssignment)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_BatchRollback.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Id"), &error)) << error.toStdString();

    std::vector<sc::SCBatchTableRequest> requests;
    sc::SCBatchTableRequest validRequest;
    validRequest.tableName = L"Beam";
    sc::SCBatchCreateRecordRequest validCreate;
    validCreate.values.push_back({L"Id", sc::SCValue::FromInt64(1)});
    validRequest.creates.push_back(validCreate);
    requests.push_back(validRequest);

    sc::SCBatchTableRequest invalidRequest;
    invalidRequest.tableName = L"Beam";
    sc::SCBatchCreateRecordRequest invalidCreate;
    invalidCreate.values.push_back({L"Missing", sc::SCValue::FromInt64(2)});
    invalidRequest.creates.push_back(invalidCreate);
    requests.push_back(invalidRequest);

    sc::SCBatchExecutionOptions options;
    options.editName = L"Import CSV";
    options.rollbackOnError = true;

    sc::SCBatchExecutionResult result;
    const sc::ErrorCode rc = sc::ExecuteBatchEdit(session.Database(), requests, options, &result);
    ASSERT_TRUE(sc::Failed(rc));

    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordCursorPtr cursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    sc::SCRecordPtr record;
    ASSERT_EQ(cursor->Next(record), sc::SC_OK);
    EXPECT_FALSE(static_cast<bool>(record));
}

TEST(DatabaseEditorSession, TableSelectionAndSchemaRemainAlignedAcrossCreateAndReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_SessionSchema.sqlite");

    {
        editor::SCDatabaseSession session;
        QString error;

        ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();

        ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
        EXPECT_EQ(session.CurrentTableName(), QStringLiteral("Beam"));

        ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Id"), &error)) << error.toStdString();
        ASSERT_TRUE(session.AddColumn(MakeStringColumn(L"Code"), &error)) << error.toStdString();
        ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Length"), &error)) << error.toStdString();
        ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();
        ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Area"), &error)) << error.toStdString();
        ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();

        ASSERT_TRUE(session.CreateTable(QStringLiteral("Colum"), &error)) << error.toStdString();
        EXPECT_EQ(session.CurrentTableName(), QStringLiteral("Colum"));

        QVector<sc::SCColumnDef> columColumns;
        ASSERT_TRUE(session.BuildSchemaSnapshot(&columColumns, &error)) << error.toStdString();
        EXPECT_TRUE(columColumns.isEmpty());

        ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"ColumnId"), &error)) << error.toStdString();

        QVector<sc::SCColumnDef> beamColumns;
        ASSERT_TRUE(session.SelectTable(QStringLiteral("Beam"), &error)) << error.toStdString();
        ASSERT_TRUE(session.BuildSchemaSnapshot(&beamColumns, &error)) << error.toStdString();
        EXPECT_EQ(CollectColumnNames(beamColumns),
                  (std::vector<std::wstring>{L"Id", L"Code", L"Length", L"Width", L"Area"}));

        ASSERT_TRUE(session.SelectTable(QStringLiteral("Colum"), &error)) << error.toStdString();
        ASSERT_TRUE(session.BuildSchemaSnapshot(&columColumns, &error)) << error.toStdString();
        EXPECT_EQ(CollectColumnNames(columColumns), (std::vector<std::wstring>{L"ColumnId"}));
    }

    {
        editor::SCDatabaseSession session;
        QString error;

        ASSERT_TRUE(session.OpenDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();

        QVector<sc::SCColumnDef> beamColumns;
        ASSERT_TRUE(session.SelectTable(QStringLiteral("Beam"), &error)) << error.toStdString();
        ASSERT_TRUE(session.BuildSchemaSnapshot(&beamColumns, &error)) << error.toStdString();
        EXPECT_EQ(CollectColumnNames(beamColumns),
                  (std::vector<std::wstring>{L"Id", L"Code", L"Length", L"Width", L"Area"}));

        QVector<sc::SCColumnDef> columColumns;
        ASSERT_TRUE(session.SelectTable(QStringLiteral("Colum"), &error)) << error.toStdString();
        ASSERT_TRUE(session.BuildSchemaSnapshot(&columColumns, &error)) << error.toStdString();
        EXPECT_EQ(CollectColumnNames(columColumns), (std::vector<std::wstring>{L"ColumnId"}));
    }
}

TEST(DatabaseEditorSession, EditColumnUpdatesSchemaSnapshotInPlace)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_EditColumn.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();

    sc::SCColumnDef updated = MakeStringColumn(L"Width");
    updated.displayName = L"Width Label";
    updated.nullable = false;
    updated.defaultValue = sc::SCValue::FromString(L"0");

    ASSERT_TRUE(session.UpdateColumn(QStringLiteral("Width"), updated, &error)) << error.toStdString();

    QVector<sc::SCColumnDef> columns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error)) << error.toStdString();
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].name, L"Width");
    EXPECT_EQ(columns[0].displayName, L"Width Label");
    EXPECT_EQ(columns[0].valueKind, sc::ValueKind::String);
    EXPECT_FALSE(columns[0].nullable);

    ASSERT_TRUE(session.CurrentTableView() != nullptr);
    std::int32_t columnCount = 0;
    ASSERT_EQ(session.CurrentTableView()->GetColumnCount(&columnCount), sc::SC_OK);
    EXPECT_EQ(columnCount, 1);
}

TEST(DatabaseEditorSession, UpdateColumnAllowsRenamingFieldAndPreservesValues)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_RenameColumn.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    sc::SCColumnDef width = MakeIntColumn(L"Width");
    width.nullable = true;
    ASSERT_TRUE(session.AddColumn(width, &error)) << error.toStdString();

    sc::SCEditPtr edit;
    ASSERT_EQ(session.Database()->BeginEdit(L"seed", edit), sc::SC_OK);
    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordPtr record;
    ASSERT_EQ(session.CurrentTable()->CreateRecord(record), sc::SC_OK);
    const sc::RecordId recordId = record->GetId();
    ASSERT_EQ(record->SetInt64(L"Width", 128), sc::SC_OK);
    ASSERT_EQ(session.Database()->Commit(edit.Get()), sc::SC_OK);

    sc::SCColumnDef updated = MakeIntColumn(L"Length");
    updated.displayName = L"Length";
    updated.nullable = true;
    updated.defaultValue = sc::SCValue::Null();
    ASSERT_TRUE(session.UpdateColumn(QStringLiteral("Width"), updated, &error))
        << error.toStdString();

    QVector<sc::SCColumnDef> columns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error)) << error.toStdString();
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].name, L"Length");

    sc::SCRecordPtr reloaded;
    ASSERT_EQ(session.CurrentTable()->GetRecord(recordId, reloaded), sc::SC_OK);
    std::int64_t length = 0;
    ASSERT_EQ(reloaded->GetInt64(L"Length", &length), sc::SC_OK);
    EXPECT_EQ(length, 128);
}

TEST(DatabaseEditorSession, UpdateColumnRenamesReferencesInConstraintsIndexesAndRelations)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_RenameColumnReferences.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();

    sc::SCColumnDef code = MakeStringColumn(L"Code");
    code.nullable = false;
    ASSERT_TRUE(session.AddColumn(code, &error)) << error.toStdString();
    ASSERT_TRUE(session.AddIndex(MakeIndex(L"idx_Beam_Code", L"Code"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CurrentTable() != nullptr);
    {
        sc::SCSchemaPtr beamSchema;
        ASSERT_EQ(session.CurrentTable()->GetSchema(beamSchema), sc::SC_OK);
        AddUniqueConstraint(beamSchema, L"uq_Beam_Code", L"Code");
    }

    ASSERT_TRUE(session.CreateTable(QStringLiteral("Floor"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(
                    MakeBusinessKeyRelationColumn(L"BeamRef", L"Beam", L"Code",
                                                  L"Code"),
                    &error))
        << error.toStdString();

    ASSERT_TRUE(session.SelectTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    sc::SCColumnDef renamed = MakeStringColumn(L"BeamCode");
    renamed.nullable = false;
    ASSERT_TRUE(session.UpdateColumn(QStringLiteral("Code"), renamed, &error))
        << error.toStdString();

    QVector<sc::SCColumnDef> columns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error)) << error.toStdString();
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].name, L"BeamCode");

    sc::SCSchemaPtr beamSchema;
    ASSERT_TRUE(session.CurrentTable() != nullptr);
    ASSERT_EQ(session.CurrentTable()->GetSchema(beamSchema), sc::SC_OK);
    sc::SCTableSchemaSnapshot beamSnapshot;
    ASSERT_EQ(beamSchema->GetSchemaSnapshot(&beamSnapshot), sc::SC_OK);

    ASSERT_EQ(beamSnapshot.constraints.size(), 1u);
    EXPECT_EQ(beamSnapshot.constraints[0].name, L"uq_Beam_Code");
    ASSERT_EQ(beamSnapshot.constraints[0].columns.size(), 1u);
    EXPECT_EQ(beamSnapshot.constraints[0].columns[0], L"BeamCode");

    ASSERT_EQ(beamSnapshot.indexes.size(), 1u);
    EXPECT_EQ(beamSnapshot.indexes[0].name, L"idx_Beam_Code");
    ASSERT_EQ(beamSnapshot.indexes[0].columns.size(), 1u);
    EXPECT_EQ(beamSnapshot.indexes[0].columns[0].columnName, L"BeamCode");

    ASSERT_TRUE(session.SelectTable(QStringLiteral("Floor"), &error)) << error.toStdString();
    sc::SCSchemaPtr floorSchema;
    ASSERT_TRUE(session.CurrentTable() != nullptr);
    ASSERT_EQ(session.CurrentTable()->GetSchema(floorSchema), sc::SC_OK);
    sc::SCTableSchemaSnapshot floorSnapshot;
    ASSERT_EQ(floorSchema->GetSchemaSnapshot(&floorSnapshot), sc::SC_OK);

    ASSERT_EQ(floorSnapshot.columns.size(), 1u);
    EXPECT_EQ(floorSnapshot.columns[0].referenceTable, L"Beam");
    EXPECT_EQ(floorSnapshot.columns[0].referenceStorageColumn, L"BeamCode");
    EXPECT_EQ(floorSnapshot.columns[0].referenceDisplayColumn, L"BeamCode");
}

TEST(DatabaseEditorSession, UpdateColumnRejectsNonNullableWhenNullValuesExist)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_NullableGuard.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();

    sc::SCColumnDef width;
    width.name = L"Width";
    width.displayName = L"Width";
    width.valueKind = sc::ValueKind::Int64;
    width.nullable = true;
    width.defaultValue = sc::SCValue::Null();
    ASSERT_TRUE(session.AddColumn(width, &error)) << error.toStdString();

    sc::SCEditPtr edit;
    ASSERT_EQ(session.Database()->BeginEdit(L"seed", edit), sc::SC_OK);
    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordPtr record;
    ASSERT_EQ(session.CurrentTable()->CreateRecord(record), sc::SC_OK);
    ASSERT_EQ(session.Database()->Commit(edit.Get()), sc::SC_OK);

    sc::SCColumnDef requiredWidth = width;
    requiredWidth.nullable = false;
    ASSERT_FALSE(session.UpdateColumn(QStringLiteral("Width"), requiredWidth, &error));
    EXPECT_NE(error.indexOf(QStringLiteral("空值"), 0, Qt::CaseInsensitive), -1);
}

TEST(DatabaseEditorSession, RenameTableUpdatesSelectionAndPreservesData)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_RenameTable.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();

    sc::SCEditPtr edit;
    ASSERT_EQ(session.Database()->BeginEdit(L"seed", edit), sc::SC_OK);
    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordPtr record;
    ASSERT_EQ(session.CurrentTable()->CreateRecord(record), sc::SC_OK);
    const sc::RecordId recordId = record->GetId();
    ASSERT_EQ(record->SetInt64(L"Width", 256), sc::SC_OK);
    ASSERT_EQ(session.Database()->Commit(edit.Get()), sc::SC_OK);

    ASSERT_TRUE(session.RenameTable(QStringLiteral("Beam"), QStringLiteral("BeamRenamed"), &error))
        << error.toStdString();
    EXPECT_EQ(session.CurrentTableName(), QStringLiteral("BeamRenamed"));
    EXPECT_EQ(session.TableNames(), (QStringList{QStringLiteral("BeamRenamed")}));

    QVector<sc::SCColumnDef> columns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error)) << error.toStdString();
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].name, L"Width");

    sc::SCRecordPtr reloaded;
    ASSERT_EQ(session.CurrentTable()->GetRecord(recordId, reloaded), sc::SC_OK);
    std::int64_t width = 0;
    ASSERT_EQ(reloaded->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 256);
}

TEST(DatabaseEditorSession, RenameTableRollsBackWhenViewRebuildFails)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_RenameTableRollback.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();

    session.SetForceRebuildCurrentTableViewFailureForTest(true);
    EXPECT_FALSE(session.RenameTable(QStringLiteral("Beam"),
                                     QStringLiteral("BeamRenamed"),
                                     &error));
    EXPECT_EQ(error, QStringLiteral("Forced rebuild failure for test."));
    session.SetForceRebuildCurrentTableViewFailureForTest(false);

    EXPECT_EQ(session.CurrentTableName(), QStringLiteral("Beam"));
    EXPECT_EQ(session.TableNames(), (QStringList{QStringLiteral("Beam")}));

    QVector<sc::SCColumnDef> columns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error)) << error.toStdString();
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].name, L"Width");

    ASSERT_TRUE(session.SelectTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    EXPECT_FALSE(session.SelectTable(QStringLiteral("BeamRenamed"), &error));
}

TEST(DatabaseEditorSession, RenameTableParticipatesInUndoRedo)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_RenameTableUndoRedo.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();

    ASSERT_TRUE(session.RenameTable(QStringLiteral("Beam"),
                                    QStringLiteral("BeamRenamed"),
                                    &error)) << error.toStdString();
    EXPECT_EQ(session.CurrentTableName(), QStringLiteral("BeamRenamed"));
    EXPECT_EQ(session.TableNames(), (QStringList{QStringLiteral("BeamRenamed")}));

    ASSERT_TRUE(session.Undo(&error)) << error.toStdString();
    EXPECT_EQ(session.CurrentTableName(), QStringLiteral("Beam"));
    EXPECT_EQ(session.TableNames(), (QStringList{QStringLiteral("Beam")}));

    QVector<sc::SCColumnDef> columns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error)) << error.toStdString();
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].name, L"Width");

    ASSERT_TRUE(session.Redo(&error)) << error.toStdString();
    EXPECT_EQ(session.CurrentTableName(), QStringLiteral("BeamRenamed"));
    EXPECT_EQ(session.TableNames(), (QStringList{QStringLiteral("BeamRenamed")}));

    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error)) << error.toStdString();
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].name, L"Width");
}

TEST(DatabaseEditorSession, RenameTableUndoRedoKeepsCurrentSelectionAcrossMultipleTables)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_RenameTableSelection.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Floor"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Height"), &error)) << error.toStdString();
    ASSERT_TRUE(session.SelectTable(QStringLiteral("Floor"), &error)) << error.toStdString();

    ASSERT_TRUE(session.RenameTable(QStringLiteral("Floor"),
                                    QStringLiteral("FloorRenamed"),
                                    &error)) << error.toStdString();
    EXPECT_EQ(session.CurrentTableName(), QStringLiteral("FloorRenamed"));
    EXPECT_EQ(session.TableNames(),
              (QStringList{QStringLiteral("Beam"), QStringLiteral("FloorRenamed")}));

    ASSERT_TRUE(session.Undo(&error)) << error.toStdString();
    EXPECT_EQ(session.CurrentTableName(), QStringLiteral("Floor"));
    EXPECT_EQ(session.TableNames(),
              (QStringList{QStringLiteral("Beam"), QStringLiteral("Floor")}));

    ASSERT_TRUE(session.Redo(&error)) << error.toStdString();
    EXPECT_EQ(session.CurrentTableName(), QStringLiteral("FloorRenamed"));
    EXPECT_EQ(session.TableNames(),
              (QStringList{QStringLiteral("Beam"), QStringLiteral("FloorRenamed")}));
}

TEST(DatabaseEditorSession, RenameTableUndoRedoRewritesComputedColumnsAcrossTables)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_RenameTableComputedColumns.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();
    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordCursorPtr beamCursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(beamCursor), sc::SC_OK);
    sc::SCRecordPtr beamRecord;
    ASSERT_EQ(beamCursor->Next(beamRecord), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(beamRecord));
    const sc::RecordId beamRecordId = beamRecord->GetId();
    ASSERT_TRUE(session.SetCellValue(beamRecordId,
                                     QStringLiteral("Width"),
                                     QVariant::fromValue<qlonglong>(7),
                                     &error))
        << error.toStdString();

    ASSERT_TRUE(session.CreateTable(QStringLiteral("Floor"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Height"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();
    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordCursorPtr floorCursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(floorCursor), sc::SC_OK);
    sc::SCRecordPtr floorRecord;
    ASSERT_EQ(floorCursor->Next(floorRecord), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(floorRecord));
    const sc::RecordId floorRecordId = floorRecord->GetId();
    ASSERT_TRUE(session.SetCellValue(floorRecordId,
                                     QStringLiteral("Height"),
                                     QVariant::fromValue<qlonglong>(5),
                                     &error))
        << error.toStdString();

    ASSERT_TRUE(session.SelectTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    sc::SCComputedColumnDef beamComputed =
        MakeExpressionComputedColumn(L"BeamScaled", L"Width * 2", L"Width");
    ASSERT_TRUE(session.AddSessionComputedColumn(beamComputed, &error)) << error.toStdString();

    ASSERT_TRUE(session.SelectTable(QStringLiteral("Floor"), &error)) << error.toStdString();
    sc::SCComputedColumnDef floorComputed =
        MakeExpressionComputedColumnForTable(L"FloorScaled",
                                             L"Height * 3",
                                             L"Floor",
                                             L"Height");
    ASSERT_TRUE(session.AddSessionComputedColumn(floorComputed, &error)) << error.toStdString();

    sc::SCComputedColumnDef loaded;
    std::int64_t computedValue = 0;
    ASSERT_TRUE(LoadSessionComputedColumnOnTable(session,
                                                 QStringLiteral("Beam"),
                                                 QStringLiteral("BeamScaled"),
                                                 &loaded,
                                                 &error))
        << error.toStdString();
    EXPECT_EQ(FirstFactDependencyTableName(loaded), L"Beam");
    ASSERT_TRUE(ReadCurrentComputedInt64(session,
                                         beamRecordId,
                                         L"BeamScaled",
                                         &computedValue));
    EXPECT_EQ(computedValue, 14);
    ASSERT_TRUE(LoadSessionComputedColumnOnTable(session,
                                                 QStringLiteral("Floor"),
                                                 QStringLiteral("FloorScaled"),
                                                 &loaded,
                                                 &error))
        << error.toStdString();
    EXPECT_EQ(FirstFactDependencyTableName(loaded), L"Floor");
    ASSERT_TRUE(ReadCurrentComputedInt64(session,
                                         floorRecordId,
                                         L"FloorScaled",
                                         &computedValue));
    EXPECT_EQ(computedValue, 15);

    ASSERT_TRUE(session.RenameTable(QStringLiteral("Beam"),
                                    QStringLiteral("BeamRenamed"),
                                    &error)) << error.toStdString();

    ASSERT_TRUE(LoadSessionComputedColumnOnTable(session,
                                                 QStringLiteral("BeamRenamed"),
                                                 QStringLiteral("BeamScaled"),
                                                 &loaded,
                                                 &error))
        << error.toStdString();
    EXPECT_EQ(FirstFactDependencyTableName(loaded), L"BeamRenamed");
    ASSERT_TRUE(ReadCurrentComputedInt64(session,
                                         beamRecordId,
                                         L"BeamScaled",
                                         &computedValue));
    EXPECT_EQ(computedValue, 14);
    ASSERT_TRUE(LoadSessionComputedColumnOnTable(session,
                                                 QStringLiteral("Floor"),
                                                 QStringLiteral("FloorScaled"),
                                                 &loaded,
                                                 &error))
        << error.toStdString();
    EXPECT_EQ(FirstFactDependencyTableName(loaded), L"Floor");
    ASSERT_TRUE(ReadCurrentComputedInt64(session,
                                         floorRecordId,
                                         L"FloorScaled",
                                         &computedValue));
    EXPECT_EQ(computedValue, 15);

    ASSERT_TRUE(session.RenameTable(QStringLiteral("Floor"),
                                    QStringLiteral("FloorRenamed"),
                                    &error)) << error.toStdString();

    ASSERT_TRUE(LoadSessionComputedColumnOnTable(session,
                                                 QStringLiteral("BeamRenamed"),
                                                 QStringLiteral("BeamScaled"),
                                                 &loaded,
                                                 &error))
        << error.toStdString();
    EXPECT_EQ(FirstFactDependencyTableName(loaded), L"BeamRenamed");
    ASSERT_TRUE(ReadCurrentComputedInt64(session,
                                         beamRecordId,
                                         L"BeamScaled",
                                         &computedValue));
    EXPECT_EQ(computedValue, 14);
    ASSERT_TRUE(LoadSessionComputedColumnOnTable(session,
                                                 QStringLiteral("FloorRenamed"),
                                                 QStringLiteral("FloorScaled"),
                                                 &loaded,
                                                 &error))
        << error.toStdString();
    EXPECT_EQ(FirstFactDependencyTableName(loaded), L"FloorRenamed");
    ASSERT_TRUE(ReadCurrentComputedInt64(session,
                                         floorRecordId,
                                         L"FloorScaled",
                                         &computedValue));
    EXPECT_EQ(computedValue, 15);

    ASSERT_TRUE(session.Undo(&error)) << error.toStdString();
    ASSERT_TRUE(LoadSessionComputedColumnOnTable(session,
                                                 QStringLiteral("BeamRenamed"),
                                                 QStringLiteral("BeamScaled"),
                                                 &loaded,
                                                 &error))
        << error.toStdString();
    EXPECT_EQ(FirstFactDependencyTableName(loaded), L"BeamRenamed");
    ASSERT_TRUE(ReadCurrentComputedInt64(session,
                                         beamRecordId,
                                         L"BeamScaled",
                                         &computedValue));
    EXPECT_EQ(computedValue, 14);
    ASSERT_TRUE(LoadSessionComputedColumnOnTable(session,
                                                 QStringLiteral("Floor"),
                                                 QStringLiteral("FloorScaled"),
                                                 &loaded,
                                                 &error))
        << error.toStdString();
    EXPECT_EQ(FirstFactDependencyTableName(loaded), L"Floor");
    ASSERT_TRUE(ReadCurrentComputedInt64(session,
                                         floorRecordId,
                                         L"FloorScaled",
                                         &computedValue));
    EXPECT_EQ(computedValue, 15);

    ASSERT_TRUE(session.Undo(&error)) << error.toStdString();
    ASSERT_TRUE(LoadSessionComputedColumnOnTable(session,
                                                 QStringLiteral("Beam"),
                                                 QStringLiteral("BeamScaled"),
                                                 &loaded,
                                                 &error))
        << error.toStdString();
    EXPECT_EQ(FirstFactDependencyTableName(loaded), L"Beam");
    ASSERT_TRUE(ReadCurrentComputedInt64(session,
                                         beamRecordId,
                                         L"BeamScaled",
                                         &computedValue));
    EXPECT_EQ(computedValue, 14);
    ASSERT_TRUE(LoadSessionComputedColumnOnTable(session,
                                                 QStringLiteral("Floor"),
                                                 QStringLiteral("FloorScaled"),
                                                 &loaded,
                                                 &error))
        << error.toStdString();
    EXPECT_EQ(FirstFactDependencyTableName(loaded), L"Floor");
    ASSERT_TRUE(ReadCurrentComputedInt64(session,
                                         floorRecordId,
                                         L"FloorScaled",
                                         &computedValue));
    EXPECT_EQ(computedValue, 15);

    ASSERT_TRUE(session.Redo(&error)) << error.toStdString();
    ASSERT_TRUE(LoadSessionComputedColumnOnTable(session,
                                                 QStringLiteral("BeamRenamed"),
                                                 QStringLiteral("BeamScaled"),
                                                 &loaded,
                                                 &error))
        << error.toStdString();
    EXPECT_EQ(FirstFactDependencyTableName(loaded), L"BeamRenamed");
    ASSERT_TRUE(ReadCurrentComputedInt64(session,
                                         beamRecordId,
                                         L"BeamScaled",
                                         &computedValue));
    EXPECT_EQ(computedValue, 14);
    ASSERT_TRUE(LoadSessionComputedColumnOnTable(session,
                                                 QStringLiteral("Floor"),
                                                 QStringLiteral("FloorScaled"),
                                                 &loaded,
                                                 &error))
        << error.toStdString();
    EXPECT_EQ(FirstFactDependencyTableName(loaded), L"Floor");
    ASSERT_TRUE(ReadCurrentComputedInt64(session,
                                         floorRecordId,
                                         L"FloorScaled",
                                         &computedValue));
    EXPECT_EQ(computedValue, 15);

    ASSERT_TRUE(session.Redo(&error)) << error.toStdString();
    ASSERT_TRUE(LoadSessionComputedColumnOnTable(session,
                                                 QStringLiteral("BeamRenamed"),
                                                 QStringLiteral("BeamScaled"),
                                                 &loaded,
                                                 &error))
        << error.toStdString();
    EXPECT_EQ(FirstFactDependencyTableName(loaded), L"BeamRenamed");
    ASSERT_TRUE(ReadCurrentComputedInt64(session,
                                         beamRecordId,
                                         L"BeamScaled",
                                         &computedValue));
    EXPECT_EQ(computedValue, 14);
    ASSERT_TRUE(LoadSessionComputedColumnOnTable(session,
                                                 QStringLiteral("FloorRenamed"),
                                                 QStringLiteral("FloorScaled"),
                                                 &loaded,
                                                 &error))
        << error.toStdString();
    EXPECT_EQ(FirstFactDependencyTableName(loaded), L"FloorRenamed");
    ASSERT_TRUE(ReadCurrentComputedInt64(session,
                                         floorRecordId,
                                         L"FloorScaled",
                                         &computedValue));
    EXPECT_EQ(computedValue, 15);

    EXPECT_EQ(session.TableNames(),
              (QStringList{QStringLiteral("BeamRenamed"), QStringLiteral("FloorRenamed")}));
}

TEST(DatabaseEditorSession, SchemaSnapshotKeepsFieldNameAndDisplayNameDistinct)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_SchemaDisplayName.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();

    sc::SCColumnDef column = MakeIntColumn(L"Width");
    column.displayName = L"Beam Width";
    ASSERT_TRUE(session.AddColumn(column, &error)) << error.toStdString();

    QVector<sc::SCColumnDef> columns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error)) << error.toStdString();
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].name, L"Width");
    EXPECT_EQ(columns[0].displayName, L"Beam Width");
}

TEST(DatabaseEditorSession, GetTableColumnNamesReturnsLiveSchemaColumns)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_GetTableColumnNames.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Floor"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeStringColumn(L"Code"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeStringColumn(L"Name"), &error)) << error.toStdString();
    ASSERT_TRUE(session.CurrentTable() != nullptr);

    QStringList columns;
    ASSERT_TRUE(session.GetTableColumnNames(QStringLiteral("Floor"), &columns, &error))
        << error.toStdString();
    EXPECT_EQ(columns, (QStringList{QStringLiteral("Code"), QStringLiteral("Name")}));

    QStringList missingColumns;
    EXPECT_FALSE(session.GetTableColumnNames(QStringLiteral("Missing"), &missingColumns, &error));
    EXPECT_FALSE(error.isEmpty());
}

TEST(DatabaseEditorSession, CloseDatabaseClearsOpenState)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_CloseDatabase.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();
    ASSERT_TRUE(session.CurrentTableView() != nullptr);

    ASSERT_TRUE(session.CloseDatabase(&error)) << error.toStdString();
    EXPECT_TRUE(session.DatabasePath().isEmpty());
    EXPECT_TRUE(session.CurrentTableName().isEmpty());
    EXPECT_FALSE(session.IsOpen());
    EXPECT_EQ(session.CurrentTable(), nullptr);
    EXPECT_EQ(session.CurrentTableView(), nullptr);

    QVector<sc::SCColumnDef> columns;
    EXPECT_TRUE(session.BuildSchemaSnapshot(&columns, &error));
    EXPECT_TRUE(columns.isEmpty());
}

TEST(DatabaseEditorSession, AddColumnParticipatesInUndoRedo)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_AddColumnUndoRedo.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();

    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();

    QVector<sc::SCColumnDef> columns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error)) << error.toStdString();
    ASSERT_EQ(columns.size(), 1);

    ASSERT_TRUE(session.Undo(&error)) << error.toStdString();
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error)) << error.toStdString();
    ASSERT_TRUE(columns.isEmpty());

    ASSERT_TRUE(session.Redo(&error)) << error.toStdString();
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error)) << error.toStdString();
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].name, L"Width");
}

TEST(DatabaseEditorSession, CreateTableSelectsCurrentTableForEditing)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_CreateTableSelect.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();

    EXPECT_EQ(session.CurrentTableName(), QStringLiteral("Beam"));
    EXPECT_TRUE(session.CurrentTable() != nullptr);

    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();
}

TEST(DatabaseEditorSession, DeleteTableRemovesTableAndKeepsFallbackSelection)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_DeleteTable.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Colum"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeStringColumn(L"Name"), &error)) << error.toStdString();

    ASSERT_TRUE(session.SelectTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.DeleteTable(QStringLiteral("Beam"), &error)) << error.toStdString();

    EXPECT_EQ(session.CurrentTableName(), QStringLiteral("Colum"));
    EXPECT_EQ(session.TableNames(), (QStringList{QStringLiteral("Colum")}));

    QVector<sc::SCColumnDef> columns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error)) << error.toStdString();
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].name, L"Name");

    ASSERT_TRUE(session.CloseDatabase(&error)) << error.toStdString();
    ASSERT_TRUE(session.OpenDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();

    EXPECT_FALSE(session.SelectTable(QStringLiteral("Beam"), &error));
    ASSERT_TRUE(session.SelectTable(QStringLiteral("Colum"), &error)) << error.toStdString();
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error)) << error.toStdString();
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].name, L"Name");
}

TEST(DatabaseEditorSession, AddAndDeleteRecordStayOnCurrentTable)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_AddDeleteRecord.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();

    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();
    ASSERT_TRUE(session.CurrentTable() != nullptr);

    sc::SCRecordCursorPtr cursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    sc::SCRecordPtr record;
    ASSERT_EQ(cursor->Next(record), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(record));
    ASSERT_TRUE(session.DeleteRecord(record->GetId(), &error)) << error.toStdString();

    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    ASSERT_EQ(cursor->Next(record), sc::SC_OK);
    EXPECT_FALSE(static_cast<bool>(record));
}

TEST(DatabaseEditorSession, RemoveColumnParticipatesInUndoRedo)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_RemoveColumnUndoRedo.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Height"), &error)) << error.toStdString();

    ASSERT_TRUE(session.RemoveColumn(QStringLiteral("Width"), &error)) << error.toStdString();

    QVector<sc::SCColumnDef> columns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error)) << error.toStdString();
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].name, L"Height");

    ASSERT_TRUE(session.Undo(&error)) << error.toStdString();
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error)) << error.toStdString();
    EXPECT_EQ(CollectColumnNames(columns), (std::vector<std::wstring>{L"Width", L"Height"}));

    ASSERT_TRUE(session.Redo(&error)) << error.toStdString();
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error)) << error.toStdString();
    EXPECT_EQ(CollectColumnNames(columns), (std::vector<std::wstring>{L"Height"}));
}

TEST(DatabaseEditorSession, EditColumnMigratesCompatibleValues)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_EditColumnMigrate.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();

    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordCursorPtr cursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    sc::SCRecordPtr record;
    ASSERT_EQ(cursor->Next(record), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(record));
    const sc::RecordId recordId = record->GetId();
    ASSERT_TRUE(session.SetCellValue(recordId, QStringLiteral("Width"), QVariant::fromValue<qlonglong>(11), &error))
        << error.toStdString();

    sc::SCColumnDef updated = MakeStringColumn(L"Width");
    updated.displayName = L"Width Label";
    updated.defaultValue = sc::SCValue::FromString(L"0");
    ASSERT_TRUE(session.UpdateColumn(QStringLiteral("Width"), updated, &error)) << error.toStdString();

    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordPtr reloaded;
    ASSERT_EQ(session.CurrentTable()->GetRecord(recordId, reloaded), sc::SC_OK);

    std::wstring width;
    ASSERT_EQ(reloaded->GetStringCopy(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, L"11");

    QVector<sc::SCColumnDef> columns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error)) << error.toStdString();
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].valueKind, sc::ValueKind::String);
}

TEST(DatabaseEditorSession, EditColumnRollsBackWhenViewRebuildFails)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_EditColumnRollback.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();

    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordCursorPtr cursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    sc::SCRecordPtr record;
    ASSERT_EQ(cursor->Next(record), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(record));
    const sc::RecordId recordId = record->GetId();
    ASSERT_TRUE(session.SetCellValue(recordId, QStringLiteral("Width"), QVariant::fromValue<qlonglong>(17), &error))
        << error.toStdString();

    session.SetForceRebuildCurrentTableViewFailureForTest(true);
    sc::SCColumnDef updated = MakeStringColumn(L"Width");
    updated.defaultValue = sc::SCValue::FromString(L"0");
    EXPECT_FALSE(session.UpdateColumn(QStringLiteral("Width"), updated, &error));
    EXPECT_EQ(error, QStringLiteral("Forced rebuild failure for test."));
    session.SetForceRebuildCurrentTableViewFailureForTest(false);

    QVector<sc::SCColumnDef> columns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error)) << error.toStdString();
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].valueKind, sc::ValueKind::Int64);

    sc::SCRecordPtr reloaded;
    ASSERT_EQ(session.CurrentTable()->GetRecord(recordId, reloaded), sc::SC_OK);
    std::int64_t width = 0;
    ASSERT_EQ(reloaded->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 17);
}

TEST(DatabaseEditorSession, ComputedColumnWorkflowKeepsViewAndSessionAligned)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_ComputedWorkflow.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();

    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();
    ASSERT_TRUE(session.CurrentTableView() != nullptr);

    std::int32_t columnCount = 0;
    ASSERT_EQ(session.CurrentTableView()->GetColumnCount(&columnCount), sc::SC_OK);
    EXPECT_EQ(columnCount, 1);

    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Height"), &error)) << error.toStdString();
    ASSERT_EQ(session.CurrentTableView()->GetColumnCount(&columnCount), sc::SC_OK);
    EXPECT_EQ(columnCount, 2);

    sc::SCComputedColumnDef computed;
    computed.name = L"ScaledWidth";
    computed.displayName = L"ScaledWidth";
    computed.valueKind = sc::ValueKind::Double;
    computed.kind = sc::ComputedFieldKind::Expression;
    computed.expression = L"Width * 2";
    computed.dependencies.factFields = {{L"Beam", L"Width"}};

    ASSERT_TRUE(session.AddSessionComputedColumn(computed, &error)) << error.toStdString();
    ASSERT_EQ(session.CurrentSessionComputedColumns().size(), 1u);
    ASSERT_EQ(session.CurrentTableView()->GetColumnCount(&columnCount), sc::SC_OK);
    EXPECT_EQ(columnCount, 3);

    sc::SCComputedColumnDef loaded;
    ASSERT_TRUE(session.GetSessionComputedColumn(QStringLiteral("ScaledWidth"), &loaded, &error))
        << error.toStdString();
    EXPECT_EQ(loaded.name, L"ScaledWidth");
    EXPECT_EQ(loaded.expression, L"Width * 2");

    ASSERT_TRUE(session.RemoveSessionComputedColumn(QStringLiteral("ScaledWidth"), &error)) << error.toStdString();
    EXPECT_TRUE(session.CurrentSessionComputedColumns().isEmpty());
    ASSERT_EQ(session.CurrentTableView()->GetColumnCount(&columnCount), sc::SC_OK);
    EXPECT_EQ(columnCount, 2);
}

TEST(DatabaseEditorSession, ConvertColumnToComputedRebuildsSchemaAndView)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_ConvertColumnToComputed.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Length"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();

    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordCursorPtr cursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    sc::SCRecordPtr record;
    ASSERT_EQ(cursor->Next(record), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(record));
    const sc::RecordId recordId = record->GetId();
    ASSERT_TRUE(session.SetCellValue(recordId, QStringLiteral("Length"), QVariant::fromValue<qlonglong>(7), &error))
        << error.toStdString();
    ASSERT_TRUE(session.SetCellValue(recordId, QStringLiteral("Width"), QVariant::fromValue<qlonglong>(5), &error))
        << error.toStdString();

    const sc::SCComputedColumnDef computed = MakeExpressionComputedColumn(L"Width", L"Length * 2", L"Length");
    ASSERT_TRUE(session.ConvertColumnToComputed(QStringLiteral("Width"), computed, &error)) << error.toStdString();
    EXPECT_EQ(session.CurrentSessionComputedColumns().size(), 1u);

    ASSERT_TRUE(session.ConvertComputedToColumn(QStringLiteral("Width"), MakeRequiredIntColumn(L"Width"), &error))
        << error.toStdString();
    EXPECT_TRUE(session.CurrentSessionComputedColumns().isEmpty());

    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordCursorPtr restoredCursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(restoredCursor), sc::SC_OK);
    sc::SCRecordPtr restoredRecord;
    ASSERT_EQ(restoredCursor->Next(restoredRecord), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(restoredRecord));
    std::int64_t restoredWidth = -1;
    ASSERT_EQ(restoredRecord->GetInt64(L"Width", &restoredWidth), sc::SC_OK);
    EXPECT_EQ(restoredWidth, 0);

    QVector<sc::SCColumnDef> schemaColumns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&schemaColumns, &error)) << error.toStdString();
    EXPECT_EQ(CollectColumnNames(schemaColumns), (std::vector<std::wstring>{L"Length", L"Width"}));
    EXPECT_TRUE(session.CurrentSessionComputedColumns().isEmpty());
    ASSERT_TRUE(session.CurrentTableView() != nullptr);
    std::int32_t columnCount = 0;
    ASSERT_EQ(session.CurrentTableView()->GetColumnCount(&columnCount), sc::SC_OK);
    EXPECT_EQ(columnCount, 2);
}

TEST(DatabaseEditorSession, ConvertColumnToComputedRollsBackWhenViewRebuildFails)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_ConvertColumnRollback.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Length"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();

    QVector<sc::SCColumnDef> beforeColumns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&beforeColumns, &error)) << error.toStdString();
    EXPECT_EQ(CollectColumnNames(beforeColumns), (std::vector<std::wstring>{L"Length", L"Width"}));

    session.SetForceRebuildCurrentTableViewFailureForTest(true);
    const sc::SCComputedColumnDef computed = MakeExpressionComputedColumn(L"Width", L"Length * 2", L"Length");
    EXPECT_FALSE(session.ConvertColumnToComputed(QStringLiteral("Width"), computed, &error));
    EXPECT_EQ(error, QStringLiteral("Forced rebuild failure for test."));
    session.SetForceRebuildCurrentTableViewFailureForTest(false);

    QVector<sc::SCColumnDef> afterColumns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&afterColumns, &error)) << error.toStdString();
    EXPECT_EQ(CollectColumnNames(afterColumns), (std::vector<std::wstring>{L"Length", L"Width"}));
    EXPECT_TRUE(session.CurrentSessionComputedColumns().isEmpty());
    ASSERT_TRUE(session.CurrentTableView() != nullptr);
    std::int32_t columnCount = 0;
    ASSERT_EQ(session.CurrentTableView()->GetColumnCount(&columnCount), sc::SC_OK);
    EXPECT_EQ(columnCount, 2);
}

TEST(DatabaseEditorSession, ConvertNonNullableColumnToComputedClearsDataStructurally)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_ConvertNonNullableColumn.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeRequiredIntColumn(L"Width"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Length"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();

    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordCursorPtr cursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    sc::SCRecordPtr record;
    ASSERT_EQ(cursor->Next(record), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(record));
    const sc::RecordId recordId = record->GetId();
    ASSERT_TRUE(session.SetCellValue(recordId, QStringLiteral("Width"), QVariant::fromValue<qlonglong>(11), &error))
        << error.toStdString();

    const sc::SCComputedColumnDef computed = MakeExpressionComputedColumn(L"Width", L"Length * 2", L"Length");
    ASSERT_TRUE(session.ConvertColumnToComputed(QStringLiteral("Width"), computed, &error)) << error.toStdString();
    EXPECT_EQ(session.CurrentSessionComputedColumns().size(), 1u);

    ASSERT_TRUE(session.ConvertComputedToColumn(QStringLiteral("Width"), MakeRequiredIntColumn(L"Width"), &error))
        << error.toStdString();
    EXPECT_TRUE(session.CurrentSessionComputedColumns().isEmpty());

    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordCursorPtr restoredCursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(restoredCursor), sc::SC_OK);
    sc::SCRecordPtr restoredRecord;
    ASSERT_EQ(restoredCursor->Next(restoredRecord), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(restoredRecord));
    std::int64_t restoredWidth = -1;
    ASSERT_EQ(restoredRecord->GetInt64(L"Width", &restoredWidth), sc::SC_OK);
    EXPECT_EQ(restoredWidth, 0);

    QVector<sc::SCColumnDef> schemaColumns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&schemaColumns, &error)) << error.toStdString();
    EXPECT_EQ(CollectColumnNames(schemaColumns), (std::vector<std::wstring>{L"Length", L"Width"}));
    EXPECT_TRUE(session.CurrentSessionComputedColumns().isEmpty());
}

TEST(DatabaseEditorSession, ConvertComputedToColumnRestoresSchemaColumn)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_ConvertComputedToColumn.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Length"), &error)) << error.toStdString();

    const sc::SCComputedColumnDef computed = MakeExpressionComputedColumn(L"Width", L"Length * 2", L"Length");
    ASSERT_TRUE(session.AddSessionComputedColumn(computed, &error)) << error.toStdString();

    sc::SCColumnDef convertedColumn = MakeIntColumn(L"Width");
    convertedColumn.displayName = L"Width";
    ASSERT_TRUE(session.ConvertComputedToColumn(QStringLiteral("Width"), convertedColumn, &error))
        << error.toStdString();

    QVector<sc::SCColumnDef> schemaColumns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&schemaColumns, &error)) << error.toStdString();
    EXPECT_EQ(CollectColumnNames(schemaColumns), (std::vector<std::wstring>{L"Length", L"Width"}));
    EXPECT_TRUE(session.CurrentSessionComputedColumns().isEmpty());
    ASSERT_TRUE(session.CurrentTableView() != nullptr);
    std::int32_t columnCount = 0;
    ASSERT_EQ(session.CurrentTableView()->GetColumnCount(&columnCount), sc::SC_OK);
    EXPECT_EQ(columnCount, 2);
}

TEST(DatabaseEditorSession, RejectsFactColumnsThatConflictWithComputedColumns)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_ColumnConflict.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();

    sc::SCComputedColumnDef computed;
    computed.name = L"ScaledWidth";
    computed.displayName = L"ScaledWidth";
    computed.valueKind = sc::ValueKind::Double;
    computed.kind = sc::ComputedFieldKind::Expression;
    computed.expression = L"Width * 2";
    computed.dependencies.factFields = {{L"Beam", L"Width"}};

    ASSERT_TRUE(session.AddSessionComputedColumn(computed, &error)) << error.toStdString();
    ASSERT_TRUE(session.CurrentTableView() != nullptr);

    QVector<sc::SCColumnDef> beforeColumns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&beforeColumns, &error)) << error.toStdString();
    EXPECT_EQ(CollectColumnNames(beforeColumns), (std::vector<std::wstring>{L"Width"}));

    EXPECT_FALSE(session.AddColumn(MakeIntColumn(L"ScaledWidth"), &error));
    EXPECT_EQ(error, QStringLiteral("同名计算字段已存在。"));

    QVector<sc::SCColumnDef> afterColumns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&afterColumns, &error)) << error.toStdString();
    EXPECT_EQ(CollectColumnNames(afterColumns), (std::vector<std::wstring>{L"Width"}));

    std::int32_t columnCount = 0;
    ASSERT_EQ(session.CurrentTableView()->GetColumnCount(&columnCount), sc::SC_OK);
    EXPECT_EQ(columnCount, 2);
}

TEST(DatabaseEditorSession, RejectsComputedColumnsWithoutDependencies)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_ComputedValidation.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();

    sc::SCComputedColumnDef computed;
    computed.name = L"Broken";
    computed.valueKind = sc::ValueKind::Double;
    computed.kind = sc::ComputedFieldKind::Expression;
    computed.expression = L"Width * 2";

    EXPECT_FALSE(session.AddSessionComputedColumn(computed, &error));
    EXPECT_EQ(error, QStringLiteral("At least one dependency is required."));

    computed.dependencies.factFields = {{L"Beam", L"Width"}};
    ASSERT_TRUE(session.AddSessionComputedColumn(computed, &error)) << error.toStdString();

    sc::SCComputedColumnDef updated = computed;
    updated.name = L"BrokenUpdated";
    updated.dependencies.factFields.clear();
    EXPECT_FALSE(session.UpdateSessionComputedColumn(QStringLiteral("Broken"), updated, &error));
    EXPECT_EQ(error, QStringLiteral("At least one dependency is required."));
}

TEST(DatabaseEditorSession, AddColumnRollsBackSchemaOnViewRebuildFailure)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_AddColumnRollback.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();

    QVector<sc::SCColumnDef> beforeColumns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&beforeColumns, &error)) << error.toStdString();
    EXPECT_EQ(CollectColumnNames(beforeColumns), (std::vector<std::wstring>{L"Width"}));

    session.SetForceRebuildCurrentTableViewFailureForTest(true);
    EXPECT_FALSE(session.AddColumn(MakeIntColumn(L"Height"), &error));
    EXPECT_EQ(error, QStringLiteral("Forced rebuild failure for test."));
    session.SetForceRebuildCurrentTableViewFailureForTest(false);

    QVector<sc::SCColumnDef> afterColumns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&afterColumns, &error)) << error.toStdString();
    EXPECT_EQ(CollectColumnNames(afterColumns), (std::vector<std::wstring>{L"Width"}));

    ASSERT_TRUE(session.CurrentTableView() != nullptr);
    std::int32_t columnCount = 0;
    ASSERT_EQ(session.CurrentTableView()->GetColumnCount(&columnCount), sc::SC_OK);
    EXPECT_EQ(columnCount, 1);
}

TEST(DatabaseEditorSession, EditingStateAndEditLogAreExposedAfterEdits)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_StateSummary.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();

    sc::SCEditingDatabaseState editingState;
    ASSERT_TRUE(session.GetEditingState(&editingState, &error)) << error.toStdString();
    EXPECT_TRUE(editingState.open);
    EXPECT_EQ(editingState.openMode, sc::SCDatabaseOpenMode::Normal);
    EXPECT_GE(editingState.undoCount, 1u);

    sc::SCEditLogState logState;
    ASSERT_TRUE(session.GetEditLogState(&logState, &error)) << error.toStdString();
    EXPECT_FALSE(logState.undoItems.empty());
}

TEST(DatabaseEditorSession, CreateBackupCopyWritesAnOpenableDatabaseCopy)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_BackupSource.sqlite");
    const fs::path backupPath = MakeTempDbPath(L"StableCoreStorage_DbEditor_BackupCopy.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(QString::fromStdWString(dbPath.wstring()), &error)) << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error)) << error.toStdString();
    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();

    sc::SCBackupOptions options;
    options.overwriteExisting = true;
    sc::SCBackupResult result;
    ASSERT_TRUE(session.CreateBackupCopy(QString::fromStdWString(backupPath.wstring()), options, &result, &error))
        << error.toStdString();

    editor::SCDatabaseSession reopened;
    ASSERT_TRUE(reopened.OpenDatabase(QString::fromStdWString(backupPath.wstring()), &error)) << error.toStdString();

    QVector<sc::SCColumnDef> columns;
    ASSERT_TRUE(reopened.SelectTable(QStringLiteral("Beam"), &error)) << error.toStdString();
    ASSERT_TRUE(reopened.BuildSchemaSnapshot(&columns, &error)) << error.toStdString();
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].name, L"Width");
}

