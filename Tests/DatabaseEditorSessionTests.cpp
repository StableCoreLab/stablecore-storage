#include <filesystem>

#include <gtest/gtest.h>
#include <QVariant>

#include "SCBatch.h"
#include "SCDatabaseSession.h"

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

    sc::SCColumnDef MakeStringColumn(const wchar_t* name)
    {
        sc::SCColumnDef column;
        column.name = name;
        column.displayName = name;
        column.valueKind = sc::ValueKind::String;
        column.defaultValue = sc::SCValue::FromString(L"");
        return column;
    }

    sc::SCComputedColumnDef MakeExpressionComputedColumn(
        const wchar_t* name, const wchar_t* expression,
        const wchar_t* dependencyField)
    {
        sc::SCComputedColumnDef column;
        column.name = name;
        column.displayName = name;
        column.valueKind = sc::ValueKind::Int64;
        column.kind = sc::ComputedFieldKind::Expression;
        column.expression = expression;
        column.dependencies.factFields = {{L"Beam", dependencyField}};
        return column;
    }

    std::vector<std::wstring> CollectColumnNames(
        const QVector<sc::SCColumnDef>& columns)
    {
        std::vector<std::wstring> names;
        for (const sc::SCColumnDef& column : columns)
        {
            names.push_back(column.name);
        }
        return names;
    }

}  // namespace

TEST(DatabaseEditorSession, ExecuteBatchEditCreatesRecordsWithValues)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_BatchImport.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(
        QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Id"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeStringColumn(L"Code"), &error))
        << error.toStdString();

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
    const sc::ErrorCode rc =
        sc::ExecuteBatchEdit(session.Database(), requests, options, &result);
    ASSERT_EQ(rc, sc::SC_OK);
    EXPECT_EQ(result.createdCount, 1u);

    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordCursorPtr cursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    bool hasRow = false;
    ASSERT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
    ASSERT_TRUE(hasRow);
    sc::SCRecordPtr record;
    ASSERT_EQ(cursor->GetCurrent(record), sc::SC_OK);

    std::int64_t id = 0;
    ASSERT_EQ(record->GetInt64(L"Id", &id), sc::SC_OK);
    EXPECT_EQ(id, 7);

    std::wstring code;
    ASSERT_EQ(record->GetStringCopy(L"Code", &code), sc::SC_OK);
    EXPECT_EQ(code, L"Beam-A");
}

TEST(DatabaseEditorSession, ExecuteBatchEditRollsBackOnInvalidAssignment)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_BatchRollback.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(
        QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Id"), &error))
        << error.toStdString();

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
    const sc::ErrorCode rc =
        sc::ExecuteBatchEdit(session.Database(), requests, options, &result);
    ASSERT_TRUE(sc::Failed(rc));

    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordCursorPtr cursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    bool hasRow = false;
    ASSERT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
    EXPECT_FALSE(hasRow);
}

TEST(DatabaseEditorSession,
     TableSelectionAndSchemaRemainAlignedAcrossCreateAndReopen)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_SessionSchema.sqlite");

    {
        editor::SCDatabaseSession session;
        QString error;

        ASSERT_TRUE(session.CreateDatabase(
            QString::fromStdWString(dbPath.wstring()), &error))
            << error.toStdString();

        ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error))
            << error.toStdString();
        EXPECT_EQ(session.CurrentTableName(), QStringLiteral("Beam"));

        ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Id"), &error))
            << error.toStdString();
        ASSERT_TRUE(session.AddColumn(MakeStringColumn(L"Code"), &error))
            << error.toStdString();
        ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Length"), &error))
            << error.toStdString();
        ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error))
            << error.toStdString();
        ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Area"), &error))
            << error.toStdString();
        ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();

        ASSERT_TRUE(session.CreateTable(QStringLiteral("Colum"), &error))
            << error.toStdString();
        EXPECT_EQ(session.CurrentTableName(), QStringLiteral("Colum"));

        QVector<sc::SCColumnDef> columColumns;
        ASSERT_TRUE(session.BuildSchemaSnapshot(&columColumns, &error))
            << error.toStdString();
        EXPECT_TRUE(columColumns.isEmpty());

        ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"ColumnId"), &error))
            << error.toStdString();

        QVector<sc::SCColumnDef> beamColumns;
        ASSERT_TRUE(session.SelectTable(QStringLiteral("Beam"), &error))
            << error.toStdString();
        ASSERT_TRUE(session.BuildSchemaSnapshot(&beamColumns, &error))
            << error.toStdString();
        EXPECT_EQ(CollectColumnNames(beamColumns),
                  (std::vector<std::wstring>{L"Id", L"Code", L"Length",
                                             L"Width", L"Area"}));

        ASSERT_TRUE(session.SelectTable(QStringLiteral("Colum"), &error))
            << error.toStdString();
        ASSERT_TRUE(session.BuildSchemaSnapshot(&columColumns, &error))
            << error.toStdString();
        EXPECT_EQ(CollectColumnNames(columColumns),
                  (std::vector<std::wstring>{L"ColumnId"}));
    }

    {
        editor::SCDatabaseSession session;
        QString error;

        ASSERT_TRUE(session.OpenDatabase(
            QString::fromStdWString(dbPath.wstring()), &error))
            << error.toStdString();

        QVector<sc::SCColumnDef> beamColumns;
        ASSERT_TRUE(session.SelectTable(QStringLiteral("Beam"), &error))
            << error.toStdString();
        ASSERT_TRUE(session.BuildSchemaSnapshot(&beamColumns, &error))
            << error.toStdString();
        EXPECT_EQ(CollectColumnNames(beamColumns),
                  (std::vector<std::wstring>{L"Id", L"Code", L"Length",
                                             L"Width", L"Area"}));

        QVector<sc::SCColumnDef> columColumns;
        ASSERT_TRUE(session.SelectTable(QStringLiteral("Colum"), &error))
            << error.toStdString();
        ASSERT_TRUE(session.BuildSchemaSnapshot(&columColumns, &error))
            << error.toStdString();
        EXPECT_EQ(CollectColumnNames(columColumns),
                  (std::vector<std::wstring>{L"ColumnId"}));
    }
}

TEST(DatabaseEditorSession, EditColumnUpdatesSchemaSnapshotInPlace)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_EditColumn.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(
        QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error))
        << error.toStdString();

    sc::SCColumnDef updated = MakeStringColumn(L"Width");
    updated.displayName = L"Width Label";
    updated.nullable = false;
    updated.defaultValue = sc::SCValue::FromString(L"0");

    ASSERT_TRUE(session.UpdateColumn(QStringLiteral("Width"), updated, &error))
        << error.toStdString();

    QVector<sc::SCColumnDef> columns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error))
        << error.toStdString();
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].name, L"Width");
    EXPECT_EQ(columns[0].displayName, L"Width Label");
    EXPECT_EQ(columns[0].valueKind, sc::ValueKind::String);
    EXPECT_FALSE(columns[0].nullable);

    ASSERT_TRUE(session.CurrentTableView() != nullptr);
    std::int32_t columnCount = 0;
    ASSERT_EQ(session.CurrentTableView()->GetColumnCount(&columnCount),
              sc::SC_OK);
    EXPECT_EQ(columnCount, 1);
}

TEST(DatabaseEditorSession, AddColumnParticipatesInUndoRedo)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_AddColumnUndoRedo.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(
        QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error))
        << error.toStdString();

    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error))
        << error.toStdString();

    QVector<sc::SCColumnDef> columns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error))
        << error.toStdString();
    ASSERT_EQ(columns.size(), 1);

    ASSERT_TRUE(session.Undo(&error)) << error.toStdString();
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error))
        << error.toStdString();
    ASSERT_TRUE(columns.isEmpty());

    ASSERT_TRUE(session.Redo(&error)) << error.toStdString();
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error))
        << error.toStdString();
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].name, L"Width");
}

TEST(DatabaseEditorSession, CreateTableSelectsCurrentTableForEditing)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_CreateTableSelect.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(
        QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error))
        << error.toStdString();

    EXPECT_EQ(session.CurrentTableName(), QStringLiteral("Beam"));
    EXPECT_TRUE(session.CurrentTable() != nullptr);

    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error))
        << error.toStdString();
}

TEST(DatabaseEditorSession, AddAndDeleteRecordStayOnCurrentTable)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_AddDeleteRecord.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(
        QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error))
        << error.toStdString();

    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();
    ASSERT_TRUE(session.CurrentTable() != nullptr);

    sc::SCRecordCursorPtr cursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    bool hasRow = false;
    ASSERT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
    ASSERT_TRUE(hasRow);

    sc::SCRecordPtr record;
    ASSERT_EQ(cursor->GetCurrent(record), sc::SC_OK);
    ASSERT_TRUE(session.DeleteRecord(record->GetId(), &error))
        << error.toStdString();

    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    ASSERT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
    EXPECT_FALSE(hasRow);
}

TEST(DatabaseEditorSession, RemoveColumnParticipatesInUndoRedo)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_RemoveColumnUndoRedo.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(
        QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Height"), &error))
        << error.toStdString();

    ASSERT_TRUE(session.RemoveColumn(QStringLiteral("Width"), &error))
        << error.toStdString();

    QVector<sc::SCColumnDef> columns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error))
        << error.toStdString();
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].name, L"Height");

    ASSERT_TRUE(session.Undo(&error)) << error.toStdString();
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error))
        << error.toStdString();
    EXPECT_EQ(CollectColumnNames(columns),
              (std::vector<std::wstring>{L"Width", L"Height"}));

    ASSERT_TRUE(session.Redo(&error)) << error.toStdString();
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error))
        << error.toStdString();
    EXPECT_EQ(CollectColumnNames(columns),
              (std::vector<std::wstring>{L"Height"}));
}

TEST(DatabaseEditorSession, EditColumnMigratesCompatibleValues)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_EditColumnMigrate.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(
        QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();

    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordCursorPtr cursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    bool hasRow = false;
    ASSERT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
    ASSERT_TRUE(hasRow);
    sc::SCRecordPtr record;
    ASSERT_EQ(cursor->GetCurrent(record), sc::SC_OK);
    const sc::RecordId recordId = record->GetId();
    ASSERT_TRUE(session.SetCellValue(recordId, QStringLiteral("Width"),
                                     QVariant::fromValue<qlonglong>(11),
                                     &error))
        << error.toStdString();

    sc::SCColumnDef updated = MakeStringColumn(L"Width");
    updated.displayName = L"Width Label";
    updated.defaultValue = sc::SCValue::FromString(L"0");
    ASSERT_TRUE(session.UpdateColumn(QStringLiteral("Width"), updated, &error))
        << error.toStdString();

    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordPtr reloaded;
    ASSERT_EQ(session.CurrentTable()->GetRecord(recordId, reloaded), sc::SC_OK);

    std::wstring width;
    ASSERT_EQ(reloaded->GetStringCopy(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, L"11");

    QVector<sc::SCColumnDef> columns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error))
        << error.toStdString();
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].valueKind, sc::ValueKind::String);
}

TEST(DatabaseEditorSession, EditColumnRollsBackWhenViewRebuildFails)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_EditColumnRollback.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(
        QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();

    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordCursorPtr cursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    bool hasRow = false;
    ASSERT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
    ASSERT_TRUE(hasRow);
    sc::SCRecordPtr record;
    ASSERT_EQ(cursor->GetCurrent(record), sc::SC_OK);
    const sc::RecordId recordId = record->GetId();
    ASSERT_TRUE(session.SetCellValue(recordId, QStringLiteral("Width"),
                                     QVariant::fromValue<qlonglong>(17),
                                     &error))
        << error.toStdString();

    session.SetForceRebuildCurrentTableViewFailureForTest(true);
    sc::SCColumnDef updated = MakeStringColumn(L"Width");
    updated.defaultValue = sc::SCValue::FromString(L"0");
    EXPECT_FALSE(session.UpdateColumn(QStringLiteral("Width"), updated, &error));
    EXPECT_EQ(error, QStringLiteral("Forced rebuild failure for test."));
    session.SetForceRebuildCurrentTableViewFailureForTest(false);

    QVector<sc::SCColumnDef> columns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&columns, &error))
        << error.toStdString();
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
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_ComputedWorkflow.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(
        QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error))
        << error.toStdString();

    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CurrentTableView() != nullptr);

    std::int32_t columnCount = 0;
    ASSERT_EQ(session.CurrentTableView()->GetColumnCount(&columnCount),
              sc::SC_OK);
    EXPECT_EQ(columnCount, 1);

    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Height"), &error))
        << error.toStdString();
    ASSERT_EQ(session.CurrentTableView()->GetColumnCount(&columnCount),
              sc::SC_OK);
    EXPECT_EQ(columnCount, 2);

    sc::SCComputedColumnDef computed;
    computed.name = L"ScaledWidth";
    computed.displayName = L"ScaledWidth";
    computed.valueKind = sc::ValueKind::Double;
    computed.kind = sc::ComputedFieldKind::Expression;
    computed.expression = L"Width * 2";
    computed.dependencies.factFields = {{L"Beam", L"Width"}};

    ASSERT_TRUE(session.AddSessionComputedColumn(computed, &error))
        << error.toStdString();
    ASSERT_EQ(session.CurrentSessionComputedColumns().size(), 1u);
    ASSERT_EQ(session.CurrentTableView()->GetColumnCount(&columnCount),
              sc::SC_OK);
    EXPECT_EQ(columnCount, 3);

    sc::SCComputedColumnDef loaded;
    ASSERT_TRUE(session.GetSessionComputedColumn(QStringLiteral("ScaledWidth"),
                                                 &loaded, &error))
        << error.toStdString();
    EXPECT_EQ(loaded.name, L"ScaledWidth");
    EXPECT_EQ(loaded.expression, L"Width * 2");

    ASSERT_TRUE(session.RemoveSessionComputedColumn(
        QStringLiteral("ScaledWidth"), &error))
        << error.toStdString();
    EXPECT_TRUE(session.CurrentSessionComputedColumns().isEmpty());
    ASSERT_EQ(session.CurrentTableView()->GetColumnCount(&columnCount),
              sc::SC_OK);
    EXPECT_EQ(columnCount, 2);
}

TEST(DatabaseEditorSession, ConvertColumnToComputedRebuildsSchemaAndView)
{
    const fs::path dbPath = MakeTempDbPath(
        L"StableCoreStorage_DbEditor_ConvertColumnToComputed.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(
        QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Length"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();

    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordCursorPtr cursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    bool hasRow = false;
    ASSERT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
    ASSERT_TRUE(hasRow);
    sc::SCRecordPtr record;
    ASSERT_EQ(cursor->GetCurrent(record), sc::SC_OK);
    const sc::RecordId recordId = record->GetId();
    ASSERT_TRUE(session.SetCellValue(recordId, QStringLiteral("Length"),
                                     QVariant::fromValue<qlonglong>(7),
                                     &error))
        << error.toStdString();
    ASSERT_TRUE(session.SetCellValue(recordId, QStringLiteral("Width"),
                                     QVariant::fromValue<qlonglong>(5),
                                     &error))
        << error.toStdString();

    const sc::SCComputedColumnDef computed =
        MakeExpressionComputedColumn(L"Width", L"Length * 2", L"Length");
    ASSERT_TRUE(session.ConvertColumnToComputed(QStringLiteral("Width"),
                                                computed, &error))
        << error.toStdString();
    EXPECT_EQ(session.CurrentSessionComputedColumns().size(), 1u);

    ASSERT_TRUE(session.ConvertComputedToColumn(QStringLiteral("Width"),
                                                MakeRequiredIntColumn(L"Width"),
                                                &error))
        << error.toStdString();
    EXPECT_TRUE(session.CurrentSessionComputedColumns().isEmpty());

    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordCursorPtr restoredCursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(restoredCursor),
              sc::SC_OK);
    hasRow = false;
    ASSERT_EQ(restoredCursor->MoveNext(&hasRow), sc::SC_OK);
    ASSERT_TRUE(hasRow);
    sc::SCRecordPtr restoredRecord;
    ASSERT_EQ(restoredCursor->GetCurrent(restoredRecord), sc::SC_OK);
    std::int64_t restoredWidth = -1;
    ASSERT_EQ(restoredRecord->GetInt64(L"Width", &restoredWidth), sc::SC_OK);
    EXPECT_EQ(restoredWidth, 0);

    QVector<sc::SCColumnDef> schemaColumns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&schemaColumns, &error))
        << error.toStdString();
    EXPECT_EQ(CollectColumnNames(schemaColumns),
              (std::vector<std::wstring>{L"Length", L"Width"}));
    EXPECT_TRUE(session.CurrentSessionComputedColumns().isEmpty());
    ASSERT_TRUE(session.CurrentTableView() != nullptr);
    std::int32_t columnCount = 0;
    ASSERT_EQ(session.CurrentTableView()->GetColumnCount(&columnCount),
              sc::SC_OK);
    EXPECT_EQ(columnCount, 2);
}

TEST(DatabaseEditorSession, ConvertColumnToComputedRollsBackWhenViewRebuildFails)
{
    const fs::path dbPath = MakeTempDbPath(
        L"StableCoreStorage_DbEditor_ConvertColumnRollback.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(
        QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Length"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error))
        << error.toStdString();

    QVector<sc::SCColumnDef> beforeColumns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&beforeColumns, &error))
        << error.toStdString();
    EXPECT_EQ(CollectColumnNames(beforeColumns),
              (std::vector<std::wstring>{L"Length", L"Width"}));

    session.SetForceRebuildCurrentTableViewFailureForTest(true);
    const sc::SCComputedColumnDef computed =
        MakeExpressionComputedColumn(L"Width", L"Length * 2", L"Length");
    EXPECT_FALSE(session.ConvertColumnToComputed(QStringLiteral("Width"),
                                                 computed, &error));
    EXPECT_EQ(error, QStringLiteral("Forced rebuild failure for test."));
    session.SetForceRebuildCurrentTableViewFailureForTest(false);

    QVector<sc::SCColumnDef> afterColumns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&afterColumns, &error))
        << error.toStdString();
    EXPECT_EQ(CollectColumnNames(afterColumns),
              (std::vector<std::wstring>{L"Length", L"Width"}));
    EXPECT_TRUE(session.CurrentSessionComputedColumns().isEmpty());
    ASSERT_TRUE(session.CurrentTableView() != nullptr);
    std::int32_t columnCount = 0;
    ASSERT_EQ(session.CurrentTableView()->GetColumnCount(&columnCount),
              sc::SC_OK);
    EXPECT_EQ(columnCount, 2);
}

TEST(DatabaseEditorSession, ConvertNonNullableColumnToComputedClearsDataStructurally)
{
    const fs::path dbPath = MakeTempDbPath(
        L"StableCoreStorage_DbEditor_ConvertNonNullableColumn.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(
        QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeRequiredIntColumn(L"Width"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Length"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();

    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordCursorPtr cursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(cursor), sc::SC_OK);
    bool hasRow = false;
    ASSERT_EQ(cursor->MoveNext(&hasRow), sc::SC_OK);
    ASSERT_TRUE(hasRow);
    sc::SCRecordPtr record;
    ASSERT_EQ(cursor->GetCurrent(record), sc::SC_OK);
    const sc::RecordId recordId = record->GetId();
    ASSERT_TRUE(session.SetCellValue(recordId, QStringLiteral("Width"),
                                     QVariant::fromValue<qlonglong>(11),
                                     &error))
        << error.toStdString();

    const sc::SCComputedColumnDef computed =
        MakeExpressionComputedColumn(L"Width", L"Length * 2", L"Length");
    ASSERT_TRUE(session.ConvertColumnToComputed(QStringLiteral("Width"),
                                                computed, &error))
        << error.toStdString();
    EXPECT_EQ(session.CurrentSessionComputedColumns().size(), 1u);

    ASSERT_TRUE(session.ConvertComputedToColumn(QStringLiteral("Width"),
                                                MakeRequiredIntColumn(L"Width"),
                                                &error))
        << error.toStdString();
    EXPECT_TRUE(session.CurrentSessionComputedColumns().isEmpty());

    ASSERT_TRUE(session.CurrentTable() != nullptr);
    sc::SCRecordCursorPtr restoredCursor;
    ASSERT_EQ(session.CurrentTable()->EnumerateRecords(restoredCursor),
              sc::SC_OK);
    hasRow = false;
    ASSERT_EQ(restoredCursor->MoveNext(&hasRow), sc::SC_OK);
    ASSERT_TRUE(hasRow);
    sc::SCRecordPtr restoredRecord;
    ASSERT_EQ(restoredCursor->GetCurrent(restoredRecord), sc::SC_OK);
    std::int64_t restoredWidth = -1;
    ASSERT_EQ(restoredRecord->GetInt64(L"Width", &restoredWidth), sc::SC_OK);
    EXPECT_EQ(restoredWidth, 0);

    QVector<sc::SCColumnDef> schemaColumns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&schemaColumns, &error))
        << error.toStdString();
    EXPECT_EQ(CollectColumnNames(schemaColumns),
              (std::vector<std::wstring>{L"Length", L"Width"}));
    EXPECT_TRUE(session.CurrentSessionComputedColumns().isEmpty());
}

TEST(DatabaseEditorSession, ConvertComputedToColumnRestoresSchemaColumn)
{
    const fs::path dbPath = MakeTempDbPath(
        L"StableCoreStorage_DbEditor_ConvertComputedToColumn.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(
        QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Length"), &error))
        << error.toStdString();

    const sc::SCComputedColumnDef computed =
        MakeExpressionComputedColumn(L"Width", L"Length * 2", L"Length");
    ASSERT_TRUE(session.AddSessionComputedColumn(computed, &error))
        << error.toStdString();

    sc::SCColumnDef convertedColumn = MakeIntColumn(L"Width");
    convertedColumn.displayName = L"Width";
    ASSERT_TRUE(session.ConvertComputedToColumn(QStringLiteral("Width"),
                                                convertedColumn, &error))
        << error.toStdString();

    QVector<sc::SCColumnDef> schemaColumns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&schemaColumns, &error))
        << error.toStdString();
    EXPECT_EQ(CollectColumnNames(schemaColumns),
              (std::vector<std::wstring>{L"Length", L"Width"}));
    EXPECT_TRUE(session.CurrentSessionComputedColumns().isEmpty());
    ASSERT_TRUE(session.CurrentTableView() != nullptr);
    std::int32_t columnCount = 0;
    ASSERT_EQ(session.CurrentTableView()->GetColumnCount(&columnCount),
              sc::SC_OK);
    EXPECT_EQ(columnCount, 2);
}

TEST(DatabaseEditorSession, RejectsFactColumnsThatConflictWithComputedColumns)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_ColumnConflict.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(
        QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error))
        << error.toStdString();

    sc::SCComputedColumnDef computed;
    computed.name = L"ScaledWidth";
    computed.displayName = L"ScaledWidth";
    computed.valueKind = sc::ValueKind::Double;
    computed.kind = sc::ComputedFieldKind::Expression;
    computed.expression = L"Width * 2";
    computed.dependencies.factFields = {{L"Beam", L"Width"}};

    ASSERT_TRUE(session.AddSessionComputedColumn(computed, &error))
        << error.toStdString();
    ASSERT_TRUE(session.CurrentTableView() != nullptr);

    QVector<sc::SCColumnDef> beforeColumns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&beforeColumns, &error))
        << error.toStdString();
    EXPECT_EQ(CollectColumnNames(beforeColumns),
              (std::vector<std::wstring>{L"Width"}));

    EXPECT_FALSE(session.AddColumn(MakeIntColumn(L"ScaledWidth"), &error));
    EXPECT_EQ(
        error,
        QStringLiteral("A computed column with the same name already exists."));

    QVector<sc::SCColumnDef> afterColumns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&afterColumns, &error))
        << error.toStdString();
    EXPECT_EQ(CollectColumnNames(afterColumns),
              (std::vector<std::wstring>{L"Width"}));

    std::int32_t columnCount = 0;
    ASSERT_EQ(session.CurrentTableView()->GetColumnCount(&columnCount),
              sc::SC_OK);
    EXPECT_EQ(columnCount, 2);
}

TEST(DatabaseEditorSession, RejectsComputedColumnsWithoutDependencies)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_ComputedValidation.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(
        QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error))
        << error.toStdString();

    sc::SCComputedColumnDef computed;
    computed.name = L"Broken";
    computed.valueKind = sc::ValueKind::Double;
    computed.kind = sc::ComputedFieldKind::Expression;
    computed.expression = L"Width * 2";

    EXPECT_FALSE(session.AddSessionComputedColumn(computed, &error));
    EXPECT_EQ(error, QStringLiteral("At least one dependency is required."));

    computed.dependencies.factFields = {{L"Beam", L"Width"}};
    ASSERT_TRUE(session.AddSessionComputedColumn(computed, &error))
        << error.toStdString();

    sc::SCComputedColumnDef updated = computed;
    updated.name = L"BrokenUpdated";
    updated.dependencies.factFields.clear();
    EXPECT_FALSE(session.UpdateSessionComputedColumn(QStringLiteral("Broken"),
                                                     updated, &error));
    EXPECT_EQ(error, QStringLiteral("At least one dependency is required."));
}

TEST(DatabaseEditorSession, AddColumnRollsBackSchemaOnViewRebuildFailure)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_AddColumnRollback.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(
        QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error))
        << error.toStdString();

    QVector<sc::SCColumnDef> beforeColumns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&beforeColumns, &error))
        << error.toStdString();
    EXPECT_EQ(CollectColumnNames(beforeColumns),
              (std::vector<std::wstring>{L"Width"}));

    session.SetForceRebuildCurrentTableViewFailureForTest(true);
    EXPECT_FALSE(session.AddColumn(MakeIntColumn(L"Height"), &error));
    EXPECT_EQ(error, QStringLiteral("Forced rebuild failure for test."));
    session.SetForceRebuildCurrentTableViewFailureForTest(false);

    QVector<sc::SCColumnDef> afterColumns;
    ASSERT_TRUE(session.BuildSchemaSnapshot(&afterColumns, &error))
        << error.toStdString();
    EXPECT_EQ(CollectColumnNames(afterColumns),
              (std::vector<std::wstring>{L"Width"}));

    ASSERT_TRUE(session.CurrentTableView() != nullptr);
    std::int32_t columnCount = 0;
    ASSERT_EQ(session.CurrentTableView()->GetColumnCount(&columnCount),
              sc::SC_OK);
    EXPECT_EQ(columnCount, 1);
}

TEST(DatabaseEditorSession, EditingStateAndEditLogAreExposedAfterEdits)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_StateSummary.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(
        QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();

    sc::SCEditingDatabaseState editingState;
    ASSERT_TRUE(session.GetEditingState(&editingState, &error))
        << error.toStdString();
    EXPECT_TRUE(editingState.open);
    EXPECT_EQ(editingState.openMode, sc::SCDatabaseOpenMode::Normal);
    EXPECT_GE(editingState.undoCount, 1u);

    sc::SCEditLogState logState;
    ASSERT_TRUE(session.GetEditLogState(&logState, &error))
        << error.toStdString();
    EXPECT_FALSE(logState.undoItems.empty());
}

TEST(DatabaseEditorSession, CreateBackupCopyWritesAnOpenableDatabaseCopy)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_BackupSource.sqlite");
    const fs::path backupPath =
        MakeTempDbPath(L"StableCoreStorage_DbEditor_BackupCopy.sqlite");

    editor::SCDatabaseSession session;
    QString error;

    ASSERT_TRUE(session.CreateDatabase(
        QString::fromStdWString(dbPath.wstring()), &error))
        << error.toStdString();
    ASSERT_TRUE(session.CreateTable(QStringLiteral("Beam"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddColumn(MakeIntColumn(L"Width"), &error))
        << error.toStdString();
    ASSERT_TRUE(session.AddRecord(&error)) << error.toStdString();

    sc::SCBackupOptions options;
    options.overwriteExisting = true;
    sc::SCBackupResult result;
    ASSERT_TRUE(session.CreateBackupCopy(
        QString::fromStdWString(backupPath.wstring()), options, &result,
        &error))
        << error.toStdString();

    editor::SCDatabaseSession reopened;
    ASSERT_TRUE(reopened.OpenDatabase(
        QString::fromStdWString(backupPath.wstring()), &error))
        << error.toStdString();

    QVector<sc::SCColumnDef> columns;
    ASSERT_TRUE(reopened.SelectTable(QStringLiteral("Beam"), &error))
        << error.toStdString();
    ASSERT_TRUE(reopened.BuildSchemaSnapshot(&columns, &error))
        << error.toStdString();
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].name, L"Width");
}
