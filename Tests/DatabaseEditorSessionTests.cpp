#include <filesystem>

#include <gtest/gtest.h>

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

sc::SCColumnDef MakeStringColumn(const wchar_t* name)
{
    sc::SCColumnDef column;
    column.name = name;
    column.displayName = name;
    column.valueKind = sc::ValueKind::String;
    column.defaultValue = sc::SCValue::FromString(L"");
    return column;
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

}  // namespace

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
        EXPECT_EQ(
            CollectColumnNames(beamColumns),
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
        EXPECT_EQ(
            CollectColumnNames(beamColumns),
            (std::vector<std::wstring>{L"Id", L"Code", L"Length", L"Width", L"Area"}));

        QVector<sc::SCColumnDef> columColumns;
        ASSERT_TRUE(session.SelectTable(QStringLiteral("Colum"), &error)) << error.toStdString();
        ASSERT_TRUE(session.BuildSchemaSnapshot(&columColumns, &error)) << error.toStdString();
        EXPECT_EQ(CollectColumnNames(columColumns), (std::vector<std::wstring>{L"ColumnId"}));
    }
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
    ASSERT_TRUE(session.GetSessionComputedColumn(QStringLiteral("ScaledWidth"), &loaded, &error)) << error.toStdString();
    EXPECT_EQ(loaded.name, L"ScaledWidth");
    EXPECT_EQ(loaded.expression, L"Width * 2");

    ASSERT_TRUE(session.RemoveSessionComputedColumn(QStringLiteral("ScaledWidth"), &error)) << error.toStdString();
    EXPECT_TRUE(session.CurrentSessionComputedColumns().isEmpty());
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
    EXPECT_EQ(error, QStringLiteral("A computed column with the same name already exists."));

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
