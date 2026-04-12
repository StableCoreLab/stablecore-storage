#include <filesystem>

#include <gtest/gtest.h>

#include "DatabaseSession.h"

namespace sc = stablecore::storage;
namespace editor = stablecore::storage::editor;
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
        editor::DatabaseSession session;
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
        editor::DatabaseSession session;
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
