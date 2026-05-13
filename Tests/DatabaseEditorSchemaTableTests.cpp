#include <gtest/gtest.h>

#include "SCSchemaTableImport.h"
#include "SCSchemaTableGenerator.h"

namespace sc = StableCore::Storage;
namespace editor = StableCore::Storage::Editor;

namespace
{

    sc::SCColumnDef MakeColumn(const wchar_t* name, sc::ValueKind kind,
                               bool nullable = false)
    {
        sc::SCColumnDef column;
        column.name = name;
        column.displayName = name;
        column.valueKind = kind;
        column.nullable = nullable;
        return column;
    }

}  // namespace

TEST(DatabaseEditorSchemaTable, ParsesSchemaDescriptionForCreateTable)
{
    const QString schemaText = QStringLiteral(R"(SC_SCHEMA_TABLE(ProjectInfo)
{
    Table("ProjectInfo")
        .Column("GUID", SCType::String)
        .Column("ProjectName", SCType::String)
            .Description("Project Name")
        .Column("ProjectCode", SCType::String)
            .Description("Project Code")
        .Column("OwnerUnit", SCType::String)
            .Description("Owner Unit")
        .Column("CreatedAt", SCType::String)
            .Description("Created At");
})");

    editor::SCSchemaTableImportResult result;
    QString error;
    EXPECT_TRUE(editor::ParseSchemaTableDescription(schemaText, &result, &error))
        << error.toStdString();

    EXPECT_EQ(result.tableMacroName, QStringLiteral("ProjectInfo"));
    EXPECT_EQ(result.tableName, QStringLiteral("ProjectInfo"));
    EXPECT_EQ(result.columns.size(), 5);
    EXPECT_EQ(result.columns[0].name, L"GUID");
    EXPECT_EQ(result.columns[1].name, L"ProjectName");
    EXPECT_EQ(QString::fromStdWString(result.columns[1].displayName),
              QStringLiteral("Project Name"));
    EXPECT_EQ(QString::fromStdWString(result.columns[2].displayName),
              QStringLiteral("Project Code"));
    EXPECT_EQ(QString::fromStdWString(result.columns[3].displayName),
              QStringLiteral("Owner Unit"));
    EXPECT_EQ(QString::fromStdWString(result.columns[4].displayName),
              QStringLiteral("Created At"));
    EXPECT_TRUE(result.primaryKeyColumnName.isEmpty());
    EXPECT_TRUE(result.indexes.isEmpty());
    EXPECT_FALSE(result.warnings.isEmpty());
}

TEST(DatabaseEditorSchemaTable, ParsesNotNullWithTrailingSemicolon)
{
    const QString schemaText = QStringLiteral(R"(SC_SCHEMA_TABLE(t1)
{
    Table("t1")
        .Column("f1", SCType::Int64)
            .NotNull();
        .Column("f2", SCType::String)
            .NotNull();
})");

    editor::SCSchemaTableImportResult result;
    QString error;
    EXPECT_TRUE(editor::ParseSchemaTableDescription(schemaText, &result, &error))
        << error.toStdString();

    ASSERT_EQ(result.columns.size(), 2);
    EXPECT_FALSE(result.columns[0].nullable);
    EXPECT_FALSE(result.columns[1].nullable);
    for (const QString& warning : result.warnings)
    {
        EXPECT_FALSE(warning.startsWith(
            QStringLiteral("Ignored schema token:")))
            << warning.toStdString();
    }
}

TEST(DatabaseEditorSchemaTable, UsesMacroNameAsCreatedTableName)
{
    const QString schemaText = QStringLiteral(R"(SC_SCHEMA_TABLE(Beam)
{
    Table("ProjectInfo")
        .Column("GUID", SCType::String)
        .Column("Name", SCType::String)
            .Description("Name");
})");

    editor::SCSchemaTableImportResult result;
    QString error;
    EXPECT_TRUE(editor::ParseSchemaTableDescription(schemaText, &result, &error))
        << error.toStdString();

    EXPECT_EQ(result.tableMacroName, QStringLiteral("Beam"));
    EXPECT_EQ(result.tableName, QStringLiteral("Beam"));
    EXPECT_EQ(result.columns.size(), 2);
    EXPECT_FALSE(result.warnings.isEmpty());
}

TEST(DatabaseEditorSchemaTable, BuildsCurrentTableSchemaCode)
{
    sc::SCTableSchemaSnapshot snapshot;
    snapshot.table.name = L"Element";
    snapshot.table.description = L"Element main table";

    sc::SCColumnDef id = MakeColumn(L"Id", sc::ValueKind::Int64);
    id.displayName = L"Unique element ID";
    id.nullable = false;
    snapshot.columns.push_back(id);

    sc::SCColumnDef floorId = MakeColumn(L"FloorId", sc::ValueKind::Int64);
    floorId.displayName = L"Belongs to floor";
    floorId.nullable = false;
    floorId.referenceTable = L"Floor";
    snapshot.columns.push_back(floorId);

    sc::SCColumnDef elementType =
        MakeColumn(L"ElementType", sc::ValueKind::Int64);
    elementType.displayName = L"Element type";
    elementType.nullable = false;
    snapshot.columns.push_back(elementType);

    sc::SCColumnDef name = MakeColumn(L"Name", sc::ValueKind::String);
    name.displayName = L"Element name";
    name.nullable = false;
    snapshot.columns.push_back(name);

    sc::SCConstraintDef primaryKey;
    primaryKey.kind = sc::SCConstraintKind::PrimaryKey;
    primaryKey.columns.push_back(L"Id");
    snapshot.constraints.push_back(primaryKey);

    sc::SCIndexDef index;
    index.name = L"idx_Element_FloorId";
    index.columns.push_back(sc::SCIndexColumnDef{L"FloorId", false});
    snapshot.indexes.push_back(index);

    const QString code = editor::BuildSchemaTableCode(snapshot);

    const QString expected = QStringLiteral(
        "SC_SCHEMA_TABLE(Element)\n"
        "{\n"
        "    Table(\"Element\")\n"
        "        .Description(\"Element main table\")\n"
        "        .PrimaryKey(\"Id\")\n"
        "        .Column(\"Id\", SCType::Int64)\n"
        "            .NotNull()\n"
        "            .Description(\"Unique element ID\")\n"
        "        .Column(\"FloorId\", SCType::Int64)\n"
        "            .NotNull()\n"
        "            .Ref(\"Floor\", \"Id\")\n"
        "            .Description(\"Belongs to floor\")\n"
        "        .Column(\"ElementType\", SCType::Int64)\n"
        "            .NotNull()\n"
        "            .Description(\"Element type\")\n"
        "        .Column(\"Name\", SCType::String)\n"
        "            .NotNull()\n"
        "            .Description(\"Element name\")\n"
        "        .Index(\"idx_Element_FloorId\").Columns(\"FloorId\");\n"
        "}\n");

    EXPECT_EQ(code, expected);
}
