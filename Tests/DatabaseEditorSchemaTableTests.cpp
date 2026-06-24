#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "SCSchemaTableImport.h"
#include "SCSchemaTableGenerator.h"

namespace sc = StableCore::Storage;
namespace editor = StableCore::Storage::Editor;

namespace
{

    sc::SCColumnDef MakeColumn(const wchar_t* name, sc::ValueKind kind, bool nullable = false)
    {
        sc::SCColumnDef column;
        column.name = name;
        column.displayName = name;
        column.valueKind = kind;
        column.nullable = nullable;
        return column;
    }

    void ExpectStringValue(const sc::SCValue& value, const wchar_t* expected)
    {
        ASSERT_EQ(value.GetKind(), sc::ValueKind::String);
        std::wstring actual;
        EXPECT_EQ(value.AsStringCopy(&actual), sc::SC_OK);
        EXPECT_EQ(actual, expected);
    }

    void ExpectEnumValue(const sc::SCValue& value, const wchar_t* expected)
    {
        ASSERT_EQ(value.GetKind(), sc::ValueKind::Enum);
        std::wstring actual;
        EXPECT_EQ(value.AsEnumCopy(&actual), sc::SC_OK);
        EXPECT_EQ(actual, expected);
    }

    void ExpectBinaryValue(const sc::SCValue& value,
                           const std::vector<std::uint8_t>& expected)
    {
        ASSERT_EQ(value.GetKind(), sc::ValueKind::Binary);
        std::vector<std::uint8_t> actual;
        EXPECT_EQ(value.AsBinaryCopy(&actual), sc::SC_OK);
        EXPECT_EQ(actual, expected);
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
    EXPECT_TRUE(editor::ParseSchemaTableDescription(schemaText, &result, &error)) << error.toStdString();

    EXPECT_EQ(result.tableMacroName, QStringLiteral("ProjectInfo"));
    EXPECT_EQ(result.tableName, QStringLiteral("ProjectInfo"));
    EXPECT_EQ(result.columns.size(), 5);
    EXPECT_EQ(result.columns[0].name, L"GUID");
    EXPECT_EQ(result.columns[1].name, L"ProjectName");
    EXPECT_EQ(QString::fromStdWString(result.columns[1].displayName), QStringLiteral("Project Name"));
    EXPECT_EQ(QString::fromStdWString(result.columns[2].displayName), QStringLiteral("Project Code"));
    EXPECT_EQ(QString::fromStdWString(result.columns[3].displayName), QStringLiteral("Owner Unit"));
    EXPECT_EQ(QString::fromStdWString(result.columns[4].displayName), QStringLiteral("Created At"));
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
    EXPECT_TRUE(editor::ParseSchemaTableDescription(schemaText, &result, &error)) << error.toStdString();

    ASSERT_EQ(result.columns.size(), 2);
    EXPECT_FALSE(result.columns[0].nullable);
    EXPECT_FALSE(result.columns[1].nullable);
    for (const QString& warning : result.warnings)
    {
        EXPECT_FALSE(warning.startsWith(QStringLiteral("Ignored schema token:"))) << warning.toStdString();
    }
}

TEST(DatabaseEditorSchemaTable, ParsesDefaultValues)
{
    const QString schemaText = QStringLiteral(R"(SC_SCHEMA_TABLE(Beam)
{
    Table("Beam")
        .Column("Width", SCType::Int64)
            .Default(300)
        .Column("Length", SCType::Double)
            .Default(12.5)
        .Column("Active", SCType::Bool)
            .Default(true)
        .Column("Name", SCType::String)
            .Default("")
        .Column("Alias", SCType::String)
            .Default("Beam \"A\"")
        .Column("Category", SCType::Enum)
            .Default("Primary")
        .Column("Payload", SCType::Binary)
            .Default(0xAABBCC)
        .Column("FloorRef", SCType::RecordId)
            .Default(42);
})");

    editor::SCSchemaTableImportResult result;
    QString error;
    EXPECT_TRUE(editor::ParseSchemaTableDescription(schemaText, &result, &error)) << error.toStdString();

    ASSERT_EQ(result.columns.size(), 8);

    EXPECT_EQ(result.columns[0].defaultValue.GetKind(), sc::ValueKind::Int64);
    std::int64_t widthDefault = 0;
    EXPECT_EQ(result.columns[0].defaultValue.AsInt64(&widthDefault), sc::SC_OK);
    EXPECT_EQ(widthDefault, 300);

    EXPECT_EQ(result.columns[1].defaultValue.GetKind(), sc::ValueKind::Double);
    double lengthDefault = 0.0;
    EXPECT_EQ(result.columns[1].defaultValue.AsDouble(&lengthDefault), sc::SC_OK);
    EXPECT_DOUBLE_EQ(lengthDefault, 12.5);

    EXPECT_EQ(result.columns[2].defaultValue.GetKind(), sc::ValueKind::Bool);
    bool activeDefault = false;
    EXPECT_EQ(result.columns[2].defaultValue.AsBool(&activeDefault), sc::SC_OK);
    EXPECT_TRUE(activeDefault);

    ExpectStringValue(result.columns[3].defaultValue, L"");
    ExpectStringValue(result.columns[4].defaultValue, L"Beam \"A\"");
    ExpectEnumValue(result.columns[5].defaultValue, L"Primary");
    ExpectBinaryValue(result.columns[6].defaultValue,
                      {0xAA, 0xBB, 0xCC});

    EXPECT_EQ(result.columns[7].defaultValue.GetKind(), sc::ValueKind::RecordId);
    sc::RecordId floorRefDefault = 0;
    EXPECT_EQ(result.columns[7].defaultValue.AsRecordId(&floorRefDefault), sc::SC_OK);
    EXPECT_EQ(floorRefDefault, 42);
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
    EXPECT_TRUE(editor::ParseSchemaTableDescription(schemaText, &result, &error)) << error.toStdString();

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
    id.defaultValue = sc::SCValue::FromInt64(7);
    snapshot.columns.push_back(id);

    sc::SCColumnDef width = MakeColumn(L"Width", sc::ValueKind::Double);
    width.displayName = L"Beam width";
    width.nullable = false;
    width.defaultValue = sc::SCValue::FromDouble(12.5);
    snapshot.columns.push_back(width);

    sc::SCColumnDef active = MakeColumn(L"Active", sc::ValueKind::Bool);
    active.displayName = L"Enabled flag";
    active.nullable = false;
    active.defaultValue = sc::SCValue::FromBool(true);
    snapshot.columns.push_back(active);

    sc::SCColumnDef name = MakeColumn(L"Name", sc::ValueKind::String);
    name.displayName = L"Element name";
    name.nullable = false;
    name.defaultValue = sc::SCValue::FromString(L"");
    snapshot.columns.push_back(name);

    sc::SCColumnDef alias = MakeColumn(L"Alias", sc::ValueKind::String);
    alias.displayName = L"Display alias";
    alias.nullable = false;
    alias.defaultValue = sc::SCValue::FromString(L"Beam \"A\"");
    snapshot.columns.push_back(alias);

    sc::SCColumnDef category = MakeColumn(L"Category", sc::ValueKind::Enum);
    category.displayName = L"Category";
    category.nullable = false;
    category.defaultValue = sc::SCValue::FromEnum(L"Primary");
    snapshot.columns.push_back(category);

    sc::SCColumnDef payload = MakeColumn(L"Payload", sc::ValueKind::Binary);
    payload.displayName = L"Payload bytes";
    payload.nullable = false;
    payload.defaultValue =
        sc::SCValue::FromBinary(std::vector<std::uint8_t>{0xAA, 0xBB, 0xCC});
    snapshot.columns.push_back(payload);

    sc::SCColumnDef floorCode = MakeColumn(L"FloorCode", sc::ValueKind::String);
    floorCode.displayName = L"Belongs to floor";
    floorCode.nullable = false;
    floorCode.referenceTable = L"Floor";
    floorCode.referenceStorageColumn = L"Code";
    floorCode.referenceDisplayColumn = L"Name";
    floorCode.defaultValue = sc::SCValue::FromString(L"F-001");
    snapshot.columns.push_back(floorCode);

    sc::SCConstraintDef primaryKey;
    primaryKey.kind = sc::SCConstraintKind::PrimaryKey;
    primaryKey.columns.push_back(L"Id");
    snapshot.constraints.push_back(primaryKey);

    sc::SCIndexDef index;
    index.name = L"idx_Element_FloorCode";
    index.columns.push_back(sc::SCIndexColumnDef{L"FloorCode", false});
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
        "            .Default(7)\n"
        "            .Description(\"Unique element ID\")\n"
        "        .Column(\"Width\", SCType::Double)\n"
        "            .NotNull()\n"
        "            .Default(12.5)\n"
        "            .Description(\"Beam width\")\n"
        "        .Column(\"Active\", SCType::Bool)\n"
        "            .NotNull()\n"
        "            .Default(true)\n"
        "            .Description(\"Enabled flag\")\n"
        "        .Column(\"Name\", SCType::String)\n"
        "            .NotNull()\n"
        "            .Default(\"\")\n"
        "            .Description(\"Element name\")\n"
        "        .Column(\"Alias\", SCType::String)\n"
        "            .NotNull()\n"
        "            .Default(\"Beam \\\"A\\\"\")\n"
        "            .Description(\"Display alias\")\n"
        "        .Column(\"Category\", SCType::Enum)\n"
        "            .NotNull()\n"
        "            .Default(\"Primary\")\n"
        "        .Column(\"Payload\", SCType::Binary)\n"
        "            .NotNull()\n"
        "            .Default(0xAABBCC)\n"
        "            .Description(\"Payload bytes\")\n"
        "        .Column(\"FloorCode\", SCType::String)\n"
        "            .NotNull()\n"
        "            .Default(\"F-001\")\n"
        "            .Ref(\"Floor\", \"Code\", \"Name\")\n"
        "            .Description(\"Belongs to floor\")\n"
        "        .Index(\"idx_Element_FloorCode\").Columns(\"FloorCode\");\n"
        "}\n");

    EXPECT_EQ(code, expected);
}

TEST(DatabaseEditorSchemaTable, ParsesRelationReferenceStorageAndDisplayColumns)
{
    const QString schemaText = QStringLiteral(R"(SC_SCHEMA_TABLE(Beam)
{
    Table("Beam")
        .Column("FloorRef", SCType::String)
            .Ref("Floor", "Code", "Name");
})");

    editor::SCSchemaTableImportResult result;
    QString error;
    EXPECT_TRUE(editor::ParseSchemaTableDescription(schemaText, &result, &error)) << error.toStdString();

    ASSERT_EQ(result.columns.size(), 1);
    EXPECT_EQ(result.columns[0].columnKind, sc::ColumnKind::Relation);
    EXPECT_EQ(result.columns[0].referenceTable, L"Floor");
    EXPECT_EQ(result.columns[0].referenceStorageColumn, L"Code");
    EXPECT_EQ(result.columns[0].referenceDisplayColumn, L"Name");
}

TEST(DatabaseEditorSchemaTable, ParsesRelationReferenceStorageColumnOnly)
{
    const QString schemaText = QStringLiteral(R"(SC_SCHEMA_TABLE(Beam)
{
    Table("Beam")
        .Column("FloorRef", SCType::String)
            .Ref("Floor", "Code");
})");

    editor::SCSchemaTableImportResult result;
    QString error;
    EXPECT_TRUE(editor::ParseSchemaTableDescription(schemaText, &result, &error)) << error.toStdString();

    ASSERT_EQ(result.columns.size(), 1);
    EXPECT_EQ(result.columns[0].columnKind, sc::ColumnKind::Relation);
    EXPECT_EQ(result.columns[0].referenceTable, L"Floor");
    EXPECT_EQ(result.columns[0].referenceStorageColumn, L"Code");
    EXPECT_EQ(result.columns[0].referenceDisplayColumn, L"Code");
}
