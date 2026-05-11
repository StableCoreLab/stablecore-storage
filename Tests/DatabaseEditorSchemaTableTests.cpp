#include <gtest/gtest.h>

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

TEST(DatabaseEditorSchemaTable, BuildsCurrentTableSchemaCode)
{
    sc::SCTableSchemaSnapshot snapshot;
    snapshot.table.name = L"Element";
    snapshot.table.description = L"构件主表";

    sc::SCColumnDef id = MakeColumn(L"Id", sc::ValueKind::Int64);
    id.displayName = L"构件唯一 ID";
    id.nullable = false;
    snapshot.columns.push_back(id);

    sc::SCColumnDef floorId = MakeColumn(L"FloorId", sc::ValueKind::Int64);
    floorId.displayName = L"所属楼层";
    floorId.nullable = false;
    floorId.referenceTable = L"Floor";
    snapshot.columns.push_back(floorId);

    sc::SCColumnDef elementType = MakeColumn(L"ElementType", sc::ValueKind::Int64);
    elementType.displayName = L"构件类型";
    elementType.nullable = false;
    snapshot.columns.push_back(elementType);

    sc::SCColumnDef name = MakeColumn(L"Name", sc::ValueKind::String);
    name.displayName = L"构件名称";
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
        "        .Description(\"构件主表\")\n"
        "        .PrimaryKey(\"Id\")\n"
        "        .Column(\"Id\", SCType::Int64)\n"
        "            .NotNull()\n"
        "            .Description(\"构件唯一 ID\")\n"
        "        .Column(\"FloorId\", SCType::Int64)\n"
        "            .NotNull()\n"
        "            .Ref(\"Floor\", \"Id\")\n"
        "            .Description(\"所属楼层\")\n"
        "        .Column(\"ElementType\", SCType::Int64)\n"
        "            .NotNull()\n"
        "            .Description(\"构件类型\")\n"
        "        .Column(\"Name\", SCType::String)\n"
        "            .NotNull()\n"
        "            .Description(\"构件名称\")\n"
        "        .Index(\"idx_Element_FloorId\").Columns(\"FloorId\");\n"
        "}\n");

    EXPECT_EQ(code, expected);
}
