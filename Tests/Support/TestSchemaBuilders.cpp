#include "TestSchemaBuilders.h"
#include <gtest/gtest.h>

sc::SCColumnDef MakeIntColumn(const wchar_t* name, bool nullable)
{
    sc::SCColumnDef column;
    column.name = name;
    column.displayName = name;
    column.valueKind = sc::ValueKind::Int64;
    column.nullable = nullable;
    column.defaultValue = nullable ? sc::SCValue::Null() : sc::SCValue::FromInt64(0);
    return column;
}

sc::SCColumnDef MakeStringColumn(const wchar_t* name, bool nullable)
{
    sc::SCColumnDef column;
    column.name = name;
    column.displayName = name;
    column.valueKind = sc::ValueKind::String;
    column.nullable = nullable;
    column.defaultValue = nullable ? sc::SCValue::Null() : sc::SCValue::FromString(L"");
    return column;
}

sc::SCConstraintDef MakeUniqueConstraint(const wchar_t* name, const wchar_t* columnName)
{
    sc::SCConstraintDef constraint;
    constraint.kind = sc::SCConstraintKind::Unique;
    constraint.name = name;
    constraint.columns.push_back(columnName);
    return constraint;
}

sc::SCConstraintDef MakeForeignKeyConstraint(const wchar_t* name,
                                             const wchar_t* columnName,
                                             const wchar_t* targetTable,
                                             const wchar_t* targetColumn)
{
    sc::SCConstraintDef constraint;
    constraint.kind = sc::SCConstraintKind::ForeignKey;
    constraint.name = name;
    constraint.columns.push_back(columnName);
    constraint.referencedTable = targetTable;
    constraint.referencedColumns.push_back(targetColumn);
    return constraint;
}

sc::SCIndexDef MakeIndex(const wchar_t* name, const wchar_t* columnName)
{
    sc::SCIndexDef index;
    index.name = name;
    index.columns.push_back(sc::SCIndexColumnDef{columnName, false});
    return index;
}

sc::SCIndexDef MakeCompositeIndex(const wchar_t* name, const wchar_t* firstColumn, const wchar_t* secondColumn)
{
    sc::SCIndexDef index;
    index.name = name;
    index.columns.push_back(sc::SCIndexColumnDef{firstColumn, false});
    index.columns.push_back(sc::SCIndexColumnDef{secondColumn, false});
    return index;
}

sc::SCTablePtr CreateWidthOnlyBeamTable(sc::SCDbPtr& db)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
    EXPECT_EQ(schema->AddColumn(MakeIntColumn(L"Width")), sc::SC_OK);
    return table;
}

sc::SCTablePtr CreateBeamTable(sc::SCDbPtr& db)
{
    sc::SCTablePtr table = CreateWidthOnlyBeamTable(db);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
    EXPECT_EQ(schema->AddColumn(MakeStringColumn(L"Name", true)), sc::SC_OK);
    return table;
}

sc::SCTablePtr CreateQueryableBeamTable(sc::SCDbPtr& db)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef width;
    width.name = L"Width";
    width.displayName = L"Width";
    width.valueKind = sc::ValueKind::Int64;
    width.indexed = true;
    width.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(width), sc::SC_OK);

    sc::SCColumnDef name;
    name.name = L"Name";
    name.displayName = L"Name";
    name.valueKind = sc::ValueKind::String;
    name.indexed = true;
    name.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);

    return table;
}

sc::SCTablePtr CreateCompositeIndexedBeamTable(sc::SCDbPtr& db)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef width;
    width.name = L"Width";
    width.displayName = L"Width";
    width.valueKind = sc::ValueKind::Int64;
    width.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(width), sc::SC_OK);

    sc::SCColumnDef name;
    name.name = L"Name";
    name.displayName = L"Name";
    name.valueKind = sc::ValueKind::String;
    name.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);

    sc::SCIndexDef compositeIndex;
    compositeIndex.name = L"idx_Beam_Width_Name";
    compositeIndex.columns.push_back(sc::SCIndexColumnDef{L"Width", false});
    compositeIndex.columns.push_back(sc::SCIndexColumnDef{L"Name", false});
    EXPECT_EQ(schema->AddIndex(compositeIndex), sc::SC_OK);

    return table;
}

sc::SCTablePtr CreateCompositeIndexedBeamTableWithLegacyWidth(sc::SCDbPtr& db)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef width;
    width.name = L"Width";
    width.displayName = L"Width";
    width.valueKind = sc::ValueKind::Int64;
    width.indexed = true;
    width.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(width), sc::SC_OK);

    sc::SCColumnDef name;
    name.name = L"Name";
    name.displayName = L"Name";
    name.valueKind = sc::ValueKind::String;
    name.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);

    sc::SCIndexDef compositeIndex;
    compositeIndex.name = L"idx_Beam_Width_Name";
    compositeIndex.columns.push_back(sc::SCIndexColumnDef{L"Width", false});
    compositeIndex.columns.push_back(sc::SCIndexColumnDef{L"Name", false});
    EXPECT_EQ(schema->AddIndex(compositeIndex), sc::SC_OK);

    return table;
}

sc::SCTablePtr CreateDescendingCompositeIndexedBeamTable(sc::SCDbPtr& db)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef width;
    width.name = L"Width";
    width.displayName = L"Width";
    width.valueKind = sc::ValueKind::Int64;
    width.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(width), sc::SC_OK);

    sc::SCColumnDef name;
    name.name = L"Name";
    name.displayName = L"Name";
    name.valueKind = sc::ValueKind::String;
    name.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);

    sc::SCIndexDef compositeIndex;
    compositeIndex.name = L"idx_Beam_Width_Name_Desc";
    compositeIndex.columns.push_back(sc::SCIndexColumnDef{L"Width", false});
    compositeIndex.columns.push_back(sc::SCIndexColumnDef{L"Name", true});
    EXPECT_EQ(schema->AddIndex(compositeIndex), sc::SC_OK);

    return table;
}

sc::SCTablePtr CreateTripleCompositeIndexedBeamTable(sc::SCDbPtr& db)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef width;
    width.name = L"Width";
    width.displayName = L"Width";
    width.valueKind = sc::ValueKind::Int64;
    width.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(width), sc::SC_OK);

    sc::SCColumnDef name;
    name.name = L"Name";
    name.displayName = L"Name";
    name.valueKind = sc::ValueKind::String;
    name.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);

    sc::SCColumnDef height;
    height.name = L"Height";
    height.displayName = L"Height";
    height.valueKind = sc::ValueKind::Int64;
    height.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(height), sc::SC_OK);

    sc::SCIndexDef compositeIndex;
    compositeIndex.name = L"idx_Beam_Width_Name_Height";
    compositeIndex.columns.push_back(sc::SCIndexColumnDef{L"Width", false});
    compositeIndex.columns.push_back(sc::SCIndexColumnDef{L"Name", false});
    compositeIndex.columns.push_back(sc::SCIndexColumnDef{L"Height", false});
    EXPECT_EQ(schema->AddIndex(compositeIndex), sc::SC_OK);

    return table;
}

sc::SCTablePtr CreateDescendingTripleCompositeIndexedBeamTable(sc::SCDbPtr& db)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef width;
    width.name = L"Width";
    width.displayName = L"Width";
    width.valueKind = sc::ValueKind::Int64;
    width.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(width), sc::SC_OK);

    sc::SCColumnDef name;
    name.name = L"Name";
    name.displayName = L"Name";
    name.valueKind = sc::ValueKind::String;
    name.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);

    sc::SCColumnDef height;
    height.name = L"Height";
    height.displayName = L"Height";
    height.valueKind = sc::ValueKind::Int64;
    height.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(height), sc::SC_OK);

    sc::SCIndexDef compositeIndex;
    compositeIndex.name = L"idx_Beam_Width_Name_Height_Desc";
    compositeIndex.columns.push_back(sc::SCIndexColumnDef{L"Width", false});
    compositeIndex.columns.push_back(sc::SCIndexColumnDef{L"Name", true});
    compositeIndex.columns.push_back(sc::SCIndexColumnDef{L"Height", true});
    EXPECT_EQ(schema->AddIndex(compositeIndex), sc::SC_OK);

    return table;
}

sc::SCTablePtr CreateCompetingCompositeIndexedBeamTable(sc::SCDbPtr& db)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef width;
    width.name = L"Width";
    width.displayName = L"Width";
    width.valueKind = sc::ValueKind::Int64;
    width.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(width), sc::SC_OK);

    sc::SCColumnDef name;
    name.name = L"Name";
    name.displayName = L"Name";
    name.valueKind = sc::ValueKind::String;
    name.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);

    sc::SCColumnDef height;
    height.name = L"Height";
    height.displayName = L"Height";
    height.valueKind = sc::ValueKind::Int64;
    height.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(height), sc::SC_OK);

    sc::SCIndexDef widthNameIndex;
    widthNameIndex.name = L"idx_Beam_Width_Name";
    widthNameIndex.columns.push_back(sc::SCIndexColumnDef{L"Width", false});
    widthNameIndex.columns.push_back(sc::SCIndexColumnDef{L"Name", false});
    EXPECT_EQ(schema->AddIndex(widthNameIndex), sc::SC_OK);

    sc::SCIndexDef widthHeightIndex;
    widthHeightIndex.name = L"idx_Beam_Width_Height";
    widthHeightIndex.columns.push_back(sc::SCIndexColumnDef{L"Width", false});
    widthHeightIndex.columns.push_back(sc::SCIndexColumnDef{L"Height", false});
    EXPECT_EQ(schema->AddIndex(widthHeightIndex), sc::SC_OK);

    return table;
}

sc::SCTablePtr CreateNullableCompositeIndexedBeamTable(sc::SCDbPtr& db)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef width;
    width.name = L"Width";
    width.displayName = L"Width";
    width.valueKind = sc::ValueKind::Int64;
    width.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(width), sc::SC_OK);

    sc::SCColumnDef name;
    name.name = L"Name";
    name.displayName = L"Name";
    name.valueKind = sc::ValueKind::String;
    name.nullable = true;
    name.defaultValue = sc::SCValue::Null();
    EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);

    sc::SCIndexDef compositeIndex;
    compositeIndex.name = L"idx_Beam_Width_Name";
    compositeIndex.columns.push_back(sc::SCIndexColumnDef{L"Width", false});
    compositeIndex.columns.push_back(sc::SCIndexColumnDef{L"Name", false});
    EXPECT_EQ(schema->AddIndex(compositeIndex), sc::SC_OK);

    return table;
}
