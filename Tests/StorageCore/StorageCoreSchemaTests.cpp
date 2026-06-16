#include <gtest/gtest.h>

#include "SCStorage.h"

#include "Support/TestFixtures.h"

namespace sc = StableCore::Storage;

// 迁移自 M1Tests.cpp - SchemaRejectsInvalidRelationDefault
// 测试模式拒绝无效的关系字段默认值
TEST_F(FileDbTest, SchemaRejectsInvalidRelationDefault)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db()->CreateTable(L"RelationHolder", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef relation;
    relation.name = L"FloorRef";
    relation.columnKind = sc::ColumnKind::Relation;
    relation.valueKind = sc::ValueKind::RecordId;
    relation.defaultValue = sc::SCValue::FromString(L"bad");

    EXPECT_EQ(schema->AddColumn(relation), sc::SC_E_SCHEMA_VIOLATION);
}

// 新增测试：模式拒绝无效的引用表
TEST_F(FileDbTest, SchemaRejectsInvalidReferenceTableUsage)
{
    sc::SCTablePtr beamTable;
    EXPECT_EQ(db()->CreateTable(L"Beam", beamTable), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(beamTable->GetSchema(schema), sc::SC_OK);

    // 普通字段不能设置 referenceTable
    sc::SCColumnDef factWithRef;
    factWithRef.name = L"Width";
    factWithRef.valueKind = sc::ValueKind::Int64;
    factWithRef.referenceTable = L"Floor";
    EXPECT_EQ(schema->AddColumn(factWithRef), sc::SC_E_SCHEMA_VIOLATION);

    // 关系字段必须设置 referenceTable
    sc::SCColumnDef relationWithoutRef;
    relationWithoutRef.name = L"FloorRef";
    relationWithoutRef.valueKind = sc::ValueKind::RecordId;
    relationWithoutRef.columnKind = sc::ColumnKind::Relation;
    EXPECT_EQ(schema->AddColumn(relationWithoutRef), sc::SC_E_SCHEMA_VIOLATION);
}

// 新增测试：关系字段支持显式存储列和显示列
TEST_F(FileDbTest, SchemaAcceptsRelationStorageAndDisplayColumns)
{
    sc::SCTablePtr floorTable;
    EXPECT_EQ(db()->CreateTable(L"Floor", floorTable), sc::SC_OK);

    sc::SCSchemaPtr floorSchema;
    EXPECT_EQ(floorTable->GetSchema(floorSchema), sc::SC_OK);

    sc::SCColumnDef code;
    code.name = L"Code";
    code.displayName = L"Code";
    code.valueKind = sc::ValueKind::String;
    code.nullable = false;
    code.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(floorSchema->AddColumn(code), sc::SC_OK);

    sc::SCConstraintDef codeUnique;
    codeUnique.kind = sc::SCConstraintKind::Unique;
    codeUnique.name = L"uq_Floor_Code";
    codeUnique.columns.push_back(L"Code");
    EXPECT_EQ(floorSchema->AddConstraint(codeUnique), sc::SC_OK);

    sc::SCColumnDef name;
    name.name = L"Name";
    name.displayName = L"Name";
    name.valueKind = sc::ValueKind::String;
    name.nullable = false;
    name.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(floorSchema->AddColumn(name), sc::SC_OK);

    sc::SCTablePtr beamTable;
    EXPECT_EQ(db()->CreateTable(L"Beam", beamTable), sc::SC_OK);

    sc::SCSchemaPtr beamSchema;
    EXPECT_EQ(beamTable->GetSchema(beamSchema), sc::SC_OK);

    sc::SCColumnDef floorRef;
    floorRef.name = L"FloorRef";
    floorRef.displayName = L"FloorRef";
    floorRef.valueKind = sc::ValueKind::String;
    floorRef.columnKind = sc::ColumnKind::Relation;
    floorRef.referenceTable = L"Floor";
    floorRef.referenceStorageColumn = L"Code";
    floorRef.referenceDisplayColumn = L"Name";
    EXPECT_EQ(beamSchema->AddColumn(floorRef), sc::SC_OK);
}

// 新增测试：关系字段存储列类型必须与字段值类型一致
TEST_F(FileDbTest, SchemaRejectsRelationValueKindMismatchWithStorageColumn)
{
    sc::SCTablePtr floorTable;
    EXPECT_EQ(db()->CreateTable(L"Floor", floorTable), sc::SC_OK);

    sc::SCSchemaPtr floorSchema;
    EXPECT_EQ(floorTable->GetSchema(floorSchema), sc::SC_OK);

    sc::SCColumnDef code;
    code.name = L"Code";
    code.displayName = L"Code";
    code.valueKind = sc::ValueKind::String;
    code.nullable = false;
    code.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(floorSchema->AddColumn(code), sc::SC_OK);

    sc::SCConstraintDef codeUnique;
    codeUnique.kind = sc::SCConstraintKind::Unique;
    codeUnique.name = L"uq_Floor_Code";
    codeUnique.columns.push_back(L"Code");
    EXPECT_EQ(floorSchema->AddConstraint(codeUnique), sc::SC_OK);

    sc::SCTablePtr beamTable;
    EXPECT_EQ(db()->CreateTable(L"Beam", beamTable), sc::SC_OK);

    sc::SCSchemaPtr beamSchema;
    EXPECT_EQ(beamTable->GetSchema(beamSchema), sc::SC_OK);

    sc::SCColumnDef floorRef;
    floorRef.name = L"FloorRef";
    floorRef.displayName = L"FloorRef";
    floorRef.valueKind = sc::ValueKind::RecordId;
    floorRef.columnKind = sc::ColumnKind::Relation;
    floorRef.referenceTable = L"Floor";
    floorRef.referenceStorageColumn = L"Code";
    floorRef.referenceDisplayColumn = L"Name";

    EXPECT_EQ(beamSchema->AddColumn(floorRef), sc::SC_E_SCHEMA_VIOLATION);
}

// 新增测试：显示列不能单独存在
TEST_F(FileDbTest, SchemaRejectsRelationDisplayColumnWithoutStorageColumn)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db()->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef relation;
    relation.name = L"FloorRef";
    relation.displayName = L"FloorRef";
    relation.valueKind = sc::ValueKind::String;
    relation.columnKind = sc::ColumnKind::Relation;
    relation.referenceTable = L"Floor";
    relation.referenceDisplayColumn = L"Name";

    EXPECT_EQ(schema->AddColumn(relation), sc::SC_E_SCHEMA_VIOLATION);
}

// 新增测试：关系字段引用不存在的表时应被拒绝
TEST_F(FileDbTest, SchemaRejectsMissingRelationTargetTable)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db()->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef relation;
    relation.name = L"FloorRef";
    relation.displayName = L"FloorRef";
    relation.valueKind = sc::ValueKind::String;
    relation.columnKind = sc::ColumnKind::Relation;
    relation.referenceTable = L"MissingFloor";
    relation.referenceStorageColumn = L"Code";
    relation.referenceDisplayColumn = L"Name";

    EXPECT_EQ(schema->AddColumn(relation), sc::SC_E_TABLE_NOT_FOUND);
}

// 新增测试：关系字段引用不存在的存储列时应被拒绝
TEST_F(FileDbTest, SchemaRejectsMissingRelationStorageColumn)
{
    sc::SCTablePtr floorTable;
    EXPECT_EQ(db()->CreateTable(L"Floor", floorTable), sc::SC_OK);

    sc::SCSchemaPtr floorSchema;
    EXPECT_EQ(floorTable->GetSchema(floorSchema), sc::SC_OK);

    sc::SCColumnDef code;
    code.name = L"Code";
    code.displayName = L"Code";
    code.valueKind = sc::ValueKind::String;
    code.nullable = false;
    code.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(floorSchema->AddColumn(code), sc::SC_OK);

    sc::SCConstraintDef codeUnique;
    codeUnique.kind = sc::SCConstraintKind::Unique;
    codeUnique.name = L"uq_Floor_Code";
    codeUnique.columns.push_back(L"Code");
    EXPECT_EQ(floorSchema->AddConstraint(codeUnique), sc::SC_OK);

    sc::SCTablePtr beamTable;
    EXPECT_EQ(db()->CreateTable(L"Beam", beamTable), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(beamTable->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef relation;
    relation.name = L"FloorRef";
    relation.displayName = L"FloorRef";
    relation.valueKind = sc::ValueKind::String;
    relation.columnKind = sc::ColumnKind::Relation;
    relation.referenceTable = L"Floor";
    relation.referenceStorageColumn = L"MissingCode";
    relation.referenceDisplayColumn = L"Code";

    EXPECT_EQ(schema->AddColumn(relation), sc::SC_E_COLUMN_NOT_FOUND);
}

// 新增测试：关系字段引用不存在的显示列时应被拒绝
TEST_F(FileDbTest, SchemaRejectsMissingRelationDisplayColumn)
{
    sc::SCTablePtr floorTable;
    EXPECT_EQ(db()->CreateTable(L"Floor", floorTable), sc::SC_OK);

    sc::SCSchemaPtr floorSchema;
    EXPECT_EQ(floorTable->GetSchema(floorSchema), sc::SC_OK);

    sc::SCColumnDef code;
    code.name = L"Code";
    code.displayName = L"Code";
    code.valueKind = sc::ValueKind::String;
    code.nullable = false;
    code.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(floorSchema->AddColumn(code), sc::SC_OK);

    sc::SCConstraintDef codeUnique;
    codeUnique.kind = sc::SCConstraintKind::Unique;
    codeUnique.name = L"uq_Floor_Code";
    codeUnique.columns.push_back(L"Code");
    EXPECT_EQ(floorSchema->AddConstraint(codeUnique), sc::SC_OK);

    sc::SCTablePtr beamTable;
    EXPECT_EQ(db()->CreateTable(L"Beam", beamTable), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(beamTable->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef relation;
    relation.name = L"FloorRef";
    relation.displayName = L"FloorRef";
    relation.valueKind = sc::ValueKind::String;
    relation.columnKind = sc::ColumnKind::Relation;
    relation.referenceTable = L"Floor";
    relation.referenceStorageColumn = L"Code";
    relation.referenceDisplayColumn = L"MissingName";

    EXPECT_EQ(schema->AddColumn(relation), sc::SC_E_COLUMN_NOT_FOUND);
}

// 新增测试：关系字段存储列不能为空列
TEST_F(FileDbTest, SchemaRejectsNullableRelationStorageColumn)
{
    sc::SCTablePtr floorTable;
    EXPECT_EQ(db()->CreateTable(L"Floor", floorTable), sc::SC_OK);

    sc::SCSchemaPtr floorSchema;
    EXPECT_EQ(floorTable->GetSchema(floorSchema), sc::SC_OK);

    sc::SCColumnDef code;
    code.name = L"Code";
    code.displayName = L"Code";
    code.valueKind = sc::ValueKind::String;
    code.nullable = true;
    EXPECT_EQ(floorSchema->AddColumn(code), sc::SC_OK);

    sc::SCConstraintDef codeUnique;
    codeUnique.kind = sc::SCConstraintKind::Unique;
    codeUnique.name = L"uq_Floor_Code";
    codeUnique.columns.push_back(L"Code");
    EXPECT_EQ(floorSchema->AddConstraint(codeUnique), sc::SC_OK);

    sc::SCTablePtr beamTable;
    EXPECT_EQ(db()->CreateTable(L"Beam", beamTable), sc::SC_OK);

    sc::SCSchemaPtr beamSchema;
    EXPECT_EQ(beamTable->GetSchema(beamSchema), sc::SC_OK);

    sc::SCColumnDef relation;
    relation.name = L"FloorRef";
    relation.displayName = L"FloorRef";
    relation.valueKind = sc::ValueKind::String;
    relation.columnKind = sc::ColumnKind::Relation;
    relation.referenceTable = L"Floor";
    relation.referenceStorageColumn = L"Code";
    relation.referenceDisplayColumn = L"Code";

    EXPECT_EQ(beamSchema->AddColumn(relation), sc::SC_E_SCHEMA_VIOLATION);
}

// 新增测试：关系字段存储列必须唯一
TEST_F(FileDbTest, SchemaRejectsNonUniqueRelationStorageColumn)
{
    sc::SCTablePtr floorTable;
    EXPECT_EQ(db()->CreateTable(L"Floor", floorTable), sc::SC_OK);

    sc::SCSchemaPtr floorSchema;
    EXPECT_EQ(floorTable->GetSchema(floorSchema), sc::SC_OK);

    sc::SCColumnDef code;
    code.name = L"Code";
    code.displayName = L"Code";
    code.valueKind = sc::ValueKind::String;
    code.nullable = false;
    code.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(floorSchema->AddColumn(code), sc::SC_OK);

    sc::SCTablePtr beamTable;
    EXPECT_EQ(db()->CreateTable(L"Beam", beamTable), sc::SC_OK);

    sc::SCSchemaPtr beamSchema;
    EXPECT_EQ(beamTable->GetSchema(beamSchema), sc::SC_OK);

    sc::SCColumnDef relation;
    relation.name = L"FloorRef";
    relation.displayName = L"FloorRef";
    relation.valueKind = sc::ValueKind::String;
    relation.columnKind = sc::ColumnKind::Relation;
    relation.referenceTable = L"Floor";
    relation.referenceStorageColumn = L"Code";
    relation.referenceDisplayColumn = L"Code";

    EXPECT_EQ(beamSchema->AddColumn(relation), sc::SC_E_SCHEMA_VIOLATION);
}

// 新增测试：模式允许有效的列定义
TEST_F(FileDbTest, SchemaAcceptsValidColumnDefinition)
{
    sc::SCTablePtr beamTable;
    EXPECT_EQ(db()->CreateTable(L"Beam", beamTable), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(beamTable->GetSchema(schema), sc::SC_OK);

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
    EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);

    // 验证列已添加
    sc::SCTableSchemaSnapshot snapshot;
    EXPECT_EQ(schema->GetSchemaSnapshot(&snapshot), sc::SC_OK);
    EXPECT_EQ(snapshot.columns.size(), 2u);
}

// 新增测试：模式拒绝重复列名
TEST_F(FileDbTest, SchemaRejectsDuplicateColumnName)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db()->CreateTable(L"TestTable", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef col1;
    col1.name = L"Width";
    col1.valueKind = sc::ValueKind::Int64;
    EXPECT_EQ(schema->AddColumn(col1), sc::SC_OK);

    // 尝试添加同名列
    sc::SCColumnDef col2;
    col2.name = L"Width";
    col2.valueKind = sc::ValueKind::String;
    EXPECT_EQ(schema->AddColumn(col2), sc::SC_E_COLUMN_EXISTS);
}

// 新增测试：模式支持非空约束
TEST_F(FileDbTest, SchemaSupportsNotNullableConstraint)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db()->CreateTable(L"TestTable", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef requiredCol;
    requiredCol.name = L"RequiredField";
    requiredCol.valueKind = sc::ValueKind::Int64;
    requiredCol.nullable = false;
    requiredCol.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(requiredCol), sc::SC_OK);

    sc::SCColumnDef optionalCol;
    optionalCol.name = L"OptionalField";
    optionalCol.valueKind = sc::ValueKind::String;
    optionalCol.nullable = true;
    EXPECT_EQ(schema->AddColumn(optionalCol), sc::SC_OK);

    sc::SCTableSchemaSnapshot snapshot;
    EXPECT_EQ(schema->GetSchemaSnapshot(&snapshot), sc::SC_OK);

    // 验证 nullable 设置
    auto findCol = [&snapshot](const std::wstring& name) -> const sc::SCColumnDef*
    {
        for (const auto& col : snapshot.columns)
        {
            if (col.name == name)
            {
                return &col;
            }
        }
        return nullptr;
    };

    const sc::SCColumnDef* requiredColSnapshot = findCol(L"RequiredField");
    const sc::SCColumnDef* optionalColSnapshot = findCol(L"OptionalField");

    ASSERT_NE(requiredColSnapshot, nullptr);
    ASSERT_NE(optionalColSnapshot, nullptr);

    EXPECT_FALSE(requiredColSnapshot->nullable);
    EXPECT_TRUE(optionalColSnapshot->nullable);
}

// 补回自 M1Tests.cpp - SchemaUpdateColumnReplacesDefinition
// 测试更新列定义会被替换
TEST_F(FileDbTest, SchemaUpdateColumnReplacesDefinition)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db()->CreateTable(L"UpdateDefTable", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef col1;
    col1.name = L"Col1";
    col1.displayName = L"Col1";
    col1.valueKind = sc::ValueKind::Int64;
    col1.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(col1), sc::SC_OK);

    sc::SCColumnDef updatedCol1;
    updatedCol1.name = L"Col1";
    updatedCol1.displayName = L"Col1 Label";
    updatedCol1.valueKind = sc::ValueKind::String;
    updatedCol1.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(schema->UpdateColumn(updatedCol1), sc::SC_OK);

    sc::SCColumnDef found;
    EXPECT_EQ(schema->FindColumn(L"Col1", &found), sc::SC_OK);
    EXPECT_EQ(found.displayName, L"Col1 Label");
    EXPECT_EQ(found.valueKind, sc::ValueKind::String);
}

// 补回自 M1Tests.cpp - SchemaUpdateColumnMigratesCompatibleValues
// 测试兼容的值迁移
TEST_F(FileDbTest, SchemaUpdateColumnMigratesCompatibleValues)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db()->CreateTable(L"MigrateTable", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef col1;
    col1.name = L"Col1";
    col1.valueKind = sc::ValueKind::Int64;
    col1.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->AddColumn(col1), sc::SC_OK);

    sc::SCEditPtr createEdit;
    EXPECT_EQ(db()->BeginEdit(L"create", createEdit), sc::SC_OK);

    sc::SCRecordPtr record;
    EXPECT_EQ(table->CreateRecord(record), sc::SC_OK);
    EXPECT_EQ(record->SetInt64(L"Col1", 42), sc::SC_OK);

    EXPECT_EQ(db()->Commit(createEdit.Get()), sc::SC_OK);

    sc::SCColumnDef updatedCol1;
    updatedCol1.name = L"Col1";
    updatedCol1.valueKind = sc::ValueKind::String;
    updatedCol1.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(schema->UpdateColumn(updatedCol1), sc::SC_OK);

    std::wstring value;
    EXPECT_EQ(record->GetStringCopy(L"Col1", &value), sc::SC_OK);
    EXPECT_EQ(value, L"42");
}

// 补回自 M1Tests.cpp - SchemaUpdateColumnRejectsIncompatibleValues
// 测试不兼容的值迁移被拒绝
TEST_F(FileDbTest, SchemaUpdateColumnRejectsIncompatibleValues)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db()->CreateTable(L"IncompatTable", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef data;
    data.name = L"Data";
    data.valueKind = sc::ValueKind::String;
    data.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(schema->AddColumn(data), sc::SC_OK);

    sc::SCEditPtr createEdit;
    EXPECT_EQ(db()->BeginEdit(L"create", createEdit), sc::SC_OK);

    sc::SCRecordPtr record;
    EXPECT_EQ(table->CreateRecord(record), sc::SC_OK);
    EXPECT_EQ(record->SetString(L"Data", L"not-a-number"), sc::SC_OK);

    EXPECT_EQ(db()->Commit(createEdit.Get()), sc::SC_OK);

    sc::SCColumnDef updatedData;
    updatedData.name = L"Data";
    updatedData.valueKind = sc::ValueKind::Int64;
    updatedData.defaultValue = sc::SCValue::FromInt64(0);
    EXPECT_EQ(schema->UpdateColumn(updatedData), sc::SC_E_TYPE_MISMATCH);

    std::wstring value;
    EXPECT_EQ(record->GetStringCopy(L"Data", &value), sc::SC_OK);
    EXPECT_EQ(value, L"not-a-number");

    sc::SCColumnDef found;
    EXPECT_EQ(schema->FindColumn(L"Data", &found), sc::SC_OK);
    EXPECT_EQ(found.valueKind, sc::ValueKind::String);
}

// 补回自 M1Tests.cpp - CommitRejectsMissingRequiredValuesForNewRecords
// 测试提交时拒绝缺少必填值的新记录
TEST_F(FileDbTest, CommitRejectsMissingRequiredValuesForNewRecords)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db()->CreateTable(L"ReqFieldTable", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef requiredCol;
    requiredCol.name = L"RequiredField";
    requiredCol.valueKind = sc::ValueKind::Int64;
    requiredCol.nullable = false;
    requiredCol.defaultValue = sc::SCValue::Null();
    EXPECT_EQ(schema->AddColumn(requiredCol), sc::SC_OK);

    sc::SCEditPtr createEdit;
    EXPECT_EQ(db()->BeginEdit(L"create", createEdit), sc::SC_OK);

    sc::SCRecordPtr record;
    EXPECT_EQ(table->CreateRecord(record), sc::SC_OK);

    EXPECT_EQ(db()->Commit(createEdit.Get()), sc::SC_E_SCHEMA_VIOLATION);

    // 提交失败后需要显式回滚来清除未提交的记录
    EXPECT_EQ(db()->Rollback(createEdit.Get()), sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(table->EnumerateRecords(cursor), sc::SC_OK);
    sc::SCRecordPtr result;
    EXPECT_EQ(cursor->Next(result), sc::SC_OK);
    EXPECT_FALSE(static_cast<bool>(result));
}

// 补回自 M1Tests.cpp - CommitAllowsExplicitValuesForRequiredColumns
// 测试显式赋值后允许提交必填列
TEST_F(FileDbTest, CommitAllowsExplicitValuesForRequiredColumns)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db()->CreateTable(L"AllowReqTable", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef requiredCol;
    requiredCol.name = L"RequiredField";
    requiredCol.valueKind = sc::ValueKind::Int64;
    requiredCol.nullable = false;
    requiredCol.defaultValue = sc::SCValue::Null();
    EXPECT_EQ(schema->AddColumn(requiredCol), sc::SC_OK);

    sc::SCEditPtr createEdit;
    EXPECT_EQ(db()->BeginEdit(L"create", createEdit), sc::SC_OK);

    sc::SCRecordPtr record;
    EXPECT_EQ(table->CreateRecord(record), sc::SC_OK);
    EXPECT_EQ(record->SetInt64(L"RequiredField", 42), sc::SC_OK);

    EXPECT_EQ(db()->Commit(createEdit.Get()), sc::SC_OK);
}

// 补回自 M1Tests.cpp - AddColumnAllowsRequiredColumnOnEmptyTableWithoutDefault
// 测试空表上允许添加必填列且无默认值
TEST_F(FileDbTest, AddColumnAllowsRequiredColumnOnEmptyTableWithoutDefault)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db()->CreateTable(L"EmptyReqTable", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    // 空表上允许添加必填列且无默认值
    sc::SCColumnDef requiredCol;
    requiredCol.name = L"RequiredField";
    requiredCol.valueKind = sc::ValueKind::Int64;
    requiredCol.nullable = false;
    requiredCol.defaultValue = sc::SCValue::Null();
    EXPECT_EQ(schema->AddColumn(requiredCol), sc::SC_OK);

    // 验证列已添加
    sc::SCColumnDef found;
    EXPECT_EQ(schema->FindColumn(L"RequiredField", &found), sc::SC_OK);
    EXPECT_FALSE(found.nullable);
}

// 补回自 M1Tests.cpp - UpdateColumnAllowsRequiredColumnOnEmptyTableWithoutDefault
// 测试空表上允许更新列为必填且无默认值
TEST_F(FileDbTest, UpdateColumnAllowsRequiredColumnOnEmptyTableWithoutDefault)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db()->CreateTable(L"UpdateReqTable", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    // 先添加一个可选列
    sc::SCColumnDef optionalCol;
    optionalCol.name = L"OptionalField";
    optionalCol.valueKind = sc::ValueKind::Int64;
    optionalCol.nullable = true;
    optionalCol.defaultValue = sc::SCValue::Null();
    EXPECT_EQ(schema->AddColumn(optionalCol), sc::SC_OK);

    // 空表上允许将列更新为必填且无默认值
    sc::SCColumnDef requiredCol;
    requiredCol.name = L"OptionalField";
    requiredCol.valueKind = sc::ValueKind::Int64;
    requiredCol.nullable = false;
    requiredCol.defaultValue = sc::SCValue::Null();
    EXPECT_EQ(schema->UpdateColumn(requiredCol), sc::SC_OK);

    // 验证列已更新为必填
    sc::SCColumnDef found;
    EXPECT_EQ(schema->FindColumn(L"OptionalField", &found), sc::SC_OK);
    EXPECT_FALSE(found.nullable);
}
