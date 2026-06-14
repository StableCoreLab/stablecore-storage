#include <algorithm>
#include <filesystem>

#include <gtest/gtest.h>
#include <sqlite3.h>

#include "ISCQuery.h"
#include "SCStorage.h"

#include "Support/TestPaths.h"
#include "Support/TestSqliteHelpers.h"
#include "Support/TestSchemaBuilders.h"
#include "Support/TestQueryHelpers.h"

namespace sc = StableCore::Storage;
namespace fs = std::filesystem;

TEST(SchemaEdit, CreateTableFromSchemaBuildsTableAndColumns)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_CreateTableSuccess.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTableSchemaSnapshot schema;
    schema.table.name = L"Beam";
    schema.columns.push_back(MakeIntColumn(L"Width"));
    schema.columns.push_back(MakeStringColumn(L"Name", true));

    sc::SCSchemaEditResult result;
    EXPECT_EQ(sc::CreateTableFromSchema(db.Get(), schema, &result), sc::SC_OK);
    EXPECT_TRUE(result.applied);
    EXPECT_GT(result.committedVersion, 0u);
    EXPECT_EQ(result.tableName, L"Beam");
    ASSERT_EQ(result.addedColumns.size(), 2u);
    EXPECT_EQ(result.addedColumns[0], L"Width");
    EXPECT_EQ(result.addedColumns[1], L"Name");

    sc::SCTablePtr table;
    EXPECT_EQ(db->GetTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr loadedSchema;
    EXPECT_EQ(table->GetSchema(loadedSchema), sc::SC_OK);

    sc::SCColumnDef width;
    EXPECT_EQ(loadedSchema->FindColumn(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width.valueKind, sc::ValueKind::Int64);
    EXPECT_FALSE(width.nullable);

    sc::SCColumnDef name;
    EXPECT_EQ(loadedSchema->FindColumn(L"Name", &name), sc::SC_OK);
    EXPECT_EQ(name.valueKind, sc::ValueKind::String);
    EXPECT_TRUE(name.nullable);
}

TEST(SchemaEdit, CreateTableFromSchemaRollsBackWhenColumnAddFails)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_CreateTableRollback.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTableSchemaSnapshot schema;
    schema.table.name = L"Beam";
    schema.columns.push_back(MakeIntColumn(L"Width"));

    sc::SCColumnDef invalidRelation;
    invalidRelation.name = L"FloorRef";
    invalidRelation.displayName = L"FloorRef";
    invalidRelation.columnKind = sc::ColumnKind::Relation;
    invalidRelation.valueKind = sc::ValueKind::RecordId;
    invalidRelation.referenceTable = L"Floor";
    invalidRelation.defaultValue = sc::SCValue::FromString(L"bad");
    schema.columns.push_back(invalidRelation);

    sc::SCSchemaEditResult result;
    EXPECT_EQ(sc::CreateTableFromSchema(db.Get(), schema, &result), sc::SC_E_SCHEMA_VIOLATION);
    EXPECT_FALSE(result.applied);
    EXPECT_EQ(result.committedVersion, 0u);

    sc::SCTablePtr table;
    EXPECT_EQ(db->GetTable(L"Beam", table), sc::SC_E_TABLE_NOT_FOUND);
}

TEST(SchemaEdit, CreateTableFromSchemaBuildsConstraintsAndIndexes)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_CreateTableConstraints.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTableSchemaSnapshot schema;
    schema.table.name = L"Beam";
    schema.columns.push_back(MakeIntColumn(L"Width"));
    schema.columns.push_back(MakeStringColumn(L"Name", true));

    schema.constraints.push_back(MakeUniqueConstraint(L"uq_Beam_Width", L"Width"));
    schema.indexes.push_back(MakeIndex(L"idx_Beam_Name", L"Name"));

    sc::SCSchemaEditResult result;
    EXPECT_EQ(sc::CreateTableFromSchema(db.Get(), schema, &result), sc::SC_OK);
    EXPECT_TRUE(result.applied);

    sc::SCTablePtr table;
    EXPECT_EQ(db->GetTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr loadedSchema;
    EXPECT_EQ(table->GetSchema(loadedSchema), sc::SC_OK);

    sc::SCConstraintDef constraint;
    EXPECT_EQ(loadedSchema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_OK);
    EXPECT_EQ(constraint.kind, sc::SCConstraintKind::Unique);
    ASSERT_EQ(constraint.columns.size(), 1u);
    EXPECT_EQ(constraint.columns.front(), L"Width");

    sc::SCIndexDef index;
    EXPECT_EQ(loadedSchema->FindIndex(L"idx_Beam_Name", &index), sc::SC_OK);
    ASSERT_EQ(index.columns.size(), 1u);
    EXPECT_EQ(index.columns.front().columnName, L"Name");
}

TEST(SchemaEdit, BackendCreateTableFailureDoesNotLeaveVisibleTable)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_BackendCreateFailureCleanup.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    EXPECT_TRUE(ExecSqliteScript(dbPath, "DROP TABLE schema_tables;"));

    sc::SCTablePtr table;
    EXPECT_NE(db->CreateTable(L"Beam", table), sc::SC_OK);
    EXPECT_EQ(db->GetTable(L"Beam", table), sc::SC_E_TABLE_NOT_FOUND);
}

TEST(SchemaEdit, CreateTableFromSchemaSupportsSelfReferentialForeignKey)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_CreateTableSelfForeignKey.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTableSchemaSnapshot schema;
    schema.table.name = L"Node";
    schema.columns.push_back(MakeIntColumn(L"Id"));
    schema.columns.push_back(MakeIntColumn(L"ParentId", true));
    schema.constraints.push_back(MakeForeignKeyConstraint(L"fk_Node_Parent", L"ParentId", L"Node", L"Id"));

    sc::SCSchemaEditResult result;
    EXPECT_EQ(sc::CreateTableFromSchema(db.Get(), schema, &result), sc::SC_OK);
    EXPECT_TRUE(result.applied);

    sc::SCTablePtr table;
    EXPECT_EQ(db->GetTable(L"Node", table), sc::SC_OK);

    sc::SCSchemaPtr loadedSchema;
    EXPECT_EQ(table->GetSchema(loadedSchema), sc::SC_OK);

    sc::SCConstraintDef constraint;
    EXPECT_EQ(loadedSchema->FindConstraint(L"fk_Node_Parent", &constraint), sc::SC_OK);
    EXPECT_EQ(constraint.kind, sc::SCConstraintKind::ForeignKey);
    EXPECT_EQ(constraint.referencedTable, L"Node");
    ASSERT_EQ(constraint.columns.size(), 1u);
    EXPECT_EQ(constraint.columns.front(), L"ParentId");
    ASSERT_EQ(constraint.referencedColumns.size(), 1u);
    EXPECT_EQ(constraint.referencedColumns.front(), L"Id");
}

TEST(SchemaEdit, CreateTableFromSchemaRollsBackWhenForeignKeyIsInvalid)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_CreateTableForeignKeyRollback.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTableSchemaSnapshot schema;
    schema.table.name = L"Beam";
    schema.columns.push_back(MakeIntColumn(L"Width"));
    schema.columns.push_back(MakeIntColumn(L"FloorId"));
    schema.constraints.push_back(MakeForeignKeyConstraint(L"fk_Beam_Floor", L"FloorId", L"Floor", L"Id"));

    sc::SCSchemaEditResult result;
    EXPECT_EQ(sc::CreateTableFromSchema(db.Get(), schema, &result), sc::SC_E_TABLE_NOT_FOUND);
    EXPECT_FALSE(result.applied);

    sc::SCTablePtr table;
    EXPECT_EQ(db->GetTable(L"Beam", table), sc::SC_E_TABLE_NOT_FOUND);
}

TEST(SchemaEdit, CreateTableFromSchemaRollsBackWhenIndexColumnIsInvalid)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_CreateTableIndexRollback.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTableSchemaSnapshot schema;
    schema.table.name = L"Beam";
    schema.columns.push_back(MakeIntColumn(L"Width"));
    schema.indexes.push_back(MakeIndex(L"idx_Beam_Missing", L"Missing"));

    sc::SCSchemaEditResult result;
    EXPECT_EQ(sc::CreateTableFromSchema(db.Get(), schema, &result), sc::SC_E_COLUMN_NOT_FOUND);
    EXPECT_FALSE(result.applied);

    sc::SCTablePtr table;
    EXPECT_EQ(db->GetTable(L"Beam", table), sc::SC_E_TABLE_NOT_FOUND);
}