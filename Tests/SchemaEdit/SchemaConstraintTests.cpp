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

TEST(SchemaEdit, SchemaConstraintAndIndexPrimitivesSupportUndoAndRedo)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_ConstraintIndexUndoRedo.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"add constraint/index", edit), sc::SC_OK);
    EXPECT_EQ(schema->AddConstraint(MakeUniqueConstraint(L"uq_Beam_Width", L"Width")), sc::SC_OK);
    EXPECT_EQ(schema->AddIndex(MakeIndex(L"idx_Beam_Name", L"Name")), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    std::int32_t constraintCount = 0;
    EXPECT_EQ(schema->GetConstraintCount(&constraintCount), sc::SC_OK);
    EXPECT_EQ(constraintCount, 1);

    std::int32_t indexCount = 0;
    EXPECT_EQ(schema->GetIndexCount(&indexCount), sc::SC_OK);
    EXPECT_EQ(indexCount, 1);

    sc::SCConstraintDef constraint;
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_OK);
    EXPECT_EQ(constraint.kind, sc::SCConstraintKind::Unique);
    ASSERT_EQ(constraint.columns.size(), 1u);
    EXPECT_EQ(constraint.columns.front(), L"Width");

    sc::SCIndexDef index;
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_OK);
    ASSERT_EQ(index.columns.size(), 1u);
    EXPECT_EQ(index.columns.front().columnName, L"Name");

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_E_CONSTRAINT_NOT_FOUND);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_E_INDEX_NOT_FOUND);

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_OK);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_OK);
}

TEST(SchemaEdit, SchemaConstraintAndIndexPrimitivesPersistAcrossReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_ConstraintIndexReopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

        sc::SCTablePtr table = CreateBeamTable(db);
        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"add metadata", edit), sc::SC_OK);
        EXPECT_EQ(schema->AddConstraint(MakeUniqueConstraint(L"uq_Beam_Width", L"Width")), sc::SC_OK);
        EXPECT_EQ(schema->AddIndex(MakeIndex(L"idx_Beam_Name", L"Name")), sc::SC_OK);
        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    }

    sc::SCDbPtr reopened;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, reopened), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(reopened->GetTable(L"Beam", table), sc::SC_OK);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCConstraintDef constraint;
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_OK);
    sc::SCIndexDef index;
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_OK);

    EXPECT_EQ(reopened->Undo(), sc::SC_OK);
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_E_CONSTRAINT_NOT_FOUND);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_E_INDEX_NOT_FOUND);

    EXPECT_EQ(reopened->Redo(), sc::SC_OK);
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_OK);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_OK);
}

TEST(SchemaEdit, SchemaConstraintAndIndexPrimitivesSupportRemovalUndoAndRedo)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEdit_ConstraintIndexRemove.sqlite");
    sc::SCDbPtr db;
    EXPECT_EQ(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    EXPECT_EQ(schema->AddConstraint(MakeUniqueConstraint(L"uq_Beam_Width", L"Width")), sc::SC_OK);
    EXPECT_EQ(schema->AddIndex(MakeIndex(L"idx_Beam_Name", L"Name")), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"remove constraint/index", edit), sc::SC_OK);
    EXPECT_EQ(schema->RemoveConstraint(L"uq_Beam_Width"), sc::SC_OK);
    EXPECT_EQ(schema->RemoveIndex(L"idx_Beam_Name"), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    sc::SCConstraintDef constraint;
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_E_CONSTRAINT_NOT_FOUND);
    sc::SCIndexDef index;
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_E_INDEX_NOT_FOUND);

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_OK);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_OK);

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_EQ(schema->FindConstraint(L"uq_Beam_Width", &constraint), sc::SC_E_CONSTRAINT_NOT_FOUND);
    EXPECT_EQ(schema->FindIndex(L"idx_Beam_Name", &index), sc::SC_E_INDEX_NOT_FOUND);
}