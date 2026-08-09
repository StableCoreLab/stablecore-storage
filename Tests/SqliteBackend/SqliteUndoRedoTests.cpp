#include <filesystem>
#include <sstream>
#include <vector>

#include <gtest/gtest.h>

#include "SCStorage.h"

#include "Support/TestPaths.h"
#include "Support/TestSqliteHelpers.h"

// Internal header for fault injection testing
#include "Sqlite/SqliteImplDatabase.h"

namespace sc = StableCore::Storage;
namespace fs = std::filesystem;

namespace
{
    struct RecordingObserver final : sc::ISCDatabaseObserver
    {
        void OnDatabaseChanged(const sc::SCChangeSet& changeSet) override
        {
            seen.push_back(changeSet);
        }

        std::vector<sc::SCChangeSet> seen;
    };

    struct ReentrantResetObserver final : sc::ISCDatabaseObserver
    {
        explicit ReentrantResetObserver(sc::SCDbPtr database) : db(database) {}

        void OnDatabaseChanged(const sc::SCChangeSet&) override
        {
            resetRc = db->ResetHistoryBaseline(nullptr);
        }

        sc::SCDbPtr db;
        sc::ErrorCode resetRc{sc::SC_OK};
    };

    sc::SCTablePtr CreateBeamTable(sc::SCDbPtr& db)
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

        return table;
    }
}

TEST(SqliteUndoRedo, UndoRedoSurvivesReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_UndoRedo_Reopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
        sc::SCTablePtr beamTable = CreateBeamTable(db);

        sc::SCEditPtr createEdit;
        EXPECT_EQ(db->BeginEdit(L"create", createEdit), sc::SC_OK);
        sc::SCRecordPtr beam;
        EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
        EXPECT_EQ(beam->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(db->Commit(createEdit.Get()), sc::SC_OK);

        sc::SCEditPtr modifyEdit;
        EXPECT_EQ(db->BeginEdit(L"modify", modifyEdit), sc::SC_OK);
        EXPECT_EQ(beam->SetInt64(L"Width", 450), sc::SC_OK);
        EXPECT_EQ(db->Commit(modifyEdit.Get()), sc::SC_OK);

        EXPECT_EQ(db->Undo(), sc::SC_OK);
    }

    {
        sc::SCDbPtr reopened;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);

        sc::SCTablePtr beamTable;
        EXPECT_EQ(reopened->GetTable(L"Beam", beamTable), sc::SC_OK);

        sc::SCRecordCursorPtr cursor;
        EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
        sc::SCRecordPtr beam;
        EXPECT_EQ(cursor->Next(beam), sc::SC_OK);
        EXPECT_TRUE(static_cast<bool>(beam));

        std::int64_t width = 0;
        EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
        EXPECT_EQ(width, 100);

        EXPECT_EQ(reopened->Redo(), sc::SC_OK);
        EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
        EXPECT_EQ(width, 450);
    }
}

TEST(SqliteUndoRedo, BeginEdit_RejectsNestedActiveEdit)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_BeginEdit_NestedRejected.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCEditPtr firstEdit;
    EXPECT_EQ(db->BeginEdit(L"first", firstEdit), sc::SC_OK);

    sc::SCEditPtr secondEdit;
    EXPECT_EQ(db->BeginEdit(L"second", secondEdit), sc::SC_E_WRITE_CONFLICT);
    EXPECT_FALSE(static_cast<bool>(secondEdit));

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(db->Commit(firstEdit.Get()), sc::SC_OK);
}

TEST(SqliteUndoRedo, MoveHistory_RejectsActiveEdit)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_MoveHistory_ActiveEditRejected.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"create", edit), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 100), sc::SC_OK);

    sc::SCHistoryMoveRequest request;
    request.direction = sc::SCHistoryMoveDirection::Undo;
    sc::SCHistoryMoveResult result;
    EXPECT_EQ(db->MoveHistory(request, &result), sc::SC_E_WRITE_CONFLICT);
    EXPECT_EQ(result.outcome, sc::SCHistoryMoveOutcome::NotMoved);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
}

TEST(SqliteUndoRedo, SchemaColumnJournalSurvivesReopenAndUndoRedo)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_UndoRedo_SchemaJournal.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr table = CreateBeamTable(db);
        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

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

        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"add height", edit), sc::SC_OK);
        EXPECT_EQ(schema->AddColumn(height), sc::SC_OK);
        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    }

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr table;
        EXPECT_EQ(db->GetTable(L"Beam", table), sc::SC_OK);
        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

        sc::SCColumnDef loaded;
        EXPECT_EQ(schema->FindColumn(L"Height", &loaded), sc::SC_OK);
        EXPECT_EQ(loaded.displayName, L"Height");

        EXPECT_EQ(db->Undo(), sc::SC_OK);
        EXPECT_EQ(schema->FindColumn(L"Height", &loaded), sc::SC_E_COLUMN_NOT_FOUND);

        EXPECT_EQ(db->Redo(), sc::SC_OK);
        EXPECT_EQ(schema->FindColumn(L"Height", &loaded), sc::SC_OK);
        EXPECT_EQ(loaded.displayName, L"Height");
    }
}

TEST(SqliteUndoRedo, SchemaColumnUndoRestoreKeepsOriginalColumnOrder)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_UndoRedo_SchemaOrder.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateBeamTable(db);
    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

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

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"add height", edit), sc::SC_OK);
    EXPECT_EQ(schema->AddColumn(height), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    std::int32_t columnCount = 0;
    sc::SCColumnDef column;
    EXPECT_EQ(schema->GetColumnCount(&columnCount), sc::SC_OK);
    EXPECT_EQ(columnCount, 3);
    EXPECT_EQ(schema->GetColumn(0, &column), sc::SC_OK);
    EXPECT_EQ(column.name, L"Width");
    EXPECT_EQ(schema->GetColumn(1, &column), sc::SC_OK);
    EXPECT_EQ(column.name, L"Name");
    EXPECT_EQ(schema->GetColumn(2, &column), sc::SC_OK);
    EXPECT_EQ(column.name, L"Height");

    EXPECT_EQ(db->BeginEdit(L"remove width", edit), sc::SC_OK);
    EXPECT_EQ(schema->RemoveColumn(L"Width"), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    EXPECT_EQ(schema->GetColumnCount(&columnCount), sc::SC_OK);
    EXPECT_EQ(columnCount, 2);
    EXPECT_EQ(schema->GetColumn(0, &column), sc::SC_OK);
    EXPECT_EQ(column.name, L"Name");
    EXPECT_EQ(schema->GetColumn(1, &column), sc::SC_OK);
    EXPECT_EQ(column.name, L"Height");

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_EQ(schema->GetColumnCount(&columnCount), sc::SC_OK);
    EXPECT_EQ(columnCount, 3);
    EXPECT_EQ(schema->GetColumn(0, &column), sc::SC_OK);
    EXPECT_EQ(column.name, L"Width");
    EXPECT_EQ(schema->GetColumn(1, &column), sc::SC_OK);
    EXPECT_EQ(column.name, L"Name");
    EXPECT_EQ(schema->GetColumn(2, &column), sc::SC_OK);
    EXPECT_EQ(column.name, L"Height");

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_EQ(schema->GetColumnCount(&columnCount), sc::SC_OK);
    EXPECT_EQ(columnCount, 2);
    EXPECT_EQ(schema->GetColumn(0, &column), sc::SC_OK);
    EXPECT_EQ(column.name, L"Name");
    EXPECT_EQ(schema->GetColumn(1, &column), sc::SC_OK);
    EXPECT_EQ(column.name, L"Height");
}

TEST(SqliteUndoRedo, PersistedRedoRejectsParentUpdateThatViolatesReopenedForeignKey)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_UndoRedo_PersistedRedoForeignKey.sqlite");
    sc::RecordId floorId = 0;
    sc::RecordId beamId = 0;

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr floorTable;
        EXPECT_EQ(db->CreateTable(L"Floor", floorTable), sc::SC_OK);
        sc::SCSchemaPtr floorSchema;
        EXPECT_EQ(floorTable->GetSchema(floorSchema), sc::SC_OK);

        sc::SCColumnDef floorCode;
        floorCode.name = L"Code";
        floorCode.displayName = L"Code";
        floorCode.valueKind = sc::ValueKind::String;
        floorCode.nullable = false;
        floorCode.defaultValue = sc::SCValue::FromString(L"");
        EXPECT_EQ(floorSchema->AddColumn(floorCode), sc::SC_OK);

        sc::SCConstraintDef floorCodeUnique;
        floorCodeUnique.kind = sc::SCConstraintKind::Unique;
        floorCodeUnique.name = L"uq_Floor_Code";
        floorCodeUnique.columns.push_back(L"Code");
        EXPECT_EQ(floorSchema->AddConstraint(floorCodeUnique), sc::SC_OK);

        sc::SCTablePtr beamTable;
        EXPECT_EQ(db->CreateTable(L"Beam", beamTable), sc::SC_OK);
        sc::SCSchemaPtr beamSchema;
        EXPECT_EQ(beamTable->GetSchema(beamSchema), sc::SC_OK);

        sc::SCColumnDef beamFloorCode;
        beamFloorCode.name = L"FloorCode";
        beamFloorCode.displayName = L"FloorCode";
        beamFloorCode.valueKind = sc::ValueKind::String;
        beamFloorCode.nullable = true;
        beamFloorCode.defaultValue = sc::SCValue::Null();
        EXPECT_EQ(beamSchema->AddColumn(beamFloorCode), sc::SC_OK);

        sc::SCEditPtr seedEdit;
        EXPECT_EQ(db->BeginEdit(L"seed rows", seedEdit), sc::SC_OK);
        sc::SCRecordPtr floor;
        EXPECT_EQ(floorTable->CreateRecord(floor), sc::SC_OK);
        EXPECT_EQ(floor->SetString(L"Code", L"F-001"), sc::SC_OK);
        sc::SCRecordPtr beam;
        EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
        EXPECT_EQ(beam->SetString(L"FloorCode", L"F-001"), sc::SC_OK);
        EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);
        floorId = floor->GetId();
        beamId = beam->GetId();

        sc::SCEditPtr parentUpdateEdit;
        EXPECT_EQ(db->BeginEdit(L"update parent code", parentUpdateEdit), sc::SC_OK);
        EXPECT_EQ(floor->SetString(L"Code", L"F-002"), sc::SC_OK);
        EXPECT_EQ(db->Commit(parentUpdateEdit.Get()), sc::SC_OK);

        sc::SCEditPtr childUpdateEdit;
        EXPECT_EQ(db->BeginEdit(L"update child code", childUpdateEdit), sc::SC_OK);
        EXPECT_EQ(beam->SetString(L"FloorCode", L"F-002"), sc::SC_OK);
        EXPECT_EQ(db->Commit(childUpdateEdit.Get()), sc::SC_OK);

        EXPECT_EQ(db->Undo(), sc::SC_OK);
        EXPECT_EQ(db->Undo(), sc::SC_OK);

        std::int64_t beamTableId = 0;
        ASSERT_TRUE(QuerySqliteInt64(dbPath, "SELECT table_id FROM tables WHERE name = 'Beam';", &beamTableId));

        std::ostringstream sql;
        sql << "BEGIN;"
            << "INSERT INTO schema_constraints(table_id, kind, name, source_kind, referenced_table, "
               "on_delete_action, on_update_action, check_expression) VALUES("
            << beamTableId << ", "
            << static_cast<int>(sc::SCConstraintKind::ForeignKey) << ", 'fk_Beam_FloorCode', "
            << static_cast<int>(sc::SCSchemaSourceKind::Explicit) << ", 'Floor', "
            << static_cast<int>(sc::SCForeignKeyAction::Restrict) << ", "
            << static_cast<int>(sc::SCForeignKeyAction::Restrict) << ", '');"
            << "INSERT INTO schema_constraint_columns(constraint_id, column_ordinal, column_name, "
               "referenced_column_name) VALUES(last_insert_rowid(), 0, 'FloorCode', 'Code');"
            << "COMMIT;";
        ASSERT_TRUE(ExecSqliteScript(dbPath, sql.str().c_str()));
    }

    sc::SCDbPtr reopened;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);

    sc::SCTablePtr floorTable;
    EXPECT_EQ(reopened->GetTable(L"Floor", floorTable), sc::SC_OK);
    sc::SCTablePtr beamTable;
    EXPECT_EQ(reopened->GetTable(L"Beam", beamTable), sc::SC_OK);

    EXPECT_EQ(reopened->Redo(), sc::SC_E_CONSTRAINT_VIOLATION);

    sc::SCRecordPtr floor;
    EXPECT_EQ(floorTable->GetRecord(floorId, floor), sc::SC_OK);
    const wchar_t* floorCode = nullptr;
    EXPECT_EQ(floor->GetString(L"Code", &floorCode), sc::SC_OK);
    EXPECT_STREQ(floorCode, L"F-001");

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->GetRecord(beamId, beam), sc::SC_OK);
    const wchar_t* beamFloorCodeText = nullptr;
    EXPECT_EQ(beam->GetString(L"FloorCode", &beamFloorCodeText), sc::SC_OK);
    EXPECT_STREQ(beamFloorCodeText, L"F-001");
}

// ==== MoveHistory tests ====

TEST(SqliteUndoRedo, MoveHistory_SingleUndo)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_MoveHistory_Undo.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    // Commit one record
    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"create", edit), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    // Verify data present
    std::int64_t width = 0;
    EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 100);

    // MoveHistory Undo
    sc::SCHistoryMoveRequest request;
    request.direction = sc::SCHistoryMoveDirection::Undo;
    sc::SCHistoryMoveResult result;
    EXPECT_EQ(db->MoveHistory(request, &result), sc::SC_OK);
    EXPECT_EQ(result.outcome, sc::SCHistoryMoveOutcome::Moved);
    EXPECT_TRUE(result.stateChanged());
    EXPECT_EQ(result.undoCount, 0);
    EXPECT_EQ(result.redoCount, 1);

    // Verify the created record was removed
    EXPECT_TRUE(beam->IsDeleted());
    EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_E_RECORD_DELETED);
}

TEST(SqliteUndoRedo, MoveHistory_SingleRedo)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_MoveHistory_Redo.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    // Commit one record
    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"create", edit), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    // Undo
    EXPECT_EQ(db->Undo(), sc::SC_OK);

    // MoveHistory Redo
    sc::SCHistoryMoveRequest request;
    request.direction = sc::SCHistoryMoveDirection::Redo;
    sc::SCHistoryMoveResult result;
    EXPECT_EQ(db->MoveHistory(request, &result), sc::SC_OK);
    EXPECT_EQ(result.outcome, sc::SCHistoryMoveOutcome::Moved);
    EXPECT_EQ(result.undoCount, 1);
    EXPECT_EQ(result.redoCount, 0);

    // Verify data restored
    std::int64_t width = 0;
    EXPECT_FALSE(beam->IsDeleted());
    EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 100);
}

TEST(SqliteUndoRedo, MoveHistory_UndoToBaseline)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_MoveHistory_Baseline.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    // Commit one record
    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"create", edit), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    // MoveHistory Undo
    sc::SCHistoryMoveRequest request;
    request.direction = sc::SCHistoryMoveDirection::Undo;
    sc::SCHistoryMoveResult result;
    EXPECT_EQ(db->MoveHistory(request, &result), sc::SC_OK);
    EXPECT_EQ(result.outcome, sc::SCHistoryMoveOutcome::Moved);

    // Current position should be at baseline: commitId=0
    EXPECT_EQ(result.currentPosition.commitId, 0u);
    EXPECT_EQ(result.undoCount, 0);
    EXPECT_TRUE(beam->IsDeleted());
}

TEST(SqliteUndoRedo, MoveHistory_PositionMismatch_Current)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_MoveHistory_MismatchCurrent.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    // Commit one record
    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"create", edit), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    // Get the current state
    sc::SCEditingDatabaseState state;
    EXPECT_EQ(db->GetEditingState(&state), sc::SC_OK);
    EXPECT_EQ(state.undoCount, 1u);

    // MoveHistory Undo with wrong expectedCurrent commitId
    sc::SCHistoryMoveRequest request;
    request.direction = sc::SCHistoryMoveDirection::Undo;
    request.expectedCurrent.commitIdEnabled = true;
    request.expectedCurrent.position.commitId = 999; // wrong
    sc::SCHistoryMoveResult result;
    EXPECT_EQ(db->MoveHistory(request, &result), sc::SC_E_HISTORY_POSITION_MISMATCH);
    EXPECT_EQ(result.outcome, sc::SCHistoryMoveOutcome::NotMoved);
    EXPECT_FALSE(result.stateChanged());

    // Verify undoCount unchanged (state not changed)
    EXPECT_EQ(db->GetEditingState(&state), sc::SC_OK);
    EXPECT_EQ(state.undoCount, 1u);
}

TEST(SqliteUndoRedo, MoveHistory_PositionMismatch_Target)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_MoveHistory_MismatchTarget.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    // Commit two records
    sc::SCEditPtr edit1;
    EXPECT_EQ(db->BeginEdit(L"create1", edit1), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit1.Get()), sc::SC_OK);

    sc::SCEditPtr edit2;
    EXPECT_EQ(db->BeginEdit(L"create2", edit2), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit2.Get()), sc::SC_OK);

    // MoveHistory Undo with wrong expectedTarget commitId
    sc::SCHistoryMoveRequest request;
    request.direction = sc::SCHistoryMoveDirection::Undo;
    request.expectedTarget.commitIdEnabled = true;
    request.expectedTarget.position.commitId = 999; // wrong
    sc::SCHistoryMoveResult result;
    EXPECT_EQ(db->MoveHistory(request, &result), sc::SC_E_HISTORY_POSITION_MISMATCH);
    EXPECT_EQ(result.outcome, sc::SCHistoryMoveOutcome::NotMoved);

    // Verify state unchanged
    sc::SCEditingDatabaseState state;
    EXPECT_EQ(db->GetEditingState(&state), sc::SC_OK);
    EXPECT_EQ(state.undoCount, 2u);
}

TEST(SqliteUndoRedo, MoveHistory_PositionMismatch_CommittedVersion)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_MoveHistory_MismatchVersion.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"create", edit), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    // MoveHistory Undo with wrong expectedCurrent committedVersion
    sc::SCHistoryMoveRequest request;
    request.direction = sc::SCHistoryMoveDirection::Undo;
    request.expectedCurrent.committedVersionEnabled = true;
    request.expectedCurrent.position.committedVersion = 999; // wrong
    sc::SCHistoryMoveResult result;
    EXPECT_EQ(db->MoveHistory(request, &result), sc::SC_E_HISTORY_POSITION_MISMATCH);
    EXPECT_EQ(result.outcome, sc::SCHistoryMoveOutcome::NotMoved);
}

TEST(SqliteUndoRedo, MoveHistory_EmptyUndoStack)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_MoveHistory_EmptyUndo.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    // No commits yet, undo stack is empty
    sc::SCHistoryMoveRequest request;
    request.direction = sc::SCHistoryMoveDirection::Undo;
    sc::SCHistoryMoveResult result;
    EXPECT_EQ(db->MoveHistory(request, &result), sc::SC_E_UNDO_STACK_EMPTY);
    EXPECT_EQ(result.outcome, sc::SCHistoryMoveOutcome::NotMoved);
    EXPECT_FALSE(result.stateChanged());
}

TEST(SqliteUndoRedo, MoveHistory_EmptyRedoStack)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_MoveHistory_EmptyRedo.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    // No redo items
    sc::SCHistoryMoveRequest request;
    request.direction = sc::SCHistoryMoveDirection::Redo;
    sc::SCHistoryMoveResult result;
    EXPECT_EQ(db->MoveHistory(request, &result), sc::SC_E_REDO_STACK_EMPTY);
    EXPECT_EQ(result.outcome, sc::SCHistoryMoveOutcome::NotMoved);
}

TEST(SqliteUndoRedo, MoveHistory_InvalidDirection)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_MoveHistory_InvalidDirection.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    // Use an invalid direction value
    sc::SCHistoryMoveRequest request;
    request.direction = static_cast<sc::SCHistoryMoveDirection>(999);
    sc::SCHistoryMoveResult result;
    EXPECT_EQ(db->MoveHistory(request, &result), sc::SC_E_INVALIDARG);
    EXPECT_FALSE(result.stateChanged());
}

TEST(SqliteUndoRedo, LegacyUndo_DelegatesToMoveHistory)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_LegacyUndoDelegate.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    // Commit one record
    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"create", edit), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    // Use legacy Undo
    EXPECT_EQ(db->Undo(), sc::SC_OK);

    // The record should now be deleted
    std::int64_t width = 0;
    EXPECT_TRUE(beam->IsDeleted());
    EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_E_RECORD_DELETED);

    // Use legacy Redo
    EXPECT_EQ(db->Redo(), sc::SC_OK);

    // Verify data restored
    EXPECT_FALSE(beam->IsDeleted());
    EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 100);
}

TEST(SqliteUndoRedo, MoveHistory_SurvivesReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_MoveHistory_Reopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
        sc::SCTablePtr beamTable = CreateBeamTable(db);

        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"create", edit), sc::SC_OK);
        sc::SCRecordPtr beam;
        EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
        EXPECT_EQ(beam->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

        // Undo via MoveHistory
        sc::SCHistoryMoveRequest request;
        request.direction = sc::SCHistoryMoveDirection::Undo;
        sc::SCHistoryMoveResult result;
        EXPECT_EQ(db->MoveHistory(request, &result), sc::SC_OK);
        EXPECT_EQ(result.outcome, sc::SCHistoryMoveOutcome::Moved);
    }

    {
        sc::SCDbPtr reopened;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);

        sc::SCTablePtr beamTable;
        EXPECT_EQ(reopened->GetTable(L"Beam", beamTable), sc::SC_OK);

        sc::SCRecordCursorPtr cursor;
        EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
        sc::SCRecordPtr beam;
        EXPECT_EQ(cursor->Next(beam), sc::SC_OK);
        EXPECT_FALSE(static_cast<bool>(beam));

        // Redo via MoveHistory
        sc::SCHistoryMoveRequest request;
        request.direction = sc::SCHistoryMoveDirection::Redo;
        sc::SCHistoryMoveResult result;
        EXPECT_EQ(reopened->MoveHistory(request, &result), sc::SC_OK);
        EXPECT_EQ(result.outcome, sc::SCHistoryMoveOutcome::Moved);

        sc::SCRecordCursorPtr postRedoCursor;
        EXPECT_EQ(beamTable->EnumerateRecords(postRedoCursor), sc::SC_OK);
        EXPECT_EQ(postRedoCursor->Next(beam), sc::SC_OK);
        EXPECT_TRUE(static_cast<bool>(beam));

        std::int64_t width = 0;
        EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
        EXPECT_EQ(width, 100);
    }
}

TEST(SqliteUndoRedo, MoveHistory_ResultFields)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_MoveHistory_ResultFields.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    // Commit one record
    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"create", edit), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    sc::SCEditingDatabaseState stateBefore;
    EXPECT_EQ(db->GetEditingState(&stateBefore), sc::SC_OK);

    sc::SCHistoryMoveRequest request;
    request.direction = sc::SCHistoryMoveDirection::Undo;
    sc::SCHistoryMoveResult result;
    EXPECT_EQ(db->MoveHistory(request, &result), sc::SC_OK);
    EXPECT_EQ(result.outcome, sc::SCHistoryMoveOutcome::Moved);

    // Check result fields
    EXPECT_GT(result.previousPosition.commitId, 0u);
    EXPECT_EQ(result.currentPosition.commitId, 0u); // back to baseline
    EXPECT_GT(result.databaseVersion, 0u);
    EXPECT_EQ(result.undoCount, 0u);
    EXPECT_EQ(result.redoCount, 1u);
}

TEST(SqliteUndoRedo, MoveHistory_ObserverNotifications)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_MoveHistory_Observer.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    RecordingObserver observer;
    EXPECT_EQ(db->AddObserver(&observer), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"create", edit), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    observer.seen.clear();

    sc::SCHistoryMoveRequest request;
    request.direction = sc::SCHistoryMoveDirection::Undo;
    sc::SCHistoryMoveResult result;
    EXPECT_EQ(db->MoveHistory(request, &result), sc::SC_OK);
    EXPECT_EQ(result.outcome, sc::SCHistoryMoveOutcome::Moved);
    EXPECT_EQ(observer.seen.size(), 1u);

    request.direction = sc::SCHistoryMoveDirection::Redo;
    EXPECT_EQ(db->MoveHistory(request, &result), sc::SC_OK);
    EXPECT_EQ(result.outcome, sc::SCHistoryMoveOutcome::Moved);
    EXPECT_EQ(observer.seen.size(), 2u);

    request.direction = sc::SCHistoryMoveDirection::Undo;
    request.expectedCurrent.commitIdEnabled = true;
    request.expectedCurrent.position.commitId = 999;
    EXPECT_EQ(db->MoveHistory(request, &result), sc::SC_E_HISTORY_POSITION_MISMATCH);
    EXPECT_EQ(observer.seen.size(), 2u);

    EXPECT_EQ(db->RemoveObserver(&observer), sc::SC_OK);
}

TEST(SqliteUndoRedo, MoveHistory_ObserverReentryBlocked)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_MoveHistory_ReentryBlocked.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    ReentrantResetObserver observer(db);
    EXPECT_EQ(db->AddObserver(&observer), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"create", edit), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    sc::SCHistoryMoveRequest request;
    request.direction = sc::SCHistoryMoveDirection::Undo;
    sc::SCHistoryMoveResult result;
    EXPECT_EQ(db->MoveHistory(request, &result), sc::SC_OK);
    EXPECT_EQ(result.outcome, sc::SCHistoryMoveOutcome::Moved);
    EXPECT_EQ(observer.resetRc, sc::SC_E_WRITE_CONFLICT);

    EXPECT_EQ(db->RemoveObserver(&observer), sc::SC_OK);
}

TEST(SqliteUndoRedo, MoveHistory_ConsistencyLost_OnCompensationFailure)
{
    const fs::path dbPath = MakeTempDbPath(
        L"StableCoreStorage_Sqlite_MoveHistory_ConsistencyLost.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    RecordingObserver observer;
    EXPECT_EQ(db->AddObserver(&observer), sc::SC_OK);

    // Commit one record
    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"create", edit), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    observer.seen.clear();

    // Set up fault injection: primary fails after 0 entries, compensation also fails
    auto* sqliteDb = dynamic_cast<sc::SqliteDatabase*>(db.Get());
    ASSERT_NE(sqliteDb, nullptr);

    sc::SqliteDatabase::JournalFailureInjection injection;
    injection.operation = sc::SqliteDatabase::JournalFailureOperation::Undo;
    injection.failPrimaryAfterAppliedEntries = 0;
    injection.failCompensationAfterAppliedEntries = 0;
    sqliteDb->SetJournalFailureInjectionForTesting(injection);

    // Attempt Undo — should fail with consistency lost
    sc::SCHistoryMoveRequest request;
    request.direction = sc::SCHistoryMoveDirection::Undo;
    sc::SCHistoryMoveResult result;
    const sc::ErrorCode rc = db->MoveHistory(request, &result);
    EXPECT_TRUE(sc::Failed(rc));
    EXPECT_EQ(result.outcome, sc::SCHistoryMoveOutcome::ConsistencyLost);
    EXPECT_TRUE(result.stateChanged());

    // GetEditingState should report consistencyLost
    sc::SCEditingDatabaseState state;
    EXPECT_EQ(db->GetEditingState(&state), sc::SC_OK);
    EXPECT_TRUE(state.consistencyLost);
    EXPECT_TRUE(state.dirty);
    EXPECT_TRUE(observer.seen.empty());

    // Further writes must be blocked until reopen
    sc::SCEditPtr blockedEdit;
    EXPECT_EQ(db->BeginEdit(L"blocked", blockedEdit), sc::SC_E_WRITE_CONFLICT);

    // Clear injection
    sqliteDb->ClearJournalFailureInjectionForTesting();
    EXPECT_EQ(db->RemoveObserver(&observer), sc::SC_OK);
}
