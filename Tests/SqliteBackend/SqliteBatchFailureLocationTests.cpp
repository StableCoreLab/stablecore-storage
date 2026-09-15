#include <filesystem>

#include <gtest/gtest.h>

#include "SCBatch.h"
#include "SCStorage.h"

#include "Support/TestPaths.h"

namespace sc = StableCore::Storage;
namespace fs = std::filesystem;

namespace
{
    sc::ErrorCode CreateFileDb(const wchar_t* path, sc::SCDbPtr& db)
    {
        return sc::CreateFileDatabase(path, sc::SCOpenDatabaseOptions{}, db);
    }

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
        width.nullable = false;
        width.defaultValue = sc::SCValue::FromInt64(0);
        EXPECT_EQ(schema->AddColumn(width), sc::SC_OK);

        return table;
    }

    void AddUniqueWidthConstraint(sc::SCTablePtr& table)
    {
        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

        sc::SCConstraintDef constraint;
        constraint.kind = sc::SCConstraintKind::Unique;
        constraint.name = L"uq_Beam_Width";
        constraint.columns.push_back(L"Width");
        EXPECT_EQ(schema->AddConstraint(constraint), sc::SC_OK);
    }

    std::size_t CountAliveRecords(sc::SCTablePtr& table)
    {
        sc::SCRecordCursorPtr cursor;
        if (sc::Failed(table->EnumerateRecords(cursor)) || !cursor)
        {
            return 0;
        }

        std::size_t count = 0;
        sc::SCRecordPtr record;
        while (sc::Succeeded(cursor->Next(record)) && record)
        {
            ++count;
        }
        return count;
    }

    void ExpectNoActiveEditLeft(sc::SCDbPtr& db)
    {
        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"after failed batch", edit), sc::SC_OK);
        if (edit)
        {
            EXPECT_EQ(db->Rollback(edit.Get()), sc::SC_OK);
        }
    }
}  // namespace

// A constraint conflict that only surfaces inside Commit() is mapped back onto
// the originating table request / operation / RecordId.
TEST(SqliteBatchFailureLocation, CommitUniqueConflictIsMappedBackToCreateOperation)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Batch_CommitFailureLocation.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);
    AddUniqueWidthConstraint(beamTable);

    sc::SCBatchExecutionOptions options;
    options.editName = L"duplicate widths";

    std::vector<sc::SCBatchTableRequest> requests;
    sc::SCBatchTableRequest request;
    request.tableName = L"Beam";
    request.creates.push_back(sc::SCBatchCreateRecordRequest{{{L"Width", sc::SCValue::FromInt64(100)}}});
    request.creates.push_back(sc::SCBatchCreateRecordRequest{{{L"Width", sc::SCValue::FromInt64(100)}}});
    requests.push_back(request);

    sc::SCBatchExecutionResult result;
    EXPECT_EQ(sc::ExecuteBatchEdit(db.Get(), requests, options, &result), sc::SC_E_CONSTRAINT_VIOLATION);

    const sc::SCBatchFailureLocation& location = result.failureLocation;
    EXPECT_TRUE(location.valid);
    EXPECT_TRUE(location.hasOperation);
    EXPECT_EQ(location.operationKind, sc::SCBatchOperationKind::Create);
    EXPECT_EQ(location.tableRequestIndex, 0u);
    // The reported record id is the one the storage layer blamed, never a
    // default constructed placeholder.
    EXPECT_EQ(location.recordId, location.constraintViolation.recordId);
    EXPECT_NE(location.recordId, 0u);

    ASSERT_TRUE(location.hasConstraintViolation);
    EXPECT_EQ(location.constraintViolation.kind, sc::SCConstraintKind::Unique);
    EXPECT_EQ(location.constraintViolation.constraintName, std::wstring(L"uq_Beam_Width"));
    EXPECT_EQ(location.constraintViolation.source, sc::SCConstraintViolationSource::SchemaConstraint);
    ASSERT_EQ(location.constraintViolation.columns.size(), 1u);
    EXPECT_EQ(location.constraintViolation.columns.front(), std::wstring(L"Width"));

    // A commit time failure must not fake a single field position.
    EXPECT_TRUE(location.fieldName.empty());
    EXPECT_EQ(location.assignmentIndex, 0u);

    // Atomic rollback: nothing persisted and no active edit left behind.
    EXPECT_EQ(CountAliveRecords(beamTable), 0u);
    ExpectNoActiveEditLeft(db);
}

// An immediate Update failure reports the requested RecordId and keeps
// hasConstraintViolation false.
TEST(SqliteBatchFailureLocation, ImmediateUpdateFailureKeepsRequestedRecordIdWithoutConstraintDetails)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Batch_UpdateFailureLocation.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCBatchExecutionOptions options;
    options.editName = L"missing update target";

    std::vector<sc::SCBatchTableRequest> requests;
    sc::SCBatchTableRequest request;
    request.tableName = L"Beam";
    request.updates.push_back(
        sc::SCBatchUpdateRecordRequest{4242, {{L"Width", sc::SCValue::FromInt64(123)}}});
    requests.push_back(request);

    sc::SCBatchExecutionResult result;
    EXPECT_EQ(sc::ExecuteBatchEdit(db.Get(), requests, options, &result), sc::SC_E_RECORD_NOT_FOUND);

    const sc::SCBatchFailureLocation& location = result.failureLocation;
    EXPECT_TRUE(location.valid);
    EXPECT_TRUE(location.hasOperation);
    EXPECT_EQ(location.operationKind, sc::SCBatchOperationKind::Update);
    EXPECT_EQ(location.operationIndex, 0u);
    EXPECT_EQ(location.recordId, 4242u);
    // No storage layer constraint detail exists for a missing record.
    EXPECT_FALSE(location.hasConstraintViolation);
    EXPECT_TRUE(location.fieldName.empty());

    ExpectNoActiveEditLeft(db);
}

// A field assignment failure reports the assignment index and field name.
TEST(SqliteBatchFailureLocation, ImmediateFieldAssignmentFailureReportsFieldPosition)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Batch_FieldFailureLocation.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCBatchExecutionOptions options;
    options.editName = L"type mismatch";

    std::vector<sc::SCBatchTableRequest> requests;
    sc::SCBatchTableRequest request;
    request.tableName = L"Beam";
    request.creates.push_back(sc::SCBatchCreateRecordRequest{{
        {L"Width", sc::SCValue::FromInt64(100)},
        {L"Width", sc::SCValue::FromString(L"not a number")},
    }});
    requests.push_back(request);

    sc::SCBatchExecutionResult result;
    EXPECT_EQ(sc::ExecuteBatchEdit(db.Get(), requests, options, &result), sc::SC_E_TYPE_MISMATCH);

    const sc::SCBatchFailureLocation& location = result.failureLocation;
    EXPECT_TRUE(location.valid);
    EXPECT_TRUE(location.hasOperation);
    EXPECT_EQ(location.operationKind, sc::SCBatchOperationKind::Create);
    EXPECT_EQ(location.operationIndex, 0u);
    // CreateRecord() succeeded, so the allocated RecordId is reported even
    // though the batch is rolled back.
    EXPECT_NE(location.recordId, 0u);
    EXPECT_EQ(location.assignmentIndex, 1u);
    EXPECT_EQ(location.fieldName, std::wstring(L"Width"));
    EXPECT_FALSE(location.hasConstraintViolation);

    EXPECT_EQ(CountAliveRecords(beamTable), 0u);
    ExpectNoActiveEditLeft(db);
}

// A table request level failure is attributable without faking an operation.
TEST(SqliteBatchFailureLocation, TableRequestFailureHasNoOperation)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Batch_RequestFailureLocation.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCBatchExecutionOptions options;
    options.editName = L"empty table name";

    std::vector<sc::SCBatchTableRequest> requests;
    sc::SCBatchTableRequest request;
    request.tableName.clear();
    request.creates.push_back(sc::SCBatchCreateRecordRequest{{{L"Width", sc::SCValue::FromInt64(100)}}});
    requests.push_back(request);

    sc::SCBatchExecutionResult result;
    EXPECT_EQ(sc::ExecuteBatchEdit(db.Get(), requests, options, &result), sc::SC_E_INVALIDARG);

    const sc::SCBatchFailureLocation& location = result.failureLocation;
    EXPECT_TRUE(location.valid);
    EXPECT_FALSE(location.hasOperation);
    EXPECT_EQ(location.tableRequestIndex, 0u);
    EXPECT_EQ(location.recordId, 0u);
    EXPECT_TRUE(location.fieldName.empty());
    EXPECT_FALSE(location.hasConstraintViolation);

    ExpectNoActiveEditLeft(db);
}

// FinalizeImportSession() is the transaction owner of the import flow: the
// commit failure is reported with the originating chunk.
TEST(SqliteBatchFailureLocation, ImportCommitFailureIsMappedBackToChunk)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Batch_ImportFailureLocation.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);
    AddUniqueWidthConstraint(beamTable);

    sc::SCImportExecutionOptions options;
    options.editName = L"import duplicate widths";
    options.chunkSize = 1;

    std::vector<sc::SCBatchTableRequest> requests;
    sc::SCBatchTableRequest first;
    first.tableName = L"Beam";
    first.creates.push_back(sc::SCBatchCreateRecordRequest{{{L"Width", sc::SCValue::FromInt64(100)}}});
    requests.push_back(first);

    sc::SCBatchTableRequest second;
    second.tableName = L"Beam";
    second.creates.push_back(sc::SCBatchCreateRecordRequest{{{L"Width", sc::SCValue::FromInt64(100)}}});
    requests.push_back(second);

    sc::SCBatchExecutionResult result;
    EXPECT_EQ(sc::ExecuteImport(db.Get(), requests, options, &result), sc::SC_E_CONSTRAINT_VIOLATION);

    const sc::SCBatchFailureLocation& location = result.failureLocation;
    EXPECT_TRUE(location.valid);
    EXPECT_TRUE(location.hasOperation);
    EXPECT_NE(location.chunkId, 0u);
    EXPECT_EQ(location.recordId, location.constraintViolation.recordId);
    ASSERT_TRUE(location.hasConstraintViolation);
    EXPECT_EQ(location.constraintViolation.kind, sc::SCConstraintKind::Unique);

    EXPECT_EQ(CountAliveRecords(beamTable), 0u);
    ExpectNoActiveEditLeft(db);
}

// A successful batch is one transaction: it enters undo / redo as a whole.
TEST(SqliteBatchFailureLocation, SuccessfulBatchEntersUndoRedoAsOneTransaction)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Batch_UndoRedoWhole.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCBatchExecutionOptions options;
    options.editName = L"two records";

    std::vector<sc::SCBatchTableRequest> requests;
    sc::SCBatchTableRequest request;
    request.tableName = L"Beam";
    request.creates.push_back(sc::SCBatchCreateRecordRequest{{{L"Width", sc::SCValue::FromInt64(100)}}});
    request.creates.push_back(sc::SCBatchCreateRecordRequest{{{L"Width", sc::SCValue::FromInt64(200)}}});
    requests.push_back(request);

    sc::SCBatchExecutionResult result;
    EXPECT_EQ(sc::ExecuteBatchEdit(db.Get(), requests, options, &result), sc::SC_OK);
    EXPECT_EQ(result.createdCount, 2u);
    EXPECT_FALSE(result.failureLocation.valid);
    EXPECT_EQ(CountAliveRecords(beamTable), 2u);

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    EXPECT_EQ(CountAliveRecords(beamTable), 0u);

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    EXPECT_EQ(CountAliveRecords(beamTable), 2u);
}
