#include <chrono>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <vector>

#include <gtest/gtest.h>

#include "ISCQuery.h"
#include "SCStorage.h"

namespace sc = StableCore::Storage;
namespace fs = std::filesystem;

namespace
{

constexpr std::size_t kSmokeRowCount = 2000;
constexpr long long kBulkImportMaxMs = 15000;
constexpr long long kQueryAndSortMaxMs = 15000;
constexpr long long kRecoveryFinalizeMaxMs = 15000;

fs::path MakeTempDbPath(const wchar_t* fileName)
{
    fs::path path = fs::temp_directory_path() / fileName;
    std::error_code ec;
    fs::remove(path, ec);
    return path;
}

sc::ErrorCode CreateFileDb(const wchar_t* path, sc::SCDbPtr& db)
{
    return sc::CreateFileDatabase(path, sc::SCOpenDatabaseOptions{}, db);
}

template <typename Fn>
bool MeasureMs(Fn&& fn, long long* outElapsedMs)
{
    if (outElapsedMs == nullptr)
    {
        return false;
    }

    const auto start = std::chrono::steady_clock::now();
    const bool ok = fn();
    const auto elapsed = std::chrono::steady_clock::now() - start;
    *outElapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
    return ok;
}

sc::SCTablePtr CreateFloorTable(sc::SCDbPtr& db)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Floor", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef name;
    name.name = L"Name";
    name.displayName = L"Name";
    name.valueKind = sc::ValueKind::String;
    name.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);
    return table;
}

sc::SCTablePtr CreateSmokeBeamTable(sc::SCDbPtr& db)
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
    width.indexed = true;
    EXPECT_EQ(schema->AddColumn(width), sc::SC_OK);

    sc::SCColumnDef name;
    name.name = L"Name";
    name.displayName = L"Name";
    name.valueKind = sc::ValueKind::String;
    name.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);

    sc::SCColumnDef floorRef;
    floorRef.name = L"FloorRef";
    floorRef.displayName = L"FloorRef";
    floorRef.valueKind = sc::ValueKind::RecordId;
    floorRef.columnKind = sc::ColumnKind::Relation;
    floorRef.referenceTable = L"Floor";
    floorRef.indexed = true;
    EXPECT_EQ(schema->AddColumn(floorRef), sc::SC_OK);

    return table;
}

void SeedFloorRecord(const sc::SCTablePtr& floorTable, sc::SCDbPtr& db, sc::SCRecordPtr* outFloor)
{
    ASSERT_NE(outFloor, nullptr);

    sc::SCEditPtr edit;
    ASSERT_EQ(db->BeginEdit(L"seed floor", edit), sc::SC_OK);

    sc::SCRecordPtr floor;
    ASSERT_EQ(floorTable->CreateRecord(floor), sc::SC_OK);
    ASSERT_EQ(floor->SetString(L"Name", L"2F"), sc::SC_OK);
    ASSERT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    *outFloor = floor;
}

std::vector<sc::SCBatchTableRequest> BuildBeamImportRequests(sc::RecordId floorId, std::size_t rowCount)
{
    std::vector<sc::SCBatchTableRequest> requests;
    sc::SCBatchTableRequest request;
    request.tableName = L"Beam";
    request.creates.reserve(rowCount);

    for (std::size_t index = 0; index < rowCount; ++index)
    {
        request.creates.push_back(sc::SCBatchCreateRecordRequest{{
            {L"Width", sc::SCValue::FromInt64(static_cast<std::int64_t>(index))},
            {L"Name", sc::SCValue::FromString(L"Beam-" + std::to_wstring(index))},
            {L"FloorRef", sc::SCValue::FromRecordId(floorId)},
        }});
    }

    requests.push_back(std::move(request));
    return requests;
}

std::size_t CountRecords(const sc::SCRecordCursorPtr& cursor)
{
    std::size_t count = 0;
    bool hasRow = false;
    auto it = cursor;
    while (it->MoveNext(&hasRow) == sc::SC_OK && hasRow)
    {
        ++count;
    }
    return count;
}

void AssertSortedDescendingByWidth(const sc::SCRecordCursorPtr& cursor, std::size_t expectedCount)
{
    bool hasRow = false;
    sc::SCRecordPtr record;
    std::int64_t previousWidth = std::numeric_limits<std::int64_t>::max();
    std::size_t count = 0;

    auto it = cursor;
    while (it->MoveNext(&hasRow) == sc::SC_OK && hasRow)
    {
        ++count;
        ASSERT_EQ(it->GetCurrent(record), sc::SC_OK);
        std::int64_t width = 0;
        ASSERT_EQ(record->GetInt64(L"Width", &width), sc::SC_OK);
        EXPECT_LE(width, previousWidth);
        previousWidth = width;
    }

    EXPECT_EQ(count, expectedCount);
}

}  // namespace

TEST(StoragePerformance, SqliteBulkImportBaseline)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_PerfSmoke_BulkImport.sqlite");

    sc::SCDbPtr db;
    ASSERT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr floorTable = CreateFloorTable(db);
    sc::SCTablePtr beamTable = CreateSmokeBeamTable(db);

    sc::SCRecordPtr floor;
    SeedFloorRecord(floorTable, db, &floor);

    const auto requests = BuildBeamImportRequests(floor->GetId(), kSmokeRowCount);

    sc::SCBatchExecutionOptions options;
    options.editName = L"perf bulk import";
    options.chunkSize = 256;

    sc::SCBatchExecutionResult result;
    long long elapsedMs = 0;
    ASSERT_TRUE(MeasureMs([&]() -> bool
    {
        return sc::ExecuteImport(db.Get(), requests, options, &result) == sc::SC_OK;
    }, &elapsedMs));

    sc::SCRecordCursorPtr cursor;
    ASSERT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);

    RecordProperty("elapsed_ms", elapsedMs);
    RecordProperty("created_count", static_cast<unsigned long long>(result.createdCount));
    RecordProperty("chunk_count", static_cast<unsigned long long>(result.chunkCount));
    RecordProperty("checkpoint_count", static_cast<unsigned long long>(result.checkpointCount));

    EXPECT_EQ(result.createdCount, kSmokeRowCount);
    EXPECT_LE(elapsedMs, kBulkImportMaxMs);
    EXPECT_EQ(CountRecords(cursor), kSmokeRowCount);
}

TEST(StoragePerformance, SqliteQueryAndSortBaseline)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_PerfSmoke_Query.sqlite");

    sc::SCDbPtr db;
    ASSERT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr floorTable = CreateFloorTable(db);
    sc::SCTablePtr beamTable = CreateSmokeBeamTable(db);

    sc::SCRecordPtr floor;
    SeedFloorRecord(floorTable, db, &floor);

    std::vector<sc::SCBatchTableRequest> requests;
    sc::SCBatchTableRequest request;
    request.tableName = L"Beam";
    request.creates.reserve(kSmokeRowCount);
    for (std::size_t index = 0; index < kSmokeRowCount; ++index)
    {
        request.creates.push_back(sc::SCBatchCreateRecordRequest{{
            {L"Width", sc::SCValue::FromInt64(static_cast<std::int64_t>(index))},
            {L"Name", sc::SCValue::FromString(L"Sort-" + std::to_wstring(index))},
            {L"FloorRef", sc::SCValue::FromRecordId(floor->GetId())},
        }});
    }
    requests.push_back(std::move(request));

    sc::SCBatchExecutionOptions options;
    options.editName = L"perf query seed";
    options.chunkSize = 256;

    sc::SCBatchExecutionResult importResult;
    ASSERT_EQ(sc::ExecuteImport(db.Get(), requests, options, &importResult), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    ASSERT_EQ(planner->BuildPlan(
                  sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                  {sc::QueryConditionGroup{
                      sc::QueryLogicOperator::And,
                      {sc::QueryCondition{L"FloorRef", sc::QueryConditionOperator::Equal, {sc::SCValue::FromRecordId(floor->GetId())}}}}},
                  sc::QueryLogicOperator::And,
                  {sc::SortSpec{L"Width", sc::QueryOrderDirection::Descending}},
                  sc::QueryPage{0, std::numeric_limits<std::uint64_t>::max()},
                  {},
                  {},
                  &plan),
              sc::SC_OK);
    ASSERT_EQ(plan.state, sc::QueryPlanState::DirectIndex);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    long long elapsedMs = 0;
    ASSERT_TRUE(MeasureMs([&]() -> bool
    {
        return sc::ExecuteQueryPlan(plan, context, &result) == sc::SC_OK;
    }, &elapsedMs));

    RecordProperty("elapsed_ms", elapsedMs);
    RecordProperty("matched_rows", static_cast<unsigned long long>(result.matchedRows));
    RecordProperty("returned_rows", static_cast<unsigned long long>(result.returnedRows));

    EXPECT_EQ(result.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(result.matchedRows, kSmokeRowCount);
    EXPECT_EQ(result.returnedRows, kSmokeRowCount);
    EXPECT_FALSE(result.usedIndexIds.empty());
    EXPECT_LE(elapsedMs, kQueryAndSortMaxMs);
    AssertSortedDescendingByWidth(cursor, kSmokeRowCount);
}

TEST(StoragePerformance, MemoryQuerySortBaseline)
{
    sc::SCDbPtr db;
    ASSERT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

    sc::SCTablePtr floorTable = CreateFloorTable(db);
    sc::SCTablePtr beamTable = CreateSmokeBeamTable(db);

    sc::SCRecordPtr floor;
    SeedFloorRecord(floorTable, db, &floor);

    sc::SCEditPtr edit;
    ASSERT_EQ(db->BeginEdit(L"perf memory sort seed", edit), sc::SC_OK);
    for (std::size_t index = 0; index < kSmokeRowCount; ++index)
    {
        sc::SCRecordPtr row;
        ASSERT_EQ(beamTable->CreateRecord(row), sc::SC_OK);
        ASSERT_EQ(row->SetInt64(L"Width", static_cast<std::int64_t>(index)), sc::SC_OK);
        const std::wstring name = L"Sort-" + std::to_wstring(index);
        ASSERT_EQ(row->SetString(L"Name", name.c_str()), sc::SC_OK);
        ASSERT_EQ(row->SetRef(L"FloorRef", floor->GetId()), sc::SC_OK);
    }
    ASSERT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    ASSERT_EQ(planner->BuildPlan(
                  sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                  {sc::QueryConditionGroup{
                      sc::QueryLogicOperator::And,
                      {sc::QueryCondition{L"FloorRef", sc::QueryConditionOperator::Equal, {sc::SCValue::FromRecordId(floor->GetId())}}}}},
                  sc::QueryLogicOperator::And,
                  {sc::SortSpec{L"Width", sc::QueryOrderDirection::Descending}},
                  sc::QueryPage{0, std::numeric_limits<std::uint64_t>::max()},
                  {},
                  {},
                  &plan),
              sc::SC_OK);
    ASSERT_EQ(plan.state, sc::QueryPlanState::DirectIndex);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::Memory;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    long long elapsedMs = 0;
    ASSERT_TRUE(MeasureMs([&]() -> bool
    {
        return sc::ExecuteQueryPlan(plan, context, &result) == sc::SC_OK;
    }, &elapsedMs));

    RecordProperty("elapsed_ms", elapsedMs);
    RecordProperty("matched_rows", static_cast<unsigned long long>(result.matchedRows));
    RecordProperty("returned_rows", static_cast<unsigned long long>(result.returnedRows));

    EXPECT_EQ(result.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(result.matchedRows, kSmokeRowCount);
    EXPECT_EQ(result.returnedRows, kSmokeRowCount);
    EXPECT_FALSE(result.usedIndexIds.empty());
    EXPECT_LE(elapsedMs, kQueryAndSortMaxMs);

    AssertSortedDescendingByWidth(cursor, kSmokeRowCount);
}

TEST(StoragePerformance, SqliteRecoveryFinalizeBaseline)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_PerfSmoke_Recovery.sqlite");

    sc::SCImportSessionId sessionId = 0;
    {
        sc::SCDbPtr db;
        ASSERT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr floorTable = CreateFloorTable(db);
        sc::SCTablePtr beamTable = CreateSmokeBeamTable(db);

        sc::SCRecordPtr floor;
        SeedFloorRecord(floorTable, db, &floor);

        sc::SCImportSessionOptions sessionOptions;
        sessionOptions.sessionName = L"perf recovery";
        sessionOptions.chunkSize = 256;

        sc::SCImportStagingArea session;
        ASSERT_EQ(sc::BeginImportSession(db.Get(), sessionOptions, &session), sc::SC_OK);
        sessionId = session.sessionId;

        std::size_t chunkId = 1;
        for (std::size_t offset = 0; offset < kSmokeRowCount; offset += sessionOptions.chunkSize)
        {
            sc::SCImportChunk chunk;
            chunk.chunkId = chunkId++;

            sc::SCBatchTableRequest request;
            request.tableName = L"Beam";
            const std::size_t chunkEnd = std::min(offset + sessionOptions.chunkSize, kSmokeRowCount);
            for (std::size_t index = offset; index < chunkEnd; ++index)
            {
                request.creates.push_back(sc::SCBatchCreateRecordRequest{{
                    {L"Width", sc::SCValue::FromInt64(static_cast<std::int64_t>(index))},
                    {L"Name", sc::SCValue::FromString(L"Recover-" + std::to_wstring(index))},
                    {L"FloorRef", sc::SCValue::FromRecordId(floor->GetId())},
                }});
            }
            chunk.requests.push_back(std::move(request));

            sc::SCImportCheckpoint checkpoint;
            ASSERT_EQ(sc::AppendImportChunk(db.Get(), &session, chunk, &checkpoint), sc::SC_OK);
            EXPECT_EQ(checkpoint.sessionId, sessionId);
        }
    }

    sc::SCDbPtr reopened;
    ASSERT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);

    sc::SCImportRecoveryState recoveryState;
    ASSERT_EQ(reopened->LoadImportRecoveryState(sessionId, &recoveryState), sc::SC_OK);
    ASSERT_TRUE(recoveryState.canResume);
    ASSERT_TRUE(recoveryState.canFinalize);

    sc::SCBatchExecutionResult result;
    sc::SCImportFinalizeCommit commit;
    commit.sessionId = sessionId;
    commit.confirmed = true;
    commit.commitName = L"perf recovery";

    long long elapsedMs = 0;
    ASSERT_TRUE(MeasureMs([&]() -> bool
    {
        return sc::FinalizeImportSession(reopened.Get(), recoveryState.stagingArea, commit, &result) == sc::SC_OK;
    }, &elapsedMs));

    RecordProperty("elapsed_ms", elapsedMs);
    RecordProperty("created_count", static_cast<unsigned long long>(result.createdCount));
    RecordProperty("chunk_count", static_cast<unsigned long long>(result.chunkCount));

    EXPECT_EQ(result.importSessionId, sessionId);
    EXPECT_EQ(result.createdCount, kSmokeRowCount);
    EXPECT_LE(elapsedMs, kRecoveryFinalizeMaxMs);

    sc::SCTablePtr beamTable;
    ASSERT_EQ(reopened->GetTable(L"Beam", beamTable), sc::SC_OK);
    sc::SCRecordCursorPtr cursor;
    ASSERT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);
    EXPECT_EQ(CountRecords(cursor), kSmokeRowCount);

    ASSERT_EQ(reopened->LoadImportRecoveryState(sessionId, &recoveryState), sc::SC_E_RECORD_NOT_FOUND);
}

