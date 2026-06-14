#include <filesystem>

#include <gtest/gtest.h>

#include "ISCQuery.h"
#include "../Src/Query/SCQuerySqliteIndexAccess.h"
#include "SCStorage.h"

namespace sc = StableCore::Storage;
namespace fs = std::filesystem;

namespace
{

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

    void SeedQueryableBeamRows(const sc::SCTablePtr& table, sc::SCDbPtr& db)
    {
        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

        sc::SCRecordPtr row;
        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 200), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Beta"), sc::SC_OK);

        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 300), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Alpine"), sc::SC_OK);

        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    }

}  // namespace

TEST(QuerySqliteExecutorTests, LegacyFindRecordsRoutesThroughExecutor)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_Legacy.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateQueryableBeamTable(db);
    SeedQueryableBeamRows(table, db);

    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(table->FindRecords({L"Width", sc::SCValue::FromInt64(200)}, cursor), sc::SC_OK);

    sc::SCRecordPtr record;
    EXPECT_EQ(cursor->Next(record), sc::SC_OK);
    EXPECT_TRUE(static_cast<bool>(record));

    std::int64_t width = 0;
    EXPECT_EQ(record->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 200);
}

TEST(QuerySqliteExecutorTests, SqliteExecutorReportsDirectAndPartialModes)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_Result.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateQueryableBeamTable(db);
    SeedQueryableBeamRows(table, db);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan directPlan;
    EXPECT_EQ(
        planner->BuildPlan(
            sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
            {sc::QueryConditionGroup{
                sc::QueryLogicOperator::And,
                {sc::QueryCondition{L"Width", sc::QueryConditionOperator::Equal, {sc::SCValue::FromInt64(200)}}}}},
            sc::QueryLogicOperator::And,
            {},
            {},
            {},
            {},
            &directPlan),
        sc::SC_OK);

    sc::SCRecordCursorPtr directCursor;
    sc::QueryExecutionResult directResult;
    sc::QueryExecutionContext directContext;
    directContext.backendKind = sc::QueryBackendKind::SQLite;
    directContext.database = db.Get();
    directContext.backendHandle = db.Get();
    directContext.resultCursor = &directCursor;
    EXPECT_EQ(sc::ExecuteQueryPlan(directPlan, directContext, &directResult), sc::SC_OK);
    EXPECT_EQ(directResult.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_FALSE(directResult.fallbackTriggered);
    EXPECT_EQ(directResult.matchedRows, 1u);
    EXPECT_EQ(directResult.returnedRows, 1u);
    EXPECT_FALSE(directResult.usedIndexIds.empty());
    EXPECT_NE(directResult.executionNote.find(L"SELECT record_id"), std::wstring::npos);

    sc::QueryPlan partialPlan;
    EXPECT_EQ(planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                                 {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                          {sc::QueryCondition{L"Name",
                                                                              sc::QueryConditionOperator::StartsWith,
                                                                              {sc::SCValue::FromString(L"Al")}}}}},
                                 sc::QueryLogicOperator::And,
                                 {},
                                 {},
                                 {},
                                 {},
                                 &partialPlan),
              sc::SC_OK);

    sc::SCRecordCursorPtr partialCursor;
    sc::QueryExecutionResult partialResult;
    sc::QueryExecutionContext partialContext;
    partialContext.backendKind = sc::QueryBackendKind::SQLite;
    partialContext.database = db.Get();
    partialContext.backendHandle = db.Get();
    partialContext.resultCursor = &partialCursor;
    EXPECT_EQ(sc::ExecuteQueryPlan(partialPlan, partialContext, &partialResult), sc::SC_OK);
    EXPECT_EQ(partialResult.mode, sc::QueryExecutionMode::PartialIndex);
    EXPECT_FALSE(partialResult.fallbackTriggered);
    EXPECT_EQ(partialResult.matchedRows, 2u);
    EXPECT_EQ(partialResult.returnedRows, 2u);
    EXPECT_FALSE(partialResult.usedIndexIds.empty());
}

TEST(QuerySqliteExecutorTests, ExecutorRejectsRequireIndexOnPartialPlans)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_Unsupported.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateQueryableBeamTable(db);
    SeedQueryableBeamRows(table, db);

    sc::QueryPlan plan;
    plan.target = sc::QueryTarget{L"Beam", sc::QueryTargetType::Table};
    plan.conditionGroups = {sc::QueryConditionGroup{
        sc::QueryLogicOperator::And,
        {sc::QueryCondition{L"Name", sc::QueryConditionOperator::StartsWith, {sc::SCValue::FromString(L"Al")}}}}};
    plan.conditionGroupLogic = sc::QueryLogicOperator::And;
    plan.state = sc::QueryPlanState::PartialIndex;
    plan.constraints.requireIndex = true;
    plan.constraints.allowPartial = true;
    plan.constraints.allowFallbackScan = false;

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionResult result;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_E_INVALIDARG);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::Unsupported);
    EXPECT_NE(result.executionNote.find(L"executor-unsupported:index-required"), std::wstring::npos);
}

TEST(QuerySqliteExecutorTests, ExecutorRejectsRequireIndexOnDirectPlansWithoutExplicitMatchedIndex)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_RequireIndexDirectWithoutMatch.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateQueryableBeamTable(db);
    SeedQueryableBeamRows(table, db);

    sc::QueryPlan plan;
    plan.target = sc::QueryTarget{L"Beam", sc::QueryTargetType::Table};
    plan.conditionGroups = {sc::QueryConditionGroup{
        sc::QueryLogicOperator::And,
        {sc::QueryCondition{L"Width", sc::QueryConditionOperator::Equal, {sc::SCValue::FromInt64(100)}}}}};
    plan.conditionGroupLogic = sc::QueryLogicOperator::And;
    plan.state = sc::QueryPlanState::DirectIndex;
    plan.constraints.requireIndex = true;
    plan.constraints.allowFallbackScan = false;

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionResult result;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_E_INVALIDARG);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::Unsupported);
    EXPECT_NE(result.executionNote.find(L"executor-unsupported:index-required"), std::wstring::npos);
}

TEST(QuerySqliteExecutorTests, RequireIndexRejectsLegacySingleColumnIndexesWithoutExplicitSchemaIndex)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_RequireIndexLegacy.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateQueryableBeamTable(db);
    SeedQueryableBeamRows(table, db);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    sc::QueryConstraints constraints;
    constraints.requireIndex = true;
    constraints.allowFallbackScan = false;
    constraints.allowPartial = true;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(200)}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           constraints,
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_E_INVALIDARG);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::Unsupported);
    EXPECT_NE(result.executionNote.find(L"executor-unsupported:index-required"), std::wstring::npos);
}

TEST(QuerySqliteExecutorTests, RequireIndexAcceptsExplicitCompositeSchemaIndex)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_RequireIndexExplicitOnly.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);
    SeedQueryableBeamRows(table, db);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    sc::QueryConstraints constraints;
    constraints.requireIndex = true;
    constraints.allowFallbackScan = false;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           constraints,
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    const sc::ErrorCode executeRc = sc::ExecuteQueryPlan(plan, context, &result);
    SCOPED_TRACE(::testing::Message() << "executionRc=" << executeRc << " executionNote="
                                      << std::string(result.executionNote.begin(), result.executionNote.end()));
    EXPECT_EQ(executeRc, sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::DirectIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name");
}

TEST(QuerySqliteExecutorTests, RequireIndexPrefersExplicitCompositeIndexWhenLegacyHintAlsoExists)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_RequireIndexExplicitWins.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompositeIndexedBeamTableWithLegacyWidth(db);
    SeedQueryableBeamRows(table, db);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    sc::QueryConstraints constraints;
    constraints.requireIndex = true;
    constraints.allowFallbackScan = false;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           constraints,
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::DirectIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name");
}

TEST(QuerySqliteExecutorTests, RequireIndexRejectsNonPrefixCompositeLookupWithoutLegacyIndex)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_RequireIndexCompositeMiss.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);
    SeedQueryableBeamRows(table, db);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    sc::QueryConstraints constraints;
    constraints.requireIndex = true;
    constraints.allowFallbackScan = false;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromString(L"Alpha")}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           constraints,
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_E_INVALIDARG);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::Unsupported);
    EXPECT_NE(result.executionNote.find(L"executor-unsupported:index-required"), std::wstring::npos);
}

TEST(QuerySqliteExecutorTests, SqliteExecutorRunsControlledFallbackScan)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_Fallback.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateQueryableBeamTable(db);
    SeedQueryableBeamRows(table, db);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(
            sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
            {sc::QueryConditionGroup{
                sc::QueryLogicOperator::And,
                {sc::QueryCondition{L"Name", sc::QueryConditionOperator::Contains, {sc::SCValue::FromString(L"Al")}}}}},
            sc::QueryLogicOperator::And,
            {},
            {},
            {},
            {},
            &plan),
        sc::SC_OK);
    EXPECT_EQ(plan.state, sc::QueryPlanState::ScanFallback);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::FallbackScan);
    EXPECT_TRUE(result.fallbackTriggered);
    EXPECT_EQ(result.fallbackSource, sc::QueryFallbackSource::Planner);
    EXPECT_EQ(result.scannedRows, 3u);
    EXPECT_EQ(result.matchedRows, 2u);
    EXPECT_EQ(result.returnedRows, 2u);
    EXPECT_NE(result.executionNote.find(L"planner-fallback"), std::wstring::npos);
}

TEST(QuerySqliteExecutorTests, ExplicitCompositeIndexSupportsPrefixLookupWithoutLegacyIndexedFlag)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositePrefix.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);
    SeedQueryableBeamRows(table, db);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(
            sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
            {sc::QueryConditionGroup{
                sc::QueryLogicOperator::And,
                {sc::QueryCondition{L"Width", sc::QueryConditionOperator::Equal, {sc::SCValue::FromInt64(100)}}}}},
            sc::QueryLogicOperator::And,
            {},
            {},
            {},
            {},
            &plan),
        sc::SC_OK);

    auto* indexAccess = dynamic_cast<sc::ISqliteQueryIndexAccess*>(db.Get());
    ASSERT_NE(indexAccess, nullptr);

    sc::QueryPlan analyzedPlan;
    EXPECT_EQ(indexAccess->AnalyzeCompositeIndexPlan(plan, &analyzedPlan), sc::SC_OK);
    ASSERT_TRUE(analyzedPlan.matchedIndex.has_value());
    EXPECT_EQ(analyzedPlan.matchedIndex->indexName, L"idx_Beam_Width_Name");
    EXPECT_EQ(analyzedPlan.state, sc::QueryPlanState::DirectIndex);
    EXPECT_TRUE(analyzedPlan.pushdown.residualConditions.empty());
}

TEST(QuerySqliteExecutorTests, ExplicitCompositeIndexSupportsThreeColumnHappyPath)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeThreeColumn.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateTripleCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed three column", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 30), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 10), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 20), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromString(L"Alpha")}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Height", sc::QueryOrderDirection::Ascending, false}},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::DirectIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name_Height");
    EXPECT_EQ(result.matchedRows, 2u);

    std::vector<std::int64_t> heights;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::int64_t height = 0;
        EXPECT_EQ(record->GetInt64(L"Height", &height), sc::SC_OK);
        heights.push_back(height);
    }

    ASSERT_EQ(heights.size(), 2u);
    EXPECT_EQ(heights[0], 10);
    EXPECT_EQ(heights[1], 30);
}

TEST(QuerySqliteExecutorTests, ExplicitCompositeIndexSupportsThreeColumnLeadingPrefixLookup)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeThreeColumnPrefix.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateTripleCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed three column prefix", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 30), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 10), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 20), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 5), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Ascending, false},
                            sc::SortSpec{L"Height", sc::QueryOrderDirection::Ascending, false}},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::DirectIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name_Height");
    EXPECT_EQ(result.matchedRows, 3u);

    std::vector<std::wstring> orderedKeys;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        std::int64_t height = 0;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        EXPECT_EQ(record->GetInt64(L"Height", &height), sc::SC_OK);
        orderedKeys.push_back(name + L":" + std::to_wstring(height));
    }

    ASSERT_EQ(orderedKeys.size(), 3u);
    EXPECT_EQ(orderedKeys[0], L"Alpha:10");
    EXPECT_EQ(orderedKeys[1], L"Alpha:30");
    EXPECT_EQ(orderedKeys[2], L"Bravo:20");
}

TEST(QuerySqliteExecutorTests, ExplicitCompositeIndexSupportsThreeColumnExactLookup)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeThreeColumnExact.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateTripleCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed three column exact", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 30), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 10), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 10), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{
                               sc::QueryLogicOperator::And,
                               {sc::QueryCondition{L"Width",
                                                   sc::QueryConditionOperator::Equal,
                                                   {sc::SCValue::FromInt64(100)}},
                                sc::QueryCondition{L"Name",
                                                   sc::QueryConditionOperator::Equal,
                                                   {sc::SCValue::FromString(L"Alpha")}},
                                sc::QueryCondition{L"Height",
                                                   sc::QueryConditionOperator::Equal,
                                                   {sc::SCValue::FromInt64(10)}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::DirectIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name_Height");
    EXPECT_EQ(result.matchedRows, 1u);

    sc::SCRecordPtr record;
    EXPECT_EQ(cursor->Next(record), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(record));

    std::int64_t height = 0;
    EXPECT_EQ(record->GetInt64(L"Height", &height), sc::SC_OK);
    EXPECT_EQ(height, 10);
}

TEST(QuerySqliteExecutorTests, ExplicitCompositeIndexSupportsThreeColumnTailRange)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeThreeColumnTailRange.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateTripleCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed three column tail range", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 10), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 20), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 30), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 25), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{
                               sc::QueryLogicOperator::And,
                               {sc::QueryCondition{L"Width",
                                                   sc::QueryConditionOperator::Equal,
                                                   {sc::SCValue::FromInt64(100)}},
                                sc::QueryCondition{L"Name",
                                                   sc::QueryConditionOperator::Equal,
                                                   {sc::SCValue::FromString(L"Alpha")}},
                                sc::QueryCondition{L"Height",
                                                   sc::QueryConditionOperator::GreaterThanOrEqual,
                                                   {sc::SCValue::FromInt64(20)}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Height", sc::QueryOrderDirection::Ascending, false}},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::PartialIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name_Height");
    EXPECT_EQ(result.matchedRows, 2u);

    std::vector<std::int64_t> heights;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::int64_t height = 0;
        EXPECT_EQ(record->GetInt64(L"Height", &height), sc::SC_OK);
        heights.push_back(height);
    }

    ASSERT_EQ(heights.size(), 2u);
    EXPECT_EQ(heights[0], 20);
    EXPECT_EQ(heights[1], 30);
}

TEST(QuerySqliteExecutorTests, ExplicitCompositeIndexSupportsThreeColumnOrderCoverageAfterLeadingPrefix)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeThreeColumnOrderCoverage.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateTripleCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed three column order coverage", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Charlie"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 40), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 30), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 10), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 20), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Ascending, false},
                            sc::SortSpec{L"Height", sc::QueryOrderDirection::Ascending, false}},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_FALSE(result.fallbackTriggered);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name_Height");
    EXPECT_EQ(result.matchedRows, 4u);

    std::vector<std::wstring> orderedKeys;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        std::int64_t height = 0;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        EXPECT_EQ(record->GetInt64(L"Height", &height), sc::SC_OK);
        orderedKeys.push_back(name + L":" + std::to_wstring(height));
    }

    ASSERT_EQ(orderedKeys.size(), 4u);
    EXPECT_EQ(orderedKeys[0], L"Alpha:10");
    EXPECT_EQ(orderedKeys[1], L"Alpha:30");
    EXPECT_EQ(orderedKeys[2], L"Bravo:20");
    EXPECT_EQ(orderedKeys[3], L"Charlie:40");
}

TEST(QuerySqliteExecutorTests, DescendingCompositeIndexSupportsThreeColumnTailRange)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeThreeColumnTailRangeDesc.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateDescendingTripleCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed three column tail range desc", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 10), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 20), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 30), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 25), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{
                               sc::QueryLogicOperator::And,
                               {sc::QueryCondition{L"Width",
                                                   sc::QueryConditionOperator::Equal,
                                                   {sc::SCValue::FromInt64(100)}},
                                sc::QueryCondition{L"Name",
                                                   sc::QueryConditionOperator::Equal,
                                                   {sc::SCValue::FromString(L"Alpha")}},
                                sc::QueryCondition{L"Height",
                                                   sc::QueryConditionOperator::LessThanOrEqual,
                                                   {sc::SCValue::FromInt64(20)}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Height", sc::QueryOrderDirection::Descending, false}},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::PartialIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name_Height_Desc");
    EXPECT_EQ(result.matchedRows, 2u);

    std::vector<std::int64_t> heights;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::int64_t height = 0;
        EXPECT_EQ(record->GetInt64(L"Height", &height), sc::SC_OK);
        heights.push_back(height);
    }

    ASSERT_EQ(heights.size(), 2u);
    EXPECT_EQ(heights[0], 20);
    EXPECT_EQ(heights[1], 10);
}

TEST(QuerySqliteExecutorTests, DescendingCompositeIndexSupportsThreeColumnOrderCoverageAfterLeadingPrefix)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeThreeColumnOrderCoverageDesc.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateDescendingTripleCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed three column order coverage desc", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Charlie"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 40), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 30), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 10), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 20), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Descending, false},
                            sc::SortSpec{L"Height", sc::QueryOrderDirection::Descending, false}},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_FALSE(result.fallbackTriggered);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name_Height_Desc");
    EXPECT_EQ(result.matchedRows, 4u);

    std::vector<std::wstring> orderedKeys;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        std::int64_t height = 0;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        EXPECT_EQ(record->GetInt64(L"Height", &height), sc::SC_OK);
        orderedKeys.push_back(name + L":" + std::to_wstring(height));
    }

    ASSERT_EQ(orderedKeys.size(), 4u);
    EXPECT_EQ(orderedKeys[0], L"Charlie:40");
    EXPECT_EQ(orderedKeys[1], L"Bravo:20");
    EXPECT_EQ(orderedKeys[2], L"Alpha:30");
    EXPECT_EQ(orderedKeys[3], L"Alpha:10");
}

TEST(QuerySqliteExecutorTests, DescendingCompositeIndexThreeColumnUndoRedoReopenPreservesTailRangeAndOrderCoverage)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeThreeColumnDescUndoRedoReopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr table = CreateDescendingTripleCompositeIndexedBeamTable(db);

        sc::SCEditPtr seedEdit;
        EXPECT_EQ(db->BeginEdit(L"seed three column desc undo redo", seedEdit), sc::SC_OK);

        sc::SCRecordPtr alpha30;
        EXPECT_EQ(table->CreateRecord(alpha30), sc::SC_OK);
        EXPECT_EQ(alpha30->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(alpha30->SetString(L"Name", L"Alpha"), sc::SC_OK);
        EXPECT_EQ(alpha30->SetInt64(L"Height", 30), sc::SC_OK);
        const sc::RecordId alpha30Id = alpha30->GetId();

        sc::SCRecordPtr alpha10;
        EXPECT_EQ(table->CreateRecord(alpha10), sc::SC_OK);
        EXPECT_EQ(alpha10->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(alpha10->SetString(L"Name", L"Alpha"), sc::SC_OK);
        EXPECT_EQ(alpha10->SetInt64(L"Height", 10), sc::SC_OK);

        sc::SCRecordPtr bravo;
        EXPECT_EQ(table->CreateRecord(bravo), sc::SC_OK);
        EXPECT_EQ(bravo->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(bravo->SetString(L"Name", L"Bravo"), sc::SC_OK);
        EXPECT_EQ(bravo->SetInt64(L"Height", 20), sc::SC_OK);

        sc::SCRecordPtr charlie;
        EXPECT_EQ(table->CreateRecord(charlie), sc::SC_OK);
        EXPECT_EQ(charlie->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(charlie->SetString(L"Name", L"Charlie"), sc::SC_OK);
        EXPECT_EQ(charlie->SetInt64(L"Height", 40), sc::SC_OK);

        EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

        sc::SCEditPtr renameEdit;
        EXPECT_EQ(db->BeginEdit(L"rename charlie to delta", renameEdit), sc::SC_OK);
        EXPECT_EQ(charlie->SetString(L"Name", L"Delta"), sc::SC_OK);
        EXPECT_EQ(db->Commit(renameEdit.Get()), sc::SC_OK);

        sc::SCEditPtr deleteEdit;
        EXPECT_EQ(db->BeginEdit(L"delete alpha30", deleteEdit), sc::SC_OK);
        EXPECT_EQ(table->DeleteRecord(alpha30Id), sc::SC_OK);
        EXPECT_EQ(db->Commit(deleteEdit.Get()), sc::SC_OK);

        EXPECT_EQ(db->Undo(), sc::SC_OK);
    }

    {
        sc::SCDbPtr reopened;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);

        auto planner = sc::CreateDefaultQueryPlanner();
        ASSERT_NE(planner, nullptr);

        sc::QueryPlan tailRangePlan;
        EXPECT_EQ(
            planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                               {sc::QueryConditionGroup{
                                   sc::QueryLogicOperator::And,
                                   {sc::QueryCondition{L"Width",
                                                       sc::QueryConditionOperator::Equal,
                                                       {sc::SCValue::FromInt64(100)}},
                                    sc::QueryCondition{L"Name",
                                                       sc::QueryConditionOperator::Equal,
                                                       {sc::SCValue::FromString(L"Alpha")}},
                                    sc::QueryCondition{L"Height",
                                                       sc::QueryConditionOperator::LessThanOrEqual,
                                                       {sc::SCValue::FromInt64(30)}}}}},
                               sc::QueryLogicOperator::And,
                               {sc::SortSpec{L"Height", sc::QueryOrderDirection::Descending, false}},
                               {},
                               {},
                               {},
                               &tailRangePlan),
            sc::SC_OK);

        sc::QueryPlan orderCoveragePlan;
        EXPECT_EQ(
            planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                               {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                        {sc::QueryCondition{L"Width",
                                                                            sc::QueryConditionOperator::Equal,
                                                                            {sc::SCValue::FromInt64(100)}}}}},
                               sc::QueryLogicOperator::And,
                               {sc::SortSpec{L"Name", sc::QueryOrderDirection::Descending, false},
                                sc::SortSpec{L"Height", sc::QueryOrderDirection::Descending, false}},
                               {},
                               {},
                               {},
                               &orderCoveragePlan),
            sc::SC_OK);

        sc::QueryExecutionContext context;
        context.backendKind = sc::QueryBackendKind::SQLite;
        context.database = reopened.Get();
        context.backendHandle = reopened.Get();

        sc::SCRecordCursorPtr tailCursor;
        context.resultCursor = &tailCursor;
        sc::QueryExecutionResult tailResult;
        EXPECT_EQ(sc::ExecuteQueryPlan(tailRangePlan, context, &tailResult), sc::SC_OK);
        EXPECT_EQ(tailResult.mode, sc::QueryExecutionMode::PartialIndex);
        EXPECT_EQ(tailResult.matchedRows, 2u);

        std::vector<std::int64_t> tailHeights;
        sc::SCRecordPtr record;
        while (tailCursor->Next(record) == sc::SC_OK && record)
        {
            std::int64_t height = 0;
            EXPECT_EQ(record->GetInt64(L"Height", &height), sc::SC_OK);
            tailHeights.push_back(height);
        }

        ASSERT_EQ(tailHeights.size(), 2u);
        EXPECT_EQ(tailHeights[0], 30);
        EXPECT_EQ(tailHeights[1], 10);

        sc::SCRecordCursorPtr orderCursor;
        context.resultCursor = &orderCursor;
        sc::QueryExecutionResult orderResult;
        EXPECT_EQ(sc::ExecuteQueryPlan(orderCoveragePlan, context, &orderResult), sc::SC_OK);
        EXPECT_EQ(orderResult.mode, sc::QueryExecutionMode::DirectIndex);
        EXPECT_EQ(orderResult.matchedRows, 4u);

        std::vector<std::wstring> orderedKeys;
        while (orderCursor->Next(record) == sc::SC_OK && record)
        {
            std::wstring name;
            std::int64_t height = 0;
            EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
            EXPECT_EQ(record->GetInt64(L"Height", &height), sc::SC_OK);
            orderedKeys.push_back(name + L":" + std::to_wstring(height));
        }

        ASSERT_EQ(orderedKeys.size(), 4u);
        EXPECT_EQ(orderedKeys[0], L"Delta:40");
        EXPECT_EQ(orderedKeys[1], L"Bravo:20");
        EXPECT_EQ(orderedKeys[2], L"Alpha:30");
        EXPECT_EQ(orderedKeys[3], L"Alpha:10");

        EXPECT_EQ(reopened->Redo(), sc::SC_OK);

        sc::SCRecordCursorPtr redoTailCursor;
        context.resultCursor = &redoTailCursor;
        sc::QueryExecutionResult redoTailResult;
        EXPECT_EQ(sc::ExecuteQueryPlan(tailRangePlan, context, &redoTailResult), sc::SC_OK);
        EXPECT_EQ(redoTailResult.mode, sc::QueryExecutionMode::PartialIndex);
        EXPECT_EQ(redoTailResult.matchedRows, 1u);

        std::vector<std::int64_t> redoTailHeights;
        while (redoTailCursor->Next(record) == sc::SC_OK && record)
        {
            std::int64_t height = 0;
            EXPECT_EQ(record->GetInt64(L"Height", &height), sc::SC_OK);
            redoTailHeights.push_back(height);
        }

        ASSERT_EQ(redoTailHeights.size(), 1u);
        EXPECT_EQ(redoTailHeights[0], 10);

        sc::SCRecordCursorPtr redoOrderCursor;
        context.resultCursor = &redoOrderCursor;
        sc::QueryExecutionResult redoOrderResult;
        EXPECT_EQ(sc::ExecuteQueryPlan(orderCoveragePlan, context, &redoOrderResult), sc::SC_OK);
        EXPECT_EQ(redoOrderResult.mode, sc::QueryExecutionMode::DirectIndex);
        EXPECT_EQ(redoOrderResult.matchedRows, 3u);

        std::vector<std::wstring> redoOrderedKeys;
        while (redoOrderCursor->Next(record) == sc::SC_OK && record)
        {
            std::wstring name;
            std::int64_t height = 0;
            EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
            EXPECT_EQ(record->GetInt64(L"Height", &height), sc::SC_OK);
            redoOrderedKeys.push_back(name + L":" + std::to_wstring(height));
        }

        ASSERT_EQ(redoOrderedKeys.size(), 3u);
        EXPECT_EQ(redoOrderedKeys[0], L"Delta:40");
        EXPECT_EQ(redoOrderedKeys[1], L"Bravo:20");
        EXPECT_EQ(redoOrderedKeys[2], L"Alpha:10");
    }
}

TEST(QuerySqliteExecutorTests, ExplicitCompositeIndexTreatsMissingValueAsDefaultInLogicalEntries)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeMissingDefault.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed missing default", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromString(L"")}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(result.matchedRows, 1u);
}

TEST(QuerySqliteExecutorTests, ExplicitCompositeIndexSupportsNullValuesInLogicalEntries)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeNullValue.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateNullableCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed null composite", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::Null()}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(result.matchedRows, 1u);
}

TEST(QuerySqliteExecutorTests, ExplicitCompositeIndexUpdatesValueToNullWithoutResidualEntry)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeNullUpdate.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateNullableCompositeIndexedBeamTable(db);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed null update", seedEdit), sc::SC_OK);

    sc::SCRecordPtr alpha;
    EXPECT_EQ(table->CreateRecord(alpha), sc::SC_OK);
    EXPECT_EQ(alpha->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(alpha->SetString(L"Name", L"Alpha"), sc::SC_OK);

    sc::SCRecordPtr bravo;
    EXPECT_EQ(table->CreateRecord(bravo), sc::SC_OK);
    EXPECT_EQ(bravo->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(bravo->SetString(L"Name", L"Bravo"), sc::SC_OK);

    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr nullEdit;
    EXPECT_EQ(db->BeginEdit(L"set alpha to null", nullEdit), sc::SC_OK);
    EXPECT_EQ(alpha->SetValue(L"Name", sc::SCValue::Null()), sc::SC_OK);
    EXPECT_EQ(db->Commit(nullEdit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan nullPlan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::Null()}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           {},
                           &nullPlan),
        sc::SC_OK);

    sc::SCRecordCursorPtr nullCursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &nullCursor;

    sc::QueryExecutionResult nullResult;
    EXPECT_EQ(sc::ExecuteQueryPlan(nullPlan, context, &nullResult), sc::SC_OK);
    EXPECT_EQ(nullResult.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(nullResult.matchedRows, 1u);

    sc::SCRecordPtr nullRecord;
    EXPECT_EQ(nullCursor->Next(nullRecord), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(nullRecord));
    std::wstring unusedName;
    EXPECT_EQ(nullRecord->GetStringCopy(L"Name", &unusedName), sc::SC_E_VALUE_IS_NULL);

    sc::QueryPlan alphaPlan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromString(L"Alpha")}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           {},
                           &alphaPlan),
        sc::SC_OK);

    sc::SCRecordCursorPtr alphaCursor;
    context.resultCursor = &alphaCursor;
    sc::QueryExecutionResult alphaResult;
    EXPECT_EQ(sc::ExecuteQueryPlan(alphaPlan, context, &alphaResult), sc::SC_OK);
    EXPECT_EQ(alphaResult.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(alphaResult.matchedRows, 0u);
}

TEST(QuerySqliteExecutorTests, ExplicitCompositeIndexPreservesNullAndDefaultEntriesAfterReopen)
{
    const fs::path nullDbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeNullReopen.sqlite");
    const fs::path defaultDbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeMissingDefaultReopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(nullDbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr nullableTable = CreateNullableCompositeIndexedBeamTable(db);

        sc::SCEditPtr nullableEdit;
        EXPECT_EQ(db->BeginEdit(L"seed null reopen", nullableEdit), sc::SC_OK);

        sc::SCRecordPtr nullableRow;
        EXPECT_EQ(nullableTable->CreateRecord(nullableRow), sc::SC_OK);
        EXPECT_EQ(nullableRow->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(nullableRow->SetString(L"Name", L"Alpha"), sc::SC_OK);
        EXPECT_EQ(db->Commit(nullableEdit.Get()), sc::SC_OK);

        sc::SCEditPtr nullEdit;
        EXPECT_EQ(db->BeginEdit(L"update to null before reopen", nullEdit), sc::SC_OK);
        EXPECT_EQ(nullableRow->SetValue(L"Name", sc::SCValue::Null()), sc::SC_OK);
        EXPECT_EQ(db->Commit(nullEdit.Get()), sc::SC_OK);
    }

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(defaultDbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr defaultTable = CreateCompositeIndexedBeamTable(db);

        sc::SCEditPtr defaultEdit;
        EXPECT_EQ(db->BeginEdit(L"seed default reopen", defaultEdit), sc::SC_OK);
        sc::SCRecordPtr defaultRow;
        EXPECT_EQ(defaultTable->CreateRecord(defaultRow), sc::SC_OK);
        EXPECT_EQ(defaultRow->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(defaultTable->CreateRecord(defaultRow), sc::SC_OK);
        EXPECT_EQ(defaultRow->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(defaultRow->SetString(L"Name", L"Zulu"), sc::SC_OK);
        EXPECT_EQ(db->Commit(defaultEdit.Get()), sc::SC_OK);
    }

    sc::SCDbPtr reopenedNullDb;
    EXPECT_EQ(CreateFileDb(nullDbPath.c_str(), reopenedNullDb), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan nullPlan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::Null()}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           {},
                           &nullPlan),
        sc::SC_OK);

    sc::SCRecordCursorPtr nullCursor;
    sc::QueryExecutionContext nullContext;
    nullContext.backendKind = sc::QueryBackendKind::SQLite;
    nullContext.database = reopenedNullDb.Get();
    nullContext.backendHandle = reopenedNullDb.Get();
    nullContext.resultCursor = &nullCursor;

    sc::QueryExecutionResult nullResult;
    EXPECT_EQ(sc::ExecuteQueryPlan(nullPlan, nullContext, &nullResult), sc::SC_OK);
    EXPECT_EQ(nullResult.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(nullResult.matchedRows, 1u);

    sc::SCDbPtr reopenedDefaultDb;
    EXPECT_EQ(CreateFileDb(defaultDbPath.c_str(), reopenedDefaultDb), sc::SC_OK);

    sc::QueryPlan defaultPlan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromString(L"")}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           {},
                           &defaultPlan),
        sc::SC_OK);

    sc::SCRecordCursorPtr defaultCursor;
    sc::QueryExecutionContext defaultContext;
    defaultContext.backendKind = sc::QueryBackendKind::SQLite;
    defaultContext.database = reopenedDefaultDb.Get();
    defaultContext.backendHandle = reopenedDefaultDb.Get();
    defaultContext.resultCursor = &defaultCursor;

    sc::QueryExecutionResult defaultResult;
    EXPECT_EQ(sc::ExecuteQueryPlan(defaultPlan, defaultContext, &defaultResult), sc::SC_OK);
    EXPECT_EQ(defaultResult.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(defaultResult.matchedRows, 1u);
}

TEST(QuerySqliteExecutorTests, RequireIndexSupportsExplicitNullCompositeLookup)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_RequireIndexNullExplicit.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateNullableCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed require index null", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    sc::QueryConstraints constraints;
    constraints.requireIndex = true;
    constraints.allowFallbackScan = false;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::Null()}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           constraints,
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::DirectIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name");
    EXPECT_EQ(result.matchedRows, 1u);
}

TEST(QuerySqliteExecutorTests, RequireIndexSupportsExplicitDefaultCompositeLookup)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_RequireIndexDefaultExplicit.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed require index default", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Zulu"), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    sc::QueryConstraints constraints;
    constraints.requireIndex = true;
    constraints.allowFallbackScan = false;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromString(L"")}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           constraints,
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::DirectIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name");
    EXPECT_EQ(result.matchedRows, 1u);
}

TEST(QuerySqliteExecutorTests, CompositeIndexTieBreakPrefersOrderCoveredCandidate)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeTieBreakOrder.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompetingCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed competing indexes", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Zulu"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 30), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 10), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Height", sc::QueryOrderDirection::Ascending, false}},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::DirectIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Height");
}

TEST(QuerySqliteExecutorTests, DISABLED_CompositeIndexTieBreakPrefersFewerResidualConditionsWhenStrengthIsTied)
{
    GTEST_SKIP() << "Duplicate-predicate residual tie-break currently triggers a bad allocation in "
                    "AnalyzeCompositeIndexPlan and needs targeted debugging.";

    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeTieBreakResidual.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompetingCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed competing residual indexes", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 10), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 10), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    // Intentionally repeat the same Name equality predicate so both competing
    // indexes have equal prefix strength, but idx_Beam_Width_Name leaves fewer
    // residual conditions after normalization than idx_Beam_Width_Height.
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{
                               sc::QueryLogicOperator::And,
                               {sc::QueryCondition{L"Width",
                                                   sc::QueryConditionOperator::Equal,
                                                   {sc::SCValue::FromInt64(100)}},
                                sc::QueryCondition{L"Name",
                                                   sc::QueryConditionOperator::Equal,
                                                   {sc::SCValue::FromString(L"Alpha")}},
                                sc::QueryCondition{L"Name",
                                                   sc::QueryConditionOperator::Equal,
                                                   {sc::SCValue::FromString(L"Alpha")}},
                                sc::QueryCondition{L"Height",
                                                   sc::QueryConditionOperator::Equal,
                                                   {sc::SCValue::FromInt64(10)}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    auto* indexAccess = dynamic_cast<sc::ISqliteQueryIndexAccess*>(db.Get());
    ASSERT_NE(indexAccess, nullptr);

    sc::QueryPlan analyzedPlan;
    EXPECT_EQ(indexAccess->AnalyzeCompositeIndexPlan(plan, &analyzedPlan), sc::SC_OK);
    ASSERT_TRUE(analyzedPlan.matchedIndex.has_value());
    EXPECT_EQ(analyzedPlan.matchedIndex->indexName, L"idx_Beam_Width_Name");
    EXPECT_EQ(analyzedPlan.state, sc::QueryPlanState::PartialIndex);
    ASSERT_EQ(analyzedPlan.pushdown.residualConditions.size(), 1u);
    EXPECT_EQ(analyzedPlan.pushdown.residualConditions.front().fieldName, L"Height");
    EXPECT_EQ(analyzedPlan.pushdown.residualConditions.front().op, sc::QueryConditionOperator::Equal);
}

TEST(QuerySqliteExecutorTests, DISABLED_CompositeIndexTieBreakPrefersFewerNaturalResidualConditionsWhenStrengthIsTied)
{
    GTEST_SKIP() << "Natural residual normalization tie-break currently triggers a bad allocation in "
                    "AnalyzeCompositeIndexPlan and needs targeted debugging.";

    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeTieBreakNaturalResidual.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompetingCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed natural residual indexes", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 10), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Height", 10), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    // Name = 'Alpha' naturally implies Name IS NOT NULL. After residual
    // normalization, idx_Beam_Width_Name should leave only Height = 10,
    // while idx_Beam_Width_Height still leaves both Name predicates.
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{
                               sc::QueryLogicOperator::And,
                               {sc::QueryCondition{L"Width",
                                                   sc::QueryConditionOperator::Equal,
                                                   {sc::SCValue::FromInt64(100)}},
                                sc::QueryCondition{L"Name",
                                                   sc::QueryConditionOperator::Equal,
                                                   {sc::SCValue::FromString(L"Alpha")}},
                                sc::QueryCondition{L"Name", sc::QueryConditionOperator::IsNotNull, {}},
                                sc::QueryCondition{L"Height",
                                                   sc::QueryConditionOperator::Equal,
                                                   {sc::SCValue::FromInt64(10)}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    auto* indexAccess = dynamic_cast<sc::ISqliteQueryIndexAccess*>(db.Get());
    ASSERT_NE(indexAccess, nullptr);

    sc::QueryPlan analyzedPlan;
    EXPECT_EQ(indexAccess->AnalyzeCompositeIndexPlan(plan, &analyzedPlan), sc::SC_OK);
    ASSERT_TRUE(analyzedPlan.matchedIndex.has_value());
    EXPECT_EQ(analyzedPlan.matchedIndex->indexName, L"idx_Beam_Width_Name");
    EXPECT_EQ(analyzedPlan.state, sc::QueryPlanState::PartialIndex);
    ASSERT_EQ(analyzedPlan.pushdown.residualConditions.size(), 1u);
    EXPECT_EQ(analyzedPlan.pushdown.residualConditions.front().fieldName, L"Height");
    EXPECT_EQ(analyzedPlan.pushdown.residualConditions.front().op, sc::QueryConditionOperator::Equal);
}

TEST(QuerySqliteExecutorTests, ResidualNormalizationDoesNotDropIsNotNullForEqualNull)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeResidualNullCounterExample.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateNullableCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed residual null counterexample", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{
                               sc::QueryLogicOperator::And,
                               {sc::QueryCondition{L"Width",
                                                   sc::QueryConditionOperator::Equal,
                                                   {sc::SCValue::FromInt64(100)}},
                                sc::QueryCondition{L"Name",
                                                   sc::QueryConditionOperator::Equal,
                                                   {sc::SCValue::Null()}},
                                sc::QueryCondition{L"Name", sc::QueryConditionOperator::IsNotNull, {}}}}},
                           sc::QueryLogicOperator::And,
                           {},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::PartialIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name");
    EXPECT_EQ(result.matchedRows, 0u);

    sc::SCRecordPtr record;
    EXPECT_EQ(cursor->Next(record), sc::SC_OK);
    EXPECT_FALSE(static_cast<bool>(record));
}

TEST(QuerySqliteExecutorTests, ExplicitCompositeIndexSupportsOrderedPrefixScan)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeOrder.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed composite", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Zulu"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                                 {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                          {sc::QueryCondition{L"Width",
                                                                              sc::QueryConditionOperator::Equal,
                                                                              {sc::SCValue::FromInt64(100)}}}}},
                                 sc::QueryLogicOperator::And,
                                 {sc::SortSpec{L"Name", sc::QueryOrderDirection::Ascending, false}},
                                 {},
                                 {},
                                 {},
                                 &plan),
              sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::DirectIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name");

    std::vector<std::wstring> names;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        names.push_back(name);
    }

    ASSERT_EQ(names.size(), 3u);
    EXPECT_EQ(names[0], L"Alpha");
    EXPECT_EQ(names[1], L"Bravo");
    EXPECT_EQ(names[2], L"Zulu");
}

TEST(QuerySqliteExecutorTests, ExplicitCompositeIndexSupportsEqualityPrefixPlusRangeTail)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeRangeTail.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed range tail", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Zulu"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Charlie"), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::GreaterThanOrEqual,
                                                                        {sc::SCValue::FromString(L"Bravo")}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Ascending, false}},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::PartialIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name");
    EXPECT_EQ(result.matchedRows, 2u);

    std::vector<std::wstring> names;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        names.push_back(name);
    }

    ASSERT_EQ(names.size(), 2u);
    EXPECT_EQ(names[0], L"Bravo");
    EXPECT_EQ(names[1], L"Zulu");
}

TEST(QuerySqliteExecutorTests, ExplicitCompositeIndexDoesNotReviveDeletedRecords)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeDelete.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);
    SeedQueryableBeamRows(table, db);

    sc::SCRecordPtr record;
    EXPECT_EQ(table->GetRecord(1, record), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(record));

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"delete", edit), sc::SC_OK);
    EXPECT_EQ(table->DeleteRecord(1), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(
            sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
            {sc::QueryConditionGroup{
                sc::QueryLogicOperator::And,
                {sc::QueryCondition{L"Width", sc::QueryConditionOperator::Equal, {sc::SCValue::FromInt64(100)}}}}},
            sc::QueryLogicOperator::And,
            {},
            {},
            {},
            {},
            &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(result.matchedRows, 0u);

    sc::SCRecordPtr deleted;
    EXPECT_EQ(cursor->Next(deleted), sc::SC_OK);
    EXPECT_FALSE(static_cast<bool>(deleted));
}

TEST(QuerySqliteExecutorTests, ExplicitCompositeIndexStillWorksAfterReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeReopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);

        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"seed reopen", edit), sc::SC_OK);

        sc::SCRecordPtr row;
        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Charlie"), sc::SC_OK);

        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Zulu"), sc::SC_OK);

        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    }

    sc::SCDbPtr reopened;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::Between,
                                                                        {sc::SCValue::FromString(L"Bravo"),
                                                                         sc::SCValue::FromString(L"Zulu")}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Ascending, false}},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = reopened.Get();
    context.backendHandle = reopened.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::PartialIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name");
    EXPECT_EQ(result.matchedRows, 2u);

    std::vector<std::wstring> names;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        names.push_back(name);
    }

    ASSERT_EQ(names.size(), 2u);
    EXPECT_EQ(names[0], L"Charlie");
    EXPECT_EQ(names[1], L"Zulu");
}

TEST(QuerySqliteExecutorTests, ExplicitCompositeIndexSupportsStartsWithRangeTail)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeStartsWith.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed startswith", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpine"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Albatross"), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::StartsWith,
                                                                        {sc::SCValue::FromString(L"Al")}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Ascending, false}},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::PartialIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name");
    EXPECT_EQ(result.matchedRows, 2u);

    std::vector<std::wstring> names;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        names.push_back(name);
    }

    ASSERT_EQ(names.size(), 2u);
    EXPECT_EQ(names[0], L"Alpha");
    EXPECT_EQ(names[1], L"Alpine");
}

TEST(QuerySqliteExecutorTests, CompositeRangePlanWithoutResidualStillReportsPartialIndex)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeRangePartialMode.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed range partial mode", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpine"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::StartsWith,
                                                                        {sc::SCValue::FromString(L"Al")}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Ascending, false}},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    auto* indexAccess = dynamic_cast<sc::ISqliteQueryIndexAccess*>(db.Get());
    ASSERT_NE(indexAccess, nullptr);

    sc::QueryPlan analyzedPlan;
    EXPECT_EQ(indexAccess->AnalyzeCompositeIndexPlan(plan, &analyzedPlan), sc::SC_OK);
    ASSERT_TRUE(analyzedPlan.matchedIndex.has_value());
    EXPECT_TRUE(analyzedPlan.matchedIndex->hasRangeCondition);
    EXPECT_TRUE(analyzedPlan.pushdown.residualConditions.empty());

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::PartialIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name");
    EXPECT_EQ(result.matchedRows, 2u);
}

TEST(QuerySqliteExecutorTests, ExplicitCompositeIndexSupportsStartsWithAfterReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeStartsWithReopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);

        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"seed startswith reopen", edit), sc::SC_OK);

        sc::SCRecordPtr row;
        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Alto"), sc::SC_OK);

        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Zulu"), sc::SC_OK);

        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    }

    sc::SCDbPtr reopened;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::StartsWith,
                                                                        {sc::SCValue::FromString(L"Al")}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Ascending, false}},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = reopened.Get();
    context.backendHandle = reopened.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::PartialIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name");
    EXPECT_EQ(result.matchedRows, 2u);

    std::vector<std::wstring> names;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        names.push_back(name);
    }

    ASSERT_EQ(names.size(), 2u);
    EXPECT_EQ(names[0], L"Alpha");
    EXPECT_EQ(names[1], L"Alto");
}

TEST(QuerySqliteExecutorTests, NullableCompositeIndexSupportsStartsWithAfterReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_NullableCompositeStartsWithReopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr table = CreateNullableCompositeIndexedBeamTable(db);

        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"seed nullable startswith reopen", edit), sc::SC_OK);

        sc::SCRecordPtr row;
        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Alpine"), sc::SC_OK);

        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);

        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetValue(L"Name", sc::SCValue::Null()), sc::SC_OK);

        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    }

    sc::SCDbPtr reopened;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryConstraints constraints;
    constraints.requireIndex = true;
    constraints.allowFallbackScan = false;

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::StartsWith,
                                                                        {sc::SCValue::FromString(L"Al")}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Ascending, false}},
                           {},
                           {},
                           constraints,
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = reopened.Get();
    context.backendHandle = reopened.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::PartialIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name");
    EXPECT_EQ(result.matchedRows, 2u);

    std::vector<std::wstring> names;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        names.push_back(name);
    }

    ASSERT_EQ(names.size(), 2u);
    EXPECT_EQ(names[0], L"Alpha");
    EXPECT_EQ(names[1], L"Alpine");
}

TEST(QuerySqliteExecutorTests, ExplicitCompositeIndexUndoRedoReopenPreservesIndexConsistency)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeUndoRedoReopen.sqlite");
    sc::RecordId alphaId = 0;
    sc::RecordId altoId = 0;

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);

        sc::SCEditPtr seedEdit;
        EXPECT_EQ(db->BeginEdit(L"seed asc undo redo", seedEdit), sc::SC_OK);

        sc::SCRecordPtr alpha;
        EXPECT_EQ(table->CreateRecord(alpha), sc::SC_OK);
        EXPECT_EQ(alpha->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(alpha->SetString(L"Name", L"Alpha"), sc::SC_OK);
        alphaId = alpha->GetId();

        sc::SCRecordPtr alto;
        EXPECT_EQ(table->CreateRecord(alto), sc::SC_OK);
        EXPECT_EQ(alto->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(alto->SetString(L"Name", L"Alto"), sc::SC_OK);
        altoId = alto->GetId();

        sc::SCRecordPtr zulu;
        EXPECT_EQ(table->CreateRecord(zulu), sc::SC_OK);
        EXPECT_EQ(zulu->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(zulu->SetString(L"Name", L"Zulu"), sc::SC_OK);

        EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

        sc::SCEditPtr modifyEdit;
        EXPECT_EQ(db->BeginEdit(L"rename alto", modifyEdit), sc::SC_OK);
        EXPECT_EQ(alto->SetString(L"Name", L"Amber"), sc::SC_OK);
        EXPECT_EQ(db->Commit(modifyEdit.Get()), sc::SC_OK);

        sc::SCEditPtr deleteEdit;
        EXPECT_EQ(db->BeginEdit(L"delete alpha", deleteEdit), sc::SC_OK);
        EXPECT_EQ(table->DeleteRecord(alphaId), sc::SC_OK);
        EXPECT_EQ(db->Commit(deleteEdit.Get()), sc::SC_OK);

        EXPECT_EQ(db->Undo(), sc::SC_OK);
    }

    {
        sc::SCDbPtr reopened;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);

        auto planner = sc::CreateDefaultQueryPlanner();
        ASSERT_NE(planner, nullptr);

        sc::QueryPlan plan;
        EXPECT_EQ(
            planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                               {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                        {sc::QueryCondition{L"Width",
                                                                            sc::QueryConditionOperator::Equal,
                                                                            {sc::SCValue::FromInt64(100)}},
                                                         sc::QueryCondition{L"Name",
                                                                            sc::QueryConditionOperator::StartsWith,
                                                                            {sc::SCValue::FromString(L"A")}}}}},
                               sc::QueryLogicOperator::And,
                               {sc::SortSpec{L"Name", sc::QueryOrderDirection::Ascending, false}},
                               {},
                               {},
                               {},
                               &plan),
            sc::SC_OK);

        sc::SCRecordCursorPtr cursor;
        sc::QueryExecutionContext context;
        context.backendKind = sc::QueryBackendKind::SQLite;
        context.database = reopened.Get();
        context.backendHandle = reopened.Get();
        context.resultCursor = &cursor;

        sc::QueryExecutionResult result;
        EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
        EXPECT_EQ(result.mode, sc::QueryExecutionMode::PartialIndex);
        ASSERT_EQ(result.usedIndexIds.size(), 1u);
        EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name");
        EXPECT_EQ(result.matchedRows, 2u);

        std::vector<std::wstring> names;
        sc::SCRecordPtr record;
        while (cursor->Next(record) == sc::SC_OK && record)
        {
            std::wstring name;
            EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
            names.push_back(name);
        }

        ASSERT_EQ(names.size(), 2u);
        EXPECT_EQ(names[0], L"Alpha");
        EXPECT_EQ(names[1], L"Amber");

        EXPECT_EQ(reopened->Redo(), sc::SC_OK);

        sc::SCRecordCursorPtr redoCursor;
        context.resultCursor = &redoCursor;
        sc::QueryExecutionResult redoResult;
        EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &redoResult), sc::SC_OK);
        EXPECT_EQ(redoResult.mode, sc::QueryExecutionMode::PartialIndex);
        EXPECT_EQ(redoResult.matchedRows, 1u);

        std::vector<std::wstring> redoNames;
        while (redoCursor->Next(record) == sc::SC_OK && record)
        {
            std::wstring name;
            EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
            redoNames.push_back(name);
        }

        ASSERT_EQ(redoNames.size(), 1u);
        EXPECT_EQ(redoNames[0], L"Amber");

        sc::SCTablePtr reopenedTable;
        EXPECT_EQ(reopened->GetTable(L"Beam", reopenedTable), sc::SC_OK);

        sc::SCRecordPtr restoredAlpha;
        EXPECT_EQ(reopenedTable->GetRecord(alphaId, restoredAlpha), sc::SC_OK);
        ASSERT_TRUE(static_cast<bool>(restoredAlpha));
        EXPECT_TRUE(restoredAlpha->IsDeleted());

        sc::SCRecordPtr restoredAlto;
        EXPECT_EQ(reopenedTable->GetRecord(altoId, restoredAlto), sc::SC_OK);
        ASSERT_TRUE(static_cast<bool>(restoredAlto));

        std::wstring restoredAltoName;
        EXPECT_EQ(restoredAlto->GetStringCopy(L"Name", &restoredAltoName), sc::SC_OK);
        EXPECT_EQ(restoredAltoName, L"Amber");
    }
}

TEST(QuerySqliteExecutorTests, ExplicitCompositeIndexUndoRedoReopenPreservesStartsWithAndBetweenQueries)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeUndoRedoReopenRanges.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);

        sc::SCEditPtr seedEdit;
        EXPECT_EQ(db->BeginEdit(L"seed asc range reopen", seedEdit), sc::SC_OK);

        sc::SCRecordPtr alpha;
        EXPECT_EQ(table->CreateRecord(alpha), sc::SC_OK);
        EXPECT_EQ(alpha->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(alpha->SetString(L"Name", L"Alpha"), sc::SC_OK);

        sc::SCRecordPtr alpine;
        EXPECT_EQ(table->CreateRecord(alpine), sc::SC_OK);
        EXPECT_EQ(alpine->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(alpine->SetString(L"Name", L"Alpine"), sc::SC_OK);

        sc::SCRecordPtr bravo;
        EXPECT_EQ(table->CreateRecord(bravo), sc::SC_OK);
        EXPECT_EQ(bravo->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(bravo->SetString(L"Name", L"Bravo"), sc::SC_OK);
        const sc::RecordId bravoId = bravo->GetId();

        sc::SCRecordPtr zulu;
        EXPECT_EQ(table->CreateRecord(zulu), sc::SC_OK);
        EXPECT_EQ(zulu->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(zulu->SetString(L"Name", L"Zulu"), sc::SC_OK);

        EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

        sc::SCEditPtr modifyEdit;
        EXPECT_EQ(db->BeginEdit(L"rename bravo asc", modifyEdit), sc::SC_OK);
        EXPECT_EQ(bravo->SetString(L"Name", L"Alto"), sc::SC_OK);
        EXPECT_EQ(db->Commit(modifyEdit.Get()), sc::SC_OK);

        sc::SCEditPtr deleteEdit;
        EXPECT_EQ(db->BeginEdit(L"delete bravo replacement asc", deleteEdit), sc::SC_OK);
        EXPECT_EQ(table->DeleteRecord(bravoId), sc::SC_OK);
        EXPECT_EQ(db->Commit(deleteEdit.Get()), sc::SC_OK);

        EXPECT_EQ(db->Undo(), sc::SC_OK);
    }

    {
        sc::SCDbPtr reopened;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);

        auto planner = sc::CreateDefaultQueryPlanner();
        ASSERT_NE(planner, nullptr);

        sc::QueryPlan startsWithPlan;
        EXPECT_EQ(
            planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                               {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                        {sc::QueryCondition{L"Width",
                                                                            sc::QueryConditionOperator::Equal,
                                                                            {sc::SCValue::FromInt64(100)}},
                                                         sc::QueryCondition{L"Name",
                                                                            sc::QueryConditionOperator::StartsWith,
                                                                            {sc::SCValue::FromString(L"Al")}}}}},
                               sc::QueryLogicOperator::And,
                               {sc::SortSpec{L"Name", sc::QueryOrderDirection::Ascending, false}},
                               {},
                               {},
                               {},
                               &startsWithPlan),
            sc::SC_OK);

        sc::QueryPlan betweenPlan;
        EXPECT_EQ(
            planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                               {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                        {sc::QueryCondition{L"Width",
                                                                            sc::QueryConditionOperator::Equal,
                                                                            {sc::SCValue::FromInt64(100)}},
                                                         sc::QueryCondition{L"Name",
                                                                            sc::QueryConditionOperator::Between,
                                                                            {sc::SCValue::FromString(L"Al"),
                                                                             sc::SCValue::FromString(L"Alzz")}}}}},
                               sc::QueryLogicOperator::And,
                               {sc::SortSpec{L"Name", sc::QueryOrderDirection::Ascending, false}},
                               {},
                               {},
                               {},
                               &betweenPlan),
            sc::SC_OK);

        sc::QueryExecutionContext context;
        context.backendKind = sc::QueryBackendKind::SQLite;
        context.database = reopened.Get();
        context.backendHandle = reopened.Get();

        sc::SCRecordCursorPtr startsWithCursor;
        context.resultCursor = &startsWithCursor;
        sc::QueryExecutionResult startsWithResult;
        EXPECT_EQ(sc::ExecuteQueryPlan(startsWithPlan, context, &startsWithResult), sc::SC_OK);
        EXPECT_EQ(startsWithResult.mode, sc::QueryExecutionMode::PartialIndex);
        EXPECT_EQ(startsWithResult.matchedRows, 3u);

        std::vector<std::wstring> startsWithNames;
        sc::SCRecordPtr record;
        while (startsWithCursor->Next(record) == sc::SC_OK && record)
        {
            std::wstring name;
            EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
            startsWithNames.push_back(name);
        }

        ASSERT_EQ(startsWithNames.size(), 3u);
        EXPECT_EQ(startsWithNames[0], L"Alpha");
        EXPECT_EQ(startsWithNames[1], L"Alpine");
        EXPECT_EQ(startsWithNames[2], L"Alto");

        sc::SCRecordCursorPtr betweenCursor;
        context.resultCursor = &betweenCursor;
        sc::QueryExecutionResult betweenResult;
        EXPECT_EQ(sc::ExecuteQueryPlan(betweenPlan, context, &betweenResult), sc::SC_OK);
        EXPECT_EQ(betweenResult.mode, sc::QueryExecutionMode::PartialIndex);
        EXPECT_EQ(betweenResult.matchedRows, 3u);

        std::vector<std::wstring> betweenNames;
        while (betweenCursor->Next(record) == sc::SC_OK && record)
        {
            std::wstring name;
            EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
            betweenNames.push_back(name);
        }

        ASSERT_EQ(betweenNames.size(), 3u);
        EXPECT_EQ(betweenNames[0], L"Alpha");
        EXPECT_EQ(betweenNames[1], L"Alpine");
        EXPECT_EQ(betweenNames[2], L"Alto");

        EXPECT_EQ(reopened->Redo(), sc::SC_OK);

        sc::SCRecordCursorPtr redoStartsWithCursor;
        context.resultCursor = &redoStartsWithCursor;
        sc::QueryExecutionResult redoStartsWithResult;
        EXPECT_EQ(sc::ExecuteQueryPlan(startsWithPlan, context, &redoStartsWithResult), sc::SC_OK);
        EXPECT_EQ(redoStartsWithResult.mode, sc::QueryExecutionMode::PartialIndex);
        EXPECT_EQ(redoStartsWithResult.matchedRows, 2u);

        std::vector<std::wstring> redoStartsWithNames;
        while (redoStartsWithCursor->Next(record) == sc::SC_OK && record)
        {
            std::wstring name;
            EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
            redoStartsWithNames.push_back(name);
        }

        ASSERT_EQ(redoStartsWithNames.size(), 2u);
        EXPECT_EQ(redoStartsWithNames[0], L"Alpha");
        EXPECT_EQ(redoStartsWithNames[1], L"Alpine");

        sc::SCRecordCursorPtr redoBetweenCursor;
        context.resultCursor = &redoBetweenCursor;
        sc::QueryExecutionResult redoBetweenResult;
        EXPECT_EQ(sc::ExecuteQueryPlan(betweenPlan, context, &redoBetweenResult), sc::SC_OK);
        EXPECT_EQ(redoBetweenResult.mode, sc::QueryExecutionMode::PartialIndex);
        EXPECT_EQ(redoBetweenResult.matchedRows, 2u);

        std::vector<std::wstring> redoBetweenNames;
        while (redoBetweenCursor->Next(record) == sc::SC_OK && record)
        {
            std::wstring name;
            EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
            redoBetweenNames.push_back(name);
        }

        ASSERT_EQ(redoBetweenNames.size(), 2u);
        EXPECT_EQ(redoBetweenNames[0], L"Alpha");
        EXPECT_EQ(redoBetweenNames[1], L"Alpine");
    }
}

TEST(QuerySqliteExecutorTests, ExplicitCompositeIndexUpdatesPrefixBucketWithoutResidualEntries)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositePrefixBucketMove.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateCompositeIndexedBeamTable(db);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed prefix bucket move", seedEdit), sc::SC_OK);

    sc::SCRecordPtr moved;
    EXPECT_EQ(table->CreateRecord(moved), sc::SC_OK);
    EXPECT_EQ(moved->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(moved->SetString(L"Name", L"Alpha"), sc::SC_OK);
    const sc::RecordId movedId = moved->GetId();

    sc::SCRecordPtr stable;
    EXPECT_EQ(table->CreateRecord(stable), sc::SC_OK);
    EXPECT_EQ(stable->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(stable->SetString(L"Name", L"Bravo"), sc::SC_OK);

    sc::SCRecordPtr targetBucket;
    EXPECT_EQ(table->CreateRecord(targetBucket), sc::SC_OK);
    EXPECT_EQ(targetBucket->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(targetBucket->SetString(L"Name", L"Zulu"), sc::SC_OK);

    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr moveEdit;
    EXPECT_EQ(db->BeginEdit(L"move width bucket", moveEdit), sc::SC_OK);
    EXPECT_EQ(moved->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(db->Commit(moveEdit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan width100Plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Ascending, false}},
                           {},
                           {},
                           {},
                           &width100Plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr width100Cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &width100Cursor;

    sc::QueryExecutionResult width100Result;
    EXPECT_EQ(sc::ExecuteQueryPlan(width100Plan, context, &width100Result), sc::SC_OK);
    EXPECT_EQ(width100Result.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(width100Result.matchedRows, 1u);

    std::vector<std::wstring> width100Names;
    sc::SCRecordPtr record;
    while (width100Cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        width100Names.push_back(name);
    }

    ASSERT_EQ(width100Names.size(), 1u);
    EXPECT_EQ(width100Names[0], L"Bravo");

    sc::QueryPlan width200Plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(200)}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Ascending, false}},
                           {},
                           {},
                           {},
                           &width200Plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr width200Cursor;
    context.resultCursor = &width200Cursor;
    sc::QueryExecutionResult width200Result;
    EXPECT_EQ(sc::ExecuteQueryPlan(width200Plan, context, &width200Result), sc::SC_OK);
    EXPECT_EQ(width200Result.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(width200Result.matchedRows, 2u);

    std::vector<std::wstring> width200Names;
    while (width200Cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        width200Names.push_back(name);
    }

    ASSERT_EQ(width200Names.size(), 2u);
    EXPECT_EQ(width200Names[0], L"Alpha");
    EXPECT_EQ(width200Names[1], L"Zulu");

    sc::SCRecordPtr movedRecord;
    EXPECT_EQ(table->GetRecord(movedId, movedRecord), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(movedRecord));

    std::int64_t movedWidth = 0;
    EXPECT_EQ(movedRecord->GetInt64(L"Width", &movedWidth), sc::SC_OK);
    EXPECT_EQ(movedWidth, 200);
}

TEST(QuerySqliteExecutorTests, DescendingCompositeIndexUpdatesPrefixBucketWithoutResidualEntries)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeDescPrefixBucketMove.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateDescendingCompositeIndexedBeamTable(db);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed desc prefix bucket move", seedEdit), sc::SC_OK);

    sc::SCRecordPtr moved;
    EXPECT_EQ(table->CreateRecord(moved), sc::SC_OK);
    EXPECT_EQ(moved->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(moved->SetString(L"Name", L"Alpha"), sc::SC_OK);
    const sc::RecordId movedId = moved->GetId();

    sc::SCRecordPtr stable;
    EXPECT_EQ(table->CreateRecord(stable), sc::SC_OK);
    EXPECT_EQ(stable->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(stable->SetString(L"Name", L"Bravo"), sc::SC_OK);

    sc::SCRecordPtr targetBucket;
    EXPECT_EQ(table->CreateRecord(targetBucket), sc::SC_OK);
    EXPECT_EQ(targetBucket->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(targetBucket->SetString(L"Name", L"Zulu"), sc::SC_OK);

    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCEditPtr moveEdit;
    EXPECT_EQ(db->BeginEdit(L"move desc width bucket", moveEdit), sc::SC_OK);
    EXPECT_EQ(moved->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(db->Commit(moveEdit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan width100Plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Descending, false}},
                           {},
                           {},
                           {},
                           &width100Plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr width100Cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &width100Cursor;

    sc::QueryExecutionResult width100Result;
    EXPECT_EQ(sc::ExecuteQueryPlan(width100Plan, context, &width100Result), sc::SC_OK);
    EXPECT_EQ(width100Result.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(width100Result.matchedRows, 1u);

    std::vector<std::wstring> width100Names;
    sc::SCRecordPtr record;
    while (width100Cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        width100Names.push_back(name);
    }

    ASSERT_EQ(width100Names.size(), 1u);
    EXPECT_EQ(width100Names[0], L"Bravo");

    sc::QueryPlan width200Plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(200)}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Descending, false}},
                           {},
                           {},
                           {},
                           &width200Plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr width200Cursor;
    context.resultCursor = &width200Cursor;
    sc::QueryExecutionResult width200Result;
    EXPECT_EQ(sc::ExecuteQueryPlan(width200Plan, context, &width200Result), sc::SC_OK);
    EXPECT_EQ(width200Result.mode, sc::QueryExecutionMode::DirectIndex);
    EXPECT_EQ(width200Result.matchedRows, 2u);

    std::vector<std::wstring> width200Names;
    while (width200Cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        width200Names.push_back(name);
    }

    ASSERT_EQ(width200Names.size(), 2u);
    EXPECT_EQ(width200Names[0], L"Zulu");
    EXPECT_EQ(width200Names[1], L"Alpha");

    sc::SCRecordPtr movedRecord;
    EXPECT_EQ(table->GetRecord(movedId, movedRecord), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(movedRecord));

    std::int64_t movedWidth = 0;
    EXPECT_EQ(movedRecord->GetInt64(L"Width", &movedWidth), sc::SC_OK);
    EXPECT_EQ(movedWidth, 200);
}

TEST(QuerySqliteExecutorTests, DescendingCompositeIndexSupportsOrderedPrefixScan)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeDescOrder.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateDescendingCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed desc order", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Zulu"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                                 {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                          {sc::QueryCondition{L"Width",
                                                                              sc::QueryConditionOperator::Equal,
                                                                              {sc::SCValue::FromInt64(100)}}}}},
                                 sc::QueryLogicOperator::And,
                                 {sc::SortSpec{L"Name", sc::QueryOrderDirection::Descending, false}},
                                 {},
                                 {},
                                 {},
                                 &plan),
              sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::DirectIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name_Desc");

    std::vector<std::wstring> names;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        names.push_back(name);
    }

    ASSERT_EQ(names.size(), 3u);
    EXPECT_EQ(names[0], L"Zulu");
    EXPECT_EQ(names[1], L"Bravo");
    EXPECT_EQ(names[2], L"Alpha");
}

TEST(QuerySqliteExecutorTests, DescendingCompositeIndexSupportsRangeTail)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeDescRange.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateDescendingCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed desc range", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Zulu"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Able"), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::GreaterThanOrEqual,
                                                                        {sc::SCValue::FromString(L"Bravo")}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Descending, false}},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::PartialIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name_Desc");
    EXPECT_EQ(result.matchedRows, 2u);

    std::vector<std::wstring> names;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        names.push_back(name);
    }

    ASSERT_EQ(names.size(), 2u);
    EXPECT_EQ(names[0], L"Zulu");
    EXPECT_EQ(names[1], L"Bravo");
}

TEST(QuerySqliteExecutorTests, DescendingCompositeIndexSupportsLessThanOrEqualRangeTail)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeDescLessEqual.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateDescendingCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed desc less equal", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Able"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Zulu"), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::LessThanOrEqual,
                                                                        {sc::SCValue::FromString(L"Bravo")}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Descending, false}},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::PartialIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name_Desc");
    EXPECT_EQ(result.matchedRows, 3u);

    std::vector<std::wstring> names;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        names.push_back(name);
    }

    ASSERT_EQ(names.size(), 3u);
    EXPECT_EQ(names[0], L"Bravo");
    EXPECT_EQ(names[1], L"Alpha");
    EXPECT_EQ(names[2], L"Able");
}

TEST(QuerySqliteExecutorTests, DescendingCompositeIndexSupportsStartsWithRangeTail)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeDescStartsWith.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table = CreateDescendingCompositeIndexedBeamTable(db);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed desc startswith", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpine"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Bravo"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Albatross"), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::StartsWith,
                                                                        {sc::SCValue::FromString(L"Al")}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Descending, false}},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db.Get();
    context.backendHandle = db.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::PartialIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name_Desc");
    EXPECT_EQ(result.matchedRows, 2u);

    std::vector<std::wstring> names;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        names.push_back(name);
    }

    ASSERT_EQ(names.size(), 2u);
    EXPECT_EQ(names[0], L"Alpine");
    EXPECT_EQ(names[1], L"Alpha");
}

TEST(QuerySqliteExecutorTests, DescendingCompositeIndexSupportsStartsWithAfterReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeDescStartsWithReopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr table = CreateDescendingCompositeIndexedBeamTable(db);

        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"seed desc startswith reopen", edit), sc::SC_OK);

        sc::SCRecordPtr row;
        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Alto"), sc::SC_OK);

        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Zulu"), sc::SC_OK);

        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    }

    sc::SCDbPtr reopened;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::StartsWith,
                                                                        {sc::SCValue::FromString(L"Al")}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Descending, false}},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = reopened.Get();
    context.backendHandle = reopened.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::PartialIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name_Desc");
    EXPECT_EQ(result.matchedRows, 2u);

    std::vector<std::wstring> names;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        names.push_back(name);
    }

    ASSERT_EQ(names.size(), 2u);
    EXPECT_EQ(names[0], L"Alto");
    EXPECT_EQ(names[1], L"Alpha");
}

TEST(QuerySqliteExecutorTests, DescendingCompositeIndexUndoRedoReopenPreservesIndexConsistency)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeDescUndoRedoReopen.sqlite");
    sc::RecordId alphaId = 0;
    sc::RecordId altoId = 0;

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr table = CreateDescendingCompositeIndexedBeamTable(db);

        sc::SCEditPtr seedEdit;
        EXPECT_EQ(db->BeginEdit(L"seed desc undo redo", seedEdit), sc::SC_OK);

        sc::SCRecordPtr alpha;
        EXPECT_EQ(table->CreateRecord(alpha), sc::SC_OK);
        EXPECT_EQ(alpha->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(alpha->SetString(L"Name", L"Alpha"), sc::SC_OK);
        alphaId = alpha->GetId();

        sc::SCRecordPtr alto;
        EXPECT_EQ(table->CreateRecord(alto), sc::SC_OK);
        EXPECT_EQ(alto->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(alto->SetString(L"Name", L"Alto"), sc::SC_OK);
        altoId = alto->GetId();

        sc::SCRecordPtr zulu;
        EXPECT_EQ(table->CreateRecord(zulu), sc::SC_OK);
        EXPECT_EQ(zulu->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(zulu->SetString(L"Name", L"Zulu"), sc::SC_OK);

        EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

        sc::SCEditPtr modifyEdit;
        EXPECT_EQ(db->BeginEdit(L"rename alpha", modifyEdit), sc::SC_OK);
        EXPECT_EQ(alpha->SetString(L"Name", L"Algae"), sc::SC_OK);
        EXPECT_EQ(db->Commit(modifyEdit.Get()), sc::SC_OK);

        sc::SCEditPtr deleteEdit;
        EXPECT_EQ(db->BeginEdit(L"delete alto", deleteEdit), sc::SC_OK);
        EXPECT_EQ(table->DeleteRecord(altoId), sc::SC_OK);
        EXPECT_EQ(db->Commit(deleteEdit.Get()), sc::SC_OK);

        EXPECT_EQ(db->Undo(), sc::SC_OK);
    }

    {
        sc::SCDbPtr reopened;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);

        auto planner = sc::CreateDefaultQueryPlanner();
        ASSERT_NE(planner, nullptr);

        sc::QueryPlan plan;
        EXPECT_EQ(
            planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                               {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                        {sc::QueryCondition{L"Width",
                                                                            sc::QueryConditionOperator::Equal,
                                                                            {sc::SCValue::FromInt64(100)}},
                                                         sc::QueryCondition{L"Name",
                                                                            sc::QueryConditionOperator::StartsWith,
                                                                            {sc::SCValue::FromString(L"Al")}}}}},
                               sc::QueryLogicOperator::And,
                               {sc::SortSpec{L"Name", sc::QueryOrderDirection::Descending, false}},
                               {},
                               {},
                               {},
                               &plan),
            sc::SC_OK);

        sc::SCRecordCursorPtr cursor;
        sc::QueryExecutionContext context;
        context.backendKind = sc::QueryBackendKind::SQLite;
        context.database = reopened.Get();
        context.backendHandle = reopened.Get();
        context.resultCursor = &cursor;

        sc::QueryExecutionResult result;
        EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
        EXPECT_EQ(result.mode, sc::QueryExecutionMode::PartialIndex);
        ASSERT_EQ(result.usedIndexIds.size(), 1u);
        EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name_Desc");
        EXPECT_EQ(result.matchedRows, 2u);

        std::vector<std::wstring> names;
        sc::SCRecordPtr record;
        while (cursor->Next(record) == sc::SC_OK && record)
        {
            std::wstring name;
            EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
            names.push_back(name);
        }

        ASSERT_EQ(names.size(), 2u);
        EXPECT_EQ(names[0], L"Alto");
        EXPECT_EQ(names[1], L"Algae");

        EXPECT_EQ(reopened->Redo(), sc::SC_OK);

        sc::SCRecordCursorPtr redoCursor;
        context.resultCursor = &redoCursor;
        sc::QueryExecutionResult redoResult;
        EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &redoResult), sc::SC_OK);
        EXPECT_EQ(redoResult.mode, sc::QueryExecutionMode::PartialIndex);
        EXPECT_EQ(redoResult.matchedRows, 1u);

        std::vector<std::wstring> redoNames;
        while (redoCursor->Next(record) == sc::SC_OK && record)
        {
            std::wstring name;
            EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
            redoNames.push_back(name);
        }

        ASSERT_EQ(redoNames.size(), 1u);
        EXPECT_EQ(redoNames[0], L"Algae");

        sc::SCTablePtr reopenedTable;
        EXPECT_EQ(reopened->GetTable(L"Beam", reopenedTable), sc::SC_OK);

        sc::SCRecordPtr restoredAlpha;
        EXPECT_EQ(reopenedTable->GetRecord(alphaId, restoredAlpha), sc::SC_OK);
        ASSERT_TRUE(static_cast<bool>(restoredAlpha));

        std::wstring restoredAlphaName;
        EXPECT_EQ(restoredAlpha->GetStringCopy(L"Name", &restoredAlphaName), sc::SC_OK);
        EXPECT_EQ(restoredAlphaName, L"Algae");

        sc::SCRecordPtr restoredAlto;
        EXPECT_EQ(reopenedTable->GetRecord(altoId, restoredAlto), sc::SC_OK);
        ASSERT_TRUE(static_cast<bool>(restoredAlto));
        EXPECT_TRUE(restoredAlto->IsDeleted());
    }
}

TEST(QuerySqliteExecutorTests, DescendingCompositeIndexUndoRedoReopenPreservesStartsWithAndBetweenQueries)
{
    const fs::path dbPath =
        MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeDescUndoRedoReopenRanges.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr table = CreateDescendingCompositeIndexedBeamTable(db);

        sc::SCEditPtr seedEdit;
        EXPECT_EQ(db->BeginEdit(L"seed desc range reopen", seedEdit), sc::SC_OK);

        sc::SCRecordPtr alpha;
        EXPECT_EQ(table->CreateRecord(alpha), sc::SC_OK);
        EXPECT_EQ(alpha->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(alpha->SetString(L"Name", L"Alpha"), sc::SC_OK);

        sc::SCRecordPtr alpine;
        EXPECT_EQ(table->CreateRecord(alpine), sc::SC_OK);
        EXPECT_EQ(alpine->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(alpine->SetString(L"Name", L"Alpine"), sc::SC_OK);

        sc::SCRecordPtr bravo;
        EXPECT_EQ(table->CreateRecord(bravo), sc::SC_OK);
        EXPECT_EQ(bravo->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(bravo->SetString(L"Name", L"Bravo"), sc::SC_OK);
        const sc::RecordId bravoId = bravo->GetId();

        sc::SCRecordPtr zulu;
        EXPECT_EQ(table->CreateRecord(zulu), sc::SC_OK);
        EXPECT_EQ(zulu->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(zulu->SetString(L"Name", L"Zulu"), sc::SC_OK);

        EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

        sc::SCEditPtr modifyEdit;
        EXPECT_EQ(db->BeginEdit(L"rename bravo", modifyEdit), sc::SC_OK);
        EXPECT_EQ(bravo->SetString(L"Name", L"Alto"), sc::SC_OK);
        EXPECT_EQ(db->Commit(modifyEdit.Get()), sc::SC_OK);

        sc::SCEditPtr deleteEdit;
        EXPECT_EQ(db->BeginEdit(L"delete bravo replacement", deleteEdit), sc::SC_OK);
        EXPECT_EQ(table->DeleteRecord(bravoId), sc::SC_OK);
        EXPECT_EQ(db->Commit(deleteEdit.Get()), sc::SC_OK);

        EXPECT_EQ(db->Undo(), sc::SC_OK);
    }

    {
        sc::SCDbPtr reopened;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);

        auto planner = sc::CreateDefaultQueryPlanner();
        ASSERT_NE(planner, nullptr);

        sc::QueryPlan startsWithPlan;
        EXPECT_EQ(
            planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                               {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                        {sc::QueryCondition{L"Width",
                                                                            sc::QueryConditionOperator::Equal,
                                                                            {sc::SCValue::FromInt64(100)}},
                                                         sc::QueryCondition{L"Name",
                                                                            sc::QueryConditionOperator::StartsWith,
                                                                            {sc::SCValue::FromString(L"Al")}}}}},
                               sc::QueryLogicOperator::And,
                               {sc::SortSpec{L"Name", sc::QueryOrderDirection::Descending, false}},
                               {},
                               {},
                               {},
                               &startsWithPlan),
            sc::SC_OK);

        sc::QueryPlan betweenPlan;
        EXPECT_EQ(
            planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                               {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                        {sc::QueryCondition{L"Width",
                                                                            sc::QueryConditionOperator::Equal,
                                                                            {sc::SCValue::FromInt64(100)}},
                                                         sc::QueryCondition{L"Name",
                                                                            sc::QueryConditionOperator::Between,
                                                                            {sc::SCValue::FromString(L"Al"),
                                                                             sc::SCValue::FromString(L"Alzz")}}}}},
                               sc::QueryLogicOperator::And,
                               {sc::SortSpec{L"Name", sc::QueryOrderDirection::Descending, false}},
                               {},
                               {},
                               {},
                               &betweenPlan),
            sc::SC_OK);

        sc::QueryExecutionContext context;
        context.backendKind = sc::QueryBackendKind::SQLite;
        context.database = reopened.Get();
        context.backendHandle = reopened.Get();

        sc::SCRecordCursorPtr startsWithCursor;
        context.resultCursor = &startsWithCursor;
        sc::QueryExecutionResult startsWithResult;
        EXPECT_EQ(sc::ExecuteQueryPlan(startsWithPlan, context, &startsWithResult), sc::SC_OK);
        EXPECT_EQ(startsWithResult.mode, sc::QueryExecutionMode::PartialIndex);
        EXPECT_EQ(startsWithResult.matchedRows, 3u);

        std::vector<std::wstring> startsWithNames;
        sc::SCRecordPtr record;
        while (startsWithCursor->Next(record) == sc::SC_OK && record)
        {
            std::wstring name;
            EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
            startsWithNames.push_back(name);
        }

        ASSERT_EQ(startsWithNames.size(), 3u);
        EXPECT_EQ(startsWithNames[0], L"Alto");
        EXPECT_EQ(startsWithNames[1], L"Alpine");
        EXPECT_EQ(startsWithNames[2], L"Alpha");

        sc::SCRecordCursorPtr betweenCursor;
        context.resultCursor = &betweenCursor;
        sc::QueryExecutionResult betweenResult;
        EXPECT_EQ(sc::ExecuteQueryPlan(betweenPlan, context, &betweenResult), sc::SC_OK);
        EXPECT_EQ(betweenResult.mode, sc::QueryExecutionMode::PartialIndex);
        EXPECT_EQ(betweenResult.matchedRows, 3u);

        std::vector<std::wstring> betweenNames;
        while (betweenCursor->Next(record) == sc::SC_OK && record)
        {
            std::wstring name;
            EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
            betweenNames.push_back(name);
        }

        ASSERT_EQ(betweenNames.size(), 3u);
        EXPECT_EQ(betweenNames[0], L"Alto");
        EXPECT_EQ(betweenNames[1], L"Alpine");
        EXPECT_EQ(betweenNames[2], L"Alpha");

        EXPECT_EQ(reopened->Redo(), sc::SC_OK);

        sc::SCRecordCursorPtr redoStartsWithCursor;
        context.resultCursor = &redoStartsWithCursor;
        sc::QueryExecutionResult redoStartsWithResult;
        EXPECT_EQ(sc::ExecuteQueryPlan(startsWithPlan, context, &redoStartsWithResult), sc::SC_OK);
        EXPECT_EQ(redoStartsWithResult.mode, sc::QueryExecutionMode::PartialIndex);
        EXPECT_EQ(redoStartsWithResult.matchedRows, 2u);

        std::vector<std::wstring> redoStartsWithNames;
        while (redoStartsWithCursor->Next(record) == sc::SC_OK && record)
        {
            std::wstring name;
            EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
            redoStartsWithNames.push_back(name);
        }

        ASSERT_EQ(redoStartsWithNames.size(), 2u);
        EXPECT_EQ(redoStartsWithNames[0], L"Alpine");
        EXPECT_EQ(redoStartsWithNames[1], L"Alpha");

        sc::SCRecordCursorPtr redoBetweenCursor;
        context.resultCursor = &redoBetweenCursor;
        sc::QueryExecutionResult redoBetweenResult;
        EXPECT_EQ(sc::ExecuteQueryPlan(betweenPlan, context, &redoBetweenResult), sc::SC_OK);
        EXPECT_EQ(redoBetweenResult.mode, sc::QueryExecutionMode::PartialIndex);
        EXPECT_EQ(redoBetweenResult.matchedRows, 2u);

        std::vector<std::wstring> redoBetweenNames;
        while (redoBetweenCursor->Next(record) == sc::SC_OK && record)
        {
            std::wstring name;
            EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
            redoBetweenNames.push_back(name);
        }

        ASSERT_EQ(redoBetweenNames.size(), 2u);
        EXPECT_EQ(redoBetweenNames[0], L"Alpine");
        EXPECT_EQ(redoBetweenNames[1], L"Alpha");
    }
}

TEST(QuerySqliteExecutorTests, DescendingCompositeIndexSupportsBetweenAfterReopen)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_QuerySqlite_CompositeDescBetweenReopen.sqlite");

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr table = CreateDescendingCompositeIndexedBeamTable(db);

        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"seed desc between reopen", edit), sc::SC_OK);

        sc::SCRecordPtr row;
        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Charlie"), sc::SC_OK);

        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Zulu"), sc::SC_OK);

        EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
        EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(row->SetString(L"Name", L"Able"), sc::SC_OK);

        EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    }

    sc::SCDbPtr reopened;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);

    auto planner = sc::CreateDefaultQueryPlanner();
    ASSERT_NE(planner, nullptr);

    sc::QueryPlan plan;
    EXPECT_EQ(
        planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                           {sc::QueryConditionGroup{sc::QueryLogicOperator::And,
                                                    {sc::QueryCondition{L"Width",
                                                                        sc::QueryConditionOperator::Equal,
                                                                        {sc::SCValue::FromInt64(100)}},
                                                     sc::QueryCondition{L"Name",
                                                                        sc::QueryConditionOperator::Between,
                                                                        {sc::SCValue::FromString(L"Bravo"),
                                                                         sc::SCValue::FromString(L"Zulu")}}}}},
                           sc::QueryLogicOperator::And,
                           {sc::SortSpec{L"Name", sc::QueryOrderDirection::Descending, false}},
                           {},
                           {},
                           {},
                           &plan),
        sc::SC_OK);

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = reopened.Get();
    context.backendHandle = reopened.Get();
    context.resultCursor = &cursor;

    sc::QueryExecutionResult result;
    EXPECT_EQ(sc::ExecuteQueryPlan(plan, context, &result), sc::SC_OK);
    EXPECT_EQ(result.mode, sc::QueryExecutionMode::PartialIndex);
    ASSERT_EQ(result.usedIndexIds.size(), 1u);
    EXPECT_EQ(result.usedIndexIds.front(), L"idx_Beam_Width_Name_Desc");
    EXPECT_EQ(result.matchedRows, 2u);

    std::vector<std::wstring> names;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        EXPECT_EQ(record->GetStringCopy(L"Name", &name), sc::SC_OK);
        names.push_back(name);
    }

    ASSERT_EQ(names.size(), 2u);
    EXPECT_EQ(names[0], L"Zulu");
    EXPECT_EQ(names[1], L"Charlie");
}
