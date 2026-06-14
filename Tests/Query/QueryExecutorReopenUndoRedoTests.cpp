#include <filesystem>

#include <gtest/gtest.h>

#include "ISCQuery.h"
#include "../Src/Query/SCQuerySqliteIndexAccess.h"
#include "SCStorage.h"

#include "Support/TestPaths.h"
#include "Support/TestSqliteHelpers.h"
#include "Support/TestSchemaBuilders.h"
#include "Support/TestSeedData.h"

namespace sc = StableCore::Storage;
namespace fs = std::filesystem;

TEST(QueryExecutorReopenUndoRedoTests, DescendingCompositeIndexThreeColumnUndoRedoReopenPreservesTailRangeAndOrderCoverage)
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

TEST(QueryExecutorReopenUndoRedoTests, ExplicitCompositeIndexStillWorksAfterReopen)
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

TEST(QueryExecutorReopenUndoRedoTests, ExplicitCompositeIndexUndoRedoReopenPreservesIndexConsistency)
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

TEST(QueryExecutorReopenUndoRedoTests, ExplicitCompositeIndexUndoRedoReopenPreservesStartsWithAndBetweenQueries)
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

TEST(QueryExecutorReopenUndoRedoTests, DescendingCompositeIndexUndoRedoReopenPreservesIndexConsistency)
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

TEST(QueryExecutorReopenUndoRedoTests, DescendingCompositeIndexUndoRedoReopenPreservesStartsWithAndBetweenQueries)
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
