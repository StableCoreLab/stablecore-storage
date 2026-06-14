#include "TestQueryHelpers.h"
#include <gtest/gtest.h>

sc::ErrorCode ExecuteQueryForBeam(sc::ISCDatabase* db,
                                  std::vector<sc::QueryCondition> conditions,
                                  const sc::QueryConstraints& constraints,
                                  std::vector<std::wstring>* outNames,
                                  sc::QueryExecutionResult* outResult)
{
    if (db == nullptr || outNames == nullptr || outResult == nullptr)
    {
        return sc::SC_E_POINTER;
    }

    auto planner = sc::CreateDefaultQueryPlanner();
    if (planner == nullptr)
    {
        return sc::SC_E_FAIL;
    }

    sc::QueryPlan plan;
    const sc::ErrorCode planRc = planner->BuildPlan(sc::QueryTarget{L"Beam", sc::QueryTargetType::Table},
                                                    {sc::QueryConditionGroup{sc::QueryLogicOperator::And, std::move(conditions)}},
                                                    sc::QueryLogicOperator::And,
                                                    {sc::SortSpec{L"Name", sc::QueryOrderDirection::Ascending, false}},
                                                    {},
                                                    {},
                                                    constraints,
                                                    &plan);
    if (sc::Failed(planRc))
    {
        return planRc;
    }

    sc::SCRecordCursorPtr cursor;
    sc::QueryExecutionContext context;
    context.backendKind = sc::QueryBackendKind::SQLite;
    context.database = db;
    context.backendHandle = db;
    context.resultCursor = &cursor;

    const sc::ErrorCode execRc = sc::ExecuteQueryPlan(plan, context, outResult);
    if (sc::Failed(execRc))
    {
        return execRc;
    }

    outNames->clear();
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && record)
    {
        std::wstring name;
        const sc::ErrorCode nameRc = record->GetStringCopy(L"Name", &name);
        if (nameRc == sc::SC_OK)
        {
            outNames->push_back(name);
        }
        else if (nameRc == sc::SC_E_VALUE_IS_NULL)
        {
            outNames->push_back(L"<NULL>");
        }
        else
        {
            return nameRc;
        }
    }

    return sc::SC_OK;
}
