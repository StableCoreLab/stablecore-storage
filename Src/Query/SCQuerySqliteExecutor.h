#pragma once

#include "ISCQuery.h"

namespace StableCore::Storage
{

ErrorCode ExecuteSqliteQueryDispatch(
    const QueryPlan& plan,
    const QueryExecutionContext& context,
    QueryExecutionResult* outResult);

}  // namespace StableCore::Storage
