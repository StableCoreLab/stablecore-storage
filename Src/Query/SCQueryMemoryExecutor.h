#pragma once

#include "ISCQuery.h"

namespace StableCore::Storage
{

ErrorCode ExecuteMemoryQueryDispatch(
    const QueryPlan& plan,
    const QueryExecutionContext& context,
    QueryExecutionResult* outResult);

}  // namespace StableCore::Storage
