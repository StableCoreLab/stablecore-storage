#pragma once

#include <vector>
#include <string>

#include "SCStorage.h"
#include "ISCQuery.h"

namespace sc = StableCore::Storage;

// Query execution helper function for test support
sc::ErrorCode ExecuteQueryForBeam(sc::ISCDatabase* db,
                                  std::vector<sc::QueryCondition> conditions,
                                  const sc::QueryConstraints& constraints,
                                  std::vector<std::wstring>* outNames,
                                  sc::QueryExecutionResult* outResult);
