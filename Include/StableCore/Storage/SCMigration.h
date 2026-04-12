#pragma once

#include <vector>

#include "StableCore/Storage/SCErrors.h"
#include "StableCore/Storage/SCTypes.h"

namespace stablecore::storage
{

struct SCMigrationStep
{
    std::int32_t fromVersion{0};
    std::int32_t toVersion{0};
    std::wstring name;
    std::wstring description;
};

struct SCMigrationPlan
{
    std::int32_t currentVersion{0};
    std::int32_t targetVersion{0};
    std::vector<SCMigrationStep> orderedSteps;
};

ErrorCode BuildMigrationPlan(
    std::int32_t currentVersion,
    std::int32_t targetVersion,
    const std::vector<SCMigrationStep>& availableSteps,
    SCMigrationPlan* outPlan);

}  // namespace stablecore::storage
