#pragma once

#include <vector>

#include "StableCore/Storage/Errors.h"
#include "StableCore/Storage/Types.h"

namespace stablecore::storage
{

struct MigrationStep
{
    std::int32_t fromVersion{0};
    std::int32_t toVersion{0};
    std::wstring name;
    std::wstring description;
};

struct MigrationPlan
{
    std::int32_t currentVersion{0};
    std::int32_t targetVersion{0};
    std::vector<MigrationStep> orderedSteps;
};

ErrorCode BuildMigrationPlan(
    std::int32_t currentVersion,
    std::int32_t targetVersion,
    const std::vector<MigrationStep>& availableSteps,
    MigrationPlan* outPlan);

}  // namespace stablecore::storage
