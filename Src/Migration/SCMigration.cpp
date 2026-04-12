#include "StableCore/Storage/SCMigration.h"

#include <algorithm>

namespace StableCore::Storage
{

ErrorCode BuildMigrationPlan(
    std::int32_t currentVersion,
    std::int32_t targetVersion,
    const std::vector<SCMigrationStep>& availableSteps,
    SCMigrationPlan* outPlan)
{
    if (outPlan == nullptr)
    {
        return SC_E_POINTER;
    }
    if (currentVersion < 0 || targetVersion < currentVersion)
    {
        return SC_E_INVALIDARG;
    }

    SCMigrationPlan plan;
    plan.currentVersion = currentVersion;
    plan.targetVersion = targetVersion;

    std::int32_t cursor = currentVersion;
    while (cursor < targetVersion)
    {
        const auto stepIt = std::find_if(
            availableSteps.begin(),
            availableSteps.end(),
            [&](const SCMigrationStep& step)
            {
                return step.fromVersion == cursor;
            });
        if (stepIt == availableSteps.end())
        {
            return SC_E_RECORD_NOT_FOUND;
        }

        plan.orderedSteps.push_back(*stepIt);
        cursor = stepIt->toVersion;
    }

    if (cursor != targetVersion)
    {
        return SC_E_INVALIDARG;
    }

    *outPlan = std::move(plan);
    return SC_OK;
}

}  // namespace StableCore::Storage
