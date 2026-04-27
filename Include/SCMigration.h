#pragma once

#include <vector>

#include "SCErrors.h"
#include "SCTypes.h"

namespace StableCore::Storage
{

    enum class SCVersionNodeState
    {
        Active,
        Deprecated,
        Archived,
        Blocked,
    };

    enum class SCOpenMode
    {
        ReadWrite,
        ReadOnly,
        UpgradeRequired,
        Unsupported,
    };

    enum class SCUpgradeStatus
    {
        Success,
        Failed,
        RolledBack,
        NotRequired,
        Unsupported,
    };

    struct SCVersionNode
    {
        std::int32_t version{0};
        std::wstring name;
        std::wstring description;
        bool readable{true};
        bool writable{true};
        SCVersionNodeState state{SCVersionNodeState::Active};
    };

    struct SCMigrationEdge
    {
        std::int32_t fromVersion{0};
        std::int32_t toVersion{0};
        std::wstring name;
        std::wstring description;
        bool forwardOnly{true};
        bool required{true};
    };

    struct SCCompatibilityWindow
    {
        std::int32_t minReadableVersion{0};
        std::int32_t maxReadableVersion{0};
        std::int32_t minWritableVersion{0};
        std::int32_t maxWritableVersion{0};
        bool readOnlyAllowed{true};
        bool upgradeAllowed{true};
    };

    struct SCVersionGraph
    {
        std::int32_t currentVersion{0};
        std::int32_t latestSupportedVersion{0};
        std::vector<SCVersionNode> nodes;
        std::vector<SCMigrationEdge> edges;
        SCCompatibilityWindow compatibilityWindow;
    };

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

    struct SCUpgradePlan
    {
        std::int32_t currentVersion{0};
        std::int32_t targetVersion{0};
        bool requiresConfirmation{true};
        bool upgradeRequired{false};
        SCCompatibilityWindow compatibilityWindow;
        std::vector<SCMigrationStep> orderedSteps;
        std::wstring reason;
    };

    struct SCUpgradeResult
    {
        SCUpgradeStatus status{SCUpgradeStatus::Unsupported};
        std::int32_t sourceVersion{0};
        std::int32_t targetVersion{0};
        bool rolledBack{false};
        std::wstring failureReason;
    };

    struct SCOpenDecision
    {
        SCOpenMode mode{SCOpenMode::Unsupported};
        bool needsUpgrade{false};
        bool readOnlyOnly{false};
        bool writable{false};
        std::wstring reason;
        SCCompatibilityWindow compatibilityWindow;
    };

    ErrorCode BuildDefaultVersionGraph(SCVersionGraph* outGraph);
    ErrorCode BuildVersionGraph(
        std::int32_t currentVersion, std::int32_t targetVersion,
        const std::vector<SCMigrationStep>& availableSteps,
        SCVersionGraph* outGraph);

    ErrorCode BuildMigrationPlan(
        std::int32_t currentVersion, std::int32_t targetVersion,
        const std::vector<SCMigrationStep>& availableSteps,
        SCMigrationPlan* outPlan);

    ErrorCode BuildUpgradePlan(std::int32_t currentVersion,
                               std::int32_t targetVersion,
                               const SCVersionGraph& graph,
                               SCUpgradePlan* outPlan);

    ErrorCode EvaluateCompatibilityWindow(const SCVersionGraph& graph,
                                          std::int32_t schemaVersion,
                                          SCCompatibilityWindow* outWindow);

    ErrorCode EvaluateOpenDecision(const SCVersionGraph& graph,
                                   std::int32_t schemaVersion,
                                   bool cleanShutdown,
                                   SCOpenDecision* outDecision);

    std::int32_t GetLatestSupportedSchemaVersion() noexcept;

}  // namespace StableCore::Storage
