#include "SCMigration.h"

#include <algorithm>

namespace StableCore::Storage
{
    namespace
    {

        constexpr std::int32_t kMinimumSupportedSchemaVersion = 1;

        std::vector<SCMigrationStep> GetDefaultMigrationSteps()
        {
            return {
                SCMigrationStep{
                    1,
                    2,
                    L"sqlite-schema-v2",
                    L"Add startup diagnostics table and field-value record "
                    L"lookup index.",
                },
            };
        }

        std::int32_t ResolveLatestSupportedVersion(
            const std::vector<SCMigrationStep>& steps)
        {
            std::int32_t latest = kMinimumSupportedSchemaVersion;
            for (const auto& step : steps)
            {
                latest = std::max(latest, step.toVersion);
            }
            return latest;
        }

        void AppendVersionNode(std::vector<SCVersionNode>& nodes,
                               std::int32_t version, const std::wstring& name,
                               const std::wstring& description)
        {
            const auto it = std::find_if(nodes.begin(), nodes.end(),
                                         [version](const SCVersionNode& node) {
                                             return node.version == version;
                                         });

            if (it != nodes.end())
            {
                return;
            }

            SCVersionNode node;
            node.version = version;
            node.name = name;
            node.description = description;
            nodes.push_back(std::move(node));
        }

        void FinalizeGraphNodes(std::vector<SCVersionNode>& nodes,
                                std::int32_t latestVersion,
                                std::int32_t minimumVersion)
        {
            for (auto& node : nodes)
            {
                node.readable = node.version >= minimumVersion &&
                                node.version <= latestVersion;
                node.writable = node.version == latestVersion;

                if (node.version < minimumVersion)
                {
                    node.state = SCVersionNodeState::Blocked;
                } else if (node.version < latestVersion)
                {
                    node.state = SCVersionNodeState::Deprecated;
                } else if (node.version == latestVersion)
                {
                    node.state = SCVersionNodeState::Active;
                } else
                {
                    node.state = SCVersionNodeState::Archived;
                }
            }
        }

        SCCompatibilityWindow BuildCompatibilityWindow(
            std::int32_t latestVersion)
        {
            SCCompatibilityWindow window;
            window.minReadableVersion = kMinimumSupportedSchemaVersion;
            window.maxReadableVersion = latestVersion;
            window.minWritableVersion = latestVersion;
            window.maxWritableVersion = latestVersion;
            window.readOnlyAllowed = true;
            window.upgradeAllowed = true;
            return window;
        }

        std::vector<SCMigrationStep> EdgesToSteps(
            const std::vector<SCMigrationEdge>& edges)
        {
            std::vector<SCMigrationStep> steps;
            steps.reserve(edges.size());
            for (const auto& edge : edges)
            {
                steps.push_back(SCMigrationStep{
                    edge.fromVersion,
                    edge.toVersion,
                    edge.name,
                    edge.description,
                });
            }
            return steps;
        }

    }  // namespace

    std::int32_t GetLatestSupportedSchemaVersion() noexcept
    {
        return ResolveLatestSupportedVersion(GetDefaultMigrationSteps());
    }

    ErrorCode BuildDefaultVersionGraph(SCVersionGraph* outGraph)
    {
        return BuildVersionGraph(0, GetLatestSupportedSchemaVersion(),
                                 GetDefaultMigrationSteps(), outGraph);
    }

    ErrorCode BuildVersionGraph(
        std::int32_t currentVersion, std::int32_t targetVersion,
        const std::vector<SCMigrationStep>& availableSteps,
        SCVersionGraph* outGraph)
    {
        if (outGraph == nullptr)
        {
            return SC_E_POINTER;
        }
        if (currentVersion < 0 || targetVersion < 0)
        {
            return SC_E_INVALIDARG;
        }

        SCVersionGraph graph;
        graph.currentVersion = currentVersion;
        graph.latestSupportedVersion =
            ResolveLatestSupportedVersion(availableSteps);
        if (targetVersion > graph.latestSupportedVersion)
        {
            graph.latestSupportedVersion = targetVersion;
        }

        const std::int32_t minimumVersion = kMinimumSupportedSchemaVersion;
        graph.compatibilityWindow =
            BuildCompatibilityWindow(graph.latestSupportedVersion);

        AppendVersionNode(graph.nodes, minimumVersion,
                          L"v" + std::to_wstring(minimumVersion),
                          L"Minimum supported schema version.");
        AppendVersionNode(graph.nodes, currentVersion,
                          L"v" + std::to_wstring(currentVersion),
                          L"Current schema version.");
        AppendVersionNode(graph.nodes, targetVersion,
                          L"v" + std::to_wstring(targetVersion),
                          L"Target schema version.");

        for (const auto& step : availableSteps)
        {
            if (step.fromVersion < 0 || step.toVersion < 0 ||
                step.toVersion <= step.fromVersion)
            {
                return SC_E_INVALIDARG;
            }

            graph.edges.push_back(SCMigrationEdge{
                step.fromVersion,
                step.toVersion,
                step.name,
                step.description,
                true,
                true,
            });

            AppendVersionNode(graph.nodes, step.fromVersion,
                              L"v" + std::to_wstring(step.fromVersion),
                              step.name);
            AppendVersionNode(graph.nodes, step.toVersion,
                              L"v" + std::to_wstring(step.toVersion),
                              step.name);
        }

        FinalizeGraphNodes(graph.nodes, graph.latestSupportedVersion,
                           minimumVersion);
        *outGraph = std::move(graph);
        return SC_OK;
    }

    ErrorCode BuildMigrationPlan(
        std::int32_t currentVersion, std::int32_t targetVersion,
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
            const auto stepIt =
                std::find_if(availableSteps.begin(), availableSteps.end(),
                             [&](const SCMigrationStep& step) {
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

    ErrorCode BuildUpgradePlan(std::int32_t currentVersion,
                               std::int32_t targetVersion,
                               const SCVersionGraph& graph,
                               SCUpgradePlan* outPlan)
    {
        if (outPlan == nullptr)
        {
            return SC_E_POINTER;
        }

        SCUpgradePlan plan;
        plan.currentVersion = currentVersion;
        plan.targetVersion = targetVersion;
        plan.compatibilityWindow = graph.compatibilityWindow;
        plan.upgradeRequired = targetVersion > currentVersion;
        plan.requiresConfirmation = plan.upgradeRequired;

        const std::vector<SCMigrationStep> availableSteps =
            EdgesToSteps(graph.edges);
        SCMigrationPlan migrationPlan;
        const ErrorCode planRc = BuildMigrationPlan(
            currentVersion, targetVersion, availableSteps, &migrationPlan);
        if (Failed(planRc))
        {
            return planRc;
        }
        plan.orderedSteps = std::move(migrationPlan.orderedSteps);

        if (currentVersion == targetVersion)
        {
            plan.reason = L"Target version already matches current version.";
        } else if (currentVersion < targetVersion)
        {
            plan.reason =
                L"Upgrade is required to reach the target schema version.";
        } else
        {
            plan.reason =
                L"Requested target version is older than the current version.";
        }

        *outPlan = std::move(plan);
        return SC_OK;
    }

    ErrorCode EvaluateCompatibilityWindow(const SCVersionGraph& graph,
                                          std::int32_t schemaVersion,
                                          SCCompatibilityWindow* outWindow)
    {
        if (outWindow == nullptr)
        {
            return SC_E_POINTER;
        }

        *outWindow = graph.compatibilityWindow;
        if (schemaVersion < graph.compatibilityWindow.minReadableVersion)
        {
            outWindow->readOnlyAllowed = false;
            outWindow->upgradeAllowed = false;
        } else if (schemaVersion > graph.compatibilityWindow.maxReadableVersion)
        {
            outWindow->upgradeAllowed = false;
        } else if (schemaVersion < graph.compatibilityWindow.minWritableVersion)
        {
            outWindow->upgradeAllowed = true;
        }
        return SC_OK;
    }

    ErrorCode EvaluateOpenDecision(const SCVersionGraph& graph,
                                   std::int32_t schemaVersion,
                                   bool cleanShutdown,
                                   SCOpenDecision* outDecision)
    {
        if (outDecision == nullptr)
        {
            return SC_E_POINTER;
        }

        SCOpenDecision decision;
        decision.compatibilityWindow = graph.compatibilityWindow;

        if (schemaVersion < graph.compatibilityWindow.minReadableVersion)
        {
            decision.mode = SCOpenMode::Unsupported;
            decision.reason =
                L"Schema version is older than the minimum supported version.";
            *outDecision = std::move(decision);
            return SC_OK;
        }

        if (!cleanShutdown)
        {
            decision.mode = SCOpenMode::ReadOnly;
            decision.readOnlyOnly = true;
            decision.writable = false;
            decision.reason =
                L"Previous shutdown was unclean; open in read-only mode only.";
            *outDecision = std::move(decision);
            return SC_OK;
        }

        if (schemaVersion > graph.compatibilityWindow.maxReadableVersion)
        {
            decision.mode = graph.compatibilityWindow.readOnlyAllowed
                                ? SCOpenMode::ReadOnly
                                : SCOpenMode::Unsupported;
            decision.readOnlyOnly = graph.compatibilityWindow.readOnlyAllowed;
            decision.writable = false;
            decision.reason = graph.compatibilityWindow.readOnlyAllowed
                                  ? L"Schema version is newer than the "
                                    L"supported write window; open read-only."
                                  : L"Schema version is newer than the "
                                    L"supported read window.";
            *outDecision = std::move(decision);
            return SC_OK;
        }

        if (schemaVersion < graph.compatibilityWindow.minWritableVersion)
        {
            decision.mode = graph.compatibilityWindow.upgradeAllowed
                                ? SCOpenMode::UpgradeRequired
                                : SCOpenMode::ReadOnly;
            decision.needsUpgrade = graph.compatibilityWindow.upgradeAllowed;
            decision.readOnlyOnly = true;
            decision.writable = false;
            decision.reason =
                graph.compatibilityWindow.upgradeAllowed
                    ? L"Schema version is within the readable window but "
                      L"requires upgrade before write."
                    : L"Schema version is readable but not writable in the "
                      L"current compatibility window.";
            *outDecision = std::move(decision);
            return SC_OK;
        }

        decision.mode = SCOpenMode::ReadWrite;
        decision.writable = true;
        decision.reason =
            L"Schema version is within the writable compatibility window.";
        *outDecision = std::move(decision);
        return SC_OK;
    }

}  // namespace StableCore::Storage
