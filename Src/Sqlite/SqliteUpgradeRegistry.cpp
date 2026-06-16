#include "SqliteUpgradeRegistry.h"

#include <algorithm>

namespace StableCore::Storage
{
    SqliteUpgradeRegistry& SqliteUpgradeRegistry::Instance()
    {
        static SqliteUpgradeRegistry instance;
        return instance;
    }

    void SqliteUpgradeRegistry::Register(SqliteUpgradeStepRegistration registration)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto it = std::find_if(registrations_.begin(),
                                     registrations_.end(),
                                     [&registration](const SqliteUpgradeStepRegistration& existing) {
                                         return existing.step.fromVersion == registration.step.fromVersion &&
                                                existing.step.toVersion == registration.step.toVersion;
                                     });
        if (it != registrations_.end())
        {
            *it = std::move(registration);
            return;
        }

        registrations_.push_back(std::move(registration));
    }

    const std::vector<SqliteUpgradeStepRegistration>& SqliteUpgradeRegistry::GetRegisteredSteps() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return registrations_;
    }

    const SqliteUpgradeStepRegistration* SqliteUpgradeRegistry::Find(std::int32_t fromVersion,
                                                                     std::int32_t toVersion) const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto it = std::find_if(registrations_.begin(),
                                     registrations_.end(),
                                     [fromVersion, toVersion](const SqliteUpgradeStepRegistration& registration) {
                                         return registration.step.fromVersion == fromVersion &&
                                                registration.step.toVersion == toVersion;
                                     });
        return it == registrations_.end() ? nullptr : &*it;
    }

    const std::vector<SqliteUpgradeStepRegistration>& GetRegisteredSqliteUpgradeSteps()
    {
        return SqliteUpgradeRegistry::Instance().GetRegisteredSteps();
    }

    const SqliteUpgradeStepRegistration* FindRegisteredSqliteUpgradeStep(std::int32_t fromVersion,
                                                                         std::int32_t toVersion)
    {
        return SqliteUpgradeRegistry::Instance().Find(fromVersion, toVersion);
    }
}  // namespace StableCore::Storage
