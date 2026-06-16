#pragma once

#include <cstdint>
#include <mutex>
#include <vector>

#include "SqliteUpgradeInterfaces.h"

namespace StableCore::Storage
{
    class SqliteUpgradeRegistry
    {
    public:
        static SqliteUpgradeRegistry& Instance();

        void Register(SqliteUpgradeStepRegistration registration);
        const std::vector<SqliteUpgradeStepRegistration>& GetRegisteredSteps() const;
        const SqliteUpgradeStepRegistration* Find(std::int32_t fromVersion, std::int32_t toVersion) const;

    private:
        SqliteUpgradeRegistry() = default;

        mutable std::mutex mutex_;
        std::vector<SqliteUpgradeStepRegistration> registrations_;
    };

    const std::vector<SqliteUpgradeStepRegistration>& GetRegisteredSqliteUpgradeSteps();
    const SqliteUpgradeStepRegistration* FindRegisteredSqliteUpgradeStep(std::int32_t fromVersion,
                                                                         std::int32_t toVersion);

#define SC_SQLITE_UPGRADE_REGISTER_STEP_IMPL(id, from_version, to_version, step_name, step_description, handler) \
    namespace                                                                                                   \
    {                                                                                                            \
        const bool sc_sqlite_upgrade_registered_##id = []() {                                                   \
            ::StableCore::Storage::SqliteUpgradeRegistry::Instance().Register(                                  \
                ::StableCore::Storage::SqliteUpgradeStepRegistration{                                          \
                    ::StableCore::Storage::SCMigrationStep{from_version, to_version, step_name,                \
                                                           step_description},                                    \
                    handler});                                                                                  \
            return true;                                                                                         \
        }();                                                                                                     \
    }

#define SC_SQLITE_UPGRADE_REGISTER_STEP(from_version, to_version, step_name, step_description, handler) \
    SC_SQLITE_UPGRADE_REGISTER_STEP_IMPL(__COUNTER__, from_version, to_version, step_name, step_description, handler)
}
