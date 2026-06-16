#pragma once

#include "ISCInterfaces.h"

namespace StableCore::Storage
{
    // Opens a file-backed database and auto-upgrades supported schema versions
    // before returning the live handle.
    ErrorCode CreateFileDatabase(const wchar_t* path, const SCOpenDatabaseOptions& options, SCDbPtr& outDatabase);
    // Explicit upgrade entry for callers that want to drive upgrade separately.
    ErrorCode UpgradeFileDatabase(const wchar_t* path, SCDbPtr& outDatabase, SCUpgradeResult* outResult = nullptr);

}  // namespace StableCore::Storage
