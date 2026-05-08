#pragma once

#include "ISCInterfaces.h"

namespace StableCore::Storage
{
    ErrorCode CreateFileDatabase(const wchar_t* path,
                                 const SCOpenDatabaseOptions& options,
                                 SCDbPtr& outDatabase);

}  // namespace StableCore::Storage
