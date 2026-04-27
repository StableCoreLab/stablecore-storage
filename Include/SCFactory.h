#pragma once

#include "ISCInterfaces.h"

namespace StableCore::Storage
{

    ErrorCode CreateInMemoryDatabase(const SCOpenDatabaseOptions& options,
                                     SCDbPtr& outDatabase);
    ErrorCode CreateInMemoryDatabase(SCDbPtr& outDatabase);
    ErrorCode CreateFileDatabase(const wchar_t* path,
                                 const SCOpenDatabaseOptions& options,
                                 SCDbPtr& outDatabase);

}  // namespace StableCore::Storage
