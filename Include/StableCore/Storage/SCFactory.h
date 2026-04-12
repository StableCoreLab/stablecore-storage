#pragma once

#include "StableCore/Storage/ISCInterfaces.h"

namespace stablecore::storage
{

ErrorCode CreateInMemoryDatabase(SCDbPtr& outDatabase);
ErrorCode CreateSqliteDatabase(const wchar_t* path, SCDbPtr& outDatabase);

}  // namespace stablecore::storage
