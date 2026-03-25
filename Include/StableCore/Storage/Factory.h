#pragma once

#include "StableCore/Storage/Interfaces.h"

namespace stablecore::storage
{

ErrorCode CreateInMemoryDatabase(DbPtr& outDatabase);
ErrorCode CreateSqliteDatabase(const wchar_t* path, DbPtr& outDatabase);

}  // namespace stablecore::storage
