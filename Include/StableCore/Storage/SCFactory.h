#pragma once

#include "StableCore/Storage/ISCInterfaces.h"

namespace StableCore::Storage
{

ErrorCode CreateInMemoryDatabase(SCDbPtr& outDatabase);
ErrorCode CreateSqliteDatabase(const wchar_t* path, SCDbPtr& outDatabase);

}  // namespace StableCore::Storage
