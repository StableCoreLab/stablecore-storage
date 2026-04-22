#pragma once

#include "ISCInterfaces.h"

namespace StableCore::Storage
{

ErrorCode CreateInMemoryDatabase(SCDbPtr& outDatabase);
ErrorCode CreateSqliteDatabase(const wchar_t* path, SCDbPtr& outDatabase);
ErrorCode CreateSqliteDatabase(const wchar_t* path, bool readOnly, SCDbPtr& outDatabase);

}  // namespace StableCore::Storage
