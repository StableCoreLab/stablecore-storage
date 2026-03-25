#include "StableCore/Storage/Factory.h"

namespace stablecore::storage
{

ErrorCode CreateSqliteDatabase(const wchar_t* path, DbPtr& outDatabase)
{
    if (path == nullptr)
    {
        return SC_E_POINTER;
    }

    outDatabase.Reset();
    return SC_E_NOTIMPL;
}

}  // namespace stablecore::storage
