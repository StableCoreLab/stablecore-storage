#pragma once

#include <atomic>

#include "StableCore/Storage/ISCRefPtr.h"

namespace stablecore::storage
{

class SCRefCountedObject : public virtual ISCRefObject
{
public:
    unsigned int AddRef() override
    {
        return ++refCount_;
    }

    unsigned int Release() override
    {
        const unsigned int remaining = --refCount_;
        if (remaining == 0)
        {
            delete this;
        }
        return remaining;
    }

protected:
    ~SCRefCountedObject() override = default;

private:
    std::atomic<unsigned int> refCount_{1};
};

}  // namespace stablecore::storage
