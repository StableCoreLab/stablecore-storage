#pragma once

#include <atomic>

#include "StableCore/Storage/RefPtr.h"

namespace stablecore::storage
{

class RefCountedObject : public virtual IRefObject
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
    ~RefCountedObject() override = default;

private:
    std::atomic<unsigned int> refCount_{1};
};

}  // namespace stablecore::storage
