#pragma once

#include <utility>

namespace stablecore::storage
{

class IRefObject
{
public:
    virtual unsigned int AddRef() = 0;
    virtual unsigned int Release() = 0;

protected:
    virtual ~IRefObject() = default;
};

template <class T>
class RefPtr
{
public:
    RefPtr() noexcept = default;
    RefPtr(std::nullptr_t) noexcept {}

    explicit RefPtr(T* ptr, bool addRef = true) noexcept
        : ptr_(ptr)
    {
        if (ptr_ != nullptr && addRef)
        {
            ptr_->AddRef();
        }
    }

    RefPtr(const RefPtr& other) noexcept
        : RefPtr(other.ptr_)
    {
    }

    template <class U>
    RefPtr(const RefPtr<U>& other) noexcept
        : RefPtr(other.Get())
    {
    }

    RefPtr(RefPtr&& other) noexcept
        : ptr_(other.ptr_)
    {
        other.ptr_ = nullptr;
    }

    template <class U>
    RefPtr(RefPtr<U>&& other) noexcept
        : ptr_(other.Detach())
    {
    }

    ~RefPtr()
    {
        Reset();
    }

    RefPtr& operator=(const RefPtr& other) noexcept
    {
        if (this != &other)
        {
            Reset(other.ptr_);
        }
        return *this;
    }

    RefPtr& operator=(RefPtr&& other) noexcept
    {
        if (this != &other)
        {
            Reset();
            ptr_ = other.ptr_;
            other.ptr_ = nullptr;
        }
        return *this;
    }

    T* Get() const noexcept
    {
        return ptr_;
    }

    T* operator->() const noexcept
    {
        return ptr_;
    }

    T& operator*() const noexcept
    {
        return *ptr_;
    }

    explicit operator bool() const noexcept
    {
        return ptr_ != nullptr;
    }

    void Reset(T* ptr = nullptr, bool addRef = true) noexcept
    {
        if (ptr_ == ptr)
        {
            return;
        }

        T* old = ptr_;
        ptr_ = ptr;
        if (ptr_ != nullptr && addRef)
        {
            ptr_->AddRef();
        }
        if (old != nullptr)
        {
            old->Release();
        }
    }

    T* Detach() noexcept
    {
        T* ptr = ptr_;
        ptr_ = nullptr;
        return ptr;
    }

private:
    template <class U>
    friend class RefPtr;

    T* ptr_{nullptr};
};

template <class T, class... Args>
RefPtr<T> MakeRef(Args&&... args)
{
    return RefPtr<T>(new T(std::forward<Args>(args)...), false);
}

}  // namespace stablecore::storage
