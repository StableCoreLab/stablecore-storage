#pragma once

#include <utility>

namespace StableCore::Storage
{

class ISCRefObject
{
public:
    virtual unsigned int AddRef() = 0;
    virtual unsigned int Release() = 0;

protected:
    virtual ~ISCRefObject() = default;
};

template <class T>
class SCRefPtr
{
public:
    SCRefPtr() noexcept = default;
    SCRefPtr(std::nullptr_t) noexcept {}

    explicit SCRefPtr(T* ptr, bool addRef = true) noexcept
        : ptr_(ptr)
    {
        if (ptr_ != nullptr && addRef)
        {
            ptr_->AddRef();
        }
    }

    SCRefPtr(const SCRefPtr& other) noexcept
        : SCRefPtr(other.ptr_)
    {
    }

    template <class U>
    SCRefPtr(const SCRefPtr<U>& other) noexcept
        : SCRefPtr(other.Get())
    {
    }

    SCRefPtr(SCRefPtr&& other) noexcept
        : ptr_(other.ptr_)
    {
        other.ptr_ = nullptr;
    }

    template <class U>
    SCRefPtr(SCRefPtr<U>&& other) noexcept
        : ptr_(other.Detach())
    {
    }

    ~SCRefPtr()
    {
        Reset();
    }

    SCRefPtr& operator=(const SCRefPtr& other) noexcept
    {
        if (this != &other)
        {
            Reset(other.ptr_);
        }
        return *this;
    }

    SCRefPtr& operator=(SCRefPtr&& other) noexcept
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
    friend class SCRefPtr;

    T* ptr_{nullptr};
};

template <class T, class... Args>
SCRefPtr<T> SCMakeRef(Args&&... args)
{
    return SCRefPtr<T>(new T(std::forward<Args>(args)...), false);
}

}  // namespace StableCore::Storage
