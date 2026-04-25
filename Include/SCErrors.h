#pragma once

namespace StableCore::Storage
{

using ErrorCode = long;

constexpr ErrorCode SC_OK                     = 0x00000000L;
constexpr ErrorCode SC_FALSE_RESULT           = 0x00000001L;

constexpr ErrorCode SC_E_INVALIDARG           = 0x80070057L;
constexpr ErrorCode SC_E_POINTER              = 0x80004003L;
constexpr ErrorCode SC_E_FAIL                 = 0x80004005L;
constexpr ErrorCode SC_E_NOTIMPL              = 0x80004001L;

constexpr ErrorCode SC_E_TABLE_NOT_FOUND      = 0xA0010001L;
constexpr ErrorCode SC_E_COLUMN_NOT_FOUND     = 0xA0010002L;
constexpr ErrorCode SC_E_RECORD_NOT_FOUND     = 0xA0010003L;
constexpr ErrorCode SC_E_RECORD_DELETED       = 0xA0010004L;
constexpr ErrorCode SC_E_VALUE_IS_NULL        = 0xA0010005L;
constexpr ErrorCode SC_E_TYPE_MISMATCH        = 0xA0010006L;
constexpr ErrorCode SC_E_COLUMN_EXISTS        = 0xA0010007L;
constexpr ErrorCode SC_E_SCHEMA_VIOLATION     = 0xA0010008L;
constexpr ErrorCode SC_E_READ_ONLY_COLUMN     = 0xA0010009L;

constexpr ErrorCode SC_E_NO_ACTIVE_EDIT       = 0xA0010101L;
constexpr ErrorCode SC_E_EDIT_MISMATCH        = 0xA0010102L;
constexpr ErrorCode SC_E_EDIT_ALREADY_CLOSED  = 0xA0010103L;
constexpr ErrorCode SC_E_WRITE_CONFLICT       = 0xA0010104L;

constexpr ErrorCode SC_E_UNDO_STACK_EMPTY     = 0xA0010201L;
constexpr ErrorCode SC_E_REDO_STACK_EMPTY     = 0xA0010202L;

constexpr ErrorCode SC_E_CONSTRAINT_VIOLATION = 0xA0010301L;
constexpr ErrorCode SC_E_REFERENCE_INVALID    = 0xA0010302L;
constexpr ErrorCode SC_E_READ_ONLY_DATABASE   = 0xA0010303L;
constexpr ErrorCode SC_E_EXPORT_TOO_LARGE      = 0xA0010304L;
constexpr ErrorCode SC_E_EXPORT_CANCELLED      = 0xA0010305L;
constexpr ErrorCode SC_E_EXPORT_WRITE_FAILED   = 0xA0010306L;
constexpr ErrorCode SC_E_EXPORT_INVALID_STATE  = 0xA0010307L;
constexpr ErrorCode SC_E_FILE_EXISTS           = 0xA0010308L;
constexpr ErrorCode SC_E_VALIDATION_FAILED     = 0xA0010309L;
constexpr ErrorCode SC_E_IO_ERROR              = 0xA001030AL;

inline bool Succeeded(ErrorCode code) noexcept
{
    return code >= 0;
}

inline bool Failed(ErrorCode code) noexcept
{
    return !Succeeded(code);
}

}  // namespace StableCore::Storage
