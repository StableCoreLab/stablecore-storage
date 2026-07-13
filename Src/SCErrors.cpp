#include "SCErrors.h"

#include <cstdint>
#include <iomanip>
#include <sstream>

namespace StableCore::Storage
{

    namespace
    {

        struct ErrorCodeEntry
        {
            ErrorCode code;
            const wchar_t* name;
        };

        constexpr ErrorCodeEntry kErrorCodeEntries[] = {
            {SC_OK, L"SC_OK"},
            {SC_FALSE_RESULT, L"SC_FALSE_RESULT"},
            {SC_E_INVALIDARG, L"SC_E_INVALIDARG"},
            {SC_E_POINTER, L"SC_E_POINTER"},
            {SC_E_FAIL, L"SC_E_FAIL"},
            {SC_E_NOTIMPL, L"SC_E_NOTIMPL"},
            {SC_E_TABLE_NOT_FOUND, L"SC_E_TABLE_NOT_FOUND"},
            {SC_E_COLUMN_NOT_FOUND, L"SC_E_COLUMN_NOT_FOUND"},
            {SC_E_RECORD_NOT_FOUND, L"SC_E_RECORD_NOT_FOUND"},
            {SC_E_RECORD_DELETED, L"SC_E_RECORD_DELETED"},
            {SC_E_VALUE_IS_NULL, L"SC_E_VALUE_IS_NULL"},
            {SC_E_TYPE_MISMATCH, L"SC_E_TYPE_MISMATCH"},
            {SC_E_COLUMN_EXISTS, L"SC_E_COLUMN_EXISTS"},
            {SC_E_SCHEMA_VIOLATION, L"SC_E_SCHEMA_VIOLATION"},
            {SC_E_READ_ONLY_COLUMN, L"SC_E_READ_ONLY_COLUMN"},
            {SC_E_CONSTRAINT_NOT_FOUND, L"SC_E_CONSTRAINT_NOT_FOUND"},
            {SC_E_INDEX_NOT_FOUND, L"SC_E_INDEX_NOT_FOUND"},
            {SC_E_NO_ACTIVE_EDIT, L"SC_E_NO_ACTIVE_EDIT"},
            {SC_E_EDIT_MISMATCH, L"SC_E_EDIT_MISMATCH"},
            {SC_E_EDIT_ALREADY_CLOSED, L"SC_E_EDIT_ALREADY_CLOSED"},
            {SC_E_WRITE_CONFLICT, L"SC_E_WRITE_CONFLICT"},
            {SC_E_UNDO_STACK_EMPTY, L"SC_E_UNDO_STACK_EMPTY"},
            {SC_E_REDO_STACK_EMPTY, L"SC_E_REDO_STACK_EMPTY"},
            {SC_E_CONSTRAINT_VIOLATION, L"SC_E_CONSTRAINT_VIOLATION"},
            {SC_E_REFERENCE_INVALID, L"SC_E_REFERENCE_INVALID"},
            {SC_E_READ_ONLY_DATABASE, L"SC_E_READ_ONLY_DATABASE"},
            {SC_E_EXPORT_TOO_LARGE, L"SC_E_EXPORT_TOO_LARGE"},
            {SC_E_EXPORT_CANCELLED, L"SC_E_EXPORT_CANCELLED"},
            {SC_E_EXPORT_WRITE_FAILED, L"SC_E_EXPORT_WRITE_FAILED"},
            {SC_E_EXPORT_INVALID_STATE, L"SC_E_EXPORT_INVALID_STATE"},
            {SC_E_FILE_EXISTS, L"SC_E_FILE_EXISTS"},
            {SC_E_VALIDATION_FAILED, L"SC_E_VALIDATION_FAILED"},
            {SC_E_IO_ERROR, L"SC_E_IO_ERROR"},
            {SC_E_UPGRADE_PATH_NOT_FOUND, L"SC_E_UPGRADE_PATH_NOT_FOUND"},
            {SC_E_JOURNAL_TABLE_MISSING, L"SC_E_JOURNAL_TABLE_MISSING"},
        };

    }  // namespace

    const wchar_t* GetErrorCodeName(ErrorCode code) noexcept
    {
        for (const ErrorCodeEntry& entry : kErrorCodeEntries)
        {
            if (entry.code == code)
            {
                return entry.name;
            }
        }
        return L"SC_E_UNKNOWN";
    }

    const wchar_t* GetErrorCodeCategory(ErrorCode code) noexcept
    {
        if (code == SC_OK || code == SC_FALSE_RESULT)
        {
            return L"结果状态";
        }

        if (code == SC_E_INVALIDARG || code == SC_E_POINTER || code == SC_E_FAIL || code == SC_E_NOTIMPL)
        {
            return L"通用错误";
        }

        const std::uint32_t rawCode = static_cast<std::uint32_t>(code);
        switch (rawCode & 0xFFFFFF00u)
        {
        case 0xA0010000u:
            return L"表结构与记录错误";
        case 0xA0010100u:
            return L"编辑会话错误";
        case 0xA0010200u:
            return L"撤销重做错误";
        case 0xA0010300u:
            return L"约束与存储错误";
        default:
            return Failed(code) ? L"未知错误" : L"未知状态";
        }
    }

    std::wstring FormatErrorCode(ErrorCode code)
    {
        std::wostringstream stream;
        stream << GetErrorCodeName(code)
               << L" ["
               << GetErrorCodeCategory(code)
               << L"] (0x"
               << std::uppercase
               << std::hex
               << std::setw(8)
               << std::setfill(L'0')
               << static_cast<std::uint32_t>(code)
               << std::dec
               << L" / "
               << code
               << L")";
        return stream.str();
    }

}  // namespace StableCore::Storage
