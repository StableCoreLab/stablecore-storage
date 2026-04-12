#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "StableCore/Storage/SCErrors.h"

namespace StableCore::Storage
{

using RecordId = std::int64_t;
using VersionId = std::uint64_t;

struct SCEnumValue
{
    std::wstring value;

    bool operator==(const SCEnumValue& other) const noexcept
    {
        return value == other.value;
    }
};

struct RecordIdValue
{
    RecordId value{0};

    bool operator==(const RecordIdValue& other) const noexcept
    {
        return value == other.value;
    }
};

enum class ValueKind
{
    Null,
    Int64,
    Double,
    Bool,
    String,
    RecordId,
    Enum,
};

enum class ColumnKind
{
    Fact,
    Relation,
};

enum class TableColumnLayer
{
    Fact,
    Computed,
};

enum class ChangeSource
{
    UserEdit,
    Undo,
    Redo,
    Import,
    RuleWriteback,
};

enum class ChangeKind
{
    FieldUpdated,
    RecordCreated,
    RecordDeleted,
    RelationUpdated,
};

enum class JournalOp
{
    SetValue,
    CreateRecord,
    DeleteRecord,
    SetRelation,
};

enum class RecordState
{
    Alive,
    Deleted,
};

enum class EditState
{
    Active,
    Committed,
    RolledBack,
};

class SCValue
{
public:
    using Storage = std::variant<std::monostate, std::int64_t, double, bool, std::wstring, RecordIdValue, SCEnumValue>;

    SCValue() = default;

    static SCValue Null() { return SCValue(); }
    static SCValue FromInt64(std::int64_t value) { return SCValue(value); }
    static SCValue FromDouble(double value) { return SCValue(value); }
    static SCValue FromBool(bool value) { return SCValue(value); }
    static SCValue FromString(std::wstring value) { return SCValue(std::move(value)); }
    static SCValue FromRecordId(RecordId value) { return SCValue(RecordIdValue{value}); }
    static SCValue FromEnum(std::wstring value) { return SCValue(SCEnumValue{std::move(value)}); }

    ValueKind GetKind() const noexcept
    {
        switch (value_.index())
        {
        case 0: return ValueKind::Null;
        case 1: return ValueKind::Int64;
        case 2: return ValueKind::Double;
        case 3: return ValueKind::Bool;
        case 4: return ValueKind::String;
        case 5: return ValueKind::RecordId;
        case 6: return ValueKind::Enum;
        default: return ValueKind::Null;
        }
    }

    bool IsNull() const noexcept
    {
        return std::holds_alternative<std::monostate>(value_);
    }

    ErrorCode AsInt64(std::int64_t* outValue) const noexcept
    {
        if (outValue == nullptr)
        {
            return SC_E_POINTER;
        }
        if (const auto* typed = TryGet<std::int64_t>())
        {
            *outValue = *typed;
            return SC_OK;
        }
        return IsNull() ? SC_E_VALUE_IS_NULL : SC_E_TYPE_MISMATCH;
    }

    ErrorCode AsDouble(double* outValue) const noexcept
    {
        if (outValue == nullptr)
        {
            return SC_E_POINTER;
        }
        if (const auto* typed = TryGet<double>())
        {
            *outValue = *typed;
            return SC_OK;
        }
        return IsNull() ? SC_E_VALUE_IS_NULL : SC_E_TYPE_MISMATCH;
    }

    ErrorCode AsBool(bool* outValue) const noexcept
    {
        if (outValue == nullptr)
        {
            return SC_E_POINTER;
        }
        if (const auto* typed = TryGet<bool>())
        {
            *outValue = *typed;
            return SC_OK;
        }
        return IsNull() ? SC_E_VALUE_IS_NULL : SC_E_TYPE_MISMATCH;
    }

    ErrorCode AsString(const wchar_t** outValue) const noexcept
    {
        if (outValue == nullptr)
        {
            return SC_E_POINTER;
        }
        if (const auto* typed = TryGet<std::wstring>())
        {
            *outValue = typed->c_str();
            return SC_OK;
        }
        return IsNull() ? SC_E_VALUE_IS_NULL : SC_E_TYPE_MISMATCH;
    }

    ErrorCode AsStringCopy(std::wstring* outValue) const
    {
        if (outValue == nullptr)
        {
            return SC_E_POINTER;
        }
        if (const auto* typed = TryGet<std::wstring>())
        {
            *outValue = *typed;
            return SC_OK;
        }
        return IsNull() ? SC_E_VALUE_IS_NULL : SC_E_TYPE_MISMATCH;
    }

    ErrorCode AsRecordId(RecordId* outValue) const noexcept
    {
        if (outValue == nullptr)
        {
            return SC_E_POINTER;
        }
        if (const auto* typed = TryGet<RecordIdValue>())
        {
            *outValue = typed->value;
            return SC_OK;
        }
        return IsNull() ? SC_E_VALUE_IS_NULL : SC_E_TYPE_MISMATCH;
    }

    ErrorCode AsEnum(const wchar_t** outValue) const noexcept
    {
        if (outValue == nullptr)
        {
            return SC_E_POINTER;
        }
        if (const auto* typed = TryGet<SCEnumValue>())
        {
            *outValue = typed->value.c_str();
            return SC_OK;
        }
        return IsNull() ? SC_E_VALUE_IS_NULL : SC_E_TYPE_MISMATCH;
    }

    ErrorCode AsEnumCopy(std::wstring* outValue) const
    {
        if (outValue == nullptr)
        {
            return SC_E_POINTER;
        }
        if (const auto* typed = TryGet<SCEnumValue>())
        {
            *outValue = typed->value;
            return SC_OK;
        }
        return IsNull() ? SC_E_VALUE_IS_NULL : SC_E_TYPE_MISMATCH;
    }

    bool operator==(const SCValue& other) const noexcept
    {
        return value_ == other.value_;
    }

    bool operator!=(const SCValue& other) const noexcept
    {
        return !(*this == other);
    }

    template <class T>
    const T* TryGet() const noexcept
    {
        return std::get_if<T>(&value_);
    }

private:
    explicit SCValue(std::int64_t value)
        : value_(value)
    {
    }

    explicit SCValue(double value)
        : value_(value)
    {
    }

    explicit SCValue(bool value)
        : value_(value)
    {
    }

    explicit SCValue(std::wstring value)
        : value_(std::move(value))
    {
    }

    explicit SCValue(RecordIdValue value)
        : value_(std::move(value))
    {
    }

    explicit SCValue(SCEnumValue value)
        : value_(std::move(value))
    {
    }

    Storage value_{};
};

struct SCColumnDef
{
    std::wstring name;
    std::wstring displayName;
    ValueKind valueKind{ValueKind::Null};
    ColumnKind columnKind{ColumnKind::Fact};
    bool nullable{true};
    bool editable{true};
    bool userDefined{false};
    bool indexed{false};
    bool participatesInCalc{false};
    std::wstring unit;
    std::wstring referenceTable;
    SCValue defaultValue;
};

enum class ComputedFieldKind
{
    Expression,
    Rule,
    Aggregate,
};

enum class SCAggregateKind
{
    Count,
    Sum,
    Min,
    Max,
};

struct SCFieldDependency
{
    std::wstring tableName;
    std::wstring fieldName;
};

struct SCComputedDependencySet
{
    std::vector<SCFieldDependency> factFields;
    std::vector<SCFieldDependency> relationFields;
};

struct SCComputedColumnDef
{
    std::wstring name;
    std::wstring displayName;
    ValueKind valueKind{ValueKind::Null};
    TableColumnLayer layer{TableColumnLayer::Computed};

    ComputedFieldKind kind{ComputedFieldKind::Expression};
    std::wstring expression;
    std::wstring ruleId;
    SCComputedDependencySet dependencies;
    SCAggregateKind aggregateKind{SCAggregateKind::Count};
    std::wstring aggregateRelation;
    std::wstring aggregateField;

    bool cacheable{true};
    bool editable{false};
};

struct SCQueryCondition
{
    std::wstring fieldName;
    SCValue expectedValue;
};

struct JournalEntry
{
    JournalOp op{};
    std::wstring tableName;
    RecordId recordId{0};
    std::wstring fieldName;
    SCValue oldValue;
    SCValue newValue;
    bool oldDeleted{false};
    bool newDeleted{false};
};

struct JournalTransaction
{
    std::wstring actionName;
    std::vector<JournalEntry> entries;
};

struct SCDataChange
{
    ChangeKind kind{};
    std::wstring tableName;
    RecordId recordId{0};
    std::wstring fieldName;
    SCValue oldValue;
    SCValue newValue;
    bool structuralChange{false};
    bool relationChange{false};
};

struct SCChangeSet
{
    std::wstring actionName;
    ChangeSource source{ChangeSource::UserEdit};
    VersionId version{0};
    std::vector<SCDataChange> changes;
};

}  // namespace StableCore::Storage
