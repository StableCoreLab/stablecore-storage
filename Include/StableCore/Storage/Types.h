#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "StableCore/Storage/Errors.h"

namespace stablecore::storage
{

using RecordId = std::int64_t;
using VersionId = std::uint64_t;

struct EnumValue
{
    std::wstring value;

    bool operator==(const EnumValue& other) const noexcept
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

class Value
{
public:
    using Storage = std::variant<std::monostate, std::int64_t, double, bool, std::wstring, RecordIdValue, EnumValue>;

    Value() = default;

    static Value Null() { return Value(); }
    static Value FromInt64(std::int64_t value) { return Value(value); }
    static Value FromDouble(double value) { return Value(value); }
    static Value FromBool(bool value) { return Value(value); }
    static Value FromString(std::wstring value) { return Value(std::move(value)); }
    static Value FromRecordId(RecordId value) { return Value(RecordIdValue{value}); }
    static Value FromEnum(std::wstring value) { return Value(EnumValue{std::move(value)}); }

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
        if (const auto* typed = TryGet<EnumValue>())
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
        if (const auto* typed = TryGet<EnumValue>())
        {
            *outValue = typed->value;
            return SC_OK;
        }
        return IsNull() ? SC_E_VALUE_IS_NULL : SC_E_TYPE_MISMATCH;
    }

    bool operator==(const Value& other) const noexcept
    {
        return value_ == other.value_;
    }

    bool operator!=(const Value& other) const noexcept
    {
        return !(*this == other);
    }

    template <class T>
    const T* TryGet() const noexcept
    {
        return std::get_if<T>(&value_);
    }

private:
    explicit Value(std::int64_t value)
        : value_(value)
    {
    }

    explicit Value(double value)
        : value_(value)
    {
    }

    explicit Value(bool value)
        : value_(value)
    {
    }

    explicit Value(std::wstring value)
        : value_(std::move(value))
    {
    }

    explicit Value(RecordIdValue value)
        : value_(std::move(value))
    {
    }

    explicit Value(EnumValue value)
        : value_(std::move(value))
    {
    }

    Storage value_{};
};

struct ColumnDef
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
    Value defaultValue;
};

enum class ComputedFieldKind
{
    Expression,
    Rule,
    Aggregate,
};

enum class AggregateKind
{
    Count,
    Sum,
    Min,
    Max,
};

struct FieldDependency
{
    std::wstring tableName;
    std::wstring fieldName;
};

struct ComputedDependencySet
{
    std::vector<FieldDependency> factFields;
    std::vector<FieldDependency> relationFields;
};

struct ComputedColumnDef
{
    std::wstring name;
    std::wstring displayName;
    ValueKind valueKind{ValueKind::Null};
    TableColumnLayer layer{TableColumnLayer::Computed};

    ComputedFieldKind kind{ComputedFieldKind::Expression};
    std::wstring expression;
    std::wstring ruleId;
    ComputedDependencySet dependencies;
    AggregateKind aggregateKind{AggregateKind::Count};
    std::wstring aggregateRelation;
    std::wstring aggregateField;

    bool cacheable{true};
    bool editable{false};
};

struct QueryCondition
{
    std::wstring fieldName;
    Value expectedValue;
};

struct JournalEntry
{
    JournalOp op{};
    std::wstring tableName;
    RecordId recordId{0};
    std::wstring fieldName;
    Value oldValue;
    Value newValue;
    bool oldDeleted{false};
    bool newDeleted{false};
};

struct JournalTransaction
{
    std::wstring actionName;
    std::vector<JournalEntry> entries;
};

struct DataChange
{
    ChangeKind kind{};
    std::wstring tableName;
    RecordId recordId{0};
    std::wstring fieldName;
    Value oldValue;
    Value newValue;
    bool structuralChange{false};
    bool relationChange{false};
};

struct ChangeSet
{
    std::wstring actionName;
    ChangeSource source{ChangeSource::UserEdit};
    VersionId version{0};
    std::vector<DataChange> changes;
};

}  // namespace stablecore::storage
