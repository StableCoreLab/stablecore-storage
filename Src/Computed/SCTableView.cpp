#include "ISCTableView.h"
#include "SCQuery.h"

#include <algorithm>
#include <cwctype>
#include <sstream>
#include <unordered_map>
#include <utility>

#include "SCRefCounted.h"

namespace StableCore::Storage
{
namespace
{

std::vector<std::wstring> SplitParts(const std::wstring& text, wchar_t separator)
{
    std::vector<std::wstring> parts;
    std::wstring current;
    for (wchar_t ch : text)
    {
        if (ch == separator)
        {
            parts.push_back(current);
            current.clear();
            continue;
        }
        current.push_back(ch);
    }
    parts.push_back(current);
    return parts;
}

bool IsBlank(const std::wstring& text)
{
    return std::all_of(
        text.begin(),
        text.end(),
        [](wchar_t ch)
        {
            return std::iswspace(ch) != 0;
        });
}

ErrorCode ValidateComputedColumnDefinition(const SCComputedColumnDef& column)
{
    if (column.name.empty() || IsBlank(column.name))
    {
        return SC_E_INVALIDARG;
    }
    if (column.valueKind == ValueKind::Null)
    {
        return SC_E_INVALIDARG;
    }
    if (column.editable)
    {
        return SC_E_INVALIDARG;
    }

    switch (column.kind)
    {
    case ComputedFieldKind::Expression:
        if (column.expression.empty() || IsBlank(column.expression))
        {
            return SC_E_INVALIDARG;
        }
        if (!column.ruleId.empty() || !column.aggregateRelation.empty() || !column.aggregateField.empty())
        {
            return SC_E_INVALIDARG;
        }
        if (column.dependencies.factFields.empty() && column.dependencies.relationFields.empty())
        {
            return SC_E_INVALIDARG;
        }
        return SC_OK;

    case ComputedFieldKind::Rule:
        if (column.ruleId.empty() || IsBlank(column.ruleId))
        {
            return SC_E_INVALIDARG;
        }
        if (!column.expression.empty() || !column.aggregateRelation.empty() || !column.aggregateField.empty())
        {
            return SC_E_INVALIDARG;
        }
        if (column.dependencies.factFields.empty() && column.dependencies.relationFields.empty())
        {
            return SC_E_INVALIDARG;
        }
        return SC_OK;

    case ComputedFieldKind::Aggregate:
    {
        if (!column.expression.empty() || !column.ruleId.empty())
        {
            return SC_E_INVALIDARG;
        }
        if (column.aggregateRelation.empty())
        {
            return SC_E_INVALIDARG;
        }
        const std::vector<std::wstring> parts = SplitParts(column.aggregateRelation, L'.');
        if (parts.size() != 2 || parts[0].empty() || parts[1].empty())
        {
            return SC_E_INVALIDARG;
        }
        if (column.aggregateRelation != parts[0] + L"." + parts[1])
        {
            return SC_E_INVALIDARG;
        }
        if (column.dependencies.factFields.empty() && column.dependencies.relationFields.empty())
        {
            return SC_E_INVALIDARG;
        }
        switch (column.aggregateKind)
        {
        case SCAggregateKind::Count:
            if (column.valueKind != ValueKind::Int64)
            {
                return SC_E_INVALIDARG;
            }
            break;
        case SCAggregateKind::Sum:
        case SCAggregateKind::Min:
        case SCAggregateKind::Max:
            if (column.aggregateField.empty() || IsBlank(column.aggregateField))
            {
                return SC_E_INVALIDARG;
            }
            if (column.valueKind != ValueKind::Double)
            {
                return SC_E_INVALIDARG;
            }
            break;
        default:
            return SC_E_INVALIDARG;
        }
        return SC_OK;
    }

    default:
        return SC_E_INVALIDARG;
    }
}

class TableViewRecordContext final : public ISCComputedContext, public SCRefCountedObject
{
public:
    TableViewRecordContext(ISCDatabase* database, std::wstring tableName, SCRecordPtr record)
        : database_(database)
        , tableName_(std::move(tableName))
        , record_(std::move(record))
    {
    }

    ErrorCode GetValue(const wchar_t* fieldName, SCValue* outValue) override
    {
        return record_->GetValue(fieldName, outValue);
    }

    ErrorCode GetRef(const wchar_t* fieldName, RecordId* outValue) override
    {
        return record_->GetRef(fieldName, outValue);
    }

    ErrorCode GetRelated(const wchar_t* relationName, SCRecordCursorPtr& outCursor) override
    {
        if (relationName == nullptr || *relationName == L'\0')
        {
            return SC_E_INVALIDARG;
        }
        if (!database_)
        {
            return SC_E_POINTER;
        }

        const std::vector<std::wstring> parts = SplitParts(relationName, L'.');
        if (parts.size() != 2)
        {
            return SC_E_INVALIDARG;
        }

        SCTablePtr relatedTable;
        ErrorCode rc = database_->GetTable(parts[0].c_str(), relatedTable);
        if (Failed(rc))
        {
            return rc;
        }

        [[maybe_unused]] QueryPlan legacyPlan;
        rc = SCQueryBridge::BuildPlanFromLegacyFindRecords(
            parts[0],
            SCQueryCondition{parts[1], SCValue::FromRecordId(record_->GetId())},
            &legacyPlan);
        if (Failed(rc))
        {
            return rc;
        }

        return relatedTable->FindRecords(SCQueryCondition{parts[1], SCValue::FromRecordId(record_->GetId())}, outCursor);
    }

private:
    SCRefPtr<ISCDatabase> database_;
    std::wstring tableName_;
    SCRecordPtr record_;
};

ErrorCode AggregateCursorValues(
    const SCComputedColumnDef& column,
    SCRecordCursorPtr& cursor,
    SCValue* outValue)
{
    if (outValue == nullptr)
    {
        return SC_E_POINTER;
    }

    bool hasRow = false;
    std::size_t count = 0;
    double sum = 0.0;
    double minValue = 0.0;
    double maxValue = 0.0;
    bool initialized = false;

    while (cursor->MoveNext(&hasRow) == SC_OK && hasRow)
    {
        SCRecordPtr record;
        const ErrorCode currentRc = cursor->GetCurrent(record);
        if (Failed(currentRc))
        {
            return currentRc;
        }

        ++count;
        if (column.aggregateKind == SCAggregateKind::Count)
        {
            continue;
        }

        SCValue SCValue;
        ErrorCode valueRc = record->GetValue(column.aggregateField.c_str(), &SCValue);
        if (valueRc == SC_E_VALUE_IS_NULL)
        {
            continue;
        }
        if (Failed(valueRc))
        {
            return valueRc;
        }

        double numeric = 0.0;
        if (SCValue.AsDouble(&numeric) != SC_OK)
        {
            std::int64_t intValue = 0;
            const ErrorCode intRc = SCValue.AsInt64(&intValue);
            if (Failed(intRc))
            {
                return intRc;
            }
            numeric = static_cast<double>(intValue);
        }

        sum += numeric;
        if (!initialized)
        {
            minValue = numeric;
            maxValue = numeric;
            initialized = true;
        }
        else
        {
            minValue = std::min(minValue, numeric);
            maxValue = std::max(maxValue, numeric);
        }
    }

    switch (column.aggregateKind)
    {
    case SCAggregateKind::Count:
        *outValue = SCValue::FromInt64(static_cast<std::int64_t>(count));
        return SC_OK;
    case SCAggregateKind::Sum:
        *outValue = SCValue::FromDouble(sum);
        return SC_OK;
    case SCAggregateKind::Min:
        *outValue = initialized ? SCValue::FromDouble(minValue) : SCValue::Null();
        return initialized ? SC_OK : SC_E_VALUE_IS_NULL;
    case SCAggregateKind::Max:
        *outValue = initialized ? SCValue::FromDouble(maxValue) : SCValue::Null();
        return initialized ? SC_OK : SC_E_VALUE_IS_NULL;
    default:
        return SC_E_NOTIMPL;
    }
}

class ComputedTableView final : public ISCComputedTableView, public ISCDatabaseObserver, public SCRefCountedObject
{
public:
    ComputedTableView(ISCDatabase* database, std::wstring tableName, ISCRuleRegistry* ruleRegistry)
        : database_(database)
        , tableName_(std::move(tableName))
        , ruleRegistry_(ruleRegistry)
    {
        CreateComputedCache(cache_);
    }

    ~ComputedTableView() override
    {
        if (database_)
        {
            database_->RemoveObserver(this);
        }
    }

    ErrorCode Initialize()
    {
        if (!database_)
        {
            return SC_E_POINTER;
        }

        ErrorCode rc = database_->GetTable(tableName_.c_str(), table_);
        if (Failed(rc))
        {
            return rc;
        }

        rc = table_->GetSchema(schema_);
        if (Failed(rc))
        {
            return rc;
        }

        return database_->AddObserver(this);
    }

    ErrorCode GetTableName(std::wstring* outTableName) override
    {
        if (outTableName == nullptr)
        {
            return SC_E_POINTER;
        }
        *outTableName = tableName_;
        return SC_OK;
    }

    ErrorCode GetColumnCount(std::int32_t* outCount) override
    {
        if (outCount == nullptr)
        {
            return SC_E_POINTER;
        }

        std::int32_t factCount = 0;
        ErrorCode rc = schema_->GetColumnCount(&factCount);
        if (Failed(rc))
        {
            return rc;
        }

        *outCount = factCount + static_cast<std::int32_t>(computedColumns_.size());
        return SC_OK;
    }

    ErrorCode GetColumn(std::int32_t index, SCTableViewColumnDef* outColumn) override
    {
        if (outColumn == nullptr)
        {
            return SC_E_POINTER;
        }
        if (index < 0)
        {
            return SC_E_INVALIDARG;
        }

        std::int32_t factCount = 0;
        ErrorCode rc = schema_->GetColumnCount(&factCount);
        if (Failed(rc))
        {
            return rc;
        }

        if (index < factCount)
        {
            SCColumnDef fact;
            rc = schema_->GetColumn(index, &fact);
            if (Failed(rc))
            {
                return rc;
            }
            outColumn->layer = TableColumnLayer::Fact;
            outColumn->name = fact.name;
            outColumn->displayName = fact.displayName;
            outColumn->valueKind = fact.valueKind;
            outColumn->editable = fact.editable;
            return SC_OK;
        }

        const std::size_t computedIndex = static_cast<std::size_t>(index - factCount);
        if (computedIndex >= computedColumns_.size())
        {
            return SC_E_INVALIDARG;
        }

        const SCComputedColumnDef& computed = computedColumns_[computedIndex];
        outColumn->layer = TableColumnLayer::Computed;
        outColumn->name = computed.name;
        outColumn->displayName = computed.displayName;
        outColumn->valueKind = computed.valueKind;
        outColumn->editable = computed.editable;
        return SC_OK;
    }

    ErrorCode AddComputedColumn(const SCComputedColumnDef& column) override
    {
        const ErrorCode validate = ValidateComputedColumnDefinition(column);
        if (Failed(validate))
        {
            return validate;
        }
        const auto duplicate = std::find_if(
            computedColumns_.begin(),
            computedColumns_.end(),
            [&](const SCComputedColumnDef& existing)
            {
                return existing.name == column.name;
            });
        if (duplicate != computedColumns_.end())
        {
            return SC_E_COLUMN_EXISTS;
        }

        computedColumns_.push_back(column);
        return SC_OK;
    }

    ErrorCode EnumerateRecords(SCRecordCursorPtr& outCursor) override
    {
        return table_->EnumerateRecords(outCursor);
    }

    ErrorCode GetCellValue(RecordId recordId, const wchar_t* columnName, SCValue* outValue) override
    {
        if (columnName == nullptr || outValue == nullptr)
        {
            return SC_E_POINTER;
        }

        SCColumnDef factColumn;
        const ErrorCode factRc = schema_->FindColumn(columnName, &factColumn);
        if (Succeeded(factRc))
        {
            SCRecordPtr record;
            ErrorCode rc = table_->GetRecord(recordId, record);
            if (Failed(rc))
            {
                return rc;
            }
            return record->GetValue(columnName, outValue);
        }

        const auto computedIt = std::find_if(
            computedColumns_.begin(),
            computedColumns_.end(),
            [&](const SCComputedColumnDef& column)
            {
                return column.name == columnName;
            });
        if (computedIt == computedColumns_.end())
        {
            return SC_E_COLUMN_NOT_FOUND;
        }

        const SCComputedColumnDef& column = *computedIt;
        // Cache identity is scoped to record + computed column; version is retained only as metadata.
        const SCComputedCacheKey cacheKey{recordId, column.name, database_->GetCurrentVersion()};
        if (column.cacheable)
        {
            ErrorCode cacheRc = cache_->TryGet(cacheKey, outValue);
            if (cacheRc == SC_OK)
            {
                return SC_OK;
            }
        }

        SCRecordPtr record;
        ErrorCode rc = table_->GetRecord(recordId, record);
        if (Failed(rc))
        {
            return rc;
        }

        SCRefPtr<TableViewRecordContext> context = SCMakeRef<TableViewRecordContext>(database_.Get(), tableName_, record);
        if (column.kind == ComputedFieldKind::Aggregate)
        {
            SCRecordCursorPtr related;
            rc = context->GetRelated(column.aggregateRelation.c_str(), related);
            if (Failed(rc))
            {
                return rc;
            }
            rc = AggregateCursorValues(column, related, outValue);
        }
        else
        {
            rc = EvaluateComputedColumn(column, context.Get(), ruleRegistry_.Get(), outValue);
        }
        if (Failed(rc))
        {
            return rc;
        }

        if (column.cacheable)
        {
            cache_->Put(SCComputedCacheEntry{cacheKey, column.dependencies, *outValue});
        }
        return SC_OK;
    }

    ErrorCode InvalidateComputedCache() override
    {
        return cache_->Clear();
    }

    void OnDatabaseChanged(const SCChangeSet& SCChangeSet) override
    {
        cache_->Invalidate(SCChangeSet, computedColumns_);
    }

private:
    SCRefPtr<ISCDatabase> database_;
    std::wstring tableName_;
    SCRuleRegistryPtr ruleRegistry_;
    SCTablePtr table_;
    SCSchemaPtr schema_;
    SCComputedCachePtr cache_;
    std::vector<SCComputedColumnDef> computedColumns_;
};

}  // namespace

ErrorCode CreateComputedTableView(
    ISCDatabase* database,
    const wchar_t* tableName,
    ISCRuleRegistry* ruleRegistry,
    SCComputedTableViewPtr& outView)
{
    if (database == nullptr || tableName == nullptr || *tableName == L'\0')
    {
        return SC_E_INVALIDARG;
    }

    SCRefPtr<ComputedTableView> view = SCMakeRef<ComputedTableView>(database, std::wstring{tableName}, ruleRegistry);
    ErrorCode rc = view->Initialize();
    if (Failed(rc))
    {
        return rc;
    }

    outView = view;
    return SC_OK;
}

}  // namespace StableCore::Storage
