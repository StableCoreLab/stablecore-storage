#include "StableCore/Storage/TableView.h"

#include <algorithm>
#include <cwctype>
#include <sstream>
#include <unordered_map>
#include <utility>

#include "StableCore/Storage/RefCounted.h"

namespace stablecore::storage
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

class TableViewRecordContext final : public IComputedContext, public RefCountedObject
{
public:
    TableViewRecordContext(IDatabase* database, std::wstring tableName, RecordPtr record)
        : database_(database)
        , tableName_(std::move(tableName))
        , record_(std::move(record))
    {
    }

    ErrorCode GetValue(const wchar_t* fieldName, Value* outValue) override
    {
        return record_->GetValue(fieldName, outValue);
    }

    ErrorCode GetRef(const wchar_t* fieldName, RecordId* outValue) override
    {
        return record_->GetRef(fieldName, outValue);
    }

    ErrorCode GetRelated(const wchar_t* relationName, RecordCursorPtr& outCursor) override
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

        TablePtr relatedTable;
        ErrorCode rc = database_->GetTable(parts[0].c_str(), relatedTable);
        if (Failed(rc))
        {
            return rc;
        }

        return relatedTable->FindRecords(QueryCondition{parts[1], Value::FromRecordId(record_->GetId())}, outCursor);
    }

private:
    RefPtr<IDatabase> database_;
    std::wstring tableName_;
    RecordPtr record_;
};

ErrorCode AggregateCursorValues(
    const ComputedColumnDef& column,
    RecordCursorPtr& cursor,
    Value* outValue)
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
        RecordPtr record;
        const ErrorCode currentRc = cursor->GetCurrent(record);
        if (Failed(currentRc))
        {
            return currentRc;
        }

        ++count;
        if (column.aggregateKind == AggregateKind::Count)
        {
            continue;
        }

        Value value;
        ErrorCode valueRc = record->GetValue(column.aggregateField.c_str(), &value);
        if (valueRc == SC_E_VALUE_IS_NULL)
        {
            continue;
        }
        if (Failed(valueRc))
        {
            return valueRc;
        }

        double numeric = 0.0;
        if (value.AsDouble(&numeric) != SC_OK)
        {
            std::int64_t intValue = 0;
            const ErrorCode intRc = value.AsInt64(&intValue);
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
    case AggregateKind::Count:
        *outValue = Value::FromInt64(static_cast<std::int64_t>(count));
        return SC_OK;
    case AggregateKind::Sum:
        *outValue = Value::FromDouble(sum);
        return SC_OK;
    case AggregateKind::Min:
        *outValue = initialized ? Value::FromDouble(minValue) : Value::Null();
        return initialized ? SC_OK : SC_E_VALUE_IS_NULL;
    case AggregateKind::Max:
        *outValue = initialized ? Value::FromDouble(maxValue) : Value::Null();
        return initialized ? SC_OK : SC_E_VALUE_IS_NULL;
    default:
        return SC_E_NOTIMPL;
    }
}

class ComputedTableView final : public IComputedTableView, public IDatabaseObserver, public RefCountedObject
{
public:
    ComputedTableView(IDatabase* database, std::wstring tableName, IRuleRegistry* ruleRegistry)
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

    ErrorCode GetColumn(std::int32_t index, TableViewColumnDef* outColumn) override
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
            ColumnDef fact;
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

        const ComputedColumnDef& computed = computedColumns_[computedIndex];
        outColumn->layer = TableColumnLayer::Computed;
        outColumn->name = computed.name;
        outColumn->displayName = computed.displayName;
        outColumn->valueKind = computed.valueKind;
        outColumn->editable = computed.editable;
        return SC_OK;
    }

    ErrorCode AddComputedColumn(const ComputedColumnDef& column) override
    {
        if (column.name.empty())
        {
            return SC_E_INVALIDARG;
        }
        const auto duplicate = std::find_if(
            computedColumns_.begin(),
            computedColumns_.end(),
            [&](const ComputedColumnDef& existing)
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

    ErrorCode EnumerateRecords(RecordCursorPtr& outCursor) override
    {
        return table_->EnumerateRecords(outCursor);
    }

    ErrorCode GetCellValue(RecordId recordId, const wchar_t* columnName, Value* outValue) override
    {
        if (columnName == nullptr || outValue == nullptr)
        {
            return SC_E_POINTER;
        }

        ColumnDef factColumn;
        const ErrorCode factRc = schema_->FindColumn(columnName, &factColumn);
        if (Succeeded(factRc))
        {
            RecordPtr record;
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
            [&](const ComputedColumnDef& column)
            {
                return column.name == columnName;
            });
        if (computedIt == computedColumns_.end())
        {
            return SC_E_COLUMN_NOT_FOUND;
        }

        const ComputedColumnDef& column = *computedIt;
        const ComputedCacheKey cacheKey{recordId, column.name, database_->GetCurrentVersion()};
        if (column.cacheable)
        {
            ErrorCode cacheRc = cache_->TryGet(cacheKey, outValue);
            if (cacheRc == SC_OK)
            {
                return SC_OK;
            }
        }

        RecordPtr record;
        ErrorCode rc = table_->GetRecord(recordId, record);
        if (Failed(rc))
        {
            return rc;
        }

        RefPtr<TableViewRecordContext> context = MakeRef<TableViewRecordContext>(database_.Get(), tableName_, record);
        if (column.kind == ComputedFieldKind::Aggregate)
        {
            RecordCursorPtr related;
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
            cache_->Put(ComputedCacheEntry{cacheKey, column.dependencies, *outValue});
        }
        return SC_OK;
    }

    ErrorCode InvalidateComputedCache() override
    {
        return cache_->Clear();
    }

    void OnDatabaseChanged(const ChangeSet& changeSet) override
    {
        cache_->Invalidate(changeSet, computedColumns_);
    }

private:
    RefPtr<IDatabase> database_;
    std::wstring tableName_;
    RuleRegistryPtr ruleRegistry_;
    TablePtr table_;
    SchemaPtr schema_;
    ComputedCachePtr cache_;
    std::vector<ComputedColumnDef> computedColumns_;
};

}  // namespace

ErrorCode CreateComputedTableView(
    IDatabase* database,
    const wchar_t* tableName,
    IRuleRegistry* ruleRegistry,
    ComputedTableViewPtr& outView)
{
    if (database == nullptr || tableName == nullptr || *tableName == L'\0')
    {
        return SC_E_INVALIDARG;
    }

    RefPtr<ComputedTableView> view = MakeRef<ComputedTableView>(database, std::wstring{tableName}, ruleRegistry);
    ErrorCode rc = view->Initialize();
    if (Failed(rc))
    {
        return rc;
    }

    outView = view;
    return SC_OK;
}

}  // namespace stablecore::storage
