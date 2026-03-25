#pragma once

#include "StableCore/Storage/Computed.h"

namespace stablecore::storage
{

class IComputedTableView;

using ComputedTableViewPtr = RefPtr<IComputedTableView>;

struct TableViewColumnDef
{
    TableColumnLayer layer{TableColumnLayer::Fact};
    std::wstring name;
    std::wstring displayName;
    ValueKind valueKind{ValueKind::Null};
    bool editable{false};
};

class IComputedTableView : public virtual IRefObject
{
public:
    virtual ErrorCode GetTableName(std::wstring* outTableName) = 0;
    virtual ErrorCode GetColumnCount(std::int32_t* outCount) = 0;
    virtual ErrorCode GetColumn(std::int32_t index, TableViewColumnDef* outColumn) = 0;
    virtual ErrorCode AddComputedColumn(const ComputedColumnDef& column) = 0;
    virtual ErrorCode EnumerateRecords(RecordCursorPtr& outCursor) = 0;
    virtual ErrorCode GetCellValue(RecordId recordId, const wchar_t* columnName, Value* outValue) = 0;
    virtual ErrorCode InvalidateComputedCache() = 0;
};

ErrorCode CreateComputedTableView(
    IDatabase* database,
    const wchar_t* tableName,
    IRuleRegistry* ruleRegistry,
    ComputedTableViewPtr& outView);

}  // namespace stablecore::storage
