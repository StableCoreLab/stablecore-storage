#pragma once

#include "StableCore/Storage/ISCComputed.h"

namespace StableCore::Storage
{

class ISCComputedTableView;

using SCComputedTableViewPtr = SCRefPtr<ISCComputedTableView>;

struct SCTableViewColumnDef
{
    TableColumnLayer layer{TableColumnLayer::Fact};
    std::wstring name;
    std::wstring displayName;
    ValueKind valueKind{ValueKind::Null};
    bool editable{false};
};

class ISCComputedTableView : public virtual ISCRefObject
{
public:
    virtual ErrorCode GetTableName(std::wstring* outTableName) = 0;
    virtual ErrorCode GetColumnCount(std::int32_t* outCount) = 0;
    virtual ErrorCode GetColumn(std::int32_t index, SCTableViewColumnDef* outColumn) = 0;
    virtual ErrorCode AddComputedColumn(const SCComputedColumnDef& column) = 0;
    virtual ErrorCode EnumerateRecords(SCRecordCursorPtr& outCursor) = 0;
    virtual ErrorCode GetCellValue(RecordId recordId, const wchar_t* columnName, SCValue* outValue) = 0;
    virtual ErrorCode InvalidateComputedCache() = 0;
};

ErrorCode CreateComputedTableView(
    ISCDatabase* database,
    const wchar_t* tableName,
    ISCRuleRegistry* ruleRegistry,
    SCComputedTableViewPtr& outView);

}  // namespace StableCore::Storage
