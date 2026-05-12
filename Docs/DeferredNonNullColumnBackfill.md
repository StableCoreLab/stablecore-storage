# Deferred Non-Null Column Backfill

Current minimal rule:

- Empty tables may add a non-null column without a default.
- Tables with existing live records must still require a default for that case.
- Commit-time validation must reject new records that leave required columns unset.

新增记录时的当前 API 约定：

- `CreateRecord()` 只负责创建草稿记录。
- 若表中存在 `non-null + no default` 字段，调用方必须在提交前显式补齐这些字段。
- 调用方可以通过 `Commit()` 完成保存，或通过 `Rollback()` 放弃本次编辑。
- 在数据库编辑器中，`Add Record` 会在这类表上创建草稿，用户补齐字段后用 `Save Pending Changes` 保存，或用 `Discard Pending Changes` 放弃。

新增记录示例：

```cpp
using namespace StableCore::Storage;

SCDbPtr db;
SCEditPtr edit;
SCTablePtr table;
SCRecordPtr record;
ErrorCode rc = db->BeginEdit(L"Add Record", edit);
if (Failed(rc))
{
    return rc;
}

rc = db->GetTable(L"Beam", table);
if (Failed(rc))
{
    db->Rollback(edit.Get());
    return rc;
}

rc = table->CreateRecord(record);
if (Failed(rc))
{
    db->Rollback(edit.Get());
    return rc;
}

// 对于非空且无默认值的字段，必须在提交前显式赋值。
rc = record->SetInt64(L"Width", 42);
if (Failed(rc))
{
    db->Rollback(edit.Get());
    return rc;
}

rc = db->Commit(edit.Get());
if (Failed(rc))
{
    db->Rollback(edit.Get());
    return rc;
}
```

如果调用方需要取消本次新增，则直接调用 `Rollback()`，不要提交空草稿。

Deferred full semantic version:

- Add an explicit backfill workflow for populated tables.
- Let the user choose a backfill strategy before committing the schema change.
- Keep the schema change and backfill in one explicit recoverable flow.

This document records the deferred work so the current minimal implementation
remains intentionally limited instead of being mistaken for the final behavior.
