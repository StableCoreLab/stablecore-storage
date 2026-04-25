# CreateBackupCopy Level 2 完整开发方案

## 1. 目标定位

`CreateBackupCopy` 只负责创建一个**可打开、可保存、可继续编辑**的工程数据副本。

本阶段定位为 **Level 2：稳定工程副本能力**，不做策略型历史压缩。

```text
CreateBackupCopy = 工程数据复制 + 可选保留 Journal History + 副本清理 + 校验 + 原子落盘
```

---

## 2. 明确不做

本次不实现：

* Replay Log 复制、裁剪、压缩
* Journal 按版本窗口裁剪
* Journal 按条数/大小裁剪
* 多个 transaction 合并
* compact history
* max recovery log bytes / entries
* Memory 后端备份
* Import 恢复链路处理

---

## 3. 核心语义收口

当前代码里的 `Recovery Log` 实际是：

```text
journal_transactions
journal_entries
```

它不是独立恢复日志，也不是 Import 恢复状态，也不是 Replay Log。

因此接口中不应继续使用：

```cpp
preserveRecoveryLog
maxRecoveryLogBytes
maxRecoveryLogEntries
trimmedRecoveryLogCount
```

统一改名为：

```text
Journal History
```

---

# 4. ABI 调整方案

## 4.1 新 SCBackupOptions

建议改为：

```cpp
struct SCBackupOptions
{
    bool preserveJournalHistory = false;
    bool vacuumTarget = true;
    bool validateTarget = true;
    bool overwriteExisting = false;
};
```

字段含义：

| 字段                       | 含义                                                    |
| ------------------------ | ----------------------------------------------------- |
| `preserveJournalHistory` | 是否在备份副本中保留 `journal_transactions` / `journal_entries` |
| `vacuumTarget`           | 是否在副本清理后执行 `VACUUM`                                   |
| `validateTarget`         | 是否对副本执行完整性校验                                          |
| `overwriteExisting`      | 目标文件已存在时是否允许覆盖                                        |

---

## 4.2 删除旧字段

从 `SCBackupOptions` 删除：

```cpp
bool preserveHistory;
bool preserveRecoveryLog;
bool compactHistory;
uint64_t maxRecoveryLogBytes;
uint32_t maxRecoveryLogEntries;
```

原因：

* `preserveHistory` 与 `preserveRecoveryLog` 当前指向同一批 journal 表，语义重复；
* `compactHistory` 暗示 Level 3 能力，本阶段不做；
* `maxRecoveryLogBytes` / `maxRecoveryLogEntries` 当前未实现，且不属于 Level 2；
* `RecoveryLog` 命名误导，会混淆 Undo/Redo Journal、Replay Log、Import Recovery。

---

## 4.3 新 SCBackupResult

建议改为：

```cpp
struct SCBackupResult
{
    uint64_t removedJournalTransactionCount = 0;
    uint64_t removedJournalEntryCount = 0;
    uint64_t outputFileSizeBytes = 0;
};
```

字段含义：

| 字段                               | 含义                              |
| -------------------------------- | ------------------------------- |
| `removedJournalTransactionCount` | 备份副本中删除的 journal transaction 数量 |
| `removedJournalEntryCount`       | 备份副本中删除的 journal entry 数量       |
| `outputFileSizeBytes`            | 最终备份文件大小                        |

---

# 5. 备份行为规则

## 5.1 preserveJournalHistory = true

副本保留：

```text
journal_transactions
journal_entries
```

要求：

* Undo/Redo 历史随副本保留；
* 不修改 baselineVersion；
* 不修改 currentVersion；
* `removedJournalTransactionCount = 0`;
* `removedJournalEntryCount = 0`.

---

## 5.2 preserveJournalHistory = false

副本清空：

```sql
DELETE FROM journal_entries;
DELETE FROM journal_transactions;
```

同时要求：

```text
baselineVersion = currentVersion
undo/redo 历史为空
```

结果统计：

```text
removedJournalTransactionCount = 删除的 transaction 数
removedJournalEntryCount = 删除的 entry 数
```

注意：

不要只删表，不更新 baseline 相关元数据。

---

# 6. SQLite 实现流程

## 6.1 总体流程

```text
1. 校验源数据库已打开
2. 校验目标路径合法
3. 判断目标文件是否存在
4. 生成临时文件路径
5. 删除残留临时文件
6. 使用 SQLite Backup API 复制当前数据库到临时文件
7. 打开临时数据库
8. 如 preserveJournalHistory=false，清理 Journal History
9. 如 vacuumTarget=true，执行 VACUUM
10. 如 validateTarget=true，执行完整性校验
11. 获取输出文件大小
12. 关闭临时数据库
13. 原子替换目标文件
14. 返回 SCBackupResult
```

核心原则：

```text
所有修改只发生在临时副本上。
成功前不覆盖目标文件。
任何失败不得影响源数据库。
```

---

## 6.2 临时文件命名

目标文件：

```text
D:\Project\A.scdb
```

临时文件建议：

```text
D:\Project\.A.scdb.backup.{uuid}.tmp
```

规则：

* 与目标文件在同一目录；
* 使用 UUID 避免并发冲突；
* 成功后 rename/replace 到目标文件；
* 失败后尽量删除临时文件。

---

## 6.3 目标文件覆盖规则

当目标文件存在：

```cpp
if (!options.overwriteExisting)
{
    return SC_E_FILE_EXISTS;
}
```

当允许覆盖：

```text
先生成临时文件
临时文件处理成功后
再原子替换目标文件
```

不要先删除目标文件。

---

# 7. Journal History 清理实现

## 7.1 清理顺序

建议在临时数据库中执行：

```sql
BEGIN IMMEDIATE;

SELECT COUNT(*) FROM journal_entries;
SELECT COUNT(*) FROM journal_transactions;

DELETE FROM journal_entries;
DELETE FROM journal_transactions;

UPDATE metadata
SET value = currentVersion
WHERE key = 'baselineVersion';

COMMIT;
```

实际表名和 metadata 字段以当前代码为准。

---

## 7.2 推荐封装

不要把清理逻辑散落在 `CreateBackupCopy()` 中。

建议新增内部函数：

```cpp
SCResult SCSqliteAdapter::ClearJournalHistoryForBackup(
    sqlite3* db,
    SCBackupResult& result);
```

职责：

* 统计 journal 删除数量；
* 删除 journal_entries；
* 删除 journal_transactions；
* 将 baselineVersion 推到 currentVersion；
* 保证事务内完成。

---

## 7.3 与 ResetHistoryBaseline 的关系

如果已有 `ResetHistoryBaseline()` 逻辑较完整，建议抽出公共内部实现：

```cpp
SCResult SCSqliteAdapter::ResetHistoryBaselineImpl(sqlite3* db);
```

然后：

```cpp
ResetHistoryBaseline()
CreateBackupCopy()
```

都调用该内部实现。

避免出现两套 baseline 重置逻辑。

---

# 8. VACUUM 规则

当：

```cpp
options.vacuumTarget == true
```

在临时数据库上执行：

```sql
VACUUM;
```

要求：

* 必须在 journal 清理之后；
* 必须在完整性校验之前或之后均可，建议在清理之后、校验之前；
* 不允许对源数据库执行 `VACUUM`。

---

# 9. 完整性校验

当：

```cpp
options.validateTarget == true
```

执行：

```sql
PRAGMA integrity_check;
PRAGMA foreign_key_check;
```

要求：

```text
integrity_check 返回 ok
foreign_key_check 返回空结果
```

失败时：

* 关闭临时数据库；
* 删除临时文件；
* 不覆盖目标文件；
* 返回校验失败错误码。

---

# 10. 原子替换策略

建议使用平台封装函数：

```cpp
SCResult ReplaceFileAtomically(
    const std::filesystem::path& tempPath,
    const std::filesystem::path& targetPath,
    bool overwriteExisting);
```

Windows 下可用：

```text
MoveFileEx / ReplaceFile
```

跨平台可用：

```text
std::filesystem::rename
```

但要注意：

* 同目录 rename 才更接近原子；
* 覆盖已有文件时要单独处理；
* 失败不能删除原目标文件。

---

# 11. Memory 后端策略

保持当前行为：

```cpp
SC_E_NOTIMPL
```

并在接口文档中明确：

```text
Memory backend does not support CreateBackupCopy.
```

不要为了接口一致性做伪实现。

---

# 12. 错误处理要求

必须覆盖：

```text
源数据库未打开
目标路径为空
目标目录不存在
目标文件存在但 overwriteExisting=false
临时文件创建失败
SQLite Backup API 失败
临时数据库打开失败
Journal History 清理失败
Baseline 重置失败
VACUUM 失败
integrity_check 失败
foreign_key_check 失败
获取文件大小失败
原子替换失败
```

失败保证：

```text
不影响源数据库
不破坏已有目标文件
尽量清理临时文件
返回明确错误码
```

---

# 13. 推荐错误码

可按现有错误码体系映射，至少区分：

```cpp
SC_E_INVALID_ARGUMENT
SC_E_NOT_OPEN
SC_E_FILE_EXISTS
SC_E_IO_ERROR
SC_E_SQL_ERROR
SC_E_VALIDATION_FAILED
SC_E_NOTIMPL
```

---

# 14. 测试用例

## 14.1 基础备份

```text
创建数据库
写入普通业务数据
调用 CreateBackupCopy
打开副本
验证业务数据一致
```

---

## 14.2 保留 Journal History

```text
创建多次事务
调用 preserveJournalHistory=true
打开副本
验证 journal_transactions 数量不变
验证 journal_entries 数量不变
验证 baselineVersion 不变
```

---

## 14.3 清理 Journal History

```text
创建多次事务
调用 preserveJournalHistory=false
打开副本
验证 journal_transactions 为空
验证 journal_entries 为空
验证 baselineVersion == currentVersion
验证 removedJournalTransactionCount 正确
验证 removedJournalEntryCount 正确
```

---

## 14.4 VACUUM 生效

```text
构造大量 journal 数据
preserveJournalHistory=false
vacuumTarget=true
执行备份
验证副本文件体积小于未 vacuum 副本
```

---

## 14.5 不覆盖已有文件

```text
目标文件已存在
overwriteExisting=false
调用失败
验证目标文件未被修改
```

---

## 14.6 覆盖已有文件

```text
目标文件已存在
overwriteExisting=true
调用成功
验证目标文件内容变为新副本内容
```

---

## 14.7 校验失败路径

```text
模拟临时副本完整性校验失败
验证不覆盖目标文件
验证临时文件被清理
```

---

## 14.8 Memory 后端

```text
Memory backend 调用 CreateBackupCopy
返回 SC_E_NOTIMPL
```

---

# 15. 文档同步要求

需要同步更新：

```text
Include/ISCInterfaces.h
Docs/Quantity/DevGuide/StorageBackup.md
Docs/Quantity/DevGuide/JournalHistory.md
Tests/Sqlite/SCSqliteBackupTests.cpp
Tests/Memory/SCMemoryDatabaseTests.cpp
```

如果当前没有对应文档，可以新增：

```text
Docs/Backup/CreateBackupCopy.md
```

---

# 16. 对 AI / Codex 的开发提示词

```text
你现在需要重构 stablecore-storage 中的 CreateBackupCopy 能力，将其收口为 Level 2 工程副本能力。

核心要求：

1. 调整 SCBackupOptions ABI：
   - 删除 preserveHistory
   - 删除 preserveRecoveryLog
   - 删除 compactHistory
   - 删除 maxRecoveryLogBytes
   - 删除 maxRecoveryLogEntries
   - 新增 preserveJournalHistory
   - 新增 vacuumTarget
   - 新增 validateTarget
   - 新增 overwriteExisting

2. 调整 SCBackupResult：
   - 删除 trimmedRecoveryLogCount
   - 新增 removedJournalTransactionCount
   - 新增 removedJournalEntryCount
   - 新增 outputFileSizeBytes

3. 明确语义：
   - 当前所谓 Recovery Log 实际是 journal_transactions / journal_entries
   - 不再使用 Recovery Log 命名
   - 统一称为 Journal History
   - CreateBackupCopy 不处理 Replay Log
   - CreateBackupCopy 不处理 Import 恢复链路

4. SQLite CreateBackupCopy 实现要求：
   - 使用临时文件生成备份
   - 所有清理只发生在临时副本中
   - preserveJournalHistory=false 时清空 journal_entries / journal_transactions
   - 清空 journal 后将 baselineVersion 更新为 currentVersion
   - vacuumTarget=true 时对临时副本执行 VACUUM
   - validateTarget=true 时执行 PRAGMA integrity_check 和 PRAGMA foreign_key_check
   - 成功后原子替换目标文件
   - 失败时不得影响源数据库和已有目标文件

5. Memory backend：
   - 继续返回 SC_E_NOTIMPL
   - 更新测试和文档说明

6. 测试要求：
   - 覆盖基础备份
   - 覆盖 preserveJournalHistory=true
   - 覆盖 preserveJournalHistory=false
   - 覆盖 removedJournalTransactionCount / removedJournalEntryCount
   - 覆盖 overwriteExisting=false
   - 覆盖 overwriteExisting=true
   - 覆盖 Memory backend NotImplemented

7. 禁止事项：
   - 不实现 compact history
   - 不实现 maxRecoveryLogBytes / maxRecoveryLogEntries
   - 不裁剪 Replay Log
   - 不处理 Import 恢复链路
   - 不在源数据库上执行清理或 VACUUM
   - 不直接覆盖目标文件

完成后同步更新接口注释、开发文档和测试。
```

---

# 17. 最终收口结论

这次重构后，接口语义应变成：

```text
CreateBackupCopy 只创建工程数据副本。
Journal History 可选保留或清空。
Replay Log 不属于 CreateBackupCopy。
Import Recovery 不属于 CreateBackupCopy。
Recovery Log 命名从 Backup 接口中移除。
Level 2 不提供策略型历史裁剪。
```

最终目标：

```text
稳定、可控、原子、安全的工程副本能力。
```
