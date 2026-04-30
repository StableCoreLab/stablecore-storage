# DatabaseEditor 字段类型转换与备份入口完善

## 1. 任务标题
为 `SCStorageDatabaseEditor` 补充普通字段与会话级计算字段的显式转换流程，并完善备份复制入口。

---

## 2. 背景（Context）

当前 `SCStorageDatabaseEditor` 已支持：

- `Add Column` 创建普通 Schema 字段
- `Add Computed Column` 创建会话级计算字段
- `Edit Column` 仅支持普通字段的原地编辑
- `CreateBackupCopy` 已在底层后端实现，但 GUI 侧缺少稳定入口与会话封装

目前存在的问题：

- 普通字段与会话级计算字段之间没有明确、显式、可恢复的转换流程
- 字段编辑缺少对“普通字段 <-> 计算字段”语义的统一入口
- 备份复制能力虽已存在，但 GUI 可用性和错误提示仍需收口

影响范围：

- `Tools/DatabaseEditor`
- `Include/ISCInterfaces.h`
- `Src/Memory`
- `Src/Sqlite`
- `Tests`

任务类型：

- [ ] Bug 修复
- [ ] 小范围重构
- [x] 架构调整
- [x] 新能力开发

---

## 3. 目标（Goals）

本次必须达成的目标：

1. 为普通字段与会话级计算字段之间的转换提供显式 GUI 入口。
2. 将转换流程收敛到 `SCDatabaseSession`，保证可恢复、可回滚、可诊断。
3. 为 `CreateBackupCopy` 提供编辑器侧入口和一致的错误反馈。

---

## 4. 非目标（Non-Goals）

本次明确不做的事情：

- 不做完整 Schema 重命名迁移
- 不做批量字段转换
- 不做复杂的字段差异补丁式编辑
- 不做 UI 大改版
- 不引入第三方库

---

## 5. 修改边界（Scope）

允许修改：

- `Include/ISCInterfaces.h`
- `Src/Memory/SCMemoryDatabase.cpp`
- `Src/Sqlite/SCSqliteAdapter.cpp`
- `Tools/DatabaseEditor/SCDatabaseSession.h/.cpp`
- `Tools/DatabaseEditor/SCDatabaseEditorMainWindow.h/.cpp`
- `Tools/DatabaseEditor/SCAddColumnDialog.h/.cpp`
- `Tests/M1Tests.cpp`
- `Tests/M2SqliteTests.cpp`
- `Tests/DatabaseEditorSessionTests.cpp`
- `Docs/*.md` 中与 DatabaseEditor 相关的说明文档

禁止修改：

- 与本任务无关的 Query / Migration / Diagnostics 核心逻辑
- 未声明的 UI 模块
- 生成目录或构建产物

---

## 6. 设计约束（Constraints）

必须遵守：

- 单一真值源：正式数据仍以 Project / ChangeSet / Journal 为准
- 显式边界：所有状态变更通过显式 API 触发
- 失败可恢复：转换失败必须可回滚
- Memory / SQLite 行为一致
- 读不写、删不复活、只读模式不修改数据

额外约束：

- 普通字段与会话级计算字段之间的转换必须是显式动作，不可隐藏在通用“编辑字段”里
- 允许转换时要先清空该列现有数据
- 转换失败时必须保留原字段状态和原视图状态

---

## 7. 业务规则（Rules）

### 7.1 普通字段编辑规则

对于 `Add Column` 创建的普通字段：

- `Null` 永远允许
- 同类型允许
- `Int64 -> Double/String` 允许
- `Bool -> String` 允许
- `RecordId -> String` 允许
- `String -> 其它类型` 仅在严格解析成功时允许
- 其余组合默认拒绝，并返回明确提示

### 7.2 普通字段 <-> 计算字段转换规则

- `普通字段 -> 计算字段`
  - 允许
  - 转换前清除该列现有数据
  - 删除普通字段定义，再创建同名会话级计算字段

- `计算字段 -> 普通字段`
  - 允许
  - 先删除会话级计算字段
  - 再创建同名普通字段

- 若转换过程中出现不可恢复失败，必须回滚到转换前状态

---

## 8. 方案摘要（Implementation Plan）

### 8.1 GUI 入口

在 `SCStorageDatabaseEditor` 中提供以下入口：

- `Edit Column...`
  - 仅编辑普通字段元数据
  - 不做跨类型转换

- `Convert to Computed...`
  - 选中普通字段后可用
  - 转为会话级计算字段
  - 弹出清数据确认提示

- `Convert to Column...`
  - 选中计算字段后可用
  - 转为普通字段
  - 必要时提示会清除对应同名字段数据

- `Create Backup Copy...`
  - 从菜单和工具栏提供入口
  - 直接调用底层 `ISCDatabase::CreateBackupCopy`

### 8.2 `SCDatabaseSession` 处理流程

建议新增显式接口：

- `UpdateColumn(...)`
- `ConvertColumnToComputed(...)`
- `ConvertComputedToColumn(...)`
- `CreateBackupCopy(...)`

处理顺序应收口为同一事务状态机：

1. 预检输入与当前状态
   - 检查目标字段是否冲突
   - 检查转换规则、依赖关系和当前会话状态
   - 这一阶段只读，不进入编辑边界
2. 预构建视图
   - 在不落库的前提下验证转换后的 `ComputedTableView` 是否可重建
   - 若预览失败，直接返回错误
3. 进入显式编辑边界
   - `BeginEdit` 之后，schema / value / journal 变更必须通过同一上下文执行
4. 应用 schema / value / journal 变更
   - 普通字段编辑：应用 schema delta，必要时迁移值，并记录 schema journal
   - 普通字段 <-> 计算字段互转：先执行结构性清理，再替换定义
   - `RemoveColumn(...)` 只作为内部结构性动作使用，不作为用户可撤销删列能力暴露
5. 重建当前 `ComputedTableView`
   - 仅当当前事务内的状态已经一致时才允许继续
6. 提交或回滚
   - 提交成功后再同步会话历史栈和 UI 状态
   - 任一步失败都必须回滚同一事务上下文，并恢复旧 schema、旧 values、旧 computed 列列表和旧视图

### 8.3 后端收口

- `ISCSchema` 保留原地更新能力，供普通字段编辑使用
- `Memory` 和 `SQLite` 必须在同一事务语义下实现 schema / value / journal 变更
- 后端内部应通过统一的 mutation scope 记录旧状态、应用新状态并在失败时恢复旧状态
- SQLite 更新字段时不能静默吞掉索引失败，也不能把提交前的内存缓存改动遗留到失败路径中

### 8.4 事务状态机收口（整合版）

本次修改方案最终应收敛为以下状态机：

| 状态 | 允许做的事 | 失败后的要求 |
|---|---|---|
| `Preflight` | 只读校验、依赖检查、冲突检查 | 直接返回，不进入编辑边界 |
| `Preview` | 构建预览视图，验证转换后 schema 是否可用 | 直接返回，保持原状态 |
| `BeginEdit` | 打开显式编辑边界，初始化 mutation scope | 若失败，不修改任何缓存 |
| `ApplySchemaDelta` | 变更 schema 定义 | 失败时恢复旧 schema 对象 |
| `ApplyValueMigration` | 迁移或清理受影响记录值 | 失败时恢复旧值 |
| `RecordJournal` | 写入 schema / value journal | 失败时回滚整个编辑上下文 |
| `RebuildView` | 重建当前 `ComputedTableView` | 失败时回滚并恢复旧视图 |
| `Commit` | 提交底层事务并同步历史栈 | 仅在底层提交成功后更新 undo/redo 栈 |
| `Rollback` | 回退所有已应用变更 | 必须恢复 schema、values、journal、view 四类状态 |

核心原则：

- 一个编辑动作只能有一个事务上下文
- 不允许“先改缓存，再补 journal”
- 不允许“先写 journal，再赌后续提交成功”
- 不允许用户入口直接依赖 `RemoveColumn()` 作为可撤销删列能力

---

## 9. 测试要求（Testing）

必须补充覆盖：

- 普通字段编辑成功
- 不兼容字段类型变更被拒绝
- `String -> 其它类型` 严格解析成功 / 失败
- 普通字段 -> 计算字段的清数据与回滚
- 计算字段 -> 普通字段的转换与回滚
- 转换失败后当前 schema / computed 列表 / 视图仍保持旧状态
- `CreateBackupCopy` 入口可用，且副本可重新打开
- Memory / SQLite 两个后端一致性

建议测试文件：

- `Tests/M1Tests.cpp`
- `Tests/M2SqliteTests.cpp`
- `Tests/DatabaseEditorSessionTests.cpp`

---

## 10. 文档同步（Docs）

本次需要同步的文档：

- `Docs/DatabaseEditorGuiDesign.md`
- `Docs/DatabaseEditorGuiImplementationScope.md`
- `Docs/DatabaseEditorComputedColumnLifecycle.md`
- `Docs/当前实现状态.md`
- `Docs/NeedUpdateDocs.md`
- `Tools/DatabaseEditor/README.md`

---

## 11. 风险（Risks）

已知风险：

- 字段类型变更后的历史数据语义可能不一致
- 普通字段与计算字段互转会引入数据清理行为，必须明确提示用户
- SQLite 后端若索引更新失败，可能导致物理存储与内存镜像偏离

缓解策略：

- 所有转换前做预检
- 转换过程使用统一编辑边界
- 后端失败必须显式返回错误并回滚

---

## 12. 执行方式要求（For Codex）

必须按以下顺序执行：

1. 先分析边界
2. 再输出修改方案
3. 再写代码
4. 再补测试
5. 最后做静态自检

禁止跳步。
## Implementation Note

- `Edit Column...` is no longer metadata-only: compatible existing values are migrated, unsupported conversions are rejected.

## Schema Journal Closure

- `AddColumn(...)`, `UpdateColumn(...)`, and `RemoveColumn(...)` now emit schema journal entries alongside any value migration.
- Schema journal entries are persisted in `journal_schema_entries` and loaded back into the undo/redo stacks on open.
- Schema undo/redo preserves the original column order when a removed column is restored.
- Undo / Redo and rollback replay schema delta and value migration in the same transaction boundary.
- `RemoveColumn(...)` must not be exposed as a user-facing, undoable delete capability; it stays as an internal structural action for conversion / rollback closure only.
- Recovery order is now deterministic:
  1. prebuild the post-edit view
  2. enter edit
  3. apply schema delta
  4. apply value migration
  5. commit
  6. on failure, rollback the edit and restore the previous schema/view pair
- This keeps the editor contract aligned with the core storage contract: no half-success schema edits and no unjournaled schema mutation.

## Final Transaction Closure

This section is the final implementation contract for the current task. It is intended to eliminate the remaining failure-window bugs in one pass.

### Hard constraints

- The failure recovery path must be **pre-image based**, not best-effort reloading from `field_values`.
- The pre-image snapshot must be captured **before the first mutation** to schema, record cache, or persisted field values.
- `RemoveColumn(...)` is an internal structural operation only; it must not become a user-visible undoable delete capability.
- `ApplyColumnMutation(...)` and `BeginAndCommitSingleAction(...)` must share the same rollback helper and error ordering.
- A rollback failure is more important than the original business error and must be surfaced explicitly.

### Problem 1 closure: `PersistRemovedColumn(...)`

- Add an internal `ColumnRemovalSnapshot` that captures:
  - the previous schema column definition
  - the target table
  - the affected record values for the removed column
  - the set of records that are deleted, active, or pending in the current edit
- Wrap `PersistRemovedColumn(...)` in a RAII scope that:
  - captures the snapshot before deletion
  - applies the schema/value deletion inside the transaction
  - restores the pre-image on transaction failure or exception
- `ReloadColumnValuesFromStorage(...)` may remain as a diagnostic/auxiliary helper, but it must not be the primary recovery path.
- The replay path may keep legacy compatibility for old journals, but the current edit path must not generate a user-facing undoable `RemoveColumn` capability.

### Problem 2 closure: `ApplyColumnMutation(...)` / `BeginAndCommitSingleAction(...)`

- Introduce one rollback helper, for example `RollbackEditAndReport(...)`.
- The helper must be used by:
  - `ApplyColumnMutation(...)`
  - `BeginAndCommitSingleAction(...)`
- Both failure branches must route through the helper:
  - mutation failure
  - commit failure
- The helper must:
  - call rollback
  - check rollback return code
  - return rollback failure if rollback fails
  - keep the primary error as context
- No call site may swallow rollback failure or return only the original business error when rollback itself failed.

### Session flow contract

- `Add Column`, `Edit Column`, `Convert to Computed`, and `Convert to Column` must all call the same mutation helper.
- The mutation helper is responsible for:
  1. begin edit
  2. apply schema delta / value migration / computed-column list changes
  3. build the post-mutation preview view
  4. commit
  5. rollback + restore pre-image on any failure
- Entry functions must not keep their own custom rollback logic.

### Test closure

- `M2SqliteTests.cpp`
  - verify failed `RemoveColumn(...)` restores schema and cached values, including deleted-record edge cases
  - verify failed `UpdateColumn(...)` value rewrite restores schema, cached values, and active edit rollback behavior
  - verify failed `ResetHistoryBaseline(...)` commit does not publish a cleared baseline / undo / redo view
  - verify rollback failure is surfaced as a distinct error path
- `DatabaseEditorSessionTests.cpp`
  - verify mutation failure and commit failure both report rollback failure explicitly
  - verify all four column mutation entry points use the shared helper path

### Acceptance criteria

- No half-success column removal state
- No swallowed rollback failure
- No split recovery path between `ApplyColumnMutation(...)` and `BeginAndCommitSingleAction(...)`
- No reliance on `field_values` reload as the main restore mechanism for `RemoveColumn(...)`
- SQLite must journal migrated column values during `UpdateColumn(...)`, and reopened backups must report the same `currentVersion` that was committed before backup creation.
- Failed `UpdateColumn(...)` value migration must leave `schema`, cached record values, and `activeJournal_` at the pre-image checkpoint.
- Failed `ResetHistoryBaseline(...)` commit must leave `baselineVersion`, `undoStack`, and `redoStack` unchanged in memory.
- Memory and SQLite remain behaviorally aligned on failure and recovery
