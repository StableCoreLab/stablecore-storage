# CreateRecord 记录生命周期修复方案

## 1. 问题背景

标准的数据库编辑流程应当是：调用方先创建记录对象，随后填写字段，最后提交编辑事务。例如：

```cpp
SCEditPtr edit;
ErrorCode rc = db->BeginEdit(L"Add Record", edit);
if (Failed(rc))
{
    return rc;
}

SCRecordPtr record;
rc = table->CreateRecord(record);
if (Failed(rc))
{
    db->Rollback(edit.Get());
    return rc;
}

rc = record->SetValue(L"Id", SCValue::FromString(L"ET-001"));
if (Failed(rc))
{
    db->Rollback(edit.Get());
    return rc;
}

rc = record->SetValue(L"Name", SCValue::FromString(L"梁类型"));
if (Failed(rc))
{
    db->Rollback(edit.Get());
    return rc;
}

rc = db->Commit(edit.Get());
```

当前 `CreateRecord()` 在创建记录后、返回调用方之前执行了完整约束校验。
对于主键必填的表，创建出来的记录此时主键仍为空，因此会立即返回约束违反错误，调用方没有机会通过 `SetValue(...)` 填写主键。

这使 `CreateRecord()` 的生命周期与“创建对象 -> 填写字段 -> 提交”的正常使用方式不一致。

## 2. 修复目标

将 `CreateRecord()` 调整为“创建待编辑记录”操作：

1. 创建记录对象并返回给调用方。
2. 允许记录在当前编辑事务中暂时缺少主键、非空字段或其他完整约束值。
3. 允许调用方通过 `SetValue(...)` 填写和修改记录。
4. 在外层 `Commit()` 阶段统一校验所有记录的完整约束。
5. 提交失败时不产生部分成功，当前编辑事务保持活动，调用方可以继续修正或执行 `Rollback()`。
6. 提交成功后，记录才成为满足 Schema 约束的持久化状态。

必须继续满足存储层约束：

- 不允许部分提交。
- 失败提交必须可以完整回滚。
- 不能让无效记录被持久化。
- 不能破坏 Undo / Redo 的记录身份一致性。
- SQLite 细节不能泄漏到公共接口。

## 3. 推荐接口语义

### 3.1 `CreateRecord()`

`CreateRecord()` 应负责创建记录对象和登记创建操作，不负责要求记录在此刻已经满足全部业务约束。

允许的操作顺序为：

```cpp
SCEditPtr edit;
ErrorCode rc = db->BeginEdit(L"Add Record", edit);
if (Failed(rc))
{
    return rc;
}

SCRecordPtr record;
rc = table->CreateRecord(record);
if (Failed(rc))
{
    db->Rollback(edit.Get());
    return rc;
}

rc = record->SetValue(L"PrimaryKey", primaryKeyValue);
if (Failed(rc))
{
    db->Rollback(edit.Get());
    return rc;
}

rc = record->SetValue(L"Description", descriptionValue);
if (Failed(rc))
{
    db->Rollback(edit.Get());
    return rc;
}

rc = db->Commit(edit.Get());
```

调用 `CreateRecord()` 时仍应立即检查以下基础条件：

- 当前存在活动编辑事务。
- 表对象有效。
- 表至少存在一个可用列。
- 记录 ID 成功分配且在当前数据库中唯一。
- 创建操作能够正确写入当前编辑事务的日志/变更集。

还应明确以下边界：

- 空表仍不能创建记录；表必须至少有一个可用列。
- 当前实现中，未显式写入的字段通过 `GetValue()` 返回 Schema 默认值；本次不改变默认值的物化或持久化策略。
- 当前 Schema 未定义自动生成主键能力；主键值仍由调用方通过 `SetValue()` 明确填写。
- RecordId 分配属于当前事务状态；回滚后记录不可见，Undo / Redo 时仍须遵守既有身份一致性规则。
- `CreateRecord()` 成功只表示记录已进入当前编辑事务，不表示记录已经持久化。

以下约束不应在 `CreateRecord()` 阶段要求最终满足：

- 主键非 NULL。
- 非空列约束。
- 唯一性约束。
- Check 约束。
- 外键引用完整性。

### 3.2 `SetValue()`

`SetValue()` 仍可执行字段级即时校验，例如：

- 列名是否存在。
- 值类型是否与列类型兼容。
- RecordId 值格式是否有效。
- 默认值和引用表元数据是否满足基本类型要求。

但对于新建记录，不应因为主键暂时为空或某个非空字段尚未填写而阻止继续编辑。

对于唯一、Check、外键等依赖完整行状态的约束，新增记录的 `SetValue()` 阶段不得将暂时不完整的记录判定为最终失败；最终结果必须以 `Commit()` 阶段的统一校验为准。

### 3.3 `Commit()`

`Commit()` 是新建记录的最终约束校验边界。提交前应：

1. 收集当前编辑事务涉及的新增、修改和删除记录。
2. 对提交候选状态执行完整 Schema 校验。
3. 检查主键、非空、唯一、Check、外键等约束。
4. 检查新增记录之间以及新增记录与已有记录之间的冲突。
5. 所有校验通过后写出事务日志和持久化状态。
6. 约束校验失败时不得产生持久化部分成功；如果底层编辑事务仍然有效，应保留编辑事务及内存中的编辑值，允许修正后再次提交。只有调用 `Rollback()` 时才结束并撤销本次编辑事务。

提交失败时必须：

- 返回明确的约束错误码。
- 保留当前编辑事务为活动状态。
- 保留调用方持有的记录对象和已填写字段。
- 不将部分字段或部分记录写入最终数据库状态。
- 允许调用方修正后再次调用 `Commit()`。

提交失败需要区分两类情况：

- 候选状态校验失败：事务保持活动，记录和已填写字段保留，调用方可以修正后再次提交。
- SQLite 写入、日志写入或底层事务提交失败：必须先判断底层事务是否仍处于可继续编辑状态。若状态不确定或不可恢复，必须执行安全回滚或进入明确的失败状态，不能让调用方继续使用可能不一致的事务。

因此，“失败后保留事务”只适用于确认可继续编辑的校验失败，不能无条件适用于所有错误码。

建议在现有一致性丢失机制上扩展内部状态机，不将 SQLite 细节暴露到公共 API：

```text
Active
  ├─ 约束校验失败 -> Active（允许继续 SetValue / Commit）
  ├─ 持久化失败 -> Poisoned（只允许 Rollback/Recover）
  └─ Commit 成功 -> Committed

Poisoned
  ├─ Rollback/Recover 成功 -> RolledBack（数据库恢复可用）
  └─ Rollback/Recover 失败 -> DatabaseFaulted（仅允许释放并重新打开数据库）
```

实现要求：

- 使用内部 `activeEditPoisoned_` 标记当前事务的临时 `Poisoned` 状态；普通写入、再次 `Commit()`、Undo、Redo 和新的 `BeginEdit()` 在该状态下必须拒绝。
- `Rollback()` 必须被允许处理 `Poisoned` 状态，不能被普通写入限制拦截；成功后清理活动日志、内存记录和引用索引，并将编辑会话标记为 `RolledBack`。
- 当 `Rollback()` 或恢复失败时，必须经由现有 `FinalizeReplayFailure(...)` 或等价的统一入口设置 `replayCompensationFailureDetected_`。该既有一致性丢失状态就是 `DatabaseFaulted` 的唯一内部真实来源，不再新增并行的 `databaseFaulted_` 标记。
- 进入 `DatabaseFaulted` 后采用严格隔离：除 `GetEditingState()` 和 `GetEditLogState()` 外，所有会读取、访问、修改或依赖数据库一致性的 `ErrorCode` 公共 API 都通过统一的 `EnsureUsable()` 返回 `SC_E_DATABASE_FAULTED`；调用方只能释放该数据库对象并重新打开数据库。触发故障的原始操作仍返回其原始 I/O、提交或恢复错误，不被 `SC_E_DATABASE_FAULTED` 覆盖。
- `GetCurrentVersion()`、`GetSchemaVersion()` 等无错误码的快照查询不得在 `DatabaseFaulted` 状态访问 SQLite，只能返回最后一次已确认的内存快照；调用方通过后续 `ErrorCode` API 返回的 `SC_E_DATABASE_FAULTED` 识别故障状态。
- `GetEditingState()` 和 `GetEditLogState()` 是诊断例外：在 `DatabaseFaulted` 状态仍可返回最后确认的纯内存快照，`GetEditingState()` 必须设置 `consistencyLost = true`；它们不得访问 SQLite，也不得允许任何写入。
- `txn.Commit()` 失败时，后端必须先基于实际事务状态决定是继续执行内存恢复还是直接进入恢复失败路径，不能假定 SQLite 已经回滚或仍处于活动事务。
- `SqliteTxn` 析构回滚后，仍必须验证内存中的记录、活动日志和引用索引已恢复到事务开始前的状态，不能只依赖 SQLite 自动回滚。

新增错误码固定为：

```cpp
constexpr ErrorCode SC_E_DATABASE_FAULTED = 0xA001030DL;
```

其分类为“约束与存储错误”。该错误只表示恢复失败后的永久连接状态，不替代导致故障的原始 `SC_E_IO_ERROR`、`SC_E_FAIL` 或提交错误。

## 4. 删除重复的草稿接口

修复后删除 `CreateDraftRecord()`，不再保留兼容别名。

原因是 `CreateRecord()` 本身就应当表示“在当前编辑事务中创建一条可继续填写的记录”。如果同时保留 `CreateDraftRecord()`，会造成两个名称表达相同生命周期，增加公共 API 和调用方的理解成本。

所有调用方统一使用：

```cpp
SCEditPtr edit;
db->BeginEdit(L"Add Record", edit);

SCRecordPtr record;
table->CreateRecord(record);
record->SetValue(L"PrimaryKey", primaryKeyValue);
```

当前仓库检索结果表明，`CreateDraftRecord()` 只有数据库编辑器使用，未被其他业务代码或测试使用。因此本次可以直接删除该公共接口，不需要在仓库内增加兼容别名。

该结论仅覆盖当前仓库。仓库外调用方不在本次检索和兼容性验证范围内；如果该公共头文件已被外部项目使用，发布前仍需执行外部调用方检查或提供版本迁移说明。

数据库编辑器中的唯一调用点改为 `CreateRecord()`，并继续在同一个活动编辑事务中填写字段和提交。

## 5. 后端实现调整

### 5.1 创建阶段

调整 `SqliteTable::CreateRecord()`：

- 创建 `SqliteRecordData`。
- 分配并登记 RecordId。
- 写入创建日志。
- 标记记录为当前事务中的新增记录。
- 不执行要求完整字段的最终约束校验。
- 返回 `SCRecordPtr`。

当前用于区分延迟校验的内部标志应统一为普通新增记录的事务状态。删除 `CreateDraftRecord()` 后，所有新增记录都通过 `CreateRecord()` 进入待提交状态。

### 5.2 写值阶段

新增记录调用 `SetValue()` 时：

- 更新内存中的记录值。
- 记录字段变更日志。
- 保持创建操作和字段修改处于同一个外层编辑事务中。
- 不因主键或非空字段暂缺而删除记录或结束事务。

如果类型校验失败，只拒绝本次字段写入，不能破坏仍在编辑中的记录草稿。

对新增记录，`SetValue()` 的即时约束处理必须覆盖两类情况：

- 记录仍然缺少主键或其他必填值时，允许继续写入；
- 主键或其他字段已经填写后，即使已经可以判断出 Unique、Check 或 ForeignKey 冲突，也将最终错误统一延迟到 `Commit()`，并保留本次字段值。

编辑器如需提前提示冲突，可以执行只读诊断，但诊断结果不能结束事务，也不能替代 `Commit()` 的最终校验。

延迟完整约束仅适用于当前活动编辑事务中新建的记录，应根据活动日志的 `createdInActiveEdit` 状态派生，而不是扩大到已有记录。已有记录的 `SetValue()` 必须继续保持现有的即时 Unique、Check 和 ForeignKey 校验语义。

### 5.3 提交阶段

将完整约束检查集中到所有公共 `Commit()` 路径：

- 普通新增记录必须参与完整校验。
- 原有记录的修改和删除继续参与完整校验。
- 删除记录不参与唯一性比较，但删除引发的外键动作仍需按 Schema 规则执行。
- 多条新增记录之间必须互相检查主键、唯一键和外键关系。
- 不扩展 `SCConstraintKind`。它只表示可定义、可保存和可由 `ISCSchema::AddConstraint()` 管理的显式 Schema 约束类型；列级 `nullable = false` 不是独立的可增删约束。
- 为 `SCConstraintViolationInfo` 增加独立的违规来源字段：

  ```cpp
  enum class SCConstraintViolationSource
  {
      SchemaConstraint,
      RequiredColumnValue,
  };

  struct SCConstraintViolationInfo
  {
      // 保持所有既有字段顺序不变。
      std::wstring tableName;
      std::wstring constraintName;
      SCConstraintKind kind{SCConstraintKind::PrimaryKey};
      std::vector<std::wstring> columns;
      RecordId recordId{0};
      RecordId referencedRecordId{0};
      std::wstring reason;

      // 必须追加在末尾，保持既有位置参数聚合初始化兼容。
      SCConstraintViolationSource source{
          SCConstraintViolationSource::SchemaConstraint};
  };
  ```

- 禁止插入、重排或重命名 `SCConstraintViolationInfo` 的既有字段；现有 PrimaryKey、Unique、ForeignKey 和 Check 的位置参数聚合初始化必须保持 `source = SchemaConstraint` 默认值。
- `ValidateRequiredValuesForCommit()` 发现普通非空字段缺失时，先按既有字段顺序构造违规信息，再显式设置 `source = RequiredColumnValue`、表名、记录 ID、缺失列和失败原因；此时 `constraintName` 为空，`kind` 不应被 UI 解释为新增的 NotNull 约束。
- 显式主键、唯一、外键和 Check 失败保持 `source = SchemaConstraint`，继续使用现有 `kind` 和 `constraintName`。主键字段为空时，若由主键约束校验产生，则保持 `SchemaConstraint + PrimaryKey`；若由通用必填扫描产生，则使用 `RequiredColumnValue` 并在 `reason` 中说明该列属于主键。
- 提交成功后清除“延迟校验/草稿”状态。

`Commit()` 的最终约束校验必须在任何 SQLite 持久化写入前完成，并以当前活动编辑事务的候选内存状态为唯一输入。现有 `ValidateTableConstraints()` 只由 `WriteValue()` 调用，不能作为延迟新增记录的最终保障；必须新增或抽取提交期校验入口，并按以下顺序完成：

1. `ValidateRequiredValuesForCommit()`；
2. 主键和唯一约束校验；
3. Check 约束校验：遍历活动事务涉及表中的未删除候选记录，对每个 `SCConstraintKind::Check` 调用既有 `ValidateCheckConstraint(...)`；
4. 外键及删除/更新引发的引用完整性校验。

Check 校验失败必须像其他显式 Schema 约束一样写入 `SCConstraintViolationInfo`，使用 `source = SchemaConstraint`、原有 `kind = Check`、约束名、记录 ID 和涉及列。不得在 `SetValue()` 已对新建记录跳过 Check 后，再遗漏 `Commit()` 阶段的补偿校验。

### 5.4 持久化尝试与失败恢复边界

最终校验全部通过后，才允许进入 SQLite 持久化尝试期。该边界从第一次可能修改 SQLite 状态的调用开始（包括 `PersistDeferredSchemaOps()`、`PersistTouchedRecords()`、删除 Redo 日志、写入 Journal、`SaveMetadata()` 和 `txn.Commit()`），直到提交成功并完成内存状态收尾为止。

- 不得在 `PersistDeferredSchemaOps()` 之后再返回约束校验错误；否则 SQLite 事务析构虽可回滚磁盘写入，内存中的 Schema、活动日志或索引仍可能保留候选变更。
- 持久化尝试期内任一错误码、异常或 `txn.Commit()` 失败，都必须进入同一个内部失败收尾入口；不能只对 `txn.Commit()` 单独处理。
- 收尾入口必须先确定 SQLite 事务实际状态；再完整恢复内存记录、Schema、活动日志、引用索引和版本快照到事务开始前状态。仅当这一恢复被明确验证成功时，才允许将编辑事务恢复为可继续使用或由 `Rollback()` 正常结束。
- 无法证明恢复完整时，设置 `activeEditPoisoned_`，拒绝普通读写、再次 `Commit()`、Undo、Redo 和新的 `BeginEdit()`；仅允许 `Rollback()` / Recover。若恢复或回滚失败，统一经 `FinalizeReplayFailure(...)` 进入 `DatabaseFaulted`。

实现时应将“最终校验”和“持久化尝试”拆成明确的两个阶段，并为第二阶段建立单一退出路径，避免 `PersistDeferredSchemaOps()`、`PersistTouchedRecords()`、Journal、元数据和 `txn.Commit()` 各自早退而遗漏内存恢复。

不能只在数据库编辑器的辅助函数中做校验；所有使用 `CreateRecord()` 的调用方都必须通过公共 `Commit()` 获得相同的最终约束保证。

## 6. 删除编辑器专用保存/放弃接口

删除以下编辑器专用接口：

- `SavePendingChanges()`
- `DiscardPendingChanges()`

数据库编辑会话负责持有当前的 `SCEditPtr`，但不再提供 `SavePendingChanges()` 或 `DiscardPendingChanges()` 这两个重复包装接口。建议提供语义中性的当前事务操作入口，由会话层负责事务指针生命周期和错误转换：

```cpp
bool CommitCurrentEdit(QString* outError);
bool RollbackCurrentEdit(QString* outError);
```

这两个入口不是新的存储事务语义，只是会话层对当前 `SCEditPtr` 的生命周期管理。其内部必须直接调用公共事务接口：

- 保存新增记录：`db->Commit(edit.Get())`
- 放弃新增记录：`db->Rollback(edit.Get())`

这样可以避免编辑器层重新包装一套与存储层 `Commit()` / `Rollback()` 重复的事务语义。主窗口调用会话层的 `CommitCurrentEdit()` / `RollbackCurrentEdit()`，不直接访问数据库内部状态；会话层负责事务指针的生命周期、界面刷新和错误传递。

对于默认的逐行自动保存模式，应移除原有的保存/放弃按钮，因为保存由切换行触发、放弃由 `Esc` 触发。对于批量原子提交模式，如需保留操作按钮，应由会话层直接触发公共 `Commit()` / `Rollback()`，按钮名称使用“提交本批次”和“放弃本批次”，不再使用 `SavePendingChanges` 或 `DiscardPendingChanges` 命名。

## 7. 数据库编辑器流程

修复后，编辑器的用户流程应为：

1. 用户点击 `Add Record`。
2. 编辑器调用 `CreateRecord()`。
3. 网格中显示带 `*` 标记的新增行。
4. 用户填写主键及其他字段。
5. 用户切换到其他行时，编辑器调用会话层的 `CommitCurrentEdit()`，由其内部转发到公共 `db->Commit(edit.Get())` 并统一校验。
6. 校验成功后移除 `*` 标记。
7. 校验失败时显示错误，恢复当前编辑行，保留草稿供用户修正。
8. 用户按 `Esc` 时调用会话层的 `RollbackCurrentEdit()`，由其内部转发到公共 `db->Rollback(edit.Get())`，回滚本次新增。

`*` 仅是编辑器视图中的未提交状态提示，不属于存储层事实，不能作为存储层 `Commit()`、`Rollback()`、Undo、Redo 或恢复判断的依据。

默认逐行自动保存模式不再显示 `Save New Record` 和 `Discard New Record` 按钮。批量模式可以显示“提交本批次”和“放弃本批次”，但它们只是对公共 `Commit()` / `Rollback()` 的界面触发入口。

## 8. 测试方案

应补充或调整以下回归测试。

### 8.1 普通创建流程

验证以下流程成功：

```cpp
SCEditPtr edit;
ASSERT_TRUE(Succeeded(db->BeginEdit(L"Add Record", edit)));

SCRecordPtr record;
ASSERT_TRUE(Succeeded(table->CreateRecord(record)));
ASSERT_TRUE(Succeeded(record->SetValue(L"PrimaryKey", primaryKey)));
ASSERT_TRUE(Succeeded(record->SetValue(
    L"RequiredValue", otherRequiredValue)));
ASSERT_TRUE(Succeeded(db->Commit(edit.Get())));
```

### 8.2 空主键提交失败

验证：

- `CreateRecord()` 成功返回记录对象。
- 不填写主键直接 `Commit()` 返回主键约束错误。
- 事务仍保持活动。
- 填写主键后再次 `Commit()` 成功。

### 8.3 非空字段补填

验证新增记录可以先创建，再逐个填写多个非空字段，直到所有字段完成后提交。

### 8.4 唯一约束冲突

验证新增记录与已有记录重复，或两条新增记录互相重复时：

- `Commit()` 失败。
- 数据库不出现部分提交。
- 修正值后可以再次提交成功。

### 8.4.1 Check 约束的提交期校验

验证新建记录填入违反 Check 的值时，`SetValue()` 因延迟完整约束而允许暂存该值，但 `Commit()` 必须返回约束错误；随后确认记录、Journal 和版本均未持久化。修正字段值后再次 `Commit()` 必须成功，并且违规信息包含 `source = SchemaConstraint` 与 `kind = Check`。

### 8.5 回滚和放弃

验证 `Rollback()`：

- 新增记录不再可见。
- 新增记录不参与后续唯一性检查。
- 不会在重新打开数据库后复活。

### 8.6 Undo / Redo

验证提交成功后：

- Undo 删除本次新增记录。
- Redo 恢复同一个 RecordId。
- 记录字段值和约束状态保持一致。

### 8.7 批量调用方

批量请求应用、事务提交和回滚由不同入口负责，测试必须覆盖实际事务拥有者：

- `ApplyBatchRequests()`：建立“分块 + 表请求 + 操作类型 + 操作索引 + RecordId”的临时映射；
- `ExecuteBatchEdit()`：在 `Commit()` 失败后执行回滚，并将约束违规信息映射回原始请求；
- `FinalizeImportSession()`：在最终 `Commit()` 失败后执行回滚，并将约束违规信息映射回原始分块请求；
- `ExecuteImport()`：验证 `rollbackOnError` 对导入会话的中断和清理行为。

批量层新增以下结果数据，用于保留 `Commit()` 阶段才产生的错误归因：

```cpp
enum class SCBatchOperationKind
{
    Create,
    Update,
    Delete,
};

struct SCBatchOperationMapping
{
    SCImportChunkId chunkId{0};
    std::size_t tableRequestIndex{0};
    SCBatchOperationKind kind{SCBatchOperationKind::Create};
    std::size_t operationIndex{0};
    RecordId recordId{0};
};

struct SCBatchApplyContext
{
    SCImportChunkId chunkId{0};
    std::vector<SCBatchOperationMapping>* operationMappings{nullptr};
};

struct SCBatchFailureLocation
{
    bool valid{false};
    SCImportChunkId chunkId{0};
    std::size_t tableRequestIndex{0};
    // false 表示表请求级失败（如 ResolveTable 失败），
    // operationKind/operationIndex/recordId 不可用。
    bool hasOperation{false};
    SCBatchOperationKind operationKind{SCBatchOperationKind::Create};
    std::size_t operationIndex{0};
    RecordId recordId{0};
    // Create/Update 的字段赋值失败位置；无字段上下文时保持默认值和空字符串。
    std::size_t assignmentIndex{0};
    std::wstring fieldName;
    // 仅当存储层实际提供了约束违规详情时为 true。
    // 类型、对象不存在等即时错误不能把默认构造值伪装成 PrimaryKey 违规。
    bool hasConstraintViolation{false};
    SCConstraintViolationInfo constraintViolation;
};

struct SCBatchApplyResult
{
    SCBatchExecutionResult counters;
    SCBatchFailureLocation immediateFailure;
};
```

批量请求是 Storage 对外 API，而非数据库编辑工具专用功能。只有 `SCBatchOperationKind`、`SCBatchFailureLocation` 和 `SCBatchExecutionResult::failureLocation` 属于公共 API，必须放入 `Include/SCBatch.h` 并更新 API 文档和调用方；`SCBatchOperationMapping`、`SCBatchApplyContext` 和 `SCBatchApplyResult` 仅服务于 `SCBatchOperations.cpp` 的内部调用，必须保留在匿名命名空间或私有实现文件中，不能扩大公共 API。

`ApplyBatchRequests()` 扩展为接收 `SCBatchApplyContext` 和 `SCBatchApplyResult`；每次创建、更新或删除成功进入活动编辑事务后，都记录 `chunkId + tableRequestIndex + operationKind + operationIndex + RecordId` 映射。处理每个 `SCBatchTableRequest` 前，必须先构造请求级失败位置（`valid = true`、`chunkId`、`tableRequestIndex`、`hasOperation = false`），以便 `ResolveTable()` 失败时能定位到分块和表请求而不伪造操作、记录或字段。每个操作开始前也必须保存当前操作位置（`hasOperation = true` 并填写操作类型和索引），以便 `SetValue()`、`DeleteRecord()` 或类型校验的即时失败可以直接生成 `immediateFailure`。Update 操作进入前必须立即将 `failureLocation.recordId = update.recordId`；Delete 操作进入前必须立即将 `failureLocation.recordId = recordId`；即使 `GetRecord()` / `DeleteRecord()` 立刻失败，也保留该请求 RecordId。Create 在 `CreateRecord()` 返回前无已分配记录 ID，失败时允许 `recordId = 0`。

`ApplyAssignments()` 必须接收当前失败位置上下文，并在每次调用 `SetValue(assignment.fieldName, ...)` 前写入 `assignmentIndex` 与 `fieldName`。Create/Update 中字段类型不匹配、列不存在、列不可编辑或关系值转换失败等即时错误，必须返回对应字段位置；Delete 操作不存在字段上下文，保持 `fieldName` 为空。`Commit()` 阶段的完整约束失败同样不伪造单一字段位置，保持 `fieldName` 为空，并通过 `constraintViolation.columns` 返回约束涉及的字段集合。

`ApplyBatchRequests()` 不持有 `SCEditPtr`，因此不得自行调用 `Rollback()`，也不得直接写入最终 `outResult`。它只返回原始错误、累计计数、操作映射和即时失败上下文；`ExecuteBatchEdit()` 与 `FinalizeImportSession()` 是唯一负责回滚和写回最终 `SCBatchExecutionResult` 的事务拥有者。

`SCBatchFailureLocation::constraintViolation` 仅在 `hasConstraintViolation == true` 时有效。约束失败的即时路径和 `Commit()` 失败路径在成功取得 `GetLastConstraintViolationInfo(...)` 后必须置该标记并复制完整信息；字段类型不匹配、表/记录不存在等没有存储层约束详情的即时错误必须保持该标记为 `false`。调用方不得依据默认构造的 `SCConstraintViolationInfo::kind` 推断错误为主键违规。

`SCBatchFailureLocation::operationKind`、`operationIndex` 和 `recordId` 仅在 `hasOperation == true` 时有效。`ResolveTable()` 失败（如表名为空、取表失败或自动建表失败）时返回请求级失败位置，`hasOperation = false`、`recordId = 0`、`fieldName` 为空、`hasConstraintViolation = false`；调用方不得依据默认构造的 `operationKind = Create` 推断为第一条创建操作失败。进入 Create/Update/Delete 后置 `hasOperation = true` 并填写操作类型和索引；Update 和 Delete 必须在调用 `GetRecord()` / `DeleteRecord()` 前将 `recordId` 预填为调用方请求的 RecordId，Create 在 `CreateRecord()` 成功返回后才填入已分配 RecordId，失败时 `recordId` 保持 `0`。

`ExecuteBatchEdit()` 与 `FinalizeImportSession()` 在 `Commit()` 失败后必须按以下顺序执行：

1. 在调用 `Rollback()` 前立即调用 `GetLastConstraintViolationInfo(...)`。
2. 优先以 `constraintViolation.recordId` 匹配操作映射；未命中时再以 `referencedRecordId` 匹配。
3. 将映射结果写入 `result.failureLocation`；仅在成功取得约束详情时设置 `hasConstraintViolation = true` 并复制完整 `SCConstraintViolationInfo`。
4. 调用 `Rollback()` 回滚整批编辑。
5. 无论函数最终返回成功还是失败，都将 `result` 写回 `*outResult`；随后返回原始 `Commit()` 错误，除非回滚失败需要返回更严重的恢复错误。

`ExecuteImport()` 仍负责 `rollbackOnError` 对导入会话的中断和清理；最终提交失败的位置映射由 `FinalizeImportSession()` 负责。

`ExecuteBatchEdit()` 不向调用方暴露活动 `SCEditPtr`，因此失败时必须始终回滚。为避免 `SCBatchExecutionOptions::rollbackOnError` 与实际行为不一致，本次将其从 `SCBatchExecutionOptions` 中移除；新增仅供 `ExecuteImport()` 使用的 `SCImportExecutionOptions`，由 `ISCmportOptions` 继续作为其兼容别名，并保留 `rollbackOnError`。这样 `rollbackOnError` 只表示导入会话在分块追加失败时是否中止和清理，不影响普通批量编辑的原子回滚契约。

选项类型定义为：

```cpp
struct SCBatchExecutionOptions
{
    std::wstring editName;
    std::size_t chunkSize{128};
    bool persistCheckpoints{true};
};

struct SCImportExecutionOptions
{
    std::wstring editName;
    std::size_t chunkSize{128};
    bool persistCheckpoints{true};
    bool rollbackOnError{true};
};

using ISCmportOptions = SCImportExecutionOptions;
```

这是有意的源码兼容性变更：`ExecuteImport()` 不再接受 `SCBatchExecutionOptions`。必须迁移当前仓库内的 `Tests/PerformanceSmokeTests.cpp`、`Tests/SqliteBackend/SqliteImportTests.cpp`、`Examples/ProductIntegrationExample.cpp`、`Docs/使用说明.md` 和其他检索到的导入调用点，改用 `SCImportExecutionOptions` 或 `ISCmportOptions`。现有 `ISCmportOptions{L"Import Floors"}` 等聚合初始化可保持第一个字段 `editName` 的位置不变。

`ExecuteBatchEdit()` 不再接受 `rollbackOnError`，失败时始终由事务拥有者原子回滚。必须迁移当前仓库内为 `ExecuteBatchEdit()` 设置该字段的调用点：

- `Tests/DatabaseEditorSessionTests.cpp` 中为 `ExecuteBatchEdit()` 设置 `options.rollbackOnError = true` 的两处（成功创建路径与失败回滚路径）：删除赋值，保留对失败后整批回滚的断言。
- `Tests/SqliteBackend/SqliteBatchTests.cpp` 中设置 `options.rollbackOnError = false` 的用例：删除赋值后该用例必须继续验证失败仍自动回滚且不遗留 active edit，不再依赖 `rollbackOnError` 选项控制行为。
- `Tools/DatabaseEditor/SCDatabaseEditorMainWindow.cpp` 中 CSV 导入经 `ExecuteBatchEdit()` 设置 `options.rollbackOnError = true` 的调用点：删除赋值，失败时仍由 `ExecuteBatchEdit()` 统一回滚。

`ApplyBatchRequests()` 内发生即时失败时，由调用它的事务拥有者执行以下流程，但不等待 `Commit()`：

1. 使用当前操作位置生成 `failureLocation`。
2. 若成功取得 `SCConstraintViolationInfo`，设置 `failureLocation.hasConstraintViolation = true` 并立即复制详情；否则保持为 `false`。
3. 调用 `Rollback(edit.Get())` 回滚活动编辑事务。
4. 将累计计数和 `failureLocation` 写回 `*outResult`，并返回原始即时错误。

若 `Commit()` 产生的 `recordId` 和 `referencedRecordId` 都不能匹配批量映射，`failureLocation.valid` 保持 `false`；若已取得约束详情，仍必须保留 `hasConstraintViolation = true` 和完整 `constraintViolation`，不得伪造原始请求位置。

必须验证：

- 多条 `CreateRecord()` 请求在同一编辑事务中完成后一次提交；
- 任意一条记录约束失败时，`ExecuteBatchEdit()` 和 `FinalizeImportSession()` 都能整批回滚；
- 失败结果能返回原始分块、表请求、操作类型、操作索引、RecordId；仅在 `hasConstraintViolation == true` 时返回 `constraintViolation.columns` 中的涉及字段和约束违规详情；
- 必填字段或空主键导致的 `ValidateRequiredValuesForCommit()` 失败，也能生成带 RecordId 和缺失列的 `failureLocation`；
- 普通非空字段缺失返回 `constraintViolation.source == RequiredColumnValue`，不会伪造 `SCConstraintKind::NotNull`；
- 显式主键、唯一、外键和 Check 失败继续返回 `constraintViolation.source == SchemaConstraint`，并保留原有 `kind`；
- 约束信息必须在回滚前捕获，并且失败返回时仍通过 `outResult` 提供；
- 更新记录的即时 Unique 冲突、删除记录的即时外键冲突和字段类型不匹配，也能通过当前操作位置生成并回传 `failureLocation`；
- 同一 Create/Update 请求包含多个字段赋值时，第 N 个字段发生类型不匹配、列不存在或列不可编辑，`failureLocation` 必须返回该请求、RecordId、`assignmentIndex == N` 和准确的 `fieldName`；
- Delete 和 Commit 期约束失败不得伪造单一 `fieldName`，字段集合仅以 `hasConstraintViolation == true` 时的 `constraintViolation.columns` 表示；
- 字段类型不匹配、表/记录不存在等无约束详情的即时失败必须返回 `hasConstraintViolation == false`，不得把默认 `SCConstraintViolationInfo` 解释为主键违规；
- 表名为空、取表失败或自动建表失败时，`failureLocation` 能定位到分块和表请求，但 `hasOperation == false`、`recordId == 0`、`fieldName` 为空、`hasConstraintViolation == false`，不得伪造操作、记录或字段；
- 不存在的 Update RecordId 和不存在的 Delete RecordId 导致 `GetRecord()` / `DeleteRecord()` 失败时，`failureLocation` 必须包含原请求 RecordId、`hasOperation == true`、操作类型和索引，且 `hasConstraintViolation == false`；
- Commit 违规信息只指向已有冲突记录时，不伪造请求位置，保留 `valid == false` 和完整违规详情；
- `ExecuteBatchEdit()` 的即时失败和 Commit 失败都由事务拥有者回滚并写回结果；不直接测试内部 `ApplyBatchRequests()` 的回滚行为；
- `SCBatchExecutionOptions` 不再包含 `rollbackOnError`；`SCImportExecutionOptions::rollbackOnError` 只影响导入会话的中断和清理；`ExecuteBatchEdit()` 失败时始终原子回滚，不提供关闭回滚的选项；
- 成功批量提交后，所有记录作为同一个事务进入 Undo / Redo。

### 8.8 持久化失败恢复

通过可控的底层写入失败或事务提交失败场景验证：

- 不允许在不确定状态下再次 `Commit()` 或继续写入；
- 内存记录、活动日志和引用索引不会停留在与磁盘不一致的状态；
- 可恢复时完整回滚；
- 不可恢复时数据库连接进入明确的不可继续使用状态。
- `PersistDeferredSchemaOps()` 已执行后、记录/Journal/元数据写入前发生失败，以及记录、Journal、元数据写入和 `txn.Commit()` 分别失败时，都走同一恢复收尾路径；每种场景均验证 Schema、记录、索引、活动日志和版本快照回到编辑前状态，或明确进入 `Poisoned` / `DatabaseFaulted`。
- 所有最终约束（尤其是 Check）都在第一次持久化写入前失败；约束失败后编辑仍保持 Active，不得触发持久化恢复路径。
- `DatabaseFaulted` 后所有 `ErrorCode` 读写 API 返回 `SC_E_DATABASE_FAULTED`，无错误码的版本快照查询不访问 SQLite 且只返回最后确认值。
- `GetEditingState()` 和 `GetEditLogState()` 在 `DatabaseFaulted` 后仍返回纯内存诊断快照，其中 `consistencyLost == true`。
- `GetErrorCodeName(SC_E_DATABASE_FAULTED)` 返回正确名称，`GetErrorCodeCategory(...)` 返回“约束与存储错误”。
- 触发故障的操作仍返回原始 I/O、提交或恢复错误；后续普通 API 调用才返回 `SC_E_DATABASE_FAULTED`。

### 8.9 兼容性

验证已有调用方在一次 `CreateRecord()` 后完成赋值并提交的代码正常。测试迁移必须覆盖两类旧断言：

- 创建阶段失败：改为验证 `CreateRecord()` 成功、`Commit()` 阶段失败；
- 写值阶段失败：改为验证 `SetValue()` 允许草稿继续编辑、`Commit()` 阶段失败。

重点检查 `SchemaConstraintTests.cpp` 中新建记录后 `SetInt64()` 等立即期待 `SC_E_CONSTRAINT_VIOLATION` 的测试，并将断言移动到 `Commit()`。

生产调用方还包括 `Src/Batch/SCBatchOperations.cpp`。延迟校验后，批量操作的约束错误可能从创建第 N 条记录时改为统一 `Commit()` 时返回；必须迁移 `ApplyBatchRequests()`、`ExecuteBatchEdit()`、`FinalizeImportSession()` 和 `ExecuteImport()`，并验证错误归因映射和各自的回滚责任。

由于 `CreateDraftRecord()` 当前只有数据库编辑器使用，删除和迁移范围限于该编辑器；同时删除 `SavePendingChanges()` 和 `DiscardPendingChanges()` 的定义、声明及调用点。

## 9. 修改范围

建议只修改以下范围：

- `ISCTable` 公共接口注释和生命周期说明。
- `SCTypes.h`：新增 `SCConstraintViolationSource`，并扩展 `SCConstraintViolationInfo`；不改变 `SCConstraintKind` 的 Schema 约束语义。
- `SCErrors.h` 和 `Src/SCErrors.cpp`：新增通用存储错误 `SC_E_DATABASE_FAULTED`，并同步错误名称和分类映射，不暴露 SQLite 细节。
- SQLite 后端 `CreateRecord()` 的创建阶段校验逻辑。
- `Commit()` 的统一候选状态校验逻辑。
- SQLite 后端新增统一 `EnsureUsable()`，覆盖表、记录、游标、查询、Schema 和历史等普通读写入口；`GetEditingState()` / `GetEditLogState()` 是唯一诊断例外。
- `Src/Sqlite/SqliteImplDatabase_Init.cpp` 的 `EnsureWritable()`，以及必要的 `EnsureWriteTransitionAllowed()` 调用点：一致性丢失后返回 `SC_E_DATABASE_FAULTED`。
- SQLite 后端的 `Poisoned` / `DatabaseFaulted` 状态处理、恢复路径和 `GetEditingState()` / `GetEditLogState()` 诊断快照。
- 删除 `CreateDraftRecord()` 公共接口及数据库编辑器中的唯一调用点。
- 删除 `SavePendingChanges()` 和 `DiscardPendingChanges()` 编辑器接口及所有调用方。
- 编辑器直接使用 `Commit()` 和 `Rollback()`。
- 数据库编辑器的新增记录流程和错误恢复流程。
- `Include/SCBatch.h` 和 `Src/Batch/SCBatchOperations.cpp`：公开失败位置结果、内部批量请求索引/RecordId 映射与失败上下文、批量创建/提交失败/回滚路径，以及批量和导入选项类型拆分。
- 所有 `ExecuteImport()` 调用点、相关示例和 API 使用文档：迁移为 `SCImportExecutionOptions` / `ISCmportOptions`。
- 为 `ExecuteBatchEdit()` 设置 `rollbackOnError` 的调用点：删除字段赋值并验证失败始终原子回滚。
- 相关单元测试和 API 文档。

不应修改：

- RecordId 分配策略。
- 已有 Schema 约束的定义方式。
- Undo / Redo 的日志格式，除非验证发现新增状态无法正确重放。
- 与本问题无关的查询、计算列和数据库打开流程。

## 10. 验收标准

修复完成后，以下代码在主键和非空字段均无默认值的表上必须成立：

```cpp
SCEditPtr edit;
ASSERT_TRUE(Succeeded(db->BeginEdit(L"Add Record", edit)));

SCRecordPtr record;
ASSERT_TRUE(Succeeded(table->CreateRecord(record)));

ASSERT_TRUE(Succeeded(record->SetValue(L"Id", SCValue::FromString(L"ET-001"))));
ASSERT_TRUE(Succeeded(record->SetValue(
    L"Name", SCValue::FromString(L"梁类型"))));

ASSERT_TRUE(Succeeded(db->Commit(edit.Get())));
```

以下代码必须失败，但失败时不得破坏编辑事务：

```cpp
SCEditPtr edit;
ASSERT_TRUE(Succeeded(db->BeginEdit(L"Add Record", edit)));

SCRecordPtr record;
ASSERT_TRUE(Succeeded(table->CreateRecord(record)));

ErrorCode rc = db->Commit(edit.Get());
ASSERT_TRUE(Failed(rc));

// 修正草稿后仍可继续提交
ASSERT_TRUE(Succeeded(record->SetValue(
    L"Id", SCValue::FromString(L"ET-001"))));
ASSERT_TRUE(Succeeded(db->Commit(edit.Get())));
```

最终验收重点是：`CreateRecord()` 返回记录对象，`SetValue()` 可以完成字段赋值，所有完整约束在 `Commit()` 阶段集中执行，并且失败提交不会产生部分持久化结果。

## 11. 一次添加多行记录

存储层的编辑事务应支持在一次 `BeginEdit()` 和 `Commit()` 之间创建多条记录：

```cpp
SCEditPtr edit;
ErrorCode rc = db->BeginEdit(L"Batch Add Records", edit);
if (Failed(rc))
{
    return rc;
}

SCRecordPtr first;
rc = table->CreateRecord(first);
if (Failed(rc))
{
    db->Rollback(edit.Get());
    return rc;
}
rc = first->SetValue(L"Id", SCValue::FromString(L"ET-001"));
if (Failed(rc))
{
    db->Rollback(edit.Get());
    return rc;
}

SCRecordPtr second;
rc = table->CreateRecord(second);
if (Failed(rc))
{
    db->Rollback(edit.Get());
    return rc;
}
rc = second->SetValue(L"Id", SCValue::FromString(L"ET-002"));
if (Failed(rc))
{
    db->Rollback(edit.Get());
    return rc;
}

rc = db->Commit(edit.Get());
```

提交时必须将所有新增记录作为一个候选状态整体校验：

- 检查每条记录自身的主键、非空、Check 和外键约束。
- 检查新增记录之间的主键和唯一值冲突。
- 检查新增记录与已有记录之间的主键和唯一值冲突。
- 任意一条记录失败时，整批记录都不得提交。
- 提交失败后所有草稿仍保留在当前编辑事务中，可以修改后重新提交，或一次性回滚。
- 提交成功后整批记录作为一个 Undo 单元处理。

### 11.1 编辑器的逐行自动保存模式

当前网格交互可以采用逐行自动保存：

1. 添加第一条记录并显示 `*`。
2. 填写字段。
3. 切换到下一行时提交第一条记录。
4. 再添加并编辑下一条记录。

这种模式下每一行都是独立事务。前一行成功后，即使后一行校验失败，前一行也不会回滚。因此它适合连续录入，但不提供整批原子性。

### 11.2 编辑器的批量原子提交模式

如果业务要求“多行要么全部成功、要么全部失败”，则不能在切换行时自动调用 `Commit()`。应采用批量模式：

1. 用户进入批量新增状态。
2. 所有新行都使用同一个活动编辑事务创建，并显示 `*`。
3. 用户可以在这些草稿行之间切换和填写数据，切换行只保存当前单元格值，不提交事务。
4. 用户选择“提交本批次”时，统一调用一次会话层 `CommitCurrentEdit()`，其内部调用公共 `Commit()`。
5. 任意一行校验失败时，整批草稿继续保留，界面定位到错误记录或字段。
6. 用户按 `Esc` 或选择“放弃本批次”时，统一调用一次会话层 `RollbackCurrentEdit()`，其内部调用公共 `Rollback()`。

批量模式可以继续使用当前 `pendingNewRecordIds_` 集合跟踪多条草稿记录，但 `HasPendingEdit()` 只能表示整个批次事务是否存在，不能表示只有一条待提交记录。

### 11.3 推荐产品行为

建议默认保留当前简单的逐行自动保存模式，并明确提示“切换行会提交当前新增记录”。如果确实存在 Excel 式连续录入或导入场景，再增加独立的批量新增入口。

批量入口不应通过隐式修改普通 `CreateRecord()` 语义实现，而应显式控制提交边界，例如：

```cpp
SCEditPtr edit;
db->BeginEdit(L"Batch Add Records", edit);
// CreateRecord + SetValue，可重复执行多次
db->Commit(edit.Get());
```

这样既能保持普通新增的易用性，也能让调用方明确选择是否需要整批原子提交。
