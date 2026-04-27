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

处理顺序：

1. 预检输入与当前状态
2. 检查目标字段是否冲突
3. 对需要迁移的场景执行清数据
4. 执行 schema / computed 定义替换
5. 重建当前 `ComputedTableView`
6. 任一步失败则回滚并恢复旧状态

### 8.3 后端收口

- `ISCSchema` 增加原地更新能力，供普通字段编辑使用
- `Memory` 和 `SQLite` 保持同语义实现
- SQLite 更新字段时不能静默吞掉索引失败

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
