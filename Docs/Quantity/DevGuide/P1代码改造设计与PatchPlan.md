# P1 代码改造设计与 Patch Plan

本文件承接《P1 接口与核心模型定义稿》，用于描述后续代码改造的落地顺序、模块切分、文件级变更和兼容策略。本文只给出重构设计与 patch plan，不包含完整实现代码。

## 一、总体改造原则

### 1.1 哪些模型先落地

优先落地以下模型：

1. Version Graph。
2. CompatibilityWindow。
3. UpgradePlan / UpgradeResult。
4. ImportSession / ImportCheckpoint / ImportFinalizeCommit。
5. ExportMode / AssetSelection / RedactionPolicy / PackageSizePolicy。
6. Subject / Role / Scope / AccessDecision。
7. QueryPlan / QueryCondition / QueryPage / SortSpec / ReferenceIndex。

这些模型先落地的原因是它们决定后续接口如何兼容、如何分层、如何扩展。

### 1.2 哪些接口先兼容

先保持兼容的接口包括：

1. `ISCDatabase`
2. `ISCTable`
3. `ISCRecord`
4. `ISCEditSession`
5. `ISCComputedTableView`
6. `ISCDatabaseDiagnosticsProvider`
7. `ExecuteBatchEdit`

兼容方式是先在现有入口内部引入新模型，不立刻删除旧接口语义。

### 1.3 哪些实现后替换

后替换的实现包括：

1. SQLite Adapter 中的全量扫描查询路径。
2. Memory Backend 中的全量扫描引用检查路径。
3. 现有 batch 导入的一次性大事务路径。
4. 当前问题包导出默认全量打包路径。
5. 现有版本升级的单线迁移路径。

### 1.4 哪些功能延后

延后功能包括：

1. 完整多租户权限体系。
2. 复杂 RBAC / ABAC。
3. 多跳降级迁移。
4. 高级联查与聚合查询。
5. 复杂分卷导出策略的自动化编排。

---

## 二、模块级改造清单

### 2.1 Storage Core

#### 新增类型

1. `VersionNode`
2. `MigrationEdge`
3. `CompatibilityWindow`
4. `UpgradePlan`
5. `UpgradeResult`
6. `ImportSessionId`
7. `ImportChunkId`
8. `ImportCheckpoint`
9. `ImportStagingArea`
10. `ImportFinalizeCommit`
11. `ImportRecoveryState`
12. `SubjectId`
13. `RoleId`
14. `ScopeId`
15. `AccessDecision`
16. `QueryPlan`
17. `QueryPage`
18. `SortSpec`
19. `ReferenceIndex`
20. `ReverseReferenceIndex`

#### 修改接口

1. `SCMigration.h`
2. `SCBatch.h`
3. `SCStorage.h`
4. `SCTypes.h`
5. `ISCInterfaces.h`
6. `ISCDiagnostics.h`

#### 替换旧逻辑

1. 用 Version Graph 替换单线迁移计划。
2. 用 Import Session 替换单次大事务导入假设。
3. 用 QueryPlan 替换隐式全库扫描假设。
4. 用 AccessDecision 替换未来硬编码角色判断。

#### 保留兼容层

1. 保留 `BuildMigrationPlan` 的旧入口，内部映射到新图谱模型。
2. 保留现有 `ExecuteBatchEdit`，在内部逐步接入导入会话与 checkpoint。
3. 保留现有 `SCQueryCondition` / `QueryCondition` 风格查询入口，增加新计划对象。

### 2.2 SQLite Adapter

#### 新增类型

1. `SqliteVersionCatalog`
2. `SqliteMigrationPlanner`
3. `SqliteImportSessionStore`
4. `SqliteExportStreamer`
5. `SqliteQueryPlanner`
6. `SqliteReferenceIndexProvider`

#### 修改接口

1. `CreateSqliteDatabase`
2. `SqliteDatabase::EnsureMigrationAndRecovery`
3. `SqliteDatabase::ApplyMigrationPlan`
4. `SqliteDatabase::LoadMetadata`
5. `SqliteDatabase::SaveMetadata`
6. `SqliteDatabase::LoadJournalStacks`
7. `SqliteTable::FindRecords`
8. `SqliteTable::EnumerateRecords`
9. `SqliteDatabase::IsRecordReferenced`

#### 替换旧逻辑

1. 版本读取和升级计划从单一迁移函数改为图谱驱动。
2. 查询从内存物化改为后端下推。
3. 引用检查从全表扫描改为索引命中。
4. 导出从一次性聚合改为流式写出。

#### 保留兼容层

1. 保留现有数据库创建和打开入口。
2. 保留当前事务语义。
3. 保留现有 Journal / Undo / Redo 行为，先在外层加模型，再逐步改内部组织。

### 2.3 Memory Backend

#### 新增类型

1. `MemoryVersionCatalog`
2. `MemoryQueryPlanner`
3. `MemoryReferenceIndex`
4. `MemoryImportSessionState`

#### 修改接口

1. `MemoryDatabase::IsRecordReferenced`
2. `MemoryDatabase::LookupRecordJournalState`
3. `MemoryDatabase::Commit`
4. `MemoryDatabase::Undo`
5. `MemoryDatabase::Redo`

#### 替换旧逻辑

1. 引用检查改为索引驱动。
2. 查询执行改为统一 QueryPlan 入口。
3. 迁移与导入语义与 SQLite 保持一致。

#### 保留兼容层

1. 继续保留当前内存后端作为测试和快速验证 backend。
2. 保留现有 Undo / Redo 语义。

### 2.4 Import / Export

#### 新增类型

1. `ImportSession`
2. `ImportChunk`
3. `ImportCheckpoint`
4. `ImportFinalizeCommit`
5. `ExportMode`
6. `AssetSelection`
7. `RedactionPolicy`
8. `PackageSizePolicy`
9. `StreamingExportContext`

#### 修改接口

1. 批处理导入入口。
2. 问题包导出入口。
3. 工程导出入口。

#### 替换旧逻辑

1. 导入从整包事务改为分块会话。
2. 问题包导出从默认全量打包改为显式选择。
3. 导出从同步聚合改为流式上下文。

#### 保留兼容层

1. 保留普通导出入口。
2. 保留旧问题排查导出入口名，但内部改走新模型。

### 2.5 Replay

#### 新增类型

1. `ReplayState`
2. `ReplayAnchor`
3. `ReplayBranch`
4. `ReplayAssetHandle`

#### 修改接口

1. 回放会话读取接口。
2. 继续编辑接口。
3. 回放入口判定接口。

#### 替换旧逻辑

1. 从布尔回放可用性改为状态机。
2. 从当前屏幕态继续编辑改为锚点分支。
3. 从自动升级到显式启用录制。

#### 保留兼容层

1. 保留旧回放入口显示逻辑，内部转到新状态模型。
2. 保留旧工程回放读取入口，逐步替换会话结构。

### 2.6 Diagnostics / Tooling

#### 新增类型

1. `PackageExportPolicy`
2. `VersionGraphDiagnostic`
3. `MigrationDiagnostic`
4. `ImportRecoveryDiagnostic`
5. `ReferenceScanDiagnostic`

#### 修改接口

1. `CollectDiagnostics`
2. `DescribeChangeSet`
3. 问题包生成工具入口。

#### 替换旧逻辑

1. 诊断从单一健康报告扩展为版本、导入、导出和引用检查诊断。
2. 问题包导出从被动收集改为策略驱动。

#### 保留兼容层

1. 保留现有诊断输出入口。
2. 保留现有健康报告结构，逐步扩展字段。

### 2.7 Product Startup Flow

#### 新增类型

1. `AppRuntimePolicy`
2. `ProjectMeta`
3. `UpgradeDecision`
4. `ReplayEntryDecision`
5. `ImportEntryDecision`

#### 修改接口

1. 启动页工程打开流程。
2. 显式升级流程。
3. 显式 enable recording 流程。
4. 回放入口显隐判断。

#### 替换旧逻辑

1. open 不再隐式升级。
2. recording 不再默认自动开启。
3. replay continue editing 不再依赖当前态。

#### 保留兼容层

1. 保留现有首页入口。
2. 保留旧工程读取流程，内部增加决策层。

---

## 三、文件级改造计划

### 3.1 `D:\code\stablecore\Storage\Include\SCTypes.h`

#### 修改内容

1. 增加 Version Graph 相关基础枚举与结构。
2. 增加 Import Session 相关基础结构。
3. 增加 Subject / Role / Scope 相关基础类型。
4. 增加 Query / Reference 相关基础类型。
5. 扩展现有 ChangeSet / Journal 语义字段。

#### 原因

这是所有核心模型的公共定义点，必须先统一类型层。

### 3.2 `D:\code\stablecore\Storage\Include\SCMigration.h`

#### 修改内容

1. 将现有单线迁移计划扩展为 Version Graph 模型。
2. 增加升级计划、兼容窗口和迁移边查询接口。

#### 原因

版本升级体系需要先有正式模型，才能让 Adapter 和 Product 一致判断升级路径。

### 3.3 `D:\code\stablecore\Storage\Include\SCBatch.h`

#### 修改内容

1. 扩展批处理请求为导入会话友好模型。
2. 增加分块边界、checkpoint 和 finalize 相关选项。

#### 原因

大型导入不能再默认单事务闭环，必须有分块能力。

### 3.4 `D:\code\stablecore\Storage\Include\ISCInterfaces.h`

#### 修改内容

1. 保持现有数据库、表、记录、编辑会话接口不变。
2. 增加面向新模型的查询和版本支持点。
3. 预留主体与作用域判断扩展点。

#### 原因

这是现有上层调用最广的稳定接口，不宜一次性破坏。

### 3.5 `D:\code\stablecore\Storage\Include\ISCDiagnostics.h`

#### 修改内容

1. 扩展诊断报告中版本、导入、导出和索引相关信息。
2. 增加问题包导出诊断策略定义点。

#### 原因

P1 需要能诊断升级、导入恢复和引用扫描问题。

### 3.6 `D:\code\stablecore\Storage\Include\ISCComputed.h`

#### 修改内容

1. 扩展缓存失效上下文，预留版本图谱和索引联动信息。
2. 扩展依赖判断模型，为后端下推提供基础字段。

#### 原因

计算列扩展不能再只依赖版本号和浅层变更。

### 3.7 `D:\code\stablecore\Storage\Include\ISCTableView.h`

#### 修改内容

1. 预留 QueryPlan 输入。
2. 预留分页和排序参数。

#### 原因

查询能力必须向统一计划收敛。

### 3.8 `D:\code\stablecore\Storage\Include\SCStorage.h`

#### 修改内容

1. 作为总入口承接新模型接口。
2. 统一导出、迁移、查询、诊断与导入能力入口。

#### 原因

需要一个总的过渡入口把新旧逻辑串起来。

### 3.9 `D:\code\stablecore\Storage\Src\Migration\SCMigration.cpp`

#### 修改内容

1. 从单线 step 聚合演进为图谱驱动计划构建。
2. 保留旧函数入口作为兼容层。

#### 原因

版本升级是 P1 最先要补的能力之一。

### 3.10 `D:\code\stablecore\Storage\Src\Batch\SCBatchOperations.cpp`

#### 修改内容

1. 从一次性批处理扩展为导入会话友好的分块执行。
2. 增加 checkpoint 记录与恢复入口。

#### 原因

这是大型模型和钢筋导入改造的主要入口之一。

### 3.11 `D:\code\stablecore\Storage\Src\Sqlite\SCSqliteAdapter.cpp`

#### 修改内容

1. 增加版本图谱读取和升级执行路径。
2. 增加分块导入和 finalize 提交路径。
3. 增加流式导出和问题包策略入口。
4. 增加后端查询下推与索引查询路径。
5. 增加版本、导入、导出诊断信息。

#### 原因

SQLite 是正式持久化适配层，P1 能力最终要在这里落地。

### 3.12 `D:\code\stablecore\Storage\Src\Memory\SCMemoryDatabase.cpp`

#### 修改内容

1. 对齐引用检查和查询计划语义。
2. 统一 Undo / Redo 与新模型边界。
3. 预留导入会话和版本判定的测试入口。

#### 原因

Memory backend 是行为验证基线，不能与 SQLite 演化脱节。

### 3.13 `D:\code\stablecore\Storage\Src\Computed\SCComputedRuntime.cpp`

#### 修改内容

1. 扩展缓存失效上下文。
2. 预留按依赖集合和版本图谱联合失效能力。

#### 原因

计算列是 10x / 100x 场景的关键放大器。

### 3.14 `D:\code\stablecore\Storage\Src\Computed\SCTableView.cpp`

#### 修改内容

1. 接入统一 QueryPlan。
2. 预留分页、排序和后端下推路径。

#### 原因

表视图查询是用户侧最直接的性能入口。

### 3.15 `D:\code\stablecore\Storage\Src\Diagnostics\SCDiagnostics.cpp`

#### 修改内容

1. 扩展版本、导入、导出和引用检查诊断。
2. 支持生成问题包导出建议。

#### 原因

P1 的升级、导入和导出都需要诊断支撑。

### 3.16 产品侧启动流文件

#### 修改内容

1. 启动页读取 `AppRuntimePolicy`。
2. 工程打开进入只读加载。
3. 显式 upgrade / enable recording / replay continue editing 接入。

#### 原因

P1 的版本、录制和回放入口需要产品侧明确承接。

---

## 四、关键接口草案

### 4.1 Version Graph / Migration

#### 职责

描述版本节点、迁移边、兼容窗口和升级计划。

#### 接口思路

1. 读取版本图谱。
2. 计算升级路径。
3. 生成升级计划。
4. 执行升级计划。
5. 返回升级结果。

#### 关键输入

- 当前版本。
- 目标版本。
- 兼容窗口。
- 可用迁移边。

---

### 4.2 Upgrade Planning / Execution

#### 职责

把升级前判断与升级执行拆开。

#### 接口思路

1. 先做前置检查。
2. 再生成计划。
3. 再显式确认。
4. 再执行。
5. 最后返回结果与恢复信息。

---

### 4.3 Import Session / Checkpoint / Finalize

#### 职责

描述分块导入、恢复点和最终确认。

#### 接口思路

1. 开始导入会话。
2. 按块提交。
3. 写 checkpoint。
4. 出现失败后依据 checkpoint 恢复。
5. 结束时进行 finalize。

#### 关键约束

- checkpoint 不等于正式状态。
- finalize 才是正式状态边界。

---

### 4.4 Debug Package Export

#### 职责

以受控策略导出诊断包。

#### 接口思路

1. 选择导出模式。
2. 选择资产。
3. 应用脱敏策略。
4. 应用大小策略。
5. 通过流式上下文写出。

---

### 4.5 Subject / Role / Scope

#### 职责

表达最小主体隔离模型。

#### 接口思路

1. 输入主体。
2. 输入角色。
3. 输入作用域。
4. 输出 AccessDecision。

#### 判定维度

- 资源类型。
- 动作类型。
- 范围。

---

### 4.6 QueryPlan / ReferenceIndex / ReverseReferenceIndex

#### 职责

统一查询计划和引用索引。

#### 接口思路

1. 将查询条件整理成 QueryPlan。
2. 将常用引用关系建立正向和反向索引。
3. 查询执行器优先命中索引。
4. 引用检查优先使用反向索引。

---

## 五、兼容性策略

### 5.1 如何兼容现有查询入口

1. 保留现有 `GetTable / FindRecords / EnumerateRecords` 风格入口。
2. 内部增加 QueryPlan 生成层。
3. 旧接口先映射到新执行器，外部语义不变。
4. 新能力以可选参数方式加入，不强制修改所有调用方。

### 5.2 如何兼容现有 Undo/Redo / Replay

1. 现有 Undo / Redo 不删除。
2. Replay 仍以提交单元为主，但增加 `CommitId` / `SessionId` / `SnapshotId` / `BranchAnchor` 语义。
3. 新的导入 finalize 边界与现有 Undo / Redo 先保持兼容映射，后续再收口。

### 5.3 如何避免一次性迁移全部老逻辑

1. 先加新模型，不先删旧路径。
2. 先做内部适配，不先改所有外部调用。
3. 先让新接口可用，再逐步切换调用点。
4. 先保证旧工程可打开，再逐步提升升级能力。

### 5.4 哪些地方需要适配层过渡

1. SQLite Adapter 与 Memory Backend 之间的行为对齐。
2. 批处理导入到 Import Session 的过渡。
3. 旧导出入口到 Debug Package 的过渡。
4. 旧查询入口到 QueryPlan 的过渡。
5. 旧版本升级入口到 Version Graph 的过渡。

---

## 六、分阶段落地任务

### Task 1：Version Graph 基础接入

- 目标
  - 让版本图谱和兼容窗口先能被读取与判断。
- 涉及模块
  - Storage Core
  - SQLite Adapter
  - Product Startup Flow
- 改动点
  - 新增版本图谱模型。
  - 扩展迁移计划接口。
  - 启动时读取兼容窗口。
- 验证点
  - 能判断工程是否需要升级。
  - 能判断工程是否只读可开。
- 风险点
  - 兼容窗口定义不准会影响历史工程打开。

### Task 2：Upgrade Planning / Execution 分离

- 目标
  - 把升级判断和升级执行拆开。
- 涉及模块
  - Storage Core
  - SQLite Adapter
  - Product Startup Flow
- 改动点
  - 引入 UpgradePlan / UpgradeResult。
  - 显式升级确认入口。
  - 升级失败恢复结果。
- 验证点
  - open 不写入。
  - upgrade 必须显式。
  - 升级失败可回退。
- 风险点
  - 旧流程中隐式升级残留。

### Task 3：Import Session 与 checkpoint

- 目标
  - 把大数据导入改成分块、可恢复、可确认的会话模型。
- 涉及模块
  - Storage Core
  - Batch
  - SQLite Adapter
  - Tooling
- 改动点
  - 增加 ImportSession / Chunk / Checkpoint / Finalize。
  - Batch 入口映射到会话模型。
  - 恢复状态记录。
- 验证点
  - 分块提交可恢复。
  - checkpoint 不等于正式状态。
  - finalize 后才生效。
- 风险点
  - 局部失败后的状态一致性。

### Task 4：Debug Package Export

- 目标
  - 将普通导出与问题包导出分离。
- 涉及模块
  - Product
  - Tooling
  - Diagnostics
- 改动点
  - 新增导出模式与资产选择。
  - 新增脱敏和大小策略。
  - 改成流式写出。
- 验证点
  - 默认不带全量 Replay。
  - 大包可压缩、可中断、可恢复。
- 风险点
  - 导出内容边界过宽导致泄露。

### Task 5：Subject / Role / Scope 最小模型

- 目标
  - 建立轻量主体隔离模型。
- 涉及模块
  - Product
  - Storage Core
  - Tooling
- 改动点
  - 新增主体判定对象。
  - 预留权限决策接口。
  - 预留作用域判断。
- 验证点
  - 可对工程、Replay、问题包做判定。
  - 默认单机场景不受影响。
- 风险点
  - 设计过重导致当前场景复杂化。

### Task 6：Query / Reference 下推

- 目标
  - 把查询与引用检查从全量扫描演进到索引驱动。
- 涉及模块
  - Storage Core
  - SQLite Adapter
  - Memory Backend
  - Computed
  - TableView
- 改动点
  - 引入 QueryPlan。
  - 引入正向 / 反向引用索引。
  - 查询入口下推。
- 验证点
  - 10x / 100x 下查询可用。
  - 引用检查不再全库扫描。
- 风险点
  - 索引与旧数据不一致。

### Task 7：诊断与工具补强

- 目标
  - 让版本、导入、导出、索引问题可诊断。
- 涉及模块
  - Diagnostics
  - Tooling
  - Product Startup Flow
- 改动点
  - 扩展健康报告。
  - 扩展问题包导出诊断。
  - 扩展迁移恢复工具。
- 验证点
  - 失败能定位。
  - 历史工程能判断状态。
- 风险点
  - 诊断信息过多影响可读性。

---

## 七、进入代码实现前必须确认的事项

1. 是否明确采用 forward-only 作为主升级策略。
2. 是否允许任何形式的自动升级，默认是否一律禁止。
3. 是否允许问题包默认包含 Project，默认是否只导出 Project。
4. 是否允许导入会话在局部失败后保留已成功块并等待恢复。
5. 是否允许导出包默认包含任何 Replay Assets，默认是否全部关闭。
6. 是否接受轻量主体模型先落地，后续再扩展完整权限体系。
7. 是否接受查询改造先保留旧接口，内部逐步切换到 QueryPlan。

---

## 八、最终结论

本阶段的代码改造应遵循“先模型、再接口、再适配、再工具”的顺序推进，先把版本、导入、导出、主体隔离和查询计划五条主链路补齐，再逐步切换旧实现，避免一次性重构导致系统风险放大。

