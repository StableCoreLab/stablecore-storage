# Task 6 代码改造设计与 Patch Plan

本文档基于已确认的《Query / Reference 接口定义稿》和《Query 执行架构设计稿》，定义查询与引用能力的代码改造边界、文件拆分、桥接方式与落地顺序。本文档只描述改造计划，不包含实现代码。

## 一、本次修改目标

- 引入 `QueryPlan` 驱动的查询执行路径。
- 保留现有单表 / 单视图查询入口，避免一次性替换。
- 为 SQLite / Memory backend 提供统一的计划执行入口。
- 将引用检查从全库扫描演进为按 source / target 查询。
- 为 fallback 扫描提供可中止、可分页、可观测的执行模型。
- 为索引初始化、索引版本检查与索引重建提供正式入口。

## 二、修改文件清单

### Storage Core
- `Storage/Include/SCQuery.h` 新增
- `Storage/Include/ISCQuery.h` 新增
- `Storage/Include/ISCInterfaces.h` 修改
- `Storage/Include/SCStorage.h` 修改
- `Storage/Include/SCTypes.h` 轻量调整

### Storage Implementation
- `Storage/Src/Query/SCQueryPlanner.cpp` 新增
- `Storage/Src/Query/SCQueryExecutor.cpp` 新增
- `Storage/Src/Query/SCReferenceIndex.cpp` 新增
- `Storage/Src/Query/SCQueryBridge.cpp` 新增
- `Storage/Src/Sqlite/SCSqliteAdapter.cpp` 修改
- `Storage/Src/Memory/SCMemoryDatabase.cpp` 修改
- `Storage/Src/Computed/SCTableView.cpp` 修改
- `Storage/Src/Batch/SCBatchOperations.cpp` 轻量接入

### Tooling / Diagnostics
- `Storage/Include/ISCDiagnostics.h` 修改
- `Storage/Src/Diagnostics/SCDiagnostics.cpp` 修改

### Tests
- `Storage/Tests/M2SqliteTests.cpp` 修改
- `Storage/Tests/M3Tests.cpp` 修改
- `Storage/Tests/PerformanceSmokeTests.cpp` 轻量扩展

## 三、每个文件的改动点

### 3.1 `Storage/Include/SCQuery.h` 新增
目的：
- 汇总查询核心类型。

预期内容：
- `QueryTargetType`
- `QueryTarget`
- `QueryConditionOperator`
- `QueryLogicOperator`
- `QueryCondition`
- `QueryConditionGroup`
- `SortSpec`
- `QueryPage`
- `QueryHints`
- `QueryConstraints`
- `QueryPlanState`
- `QueryPlan`
- `QueryExecutionMode`
- `QueryExecutionResult`
- `ReferenceDirection`

### 3.2 `Storage/Include/ISCQuery.h` 新增
目的：
- 定义查询规划、执行、引用索引提供与维护的正式接口。

预期内容：
- `IQueryPlanner`
- `IQueryExecutor`
- `IReferenceIndexProvider`
- `IReferenceIndexMaintainer`

### 3.3 `Storage/Include/ISCInterfaces.h` 修改
目的：
- 保持现有表 / 记录 / 数据库接口兼容，同时补充查询能力桥接所需类型前置声明。

改动重点：
- 前置声明 `QueryPlan`、`QueryExecutionResult`、`ReferenceIndex`、`ReverseReferenceIndex`
- 前置声明 `IQueryPlanner`、`IQueryExecutor`、`IReferenceIndexProvider`、`IReferenceIndexMaintainer`
- 保持现有 `ISCTable::FindRecords` 和 `ISCDatabase::GetTable` 等接口不变

### 3.4 `Storage/Include/SCStorage.h` 修改
目的：
- 对外统一导出查询与引用能力头文件。

改动重点：
- 新增包含 `SCQuery.h`
- 新增包含 `ISCQuery.h`
- 保持旧包含路径兼容

### 3.5 `Storage/Include/SCTypes.h` 轻量调整
目的：
- 为引用记录、查询计划和结果类型提供最小必要的基础类型对齐。

改动重点：
- 保持已有 `SCQueryCondition` 兼容声明
- 避免一次性破坏现有条件表达
- 为新类型提供桥接别名或过渡结构

### 3.6 `Storage/Src/Query/SCQueryPlanner.cpp` 新增
目的：
- 负责将新旧查询入口归一化为 `QueryPlan`。

职责：
- 接收 `QueryTarget / QueryConditionGroup / SortSpec / QueryPage / QueryHints / QueryConstraints`
- 输出 `QueryPlan`
- 判断 `DirectIndex / PartialIndex / ScanFallback / Unsupported`
- 记录 `fallbackReason`、`estimatedRows`、`estimatedCost`

### 3.7 `Storage/Src/Query/SCQueryExecutor.cpp` 新增
目的：
- 负责统一执行计划。

职责：
- 将计划分派到 SQLite / Memory backend
- 维护 fallback 扫描执行语义
- 填充 `QueryExecutionResult`

### 3.8 `Storage/Src/Query/SCReferenceIndex.cpp` 新增
目的：
- 负责引用索引的统一维护与读取桥接。

职责：
- 实现 `ReferenceIndex` / `ReverseReferenceIndex` 的内部组织
- 提供按 source / target 的检索桥接
- 提供 diagnostic-only 全量输出桥接

### 3.9 `Storage/Src/Query/SCQueryBridge.cpp` 新增
目的：
- 为现有 `FindRecords`、引用检查、表视图查询提供旧到新桥接。

职责：
- 旧接口输入 -> 生成 `QueryPlan`
- 旧接口输出 <- 转换执行结果
- 保持现有调用方语义不变

### 3.10 `Storage/Src/Sqlite/SCSqliteAdapter.cpp` 修改
目的：
- 接入 SQLite 查询执行、索引查询与引用索引读取。

改动点：
- 增加 plan 执行入口
- 将可下推条件转换为 SQL where/order/limit
- 将不可下推条件留给后置过滤
- 接入索引版本检查与索引初始化入口
- 接入引用索引 provider / maintainer

### 3.11 `Storage/Src/Memory/SCMemoryDatabase.cpp` 修改
目的：
- 接入 Memory backend 的同语义执行。

改动点：
- 计划执行入口
- 后置过滤入口
- 引用索引 provider 的内存态实现

### 3.12 `Storage/Src/Computed/SCTableView.cpp` 修改
目的：
- 将视图层查询桥接到 `QueryPlan`。

改动点：
- 视图条件映射到单目标 QueryTarget
- 视图内引用查询保持现有语义

### 3.13 `Storage/Src/Batch/SCBatchOperations.cpp` 轻量接入
目的：
- 在批处理相关引用校验路径中复用引用索引。

改动点：
- 删除前引用检查优先走 `IReferenceIndexProvider`
- 失败时回退现有扫描路径

### 3.14 `Storage/Include/ISCDiagnostics.h` / `Storage/Src/Diagnostics/SCDiagnostics.cpp`
目的：
- 扩展诊断输出，增加查询计划与索引命中分析。

改动点：
- 增加 `QueryExecutionResult` 摘要输出
- 增加 fallback / 索引命中 / 扫描成本诊断项

### 3.15 `Storage/Tests/M2SqliteTests.cpp`
目的：
- 验证 SQLite 查询下推、fallback、引用索引桥接、索引版本检查。

### 3.16 `Storage/Tests/M3Tests.cpp`
目的：
- 验证 QueryPlan 规范化与兼容行为。

### 3.17 `Storage/Tests/PerformanceSmokeTests.cpp`
目的：
- 验证 fallback 扫描与索引命中统计不会破坏性能烟测边界。

## 四、新增类型/接口

### 4.1 新增类型
- `QueryTargetType`
- `QueryTarget`
- `QueryConditionOperator`
- `QueryLogicOperator`
- `QueryCondition`
- `QueryConditionGroup`
- `SortSpec`
- `QueryPage`
- `QueryHints`
- `QueryConstraints`
- `QueryPlanState`
- `QueryPlan`
- `QueryExecutionMode`
- `QueryExecutionResult`
- `ReferenceRecord`
- `ReverseReferenceRecord`
- `ReferenceIndex`
- `ReverseReferenceIndex`

### 4.2 新增接口
- `IQueryPlanner`
- `IQueryExecutor`
- `IReferenceIndexProvider`
- `IReferenceIndexMaintainer`

### 4.3 新增桥接职责
- `QueryPlan` 生成桥接
- 旧查询入口兼容桥接
- 引用检查桥接
- fallback 扫描桥接

## 五、执行流程改造

### 5.1 查询主流程
1. 旧接口或新接口进入查询层。
2. 由 planner 生成 `QueryPlan`。
3. 判断 `QueryPlanState`。
4. executor 选择 SQLite 下推、Memory 执行或 fallback 扫描。
5. 统一填充 `QueryExecutionResult`。
6. 通过旧接口返回兼容结果。

### 5.2 SQLite 路径
1. 优先检查索引版本。
2. 若满足下推条件，组装 SQL where/order/limit。
3. 对部分下推条件保留后置过滤。
4. 若条件无法下推且允许 fallback，则进入受控扫描。
5. 输出命中的索引标识与扫描成本。

### 5.3 Memory 路径
1. 按 `QueryTarget` 选定目标集合。
2. 按条件组做快速筛选。
3. 按后置条件过滤。
4. 按排序和分页返回。

### 5.4 引用检查路径
1. 优先按 source / target 查询引用索引。
2. 索引缺失或失效时回退到受控扫描。
3. 诊断输出中标记回退原因。

### 5.5 索引初始化与重建
1. 创建库时初始化基础索引。
2. 打开库时检查索引版本。
3. 版本不匹配时进入升级补建。
4. 索引损坏时进入重建入口。

## 六、验证方式

- 构建验证：
  - Storage 库可编译
  - SQLite backend 可编译
  - Memory backend 可编译
  - 现有测试可编译
- 行为验证：
  - 旧查询入口仍可调用
  - 简单条件走下推
  - 复杂条件可 fallback
  - 引用检查可按 source / target 查询
  - 索引版本检查可触发重建或只读降级

## 七、兼容性影响

- 不破坏 Task 1 / Task 2 / Task 3 / Task 4 既有语义。
- 不引入多表查询 / join。
- 不要求现有调用方立即改用新接口。
- 旧 `FindRecords`、视图查询、引用检查入口保持兼容。
- 新接口仅作为内部计划与执行基础，不改变外部结果语义。

## 八、风险点

- 旧查询接口与新 `QueryPlan` 的映射可能存在边界差异，需要单元测试覆盖。
- SQLite 下推与后置过滤的切分若过宽，会造成性能回退。
- 引用索引的正式来源边界必须严格控制，否则会污染正式引用状态。
- fallback 扫描必须强制分页，否则在大数据场景会放大成本。
- 索引重建入口必须与升级入口隔离，避免误把诊断修复变成隐式写入。

