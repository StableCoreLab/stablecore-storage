# Task 6-1 结果确认单

## 一、最终新增/修改的公开接口

- 新增公开类型：
  - `QueryTargetType`
  - `QueryTarget`
  - `QueryConditionOperator`
  - `QueryLogicOperator`
  - `QueryCondition`
  - `QueryConditionGroup`
  - `QueryOrderDirection`
  - `SortSpec`
  - `QueryPage`
  - `QueryHints`
  - `QueryConstraints`
  - `QueryPlanState`
  - `QueryPlan`
  - `QueryExecutionMode`
  - `QueryExecutionResult`
  - `QueryIndexHealthState`
  - `QueryIndexCheckResult`
  - `ReferenceDirection`
  - `ReferenceIndexHealthState`
  - `ReferenceIndexCheckResult`
  - `ReferenceRecord`
  - `ReverseReferenceRecord`
  - `ReferenceIndex`
  - `ReverseReferenceIndex`
  - `CommitId`
  - `SessionId`
  - `SnapshotId`

- 新增公开接口：
  - `IQueryPlanner`
  - `IQueryExecutor`
  - `IQueryIndexProvider`
  - `IQueryIndexMaintainer`
  - `IReferenceIndexProvider`
  - `IReferenceIndexMaintainer`

- 旧接口保持兼容：
  - `ISCTable::FindRecords(const SCQueryCondition&, ...)`
  - `SCQueryCondition` 旧桥接结构
  - 现有 `FindRecords / EnumerateRecords / GetRecord` 行为不变

## 一补充、收敛修订

- `QueryExecutionMode` 已从单一 `Planned` 粒度收敛为：
  - `DirectIndex`
  - `PartialIndex`
  - `FallbackScan`
  - `Unsupported`
- `QueryPlan.conditionGroups` 的组合语义已显式化：
  - `conditionGroupLogic` 负责多个 group 之间的组合方式
  - 第一阶段不允许嵌套 group
  - 组合语义与 group 内部 `logic` 分离

## 二、收口点确认

- `SCQueryBridge` 仅保留前置声明，不提供实现。
- `IQueryExecutor` 仅定义统一执行入口，不绑定单一后端分支。
- 查询索引检查与引用索引检查分离，接口层均已预留。
- `QueryExecutionResult` 已包含：
  - `usedIndexIds`
  - `fallbackTriggered`
  - `scannedRows`
  - `matchedRows`
  - `returnedRows`

## 三、兼容性确认

- 现有查询入口行为不变。
- 现有后端查询逻辑不变。
- 本轮仅新增类型与接口定义，不引入执行行为变化。
