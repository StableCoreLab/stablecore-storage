# Task 6-2 结果确认单

## 一、本次新增/修改文件

- `Storage/Include/SCQuery.h`
- `Storage/Include/ISCQuery.h`
- `Storage/Include/ISCInterfaces.h`
- `Storage/Src/Query/SCQueryPlanner.cpp`
- `Storage/Src/Query/SCQueryBridge.cpp`
- `Storage/Src/Computed/SCTableView.cpp`
- `Storage/CMakeLists.txt`
- `Storage/Tests/QueryPlannerBridgeTests.cpp`
- `Quantity/Extern/stablecore-storage/Include/SCQuery.h`
- `Quantity/Extern/stablecore-storage/Include/ISCQuery.h`
- `Quantity/Extern/stablecore-storage/Include/ISCInterfaces.h`

## 二、Planner / Bridge 收口

- `SCQueryBridge` 只负责 legacy 参数到 `QueryPlan` 的结构化转换，以及 `QueryExecutionResult` 到 legacy 状态的适配。
- `SCQueryBridge` 不负责规划、不负责执行、不负责索引维护。
- `CreateDefaultQueryPlanner()` 提供默认 planner 实例。
- planner 只负责计划归一化与状态判定，不接触 SQLite / Memory backend 执行逻辑。

## 三、组合语义确认

- `QueryConditionGroup.logic` 负责 group 内条件组合。
- `QueryPlan.conditionGroupLogic` 负责多个 group 之间组合。
- 第一阶段不支持嵌套 group。
- `conditionGroupLogic == Or` 且 group 数量大于 1 时，planner 收敛为 `ScanFallback`。

## 四、旧入口映射确认

- `SCQueryCondition` 旧入口映射为单个 `QueryConditionGroup`。
- 旧入口默认映射为 `QueryTargetType::Table`。
- 旧入口只生成规范化计划，不直接改变旧查询返回语义。

## 五、计划状态确认

- `DirectIndex`：条件可直接下推。
- `PartialIndex`：存在可部分下推的条件。
- `ScanFallback`：存在 fallback 条件或多 group OR 组合。
- `Unsupported`：目标无效、条件结构无效、约束不允许 fallback / partial / index。

## 六、兼容性确认

- 现有 `FindRecords / EnumerateRecords / GetRecord` 行为不变。
- SQLite 查询执行逻辑不改。
- Memory backend 查询逻辑不改。
- ReferenceIndex 维护逻辑不改。
- fallback 扫描执行器不引入。
