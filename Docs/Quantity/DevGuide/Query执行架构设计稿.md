# Query 执行架构设计稿

本文档定义 Query / Reference 在 Storage Core、Adapter 与现有接口之间的执行分层、下推策略和 fallback 模型。本文档不包含实现代码。

> 说明：`QueryReference接口定义稿.md` 已并入本文，后续查询与引用索引语义以本文为唯一主文档。

## 0. 公共查询与引用模型

### 0.1 计划状态与执行模式

统一使用以下计划状态：

- `DirectIndex`
- `PartialIndex`
- `ScanFallback`
- `Unsupported`

统一使用以下执行模式：

- `Planned`
- `FallbackScan`
- `Unsupported`

### 0.2 查询条件与组合

查询条件模型统一包含：

- `QueryTarget`
- `QueryCondition`
- `QueryConditionGroup`
- `SortSpec`
- `QueryPage`
- `QueryHints`
- `QueryConstraints`

其中：

- `QueryTarget` 仅表达单表或单视图目标，不承载 join、union 或 cross-table lookup。
- `QueryConditionOperator` 至少覆盖等值、范围、集合、空值、前缀、模糊和尾缀判断。
- `QueryLogicOperator` 仅区分 `And` 与 `Or`，并以条件组表达组合关系。
- `QueryPage` 负责偏移与条数，`limit = 0` 代表使用默认上限。
- `QueryHints` 仅表达建议，不表达强约束。
- `QueryConstraints` 表达强约束，其中 `requireIndex` 和 `allowFallbackScan` 直接影响执行路径。

### 0.3 引用索引模型

引用模型统一以 `ReferenceDirection`、`ReferenceRecord`、`ReferenceIndex`、`ReverseReferenceRecord`、`ReverseReferenceIndex` 表达。

- `ReferenceDirection` 仅区分 `Forward` 与 `Reverse`。
- `ReferenceRecord` 记录源表、源记录、源列、目标表、目标记录与提交上下文。
- `ReferenceIndex` 表示正向引用集合。
- `ReverseReferenceIndex` 表示反向引用集合。
- 引用索引提供者负责读取，维护器负责重建、检查或增量维护。
- 当前版本允许以只读扫描型 provider 作为过渡实现，但语义上仍要区分“正式索引”与“诊断扫描”。

## 一、执行架构分层

### 1.1 Storage Core
职责：
- 持有 `QueryPlan`、`QueryHints`、`QueryConstraints` 的规范化定义。
- 负责查询计划生成与计划状态判断。
- 负责引用索引的正式语义和一致性边界。
- 负责把执行可观测信息回传到结果对象。

### 1.2 Adapter
职责：
- 将 `QueryPlan` 映射为具体后端执行路径。
- 执行 SQLite 下推、内存遍历或 fallback 扫描。
- 负责把后端特性差异收敛为统一执行结果。

### 1.3 SQLite Backend
职责：
- 承担可下推条件执行。
- 承担索引命中执行。
- 承担受控 fallback 扫描。

### 1.4 Memory Backend
职责：
- 维持与 Storage Core 语义对齐。
- 优先复用同一 `QueryPlan` 语义。
- 在不能下推时执行内存扫描与后置过滤。

---

## 二、QueryPlan → 执行路径映射

### 2.1 映射原则
- 先判定目标是否单表 / 单视图。
- 再判定条件是否可下推。
- 再判定排序与分页是否影响执行路径。
- 最后决定 `DirectIndex`、`PartialIndex`、`ScanFallback` 或 `Unsupported`。

### 2.2 条件映射
可下推条件：
- `Equal`
- `NotEqual`
- `LessThan`
- `LessThanOrEqual`
- `GreaterThan`
- `GreaterThanOrEqual`
- `IsNull`
- `IsNotNull`

可部分下推条件：
- `In`
- `Between`
- `StartsWith`

仅 fallback 条件：
- `Contains`
- `EndsWith`

### 2.3 计划判定
- `DirectIndex`：所有条件与排序可由索引满足。
- `PartialIndex`：部分条件和 / 或排序可由索引满足，剩余条件后置过滤。
- `ScanFallback`：条件无法充分下推，但允许扫描。
- `Unsupported`：目标、字段类型、约束组合或执行限制不允许执行。

---

## 三、SQLite 执行策略

### 3.1 下推规则
SQLite 可优先下推：
- 单字段比较
- 同字段范围比较
- `IN` 列表
- `NULL` 判断
- 简单前缀匹配
- 稳定排序下的索引顺序扫描

### 3.2 后置过滤
下列情况需要后置过滤：
- 条件组包含 `OR` 且不能完全转为索引表达
- `StartsWith` 只能局部下推
- `In` / `Between` 需要补充精确过滤
- 稳定排序需要额外二次整理

### 3.3 索引命中
SQLite 执行应显式记录：
- 命中的索引标识
- 命中索引数量
- 是否发生部分命中
- 是否回退扫描

---

## 四、Memory Backend 执行策略

### 4.1 执行方式
Memory backend 采用与计划一致的过滤顺序：
1. 按 `QueryTarget` 获取目标集合
2. 按可下推条件做快速筛选
3. 对剩余条件做后置过滤
4. 按排序规则整理
5. 按分页返回结果

### 4.2 语义要求
- 不改变计划语义。
- 不把 fallback 解释成“无代价扫描”。
- 仍需记录 scannedRows、matchedRows 与 returnedRows。

---

## 五、Fallback 扫描模型

### 5.1 扫描约束
Fallback 扫描必须满足：
- 可中止
- 强制分页
- 可统计 scannedRows
- 可记录 fallbackTriggered
- 可在诊断中报告成本

### 5.2 触发条件
允许 fallback 的条件：
- 目标字段无索引
- 条件组合无法完全下推
- 条件包含 `Contains` / `EndsWith`
- 目标计划允许回退

禁止 fallback 的条件：
- `requireIndex = true`
- `allowFallbackScan = false`
- 目标或字段属于 unsupported

### 5.3 扫描执行边界
- 扫描只处理单目标。
- 扫描过程中必须支持取消。
- 扫描结果必须在分页边界内返回。

---

## 六、执行可观测设计

### 6.1 必须观测的信息
- `planId`
- `state`
- `fallbackReason`
- `usedIndexIds`
- `fallbackTriggered`
- `scannedRows`
- `matchedRows`
- `returnedRows`
- `estimatedRows`
- `estimatedCost`

### 6.2 诊断输出
执行结果应能支持：
- 性能分析
- 低效查询排查
- 索引命中统计
- fallback 触发统计

---

## 七、与现有接口桥接

### 7.1 兼容原则
- 不破坏现有查询入口。
- 旧接口仍可直接调用。
- 内部先转换为 `QueryPlan`，再进入执行层。

### 7.2 桥接方式
- 旧过滤接口 -> planner -> executor
- 旧引用检查接口 -> reference index provider
- 旧视图查询接口 -> 现有视图语义 + plan

### 7.3 逐步演进
- 第一阶段保持结果语义兼容。
- 第二阶段逐步引导高频查询走索引执行。
- 第三阶段再考虑更细粒度的执行优化。
