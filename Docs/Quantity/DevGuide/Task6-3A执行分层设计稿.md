# Task 6-3A 执行分层设计稿

## 一、执行分层目标

本阶段只定义 Query 执行架构的分层职责，不落地具体执行代码。

目标是建立一条稳定的统一执行链路：

1. 旧接口或新接口进入查询层
2. 通过 planner 生成 `QueryPlan`
3. 由统一 executor 入口接收计划
4. 由 backend-specific executor 完成实际执行
5. 输出统一的 `QueryExecutionResult`

本阶段不要求引入 fallback 扫描实现，也不要求改变现有查询返回语义。

---

## 二、Executor 分层模型

### 2.1 Core Executor

Core executor 是查询执行的统一调度层，负责：

- 接收 `QueryPlan`
- 选择执行路径
- 校验执行约束
- 创建和归一化 `QueryExecutionResult`
- 统一处理执行前后状态
- 统一记录执行级观测信息的容器结构

Core executor 不负责：

- SQLite SQL 拼装
- Memory backend 数据遍历
- fallback 扫描实现
- 索引写入或索引维护
- reference index 更新

### 2.2 Backend-Specific Executor

backend-specific executor 是实际执行层，按后端分别实现：

- SQLite executor
- Memory executor

该层负责：

- 把 `QueryPlan` 转换为后端可执行动作
- 在后端内部完成筛选、分页、排序、结果收集
- 向 `QueryExecutionResult` 回填实际执行数据

该层不负责：

- 规划
- legacy 参数解析
- 与其他 backend 共享的调度逻辑

### 2.3 分层关系

建议关系为：

- `IQueryExecutor` 作为统一执行入口
- Core executor 负责调度和归一化
- backend-specific executor 负责具体执行

不得将所有 backend 分支写入单一巨型 executor。

---

## 三、SQLite Executor 角色

SQLite executor 是 SQLite 后端的执行适配层。

### 3.1 职责

- 接收 `QueryPlan`
- 将可下推条件转换为 SQLite 可执行表达
- 处理 SQLite 的排序和分页
- 将 SQLite 结果集回填为统一结果输出
- 回写可观测字段

### 3.2 边界

SQLite executor 只负责“SQLite 可执行部分”。

不应在本阶段承担：

- fallback 扫描完整实现
- 跨后端协调
- 引用索引维护
- 查询索引重建

### 3.3 与 future fallback 的关系

未来 fallback 扫描若加入 SQLite 路径，应挂在 SQLite executor 的执行分支中，由 executor 决定是否进入回退路径。

Core executor 只负责看到计划状态并转交，不负责 fallback 细节。

---

## 四、Memory Executor 角色

Memory executor 是内存后端的执行适配层。

### 4.1 职责

- 接收 `QueryPlan`
- 在内存数据集上执行条件筛选
- 处理排序、分页、结果回填
- 回写执行观测字段

### 4.2 边界

Memory executor 不应依赖 SQLite 语义。

不得把 SQLite 专用逻辑复制到 Memory executor 中。

不得在本阶段引入新的扫描框架或 fallback 机制。

### 4.3 统一性要求

Memory executor 的职责与 SQLite executor 语义一致，但实现方式可不同。

统一的是：

- 输入是 `QueryPlan`
- 输出是 `QueryExecutionResult`
- 观测字段语义一致

---

## 五、QueryExecutionResult 填充责任

### 5.1 Core executor 责任

Core executor 负责：

- 初始化 `QueryExecutionResult`
- 设置默认错误码和默认执行模式
- 在执行完成后做结果归一化
- 当后端未提供完整信息时填补安全默认值

### 5.2 Backend executor 责任

backend-specific executor 负责：

- 填充 `usedIndexIds`
- 填充 `fallbackTriggered`
- 填充 `scannedRows`
- 填充 `matchedRows`
- 填充 `returnedRows`
- 填充执行说明和后端上下文信息

### 5.3 观测字段归属

观测字段的真实来源应由 backend executor 提供，Core executor 仅做收口和标准化。

这些字段的用途包括：

- 性能诊断
- fallback 判断
- 索引命中分析
- 后续自动优化

---

## 六、与现有接口桥接

### 6.1 legacy 入口到 executor 的路径

现有 legacy 接口不直接改为 executor 接口，而是通过桥接层进入统一执行链路：

1. legacy 参数进入 `SCQueryBridge`
2. `SCQueryBridge` 生成 `QueryPlan`
3. Core executor 接收 `QueryPlan`
4. Core executor 选择 backend executor
5. backend executor 执行
6. `SCQueryBridge` 适配返回结果语义

### 6.2 保持现有接口不变

`FindRecords`、`EnumerateRecords`、`GetRecord` 等现有入口保持原语义。

本阶段只引入“内部统一执行链路”，不要求调用方改用新接口。

### 6.3 兼容要求

旧接口桥接必须满足：

- 参数转换不丢语义
- 结果适配不改变返回含义
- 不引入新的隐式写入路径

---

## 七、第一阶段不实现的内容

本阶段明确不实现：

- SQLite SQL 拼装
- Memory backend 执行逻辑
- fallback 扫描执行器
- `ReferenceIndex` 写入或维护
- `ReverseReferenceIndex` 写入或维护
- 多表联查
- join / group by / having
- 执行器内部多 backend 硬编码分支实现

---

## 八、进入 6-3B / 6-3C 前必须确认的事项

进入下一阶段前，需要先确认以下边界：

1. Core executor 是否只做调度，还是同时承担基础结果归一化
2. backend executor 是否按数据库类型独立工厂化
3. fallback 扫描未来挂在 backend executor 还是单独的 fallback executor
4. `QueryExecutionResult` 中哪些字段由 Core 填充，哪些字段由 backend 填充
5. legacy 入口是否统一通过 `SCQueryBridge` 进入执行链路，还是保留部分直接路径
6. SQLite / Memory 是否采用同一 executor 接口族，还是分开扩展

