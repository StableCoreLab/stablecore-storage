# 复合逻辑索引实现方案

## 目标

实现显式多列逻辑索引，使其具备以下能力：

- 遵循索引列顺序与前缀匹配规则
- 当查询排序与索引顺序一致时，可覆盖有序扫描
- 与记录持久化保持事务一致性
- 在 reopen / undo / redo 后保持一致，且不会让已删除记录复活
- 不向 public API 泄漏 SQLite 实现细节

本方案**不包含**历史数据库升级处理。  
当前实现仅面向现有代码路径。

## 实现方向

采用以下组合方案：

- 以 `SCIndexDef` 为驱动的存储语义级逻辑索引
- 以 SQLite 辅助表作为逻辑索引的持久化载体
- 以显式 schema version / migration 表达这套持久化结构的存在

不要把 `SCColumnDef.indexed` 视为复合索引查询行为的主语义来源。  
从当前重建后的语义开始，`requireIndex` 只认显式 `SCIndexDef`。

当前实现说明：

- 显式复合索引能力通过新的 schema version 持久化表达
- 新建库与当前代码路径统一以该版本语义创建
- 本方案不再考虑旧历史数据文件兼容与升级回填

## 主要设计

### 1. 查询计划增强

当前 public planner 接口拿不到 database/schema 上下文，因此无法仅靠 `IQueryPlanner` 完成复合索引匹配。

实现方式：

- 保留现有 public planner 的归一化能力
- 在 SQLite 执行前增加 backend 内部的 plan enrichment 步骤
- 为 `QueryPlan` 填充命中索引信息、pushdown 条件与 residual 条件

当前实现说明：

- public planner 仍然保持 schema-agnostic
- 真正的 schema-aware 复合索引匹配发生在 SQLite executor 分发前
- executor 通过 backend 私有接口解析 query-index 能力

### 2. 辅助存储

新增 SQLite 辅助表：

- `query_indexes`
- `query_index_entries`

辅助存储按 record 持久化逻辑索引 entry，内部使用编码后的复合键与前缀键。

### 3. 前缀与排序语义

对于索引 `(A, B, C)`：

- `A = ?` 命中前缀长度 1
- `A = ? AND B = ?` 命中前缀长度 2
- `B = ?` 不命中该索引前缀
- 只有当查询排序在 equality prefix 之后与索引顺序一致时，才识别为排序覆盖

第一阶段实现范围：

- 仅支持最多 3 列的显式索引
- 复合索引直达路径仅支持单个 `AND` 条件组
- 不支持的查询形态继续走现有 fallback 路径

### 3.1 Residual Normalization 规则

为避免 planner / executor 在 residual 条件处理上隐式扩张语义，当前仅允许以下非常保守的归一化：

- 若同一字段已经命中 `Equal(non-null)`，则可移除该字段上的 `IsNotNull`

当前**不**做以下归一化：

- `Equal(NULL)` 不会消掉 `IsNotNull`
- 不做跨字段推理
- 不做 `Between` / `StartsWith` / `In` 的蕴含化简
- 不做 OR 条件归一化
- 不做更一般的布尔代数化简

后续若要新增 residual normalization 规则，必须：

- 先补对应的正反例测试
- 再显式更新本文档
- 再扩实现

### 4. 持久化一致性约束

复合 query-index entries 按记录真实状态重建：

- 显式索引新增或删除时
- touched records 提交时
- 显式 repair / rebuild 时

这一策略优先保证正确性，而不是最小写放大。

注意：

- 必须具备 query-index entries 的重建能力
- 但不能在 database load / open 时自动重建
- 因为 `open()` 不得写库，reads 也不得写库
- 若需要补建或修复 query-index entries，必须通过显式写路径、显式 repair，或显式 upgrade 完成

## 计划中的代码改动

### Public 查询模型

扩展 `QueryPlan`，增加：

- 命中索引元数据
- pushdown / residual 条件分区

### Backend 私有查询桥接

增加一个仅供 SQLite backend 与 SQLite query executor 使用的私有内部接口，使 executor 可以：

- 请求带 schema 感知的 plan enhancement
- 从逻辑 query index 获取候选 record id

同时避免在上层嵌入 SQL 细节。

当前实现说明：

- executor 优先从 `context.database` 安全解析该私有接口
- 仅在调用方已显式传入精确 backend-private handle 时，才回退使用 `backendHandle`
- `backendHandle` 不是跨接口指针恒等的正式语义承诺
- 当前实现有意保持这一折中 bridge 形态，不额外扩大 query public API 或 backend-private handle 改造范围

### SQLite Backend

新增：

- 辅助表初始化
- query-index metadata 持久化
- 编码后的复合键生成
- 按 record / 按 table 的 query-index 重建路径
- legacy 列索引物化与复合 query-index 重建入口在内部已分离；两类入口都属于显式 repair/write path，不可从 load/open 调用
- SQLite backend 通过 `IQueryIndexProvider / IQueryIndexMaintainer` 暴露 query-index 健康检查与显式 repair 能力；活动编辑期间只允许检查为 `OutOfDate`，不允许直接重建
- schema add/remove index 的联动处理

### SQLite Executor

增加复合索引执行路径，用于：

- 通过 backend 私有接口增强 plan
- 从逻辑索引中收集候选 record id
- 通过 public table API materialize records
- 必要时执行 residual 过滤
- 当索引顺序已覆盖查询排序时，跳过内存排序

## 验证预期

新增或更新的测试应覆盖：

- 显式多列索引持久化
- 前缀长度匹配行为
- 排序覆盖行为
- add/remove index 语义
- undo/redo 一致性
- reopen 一致性
- 已删除记录不复活

## 当前兼容语义说明

当前实现仍保留 legacy 单列 `SCColumnDef.indexed` 参与旧的 direct/partial query 路径；  
但在 `requireIndex=true` 的场景下，只认显式 `SCIndexDef`，不再接受 legacy hint 作为满足条件。

## 第一阶段有意保留的限制

- 不处理历史升级路径
- 不做 query public API 的全局重构
- 不尝试在第一版复合执行路径中支持所有复杂 OR/range 组合
