# StableCore Storage Task Breakdown

这份文档把 [Roadmap.md](d:/code/StableCore/stablecore-storage/Docs/Roadmap.md) 中的 `M1 ~ M3` 进一步拆成顺序执行的任务列表。

原则：

- 任务按依赖顺序排列
- 每个任务尽量有明确产出
- 不跨阶段并行做高耦合事项

---

## M1 可开发

### T1. 清理公共 API 骨架

目标：

- 把当前头文件从“骨架”收敛成“V1 可用接口”

工作项：

- 审查 `Include/StableCore/Storage/Interfaces.h`
- 审查 `Include/StableCore/Storage/Types.h`
- 去掉明显不成熟或重复的接口
- 明确 `Value`、`ColumnDef`、`QueryCondition` 的最终 V1 形态
- 给所有公开接口补简短注释

产出：

- 稳定版公共头文件

### T2. 完善 `Value` 类型能力

目标：

- 让 `Value` 成为正式可用的统一值对象

工作项：

- 增加类型安全访问函数
- 增加 `AsInt64/AsDouble/AsBool/AsString/AsRecordId`
- 明确 `Null` 的判定语义
- 明确 `Enum` 和 `RecordId` 的访问方式
- 统一错误返回风格

产出：

- 可正式使用的 `Value`

### T3. 明确字符串与内存语义

目标：

- 避免 `GetString()` 的生命周期不清楚

工作项：

- 明确 `GetString()` 返回值生命周期
- 决定是否补 `GetStringCopy(...)`
- 在接口和文档中明确跨事务、跨写入后的行为

产出：

- 字符串访问约定

### T4. 收紧错误码和错误行为

目标：

- 让接口行为可测试、可预期

工作项：

- 把错误码约定同步进代码注释
- 确认哪些场景返回：
  - `SC_E_RECORD_NOT_FOUND`
  - `SC_E_RECORD_DELETED`
  - `SC_E_VALUE_IS_NULL`
  - `SC_E_TYPE_MISMATCH`
  - `SC_E_NO_ACTIVE_EDIT`
- 清理当前实现中的模糊返回

产出：

- 一致的错误语义

### T5. 修正内存实现的事务语义

目标：

- 让内存实现成为可靠语义基线

工作项：

- 检查 `BeginEdit/Commit/Rollback`
- 检查 active edit 生命周期
- 明确事务关闭后的行为
- 拒绝事务外写入
- 检查空事务提交行为

产出：

- 可靠的事务行为

### T6. 修正内存实现的删除语义

目标：

- 让 `invalid/tombstone` 语义稳定

工作项：

- 删除后的读取行为统一
- 删除后的写入禁止
- `Undo/Redo` 恢复 deleted record
- 保证 `recordId` 稳定

产出：

- 稳定的删除/恢复语义

### T7. 完善 Journal 聚合逻辑

目标：

- 降低无效 Journal 噪音，保证 Undo/Redo 可靠

工作项：

- 同事务同字段多次写入只保留首尾值
- `CreateRecord` 后初始化字段写入策略
- `DeleteRecord` 后禁止后续字段写入
- 检查 Undo/Redo 对聚合 Journal 的兼容性

产出：

- 可依赖的 Journal 行为

### T8. 完善 `ChangeSet` 生成逻辑

目标：

- 让外部观察者真正能用 `ChangeSet`

工作项：

- 修正 `Commit`
- 修正 `Undo`
- 修正 `Redo`
- 检查 `RecordCreated/RecordDeleted/RelationUpdated`
- 检查 `oldValue/newValue` 方向是否正确
- 检查 `version/actionName/source`

产出：

- 可供 UI 和缓存使用的 `ChangeSet`

### T9. 完善 Schema 校验

目标：

- 让字段定义真正约束数据

工作项：

- 检查字段新增约束
- 校验 `FactField/RelationField`
- 校验 `referenceTable`
- 校验默认值类型
- 校验 nullable / editable

产出：

- 可依赖的 Schema 校验

### T10. 完善最小查询能力

目标：

- 让 `GetRecord/EnumerateRecords/FindRecords` 具备稳定语义

工作项：

- 明确 deleted record 是否出现在枚举中
- 明确等值查询是否使用默认值
- 明确关系字段查询行为
- 明确无结果和异常的区别

产出：

- 可用的最小查询基线

### T11. 建立单元测试工程

目标：

- 用测试固定 M1 语义

工作项：

- 引入测试框架
- 建测试目录结构
- 建基础测试入口
- 先覆盖 `Value`、Schema、事务、删除、Undo/Redo、ChangeSet、查询

产出：

- 可持续扩展的测试工程

### T12. 写 M1 核心测试

目标：

- 用测试钉住关键行为

工作项：

- 表创建和字段定义测试
- 普通字段写入测试
- 关系字段写入测试
- 回滚测试
- 删除与恢复测试
- `Undo/Redo` 测试
- `ChangeSet` 测试
- 查询测试

产出：

- M1 回归测试集

### T13. 补最小示例和开发文档

目标：

- 让上层开发可直接上手

工作项：

- 写最小内存版示例
- 更新 README 中的接口示例
- 标注哪些是已实现，哪些是占位

产出：

- M1 开发接入文档

---

## M2 可持久化

### T14. 设计 SQLite 物理表结构

目标：

- 把逻辑模型映射为稳定的 SQLite 结构

工作项：

- 定义 `tables`
- 定义 `schema_columns`
- 定义 `records`
- 定义 `field_values`
- 定义 `journal_transactions`
- 定义 `journal_entries`
- 定义 `metadata`
- 定义索引

产出：

- SQLite schema 初版

### T15. 编写 SQLite 适配层

目标：

- 提供薄封装而不是 ORM

工作项：

- 实现 `SqliteDb`
- 实现 `SqliteStmt`
- 实现 `SqliteTxn`
- 实现错误码映射

产出：

- 可复用的 SQLite 薄适配层

### T16. 实现数据库初始化与元数据加载

目标：

- 第一次建库和重复打开都能正常运行

工作项：

- 建库 SQL
- 版本元数据初始化
- 表定义加载
- schema 列定义加载

产出：

- 可打开/初始化的 SQLite 数据库

### T17. 实现 SQLite 版 Schema 管理

目标：

- 把 `Schema` 真正落库

工作项：

- 持久化字段定义
- 加载字段定义
- 用户自定义字段持久化
- 默认值/关系字段/目标表信息持久化

产出：

- 可持久化的 Schema

### T18. 实现 SQLite 版记录创建与字段写入

目标：

- 把 `CreateRecord/SetValue/DeleteRecord` 真正落库

工作项：

- 创建 record
- 写 field value
- 删除 record
- 更新 lastModifiedVersion
- 关系字段写入校验

产出：

- 可写入 SQLite 的基础数据路径

### T19. 实现 SQLite 版事务对齐

目标：

- 存储事务和 SQLite 事务一致

工作项：

- `BeginEdit` 开启事务
- `Commit` 提交事务
- `Rollback` 回滚事务
- 异常中断时的回滚处理

产出：

- 一致的事务语义

### T20. 实现 SQLite 版读取与查询

目标：

- 完成最小持久化读路径

工作项：

- `GetTable`
- `GetRecord`
- `EnumerateRecords`
- `FindRecords`
- 关系字段查询

产出：

- 可读可查的 SQLite 后端

### T21. 实现 Journal 持久化

目标：

- 为持久化 Undo/Redo 打基础

工作项：

- journal transaction 入库
- journal entry 入库
- Journal 加载

产出：

- 持久化 Journal

### T22. 实现 SQLite 版 Undo/Redo

目标：

- 把内存语义迁移到持久化后端

工作项：

- Undo 栈/Redo 栈持久化策略
- 基于 Journal 执行 Undo
- 基于 Journal 执行 Redo
- 同步生成 `ChangeSet`

产出：

- 可持久化的 Undo/Redo

### T23. 建立 SQLite 集成测试

目标：

- 验证 SQLite 版与内存版语义一致

工作项：

- 建 SQLite 测试基类
- 跑与 M1 类似的行为测试
- 增加重启恢复测试
- 增加持久化 Journal 测试

产出：

- SQLite 回归测试集

---

## M3 可接产品

### T24. 跑通楼层关系基线

目标：

- 验证关系字段能承载真实业务

工作项：

- 建 `Floor` 表
- 建 `Beam/Wall` 表
- 定义 `FloorRef`
- 测试按楼层查询构件
- 测试楼层变更的 `RelationUpdated`

产出：

- 真实业务关系样例

### T25. 建立计算字段元模型

目标：

- 为数据表工具和增量重算提供正式模型

工作项：

- `ComputedColumnDef`
- 依赖声明结构
- `ComputedFieldKind`
- 列分层模型

产出：

- 计算字段定义模型

### T26. 实现最小表达式求值器

目标：

- 先支持简单公式列

工作项：

- 字段引用
- 常量
- 四则运算
- 括号
- 少量函数

产出：

- 最小可用公式引擎

### T27. 实现 `ruleId` 注册机制

目标：

- 承接复杂算量规则

工作项：

- 规则注册表
- `ruleId -> evaluator`
- 基础求值上下文

产出：

- 可扩展的规则型计算字段机制

### T28. 实现计算字段缓存与失效

目标：

- 让 `ChangeSet + Version` 真正驱动计算列

工作项：

- 缓存键设计
- 依赖失效
- 版本号失效
- 只读展示列刷新

产出：

- 可失效的计算字段缓存

### T29. 补批量写入与导入能力

目标：

- 让产品能承载真实导入和批量编辑场景

工作项：

- 批量新增
- 批量字段修改
- 批量关系更新
- 导入事务优化

产出：

- 可用于导入的批量能力

### T30. 补性能与索引优化

目标：

- 让 SQLite 版在中大数据量下可用

工作项：

- 查询热点分析
- 索引补充
- Journal 写入优化
- 批量事务优化

产出：

- 性能基线

### T31. 建数据库升级与迁移方案

目标：

- 避免后续版本迭代时数据库无法升级

工作项：

- schema version
- migration runner
- 向后兼容策略

产出：

- 数据库迁移机制

### T32. 建崩溃恢复和诊断能力

目标：

- 产品接入前必须具备基本恢复能力

工作项：

- 启动自检
- 损坏处理策略
- 日志与诊断信息
- Journal/事务异常恢复策略

产出：

- 基础恢复与诊断能力

### T33. 建产品接入示例

目标：

- 让上层能直接集成

工作项：

- 数据表编辑工具示例
- 楼层/构件关系示例
- 计算字段示例
- Observer + ChangeSet 示例

产出：

- 产品接入样例

---

## 推荐执行顺序

严格建议按下面顺序推进：

1. `T1 ~ T13`
2. `T14 ~ T23`
3. `T24 ~ T33`

不要在 `M1` 未稳定前大规模进入 SQLite 实现。

## 建议下一步

当前最合理的起点是：

1. `T1` 清理公共 API 骨架
2. `T2` 完善 `Value`
3. `T5` 修正内存实现事务语义
4. `T11` 建立单元测试工程

这是最小闭环。
