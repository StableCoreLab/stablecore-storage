# 性能冒烟基线

本文定义 Storage 当前可执行的性能 smoke baseline，以及对应的回归门槛。

## 目标

- 保留当前主路径“可运行”验证。
- 补充可重复的导入、查询、恢复和排序样本。
- 为后续趋势观察提供稳定输出字段，而不是只看单次耗时。

## 当前基线

### 1. 大批量导入

- 测试名称：`StoragePerformance.SqliteBulkImportBaseline`
- 规模：`2000` 条记录
- 路径：SQLite 持久化导入
- 关注点：导入结果、chunk 数、checkpoint 数、总耗时

### 2. 查询与排序

- 测试名称：`StoragePerformance.SqliteQueryAndSortBaseline`
- 规模：`2000` 条记录
- 路径：SQLite 查询执行
- 条件：`FloorRef = floorId`
- 排序：`Width DESC`
- 关注点：计划状态、命中索引、返回条数、排序正确性、总耗时

### 3. 恢复与 finalize

- 测试名称：`StoragePerformance.SqliteRecoveryFinalizeBaseline`
- 规模：`2000` 条记录
- 路径：SQLite 导入恢复后 finalize
- 关注点：恢复状态可加载、finalize 可完成、恢复状态最终清空、总耗时

## 回归门槛

当前门槛按“足够宽松、足够可执行”的原则设置：

- 大批量导入：`<= 15000 ms`
- 查询与排序：`<= 15000 ms`
- 恢复 finalize：`<= 15000 ms`

说明：

- 这些门槛是 smoke gate，不是产品级 SLA。
- 如果后续在 CI 或不同机器上明显波动，可以把阈值外置为配置，但测试必须继续输出同样的趋势字段。

## 趋势字段

建议持续保留以下输出：

- `elapsed_ms`
- `created_count`
- `chunk_count`
- `checkpoint_count`
- `matched_rows`
- `returned_rows`

## 使用原则

- 修改查询、导入、恢复、缓存或 SQLite 持久化逻辑后，应先跑通过这些 smoke baseline。
- 如果某次改动让基线变慢，必须先判断是否为预期退化，再决定是否调整门槛。
- 如果基线失败，不应直接降低门槛掩盖问题，应优先确认功能行为是否被破坏。
