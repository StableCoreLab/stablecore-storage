# StableCore Storage 开发任务清单

本文档用于将当前代码与现有设计文档中的差距，拆成可执行、可验收的开发任务。

适用范围：
- `Storage` 核心库
- SQLite 持久化层
- 查询与计算列能力
- 数据库编辑器接入层

任务排序原则：
- 先修事务与数据一致性问题
- 再收敛查询与索引语义
- 再补计算列与导入恢复边界
- 最后完善编辑器体验与测试覆盖

## T1. 修复批处理失败时的事务悬挂问题

目标：
- 保证批处理或导入在任意失败路径下都不会留下悬挂的 active edit。

文档输入：
- [Capability Gap Assessment](CapabilityGapAssessment.md)
- [Roadmap](Roadmap.md)
- [Task Breakdown](TaskBreakdown.md)

涉及代码：
- `Src/Batch/SCBatchOperations.cpp`
- `Src/Memory/SCMemoryDatabase.cpp`
- `Src/Sqlite/SCSqliteAdapter.cpp`

验收标准：
- 当 `ExecuteBatchEdit()` 或 `ExecuteImport()` 中任意一步失败时，数据库最终状态必须可继续开启新的 `BeginEdit()`。
- `rollbackOnError=false` 时也不能遗留 active edit。
- 新增测试覆盖失败后再次写入、再次导入、再次撤销的场景。

## T2. 收敛查询计划与执行语义

目标：
- 让 `DirectIndex`、`PartialIndex`、`ScanFallback` 的执行语义与文档描述一致，避免 planner 只是“标记”索引而 executor 仍然全表扫描的错觉。

文档输入：
- [V1 Baseline Decisions](V1BaselineDecisions.md)
- [Capability Gap Assessment](CapabilityGapAssessment.md)
- [Computed Table View And Startup](ComputedTableViewAndStartup.md)

涉及代码：
- `Src/Query/SCQueryPlanner.cpp`
- `Src/Query/SCQueryMemoryExecutor.cpp`
- `Src/Query/SCQuerySqliteExecutor.cpp`

验收标准：
- 查询计划输出的状态、回退原因、观测信息与执行结果保持一致。
- 对于明确要求 `requireIndex=true` 的查询，执行层必须拒绝非 DirectIndex 路径。
- 代码和文档中对“索引”一词的含义必须一致，必要时补充说明“观测索引”与“实际索引”。
- 新增测试覆盖 `Equal / In / Between / StartsWith / Contains / EndsWith` 与 `And / Or` 的计划和执行一致性。

## T3. 补齐引用索引维护器能力

目标：
- 将当前“只读扫描型引用索引提供者”补成可维护、可诊断、可扩展的接口。

文档输入：
- [V1 Baseline Decisions](V1BaselineDecisions.md)
- [Capability Gap Assessment](CapabilityGapAssessment.md)
- [Task Breakdown](TaskBreakdown.md)
- [Roadmap](Roadmap.md)

涉及代码：
- `Include/ISCQuery.h`
- `Src/Memory/SCMemoryDatabase.cpp`
- `Src/Sqlite/SCSqliteAdapter.cpp`

验收标准：
- `IReferenceIndexMaintainer` 至少有一个明确实现，或者文档明确说明当前版本不支持维护。
- `CheckReferenceIndex()` 不能永远返回同一个 `Missing` 结果，必须区分未建立、只读、健康、失效等状态。
- `GetAllReferencesDiagnosticOnly()` 的用途、边界和返回数据定义清晰。
- 新增测试覆盖正向引用、反向引用、删除引用目标、重建/检查路径。

## T4. 收紧计算列定义校验

目标：
- 将计算列的非法定义尽量前移到配置阶段，减少运行时才暴露错误。

文档输入：
- [DatabaseEditor Computed Column Lifecycle](DatabaseEditorComputedColumnLifecycle.md)
- [Computed Table View And Startup](ComputedTableViewAndStartup.md)
- [V1 Baseline Decisions](V1BaselineDecisions.md)
- [Phase2-P1 平台能力修复方案](Quantity/DevGuide/Phase2-P1平台能力修复方案.md)

涉及代码：
- `Src/Computed/SCTableView.cpp`
- `Src/Computed/SCComputedRuntime.cpp`
- `Tools/DatabaseEditor/SCDatabaseSession.cpp`

验收标准：
- 新增或编辑计算列时，必须校验 `kind`、`valueKind`、`ruleId`、`dependencies`、`aggregateRelation`、`aggregateField` 的一致性。
- 非法定义必须返回明确错误，而不是在 `GetCellValue()` 时才失败。
- 编辑器新增/编辑计算列界面必须同步体现这些约束。
- 新增测试覆盖表达式、规则、聚合三类计算列的合法与非法定义。

## T5. 改进计算列缓存失效策略

目标：
- 避免当前缓存因版本号切分过细，导致命中率过低。

文档输入：
- [DatabaseEditor Computed Column Lifecycle](DatabaseEditorComputedColumnLifecycle.md)
- [Computed Table View And Startup](ComputedTableViewAndStartup.md)
- [Task Breakdown](TaskBreakdown.md)

涉及代码：
- `Src/Computed/SCTableView.cpp`
- `Src/Computed/SCComputedRuntime.cpp`

验收标准：
- 缓存失效必须与依赖变化直接相关，而不是完全依赖全局版本号。
- Undo/Redo、Commit、Import、RuleWriteback 等变更源要能正确触发失效。
- 同一记录、同一计算列在无依赖变化时应能命中缓存。
- 新增测试验证缓存命中、失效和重算行为。

## T6. 补齐 SQLite 导入与恢复收尾原子性

目标：
- 让导入会话状态、业务数据提交、恢复清理形成可解释的一致性闭环。

文档输入：
- [Capability Gap Assessment](CapabilityGapAssessment.md)
- [Performance Smoke Baseline](PerformanceSmokeBaseline.md)
- [Roadmap](Roadmap.md)

涉及代码：
- `Src/Batch/SCBatchOperations.cpp`
- `Src/Sqlite/SCSqliteAdapter.cpp`

## T7 ???????? / ???????

### ???
- ?????????? active edit?
- Import finalize ????????????????????
- Import abort ?????????
- SQLite ???????? `SCImportRecoveryState` ??? finalize?
- ????????`schema_version` ?????
- ??????????????
- ???? `Commit` / `Undo` / `Redo` ???????
- ????????????????????
- ????? rebuild?? `Dirty / Healthy / OutOfDate` ??????

### ?????
- SQLite ??????? payload??? `import_sessions.payload` ????????
- SQLite ??????????????? `SaveMetadata()` ???????I/O ?????
- ??????????????????? `ruleId`?????????????
- ????????????????????????????? rebuild?
- ???????????????????????????
- ??????????????????????????????????

验收标准：
- 导入 finalize 失败时，系统状态必须可恢复，并且恢复信息可读。
- 导入会话的 checkpoint、payload、finalize、abort 行为要有明确状态迁移。
- 断点恢复后可继续加载 `SCImportRecoveryState`，且结果一致。
- 新增测试覆盖：中断后恢复、finalize 失败、abort 清理、重复 finalize。

## T7. 补充边界测试与集成测试

目标：
- 将当前“主路径可用”提升到“边界行为可验证”。

文档输入：
- [Capability Gap Assessment](CapabilityGapAssessment.md)
- [Performance Smoke Baseline](PerformanceSmokeBaseline.md)

涉及代码：
- `Tests/M1Tests.cpp`
- `Tests/M2SqliteTests.cpp`
- `Tests/M3Tests.cpp`
- `Tests/ComputedTests.cpp`
- `Tests/QueryMemoryExecutorTests.cpp`
- `Tests/QuerySqliteExecutorTests.cpp`

验收标准：
- 至少覆盖以下场景：非法输入、删除后访问、重复删除、重复提交、回滚后再次写入、恢复失败、查询不支持、计算列非法定义、缓存失效。
- 每个新修复点都至少有一个回归测试。
- 关键测试必须分别覆盖 Memory 和 SQLite 两个后端。

## T8. 完善数据库编辑器的错误反馈与计算列工作流

目标：
- 让数据库编辑器与 Storage 的实际能力一致，减少“界面可操作但底层不支持”的情况。

文档输入：
- [DatabaseEditor GUI Design](DatabaseEditorGuiDesign.md)
- [DatabaseEditor GUI Implementation Scope](DatabaseEditorGuiImplementationScope.md)
- [DatabaseEditor Computed Column Lifecycle](DatabaseEditorComputedColumnLifecycle.md)
- [Computed Table View And Startup](ComputedTableViewAndStartup.md)

涉及代码：
- `Tools/DatabaseEditor/SCDatabaseSession.cpp`
- `Tools/DatabaseEditor/SCDatabaseEditorMainWindow.cpp`
- `Tools/DatabaseEditor/SCComputedColumnDialog.cpp`
- `Tools/DatabaseEditor/SCRelationPickerDialog.cpp`

验收标准：
- 计算列新增、编辑、删除时，错误信息必须可理解，且能定位到具体字段。
- 关系候选、表切换、记录编辑、计算列重建后的 UI 状态必须一致。
- 删除或修改后应自动刷新当前视图，不出现过期的计算列显示。
- 新增或更新编辑器流程测试，验证基础操作链路可用。

## T9. 收敛文档与代码术语

目标：
- 消除“文档说的是一种能力，代码实现的是另一种能力”的歧义。
- 为“索引”“引用索引”“计算列”“回放执行”“持久化导入会话”等核心术语建立统一口径。

文档输入：
- [Roadmap](Roadmap.md)
- [Task Breakdown](TaskBreakdown.md)
- [Capability Gap Assessment](CapabilityGapAssessment.md)
- [V1 Baseline Decisions](V1BaselineDecisions.md)

涉及代码：
- `Include/*.h`
- `Docs/*.md`

验收标准：
- “索引”“引用索引”“计算列”“回放执行”“持久化导入会话”等术语在文档和代码中定义一致。
- 对暂未实现的能力必须明确标注为“不支持”或“后续扩展”，不能混用“已完成”表述。
- 文档索引与任务清单保持同步。

## T10. 增补性能基线与回归门槛

目标：
- 让当前“可运行”向“可持续集成”推进。
- 为导入、查询、恢复与排序补齐可重复的 smoke baseline 和回归门槛。

文档输入：
- [Performance Smoke Baseline](PerformanceSmokeBaseline.md)
- [Capability Gap Assessment](CapabilityGapAssessment.md)
- [Roadmap](Roadmap.md)

涉及代码：
- `Tests/PerformanceSmokeTests.cpp`
- `Tests/M2SqliteTests.cpp`
- `Tests/QueryMemoryExecutorTests.cpp`
- `Tests/QuerySqliteExecutorTests.cpp`

验收标准：
- 保留现有 smoke baseline，并补充大数据量导入、查询、恢复、排序的基准样本。
- 每次修改查询、导入、缓存或 SQLite 持久化后都能跑通基线测试。
- 性能测试结果可重复，且输出可用于趋势观察。

完成情况：
- 已完成。
- 现有基线覆盖 `SqliteBulkImportBaseline`、`SqliteQueryAndSortBaseline`、`SqliteRecoveryFinalizeBaseline`。
- 测试输出包含 elapsed time、created count、chunk count、matched rows、returned rows 等趋势字段。

## 建议执行顺序

1. `T1` 修复事务悬挂
2. `T6` 补齐导入与恢复原子性
3. `T3` 补齐引用索引维护器
4. `T4` 收紧计算列定义校验
5. `T5` 改进缓存失效策略
6. `T2` 收敛查询语义
7. `T7` 补测试
8. `T8` 完善编辑器工作流
9. `T9` 收敛术语
10. `T10` 完善性能门槛

## 交付建议

- 每个任务至少对应 1 个代码 PR 和 1 组回归测试。
- 任何影响 SQLite 持久化、Undo/Redo、导入恢复、计算列失效的改动，必须先补测试再改主逻辑。
- 若单次迭代只能做少量任务，优先级应固定为：事务一致性 > 恢复一致性 > 查询语义 > 计算列 > 编辑器体验。
