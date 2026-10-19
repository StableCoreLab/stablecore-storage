# SCStorage 能力差距实现方案

> 状态：实现设计稿
>
> 目标：为 CAD 导入 D07/M1 和后续 D07/D08 查询接入提供可评审、可分阶段实施的 SCStorage 方案。
>
> 编制日期：2026-10-19
>
> 依据：本文件所列 Storage 公开接口、SQLite 后端、Batch、Query 和既有测试；当前无须保留的历史数据库或导入会话，允许删除旧导入接口并采用新建 schema。本次仅做静态核对，未编译、未运行测试。

## 1. 方案结论

按依赖顺序分四条工作线推进：

1. **P0：磁盘分块 staging**。把完整会话 payload 和 SCImportStagingArea::chunks 的无界内存累积，改为独立的会话元数据、chunk 元数据和 chunk 负载；恢复默认只读元数据，数据由有界 reader 逐块读取。
2. **P0：原子 Finalize**。由 Batch/Core 逐块读取并应用 staging；由数据库提交入口在一个 SQLite 事务中提交正式记录、版本/Undo 信息、最终 receipt 并清理 staging。移除“先 Commit()、再清 checkpoint”的两阶段成功边界。
3. **P1：恢复诊断与扩展容量**。在 P0 已实现的状态转换、安全取消、失败重试、基础配额和 Finalize 容量门槛上，补足恢复诊断、清理策略及扩展场景的性能合同。
4. **P1：复合查询与显式启动维护**。在现有复合索引和 Query 设施上扩展字段数和稳定 cursor 契约；把只读健康检查与显式修复分开，禁止打开数据库时隐式重建索引或修复数据。

S-01 与 S-02 完成、T01–T05 及 T07 的 Open/新建边界子集通过，且 10 万/100 万及产品目标上限的**完整 Finalize** 峰值内存、耗时和临时空间落入已确认预算后，才解除 D07/M1 正式导入阻塞；候选态或 journal 超预算时须先完成有界化，不能把该问题留到 P1。阶段 B 即须移除普通 Open 的隐式升级/建表/索引物化：显式 Create 建立新 schema，已有旧格式文件返回不支持状态，不做历史导入数据迁移；阶段 F 完成 T07 的健康诊断与各类显式修复全覆盖。S-04 与 S-05 完成并满足查询/索引重建容量门槛后，再开放大规模版本查询、空间索引缓存和 Replay 隔离恢复的业务接入。

## 2. 范围与架构边界

阶段 B/P0 的 T07 放行范围包含当前格式文件的只读 Inspect 与显式文件级 Recover，以及旧格式在 Open/Upgrade/Recover 入口的无写拒绝；阶段 F 才补全面健康诊断和派生索引的对象级 Repair。

继续遵循 Core -> Adapter -> Backend：

- **Core / Batch**：定义导入状态转换、确认条件、chunk 应用顺序、错误归因和 receipt 语义；不拼 SQL。
- **Adapter / Public API**：提供 opaque session、chunk reader、Finalize receipt、查询 cursor 和检查/修复入口；不暴露 SQLite 表、SQL、rowid 或 BLOB 布局。
- **Backend / SQLite**：持久化 staging、版本/journal、receipt 和派生索引；执行原子事务、校验摘要、索引检查与重建；不决定 CAD 身份规则或 Quantity 工作流。
- **Quantity**：仅通过 Storage 公共接口提交 chunk、读取恢复信息、确认 Finalize 和执行查询；不直接访问 SQLite 或建立旁路 staging。

**跨模块代际规则**：在持久 metadata 中维护单调递增的内部 storage generation，表示会影响正式模型、schema、历史或派生索引可见性的已提交写入；它与业务 VersionId 分开，不能假定 Upgrade、Repair 或 ResetHistoryBaseline 会推进 VersionId。普通 Commit、Undo/Redo/MoveHistory、导入 Finalize、显式 Upgrade/Repair、历史基线重置以及可独立提交的 schema/派生索引写入，在各自取得写锁后的同一事务中比较打开时的 generation，并在成功提交时推进 generation；冲突时不得写回旧内存模型。成功提交的发起对象须在受锁发布步骤同步更新自身 generation 与完整可见模型，旧句柄按代际失效；已确认提交而发布失败时 fault 发起对象并返回“已提交、需重开”，提交结果无法判定时返回 OutcomeUnknown，不能成功返回一个立即自过期或半更新的对象。可独立提交的 schema 路径至少包括 CreateTable/DeleteTable、无活动编辑时的 RenameTable，以及 ISCSchema 的列增改删、约束与索引增删；不能只在 Commit 加代际检查。已有活动编辑中的 schema 修改必须保持候选态，统一在 Commit 的持久事务中落盘并推进一次 generation；Rollback 只丢弃候选态，不再依赖先独立提交、后补偿写回的路径，否则另一连接在两次提交间打开或写入会破坏完整回滚和代际一致性。任何不经这些入口直接改写同一数据库文件的行为不受 Storage 一致性合同保护。Begin/Append/Abort/attempt sidecar 等仅改变 staging 的操作不推进正式 generation，但仍用 session state 及 Storage 内部 attempt token 做事务互斥；恢复查询直接读取最新 session 元数据。公开 API 只使用 opaque 代际/预期状态，不暴露 SQLite metadata 布局。

不重做已有普通事务、Undo/Redo、约束和备份实现；为满足跨连接完整回滚，活动编辑中的 schema 独立持久化路径须收敛到既有候选编辑/Commit 边界。现有实现和测试作为底座，改动集中在 Batch、公开接口、SQLite import/index、数据库创建/Open 边界、Diagnostics 入口及对应测试。删除旧导入接口及序列化路径，不提供源码兼容桥；Quantity 同步迁移到唯一的新 handle/reader API。

## 3. 已核实的实现基线

| 主题 | 现状及代码依据 | 设计含义 |
|---|---|---|
| ImportSession 模型 | Include/SCBatch.h 中 SCImportStagingArea 持有完整 std::vector<SCImportChunk>。 | 删除完整会话 vector 类型与旧导入入口，改为轻量 session handle。 |
| staging 持久化 | SqliteImplDatabase_Init.cpp 的 import_sessions.payload TEXT 保存整会话序列化数据。 | 改为独立元数据与 chunk 负载；每次追加不得重写历史 chunk。 |
| Append/Recovery | AppendImportChunk 先向 vector 追加，再序列化完整 session；LoadImportRecoveryState 读取并反序列化完整 payload。 | Append 和默认 Recovery 都必须改为有界处理。 |
| Finalize | SCBatchOperations.cpp 从 session.chunks 逐项应用，调用 database->Commit() 后再调 database->FinalizeImportSession()；清理失败返回 SC_E_IMPORT_CLEANUP_FAILED。 | 需新的单一提交入口，不能把清理失败留在正式提交之后。 |
| 候选态可见性 | SqliteTable::CreateRecord() 立即把草稿加入内存 records_；EnumerateRecords() 可枚举这些记录，EnsureUsable() 不隔离活动编辑。 | Finalize 不能在供普通读者共享的数据库对象上直接回放 chunk；候选态和已发布视图必须隔离。 |
| 提交后异常 | CommitPersistencePhase() 在 SQLite txn.Commit() 之后仍更新版本、记录和 Undo 栈；异常也进入 FinalizeCommitFailure()，仅检查 autocommit 不足以判断提交结果。 | Import Finalize 必须以同事务 receipt 判定持久结果，并在内存发布失败时返回明确的 durable outcome。 |
| 恢复读取 | LoadImportRecoveryState() 调用 EnsureImportSessionStore()，后者在普通打开模式下可能执行 CREATE TABLE。 | 新接口读取只检查既有结构；建表只能发生在显式新建路径。 |
| 复合索引 | 已有 SCIndexDef、复合键编码、复合 Query planner/executor 和索引健康检查；kCompositeIndexMaxColumns 当前为 3，复合范围/排序、reopen、Undo/Redo 有现有测试。 | S-04 是扩展、稳定化和补足公开 cursor，不是从零实现索引。 |
| 查询结果 | 公开 ISCRecordCursor 和 IQueryExecutor 已存在；SQLite Query executor 有将匹配记录装入 vector 再生成 cursor 的路径。 | 复合查找需支持惰性/分页读取；保持现有 ISCRecordCursor 语义，避免创建第二套记录 cursor。 |
| 启动维护 | CheckQueryIndex() / RebuildQueryIndex() 是显式检查和重建入口；RunStartupIntegrityCheck() 当前未接入主启动链路，且其诊断写入函数会持久化到 startup_diagnostics。 | 健康检查结果应只读返回；打开数据库不能偷偷写诊断或重建索引。 |
| Schema | SQLite upgrade registry 当前按相邻版本登记升级步骤；已有 5→6、6→7、7→8 示例。 | 本次无历史库/旧 checkpoint 迁移；新库由显式 Create 使用新 schema，旧格式文件不由普通 Open 自动升级。未来有持久数据后的变更仍走显式 Upgrade。 |
| Open / Upgrade | SCFactory.h 说明 CreateFileDatabase 会自动升级；SCSqliteAdapter.cpp 在普通打开路径中执行升级。 | 这与本任务的“打开不隐式写入、升级显式执行”约束冲突；必须调整打开/升级调用契约并迁移调用方。 |
| 打开与关闭副作用 | SqliteImplDatabase_Init.cpp 的构造函数在缺少 metadata 时初始化 schema、在缺少 schema_version 时 SaveMetadata；析构函数对非只读对象调用 SetCleanShutdownFlag(true)。 | Open 不能触发上述补写；普通打开后仅做读取再关闭也不得因析构写入 metadata，缺失结构须报告不支持/损坏并由显式 Create 或 Repair 处理。 |
| 初始装载一致性 | SqliteImplDatabase_Init.cpp 依次 LoadMetadata、LoadTables、LoadJournalStacks；部分循环以 Step() 不再返回 SC_OK 作为结束，没有统一的初始只读快照和错误传播。 | Open 必须在同一只读快照内装载 generation、metadata、schema、records、journal 与派生状态，区分正常结束和读取失败；并发提交不能得到跨代混合模型。 |
| 正式写入口 | ISCDatabase 还有 MoveHistory；SqliteImplDatabase_Edit.cpp 的 CreateTable/DeleteTable 和无活动编辑的 RenameTable 可直接提交；SqliteImplDatabase_Index.cpp 的列、约束、索引持久化有独立事务，活动编辑的 Rollback 会用 journal 补偿部分已落盘 schema。 | storage generation 必须覆盖这些直接提交，活动编辑的 schema 修改须延至 Commit，避免跨连接写入后 Rollback 补偿覆盖新状态。 |

参考入口：Include/ISCInterfaces.h、Include/SCBatch.h、Include/ISCQuery.h、Include/SCQuery.h、Src/Batch/SCBatchOperations.cpp、Src/Sqlite/SqliteImplDatabase_Init.cpp、Src/Sqlite/SqliteImplDatabase_Index.cpp、Src/Sqlite/SCSqliteAdapter.cpp。

## 4. S-01 / S-03：分块 staging 与恢复

### 4.1 公共接口

以 SCImportSessionHandle（session id 加必要元数据）作为唯一导入会话类型，移除调用方持有全部 chunk 的 SCImportStagingArea：

- Begin 接收调用方在请求前持久保存的、长度受限的 opaque begin request id，并返回轻量 handle、base version、opaque base storage generation、数据库 file-id 以及当前 `nextSequence`；base version/generation 共同标识待提交正式快照，file-id 绑定数据库身份，`nextSequence` 是首次 Append 必须通过 `BeginChunk` 交给 source 的 sequence。同一 request id 与相同规范化 Begin 选项重复提交时，活动 session 返回原 handle；已进入终态的 session 返回绑定原 session/file-id 的 stale handle，调用方必须通过文件级 outcome 查询取得 receipt/tombstone，不分配新 id；选项不同返回冲突。request id 只作为导入请求身份，不编码 CAD 业务规则。
- Append 接收一个有大小上限的 chunk，返回**当前最新**已落盘 checkpoint 和 `alreadyApplied` 标志；成功后调用方可以释放 chunk。重复提交较早 chunk 时 checkpoint 也不倒退。
- Recovery 默认返回 SCImportRecoveryInfo：状态、session name、创建时间、base version/opaque generation、当前最新 checkpoint、chunk 数、累计字节、记录/操作计数、配额摘要、当前 attempt 是否占用及可执行动作；ReadyToFinalize 时另返回冻结 checkpoint 与有版本的整体输入摘要，不带 chunk payload；只读查询不得自动夺取租约或修改状态。
- ListImportSessions 提供只读、有界 keyset 分页的活动 session 元数据枚举，按单调 session id 稳定排序，允许按 session name/状态筛选；无并发会话变化时分页不重复、不漏项，并发新建/终止时各页反映读取时的活动状态，调用方需按 id 再用 Recovery 核实。另提供按 begin request id 的文件级只读结果查询，跨活动行与终态 outcome 返回同一 session id/状态；Begin 已提交但响应丢失时，调用方用预存 request id 精确恢复，不依赖活动列表是否仍有该行。终态按 id 或 request id 查询，不在默认活动列表中全量加载。
- OpenImportChunkReader 返回 Storage 公共 reader；打开时固定当前完整 checkpoint 作为本 reader 的读取上界，后续 Append 不延长该 reader 的范围，调用方可重新打开以读取新 checkpoint。每次最多读出一个受限 chunk，按稳定序号递增，可取消并报告 chunk 位置。Begin 时持久保存数据库 file-id/identity；handle、Recovery、reader 每次短快照都必须同时校验该 identity、generation 和 session 状态，路径或 file-id 变化返回 `PathChanged`/stale，不得因 generation 恰好相同而继续读取。每次 Next 使用新的短只读快照，在同一快照内核对 session 状态、正式 generation、冻结 checkpoint（若已 Ready）及目标 chunk 元数据/负载，读完立即释放快照，不跨 Next 固定 WAL 读事务。若会话已 Aborted/Finalized、代际过期或 chunk 在并发清理后不可见，返回明确状态并终止 reader，不继续拼接不同代次的数据；Finalize 专用 reader 还须核对 Storage 在当前 attempt 上持有的内部 token，调用方不提供也不回传该 token。
- Resume、Ready、Finalize、Abort 都是显式操作。Ready 接收调用方期望的当前 checkpoint，在短写事务中冻结它并返回 checkpoint 与有版本的整体输入摘要；若 Ready 已提交但响应丢失，同一期望 checkpoint 的重复 Ready 返回原冻结结果，不推进状态或重算输入，期望值不同返回冲突。Finalize 请求只接受 session id、确认信息、期望的 base version/opaque generation、冻结 checkpoint/输入摘要及 commit name，不接受调用方拼装的完整 staging vector，也不包含 attempt token。这样提交前校验和提交后幂等查询都能比较同一组期望输入。
- Abort 使用唯一的新接口返回 `Aborted` tombstone；首次及重复 Abort 均返回同一终态结果，完整 `SCImportRecoveryInfo` 由只读 Recovery 查询取得。

新接口在阶段 A 必须冻结为可编译、可测试的公共合同，而不是仅保留概念名称。公开头文件至少固定以下签名语义：`BeginImport(requestId, options, outHandle)`、`AppendImportChunk(handle, source, outCheckpoint)`、`OpenImportChunkReader(handle, outReader)`、`ReadyImport(handle, expectedCheckpoint, outReady)`、`FinalizeImport(request, outReceipt)`、`AbortImport(handle, outTombstone)` 和 `GetImportOutcome(query, outOutcome)`；所有 `out*` 只在成功或明确的 durable outcome 时赋值。Begin 返回的 handle 和后续所有文件级查询都绑定 Begin 时持久的 file-id/identity，路径只作为定位提示，identity 不匹配返回 `PathChanged`/OutcomeUnknown。`BeginImport` 接收调用方拥有的、固定长度的 request id 和显式选项，返回不可伪造的 session handle；chunk sequence 由 Storage 从持久 checkpoint 的 `nextSequence` 分配，调用方不得选择或修改。`AppendImportChunk` 接收只能顺序调用的 `SCImportChunkSource`（固定 `BeginChunk(nextSequence)`、`NextRequest`、`EndChunk`、`ResetChunk(nextSequence)`；source 必须可重放同一 sequence，Storage 在确认持久提交前不得要求调用方修改 sequence）；Append 失败由 Storage 返回需重放的 sequence，调用方在同一 source 上调用 Reset 后重新 Append，重复成功只返回当前最新 checkpoint。每次 request 提供有界字段片段，返回 `End` 或错误，Storage 只保留当前片段的编码缓冲；字段字符串/BLOB 也必须通过片段接口提供，禁止 source 返回无界值指针。`OpenImportChunkReader` 返回拥有明确 Close/Cancel 生命周期的 reader；所有 handle/reader/result 的所有权、并发调用限制、关闭后的行为、Next 输出缓冲生命周期、stale generation、OutcomeUnknown、资源超限、PathChanged 和已提交需重开等错误码必须在公开头文件中定义，并在编译期和行为测试中覆盖。reader/source 不允许调用方把整批 requests 预先 materialize 后再传入；若调用方需要重试，重试身份由持久 request id、file-id、chunk sequence 和规范化字节摘要确定。

**Finalize attempt token 合同（冻结）**：attempt token 是 Storage 内部生成、持久化并管理的 opaque fencing 状态，不属于调用方合同，不加入 `SCImportFinalizeRequest`，也不提供领取/回传 token 的公共 API。Ready 后首次 Finalize 由 Storage 在短写事务中分配递增 attempt 代次及其 token，并将当前租约/代次写入 session；每次 reader、sidecar、候选提交、Abort、`ReclaimImportAttempt` 和恢复扫描都在同一短事务内读取并校验该持久 token。调用方重试只重新提交原 Finalize 请求字段；Storage 先查询持久化 Finalized/Aborted outcome，再按 session 的当前 attempt 状态判断 `InProgress`、安全失效后的可重试状态或冲突。租约失效、受控撤销或失败释放后，Storage 才能分配更高代次；旧 token 的所有后续写入一律返回稳定的 stale/Conflict，不得由调用方自行续租、伪造或选择 token。Receipt/tombstone 和 candidate manifest 可保存该内部 token 供 Storage 诊断与恢复 fencing，但这些值不作为公共请求参数或调用方幂等比较字段。

文件级入口也在阶段 A 冻结：`OpenFileDatabase(path, options, outDatabase)` 只打开已有受支持文件；`InspectFileDatabase(path, outInspection)` 只读返回 schema/file-id/recovery 状态；`RecoverFileDatabase(path, expectedFileId, outResult)` 只接受显式恢复请求，并返回 `Recovered`、`RecoveryPending`、`OutcomeUnknown` 或稳定拒绝状态。三者均不接受隐式 Create/Upgrade，`Inspect` 不写入，`Recover` 只处理 manifest 校验通过的当前格式。

最小公开类型也在阶段 A 固定：`SCImportFileIdentity`（opaque file-id、identity epoch）；`SCImportBeginRequestId`、`SCImportBeginOptions`、`SCImportResourceLimits`；`SCImportSessionHandle`（session id、file identity、base version/generation、next sequence）；`SCImportRequestFragment`（有界字段/字符串/BLOB 分片、fragment kind、字节数和 End 标志）与 `SCImportChunkView`（只读 chunk 字节视图、sequence、长度和 checksum）；`SCImportChunkSource`、`SCImportChunkReader`；`SCImportChunkCheckpoint`、`SCImportRecoveryInfo`、`SCImportReadyResult`、`SCImportFinalizeRequest`、`SCImportReceipt`、`SCImportTombstone` 和 `SCImportOutcomeQuery`（file identity 加二选一的 session id 或 request id，禁止同时为空）；`SCFileInspection`、`SCFileRecoveryResult`；`SCImportOutcome`（state、receipt/tombstone、committed generation、reason）；`SCImportError` 使用第 85 行冻结的完整集合。公开方法仍返回仓库现有 `ErrorCode`；阶段 A 同时提供唯一的 `SCImportError -> ErrorCode` 映射，禁止 Backend/Adapter 各自定义转换。`GetImportOutcome` 只接受 `SCImportOutcomeQuery`，不再使用未定义的“sessionId/requestId”二选一伪签名。

删除 `SCImportStagingArea`、`SCImportRecoveryState` 的完整 payload 路径、旧 `BeginImportSession`/`AppendImportChunk`/`FinalizeImportSession`/`AbortImportSession` 签名、旧 vector 版 `ExecuteImport`、导入专用 `SCImportExecutionOptions`/`SCImportSessionOptions` 中的 `rollbackOnError` 与 `persistCheckpoints` 选项及对应序列化器；普通 `ExecuteBatchEdit` 与其 `SCBatchExecutionOptions` 不在删除范围内。新导入只接受逐块提供的请求源；Append 失败保留已提交 checkpoint，是否 Abort 由调用方显式决定。Quantity 直接迁移到 handle/reader/Ready/Finalize API；未迁移的调用方应在编译阶段暴露错误，不设兼容桥或隐式自动确认。

接口补充合同：Begin 的 handle 必须携带或可查询 file-id 与当前 `nextSequence`；Storage 在调用 source 前通过 `BeginChunk(nextSequence)` 传入序号，source 不自行分配序号。session metadata、terminal outcome、recovery manifest 和 candidate manifest 均持久保存同一 file-id/identity；file-id 不匹配时，活动/终态查询、Recovery 和 reader 均返回 `PathChanged` 或 `OutcomeUnknown`，不能从替换后的路径接受结果。

阶段 A 的头文件草案必须同时冻结可编译的最小 C++ 形状，不能只冻结概念名称。公共错误集合固定为 `None`、`NotFound`、`AlreadyExists`、`ReadOnly`、`UnsupportedSchema`、`NeedsRecovery`、`PathChanged`、`StaleGeneration`、`StaleHandle`、`OutcomeUnknown`、`InProgress`、`InvalidState`、`Conflict`、`ResourceExceeded`、`Cancelled`、`DiskFull`、`Corrupt` 和 `CommittedNeedsReopen`；SQLite/OS 原始码只进入诊断详情。`SCImportChunkSource` 固定提供 `BeginChunk(sequence)`、`NextRequest(outFragment)`、`EndChunk()`、`ResetChunk(sequence)`，每次只返回一个有界 request fragment；`SCImportChunkReader` 固定提供 `Next(outChunk)`、`Cancel()`、`Close() noexcept`。source、reader 禁止并发调用和回调重入 Storage；fragment 只在下一次 source 调用前有效，chunk view 只在下一次 reader 调用或 `Close` 前有效，调用方不得保存指针。公开入口固定为：`BeginImport(requestId, options, outHandle)`、`AppendImportChunk(handle, source, outCheckpoint)`、`ReadyImport(handle, expectedCheckpoint, outReady)`、`FinalizeImport(request, outReceipt)`、`AbortImport(handle, outTombstone)`、`GetImportOutcome(query, outOutcome)`；所有 out 参数只在成功或明确 durable outcome 时赋值。上述形状、所有权、关闭后行为和错误枚举必须直接写入公开头文件并由编译期测试覆盖。

上段原有“至少包含”表述在本方案中收敛为上述完整集合，不允许实现阶段再新增跨平台公共错误名。`SCImportRequestFragment` 的单片最大字节数、字段编码和 `SCImportChunkView` 的 checksum 表示在 `SCImportResourceLimits` 中固定；fragment 只在下一次 source 回调前有效，chunk view 只在下一次 reader 调用或 `Close` 前有效。所有 source/reader 错误先转换为 `SCImportError`，再通过唯一映射返回 `ErrorCode`。

Recovery 相关公开入口也在阶段 A 一并冻结：`ResumeImport(handle, outRecoveryInfo)`、`ReclaimImportAttempt(handle, expectedAttemptState, outRecoveryInfo)`、`GetImportRecovery(query, outRecoveryInfo)` 和 `ListImportSessions(filter, pageToken, outPage)`。`ResumeImport` 永远只读，只返回当前活动 session 的可继续状态或明确的 `InProgress`/冲突/终态错误，不夺取或修改 attempt；租约到期或受控撤销后的 token 接管只能由显式 `ReclaimImportAttempt` 在短写事务中执行，并以 attempt state/token fencing 保证幂等。`GetImportRecovery` 只读查询活动 session 或终态摘要，`ListImportSessions` 使用不透明 keyset page token，均不加载 chunk payload、不建表、不改变状态。四者的 out 对象所有权、关闭行为和 `SCImportError -> ErrorCode` 映射与前述入口相同；调用方可用 `GetImportOutcome` 查询最终 receipt/tombstone，但不能用它替代活动 session 的 Recovery。

后文所有 token 接管、租约回收和代次推进均由显式 `ReclaimImportAttempt` 执行；`ResumeImport` 本身不得写 session、attempt、token 或 checkpoint。

候选编辑的内部数据流固定为 `SCImportCandidateEdit`（仅 Core/Adapter/Backend 可见）：它持有候选表/记录/字段值、候选 schema/约束变更、候选索引状态、session/chunk/operation 作用域内的临时记录键和来源 sidecar 句柄，不持有 SQLite 句柄，也不作为公共 API 类型。临时记录键只在 Append/候选态内部有效；正式 `RecordId` 仅在 Finalize 的最终 SQLite 写事务中分配。`ApplyBatchRequests` 只写入该候选对象；`CommitImportSession(SCImportCandidateEdit&, finalizeRequest, receipt)` 在同一 Storage 写事务中读取候选对象并调用既有 `CommitPersistencePhase` 的校验、记录/journal/Undo 持久化逻辑。事务提交前候选对象仍不可被普通 Query 观察，提交失败销毁候选对象并保留可恢复 staging；提交成功后按 receipt 规则一次性发布完整模型。

### 4.2 SQLite 私有持久化结构

实现细节留在 SQLite Backend，不作为公开协议：

- session metadata：session id/name、唯一 begin request id、规范化的调用方显式 Begin 选项、创建时间、state、base version、base storage generation、实际生效的资源上限、当前 checkpoint、chunk/记录/字节总计、有版本的增量输入摘要、Ready 时冻结的 checkpoint/输入摘要、序列化版本和诊断摘要。重复 Begin 只比较原始请求经稳定规则规范化后的显式选项，不因部署后的默认配额变化而把同一请求误判为冲突；原 session 继续使用创建时持久冻结的有效配额。
- chunk metadata：session id、单调递增 sequence、payload byte length、record/operation count、checksum 算法/版本、checksum、完成状态。
- chunk payload：每个 chunk 独立存放，按 sequence 可定位读取；staging 与正式 records / field_values 查询空间隔离。
- session metadata 与 terminal outcome 必须同时保存数据库 file-id/identity 和下一个 sequence；归档后按 request id/session id 查询仍先校验该 identity，路径替换或 identity 不一致时不得返回另一数据库的 receipt。
- terminal outcome 和 `candidate_cleanup_pending` 还必须保存最终 attempt token；Finalize/Abort 删除活动 session 后，Storage scanner 仅在 outcome/pending token 与 candidate manifest 的 session/attempt/token/file-id 全部匹配时执行清理，旧 token 一律拒绝。该 token 仅供内部 fencing 和恢复使用。
- 导入操作来源 sidecar：固定为同一 Storage SQLite 数据库内的独立表（不使用外部索引文件），以磁盘索引保存临时记录键到 chunk/table/operation/field 位置的映射，Finalize 时再在同一事务内补写对应正式 `RecordId`，避免 SCBatchOperations.cpp 的 operationMappings vector 随总操作数增长；每行必须绑定 session id、attempt token、storage generation 和 chunk sequence，并明确区分 `temporaryRecordKey` 与 Finalize 后的 `recordId`。sidecar 是内部 attempt 的恢复状态，不是正式数据：应用 chunk 时的预写入使用同一数据库内独立短事务并做 token fencing；提交前失败、进程中断或 OutcomeUnknown 时保留并由 Recovery 按内部 token 归属；新 attempt 只能创建新 token 的 sidecar，不能读取或覆盖旧 token；确认未提交后由显式 `ReclaimImportAttempt`/Abort 在清理事务中回收，确认 Finalized/Aborted 后与正式记录、receipt 一起在最终 Finalize/Abort 事务中删除。预写入、最终删除和 receipt 的事务边界不同但载体相同；清理失败不得伪造成功，需返回可重试/OutcomeUnknown 并保留可定位的清理状态。
- terminal outcome：Backend 使用同一 import_outcomes 表，以 session id 为唯一键、kind 为 Finalized/Aborted；Finalize 的行含唯一 commit/version receipt，Abort 的行含 tombstone。两种终态都复制只读恢复所需的 session id、唯一 begin request id/规范化的调用方显式 Begin 选项、持久 session_name、base version/generation、最后完整 chunk id、chunk 数、累计字节数、记录/操作数、预算摘要、输入摘要与完成时间，Abort 另含原因；不保留 chunk payload。持久 session_name 也用于清理活动行后规范化空 commit name 的重试。唯一 request id 在活动与终态间由持久身份映射统一约束，不能只靠两张表各自的 unique index；同一事务内先校验无其他 outcome，再插入终态并删除活动行和 chunk，数据库唯一键排除两种终态并存；不自动删除终态。session id 由新 schema 中持久的单调分配器产生，清理活动 session 和归档 outcome 后也不得复用身份；显式归档仍须保留新终态按 id/request id 查询和重复 Abort 的结果。

建议 chunk payload 使用 BLOB；摘要针对规范化序列化字节计算。摘要算法及编码格式需带版本，避免跨版本误判。每个 chunk 限制字节数，因此序列化缓冲和单块读取均有上限。

P0 schema 不创建 `candidate_cleanup_pending` 列或 candidate manifest 表/文件；第 97-99 行所述字段仅在未来引入磁盘候选文件时通过版本化扩展启用。P0 的 Finalize/Abort 只在同一 SQLite 事务内清理内存候选对应的 sidecar，并以 receipt/tombstone 判定结果，不启动外部 candidate scanner。

**RecordId 预留合同（P0，已废止）**：本段旧规则不再适用。Append 不预留正式 `RecordId`，也不写入临时键到正式 ID 的持久映射；以第 113 行的 Finalize 内分配合同为唯一实现规则。

**RecordId 方案修订（P0）**：Append 阶段不预留正式 RecordId，也不写“候选临时键到正式 ID”的持久映射；流式编码只为新增操作生成 session/chunk/operation 作用域内的不可伪造临时键，所有候选引用、外键和 sidecar 来源位置使用该临时键。Finalize 的最终 SQLite 写事务取得写锁后，按稳定的临时键顺序从持久 RecordId 分配器一次性分配正式 ID，并在同一事务内重写 records、field_values、引用/外键、journal、Undo/Redo、来源映射和完整模型。事务失败不消耗正式 ID；提交结果未知时保留 attempt 和临时映射，待 receipt/outcome 判定后再清理。正式 RecordId 不在 Append、Abort 或失败重试之间预留或复用，避免在尚未知道新增记录数和约束结果时猜测区间。

本段取代前文“Append 在 token fencing 短事务中预留 RecordId 区间”的旧表述；P0 以本段的 Finalize 内分配规则为唯一实现合同。

### 4.3 Append 与 checkpoint 原子性

现有实现的 `BeginImportSession` 在同一写事务中校验并占用唯一 begin request id、读取当前业务版本/storage generation，并创建 session 元数据和初始 checkpoint；事务提交后才返回 session id，避免当前“先插空行、再补 payload”失败时留下不可恢复行。重复 request id 先按持久身份映射读取原活动或终态，再比较规范化选项；并发 Begin 只有一个可成功创建，同键重试不因当前正式代际已变化而另建会话。阶段 B 的新 `BeginImport` 替换该入口。

单次 Append 的事务顺序：source 消费和规范化编码必须在 SQLite 写事务之外执行。Storage 先以有界窗口调用 source，完成 `BeginChunk/NextRequest/EndChunk`、摘要和配额检查；source 回调不得在持有 SQLite 锁时执行，也不得重入 Storage。编码完成后才 `BEGIN IMMEDIATE`，在同一短事务内重新校验 session/state/base generation/sequence，再插入已编码的有界 payload；校验失败只丢弃临时窗口，调用方按 `ResetChunk(sequence)` 重放。写锁等待、source 取消和编码失败均有独立错误语义。

1. 在取得写锁后的短事务内校验 session 存在、状态允许追加、调用方 base version/generation 与 session 一致，且当前正式 generation 仍等于 session base generation；若正式代际已变化，保留 checkpoint 并返回过期会话错误，由调用方显式 Abort/新建 session。source 消费阶段不得持有该写锁。
2. 对单个 chunk 使用有上限的增量编码/摘要计算，编码时即检查字节、记录和操作数，超过单块预算立即停止并返回稳定错误，不先构造无界序列化缓冲；按 sequence 查询已有 chunk，先校验已存负载 checksum，再用有界 reader 对规范化字节逐字节比对，完全相同才作为幂等重试，摘要相同但字节不同仍返回冲突，已存负载损坏则返回损坏错误。仅对真正新增的 chunk 检查下一个序号和 session 总配额，不能让“必须为下一个序号”的检查先拒绝合法重试。不复制、反序列化或重写历史 chunk。
3. 同一写事务内插入 chunk payload/metadata，并更新 session 当前 checkpoint、计数、字节预算、输入摘要链和状态；摘要链按规范化 chunk 摘要、序号及长度顺序扩展，重复 Append 不改变它。Ready 只冻结已持久化摘要，Finalize 按 reader 顺序独立重算并校验。
4. 新 chunk 的事务提交后，或逐字节相同的重试已确认旧 chunk 持久存在后，才把**当前最新** checkpoint 返回/同步给调用方；对重复旧 chunk 不回写该 chunk 当时的历史 checkpoint。

Append 的幂等窗口只覆盖仍处于 Staging/Checkpointed 且正式 generation 未变化的活动 session：在这些条件满足后，已有 sequence 与规范化 payload 字节完全相同的重试返回**当前最新** checkpoint 并标记 `alreadyApplied`，sequence 相同但内容不同返回冲突；无论重试的是最后一块还是更早的块，返回值都不能使调用方 checkpoint 倒退。ReadyToFinalize/Failed/Aborted/Finalized 或正式 generation 已变化时先返回对应状态/过期错误，即使迟到请求的 sequence 与历史 chunk 相同也不重新开放 Append；调用方通过 Recovery/终态查询获取已确认 checkpoint 或 receipt。任意编码、空间不足、写入或校验失败时事务回滚，checkpoint 不前移。崩溃恢复只承认完整事务提交的 chunk；payload 摘要错误报告明确 chunk id/sequence。

现有实现的 `AbortImportSession` 取得写锁后在一个事务中使当前 Finalize attempt token 失效、写入唯一 Aborted tombstone，并删除 session、chunk 和来源 sidecar；重复 Abort 读取并返回同一终态结果，针对不存在的 session 返回 NotFound，针对已有 Finalized receipt 返回状态冲突。提交前任一步失败整体回滚，原有 session/checkpoint 可继续查询；一旦尝试 SQLite COMMIT 而结果或后续返回不确定，须用独立只读连接按 session id 查询 tombstone/活动行，查到 tombstone 报告已 Aborted，仅在证明原事务回滚且活动 session 仍存在时报告未提交，其余返回 OutcomeUnknown，不得把已提交的 Abort 当成失败。新 Recovery 接口只读查询活动 session 与 import_outcomes，能区分上述终态与未知 session；常规记录枚举和 Query 不读取 staging 表。与 Finalize 竞争时，以取得写锁并校验 session state、attempt token 和唯一 outcome 的事务为准，不能同时得到 Aborted 和 Finalized。阶段 B 的新 `AbortImport` 替换该入口。

### 4.4 状态与失败归因

**Failed 触发入口（冻结）**：`Failed` 没有额外的调用方公共 API，且只有 `ReadyToFinalize -> Failed` 这一条进入路径。只有 `FinalizeImport` 在候选回滚完成后，才可调用 Backend 私有的 `MarkImportFailed(session, attemptToken, diagnostic)` 显式状态事务；该事务重新核对 session、attempt token、Ready 状态及尚无终态 outcome，条件仍成立才保存诊断并标记 Failed。`Staging`/`Checkpointed` 的损坏、取消或放弃保持原状态，由调用方调用公共 `AbortImport` 清理；普通读取、Recovery、reader 和健康检查不得触发 Failed。状态转换自身失败时保持原状态并返回原始错误与转换错误，不能把转换失败伪装成 Failed。

任一活动状态均可显式 Abort；不可恢复的 Finalize 语义错误在候选回滚后才可通过上述内部事务转 Failed。主流程为 `Staging -> Checkpointed -> ReadyToFinalize -> Finalized`；零 chunk 会话可由 `Staging` 直接显式确认到 `ReadyToFinalize`。`Staging`、`Checkpointed` 和 `ReadyToFinalize` 均可转到 `Aborted`，只有 `ReadyToFinalize` 可转到 `Failed`。`Failed` 不可 Resume 或 Finalize，但调用方仍可显式 Abort 以清理并转为 `Aborted`；`Finalized` 和 `Aborted` 的终态分别由 receipt 和 tombstone 查询。恢复读取、chunk 校验和后台健康检查不得隐式改变状态或提交正式数据。

- Begin 创建 Staging；成功 Append 后为 Checkpointed。
- Resume 在 Staging/Checkpointed 只读取原 checkpoint，并在新的只读快照中核对当前正式 generation 与 session base generation；一致才报告可继续追加，过期则返回冲突且不修改 checkpoint，不新建会话、不改正式数据。在 ReadyToFinalize 保持冻结输入不变，无活动 attempt 时只读回传 Ready 信息，由后续 Finalize 领取内部 token。已有 attempt 仅由 `ReclaimImportAttempt` 在租约安全失效或显式受控撤销时以短写事务领取更高内部 token，旧 attempt 后续 sidecar 写入与最终提交均因持久 token 不匹配而拒绝；活跃租约未经受控撤销时返回 InProgress。调用方不持有或回传 token。Failed/Finalized/Aborted 不可 Resume，终态改走只读结果查询。
- 调用方显式确认输入完整时，在短写事务中核对当前正式 generation 仍等于 session base generation、调用方期望 checkpoint 等于当前持久 checkpoint，冻结最后 chunk id、chunk 数、累计字节/操作计数与有版本的增量输入摘要，再转 ReadyToFinalize；已 Ready 且期望 checkpoint 相同的重试只读回传冻结结果，不修改数据库，期望不同返回冲突。Ready 成功提交却未返回时，Recovery 同样提供冻结 checkpoint/摘要供 Finalize 使用；若已转 Finalized/Aborted，则按终态查询，不重新 Ready。Ready 后 Append 一律返回非法状态，要更改输入须显式 Abort 并新建 session。Finalize 在原子写事务内重新验证 base version/generation、Ready 状态、冻结摘要和确认信息；零 chunk 使用规范化空输入摘要。
- 格式损坏、摘要不符等错误在 reader/健康检查中只返回诊断，不写 Failed。显式 Finalize 尝试发现不可继续错误时，先回滚候选态，再调用上述内部失败状态转换；若并发 Abort/Finalize 已形成终态，则只读查询并返回该终态，不覆盖结果。Failed 仅允许查询诊断或显式 Abort 清理，不可恢复重放；短暂 I/O 或取消保留可重试 checkpoint。
- 失败位置包括 chunk、table request、operation、field assignment；延迟约束错误用约束信息中的 record id 查磁盘来源映射。没有精确 record id 的全局错误需返回约束和表级位置，不能伪造精确 operation。
- Finalized 通过持久 receipt 查询；不要求已清理的 staging 行继续存在。

## 5. S-02：原子 Finalize 与 receipt

### 5.1 调用流

**P0 候选态边界（冻结）**：本文“私有候选数据库/编辑上下文”中的“数据库”仅指同一 `SqliteDatabase` 对象内的私有内存候选编辑上下文，不是第二个 SQLite 文件或独立数据库。候选记录、schema、索引和 journal 在 Finalize 提交前不得写入正式 SQLite 表；sidecar 只保存来源映射和 attempt fencing 元数据。候选上下文关闭后，唯一的 `CommitImportSession` 写事务在主 SQLite 写连接上重新校验候选编辑，把候选变更、journal/Undo、版本、storage generation、receipt 以及 staging/sidecar 清理一次性写入并提交。事务失败时候选不发布且 staging 保留；提交后的内存发布按 receipt 规则处理。P0 不支持跨文件候选库复制、合并或把外部 candidate manifest 当作正式数据原子性的一部分；未来若引入磁盘候选文件，必须另立版本化协议。

因此，后文关于 Finalize candidate manifest、外部候选文件和 receipt-first 跨载体清理的段落只适用于未来扩展，不属于 P0 实现；P0 不创建该类文件，`candidate_cleanup_pending` 仅在未来扩展启用磁盘候选文件时才出现。

Storage 为每次 Finalize 创建私有候选数据库/编辑上下文，Batch/Core 仅通过该上下文的公共 Table、Record、Edit 接口复用 ApplyBatchRequests。领取 attempt 时先在发起导入的公开数据库对象上确认无活动普通编辑并设置本地写入屏障；直到候选回滚或提交后模型发布完成，BeginEdit、Undo/Redo、Upgrade、Repair、历史基线重置及其他正式写入口返回稳定的写冲突，普通读取仍见旧代际，不能让未提交普通编辑被模型切换覆盖。该候选上下文及其中的 record handle、引用/查询索引候选态不向普通查询或原有公开数据库对象发布；公开对象及其已分发的记录句柄在提交前继续呈现旧代际的一致快照。不能直接在供普通读者共享的 SqliteDatabase::records_ 上回放 chunk，因为现有 CreateRecord() 会立即加入内存表，而 EnumerateRecords() 可读到它。跨连接读者在最终 SQLite 提交前仍只读旧持久代际。每个已打开数据库对象绑定明确的 storage generation 和业务版本；提交后发起导入的公开对象在读写锁下切换到新代，原对象先前发出的 Table/Record/Cursor/Schema 句柄标记过期，返回 ErrorCode 的数据读写/Next 方法报稳定 stale-handle 错误；GetId()、IsDeleted()、GetLastModifiedVersion() 等 noexcept 身份/状态访问器只返回句柄缓存值，不承诺是当前数据库值。其他独立打开的对象在每次公开读取时先开启**新的** SQLite 只读快照，在同一快照内核对持久 generation 与对象 generation，再读取任何 SQLite 索引；若代际不同即返回稳定的 stale-database 错误，须显式重新打开/刷新，不能用旧内存记录混读新 SQLite 索引。cursor 在每次 Next 的新只读快照中先核对 generation 再推进有界扫描；变化时立即报过期并终止，不能继续返回旧 Record 句柄或跨代际拼接结果，旧 cursor 不得把公开连接固定在旧读快照上。纯内存 Record/Table/Schema 读取也先做新的只读代际核对；现有无错误返回值的 GetCurrentVersion()、GetSchemaVersion() 及编辑会话身份/状态访问器只表示对象缓存状态，新增只读代际状态入口供调用方识别过期对象。

1. 预检 baseVersion/base generation，确认发起导入的公开对象无活动编辑并设置写入屏障；在短写事务中按 session id 原子领取由 Storage 生成的私有 Finalize attempt 代次/token，清理旧失败代次的来源映射。领取失败须撤销本地屏障；活跃租约未失效时第二个 Finalize 返回 InProgress；进程中断后由显式 `ReclaimImportAttempt` 通过租约到期或受控撤销领取更高内部代次，旧 token 的任何后续 sidecar 写入和最终提交均被拒绝。随后打开绑定该内部 token 的 chunk reader，按序只读取一块。
2. 复用现有 ApplyBatchRequests 与正式表接口应用该块；完成后释放块 buffer。operation origin 通过通用 Storage 接口按 chunk 批量写入本次 attempt 的 session sidecar，而不是在 Batch 层保留无界 operationMappings vector；每批映射写入、内部 token 校验和 chunk 边界租约续期必须在同一短写事务中完成，避免先校验后被 Abort/新代次撤销。sidecar 需包含 `temporaryRecordKey`、chunk/table/operation 和被赋值字段位置；Finalize 事务内再补写正式 `recordId`，以便候选态约束校验失败时有界查回来源。Rollback 后保留本次映射用于诊断/恢复，成功 Finalize 或 Abort 时随 staging 一起删除；进程在该短事务后、正式提交前中断时，Recovery 必须按 session/内部 attempt token 识别并决定保留、回收或标记 OutcomeUnknown，禁止新 attempt 复用旧映射。
3. 继续到冻结 checkpoint 末尾并验证 chunk 序号、总数与整体摘要；读取、应用、约束校验或提交前任一步失败，先 Rollback() 私有候选编辑，正式数据和公开视图不变，staging 保留以供显式恢复或 Abort。可重试失败在短事务中释放本次内部 token 并保留诊断映射，下一次 Finalize 可领取新代次；不可重试失败在显式 Failed 转换中同时释放 token。释放操作失败则保持租约，由显式 `ReclaimImportAttempt` 在安全过期后用更高代次回收，不能假称可立即重试。确认候选上下文关闭后解除发起导入对象的本地写入屏障；若无法证明候选已关闭则使该对象 faulted，不允许普通写入。Rollback 无法证明完成时只 fault 私有候选上下文并返回明确恢复状态，不能把候选数据发布出去。
4. 调用新增的数据库原子提交入口（概念上为 CommitImportSession(edit, finalizeRequest, receipt)），不再先调用普通 Commit() 再单独清理 staging。该入口复用普通 Commit 的候选态校验和持久化代码，通过可选 Finalize 附加动作扩展同一 CommitPersistencePhase，不复制另一套记录/journal 提交实现。提交入口的所有失败路径均有候选回滚或持久 receipt 判定，不能遗留可由公开读操作观察到的半成品。

`CommitImportSession` 是 Core/Batch 到 Adapter/Backend 的内部数据库语义桥接，不是调用方可直接调用的导入公共 API；它只由 `FinalizeImport` 在已领取 attempt 后调用，参数中的私有 edit 上下文和 receipt 输出不得暴露 SQLite 句柄或候选表。普通 batch edit 继续走既有 `Commit()`；公开头文件只冻结第 73/85 行列出的导入入口。

### 5.2 单一持久化事务

SQLite Backend 在一个写事务中执行：

1. 在应用 chunk 前对 baseVersion 和 base storage generation 做预检以尽早报告冲突；最终事务使用现有 BEGIN IMMEDIATE 写事务语义取得写锁后，再校验活动 session 的 ReadyToFinalize 状态、checkpoint/input 摘要、本次 attempt token、无终态 outcome，以及当前业务版本与 storage generation 均等于 session 的 base 值。任一不符则回滚私有候选编辑并保持已提交的 session/outcome 状态。普通 Commit、Undo/Redo/MoveHistory、独立 schema 写入、Upgrade/Repair/ResetHistoryBaseline 等正式写提交也必须在自身取得写锁后核对持久 generation 与其已打开 generation 一致，并在同事务中推进 generation；旧对象在代际变化后普通数据读写均返回稳定过期状态，直到显式重新打开/刷新。已有活动编辑的 Rollback/close 作为恢复例外仍可执行，但只丢弃未持久化的候选态，不能以 journal 补偿向新持久代际写回旧 records、schema、metadata 或索引；为此活动编辑中的 schema 写入须先改为 Commit 时持久化。receipt/代际状态诊断也必须可只读访问。
2. 使用普通 Commit() 相同的约束校验、记录持久化、journal、Undo/Redo 和版本号写入逻辑。
3. 若活动版本指针属于同一 Storage 数据库，将其变更纳入同一事务和 journal。若 Quantity 的 ActiveVersionKey 位于事务外部系统，则 Storage 单独无法提供跨系统原子性，见风险项 R-01。
4. 在同一事务推进 storage generation，写入唯一 receipt（session id、commit id、committed version、base version/generation、committed generation、完成时间/摘要），并删除 session、chunk 和临时来源映射。
5. SQLite 提交成功并确认 receipt 后，使用新的只读快照在**同一快照内**先核对持久 storage generation 等于 receipt 的 committed generation，再装载/重绑定完整公开模型；不能沿用提交前快照，也不能分别查询 generation 与模型。若已由其他连接推进到更高 generation、无法取得匹配快照或重绑定失败，发起导入的公开对象进入 faulted，返回带原 receipt 的“已持久提交、需重新打开”结果，不发布不匹配模型。匹配时才在受锁的发布步骤中一次切换**完整内存代际**：表/记录映射、schema、下一个 RecordId 分配器、业务版本与 storage generation、journal/Undo/Redo 栈、查询/引用/外键缓存和句柄 generation 一起切换；候选表/记录对象若仍绑定私有 SQLite 连接，不能只移植指针或只改版本号。发布后释放重载快照，后续代际检查必须取得新快照；旧句柄失效后解除本地写入屏障并通知 observer，不能看到半更新对象、复用已用 RecordId 或再写入旧记录。发布完成后若其他连接又推进 generation，后续普通读写按新的只读代际核对返回过期状态。其他独立打开的对象在下一次读写前检测持久 generation 变化并返回过期状态；进行中的读取按上一段的快照/终止规则处理。

现有 CommitPersistencePhase() 在 SQLite txn.Commit() 成功后仍执行可能抛异常的内存更新，故 Import 提交点前后必须分别处理：提交前错误完整回滚并复用候选校验/持久化逻辑；一旦尝试 SQLite COMMIT，若返回值或后续内存发布异常使结果不确定，就用独立只读连接按 session id 查询同事务 receipt。查到匹配 receipt 表示正式记录、版本和 staging 清理均已提交；查不到且能证明事务回滚才返回未提交失败；无法查询或证明时返回独立的 OutcomeUnknown 状态并 fault 当前对象，允许后续按 session id 只读查询结果。`sqlite3_get_autocommit()` 只能证明事务已结束，不能独自判定提交或回滚；现有 FinalizeCommitFailure() 的 autocommit 判断不可直接作为 Import 成败依据。

零 chunk 或零记录的已确认导入必须有明确合同：本方案允许 Finalize，仍在单一事务中生成唯一版本/commit receipt 并清理 staging；Import 提交入口不能走普通 Commit() 在空 activeJournal_ 时直接返回的短路分支。

receipt 对 session id 唯一，包含最终 attempt token、确认的 base version/generation、**规范化后的有效 commit name**、完整 checkpoint/输入摘要及终态恢复摘要。有效名称沿用现有 Batch 规则：调用方名称非空时原样使用，空字符串时使用持久 session_name；session_name 在 Begin 时为空则固定为 `Import`，名称不做额外 trim 或大小写折叠。首次 Finalize 在领取 attempt 前以持久 session metadata 计算并固定有效名称，journal 和 receipt 均使用同一值。Finalize 重试先查 receipt：同一 session 且摘要、base version/generation、有效 commit name 一致时返回原 receipt；attempt token 仅由 Storage 用于核对持久 outcome、候选清理和旧 attempt fencing，不由调用方提供或比较。显式传入与 session_name 相同的名称和传入空名称是同一请求。若当前公开对象尚未发布 committed generation，则返回带同一 receipt 的“已提交、需重新打开”结果，不能把旧对象当作成功可用。参数不一致时返回冲突；没有 receipt 时须先排除 OutcomeUnknown 和进行中的旧 attempt，才能继续执行。为故障对象提供独立于该对象 EnsureUsable() 的文件级只读 outcome 查询公共入口，调用方可用 session id 在重新打开前判定结果，Backend 内部完成 SQL 访问。Undo/Redo 只移动已提交历史，不读取 staging、不重新读外部文件；receipt 是提交审计/幂等记录，不随 Undo/Redo 删除。

receipt 的持久字段还必须包含最终 attempt token，供 Storage 内部 scanner 和旧候选 fencing 使用；缺少该 token 的 receipt 只能报告为损坏/OutcomeUnknown，不能由调用方据此自行重试或构造新请求。

Receipt 字段清单固定包含：session id、file-id、attempt token、commit id/version、base/committed generation、有效 commit name、完整 checkpoint/输入摘要和终态完成时间。

清理发生在正式数据提交之前的同一 SQLite 事务中，因此清理 SQL 失败时整个 Finalize 回滚，旧项目版本、活动指针、Undo 栈和 staging 都保留。进程在 SQLite commit 后、API 返回前退出时，调用方通过 receipt 查询得到已提交结果；内存发布失败或提交结果无法立即确定时必须返回上述明确的持久状态，不能收到“普通失败但数据已生效”的模糊结果。

## 6. S-04：复合身份与查询索引

### 6.1 先沿用、再扩展

复用现有 SCConstraintDef、SCIndexDef、QueryPlan、IQueryIndexProvider、IQueryIndexMaintainer 和 ISCRecordCursor。当前已有复合索引维护、查询计划可观测性、requireIndex 和多个 reopen/Undo/Redo 测试，不重做这些模块。

补齐的重点：

- 与 Quantity 确认完整来源身份字段集合、字段类型、大小写/排序规则、缺省值及 null 语义；Storage 不硬编码 CAD 字段名或业务含义。
- 解除/参数化 kCompositeIndexMaxColumns == 3 和固定 key_prefix_1..3 表示；支持确认后的复合键宽度及等值前缀 + 范围尾列。编码格式带版本，索引重建可从 schema 和 records 重新生成。
- 对身份必须唯一的组合声明复合 unique constraint；普通查询优化组合声明 index。建约束遇到既有重复项时返回冲突诊断，不静默删改 records。
- 使用现有 ISCRecordCursor 暴露稳定排序和分页结果，并提供类型明确的公开输出参数；SQLite 索引扫描按有界批次推进，以完整排序键加 RecordId 的确定性续位点在每次新快照中继续扫描，不依赖跨 Next 持有旧读事务，也不先收集全量 record id 或 SCRecordPtr。正式 generation 一旦变化，Next 报过期并终止；requireIndex=true 且没有有效索引时返回 SC_E_INDEX_NOT_FOUND 或更细的稳定错误和诊断，不静默退化为全表扫描。
- 固定 NULL 排序、null 等值匹配、缺省值、排序 tie-breaker 和分页边界语义；让 Query planner、执行器和索引编码使用同一规则。
- 复合查询索引、引用索引及空间索引缓存都属于可派生状态：记录变更时增量维护，或在同一提交中将缓存标记为待重建；其一致性状态与正式 records/版本一同持久化。只读检查分别报告各索引类型的缺失、过期或损坏；显式重建失败完整回滚且不删除源数据。索引重建和恢复只能从当前正式数据/候选版本生成派生项，不得依据过期缓存重新引入已删除记录。
- Replay 隔离恢复在独立候选版本/编辑上下文中重放并维护候选索引；重放期间普通查询仍只看当前活动版本，取消、失败或 Abort 不改变活动指针和活动索引。只有显式 Finalize 才能发布候选版本、活动指针、receipt 及对应索引状态；若活动指针位于外部事务系统，沿用 R-01 的跨系统恢复协议约束。

复合身份列数量和最大编码字节必须由业务字段清单和目标数据确定。不要仅将常量从 3 改成 4；编码器、planner、排序/范围边界、持久化布局和校验器需同步支持。当前无旧库数据，索引新格式由显式 Create 直接建立；未来若有持久数据，再单独设计迁移。

## 7. S-05：只读启动检查与显式修复

### 7.1 打开与升级

- open(ReadOnly) 只读打开现有、受支持 schema；缺少表、旧 schema 或索引异常都返回可读状态/错误，不创建表、不写 metadata、不升级、不修复。
- 将现有兼具打开/创建行为的 `CreateFileDatabase` 收口为仅显式新建：`SCDatabaseOpenMode::ReadOnly` 选项返回参数错误；通过排他创建保证目标路径已有文件或并发被创建时返回稳定的 already-exists 错误，不以“先检查、再打开”代替排他性，也不打开、截断或覆盖已有文件。公开入口名称固定为 `OpenFileDatabase`，只打开已有受支持文件，文件不存在时返回 not-found；Backend 打开连接不得携带创建语义，避免存在性检查后被移走时又新建空库，现有 `Normal`、`NoHistory`、`ReadOnly` 三种模式均不得隐式创建。两者均不执行隐式升级；现有旧格式文件返回不支持且保持原样。保留通用显式 UpgradeFileDatabase/ExecuteUpgradePlan 机制供未来有持久数据的版本升级使用；本次切换须在显式 UpgradeFileDatabase 的只读预检中同样拒绝旧格式，并停用或隔离已有 5→6→7→8 注册路径，不能经现存升级注册表绕过“不支持旧格式”边界；不为本次旧格式编写升级计划。数据库构造函数只加载状态，不成为写路径。
- Open 的构造与销毁都不得作为隐式写入口：现有缺 metadata 时 InitializeSchema、缺 schema_version 时 SaveMetadata 的分支仅允许显式 Create 使用，Open 遇到这些情况返回结构化 schema 状态；普通 Open 后仅执行读取并关闭，不得由析构函数 SetCleanShutdownFlag(true) 写 metadata。若仍需 clean-shutdown 标记，只能在明确的写操作生命周期中记录，且不得用关闭动作在失败或只读路径伪造“干净关闭”。
- 普通 Open 的只读预检结束后也不能立即创建现有 `READWRITE | CREATE` 连接：预检与可写打开之间可能出现新的待恢复状态，连接本身或首次读取即可写主库、journal/WAL/SHM。阶段 B 将 Normal/NoHistory/ReadOnly 的打开、初始装载、只读 Inspect、文件级 outcome/Recovery 查询都限制在 Backend 可证明不创建、不改写主库及附属文件的读连接/文件访问路径；普通模式的写连接只在调用方发起明确的写操作后建立，且不带 CREATE。若当前 SQLite/VFS 无法在不修改原文件的条件下读取某种 WAL/待恢复状态，应返回结构化 needs-recovery/不可只读装载，由显式文件级 Recover 处理；不能用 `immutable` 绕过活跃数据库的锁和代际检查。所有 Storage 入口在预检、装载和显式写连接切换时须遵守同一文件身份与锁协议，防止路径被替换或两个入口并发导致预检结论失效；对不经 Storage 修改文件的外部进程沿用第 2 节的一致性边界。
- 现有 `SqliteDatabase` 只有一个按 `openMode` 在构造时选择读写权限的 `SqliteDb db_`，大量 `Prepare/Execute/SqliteTxn` 直接使用该成员；把构造连接改为只读却不调整这些调用会使 Normal/NoHistory 的编辑、schema 与导入写入失败。阶段 B 在 SQLite Backend 内建立明确的读连接与按需写事务连接边界：初始装载及普通数据读取走无写读连接；显式持久写事务在核对原文件身份、generation 和所需恢复状态后取得无 CREATE 的写连接，其 SQL、预编译语句、`BEGIN IMMEDIATE`、提交/回滚都绑定同一连接，不能跨两个连接拆分同一事务。写事务提交后只在新的读快照重载/发布对象，旧读快照及时释放；ReadOnly 模式始终拒绝写，Normal/NoHistory 的既有写语义均保持可用。连接选择封装在 Backend，不向公共接口暴露 SQLite 句柄或要求上层选连接。
- 连接选择不得通过修改对象级“当前 `db_`”指针或其他可被并发公开读取借用的环境状态实现。每次普通读取固定其只读连接/短快照，每次显式写事务固定其写连接/事务上下文，私有 Finalize 候选上下文只供该 attempt 使用；内部 Record/Table/Schema/Query 方法须沿本次操作的上下文取连接，不能在同一次调用中因另一线程切换连接而读到未提交状态。阶段 B 的普通写入和阶段 C 的 Finalize 均须保持写事务未提交时的公开读视图隔离，提交后再按 generation 发布；来源映射 sidecar 的独立短事务不得与尚未结束的正式提交事务交错共用连接，也不得使候选记录进入公开读视图。
- Open 的初始装载在一个新的只读快照中先读取持久 generation，再装载 metadata、schema/records、journal/Undo/Redo 和需要随模型绑定的派生状态，并在同一快照中确认所装载的 generation；装载完释放快照，不让公开对象长期固定 WAL 读事务。任何必需表/列缺失或 Step 读取失败均返回结构化错误，不能把读取失败当作空表、空 journal 或正常游标结束；失败对象不对外发布。并发正式提交只能让 Open 得到提交前或提交后的完整代际，之后的公开数据读取仍按代际规则发现过期。
- 文件若处于必须执行持久恢复才能得到一致视图的状态，Normal/NoHistory/ReadOnly Open 与文件级只读 Inspect 均返回明确的 needs-recovery/不可只读装载结果并保持文件原样；检查必须先于任何可写连接的创建，不能借底层连接打开或首个查询隐式修改 journal、WAL、metadata 或正式记录。阶段 B 提供独立于数据库对象的显式文件级 RecoverFileDatabase 入口，因为此时普通 Open 本身无法返回可用于对象级 Repair 的句柄。仅对本次受支持格式执行恢复：先取得排他访问并核对文件身份/格式，再由调用方显式请求底层持久恢复；成功后重新只读检查并装载一致视图才报告成功，无法确认结果时报告 OutcomeUnknown/需重新检查，不将中途失败视为恢复完成。恢复若仅使已提交事务可见或回滚未提交事务，沿用持久事务中的 generation，不额外伪造正式写入；若另行修复正式数据或索引，须走显式 Repair 的事务及 generation 规则。
 - 文件级恢复的格式核对不得为了读取 schema_version 先对原文件触发恢复。若只读预检因待恢复状态无法可靠判断格式，在排他访问下取得同一时点的主库与持久 journal/WAL 文件副本，在私有临时位置恢复副本并确认当前受支持格式与一致性；`-shm` 是可重建的 WAL 索引，不将原目录的共享内存文件当作持久真相复制使用。副本核验通过且原文件身份/文件集未变，才对原文件执行调用方已请求的恢复。原文件恢复必须使用同目录临时输出、排他锁和文件身份校验：先写临时主库并刷新文件内容，再按 SQLite VFS 要求刷新 WAL/journal 和目录，最后以同目录原子替换并保留可识别的旧文件备份标记；替换前后均重新校验 inode/file-id、格式和 generation。任何一步中断都必须留下可识别的 `RecoveryPending` 状态，下一次 Inspect 只能报告待恢复/结果未知，不能把半替换文件当作可用库，也不能自动回滚或自动重试。副本不足以判定时保持原文件原样并返回不可判定状态；临时副本仅作预检，不能作为正式库发布；清理失败单独报告，不得误删原文件或并发接管的文件。
- import_sessions/chunk/outcome 表只在显式新建数据库时建立。移除 LoadImportRecoveryState()、reader 和健康检查对可能执行 CREATE TABLE 的 EnsureImportSessionStore() 的调用；普通读写模式下的读操作同样只读既有结构，缺表返回明确 schema 状态，不能顺手修复。Begin/Append 也不得借惰性建表绕过显式创建。
- 将当前依赖 CreateFileDatabase 自动升级的调用方改为显式区分 Create 和 Open；不支持格式只允许文件级只读检查，不能由普通 Open 隐式升级、删除或覆盖。

### 7.2 检查与修复 API

- 提供只读健康检查结果，涵盖 schema 版本、SQLite integrity、正式数据一致性、复合查询/引用/空间索引状态、未完成 import session 和损坏 chunk。检查在内存返回结构化 diagnostics，不通过 LogStartupDiagnostic() 写 SQLite，也不因发现损坏 chunk 写入 Failed；需要持久化 Failed 时由调用方显式触发独立写事务。
- 对无法以当前 SCStorage 对象打开的不支持 schema 或 needs-recovery 文件，阶段 B 提供文件级只读 InspectFileDatabase 入口；检查不触发升级，也不绕过公开 Storage API 暴露 SQL。文件级 RecoverFileDatabase 只处理当前受支持格式的持久恢复，不兼作旧格式升级或派生索引重建；阶段 F 再补齐健康诊断及对象级显式 Repair。未来受支持版本升级需要时再提供显式 upgrade-plan 查询。
- 未完成 import session 只显示可 Resume/Abort 状态，不自动 Finalize、清理或切活动版本。
- 复用 CheckQueryIndex() 做只读查询索引检查；EnsureLegacyColumnIndexes() 与 RebuildCompositeQueryIndexes() 仅作为 Backend 私有的显式修复步骤，不能从 Open、构造函数或普通 Query 调用。公开修复入口需将所选索引修复作为独立操作执行，使用可回滚事务并返回结构化结果/receipt；若多类索引不能放在同一事务，需分别报告每类结果，不得返回笼统的全局成功。重建不与 Open 或查询调用绑定。
- 复合查询索引、引用索引和空间索引缓存分别提供可检查状态与显式修复入口；仅已有公共 RebuildQueryIndex() 不足以代表其他派生索引已修复。失败时保留源 records 和上一版有效索引/可恢复状态。
- RunStartupIntegrityCheck() 若继续使用，拆成只读校验与 diagnostics collector；禁止在只读打开期间持久写诊断表。持久化诊断若有审计需求，作为单独显式写操作。
- 参考索引与复合 query index 独立检查，避免“Query index healthy”误代表所有派生结构健康。

## 8. S-06：资源合同与错误

定义公开 `SCImportResourceLimits` 并在 Begin/Append/Finalize/Recovery 中返回实际有效预算。该类型名称、字段和错误码在阶段 A 冻结，不允许以“命名可调整”推迟接口兼容性决策。单 chunk、session 总量和 P0 Finalize 工作集限额由阶段 B/C 实施并在超限时返回稳定错误、保留可恢复 checkpoint；阶段 D 只扩展恢复诊断与额外场景基线，不替代 P0 的产品目标上限验收。索引重建单独使用检查结果、重建选项和重建结果：

- begin request id 长度、单 chunk 字节数、单 request 编码字节数、单字段字符串/BLOB 字节数、session 总字节数、chunk 数、记录数、操作数上限；source 的片段窗口和 Storage 编码缓冲也必须有独立上限。
- Append 额外工作内存上限；Recovery 最多一个 chunk buffer；Finalize 的 staging 读取最多一个 chunk。新 `SCImportChunkSource` 保证调用方和 Storage 都按单 request/有界窗口供给输入；若旧调用方已经把完整 vector materialize，内存只能按调用方外部输入统计，不能宣称端到端有界。完整 Finalize 峰值 RSS 定义为项目基础模型、私有候选态、activeJournal、索引增量、chunk buffer 和 SQLite 事务缓存之和，分别报告各分量与总体值；不能把随操作数增长的候选态/journal 误称为固定 metadata。
- staging 可用磁盘空间下限/写入失败、最大事务/WAL 增量和取消检查边界；sidecar 的最大行数、磁盘占用、attempt token 归属和恢复/清理状态也必须计入配额及诊断。
- 复合索引键列数、单键字节数、重建行数/批次预算由索引维护 API 单独声明。
- 为超限、chunk 损坏、摘要不符、base version 冲突、非法状态、取消和存储空间不足定义稳定错误码；错误附带 session/chunk/字段/操作位置及可重试标志。OS/SQLite 原始错误码留在诊断详情，不泄漏为跨平台公共语义。

当前需求没有给出产品设备上的可接受数值，故阶段 A 需由 Quantity/Storage 共同锁定目标硬件和数值阈值，未锁定阈值不能宣称容量验收通过。至少采集 10 万、100 万和产品目标上限下的追加吞吐、完整 Finalize 时间、open/recovery 时间、峰值 RSS、临时/WAL 空间、取消后清理时间。现有活动编辑与 journal 会随操作数增长；若完整 Finalize 峰值超出预算，必须在解除 D07/M1 阻塞前完成 pending journal 磁盘化/流式验证或等效的有界方案，并重新测量，不能仅以分块 Append 的 T04 结果放行。

S-04/S-05 的规模验收还需在同样三档数据量上报告复合键等值/范围/排序与分页查询的 p50/p95 延迟、cursor 峰值内存、实际扫描行数和索引命中路径，以及复合/引用/空间派生索引检查与重建的耗时、峰值 RSS、临时/WAL 空间。阈值先由产品设备预算确认；功能结果正确但超出预算时，不开放大规模查询或索引缓存接入。

取消只在 chunk 边界、checksum/重建批次边界以及开始最终提交前响应。最终 SQLite 写事务开始后，应完成原子提交或完整 rollback，不能把取消转成部分提交。

## 9. 新建 Schema 与接口切换

1. 当前没有需要保留的历史数据库或旧 import session；新建数据库直接创建 session/chunk/import_outcomes、begin request id 持久身份映射、持久 storage generation 和新索引元数据，格式版本从新 schema 起算。删掉旧 `import_sessions.payload` 序列化/反序列化、流式迁移器、旧会话兼容检查和旧自增序列转换；不对历史文件执行隐式升级或原地覆盖。
2. 普通 Open 与本次显式 Upgrade 都只接受当前受支持 schema，旧格式返回明确 unsupported-schema 状态并保持文件原样；文件不存在时不隐式创建。Create 只接受不存在的目标路径，已有文件一律不覆盖。若开发环境仍有旧文件，由使用者在 Storage 外显式选择其他路径新建或自行处理旧文件；Open/Create/Upgrade 均不替用户删除或重建数据。未来出现需要保留的持久数据后，新增版本变化才使用显式 Upgrade/可回滚迁移。
3. 删除旧导入公开头类型、函数签名、Batch vector 包装入口和 SQLite payload 代码，同步将 Quantity 调用点迁到新 handle/reader/Ready/Finalize API；编译失败直接暴露遗漏调用点，不提供兼容层。新导入数据使用有版本的规范化 chunk 编码、checksum 和输入摘要链；新建库的零 chunk/多 chunk 会话按新格式直接验收。
4. 复合索引编码在新库直接采用新格式；显式重建仍须可回滚，任何重建失败保留源 records 和上一版有效索引/可恢复状态。若未来升级已有持久库，再另行设计索引格式迁移。

## 10. 实施阶段与退出条件

**ActiveVersionKey 硬门槛（P0）**：阶段 A 未取得可核查证据证明 `ActiveVersionKey` 与正式 records/version 指针位于同一 Storage SQLite 事务边界前，不得进入阶段 C，也不得宣称 Finalize 原子性或解除 D07/M1 阻塞。若该指针属于 Quantity 外部文件/服务，必须先确定 outbox、补偿和恢复协议，并把 T02/T03 的“同一结果”验收改为跨系统 durable outcome；在协议落地前，P0 只能交付 staging/recovery，不能提交正式导入。

| 阶段 | 交付内容 | 退出条件 |
|---|---|---|
| A. 合同冻结 | 完整来源身份字段/null 规则、ActiveVersionKey 所属事务、Quantity 在 Begin 前持久保存 opaque request id 的位置与唯一性规则、导入配额、storage generation 的正式写入口、当前无历史数据的切换边界、Finalized/Aborted 终态的持久查询/归档政策及导入/查询/索引重建的设备阈值。 | Quantity 与 Storage 对唯一新 API、错误结果和可量化容量边界签字确认；没有跨数据库伪原子需求。 |
| B. Staging API/Schema 与 Open 边界 | 删除旧导入公开接口/Batch vector 包装/SQLite payload 代码并迁移 Quantity 调用点；新建库直接创建轻量 handle、chunk reader、分块表/摘要/checkpoint 与增量输入摘要、唯一 Begin request id 的持久身份映射及按键只读结果查询、Ready 冻结结果的只读恢复、metadata-only recovery 与分页活动会话枚举、Aborted tombstone、显式失败转换、普通 Commit/MoveHistory/独立 schema 与索引写入/Upgrade/Repair/历史重置等正式写入口的 generation 校验与推进、活动编辑 schema 写入收敛到 Commit、单 chunk/session 预算与稳定超限错误；移除普通 Open 的构造/析构补写、隐式升级、建表和索引物化，并以单一只读快照完成初始装载；提供文件级只读 Inspect 与受支持格式的显式 Recover，显式 Upgrade 对旧格式也须无写拒绝。 | T04 的追加部分、T05、T08 的 staging/终态/读写错误子集及 T07 的 Open/新建/文件级恢复子集通过；跨入口代际与活动编辑 schema 回滚子集通过；Begin/Ready 提交后丢失返回值仍可分别按 request id 重试/查询和通过 Recovery 获取冻结摘要，session id 不复用；旧格式文件在 Open/Upgrade 下均不变且返回不支持，新接口调用点全部迁移，普通/ReadOnly Open、只读 Inspect、只读操作后的关闭和 Recovery 均不写入；需要持久恢复的新格式文件可经文件级显式入口恢复，并发提交与读取错误不能产生可用的混合代际对象。 |
| C. Atomic Finalize 与 P0 容量 | Batch 在私有候选态分块消费、操作来源映射、attempt 代次隔离、同事务提交与清理、唯一 receipt、base version/generation 并发冲突、有效 commit name 规范化、接入阶段 B 的正式写入 generation 规则、Finalize 工作集限额及提交后结果判定。 | T01、T02、T03、T04 的完整 Finalize 部分及 T08 的 Finalize/来源归因子集在 10 万/100 万/目标上限通过；包括并发读取、Abort/Finalize 竞争、旧 attempt fencing、候选回滚、Repair/历史重置竞争、SQLite commit 后异常、内存发布失败、提交后别的连接推进 generation 的重载竞争、空导入和容量超限；未达预算不得解除 D07/M1 阻塞。 |
| D. 恢复和扩展容量 | 在阶段 B/C 已交付的状态转换、安全取消、失败重试、错误归因及 P0 限额上，补充恢复诊断、残留清理策略和扩展场景性能基线。 | S-03 全覆盖；取消/崩溃残留能显式恢复或清理，不倒置 P0 取消、重试、状态转换与容量验收的依赖。 |
| E. 查询/索引 | 复合身份约束、扩宽复合键、惰性 cursor、查询/引用/空间派生索引维护和显式诊断/重建。 | T06 及目标规模查询/索引重建预算通过，requireIndex 不会静默扫描。 |
| F. 启动诊断与显式修复收口 | 在阶段 B 已完成的只读 Open/显式 Create/文件级 Inspect 与 Recover 边界上，补齐全面健康诊断、对象级显式 Repair、readonly 与不支持 schema 的检查覆盖；收口 EnsureLegacyColumnIndexes 和 RebuildCompositeQueryIndexes 的显式修复调用边界。 | T07 全部通过；各类检查不写入，修复仅在显式调用时执行且失败可回滚。 |
| G. Replay 隔离恢复接入 | 在 S-04/S-05 已完成的索引和只读/修复合同上实现候选版本重放、隔离查询、取消/失败清理与显式发布。 | T09 通过；失败/取消不改变活动版本，Finalize 后候选版本和派生索引一致发布。 |

阶段 B 的 Open 无写验收必须与首次显式写入回归同时完成：Normal/NoHistory 对象先以无写读连接打开并完成初始装载，调用方明确发起 Commit/schema/导入后，Backend 在同一文件 lease 和 generation 校验下按需取得无 CREATE 写连接，再由该写连接独占完成事务；正常与 NoHistory 对象的既有写语义保持可用，ReadOnly 保持拒写，写事务不能跨读写连接拆分。

## 11. 测试覆盖映射

新增或调整 GoogleTest；本次方案编写阶段不执行测试。优先扩展 Tests/SqliteBackend/SqliteImportTests.cpp、SqliteReadOnlyOpenTests.cpp、数据库显式新建/不支持格式测试、Batch failure tests 和 Tests/Query/*Composite*Tests.cpp。本次不安排旧格式升级测试。

| 验收项 | 必测边界 |
|---|---|
| T01 追加/取消/崩溃 | 对 chunk 负载写、metadata/输入摘要链写、checkpoint 更新、事务 commit 分别注入失败；验证原 ActiveVersionKey/正式版本不变，checkpoint 只指向完整 chunk，重试幂等；通过 `SCImportChunkSource` 喂入超限 request/chunk，验证增量编码期间拒绝且 Storage 附加内存受窗口上限约束；内存验收只针对新 source 及其分片窗口，旧 vector 接口已删除，不属于兼容路径或验收对象。 |
| T02 Finalize 成功 | 正式记录、版本、storage generation、Undo/journal、活动指针（若属同一数据库）、receipt 和 staging 清理同一结果；发起导入的公开对象提交前只见旧代际、提交后只见完整新代际，旧 Table/Record/Cursor/Schema 的 ErrorCode 数据操作返回过期错误，noexcept 身份访问器仅返回缓存值；新建记录的 RecordId 不与导入记录冲突，Undo/Redo 后身份和引用索引一致，公开表对象不得仍绑定私有候选连接；独立打开的旧对象在下一次数据读写返回过期状态，进行中的 cursor 在下一次 Next 的新读快照中先核对 generation 再读索引，变化时报过期并终止，不返回不可读取的旧 Record 句柄，重新打开后看到新代际；恢复 API 返回 Finalized receipt，零 chunk/零记录也返回唯一 receipt，空导入 Undo/Redo 仅移动已提交空变更历史且不重读 staging。 |
| T02/T03 提交后重载竞争 | 在 Import SQLite COMMIT 与公开对象模型重载之间，让另一连接完成正式提交；重载须在同一新快照内核对 receipt 的 committed generation，代际已前进则返回带原 receipt 的“已提交、需重新打开”并 fault 原对象，不能把其他提交的模型当作本次导入结果发布；代际匹配时一次发布完整模型。另以未耗尽的旧 cursor 占用快照，确认不会让公开对象的后续代际核对复用旧快照；重载快照发布后释放，其他连接继续写入时普通读取能报过期。 |
| T03 Finalize 任一失败 | 读取/应用、候选约束校验、记录写、journal、receipt、chunk/sidecar 清理、最终 commit 和提交后内存发布失败均验证结果；提交前失败完整 rollback 且旧公开视图不变。对 COMMIT 返回异常、COMMIT 成功后异常和进程无回复分别经文件级只读入口查 receipt，区分未提交、已提交需重开、OutcomeUnknown，不返回普通失败但已持久提交；故障对象自身不可用时仍可查询 outcome，重试查到 receipt 但对象未发布新代际时须提示重新打开；同一公开对象已有活动编辑时 Finalize 拒绝，Finalize 屏障期间普通 BeginEdit/Undo/Redo/Upgrade/Repair/历史基线重置拒绝，失败解除屏障或 fault、成功发布后解除；跨对象旧代际普通 Commit/Undo/Redo/Upgrade/Repair/历史重置报冲突，Rollback 仅丢弃旧内存候选；业务版本不变但 Upgrade/Repair 推进 generation 时旧对象同样过期且 Finalize 冲突；两个 Finalize 与 Abort/Finalize 竞争只能产生一个终态，旧 attempt 在新代次领取后不得写 sidecar 或提交；sidecar 短事务提交后进程中断时，Recovery 能按 token 隔离、保留或回收映射。 |
| 跨入口代际与 schema 回滚 | 分别通过 MoveHistory、CreateTable、DeleteTable、无活动编辑的 RenameTable、ISCSchema 列增改删、约束与索引增删及显式索引重建推进 generation；发起对象成功后可继续读写新代际，旧句柄失效，已确认提交但发布失败返回“已提交、需重开”并 fault，提交结果不能判定时返回 OutcomeUnknown，其他对象及旧 session 在业务 VersionId 未变时仍报过期。活动编辑中的 schema 修改在 Commit 前对其他连接不可见，Rollback 完整丢弃候选、原 schema/索引/记录保持不变且不覆盖其他连接的新提交；Commit 对 records、schema、journal 和 generation 同事务提交。用两个独立打开对象交叉执行上述入口，逐项验证失败回滚与代际冲突。 |
| T04 内存与 P0 放行 | 10 万/100 万/目标上限连续追加及完整 Finalize；确认追加内存不随历史 payload 线性增长，分别记录项目基础模型、私有候选、activeJournal、索引、chunk、SQLite 工作集及总体峰值 RSS/耗时/WAL。完整 Finalize 超预算时不得放行。 |
| T05 Recovery | metadata-only recovery 不加载 chunk 负载；Begin 已提交但返回丢失时，同 request id/选项重试返回原 session/终态，选项不同冲突，并发同键仅创建一次；按 request id 只读查询在活动、Finalized、Aborted 及归档后都能定位同一 session，不能依赖活动列表；Ready 已提交但返回丢失时，重复 Ready 和只读 Recovery 均返回同一冻结 checkpoint/摘要，期望 checkpoint 不同返回冲突，随后 Finalize 可继续；分页 ListImportSessions 在静态集合分页/筛选不重复不漏项且不写库，并发新建/终止时按文档规则复查 id；Abort 结果不确定时用已知 id/request id 查询活动或终态；reader 单 chunk 读取；损坏 chunk 给出确定序号/摘要且读操作不写 Failed；取消 reader 不改正式数据；分别查询活动、Finalized、Aborted 和未知 session，终态行删除 chunk 后仍返回最后序号、总数、字节数和预算摘要；只读查询活动 attempt/租约不修改它，Resume 在追加态若正式 generation 已变化则报冲突且不改 checkpoint、在 Ready 态只验证当前状态并由显式 `ReclaimImportAttempt` 在租约失效或受控撤销后领取更高 token，活跃租约返回 InProgress，终态不可 Resume。 |
| T05 reader 并发 | reader 打开后并发 Append 只使新 reader 看到新增尾部，旧 reader 在固定 checkpoint 结束；每次 Next 单块读取并释放短快照，不跨 Next 钉住 WAL；并发 Abort/Finalize/正式代际变更使旧 reader 返回终态或过期且停止，Finalize 专用 reader 在 token 被撤销后停止，不继续发布旧 attempt 的数据。重复 Begin 在有效默认配额变化后仍按原显式选项命中原 session，并继续使用创建时冻结的有效配额。 |
| T06 复合查询与派生索引 | 4 列及最终身份字段组合的 unique/equality/range/order/null 行为；requireIndex、缺失/损坏索引、重建失败；create/update/delete/reopen/Undo/Redo/恢复后复合查询、引用索引和空间索引缓存一致，重建不复活当前正式数据中已删除的记录。三档目标数据量记录查询 p50/p95、cursor RSS、实际扫描行数及索引检查/重建耗时、RSS、临时/WAL 空间，并与阶段 A 阈值比较。 |
| T07 ReadOnly/Open | **阶段 B/P0 子集**：显式 Create 在不存在的路径创建完整新 schema，已有文件与并发创建均不得覆盖，Create(ReadOnly) 返回参数错误；注入 schema 创建中断/空间不足，只清理可证明由本次创建的文件，清理失败返回明确状态，重试不误开半成品；Open(Normal/NoHistory/ReadOnly) 对不存在路径均不创建文件，存在性检查后文件被移走也不得新建；对文件和 WAL/SHM 相关状态做前后核对，ReadOnly 与普通 Open/Recovery 均不建表、不升级、不物化索引；普通 Open 后仅查询再关闭不写 clean_shutdown，缺 metadata/schema_version 的文件返回状态且不由构造函数补写，构造失败后析构也不写；在初始装载 metadata、records、journal 中间注入另一连接提交，只能打开完整旧代或新代，初始读快照释放后可检测后续代际变化；必需数据读取失败不能被当成空表或空历史并返回可用对象；需要持久恢复的文件由 Open/Inspect 返回 needs-recovery 且文件原样，恢复只由显式入口执行；不支持的旧格式返回稳定错误且文件原样，依赖 CreateFileDatabase 自动升级的调用方已改为区分 Create/Open。**阶段 F 全集**：健康检查不改变 Failed 或写诊断；Repair 仅在显式调用后改变并在事务内推进 storage generation，旧对象随后报过期；各类派生索引检查/修复分别验证。 |
| T08 错误诊断 | chunk 损坏、摘要不符、各类超限、base version/generation 过期、非法 state、取消、磁盘不足映射稳定错误码并给出可用位置；正式代际变化后旧 session 的 Append/Ready 拒绝且 checkpoint 不变；在 Staging/Checkpointed 且正式代际未变化时相同 sequence+相同规范化字节重试返回当前最新 checkpoint 与 `alreadyApplied`，相同摘要但不同字节返回冲突，已存 chunk 损坏则返回损坏错误；Ready 后迟到的相同 Append 仍返回非法状态且冻结摘要不变，调用方可通过 Recovery 得到 checkpoint；可重试 Finalize 失败释放 token 后可立即重试，释放失败须等显式 `ReclaimImportAttempt` 在安全过期后回收；不可重试 Finalize 失败与 Abort/其他 Finalize 竞争时 Failed 转换必须核对 token/state/outcome，不能覆盖已提交终态；Abort 重复调用幂等，Recovery 返回同一 tombstone；未知 session 返回 NotFound、Finalized 后 Abort 返回冲突；Failed session 只能显式 Abort 清理，归档后 session id 不复用。 |
| 新接口与幂等 | 只保留 handle/reader/Ready/Finalize 导入接口，静态检查无旧导入类型、签名、导入专用旧选项或调用点，普通 ExecuteBatchEdit/SCBatchExecutionOptions 仍可用；非空/零 chunk 会话均先 Ready 后 Finalize，已 Ready/Finalized 重试分别核对冻结摘要/receipt；Append 失败保留 checkpoint，由调用方显式 Abort；Abort 的 COMMIT 后异常先通过只读 outcome 查询分辨 Aborted tombstone、Finalized receipt、仍活动且证明回滚或 OutcomeUnknown。空 commitName 与显式等于 session_name 的名称产生同一有效名称和 receipt；不同有效名称返回冲突，终态删除活动行后仍能规范化重试。 |
| T09 Replay 隔离恢复 | 候选重放期间活动版本和普通查询结果不变；取消、失败、Abort 后候选索引可清理或重试；Finalize 后候选版本、活动指针、receipt 和派生索引一致发布。 |
| 新建格式 | 显式 Create 后检查 session/chunk/outcome 表、begin request id 身份映射、storage generation、索引元数据和新格式版本完整；空/多 chunk 会话的 checksum/摘要链与 reader 独立重算一致；不支持格式的 Open/Inspect 保持文件原样；新库中终态清理/归档后 session id 与 begin request id 均不复用。 |

T07 的阶段 B/P0 子集还须覆盖：制造当前受支持格式的需持久恢复状态，确认 Open/Inspect 在任何可写连接前报告 needs-recovery 且文件及附属文件不变；普通 Open、文件级 outcome/Recovery 查询及关闭过程中，主库与 journal/WAL/SHM 的存在性和内容均不变，覆盖 WAL 附属文件缺失或需创建 `-shm`、预检后并发替换路径/生成待恢复状态、首次查询触发恢复及对象析构；无法无写读取时返回结构化状态。显式 RecoverFileDatabase 无需可用数据库对象，在排他访问下恢复，随后只读检查可装载一致代际；模拟排他锁竞争、恢复中断及结果无法确认，分别验证不宣称成功、可重试或报告 OutcomeUnknown。对旧格式同时调用 Open、Inspect、显式 Upgrade 和文件级 Recover，除 Inspect 返回诊断外均稳定拒绝且原文件不变；停用的旧升级路径不得经其他公开入口执行。

T05 还须验证：Finalize/Abort 后按 request id 查询终态时替换数据库路径，且替换库的 generation 恰好相同；`GetImportOutcome(query, outOutcome)` 必须按持久 file-id 返回 `PathChanged`/`OutcomeUnknown`，不能返回替换库的 receipt。

恢复专项还须验证：替换前生成同目录临时输出并记录文件身份，主库/WAL/journal/目录刷新和原子替换按固定顺序执行；任一阶段进程中断后，下一次 Inspect 只能返回 `RecoveryPending`/OutcomeUnknown，不能装载半替换文件或自动宣称成功；旧文件备份、临时文件和清理失败均有可查询归属，路径被替换或文件身份变化时恢复拒绝且原目标不被覆盖。

恢复 manifest 必须位于待恢复主库之外的持久位置，并在替换前记录目标 file-id、临时主库、备份、WAL/journal 集合、generation 和 phase；每次 phase 更新原子写入并刷新目录。phase 固定为 `Prepared`、`Replacing`、`Committed`、`CleanupPending`、`Archived`：新主库和附属文件只读校验成功后先写 `Committed`，再清理备份/临时文件；清理中断保留 `CleanupPending`，主库仍可只读打开，后续显式清理完成后才归档 manifest，不能永久停在 Pending。Inspect 先读取 manifest，`Prepared/Replacing` 只能报告 `RecoveryPending`/OutcomeUnknown。WAL/journal 采用单一路径：私有副本先完成 checkpoint/truncate，使发布目录只需原子替换已关闭 WAL 的主库；若源为 rollback-journal 模式，必须在排他 lease 下完成事务回滚并确认 journal 已删除/归档后再替换主库，任何残留附属文件均使状态为 Pending，不允许用普通多文件 rename 宣称整体原子。Recover 取得 Storage 级文件 lease，等待或拒绝已有连接、游标和其他 Recover；替换后旧对象/游标统一标记 stale，必须重新打开才能使用。Finalize 的私有候选上下文若产生临时文件，也必须写入绑定 session/attempt/token 的 candidate manifest，文件集和 phase 固定为 `Prepared`、`Committed`、`Aborted`、`CleanupPending`；清理按“先原子标记文件集并刷新目录、再按 file-id/token 删除主库/WAL/journal、确认删除后进入 Archived”顺序执行，manifest 不得先删除；进程中断时扫描按 file-id/token 决定继续清理或保留 CleanupPending。提交 receipt 后候选清理失败返回“已提交、需重开”并保留 `CleanupPending`，未提交失败由 `ReclaimImportAttempt`/Abort 或内部恢复扫描器清理；该段仅适用于未来启用 candidate manifest 的扩展协议，P0 不调用 Resume 执行写入或清理，不能留下不可发现的磁盘状态。

source 回归还须覆盖：单 request、单字符串/BLOB 的超限在片段窗口内拒绝；Storage 分配并返回 next sequence，source 在 `ResetChunk(sequence)` 后逐字节重放得到同一摘要；source 消费、取消和 I/O 失败期间不持有 SQLite 写锁，随后短事务重新校验 generation/sequence；回调重入 Storage、锁等待超时和并发 Abort 均返回稳定结果。文件恢复回归还须覆盖 manifest 的 Prepared/Replacing/Committed/CleanupPending/Archived 各 phase、主库/WAL/journal checkpoint/truncate 中断、路径/file-id 变化、已有连接/游标和并发 Recover，确认旧对象只能 stale/重新打开；candidate manifest 的未提交清理、已提交需重开和 CleanupPending 也必须可扫描、可重试且不误删正式库。
Candidate manifest 是外部候选文件的恢复标记，不能与 SQLite 事务宣称跨载体原子。Finalize 采用 receipt-first 协议：SQLite 事务先写正式记录、receipt、staging/sidecar 清理结果和 `candidate_cleanup_pending` 标记，并使 attempt token 失效；提交后由持有匹配 durable receipt 的恢复扫描器以 session/attempt/token/file-id 更新外部 manifest 到 `Committed`，开始文件清理时转为 `CleanupPending`，完成后转为 `Archived`。若进程在两步之间中断，扫描以 receipt 和联合身份为准补写 manifest；manifest 已是 Committed 但 receipt 不存在时只允许按 token 丢弃候选。Abort 的 SQLite tombstone 事务同时使 attempt token 失效并写 `candidate_cleanup_pending`；候选进程后续 manifest 更新必须因 token 不匹配拒绝，扫描见 Aborted tombstone 后将外部 manifest 标记 `Aborted/CleanupPending` 再清理。扫描不能仅依据 manifest phase 清理候选文件。
Candidate manifest 的 phase 统一为 `Prepared`、`Committed`、`Aborted`、`CleanupPending`、`Archived`；清理确认后必须进入 `Archived`，并保留 session/attempt/file-id/清理结果供只读查询。Finalize receipt 事务同时使 attempt token 失效；候选进程每次更新 manifest 前必须读取 durable outcome 并重新校验 token，已 Finalized/Aborted 的旧 token 一律拒绝。仅持有匹配 durable receipt 或 Aborted tombstone 的恢复扫描器可执行一次受限的 terminal reconciliation：联合校验 session/attempt/token/file-id 后补写 `Committed` 或 `Aborted`，并推进清理；该恢复权限不恢复 token 的一般写入能力，也不允许候选进程绕过 fencing。T03/T09 覆盖 receipt 已提交、manifest 尚未更新、Abort 竞争、terminal reconciliation 和 CleanupPending 到 Archived 的中断恢复。

阶段 B 另须回归：用 Normal 与 NoHistory 分别无写 Open、只读查询和关闭，再重新 Open 执行普通编辑 Commit、独立 schema 写入及导入 Begin/Append/Abort；确认写事务中全部 SQL 使用同一写连接，提交后读连接的新快照能看到完整新代际，失败回滚后读连接仍只见原状态。ReadOnly 对这些写入返回稳定拒绝，读连接及其未释放语句不得阻断本对象写事务或把旧快照用于提交后发布；文件在 Open 与首次写入间被替换时不得写入替换后的文件。

连接隔离回归须在普通 Commit、schema 独立事务、Append/Abort 的短事务和阶段 C Finalize/来源映射 sidecar 事务中插入并发公开读：读者在提交前只能得到上一正式代际或稳定冲突/忙状态，不能经共享连接看到未提交记录、schema、索引、receipt 或候选态；提交后只得到完整新代际，回滚后始终只得到原代际。并发切换连接及复用预编译语句时仍须满足同一事务归属与 token fencing。

测试只能验证契约，性能数字另由可重复 benchmark 报告；不得以只跑轻量 fixture 代替 10 万/100 万容量基线。

## 12. 风险评估

- **R-01 活动版本指针边界（阻断原子承诺）**：需确认 ActiveVersionKey 是否存于同一个 SQLite 数据库。若属于 Quantity 外部文件/服务，SQLite 事务无法同时提交两者；必须调整事实来源或另立 outbox/恢复协议，不能宣称单事务原子。
- **R-02 接口切换遗漏**：删除旧导入接口后，Quantity 若仍持有完整 staging vector 或依赖旧选项会编译失败；阶段 B 同步迁移全部调用点，公开头、Batch 包装和 SQLite 序列化代码一并删除。
- **R-03 内存基线（P0 放行项）**：SqliteDatabase 维护加载后的记录模型，私有候选态和活动编辑/journal 也可能按导入操作增长；staging 分块只能限制 chunk 与序列化附加开销。完整 Finalize 的总体峰值需在 D07/M1 放行前达到已确认预算，超限则先实现有界 journal/候选态方案。
- **R-04 新建格式完整性**：显式 Create 若只建立部分分块表、终态表或 generation 元数据，后续只读恢复会报告缺表；创建步骤须在一个可回滚事务内建立完整 schema。失败时只能清理由本次排他创建且可证明归属的目标文件及其临时文件，清理失败要报告可恢复状态，不得误删先前存在或并发接管的文件；重试不能把半成品误判为可打开的新库。
- **R-05 查询键语义**：字段身份、大小写、null、排序规则由 Quantity 提供；Storage 不应猜业务 key。超过三列会影响编码器、索引物化、查询计划和既有数据重建。
- **R-06 长事务与空间**：原子 Finalize 可能形成较长写事务和 WAL 峰值；要按目标数据量、并发访问、磁盘剩余空间设预算，并在写事务前尽早拒绝明显超限。
- **R-07 提交结果不确定及发布竞态**：进程可能在 SQLite commit 成功但 API 未返回时退出；CommitPersistencePhase 也可能在 commit 后内存更新时抛异常。receipt 必须在同一事务落盘，并可按 session id 独立只读查询；仅 autocommit=true 不能证明已回滚。公开模型重载必须在同一新快照内核对 receipt 的 committed generation，防止跨连接后续提交被误发布；不匹配或发布失败时返回已提交需重开结果并禁止故障句柄继续读写。
- **R-08 Create/Open 边界与 P0 放行**：当前 CreateFileDatabase 普通打开会自动升级旧 schema，SqliteDatabase 构造与析构还可能补写 schema/metadata。阶段 B 必须将调用方改为显式区分 Create/Open，旧格式返回不支持且保持原样；ReadOnly 与普通 Open 及无写关闭都不得升级或补写。T07 的 Open/新建子集是 D07/M1 的 P0 放行条件，不得留到阶段 F。
- **R-09 候选态可见性与旧对象**：现有 CreateRecord() 直接写入内存 records_，普通枚举可读取活动编辑中的草稿。导入必须使用 Storage 内部私有候选上下文；发起导入的公开对象提交后切换完整模型/RecordId 分配器并失效旧句柄，不能移植仍绑定候选连接的表对象。其他对象不能用旧内存记录与新 SQLite 索引混读，须在下一次读写前检测持久 storage generation 并报告过期；所有正式写提交仍在写锁下校验并推进 generation，不能覆盖导入或维护结果。
- **R-10 终态与只读语义**：Abort 删除 chunk 负载时必须同事务保留可查询 tombstone；session id 不可复用，显式归档仍须支持按 id 查询。新 Recovery/reader/健康检查不得调用惰性建表或因发现损坏而写 Failed。终态查询/归档策略在阶段 A 冻结。
- **R-11 跨入口代际一致性**：业务 VersionId 不覆盖 MoveHistory、独立 schema/索引写入、显式 Upgrade/Repair 和历史基线重置；若任一正式写入口漏掉 storage generation 校验或推进，旧对象可能混读或覆盖新状态。阶段 B 必须按公开写入口和 Backend 的独立持久事务逐项接入，尤其覆盖 CreateTable/DeleteTable、无活动编辑的 RenameTable、ISCSchema 列增改删、约束与索引增删及 MoveHistory；阶段 C 复用同一规则，并以跨连接竞争测试封口。
- **R-12 Append 重试与 reader/cursor 快照**：相同序号重试只在可追加状态且正式代际未变化时幂等；Ready 后迟到重试返回状态错误并由 Recovery 提供 checkpoint。import reader 固定打开时的 checkpoint 上界、每块使用短快照并在终态/代际变化时终止，避免长期钉住 WAL 或读到并发追加的不确定尾部；查询 cursor 每次 Next 都用新快照核对 generation 和确定性续位点，避免旧快照遮蔽新提交或返回已失效记录。
- **R-13 Begin 身份与会话发现**：Quantity 若未在 Begin 前持久保存 request id，响应丢失且会话随后终结时无法精确定位结果；阶段 A 冻结键的保存位置，阶段 B 建立活动与终态共享的唯一身份映射及按键只读查询，活动列表仅用于人工排查。空 commitName 必须在活动行删除后仍按持久 session_name 规范化；Abort COMMIT 后异常须查询 tombstone 并保留 OutcomeUnknown 分支，阶段 C 用同一持久输入摘要和 receipt 完成幂等。
- **R-14 Ready 恢复与 checkpoint 单调性**：Ready 结果若只由 API 返回而不随活动 session 持久保存，响应丢失后调用方无法提供 Finalize 所需摘要；重复较早 chunk 若返回历史 checkpoint 会让调用方倒退。阶段 B 须将冻结结果放入只读 Recovery、规范 Ready 重试与最新 checkpoint 返回。
- **R-15 活动编辑的 schema 提前落盘**：现有部分 schema 方法在活动编辑中独立提交，Rollback 再靠 journal 写入补偿；跨连接提交后这种补偿可能覆盖新状态，且只在 Commit 增加 generation 会漏掉已可见的 schema。阶段 B 须把活动编辑中的 schema 操作收敛到候选态与 Commit 单事务，独立 schema 写入则在自身事务校验并推进 generation；未完成跨对象回滚/冲突测试前不得放行 P0。
- **R-16 打开时混合代际与静默截断**：若初始装载逐段独立读取，另一连接提交可能使元数据、记录和 journal 来自不同代际；若 Step 错误被当作结果结束，损坏数据库可能被当作空历史。阶段 B 的 Open 必须用单一短只读快照与明确错误传播，构造失败不发布对象或触发析构补写；需持久恢复的文件只报告状态，不在打开时自行修复。
- **R-17 无句柄恢复与旧升级路径**：需持久恢复的文件无法取得可用对象句柄，若显式恢复仅留在阶段 F 的对象级 Repair，阶段 B 会出现无法恢复的新库；故文件级 Recover 与只读 Inspect 均须在阶段 B 交付，并在排他访问、确认结果后才报告成功。已有 5→8 升级注册路径若仍可从显式 Upgrade 进入，将绕过本次旧格式不支持边界；必须在升级入口预检拒绝旧格式并停用或隔离旧路径，验证拒绝不写库。
- **R-21 新接口与流式输入合同**：仅写概念性的 handle/reader/source/receipt 无法指导头文件、所有权和错误码实现；完整 `SCImportChunk` vector 也无法满足端到端输入有界。阶段 A 必须冻结可编译接口、source 生命周期和并发规则，并明确调用方输入内存与 Storage 附加内存的统计边界。
- **R-22 文件恢复替换安全**：私有副本恢复后如何安全替换主库、WAL/journal 和目录，若无固定刷新顺序、文件身份校验和 `RecoveryPending` 状态，进程中断可能留下半替换文件并被误打开。恢复协议必须支持重启判定、可追踪备份和结果未知，不得自动把中途失败当作成功。
- **R-23 Sidecar 崩溃归属**：sidecar 先于最终 Finalize 事务提交时，进程可能在两次提交之间退出；若没有 session/attempt/generation 归属和明确回收状态，旧映射可能泄漏或被新 attempt 误用。sidecar 清理失败必须进入可查询恢复状态，不能只返回普通失败。
- **R-18 只读预检后的可写连接竞态**：现有 SQLite 普通连接携带 `READWRITE | CREATE`，只在连接前做一次只读预检不足以保证 Open 无写；WAL 的读连接也可能涉及 `-shm` 创建。阶段 B 须将打开和只读查询全程约束为主库及附属文件无写，显式写操作才可取得无 CREATE 的写连接，并按同一文件身份/锁协议重新核对状态；不能以 `immutable` 读取活跃库掩盖代际变化。不能证明无写的格式状态必须报需显式恢复，T07 覆盖预检与打开之间的竞态及 WAL 附属文件变化。
- **R-19 连接切换与事务归属**：现有 `SqliteDatabase` 的单个 `db_` 同时服务读写，若直接改为只读连接，Normal/NoHistory 的 Commit、schema、导入写事务会失败；若临时另开写连接但沿用原连接的语句，又会造成跨连接分裂事务。阶段 B 必须在 Backend 收口读写连接选择，让每次显式持久写入的语句与事务绑定同一个写连接，提交后以新读快照发布完整代际，并覆盖只读打开后首次写入、Rollback、ReadOnly 拒写和文件路径替换。
- **R-20 并发读借用写连接**：若通过可变对象级当前连接把旧 `db_` 在读写间切换，公开读取可能在写事务未提交时借用写连接，看到候选记录、schema 或 receipt，并破坏 Finalize 隔离。Backend 必须按操作固定读连接/短快照和写事务连接，私有候选上下文独占 attempt；阶段 B/C 用并发读与故障注入确认提交前视图、回滚后视图和 token fencing。

## 13. 建议下一步

1. 先由 Quantity 确认来源身份字段、ActiveVersionKey 的存放位置、目标硬件/产品最大实体量、完整 Finalize/查询/索引重建预算和无历史数据的切换边界；评审并迁移当前依赖 CreateFileDatabase 自动升级行为的调用方。
2. 以 S-01/S-02 为独立 P0 交付，先评审私有候选态、公开读视图、提交后 receipt 核对和终态查询合同，再落公开头；同时逐项核对 MoveHistory、直接 schema 写入及活动编辑 schema 回滚的代际边界，新 schema 创建、容量测试和提交点故障注入同步设计。
3. 优先让 CAD 导入调用方使用轻量 session + chunk source，不再构造完整 staging vector；并行补全恢复/资源合同。
4. 查询扩展复用现有 Query planner/index tests；确认复合键字段和 null 排序后再扩宽索引格式，并覆盖引用索引和空间索引缓存。
5. 分别完成显式健康检查/修复与 ReadOnly/Open/Create/不支持格式边界测试后再开放 P1 接入。
6. S-04/S-05 收口后，再接入空间索引缓存和 Replay 隔离恢复，并通过 T09 验证失败/取消时活动版本不变。

## 审查记录

以下记录保留方案演进过程；涉及旧接口兼容或历史数据库迁移的结论均已由“无历史数据、直接删除旧接口”的当前决策取代，不作为实施合同。当前实施以第 1–13 节为准。

- 第一轮独立审查：发现并修订 8 项——补充当前 CreateFileDatabase 的自动升级现状及迁移要求；将 schema 转换明确为现有 ExecuteUpgradePlan 的整体事务；规定 BEGIN IMMEDIATE 后再次检查 base version；补齐旧 eager API 的显式兼容入口和成功后更新本地 checkpoint；将失败归因 sidecar 定为必需；避免 Commit 持久化逻辑复制；把索引容量移出 import limits；要求索引 cursor 同时避免全量 materialize record id 和 record object。无代码或测试执行。
- 第一轮补充审查：又发现并修订 4 项——BeginImportSession 的初始行/checkpoint 原子创建；Commit 前将 operation origin 落入可查询的 sidecar，以覆盖候选态约束失败；区分 staging chunk 上限与 activeJournal/候选态内存；为不支持 schema 增加不升级的只读文件检查路径，并说明升级整体事务的 WAL 空间风险。
- 第二轮独立审查：发现并修订 2 项——将 Finalize attempt 初始化/清理映射明确放到读取和应用 chunk 之前；明确所有活动状态到 Failed/Aborted 的合法转换及终态语义。
- 第三轮独立审查：发现并修订 1 项——补入需求结尾要求的空间索引缓存与 Replay 隔离恢复约束，并增加对应验收覆盖。
- 第四轮独立审查：发现并修订 2 项——将空间索引缓存和 Replay 隔离恢复的接入阶段移至 S-04/S-05 之后；明确 EnsureLegacyColumnIndexes 与 RebuildCompositeQueryIndexes 仅由显式可回滚修复入口调用，并分别报告不同派生索引的修复结果。
- 第五轮独立审查：发现并修订 1 项——明确 Failed 不可恢复/Finalize，但可由调用方显式 Abort 清理，消除“Failed 为终态”与保留诊断供 Abort 之间的状态机矛盾。
- 第六轮独立审查：发现并修订 1 项——明确索引恢复只能从当前正式数据/候选版本重建，不能依赖过期缓存复活已删除记录，并加入验收覆盖。
- 第七轮独立审查：按 S-01 至 S-06、T01 至 T09、实施依赖、状态机、事务/回滚、显式 Open/Upgrade/Repair 及 Storage 分层硬约束复核；未发现新问题。

### 开发前审查修订（2026-10-19）

- 本轮先修订 6 项：Finalize 私有候选视图与失败回滚；提交后用 receipt 判定 durable outcome；把完整 Finalize 容量纳入 P0 放行；为 Aborted 保留 tombstone；移除读路径惰性建表并分离只读诊断和显式 Failed 转换；补充查询/索引重建规模门槛。同时明确空导入的提交合同。
- 第一轮独立复审：发现并修订 3 项——旧数据库对象在导入提交后可能覆盖新版本；原公开 Table/Record/Cursor 句柄在模型发布后的行为不明确；Abort 新旧接口及跨终态表的 session id 唯一性未定义。现规定所有写提交校验持久版本、旧句柄失效、终态可查询且 id 不复用。
- 第二轮独立复审：发现并修订 3 项——独立旧对象可能把旧内存记录与新 SQLite 索引混读，现改为读写前检查持久版本且进行中的 cursor 固定快照或终止；迁移旧自增序列时可能复用已删除会话的 id，现保存旧序列并禁止伪造历史终态；阶段 B 不可能独立通过覆盖 Finalize 的整个 T08，现拆分子集和阶段 C 验收。
- 第三轮独立复审：发现并修订 2 项——版本检查与 SQLite 索引读取之间有竞态，现规定在同一只读快照中核对版本并固定 cursor 结果；分表终态与并发 Finalize/Abort 缺少可执行互斥，现使用 session id 唯一的 import_outcomes、attempt 代次 fencing 与最终事务内的状态/摘要校验。
- 第四轮独立复审：发现并修订 3 项——noexcept 身份访问器无法返回 stale 错误，现只返回缓存值并限定 ErrorCode 数据方法的过期行为；旧对象有活动编辑时 Rollback/close 必须作为恢复例外且不可写回旧数据；故障对象上的恢复查询可能被 EnsureUsable() 拦截，现提供文件级只读 outcome 入口并规定重试时旧对象返回需重新打开。
- 第五轮独立复审：发现并修订 4 项——终态删除 chunk 后缺少 S-03 所需的 checkpoint/容量摘要；Ready 确认没有冻结输入且零 chunk 的状态路径未定义；可重试 Finalize 失败未释放 attempt token；receipt 的终态摘要与恢复查询字段未对齐。现补全终态元数据、Ready 摘要、token 释放及相应验收。
- 第六轮独立复审：发现并修订 2 项——P0 容量放行所需的单块/session/Finalize 工作集限额不应留到阶段 D；sidecar 写入与 token 检查/续租若分开执行会有撤销竞态。现前移限额并规定同一短事务内校验、续租、写映射；补充空导入 Undo/Redo 验收。
- 第七轮独立复审：发现并修订 1 项——私有 Finalize 期间发起导入的公开对象可能开始普通编辑并在模型发布时丢失；现规定领取 attempt 前确认无活动编辑、期间设置正式写入屏障，并对失败解除/故障状态补充验收。
- 第八轮独立复审：发现并修订 1 项——只发布版本、Undo 栈和派生索引会遗漏表/记录映射及 RecordId 分配器，且私有候选表对象可能仍绑定候选连接。现要求完整内存代际切换、公开连接重绑定与后续创建/Undo/Redo 身份验收。
- 第九轮独立复审：发现并修订 1 项——总览及阶段 D 把 P0 必需的 Ready/Failed/Aborted 状态转换和容量限额表述成 P1 待实现内容。现明确 P0 阶段 B/C 交付这些基础合同，阶段 D 仅补恢复诊断、取消/清理策略和扩展规模验证。
- 第十轮独立复审：发现并修订 1 项——总览及阶段 D 仍把安全取消和失败重试写成 P1 补充项，与 P0 的 T01 及 Finalize 失败路径冲突。现明确 B/C 已交付安全取消和失败重试，D 只完善恢复诊断、残留清理和扩展场景性能。
- 第十一轮独立复审：重新按 S-01 至 S-06、T01 至 T09 核对 P0/P1 阶段依赖、活动/终态状态机、并发写入与旧句柄、提交结果判定、Open/Upgrade/Repair 只读边界、容量门槛和迁移回滚；当时未发现新问题。本轮仅做静态审查，未执行构建、脚本或测试。

### 全局一致性复审（2026-10-19）

- 将需求、公开接口和各实施阶段统一为一条跨模块代际规则：正式写入由持久 storage generation 保护；session 同时记录业务 base version 和 base generation；读取在同一快照核对 generation；Finalize 在最终写事务校验并推进 generation；已发布模型、旧句柄、receipt、迁移和阶段 B/C 验收均按同一代际处理。
- 修订旧 session 升级兼容路径：保留 checkpoint，只有经有界兼容性验证的旧请求才能绑定升级后代际；否则显式升级事务将其标为 Failed，要求 Abort/新建 session，避免升级后错误提交旧请求。
- 按 S-01 至 S-06、T01 至 T09、状态转移、事务/回滚、只读 Open、跨连接并发及 P0 放行条件再次交叉检查；未发现剩余的文档内冲突。本轮只做静态审查，未执行构建、脚本或测试。

### 本次开发前审查修订（2026-10-19）

- 初审修订 3 项：明确 Append 幂等窗口及 Ready 后迟到重试的结果；把普通 Open 不写入和显式 Upgrade 的 T07 子集前移至阶段 B/P0 放行；提交后重载在同一新快照核对 receipt 的 committed generation。
- 第一轮复审修订 1 项：旧 cursor 固定公开连接读快照会遮蔽新 generation；改为每次 Next 在新快照核对代际，并要求发布后释放模型重载快照。
- 第二轮复审修订 1 项：固定快照 cursor 与旧 Record 句柄过期规则矛盾；统一为代际变化时 Next 报过期并终止，查询续位点使用完整排序键加 RecordId。
- 第三轮复审修订 1 项：Append 若先检查“下一个序号”会错误拒绝同序号重试；改为规范化编码和摘要比对之后，仅对新增 chunk 检查序号与配额。
- 第四轮独立复审：按事务发布、状态转移、跨连接代际、P0 放行、查询 cursor 和 T01–T09 验收交叉核对；未发现新的文档内冲突。本轮只做静态审查，未执行脚本、构建或测试。

### 已废止的旧入口兼容复审（2026-10-19）

- 初审修订旧 ExecuteImport 与旧 Finalize 的有界 Ready 桥接、持久输入核对、rollbackOnError 自动清理、persistCheckpoints 的既有 SQLite 行为和有效 commit name 规范化。
- 第一轮复审发现并修订终态查询顺序：旧 Finalize 重试必须先查 outcome，再查活动 session，不能在活动行已删除后直接报 NotFound；终态需保留 session_name 以规范化空名称。
- 第二轮复审发现并修订新 Finalize 请求缺少期望输入摘要的问题：Ready 返回冻结 checkpoint/摘要，Finalize 带入并在提交前及 receipt 重试时比较，旧桥接从持久 Ready 信息构造期望值。
- 第三轮复审发现并修订失去 session id 后无法发现遗留会话的问题：增加只读、有界 keyset 分页的活动会话枚举，并明确并发新建/终止时的可见性与复查规则。
- 第四轮复审发现并修订自动 Abort 的提交后歧义：COMMIT 后异常不得假称活动 session 仍存在，须先独立查询 tombstone；只有证明回滚且活动行存在时报告清理失败，无法判定时返回 OutcomeUnknown。
- 第五轮复审发现并修订自动 Abort 与其他调用方 Finalize 竞争的结果：若查询到 Finalized receipt，必须返回明确 durable outcome，不能把已提交结果报告为普通 Append 失败。
- 第六轮独立复审：对照新旧入口、活动/终态、名称与摘要幂等、崩溃恢复、阶段 B/C 及 T01–T09 再次核对；未发现新的文档内冲突。本轮只做静态审查，未执行脚本、构建或测试。

### Ready 与 checkpoint 复审（2026-10-19）

- Ready 在活动 session 持久冻结 checkpoint/输入摘要；响应丢失后，重复 Ready 和只读 Recovery 均可取回同一结果。
- Append 对任意已存在 chunk 的合法幂等重试均返回当前最新 checkpoint 与 `alreadyApplied`，不重复推进摘要链；新增事务提交或确认旧 chunk 已持久存在后才同步返回值。
- 按 Begin/Append/Ready/Finalize 响应丢失路径、阶段 B/C 及 T01–T09 静态复核，以上合同与唯一新接口一致。

### 无历史数据的接口切换复审（2026-10-19）

- 按用户确认，删除旧导入接口、旧 payload 序列化和会话迁移路径；新库由显式 Create 建立当前 schema，旧格式仅可只读检查且 Open 返回不支持。同步更新阶段、测试、风险和下一步。
- 独立复审关注 Ready 响应丢失、Append 重试的最新 checkpoint、终态查询与 Create/Open 边界；这些合同均由唯一新接口承载。
- 再次逐项核对第 1–13 节的公开接口、状态机、原子提交、只读打开、阶段退出条件、测试和风险；修正恢复函数名及索引格式表述后，未发现新的正文内冲突。本轮仅做静态审查，未执行脚本、构建或测试。

### 再次开发前审查（2026-10-19）

- 第一轮发现并修订四项：明确 `CreateFileDatabase` 排他新建与 `OpenFileDatabase` 只打开已有文件的边界；删除选项仅限导入接口，不影响普通 Batch；超大 chunk 在有界增量编码期间拒绝；不可重试 Finalize 的 Failed 转换必须核对 attempt token/state/outcome，避免覆盖并发终态。
- 第二轮发现并修订两项：Append 幂等重试对已存 chunk 校验并逐字节比对，不能仅凭摘要认定相同；Create(ReadOnly) 明确拒绝，Open 的各模式均不得隐式创建。相应测试映射已更新。
- 第三轮按导入状态机、排他新建、只读打开、事务回滚、旧接口删除范围及 T01–T09 再次静态核对，未发现新的正文内冲突。未执行脚本、构建或测试。

### 本轮开发前审查（2026-10-19）

- 第一轮发现并修订 Begin 响应丢失后的身份缺口：活动列表无法定位已终结的会话。新增调用方预存的 opaque request id、跨活动与终态的持久唯一映射、按键只读查询及重复 Begin 选项冲突规则；同步更新阶段 A/B、T05、新建格式和 R-13。
- 第二轮核对 Append 与 Resume：重试返回须以逐字节相同为准；Resume 在追加态须只读检查正式 generation，Ready 态保持冻结输入并按 attempt token 规则恢复；相关验收已补齐。
- 第三轮按 S-01 至 S-06、T01 至 T09、接口删除范围、事务原子性、只读打开、终态归档与 P0 放行条件再次静态复核，未发现新的正文内冲突。未执行脚本、构建或测试。

### 新一轮开发前审查（2026-10-19）

- 第一轮发现并修订两项：现有公开模式为 Normal/NoHistory/ReadOnly，修正方案中不存在的 ReadWrite；import reader 若跨 Next 持有读事务，会钉住 WAL 且可能读取并发追加的移动尾部，现固定打开时 checkpoint 并改为逐块短快照及终态/token 校验。
- 第二轮发现并修订重复 Begin 的默认配额漂移问题：同 request id 比较调用方显式选项，实际生效配额在首次 Begin 时冻结；新增 reader 并发及默认配额变化验收。
- 第三轮对公开模式、Begin 身份、reader 生命周期、Finalize fencing、终态归档及测试映射再次静态复核，未发现新的正文内冲突。未执行脚本、构建或测试。

### 本次开发前审查（2026-10-19）

- 第一轮对照 ISCDatabase/ISCSchema 和 SQLite 实现发现正式写入口遗漏：MoveHistory、CreateTable/DeleteTable、无活动编辑的 RenameTable、列增改删及约束/索引增删可独立影响持久状态；已列入 generation 规则、阶段 B、测试与风险。
- 第二轮发现活动编辑中的部分 schema 写入先独立提交、Rollback 再写 journal 补偿，与跨连接回滚和旧对象保护矛盾；要求活动编辑 schema 保持候选态并在 Commit 同事务落盘，旧对象 Rollback 不向新代际补偿写回。
- 第三轮发现正式写入成功后若发起对象不更新自身 generation，会立即自过期；补充受锁发布、句柄失效、已提交需重开及 OutcomeUnknown 合同。
- 第四轮按正式写入口、编辑回滚、导入 Finalize、读视图、P0 阶段退出条件与验收项再次静态复核，未发现新的正文内冲突。未执行脚本、构建或测试。

### 打开路径专项复审（2026-10-19）

- 第一轮对照构造/析构调用链发现 Open 无写合同仍有漏洞：缺 metadata/schema_version 时构造会补写，普通对象析构会写 clean_shutdown；已要求这些写入退出 Open/无写关闭路径，并加入 T07。
- 第二轮发现 Backend 普通连接携带创建语义，存在性检查后文件消失仍可能被新建；同时显式 Create 失败后的半成品清理缺少归属规则。已明确 Backend Open 无创建语义、Create 仅清理可证明由本次排他创建的文件，失败可恢复。
- 第三轮发现 Open 的 metadata、records、journal 初始装载未固定同一快照，部分 Step 错误可能静默结束；已要求单一短只读快照、必需读错误传播和并发装载验收。
- 第四轮补充需要持久恢复的文件状态：Open/Inspect 应报告 needs-recovery，恢复必须显式执行，不由底层连接打开或首个查询触发。
- 第五轮按 Open/Create、无写关闭、初始快照、generation、schema 回滚、阶段 B 与 T07 再次静态复核，未发现新的正文内冲突。未执行脚本、构建或测试。

### 显式文件恢复与旧格式入口复审（2026-10-19）

- 第一轮发现并修订两项：需持久恢复的文件无法通过 Open 取得对象句柄，原方案却将显式 Repair 留到阶段 F，已将文件级 Inspect/Recover 和 T07 对应验收前移至阶段 B；现存 5→8 升级注册路径可能让显式 Upgrade 绕过旧格式不支持边界，已要求升级预检拒绝并停用或隔离旧路径。
- 第二轮检查恢复预检的可实现性：若原文件因待恢复状态无法只读辨认 schema，先在排他访问下对私有副本恢复并核对格式，原文件仍须保持不变；补入无法判定时的返回条件。再按 Open/Create/Upgrade/Recover、阶段 B/F、T07 和风险映射静态复核，未发现新的正文内冲突。未执行脚本、构建或测试。

### 读连接与 WAL 边界复审（2026-10-19）

- 第一轮对照现有 `SqliteDb` 的 `READWRITE | CREATE` 打开方式与 SQLite WAL 文件规则，发现仅做 Open 前只读预检不能保证普通打开和只读查询全过程无写；补入读连接及附属文件的无写要求、显式写连接延迟建立、同一文件身份/锁协议，并扩充 T07 与 R-18。
- 第二轮复核文件级恢复副本：`-shm` 是可重建的 WAL 索引，不应作为持久文件集的真相复制；改为在排他访问下复制主库和持久 journal/WAL，并在私有位置重建索引、核对格式。按打开、只读查询、显式写入、文件级恢复、旧格式拒绝及阶段 B/F 再次交叉检查，未发现新的正文内冲突。未执行脚本、构建或测试。

### 读写连接切换复审（2026-10-19）

- 第一轮对照 `SqliteDatabase` 的单个 `SqliteDb db_` 和大量直接使用该成员的写语句/事务，发现上轮要求的无写读连接若直接替换构造连接，会使 Normal/NoHistory 的显式写入失效；已要求 Backend 内收口读写连接选择，每个写事务的语句与事务归同一写连接，并增加阶段 B、测试及 R-19 约束。
- 第二轮按 Open 无写、首次 Commit/schema/导入写入、ReadOnly 拒写、读快照释放、路径替换与 generation 发布顺序静态复核；未发现新的正文内冲突。本轮未运行脚本、构建或测试。

### 并发连接隔离复审（2026-10-19）

- 第一轮发现上轮“Backend 选择读写连接”的合同仍允许以可变的对象级当前连接实现；此时并发公开读可能借用未提交写事务的连接。已要求读写调用各自固定连接/事务上下文、私有候选独占 attempt，并补入并发读回归与 R-20。
- 第二轮按普通 Commit、独立 schema、Append/Abort、Finalize/sidecar、Rollback 和 generation 发布顺序再次静态复核；未发现新的正文内冲突。未执行脚本、构建或测试。

### 本轮开发前审查修订（2026-10-19）

- 初审发现新 handle/reader/receipt、文件级 Open/Inspect/Recover 和 outcome 查询只有概念名称，且完整 vector 输入无法保证端到端有界；补充可编译 API 合同、`SCImportChunkSource` 流式输入和内存统计边界。
- 初审发现文件级恢复缺少替换中断后的可识别状态；补充同目录临时输出、文件身份校验、固定刷新/原子替换顺序、`RecoveryPending` 和不可自动宣称成功的协议。
- 初审发现 sidecar 独立事务提交后到 Finalize 之间存在崩溃窗口；补充 session/attempt/generation 归属、旧 token 隔离、恢复回收和清理失败语义，并同步 T01/T03/T07 与风险项。
- 第二轮独立复审发现 source 的 chunk 重放、单字段上限和写锁内回调风险；补充固定 chunk/source 生命周期、字段分片预算、事务外编码与短事务二次校验，并明确 Normal/NoHistory 首次写入的连接切换。
- 第二轮独立复审发现 sidecar 载体、恢复 manifest、WAL/journal 成组切换、已有连接失效及候选临时文件归属未冻结；补充 sidecar 同库事务表、外部 recovery/candidate manifest、Storage 文件 lease、旧对象 stale 和中断矩阵验收。
- 第三轮独立复审发现 chunk sequence 分配、文件身份绑定、manifest 成功/清理终态和 WAL/journal 恢复路径仍不够确定；补充 Storage 分配 next sequence、file-id 校验、manifest phase 状态机、SQLite journal-mode 分路径和 candidate cleanup-pending 语义。
- 本轮修订仅针对上一轮五项发现：补充首个 `nextSequence` 的 source 交互、终态与 manifest 的 file-id 持久化、文件级 API 签名、移除旧 vector 验收歧义，并规定 Finalize 事务同时推进 receipt 与 candidate manifest 的 `Committed` 状态。
- 本轮独立复审后再次收敛两项边界：补充最小公开类型和错误枚举；将外部 candidate manifest 从“同事务原子推进”改为 receipt-first 恢复协议，并补充 Abort 的 token fencing、tombstone 和候选清理顺序。

### Attempt token 合同复审（2026-10-19）

- 初审发现 Finalize 请求字段与后文 attempt token fencing 的归属未闭合；已冻结为 Storage 内部生成、持久化和校验的 opaque token，不加入 `SCImportFinalizeRequest`，不提供领取/回传 API。Finalize 重试只提交原请求字段，由 Storage 先查持久化 outcome，再按当前 attempt/租约状态返回结果；reader、sidecar、候选提交、Abort、Resume 和恢复 scanner 的 token fencing 均属于内部实现合同。
- 第一轮独立复审按公共头文件、Ready/Finalize/Abort/Resume 状态机、租约失效、receipt 幂等、reader/sidecar/candidate manifest 和 T03/T05/T08 验收逐项核对，未发现新的 token 语义冲突。本轮仅做静态审查，未执行构建、脚本或测试。

### 本轮开发前审查修订（2026-10-19）

- 固定 P0 候选态为同一 Storage 对象内的私有内存编辑上下文，明确候选变更只能由单一 `CommitImportSession` SQLite 事务写入正式表、journal、generation、receipt 和清理结果；未来磁盘候选文件不属于 P0。
- 补充可直接落到公开头文件的最小 source/reader/入口形状、缓冲区生命周期、并发限制和完整公共错误集合，消除“可编译合同”只有概念名称的问题。
- 明确 `Failed` 只能由 Finalize 回滚后的 Backend 私有 `MarkImportFailed` 状态事务触发；Recovery、reader、健康检查和普通读取不得隐式改变状态，调用方放弃统一使用 Abort。

### 本轮开发前审查修订（2026-10-19）

- 将 Failed 状态机收敛为唯一的 `ReadyToFinalize -> Failed` 路径；Staging/Checkpointed 的放弃或损坏保持原状态并通过 Abort 清理。
- 将 `CommitImportSession` 明确为 Core/Batch 到 Adapter/Backend 的内部桥接，不加入调用方公共导入 API。
- 将 `SCImportRequestFragment`、`SCImportChunkView` 和 `SCImportError` 到现有 `ErrorCode` 的唯一映射补入公共合同，并明确 P0 不创建 candidate manifest 或 `candidate_cleanup_pending`。

### 本轮开发前审查修订（2026-10-19）

- 补齐 `ResumeImport`、Recovery 查询和 `ListImportSessions` 的公开入口与只读/不夺取租约合同。
- 固定 `SCImportCandidateEdit` 的内部数据流，并规定 `ApplyBatchRequests` 到 `CommitImportSession`/`CommitPersistencePhase` 的边界。
- 固定 RecordId 预留策略：Append 在 token fencing 短事务中从持久分配器预留 ID，Abort/回滚不复用已预留 ID，Finalize 沿用映射提交。

### 本轮开发前审查修订（2026-10-19）

- 将 RecordId 分配改为 Finalize 最终写事务内按稳定临时键一次性分配，Append 不再猜测新增记录数量或预留正式 ID。
- 将 ActiveVersionKey 所属事务核实设为阶段 A 到阶段 C 的硬门槛；外部指针未建立恢复协议前不得宣称 P0 原子 Finalize。
- 固定 Resume 为只读，新增显式 `ReclaimImportAttempt` 承载租约失效后的 attempt/token 接管写入。
- 本轮原地修复两项合同残留：统一 `SCImportCandidateEdit`、sidecar 和 Finalize 使用临时记录键后再分配正式 `RecordId`；所有 attempt/token 接管和回收统一归属 `ReclaimImportAttempt`，`ResumeImport` 保持只读，并同步更新 T05/T08 验收。
- 修复未来 candidate manifest 清理段残留的 Resume 写入语义，统一改为 `ReclaimImportAttempt`/Abort 或内部恢复扫描器，并明确该协议不属于 P0。
