# SCStorage 能力差距实现方案

> 状态：开发前方案，按 Quantity 原始需求收敛
>
> 上游依据：D:/code/StableCore/Quantity/Docs/Design/SCStorage/SCStorage能力差距需求.md
>
> 实施边界：Storage 的公开语义、只读不写、受控升级、完整回滚和 SQLite 不外泄约束仍适用。现有 `CreateFileDatabase()` 可在返回前升级受支持的 schema；底层打开/构造和读操作不得自行写入。本文只规划上游 S-01～S-06 与 T01～T08，不以未来扩展代替当前验收。
>

## 1. 范围与优先级

| 需求 | 优先级 | 本方案交付 |
| --- | --- | --- |
| S-01 磁盘分块 staging | P0 | 会话元数据、独立 chunk、原子 checkpoint、元数据恢复和逐块读取 |
| S-02 原子 Finalize | P0 | 正式数据、版本/活动指针、历史、receipt 与 staging 清理的单一提交结果 |
| S-03 有界恢复与失败归因 | P1 | 完整状态查询、按块校验、位置诊断和显式继续/清理 |
| S-04 复合身份与查询索引 | P1 | 复合约束、正式 cursor、索引检查/重建及一致性 |
| S-05 启动检查与显式修复 | P1 | 只读检查、显式事务修复及结果 receipt |
| S-06 资源与容量合同 | P1 | 完整预算、错误和三档规模基线 |

Quantity 仅在 S-01、S-02 完成且相应 P0 验收通过后解除 D07/M1 正式导入阻塞。图纸版本查询、空间索引缓存和 Replay 隔离恢复的接入，等待 S-04、S-05 完成。S-03、S-06 的 P1 全集不作为 D07/M1 的新增前置门槛；S-01 自身要求的单次 Append 有界和 S-02 自身要求的失败原子性仍须在 P0 验证。

## 2. 已有实现与修改边界

- Include/SCBatch.h 的 SCImportStagingArea.chunks 持有完整会话；Src/Sqlite/SqliteImplDatabase_Init.cpp 的 import_sessions.payload 保存完整序列化会话。现有 Append 重写整份 payload，Recovery 反序列化全部 payload。
- Src/Batch/SCBatchOperations.cpp 当前先调用数据库 Commit，再调用 FinalizeImportSession 清理 checkpoint；清理失败可能表现为正式数据已提交而接口返回普通失败。
- 普通事务、约束、Undo/Redo、迁移、备份、复合索引和 Query 基础已存在。复用这些能力，不重做无关模块。
- Core/Batch 负责导入顺序、确认和失败归因；Adapter 承接公共接口；SQLite Backend 负责持久化与事务。Quantity 不直接访问 SQLite。

原需求建议新增不破坏现有接口的 opaque session/chunk reader，或扩展现有恢复接口。因此保留现有导入接口与类型，增量提供面向大规模 CAD 导入的轻量会话和逐块读取路径；Quantity 的大规模导入调用点迁往该路径。保留旧 `AppendImportChunk` 签名时，其实现同样不得向 `SCImportStagingArea.chunks` 累积历史 chunk，须满足 S-01 的单次追加有界要求；旧 `ExecuteImport` 接收调用方一次性构造的完整请求 vector，不能以该整批入口声称端到端有界。所有仍可调用的 Finalize 入口都必须按持久 session 逐块读取并使用同一原子提交语义，不能依赖传入的完整 `session.chunks` 或保留可到达的“两阶段成功”路径。

## 3. P0：S-01 磁盘分块 staging

### 3.1 数据和接口

Backend 将会话元数据、chunk 元数据和 chunk 负载分开保存。会话记录保留 session id、状态、base version、最后完整 checkpoint 与累计计数；chunk 记录保留 session id、稳定序号、字节数、摘要和完整状态。具体 SQLite 表和编码不进入公开 API。

新增轻量 session 句柄及逐块追加/读取能力。单次追加只接收受限 chunk，不要求调用方持有完整会话 vector；成功后调用方可释放该 chunk。公开 reader 每次最多交付一个受限 chunk，调用方通过 Storage 接口获取它，不访问内部表。旧 `SCImportStagingArea` 类型保持源码兼容，但 Append 和默认 Recovery 返回的 `chunks` 不承载历史负载；需要读取负载的调用方使用逐块 reader。旧式整批输入的调用方已持有的完整请求 vector 不计入 `AppendImportChunk` 自身的内存峰值。

单次 Append 在一个事务中写新增 chunk、元数据、摘要和 checkpoint。失败或崩溃只留下此前完整 checkpoint；同一 chunk 的重试不得重复计数或改写历史 chunk。Append 的工作量与本次新增负载成正比，进程内附加内存受单批预算限制。损坏 chunk 须报告其序号和摘要错误，不能被当成正常结束。

默认恢复查询只读取会话元数据和 checkpoint，不装入完整 staging。reader 按序读取并校验 chunk；正式记录查询不包含 staging。Abort 在任意已提交 checkpoint 状态下显式、幂等地清理会话及 chunk，失败时不得留下看似清理成功的状态。P1 再补完整状态诊断、位置归因和可取消恢复流程。

### 3.2 数据库创建与升级

新建库由 `CreateFileDatabase()` 建立分块结构。若已有库需要启用新路径，先核查其 schema 和导入会话数据，再将所需结构或数据转换纳入受控升级流程；不得假定旧 payload 或旧会话不存在。保留现有 `CreateFileDatabase()` 在非只读模式下、返回句柄前执行受支持升级的公开行为，也可由调用方使用 `UpgradeFileDatabase()`；升级失败不得留下部分转换结果。`ReadOnly` 工厂调用、恢复元数据查询和 reader 不写数据库，也不触发升级。数据库对象的底层打开/构造和无编辑关闭不借升级名义补写；新建、升级与读取的写入边界须在 P0 新路径接入前厘清。

## 4. P0：S-02 原子 Finalize

Batch/Core 从已确认的 session 按 chunk 顺序读取并应用输入，提交前的候选变更不向普通查询发布。先核对 session、确认标志和持久当前版本；核对通过后，在持久会话状态中冻结最后完整 checkpoint，并阻止后续 Append。调用方的 `confirmed` 标志不能代替这一状态转换。Finalize 只应用该 checkpoint 覆盖的 chunk。最终写事务取得写锁后，从持久状态再次核对 session 状态、冻结的 checkpoint 和当前正式版本。并发 Append 或 Abort 不得使未应用的 chunk 被清理，也不得让已取消会话提交。

在同一 Storage 持久事务中完成正式数据、版本记录、Undo/Redo 所需历史、活动版本指针、唯一 commit/version receipt 和 staging 清理。复用已有普通 Commit 的约束与持久化规则，避免复制一套独立的记录写库逻辑。若 ActiveVersionKey 位于 Storage 事务之外，须先明确其所属系统和原子提交边界；在此问题解决前不能宣称 S-02 已完成，也不能解除正式导入阻塞。

提交前任一步失败时，候选变更、正式数据、历史和 staging 清理一起回滚，旧版本与活动指针保持不变。若 SQLite COMMIT 已尝试而返回或内存发布结果不确定，须通过同事务持久 receipt 判定：已提交则返回明确 durable-commit/需恢复状态，已证明回滚才返回未提交失败，其余返回结果未知。不得把已提交的正式数据报告为普通清理失败。提交后内存视图若无法一致发布，应要求重新打开，不暴露部分更新对象。

成功 receipt 至少标识 session、commit 和版本，能够在重试或恢复时查到同一个持久结果。Undo/Redo 只操作已提交历史，不重新读取 staging 或外部文件。Finalize 对空输入、取消、并发 Abort 和失败清理的具体行为须与上述单一结果边界一致；实现阶段按现有存储状态机补测试，不额外发明一套跨文件候选协议。

## 5. P1 工作线

### S-03 恢复与失败归因

恢复查询返回状态、base version、最后完整 chunk、总数及字节预算；按块读取和校验，不构造完整 SCImportStagingArea。诊断定位到可确定的 chunk、表、字段或操作；不能确定时明确说明。区分 Staging、Checkpointed、ReadyToFinalize、Finalized、Aborted 和 Failed。继续、放弃、清理均由调用方显式发起，取消不改正式数据，也不自动切活动版本。

### S-04 复合查询与索引

在既有 SCIndexDef、Query planner/executor 和 ISCRecordCursor 上扩展来源身份所需的复合唯一约束、等值/范围查询、排序和 null 规则；覆盖 `VersionKey`、`RecordType`、`ExchangeId`、`Sequence` 及完整来源身份。正式 cursor 由存储层按索引逐批取数，不先在 C++ 内存镜像中全量过滤、排序和分页。索引缺失或损坏时返回可诊断状态，不静默宣称索引命中；索引重建失败保留源数据。复合查询索引和引用索引在插入、更新、删除、恢复及 Undo/Redo 后保持一致，并提供可诊断的检查与重建结果。查询路径不得要求 Quantity 全表扫描；具体复合键宽度和容量阈值由来源字段清单确定。

### S-05 只读检查与显式修复

Open(ReadOnly) 与检查只读取状态，不创建 staging、不升级 schema、不重建索引或写诊断。启动检查报告 schema、数据、索引和未完成 import session。legacy 索引补建、索引重建及数据修复是独立显式写操作，具有事务、失败回滚和结果 receipt。未完成会话只提示继续或清理，不由启动过程自动 Finalize。底层打开/构造的无写边界已在 P0 接入前落实；本阶段补齐全面检查与显式修复。

### S-06 资源与容量

定义单 chunk、会话总量、chunk 数及记录数上限，明确 Append、Finalize、Recovery 和索引重建的内存峰值口径及空间不足、损坏、摘要不符、超限错误。采集 10 万、100 万和产品目标上限下的时间、峰值内存、磁盘空间和恢复基线，并规定取消、崩溃后的残留清理策略。指标由目标设备预算确认，不在本方案虚构阈值。

## 6. 实施顺序与退出条件

| 阶段 | 交付与退出 |
| --- | --- |
| A. 事务边界核对 | 确认 ActiveVersionKey 的持久位置和现有调用点；若活动指针不在同一事务边界，先解决 S-02 的原子性设计。 |
| B. S-01 | 增量接口和分块持久化可用；新旧 Append 签名、checkpoint、默认恢复和 reader 满足单批有界，旧接口仍可调用；盘点并迁移依赖 `session.chunks` 的调用点；底层打开/构造及新增读入口不写，工厂升级仍受控执行；通过 T01、T04、T05 的元数据/单块子集及 T07 的只读无写子集。 |
| C. S-02 | 所有可到达的导入 Finalize 使用同一持久结果边界；正式数据、活动指针、历史、receipt 和清理一致；通过 T02、T03 后解除 D07/M1 正式导入阻塞。 |
| D. S-03～S-06 | 按上游 P1 顺序补齐恢复诊断、查询索引、只读检查/显式修复和完整容量合同；通过 T05～T08 对应全集。 |

## 7. 测试映射

| 验收 | 必测行为 |
| --- | --- |
| T01 | Append 写入、checkpoint 更新、取消或崩溃时，旧 ActiveVersionKey 不变，恢复只承认完整 chunk。 |
| T02 | Finalize 成功时正式数据、版本/活动指针、Undo 信息、receipt 和 staging 清理一致；并发 Append/Abort 或正式版本变化不得提交不完整输入；Undo/Redo 不读 staging。 |
| T03 | 记录写入、receipt、清理和 COMMIT 各处故障：提交前完整回滚；提交结果不确定时返回可判定的持久结果或结果未知。 |
| T04 | 10 万、100 万实体连续追加，新旧 Append 签名的内部附加内存不随历史 payload 线性增长，单次工作量与新增负载相符；旧整批输入的调用方持有内存单独统计。 |
| T05 | 默认 Recovery 仅取元数据，reader 一次最多读取并校验一个 chunk；P1 再验收完整状态与取消。 |
| T06 | 复合键查询、排序/null、索引缺失与重建、增删改及 Undo/Redo 后结果一致；大结果集的正式 cursor 不预先物化全量结果。 |
| T07 | ReadOnly 打开和只读检查不写入、不升级、不重建索引；显式修复失败回滚。 |
| T08 | 损坏 chunk、摘要不符、超限及磁盘不足返回稳定错误，P1 诊断给出可确定的位置。 |

新增行为及缺陷修复使用 GoogleTest 覆盖；本方案审查不执行测试、脚本或构建。容量数字另由实施阶段的可重复基线报告提供。

## 8. 风险与待确认事项

1. ActiveVersionKey 若不在同一 Storage 事务，S-02 无法仅靠 Storage 声称原子成功；阶段 A 必须先确认。
2. 旧 `SCImportStagingArea.chunks` 不再镜像已追加的完整会话，虽保持源码兼容，仍会改变依赖该字段的调用方行为；实施前须盘点调用点并验证逐块 reader 的替代路径。旧 `ExecuteImport` 整批输入也可能占用整会话内存；Quantity 必须使用分块输入，旧 Append/Finalize 则分别遵守单批有界和原子提交要求。
3. S-06 的目标设备与数值预算尚未给出；P1 容量结论须待实测，P0 仍需证明 Append 的单批有界性和 Finalize 的正确性。
