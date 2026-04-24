# 需更新文档清单

更新条件：
- 当前文档比代码落后。
- 当前文档内容已经失效或会误导读者。
- 如果文档描述的设计本身是对的，但代码尚未实现，则优先更新代码，不把它算作“需要改文档”的项。

## 需更新文档

| 文档 | 触发原因 | 主要问题 | 优先级 |
|---|---|---|---|
| [Query 执行架构设计稿](Quantity/DevGuide/Query执行架构设计稿.md) | 文档与代码事实不一致 | 文档把查询与引用索引的设计语义写得比当前实现更强；代码实际仍是枚举内存镜像后在 C++ 里过滤和排序 | 高 |
| [Computed Table View And Startup](ComputedTableViewAndStartup.md) | 文档内容部分失效 | 文档把启动检查、索引物化、恢复链路写得过于闭环；代码里相关函数存在，但未接入主启动路径 | 高 |
| [Capability Gap Assessment](CapabilityGapAssessment.md) | 文档状态落后 | 文档没有完整反映当前代码已经实现的内容，也没有把最新的实现边界写清楚 | 高 |
| [Roadmap](Roadmap.md) | 文档表述偏乐观 | 对查询、索引、启动恢复、产品化收口的描述比当前代码成熟度更前置 | 中 |
| [V1 Baseline Decisions](V1BaselineDecisions.md) | 文档边界需要收敛 | SQLite 的角色、查询与索引边界、平台级能力和持久化层职责需要重新表述得更准确 | 中 |
| [Performance Smoke Baseline](PerformanceSmokeBaseline.md) | 文档容易误读 | 该文档是 smoke baseline，不应被读成“索引驱动查询或 SQLite 下推已经完成”的证据 | 中 |

## 不放入本清单的情况

- 文档设计是对的，但代码还没实现。
- 这种情况应优先更新代码，而不是改文档降级描述。
- 典型例子：启动检查未接入、索引物化未挂载、查询未真正下推、引用索引未做增量维护。
