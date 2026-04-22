# Task 4 结果确认单 v2

## 一、Diagnostics 的默认包含范围
- 默认包含：`Backend`、`SchemaVersion`、诊断条目摘要。
- 默认不包含：原始 `Replay Assets`、原始日志全文、工程数据全量转储、用户配置明文、系统配置明文。
- 摘要内容：`BuildStorageHealthReport` 生成的诊断概览与诊断条目。
- 原始内容：当前默认不导出。

## 二、大小上限处理规则
- 当前实现只有单一硬上限，没有独立 soft limit。
- 超过上限：直接拒绝写入并返回失败，不裁剪为静默成功。
- 默认策略：`Fail`。
- 分卷：当前未启用。
- 摘要化：当前未作为自动降级策略启用。

## 三、脱敏默认规则
- 默认开启：路径脱敏、用户名脱敏、敏感文本脱敏、Replay 载荷脱敏。
- 可关闭：当前普通导出策略下可由调用方显式调整。
- 强制脱敏：当前实现未引入“绝对不可关闭”的独立强制项，但默认问题包策略是全开。

## 四、流式写出失败规则
- 临时文件：导出开始时即创建 `QSaveFile` 临时目标。
- 失败删除：失败或取消时不提交目标文件，临时内容由 `QSaveFile` 自行清理。
- 取消副作用：取消导出不应留下可见成品文件。
- 成功替换：仅在全部写出完成并 `commit()` 成功后原子替换目标文件。

## 五、默认 AssetSelection
- 普通导出：默认只选择 `Project`。
- 问题包导出：默认选择 `Project + Diagnostics`。
- 默认不选：`System Config`、`User Config`、`Replay Journal`、`Replay Snapshot`、`Replay Session`、`Log`。

## 六、与 Replay 的边界确认
- 当前问题包默认不带 `Replay`。
- 后续若允许勾选 `Replay`，只能作为可选附加资产。
- 该扩展不改变当前默认边界：问题包默认仍不包含全部 `Replay Assets`。

