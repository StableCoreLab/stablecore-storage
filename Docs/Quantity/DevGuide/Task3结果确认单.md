# Task 3 结果确认单

## 一、Import Session 状态机

- `Begin`
  - 允许：创建 session、进入 staging、初始化 checkpoint 元数据。
- `Appending`
  - 允许：追加 chunk、更新 staging payload、刷新 checkpoint。
- `Checkpointed`
  - 允许：继续 append、读取 recovery state、进入 finalize 准备态。
- `Finalizable`
  - 允许：执行 finalize、读取 recovery state、放弃 session。
- `Finalized`
  - 允许：查询结果，不允许继续 append。
- `Aborted`
  - 允许：读取失败原因，不允许继续 append 或 finalize。
- `Failed`
  - 允许：读取 recovery state，不允许继续 append，必要时只能 abort / 重新开始。

## 二、chunk 幂等性规则

1. 同一 `chunkId` 重复 append 视为重复提交，不应再次影响 live 数据。
2. 同一 session 下 `chunkIndex` 必须唯一。
3. 恢复后重复执行已提交 chunk，不应产生重复写入；恢复流程应从最后一个已确认 checkpoint 继续。

## 三、finalize 原子性规则

1. finalize 必须表现为“要么全部正式生效，要么全部不生效”。
2. hidden staging 记录在 finalize 成功后删除。
3. finalize 失败后，recovery state 必须保留最后一次可恢复 checkpoint，并标记可恢复或失败原因。

## 四、abort 语义

1. abort 删除 staging / checkpoint。
2. abort 后不允许恢复。
3. abort 不影响已正式提交的数据。

## 五、Undo / Redo / Replay 绑定确认

1. append chunk 不生成 `ChangeSet`。
2. append chunk 不进入 `Undo/Redo`。
3. append chunk 不进入 `Replay` 主链。
4. finalize 是唯一正式提交边界。

## 六、当前仓库内的最小恢复流程

1. 打开数据库。
2. 读取 `ImportRecoveryState`。
3. 如果 session 可恢复，则从最后一个 checkpoint 继续 append。
4. 重新执行未 finalize 的 chunk。
5. 确认后执行 finalize。
6. finalize 成功后清理 staging 记录并返回结果。

