# Storage 备份与基线

> 状态：已系统化
> 适用范围：Storage 层 `CreateBackupCopy`、`ResetHistoryBaseline`、`baselineVersion`
> 说明：本文将原 `Docs/CreateBackupCopy Level 2 完整开发方案.md` 的内容收敛为长期维护的系统文档；原专题方案已作为一次性开发材料归档。

## 1. 文档目标

本文档只定义 Storage 层与备份、基线相关的稳定语义，不描述产品层工程目录、文件替换流程或 UI 交互。

本文要回答的核心问题是：

- `CreateBackupCopy` 在当前版本里到底做什么
- `ResetHistoryBaseline` 与备份副本的关系是什么
- `baselineVersion` 如何跟踪和持久化
- Memory / SQLite 两个后端如何保持一致语义

## 2. 能力边界

### 2.1 对外能力

- `CreateBackupCopy(...)`
  - 生成当前数据库的一致性副本
  - 仅文件后端支持
  - Memory 后端必须返回 `SC_E_NOTIMPL`
- `ResetHistoryBaseline(...)`
  - 重置当前基线
  - 清空可见的 Journal History
  - 更新 `baselineVersion`

### 2.2 不包含的能力

- 不做策略型历史压缩
- 不做恢复日志大小限制
- 不做 Replay Log
- 不做 Import 恢复链路
- 不在源库上直接清理或直接 VACUUM
- 不把产品层文件替换策略写进 Storage ABI

## 3. 公共 ABI

当前公开接口使用 Level 2 语义：

- `SCBackupOptions`
  - `preserveJournalHistory`
  - `vacuumTarget`
  - `validateTarget`
  - `overwriteExisting`
- `SCBackupResult`
  - `removedJournalTransactionCount`
  - `removedJournalEntryCount`
  - `outputFileSizeBytes`

### 3.1 选项含义

- `preserveJournalHistory = true`
  - 保留目标副本中的 Journal History
- `preserveJournalHistory = false`
  - 目标副本清空 Journal History
  - `baselineVersion` 跟随当前版本推进
- `vacuumTarget = true`
  - 对目标副本执行压缩整理
- `validateTarget = true`
  - 对目标副本做打开/校验检查
- `overwriteExisting = false`
  - 当目标已存在时返回 `SC_E_FILE_EXISTS`

## 4. 实现规则

### 4.1 SQLite 后端

SQLite 的实现流程为：

1. 在目标同目录创建临时文件
2. 将当前数据库复制到临时文件
3. 按需清理 Journal History
4. 按需执行 `VACUUM`
5. 按需执行校验
6. 原子替换到目标路径

失败语义要求：

- 不能污染源库
- 不能留下半成功目标文件
- 发生校验失败时返回 `SC_E_VALIDATION_FAILED`
- 发生 I/O 失败时返回 `SC_E_IO_ERROR`

### 4.2 Memory 后端

Memory 后端不提供文件型备份落点，因此：

- `CreateBackupCopy(...)` 返回 `SC_E_NOTIMPL`
- `ResetHistoryBaseline(...)` 仍然可用
- `baselineVersion` 仍然需要保持一致语义

### 4.3 基线语义

- `baselineVersion` 表示最近一次基线重置后所处的版本
- 它不是当前版本快照的别名
- 它在 Memory / SQLite 两端都必须可查询、可持久化、可回放

## 5. 当前实现状态

- `CreateBackupCopy` 已按 Level 2 语义落地到 SQLite
- `ResetHistoryBaseline` 已与 Journal History 清理路径对齐
- `baselineVersion` 已从当前版本中独立出来并持久化
- `Memory` 后端保持 `SC_E_NOTIMPL`
- `SC_E_FILE_EXISTS`、`SC_E_VALIDATION_FAILED`、`SC_E_IO_ERROR` 已进入错误码表

## 6. 已知后续工作

- 如果未来需要策略型备份，需要重新定义语义，而不是在现有 Level 2 上继续堆字段
- 如果未来需要跨平台文件后端备份，需要另行设计非 SQLite 路径
- 当前文档不再保留专题方案内容，专题细节以本页为准
