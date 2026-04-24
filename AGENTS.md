## 1. Project Overview

**Repository:** `stablecore-storage`
**Role:** 算量产品的存储平台核心库

核心目标：

* 提供**数据层 Undo / Redo 能力**
* 支撑 **Project / ChangeSet / Journal / Snapshot / Replay**
* 建立**可恢复、可升级、可回放**的数据基础设施

---

## 2. Tech Stack

* Language: **C++20**
* Build: **CMake**
* Database: **SQLite**
* Test: **GoogleTest**
* Compiler: **MSVC (VS2022)**（未来需支持跨平台）

---

## 3. Directory Structure

```
├─cmake
├─Docs
├─Examples
├─Include                # 公共接口层
├─Src
│  ├─Batch               # 批处理 / 导入
│  ├─Computed            # 计算列
│  ├─Diagnostics         # 诊断 / Debug Package
│  ├─Memory              # 内存后端（核心语义）
│  ├─Migration           # 升级 / 版本图
│  ├─Query               # 查询执行
│  └─Sqlite              # 持久化后端
├─Tests
└─Tools
```

---

## 4. Core Architecture

### 4.1 分层模型（必须遵守）

```
Core（语义/规则）
   ↓
Adapter（接口桥接）
   ↓
Backend（Memory / SQLite）
```

❗ **禁止行为：**

* 上层直接访问 SQLite 实现细节
* Query / Computed 绕过接口访问底层
* Memory 与 SQLite 行为不一致

---

### 4.2 核心模块职责

| 模块          | 职责                   |
| ----------- | -------------------- |
| Include     | 公共接口定义（唯一对外语义入口）     |
| Memory      | 真正的语义执行、Undo/Redo、事务 |
| Sqlite      | 持久化、恢复、日志            |
| Query       | 查询计划与执行              |
| Computed    | 表达式、计算列              |
| Batch       | 批量编辑、导入              |
| Migration   | 升级、兼容                |
| Diagnostics | 诊断、调试、导出             |

---

## 5. Mandatory Design Principles（强制执行）

以下不是“建议”，是**必须遵守的约束**：

---

### 5.1 单一真值源（P0）

* 正式数据只来自：

  * Project
  * ChangeSet
  * Journal
* UI / Cache / Session **不得作为真值**

❗ 禁止：

* 从缓存反推数据
* UI 状态写回数据层

---

### 5.2 显式边界（P0）

所有状态变更必须通过显式 API：

* `BeginEdit`
* `Commit`
* `Rollback`
* `Open`
* `Upgrade`
* `Finalize`
* `Abort`

❗ 禁止：

* 在 `open()` 中写数据
* 隐式触发升级
* 读操作触发写入

---

### 5.3 失败可恢复（P0）

所有写路径必须保证：

* 不产生半成功状态
* 可回滚或可恢复

必须覆盖：

* 导入中断
* 升级失败
* 回滚失败
* 删除后访问

---

### 5.4 接口一致性

* Memory / SQLite **语义必须一致**
* 不允许某后端“多行为”

---

### 5.5 分层隔离

* Query / Computed 不得访问 Backend 实现
* Backend 不包含业务语义

---

### 5.6 语义优先

* 正确性 > 性能
* 优化不得改变行为

---

### 5.7 最小惊讶原则

* 读不写
* 删除不复活
* 只读模式不修改数据

---

### 5.8 组合优于分叉

* 不复制逻辑
* Memory / SQLite 差异收敛到 Adapter

---

### 5.9 可测试

必须覆盖：

* 回滚后再写
* 删除后访问
* 导入中断
* 恢复失败

---

### 5.10 可诊断

错误必须包含：

* 原因
* 上下文
* 阶段

---

## 6. Coding Rules

### 6.1 命名规范

* 接口：`ISC*`
* 实现：`SC*`
* 成员变量：`m_`
* 常量：`k*`

示例：

* `ISCStorageService`
* `SCProjectContext`

---

### 6.2 强制规则

❗ 禁止：

* 无关重命名
* 大规模格式化
* 风格性修改
* 修改不相关模块

---

## 7. Testing Rules

* 必须使用 **GoogleTest**
* 新增行为必须补测试
* 修 bug 必须补回归测试

允许：

* 运行测试

---

## 8. Documentation Rules（强制同步）

### 8.1 全局文档（必须同步）

* Docs/文档索引.md
* Docs/当前实现状态.md
* Docs/NeedUpdateDocs.md
* Docs/CapabilityGapAssessment.md
* Docs/Roadmap.md
* Docs/V1BaselineDecisions.md
* Docs/TaskBreakdown.md
* Docs/task.md

---

### 8.2 按模块同步

| 修改内容           | 必须同步                          |
| -------------- | ----------------------------- |
| Query / 引用索引   | Query执行架构设计稿.md               |
| Computed       | ComputedTableView / Lifecycle |
| Migration / 导入 | CapabilityGap / Performance   |
| Editor         | GUI Design / Scope            |

---

## 9. Codex Execution Rules（关键）

### 9.1 允许

* 修改代码
* 修改 CMake
* 运行测试

---

### 9.2 禁止

* ❌ 编译
* ❌ 提交 Git
* ❌ 引入第三方库
* ❌ 擅自扩大修改范围

---

### 9.3 工作方式（强制）

每次任务必须按以下顺序：

1. **先分析边界**
2. **再实施代码**
3. **再补测试**
4. **最后自检**

---

## 10. Output Requirements（必须输出）

每次任务结束必须提供：

### 10.1 改动摘要

* 做了什么
* 修改了哪些模块

### 10.2 测试结果

* 覆盖范围
* 是否包含边界场景

### 10.3 风险点

* 语义风险
* 回归风险
* 未覆盖路径

### 10.4 下一步建议

* 最合理的后续任务

---

## 11. Strict Prohibitions（重点）

❗ 绝对禁止：

* 在 open 中写入数据
* 绕过接口直接访问 SQLite
* Memory / SQLite 行为不一致
* 引入隐式状态变更
* 用文档替代实现
* 改动未声明的模块

---

## 12. Working Philosophy

Codex 在本仓库中的角色是：

> **“受约束的工程执行者”，而不是“自由发挥的重构者”**

必须：

* 遵守边界
* 保持语义一致
* 优先正确性
* 小步演进

## Task Input Requirement

所有任务必须遵循 TASK_TEMPLATE.md 结构。

如果输入不完整，必须先要求补充，而不是直接实现。