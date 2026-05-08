# AGENTS.md

## Fast Path

### Repository

- Repository: `stablecore-storage`
- Role: quantity/takeoff system 的核心 SQLite 存储基础设施
- Focus: persistence, undo/redo, transaction semantics, changeset/journal/snapshot/replay, recoverable editing, upgrade/migration

### Hard Constraints

- `open()` must not mutate data
- reads must not write
- upgrades must be explicit
- rollback must fully restore state
- deleted records must never revive
- undo / redo must preserve identity consistency
- transactions must not leave partial success states
- SQLite implementation details must not leak into public APIs

### Architecture

```text
Core (semantic rules)
    -> Adapter (public interface bridge)
    -> Backend (SQLite)
```

- Upper layers must NOT access SQLite directly
- Query / Computed modules must NOT bypass public interfaces
- Backend modules must NOT contain business semantics

### Execution Boundary

Allowed:

- modify source code
- modify tests
- modify CMake
- update documentation
- static reasoning / analysis

Forbidden:

- compile
- run executables
- run tests
- execute scripts
- invoke build systems
- modify generated build artifacts

### Change Strategy

- Keep changes strictly localized
- Prefer extension over rewrite
- Prefer local fixes over global refactors
- Do not refactor unrelated modules
- Do not rename symbols without necessity
- Do not perform repository-wide formatting
- Do not introduce speculative abstractions

### Required Workflow

1. Analyze scope and boundaries
2. Understand storage semantic constraints
3. Implement minimal localized changes
4. Add or update tests
5. Perform static self-review
6. Summarize risks and remaining gaps

### Required Output

- Change Summary
- Test Coverage
- Risk Assessment
- Suggested Next Steps

### Priority Order

```text
Correctness
    > Recoverability
        > Storage Semantic Stability
            > Maintainability
                > Performance
```

## Read By Task Type

只按当前任务读取需要的文档，不要默认全部展开。

| Task Type | Read |
| --- | --- |
| 架构调整、分层边界、接口归属、语义约束判断 | `Docs/AI/ArchitectureAndSemantics.md` |
| 能否执行某类操作、修改范围边界、AI 工作方式 | `Docs/AI/ExecutionBoundaryAndScope.md` |
| 测试补充、回归覆盖、任务收尾输出格式 | `Docs/AI/TestingWorkflowAndOutput.md` |
| 目录职责、命名规则、常见违规模式、参考文档入口 | `Docs/AI/ModuleMapAndReference.md` |

## Loading Rule

- 先读本文件
- 只在任务需要时再读对应 `Docs/AI/*.md`
- 若任务跨多个维度，按最少必要原则补充读取
- 若与仓库现状冲突，以“语义正确性、可恢复性、显式状态转换”优先
