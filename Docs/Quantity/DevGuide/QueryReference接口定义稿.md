# Query / Reference 接口定义稿

本文档定义单表 / 单视图目标下的查询与引用检查接口边界。本文档只描述类型、接口、枚举、状态和职责，不包含实现细节。

## 一、核心枚举与基础类型

### 1.1 QueryPlanState

```cpp
enum class QueryPlanState
{
    DirectIndex,
    PartialIndex,
    ScanFallback,
    Unsupported,
};
```

职责：
- `DirectIndex`：计划可完全由索引满足。
- `PartialIndex`：计划可由索引部分满足，其余条件后置过滤。
- `ScanFallback`：计划需要回退扫描。
- `Unsupported`：当前能力无法表达或无法执行。

### 1.2 QueryConditionOperator

```cpp
enum class QueryConditionOperator
{
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    Between,
    IsNull,
    IsNotNull,
    StartsWith,
    Contains,
    EndsWith,
};
```

第一阶段支持范围：
- 可下推：`Equal`、`NotEqual`、`LessThan`、`LessThanOrEqual`、`GreaterThan`、`GreaterThanOrEqual`、`IsNull`、`IsNotNull`
- 可部分下推：`In`、`Between`、`StartsWith`
- 仅 fallback：`Contains`、`EndsWith`
- 直接 unsupported：当字段类型不匹配、目标不支持、或执行约束冲突时可判定为 `Unsupported`

### 1.3 QueryLogicOperator

```cpp
enum class QueryLogicOperator
{
    And,
    Or,
};
```

职责：
- 定义条件组之间的组合关系。
- 第一阶段不支持嵌套 group。

### 1.4 QueryOrderDirection

```cpp
enum class QueryOrderDirection
{
    Ascending,
    Descending,
};
```

### 1.5 QueryExecutionMode

```cpp
enum class QueryExecutionMode
{
    Planned,
    FallbackScan,
    Unsupported,
};
```

### 1.6 ReferenceDirection

```cpp
enum class ReferenceDirection
{
    Forward,
    Reverse,
};
```

---

## 二、QueryTarget / QueryCondition / QueryConditionGroup / SortSpec / QueryPage / QueryHints

### 2.1 QueryTarget

```cpp
enum class QueryTargetType
{
    Table,
    View,
};

struct QueryTarget
{
    std::wstring name;
    QueryTargetType type{QueryTargetType::Table};
};
```

职责：
- 表示单表或单视图查询目标。
- 不允许同时携带多个目标字段。
- 第一阶段仅允许一个目标，不支持 join、union、cross-table lookup。

### 2.2 QueryCondition

```cpp
struct QueryCondition
{
    std::wstring fieldName;
    QueryConditionOperator op{QueryConditionOperator::Equal};
    std::vector<SCValue> values;
};
```

字段含义：
- `fieldName`：目标字段名。
- `op`：比较操作符。
- `values`：比较值集合。

约束：
- `In` 必须携带多个值。
- `Between` 必须携带两个值。
- `IsNull` / `IsNotNull` 不携带值。
- `StartsWith` / `Contains` / `EndsWith` 仅适用于文本字段。

### 2.3 QueryConditionGroup

```cpp
struct QueryConditionGroup
{
    QueryLogicOperator logic{QueryLogicOperator::And};
    std::vector<QueryCondition> conditions;
};
```

约束：
- 第一阶段只允许一层 group。
- 不支持 group 嵌套。
- group 内条件按同一目标字段集合解释。

### 2.4 SortSpec

```cpp
struct SortSpec
{
    std::wstring fieldName;
    QueryOrderDirection direction{QueryOrderDirection::Ascending};
    bool requireStableSort{false};
};
```

约束：
- `requireStableSort` 表示是否需要二次排序保证稳定顺序。
- 该字段可能影响执行路径是否允许直接下推。

### 2.5 QueryPage

```cpp
struct QueryPage
{
    std::uint64_t offset{0};
    std::uint64_t limit{0};
};
```

约束：
- `limit == 0` 表示使用系统默认上限。
- 禁止无边界查询。

### 2.6 QueryHints

```cpp
struct QueryHints
{
    bool preferIndex{true};
    bool needReferenceInfo{false};
    bool requireSummaryOnly{false};
};

struct QueryConstraints
{
    bool allowFallbackScan{true};
    bool requireIndex{false};
    bool allowPartial{true};
};
```

职责：
- `QueryHints` 表示建议性信息。
- `QueryConstraints` 表示强约束。
- `allowFallbackScan` 属于强约束，不属于 hint。

---

## 三、QueryPlan / QueryExecutionResult

### 3.1 QueryPlan

```cpp
struct QueryPlan
{
    QueryTarget target;
    std::vector<QueryConditionGroup> conditionGroups;
    std::vector<SortSpec> orderBy;
    QueryPage page;
    QueryHints hints;
    QueryConstraints constraints;
    QueryPlanState state{QueryPlanState::Unsupported};
    std::wstring planId;
    std::wstring fallbackReason;
    std::uint64_t estimatedRows{0};
    std::uint64_t estimatedCost{0};
    std::uint32_t pushdownConditionCount{0};
    std::uint32_t fallbackConditionCount{0};
};
```

可观测字段：
- `planId`
- `state`
- `fallbackReason`
- `estimatedRows`
- `estimatedCost`
- `pushdownConditionCount`
- `fallbackConditionCount`
- `target`
- `page`
- `orderBy`
- `hints`
- `constraints`

### 3.2 QueryExecutionResult

```cpp
struct QueryExecutionResult
{
    ErrorCode rc{SC_OK};
    QueryExecutionMode mode{QueryExecutionMode::Unsupported};
    std::uint64_t scannedRows{0};
    std::uint64_t matchedRows{0};
    std::uint64_t returnedRows{0};
    std::vector<std::wstring> usedIndexIds;
    bool fallbackTriggered{false};
    std::wstring executionNote;
};
```

职责：
- 记录查询执行结果和执行模式。
- 支持性能分析、诊断工具和后续自动优化。

---

## 四、ReferenceIndex / ReverseReferenceIndex

### 4.1 ReferenceRecord

```cpp
struct ReferenceRecord
{
    std::wstring sourceTable;
    RecordId sourceRecordId{0};
    std::wstring sourceColumn;
    std::wstring targetTable;
    RecordId targetRecordId{0};
    CommitId commitId{0};
    SessionId sessionId{0};
    std::optional<SnapshotId> snapshotId;
};
```

### 4.2 ReferenceIndex

```cpp
struct ReferenceIndex
{
    std::vector<ReferenceRecord> records;
};
```

职责：
- 从源对象视角记录其引用目标。
- 只记录正式提交后的引用关系。

### 4.3 ReverseReferenceRecord

```cpp
struct ReverseReferenceRecord
{
    std::wstring targetTable;
    RecordId targetRecordId{0};
    std::wstring sourceTable;
    RecordId sourceRecordId{0};
    std::wstring sourceColumn;
    CommitId commitId{0};
    SessionId sessionId{0};
    std::optional<SnapshotId> snapshotId;
};
```

### 4.4 ReverseReferenceIndex

```cpp
struct ReverseReferenceIndex
{
    std::vector<ReverseReferenceRecord> records;
};
```

职责：
- 从目标对象视角记录被谁引用。
- 只记录正式提交后的反向引用关系。

### 4.5 正式数据来源边界

ReferenceIndex / ReverseReferenceIndex 的正式来源仅限：
- `save`
- `save as`
- `upgrade`
- `finalize import`
- 正式 `Commit`

不作为正式来源的状态：
- `checkpoint`
- `append chunk`
- 草稿编辑
- Replay 中间态
- 临时诊断导出

---

## 五、接口定义

### 5.1 IQueryPlanner

```cpp
class IQueryPlanner
{
public:
    virtual ~IQueryPlanner() = default;
    virtual ErrorCode BuildPlan(const QueryTarget& target,
                                const std::vector<QueryConditionGroup>& conditionGroups,
                                const std::vector<SortSpec>& orderBy,
                                const QueryPage& page,
                                const QueryHints& hints,
                                const QueryConstraints& constraints,
                                QueryPlan* outPlan) const = 0;
};
```

职责：
- 规范化查询请求。
- 判断计划是否可下推、部分下推、fallback 或 unsupported。
- 不执行查询。

### 5.2 IQueryExecutor

```cpp
class IQueryExecutor
{
public:
    virtual ~IQueryExecutor() = default;
    virtual ErrorCode Execute(const QueryPlan& plan,
                              QueryExecutionResult* outResult) = 0;
};
```

职责：
- 按计划执行查询。
- 将结果交给现有查询入口或迭代器适配层。
- 不负责规划。

### 5.3 IReferenceIndexProvider

```cpp
class IReferenceIndexProvider
{
public:
    virtual ~IReferenceIndexProvider() = default;
    virtual ErrorCode GetReferencesBySource(
        const std::wstring& sourceTable,
        RecordId sourceRecordId,
        std::vector<ReferenceRecord>* outRecords) const = 0;
    virtual ErrorCode GetReferencesByTarget(
        const std::wstring& targetTable,
        RecordId targetRecordId,
        std::vector<ReverseReferenceRecord>* outRecords) const = 0;
    virtual ErrorCode GetAllReferencesDiagnosticOnly(ReferenceIndex* outIndex) const = 0;
};
```

职责：
- 提供按源 / 按目标的引用索引读取能力。
- 全量接口仅用于诊断，不作为常规执行路径。

### 5.4 IReferenceIndexMaintainer

```cpp
class IReferenceIndexMaintainer
{
public:
    virtual ~IReferenceIndexMaintainer() = default;
    virtual ErrorCode RebuildReferenceIndexes() = 0;
    virtual ErrorCode CommitReferenceDelta(const ReferenceIndex& forwardDelta,
                                           const ReverseReferenceIndex& reverseDelta) = 0;
};
```

职责：
- 负责索引重建与正式提交后的增量维护。

---

## 六、与现有接口的桥接关系

### 6.1 保留现有入口
现有查询入口继续保留，不强制改造调用方：
- 现有表枚举
- 现有记录过滤
- 现有引用检查
- 现有视图查询

### 6.2 桥接原则
- 旧入口进入后先生成 `QueryPlan`
- 简单条件优先下推
- 复杂条件由 executor 决定是否 fallback
- 现有引用检查入口可逐步切换到 `IReferenceIndexProvider`

### 6.3 结果兼容
- 旧接口返回结果形态保持不变
- 新接口只用于增强执行路径和诊断能力

---

## 七、第一阶段不支持的能力

以下能力在第一阶段明确不支持：
- 多表联查
- join / group by / having
- 子查询
- 分布式查询
- 跨工程查询
- 跨租户查询
- 写时查询副作用
- 自动改写复杂逻辑表达式
- 依赖全文索引的模糊检索作为默认能力

