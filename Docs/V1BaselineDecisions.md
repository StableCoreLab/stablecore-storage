# StableCore Storage V1 Baseline Decisions

这份文档用于拍板 V1 最关键的 4 组约束：

- `Schema`
- `Value/Variant`
- `Record` 删除与失效语义
- `ChangeSet`

目标不是覆盖全部设计，而是先固定后续实现最容易反复修改的接口边界。

## 1. Schema Baseline

### 1.1 设计结论

V1 采用“受控动态 Schema”，不采用完全自由的 Key-Value。

结论：

- 每个 `Table` 都有正式 `Schema`
- 字段必须先注册，再读写
- 用户允许新增字段
- 用户新增字段一旦创建，也进入正式 `Schema`
- 不允许未注册字段直接写入 Record

### 1.2 字段分类

V1 字段分为两类：

1. `FactField`
2. `RelationField`

不把 `ComputedField` 放进存储核心 Schema。派生列属于上层表格模型或计算层。

### 1.3 Column 元信息

V1 建议每个字段至少具备这些元信息：

```cpp
enum class ValueKind
{
    Null,
    Int64,
    Double,
    Bool,
    String,
    RecordId,
    Enum
};

enum class ColumnKind
{
    Fact,
    Relation
};

struct ColumnDef
{
    std::wstring name;
    std::wstring displayName;
    ValueKind valueKind{ValueKind::Null};
    ColumnKind columnKind{ColumnKind::Fact};

    bool nullable{true};
    bool editable{true};
    bool userDefined{false};
    bool indexed{false};
    bool participatesInCalc{false};

    std::wstring unit;
    Value defaultValue;
};
```

### 1.4 V1 约束

- 字段名在单表内唯一
- 字段类型注册后不可直接漂移
- `RelationField` 的值类型必须是 `RecordId`
- `Enum` 字段在 V1 可先按“受控字符串”实现
- 字段删除和字段重命名不作为 V1 核心能力

### 1.5 推荐接口

```cpp
class ISchema : public IRefObject
{
public:
    virtual long GetColumnCount(int32_t* outCount) = 0;
    virtual long GetColumn(int32_t index, ColumnDef* outDef) = 0;
    virtual long FindColumn(const wchar_t* name, ColumnDef* outDef) = 0;
    virtual long AddColumn(const ColumnDef& def) = 0;
};
```

### 1.6 原因

这样可以同时满足：

- 算量产品对字段语义、类型、单位的要求
- 未来数据表编辑工具支持用户新增事实列
- 不让 Schema 退化成不可控的字符串字典

## 2. Value Baseline

### 2.1 设计结论

V1 不继续使用“模糊 Variant 概念”，而是固定一个最小可用值类型集合。

V1 支持：

- `Null`
- `Int64`
- `Double`
- `Bool`
- `String`
- `RecordId`
- `Enum`

暂不纳入核心：

- `RecordIdList`
- `Binary`
- `DateTime`
- `Json`

这些保留为 V2 扩展。

### 2.2 推荐值对象

```cpp
using RecordId = int64_t;

class Value
{
public:
    ValueKind GetKind() const noexcept;

    bool IsNull() const noexcept;

    long AsInt64(int64_t* outValue) const noexcept;
    long AsDouble(double* outValue) const noexcept;
    long AsBool(bool* outValue) const noexcept;
    long AsString(const wchar_t** outValue) const noexcept;
    long AsRecordId(RecordId* outValue) const noexcept;
};
```

### 2.3 类型规则

- `FactField` 只能存标量事实值
- `RelationField` 只能存 `RecordId`
- 不允许隐式把 `String` 当成 `RecordId`
- 不允许隐式把 `Int64` 当成 `Bool`
- `Null` 只允许写入可空字段

### 2.4 字符串规则

V1 需要明确字符串内存语义：

- `GetString` 返回的字符串内存由 `Record/Value` 持有
- 调用方不得释放
- 同一个对象下一次写入后，旧指针不再保证稳定

如果后续跨线程或缓存场景更复杂，可以再增加“复制型取值接口”。

### 2.5 推荐接口

比纯类型化接口更稳妥的方式，是同时保留统一读写入口：

```cpp
class IRecord : public IRefObject
{
public:
    virtual long GetValue(const wchar_t* name, Value* outValue) = 0;
    virtual long SetValue(const wchar_t* name, const Value& value) = 0;

    virtual long GetInt64(const wchar_t* name, int64_t* outValue) = 0;
    virtual long SetInt64(const wchar_t* name, int64_t value) = 0;

    virtual long GetDouble(const wchar_t* name, double* outValue) = 0;
    virtual long SetDouble(const wchar_t* name, double value) = 0;

    virtual long GetBool(const wchar_t* name, bool* outValue) = 0;
    virtual long SetBool(const wchar_t* name, bool value) = 0;

    virtual long GetString(const wchar_t* name, const wchar_t** outValue) = 0;
    virtual long SetString(const wchar_t* name, const wchar_t* value) = 0;

    virtual long GetRef(const wchar_t* name, RecordId* outValue) = 0;
    virtual long SetRef(const wchar_t* name, RecordId value) = 0;
};
```

### 2.6 原因

这套设计兼顾了：

- C++ 调用便利性
- 字段编辑工具的通用读写
- 后续 Journal/ChangeSet 复用统一值类型

## 3. Record Delete Baseline

### 3.1 设计结论

删除不是“对象消失”，而是“对象进入失效状态，并可被 Undo 恢复”。

V1 采用 `invalid/tombstone` 语义。

### 3.2 状态模型

```cpp
enum class RecordState
{
    Alive,
    Deleted
};
```

### 3.3 删除规则

- `DeleteRecord(recordId)` 后，记录状态从 `Alive` 变成 `Deleted`
- 记录对象的内部实例仍可存在，以支持旧 `RecordPtr`
- 已删除记录不允许写入
- 对已删除记录的读取返回统一错误码，或仅允许读取少量元信息
- `recordId` 永不复用

### 3.4 Undo/Redo 规则

- `Undo` 删除操作时，原记录恢复为 `Alive`
- 恢复后保持原 `recordId`
- 旧 `RecordPtr` 恢复可用，不重新分配新的逻辑身份
- `Redo` 删除后再次回到 `Deleted`

### 3.5 推荐接口

```cpp
class IRecord : public IRefObject
{
public:
    virtual RecordId GetId() const noexcept = 0;
    virtual bool IsDeleted() const noexcept = 0;
    virtual uint64_t GetLastModifiedVersion() const noexcept = 0;
};
```

### 3.6 V1 删除策略

V1 推荐：

- 不自动级联删除
- 不自动修复悬挂引用
- 存储层负责忠实记录“删除了什么”
- 上层业务或规则层决定是否允许删除、是否拦截、是否补偿处理

### 3.7 原因

这样可以避免两种常见问题：

- 存储层偷偷做级联，导致业务语义不可控
- 存储层直接释放对象，导致长期持有句柄失效成野指针

## 4. ChangeSet Baseline

### 4.1 设计结论

`ChangeSet` 是 V1 最核心的集成接口之一，不只是 UI 刷新结构。

它要同时服务：

- UI 局部刷新
- Model 缓存失效
- 算量增量重算
- 查询索引维护

### 4.2 事务级别语义

规则：

- 一个 `Commit` 生成一个 `ChangeSet`
- 一个 `Undo` 生成一个 `ChangeSet`
- 一个 `Redo` 生成一个 `ChangeSet`
- 不做单字段即时通知

### 4.3 变更来源

```cpp
enum class ChangeSource
{
    UserEdit,
    Undo,
    Redo,
    Import,
    RuleWriteback
};
```

### 4.4 变更类型

```cpp
enum class ChangeKind
{
    FieldUpdated,
    RecordCreated,
    RecordDeleted,
    RelationUpdated
};
```

### 4.5 推荐结构

```cpp
struct DataChange
{
    ChangeKind kind{};
    std::wstring tableName;
    RecordId recordId{0};
    std::wstring fieldName;

    Value oldValue;
    Value newValue;

    bool structuralChange{false};
    bool relationChange{false};
};

struct ChangeSet
{
    std::wstring actionName;
    ChangeSource source{ChangeSource::UserEdit};
    uint64_t version{0};
    std::vector<DataChange> changes;
};
```

### 4.6 聚合规则

V1 建议同一事务内做有限聚合：

- 同一字段多次改值，只保留“初始旧值 + 最终新值”
- 新建记录后多次赋值，保留 `RecordCreated`，字段赋值可按需要展开
- 删除记录前的多次字段变更，不再额外重复广播无意义字段更新

### 4.7 Observer 语义

V1 建议明确：

- 回调发生在事务提交完成之后
- 回调收到 `ChangeSet` 时，数据库已处于新状态
- Observer 不负责回滚
- Observer 中如果要再次写库，应开启新事务，不复用当前事务

### 4.8 原因

这能保证：

- UI 和计算层看到的是一致的新状态
- `ChangeSet` 可直接用于缓存失效与增量重算
- 不会因为逐字段广播造成大量噪音

## 5. 推荐的下一步

## 5. Error Code Baseline

### 5.1 设计结论

V1 继续使用 `long/HRESULT` 风格错误码，但要建立明确且稳定的错误语义，而不是只返回“失败”。

### 5.2 推荐原则

- 成功路径统一返回 `S_OK`
- “找到但值为空”与“字段不存在”必须区分
- “记录不存在”与“记录已删除”必须区分
- “类型不匹配”必须单独编码
- “事务上下文错误”必须单独编码

### 5.3 推荐错误码集合

```cpp
constexpr long SC_OK                     = 0x00000000L;
constexpr long SC_FALSE_RESULT           = 0x00000001L;

constexpr long SC_E_INVALIDARG           = 0x80070057L;
constexpr long SC_E_POINTER              = 0x80004003L;
constexpr long SC_E_FAIL                 = 0x80004005L;
constexpr long SC_E_NOTIMPL              = 0x80004001L;

constexpr long SC_E_TABLE_NOT_FOUND      = 0xA0010001L;
constexpr long SC_E_COLUMN_NOT_FOUND     = 0xA0010002L;
constexpr long SC_E_RECORD_NOT_FOUND     = 0xA0010003L;
constexpr long SC_E_RECORD_DELETED       = 0xA0010004L;
constexpr long SC_E_VALUE_IS_NULL        = 0xA0010005L;
constexpr long SC_E_TYPE_MISMATCH        = 0xA0010006L;
constexpr long SC_E_COLUMN_EXISTS        = 0xA0010007L;
constexpr long SC_E_SCHEMA_VIOLATION     = 0xA0010008L;
constexpr long SC_E_READ_ONLY_COLUMN     = 0xA0010009L;

constexpr long SC_E_NO_ACTIVE_EDIT       = 0xA0010101L;
constexpr long SC_E_EDIT_MISMATCH        = 0xA0010102L;
constexpr long SC_E_EDIT_ALREADY_CLOSED  = 0xA0010103L;
constexpr long SC_E_WRITE_CONFLICT       = 0xA0010104L;

constexpr long SC_E_UNDO_STACK_EMPTY     = 0xA0010201L;
constexpr long SC_E_REDO_STACK_EMPTY     = 0xA0010202L;

constexpr long SC_E_CONSTRAINT_VIOLATION = 0xA0010301L;
constexpr long SC_E_REFERENCE_INVALID    = 0xA0010302L;
```

### 5.4 语义说明

- `SC_E_RECORD_NOT_FOUND`：目标 `recordId` 从未存在或当前表中不存在
- `SC_E_RECORD_DELETED`：目标记录存在逻辑身份，但当前为 deleted 状态
- `SC_E_VALUE_IS_NULL`：字段存在，但当前值是 `Null`
- `SC_E_TYPE_MISMATCH`：字段类型或读取方式不匹配
- `SC_E_NO_ACTIVE_EDIT`：当前写操作不在事务内
- `SC_E_EDIT_MISMATCH`：提交/回滚的 `EditSession` 与当前上下文不一致
- `SC_E_REFERENCE_INVALID`：关系字段写入了不存在或非法引用

### 5.5 原因

这些错误码能直接支撑：

- 数据表编辑器的用户提示
- 上层规则层的精确错误处理
- 单元测试的稳定断言

## 6. EditSession Baseline

### 6.1 核心问题

当前接口形态是：

```cpp
record->SetInt64(...);
db->Commit(edit.Get());
```

`SetXxx()` 没显式传入 `EditSession`，所以必须明确写上下文归属模型。

### 6.2 设计结论

V1 采用“数据库级单活动事务”模型。

规则：

- 一个 `Database` 在任意时刻最多只有一个活动 `EditSession`
- 所有写操作默认写入当前活动事务
- 没有活动事务时，任何写操作返回 `SC_E_NO_ACTIVE_EDIT`
- `Commit` / `Rollback` 必须传入当前活动事务对象

### 6.3 推荐约束

- 不支持嵌套事务
- 不支持并行多个活动事务
- `EditSession` 一旦 `Commit` 或 `Rollback` 后即关闭
- 对已关闭事务再次提交，返回 `SC_E_EDIT_ALREADY_CLOSED`

### 6.4 推荐状态

```cpp
enum class EditState
{
    Active,
    Committed,
    RolledBack
};
```

### 6.5 推荐接口

```cpp
class IEditSession : public IRefObject
{
public:
    virtual const wchar_t* GetName() const = 0;
    virtual EditState GetState() const noexcept = 0;
    virtual uint64_t GetOpenedVersion() const noexcept = 0;
};
```

### 6.6 为什么不让写接口显式传 edit

V1 不建议改成：

```cpp
record->SetInt64(edit, name, value);
```

原因：

- 业务调用会变重
- `Record` 长期持有场景会更难用
- 当前单活动事务模型已经能覆盖 V1 需要

如果未来需要多事务并发，再升级接口。

## 7. Table Query Baseline

### 7.1 设计结论

V1 不能只剩 `GetRecord(recordId)`，但也不需要一开始就引入复杂查询语言。

V1 只拍板“最小可用查询集合”。

### 7.2 V1 最小查询能力

1. 按 `recordId` 获取记录
2. 遍历表内全部有效记录
3. 按单字段等值过滤
4. 按关系字段等值过滤

这已经足够支持很多算量和表格场景：

- 查某层所有梁
- 查某材质所有墙
- 查关联某做法的构件

### 7.3 推荐轻量接口

```cpp
struct QueryCondition
{
    std::wstring fieldName;
    Value expectedValue;
};

class IRecordCursor : public IRefObject
{
public:
    virtual long MoveNext(bool* outHasValue) = 0;
    virtual long GetCurrent(RecordPtr& outRecord) = 0;
};

class ITable : public IRefObject
{
public:
    virtual long GetRecord(RecordId id, RecordPtr& outRecord) = 0;
    virtual long CreateRecord(RecordPtr& outRecord) = 0;
    virtual long DeleteRecord(RecordId id) = 0;

    virtual long GetSchema(RefPtr<ISchema>& outSchema) = 0;
    virtual long EnumerateRecords(RefPtr<IRecordCursor>& outCursor) = 0;
    virtual long FindRecords(const QueryCondition& condition, RefPtr<IRecordCursor>& outCursor) = 0;
};
```

### 7.4 V1 查询约束

- 默认只枚举 `Alive` 记录
- 是否包含 `Deleted` 记录不作为 V1 公共查询能力
- 查询条件仅支持单字段等值匹配
- 查询结果顺序不保证稳定排序
- 索引是实现优化，不是语义前提

### 7.5 后续可扩展方向

- 多条件组合
- 范围过滤
- 排序
- 分页
- 关系反查优化

### 7.6 原因

这样可以避免 V1 过早设计 mini-SQL，同时保证接口不会锁死成纯主键访问。

## 8. JournalEntry Baseline

### 8.1 设计结论

`JournalEntry` 是内部回滚与重做的事实记录，不应与 `ChangeSet` 完全等同。

两者关系：

- `JournalEntry`：偏内部，服务恢复
- `ChangeSet`：偏外部，服务观察者和上层系统

### 8.2 操作类型

```cpp
enum class JournalOp
{
    SetValue,
    CreateRecord,
    DeleteRecord,
    SetRelation
};
```

### 8.3 推荐结构

```cpp
struct JournalEntry
{
    JournalOp op{};
    std::wstring tableName;
    RecordId recordId{0};
    std::wstring fieldName;

    Value oldValue;
    Value newValue;

    bool oldDeleted{false};
    bool newDeleted{false};
};
```

### 8.4 语义说明

- `SetValue`：记录字段变更前后的值
- `SetRelation`：语义上仍然记录前后值，但字段必须是 `RelationField`
- `CreateRecord`：`oldDeleted=true`，`newDeleted=false`，必要时可带初始字段写入
- `DeleteRecord`：`oldDeleted=false`，`newDeleted=true`

### 8.5 V1 聚合策略

V1 建议以事务为边界做 Journal 聚合：

- 同一事务、同一记录、同一字段多次写入，合并为一条
- `CreateRecord` 后对字段的初始化写入仍可单独记录
- `DeleteRecord` 后不再接受后续字段写入

### 8.6 推荐事务结构

```cpp
struct JournalTransaction
{
    std::wstring actionName;
    std::vector<JournalEntry> entries;
};
```

Undo/Redo 栈按 `JournalTransaction` 为单位存储。

### 8.7 Journal 与 ChangeSet 的转换原则

- `JournalTransaction` 是 Undo/Redo 的唯一真实来源
- `ChangeSet` 在 `Commit/Undo/Redo` 完成后基于事务结果生成
- `ChangeSet` 可比 `Journal` 更偏聚合和观察者友好

### 8.8 原因

这样可以把“内部可恢复性”和“外部可观察性”分开，减少后期两种用途互相牵制。

## 9. 下一步

按现在这版基线，下一步就可以开始：

1. 定义公共头文件
2. 落 `RefPtr` 和基础接口
3. 写一个纯内存 `Database/Table/Record` 最小实现
4. 写覆盖事务、删除、Undo/Redo、ChangeSet 的单元测试

到这一步，设计已经足够支撑开始编码，不需要继续停留在概念讨论。

## 10. Computed Field Baseline

### 10.1 设计结论

计算字段不进入存储核心 `Schema`，而是作为上层表格模型或计算层的独立列系统存在。

结论：

- 事实字段进入 `Schema`
- 计算字段进入 `ComputedColumn` 体系
- 计算字段可以显示在数据表工具中
- 计算字段默认不直接持久化到事实存储层
- 如需缓存，应作为可失效缓存处理，而不是事实字段

### 10.2 计算字段的表达目标

这套设计需要同时支持：

- 简单公式列
- 复杂规则列
- 聚合列

典型场景：

- `Volume = Length * Width * Height`
- `TemplateArea = Rule(beam.formwork.area.v1)`
- `ChildQuantitySum = Aggregate(children.quantity)`

### 10.3 计算字段分类

```cpp
enum class ComputedFieldKind
{
    Expression,
    Rule,
    Aggregate
};
```

说明：

- `Expression`：适合单记录内简单表达式
- `Rule`：适合复杂算法、专业规则、版本化算量逻辑
- `Aggregate`：适合关系遍历后的汇总结果

### 10.4 推荐列定义

```cpp
struct ComputedColumnDef
{
    std::wstring name;
    std::wstring displayName;
    ValueKind valueKind{ValueKind::Null};

    ComputedFieldKind kind{ComputedFieldKind::Expression};

    std::wstring expression;
    std::wstring ruleId;

    bool cacheable{true};
    bool editable{false};
};
```

字段约束：

- `Expression` 类型必须提供 `expression`
- `Rule` 类型必须提供 `ruleId`
- `Aggregate` 类型可使用 `ruleId` 或专门聚合描述
- 计算字段默认 `editable=false`

### 10.5 依赖声明

计算字段必须显式声明依赖项，不能只保留一段表达式字符串。

```cpp
struct FieldDependency
{
    std::wstring tableName;
    std::wstring fieldName;
};

struct ComputedDependencySet
{
    std::vector<FieldDependency> factFields;
    std::vector<FieldDependency> relationFields;
};
```

原因：

- `ChangeSet` 到达后可以做精确失效
- UI 可判断哪些列需要重算
- 计算缓存可按依赖版本失效

### 10.6 依赖示例

示例 1：

```text
Volume
  factFields: Length, Width, Height
  relationFields: none
```

示例 2：

```text
NetVolume
  factFields: Length, Width, Height
  relationFields: OpeningRefs
```

示例 3：

```text
Quantity
  factFields: ComponentType, MaterialGrade
  relationFields: RecipeRef
```

### 10.7 推荐统一上下文

计算规则不应直接访问底层表实现，而应通过统一求值上下文取数。

```cpp
class IComputedContext
{
public:
    virtual long GetValue(const wchar_t* fieldName, Value* outValue) = 0;
    virtual long GetRef(const wchar_t* fieldName, RecordId* outId) = 0;
    virtual long GetRelated(const wchar_t* relationName, RefPtr<IRecordCursor>& outCursor) = 0;
};
```

说明：

- `GetValue`：读取当前记录的事实字段
- `GetRef`：读取当前记录的单引用关系
- `GetRelated`：读取当前记录关联的记录集合

### 10.8 推荐求值接口

```cpp
class IComputedEvaluator
{
public:
    virtual long Evaluate(
        const ComputedColumnDef& column,
        IComputedContext* context,
        Value* outValue) = 0;
};
```

如果是 `Rule` 类型，则由注册表根据 `ruleId` 找到具体求值器。

```cpp
class IRuleRegistry
{
public:
    virtual IComputedEvaluator* Find(const wchar_t* ruleId) = 0;
};
```

### 10.9 V1 表达式能力边界

V1 表达式建议只支持最小集合：

- 字段引用
- 数值常量
- 四则运算
- 括号
- 少量内建函数

建议内建函数上限：

- `min`
- `max`
- `abs`
- `if`

示例：

```text
Length * Width * Height
max(0, GrossVolume - OpeningVolume)
if(IsStructural, Length * Width * Height, 0)
```

V1 不建议支持：

- 通用脚本语言
- 自定义循环
- 动态执行代码
- 任意跨表查询表达式

### 10.10 规则型字段建议

复杂算量规则不建议硬编码成超长表达式，应改用 `ruleId`。

示例：

```text
beam.concrete.volume.v1
wall.formwork.area.v1
slab.rebar.weight.v1
```

这样有几个好处：

- 规则可以版本化
- 不污染事实存储层
- 复杂逻辑由 C++ 实现，更易测试
- 后续可以逐步替换实现，而不破坏列定义模型

### 10.11 聚合型字段建议

聚合列用于：

- 汇总子构件数量
- 汇总关联材料用量
- 汇总某关系集合的派生值

V1 建议：

- 聚合列先走 `Rule` 模式实现
- 不额外设计一套复杂 DSL
- 等真实场景积累后，再决定是否拆出独立聚合描述语言

### 10.12 缓存策略

计算字段默认不落事实层，但允许做缓存。

缓存建议：

- 缓存键包含 `recordId + computedColumn + version`
- 依赖字段变化后缓存失效
- 缓存内容不进入 `Undo/Redo`
- 缓存失败不影响事实数据一致性

### 10.13 与数据表编辑工具的关系

数据表编辑工具应同时支持：

- `SchemaColumn`：事实列，可编辑、可持久化
- `ComputedColumn`：计算列，只读、可缓存、可重算

因此列模型应分层：

```text
TableView
 ├── SchemaColumn
 └── ComputedColumn
```

这样用户可以：

- 新增事实属性列
- 新增展示型公式列
- 新增规则驱动的算量列

而不会把事实数据与派生数据混在同一个存储语义里。

### 10.14 V1 推荐结论

V1 关于计算字段建议直接拍板为：

1. 不进入核心存储 `Schema`
2. 通过 `ComputedColumnDef` 单独建模
3. 必须声明依赖字段
4. 简单规则用 `expression`
5. 复杂规则用 `ruleId`
6. 聚合规则先复用 `ruleId`
7. 结果默认不持久化，只允许做可失效缓存

## 11. Floor Relation Baseline

### 11.1 设计结论

“构件所在楼层”在数据层应表达为**关系字段**，而不是普通字符串或整数属性。

推荐建模：

- `Floor` 是独立 `Table`
- 楼层是独立 `Record`
- 构件通过 `FloorRef` 关系字段引用楼层记录

### 11.2 推荐表结构

示例：

```text
Table: Floor
  recordId=1001
    Name      = "1F"
    Code      = "F1"
    Elevation = 0

  recordId=1002
    Name      = "2F"
    Code      = "F2"
    Elevation = 3600
```

```text
Table: Beam
  recordId=2001
    Name      = "KL1"
    Width     = 300
    Height    = 600
    FloorRef  = 1002
```

其中：

- `FloorRef` 是 `RelationField`
- `FloorRef` 的值类型是 `RecordId`
- `FloorRef` 指向 `Floor` 表中的某条记录

### 11.3 为什么不用 `FloorName`

不推荐构件直接存：

- `FloorName = "2F"`
- `FloorNo = 2`

原因：

- 楼层名称变更会导致大批构件重复更新
- 无法稳定引用楼层对象
- 楼层本身的标高、类别、显示名等属性无法统一管理
- “查某层所有构件”会退化成不稳定的字符串匹配

### 11.4 推荐读取方式

数据层获取构件楼层的过程应是：

1. 从构件记录读取 `FloorRef`
2. 获取对应 `recordId`
3. 到 `Floor` 表读取楼层记录

示例：

```cpp
RecordId floorId = 0;
beamRecord->GetRef(L"FloorRef", &floorId);

RecordPtr floorRecord;
floorTable->GetRecord(floorId, floorRecord);
```

### 11.5 推荐楼层字段

`Floor` 表在 V1 建议至少具备：

- `Name : String`
- `Code : String`
- `Elevation : Double`
- `Category : Enum`

可选扩展：

- `SortIndex : Int64`
- `IsUnderground : Bool`
- `ProjectSection : String`

### 11.6 单归属与多归属

V1 推荐优先支持“单归属楼层”：

- `FloorRef : RecordId`

这是最适合大多数场景的主模型，因为它能直接支撑：

- 表格筛选
- 树结构分组
- 构件定位
- 算量统计归属

### 11.7 跨层构件建议

如果后续存在真实跨层构件，不建议破坏单归属模型，而建议补充额外字段：

- `StartFloorRef`
- `EndFloorRef`

或在后续版本支持：

- `FloorRefs`

V1 不建议一开始就把所有归属关系设计成多值集合，否则很多简单业务会被迫复杂化。

### 11.8 业务层表达建议

领域层可以把数据层关系包装成更易用的接口：

```cpp
class Beam
{
public:
    Floor GetFloor() const;
};
```

但底层存储语义保持不变：

- 构件存 `FloorRef`
- 楼层是独立 Record

### 11.9 ChangeSet 语义

如果构件楼层发生变化，存储层应把它视为关系字段更新：

- `kind = RelationUpdated`
- `fieldName = "FloorRef"`
- `oldValue = old floorId`
- `newValue = new floorId`

这样上层可直接执行：

- 从旧楼层分组中移除
- 加入新楼层分组
- 触发相关统计或局部重算

### 11.10 V1 推荐结论

V1 关于“构件所在楼层”建议直接拍板为：

1. 楼层是独立 `Floor` 表
2. 构件通过 `FloorRef : RecordId` 引用楼层
3. `FloorRef` 是 `RelationField`
4. 楼层变更属于 `RelationUpdated`
5. V1 默认采用单归属楼层模型

## 12. SQLite Engine Baseline

### 12.1 设计结论

V1 底层数据库引擎直接使用 SQLite 官方 `sqlite3`，不引入 ORM。

结论：

- 底层引擎：`sqlite3`
- 访问方式：官方 C API
- 项目内部自建一层薄 C++ 适配
- 不引入 ORM
- 不引入重型第三方 SQLite 包装库

### 12.2 为什么直接用 `sqlite3`

原因：

- SQLite 官方 API 稳定、成熟、跨平台
- 当前系统本身已经有完整存储抽象，不需要 ORM 再做一层对象映射
- 事务、Journal、Undo/Redo、关系字段、版本控制都需要精确掌控
- 直接使用 `sqlite3` 更容易控制 SQL、事务边界和错误映射
- 减少额外依赖与后期迁移成本

### 12.3 为什么不引入 ORM

不建议 V1 使用 ORM 或重封装库，例如：

- `sqlite_orm`
- `soci`
- 其他对象映射库

原因：

- 本项目不是传统“对象-表”映射问题
- 领域对象不会直接映射到底层数据库表
- 存储层已有自己的抽象模型：`Database / Table / Record / Schema / Journal`
- ORM 会增加额外语义层，反而削弱可控性

### 12.4 适配层原则

项目内部只封一层薄适配，职责仅限于 SQLite 访问便利性和资源管理。

建议适配层不承担：

- 业务语义
- `Record` 领域语义
- `ChangeSet` 逻辑
- Journal 语义
- Schema 业务校验

这些仍属于存储核心层，而不是 SQLite 工具层。

## 13. SQLite Persistence Boundary

### 13.1 分层结论

推荐明确分成三层：

```text
Public Storage API
  ├── IDatabase / ITable / IRecord / ISchema
Storage Core
  ├── transaction / journal / changeset / value / schema rules
SQLite Persistence
  ├── sqlite3 / statement / transaction / row mapping
```

含义：

- 公共接口层：对外 API
- 存储核心层：负责真正的存储语义
- SQLite 持久化层：负责把核心状态落到 SQLite

### 13.2 SQLite 持久化层职责

SQLite 持久化层建议只负责：

- 打开/关闭数据库
- 执行 SQL
- 管理预编译语句
- 绑定参数
- 读取结果集
- 管理 SQLite 事务
- 初始化数据库 schema
- SQLite 错误码转换为系统错误码

### 13.3 SQLite 持久化层不负责

SQLite 持久化层不应负责：

- 决定一个字段是不是事实字段
- 决定一个字段是不是计算字段
- 拼装 `ChangeSet`
- 组装 Undo/Redo 的上层语义
- 触发 Observer
- 执行业务规则

这些职责应保留在 Storage Core。

### 13.4 推荐适配对象

V1 建议内部提供这些基础类：

```cpp
class SqliteDb;
class SqliteStmt;
class SqliteTxn;
class SqliteError;
```

推荐职责：

- `SqliteDb`
  - 打开/关闭连接
  - 执行简单 SQL
  - 创建预编译语句
- `SqliteStmt`
  - 参数绑定
  - `Step()`
  - 读取列值
- `SqliteTxn`
  - `BEGIN`
  - `COMMIT`
  - `ROLLBACK`
- `SqliteError`
  - SQLite 返回码和消息封装

### 13.5 推荐接口风格

这层适配建议保持极薄，不暴露业务对象。

例如：

```cpp
class SqliteStmt
{
public:
    long BindInt64(int index, int64_t value);
    long BindDouble(int index, double value);
    long BindText(int index, const wchar_t* value);
    long Step(bool* outHasRow);

    long ColumnInt64(int index, int64_t* outValue) const;
    long ColumnDouble(int index, double* outValue) const;
    long ColumnText(int index, const wchar_t** outValue) const;
};
```

### 13.6 推荐数据映射策略

V1 不建议把每个业务表直接做成一个独立 SQLite 物理表。

更稳妥的方向是“逻辑表”和“物理表”分离。

推荐物理层优先采用统一元表思路，例如：

- `tables`
- `records`
- `field_values`
- `journal_entries`
- `schema_columns`

这样做的好处：

- 逻辑 `Table` 可动态扩展
- 用户新增字段更容易支持
- 存储抽象不会被 SQLite 物理表结构绑死

但这只是持久化策略建议，不影响对外接口抽象。

### 13.7 事务边界建议

SQLite 事务必须与存储层事务严格对齐。

建议：

- `BeginEdit` 对应开启一段存储层写事务
- `Commit` 时统一落库并提交 SQLite 事务
- `Rollback` 时回滚 SQLite 事务
- `Undo/Redo` 也作为完整 SQLite 事务执行

不要让 SQLite 事务和存储层事务出现两套独立节奏。

### 13.8 Journal 持久化建议

因为系统明确支持 `Undo/Redo`，SQLite 层建议持久化 Journal，而不是只保存在内存。

建议：

- 一个事务对应一个 journal transaction
- journal entry 按顺序持久化
- `Undo` / `Redo` 栈状态可持久化

这样后续才能支持：

- 重启后继续撤销重做
- 调试历史修改
- 导入/规则写回后的可追踪性

如果 V1 先不做跨重启 Undo/Redo，也应至少保证表结构预留得出来。

### 13.9 错误映射建议

SQLite 原始错误码不应直接向上暴露。

建议在适配层完成映射：

- `SQLITE_CONSTRAINT` -> `SC_E_CONSTRAINT_VIOLATION`
- `SQLITE_BUSY` -> `SC_E_WRITE_CONFLICT`
- `SQLITE_MISMATCH` -> `SC_E_TYPE_MISMATCH`
- 其他未知错误 -> `SC_E_FAIL`

### 13.10 V1 推荐结论

V1 关于 SQLite 持久化建议直接拍板为：

1. 底层引擎直接使用官方 `sqlite3`
2. 项目内部只封一层薄 C++ 适配
3. 不引入 ORM
4. 存储语义留在 Storage Core，不下沉到 SQLite 工具层
5. SQLite 事务与存储事务保持一致
6. Journal 设计按可持久化方向建模
