# StableCore Storage

## Current Status

Current implementation includes:

- Public V1 headers under `Include/StableCore/Storage`
- In-memory database baseline with transaction, schema, undo/redo, changeset, and relation field support
- SQLite persistence backend under `Src/Sqlite`
- Computed-column metadata baseline in public types for upper-layer table tools
- Minimal computed runtime (`expression`, `ruleId`, cache model)
- Batch edit/import helpers
- Migration planning and diagnostics helpers
- M1 example under `Examples/MemoryExample.cpp`
- Product integration example under `Examples/ProductIntegrationExample.cpp`
- M1/M2/M3 baseline tests under `Tests/`

Quick in-memory usage:

```cpp
#include "StableCore/Storage/Storage.h"

using namespace stablecore::storage;

DbPtr db;
CreateInMemoryDatabase(db);

TablePtr beamTable;
db->CreateTable(L"Beam", beamTable);

SchemaPtr schema;
beamTable->GetSchema(schema);

ColumnDef width;
width.name = L"Width";
width.displayName = L"Width";
width.valueKind = ValueKind::Int64;
width.defaultValue = Value::FromInt64(0);
schema->AddColumn(width);

EditPtr edit;
db->BeginEdit(L"Create Beam", edit);

RecordPtr beam;
beamTable->CreateRecord(beam);
beam->SetInt64(L"Width", 300);

db->Commit(edit.Get());
```

Computed columns are modeled separately from storage schema facts. Use `ColumnDef` for persisted fact/relation columns and `ComputedColumnDef` for read-only derived columns owned by upper-layer table or calculation modules.

For higher-level integration:

- `Computed.h` provides expression/rule evaluation and cache invalidation primitives.
- `Batch.h` provides batch-edit/import helpers that reuse the database transaction model.
- `Migration.h` provides explicit migration planning primitives.
- `Diagnostics.h` provides health-report and `ChangeSet` description helpers.

面向未来算量产品的通用存储内核。

核心目标：

- 与业务对象解耦，采用 `Database / Table / Record / Field`
- 支持事务、`Undo/Redo`、变更通知
- 支持跨 DLL 安全访问和长期持有对象
- 支持用户扩展事实属性列
- 为 UI 局部刷新、增量重算、后续查询与缓存提供稳定基础

## 1. 设计定位

本库定位为算量产品的**事实数据存储层**，而不是完整业务层或计算层。

推荐分层：

- `Storage`：保存事实数据、关系数据、事务日志、版本信息
- `Model/Domain`：把 `Record` 映射为 Beam、Wall、Floor 等领域对象
- `Calc Engine`：根据事实数据计算派生结果
- `UI/View`：订阅变更并执行局部刷新

### 1.1 事实数据与派生数据

存储层只保存**事实数据**：

- 构件基础属性
- 分类、标签、来源信息
- 用户录入参数
- 对象关系
- 用户新增的事实型属性列

派生结果不作为事实层核心职责：

- 体积
- 模板面积
- 钢筋重量
- 清单工程量
- 定额工程量

这类数据应放在上层计算结果层，或作为可失效缓存存在。

## 2. 核心设计原则

### 2.1 通用数据模型

```text
Database
 ├── Table
 │    ├── Record
 │    │    ├── Field
 │    │    └── RelationField
 │    └── Schema
 ├── Journal
 ├── Version
 └── Observer
```

### 2.2 生命周期模型

内部采用侵入式引用计数，对外统一通过 `RefPtr<T>` 管理对象生命周期。

约束：

- 禁止业务层手工调用 `Release()`
- 禁止跨 DLL `delete`
- 必须通过 `RefPtr` 持有接口对象
- 长期持有 `RecordPtr` 是允许的

### 2.3 事务优先

所有写操作都必须处于事务内。

```text
BeginEdit
   ↓
多次修改
   ↓
Commit / Rollback
```

一个用户动作对应一个事务，一个事务对应一个 `ChangeSet`，`Undo/Redo` 也以事务为单位。

## 3. 面向算量产品的 V1 范围

V1 必须覆盖以下能力：

1. 事实数据存储
2. 稳定 `recordId`
3. 事务与 `Undo/Redo`
4. 关系字段
5. 受控 `Schema`
6. 变更通知
7. 全局版本号
8. 用户扩展事实列
9. 删除语义
10. 查询与索引预留

V1 不做：

- ORM
- UI 绑定实现
- 分布式同步
- 跨进程并发写
- 通用 SQL 引擎
- 自动依赖图推导

## 4. 核心对象职责

| 对象 | 职责 |
| --- | --- |
| `Database` | 管理表、事务、版本、Undo/Redo、Observer |
| `Table` | 管理 Record、Schema、基础查询 |
| `Record` | 事实数据载体，持有稳定 `recordId` |
| `Field` | 标量事实字段 |
| `RelationField` | 引用其他 Record 的关系字段 |
| `EditSession` | 事务上下文 |
| `Journal` | 保存事务内变更，用于 Undo/Redo |
| `ChangeSet` | 提交后的变更集合，用于 UI 和增量重算 |

## 5. Record 身份与生命周期

### 5.1 稳定主键

每个 `Record` 必须具有数据库内稳定的 `recordId`。

要求：

- 删除后 `recordId` 不复用
- `Undo` 恢复记录时保持原 `recordId`
- UI 选中状态、缓存、关系引用、计算依赖均基于 `recordId`

### 5.2 长期持有语义

领域对象可以长期持有 `RecordPtr`：

```cpp
class Beam
{
    RecordPtr m_record;
};
```

### 5.3 删除后的句柄状态

记录删除后，旧的 `RecordPtr` 不应变成野指针，而应进入 `invalid/tombstone` 状态。

约束：

- 可查询其 `recordId`
- 禁止继续写入
- 读取行为由接口统一定义，可返回错误码
- `Undo` 恢复后重新回到有效状态

## 6. Schema 与用户定义属性列

### 6.1 采用受控动态 Schema

系统不采用完全自由的 Key-Value 模式，而采用**受控动态**：

- 表有正式 `Schema`
- 字段必须注册
- 用户可以新增字段
- 新增后字段纳入 Schema 管理

### 6.2 字段分类

建议把列分为两类：

#### `SchemaColumn`

事实字段，进入存储层。

示例：

- 楼层
- 材质
- 强度等级
- 宽度
- 高度
- 用户新增的损耗系数
- 用户新增的计算分组

#### `ComputedColumn`

派生展示字段，不作为事实层核心存储。

示例：

- 体积
- 模板面积
- 清单工程量
- 定额工程量

### 6.3 字段元信息建议

每个正式字段建议具备以下元信息：

- 字段名
- 显示名
- 类型
- 单位
- 默认值
- 是否可空
- 是否可编辑
- 是否用户定义
- 是否关系字段
- 是否可索引
- 是否参与计算

## 7. 值类型系统

V1 建议至少支持：

- `Int64`
- `Double`
- `Bool`
- `String`
- `Null`
- `RecordId`
- `Enum` 或受控字符串

建议预留扩展：

- `RecordIdList`
- `BinaryRef`
- `DateTime`
- `Json`

原则：

- 不用字符串伪装布尔、枚举或引用
- 关系值与普通字符串严格区分
- 字段类型一经注册，不应随意漂移

## 8. 关系模型

算量产品必须支持对象之间的关系，而不只是标量字段。

V1 至少支持：

- 单引用：一个字段指向另一个 `recordId`
- 多引用：可通过 `RecordIdList` 或关系表实现
- 关系变更通知
- 基础反查预留

典型关系：

- 构件属于楼层
- 洞口关联墙
- 梁关联轴线
- 构件关联做法
- 构件关联清单项

## 9. 事务、Journal 与 Undo/Redo

### 9.1 事务模型

写入必须通过事务进行：

```cpp
EditPtr edit;
pDb->BeginEdit(L"修改梁宽度", edit);

TablePtr table;
pDb->GetTable(L"Beam", table);

RecordPtr record;
table->GetRecord(beamId, record);

record->SetInt(L"Width", 300);

pDb->Commit(edit.Get());
```

### 9.2 Journal 记录内容

建议覆盖以下操作：

- 设置字段
- 创建记录
- 删除记录
- 设置关系
- 批量字段更新

同一事务内建议做变更聚合，减少无效 Journal 噪音。

### 9.3 Undo/Redo 语义

- `Undo/Redo` 以事务为单位
- `actionName` 应可直接显示在 UI 中
- 一次 `Undo` 对应回滚一个完整用户动作

## 10. ChangeSet 与通知模型

### 10.1 通知触发点

以下操作应广播变更：

- `Commit`
- `Undo`
- `Redo`

### 10.2 数据库级 Observer

只提供数据库级 Observer，不提供表级或记录级 Observer。

```cpp
class IDatabaseObserver
{
public:
    virtual void OnDatabaseChanged(const ChangeSet&) = 0;
};
```

原则：

- 存储层负责广播
- 过滤逻辑在 Model/Domain 层

### 10.3 ChangeSet 设计建议

`ChangeSet` 不仅用于 UI，还要能服务：

- 模型缓存失效
- 增量重算
- 查询索引更新
- 结果缓存更新

因此建议在 `DataChange` 中体现：

- 变更类型
- 表名
- `recordId`
- 字段名
- 旧值 / 新值
- 是否关系变化
- 变更来源
- 版本号

建议增加：

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

## 11. 删除语义

删除必须有稳定且可恢复的语义。

V1 建议：

- 删除后 `recordId` 不复用
- 删除后旧句柄进入 `invalid/tombstone`
- `Undo` 恢复时沿用原 `recordId`
- 默认不自动做级联删除
- 引用一致性校验由上层业务或专门规则层处理

这样可以避免算量场景中的关系悬挂被静默吞掉。

## 12. 并发模型

V1 建议采用保守模型：

- 单线程写
- `Undo/Redo` 与普通写共享同一写通道
- Observer 回调线程固定
- 后台计算优先基于稳定版本或快照读取

不建议 V1 直接支持任意线程写入 `Record`。

## 13. 版本号机制

为了支持缓存失效、局部刷新和后台计算，需要数据库级版本号。

建议：

- 数据库持有全局递增 `Version`
- 每次 `Commit/Undo/Redo` 后版本号递增
- `ChangeSet` 带版本号
- `Record` 可查询最近修改版本

这能支持：

- UI 判断是否过期
- 计算结果缓存失效
- 后台任务合并结果

## 14. 查询与索引预留

仅支持 `GetRecord(id)` 不足以支撑算量产品。

建议在接口层预留：

- 按字段过滤
- 按条件遍历
- 按关系反查
- 字段索引
- 基础排序与分页能力

V1 可先只实现最小可用能力，但接口设计不要把系统锁死为“只能按 id 访问”。

## 15. 数据表编辑工具建议

未来如果提供数据表编辑工具，建议采用两层列模型：

### 15.1 事实列

直接绑定存储层 Schema 字段。

特点：

- 可编辑
- 可持久化
- 参与事务
- 进入 Undo/Redo
- 进入 ChangeSet

### 15.2 派生列

绑定计算表达式、规则结果或缓存结果。

特点：

- 通常只读
- 不直接写入事实层
- 可由 `ChangeSet` 驱动重新计算
- 可根据版本号失效

这样既能支持用户新增属性列，也不会把事实层与计算层混成一层。

## 16. 示例接口

```cpp
class IDatabase : public IRefObject
{
public:
    virtual long BeginEdit(const wchar_t* name, EditPtr& outEdit) = 0;
    virtual long Commit(IEditSession* edit) = 0;
    virtual long Rollback(IEditSession* edit) = 0;

    virtual long Undo() = 0;
    virtual long Redo() = 0;

    virtual long GetTable(const wchar_t* name, TablePtr& outTable) = 0;
    virtual long AddObserver(IDatabaseObserver* observer) = 0;
    virtual long RemoveObserver(IDatabaseObserver* observer) = 0;
};
```

```cpp
class ITable : public IRefObject
{
public:
    virtual long GetRecord(int64_t id, RecordPtr& outRecord) = 0;
    virtual long CreateRecord(RecordPtr& outRecord) = 0;
    virtual long DeleteRecord(int64_t id) = 0;
};
```

```cpp
class IRecord : public IRefObject
{
public:
    virtual long GetInt64(const wchar_t*, int64_t*) = 0;
    virtual long SetInt64(const wchar_t*, int64_t) = 0;

    virtual long GetDouble(const wchar_t*, double*) = 0;
    virtual long SetDouble(const wchar_t*, double) = 0;

    virtual long GetBool(const wchar_t*, bool*) = 0;
    virtual long SetBool(const wchar_t*, bool) = 0;

    virtual long GetString(const wchar_t*, const wchar_t**) = 0;
    virtual long SetString(const wchar_t*, const wchar_t*) = 0;

    virtual long GetRef(const wchar_t*, int64_t*) = 0;
    virtual long SetRef(const wchar_t*, int64_t) = 0;
};
```

## 17. V1 结论

这套设计在算量产品场景下的核心结论是：

- 存储层保存事实，不直接承载全部派生结果
- 用户自定义属性列允许存在，但应纳入受控 Schema
- 关系字段、稳定 `recordId`、事务级 `Undo/Redo` 是必需能力
- `ChangeSet + Version` 同时服务 UI 和增量重算
- 删除、失效句柄、并发模型必须尽早明确

这能让本库同时承担：

- 通用属性存储
- 构件关系管理
- 数据表编辑
- UI 局部刷新
- 后续算量引擎的数据底座
