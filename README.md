# StableCore Storage

本文件为 Storage 模块的总体说明，采用统一中文术语：

- 表（Table）
- 记录（Record）
- 列（Column，通常称“列”）
- 计算列（Computed / 计算列）
- 变更集（ChangeSet）
- 撤销/重做（Undo/Redo）

目标：描述模块的定位、核心概念、构建与使用说明以及 V1 范围。

## 概要

目前实现要点：

- 公共 V1 头文件位于 `Include`，安装后位于 `Include/Storage`
- 生成共享库目标 `SCStorage`（Windows 为 `.dll`）
- 内存型数据库（支持事务、Schema、撤销/重做、变更集与关系列）
- SQLite 持久化后端（位于 `Src/Sqlite`）
- 在公共类型中提供计算列元数据支持，供上层表工具使用
- 简要的计算运行时：表达式、ruleId、缓存模型
- 统一的表视图（合并事实列与计算列）
- 批量编辑/导入辅助工具
- 迁移、启动恢复、索引物化与诊断辅助
- 示例：`Examples/MemoryExample.cpp`、`Examples/ProductIntegrationExample.cpp`
- 测试：`Tests/` 下包含基础测试
- 可选的 Qt 数据库编辑器：`Tools/DatabaseEditor`

示例：内存数据库快速用法（C++）：

```cpp
#include "SCStorage.h"

using namespace StableCore::Storage;

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
width.defaultValue = SCValue::FromInt64(0);
schema->AddColumn(width);

EditPtr edit;
db->BeginEdit(L"Create Beam", edit);

RecordPtr beam;
beamTable->CreateRecord(beam);
beam->SetInt64(L"Width", 300);

db->Commit(edit.Get());
```

说明：计算列（ComputedColumn）由上层展示或计算模块管理；事实列（SchemaColumn）由存储层持久化。

## 构建说明

生成脚本（默认 Visual Studio）：

```bat
Storage\GenerateStorageVs2022.bat
```

默认选项说明：

- `SCStorage` 生成为共享库
- 数据库编辑器默认不会参与构建，需要显式开启

数据库编辑器依赖 Qt 6.8 Widgets。运行前请在环境中设置其中一个变量：

```bat
set QT6_8_x64=C:\Qt\6.8.0\msvc2022_64
```

或：

```bat
set CMAKE_PREFIX_PATH=C:\Qt\6.8.0\msvc2022_64
```

或：

```bat
set Qt6_DIR=C:\Qt\6.8.0\msvc2022_64\lib\cmake\Qt6
```

若未正确配置 Qt，CMake 在生成编辑器目标时会失败。`CMakeLists.txt` 与 `GenerateStorageVs2022.bat` 都支持读取 `QT6_8_x64` 并自动推导 `Qt6_DIR`。

## 核心概念（统一术语）

- Database（数据库）：管理表、事务、版本与观察者。
- Table（表）：管理记录、Schema 与基础查询。
- Record（记录）：事实数据载体，具有稳定的 `recordId`。
- Column（列）：表中的字段，区分事实列（持久化）与计算列（派生、只读或会话级）。
- EditSession / Transaction（事务）：包裹写操作，提交后生成 `ChangeSet`。
- Journal（日志）：用于记录事务内变更以支持撤销/重做和恢复。

## 设计定位与分层

本库定位为算量产品的事实数据存储引擎（Storage），不包含上层领域模型或完整计算引擎。推荐的分层：

1. Storage：保存事实数据、关系数据、事务与版本信息。
2. Model/Domain：将 `Record` 映射到业务对象（如 Beam、Wall、Floor）。
3. Calc Engine：基于事实数据产生派生结果（计算列、汇总等）。
4. UI/View：订阅变更并做局部刷新。

## V1 范围（必需能力）

V1 应覆盖：

1. 事实数据持久化存储
2. 稳定的 `recordId`
3. 事务与撤销/重做（Undo/Redo）
4. 关系列支持
5. 受控的 Schema 管理
6. 变更通知机制
7. 全局版本管理
8. 用户可扩展的事实列
9. 删除语义与回滚支持
10. 基础查询与索引支持

V1 不涵盖：ORM、UI 绑定实现、分布式写或通用 SQL 引擎。

## 对象职责（摘要）

- `Database`：管理表、事务、版本与观察者。
- `Table`：管理记录集合、Schema 与查询。
- `Record`：持有具体事实值与 `recordId`。
- `Column`：列定义与元信息（名称、显示名、类型、单位、默认值、是否可空、是否用户定义、是否为关系列、是否参与索引/计算等）。
- `EditSession`：事务上下文。
- `Journal`：事务内变更记录。
- `ChangeSet`：提交后的变更集合，用于 UI 与增量重算。

## 事务与撤销/重做

写操作必须在事务内进行，提交后生成 `ChangeSet` 并广播通知。撤销/重做以事务为单位。

示例：

```cpp
EditPtr edit;
pDb->BeginEdit(L"修改梁宽度", edit);

TablePtr table;
pDb->GetTable(L"Beam", table);

RecordPtr record;
table->GetRecord(beamId, record);

record->SetInt64(L"Width", 300);

pDb->Commit(edit.Get());
```

## Schema 与列分类

建议将列分为两类：

- SchemaColumn（事实列）：进入存储层并持久化。
- ComputedColumn（计算列）：由上层计算或会话维护，通常为只读显示字段。

字段元信息建议包含：名称、显示名、类型、单位、默认值、是否可空、是否用户定义、是否为关系列、是否参与索引/计算等。

## 值类型（建议）

初始支持：`Int64`、`Double`、`Bool`、`String`、`Null`、`RecordId`、`Enum`（或受控字符串）；可扩展到 `RecordIdList`、`BinaryRef`、`DateTime`、`Json` 等。

原则：不要用字符串伪装布尔、枚举或引用；关系值应与普通字符串严格区分；字段类型一经注册不随意变更。

## 关系模型

V1 至少支持：单引用、多引用（`RecordIdList` 或关系表）、关系变更通知与基础反查。

## 诊断与健康检查

提供基本的启动诊断、健康摘要与变更集描述，便于 UI 与工具定位问题。

## 参考文件与目录

- 头文件：源码树为 `Include`，安装后为 `Include/Storage`
- 源码：`Src/`
- 测试：`Tests/`
- 示例：`Examples/`
- 数据库编辑器：`Tools/DatabaseEditor`（见其 README）

---

如需我现在把 `Storage/Docs` 下的文档也按相同术语和风格再快速扫描并做小幅统一，请回复“继续”。

### 10.2 数据库级 Observer

只提供数据库�?Observer，不提供表级或记录级 Observer�?

```cpp
class IDatabaseObserver
{
public:
    virtual void OnDatabaseChanged(const ChangeSet&) = 0;
};
```

原则�?

- 存储层负责广�?
- 过滤逻辑�?Model/Domain �?

### 10.3 ChangeSet 设计建议

`ChangeSet` 不仅用于 UI，还要能服务�?

- 模型缓存失效
- 增量重算
- 查询索引更新
- 结果缓存更新

因此建议�?`DataChange` 中体现：

- 变更类型
- 表名
- `recordId`
- 字段�?
- 旧�?/ 新�?
- 是否关系变化
- 变更来源
- 版本�?

建议增加�?

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

删除必须有稳定且可恢复的语义�?

V1 建议�?

- 删除�?`recordId` 不复�?
- 删除后旧句柄进入 `invalid/tombstone`
- `Undo` 恢复时沿用原 `recordId`
- 默认不自动做级联删除
- 引用一致性校验由上层业务或专门规则层处理

这样可以避免算量场景中的关系悬挂被静默吞掉�?

## 12. 并发模型

V1 建议采用保守模型�?

- 单线程写
- `Undo/Redo` 与普通写共享同一写通道
- Observer 回调线程固定
- 后台计算优先基于稳定版本或快照读�?

不建�?V1 直接支持任意线程写入 `Record`�?

## 13. 版本号机�?

为了支持缓存失效、局部刷新和后台计算，需要数据库级版本号�?

建议�?

- 数据库持有全局递增 `Version`
- 每次 `Commit/Undo/Redo` 后版本号递增
- `ChangeSet` 带版本号
- `Record` 可查询最近修改版�?

这能支持�?

- UI 判断是否过期
- 计算结果缓存失效
- 后台任务合并结果

## 14. 查询与索引预�?

仅支�?`GetRecord(id)` 不足以支撑算量产品�?

建议在接口层预留�?

- 按字段过�?
- 按条件遍�?
- 按关系反�?
- 字段索引
- 基础排序与分页能�?

V1 可先只实现最小可用能力，但接口设计不要把系统锁死为“只能按 id 访问”�?

## 15. 数据表编辑工具建�?

未来如果提供数据表编辑工具，建议采用两层列模型：

### 15.1 事实�?

直接绑定存储�?Schema 字段�?

特点�?

- 可编�?
- 可持久化
- 参与事务
- 进入 Undo/Redo
- 进入 ChangeSet

### 15.2 派生�?

绑定计算表达式、规则结果或缓存结果�?

特点�?

- 通常只读
- 不直接写入事实层
- 可由 `ChangeSet` 驱动重新计算
- 可根据版本号失效

这样既能支持用户新增属性列，也不会把事实层与计算层混成一层�?

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

这套设计在算量产品场景下的核心结论是�?

- 存储层保存事实，不直接承载全部派生结�?
- 用户自定义属性列允许存在，但应纳入受�?Schema
- 关系字段、稳�?`recordId`、事务级 `Undo/Redo` 是必需能力
- `ChangeSet + Version` 同时服务 UI 和增量重�?
- 删除、失效句柄、并发模型必须尽早明�?

这能让本库同时承担：

- 通用属性存�?
- 构件关系管理
- 数据表编�?
- UI 局部刷�?
- 后续算量引擎的数据底�?
