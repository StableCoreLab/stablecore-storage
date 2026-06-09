# SCStorage Schema Edit 接口草案

## 1. 目标

为 `SCStorage` 提供一组面向调用方代码的公开接口，用于：

- 以结构化方式创建表
- 以结构化方式修改字段
- 将多步 schema 变更收敛到单次显式事务中
- 保持 Undo / Redo、回滚恢复、Journal 记录与现有语义一致

本文不将 `Tools/DatabaseEditor` 中的 `SC_SCHEMA_TABLE(...)` 文本 DSL 直接提升为核心层 API。
核心层暴露的应是稳定的结构化 schema 数据，而不是工具专用代码格式。

## 2. 背景与问题

当前公开接口已经具备部分 schema 编辑原语：

- `ISCDatabase::CreateTable(...)`
- `ISCTable::GetSchema(...)`
- `ISCSchema::AddColumn(...)`
- `ISCSchema::UpdateColumn(...)`
- `ISCSchema::RemoveColumn(...)`
- `ISCSchema::GetSchemaSnapshot(...)`

现状问题不在于“完全没有 schema 能力”，而在于：

- 调用方必须手工编排 `CreateTable -> GetSchema -> AddColumn...`
- 多步 schema 编辑缺少一个高层、显式、事务化的公共入口
- 工具层已有的“导入/导出表结构”逻辑仍停留在 `Tools/DatabaseEditor`
- 调用方若直接用低层接口组织批量 schema 变更，容易引入 partial success 风险

因此，需要在现有低层原语之上增加一层公共 schema edit helper。

## 3. 设计原则

接口设计必须遵守仓库既有约束：

- 所有 schema 变更必须是显式操作
- 失败不得留下部分成功状态
- rollback 必须完整恢复
- 读取 schema 不得写库
- SQLite 细节不得泄漏到公共 API
- 公共 API 应表达存储语义，而不是 GUI / DSL 语义

由此推导出以下设计原则：

- 保留 `ISCDatabase` / `ISCSchema` 作为低层原语
- 新增 free function 风格的公共 helper，而不是立即扩大 `ISCDatabase` 虚接口面
- `CreateTable` 使用完整 `Snapshot`
- `AlterTable` 使用显式 `Patch`
- 每次 helper 调用内部只开启一次 edit session，并负责整体提交或整体回滚

## 4. 放置位置

建议新增公共头文件：

- `Include/SCSchemaEdit.h`

原因：

- 该能力属于“面向用例的公共编排接口”，而不是最小对象模型本身
- 风格上更接近 `SCBatch.h`
- 可避免把高层 schema orchestration 和 `ISCInterfaces.h` 的底层对象接口耦合在一起

建议新增实现文件：

- `Src/Schema/SCSchemaEdit.cpp`

如果当前仓库不希望新增 `Schema` 目录，也可以接受：

- `Src/Batch/SCSchemaEdit.cpp`

但从职责划分上，独立 `Schema` 目录更清晰。

## 5. 第一阶段范围

第一阶段只解决两个高价值场景：

1. 调用方用代码创建一张完整表
2. 调用方用代码对单表执行字段级 patch

第一阶段不做：

- 公开 DSL 解析器
- 整库级 schema diff
- 约束与索引的细粒度 patch
- 批量多表拓扑迁移
- 自动升级推断

## 6. 头文件草案

建议头文件内容如下：

```cpp
#pragma once

#include <string>
#include <vector>

#include "ISCInterfaces.h"
#include "SCTypes.h"

namespace StableCore::Storage
{
    struct SCTableSchemaPatch
    {
        std::wstring tableName;

        std::vector<SCColumnDef> addColumns;
        std::vector<SCColumnDef> updateColumns;
        std::vector<std::wstring> removeColumns;
    };

    struct SCSchemaEditResult
    {
        bool applied{false};
        VersionId committedVersion{0};

        std::wstring tableName;
        std::vector<std::wstring> addedColumns;
        std::vector<std::wstring> updatedColumns;
        std::vector<std::wstring> removedColumns;
    };

    ErrorCode CreateTableFromSchema(
        ISCDatabase* database,
        const SCTableSchemaSnapshot& schema,
        SCSchemaEditResult* outResult);

    ErrorCode ApplyTableSchemaPatch(
        ISCDatabase* database,
        const SCTableSchemaPatch& patch,
        SCSchemaEditResult* outResult);
}
```

## 7. 类型说明

### 7.1 `SCTableSchemaPatch`

语义：

- `tableName` 表示目标表
- `addColumns` 表示新增列
- `updateColumns` 表示按列名替换定义
- `removeColumns` 表示按列名删除

为什么不直接用 `SCTableSchemaSnapshot` 做更新：

- `Snapshot` 适合完整定义
- `Patch` 适合显式表达意图
- 如果只传完整快照，核心层难以区分“调用方要删除某列”还是“调用方未填写完整”

### 7.2 `SCSchemaEditResult`

第一阶段建议只返回最小可诊断信息：

- 是否成功应用
- 成功提交后的版本号
- 哪些列被 add / update / remove

后续如需增强，可扩展：

- `warnings`
- `diagnostics`
- `changeSetSummary`

但第一阶段不建议在结果对象中直接暴露 SQLite 或 journal 内部细节。

## 8. 语义约束

### 8.1 事务约束

`CreateTableFromSchema(...)` 和 `ApplyTableSchemaPatch(...)` 都必须：

- 在内部开启一次显式 edit session
- 所有步骤成功后统一 `Commit`
- 任一步失败则统一 `Rollback`

不允许：

- 半途写入成功后通过调用方继续补救
- 返回失败但数据库处于部分更新状态

### 8.2 输入校验

调用前必须进行最小校验：

- `database != nullptr`
- `tableName` 非空
- 列名非空
- patch 中无重复列名
- `addColumns` 不得与 `updateColumns` / `removeColumns` 冲突
- `SCColumnDef` 形状必须符合现有 schema 约束

### 8.3 Create 语义

`CreateTableFromSchema(...)` 必须：

- 先校验目标表不存在
- 使用 `ISCDatabase::CreateTable(...)` 创建表
- 再逐列应用 `SCColumnDef`
- 若任一步失败，必须整体回滚，最终表现为“表不存在”

### 8.4 Patch 语义

`ApplyTableSchemaPatch(...)` 必须：

- 先解析目标表并取到 schema
- 依序执行 remove / update / add，或在实现中定义更稳定的顺序
- 整个 patch 对调用方表现为一次原子变更

推荐固定顺序：

1. `removeColumns`
2. `updateColumns`
3. `addColumns`

理由：

- 先 remove 可减少命名冲突
- update 发生在 add 之前，便于保留旧列语义
- add 放在最后更符合“最终补齐结构”的思路

若后续发现 `update` 依赖旧列存在且 remove 可能删掉前置列，可再对 patch 引入更显式的操作序列模型。

### 8.5 Undo / Redo 语义

helper 不应绕开现有历史机制。

预期行为：

- 一次 helper 调用对应一次逻辑 edit session
- 其内部产生的 schema 变化写入现有 journal / changeset
- `Undo()` / `Redo()` 继续以当前数据库语义恢复 schema

### 8.6 读取语义

以下行为必须保持只读：

- `GetSchemaSnapshot(...)`
- 任意面向导出或预览的 schema 查询

新增 helper 不得在“仅查看结构”场景中引入任何写入。

## 9. 使用示例

### 9.1 创建表

```cpp
using namespace StableCore::Storage;

SCTableSchemaSnapshot schema;
schema.table.name = L"Beam";

SCColumnDef id;
id.name = L"Id";
id.displayName = L"Id";
id.valueKind = ValueKind::Int64;
id.nullable = false;
id.defaultValue = SCValue::FromInt64(0);
schema.columns.push_back(id);

SCColumnDef name;
name.name = L"Name";
name.displayName = L"Name";
name.valueKind = ValueKind::String;
name.nullable = false;
name.defaultValue = SCValue::FromString(L"");
schema.columns.push_back(name);

SCSchemaEditResult result;
const ErrorCode rc = CreateTableFromSchema(database, schema, &result);
```

### 9.2 修改字段

```cpp
using namespace StableCore::Storage;

SCTableSchemaPatch patch;
patch.tableName = L"Beam";

SCColumnDef width;
width.name = L"Width";
width.displayName = L"Width";
width.valueKind = ValueKind::Double;
width.nullable = false;
width.defaultValue = SCValue::FromDouble(0.0);
patch.addColumns.push_back(width);

patch.removeColumns.push_back(L"LegacyWidth");

SCSchemaEditResult result;
const ErrorCode rc = ApplyTableSchemaPatch(database, patch, &result);
```

## 10. `cpp` 实现骨架

以下代码只表达结构，不是最终实现：

```cpp
#include "SCSchemaEdit.h"

#include <algorithm>

namespace StableCore::Storage
{
    namespace
    {
        ErrorCode ValidateCreateSchema(const SCTableSchemaSnapshot& schema)
        {
            // 1. table name 非空
            // 2. 列集合非空
            // 3. 列名唯一
            // 4. 列定义形状合法
            return SC_OK;
        }

        ErrorCode ValidatePatch(const SCTableSchemaPatch& patch)
        {
            // 1. table name 非空
            // 2. patch 内部无重复列名
            // 3. add/update/remove 三组之间无冲突
            return SC_OK;
        }

        void ResetResult(SCSchemaEditResult* outResult)
        {
            if (outResult != nullptr)
            {
                *outResult = SCSchemaEditResult{};
            }
        }
    }

    ErrorCode CreateTableFromSchema(
        ISCDatabase* database,
        const SCTableSchemaSnapshot& schema,
        SCSchemaEditResult* outResult)
    {
        ResetResult(outResult);

        if (database == nullptr)
        {
            return SC_E_POINTER;
        }

        ErrorCode rc = ValidateCreateSchema(schema);
        if (Failed(rc))
        {
            return rc;
        }

        SCEditPtr edit;
        rc = database->BeginEdit(L"CreateTableFromSchema", edit);
        if (Failed(rc))
        {
            return rc;
        }

        SCTablePtr table;
        rc = database->CreateTable(schema.table.name.c_str(), table);
        if (Failed(rc))
        {
            database->Rollback(edit.Get());
            return rc;
        }

        SCSchemaPtr tableSchema;
        rc = table->GetSchema(tableSchema);
        if (Failed(rc))
        {
            database->Rollback(edit.Get());
            return rc;
        }

        for (const SCColumnDef& column : schema.columns)
        {
            rc = tableSchema->AddColumn(column);
            if (Failed(rc))
            {
                database->Rollback(edit.Get());
                return rc;
            }
        }

        rc = database->Commit(edit.Get());
        if (Failed(rc))
        {
            database->Rollback(edit.Get());
            return rc;
        }

        if (outResult != nullptr)
        {
            outResult->applied = true;
            outResult->tableName = schema.table.name;
            for (const SCColumnDef& column : schema.columns)
            {
                outResult->addedColumns.push_back(column.name);
            }
        }
        return SC_OK;
    }

    ErrorCode ApplyTableSchemaPatch(
        ISCDatabase* database,
        const SCTableSchemaPatch& patch,
        SCSchemaEditResult* outResult)
    {
        ResetResult(outResult);

        if (database == nullptr)
        {
            return SC_E_POINTER;
        }

        ErrorCode rc = ValidatePatch(patch);
        if (Failed(rc))
        {
            return rc;
        }

        SCEditPtr edit;
        rc = database->BeginEdit(L"ApplyTableSchemaPatch", edit);
        if (Failed(rc))
        {
            return rc;
        }

        SCTablePtr table;
        rc = database->GetTable(patch.tableName.c_str(), table);
        if (Failed(rc))
        {
            database->Rollback(edit.Get());
            return rc;
        }

        SCSchemaPtr schema;
        rc = table->GetSchema(schema);
        if (Failed(rc))
        {
            database->Rollback(edit.Get());
            return rc;
        }

        for (const std::wstring& columnName : patch.removeColumns)
        {
            rc = schema->RemoveColumn(columnName.c_str());
            if (Failed(rc))
            {
                database->Rollback(edit.Get());
                return rc;
            }
        }

        for (const SCColumnDef& column : patch.updateColumns)
        {
            rc = schema->UpdateColumn(column);
            if (Failed(rc))
            {
                database->Rollback(edit.Get());
                return rc;
            }
        }

        for (const SCColumnDef& column : patch.addColumns)
        {
            rc = schema->AddColumn(column);
            if (Failed(rc))
            {
                database->Rollback(edit.Get());
                return rc;
            }
        }

        rc = database->Commit(edit.Get());
        if (Failed(rc))
        {
            database->Rollback(edit.Get());
            return rc;
        }

        if (outResult != nullptr)
        {
            outResult->applied = true;
            outResult->tableName = patch.tableName;
            for (const std::wstring& name : patch.removeColumns)
            {
                outResult->removedColumns.push_back(name);
            }
            for (const SCColumnDef& column : patch.updateColumns)
            {
                outResult->updatedColumns.push_back(column.name);
            }
            for (const SCColumnDef& column : patch.addColumns)
            {
                outResult->addedColumns.push_back(column.name);
            }
        }
        return SC_OK;
    }
}
```

## 11. 实现注意事项

### 11.1 不要简单复用工具层补救逻辑

`Tools/DatabaseEditor/SCDatabaseSession` 中当前存在一些“失败后靠 Undo 或局部恢复补救”的流程。
这类逻辑可保留在工具层，但不应成为公共 helper 的对外契约。

公共 helper 的契约必须更强：

- 内部自行保证事务完整性
- 调用返回失败时，不要求调用方继续补救

### 11.2 第一阶段不要把约束和索引做满

虽然 `SCTableSchemaSnapshot` 已包含：

- `constraints`
- `indexes`

但第一阶段若强行支持完整约束/index patch，会大幅增加：

- diff 策略复杂度
- 回滚路径复杂度
- 兼容性和测试负担

因此建议：

- `CreateTableFromSchema(...)` 第一阶段可先接受完整 `Snapshot`
- 但实现里只可靠支持 `columns`
- 若传入 constraints / indexes，可先明确：
  - 要么拒绝并返回 `SC_E_NOT_SUPPORTED`
  - 要么仅支持当前后端已经稳定实现的子集

### 11.3 `committedVersion` 的赋值

若现有公共接口没有直接暴露“提交后的 version id”，第一阶段可以：

- 先保留字段
- 在实现中填 `0`

等后续存在稳定来源时再补全。

不要为了填这个字段而新增不成熟的公共读取接口。

## 12. 测试建议

第一阶段建议新增或补充以下测试：

- 成功按 schema 创建表
- 创建表时列名重复被拒绝
- 创建表过程中任一列失败会整体回滚，最终表不存在
- patch 成功执行 add / update / remove
- patch 任一步失败会整体回滚
- Undo / Redo 对 helper 产生的 schema 变更仍然有效
- 只读 / open 路径不会触发 schema 写入

测试重点仍然是：

- 正确性
- recoverability
- 无 partial success

## 13. 后续扩展方向

在第一阶段稳定后，可考虑增加：

- `DeleteTableWithSchemaEdit(...)`
- 多表 `SCSchemaEditRequest`
- constraints / indexes patch
- `ReplaceTableSchema(...)`
- 面向调用方的 schema diff helper

但这些都应建立在第一阶段接口稳定之后。

## 14. 结论

建议的公共能力边界是：

- `ISCDatabase` / `ISCSchema`：保留底层原语
- `SCSchemaEdit.h`：新增面向用例的高层 schema helper
- `Tools/DatabaseEditor`：继续承担 DSL 解析、代码生成、GUI 交互

这样可以同时满足：

- 用户代码可直接安全建表、改字段
- 工具层不再独占高层 schema 编排能力
- 核心层不被工具 DSL 绑定
- 事务、回滚、Undo/Redo 语义仍统一收敛在 `SCStorage`

## 15. Constraints / Indexes 支持方案

本节定义第一阶段之后，`CreateTableFromSchema(...)` 和后续 schema patch
如何支持：

- `SCConstraintDef`
- `SCIndexDef`

目标不是一次做成完整 schema diff 引擎，而是在现有列级 helper 之上逐步扩展。

### 15.1 总体策略

推荐分两步推进：

1. 先让 `CreateTableFromSchema(...)` 支持创建时携带 constraints / indexes
2. 再新增面向 `ApplyTableSchemaPatch(...)` 的约束和索引 patch 模型

原因：

- create-path 只需要“从空表建立完整 schema”，实现复杂度最低
- alter-path 需要 diff、冲突检测、排序和更复杂的回滚策略

### 15.2 Create 路径扩展

在 `CreateTableFromSchema(...)` 中，建议把执行顺序固定为：

1. `CreateTable`
2. `AddColumn(...)` for all columns
3. `ApplyConstraint(...)` for all constraints
4. `ApplyIndex(...)` for all indexes
5. `Commit`

推荐顺序原因：

- 约束和索引都依赖列已经存在
- 外键和唯一约束的列引用检查必须在列完整注册后进行
- 索引通常在约束之后更自然，因为部分索引可能是约束派生或语义上依附于约束

需要新增内部 helper：

```cpp
ErrorCode ApplyConstraintToTable(
    ISCTable* table,
    const SCConstraintDef& constraint);

ErrorCode ApplyIndexToTable(
    ISCTable* table,
    const SCIndexDef& index);
```

这两个 helper 第一阶段不必公开，只作为 `SCSchemaEdit.cpp` 内部实现函数。

### 15.3 Patch 模型扩展

建议将 `SCTableSchemaPatch` 扩展为：

```cpp
struct SCTableSchemaPatch
{
    std::wstring tableName;

    std::vector<SCColumnDef> addColumns;
    std::vector<SCColumnDef> updateColumns;
    std::vector<std::wstring> removeColumns;

    std::vector<SCConstraintDef> addConstraints;
    std::vector<std::wstring> removeConstraints;

    std::vector<SCIndexDef> addIndexes;
    std::vector<std::wstring> removeIndexes;
};
```

不建议在第一版 patch 中引入 `updateConstraints` / `updateIndexes`。

更稳妥的策略是：

- 约束更新 = `remove + add`
- 索引更新 = `remove + add`

这样可以避免“原地更新”带来的状态分叉和回滚复杂度。

### 15.4 Patch 执行顺序

支持约束和索引后，推荐执行顺序为：

1. `removeIndexes`
2. `removeConstraints`
3. `removeColumns`
4. `updateColumns`
5. `addColumns`
6. `addConstraints`
7. `addIndexes`

原因：

- 索引通常依赖列和约束，删除时应最先解除依赖
- 约束依赖列，删除时应先于删列
- 新增时反过来，列先准备好，再加约束，再加索引

### 15.5 名称与引用校验

新增支持后，helper 需要在进入写路径前增加如下校验：

- constraint name 唯一
- index name 唯一
- constraint / index 引用的列必须存在于最终 schema
- 外键约束引用表名非空
- 主键、唯一、索引列集合不能为空
- `removeColumns` 不得删除仍被 `addConstraints` / `addIndexes` 之前依赖的列

这里的“最终 schema”应按 patch 执行结果进行推导，而不是只看 patch 输入片段。

推荐新增内部步骤：

```cpp
ErrorCode BuildProjectedSchemaSnapshot(
    ISCTable* table,
    const SCTableSchemaPatch& patch,
    SCTableSchemaSnapshot* outProjected);
```

这个 projected snapshot 只用于校验，不做持久化。

### 15.6 回滚策略

constraints / indexes 支持后，不应改变 helper 的对外契约：

- 任一步失败，整个 helper 返回失败
- 对调用方可见状态必须回到调用前

如果底层现有列操作已进入 active journal，而约束 / 索引操作也进入同一 journal，
则仍优先依赖统一 `Rollback(edit)`。

若某些创建型操作像当前 `CreateTable(...)` 一样不受 active edit 约束，
则必须为这些操作提供显式补偿路径，例如：

- create table 失败后显式 `DeleteTable(...)`
- create index 失败后显式 `DropIndex(...)`

不允许把“调用方再手工清理”当成公共契约的一部分。

### 15.7 对底层接口的要求

要让 constraints / indexes 真正进入 helper 实现，公共层或 backend 至少需要满足其一：

1. `ISCSchema` 扩展出约束/索引增删原语
2. 提供 `ReplaceTableSchema(...)` 式整表 schema 替换入口
3. 提供只在 helper 内部使用的 backend bridge

推荐优先级：

- 优先扩展 `ISCSchema`
- 次选内部 bridge
- 最后才考虑整表替换

原因：

- `ISCSchema` 已经承担 schema 原语职责
- 在其上继续扩展最符合现有对象模型
- 整表替换对历史恢复、数据兼容和差异定位都更重

### 15.8 推荐新增底层原语

若采用 `ISCSchema` 扩展方案，建议增加：

```cpp
virtual ErrorCode AddConstraint(const SCConstraintDef& def) = 0;
virtual ErrorCode RemoveConstraint(const wchar_t* name) = 0;

virtual ErrorCode AddIndex(const SCIndexDef& def) = 0;
virtual ErrorCode RemoveIndex(const wchar_t* name) = 0;
```

第一阶段不建议增加：

- `UpdateConstraint(...)`
- `UpdateIndex(...)`

因为 remove/add 足以表达更新语义。

## 16. 最小调用示例入口

本方案对应的最小调用资料位于：

- `Docs/SchemaEditUsage.md`
- `Examples/SchemaEditExample.cpp`

其中：

- `Docs/SchemaEditUsage.md` 负责说明当前支持范围和使用方式
- `Examples/SchemaEditExample.cpp` 负责提供最小可执行示例

## 17. 实现阶段建议

若后续继续实现 constraints / indexes，建议按以下顺序推进：

1. 扩展 `ISCSchema` 约束/索引原语
2. 为 SQLite backend 增加对应 journal 和 rollback 支持
3. 先打通 `CreateTableFromSchema(...)` 完整 schema create
4. 再扩展 `SCTableSchemaPatch` 和 `ApplyTableSchemaPatch(...)`
5. 最后补充 reopen / undo / redo / failure rollback 测试

不建议直接从 patch support 开始，因为 create-path 的状态空间更小，能更早验证：

- 命名约束模型
- journal 记录形态
- sqlite 元数据持久化路径
- rollback 和 undo/redo 一致性
