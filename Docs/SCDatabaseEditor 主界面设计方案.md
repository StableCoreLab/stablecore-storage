# SCDatabaseEditor 主界面设计方案

## 1. 界面定位

`SCDatabaseEditor` 不按普通 SQLite 浏览器设计，而是定位为：

> StableCore Storage 调试、验证、修复与数据检查工作台。

核心目标：

* 查看和编辑表数据；
* 查看和编辑表结构；
* 检查字段、关系字段、计算列；
* 查看 Edit Log / Journal / Undo / Redo 状态；
* 执行健康检查；
* 导出 Debug Package；
* 辅助验证 Storage 平台行为。

---

## 2. 总体布局

```text
+--------------------------------------------------------------------------------+
| 菜单栏：文件 / 编辑 / 表 / 计算列 / 工具 / 视图                                  |
+--------------------------------------------------------------------------------+
| 主工具栏：打开库 | 新建库 | 备份 | 刷新 | Undo | Redo | 健康检查 | 导出调试包    |
+--------------------------------------------------------------------------------+
| 数据库状态条：路径 / 打开模式 / 当前表 / 记录数 / 过滤状态                      |
+----------------------+--------------------------------------+------------------+
| 左侧对象导航          | 中央表数据区                         | 右侧 Inspector   |
|                      |                                      |                  |
| 数据库对象树          | 当前表标题                           | Tab: 表结构      |
| - Tables             | 表操作工具条                         | Tab: 当前记录    |
|   - Beam             | 过滤框                               | Tab: 计算列      |
|   - Column           | QTableView 表数据                    | Tab: 关系字段    |
| - System             |                                      |                  |
|   - Edit Log         |                                      |                  |
|   - Journal          |                                      |                  |
+----------------------+--------------------------------------+------------------+
| 底部信息区：Tab: 诊断 / 编辑日志 / 健康检查 / SQL预览 / Debug Package           |
+--------------------------------------------------------------------------------+
| 状态栏：当前操作结果 / 错误提示 / 事务状态 / 只读状态                            |
+--------------------------------------------------------------------------------+
```

---

## 3. 区域职责划分

### 3.1 左侧：对象导航区

左侧只负责导航，不负责字段编辑。

推荐使用：

```cpp
QTreeWidget* objectTree_;
```

结构：

```text
Database
├─ Tables
│  ├─ Beam
│  ├─ Column
│  ├─ Wall
│  └─ Slab
├─ Computed Columns
├─ Edit Log
├─ Journal
├─ Snapshots
└─ Diagnostics
```

设计原则：

* 表是一级工作对象；
* 字段不在左侧展开；
* 计算列、日志、诊断作为系统对象入口；
* 左侧选择表后，中央数据区和右侧 Inspector 同步刷新。

---

### 3.2 中央：表数据编辑区

中央区域始终显示当前表数据。

推荐结构：

```text
当前表：Beam    记录数：128    字段数：12

[新增记录] [删除记录] [刷新] [提交修改] [撤销修改]
[过滤：________________________] [清空过滤]

QTableView
Id | Name | Length | Height | RebarId | ...
```

设计原则：

* 中央只承担数据编辑；
* 表头字段只是 Schema 的数据投影；
* 不在中央直接编辑字段类型、约束、计算列定义；
* 点击表头字段时，可以让右侧 Schema Tab 自动高亮对应字段。

---

### 3.3 右侧：Inspector 区

右侧是当前表的属性检查器和结构编辑器。

推荐使用：

```cpp
QDockWidget* inspectorDock_;
QTabWidget* inspectorTabs_;
```

页签结构：

```text
Inspector
├─ 表结构 Schema
├─ 当前记录 Record
├─ 计算列 Computed
└─ 关系字段 Relation
```

---

## 4. 表结构编辑设计

### 4.1 字段显示位置

字段统一显示在：

```text
右侧 Inspector → 表结构 Schema Tab
```

不要放在左侧对象树，也不要做成和数据区互斥的中央页签。

推荐布局：

```text
表结构 Schema

表名：Beam
字段数：12
主键：Id

字段列表：
Name       Type      PK   NotNull   Default   FK   Computed
Id         Integer   √    √
Name       Text           √
Length     Real
RebarId    Integer                       √

[新增字段] [编辑字段] [删除字段]
[转为计算列] [刷新结构]
```

---

### 4.2 字段编辑原则

字段属于 Schema。

中央 `QTableView` 只显示字段投影，不承担结构编辑职责。

设计原则：

```text
字段 Field → 右侧 Schema 编辑
数据 Record → 中央 TableView 编辑
当前行 Record Detail → 右侧 Record Tab 查看
计算列 Computed Column → 右侧 Computed Tab 编辑
```

---

### 4.3 字段点击联动

推荐支持：

```text
点击中央表格列头 Length
→ 右侧 Inspector 自动切到 Schema Tab
→ 高亮 Length 字段
```

这样编辑表结构时仍能看到真实数据，同时能快速定位字段定义。

---

## 5. 当前记录 Inspector

当前记录 Tab 用于查看和编辑当前选中行的字段值。

```text
当前记录 Record

Table：Beam
RecordId：1024

字段名        值
Id            1024
Name          KL1
Length        3000
Height        500
RebarId       32

[复制记录] [定位关系记录] [查看变更历史]
```

作用：

* 查看单条记录完整字段；
* 对长文本、JSON、二进制摘要提供更友好显示；
* 方便查看当前行关联数据；
* 为后续 Edit History / Record Diff 留入口。

---

## 6. 计算列 Inspector

计算列不要和普通字段混在一起编辑。

```text
计算列 Computed

Name             Expression
Volume           Length * Width * Height
Weight           Volume * Density

[新增计算列] [编辑计算列] [删除计算列]
[转为真实字段] [刷新计算结果]
```

设计原则：

* 普通字段和计算列视觉上分开；
* Schema Tab 可显示计算列摘要；
* Computed Tab 负责完整编辑；
* 支持“真实字段 ↔ 计算列”的显式转换。

---

## 7. 关系字段 Inspector

关系字段单独成页，方便检查外键和对象引用。

```text
关系字段 Relation

字段名      目标表       目标字段     状态
RebarId     Rebar        Id           OK
FloorId     Floor        Id           Missing

[定位目标记录] [检查断链] [修复关系]
```

作用：

* 查看当前表的引用关系；
* 检查断链；
* 定位目标记录；
* 为后续数据修复工具预留入口。

---

## 8. 底部信息区

底部统一做成一个 Dock + TabWidget。

```cpp
QDockWidget* bottomDock_;
QTabWidget* bottomTabs_;
```

页签：

```text
Diagnostics
Edit Log
Health Summary
SQL Preview
Debug Package
```

### 8.1 Diagnostics

显示最近错误、警告、操作结果。

### 8.2 Edit Log

显示编辑会话、ChangeList、Undo/Redo 摘要。

### 8.3 Health Summary

显示数据库健康检查结果。

### 8.4 SQL Preview

显示本次操作可能产生的 SQL 或结构变更摘要。

### 8.5 Debug Package

显示调试包导出结果、路径和包含内容。

---

## 9. 顶部数据库状态条

菜单和工具栏下面增加一条状态条。

```text
数据库：D:/xxx/project.scdb
模式：ReadWrite
当前表：Beam
记录数：128
过滤：未启用
事务：空闲
```

推荐成员：

```cpp
QLabel* databasePathLabel_;
QLabel* openModeLabel_;
QLabel* currentTableLabel_;
QLabel* tableStatsLabel_;
QLabel* filterStateLabel_;
QLabel* transactionStateLabel_;
```

作用：

* 当前上下文始终可见；
* 降低误操作；
* 清晰区分只读、读写、事务中、过滤中状态。

---

## 10. 菜单设计

```text
文件 File
- 新建数据库
- 打开数据库
- 关闭数据库
- 创建备份副本
- 导出 Debug Package
- 退出

编辑 Edit
- Undo
- Redo
- 刷新
- 提交修改
- 撤销修改

表 Table
- 新建表
- 删除表
- 重命名表
- 新增记录
- 删除记录
- 新增字段
- 编辑字段
- 删除字段
- 刷新表结构

计算列 Computed Column
- 新增计算列
- 编辑计算列
- 删除计算列
- 字段转计算列
- 计算列转字段
- 刷新计算结果

工具 Tools
- 健康检查
- 编辑日志摘要
- 检查关系断链
- VACUUM
- 导出结构报告

视图 View
- 显示对象导航
- 显示 Inspector
- 显示底部信息区
- 重置布局
```

---

## 11. 工具栏设计

主工具栏只放高频操作：

```text
[打开库] [新建库] [备份]
[刷新]
[Undo] [Redo]
[健康检查]
[导出调试包]
```

当前表工具栏放在中央数据区内部：

```text
[新增记录] [删除记录] [提交修改] [撤销修改]
[新增字段] [编辑字段]
[过滤框]
```

不要把所有按钮都堆到主工具栏。

---

## 12. 推荐成员变量

```cpp
// Navigation
QDockWidget* objectExplorerDock_{nullptr};
QTreeWidget* objectTree_{nullptr};

// Header status
QWidget* databaseStatusBar_{nullptr};
QLabel* databasePathLabel_{nullptr};
QLabel* openModeLabel_{nullptr};
QLabel* currentTableLabel_{nullptr};
QLabel* tableStatsLabel_{nullptr};
QLabel* filterStateLabel_{nullptr};
QLabel* transactionStateLabel_{nullptr};

// Central table area
QWidget* tablePage_{nullptr};
QLabel* tableTitleLabel_{nullptr};
QToolBar* tableToolBar_{nullptr};
QLineEdit* filterEdit_{nullptr};
QTableView* dataTable_{nullptr};

// Inspector
QDockWidget* inspectorDock_{nullptr};
QTabWidget* inspectorTabs_{nullptr};
QTreeWidget* schemaTree_{nullptr};
QTreeWidget* recordTree_{nullptr};
QTreeWidget* computedColumnsTree_{nullptr};
QTreeWidget* relationTree_{nullptr};

// Bottom panel
QDockWidget* bottomDock_{nullptr};
QTabWidget* bottomTabs_{nullptr};
QPlainTextEdit* diagnosticsText_{nullptr};
QTreeWidget* editLogTree_{nullptr};
QPlainTextEdit* healthSummaryText_{nullptr};
QPlainTextEdit* sqlPreviewText_{nullptr};
QPlainTextEdit* debugPackageText_{nullptr};

// Status bar
QLabel* statusLabel_{nullptr};
```

---

## 13. 信号联动设计

### 13.1 选择表

```text
objectTree_ 选中 Table
→ LoadTableData(tableName)
→ LoadTableSchema(tableName)
→ LoadComputedColumns(tableName)
→ LoadRelationFields(tableName)
→ UpdateDatabaseStatusBar()
```

### 13.2 选择数据行

```text
dataTable_ 当前行变化
→ UpdateRecordInspector(recordId)
→ UpdateRelationInspector(recordId)
```

### 13.3 点击表头字段

```text
dataTable_ header clicked
→ inspectorTabs_ 切到 Schema
→ schemaTree_ 高亮对应字段
```

### 13.4 修改字段

```text
点击“编辑字段”
→ 打开字段编辑对话框
→ 生成结构变更操作
→ 执行 Storage 编辑会话
→ 刷新 Schema
→ 刷新 DataView
→ 写入 Diagnostics / SQL Preview
```

---

## 14. 操作边界

### 中央 TableView 可以做

* 编辑单元格值；
* 新增记录；
* 删除记录；
* 排序；
* 过滤；
* 复制数据；
* 定位记录。

### 中央 TableView 不应该做

* 修改字段类型；
* 删除字段；
* 修改主键；
* 修改计算列表达式；
* 修改关系字段定义。

这些全部走右侧 Inspector。

---

## 15. 第一轮改造范围

第一轮不要重写全部逻辑，建议只做结构收口：

1. `tablesList_` 改为 `objectTree_`；
2. 新增顶部数据库状态条；
3. 右侧 `Inspector + Computed Columns` 合并为一个 `QTabWidget`；
4. 底部 `Diagnostics + Edit Log` 合并为一个 `QTabWidget`；
5. 字段显示统一进入 `Schema Tab`；
6. 中央数据区保持当前 `QTableView` 逻辑不大改。

第一轮目标：

> 把界面从“控件堆叠型”改成“数据库工作台型”，先统一布局和职责边界。

---

## 16. 设计原则总结

可以直接写进设计文档：

> 表数据编辑始终位于中央主工作区；表结构编辑统一位于右侧 Inspector 的 Schema 页签；字段作为 Schema 对象统一展示与编辑，中央表格仅作为字段在数据视图中的投影，不承担结构编辑职责。
>
> 左侧对象树只负责数据库对象导航，不展开字段；底部信息区承载诊断、日志、健康检查和 SQL 预览；顶部状态条始终显示当前数据库、表、记录数、打开模式和事务状态。
