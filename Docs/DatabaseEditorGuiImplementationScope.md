# 数据库编辑工具 GUI 当前实现范围

本文档说明当前已经写入仓库、但尚未编译验证的 GUI 编辑工具第一版代码范围。

对应设计文档：

- `Docs/DatabaseEditorGuiDesign.md`

## 1. 当前已实现内容

### 1.1 Qt 6.8 编辑器目标骨架

已在 `CMakeLists.txt` 中加入可选目标：

- `stablecore_storage_db_editor`

当前策略：

- 默认不构建
- 只有启用 `STABLECORE_STORAGE_BUILD_DB_EDITOR=ON` 时才参与工程

### 1.2 主窗口骨架

已实现：

- 主窗口
- 菜单栏
- 工具栏
- 状态栏
- 左侧表列表
- 中间数据表格
- 右侧 schema / 记录检查器
- 底部诊断面板

对应文件：

- `Tools/DatabaseEditor/DatabaseEditorMainWindow.h`
- `Tools/DatabaseEditor/DatabaseEditorMainWindow.cpp`

### 1.3 数据库会话包装层

已实现：

- 创建数据库
- 打开数据库
- 刷新数据库
- 读取表列表
- 选择当前表
- 创建表
- 添加字段
- 添加记录
- 删除记录
- 修改单元格
- 构建健康摘要
- 构建 schema 快照
- 构建记录详情快照

对应文件：

- `Tools/DatabaseEditor/DatabaseSession.h`
- `Tools/DatabaseEditor/DatabaseSession.cpp`

### 1.4 数据网格模型

已实现：

- 表记录枚举
- 列定义读取
- 事实列显示
- 计算列显示
- 可编辑事实列写回
- 行记录 id 映射

对应文件：

- `Tools/DatabaseEditor/RecordTableModel.h`
- `Tools/DatabaseEditor/RecordTableModel.cpp`

### 1.5 新增字段对话框

已实现：

- 字段名
- 显示名
- 类型
- 关系字段开关
- nullable / editable / indexed / participatesInCalc 等属性
- 默认值输入

对应文件：

- `Tools/DatabaseEditor/AddColumnDialog.h`
- `Tools/DatabaseEditor/AddColumnDialog.cpp`

### 1.6 存储 API 补充

为了让 GUI 可以列出数据库中的表，已补充：

- `IDatabase::GetTableCount(...)`
- `IDatabase::GetTableName(...)`

并在以下后端实现：

- `Src/Memory/MemoryDatabase.cpp`
- `Src/Sqlite/SqliteAdapter.cpp`

## 2. 当前实现的交互语义

当前编辑器实现遵循已确认方案：

- 一库一窗口
- 每次单个用户编辑动作，立即形成一次独立存储提交
- 每次提交都应对应一条 Undo 日志

当前代码层面对应操作包括：

- 添加记录
- 删除记录
- 修改单元格
- GUI 触发的 `Undo`
- GUI 触发的 `Redo`

## 3. 当前版本能做什么

按已写代码，第一版 GUI 目标是支持你做原始数据定义：

- 打开或新建 SQLite 数据库
- 查看当前数据库中的表
- 创建新表
- 给表添加事实列或关系列
- 浏览记录
- 新增记录
- 删除记录
- 修改事实字段
- 修改关系字段的原始值
- 查看 schema 详情
- 查看记录详情
- 查看数据库健康摘要
- 直接执行 Undo / Redo

## 4. 当前还没有实现的 GUI 能力

这些仍属于下一轮工作：

- 关系选择器对话框
  当前关系字段仍按原始 `recordId` 值编辑
- 计算列编辑对话框
  当前工具能显示计算列基础设施，但 GUI 里还没有“增加计算列”的独立对话框
- 多数据库最近记录页
- 布局保存
- 导入导出面板
- 更丰富的过滤、排序、搜索
- 启动诊断历史专用页面
- 专门的 Undo / Redo 历史列表视图

## 5. 当前实现的定位

当前这批代码的定位是：

- 已经从“纯设计”进入“可实现的 GUI 工程骨架”
- 重点先打通原始数据编辑主链路
- 不是完整产品级编辑器

也就是说，这一版代码主要是为了让后续 GUI 工具开发有一个正确且稳定的基础起点。
