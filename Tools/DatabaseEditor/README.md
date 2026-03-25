# StableCore Database Editor

`StableCore Database Editor` 是基于 `Qt Widgets` 的桌面工具，用于直接操作 `stablecore_storage` 的 SQLite 数据库。

当前实现重点是：

- 新建和打开 SQLite 数据库
- 创建表
- 添加事实列和关系列
- 浏览、新增、删除、编辑记录
- 通过关系选择器填写关系字段
- 查看 Schema、选中记录和健康摘要
- 为当前会话临时新增、编辑、删除计算列

## 构建要求

- CMake 3.24+
- Visual Studio 2022 或其他可用的 C++20 编译器
- Qt 6.8 `Widgets` 开发包

默认情况下数据库编辑器不会参与构建，需要显式开启：

```powershell
cmake -S stablecore-storage -B stablecore-storage\build-db-editor `
  -DSTABLECORE_STORAGE_BUILD_DB_EDITOR=ON `
  -DSTABLECORE_STORAGE_BUILD_TESTS=OFF `
  -DCMAKE_PREFIX_PATH=C:\Qt\6.8.0\msvc2022_64
cmake --build stablecore-storage\build-db-editor --config Release --target stablecore_storage_db_editor
```

如果 CMake 提示找不到 `Qt6Config.cmake`，请检查：

- `Qt 6.8` 是否已安装
- `CMAKE_PREFIX_PATH` 是否指向 Qt 安装前缀
- 或者显式设置 `Qt6_DIR`

## 启动

构建成功后运行：

```powershell
stablecore-storage\build-db-editor\Release\stablecore_storage_db_editor.exe
```

启动后可通过菜单或工具栏使用：

- `Open` / `Open Database...`
- `New DB` / `New Database...`
- `New Table`
- `Add Column`
- `Add Computed`
- `Add Record`
- `Delete Record`
- `Pick Relation`
- `Undo`
- `Redo`
- `Refresh`

## 基本使用

### 1. 创建数据库

1. 点击 `New Database...`
2. 选择一个 `.sqlite` 文件路径
3. 成功后左侧表列表会显示当前数据库中的表

### 2. 创建表

1. 点击 `Create Table...`
2. 输入表名，例如 `Beam`、`Floor`
3. 创建完成后会自动切换到该表

### 3. 添加列

1. 选中目标表
2. 点击 `Add Column...`
3. 填写列定义
4. 可添加：
   - 事实列
   - 关系列

当前工具更偏向 append-only schema 方式，适合逐步增加列，不支持完整的 schema 重构。

### 4. 编辑记录

1. 选中左侧表
2. 点击 `Add Record`
3. 直接在表格中编辑事实列
4. 选中记录后可在右侧 `Selected Record` 中查看详情

删除记录：

1. 选中一行
2. 点击 `Delete Record`
3. 确认删除

每次新增、删除、改单元格都会作为一次独立动作提交，可使用 `Undo / Redo` 回退。

### 5. 编辑关系字段

关系字段不要求用户手输 `recordId`。

操作方式：

1. 选中一个关系字段单元格
2. 点击 `Pick Relation`
3. 在弹窗里选择目标记录
4. 确认后写入选中记录的 `recordId`

关系选择器会优先尝试使用这些字段作为候选标签：

- `Name`
- `Title`
- `Code`
- `编号`
- `名称`

同时会显示 `RecordId` 和预览字段，便于确认目标记录。

### 6. 过滤和查看

主表格顶部提供过滤框：

- 输入关键字后会实时过滤当前表格视图
- 过滤不会修改数据库
- 排序也只作用于当前视图

右侧面板包括：

- `Schema`
- `Selected Record`
- `Session Computed Columns`
- `Diagnostics`

## 会话级计算列

点击 `Add Computed Column` 或 `Add Session Computed Column...` 可为当前表添加临时计算列。

支持的类型：

- `Expression`
- `Rule`
- `Aggregate`

支持配置：

- `name`
- `displayName`
- `valueKind`
- `cacheable`
- 事实依赖字段
- 关系依赖字段
- 聚合关系和聚合字段

注意：

- 计算列只在当前编辑器会话内有效
- 关闭编辑器后失效
- 不会写回 SQLite 数据库
- `Rule` 类型依赖底层 `ruleId` 注册表，GUI 本身不提供规则实现

编辑和删除：

1. 在右侧 `Session Computed Columns` 面板选中一个计算列
2. 点击 `Edit Computed` 或 `Delete Computed`
3. 编辑会整体替换该列定义；删除会从当前会话视图中移除

当前实现按“同表内 `name` 唯一”定位会话计算列。

## 当前限制

- 关系字段仅支持单值引用
- 过滤是全文本匹配，不支持高级条件构造
- 计算列当前支持新增、编辑、删除，但不支持导入、导出
- 诊断面板当前是摘要文本，不是完整诊断工作台
- 默认 CMake 配置不会构建该工具，需要显式开启 `STABLECORE_STORAGE_BUILD_DB_EDITOR`

## 相关文件

- `Docs/DatabaseEditorGuiDesign.md`
- `Docs/DatabaseEditorGuiImplementationScope.md`
- `Docs/DatabaseEditorComputedColumnLifecycle.md`
- `Tools/DatabaseEditor/DatabaseEditorMainWindow.cpp`
- `Tools/DatabaseEditor/DatabaseSession.cpp`
