# StableCore 数据库编辑器

`StableCore Database Editor` 是 `SCStorage` 的 Qt Widgets 桌面工具，用于浏览、编辑和维护 SQLite 数据库。

## 当前功能

- 创建和打开 SQLite 数据库
- 通过表名创建空表
- 通过 `SC_SCHEMA_TABLE(...)` 文本创建表结构
- 删除表
- 新增普通字段和关系字段
- 浏览、添加、删除、编辑记录
- 查看表结构、当前记录、计算列和关系绑定
- 撤销和重做结构与数据修改
- 导出和导入 CSV
- 导出调试包和备份副本

## 构建要求

- CMake 3.24+
- Visual Studio 2022 或其他支持 C++20 的编译器
- Qt 6.8 Widgets 开发包

数据库编辑器默认不参与构建，需要显式开启：

```powershell
cmake -S Storage -B Build\StorageDbEditor `
  -DSCSTORAGE_BUILD_DB_EDITOR=ON `
  -DSCSTORAGE_BUILD_TESTS=OFF `
  -DCMAKE_PREFIX_PATH=C:\Qt\6.8.0\msvc2022_64
cmake --build Build\StorageDbEditor --config Release --target SCStorageDatabaseEditor
```

如果 `CMake` 找不到 `Qt6Config.cmake`，请检查：

- 是否已安装 Qt 6.8
- `CMAKE_PREFIX_PATH` 是否指向 Qt 安装根目录
- 或者是否显式设置了 `Qt6_DIR`

## 启动

构建完成后可直接运行：

```powershell
Build\StorageDbEditor\Release\SCStorageDatabaseEditor.exe
```

## 创建表

### 通过表名创建

在菜单中选择 `Table -> Create Table...`，即可创建一个空表。

### 通过 schema 文本创建

在菜单中选择 `Table -> Create Table From Schema...`，然后粘贴类似下面的文本：

```cpp
SC_SCHEMA_TABLE(ProjectInfo)
{
    Table("ProjectInfo")
        .Column("GUID", SCType::String)
        .Column("ProjectName", SCType::String)
            .Description("项目名称")
        .Column("ProjectCode", SCType::String)
            .Description("项目编号");
}
```

这会根据 schema 文本创建表和字段。字段描述会作为显示名称导入，若 DSL 中提供默认值，也会一并导入。表描述和主键目前会作为导入提示处理，是否真正持久化取决于底层存储模型。

关系字段也支持显式指定存储列和显示列，例如：

- `.Ref("Floor")`：兼容旧的 `RecordId` 关系形式
- `.Ref("Floor", "Code")`：存储 `Floor.Code`
- `.Ref("Floor", "Code", "Name")`：存储 `Code`，界面显示 `Name`

## 删除表

`Delete Selected Table` 会显式删除当前选择的表。

- 该操作属于破坏性操作，当前不支持撤销
- 使用前请确认表确实不再需要
- 当前行为约束见 [Docs/DatabaseEditorTableDeletionSemantics.md](../../Docs/DatabaseEditorTableDeletionSemantics.md)

## 相关文件

- `Tools/DatabaseEditor/SCDatabaseEditorMainWindow.cpp`
- `Tools/DatabaseEditor/SCDatabaseSession.cpp`
- `Tools/DatabaseEditor/SCSchemaTableImport.cpp`
- `Tools/DatabaseEditor/SCSchemaTableImportDialog.cpp`
