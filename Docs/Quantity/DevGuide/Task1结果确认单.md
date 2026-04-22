# Task 1 结果确认单

## 一、最终新增/修改的公开接口

### 新增对外可见类型
- `SCVersionNodeState`
- `SCOpenMode`
- `SCUpgradeStatus`
- `SCVersionNode`
- `SCMigrationEdge`
- `SCCompatibilityWindow`
- `SCVersionGraph`
- `SCUpgradePlan`
- `SCUpgradeResult`
- `SCOpenDecision`

### 新增对外可见函数/方法
- `ISCDatabase::GetSchemaVersion() const noexcept`
- `CreateSqliteDatabase(const wchar_t* path, bool readOnly, SCDbPtr& outDatabase)`
- `BuildDefaultVersionGraph(SCVersionGraph* outGraph)`
- `BuildVersionGraph(...)`
- `BuildUpgradePlan(...)`
- `EvaluateCompatibilityWindow(...)`
- `EvaluateOpenDecision(...)`
- `GetLatestSupportedSchemaVersion() noexcept`

### 保持兼容的旧接口
- `CreateSqliteDatabase(const wchar_t* path, SCDbPtr& outDatabase)` 保持不变
- `ISCDatabase` 的既有编辑、查询、观察者接口保持不变
- 现有 `BuildMigrationPlan(...)` 保持不变

## 二、只读打开语义确认

1. 只读打开不会写 metadata。
2. 只读打开不会触发 migration。
3. 只读打开不会更新 clean shutdown。
4. 只读打开失败时，不会留下持久化副作用。

## 三、OpenDecision 语义确认

- `ReadWrite`
  - schema version 落在可写窗口内
  - `cleanShutdown == true`
- `ReadOnly`
  - 上次关闭非正常，强制只读
  - 或 schema version 超出写窗口，但仍在可读窗口内且允许只读
  - 或 schema version 可读但当前不允许写入，需要保持只读
- `UpgradeRequired`
  - schema version 处于可读窗口内
  - 但低于可写下限
  - 且兼容窗口允许升级
- `Unsupported`
  - schema version 低于最小可读版本
  - 或 schema version 高于可读窗口且不允许只读

## 四、当前最小 Version Graph 范围

- 当前支持的 schema version：`1 -> 2`
- 可读版本：
  - `1`
  - `2`
- 可写版本：
  - `2`
- 必须升级的版本：
  - `1` 在正常关闭且允许升级时
- 直接不支持的版本：
  - 低于 `1`
  - 高于当前兼容窗口且不允许只读

## 五、对产品侧接入的最小调用顺序

1. 先用只读方式打开工程数据库。
2. 读取当前 `schema version`。
3. 构建默认 `Version Graph`。
4. 调用 `EvaluateOpenDecision(...)`。
5. 根据结果进入 `ReadWrite / ReadOnly / UpgradeRequired / Unsupported` 分支。
6. 若进入升级分支，再切换到显式升级流程。

## 六、Task 2 的边界

- Task 2 只允许做：
  - 版本图谱与升级计划的扩展
  - 升级执行链路的补强
  - 兼容窗口的细化
  - 启动期判定接口的补充
- Task 2 不允许顺带做：
  - 导入导出逻辑改造
  - 查询与引用检查改造
  - 权限模型改造
  - Replay 资产模型改造
  - 任意非版本升级相关的产品侧流程改造

