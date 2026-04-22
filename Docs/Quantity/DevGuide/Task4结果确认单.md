# Task 4 结果确认单

## 已接入能力
- 普通导出与问题包导出模型分离
- `ExportMode / AssetSelection / RedactionPolicy / PackageSizePolicy / StreamingExportContext`
- 默认普通导出仅 `Project`
- 默认问题包导出不包含全部 `Replay Assets`
- 支持大小上限、脱敏、流式写出

## 已修改范围
- `Storage/Include/ISCDebugPackage.h`
- `Storage/Include/SCErrors.h`
- `Storage/Include/SCStorage.h`
- `Storage/Src/Diagnostics/SCDebugPackage.cpp`
- `Storage/CMakeLists.txt`
- `Quantity/Extern/stablecore-storage/Include/ISCDebugPackage.h`
- `Quantity/Extern/stablecore-storage/Include/SCErrors.h`
- `Quantity/Extern/stablecore-storage/Include/SCStorage.h`
- `Quantity/Source/QuantityApp/CMakeLists.txt`
- `Quantity/Source/QuantityApp/Src/SCQuantityAppBootstrap.cpp`
- `Quantity/Source/Ribbon/Include/Ribbon/SCQuantityRibbonActionIds.h`
- `Quantity/Source/Ribbon/Src/SCQuantityRibbon.cpp`
- `Quantity/Source/Common/Include/Common/SCQuantityUiText.h`
- `Storage/Tools/DatabaseEditor/SCDatabaseSession.h`
- `Storage/Tools/DatabaseEditor/SCDatabaseSession.cpp`
- `Storage/Tools/DatabaseEditor/SCDatabaseEditorMainWindow.h`
- `Storage/Tools/DatabaseEditor/SCDatabaseEditorMainWindow.cpp`

## 默认边界
- 普通导出默认只导出 `Project`
- 问题包导出默认导出 `Project + Diagnostics`
- `System Config / User Config / Replay Assets / Log` 默认不进入问题包
- 流式写出失败后不提交目标文件，避免留下半成品

## 验证结果
- `SCQuantityApp` 构建通过
- `SCStorageDatabaseEditor` 构建通过
- `SCStorage` 库构建通过
- `SCStorageTests` 发现阶段存在运行时启动异常，后续需单独排查测试环境依赖

