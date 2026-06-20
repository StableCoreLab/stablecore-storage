pushd
@echo off
set "ROOT_DIR=%~dp0.."
::cmake --install "%ROOT_DIR%\Build\ScCommonVs2022" --config Release --prefix "%~dp0Extern\stablecore"
cmake --install "%ROOT_DIR%\Build\ScCommonVs2022" --config RelWithDebInfo --prefix "%~dp0Extern\stablecore"
popd