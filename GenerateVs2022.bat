@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
if "%SCRIPT_DIR:~-1%"=="\" set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"

set "BUILD_DIR=%SCRIPT_DIR%\BuildVs2022"

if not exist "%BUILD_DIR%" (
    mkdir "%BUILD_DIR%"
)

cmake -S "%SCRIPT_DIR%" -B "%BUILD_DIR%" -G "Visual Studio 17 2022" -A x64
if errorlevel 1 (
    echo.
    echo Failed to generate Visual Studio 2022 solution.
    exit /b 1
)

echo.
echo Solution generated successfully:
echo   %BUILD_DIR%\stablecore_storage.sln

endlocal
