@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion

:: ========================================
::  TSE Options Analyzer - Build Script
::  Version: 1.0 | 64-bit EXE Builder
:: ========================================

echo.
echo  ████████╗███████╗███████╗    ██████╗  ██████╗ ███████╗
echo  ╚══██╔══╝██╔════╝██╔════╝    ██╔══██╗██╔═══██╗██╔════╝
echo     ██║   ███████╗█████╗      ██████╔╝██║   ██║███████╗
echo     ██║   ╚════██║██╔══╝      ██╔═══╝ ██║   ██║╚════██║
echo     ██║   ███████║███████╗    ██║     ╚██████╔╝███████║
echo     ╚═╝   ╚══════╝╚══════╝    ╚═╝      ╚═════╝ ╚══════╝
echo.
echo  Building TSE_Options_Analyzer (64-bit EXE)
echo  -------------------------------------------
echo.

:: === CONFIGURATION ===
set "APP_NAME=TSE_Options_Analyzer"
set "ENTRY_POINT=run.py"
set "ICON_PATH=static\favicon.ico"
set "RELEASE_DIR=Release"
set "PYTHON_ARCH=64"

:: === CHECKS ===
echo [1/5] Checking Python %PYTHON_ARCH%-bit...
python -c "import struct, sys; arch = struct.calcsize('P') * 8; sys.exit(0 if arch == %PYTHON_ARCH% else 1)" >nul 2>&1
if %errorlevel% neq 0 (
    echo.
    echo [ERROR] 32-bit Python detected!
    echo [FIX] Install 64-bit Python:
    echo.
    echo    Download: https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe
    echo    [Check] "Add Python to PATH"
    echo.
    pause
    exit /b 1
)
echo [OK] 64-bit Python confirmed

:: Check PyInstaller
echo [2/5] Checking PyInstaller...
pyinstaller --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] PyInstaller not found!
    echo [FIX] Run: pip install pyinstaller
    pause
    exit /b 1
)
echo [OK] PyInstaller ready

:: Check Icon
echo [3/5] Checking icon file...
if not exist "%ICON_PATH%" (
    echo [WARN] Icon not found: %ICON_PATH%
    echo [INFO] Using default icon...
    set "ICON_ARG="
) else (
    echo [OK] Icon found: %ICON_PATH%
    set "ICON_ARG=--icon=%ICON_PATH%"
)
echo.

:: === CLEAN OLD BUILDS ===
echo [4/5] Cleaning old builds...
if exist "build" rmdir /s /q "build" >nul 2>&1
if exist "dist" rmdir /s /q "dist" >nul 2>&1
if exist "%APP_NAME%.spec" del "%APP_NAME%.spec" >nul 2>&1
echo [OK] Cleaned

:: === BUILD EXE ===
echo [5/5] Building executable...
pyinstaller ^
  --onefile ^
  --windowed ^
  --noconsole ^
  --name "%APP_NAME%" ^
  %ICON_ARG% ^
  --add-data "server;server" ^
  --add-data "Scripts;Scripts" ^
  --add-data "templates;templates" ^
  --add-data "static;static" ^
  --add-data "Output;Output" ^
  --hidden-import=server.app ^
  --hidden-import=server.data_updater ^
  --hidden-import=flask ^
  --hidden-import=pandas ^
  --hidden-import=numpy ^
  --hidden-import=openpyxl ^
  --collect-all=flask ^
  "%ENTRY_POINT%"

if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Build failed!
    echo [DEBUG] Check logs above.
    pause
    exit /b 1
)

:: === COPY TO RELEASE ===
if not exist "%RELEASE_DIR%" mkdir "%RELEASE_DIR%" >nul
copy "dist\%APP_NAME%.exe" "%RELEASE_DIR%\" >nul

:: === FINAL CLEANUP ===
rmdir /s /q "build" >nul 2>&1
rmdir /s /q "dist" >nul 2>&1
del "%APP_NAME%.spec" >nul 2>&1

:: === SUCCESS ===
echo.
echo =================================================
echo   SUCCESS! 64-bit EXE Created
echo.
echo   App: %APP_NAME%.exe
echo   Icon: %ICON_PATH%
echo   Output: %RELEASE_DIR%\%APP_NAME%.exe
echo   Size: !size! MB
echo.
echo   Run: %RELEASE_DIR%\%APP_NAME%.exe
echo =================================================
echo.
pause