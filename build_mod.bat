@echo off
setlocal
cd /d "%~dp0"

echo Building mod...
call gradlew.bat build
if errorlevel 1 (
  echo Build failed.
  pause
  exit /b 1
)

echo Build complete. Output in build\libs
pause
endlocal
