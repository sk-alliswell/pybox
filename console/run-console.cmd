@echo off
cd /d "%~dp0"
pybox.exe manage --open
if errorlevel 1 pause
