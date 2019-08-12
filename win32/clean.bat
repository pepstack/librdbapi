@echo off
set _cdir=%cd%

rd /s /q %_cdir%\msvc\build
rd /s /q %_cdir%\msvc\target

del /s /q %_cdir%\msvc\*.sdf
del /s /q %_cdir%\msvc\*.suo