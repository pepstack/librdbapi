@echo off

set _cdir=%cd%
set redis_win_dir=%_cdir%\..\redis-win

echo *
echo * update hiredis windows libs.
echo *

if not exist %redis_win_dir% (
    echo *
    echo * ERROR: redis for windows not found at: %redis_win_dir%
    echo * Download from: https://github.com/microsoftarchive/redis.git
    echo * Make sure rename it to 'redis-win'
    echo *
    pause
) else (
    if not exist %redis_win_dir%\msvs\x64 (
        echo *
        echo * ERROR: redis-win not compiled ! not found: %redis_win_dir%\msvs\x64
        echo *
        pause
    ) else (
        echo *
        echo * INFO: update from redis: %redis_win_dir%
        echo *

        if not exist %_cdir%\lib64\Debug (
            md %_cdir%\lib64\Debug
        )

        if not exist %_cdir%\lib64\Release (
            md %_cdir%\lib64\Release
        )

        copy %redis_win_dir%\msvs\x64\Debug\* %_cdir%\lib64\Debug\
        copy %redis_win_dir%\msvs\x64\Release\* %_cdir%\lib64\Release\

        copy %redis_win_dir%\src\Win32_Interop\*.h %_cdir%\include\Win32_Interop\
        copy %redis_win_dir%\deps\hiredis\*.h %_cdir%\include\hiredis\
        copy %redis_win_dir%\src\fmacros.h %_cdir%\include\hiredis\

        copy %_cdir%\src\hiredis.h %_cdir%\include\hiredis
    )
)
