@echo off
title Telegram 25 msg/s Broadcaster
chcp 65001 >nul
cd /d "%~dp0"

echo ================================================================
echo       Telegram High-Speed Broadcaster (25 Users / Second)
echo ================================================================
echo.
set "PY=python"
if exist "..\.venv\Scripts\python.exe" (
    set "PY=..\.venv\Scripts\python.exe"
) else if exist ".venv\Scripts\python.exe" (
    set "PY=.venv\Scripts\python.exe"
)

echo Select an option:
echo [1] Start Live Broadcast (25 msg/sec to All Users)
echo [2] Dry-Run Simulation (Test Speed and DB without sending)
echo [3] Send Test Message to Admin ID (Verify Preview)
echo [4] Resume Previous Broadcast
echo [5] Exit
echo.
set /p opt="Enter choice [1-5]: "

if "%opt%"=="1" (
    echo.
    echo ================================================================
    echo  ⚠️  সতর্কবার্তা: আপনি ডাটাবেজের সকল ইউজারের কাছে ব্রডকাস্ট পাঠাতে যাচ্ছেন!
    echo ================================================================
    set /p confirm="আপনি কি নিশ্চিত? লাইভ পাঠাতে 'CONFIRM' লিখুন: "
    if "%confirm%"=="CONFIRM" (
        echo.
        echo [*] Launching live broadcast...
        %PY% broadcast.py --rate 25
    ) else (
        echo [-] ব্রডকাস্ট বাতিল করা হয়েছে। কোনো ইউজারের কাছে মেসেজ পাঠানো হয়নি।
    )
) else if "%opt%"=="2" (
    echo.
    echo [*] Launching simulation dry-run...
    %PY% broadcast.py --rate 25 --dry-run
) else if "%opt%"=="3" (
    echo.
    set "testid=8509322025"
    set /p inputid="Enter your personal Telegram User ID [Default: 8509322025]: "
    if not "%inputid%"=="" set "testid=%inputid%"
    echo [*] Sending test message to: %testid%
    %PY% broadcast.py --test-user %testid%
) else if "%opt%"=="4" (
    echo.
    echo [*] Resuming previous broadcast...
    %PY% broadcast.py --rate 25 --resume
) else (
    echo Exiting.
    exit /b
)

echo.
pause
