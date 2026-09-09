@echo off
title Telegram Broadcast Admin Controller
chcp 65001 >nul
cd /d "%~dp0"

echo ================================================================
echo      🤖 Telegram Broadcast Remote Controller Bot
echo ================================================================
echo.
echo Control Bot : @brodcastmessage416bot
echo Sender Bot  : @agentaiinvestdailybot
echo Admin ID    : 8509322025
echo.
echo [*] Starting controller bot listener...
python admin_bot.py
pause
