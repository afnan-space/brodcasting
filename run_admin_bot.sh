#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

echo "================================================================"
echo "     🤖 Telegram Broadcast Remote Controller Bot               "
echo "================================================================"
echo "Control Bot : @brodcastmessage416bot"
echo "Sender Bot  : @agentaiinvestdailybot"
echo "Admin ID    : 8509322025"
echo ""
python3 admin_bot.py
