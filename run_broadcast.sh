#!/usr/bin/env bash
# ==============================================================================
# Telegram High-Speed Broadcaster Launcher (Linux / macOS)
# ==============================================================================
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

echo "================================================================"
echo "      Telegram High-Speed Broadcaster (25 Users / Second)       "
echo "================================================================"
echo ""
echo "Select an option:"
echo "[1] Start Live Broadcast (25 msg/sec to All Users)"
echo "[2] Dry-Run Simulation (Test Speed and DB without sending)"
echo "[3] Send Test Message to Admin ID (Verify Preview)"
echo "[4] Resume Previous Broadcast"
echo "[5] Exit"
echo ""
read -p "Enter choice [1-5]: " opt

case $opt in
  1)
    echo ""
    echo "⚠️  সতর্কবার্তা: আপনি ডাটাবেজের সকল ইউজারের কাছে ব্রডকাস্ট পাঠাতে যাচ্ছেন!"
    read -p "আপনি কি নিশ্চিত? লাইভ পাঠাতে 'CONFIRM' লিখুন: " confirm
    if [ "$confirm" = "CONFIRM" ]; then
      python3 broadcast.py --rate 25
    else
      echo "[-] ব্রডকাস্ট বাতিল করা হয়েছে।"
    fi
    ;;
  2)
    python3 broadcast.py --rate 25 --dry-run
    ;;
  3)
    read -p "Enter your personal Telegram User ID [Default: 8509322025]: " testid
    testid=${testid:-8509322025}
    python3 broadcast.py --test-user "$testid"
    ;;
  4)
    python3 broadcast.py --rate 25 --resume
    ;;
  *)
    echo "Exiting."
    exit 0
    ;;
esac
