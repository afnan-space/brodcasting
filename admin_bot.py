#!/usr/bin/env python3
"""
================================================================================
🤖 Telegram Remote Control & Admin Broadcast Bot
================================================================================
Control Bot : @brodcastmessage416bot
Sender Bot  : @agentaiinvestdailybot
Admin ID    : 8509322025
================================================================================
Allows the admin to remotely send test messages or trigger high-speed broadcasts
directly from Telegram without touching the terminal!
"""

import os
import sys
import re
import time
import json
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple

import aiohttp
import asyncpg
from dotenv import load_dotenv

# Set UTF-8 encoding for Windows terminal
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

MAIN_BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_BOT_TOKEN = os.getenv("ADMIN_BOT_TOKEN", "").strip()
DB_URL = os.getenv("DATABASE_URL", "").strip()
RATE = int(os.getenv("BROADCAST_RATE", "25"))
ADMIN_USER_IDS_RAW = os.getenv("ADMIN_USER_ID", "8509322025,8829204942")
ADMIN_USER_IDS = [
    int(x.strip()) for x in ADMIN_USER_IDS_RAW.split(",") if x.strip().isdigit()
]

# Import the core broadcaster engine
from broadcast import TelegramBroadcaster, get_clean_neon_dsn, create_db_pool

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, "admin_bot.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("AdminBot")

# State Management
active_broadcast_task: Optional[asyncio.Task] = None
current_broadcaster: Optional[TelegramBroadcaster] = None
pending_broadcast: Optional[Dict[str, Any]] = None
last_progress_edit_time = 0
status_message_id: Optional[int] = None


def parse_button(text: str) -> Tuple[str, Optional[str], Optional[str]]:
    """Extract optional [Button Text | https://url] from the end of a message."""
    pattern = r'\[([^\|\]]+)\|([^\]]+)\]\s*$'
    match = re.search(pattern, text)
    if match:
        btn_text = match.group(1).strip()
        btn_url = match.group(2).strip()
        clean_text = text[:match.start()].strip()
        return clean_text, btn_text, btn_url
    return text.strip(), None, None


class AdminController:
    def __init__(self):
        self.control_api = f"https://api.telegram.org/bot{ADMIN_BOT_TOKEN}"
        self.main_api = f"https://api.telegram.org/bot{MAIN_BOT_TOKEN}"
        self.session: Optional[aiohttp.ClientSession] = None
        self.main_bot_username = ""
        self.control_bot_username = ""

    async def init_bots(self):
        """Fetch usernames of both bots."""
        # Control Bot
        async with self.session.get(f"{self.control_api}/getMe") as resp:
            data = await resp.json()
            if data.get("ok"):
                self.control_bot_username = data["result"].get("username", "")
            else:
                logger.error(f"Failed to connect to Admin Bot: {data}")

        # Main Bot
        async with self.session.get(f"{self.main_api}/getMe") as resp:
            data = await resp.json()
            if data.get("ok"):
                self.main_bot_username = data["result"].get("username", "")
            else:
                logger.error(f"Failed to connect to Main Bot: {data}")

    async def send_control_message(
        self,
        chat_id: int,
        text: str,
        reply_markup: Optional[Dict[str, Any]] = None,
        parse_mode: str = "HTML"
    ) -> Optional[int]:
        """Send a message to the admin via the control bot."""
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        try:
            async with self.session.post(f"{self.control_api}/sendMessage", json=payload) as resp:
                data = await resp.json()
                if data.get("ok"):
                    return data["result"]["message_id"]
                else:
                    logger.error(f"Error sending control message: {data}")
        except Exception as e:
            logger.error(f"Control send exception: {e}")
        return None

    async def edit_control_message(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        reply_markup: Optional[Dict[str, Any]] = None,
        parse_mode: str = "HTML"
    ):
        """Edit a message in the control bot."""
        payload = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        try:
            async with self.session.post(f"{self.control_api}/editMessageText", json=payload) as resp:
                data = await resp.json()
                return data.get("ok")
        except Exception as e:
            logger.error(f"Control edit exception: {e}")
            return False

    async def answer_callback(self, callback_id: str, text: Optional[str] = None):
        """Answer callback query."""
        payload = {"callback_query_id": callback_id}
        if text:
            payload["text"] = text
        try:
            async with self.session.post(f"{self.control_api}/answerCallbackQuery", json=payload) as resp:
                await resp.json()
        except Exception:
            pass

    async def get_photo_url_from_file_id(self, file_id: str) -> Optional[str]:
        """Get direct Telegram download URL for a photo file_id."""
        try:
            async with self.session.get(f"{self.control_api}/getFile", params={"file_id": file_id}) as resp:
                data = await resp.json()
                if data.get("ok"):
                    file_path = data["result"]["file_path"]
                    return f"https://api.telegram.org/file/bot{ADMIN_BOT_TOKEN}/{file_path}"
        except Exception as e:
            logger.error(f"Failed to get photo URL: {e}")
        return None

    async def get_db_user_count(self) -> int:
        """Get total number of valid users in Neon DB."""
        try:
            pool = await create_db_pool(DB_URL)
            async with pool.acquire() as conn:
                count = await conn.fetchval("SELECT COUNT(*) FROM users WHERE telegram_id IS NOT NULL")
            await pool.close()
            return count or 0
        except Exception as e:
            logger.error(f"DB count error: {e}")
            return 0

    async def send_test_via_main(
        self,
        target_id: int,
        text: str,
        photo_url: Optional[str] = None,
        button_text: Optional[str] = None,
        button_url: Optional[str] = None
    ) -> Tuple[bool, str]:
        """Deliver a test message to a user through the Main Bot."""
        sample_user = {
            "telegram_id": target_id,
            "first_name": "Admin",
            "username": "AdminUser"
        }
        broadcaster = TelegramBroadcaster(
            bot_token=MAIN_BOT_TOKEN,
            db_url="",
            dry_run=False
        )
        broadcaster.session = aiohttp.ClientSession()
        try:
            success = await broadcaster.send_single_message(
                user=sample_user,
                message_template=text,
                photo_url=photo_url,
                button_text=button_text,
                button_url=button_url
            )
            return success, "Success" if success else "Failed to deliver"
        except Exception as e:
            return False, str(e)
        finally:
            await broadcaster.session.close()

    async def handle_start(self, chat_id: int):
        total_users = await self.get_db_user_count()
        admins_display = ", ".join(str(uid) for uid in ADMIN_USER_IDS)
        msg = (
            f"👋 <b>স্বাগতম এডমিন! এটি আপনার টেলিগ্রাম ব্রডকাস্ট কন্ট্রোল প্যানেল।</b>\n\n"
            f"🤖 <b>মেইন সেন্ডার বট:</b> @{self.main_bot_username}\n"
            f"👥 <b>ডাটাবেজে মোট ইউজার:</b> <code>{total_users:,}</code> জন\n"
            f"⚡ <b>স্পিড লিমিট:</b> <code>25 msg/s</code>\n"
            f"🛡️ <b>অথোরাইজড এডমিন আইডি:</b> <code>{admins_display}</code>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📌 <b>সহজ কমান্ড নির্দেশিকা:</b>\n\n"
            f"1️⃣ <b>টেস্ট মেসেজ পাঠানো (শুধু নিজের আইডিতে):</b>\n"
            f"<code>/test আপনার মেসেজ...</code>\n\n"
            f"2️⃣ <b>নির্দিষ্ট ইউজারের আইডিতে টেস্ট পাঠানো:</b>\n"
            f"<code>/test 8509322025 আপনার মেসেজ...</code>\n\n"
            f"3️⃣ <b>সকল ইউজারের কাছে লাইভ ব্রডকাস্ট পাঠানো:</b>\n"
            f"<code>/broadcast আপনার সম্পূর্ণ মেসেজ...</code>\n"
            f"<i>(এটি দিলে সরাসরি চলে যাবে না, আগে কনফার্মেশন বাটন আসবে)</i>\n\n"
            f"4️⃣ <b>স্ট্যাটাস চেক করা:</b>\n"
            f"<code>/status</code>\n\n"
            f"5️⃣ <b>চলমান ব্রডকাস্ট বাতিল/থামানো:</b>\n"
            f"<code>/stop</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🖼️ <b>ছবি/ব্যানার পাঠাতে:</b> যেকোনো ছবি এই চ্যাটে আপলোড করে ক্যাপশনে <code>/test</code> বা <code>/broadcast</code> লিখে সেন্ড করুন।\n\n"
            f"🔘 <b>বাটন যুক্ত করতে:</b> মেসেজের একদম শেষে <code>[Button Text | Link]</code> লিখে দিন।"
        )
        await self.send_control_message(chat_id, msg)

    async def handle_status(self, chat_id: int):
        global current_broadcaster, active_broadcast_task
        total_users = await self.get_db_user_count()
        status_text = f"📊 <b>সিস্টেম স্ট্যাটাস:</b>\n\n"
        status_text += f"• <b>মেইন বট:</b> @{self.main_bot_username}\n"
        status_text += f"• <b>কন্ট্রোল বট:</b> @{self.control_bot_username}\n"
        status_text += f"• <b>ডাটাবেজ ইউজার:</b> <code>{total_users:,}</code> জন\n"
        status_text += f"• <b>কনফিগার করা স্পিড:</b> <code>{RATE} msg/s</code>\n\n"

        if current_broadcaster and active_broadcast_task and not active_broadcast_task.done():
            stats = current_broadcaster.stats
            processed = stats.get("processed", 0)
            total = stats.get("total_users", total_users) or 1
            percent = (processed / total) * 100
            sent = stats.get("sent", 0)
            blocked = stats.get("blocked", 0)
            elapsed = int(time.time() - stats.get("start_time", time.time()))
            speed = processed / elapsed if elapsed > 0 else 0

            status_text += (
                f"🔴 <b>ব্রডকাস্ট বর্তমানে রানিং আছে:</b>\n"
                f"• অগ্রগতি: <code>{processed:,} / {total:,}</code> ({percent:.1f}%)\n"
                f"• সফলভাবে ডেলিভার: <code>{sent:,}</code>\n"
                f"• ব্লকড/অকার্যকর: <code>{blocked:,}</code>\n"
                f"• গড় স্পিড: <code>{speed:.1f} msg/s</code>\n"
                f"• অতিবাহিত সময়: <code>{str(timedelta(seconds=elapsed))}</code>\n\n"
                f"<i>থামাতে চাইলে /stop কমান্ড দিন।</i>"
            )
        else:
            status_text += "🟢 <b>কোনো লাইভ ব্রডকাস্ট এখন রানিং নেই। ব্রডকাস্ট করতে /broadcast লিখুন।</b>"

        await self.send_control_message(chat_id, status_text)

    async def handle_test(
        self,
        chat_id: int,
        raw_text: str,
        photo_url: Optional[str] = None
    ):
        """Process /test command."""
        # Check if first word after /test is a user ID
        parts = raw_text.split(maxsplit=1)
        target_id = chat_id
        message_body = ""

        if len(parts) > 0 and parts[0].isdigit() and len(parts[0]) >= 8:
            target_id = int(parts[0])
            message_body = parts[1] if len(parts) > 1 else ""
        else:
            message_body = raw_text

        if not message_body and not photo_url:
            await self.send_control_message(
                chat_id,
                "⚠️ অনুগ্রহ করে টেস্ট মেসেজের জন্য কিছু টেক্সট বা ছবি দিন!\n\n"
                "উদাহরণ:\n<code>/test হ্যালো! এটি একটি টেস্ট মেসেজ।</code>\n"
                "বা নির্দিষ্ট আইডিতে:\n<code>/test 8509322025 হ্যালো!</code>"
            )
            return

        clean_text, btn_text, btn_url = parse_button(message_body)

        status_msg_id = await self.send_control_message(
            chat_id,
            f"⏳ @{self.main_bot_username} থেকে ইউজার <code>{target_id}</code>-এ টেস্ট মেসেজ পাঠানো হচ্ছে..."
        )

        success, err = await self.send_test_via_main(
            target_id=target_id,
            text=clean_text,
            photo_url=photo_url,
            button_text=btn_text,
            button_url=btn_url
        )

        if success:
            res_text = (
                f"✅ <b>টেস্ট মেসেজ সফলভাবে ডেলিভার হয়েছে!</b>\n\n"
                f"• <b>টার্গেট আইডি:</b> <code>{target_id}</code>\n"
                f"• <b>প্রেরক বট:</b> @{self.main_bot_username}\n"
            )
            if btn_text:
                res_text += f"• <b>ইনলাইন বাটন:</b> {btn_text} ({btn_url})\n"
            res_text += f"\n👉 আপনার টেলিগ্রাম চ্যাট চেক করে প্রিভিউ দেখে নিন।"
        else:
            res_text = (
                f"❌ <b>টেস্ট মেসেজ পাঠানো যায়নি!</b>\n\n"
                f"• কারণ / এরর: <code>{err}</code>\n"
                f"• নিশ্চিত করুন ইউজার পূর্বে @{self.main_bot_username} বটটি অন্তত একবার /start করেছে।"
            )

        if status_msg_id:
            await self.edit_control_message(chat_id, status_msg_id, res_text)
        else:
            await self.send_control_message(chat_id, res_text)

    async def handle_broadcast_request(
        self,
        chat_id: int,
        raw_text: str,
        photo_url: Optional[str] = None
    ):
        """Prepare broadcast preview and ask for explicit confirmation."""
        global pending_broadcast, active_broadcast_task
        if active_broadcast_task and not active_broadcast_task.done():
            await self.send_control_message(
                chat_id,
                "⚠️ <b>ইতিমধ্যে একটি ব্রডকাস্ট চলমান রয়েছে!</b>\n"
                "অগ্রগতি দেখতে <code>/status</code> দিন অথবা থামাতে <code>/stop</code> দিন।"
            )
            return

        clean_text, btn_text, btn_url = parse_button(raw_text)

        if not clean_text and not photo_url:
            await self.send_control_message(
                chat_id,
                "⚠️ অনুগ্রহ করে ব্রডকাস্ট করার জন্য মেসেজ টেক্সট লিখুন!\n\n"
                "উদাহরণ:\n<code>/broadcast হ্যালো মেম্বাররা! আজ ২৫% বোনাস অফার চলছে।</code>"
            )
            return

        total_users = await self.get_db_user_count()
        est_seconds = int(total_users / RATE) if RATE > 0 else 0
        est_time_str = str(timedelta(seconds=est_seconds))

        pending_broadcast = {
            "text": clean_text,
            "photo_url": photo_url,
            "button_text": btn_text,
            "button_url": btn_url,
            "total_users": total_users,
            "created_at": time.time()
        }

        preview_msg = (
            f"📢 <b>ব্রডকাস্ট প্রিভিউ ও চূড়ান্ত কনফার্মেশন</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🤖 <b>প্রেরক বট:</b> @{self.main_bot_username}\n"
            f"👥 <b>টার্গেট প্রাপক:</b> <code>{total_users:,}</code> জন ইউজার\n"
            f"⚡ <b>স্পিড:</b> <code>{RATE} msg/second</code>\n"
            f"⏱️ <b>আনুমানিক সময়:</b> <code>~{est_time_str}</code>\n"
        )
        if photo_url:
            preview_msg += f"🖼️ <b>ফটো ব্যানার:</b> সংযুক্ত করা হয়েছে\n"
        if btn_text:
            preview_msg += f"🔘 <b>ইনলাইন বাটন:</b> [{btn_text} | {btn_url}]\n"

        preview_msg += (
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📝 <b>মেসেজ প্রিভিউ:</b>\n\n"
            f"{clean_text}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️ <b>আপনি কি ডাটাবেজের সকল {total_users:,} ইউজারের কাছে এই মেসেজটি পাঠাতে চান?</b>\n"
            f"<i>নিচের বাটনে চাপ দিয়ে নিশ্চিত করুন:</i>"
        )

        keyboard = {
            "inline_keyboard": [
                [
                    {"text": "🚀 হ্যাঁ, ব্রডকাস্ট শুরু করুন", "callback_data": "confirm_broadcast"},
                    {"text": "❌ বাতিল করুন", "callback_data": "cancel_broadcast"}
                ]
            ]
        }

        await self.send_control_message(chat_id, preview_msg, reply_markup=keyboard)

    async def execute_live_broadcast(self, chat_id: int):
        """Run the actual live broadcast pipeline with live Telegram status updates."""
        global pending_broadcast, current_broadcaster, active_broadcast_task, status_message_id

        if not pending_broadcast:
            await self.send_control_message(chat_id, "❌ কোনো ব্রডকাস্ট পেন্ডিং নেই। নতুন করে /broadcast লিখুন।")
            return

        data = pending_broadcast
        pending_broadcast = None

        text = data["text"]
        photo_url = data.get("photo_url")
        btn_text = data.get("button_text")
        btn_url = data.get("button_url")
        total_users = data["total_users"]

        # Send initial tracking message
        status_msg_id = await self.send_control_message(
            chat_id,
            f"🚀 <b>ব্রডকাস্ট শুরু হয়েছে!</b>\n"
            f"• মোট টার্গেট: <code>{total_users:,}</code> জন\n"
            f"• লাইভ স্ট্যাটাস নিচে রিয়েলটাইমে আপডেট হচ্ছে...",
            reply_markup={
                "inline_keyboard": [[{"text": "⏹️ ব্রডকাস্ট থামান (/stop)", "callback_data": "stop_broadcast"}]]
            }
        )
        status_message_id = status_msg_id

        last_edit_time = 0

        async def progress_updater(stats: Dict[str, Any]):
            nonlocal last_edit_time
            now = time.time()
            # Edit every 4 seconds to respect Telegram rate limits
            if now - last_edit_time < 4.0:
                return
            last_edit_time = now

            processed = stats.get("processed", 0)
            total = stats.get("total_users", total_users) or 1
            percent = (processed / total) * 100
            sent = stats.get("sent", 0)
            blocked = stats.get("blocked", 0)
            failed = stats.get("failed", 0)
            elapsed = int(time.time() - stats.get("start_time", time.time()))
            avg_speed = processed / elapsed if elapsed > 0 else 0
            remaining_users = max(0, total - processed)
            eta_seconds = int(remaining_users / avg_speed) if avg_speed > 0 else 0

            status_card = (
                f"📊 <b>লাইভ ব্রডকাস্ট প্রগ্রেস:</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"👥 <b>অগ্রগতি:</b> <code>{processed:,} / {total:,}</code> (<b>{percent:.1f}%</b>)\n"
                f"✅ <b>সফলভাবে পাঠানো:</b> <code>{sent:,}</code>\n"
                f"🚫 <b>ব্লকড/ডিলিটেড:</b> <code>{blocked:,}</code>\n"
                f"⚠️ <b>ব্যর্থ:</b> <code>{failed:,}</code>\n"
                f"⚡ <b>বর্তমান গতি:</b> <code>{avg_speed:.1f} msg/s</code>\n"
                f"⏱️ <b>অতিবাহিত সময়:</b> <code>{str(timedelta(seconds=elapsed))}</code>\n"
                f"⏳ <b>আনুমানিক বাকি সময়:</b> <code>~{str(timedelta(seconds=eta_seconds))}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━"
            )

            keyboard = {
                "inline_keyboard": [[{"text": "⏹️ ব্রডকাস্ট থামান (/stop)", "callback_data": "stop_broadcast"}]]
            }
            if status_msg_id:
                await self.edit_control_message(chat_id, status_msg_id, status_card, reply_markup=keyboard)

        # Instantiate Broadcaster
        broadcaster = TelegramBroadcaster(
            bot_token=MAIN_BOT_TOKEN,
            db_url=DB_URL,
            rate=RATE,
            dry_run=False,
            order="desc",
            limit=None,
            progress_callback=progress_updater,
            skip_confirm=True  # Already confirmed via Telegram button
        )
        current_broadcaster = broadcaster

        try:
            await broadcaster.run(
                message_template=text,
                photo_url=photo_url,
                button_text=btn_text,
                button_url=btn_url,
                initial_offset=0
            )

            # Final summary
            stats = broadcaster.stats
            elapsed = int(time.time() - stats.get("start_time", time.time()))
            avg_speed = stats["processed"] / elapsed if elapsed > 0 else 0

            if broadcaster.stop_requested:
                final_text = (
                    f"⏹️ <b>ব্রডকাস্ট থামানো হয়েছে (Paused):</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"• প্রসেসড: <code>{stats['processed']:,} / {stats['total_users']:,}</code>\n"
                    f"• সফলভাবে ডেলিভার: <code>{stats['sent']:,}</code>\n"
                    f"• ব্লকড: <code>{stats['blocked']:,}</code>\n"
                    f"• মোট সময়: <code>{str(timedelta(seconds=elapsed))}</code>\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"<i>অগ্রগতি সেভ করা হয়েছে। পরবর্তীতে আবার চালু করতে পারবেন।</i>"
                )
            else:
                final_text = (
                    f"🎉 <b>ব্রডকাস্ট সফলভাবে সম্পন্ন হয়েছে!</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"• মোট প্রাপক: <code>{stats['processed']:,}</code> জন\n"
                    f"• সফলভাবে ডেলিভার: <code>{stats['sent']:,}</code>\n"
                    f"• ব্লকড/অকার্যকর: <code>{stats['blocked']:,}</code>\n"
                    f"• ব্যর্থ: <code>{stats['failed']:,}</code>\n"
                    f"• গড় স্পিড: <code>{avg_speed:.1f} msg/s</code>\n"
                    f"• মোট সময় লেগেছে: <code>{str(timedelta(seconds=elapsed))}</code>\n"
                    f"━━━━━━━━━━━━━━━━━━━━"
                )

            if status_msg_id:
                await self.edit_control_message(chat_id, status_msg_id, final_text)
            else:
                await self.send_control_message(chat_id, final_text)

        except Exception as e:
            logger.error(f"Live broadcast failed: {e}", exc_info=True)
            await self.send_control_message(chat_id, f"❌ <b>ব্রডকাস্টে ত্রুটি দেখা দিয়েছে:</b> <code>{e}</code>")
        finally:
            current_broadcaster = None
            active_broadcast_task = None

    async def handle_stop(self, chat_id: int):
        """Stop/pause active broadcast."""
        global current_broadcaster
        if current_broadcaster and not current_broadcaster.stop_requested:
            current_broadcaster.stop_requested = True
            await self.send_control_message(
                chat_id,
                "⏹️ <b>ব্রডকাস্ট থামানোর নির্দেশ দেওয়া হয়েছে...</b>\n"
                "বর্তমান ব্যাচটি ডেলিভার শেষ করে স্বয়ংক্রিয়ভাবে সেভ হয়ে বন্ধ হবে।"
            )
        else:
            await self.send_control_message(chat_id, "ℹ️ বর্তমানে কোনো ব্রডকাস্ট চালু নেই।")

    async def poll_updates(self):
        """Continuous long-polling loop for the control bot."""
        offset = 0
        logger.info(f"Admin Controller Bot running as @{self.control_bot_username}...")
        print(f"\n==================================================================")
        print(f"  🤖 Admin Control Bot : @{self.control_bot_username}")
        print(f"  📢 Main Sender Bot   : @{self.main_bot_username}")
        print(f"  🛡️ Admin User IDs    : {', '.join(str(i) for i in ADMIN_USER_IDS)}")
        print(f"  ⚡ Target Rate       : {RATE} msg/second")
        print(f"==================================================================")
        print("Ready and listening for Telegram commands...\n")

        while True:
            try:
                params = {"offset": offset, "timeout": 30}
                async with self.session.get(f"{self.control_api}/getUpdates", params=params, timeout=aiohttp.ClientTimeout(total=45)) as resp:
                    data = await resp.json()
                    if not data.get("ok"):
                        await asyncio.sleep(2)
                        continue

                    updates = data.get("result", [])
                    for upd in updates:
                        offset = upd["update_id"] + 1
                        await self.process_update(upd)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Polling loop error: {e}")
                await asyncio.sleep(3)

    async def process_update(self, upd: Dict[str, Any]):
        """Handle individual update from Telegram."""
        global pending_broadcast, active_broadcast_task

        # Handle Callback Queries (Button clicks)
        if "callback_query" in upd:
            cb = upd["callback_query"]
            cb_id = cb["id"]
            user_id = cb["from"]["id"]
            chat_id = cb["message"]["chat"]["id"]
            action = cb.get("data", "")

            # Security Guard: Only Admins
            if user_id not in ADMIN_USER_IDS:
                await self.answer_callback(cb_id, "⛔ Unauthorized: Access Denied")
                return

            if action == "confirm_broadcast":
                await self.answer_callback(cb_id, "🚀 ব্রডকাস্ট শুরু হচ্ছে...")
                active_broadcast_task = asyncio.create_task(self.execute_live_broadcast(chat_id))

            elif action == "cancel_broadcast":
                pending_broadcast = None
                await self.answer_callback(cb_id, "বাতিল করা হয়েছে")
                await self.edit_control_message(
                    chat_id,
                    cb["message"]["message_id"],
                    "❌ <b>ব্রডকাস্ট বাতিল করা হয়েছে।</b>\nকোনো ইউজারের কাছে মেসেজ পাঠানো হয়নি।"
                )

            elif action == "stop_broadcast":
                await self.answer_callback(cb_id, "থামানো হচ্ছে...")
                await self.handle_stop(chat_id)
            return

        # Handle Messages
        if "message" not in upd:
            return

        msg = upd["message"]
        user_id = msg.get("from", {}).get("id")
        chat_id = msg.get("chat", {}).get("id")

        if not user_id or not chat_id:
            return

        # Strict Security: Ignore anyone other than authorized Admins
        if user_id not in ADMIN_USER_IDS:
            logger.warning(f"Unauthorized access attempt from User ID: {user_id}")
            await self.send_control_message(
                chat_id,
                f"⛔ <b>অননুমোদিত প্রবেশাধিকার (Access Denied)</b>\n"
                f"আপনার আইডি: <code>{user_id}</code> এই বটের এডমিন নয়।"
            )
            return

        # Extract text and photo
        text = (msg.get("text") or msg.get("caption") or "").strip()
        photo_url = None

        if "photo" in msg and msg["photo"]:
            best_photo = msg["photo"][-1]
            photo_url = await self.get_photo_url_from_file_id(best_photo["file_id"])

        if not text and not photo_url:
            return

        # Routing Commands
        if text.startswith("/start") or text.startswith("/help"):
            await self.handle_start(chat_id)

        elif text.startswith("/status"):
            await self.handle_status(chat_id)

        elif text.startswith("/stop") or text.startswith("/cancel"):
            await self.handle_stop(chat_id)

        elif text.startswith("/test"):
            # Extract content after /test
            content = text[5:].strip()
            await self.handle_test(chat_id, content, photo_url=photo_url)

        elif text.startswith("/broadcast"):
            # Extract content after /broadcast
            content = text[10:].strip()
            await self.handle_broadcast_request(chat_id, content, photo_url=photo_url)

        else:
            # Helpful fallback
            await self.send_control_message(
                chat_id,
                "ℹ️ বুঝতে পারিনি। কমান্ড দিন:\n"
                "• <code>/test আপনার মেসেজ</code> (টেস্ট দেখতে)\n"
                "• <code>/broadcast আপনার মেসেজ</code> (ব্রডকাস্ট করতে)\n"
                "• <code>/status</code> (স্ট্যাটাস দেখতে)\n"
                "• বিস্তারিত দেখতে: <code>/help</code>"
            )


async def main():
    if not ADMIN_BOT_TOKEN:
        print("❌ [ERROR] ADMIN_BOT_TOKEN missing in .env!")
        return
    if not MAIN_BOT_TOKEN:
        print("❌ [ERROR] BOT_TOKEN missing in .env!")
        return

    controller = AdminController()
    connector = aiohttp.TCPConnector(limit=20, keepalive_timeout=60)
    controller.session = aiohttp.ClientSession(connector=connector)

    try:
        await controller.init_bots()
        await controller.poll_updates()
    finally:
        if controller.session:
            await controller.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] Admin Bot stopped by user.")
    except Exception as e:
        logger.error(f"Fatal error in Admin Bot: {e}", exc_info=True)
        print(f"\n❌ [FATAL ERROR] {e}")
