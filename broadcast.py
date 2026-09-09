#!/usr/bin/env python3
"""
================================================================================
🚀 Professional High-Speed Telegram Broadcaster (Neon PostgreSQL Edition)
================================================================================
Features:
  • Strict 25 messages/second rate limiter (Zero risk of burst 429 flood errors).
  • High-concurrency async pipeline (aiohttp + asyncpg).
  • Direct streaming from Neon PostgreSQL (orders latest users first by default).
  • Automatic recovery from 429 Flood Wait (pauses all workers and resumes).
  • Automatic detection and logging of 403 Blocked / Deactivated users.
  • Resumable checkpoints: interrupted broadcasts can be resumed anytime.
  • Supports Text (HTML/Markdown), Photos, and Inline URL Buttons.
  • Single-user test preview mode & dry-run simulation mode.
================================================================================
"""

import os
import sys
import time
import json
import signal
import asyncio
import logging
import argparse
import urllib.parse
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple

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

# ------------------------------------------------------------------------------
# Configuration & Environment Loading
# ------------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(BASE_DIR)

# Priority: brodcasting/.env > root/.env
if os.path.exists(os.path.join(BASE_DIR, ".env")):
    load_dotenv(os.path.join(BASE_DIR, ".env"))
elif os.path.exists(os.path.join(PARENT_DIR, ".env")):
    load_dotenv(os.path.join(PARENT_DIR, ".env"))
else:
    load_dotenv()

DEFAULT_BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DEFAULT_DB_URL = os.getenv("DATABASE_URL", "")
DEFAULT_RATE = int(os.getenv("BROADCAST_RATE", "25"))
DEFAULT_ADMIN_ID = os.getenv("ADMIN_USER_ID", "").split(",")[0].strip()

CHECKPOINT_FILE = os.path.join(BASE_DIR, "broadcast_state.json")
BLOCKED_FILE = os.path.join(BASE_DIR, "blocked_users.txt")
FAILED_FILE = os.path.join(BASE_DIR, "failed_users.txt")
TEMPLATE_FILE = os.path.join(BASE_DIR, "message_template.txt")

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, "broadcast.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("Broadcaster")


# ------------------------------------------------------------------------------
# Neon Database Helpers
# ------------------------------------------------------------------------------
def get_clean_neon_dsn(url: str) -> Tuple[str, Optional[str]]:
    """Clean Neon Postgres URL for asyncpg compatibility."""
    if not url:
        return "", None
    parsed = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(parsed.query)
    ssl_mode = qs.get("sslmode", ["require"])[0]
    clean_url = urllib.parse.urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        "",
        f"sslmode={ssl_mode}",
        ""
    ))
    return clean_url, "require" if ssl_mode == "require" else None


async def create_db_pool(db_url: str) -> asyncpg.Pool:
    """Create asyncpg pool configured for Neon pooler."""
    clean_url, ssl_mode = get_clean_neon_dsn(db_url)
    return await asyncpg.create_pool(
        clean_url,
        min_size=1,
        max_size=5,
        command_timeout=60,
        statement_cache_size=0,  # Required for Neon connection poolers (pgbouncer)
        ssl=ssl_mode
    )


# ------------------------------------------------------------------------------
# High-Precision Asynchronous Rate Limiter
# ------------------------------------------------------------------------------
class StrictRateLimiter:
    """
    Enforces a strict global dispatch rate of N items/second across all workers.
    Releases lock immediately so worker sleep is concurrent.
    """
    def __init__(self, rate: float = 25.0):
        self.rate = rate
        self.interval = 1.0 / rate  # 0.04s for 25 msg/s
        self.lock = asyncio.Lock()
        self.next_scheduled_time = time.monotonic()

    async def acquire(self):
        async with self.lock:
            now = time.monotonic()
            if self.next_scheduled_time <= now:
                self.next_scheduled_time = now + self.interval
                return
            wait_time = self.next_scheduled_time - now
            self.next_scheduled_time += self.interval

        # Sleep outside the lock so other workers can reserve their time slot immediately
        if wait_time > 0:
            await asyncio.sleep(wait_time)


# ------------------------------------------------------------------------------
# Telegram Bulk Broadcaster Class
# ------------------------------------------------------------------------------
class TelegramBroadcaster:
    def __init__(
        self,
        bot_token: str,
        db_url: str,
        rate: int = 25,
        dry_run: bool = False,
        order: str = "desc",
        limit: Optional[int] = None,
        progress_callback: Optional[Any] = None,
        skip_confirm: bool = False
    ):
        self.bot_token = bot_token.strip()
        self.db_url = db_url.strip()
        self.rate = rate
        self.dry_run = dry_run
        self.order = order.lower()
        self.limit = limit
        self.progress_callback = progress_callback
        self.skip_confirm = skip_confirm

        self.api_url = f"https://api.telegram.org/bot{self.bot_token}"
        self.rate_limiter = StrictRateLimiter(rate=float(rate))
        self.pause_event = asyncio.Event()
        self.pause_event.set()  # Starts unpaused

        self.stats = {
            "total_users": 0,
            "processed": 0,
            "sent": 0,
            "blocked": 0,
            "failed": 0,
            "flood_waits": 0,
            "start_time": time.time(),
        }

        self.stop_requested = False
        self.db_pool: Optional[asyncpg.Pool] = None
        self.session: Optional[aiohttp.ClientSession] = None
        self.blocked_file_handle = None
        self.failed_file_handle = None

    def setup_signal_handlers(self):
        """Handle graceful shutdown on Ctrl+C."""
        def handle_exit(signum, frame):
            print("\n[!] Graceful stop requested. Finishing current batch and saving state...")
            self.stop_requested = True

        try:
            signal.signal(signal.SIGINT, handle_exit)
            signal.signal(signal.SIGTERM, handle_exit)
        except Exception:
            pass

    def save_checkpoint(self, offset: int, completed: bool = False):
        """Save progress to broadcast_state.json."""
        state = {
            "completed": completed,
            "offset": offset,
            "processed": self.stats["processed"],
            "sent": self.stats["sent"],
            "blocked": self.stats["blocked"],
            "failed": self.stats["failed"],
            "total_users": self.stats["total_users"],
            "elapsed_seconds": int(time.time() - self.stats["start_time"]),
            "updated_at": datetime.now().isoformat()
        }
        try:
            with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def log_blocked_user(self, user_id: int, reason: str = "blocked"):
        """Log blocked/deactivated user IDs to file."""
        if not self.blocked_file_handle:
            self.blocked_file_handle = open(BLOCKED_FILE, "a", encoding="utf-8")
        self.blocked_file_handle.write(f"{user_id}\t{reason}\t{datetime.now().isoformat()}\n")
        self.blocked_file_handle.flush()

    def log_failed_user(self, user_id: int, error: str):
        """Log failed user IDs to file."""
        if not self.failed_file_handle:
            self.failed_file_handle = open(FAILED_FILE, "a", encoding="utf-8")
        self.failed_file_handle.write(f"{user_id}\t{error}\t{datetime.now().isoformat()}\n")
        self.failed_file_handle.flush()

    def format_message(self, template: str, user: Dict[str, Any]) -> str:
        """Inject user variables into the template."""
        first_name = (user.get("first_name") or "").strip()
        if not first_name:
            first_name = "User"
        username = (user.get("username") or "").strip()
        user_id = str(user.get("telegram_id", ""))

        text = template.replace("{first_name}", first_name)
        text = text.replace("{name}", first_name)
        text = text.replace("{username}", username or first_name)
        text = text.replace("{id}", user_id)
        text = text.replace("{telegram_id}", user_id)
        return text

    async def send_single_message(
        self,
        user: Dict[str, Any],
        message_template: str,
        photo_url: Optional[str] = None,
        button_text: Optional[str] = None,
        button_url: Optional[str] = None
    ) -> bool:
        """Send message to a single user with rate limiting and flood wait recovery."""
        user_id = user["telegram_id"]
        text = self.format_message(message_template, user)

        # Build inline keyboard if requested
        reply_markup = None
        if button_text and button_url:
            reply_markup = {
                "inline_keyboard": [
                    [{"text": button_text.strip(), "url": button_url.strip()}]
                ]
            }

        if self.dry_run:
            # Simulate 25 msgs/sec in dry-run mode
            await self.rate_limiter.acquire()
            self.stats["sent"] += 1
            self.stats["processed"] += 1
            return True

        # Endpoint determination
        if photo_url and photo_url.strip():
            endpoint = f"{self.api_url}/sendPhoto"
            payload = {
                "chat_id": user_id,
                "photo": photo_url.strip(),
                "caption": text,
                "parse_mode": "HTML"
            }
        else:
            endpoint = f"{self.api_url}/sendMessage"
            payload = {
                "chat_id": user_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": False
            }

        if reply_markup:
            payload["reply_markup"] = reply_markup

        max_retries = 3
        for attempt in range(max_retries):
            # Wait if paused due to global flood wait
            await self.pause_event.wait()
            # Acquire slot from rate limiter (25/sec)
            await self.rate_limiter.acquire()

            try:
                async with self.session.post(endpoint, json=payload, timeout=aiohttp.ClientTimeout(total=12)) as resp:
                    data = await resp.json()

                    if resp.status == 200 and data.get("ok"):
                        self.stats["sent"] += 1
                        self.stats["processed"] += 1
                        return True

                    status_code = resp.status
                    err_desc = data.get("description", "Unknown error")

                    # 1. FLOOD WAIT (HTTP 429)
                    if status_code == 429 or "flood" in err_desc.lower() or "too many requests" in err_desc.lower():
                        retry_after = 5
                        params = data.get("parameters", {})
                        if "retry_after" in params:
                            retry_after = int(params["retry_after"])

                        # Pause all workers
                        if self.pause_event.is_set():
                            self.pause_event.clear()
                            self.stats["flood_waits"] += 1
                            logger.warning(f"\n⚠️  [FLOOD WAIT] Telegram rate limit reached. Pausing for {retry_after}s...")
                            await asyncio.sleep(retry_after + 1)
                            self.pause_event.set()
                            logger.info("▶️  [RESUMED] Resuming broadcast...")
                        else:
                            await self.pause_event.wait()

                        continue  # Retry this user

                    # 2. BLOCKED OR DEACTIVATED (HTTP 403)
                    elif status_code == 403 or "blocked" in err_desc.lower() or "deactivated" in err_desc.lower():
                        self.stats["blocked"] += 1
                        self.stats["processed"] += 1
                        self.log_blocked_user(user_id, err_desc)
                        return False

                    # 3. CHAT NOT FOUND (HTTP 400)
                    elif status_code == 400 and ("chat not found" in err_desc.lower() or "user not found" in err_desc.lower()):
                        self.stats["failed"] += 1
                        self.stats["processed"] += 1
                        self.log_failed_user(user_id, err_desc)
                        return False

                    # 4. Other Telegram errors
                    else:
                        if attempt == max_retries - 1:
                            self.stats["failed"] += 1
                            self.stats["processed"] += 1
                            self.log_failed_user(user_id, f"HTTP {status_code}: {err_desc}")
                            return False
                        await asyncio.sleep(1.0 * (attempt + 1))

            except (aiohttp.ClientError, asyncio.TimeoutError) as net_err:
                if attempt == max_retries - 1:
                    self.stats["failed"] += 1
                    self.stats["processed"] += 1
                    self.log_failed_user(user_id, f"Network Error: {net_err}")
                    return False
                await asyncio.sleep(1.5)

        return False

    def print_progress(self):
        """Render live single-line progress dashboard in terminal."""
        processed = self.stats["processed"]
        total = self.stats["total_users"]
        elapsed = time.time() - self.stats["start_time"]
        speed = processed / elapsed if elapsed > 0 else 0.0

        percent = (processed / total * 100) if total > 0 else 0
        bar_len = 22
        filled = int(bar_len * percent // 100)
        bar = "█" * filled + "░" * (bar_len - filled)

        remaining = max(0, total - processed)
        eta_sec = int(remaining / speed) if speed > 0 else 0
        eta_str = str(timedelta(seconds=eta_sec))

        line = (
            f"\r[{percent:5.1f}%] |{bar}| {processed:,}/{total:,} "
            f"🚀 Sent: {self.stats['sent']:,} | 🚫 Blocked: {self.stats['blocked']:,} | "
            f"❌ Fail: {self.stats['failed']:,} | ⚡ {speed:4.1f} msg/s | ⏳ ETA: {eta_str}   "
        )
        sys.stdout.write(line)
        sys.stdout.flush()

    async def run(
        self,
        message_template: str,
        photo_url: Optional[str] = None,
        button_text: Optional[str] = None,
        button_url: Optional[str] = None,
        initial_offset: int = 0
    ):
        """Main broadcast execution loop."""
        self.setup_signal_handlers()

        print("=====================================================================")
        print("  🚀 Telegram High-Speed Broadcaster (25 Users / Second)           ")
        print("=====================================================================")
        print(f" • Mode         : {'🧪 DRY-RUN (Simulation)' if self.dry_run else '🔴 LIVE BROADCAST'}")
        print(f" • Target Speed : {self.rate} messages/second")
        print(f" • DB Order     : {'Latest Joined First (DESC)' if self.order == 'desc' else 'Oldest First (ASC)'}")
        if initial_offset > 0:
            print(f" • Resumed From : Offset {initial_offset:,}")
        print("=====================================================================")

        # 1. Connect to Neon Database
        print("[*] Connecting to Neon PostgreSQL database...")
        try:
            self.db_pool = await create_db_pool(self.db_url)
        except Exception as e:
            print(f"[!] Database connection failed: {e}")
            logger.error(f"DB Connection Error: {e}", exc_info=True)
            return

        # 2. Count Target Users
        async with self.db_pool.acquire() as conn:
            total_in_db = await conn.fetchval("SELECT COUNT(*) FROM users WHERE telegram_id IS NOT NULL")
            print(f"[✓] Total users registered in database: {total_in_db:,}")

        if self.limit and self.limit < total_in_db:
            target_count = self.limit
            print(f"[*] User specified target limit: {target_count:,} users")
        else:
            target_count = total_in_db

        self.stats["total_users"] = target_count

        # Safety Guard: Require explicit confirmation for live broadcast to prevent accidental mass messaging
        if not self.dry_run and not self.skip_confirm:
            print("\n" + "=" * 65)
            print("  ⚠️  ATTENTION: YOU ARE ABOUT TO LAUNCH A LIVE BROADCAST!")
            print(f"  Target: {target_count:,} real users will receive this message.")
            print("=" * 65)
            try:
                confirm = input("Type 'CONFIRM' to start broadcasting to all users (or anything else to abort): ").strip()
            except (EOFError, KeyboardInterrupt):
                confirm = "CANCEL"
            if confirm != "CONFIRM":
                print("[-] Broadcast aborted. No messages were sent to any users.")
                return

        # 3. Setup HTTP Session
        connector = aiohttp.TCPConnector(limit=50, keepalive_timeout=60, enable_cleanup_closed=True)
        self.session = aiohttp.ClientSession(connector=connector)

        # 4. Fetch and Process in Batches
        batch_size = 1000
        offset = initial_offset
        order_clause = "ORDER BY created_at DESC" if self.order == "desc" else "ORDER BY created_at ASC"

        print(f"[*] Starting broadcast pipeline at {self.rate} msg/s...\n")
        self.stats["start_time"] = time.time()

        # Concurrent worker pool matching target rate + slight buffer for network latency
        concurrency = min(self.rate + 5, 35)
        sem = asyncio.Semaphore(concurrency)

        async def worker_task(user_record: Dict[str, Any]):
            async with sem:
                if self.stop_requested:
                    return
                await self.send_single_message(
                    user=user_record,
                    message_template=message_template,
                    photo_url=photo_url,
                    button_text=button_text,
                    button_url=button_url
                )

        try:
            while offset < target_count and not self.stop_requested:
                current_limit = min(batch_size, target_count - offset)
                async with self.db_pool.acquire() as conn:
                    query = f"""
                        SELECT telegram_id, username, first_name, created_at 
                        FROM users 
                        WHERE telegram_id IS NOT NULL
                        {order_clause}
                        LIMIT $1 OFFSET $2
                    """
                    rows = await conn.fetch(query, current_limit, offset)

                if not rows:
                    break

                tasks = []
                for r in rows:
                    user_dict = dict(r)
                    tasks.append(asyncio.create_task(worker_task(user_dict)))

                # Monitor and update terminal progress while batch is executing
                last_cb_time = 0
                while any(not t.done() for t in tasks):
                    self.print_progress()
                    now = time.time()
                    if self.progress_callback and (now - last_cb_time >= 3.0):
                        last_cb_time = now
                        try:
                            if asyncio.iscoroutinefunction(self.progress_callback):
                                await self.progress_callback(dict(self.stats))
                            else:
                                self.progress_callback(dict(self.stats))
                        except Exception:
                            pass
                    await asyncio.sleep(0.3)
                    if self.stop_requested:
                        break

                await asyncio.gather(*tasks, return_exceptions=True)
                offset += len(rows)
                self.save_checkpoint(offset=offset, completed=False)

            self.print_progress()
            print()

            if self.progress_callback:
                try:
                    if asyncio.iscoroutinefunction(self.progress_callback):
                        await self.progress_callback(dict(self.stats))
                    else:
                        self.progress_callback(dict(self.stats))
                except Exception:
                    pass

            if self.stop_requested:
                print("\n[!] Broadcast paused by user. Checkpoint saved. You can resume anytime!")
            else:
                self.save_checkpoint(offset=offset, completed=True)
                print("\n\n=====================================================================")
                print("  🎉 BROADCAST COMPLETED SUCCESSFULLY!                              ")
                print("=====================================================================")
                elapsed = time.time() - self.stats["start_time"]
                print(f" • Total Processed : {self.stats['processed']:,} / {self.stats['total_users']:,}")
                print(f" • Successfully Sent: {self.stats['sent']:,}")
                print(f" • Blocked / Dead   : {self.stats['blocked']:,} (Saved in blocked_users.txt)")
                print(f" • Failed / Invalid : {self.stats['failed']:,} (Saved in failed_users.txt)")
                print(f" • Total Time Taken : {str(timedelta(seconds=int(elapsed)))}")
                avg_speed = self.stats['processed'] / elapsed if elapsed > 0 else 0
                print(f" • Average Speed    : {avg_speed:.2f} messages/second")
                print("=====================================================================")

        finally:
            if self.session:
                await self.session.close()
            if self.db_pool:
                await self.db_pool.close()
            if self.blocked_file_handle:
                self.blocked_file_handle.close()
            if self.failed_file_handle:
                self.failed_file_handle.close()


# ------------------------------------------------------------------------------
# Single Test Message Function
# ------------------------------------------------------------------------------
async def send_test_message(
    bot_token: str,
    test_user_id: int,
    message_text: str,
    photo_url: Optional[str] = None,
    button_text: Optional[str] = None,
    button_url: Optional[str] = None
) -> bool:
    """Send a single preview message to verify formatting and layout."""
    print(f"\n[*] Sending test message to Telegram ID: {test_user_id}...")
    sample_user = {
        "telegram_id": test_user_id,
        "first_name": "Admin",
        "username": "AdminUser"
    }

    broadcaster = TelegramBroadcaster(
        bot_token=bot_token,
        db_url="",
        dry_run=False
    )
    broadcaster.session = aiohttp.ClientSession()

    success = await broadcaster.send_single_message(
        user=sample_user,
        message_template=message_text,
        photo_url=photo_url,
        button_text=button_text,
        button_url=button_url
    )
    await broadcaster.session.close()

    if success:
        print("✅ [SUCCESS] Test message delivered to your Telegram! Check your chat.")
    else:
        print("❌ [FAILED] Could not deliver test message. Verify bot token & chat ID.")
    return success


# ------------------------------------------------------------------------------
# Interactive CLI Runner & Arguments
# ------------------------------------------------------------------------------
def parse_arguments():
    parser = argparse.ArgumentParser(description="Professional High-Speed Telegram Broadcaster (25 msg/s)")
    parser.add_argument("--token", type=str, default=DEFAULT_BOT_TOKEN, help="Telegram Bot Token")
    parser.add_argument("--db", type=str, default=DEFAULT_DB_URL, help="Neon PostgreSQL Database URL")
    parser.add_argument("--rate", type=int, default=DEFAULT_RATE, help="Messages per second (default: 25)")
    parser.add_argument("--message", type=str, default=None, help="Direct message text/HTML")
    parser.add_argument("--message-file", type=str, default=TEMPLATE_FILE, help="Path to message template file")
    parser.add_argument("--photo-url", type=str, default=None, help="Optional image/banner URL to send as photo post")
    parser.add_argument("--button-text", type=str, default=None, help="Optional inline button text")
    parser.add_argument("--button-url", type=str, default=None, help="Optional inline button URL")
    parser.add_argument("--order", type=str, choices=["desc", "asc"], default="desc", help="Fetch order: desc (latest first) or asc")
    parser.add_argument("--limit", type=int, default=None, help="Optional maximum number of users to broadcast to")
    parser.add_argument("--test-user", type=int, nargs="?", const=int(DEFAULT_ADMIN_ID) if DEFAULT_ADMIN_ID else 0, default=None, help="Send a test message to this Telegram ID and exit")
    parser.add_argument("--test", action="store_true", help="Send a test message to the configured ADMIN_USER_ID")
    parser.add_argument("--dry-run", action="store_true", help="Simulate broadcast without actually sending Telegram messages")
    parser.add_argument("--fresh", action="store_true", help="Ignore saved checkpoint and start fresh")
    parser.add_argument("--resume", action="store_true", help="Auto-resume from previous checkpoint")
    parser.add_argument("-y", "--yes", "--skip-confirm", action="store_true", dest="skip_confirm", help="Skip confirmation prompt for automated/CI environments")
    return parser.parse_args()


async def main():
    args = parse_arguments()

    bot_token = args.token or DEFAULT_BOT_TOKEN
    db_url = args.db or DEFAULT_DB_URL

    if not bot_token:
        print("❌ [ERROR] Bot token is missing! Set BOT_TOKEN in .env or pass --token")
        sys.exit(1)

    # Load Message Template
    message_content = ""
    if args.message:
        message_content = args.message
    elif os.path.exists(args.message_file):
        with open(args.message_file, "r", encoding="utf-8") as f:
            message_content = f.read().strip()
    else:
        print(f"❌ [ERROR] Message template not found at '{args.message_file}'. Provide --message or create {TEMPLATE_FILE}")
        sys.exit(1)

    # Mode 1: Single User Test Message
    target_test_user = args.test_user
    if args.test:
        if DEFAULT_ADMIN_ID:
            target_test_user = int(DEFAULT_ADMIN_ID)
        else:
            print("❌ [ERROR] ADMIN_USER_ID not configured in .env! Use --test-user <YOUR_ID>")
            sys.exit(1)
    elif target_test_user == 0 and DEFAULT_ADMIN_ID:
        target_test_user = int(DEFAULT_ADMIN_ID)

    if target_test_user:
        await send_test_message(
            bot_token=bot_token,
            test_user_id=target_test_user,
            message_text=message_content,
            photo_url=args.photo_url,
            button_text=args.button_text,
            button_url=args.button_url
        )
        return

    # Check for Existing Checkpoint
    checkpoint = None
    if not args.fresh and os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not data.get("completed", False):
                    checkpoint = data
        except Exception:
            checkpoint = None

    if checkpoint and not args.resume and not args.fresh and not args.skip_confirm:
        print("\n=====================================================================")
        print("  ⚠️  UNFINISHED BROADCAST DETECTED                                 ")
        print("=====================================================================")
        print(f" • Progress : {checkpoint.get('processed', 0):,} / {checkpoint.get('total_users', 0):,} users processed")
        print(f" • Sent     : {checkpoint.get('sent', 0):,} messages delivered")
        print(f" • Saved at : {checkpoint.get('updated_at', 'Unknown')}")
        print("=====================================================================")
        choice = input("Do you want to RESUME where it stopped? [Y/n] (n = start fresh): ").strip().lower()
        if choice == "n":
            checkpoint = None
            if os.path.exists(CHECKPOINT_FILE):
                os.remove(CHECKPOINT_FILE)

    broadcaster = TelegramBroadcaster(
        bot_token=bot_token,
        db_url=db_url,
        rate=args.rate,
        dry_run=args.dry_run,
        order=args.order,
        limit=args.limit,
        skip_confirm=args.skip_confirm
    )

    initial_offset = 0
    if checkpoint:
        initial_offset = checkpoint.get("offset", 0)
        broadcaster.stats["processed"] = checkpoint.get("processed", 0)
        broadcaster.stats["sent"] = checkpoint.get("sent", 0)
        broadcaster.stats["blocked"] = checkpoint.get("blocked", 0)
        broadcaster.stats["failed"] = checkpoint.get("failed", 0)

    await broadcaster.run(
        message_template=message_content,
        photo_url=args.photo_url,
        button_text=args.button_text,
        button_url=args.button_url,
        initial_offset=initial_offset
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] Process interrupted by user.")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        print(f"\n❌ [FATAL ERROR] {e}")
