import os
import gc
import re
import html
import json
import math
import time
import types
import random
import asyncio
import logging
import datetime
import traceback
import subprocess

import asyncpg
from aiogram import Bot, Dispatcher, F, types as aiotypes
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter, TelegramForbiddenError
from pyrogram import Client as PyroClient, raw

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BOT_TOKEN = os.getenv("BOT_TOKEN", "8812733722:AAEFW8oxPPQYyqrqHGtnvS8fTpu3ATxcDbo")
ADMIN_ID = int(os.getenv("ADMIN_ID", "6616272875"))
API_ID = int(os.getenv("API_ID", "26202905"))
API_HASH = os.getenv("API_HASH", "ec9fd909b90288d01befa4f87c8d71c1")
DATABASE_URL = os.getenv("DATABASE_URL")

FFMPEG_BIN = "ffmpeg"
MAX_FILE_SIZE = 300 * 1024 * 1024
PREFS_FILE = "user_prefs.json"
BACKUP_STATS_FILE = "user_stats.json"
TEXTS_FILE = os.getenv("TEXTS_FILE", "bot_texts.json")

BOT_TEXTS = {}


def load_bot_texts():
    global BOT_TEXTS
    candidates = [
        TEXTS_FILE,
        os.path.join(os.path.dirname(__file__), "bot_texts.json"),
        os.path.join(os.path.dirname(__file__), "..", "bot_texts.json"),
        "bot_texts.json"
    ]
    for p in candidates:
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    BOT_TEXTS = json.load(f)
                    logging.info(f"✅ فایل متون با موفقیت بارگذاری شد: {p}")
                    return
            except Exception as e:
                logging.error(f"❌ خطا در خواندن {p}: {e}")
    logging.warning("⚠️ فایل bot_texts.json پیدا نشد!")


load_bot_texts()


def get_text(path: str, default: str = "", **kwargs):
    keys = path.split(".")
    val = BOT_TEXTS
    for k in keys:
        if isinstance(val, dict):
            val = val.get(k)
        else:
            val = None
            break
    if val is None:
        val = default
    if kwargs and isinstance(val, str):
        try:
            return val.format(**kwargs)
        except Exception:
            return val
    return val


def get_random_start_phrase() -> str:
    phrases = get_text("buttons.start_random_phrases", [])
    if isinstance(phrases, list) and phrases:
        return random.choice(phrases)
    return "بزن نریم"


session = AiohttpSession()
bot = Bot(token=BOT_TOKEN, session=session)
dp = Dispatcher(storage=MemoryStorage())

pyro = PyroClient(
    name="bot_engine",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    ipv6=False
)

SUPPORT_MAP = {}
FAILED_JOBS = {}
USER_REQUESTS = {}
DB_POOL = None
PREFS_CACHE = {}


class SupportState(StatesGroup):
    waiting_for_message = State()


class AdminMessageState(StatesGroup):
    waiting_for_user_id = State()
    waiting_for_single_content = State()
    waiting_for_broadcast_content = State()
    waiting_for_custom_limit = State()


def load_backup_stats() -> dict:
    if os.path.exists(BACKUP_STATS_FILE):
        try:
            with open(BACKUP_STATS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"users": {}}
    return {"users": {}}


def save_backup_stats(stats: dict):
    try:
        with open(BACKUP_STATS_FILE, "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.warning(f"Backup save error: {e}")


async def init_db():
    global DB_POOL
    if not DATABASE_URL:
        logging.warning("⚠️ متغیر DATABASE_URL تنظیم نشده؛ از فایل محلی استفاده می‌شود.")
        return

    try:
        DB_POOL = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1,
            max_size=5,
            command_timeout=30
        )
        async with DB_POOL.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS bot_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
            """)
            await conn.execute("""
                INSERT INTO bot_settings (key, value)
                VALUES ('daily_limit_mb', '500')
                ON CONFLICT (key) DO NOTHING;
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS user_stats (
                    user_id BIGINT PRIMARY KEY,
                    name TEXT,
                    username TEXT,
                    total_cost DOUBLE PRECISION DEFAULT 0.0,
                    total_jobs INT DEFAULT 0,
                    today_date DATE,
                    today_mb DOUBLE PRECISION DEFAULT 0.0,
                    max_vid_file_id TEXT,
                    max_vid_cost DOUBLE PRECISION DEFAULT 0.0,
                    max_vid_size_mb DOUBLE PRECISION DEFAULT 0.0,
                    max_vid_date TEXT
                );
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS support_tickets (
                    admin_msg_id BIGINT PRIMARY KEY,
                    user_id BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
        logging.info("✅ دیتابیس متصل شد.")
    except Exception as e:
        logging.error(f"❌ خطا در اتصال به دیتابیس: {e}")


async def get_daily_limit_mb() -> int:
    if DB_POOL:
        try:
            async with DB_POOL.acquire() as conn:
                val = await conn.fetchval("SELECT value FROM bot_settings WHERE key = 'daily_limit_mb';")
                if val:
                    return int(val)
        except Exception:
            pass
    return load_backup_stats().get("daily_limit_mb", 500)


async def set_daily_limit_mb(limit_mb: int):
    if DB_POOL:
        try:
            async with DB_POOL.acquire() as conn:
                await conn.execute("""
                    INSERT INTO bot_settings (key, value)
                    VALUES ('daily_limit_mb', $1)
                    ON CONFLICT (key) DO UPDATE SET value = $1;
                """, str(limit_mb))
        except Exception as e:
            logging.error(f"DB Error set limit: {e}")
    stats = load_backup_stats()
    stats["daily_limit_mb"] = limit_mb
    save_backup_stats(stats)


async def check_and_update_daily_usage(user_id: int, file_size_mb: float) -> tuple[bool, float, int]:
    if user_id == ADMIN_ID:
        return True, 0.0, 0

    limit = await get_daily_limit_mb()
    if limit == 0:
        return True, 0.0, 0

    today = datetime.date.today()
    current_mb = 0.0

    if DB_POOL:
        try:
            async with DB_POOL.acquire() as conn:
                row = await conn.fetchrow("SELECT today_date, today_mb FROM user_stats WHERE user_id = $1;", user_id)
                if row:
                    current_mb = float(row["today_mb"]) if row["today_date"] == today else 0.0
        except Exception:
            pass
    else:
        u = load_backup_stats().get("users", {}).get(str(user_id), {})
        if u.get("today_date") == today.isoformat():
            current_mb = float(u.get("today_mb", 0.0))

    if (current_mb + file_size_mb) > limit:
        return False, current_mb, limit

    return True, current_mb, limit


async def record_job_stats(user_id: int, name: str, username: str, cost: float, file_size_mb: float, file_id: str):
    today = datetime.date.today()
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    if DB_POOL:
        try:
            async with DB_POOL.acquire() as conn:
                await conn.execute("""
                    INSERT INTO user_stats (
                        user_id, name, username, total_cost, total_jobs,
                        today_date, today_mb, max_vid_file_id, max_vid_cost, max_vid_size_mb, max_vid_date
                    )
                    VALUES ($1, $2, $3, $4, 1, $5, $6, $7, $4, $6, $8)
                    ON CONFLICT (user_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        username = EXCLUDED.username,
                        total_cost = COALESCE(user_stats.total_cost, 0.0) + EXCLUDED.total_cost,
                        total_jobs = COALESCE(user_stats.total_jobs, 0) + 1,
                        today_mb = CASE 
                            WHEN user_stats.today_date = EXCLUDED.today_date THEN COALESCE(user_stats.today_mb, 0.0) + EXCLUDED.today_mb
                            ELSE EXCLUDED.today_mb
                        END,
                        today_date = EXCLUDED.today_date,
                        max_vid_file_id = CASE 
                            WHEN EXCLUDED.total_cost >= COALESCE(user_stats.max_vid_cost, 0.0) THEN EXCLUDED.max_vid_file_id
                            ELSE user_stats.max_vid_file_id
                        END,
                        max_vid_cost = CASE 
                            WHEN EXCLUDED.total_cost >= COALESCE(user_stats.max_vid_cost, 0.0) THEN EXCLUDED.total_cost
                            ELSE user_stats.max_vid_cost
                        END,
                        max_vid_size_mb = CASE 
                            WHEN EXCLUDED.total_cost >= COALESCE(user_stats.max_vid_cost, 0.0) THEN EXCLUDED.max_vid_size_mb
                            ELSE user_stats.max_vid_size_mb
                        END,
                        max_vid_date = CASE 
                            WHEN EXCLUDED.total_cost >= COALESCE(user_stats.max_vid_cost, 0.0) THEN EXCLUDED.max_vid_date
                            ELSE user_stats.max_vid_date
                        END;
                """, user_id, name, username, cost, today, file_size_mb, file_id, now_str)
        except Exception as e:
            logging.error(f"DB insert error: {e}")

    stats = load_backup_stats()
    if "users" not in stats:
        stats["users"] = {}

    u_key = str(user_id)
    if u_key not in stats["users"]:
        stats["users"][u_key] = {
            "name": name,
            "username": username,
            "total_cost": 0.0,
            "total_jobs": 0,
            "today_date": today.isoformat(),
            "today_mb": 0.0,
            "max_video": None
        }

    u = stats["users"][u_key]
    u["name"] = name
    u["username"] = username
    u["total_cost"] = round(float(u.get("total_cost", 0.0)) + cost, 6)
    u["total_jobs"] = int(u.get("total_jobs", 0)) + 1

    if u.get("today_date") == today.isoformat():
        u["today_mb"] = round(float(u.get("today_mb", 0.0)) + file_size_mb, 2)
    else:
        u["today_date"] = today.isoformat()
        u["today_mb"] = round(file_size_mb, 2)

    cur_max = u.get("max_video")
    if cur_max is None or cost >= float(cur_max.get("cost", 0.0)):
        u["max_video"] = {
            "file_id": file_id,
            "cost": round(cost, 6),
            "size_mb": round(file_size_mb, 2),
            "date": now_str
        }

    save_backup_stats(stats)


async def register_user(user_id: int, name: str = "", username: str = ""):
    today = datetime.date.today()
    if DB_POOL:
        try:
            async with DB_POOL.acquire() as conn:
                await conn.execute("""
                    INSERT INTO user_stats (user_id, name, username, today_date, total_cost, total_jobs, today_mb)
                    VALUES ($1, $2, $3, $4, 0.0, 0, 0.0)
                    ON CONFLICT (user_id) DO UPDATE SET
                        name = CASE WHEN EXCLUDED.name <> '' THEN EXCLUDED.name ELSE user_stats.name END,
                        username = CASE WHEN EXCLUDED.username <> '' THEN EXCLUDED.username ELSE user_stats.username END;
                """, user_id, name, username, today)
        except Exception:
            pass

    stats = load_backup_stats()
    if "users" not in stats:
        stats["users"] = {}
    u_key = str(user_id)
    if u_key not in stats["users"]:
        stats["users"][u_key] = {
            "name": name,
            "username": username,
            "total_cost": 0.0,
            "total_jobs": 0,
            "today_date": today.isoformat(),
            "today_mb": 0.0,
            "max_video": None
        }
        save_backup_stats(stats)


async def get_all_user_ids() -> list[int]:
    if DB_POOL:
        try:
            async with DB_POOL.acquire() as conn:
                rows = await conn.fetch("SELECT user_id FROM user_stats;")
                if rows:
                    return [r["user_id"] for r in rows]
        except Exception:
            pass
    stats = load_backup_stats()
    return [int(k) for k in stats.get("users", {}).keys()]


async def get_top_users(limit: int = 10) -> list[dict]:
    if DB_POOL:
        try:
            async with DB_POOL.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT user_id, 
                           COALESCE(name, 'کاربر') as name, 
                           COALESCE(username, '') as username, 
                           CAST(COALESCE(total_cost, 0.0) AS FLOAT) as total_cost, 
                           CAST(COALESCE(total_jobs, 0) AS INT) as total_jobs, 
                           CAST(COALESCE(today_mb, 0.0) AS FLOAT) as today_mb,
                           max_vid_file_id, 
                           CAST(COALESCE(max_vid_cost, 0.0) AS FLOAT) as max_vid_cost, 
                           CAST(COALESCE(max_vid_size_mb, 0.0) AS FLOAT) as max_vid_size_mb, 
                           max_vid_date
                    FROM user_stats
                    ORDER BY total_cost DESC NULLS LAST, total_jobs DESC NULLS LAST
                    LIMIT $1;
                """, limit)
                if rows:
                    return [dict(r) for r in rows]
        except Exception as e:
            logging.error(f"Error fetching top users from DB: {e}")

    stats = load_backup_stats()
    users_dict = stats.get("users", {})
    if not users_dict:
        return []

    sorted_u = sorted(users_dict.items(), key=lambda x: float(x[1].get("total_cost", 0.0)), reverse=True)
    res = []
    for uid_str, data in sorted_u[:limit]:
        mv = data.get("max_video") or {}
        res.append({
            "user_id": int(uid_str),
            "name": data.get("name") or "کاربر",
            "username": data.get("username") or "",
            "total_cost": float(data.get("total_cost") or 0.0),
            "total_jobs": int(data.get("total_jobs") or 0),
            "today_mb": float(data.get("today_mb") or 0.0),
            "max_vid_file_id": mv.get("file_id"),
            "max_vid_cost": float(mv.get("cost") or 0.0),
            "max_vid_size_mb": float(mv.get("size_mb") or 0.0),
            "max_vid_date": mv.get("date")
        })
    return res


async def get_user_stat(user_id: int) -> dict | None:
    if DB_POOL:
        try:
            async with DB_POOL.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT user_id, 
                           COALESCE(name, 'کاربر') as name, 
                           COALESCE(username, '') as username, 
                           CAST(COALESCE(total_cost, 0.0) AS FLOAT) as total_cost, 
                           CAST(COALESCE(total_jobs, 0) AS INT) as total_jobs, 
                           CAST(COALESCE(today_mb, 0.0) AS FLOAT) as today_mb,
                           max_vid_file_id, 
                           CAST(COALESCE(max_vid_cost, 0.0) AS FLOAT) as max_vid_cost, 
                           CAST(COALESCE(max_vid_size_mb, 0.0) AS FLOAT) as max_vid_size_mb, 
                           max_vid_date
                    FROM user_stats
                    WHERE user_id = $1;
                """, user_id)
                if row:
                    return dict(row)
        except Exception:
            pass

    u = load_backup_stats().get("users", {}).get(str(user_id))
    if not u:
        return None
    mv = u.get("max_video") or {}
    return {
        "user_id": user_id,
        "name": u.get("name") or "کاربر",
        "username": u.get("username") or "",
        "total_cost": float(u.get("total_cost") or 0.0),
        "total_jobs": int(u.get("total_jobs") or 0),
        "today_mb": float(u.get("today_mb") or 0.0),
        "max_vid_file_id": mv.get("file_id"),
        "max_vid_cost": float(mv.get("cost") or 0.0),
        "max_vid_size_mb": float(mv.get("size_mb") or 0.0),
        "max_vid_date": mv.get("date")
    }


def get_cpu_seconds() -> float:
    try:
        if os.path.exists("/sys/fs/cgroup/cpu.stat"):
            with open("/sys/fs/cgroup/cpu.stat") as f:
                for line in f:
                    if line.startswith("usage_usec"):
                        return int(line.split()[1]) / 1_000_000.0
        elif os.path.exists("/sys/fs/cgroup/cpuacct/cpuacct.usage"):
            with open("/sys/fs/cgroup/cpuacct/cpuacct.usage") as f:
                return int(f.read().strip()) / 1_000_000_000.0
    except Exception:
        pass
    ru = os.times()
    return ru.user + ru.system + ru.children_user + ru.children_system


def init_prefs_cache():
    global PREFS_CACHE
    if os.path.exists(PREFS_FILE):
        try:
            with open(PREFS_FILE, "r", encoding="utf-8") as f:
                PREFS_CACHE = json.load(f)
        except Exception:
            PREFS_CACHE = {}


def save_prefs():
    try:
        with open(PREFS_FILE, "w", encoding="utf-8") as f:
            json.dump(PREFS_CACHE, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def get_user_show_details(user_id: int) -> bool:
    return PREFS_CACHE.get(str(user_id), {}).get("show_details", True)


def set_user_show_details(user_id: int, show_details: bool):
    u_key = str(user_id)
    if u_key not in PREFS_CACHE:
        PREFS_CACHE[u_key] = {}
    PREFS_CACHE[u_key]["show_details"] = show_details
    save_prefs()


def get_user_default_cfg(user_id: int) -> dict:
    base = {
        "mode": "video",
        "res": "720",
        "codec": "h264",
        "crf": "medium",
        "mute": False,
        "speed": "1.0",
        "fmt": "orig"
    }
    user_saved = PREFS_CACHE.get(str(user_id), {}).get("default_cfg", {})
    base.update(user_saved)
    return base


def set_user_default_cfg(user_id: int, cfg: dict):
    u_key = str(user_id)
    if u_key not in PREFS_CACHE:
        PREFS_CACHE[u_key] = {}
    PREFS_CACHE[u_key]["default_cfg"] = cfg
    save_prefs()


async def custom_save_file(self, path, file_id=None, file_part=0, progress=None, progress_args=()):
    if not path:
        return None

    if isinstance(path, (str, bytes, os.PathLike)):
        if not os.path.exists(path):
            return None
        file_size = os.path.getsize(path)
        file_name = os.path.basename(path)
        fp = open(path, "rb")
        should_close = True
    else:
        fp = path
        file_name = getattr(fp, "name", "file.bin")
        try:
            curr_pos = fp.tell()
            fp.seek(0, os.SEEK_END)
            file_size = fp.tell()
            fp.seek(curr_pos)
        except Exception:
            file_size = 0
        should_close = False

    if file_size == 0:
        if should_close:
            fp.close()
        return None

    part_size = 512 * 1024
    total_parts = math.ceil(file_size / part_size)
    fid = file_id or random.randint(1, (1 << 63) - 1)
    is_big = file_size > 10 * 1024 * 1024

    try:
        for part_index in range(total_parts):
            chunk = fp.read(part_size)
            if not chunk:
                break

            for attempt in range(3):
                try:
                    if is_big:
                        await self.invoke(
                            raw.functions.upload.SaveBigFilePart(
                                file_id=fid,
                                file_part=part_index,
                                file_total_parts=total_parts,
                                bytes=chunk
                            )
                        )
                    else:
                        await self.invoke(
                            raw.functions.upload.SaveFilePart(
                                file_id=fid,
                                file_part=part_index,
                                bytes=chunk
                            )
                        )
                    break
                except Exception as err:
                    if attempt == 2:
                        raise err
                    await asyncio.sleep(1)

            if progress:
                curr = min((part_index + 1) * part_size, file_size)
                try:
                    res = progress(curr, file_size, *progress_args)
                    if asyncio.iscoroutine(res):
                        await res
                except Exception:
                    pass
    finally:
        if should_close:
            fp.close()

    if is_big:
        return raw.types.InputFileBig(id=fid, parts=total_parts, name=file_name)
    else:
        return raw.types.InputFile(id=fid, parts=total_parts, name=file_name, md5_checksum="")

pyro.save_file = types.MethodType(custom_save_file, pyro)

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

JOB_QUEUE = asyncio.Queue()
ACTIVE_PROCESSES = {}
RUNNING_TASKS = {}


def get_main_reply_keyboard(user_id: int):
    builder = ReplyKeyboardBuilder()
    builder.button(text=get_text("buttons.reply_keyboard.settings", "⚙️ تنظیمات"))[cite: 1]
    builder.button(text=get_text("buttons.reply_keyboard.support", "📞 ارتباط با پشتیبانی"))[cite: 1]
    if user_id == ADMIN_ID:
        builder.button(text=get_text("buttons.reply_keyboard.admin_panel", "👑 پنل مدیریت"))[cite: 1]
        builder.adjust(2, 1)
    else:
        builder.adjust(2)
    return builder.as_markup(resize_keyboard=True)


def get_settings_inline_keyboard(user_id: int):
    show_details = get_user_show_details(user_id)
    builder = InlineKeyboardBuilder()
    toggle_text = get_text("buttons.settings_menu.status_full") if show_details else get_text("buttons.settings_menu.status_simple")[cite: 1]
    action_text = get_text("buttons.settings_menu.action_toggle_simple") if show_details else get_text("buttons.settings_menu.action_toggle_full")[cite: 1]
    builder.button(text=toggle_text, callback_data="none")
    builder.button(text=action_text, callback_data="toggle_details")
    builder.button(text=get_text("buttons.settings_menu.open_default_settings", "🎬 تنظیمات دیفالت ویدیوها"), callback_data="open_default_settings")[cite: 1]
    builder.button(text=get_text("buttons.settings_menu.close", "پشیمون شدم"), callback_data="close_settings")[cite: 1]
    builder.adjust(1)
    return builder.as_markup()


def get_admin_panel_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text=get_text("buttons.admin_panel.top_users", "🏆 رتبه‌بندی پرمصرف‌ترین‌ها"), callback_data="admin_top_users")[cite: 1]
    builder.button(text=get_text("buttons.admin_panel.set_limit", "⏱ سهمیه مصرف روزانه"), callback_data="admin_set_limit")[cite: 1]
    builder.button(text=get_text("buttons.admin_panel.broadcast", "📢 پیام همگانی به همه"), callback_data="admin_broadcast")[cite: 1]
    builder.button(text=get_text("buttons.admin_panel.send_single", "👤 پیام به کاربر خاص"), callback_data="admin_send_single")[cite: 1]
    builder.button(text=get_text("buttons.admin_panel.close", "پشیمون شدم"), callback_data="admin_close")[cite: 1]
    builder.adjust(1)
    return builder.as_markup()


def generate_progress_bar(percent: float) -> str:
    total_blocks = 12
    filled = int(round((percent / 100) * total_blocks))
    filled = min(total_blocks, max(0, filled))
    return f"[{'█' * filled}{'░' * (total_blocks - filled)}] {percent:.1f}%"


def encode_cfg(mode, res, codec, crf, mute, speed, fmt):
    return f"{mode}:{res}:{codec}:{crf}:{'1' if mute else '0'}:{speed}:{fmt}"


def decode_cfg(data_str):
    p = data_str.split(":")
    return {
        "mode": p[0], "res": p[1], "codec": p[2], "crf": p[3],
        "mute": p[4] == "1", "speed": p[5], "fmt": p[6]
    }


def build_config_keyboard(cfg: dict, orig_ext: str = "mp4"):
    b = InlineKeyboardBuilder()
    mode = cfg["mode"]
    res, codec, crf, mute, speed, fmt = cfg["res"], cfg["codec"], cfg["crf"], cfg["mute"], cfg["speed"], cfg["fmt"]

    b.button(text=get_text("buttons.video_config_keyboard.mode_video", "🎬 تبدیل ویدیو") + (" ✅" if mode == "video" else ""), callback_data="cfg:" + encode_cfg("video", res, codec, crf, mute, speed, "orig" if fmt not in ["mp4", "mkv", "mov"] else fmt))[cite: 1]
    b.button(text=get_text("buttons.video_config_keyboard.mode_audio", "🎵 استخراج صدا (موزیک)") + (" ✅" if mode == "audio" else ""), callback_data="cfg:" + encode_cfg("audio", res, codec, crf, mute, speed, "mp3" if fmt in ["orig", "mp4", "mkv", "mov"] else fmt))[cite: 1]

    if mode == "video":
        fmt_orig_title = get_text("buttons.video_config_keyboard.format_original", orig_ext=orig_ext.upper(), default=f"📁 فرمت خروجی: مثل فایل اصلی ({orig_ext.upper()})")[cite: 1]
        b.button(text=fmt_orig_title + (" ✅" if fmt == "orig" else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, crf, mute, speed, "orig"))
        b.button(text=get_text("buttons.video_config_keyboard.format_mp4", "MP4") + (" ✅" if fmt == "mp4" else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, crf, mute, speed, "mp4"))[cite: 1]
        b.button(text=get_text("buttons.video_config_keyboard.format_mkv", "MKV") + (" ✅" if fmt == "mkv" else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, crf, mute, speed, "mkv"))[cite: 1]
        b.button(text=get_text("buttons.video_config_keyboard.format_mov", "MOV") + (" ✅" if fmt == "mov" else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, crf, mute, speed, "mov"))[cite: 1]

        for r_k in ["orig", "1080", "720", "480"]:
            r_t = get_text(f"buttons.video_config_keyboard.resolutions.{r_k}", r_k)[cite: 1]
            b.button(text=r_t + (" ✅" if res == r_k else ""), callback_data="cfg:" + encode_cfg(mode, r_k, codec, crf, mute, speed, fmt))

        b.button(text=get_text("buttons.video_config_keyboard.codecs.h264", "H.264 (استاندارد)") + (" ✅" if codec == "h264" else ""), callback_data="cfg:" + encode_cfg(mode, res, "h264", crf, mute, speed, fmt))[cite: 1]
        b.button(text=get_text("buttons.video_config_keyboard.codecs.h265", "H.265 (فوق‌العاده کم‌حجم)") + (" ✅" if codec == "h265" else ""), callback_data="cfg:" + encode_cfg(mode, res, "h265", crf, mute, speed, fmt))[cite: 1]

        for c_k in ["light", "medium", "heavy"]:
            c_t = get_text(f"buttons.video_config_keyboard.crf_compression.{c_k}", c_k)[cite: 1]
            b.button(text=c_t + (" ✅" if crf == c_k else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, c_k, mute, speed, fmt))

        mute_btn_text = get_text("buttons.video_config_keyboard.audio_toggle.mute", "🔇 صدا: قطع") if mute else get_text("buttons.video_config_keyboard.audio_toggle.unmute", "🔊 صدا: وصل")[cite: 1]
        b.button(text=mute_btn_text, callback_data="cfg:" + encode_cfg(mode, res, codec, crf, not mute, speed, fmt))
    else:
        for af_k in ["mp3", "wav", "m4a", "ogg", "flac"]:
            af_t = get_text(f"buttons.video_config_keyboard.audio_formats.{af_k}", af_k.upper())[cite: 1]
            b.button(text=af_t + (" ✅" if fmt == af_k else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, crf, mute, speed, af_k))

    for s_k in ["1.0", "1.5", "2.0"]:
        s_t = get_text(f"buttons.video_config_keyboard.speed_options.{s_k}", f"{s_k}x")[cite: 1]
        b.button(text=s_t + (" ✅" if speed == s_k else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, crf, mute, s_k, fmt))

    start_phrase = get_random_start_phrase()
    b.button(text=start_phrase, callback_data=f"run:{encode_cfg(mode, res, codec, crf, mute, speed, fmt)}")
    b.button(text=get_text("buttons.video_config_keyboard.cancel", "پشیمون شدم"), callback_data="cancel_panel")[cite: 1]

    if mode == "video":
        b.adjust(2, 1, 3, 4, 2, 3, 1, 3, 2)
    else:
        b.adjust(2, 3, 2, 3, 2)
    return b.as_markup()


def build_default_config_keyboard(cfg: dict):
    b = InlineKeyboardBuilder()
    mode = cfg["mode"]
    res, codec, crf, mute, speed, fmt = cfg["res"], cfg["codec"], cfg["crf"], cfg["mute"], cfg["speed"], cfg["fmt"]

    b.button(text=get_text("buttons.default_config_keyboard.mode_video", "🎬 تبدیل ویدیو") + (" ✅" if mode == "video" else ""), callback_data="defcfg:" + encode_cfg("video", res, codec, crf, mute, speed, "orig" if fmt not in ["mp4", "mkv", "mov"] else fmt))[cite: 1]
    b.button(text=get_text("buttons.default_config_keyboard.mode_audio", "🎵 استخراج صدا") + (" ✅" if mode == "audio" else ""), callback_data="defcfg:" + encode_cfg("audio", res, codec, crf, mute, speed, "mp3" if fmt in ["orig", "mp4", "mkv", "mov"] else fmt))[cite: 1]

    if mode == "video":
        b.button(text=get_text("buttons.default_config_keyboard.format_original", "📁 فرمت خروجی: مثل فایل اصلی") + (" ✅" if fmt == "orig" else ""), callback_data="defcfg:" + encode_cfg(mode, res, codec, crf, mute, speed, "orig"))[cite: 1]
        b.button(text=get_text("buttons.default_config_keyboard.format_mp4", "MP4") + (" ✅" if fmt == "mp4" else ""), callback_data="defcfg:" + encode_cfg(mode, res, codec, crf, mute, speed, "mp4"))[cite: 1]
        b.button(text=get_text("buttons.default_config_keyboard.format_mkv", "MKV") + (" ✅" if fmt == "mkv" else ""), callback_data="defcfg:" + encode_cfg(mode, res, codec, crf, mute, speed, "mkv"))[cite: 1]
        b.button(text=get_text("buttons.default_config_keyboard.format_mov", "MOV") + (" ✅" if fmt == "mov" else ""), callback_data="defcfg:" + encode_cfg(mode, res, codec, crf, mute, speed, "mov"))[cite: 1]

        for r_k in ["orig", "1080", "720", "480"]:
            r_t = get_text(f"buttons.video_config_keyboard.resolutions.{r_k}", r_k)[cite: 1]
            b.button(text=r_t + (" ✅" if res == r_k else ""), callback_data="defcfg:" + encode_cfg(mode, r_k, codec, crf, mute, speed, fmt))

        b.button(text=get_text("buttons.video_config_keyboard.codecs.h264", "H.264 (استاندارد)") + (" ✅" if codec == "h264" else ""), callback_data="defcfg:" + encode_cfg(mode, res, "h264", crf, mute, speed, fmt))[cite: 1]
        b.button(text=get_text("buttons.video_config_keyboard.codecs.h265", "H.265 (فوق‌العاده کم‌حجم)") + (" ✅" if codec == "h265" else ""), callback_data="defcfg:" + encode_cfg(mode, res, "h265", crf, mute, speed, fmt))[cite: 1]

        for c_k in ["light", "medium", "heavy"]:
            c_t = get_text(f"buttons.video_config_keyboard.crf_compression.{c_k}", c_k)[cite: 1]
            b.button(text=c_t + (" ✅" if crf == c_k else ""), callback_data="defcfg:" + encode_cfg(mode, res, codec, c_k, mute, speed, fmt))

        mute_btn_text = get_text("buttons.video_config_keyboard.audio_toggle.mute", "🔇 صدا: قطع") if mute else get_text("buttons.video_config_keyboard.audio_toggle.unmute", "🔊 صدا: وصل")[cite: 1]
        b.button(text=mute_btn_text, callback_data="defcfg:" + encode_cfg(mode, res, codec, crf, not mute, speed, fmt))
    else:
        for af_k in ["mp3", "wav", "m4a", "ogg", "flac"]:
            af_t = get_text(f"buttons.video_config_keyboard.audio_formats.{af_k}", af_k.upper())[cite: 1]
            b.button(text=af_t + (" ✅" if fmt == af_k else ""), callback_data="defcfg:" + encode_cfg(mode, res, codec, crf, mute, speed, af_k))

    for s_k in ["1.0", "1.5", "2.0"]:
        s_t = get_text(f"buttons.video_config_keyboard.speed_options.{s_k}", f"{s_k}x")[cite: 1]
        b.button(text=s_t + (" ✅" if speed == s_k else ""), callback_data="defcfg:" + encode_cfg(mode, res, codec, crf, mute, s_k, fmt))

    b.button(text=get_text("buttons.default_config_keyboard.back_to_settings", "🔙 بازگشت به تنظیمات"), callback_data="back_to_settings")[cite: 1]

    if mode == "video":
        b.adjust(2, 1, 3, 4, 2, 3, 1, 3, 1)
    else:
        b.adjust(2, 3, 2, 3, 1)
    return b.as_markup()


def get_cancel_keyboard(job_id: str):
    b = InlineKeyboardBuilder()
    b.button(text=get_text("buttons.support_and_errors.stop_processing", "پشیمون شدم"), callback_data=f"stop:{job_id}")[cite: 1]
    return b.as_markup()


async def get_media_meta(file_path: str) -> dict:
    meta = {"duration": 0, "width": 1280, "height": 720}
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-show_entries", "stream=width,height,duration:format=duration",
            "-of", "json", file_path
        ]
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=4.0)
        data = json.loads(stdout.decode(errors="ignore"))

        if "format" in data and "duration" in data["format"]:
            meta["duration"] = int(float(data["format"]["duration"]))

        if "streams" in data:
            for s in data["streams"]:
                if "width" in s and "height" in s:
                    meta["width"] = int(s["width"])
                    meta["height"] = int(s["height"])
                    if meta["duration"] == 0 and "duration" in s:
                        meta["duration"] = int(float(s["duration"]))
                    break
    except Exception as e:
        logging.warning(f"Metadata read error: {e}")
    return meta


async def generate_thumbnail(video_path: str, thumb_path: str, duration: int):
    try:
        ss_time = str(max(1, duration // 3)) if duration > 2 else "0.5"
        cmd = [
            FFMPEG_BIN, "-y",
            "-ss", ss_time,
            "-i", video_path,
            "-vframes", "1",
            "-vf", "scale=320:-1:flags=fast_bilinear",
            thumb_path
        ]
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        await asyncio.wait_for(proc.communicate(), timeout=3.0)
    except Exception:
        pass


@dp.message(CommandStart())
async def start_handler(message: aiotypes.Message, state: FSMContext):
    await state.clear()
    u = message.from_user
    await register_user(u.id, u.full_name or "", u.username or "")

    builder = InlineKeyboardBuilder()
    builder.button(text=get_text("buttons.reply_keyboard.settings", "⚙️ تنظیمات"), callback_data="open_settings")[cite: 1]
    builder.button(text=get_text("buttons.support_and_errors.connect_support", "📞 ارتباط با پشتیبانی"), callback_data="start_support")[cite: 1]
    builder.adjust(2)

    welcome_text = get_text("messages.start_and_welcome.welcome_text")[cite: 1]
    quick_access = get_text("messages.start_and_welcome.quick_access", "📌 دسترسی سریع:")[cite: 1]
    await message.answer(welcome_text, reply_markup=get_main_reply_keyboard(message.from_user.id))
    await message.answer(quick_access, reply_markup=builder.as_markup())


# --- پنل مدیریت ادمین ---
@dp.message(Command("admin"))
@dp.message(lambda msg: msg.text in [get_text("buttons.reply_keyboard.admin_panel"), "👑 پنل مدیریت"])[cite: 1]
async def admin_panel_handler(message: aiotypes.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.clear()
    all_users = await get_all_user_ids()
    limit = await get_daily_limit_mb()
    limit_str = f"{limit} مگابایت" if limit > 0 else "نامحدود"
    panel_text = get_text("messages.admin.main_panel_text", user_count=len(all_users), limit_str=limit_str)[cite: 1]
    await message.answer(
        panel_text,
        reply_markup=get_admin_panel_keyboard(),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "admin_close")
async def close_admin_panel(callback: aiotypes.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await callback.answer()
    await callback.message.delete()


@dp.callback_query(F.data == "admin_top_users")
async def show_top_users(callback: aiotypes.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()

    top_users = await get_top_users(limit=10)
    if not top_users:
        return await callback.message.answer(get_text("messages.admin.top_users_empty", "📊 هنوز کاربری در دیتابیس ثبت نشده رفیق!"))[cite: 1]

    builder = InlineKeyboardBuilder()
    text_lines = [get_text("messages.admin.top_users_header")][cite: 1]

    for idx, u in enumerate(top_users, start=1):
        safe_name = html.escape(u.get("name") or "کاربر")
        cost = float(u.get("total_cost") or 0.0)
        jobs = int(u.get("total_jobs") or 0)
        uid = u.get("user_id")
        row_str = get_text("messages.admin.top_user_row", idx=idx, safe_name=safe_name, cost=cost, jobs=jobs)[cite: 1]
        text_lines.append(row_str)
        builder.button(text=f"{idx}. {safe_name[:12]} (${cost:.4f})", callback_data=f"adm_u_stat:{uid}")

    builder.button(text=get_text("buttons.admin_panel.back_to_main", "🔙 بازگشت به پنل اصلی"), callback_data="admin_back_main")[cite: 1]
    builder.adjust(1)

    await callback.message.answer("\n".join(text_lines), reply_markup=builder.as_markup(), parse_mode="HTML")


@dp.callback_query(F.data.startswith("adm_u_stat:"))
async def show_single_user_stat(callback: aiotypes.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    target_uid = int(callback.data.split(":")[1])

    u = await get_user_stat(target_uid)
    if not u:
        return await callback.message.answer(get_text("messages.admin.user_not_found", "اطلاعات این کاربر پیدا نشد!"))[cite: 1]

    safe_name = html.escape(u.get("name") or "نامشخص")
    uname = f"@{html.escape(u['username'])}" if u.get("username") else "ندارد"
    cost = float(u.get("total_cost") or 0.0)
    jobs = int(u.get("total_jobs") or 0)
    today_mb = float(u.get("today_mb") or 0.0)

    text = get_text(
        "messages.admin.user_details_text",
        safe_name=safe_name,
        uname=uname,
        target_uid=target_uid,
        cost=cost,
        jobs=jobs,
        today_mb=today_mb
    )[cite: 1]

    builder = InlineKeyboardBuilder()
    if u.get("max_vid_file_id"):
        max_cost = float(u.get("max_vid_cost") or 0.0)
        max_size = float(u.get("max_vid_size_mb") or 0.0)
        date_str = u.get("max_vid_date") or "نامشخص"
        text += get_text("messages.admin.user_max_video_section", max_cost=max_cost, max_size=max_size, date=date_str)[cite: 1]
        builder.button(text=get_text("buttons.admin_panel.get_user_max_video", "🎬 دریافت و مشاهده این ویدیو"), callback_data=f"adm_get_vid:{target_uid}")[cite: 1]

    builder.button(text=get_text("buttons.admin_panel.back_to_top_users", "🔙 بازگشت به لیست پرمصرف‌ها"), callback_data="admin_top_users")[cite: 1]
    builder.adjust(1)

    await callback.message.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")


@dp.callback_query(F.data.startswith("adm_get_vid:"))
async def send_max_consuming_video(callback: aiotypes.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer(get_text("messages.admin.sending_video_alert", "در حال فرستادن ویدیو..."))[cite: 1]
    target_uid = int(callback.data.split(":")[1])

    u = await get_user_stat(target_uid)
    if not u or not u.get("max_vid_file_id"):
        return await callback.message.answer(get_text("messages.admin.no_video_recorded", "❌ ویدیویی واسه این کاربر ثبت نشده."))[cite: 1]

    cap = get_text(
        "messages.admin.max_video_caption",
        target_uid=target_uid,
        cost=float(u.get("max_vid_cost") or 0.0),
        size_mb=float(u.get("max_vid_size_mb") or 0.0),
        date=u.get("max_vid_date") or "نامشخص"
    )[cite: 1]

    try:
        await bot.send_video(chat_id=ADMIN_ID, video=u["max_vid_file_id"], caption=cap, parse_mode="HTML")
    except Exception:
        try:
            await bot.send_document(chat_id=ADMIN_ID, document=u["max_vid_file_id"], caption=cap, parse_mode="HTML")
        except Exception as e:
            await callback.message.answer(get_text("messages.admin.send_file_error", error=html.escape(str(e))), parse_mode="HTML")[cite: 1]


@dp.callback_query(F.data.startswith("req_vid:"))
async def send_requested_video_to_admin(callback: aiotypes.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer(get_text("messages.admin.sending_requested_video_alert", "در حال فرستادن ویدیوی کاربر..."))[cite: 1]
    job_id = callback.data.split(":", 1)[1]
    req = USER_REQUESTS.get(job_id)

    if not req:
        return await callback.message.answer(get_text("messages.admin.requested_video_not_found", "❌ اطلاعات این ویدیو پیدا نشد رفیق."))[cite: 1]

    cap = get_text("messages.admin.requested_video_caption", name=html.escape(req["name"]), user_id=req["user_id"])[cite: 1]

    try:
        await bot.send_video(chat_id=ADMIN_ID, video=req["file_id"], caption=cap, parse_mode="HTML")
    except Exception:
        try:
            await bot.send_document(chat_id=ADMIN_ID, document=req["file_id"], caption=cap, parse_mode="HTML")
        except Exception as e:
            await callback.message.answer(get_text("messages.admin.send_video_error", error=html.escape(str(e))), parse_mode="HTML")[cite: 1]


@dp.callback_query(F.data == "admin_set_limit")
async def show_limit_settings(callback: aiotypes.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    cur_limit = await get_daily_limit_mb()
    cur_str = f"{cur_limit} مگابایت" if cur_limit > 0 else "نامحدود"

    builder = InlineKeyboardBuilder()
    builder.button(text=get_text("buttons.admin_panel.limit_presets.100mb", "100 MB"), callback_data="set_lim:100")[cite: 1]
    builder.button(text=get_text("buttons.admin_panel.limit_presets.300mb", "300 MB"), callback_data="set_lim:300")[cite: 1]
    builder.button(text=get_text("buttons.admin_panel.limit_presets.500mb", "500 MB"), callback_data="set_lim:500")[cite: 1]
    builder.button(text=get_text("buttons.admin_panel.limit_presets.1000mb", "1000 MB (1GB)"), callback_data="set_lim:1000")[cite: 1]
    builder.button(text=get_text("buttons.admin_panel.limit_presets.2000mb", "2000 MB (2GB)"), callback_data="set_lim:2000")[cite: 1]
    builder.button(text=get_text("buttons.admin_panel.limit_presets.unlimited", "نامحدود ♾"), callback_data="set_lim:0")[cite: 1]
    builder.button(text=get_text("buttons.admin_panel.limit_presets.custom", "✏️ عدد دلخواه"), callback_data="set_lim_custom")[cite: 1]
    builder.button(text=get_text("buttons.admin_panel.back_to_panel", "🔙 بازگشت به پنل"), callback_data="admin_back_main")[cite: 1]
    builder.adjust(3, 3, 1, 1)

    text = get_text("messages.admin.limit_settings_text", cur_str=cur_str)[cite: 1]
    await callback.message.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")


@dp.callback_query(F.data.startswith("set_lim:"))
async def apply_preset_limit(callback: aiotypes.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    val = int(callback.data.split(":")[1])
    await set_daily_limit_mb(val)
    val_str = f"{val} مگابایت" if val > 0 else "نامحدود"
    await callback.answer(get_text("messages.admin.limit_changed_alert", val_str=val_str), show_alert=True)[cite: 1]
    await show_limit_settings(callback)


@dp.callback_query(F.data == "set_lim_custom")
async def ask_custom_limit(callback: aiotypes.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    cancel_b = InlineKeyboardBuilder()
    cancel_b.button(text=get_text("buttons.admin_panel.cancel_action", "پشیمون شدم"), callback_data="cancel_admin_action")[cite: 1]

    await callback.message.answer(
        get_text("messages.admin.ask_custom_limit"),[cite: 1]
        reply_markup=cancel_b.as_markup(),
        parse_mode="HTML"
    )
    await state.set_state(AdminMessageState.waiting_for_custom_limit)


@dp.message(AdminMessageState.waiting_for_custom_limit, F.chat.id == ADMIN_ID)
async def process_custom_limit_input(message: aiotypes.Message, state: FSMContext):
    text = message.text.strip() if message.text else ""
    if not text.isdigit():
        return await message.answer(get_text("messages.admin.limit_invalid_input"))[cite: 1]

    val = int(text)
    await set_daily_limit_mb(val)
    await state.clear()
    val_str = f"{val} مگابایت" if val > 0 else "نامحدود"
    await message.answer(get_text("messages.admin.limit_set_success", val_str=val_str), parse_mode="HTML")[cite: 1]


@dp.callback_query(F.data == "admin_back_main")
async def back_to_admin_main(callback: aiotypes.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    all_users = await get_all_user_ids()
    limit = await get_daily_limit_mb()
    limit_str = f"{limit} مگابایت" if limit > 0 else "نامحدود"
    await callback.message.edit_text(
        get_text("messages.admin.main_panel_text", user_count=len(all_users), limit_str=limit_str),[cite: 1]
        reply_markup=get_admin_panel_keyboard(),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "admin_broadcast")
async def start_broadcast(callback: aiotypes.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    users = await get_all_user_ids()
    cancel_b = InlineKeyboardBuilder()
    cancel_b.button(text=get_text("buttons.admin_panel.cancel_action", "پشیمون شدم"), callback_data="cancel_admin_action")[cite: 1]

    await callback.message.answer(
        get_text("messages.admin.broadcast_prompt", users_count=len(users)),[cite: 1]
        reply_markup=cancel_b.as_markup(),
        parse_mode="HTML"
    )
    await state.set_state(AdminMessageState.waiting_for_broadcast_content)


@dp.message(AdminMessageState.waiting_for_broadcast_content, F.chat.id == ADMIN_ID)
async def process_broadcast(message: aiotypes.Message, state: FSMContext):
    users = await get_all_user_ids()
    await message.answer(get_text("messages.admin.broadcast_in_progress", users_count=len(users)))[cite: 1]

    success = 0
    failed = 0
    for uid in users:
        try:
            await message.copy_to(chat_id=uid)
            success += 1
            await asyncio.sleep(0.04)
        except Exception:
            failed += 1

    await state.clear()
    await message.answer(
        get_text("messages.admin.broadcast_result", success=success, failed=failed, users_count=len(users)),[cite: 1]
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "admin_send_single")
async def ask_user_id_for_single(callback: aiotypes.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    cancel_b = InlineKeyboardBuilder()
    cancel_b.button(text=get_text("buttons.admin_panel.cancel_action", "پشیمون شدم"), callback_data="cancel_admin_action")[cite: 1]

    await callback.message.answer(
        get_text("messages.admin.ask_single_user_id"),[cite: 1]
        reply_markup=cancel_b.as_markup(),
        parse_mode="HTML"
    )
    await state.set_state(AdminMessageState.waiting_for_user_id)


@dp.callback_query(F.data.startswith("reply_to_user:"))
async def quick_reply_to_user(callback: aiotypes.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    target_id = int(callback.data.split(":")[1])
    await state.update_data(target_id=target_id, user_name="کاربر", username="")

    cancel_b = InlineKeyboardBuilder()
    cancel_b.button(text=get_text("buttons.admin_panel.cancel_action", "پشیمون شدم"), callback_data="cancel_admin_action")[cite: 1]

    await callback.message.answer(
        get_text("messages.admin.prompt_reply_to_user", target_id=target_id),[cite: 1]
        reply_markup=cancel_b.as_markup(),
        parse_mode="HTML"
    )
    await state.set_state(AdminMessageState.waiting_for_single_content)


@dp.message(AdminMessageState.waiting_for_user_id, F.chat.id == ADMIN_ID)
async def process_user_id_input(message: aiotypes.Message, state: FSMContext):
    text = message.text.strip() if message.text else ""
    if not text.isdigit():
        return await message.answer(get_text("messages.admin.invalid_user_id"))[cite: 1]

    target_id = int(text)
    user_name = "نامشخص"
    username = "ندارد"

    try:
        chat = await bot.get_chat(target_id)
        user_name = chat.full_name or "بدون نام"
        username = f"@{chat.username}" if chat.username else "ندارد"
    except Exception:
        try:
            pyro_user = await pyro.get_users(target_id)
            user_name = f"{pyro_user.first_name} {pyro_user.last_name or ''}".strip()
            username = f"@{pyro_user.username}" if pyro_user.username else "ندارد"
        except Exception:
            pass

    await state.update_data(target_id=target_id, user_name=user_name, username=username)

    cancel_b = InlineKeyboardBuilder()
    cancel_b.button(text=get_text("buttons.admin_panel.cancel_action", "پشیمون شدم"), callback_data="cancel_admin_action")[cite: 1]

    confirm_text = get_text(
        "messages.admin.user_found_prompt",
        user_name=html.escape(user_name),
        username=html.escape(username),
        target_id=target_id
    )[cite: 1]
    await message.answer(confirm_text, reply_markup=cancel_b.as_markup(), parse_mode="HTML")
    await state.set_state(AdminMessageState.waiting_for_single_content)


@dp.message(AdminMessageState.waiting_for_single_content, F.chat.id == ADMIN_ID)
async def send_single_message_to_user(message: aiotypes.Message, state: FSMContext):
    data = await state.get_data()
    target_id = data.get("target_id")
    user_name = data.get("user_name", "کاربر")

    try:
        await bot.send_message(chat_id=target_id, text=get_text("messages.admin.admin_message_header_to_user"), parse_mode="HTML")[cite: 1]
        await message.copy_to(chat_id=target_id)
        await message.answer(get_text("messages.admin.admin_send_single_success", user_name=html.escape(user_name), target_id=target_id), parse_mode="HTML")[cite: 1]
    except TelegramForbiddenError:
        await message.answer(get_text("messages.admin.admin_send_single_blocked"))[cite: 1]
    except Exception as e:
        await message.answer(get_text("messages.admin.admin_send_single_error", error=html.escape(str(e))), parse_mode="HTML")[cite: 1]

    await state.clear()


@dp.callback_query(F.data == "cancel_admin_action")
async def cancel_admin_action(callback: aiotypes.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await callback.answer(get_text("messages.admin.action_cancelled_alert", "لغو شد."))[cite: 1]
    await callback.message.edit_text(get_text("messages.admin.action_cancelled_text", "پشیمون شدم."))[cite: 1]


# --- منوی تنظیمات و بخش تنظیمات دیفالت ویدیو ---
@dp.message(lambda msg: msg.text in [get_text("buttons.reply_keyboard.settings"), "⚙️ تنظیمات"])[cite: 1]
@dp.callback_query(F.data == "open_settings")
async def show_settings_menu(event: aiotypes.Message | aiotypes.CallbackQuery):
    user_id = event.from_user.id
    u = event.from_user
    await register_user(user_id, u.full_name or "", u.username or "")

    text = get_text("messages.settings.main_menu_text")[cite: 1]
    kb = get_settings_inline_keyboard(user_id)
    if isinstance(event, aiotypes.CallbackQuery):
        await event.answer()
        await event.message.answer(text, reply_markup=kb, parse_mode="HTML")
    else:
        await event.answer(text, reply_markup=kb, parse_mode="HTML")


@dp.callback_query(F.data == "toggle_details")
async def toggle_settings_option(callback: aiotypes.CallbackQuery):
    user_id = callback.from_user.id
    current_status = get_user_show_details(user_id)
    set_user_show_details(user_id, not current_status)
    await callback.answer(get_text("messages.settings.settings_changed_alert", "تنظیمات گزارش تغییر کرد!"))[cite: 1]
    try:
        await callback.message.edit_reply_markup(reply_markup=get_settings_inline_keyboard(user_id))
    except TelegramBadRequest:
        pass


@dp.callback_query(F.data == "open_default_settings")
async def show_default_settings(callback: aiotypes.CallbackQuery):
    user_id = callback.from_user.id
    user_cfg = get_user_default_cfg(user_id)
    text = get_text("messages.settings.default_settings_text")[cite: 1]
    await callback.answer()
    await callback.message.edit_text(
        text,
        reply_markup=build_default_config_keyboard(user_cfg),
        parse_mode="HTML"
    )


@dp.callback_query(F.data.startswith("defcfg:"))
async def update_default_settings_callback(callback: aiotypes.CallbackQuery):
    cfg = decode_cfg(callback.data[7:])
    set_user_default_cfg(callback.from_user.id, cfg)
    await callback.answer(get_text("messages.settings.saved_alert", "✅ ذخیره شد!"))[cite: 1]
    text = get_text("messages.settings.default_settings_text")[cite: 1]
    try:
        await callback.message.edit_reply_markup(reply_markup=build_default_config_keyboard(cfg))
    except TelegramBadRequest:
        pass


@dp.callback_query(F.data == "back_to_settings")
async def back_to_settings_menu(callback: aiotypes.CallbackQuery):
    user_id = callback.from_user.id
    text = get_text("messages.settings.main_menu_text")[cite: 1]
    await callback.answer()
    await callback.message.edit_text(
        text,
        reply_markup=get_settings_inline_keyboard(user_id),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "close_settings")
async def close_settings_menu(callback: aiotypes.CallbackQuery):
    await callback.answer()
    await callback.message.delete()


@dp.callback_query(F.data == "none")
async def no_action_callback(callback: aiotypes.CallbackQuery):
    await callback.answer()


# --- پشتیبانی دوطرفه ---
@dp.message(lambda msg: msg.text in [get_text("buttons.reply_keyboard.support"), "📞 ارتباط با پشتیبانی"])[cite: 1]
@dp.callback_query(F.data == "start_support")
async def ask_support_message(event: aiotypes.Message | aiotypes.CallbackQuery, state: FSMContext):
    u = event.from_user
    await register_user(u.id, u.full_name or "", u.username or "")

    cancel_b = InlineKeyboardBuilder()
    cancel_b.button(text=get_text("buttons.support_and_errors.cancel_support", "پشیمون شدم"), callback_data="cancel_support")[cite: 1]

    msg_text = get_text("messages.support.prompt_message")[cite: 1]
    if isinstance(event, aiotypes.CallbackQuery):
        await event.answer()
        await event.message.answer(msg_text, reply_markup=cancel_b.as_markup())
    else:
        await event.answer(msg_text, reply_markup=cancel_b.as_markup())

    await state.set_state(SupportState.waiting_for_message)


@dp.callback_query(F.data == "cancel_support")
async def cancel_support(callback: aiotypes.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.answer(get_text("messages.support.cancelled_alert", "لغو شد."))[cite: 1]
    await callback.message.edit_text(get_text("messages.support.cancelled_text", "پشیمون شدم."))[cite: 1]


@dp.message(SupportState.waiting_for_message)
async def forward_support_message(message: aiotypes.Message, state: FSMContext):
    u = message.from_user
    user_id = u.id
    name = html.escape(u.full_name or "بدون نام")
    username = f"@{html.escape(u.username)}" if u.username else "ندارد"
    await register_user(user_id, u.full_name or "", u.username or "")

    admin_header = get_text("messages.support.forwarded_to_admin_header", name=name, user_id=user_id, username=username)[cite: 1]

    reply_kb = InlineKeyboardBuilder()
    reply_kb.button(text=get_text("buttons.support_and_errors.reply_to_user", "✍️ پاسخ به این کاربر"), callback_data=f"reply_to_user:{user_id}")[cite: 1]

    try:
        header_msg = await bot.send_message(chat_id=ADMIN_ID, text=admin_header, reply_markup=reply_kb.as_markup(), parse_mode="HTML")
        content_msg = await message.copy_to(chat_id=ADMIN_ID)

        SUPPORT_MAP[header_msg.message_id] = user_id
        SUPPORT_MAP[content_msg.message_id] = user_id

        if DB_POOL:
            try:
                async with DB_POOL.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO support_tickets (admin_msg_id, user_id)
                        VALUES ($1, $2), ($3, $2)
                        ON CONFLICT DO NOTHING;
                    """, header_msg.message_id, user_id, content_msg.message_id)
            except Exception:
                pass

        await message.reply(get_text("messages.support.sent_to_support_confirm"))[cite: 1]
    except Exception as e:
        logging.error(f"Failed to forward message to admin: {e}")
        await message.reply(get_text("messages.support.send_error"))[cite: 1]

    await state.clear()


@dp.message(F.chat.id == ADMIN_ID, F.reply_to_message)
async def handle_admin_reply(message: aiotypes.Message):
    replied = message.reply_to_message
    target_user_id = SUPPORT_MAP.get(replied.message_id)

    if not target_user_id and DB_POOL:
        try:
            async with DB_POOL.acquire() as conn:
                target_user_id = await conn.fetchval(
                    "SELECT user_id FROM support_tickets WHERE admin_msg_id = $1;",
                    replied.message_id
                )
        except Exception:
            pass

    if not target_user_id:
        text_source = (replied.text or "") + " " + (replied.caption or "")
        match = re.search(r"آیدی عددی:\s*<code>?(\d+)</code>?", text_source)
        if not match:
            match = re.search(r"(\d{7,12})", text_source)
        if match:
            target_user_id = int(match.group(1))

    if not target_user_id:
        return

    try:
        await bot.send_message(chat_id=target_user_id, text=get_text("messages.support.admin_reply_header"), parse_mode="HTML")[cite: 1]
        await message.copy_to(chat_id=target_user_id)
        await message.reply(get_text("messages.support.admin_reply_success"))[cite: 1]
    except TelegramForbiddenError:
        await message.reply(get_text("messages.support.user_blocked_bot"))[cite: 1]
    except Exception as e:
        logging.error(f"Failed to send admin reply: {e}")
        await message.reply(get_text("messages.support.admin_reply_error", error=html.escape(str(e))), parse_mode="HTML")[cite: 1]


@dp.callback_query(F.data.startswith("err_send_vid:"))
async def handle_send_error_video(callback: aiotypes.CallbackQuery):
    job_id = callback.data.split(":", 1)[1]
    failed_job = FAILED_JOBS.pop(job_id, None)

    if not failed_job:
        await callback.answer(get_text("messages.errors_and_alerts.error_video_expired_alert", "مهلت گذشته است."), show_alert=True)[cite: 1]
        return await callback.message.edit_text(get_text("messages.errors_and_alerts.error_video_expired_text"))[cite: 1]

    await callback.answer("در حال فرستادن ویدیو...")
    try:
        await bot.send_message(
            chat_id=ADMIN_ID,
            text=get_text("messages.errors_and_alerts.admin_error_video_caption", user_name=html.escape(failed_job["user_name"]), user_id=failed_job["user_id"]),[cite: 1]
            parse_mode="HTML"
        )
        await bot.forward_message(
            chat_id=ADMIN_ID,
            from_chat_id=failed_job["chat_id"],
            message_id=failed_job["msg_id"]
        )
        await callback.message.edit_text(get_text("messages.errors_and_alerts.error_video_forwarded_success"))[cite: 1]
    except Exception as e:
        logging.error(f"Error forwarding video: {e}")
        await callback.message.edit_text(get_text("messages.errors_and_alerts.error_video_forward_failed"))[cite: 1]


@dp.callback_query(F.data.startswith("err_cancel_vid:"))
async def handle_cancel_error_video(callback: aiotypes.CallbackQuery):
    job_id = callback.data.split(":", 1)[1]
    FAILED_JOBS.pop(job_id, None)
    await callback.answer("لغو شد.")
    await callback.message.edit_text(get_text("buttons.support_and_errors.error_cancel_video", "پشیمون شدم"))[cite: 1]


def detect_file_extension(message: aiotypes.Message) -> str:
    file_name = None
    if message.video and message.video.file_name:
        file_name = message.video.file_name
    elif message.document and message.document.file_name:
        file_name = message.document.file_name

    if file_name and "." in file_name:
        ext = file_name.rsplit(".", 1)[-1].lower()
        if ext in ["mp4", "mkv", "mov", "avi", "webm", "flv", "wmv", "3gp", "ts"]:
            return ext
    return "mp4"


# --- دریافت ویدیو و شروع پردازش ---
@dp.message(F.video | F.document)
async def handle_video(message: aiotypes.Message):
    u = message.from_user
    await register_user(u.id, u.full_name or "", u.username or "")

    video = message.video or (
        message.document if message.document and (
            (message.document.mime_type and message.document.mime_type.startswith("video/")) or
            (message.document.file_name and (message.document.file_name or "").lower().endswith((".mp4", ".mkv", ".mov", ".avi", ".webm")))
        ) else None
    )
    if not video:
        return await message.answer(get_text("messages.video_validation_and_prompt.not_a_video"))[cite: 1]

    if video.file_size > MAX_FILE_SIZE and message.from_user.id != ADMIN_ID:
        support_kb = InlineKeyboardBuilder()
        support_kb.button(text=get_text("buttons.support_and_errors.send_support_msg", "📞 پیام به پشتیبانی"), callback_data="start_support")[cite: 1]
        return await message.reply(
            get_text("messages.video_validation_and_prompt.file_too_large"),[cite: 1]
            reply_markup=support_kb.as_markup(),
            parse_mode="HTML"
        )

    file_size_mb = video.file_size / (1024 * 1024)
    allowed, cur_mb, limit_mb = await check_and_update_daily_usage(message.from_user.id, file_size_mb)

    if not allowed:
        return await message.answer(
            get_text("messages.video_validation_and_prompt.daily_quota_exceeded", limit_mb=limit_mb, cur_mb=cur_mb, file_size_mb=file_size_mb),[cite: 1]
            parse_mode="HTML"
        )

    orig_ext = detect_file_extension(message)
    user_default_cfg = get_user_default_cfg(message.from_user.id)
    default_cfg = dict(user_default_cfg)

    await message.reply(
        get_text("messages.video_validation_and_prompt.config_prompt", orig_ext=orig_ext.upper()),[cite: 1]
        reply_markup=build_config_keyboard(default_cfg, orig_ext=orig_ext),
        parse_mode="HTML"
    )


@dp.callback_query(F.data.startswith("cfg:"))
async def update_settings(callback: aiotypes.CallbackQuery):
    await callback.answer()
    cfg = decode_cfg(callback.data[4:])
    orig_msg = callback.message.reply_to_message
    orig_ext = detect_file_extension(orig_msg) if orig_msg else "mp4"
    try:
        await callback.message.edit_reply_markup(reply_markup=build_config_keyboard(cfg, orig_ext=orig_ext))
    except TelegramBadRequest:
        pass


@dp.callback_query(F.data == "cancel_panel")
async def cancel_panel(callback: aiotypes.CallbackQuery):
    await callback.message.edit_text(get_text("messages.video_validation_and_prompt.panel_cancelled", "پشیمون شدم."))[cite: 1]


@dp.callback_query(F.data.startswith("stop:"))
async def stop_processing(callback: aiotypes.CallbackQuery):
    job_id = callback.data.split(":")[1]
    
    if job_id in ACTIVE_PROCESSES or job_id in RUNNING_TASKS:
        ACTIVE_PROCESSES[job_id]["cancelled"] = True
        
        proc = ACTIVE_PROCESSES.get(job_id, {}).get("proc")
        if proc:
            try:
                proc.kill()
            except ProcessLookupError:
                pass

        task = RUNNING_TASKS.get(job_id)
        if task and not task.done():
            task.cancel()

        await callback.answer(get_text("messages.queue_and_progress.stopped_alert", "پردازش متوقف شد."))[cite: 1]
        await callback.message.edit_text(get_text("messages.queue_and_progress.stopped_msg", "🛑 پردازش متوقف شد و نوبت صف آزاد گردید."))[cite: 1]
    else:
        await callback.answer(get_text("messages.queue_and_progress.not_running_alert", "پردازشی در حال اجرا نیست."), show_alert=True)[cite: 1]


@dp.callback_query(F.data.startswith("run:"))
async def enqueue_task(callback: aiotypes.CallbackQuery):
    await callback.answer()
    cfg = decode_cfg(callback.data[4:])
    orig_msg = callback.message.reply_to_message
    if not orig_msg:
        return await callback.message.edit_text(get_text("messages.queue_and_progress.reference_not_found", "❌ پیام ویدیوی مرجع پیدا نشد."))[cite: 1]

    video = orig_msg.video or (
        orig_msg.document if orig_msg.document and (
            (orig_msg.document.mime_type and orig_msg.document.mime_type.startswith("video/")) or
            (orig_msg.document.file_name and (orig_msg.document.file_name or "").lower().endswith((".mp4", ".mkv", ".mov", ".avi", ".webm")))
        ) else None
    )
    if not video:
        return await callback.message.edit_text(get_text("messages.queue_and_progress.video_not_found", "❌ ویدیویی پیدا نشد."))[cite: 1]

    orig_ext = detect_file_extension(orig_msg)
    user_id = callback.from_user.id
    user_name = callback.from_user.full_name or "کاربر"
    username = f"@{callback.from_user.username}" if callback.from_user.username else "ندارد"
    file_size_mb = video.file_size / (1024 * 1024)
    job_id = f"{callback.message.chat.id}_{callback.message.message_id}"

    USER_REQUESTS[job_id] = {
        "file_id": video.file_id,
        "user_id": user_id,
        "name": user_name
    }

    if user_id != ADMIN_ID:
        u_stat = await get_user_stat(user_id)
        total_user_cost = float(u_stat.get("total_cost") or 0.0) if u_stat else 0.0

        admin_alert_kb = InlineKeyboardBuilder()
        admin_alert_kb.button(
            text=get_text("buttons.admin_panel.get_requested_video", "📥 دریافت این ویدیو"),[cite: 1]
            callback_data=f"req_vid:{job_id}"
        )

        admin_notice = get_text(
            "messages.admin.new_job_alert",
            user_name=html.escape(user_name),
            user_id=user_id,
            username=html.escape(username),
            file_size_mb=file_size_mb,
            mode=cfg["mode"],
            res=cfg["res"],
            codec=cfg["codec"],
            total_user_cost=total_user_cost
        )[cite: 1]
        try:
            await bot.send_message(
                chat_id=ADMIN_ID,
                text=admin_notice,
                reply_markup=admin_alert_kb.as_markup(),
                parse_mode="HTML"
            )
        except Exception as e:
            logging.warning(f"Failed to alert admin on request: {e}")

    status_msg = await callback.message.edit_text(
        get_text("messages.queue_and_progress.in_queue", queue_pos=JOB_QUEUE.qsize() + 1),[cite: 1]
        reply_markup=get_cancel_keyboard(job_id),
        parse_mode="HTML"
    )

    ACTIVE_PROCESSES[job_id] = {"cancelled": False, "proc": None}
    await JOB_QUEUE.put({
        "job_id": job_id, "cfg": cfg, "msg_id": orig_msg.message_id,
        "file_size": video.file_size, "file_id": video.file_id,
        "chat_id": callback.message.chat.id, "user": callback.from_user,
        "user_id": callback.from_user.id,
        "orig_ext": orig_ext,
        "status_msg": status_msg
    })


async def queue_worker():
    while True:
        job = await JOB_QUEUE.get()
        job_id = job["job_id"]

        if ACTIVE_PROCESSES.get(job_id, {}).get("cancelled"):
            ACTIVE_PROCESSES.pop(job_id, None)
            JOB_QUEUE.task_done()
            continue

        task = asyncio.create_task(process_job(job))
        RUNNING_TASKS[job_id] = task

        try:
            await task
        except asyncio.CancelledError:
            logging.info(f"Task {job_id} cancelled.")
        except Exception as e:
            tb = traceback.format_exc()
            logging.error(f"Job {job_id} error:\n{tb}")

            tb_lines = [line for line in tb.strip().splitlines() if "site-packages" not in line]
            clean_tb = "\n".join(tb_lines[-8:]) if tb_lines else tb[-500:]

            u = job.get("user")
            user_id = job.get("user_id") or (u.id if u else job["chat_id"])
            user_name = u.full_name if u else "نامشخص"
            username = f"@{u.username}" if (u and u.username) else "ندارد"

            admin_err_alert = get_text(
                "messages.errors_and_alerts.admin_error_report",
                user_name=html.escape(user_name),
                user_id=user_id,
                username=html.escape(username),
                error_short=html.escape(str(e)[:250]),
                log=html.escape(clean_tb[:800])
            )[cite: 1]
            try:
                await bot.send_message(chat_id=ADMIN_ID, text=admin_err_alert, parse_mode="HTML")
            except Exception as adm_err:
                logging.error(f"Failed to alert admin: {adm_err}")

            if len(FAILED_JOBS) > 100:
                FAILED_JOBS.pop(next(iter(FAILED_JOBS)))

            FAILED_JOBS[job_id] = {
                "chat_id": job["chat_id"],
                "msg_id": job["msg_id"],
                "user_name": user_name,
                "user_id": user_id
            }

            err_kb = InlineKeyboardBuilder()
            err_kb.button(text=get_text("buttons.support_and_errors.error_send_video", "بله، ویدیو هم فرستاده بشه ✅"), callback_data=f"err_send_vid:{job_id}")[cite: 1]
            err_kb.button(text=get_text("buttons.support_and_errors.error_cancel_video", "پشیمون شدم"), callback_data=f"err_cancel_vid:{job_id}")[cite: 1]
            err_kb.adjust(1)

            user_notice = get_text("messages.errors_and_alerts.user_error_notice")[cite: 1]
            try:
                await job["status_msg"].edit_text(user_notice, reply_markup=err_kb.as_markup(), parse_mode="HTML")
            except Exception:
                try:
                    await bot.send_message(chat_id=job["chat_id"], text=user_notice, reply_markup=err_kb.as_markup(), parse_mode="HTML")
                except Exception:
                    pass
        finally:
            RUNNING_TASKS.pop(job_id, None)
            ACTIVE_PROCESSES.pop(job_id, None)
            JOB_QUEUE.task_done()


async def ui_updater(state: dict):
    last_text = ""
    while not state.get("done", False):
        try:
            bar = generate_progress_bar(state.get("percent", 0.0))
            act = state.get("action", "")
            if act == "download":
                text = get_text("messages.queue_and_progress.downloading", bar=bar)[cite: 1]
            elif act == "encode":
                eta_val = state.get("eta")
                if state.get("file_size", 0) >= 50 * 1024 * 1024 and eta_val:
                    text = get_text("messages.queue_and_progress.encoding_with_eta", bar=bar, eta=eta_val)[cite: 1]
                else:
                    text = get_text("messages.queue_and_progress.encoding", bar=bar)[cite: 1]
            elif act == "upload":
                text = get_text("messages.queue_and_progress.uploading", bar=bar)[cite: 1]
            else:
                text = get_text("messages.queue_and_progress.please_wait", "⏳ لطفاً کمی صبر کنید...")[cite: 1]

            if text != last_text:
                await state["status_msg"].edit_text(text, reply_markup=get_cancel_keyboard(state["job_id"]), parse_mode="HTML")
                last_text = text
        except (TelegramBadRequest, asyncio.CancelledError):
            pass
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after)
        except Exception:
            pass
        await asyncio.sleep(2)


def pyro_progress(curr, total, state):
    if total > 0:
        state["percent"] = (curr / total) * 100.0


async def download_with_retry(job: dict, input_path: str, ui_state: dict, max_retries: int = 3):
    initial_size = job["file_size"]
    last_err = None

    for attempt in range(1, max_retries + 1):
        if ACTIVE_PROCESSES[job["job_id"]]["cancelled"]:
            return

        if os.path.exists(input_path):
            try:
                os.remove(input_path)
            except OSError:
                pass

        ui_state["action"] = "download"
        ui_state["percent"] = 0.0

        if attempt > 1:
            try:
                await job["status_msg"].edit_text(
                    get_text("messages.queue_and_progress.download_retry", attempt=attempt, max_retries=max_retries),[cite: 1]
                    reply_markup=get_cancel_keyboard(job["job_id"]),
                    parse_mode="HTML"
                )
            except Exception:
                pass
            await asyncio.sleep(2)

        try:
            if initial_size < 19.5 * 1024 * 1024:
                file_info = await bot.get_file(job["file_id"])
                await bot.download_file(file_info.file_path, destination=input_path)
            else:
                msg = await pyro.get_messages(chat_id=job["chat_id"], message_ids=job["msg_id"])
                if not msg or msg.empty:
                    raise RuntimeError("پیام ویدیو در تلگرام بازخوانی نشد.")

                try:
                    with open(input_path, "wb") as f:
                        curr_bytes = 0
                        async for chunk in pyro.stream_media(msg):
                            if ACTIVE_PROCESSES[job["job_id"]]["cancelled"]:
                                return
                            f.write(chunk)
                            curr_bytes += len(chunk)
                            if initial_size > 0:
                                ui_state["percent"] = min(99.0, (curr_bytes / initial_size) * 100.0)
                except Exception:
                    await pyro.download_media(
                        msg,
                        file_name=input_path,
                        progress=pyro_progress,
                        progress_args=(ui_state,)
                    )

            if os.path.exists(input_path):
                downloaded_bytes = os.path.getsize(input_path)
                if downloaded_bytes >= (initial_size * 0.95):
                    ui_state["percent"] = 100.0
                    return
                else:
                    raise RuntimeError(f"دانلود ناقص است: {downloaded_bytes / (1024*1024):.2f}MB")
            else:
                raise RuntimeError("فایل ذخیره نشد.")

        except Exception as e:
            last_err = e
            await asyncio.sleep(2)

    raise RuntimeError(f"خطا در دانلود:\n{last_err}")


async def process_job(job: dict):
    job_id = job["job_id"]
    cfg = job["cfg"]
    status_msg = job["status_msg"]
    mode = cfg["mode"]
    initial_size = job["file_size"]
    user_id = job.get("user_id", job["chat_id"])
    u = job.get("user")
    user_name = u.full_name if u else "کاربر"
    username = u.username or ""
    orig_ext = job.get("orig_ext", "mp4")
    fmt_choice = cfg.get("fmt", "orig")
    speed_factor = float(cfg.get("speed", "1.0"))

    start_cpu_sec = get_cpu_seconds()
    start_wall_time = time.time()

    if mode == "audio":
        out_ext = fmt_choice if fmt_choice in ["mp3", "wav", "m4a", "ogg", "flac"] else "mp3"
    else:
        if fmt_choice == "orig":
            out_ext = orig_ext if orig_ext in ["mp4", "mkv", "mov"] else "mp4"
        else:
            out_ext = fmt_choice

    input_path = os.path.join(DOWNLOAD_DIR, f"in_{job_id}.{orig_ext}")
    output_path = os.path.join(DOWNLOAD_DIR, f"out_{job_id}.{out_ext}")
    thumb_path = os.path.join(DOWNLOAD_DIR, f"thumb_{job_id}.jpg")

    ui_state = {
        "status_msg": status_msg,
        "job_id": job_id,
        "action": "download",
        "percent": 0.0,
        "done": False,
        "file_size": initial_size,
        "eta": None
    }
    ui_task = asyncio.create_task(ui_updater(ui_state))

    try:
        for p in (input_path, output_path, thumb_path):
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass

        await download_with_retry(job, input_path, ui_state, max_retries=3)

        if ACTIVE_PROCESSES[job_id]["cancelled"]:
            return

        gc.collect()

        ui_state["action"] = "encode"
        ui_state["percent"] = 1.0

        in_meta = await get_media_meta(input_path)
        duration = in_meta["duration"]
        eff_duration = duration / speed_factor if (speed_factor > 0 and duration > 0) else duration

        cmd = [
            FFMPEG_BIN, "-y",
            "-threads", "1",
            "-i", input_path,
            "-max_muxing_queue_size", "1024"
        ]

        if mode == "audio":
            if out_ext == "mp3":
                cmd += ["-vn", "-c:a", "libmp3lame", "-b:a", "192k"]
            elif out_ext == "wav":
                cmd += ["-vn", "-c:a", "pcm_s16le"]
            elif out_ext == "m4a":
                cmd += ["-vn", "-c:a", "aac", "-b:a", "192k"]
            elif out_ext == "flac":
                cmd += ["-vn", "-c:a", "flac"]
            elif out_ext == "ogg":
                cmd += ["-vn", "-c:a", "libvorbis", "-q:a", "5"]

            if speed_factor != 1.0:
                cmd += ["-filter:a", f"atempo={speed_factor}"]

            cmd += ["-progress", "pipe:2", output_path]

        else:
            crf_map = {"light": "23", "medium": "28", "heavy": "34"}
            v_codec = "libx265" if cfg["codec"] == "h265" else "libx264"

            vf = []
            if speed_factor != 1.0:
                vf.append(f"setpts={1.0 / speed_factor}*PTS")
            if cfg["res"] != "orig":
                vf.append(f"scale=-2:{cfg['res']}:flags=fast_bilinear")

            cmd += [
                "-map", "0:v:0",
                "-c:v", v_codec,
                "-crf", crf_map.get(cfg["crf"], "28"),
                "-preset", "ultrafast",
                "-pix_fmt", "yuv420p"
            ]

            if vf:
                cmd += ["-vf", ",".join(vf)]

            if v_codec == "libx264":
                cmd += ["-tune", "fastdecode", "-x264opts", "rc-lookahead=0:sync-lookahead=0:bframes=0"]
            elif v_codec == "libx265":
                cmd += ["-x265-params", "pools=1:frame-threads=1:rc-lookahead=0:bframes=0"]

            if cfg["mute"]:
                cmd += ["-an"]
            elif speed_factor != 1.0:
                cmd += ["-map", "0:a:0?", "-c:a", "aac", "-b:a", "128k", "-filter:a", f"atempo={speed_factor}"]
            else:
                cmd += ["-map", "0:a:0?", "-c:a", "copy"]

            cmd += ["-avoid_negative_ts", "make_zero"]

            if out_ext in ["mp4", "mov", "m4a"]:
                cmd += ["-movflags", "+faststart"]

            cmd += ["-progress", "pipe:2", output_path]

        proc = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        ACTIVE_PROCESSES[job_id]["proc"] = proc

        time_us_pattern = re.compile(r"out_time_us=(\d+)")
        time_str_pattern = re.compile(r"out_time=(\d+):(\d+):(\d+(?:\.\d+)?)")
        last_error_lines = []
        encode_start_time = None

        while True:
            line = await proc.stderr.readline()
            if not line:
                break
            if ACTIVE_PROCESSES[job_id]["cancelled"]:
                try:
                    proc.kill()
                except Exception:
                    pass
                return

            line_str = line.decode(errors="ignore").strip()
            if line_str:
                last_error_lines.append(line_str)
                if len(last_error_lines) > 8:
                    last_error_lines.pop(0)

            current_secs = None
            match_us = time_us_pattern.search(line_str)
            if match_us:
                current_secs = float(match_us.group(1)) / 1_000_000.0
            else:
                match_str = time_str_pattern.search(line_str)
                if match_str:
                    h, m, s = map(float, match_str.groups())
                    current_secs = h * 3600 + m * 60 + s

            if current_secs is not None and eff_duration > 0:
                pct = (current_secs / eff_duration) * 100.0
                ui_state["percent"] = min(99.0, max(1.0, pct))

                loop = asyncio.get_event_loop()
                if encode_start_time is None:
                    encode_start_time = loop.time()
                else:
                    elapsed = loop.time() - encode_start_time
                    if elapsed > 3.0 and current_secs > 1.5:
                        render_speed = current_secs / elapsed
                        if render_speed > 0:
                            rem_real_secs = max(0.0, eff_duration - current_secs) / render_speed
                            mins, secs = divmod(int(rem_real_secs), 60)
                            if mins > 0:
                                ui_state["eta"] = get_text("messages.queue_and_progress.eta_format_min_sec", mins=mins, secs=secs)[cite: 1]
                            else:
                                ui_state["eta"] = get_text("messages.queue_and_progress.eta_format_sec", secs=secs)[cite: 1]

        await proc.wait()

        if ACTIVE_PROCESSES[job_id]["cancelled"]:
            return

        if proc.returncode != 0 or not os.path.exists(output_path):
            err_details = "\n".join(last_error_lines[-5:]) if last_error_lines else "لاگ نامشخص"
            raise RuntimeError(f"خطای FFmpeg ({proc.returncode}):\n{err_details}")

        out_meta = await get_media_meta(output_path)
        out_dur = out_meta["duration"] or int(eff_duration)
        out_w = out_meta["width"]
        out_h = out_meta["height"]

        if mode == "video":
            await generate_thumbnail(output_path, thumb_path, out_dur)

        ui_state["action"] = "upload"
        ui_state["percent"] = 0.0

        final_size = os.path.getsize(output_path)
        reduction = max(0, int(((initial_size - final_size) / initial_size) * 100))

        show_details = get_user_show_details(user_id)
        base_caption = get_text(
            "messages.completion_captions.base_caption",
            initial_size_mb=initial_size / (1024 * 1024),
            final_size_mb=final_size / (1024 * 1024),
            reduction=reduction
        )[cite: 1]

        if show_details:
            if mode == "audio":
                summary_text = get_text("messages.completion_captions.audio_details", out_ext=out_ext.upper())[cite: 1]
            else:
                res_name = get_text(f"messages.completion_captions.video_detail_values.resolutions.{cfg['res']}", cfg['res'])[cite: 1]
                codec_name = get_text(f"messages.completion_captions.video_detail_values.codecs.{cfg['codec']}", cfg['codec'])[cite: 1]
                crf_name = get_text(f"messages.completion_captions.video_detail_values.crf.{cfg['crf']}", cfg['crf'])[cite: 1]
                mute_name = get_text("messages.completion_captions.video_detail_values.mute_status.muted") if cfg["mute"] else get_text("messages.completion_captions.video_detail_values.mute_status.unmuted")[cite: 1]

                summary_text = get_text(
                    "messages.completion_captions.video_details",
                    out_ext=out_ext.upper(),
                    res_name=res_name,
                    codec_name=codec_name,
                    crf_name=crf_name,
                    mute_name=mute_name,
                    speed_factor=speed_factor
                )[cite: 1]
            caption = base_caption + summary_text
        else:
            caption = base_caption

        has_thumb = os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 100
        chat_id = job["chat_id"]

        if mode == "audio":
            await pyro.send_audio(
                chat_id=chat_id,
                audio=output_path,
                duration=out_dur,
                caption=caption,
                progress=pyro_progress,
                progress_args=(ui_state,)
            )
        else:
            await pyro.send_video(
                chat_id=chat_id,
                video=output_path,
                duration=out_dur,
                width=out_w,
                height=out_h,
                thumb=thumb_path if has_thumb else None,
                caption=caption,
                supports_streaming=True,
                progress=pyro_progress,
                progress_args=(ui_state,)
            )

        ui_state["done"] = True
        ui_task.cancel()
        await status_msg.delete()

        end_cpu_sec = get_cpu_seconds()
        end_wall_time = time.time()

        cpu_used_sec = max(0.01, end_cpu_sec - start_cpu_sec)
        wall_time_sec = max(0.01, end_wall_time - start_wall_time)

        cpu_cost = cpu_used_sec * 0.00000772
        ram_cost = wall_time_sec * 0.35 * 0.00000386
        egress_gb = final_size / (1024 ** 3)
        network_cost = egress_gb * 0.05
        exact_cost = cpu_cost + ram_cost + network_cost

        await record_job_stats(
            user_id=user_id,
            name=user_name,
            username=username,
            cost=exact_cost,
            file_size_mb=initial_size / (1024 * 1024),
            file_id=job["file_id"]
        )

        if user_id != ADMIN_ID:
            u_stat_now = await get_user_stat(user_id)
            new_total_cost = float(u_stat_now.get("total_cost") or 0.0) if u_stat_now else exact_cost

            try:
                alert_text = get_text(
                    "messages.admin.job_completed_alert",
                    user_name=html.escape(user_name),
                    user_id=user_id,
                    exact_cost=exact_cost,
                    new_total_cost=new_total_cost,
                    final_size_mb=final_size / (1024 * 1024)
                )[cite: 1]
                await bot.send_message(chat_id=ADMIN_ID, text=alert_text, parse_mode="HTML")
            except Exception:
                pass

    finally:
        ui_state["done"] = True
        if not ui_task.done():
            ui_task.cancel()
        for p in (input_path, output_path, thumb_path):
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass


async def main():
    load_bot_texts()
    init_prefs_cache()
    await init_db()
    logging.info("در حال اتصال کلاینت Pyrogram...")
    try:
        await pyro.start()
        logging.info("✅ کلاینت Pyrogram متصل شد.")
    except Exception as e:
        logging.error(f"خطای شروع Pyrogram: {e}")

    asyncio.create_task(queue_worker())
    logging.info("✅ ربات آنلاین و آماده دریافت ویدیو است.")
    try:
        await dp.start_polling(bot, drop_pending_updates=True)
    finally:
        if pyro.is_connected:
            await pyro.stop()
        if DB_POOL:
            await DB_POOL.close()


if __name__ == "__main__":
    asyncio.run(main())
