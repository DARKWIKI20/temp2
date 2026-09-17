import os
import gc
import re
import html
import json
import math
import time
import uuid
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
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter, TelegramForbiddenError
from pyrogram import Client as PyroClient, raw
from pyrogram.types import InlineKeyboardMarkup as PyroInlineKeyboardMarkup, InlineKeyboardButton as PyroInlineKeyboardButton

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BOT_VERSION = "2.4.4"
BOT_TOKEN = os.getenv("BOT_TOKEN", "8812733722:AAEFW8oxPPQYyqrqHGtnvS8fTpu3ATxcDbo")
ADMIN_ID = int(os.getenv("ADMIN_ID", "6616272875"))
API_ID = int(os.getenv("API_ID", "26202905"))
API_HASH = os.getenv("API_HASH", "ec9fd909b90288d01befa4f87c8d71c1")
DATABASE_URL = os.getenv("DATABASE_URL")

TEHRAN_TZ = datetime.timezone(datetime.timedelta(hours=3, minutes=30))
FFMPEG_BIN = "ffmpeg"
MAX_FILE_SIZE = 300 * 1024 * 1024
DOWNLOAD_DIR = "downloads"
PREFS_FILE = "user_prefs.json"
BACKUP_STATS_FILE = "user_stats.json"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

START_PHRASES = [
    "بزن بریم", "باشه بسته شو دیگه", "حله خداحافظ", "باشه",
    "اوکی دوکی", "خیلی ممنون", "بسته شو", "تنظیم کن",
    "حله داداش", "کاری ندارم دیگه", "همین خوبه", "ایول",
    "بسته شود بلکه پسندیده شود", "ترو خدا همین رو ذخیره کن", "حله فدات", "دمت گرم"
]

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
URL_DOWNLOADS = {}
ACTIVE_PROCESSES = {}
RUNNING_TASKS = {}
ADMIN_MEDIA_STORE = {}
VIDEO_META_CACHE = {}
DB_POOL = None
PREFS_CACHE = {}

JOB_QUEUE = asyncio.PriorityQueue()
QUEUE_COUNTER = 0


class SupportState(StatesGroup):
    waiting_for_message = State()


class AdminMessageState(StatesGroup):
    waiting_for_user_id = State()
    waiting_for_single_content = State()
    waiting_for_broadcast_content = State()
    waiting_for_custom_limit = State()
    waiting_for_reset_confirmation = State()


def get_tehran_datetime() -> tuple[str, str]:
    now = datetime.datetime.now(TEHRAN_TZ)
    time_str = now.strftime("%H:%M")
    
    gy, gm, gd = now.year, now.month, now.day
    g_d_m = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
    gy2 = gy if gm > 2 else gy - 1
    days = 355666 + (365 * gy) + ((gy2 + 3) // 4) - ((gy2 + 99) // 100) + ((gy2 + 399) // 400) + gd + g_d_m[gm - 1]
    jy = -1595 + (33 * (days // 12053))
    days %= 12053
    jy += 4 * (days // 1461)
    days %= 1461
    if days > 365:
        jy += (days - 1) // 365
        days = (days - 1) % 365
    if days < 186:
        jm = 1 + (days // 31)
        jd = 1 + (days % 31)
    else:
        jm = 7 + ((days - 186) // 30)
        jd = 1 + ((days - 186) % 30)
        
    date_str = f"{jy}/{jm:02d}/{jd:02d}"
    return time_str, date_str


def get_tehran_date() -> datetime.date:
    return datetime.datetime.now(TEHRAN_TZ).date()


def format_seconds(seconds: int) -> str:
    if not seconds or seconds <= 0:
        return "نامشخص"
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def clean_residual_downloads():
    try:
        for f in os.listdir(DOWNLOAD_DIR):
            file_path = os.path.join(DOWNLOAD_DIR, f)
            if os.path.isfile(file_path):
                os.remove(file_path)
    except Exception as e:
        logging.warning(f"Clean downloads error: {e}")


def calculate_priority(user_id: int, file_size_bytes: int) -> float:
    if user_id == ADMIN_ID:
        return -1000.0
    size_mb = file_size_bytes / (1024 * 1024)
    if size_mb <= 10:
        return size_mb
    elif size_mb <= 30:
        return 50.0 + size_mb
    elif size_mb <= 100:
        return 150.0 + size_mb
    else:
        return 400.0 + size_mb


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
        logging.warning("DATABASE_URL is not set; using local backup storage.")
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
        logging.info("PostgreSQL database synced.")
    except Exception as e:
        logging.error(f"PostgreSQL init error: {e}")


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
            logging.error(f"DB set limit error: {e}")
    stats = load_backup_stats()
    stats["daily_limit_mb"] = limit_mb
    save_backup_stats(stats)


async def reset_all_daily_usage():
    today = get_tehran_date()
    if DB_POOL:
        try:
            async with DB_POOL.acquire() as conn:
                await conn.execute("UPDATE user_stats SET today_mb = 0.0, today_date = $1;", today)
        except Exception as e:
            logging.error(f"Reset DB usage error: {e}")

    stats = load_backup_stats()
    for u in stats.get("users", {}).values():
        u["today_mb"] = 0.0
        u["today_date"] = today.isoformat()
    save_backup_stats(stats)


async def midnight_reset_worker():
    while True:
        now = datetime.datetime.now(TEHRAN_TZ)
        tomorrow = now.date() + datetime.timedelta(days=1)
        midnight = datetime.datetime.combine(tomorrow, datetime.time.min, tzinfo=TEHRAN_TZ)
        secs = (midnight - now).total_seconds()
        await asyncio.sleep(max(1.0, secs + 2.0))
        await reset_all_daily_usage()
        logging.info("Daily quota automatically reset at Tehran midnight.")


async def check_and_update_daily_usage(user_id: int, file_size_mb: float) -> tuple[bool, float, int]:
    if user_id == ADMIN_ID:
        return True, 0.0, 0

    limit = await get_daily_limit_mb()
    if limit == 0:
        return True, 0.0, 0

    today = get_tehran_date()
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
    today = get_tehran_date()
    now_str = datetime.datetime.now(TEHRAN_TZ).strftime("%Y-%m-%d %H:%M")

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
            logging.error(f"DB record error: {e}")

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
    today = get_tehran_date()
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
            logging.error(f"Error fetching top users: {e}")

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
    today = get_tehran_date()
    if DB_POOL:
        try:
            async with DB_POOL.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT user_id, 
                           COALESCE(name, 'کاربر') as name, 
                           COALESCE(username, '') as username, 
                           CAST(COALESCE(total_cost, 0.0) AS FLOAT) as total_cost, 
                           CAST(COALESCE(total_jobs, 0) AS INT) as total_jobs, 
                           today_date,
                           CAST(COALESCE(today_mb, 0.0) AS FLOAT) as today_mb, 
                           max_vid_file_id, 
                           CAST(COALESCE(max_vid_cost, 0.0) AS FLOAT) as max_vid_cost, 
                           CAST(COALESCE(max_vid_size_mb, 0.0) AS FLOAT) as max_vid_size_mb, 
                           max_vid_date
                    FROM user_stats
                    WHERE user_id = $1;
                """, user_id)
                if row:
                    res = dict(row)
                    if res.get("today_date") != today:
                        res["today_mb"] = 0.0
                    return res
        except Exception:
            pass

    u = load_backup_stats().get("users", {}).get(str(user_id))
    if not u:
        return None
    mv = u.get("max_video") or {}
    user_today_mb = float(u.get("today_mb") or 0.0) if u.get("today_date") == today.isoformat() else 0.0
    return {
        "user_id": user_id,
        "name": u.get("name") or "کاربر",
        "username": u.get("username") or "",
        "total_cost": float(u.get("total_cost") or 0.0),
        "total_jobs": int(u.get("total_jobs") or 0),
        "today_mb": user_today_mb,
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
        "mode": "video", "res": "orig", "codec": "h264",
        "crf": "medium", "mute": False, "speed": "1.0", "fmt": "orig"
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


async def run_ffmpeg_with_progress(cmd: list, ui_state: dict, total_duration: float, job_id: str) -> tuple[int, str]:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE
    )
    ACTIVE_PROCESSES[job_id]["proc"] = proc

    time_us_pattern = re.compile(r"out_time_us=(\d+)")
    time_str_pattern = re.compile(r"out_time=(\d+):(\d+):(\d+(?:\.\d+)?)")
    last_lines = []
    start_time = time.time()

    while True:
        line = await proc.stderr.readline()
        if not line:
            break

        if ACTIVE_PROCESSES.get(job_id, {}).get("cancelled"):
            try:
                proc.kill()
            except Exception:
                pass
            return -1, "cancelled"

        decoded = line.decode(errors="ignore").strip()
        if decoded:
            last_lines.append(decoded)
            if len(last_lines) > 8:
                last_lines.pop(0)

        current_secs = None
        match_us = time_us_pattern.search(decoded)
        if match_us:
            current_secs = float(match_us.group(1)) / 1_000_000.0
        else:
            match_str = time_str_pattern.search(decoded)
            if match_str:
                h, m, s = map(float, match_str.groups())
                current_secs = h * 3600 + m * 60 + s

        if current_secs is not None and total_duration > 0:
            pct = (current_secs / total_duration) * 100.0
            ui_state["percent"] = min(99.0, max(5.0, pct))
            elapsed = time.time() - start_time
            if elapsed > 2.0 and current_secs > 1.0:
                speed = current_secs / elapsed
                if speed > 0:
                    rem_secs = max(0.0, total_duration - current_secs) / speed
                    mins, secs = divmod(int(rem_secs), 60)
                    ui_state["eta"] = f"{mins} دقیقه و {secs} ثانیه" if mins > 0 else f"{secs} ثانیه"

    await proc.wait()
    
    if ACTIVE_PROCESSES.get(job_id, {}).get("cancelled"):
        return -1, "cancelled"

    if proc.returncode == -9:
        err_output = "فرایند به دلیل کمبود حافظه (OOM Killer) توسط سیستم متوقف شد."
    else:
        err_output = "\n".join(last_lines[-5:]) if last_lines else ""
        
    return proc.returncode, err_output


def get_main_reply_keyboard(user_id: int):
    builder = ReplyKeyboardBuilder()
    builder.button(text="⚙️ تنظیمات")
    builder.button(text="📊 حساب و آمار من")
    builder.button(text="📞 پشتیبانی")
    if user_id == ADMIN_ID:
        builder.button(text="👑 پنل مدیریت")
        builder.adjust(2, 2)
    else:
        builder.adjust(2, 1)
    return builder.as_markup(resize_keyboard=True)


def get_settings_inline_keyboard(user_id: int):
    show_details = get_user_show_details(user_id)
    builder = InlineKeyboardBuilder()
    toggle_text = "گزارش مشخصات: کامل ✅" if show_details else "گزارش مشخصات: خلاصه 📉"
    action_text = "تغییر به: خلاصه" if show_details else "تغییر به: کامل"
    builder.button(text=toggle_text, callback_data="none")
    builder.button(text=f"🔄 {action_text}", callback_data="toggle_details")
    builder.button(text="🎬 تنظیمات پیش‌فرض ویدیو", callback_data="open_default_settings")
    builder.button(text="💡 راهنمای فشرده‌سازی", callback_data="open_compression_guide")
    builder.button(text="🔴 پشیمون شدم", callback_data="close_settings")
    builder.adjust(1)
    return builder.as_markup()


def get_admin_panel_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🏆 پرمصرف‌ترین کاربران", callback_data="admin_top_users")
    builder.button(text="⏱ سقف سهمیه روزانه", callback_data="admin_set_limit")
    builder.button(text="📢 ارسال همگانی", callback_data="admin_broadcast")
    builder.button(text="👤 پیام به کاربر خاص", callback_data="admin_send_single")
    builder.button(text="🔴 بستن پنل", callback_data="admin_close")
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


def build_config_keyboard(cfg: dict, orig_ext: str = "mp4", max_res: int = 1080):
    b = InlineKeyboardBuilder()
    mode = cfg["mode"]
    res, codec, crf, mute, speed, fmt = cfg["res"], cfg["codec"], cfg["crf"], cfg["mute"], cfg["speed"], cfg["fmt"]

    res_options = [("orig", "کیفیت اصلی")]
    for r_val, r_label in [("1080", "1080p"), ("720", "720p"), ("480", "480p")]:
        if int(r_val) <= max_res:
            res_options.append((r_val, r_label))

    valid_keys = [k for k, _ in res_options]
    if res not in valid_keys:
        res = "orig"
        cfg["res"] = "orig"

    b.button(text="🎬 فشرده‌سازی ویدیو" + (" ✅" if mode == "video" else ""), callback_data="cfg:" + encode_cfg("video", res, codec, crf, mute, speed, "orig" if fmt not in ["mp4", "mkv", "mov"] else fmt))
    b.button(text="🎵 استخراج صوت (MP3)" + (" ✅" if mode == "audio" else ""), callback_data="cfg:" + encode_cfg("audio", res, codec, crf, mute, speed, "mp3" if fmt in ["orig", "mp4", "mkv", "mov"] else fmt))

    if mode == "video":
        b.button(text=f"📁 مثل فایل اصلی ({orig_ext.upper()})" + (" ✅" if fmt == "orig" else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, crf, mute, speed, "orig"))
        b.button(text="MP4" + (" ✅" if fmt == "mp4" else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, crf, mute, speed, "mp4"))
        b.button(text="MKV" + (" ✅" if fmt == "mkv" else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, crf, mute, speed, "mkv"))
        b.button(text="MOV" + (" ✅" if fmt == "mov" else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, crf, mute, speed, "mov"))

        for r_k, r_t in res_options:
            b.button(text=r_t + (" ✅" if res == r_k else ""), callback_data="cfg:" + encode_cfg(mode, r_k, codec, crf, mute, speed, fmt))

        b.button(text="H.264 (استاندارد)" + (" ✅" if codec == "h264" else ""), callback_data="cfg:" + encode_cfg(mode, res, "h264", crf, mute, speed, fmt))
        b.button(text="H.265 (کم‌حجم‌تر)" + (" ✅" if codec == "h265" else ""), callback_data="cfg:" + encode_cfg(mode, res, "h265", crf, mute, speed, fmt))

        for c_k, c_t in [("light", "کاهش کم (کیفیت بالا)"), ("medium", "متعادل"), ("heavy", "کاهش زیاد (فشرده)")]:
            b.button(text=c_t + (" ✅" if crf == c_k else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, c_k, mute, speed, fmt))

        b.button(text="🔇 صدا: قطع" if mute else "🔊 صدا: وصل", callback_data="cfg:" + encode_cfg(mode, res, codec, crf, not mute, speed, fmt))
    else:
        for af_k, af_t in [("mp3", "MP3"), ("wav", "WAV"), ("m4a", "M4A"), ("ogg", "OGG"), ("flac", "FLAC")]:
            b.button(text=af_t + (" ✅" if fmt == af_k else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, crf, mute, speed, af_k))

    for s_k, s_t in [("1.0", "سرعت ۱x"), ("1.5", "۱.۵ برابر"), ("2.0", "۲ برابر")]:
        b.button(text=s_t + (" ✅" if speed == s_k else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, crf, mute, s_k, fmt))

    start_phrase = random.choice(START_PHRASES)
    b.button(text=f"🟢 {start_phrase}", callback_data=f"run:{encode_cfg(mode, res, codec, crf, mute, speed, fmt)}")
    b.button(text="🔴 پشیمون شدم", callback_data="cancel_panel")

    if mode == "video":
        b.adjust(2, 1, 3, len(res_options), 2, 3, 1, 3, 2)
    else:
        b.adjust(2, 5, 3, 2)
    return b.as_markup()


def build_default_config_keyboard(cfg: dict):
    b = InlineKeyboardBuilder()
    mode = cfg["mode"]
    res, codec, crf, mute, speed, fmt = cfg["res"], cfg["codec"], cfg["crf"], cfg["mute"], cfg["speed"], cfg["fmt"]

    b.button(text="🎬 تبدیل ویدیو" + (" ✅" if mode == "video" else ""), callback_data="defcfg:" + encode_cfg("video", res, codec, crf, mute, speed, "orig" if fmt not in ["mp4", "mkv", "mov"] else fmt))
    b.button(text="🎵 استخراج صدا" + (" ✅" if mode == "audio" else ""), callback_data="defcfg:" + encode_cfg("audio", res, codec, crf, mute, speed, "mp3" if fmt in ["orig", "mp4", "mkv", "mov"] else fmt))

    if mode == "video":
        b.button(text="📁 مثل فایل اصلی" + (" ✅" if fmt == "orig" else ""), callback_data="defcfg:" + encode_cfg(mode, res, codec, crf, mute, speed, "orig"))
        b.button(text="MP4" + (" ✅" if fmt == "mp4" else ""), callback_data="defcfg:" + encode_cfg(mode, res, codec, crf, mute, speed, "mp4"))
        b.button(text="MKV" + (" ✅" if fmt == "mkv" else ""), callback_data="defcfg:" + encode_cfg(mode, res, codec, crf, mute, speed, "mkv"))
        b.button(text="MOV" + (" ✅" if fmt == "mov" else ""), callback_data="defcfg:" + encode_cfg(mode, res, codec, crf, mute, speed, "mov"))

        for r_k, r_t in [("orig", "کیفیت اصلی"), ("1080", "1080p"), ("720", "720p"), ("480", "480p")]:
            b.button(text=r_t + (" ✅" if res == r_k else ""), callback_data="defcfg:" + encode_cfg(mode, r_k, codec, crf, mute, speed, fmt))

        b.button(text="H.264 (استاندارد)" + (" ✅" if codec == "h264" else ""), callback_data="defcfg:" + encode_cfg(mode, res, "h264", crf, mute, speed, fmt))
        b.button(text="H.265 (کم‌حجم‌تر)" + (" ✅" if codec == "h265" else ""), callback_data="defcfg:" + encode_cfg(mode, res, "h265", crf, mute, speed, fmt))

        for c_k, c_t in [("light", "کاهش کم (کیفیت بالا)"), ("medium", "متعادل"), ("heavy", "کاهش زیاد (فشرده)")]:
            b.button(text=c_t + (" ✅" if crf == c_k else ""), callback_data="defcfg:" + encode_cfg(mode, res, codec, c_k, mute, speed, fmt))

        b.button(text="🔇 صدا: قطع" if mute else "🔊 صدا: وصل", callback_data="defcfg:" + encode_cfg(mode, res, codec, crf, not mute, speed, fmt))
    else:
        for af_k, af_t in [("mp3", "MP3"), ("wav", "WAV"), ("m4a", "M4A"), ("ogg", "OGG"), ("flac", "FLAC")]:
            b.button(text=af_t + (" ✅" if fmt == af_k else ""), callback_data="defcfg:" + encode_cfg(mode, res, codec, crf, mute, speed, af_k))

    for s_k, s_t in [("1.0", "سرعت ۱x"), ("1.5", "۱.۵ برابر"), ("2.0", "۲ برابر")]:
        b.button(text=s_t + (" ✅" if speed == s_k else ""), callback_data="defcfg:" + encode_cfg(mode, res, codec, crf, mute, speed, fmt))

    b.button(text="🔴 بازگشت به تنظیمات", callback_data="back_to_settings")

    if mode == "video":
        b.adjust(2, 1, 3, 4, 2, 3, 1, 3, 1)
    else:
        b.adjust(2, 5, 3, 1)
    return b.as_markup()


def build_audio_keyboard(bitrate="96k", fmt="mp3", speed="1.0"):
    b = InlineKeyboardBuilder()
    for br, txt in [("128k", "کیفیت بالا (128)"), ("96k", "متعادل (96)"), ("64k", "کاهش زیاد (64)"), ("48k", "فوق‌فشرده (48)")]:
        b.button(text=txt + (" ✅" if bitrate == br else ""), callback_data=f"acfg:{br}:{fmt}:{speed}")
    for af, txt in [("mp3", "MP3"), ("m4a", "M4A"), ("ogg", "OGG"), ("flac", "FLAC")]:
        b.button(text=txt + (" ✅" if fmt == af else ""), callback_data=f"acfg:{bitrate}:{af}:{speed}")
    for sp, txt in [("1.0", "سرعت ۱x"), ("1.25", "۱.۲۵x"), ("1.5", "۱.۵x")]:
        b.button(text=txt + (" ✅" if speed == sp else ""), callback_data=f"acfg:{bitrate}:{fmt}:{sp}")
    
    start_phrase = random.choice(START_PHRASES)
    b.button(text=f"🟢 {start_phrase}", callback_data=f"arun:{bitrate}:{fmt}:{speed}")
    b.button(text="🔴 پشیمون شدم", callback_data="cancel_panel")
    b.adjust(2, 2, 4, 3, 2)
    return b.as_markup()


def get_cancel_keyboard(job_id: str):
    b = InlineKeyboardBuilder()
    b.button(text="🔴 پشیمون شدم", callback_data=f"stop:{job_id}")
    return b.as_markup()


async def get_media_meta(file_path: str) -> dict:
    meta = {"duration": 0, "width": 1280, "height": 720, "has_audio": False}
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-show_entries", "stream=width,height,duration,codec_type:format=duration",
            "-of", "json", file_path
        ]
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=4.0)
        data = json.loads(stdout.decode(errors="ignore"))

        if "format" in data and "duration" in data["format"]:
            meta["duration"] = int(float(data["format"]["duration"]))

        if "streams" in data:
            for s in data["streams"]:
                c_type = s.get("codec_type")
                if c_type == "video":
                    if "width" in s and "height" in s:
                        meta["width"] = int(s["width"])
                        meta["height"] = int(s["height"])
                    if meta["duration"] == 0 and "duration" in s:
                        meta["duration"] = int(float(s["duration"]))
                elif c_type == "audio":
                    meta["has_audio"] = True
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


@dp.message(CommandStart(), StateFilter("*"))
async def start_handler(message: aiotypes.Message, state: FSMContext):
    await state.clear()
    u = message.from_user
    await register_user(u.id, u.full_name or "", u.username or "")

    time_str, date_str = get_tehran_datetime()
    user_name = html.escape(u.full_name or "کاربر")

    welcome_text = (
        f"👋 <b>سلام {user_name}، خوش اومدی!</b>\n\n"
        f"<blockquote>با این ربات می‌تونی ویدیو و صدات رو بهینه‌سازی و کم‌حجم کنی یا از یوتیوب و اینستاگرام ویدیو بگیری.</blockquote>\n\n"
        f"<blockquote>🤖 <b>نسخه ربات:</b> {BOT_VERSION}\n"
        f"⏰ <b>زمان جاری:</b> {time_str} | {date_str}</blockquote>\n\n"
        f"برای شروع یکی از گزینه‌های زیر رو انتخاب کن:"
    )

    builder = InlineKeyboardBuilder()
    builder.button(text="⚙️ تنظیمات", callback_data="open_settings")
    builder.button(text="📊 حساب و آمار من", callback_data="show_my_stats")
    builder.button(text="📞 پشتیبانی", callback_data="start_support")
    builder.adjust(2, 1)

    await message.answer(welcome_text, reply_markup=get_main_reply_keyboard(message.from_user.id), parse_mode="HTML")
    await message.answer("📌 منوی دسترسی سریع:", reply_markup=builder.as_markup())


@dp.message(F.text == "📊 حساب و آمار من", StateFilter("*"))
@dp.callback_query(F.data == "show_my_stats", StateFilter("*"))
async def show_user_profile_stats(event: aiotypes.Message | aiotypes.CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = event.from_user.id
    u_stat = await get_user_stat(user_id)
    limit = await get_daily_limit_mb()

    today_mb = float(u_stat.get("today_mb") or 0.0) if u_stat else 0.0
    total_jobs = int(u_stat.get("total_jobs") or 0) if u_stat else 0
    
    if limit > 0:
        pct = min(100.0, (today_mb / limit) * 100.0)
        progress_bar_str = generate_progress_bar(pct)
        rem_mb_str = f"{max(0.0, limit - today_mb):.1f} مگابایت از {limit} مگابایت"
    else:
        progress_bar_str = "[████████████] نامحدود"
        rem_mb_str = "نامحدود"

    text = (
        f"📊 <b>وضعیت حساب کاربری</b>\n\n"
        f"<blockquote>🆔 شناسه: <code>{user_id}</code>\n"
        f"🎬 پردازش‌های موفق: <b>{total_jobs} فایل</b>\n"
        f"📦 مصرف امروز: <b>{today_mb:.1f} مگابایت</b>\n\n"
        f"📈 <b>سهمیه مصرف روزانه:</b>\n"
        f"{progress_bar_str}\n"
        f"▫️ باقیمانده: <b>{rem_mb_str}</b></blockquote>\n\n"
        f"💡 <i>سهمیه روزانه هر شب ساعت ۰۰:۰۰ بازنشانی می‌شود.</i>"
    )
    if isinstance(event, aiotypes.CallbackQuery):
        await event.answer()
        await event.message.answer(text, parse_mode="HTML")
    else:
        await event.answer(text, parse_mode="HTML")


@dp.callback_query(F.data == "open_compression_guide", StateFilter("*"))
async def send_compression_guide(callback: aiotypes.CallbackQuery):
    await callback.answer()
    guide_text = (
        "💡 <b>راهنمای بهینه‌سازی و فشرده‌سازی</b>\n\n"
        "<blockquote>▫️ <b>فایل‌های بهینه‌شده:</b> ویدیوهای شبکه‌های اجتماعی قبلاً فشرده شده‌اند و فشرده‌سازی مجدد ممکن است حجم را تغییر ندهد.\n"
        "▫️ <b>نویز و حرکات سریع:</b> برفک تصویر و صحنه‌های پرحرکت بیت‌ریت فایل را افزایش می‌دهند.</blockquote>\n\n"
        "🛠 <b>ترفندهای کاربردی:</b>\n"
        "<blockquote>۱. برای ویدیو، انکودر <b>H.265</b> بالاترین کاهش حجم را بدون افت محسوس کیفیت دارد.\n"
        "۲. برای فایل‌های صوتی، بیت‌ریت‌های <b>96k</b> یا <b>64k</b> حجم را به شکل محسوسی کاهش می‌دهند.</blockquote>"
    )
    await callback.message.answer(guide_text, parse_mode="HTML")


URL_REGEX = re.compile(r'(https?://[^\s]+)')

@dp.message(F.text.regexp(URL_REGEX), StateFilter("*"))
async def handle_url_message(message: aiotypes.Message, state: FSMContext):
    await state.clear()
    u = message.from_user
    await register_user(u.id, u.full_name or "", u.username or "")

    match = URL_REGEX.search(message.text)
    if not match:
        return
    url = match.group(1).strip()

    token = uuid.uuid4().hex[:8]
    if len(URL_DOWNLOADS) > 300:
        URL_DOWNLOADS.pop(next(iter(URL_DOWNLOADS)))

    URL_DOWNLOADS[token] = {
        "url": url,
        "user_id": u.id,
        "name": u.full_name or "کاربر",
        "username": f"@{u.username}" if u.username else "ندارد",
        "chat_id": message.chat.id
    }

    builder = InlineKeyboardBuilder()
    builder.button(text="🎬 کیفیت 720p", callback_data=f"ytdl:{token}:720")
    builder.button(text="📱 کیفیت 480p", callback_data=f"ytdl:{token}:480")
    builder.button(text="🔴 پشیمون شدم", callback_data=f"ytdl_cancel:{token}")
    builder.adjust(2, 1)

    await message.reply(
        "🔗 <b>لینک ویدیو شناسایی شد</b>\n"
        "<blockquote>پشتیبانی از یوتیوب، اینستاگرام، تیک‌تاک و سایر سرویس‌ها.</blockquote>\n\n"
        "کیفیت مدنظرت رو انتخاب کن:",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )


@dp.callback_query(F.data.startswith("ytdl_cancel:"), StateFilter("*"))
async def cancel_url_dl(callback: aiotypes.CallbackQuery):
    token = callback.data.split(":")[1]
    URL_DOWNLOADS.pop(token, None)
    await callback.answer("عملیات لغو شد.")
    await callback.message.edit_text("عملیات لغو شد.")


@dp.callback_query(F.data.startswith("ytdl:"), StateFilter("*"))
async def process_ytdl_download(callback: aiotypes.CallbackQuery):
    await callback.answer()
    _, token, quality = callback.data.split(":")
    item = URL_DOWNLOADS.get(token)

    if not item:
        return await callback.message.edit_text("❌ لینک نامعتبر یا منقضی شده است.")

    url = item["url"]
    chat_id = item["chat_id"]
    user_id = item["user_id"]
    user_name = item["name"]
    username = item["username"]
    out_template = os.path.join(DOWNLOAD_DIR, f"ytdl_{token}.%(ext)s")

    status_msg = await callback.message.edit_text(
        f"⏳ <b>در حال دانلود از سرور مرجع ({quality}p)...</b>\nلطفاً چند لحظه صبر کنید.",
        parse_mode="HTML"
    )

    fmt_selector = f"bestvideo[height<={quality}]+bestaudio/best[height<={quality}]/best"
    cmd = [
        "yt-dlp",
        "--no-playlist",
        "--merge-output-format", "mp4",
        "-f", fmt_selector,
        "--max-filesize", "300M",
        "-o", out_template,
        url
    ]

    downloaded_file = None
    thumb_path = None

    try:
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        _, stderr = await proc.communicate()

        if proc.returncode != 0:
            err_text = stderr.decode(errors="ignore")
            raise RuntimeError(f"yt-dlp failure: {err_text[-400:]}")

        for fname in os.listdir(DOWNLOAD_DIR):
            if fname.startswith(f"ytdl_{token}") and not fname.endswith((".part", ".ytdl")):
                downloaded_file = os.path.join(DOWNLOAD_DIR, fname)
                break

        if not downloaded_file or not os.path.exists(downloaded_file):
            raise RuntimeError("File not found or exceeded size limit")

        fsize = os.path.getsize(downloaded_file)
        if fsize > MAX_FILE_SIZE:
            if os.path.exists(downloaded_file):
                os.remove(downloaded_file)
            return await status_msg.edit_text("⚠️ حجم ویدیو بیش از سقف مجاز ۳۰۰ مگابایت است.")

        meta = await get_media_meta(downloaded_file)
        thumb_path = os.path.join(DOWNLOAD_DIR, f"ytdl_thumb_{token}.jpg")
        await generate_thumbnail(downloaded_file, thumb_path, meta["duration"])

        await status_msg.edit_text("📤 دانلود فایل تمام شد؛ در حال ارسال...")

        sent_video = await pyro.send_video(
            chat_id=chat_id,
            video=downloaded_file,
            duration=meta["duration"],
            width=meta["width"],
            height=meta["height"],
            thumb=thumb_path if (os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 100) else None,
            caption=(
                f"🎬 <b>ویدیوی شما با موفقیت دریافت شد</b>\n\n"
                f"<blockquote>📁 فرمت: <b>MP4</b>\n"
                f"📦 حجم: <b>{fsize / (1024*1024):.2f} مگابایت</b>\n"
                f"📐 ابعاد: <b>{meta['width']}x{meta['height']} ({quality}p)</b></blockquote>"
            ),
            reply_markup=PyroInlineKeyboardMarkup([[
                PyroInlineKeyboardButton("🗜 فشرده‌سازی این ویدیو", callback_data=f"compress_from_dl:{token}")
            ]])
        )

        await status_msg.delete()

        USER_REQUESTS[f"dl_msg_{token}"] = {
            "file_id": sent_video.video.file_id,
            "user_id": user_id,
            "name": user_name,
            "chat_id": chat_id,
            "message_id": sent_video.id,
            "max_res": int(quality),
            "width": meta["width"],
            "height": meta["height"],
            "file_size": fsize
        }

    except Exception as e:
        tb = traceback.format_exc()
        logging.error(f"yt-dlp error: {tb}")

        admin_alert = (
            f"🚨 <b>خطای دانلود پیوند (yt-dlp)</b>\n\n"
            f"<blockquote>👤 <b>کاربر:</b> {html.escape(user_name)} (<code>{user_id}</code>)\n"
            f"🔗 <b>یوزرنیم:</b> {html.escape(username)}\n"
            f"🌐 <b>پیوند:</b> <code>{html.escape(url[:150])}</code></blockquote>\n\n"
            f"📋 <b>لاگ فنی:</b>\n<pre>{html.escape(str(e)[:500])}</pre>"
        )
        try:
            await bot.send_message(chat_id=ADMIN_ID, text=admin_alert, parse_mode="HTML")
        except Exception:
            pass

        try:
            await status_msg.edit_text("⚠️ دانلود ویدیو ناموفق بود. ممکن است لینک ارسالی خصوصی، نامعتبر یا دارای محدودیت باشد.", parse_mode="HTML")
        except Exception:
            pass

    finally:
        for p in (downloaded_file, thumb_path):
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass


@dp.callback_query(F.data.startswith("compress_from_dl:"), StateFilter("*"))
async def open_compress_panel_for_downloaded(callback: aiotypes.CallbackQuery):
    await callback.answer()
    token = callback.data.split(":")[1]
    saved_req = USER_REQUESTS.get(f"dl_msg_{token}")

    if not saved_req:
        return await callback.message.reply("❌ اطلاعات ویدیو منقضی شده است. لطفاً ویدیو را دوباره ارسال کنید.")

    user_default_cfg = get_user_default_cfg(callback.from_user.id)
    default_cfg = dict(user_default_cfg)
    dl_max_res = saved_req.get("max_res", 720)

    w = saved_req.get("width", 1280)
    h = saved_req.get("height", 720)
    res_str = f"{w}x{h} ({dl_max_res}p)"

    sent_panel = await callback.message.reply(
        f"⚙️ <b>تنظیمات فشرده‌سازی ویدیو</b>\n\n"
        f"<blockquote>📁 فرمت: <b>MP4</b>\n"
        f"📐 کیفیت ورودی: <b>{res_str}</b></blockquote>\n\n"
        f"تنظیمات مدنظرت رو انتخاب کن و دکمه شروع رو بزن:",
        reply_markup=build_config_keyboard(default_cfg, orig_ext="mp4", max_res=dl_max_res),
        parse_mode="HTML"
    )

    if len(VIDEO_META_CACHE) > 500:
        VIDEO_META_CACHE.pop(next(iter(VIDEO_META_CACHE)))
    VIDEO_META_CACHE[sent_panel.message_id] = {
        "orig_ext": "mp4",
        "max_res": dl_max_res,
        "res_str": res_str,
        "media_type": "video",
        "file_size": saved_req.get("file_size", 0),
        "file_id": saved_req["file_id"],
        "orig_msg_id": saved_req.get("message_id", sent_panel.message_id)
    }


# --- پنل مدیریت ادمین ---
@dp.message(Command("admin"), StateFilter("*"))
@dp.message(F.text == "👑 پنل مدیریت", StateFilter("*"))
async def admin_panel_handler(message: aiotypes.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.clear()
    all_users = await get_all_user_ids()
    limit = await get_daily_limit_mb()
    limit_str = f"{limit} مگابایت" if limit > 0 else "نامحدود"
    await message.answer(
        f"👑 <b>پنل مدیریت ربات</b>\n\n"
        f"<blockquote>👥 کاربران ثبت‌شده: <b>{len(all_users)} نفر</b>\n"
        f"⏱ سقف مصرف روزانه: <b>{limit_str}</b></blockquote>\n\n"
        f"عملیات مورد نظر را انتخاب کنید:",
        reply_markup=get_admin_panel_keyboard(),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "admin_close", StateFilter("*"))
async def close_admin_panel(callback: aiotypes.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await callback.answer()
    await callback.message.delete()


@dp.callback_query(F.data == "admin_top_users", StateFilter("*"))
async def show_top_users(callback: aiotypes.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()

    top_users = await get_top_users(limit=10)
    if not top_users:
        return await callback.message.answer("📊 هیچ کاربری در سیستم ثبت نشده است.")

    builder = InlineKeyboardBuilder()
    text_lines = ["🏆 <b>رتبه‌بندی کاربران پرمصرف:</b>\n"]

    for idx, u in enumerate(top_users, start=1):
        safe_name = html.escape(u.get("name") or "کاربر")
        cost = float(u.get("total_cost") or 0.0)
        jobs = int(u.get("total_jobs") or 0)
        uid = u.get("user_id")
        text_lines.append(f"<blockquote><b>{idx}.</b> {safe_name} | هزینه: <b>${cost:.4f}</b> ({jobs} پردازش)</blockquote>")
        builder.button(text=f"{idx}. {safe_name[:12]} (${cost:.4f})", callback_data=f"adm_u_stat:{uid}")

    builder.button(text="🔴 بازگشت به پنل", callback_data="admin_back_main")
    builder.adjust(1)

    await callback.message.answer("\n".join(text_lines), reply_markup=builder.as_markup(), parse_mode="HTML")


@dp.callback_query(F.data.startswith("adm_u_stat:"), StateFilter("*"))
async def show_single_user_stat(callback: aiotypes.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    target_uid = int(callback.data.split(":")[1])

    u = await get_user_stat(target_uid)
    if not u:
        return await callback.message.answer("اطلاعات کاربر یافت نشد.")

    safe_name = html.escape(u.get("name") or "نامشخص")
    uname = f"@{html.escape(u['username'])}" if u.get("username") else "ندارد"
    cost = float(u.get("total_cost") or 0.0)
    jobs = int(u.get("total_jobs") or 0)
    today_mb = float(u.get("today_mb") or 0.0)

    text = (
        f"👤 <b>آمار مصرف کاربر:</b>\n\n"
        f"<blockquote>▫️ نام: {safe_name}\n"
        f"▫️ نام کاربری: {uname}\n"
        f"▫️ شناسه عددی: <code>{target_uid}</code>\n"
        f"▫️ هزینه کل: <b>${cost:.5f}</b>\n"
        f"▫️ تعداد پردازش‌ها: {jobs}\n"
        f"▫️ مصرف امروز: {today_mb:.1f} مگابایت</blockquote>"
    )

    builder = InlineKeyboardBuilder()
    if u.get("max_vid_file_id"):
        max_cost = float(u.get("max_vid_cost") or 0.0)
        max_size = float(u.get("max_vid_size_mb") or 0.0)
        text += (
            f"\n\n🔥 <b>سنگین‌ترین پردازش:</b>\n"
            f"<blockquote>▫️ هزینه: <b>${max_cost:.5f}</b>\n"
            f"▫️ حجم ورودی: {max_size:.1f} مگابایت\n"
            f"▫️ تاریخ: {u.get('max_vid_date') or 'نامشخص'}</blockquote>"
        )
        builder.button(text="🎬 دریافت ویدیو", callback_data=f"adm_get_vid:{target_uid}")

    builder.button(text="🔴 بازگشت به لیست", callback_data="admin_top_users")
    builder.adjust(1)

    await callback.message.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")


@dp.callback_query(F.data.startswith("adm_get_vid:"), StateFilter("*"))
async def send_max_consuming_video(callback: aiotypes.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer("در حال ارسال ویدیو...")
    target_uid = int(callback.data.split(":")[1])

    u = await get_user_stat(target_uid)
    if not u or not u.get("max_vid_file_id"):
        return await callback.message.answer("ویدیویی برای این کاربر ثبت نشده است.")

    cap = (
        f"🎬 <b>سنگین‌ترین پردازش کاربر <code>{target_uid}</code></b>\n\n"
        f"<blockquote>💵 هزینه سرور: <b>${float(u.get('max_vid_cost') or 0.0):.5f}</b>\n"
        f"📦 حجم ورودی: <b>{float(u.get('max_vid_size_mb') or 0.0):.1f} مگابایت</b>\n"
        f"📅 تاریخ: <b>{u.get('max_vid_date') or 'نامشخص'}</b></blockquote>"
    )

    try:
        await bot.send_video(chat_id=ADMIN_ID, video=u["max_vid_file_id"], caption=cap, parse_mode="HTML")
    except Exception:
        try:
            await bot.send_document(chat_id=ADMIN_ID, document=u["max_vid_file_id"], caption=cap, parse_mode="HTML")
        except Exception as e:
            await callback.message.answer(f"خطا در ارسال فایل:\n<code>{html.escape(str(e))}</code>", parse_mode="HTML")


@dp.callback_query(F.data == "admin_set_limit", StateFilter("*"))
async def show_limit_settings(callback: aiotypes.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    cur_limit = await get_daily_limit_mb()
    cur_str = f"{cur_limit} مگابایت" if cur_limit > 0 else "نامحدود"

    builder = InlineKeyboardBuilder()
    builder.button(text="100 MB", callback_data="set_lim:100")
    builder.button(text="300 MB", callback_data="set_lim:300")
    builder.button(text="500 MB", callback_data="set_lim:500")
    builder.button(text="1000 MB (1GB)", callback_data="set_lim:1000")
    builder.button(text="2000 MB (2GB)", callback_data="set_lim:2000")
    builder.button(text="نامحدود ♾", callback_data="set_lim:0")
    builder.button(text="✏️ مقدار دلخواه", callback_data="set_lim_custom")
    builder.button(text="🔄 ریست کردن سهمیه", callback_data="admin_reset_quota_prompt")
    builder.button(text="🔴 بازگشت به پنل", callback_data="admin_back_main")
    builder.adjust(3, 3, 1, 1, 1)

    text = (
        f"⏱ <b>تنظیم سهمیه مصرف روزانه کاربران:</b>\n\n"
        f"<blockquote>▫️ سقف فعلی: <b>{cur_str}</b></blockquote>\n\n"
        f"یکی از مقادیر را انتخاب کنید یا عدد دلخواه بفرستید:"
    )
    await callback.message.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")


@dp.callback_query(F.data.startswith("set_lim:"), StateFilter("*"))
async def apply_preset_limit(callback: aiotypes.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    val = int(callback.data.split(":")[1])
    await set_daily_limit_mb(val)
    val_str = f"{val} مگابایت" if val > 0 else "نامحدود"
    await callback.answer(f"سقف مصرف روزانه روی {val_str} تنظیم شد.", show_alert=True)
    await show_limit_settings(callback)


@dp.callback_query(F.data == "admin_reset_quota_prompt", StateFilter("*"))
async def prompt_reset_quota(callback: aiotypes.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    cancel_b = InlineKeyboardBuilder()
    cancel_b.button(text="🔴 انصراف", callback_data="cancel_admin_action")

    await callback.message.answer(
        "⚠️ <b>هشدار ریست سهمیه روزانه</b>\n\n"
        "<blockquote>برای مطمئن شدن بنویس:\n"
        "<code>ریست کردن</code></blockquote>",
        reply_markup=cancel_b.as_markup(),
        parse_mode="HTML"
    )
    await state.set_state(AdminMessageState.waiting_for_reset_confirmation)


@dp.message(AdminMessageState.waiting_for_reset_confirmation, F.chat.id == ADMIN_ID)
async def process_reset_quota_confirmation(message: aiotypes.Message, state: FSMContext):
    text = message.text.strip() if message.text else ""
    if text == "ریست کردن":
        await reset_all_daily_usage()
        await state.clear()
        await message.answer("✅ سهمیه مصرف روزانه تمامی کاربران با موفقیت ریست شد.", parse_mode="HTML")
    else:
        await state.clear()
        await message.answer("❌ عبارت تأیید ارسال نشد. عملیات لغو شد.", parse_mode="HTML")


@dp.callback_query(F.data == "set_lim_custom", StateFilter("*"))
async def ask_custom_limit(callback: aiotypes.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    cancel_b = InlineKeyboardBuilder()
    cancel_b.button(text="🔴 انصراف", callback_data="cancel_admin_action")

    await callback.message.answer(
        "✏️ مقدار سقف مصرف روزانه را به <b>مگابایت (MB)</b> وارد کنید (برای نامحدود <code>0</code> بفرستید):",
        reply_markup=cancel_b.as_markup(),
        parse_mode="HTML"
    )
    await state.set_state(AdminMessageState.waiting_for_custom_limit)


@dp.message(AdminMessageState.waiting_for_custom_limit, F.chat.id == ADMIN_ID)
async def process_custom_limit_input(message: aiotypes.Message, state: FSMContext):
    text = message.text.strip() if message.text else ""
    if not text.isdigit():
        return await message.answer("لطفاً فقط عدد انگلیسی وارد کنید:")

    val = int(text)
    await set_daily_limit_mb(val)
    await state.clear()
    val_str = f"{val} مگابایت" if val > 0 else "نامحدود"
    await message.answer(f"✅ سقف مصرف روزانه روی <b>{val_str}</b> تنظیم شد.", parse_mode="HTML")


@dp.callback_query(F.data == "admin_back_main", StateFilter("*"))
async def back_to_admin_main(callback: aiotypes.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await callback.answer()
    all_users = await get_all_user_ids()
    limit = await get_daily_limit_mb()
    limit_str = f"{limit} مگابایت" if limit > 0 else "نامحدود"
    await callback.message.edit_text(
        f"👑 <b>پنل مدیریت ربات</b>\n\n"
        f"<blockquote>👥 کاربران ثبت‌شده: <b>{len(all_users)} نفر</b>\n"
        f"⏱ سقف مصرف روزانه: <b>{limit_str}</b></blockquote>\n\n"
        f"عملیات مورد نظر را انتخاب کنید:",
        reply_markup=get_admin_panel_keyboard(),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "admin_broadcast", StateFilter("*"))
async def start_broadcast(callback: aiotypes.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    users = await get_all_user_ids()
    cancel_b = InlineKeyboardBuilder()
    cancel_b.button(text="🔴 انصراف", callback_data="cancel_admin_action")

    await callback.message.answer(
        f"⚠️ <b>ارسال پیام همگانی</b>\n<blockquote>این پیام برای تمام کاربران ({len(users)} نفر) ارسال خواهد شد.</blockquote>\n\n"
        f"متن، عکس یا رسانه مورد نظر را بفرستید:",
        reply_markup=cancel_b.as_markup(),
        parse_mode="HTML"
    )
    await state.set_state(AdminMessageState.waiting_for_broadcast_content)


@dp.message(AdminMessageState.waiting_for_broadcast_content, F.chat.id == ADMIN_ID)
async def process_broadcast(message: aiotypes.Message, state: FSMContext):
    users = await get_all_user_ids()
    await message.answer(f"⏳ در حال ارسال همگانی به {len(users)} کاربر... لطفاً صبور باشید.")

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
        f"📢 <b>نتیجه ارسال همگانی:</b>\n\n"
        f"<blockquote>✅ ارسال موفق: <b>{success} نفر</b>\n"
        f"❌ ناموفق (بلاک یا غیرفعال): <b>{failed} نفر</b>\n"
        f"📊 مجموع مخاطبان: <b>{len(users)} نفر</b></blockquote>",
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "admin_send_single", StateFilter("*"))
async def ask_user_id_for_single(callback: aiotypes.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    cancel_b = InlineKeyboardBuilder()
    cancel_b.button(text="🔴 انصراف", callback_data="cancel_admin_action")

    await callback.message.answer(
        "👤 <b>شناسه عددی</b> کاربر را وارد کنید:",
        reply_markup=cancel_b.as_markup(),
        parse_mode="HTML"
    )
    await state.set_state(AdminMessageState.waiting_for_user_id)


@dp.callback_query(F.data.startswith("reply_to_user:"), StateFilter("*"))
async def quick_reply_to_user(callback: aiotypes.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    target_id = int(callback.data.split(":")[1])
    await state.update_data(target_id=target_id, user_name="کاربر", username="")

    cancel_b = InlineKeyboardBuilder()
    cancel_b.button(text="🔴 انصراف", callback_data="cancel_admin_action")

    await callback.message.answer(
        f"✉️ پاسخ خود را برای کاربر <code>{target_id}</code> ارسال کنید:",
        reply_markup=cancel_b.as_markup(),
        parse_mode="HTML"
    )
    await state.set_state(AdminMessageState.waiting_for_single_content)


@dp.message(AdminMessageState.waiting_for_user_id, F.chat.id == ADMIN_ID)
async def process_user_id_input(message: aiotypes.Message, state: FSMContext):
    text = message.text.strip() if message.text else ""
    if not text.isdigit():
        return await message.answer("لطفاً فقط شناسه عددی ارسال کنید:")

    target_id = int(text)
    user_name = "نامشخص"
    username = "ندارد"

    try:
        chat = await bot.get_chat(target_id)
        user_name = chat.full_name or "بدون نام"
        username = f"@{chat.username}" if chat.username else "ندارد"
    except Exception:
        pass

    await state.update_data(target_id=target_id, user_name=user_name, username=username)

    cancel_b = InlineKeyboardBuilder()
    cancel_b.button(text="🔴 انصراف", callback_data="cancel_admin_action")

    confirm_text = (
        f"🎯 <b>مشخصات مخاطب:</b>\n\n"
        f"<blockquote>👤 نام: {html.escape(user_name)}\n"
        f"🔗 نام کاربری: {html.escape(username)}\n"
        f"🆔 شناسه: <code>{target_id}</code></blockquote>\n\n"
        f"پیام مورد نظر خود را بنویسید:"
    )
    await message.answer(confirm_text, reply_markup=cancel_b.as_markup(), parse_mode="HTML")
    await state.set_state(AdminMessageState.waiting_for_single_content)


@dp.message(AdminMessageState.waiting_for_single_content, F.chat.id == ADMIN_ID)
async def send_single_message_to_user(message: aiotypes.Message, state: FSMContext):
    data = await state.get_data()
    target_id = data.get("target_id")
    user_name = data.get("user_name", "کاربر")

    try:
        await bot.send_message(chat_id=target_id, text="💬 <b>پیام پشتیبانی:</b>", parse_mode="HTML")
        await message.copy_to(chat_id=target_id)
        await message.answer(f"✅ پیام برای <b>{html.escape(user_name)}</b> (<code>{target_id}</code>) ارسال شد.", parse_mode="HTML")
    except TelegramForbiddenError:
        await message.answer("❌ کاربر ربات را مسدود کرده است.")
    except Exception as e:
        await message.answer(f"خطا در ارسال پیام:\n<code>{html.escape(str(e))}</code>", parse_mode="HTML")

    await state.clear()


@dp.callback_query(F.data == "cancel_admin_action", StateFilter("*"))
async def cancel_admin_action(callback: aiotypes.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await callback.answer("عملیات لغو شد.")
    await callback.message.edit_text("عملیات لغو شد.")


@dp.message(F.text == "⚙️ تنظیمات", StateFilter("*"))
@dp.callback_query(F.data == "open_settings", StateFilter("*"))
async def show_settings_menu(event: aiotypes.Message | aiotypes.CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = event.from_user.id
    u = event.from_user
    await register_user(user_id, u.full_name or "", u.username or "")

    text = (
        "⚙️ <b>تنظیمات پیشرفته ربات</b>\n\n"
        "<blockquote>می‌تونی نحوه نمایش گزارش‌ها و پیکربندی پیش‌فرض ویدیوها رو اینجا تغییر بدی:</blockquote>"
    )
    kb = get_settings_inline_keyboard(user_id)
    if isinstance(event, aiotypes.CallbackQuery):
        await event.answer()
        await event.message.answer(text, reply_markup=kb, parse_mode="HTML")
    else:
        await event.answer(text, reply_markup=kb, parse_mode="HTML")


@dp.callback_query(F.data == "toggle_details", StateFilter("*"))
async def toggle_settings_option(callback: aiotypes.CallbackQuery):
    user_id = callback.from_user.id
    current_status = get_user_show_details(user_id)
    set_user_show_details(user_id, not current_status)
    await callback.answer("تنظیمات نمایش گزارش تغییر کرد.", show_alert=False)
    try:
        await callback.message.edit_reply_markup(reply_markup=get_settings_inline_keyboard(user_id))
    except TelegramBadRequest:
        pass


@dp.callback_query(F.data == "open_default_settings", StateFilter("*"))
async def show_default_settings(callback: aiotypes.CallbackQuery):
    user_id = callback.from_user.id
    user_cfg = get_user_default_cfg(user_id)
    text = (
        "🎬 <b>تنظیمات پیش‌فرض پردازش ویدیو</b>\n\n"
        "<blockquote>هر تنظیمی رو اینجا ثبت کنی، در تبدیل‌های بعدی به صورت پیش‌فرض انتخاب می‌شه.</blockquote>\n\n"
        "💡 <i>تغییرات بلافاصله ذخیره می‌شوند.</i>"
    )
    await callback.answer()
    await callback.message.edit_text(
        text,
        reply_markup=build_default_config_keyboard(user_cfg),
        parse_mode="HTML"
    )


@dp.callback_query(F.data.startswith("defcfg:"), StateFilter("*"))
async def update_default_settings_callback(callback: aiotypes.CallbackQuery):
    cfg = decode_cfg(callback.data[7:])
    set_user_default_cfg(callback.from_user.id, cfg)
    await callback.answer("ذخیره شد.", show_alert=False)
    try:
        await callback.message.edit_reply_markup(reply_markup=build_default_config_keyboard(cfg))
    except TelegramBadRequest:
        pass


@dp.callback_query(F.data == "back_to_settings", StateFilter("*"))
async def back_to_settings_menu(callback: aiotypes.CallbackQuery):
    user_id = callback.from_user.id
    text = (
        "⚙️ <b>تنظیمات پیشرفته ربات</b>\n\n"
        "<blockquote>می‌تونی نحوه نمایش گزارش‌ها و پیکربندی پیش‌فرض ویدیوها رو اینجا تغییر بدی:</blockquote>"
    )
    await callback.answer()
    await callback.message.edit_text(
        text,
        reply_markup=get_settings_inline_keyboard(user_id),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "close_settings", StateFilter("*"))
async def close_settings_menu(callback: aiotypes.CallbackQuery):
    await callback.answer()
    await callback.message.delete()


@dp.callback_query(F.data == "none", StateFilter("*"))
async def no_action_callback(callback: aiotypes.CallbackQuery):
    await callback.answer()


@dp.message(F.text == "📞 پشتیبانی", StateFilter("*"))
@dp.callback_query(F.data == "start_support", StateFilter("*"))
async def ask_support_message(event: aiotypes.Message | aiotypes.CallbackQuery, state: FSMContext):
    u = event.from_user
    await register_user(u.id, u.full_name or "", u.username or "")

    cancel_b = InlineKeyboardBuilder()
    cancel_b.button(text="🔴 پشیمون شدم", callback_data="cancel_support")

    msg_text = "✍️ پیام، سوال یا مشکلت رو بنویس و ارسال کن (متن، عکس، ویس و...):"
    if isinstance(event, aiotypes.CallbackQuery):
        await event.answer()
        await event.message.answer(msg_text, reply_markup=cancel_b.as_markup())
    else:
        await event.answer(msg_text, reply_markup=cancel_b.as_markup())

    await state.set_state(SupportState.waiting_for_message)


@dp.callback_query(F.data == "cancel_support", StateFilter("*"))
async def cancel_support(callback: aiotypes.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.answer("لغو شد.")
    await callback.message.edit_text("عملیات لغو شد.")


@dp.message(SupportState.waiting_for_message)
async def forward_support_message(message: aiotypes.Message, state: FSMContext):
    u = message.from_user
    user_id = u.id
    name = html.escape(u.full_name or "بدون نام")
    username = f"@{html.escape(u.username)}" if u.username else "ندارد"
    await register_user(user_id, u.full_name or "", u.username or "")

    admin_header = (
        f"📩 <b>تیکت پشتیبانی جدید</b>\n\n"
        f"<blockquote>👤 نام: {name}\n"
        f"🆔 شناسه: <code>{user_id}</code>\n"
        f"🔗 نام کاربری: {username}</blockquote>"
    )

    reply_kb = InlineKeyboardBuilder()
    reply_kb.button(text="✍️ پاسخ به کاربر", callback_data=f"reply_to_user:{user_id}")

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

        await message.reply("✅ پیام شما برای پشتیبانی ارسال شد. به زودی پاسخ را دریافت خواهید کرد.")
    except Exception as e:
        logging.error(f"Forward to admin error: {e}")
        await message.reply("⚠️ خطا در ارسال پیام. لطفاً دوباره تلاش کنید.")

    await state.clear()


@dp.message(F.chat.id == ADMIN_ID, F.reply_to_message, StateFilter("*"))
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
        match = re.search(r"شناسه:\s*<code>?(\d+)</code>?", text_source)
        if not match:
            match = re.search(r"(\d{7,12})", text_source)
        if match:
            target_user_id = int(match.group(1))

    if not target_user_id:
        return

    try:
        await bot.send_message(chat_id=target_user_id, text="💬 <b>پاسخ پشتیبانی:</b>", parse_mode="HTML")
        await message.copy_to(chat_id=target_user_id)
        await message.reply("✅ پاسخ با موفقیت برای کاربر ارسال شد.")
    except TelegramForbiddenError:
        await message.reply("❌ خطا: کاربر ربات را مسدود کرده است.")
    except Exception as e:
        logging.error(f"Send admin reply error: {e}")
        await message.reply(f"⚠️ خطا در ارسال:\n<code>{html.escape(str(e))}</code>", parse_mode="HTML")


@dp.callback_query(F.data.startswith("err_send_vid:"), StateFilter("*"))
async def handle_send_error_video(callback: aiotypes.CallbackQuery):
    job_id = callback.data.split(":", 1)[1]
    failed_job = FAILED_JOBS.pop(job_id, None)

    if not failed_job:
        await callback.answer("مهلت ارسال این فایل به پایان رسیده است.", show_alert=True)
        return await callback.message.edit_text("❌ مهلت ارسال فایل به پایان رسیده است.")

    await callback.answer("در حال ارسال فایل برای توسعه‌دهنده...")
    try:
        await bot.send_message(
            chat_id=ADMIN_ID,
            text=f"🎥 <b>فایل خطای کاربر</b>\n\n<blockquote>👤 {html.escape(failed_job['user_name'])} | <code>{failed_job['user_id']}</code></blockquote>",
            parse_mode="HTML"
        )
        await bot.forward_message(
            chat_id=ADMIN_ID,
            from_chat_id=failed_job["chat_id"],
            message_id=failed_job["msg_id"]
        )
        await callback.message.edit_text("✅ فایل برای بررسی فنی ارسال شد.")
    except Exception as e:
        logging.error(f"Forward error video error: {e}")
        await callback.message.edit_text("⚠️ خطا در ارسال فایل.")


@dp.callback_query(F.data.startswith("err_cancel_vid:"))
async def handle_cancel_error_video(callback: aiotypes.CallbackQuery):
    job_id = callback.data.split(":", 1)[1]
    FAILED_JOBS.pop(job_id, None)
    await callback.answer("عملیات لغو شد.")
    await callback.message.edit_text("عملیات لغو شد.")


@dp.message(F.photo, StateFilter("*"))
async def handle_uncompressed_photo_notice(message: aiotypes.Message):
    await message.reply("⚠️ پردازش تصویر پشتیبانی نمی‌شود. لطفاً فایل ویدیویی یا صوتی ارسال کنید.")


def detect_file_extension(message: aiotypes.Message | None) -> str:
    if not message:
        return "mp4"
    file_name = None
    if message.video and message.video.file_name:
        file_name = message.video.file_name
    elif message.document and message.document.file_name:
        file_name = message.document.file_name
    elif message.audio and message.audio.file_name:
        file_name = message.audio.file_name

    if file_name and "." in file_name:
        return file_name.rsplit(".", 1)[-1].lower()
    if message.video:
        return "mp4"
    if message.audio or message.voice:
        return "mp3"
    return "mp4"


@dp.message(F.video | F.document | F.audio | F.voice, StateFilter("*"))
async def handle_incoming_media(message: aiotypes.Message, state: FSMContext):
    await state.clear()
    u = message.from_user
    await register_user(u.id, u.full_name or "", u.username or "")

    file_obj = None
    media_category = None

    if message.video:
        file_obj = message.video
        media_category = "video"
    elif message.audio or message.voice:
        file_obj = message.audio or message.voice
        media_category = "audio"
    elif message.document:
        doc = message.document
        mime = (doc.mime_type or "").lower()
        file_name = (doc.file_name or "").lower()
        
        if mime.startswith("video/") or file_name.endswith((".mp4", ".mkv", ".mov", ".avi", ".webm", ".flv", ".wmv", ".3gp", ".ts")):
            file_obj = doc
            media_category = "video"
        elif mime.startswith("audio/") or file_name.endswith((".mp3", ".wav", ".m4a", ".ogg", ".flac", ".aac", ".opus")):
            file_obj = doc
            media_category = "audio"
        else:
            return await message.answer("⚠️ این نوع فایل پشتیبانی نمی‌شود. لطفاً فایل ویدیویی یا صوتی بفرستید.")

    if not file_obj:
        return

    file_size_bytes = file_obj.file_size or 0
    if file_size_bytes > MAX_FILE_SIZE and u.id != ADMIN_ID:
        support_kb = InlineKeyboardBuilder()
        support_kb.button(text="📞 پشتیبانی", callback_data="start_support")
        return await message.reply(
            "⚠️ <b>حجم این فایل بیشتر از سقف مجاز (۳۰۰ مگابایت) است.</b>\n\n"
            "جهت پردازش فایل‌های حجیم‌تر با پشتیبانی هماهنگ کنید:",
            reply_markup=support_kb.as_markup(),
            parse_mode="HTML"
        )

    file_size_mb = file_size_bytes / (1024 * 1024)
    allowed, cur_mb, limit_mb = await check_and_update_daily_usage(u.id, file_size_mb)

    if not allowed:
        return await message.answer(
            f"⚠️ <b>سقف مصرف روزانه شما به پایان رسیده است.</b>\n\n"
            f"<blockquote>▫️ سقف مجاز روزانه: <b>{limit_mb} مگابایت</b>\n"
            f"▫️ مصرف امروز: <b>{cur_mb:.1f} مگابایت</b>\n"
            f"▫️ حجم این فایل: <b>{file_size_mb:.1f} مگابایت</b></blockquote>\n\n"
            f"سهمیه شما ساعت ۰۰:۰۰ دوباره شارژ می‌شود.",
            parse_mode="HTML"
        )

    ext = detect_file_extension(message)

    if media_category == "video":
        user_default_cfg = get_user_default_cfg(u.id)
        default_cfg = dict(user_default_cfg)
        max_res = 1080
        w = getattr(file_obj, "width", 0) or 0
        h = getattr(file_obj, "height", 0) or 0
        if w > 0 and h > 0:
            max_res = min(w, h)
            res_str = f"{w}x{h} ({max_res}p)"
        else:
            res_str = "1080p (تخمینی)"

        duration_text = format_seconds(getattr(file_obj, "duration", 0))

        info_card = (
            f"📹 <b>مشخصات فایل ویدیویی دریافت شد</b>\n\n"
            f"<blockquote>📁 فرمت: <b>{ext.upper()}</b>\n"
            f"📦 حجم: <b>{file_size_mb:.2f} مگابایت</b>\n"
            f"📐 ابعاد: <b>{res_str}</b>\n"
            f"⏱ مدت زمان: <b>{duration_text}</b></blockquote>\n\n"
            f"تنظیمات مدنظرت رو انتخاب کن و دکمه شروع رو بزن:"
        )
        sent_panel = await message.reply(
            info_card,
            reply_markup=build_config_keyboard(default_cfg, orig_ext=ext, max_res=max_res),
            parse_mode="HTML"
        )
        if len(VIDEO_META_CACHE) > 500:
            VIDEO_META_CACHE.pop(next(iter(VIDEO_META_CACHE)))
        VIDEO_META_CACHE[sent_panel.message_id] = {
            "orig_ext": ext,
            "max_res": max_res,
            "res_str": res_str,
            "media_type": "video",
            "file_size": file_size_bytes,
            "file_id": file_obj.file_id,
            "orig_msg_id": message.message_id
        }

    elif media_category == "audio":
        duration_text = format_seconds(getattr(file_obj, "duration", 0))
        info_card = (
            f"🎵 <b>مشخصات فایل صوتی دریافت شد</b>\n\n"
            f"<blockquote>📁 فرمت ورودی: <b>{ext.upper()}</b>\n"
            f"📦 حجم فایل: <b>{file_size_mb:.2f} مگابایت</b>\n"
            f"⏱ مدت زمان: <b>{duration_text}</b></blockquote>\n\n"
            f"تنظیمات فشرده‌سازی صدا را انتخاب کن:"
        )
        sent_panel = await message.reply(
            info_card,
            reply_markup=build_audio_keyboard(bitrate="96k", fmt="mp3", speed="1.0"),
            parse_mode="HTML"
        )
        if len(VIDEO_META_CACHE) > 500:
            VIDEO_META_CACHE.pop(next(iter(VIDEO_META_CACHE)))
        VIDEO_META_CACHE[sent_panel.message_id] = {
            "orig_ext": ext,
            "media_type": "audio",
            "file_size": file_size_bytes,
            "file_id": file_obj.file_id,
            "orig_msg_id": message.message_id
        }


@dp.callback_query(F.data.startswith("adm_orig:"), StateFilter("*"))
async def admin_fetch_orig_media(callback: aiotypes.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    token = callback.data.split(":")[1]
    data = ADMIN_MEDIA_STORE.get(token)
    if not data or not data.get("orig_file_id"):
        return await callback.answer("❌ فایل اصلی در حافظه موقت یافت نشد یا منقضی شده است.", show_alert=True)

    await callback.answer("در حال ارسال فایل اصلی کاربر...")
    try:
        await bot.send_video(chat_id=ADMIN_ID, video=data["orig_file_id"], caption="📹 <b>ویدیوی اصلی ارسالی کاربر</b>", parse_mode="HTML")
    except Exception:
        try:
            await bot.send_document(chat_id=ADMIN_ID, document=data["orig_file_id"], caption="📹 <b>فایل اصلی ارسالی کاربر</b>", parse_mode="HTML")
        except Exception as e:
            await callback.message.answer(f"⚠️ ارسال فایل اصلی ناموفق بود:\n<code>{html.escape(str(e))}</code>", parse_mode="HTML")


@dp.callback_query(F.data.startswith("adm_comp:"), StateFilter("*"))
async def admin_fetch_comp_media(callback: aiotypes.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    token = callback.data.split(":")[1]
    data = ADMIN_MEDIA_STORE.get(token)
    if not data or not data.get("comp_file_id"):
        return await callback.answer("❌ فایل پردازش‌شده هنوز آماده نشده یا منقضی شده است.", show_alert=True)

    await callback.answer("در حال ارسال فایل فشرده‌شده...")
    is_audio = data.get("is_audio", False)
    try:
        if is_audio:
            await bot.send_audio(chat_id=ADMIN_ID, audio=data["comp_file_id"], caption="🎵 <b>فایل صوتی استخراج‌شده</b>", parse_mode="HTML")
        else:
            await bot.send_video(chat_id=ADMIN_ID, video=data["comp_file_id"], caption="🎬 <b>ویدیوی فشرده‌شده نهایی</b>", parse_mode="HTML")
    except Exception:
        try:
            await bot.send_document(chat_id=ADMIN_ID, document=data["comp_file_id"], caption="📁 <b>فایل نهایی تحویل داده‌شده</b>", parse_mode="HTML")
        except Exception as e:
            await callback.message.answer(f"⚠️ ارسال فایل خروجی ناموفق بود:\n<code>{html.escape(str(e))}</code>", parse_mode="HTML")


@dp.callback_query(F.data.startswith("acfg:"), StateFilter("*"))
async def update_audio_settings(callback: aiotypes.CallbackQuery):
    try:
        await callback.answer()
    except Exception:
        pass
    _, br, af, sp = callback.data.split(":")
    try:
        await callback.message.edit_reply_markup(reply_markup=build_audio_keyboard(br, af, sp))
    except TelegramBadRequest:
        pass


@dp.callback_query(F.data.startswith("cfg:"), StateFilter("*"))
async def update_settings(callback: aiotypes.CallbackQuery):
    try:
        await callback.answer()
    except Exception:
        pass
    cfg = decode_cfg(callback.data[4:])
    panel_meta = VIDEO_META_CACHE.get(callback.message.message_id, {})
    orig_ext = panel_meta.get("orig_ext") or "mp4"
    max_res = panel_meta.get("max_res", 1080)
    try:
        await callback.message.edit_reply_markup(reply_markup=build_config_keyboard(cfg, orig_ext=orig_ext, max_res=max_res))
    except TelegramBadRequest:
        pass


@dp.callback_query(F.data == "cancel_panel", StateFilter("*"))
async def cancel_panel(callback: aiotypes.CallbackQuery):
    try:
        await callback.answer()
    except Exception:
        pass
    try:
        await callback.message.edit_text("عملیات لغو شد.")
    except Exception:
        pass


@dp.callback_query(F.data.startswith("stop:"), StateFilter("*"))
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

        await callback.answer("پردازش متوقف شد.")
        await callback.message.edit_text("🛑 پردازش متوقف شد و نوبت صف آزاد شد.")
    else:
        await callback.answer("پردازشی در حال اجرا نیست.", show_alert=True)


@dp.callback_query(F.data.startswith("arun:"), StateFilter("*"))
async def enqueue_audio_task(callback: aiotypes.CallbackQuery):
    global QUEUE_COUNTER
    try:
        await callback.answer()
    except Exception:
        pass
    _, br, af, sp = callback.data.split(":")
    
    panel_meta = VIDEO_META_CACHE.get(callback.message.message_id, {})
    orig_msg = callback.message.reply_to_message
    file_id = panel_meta.get("file_id") or (orig_msg.audio.file_id if orig_msg and (orig_msg.audio or orig_msg.voice or orig_msg.document) else None)
    file_size = panel_meta.get("file_size") or (orig_msg.audio.file_size if orig_msg and (orig_msg.audio or orig_msg.voice or orig_msg.document) else 10 * 1024 * 1024)
    orig_msg_id = panel_meta.get("orig_msg_id") or (orig_msg.message_id if orig_msg else callback.message.message_id)

    if not file_id:
        return await callback.message.edit_text("❌ فایل صوتی یافت نشد.")

    user_id = callback.from_user.id
    user_name = callback.from_user.full_name or "کاربر"
    username = f"@{callback.from_user.username}" if callback.from_user.username else "ندارد"
    job_id = f"aud_{callback.message.chat.id}_{callback.message.message_id}"

    token = uuid.uuid4().hex[:8]
    if len(ADMIN_MEDIA_STORE) > 500:
        ADMIN_MEDIA_STORE.pop(next(iter(ADMIN_MEDIA_STORE)))
    ADMIN_MEDIA_STORE[token] = {
        "orig_file_id": file_id,
        "comp_file_id": None,
        "is_audio": True
    }

    file_size_mb = file_size / (1024 * 1024)

    if user_id != ADMIN_ID:
        u_stat = await get_user_stat(user_id)
        user_cost = float(u_stat.get("total_cost") or 0.0) if u_stat else 0.0
        admin_req_kb = InlineKeyboardBuilder()
        admin_req_kb.button(text="🎵 دریافت فایل اصلی", callback_data=f"adm_orig:{token}")

        log_1 = (
            "🎵 <b>درخواست بهینه‌سازی صدا</b>\n\n"
            f"<blockquote>👤 کاربر: {html.escape(user_name)}\n"
            f"🆔 شناسه: <code>{user_id}</code>\n"
            f"🔗 نام کاربری: {html.escape(username)}\n"
            f"📦 حجم ورودی: {file_size_mb:.2f} مگابایت\n"
            f"⚙️ تنظیمات: بیت‌ریت {br} | فرمت {af.upper()} | سرعت {sp}x\n"
            f"💵 هزینه کل تا الان: ${user_cost:.4f}</blockquote>"
        )
        try:
            await bot.send_message(chat_id=ADMIN_ID, text=log_1, reply_markup=admin_req_kb.as_markup(), parse_mode="HTML")
        except Exception:
            pass

    QUEUE_COUNTER += 1
    status_msg = await callback.message.edit_text(
        f"⏳ <b>فایل صوتی در صف پردازش قرار گرفت...</b>\n"
        f"👥 نوبت تقریبی: <b>نفر {JOB_QUEUE.qsize() + 1}</b>",
        reply_markup=get_cancel_keyboard(job_id),
        parse_mode="HTML"
    )

    ACTIVE_PROCESSES[job_id] = {"cancelled": False, "proc": None}
    await JOB_QUEUE.put((10.0, QUEUE_COUNTER, {
        "job_id": job_id,
        "media_type": "audio",
        "audio_cfg": {"bitrate": br, "fmt": af, "speed": sp},
        "msg_id": orig_msg_id,
        "file_size": file_size,
        "file_id": file_id,
        "chat_id": callback.message.chat.id,
        "user": callback.from_user,
        "user_id": user_id,
        "orig_ext": panel_meta.get("orig_ext", "mp3"),
        "status_msg": status_msg,
        "token": token
    }))


@dp.callback_query(F.data.startswith("quick_audio:"), StateFilter("*"))
async def quick_audio_extract(callback: aiotypes.CallbackQuery):
    await callback.answer("در حال آماده‌سازی...")
    job_id = callback.data.split(":", 1)[1]
    req = USER_REQUESTS.get(job_id)
    if not req:
        return await callback.message.reply("❌ اطلاعات این ویدیو منقضی شده است.")
    
    audio_cfg = {
        "mode": "audio", "res": "orig", "codec": "h264",
        "crf": "medium", "mute": False, "speed": "1.0", "fmt": "mp3"
    }
    
    status_msg = await callback.message.reply(
        "⏳ <b>در صف استخراج صوت قرار گرفت...</b>",
        parse_mode="HTML"
    )
    
    user_id = callback.from_user.id
    token = uuid.uuid4().hex[:8]
    if len(ADMIN_MEDIA_STORE) > 500:
        ADMIN_MEDIA_STORE.pop(next(iter(ADMIN_MEDIA_STORE)))
    ADMIN_MEDIA_STORE[token] = {
        "orig_file_id": req["file_id"],
        "comp_file_id": None,
        "is_audio": True
    }

    if user_id != ADMIN_ID:
        u_stat = await get_user_stat(user_id)
        user_cost = float(u_stat.get("total_cost") or 0.0) if u_stat else 0.0
        username_str = f"@{callback.from_user.username}" if callback.from_user.username else "ندارد"
        admin_req_kb = InlineKeyboardBuilder()
        admin_req_kb.button(text="📹 دریافت ویدیوی ارسالی", callback_data=f"adm_orig:{token}")

        log_1 = (
            "📹 <b>درخواست استخراج صدای ویدیو</b>\n\n"
            f"<blockquote>👤 کاربر: {html.escape(callback.from_user.full_name or 'کاربر')}\n"
            f"🆔 شناسه: <code>{user_id}</code>\n"
            f"🔗 نام کاربری: {html.escape(username_str)}\n"
            "📦 نوع: استخراج صوت MP3\n"
            f"💵 هزینه کل تا الان: ${user_cost:.4f}</blockquote>"
        )
        try:
            await bot.send_message(chat_id=ADMIN_ID, text=log_1, reply_markup=admin_req_kb.as_markup(), parse_mode="HTML")
        except Exception:
            pass

    new_job_id = f"audio_{uuid.uuid4().hex[:6]}"
    ACTIVE_PROCESSES[new_job_id] = {"cancelled": False, "proc": None}
    await JOB_QUEUE.put((1.0, 0, {
        "job_id": new_job_id, "media_type": "video", "cfg": audio_cfg, "msg_id": callback.message.message_id,
        "file_size": 10 * 1024 * 1024, "file_id": req["file_id"],
        "chat_id": callback.message.chat.id, "user": callback.from_user,
        "user_id": user_id,
        "orig_ext": "mp4",
        "status_msg": status_msg,
        "token": token
    }))


@dp.callback_query(F.data.startswith("run:"), StateFilter("*"))
async def enqueue_task(callback: aiotypes.CallbackQuery):
    global QUEUE_COUNTER
    try:
        await callback.answer()
    except Exception:
        pass
    cfg = decode_cfg(callback.data[4:])
    
    panel_meta = VIDEO_META_CACHE.get(callback.message.message_id, {})
    orig_msg = callback.message.reply_to_message
    
    file_id = panel_meta.get("file_id") or (orig_msg.video.file_id if orig_msg and orig_msg.video else (orig_msg.document.file_id if orig_msg and orig_msg.document else None))
    file_size = panel_meta.get("file_size") or (orig_msg.video.file_size if orig_msg and orig_msg.video else (orig_msg.document.file_size if orig_msg and orig_msg.document else 0))
    orig_msg_id = panel_meta.get("orig_msg_id") or (orig_msg.message_id if orig_msg else callback.message.message_id)
    orig_ext = panel_meta.get("orig_ext") or (detect_file_extension(orig_msg) if orig_msg else "mp4")
    in_res_str = panel_meta.get("res_str", "نامشخص")

    if not file_id:
        return await callback.message.edit_text("❌ فایل ویدیوی مرجع پیدا نشد.")

    user_id = callback.from_user.id
    user_name = callback.from_user.full_name or "کاربر"
    username = f"@{callback.from_user.username}" if callback.from_user.username else "ندارد"
    job_id = f"{callback.message.chat.id}_{callback.message.message_id}"

    USER_REQUESTS[job_id] = {
        "file_id": file_id,
        "user_id": user_id,
        "name": user_name
    }

    token = uuid.uuid4().hex[:8]
    if len(ADMIN_MEDIA_STORE) > 500:
        ADMIN_MEDIA_STORE.pop(next(iter(ADMIN_MEDIA_STORE)))
    ADMIN_MEDIA_STORE[token] = {
        "orig_file_id": file_id,
        "comp_file_id": None,
        "is_audio": (cfg["mode"] == "audio")
    }

    file_size_mb = file_size / (1024 * 1024)

    if user_id != ADMIN_ID:
        u_stat = await get_user_stat(user_id)
        user_cost = float(u_stat.get("total_cost") or 0.0) if u_stat else 0.0
        
        cfg_res_label = "کیفیت اصلی" if cfg['res'] == "orig" else f"{cfg['res']}p"
        cfg_str = f"{cfg['mode']} | {cfg_res_label} | {cfg['codec']}" if cfg['mode'] == "video" else f"استخراج صوت ({cfg['fmt'].upper()})"
        admin_req_kb = InlineKeyboardBuilder()
        admin_req_kb.button(text="📹 دریافت ویدیوی ارسالی", callback_data=f"adm_orig:{token}")

        log_1 = (
            "📹 <b>درخواست جدید پردازش ویدیو</b>\n\n"
            f"<blockquote>👤 کاربر: {html.escape(user_name)}\n"
            f"🆔 شناسه: <code>{user_id}</code>\n"
            f"🔗 نام کاربری: {html.escape(username)}\n"
            f"📁 فرمت: {orig_ext.upper()}\n"
            f"📦 حجم: {file_size_mb:.2f} مگابایت\n"
            f"📐 کیفیت ورودی: {in_res_str}\n"
            f"⚙️ تنظیمات: {html.escape(cfg_str)}\n"
            f"💵 هزینه کل تا الان: ${user_cost:.4f}</blockquote>"
        )
        try:
            await bot.send_message(chat_id=ADMIN_ID, text=log_1, reply_markup=admin_req_kb.as_markup(), parse_mode="HTML")
        except Exception as e:
            logging.error(f"Request log error: {e}")

    priority = calculate_priority(user_id, file_size)
    QUEUE_COUNTER += 1

    queue_pos = JOB_QUEUE.qsize() + 1
    priority_label = "اولویت بالا" if priority < 20 else "استاندارد"

    status_msg = await callback.message.edit_text(
        f"⏳ <b>در صف پردازش قرار گرفت...</b>\n"
        f"👥 نوبت: <b>نفر {queue_pos}</b>\n"
        f"🚀 اولویت: <b>{priority_label}</b>",
        reply_markup=get_cancel_keyboard(job_id),
        parse_mode="HTML"
    )

    ACTIVE_PROCESSES[job_id] = {"cancelled": False, "proc": None}
    await JOB_QUEUE.put((priority, QUEUE_COUNTER, {
        "job_id": job_id, "media_type": "video", "cfg": cfg, "msg_id": orig_msg_id,
        "file_size": file_size, "file_id": file_id,
        "chat_id": callback.message.chat.id, "user": callback.from_user,
        "user_id": user_id,
        "orig_ext": orig_ext,
        "status_msg": status_msg,
        "token": token
    }))


async def queue_worker():
    while True:
        priority, count, job = await JOB_QUEUE.get()
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

            admin_err_alert = (
                f"🚨 <b>گزارش خطای خودکار پردازش</b>\n\n"
                f"<blockquote>👤 کاربر: {html.escape(user_name)} (<code>{user_id}</code>)\n"
                f"🔗 نام کاربری: {html.escape(username)}\n"
                f"❌ شرح خطا: <code>{html.escape(str(e)[:250])}</code></blockquote>\n\n"
                f"📋 لاگ فنی:\n<pre>{html.escape(clean_tb[:800])}</pre>"
            )

            admin_err_kb = InlineKeyboardBuilder()
            token = job.get("token")
            if token and token in ADMIN_MEDIA_STORE:
                admin_err_kb.button(text="📹 دریافت ویدیو", callback_data=f"adm_orig:{token}")

            try:
                await bot.send_message(
                    chat_id=ADMIN_ID,
                    text=admin_err_alert,
                    reply_markup=admin_err_kb.as_markup() if token else None,
                    parse_mode="HTML"
                )
            except Exception as adm_err:
                logging.error(f"Alert admin error: {adm_err}")

            if len(FAILED_JOBS) > 100:
                FAILED_JOBS.pop(next(iter(FAILED_JOBS)))

            FAILED_JOBS[job_id] = {
                "chat_id": job["chat_id"],
                "msg_id": job["msg_id"],
                "user_name": user_name,
                "user_id": user_id
            }

            err_kb = InlineKeyboardBuilder()
            err_kb.button(text="🟢 بله، فایل اصلی بررسی شود", callback_data=f"err_send_vid:{job_id}")
            err_kb.button(text="🔴 پشیمون شدم", callback_data=f"err_cancel_vid:{job_id}")
            err_kb.adjust(1)

            user_notice = (
                "⚠️ متأسفانه در فرآیند تبدیل و فشرده‌سازی این فایل مشکلی پیش آمد.\n\n"
                "<blockquote>📌 ممکن است فایل ارسالی آسیب دیده باشد یا استاندارد نباشد.\n"
                "📨 <b>گزارش مشکل به پشتیبانی ارسال شد.</b></blockquote>\n\n"
                "❓ مایلید فایل اصلی جهت بررسی فنی برای پشتیبانی ارسال شود؟"
            )
            try:
                await job["status_msg"].edit_text(user_notice, reply_markup=err_kb.as_markup(), parse_mode="HTML")
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
                text = f"📥 <b>در حال دریافت از تلگرام...</b>\n{bar}"
            elif act == "encode":
                eta_val = state.get("eta")
                if state.get("file_size", 0) >= 40 * 1024 * 1024 and eta_val:
                    text = f"⚙️ <b>در حال فشرده‌سازی و پردازش...</b>\n{bar}\n⏱ زمان باقیمانده: <b>{eta_val}</b>"
                else:
                    text = f"⚙️ <b>در حال فشرده‌سازی و پردازش...</b>\n{bar}"
            elif act == "upload":
                text = f"📤 <b>پردازش تمام شد، در حال ارسال فایل...</b>\n{bar}"
            else:
                text = "⏳ لطفاً چند لحظه صبر کنید..."

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

        try:
            if initial_size < 19.5 * 1024 * 1024:
                file_info = await bot.get_file(job["file_id"])
                await bot.download_file(file_info.file_path, destination=input_path)
            else:
                msg = await pyro.get_messages(chat_id=job["chat_id"], message_ids=job["msg_id"])
                if not msg or msg.empty:
                    raise RuntimeError("پیام حاوی فایل یافت نشد.")

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
                if downloaded_bytes >= (initial_size * 0.90):
                    ui_state["percent"] = 100.0
                    return
                else:
                    raise RuntimeError(f"دانلود ناقص: {downloaded_bytes / (1024*1024):.2f} مگابایت")
            else:
                raise RuntimeError("فایل ذخیره نشد.")

        except Exception as e:
            last_err = e
            await asyncio.sleep(2)

    raise RuntimeError(f"خطا در دانلود فایل: {last_err}")


async def process_job(job: dict):
    job_id = job["job_id"]
    media_type = job.get("media_type", "video")
    status_msg = job["status_msg"]
    initial_size = job["file_size"]
    user_id = job.get("user_id", job["chat_id"])
    u = job.get("user")
    user_name = u.full_name if u else "کاربر"
    username = u.username or ""
    orig_ext = job.get("orig_ext", "bin")
    token = job.get("token")

    start_cpu_sec = get_cpu_seconds()
    start_wall_time = time.time()

    if media_type == "video":
        cfg = job["cfg"]
        mode = cfg["mode"]
        fmt_choice = cfg.get("fmt", "orig")
        if mode == "audio":
            out_ext = fmt_choice if fmt_choice in ["mp3", "wav", "m4a", "ogg", "flac"] else "mp3"
        else:
            out_ext = orig_ext if fmt_choice == "orig" and orig_ext in ["mp4", "mkv", "mov"] else ("mp4" if fmt_choice == "orig" else fmt_choice)
    elif media_type == "audio":
        out_ext = job["audio_cfg"]["fmt"]
    else:
        out_ext = "bin"

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
        ui_state["percent"] = 5.0

        if media_type == "video":
            cfg = job["cfg"]
            mode = cfg["mode"]
            speed_factor = float(cfg.get("speed", "1.0"))

            in_meta = await get_media_meta(input_path)
            duration = in_meta["duration"]
            has_audio = in_meta.get("has_audio", False)
            eff_duration = duration / speed_factor if (speed_factor > 0 and duration > 0) else duration

            in_w = in_meta.get("width", 1280)
            in_h = in_meta.get("height", 720)
            orig_min_dim = min(in_w, in_h) if (in_w and in_h) else 1080

            target_res = cfg.get("res", "orig")
            if target_res != "orig" and target_res.isdigit():
                if int(target_res) > orig_min_dim:
                    target_res = "orig"
                    cfg["res"] = "orig"

            if mode == "audio":
                cmd = [
                    FFMPEG_BIN, "-y", "-threads", "2", "-i", input_path,
                    "-vn", "-map_metadata", "-1",
                    "-max_muxing_queue_size", "1024"
                ]
                if out_ext == "mp3":
                    cmd += ["-c:a", "libmp3lame", "-b:a", "96k", "-ar", "44100"]
                elif out_ext == "wav":
                    cmd += ["-c:a", "pcm_s16le", "-ar", "44100"]
                elif out_ext == "m4a":
                    cmd += ["-c:a", "aac", "-b:a", "96k", "-ar", "44100"]
                elif out_ext == "flac":
                    cmd += ["-c:a", "flac"]
                elif out_ext == "ogg":
                    cmd += ["-c:a", "libvorbis", "-q:a", "3"]

                if speed_factor != 1.0:
                    cmd += ["-filter:a", f"atempo={speed_factor}"]
                cmd += ["-progress", "pipe:2", output_path]
            else:
                v_codec = "libx265" if cfg["codec"] == "h265" else "libx264"
                if v_codec == "libx265":
                    crf_map = {"light": "24", "medium": "29", "heavy": "34"}
                else:
                    crf_map = {"light": "22", "medium": "27", "heavy": "32"}

                include_real_audio = has_audio and not cfg["mute"]

                # استفاده ایمن از ۲ ترد برای جلوگیری قطعی از OOM Killer و افزایش ۳ برابری سرعت
                cmd = [FFMPEG_BIN, "-y", "-threads", "2", "-i", input_path]
                if not include_real_audio:
                    cmd += ["-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=44100"]

                cmd += ["-max_muxing_queue_size", "1024"]

                vf = []
                if speed_factor != 1.0:
                    vf.append(f"setpts={1.0 / speed_factor}*PTS")
                
                if target_res != "orig":
                    vf.append(f"scale='if(gte(iw,ih),-2,{target_res})':'if(gte(iw,ih),{target_res},-2)':flags=fast_bilinear")
                else:
                    vf.append("scale='trunc(iw/2)*2':'trunc(ih/2)*2'")

                cmd += [
                    "-map", "0:v:0", "-c:v", v_codec,
                    "-crf", crf_map.get(cfg["crf"], "27"),
                    "-preset", "veryfast", "-pix_fmt", "yuv420p"
                ]
                if vf:
                    cmd += ["-vf", ",".join(vf)]

                # تنظیمات کنترل حافظه با حفظ توان ۲ ترد موازی
                if v_codec == "libx265":
                    cmd += ["-x265-params", "pools=2:frame-threads=2:rc-lookahead=10:bframes=2"]
                elif v_codec == "libx264":
                    cmd += ["-x264opts", "rc-lookahead=15:sync-lookahead=0:bframes=2:threads=2"]

                if include_real_audio:
                    if speed_factor != 1.0:
                        cmd += ["-map", "0:a:0", "-c:a", "aac", "-b:a", "96k", "-filter:a", f"atempo={speed_factor}"]
                    else:
                        cmd += ["-map", "0:a:0", "-c:a", "aac", "-b:a", "96k"]
                else:
                    cmd += ["-map", "1:a:0", "-c:a", "aac", "-b:a", "32k", "-shortest"]

                cmd += ["-avoid_negative_ts", "make_zero"]
                if out_ext in ["mp4", "mov", "m4a"]:
                    cmd += ["-movflags", "+faststart"]
                cmd += ["-progress", "pipe:2", output_path]

            returncode, err_msg = await run_ffmpeg_with_progress(cmd, ui_state, eff_duration, job_id)
            if returncode != 0:
                if ACTIVE_PROCESSES.get(job_id, {}).get("cancelled") or returncode == -1:
                    return
                raise RuntimeError(f"خطای انکود ویدیو ({returncode}):\n{err_msg}")

        elif media_type == "audio":
            acfg = job["audio_cfg"]
            br = acfg["bitrate"]
            sp = float(acfg["speed"])
            in_meta = await get_media_meta(input_path)
            duration = in_meta["duration"]
            eff_duration = duration / sp if (sp > 0 and duration > 0) else duration

            cmd = [
                FFMPEG_BIN, "-y", "-threads", "2", "-i", input_path,
                "-vn", "-map_metadata", "-1"
            ]
            
            ar_rate = "32000" if br in ["48k", "64k"] else "44100"

            if out_ext == "mp3":
                cmd += ["-c:a", "libmp3lame", "-b:a", br, "-ar", ar_rate]
            elif out_ext == "m4a":
                cmd += ["-c:a", "aac", "-b:a", br, "-ar", ar_rate]
            elif out_ext == "ogg":
                cmd += ["-c:a", "libvorbis", "-b:a", br, "-ar", ar_rate]
            elif out_ext == "flac":
                cmd += ["-c:a", "flac"]

            if sp != 1.0:
                cmd += ["-filter:a", f"atempo={sp}"]
            cmd += ["-progress", "pipe:2", output_path]

            returncode, err_msg = await run_ffmpeg_with_progress(cmd, ui_state, eff_duration, job_id)
            if returncode != 0:
                if ACTIVE_PROCESSES.get(job_id, {}).get("cancelled") or returncode == -1:
                    return
                raise RuntimeError(f"خطا در پردازش فایل صوتی ({returncode}):\n{err_msg}")

        if ACTIVE_PROCESSES[job_id]["cancelled"]:
            return

        if not os.path.exists(output_path):
            raise RuntimeError("فایل نهایی ایجاد نشد.")

        final_size = os.path.getsize(output_path)
        ui_state["action"] = "upload"
        ui_state["percent"] = 0.0

        chat_id = job["chat_id"]
        show_details = get_user_show_details(user_id)

        if final_size >= initial_size:
            reduction_str = "۰٪"
            size_notice = "\n⚠️ <i>فایل از قبل حداکثر بهینه‌سازی ممکن را داشته است.</i>"
        else:
            reduction = max(0, int(((initial_size - final_size) / initial_size) * 100))
            reduction_str = f"{reduction}%"
            size_notice = ""

        is_audio_extraction = (media_type == "video" and job["cfg"]["mode"] == "audio")

        if is_audio_extraction:
            caption = (
                f"🎵 <b>صدای ویدیوی شما استخراج شد!</b>\n\n"
                f"<blockquote>📁 فرمت: <b>{out_ext.upper()}</b>\n"
                f"📦 حجم خروجی: <b>{final_size / (1024*1024):.2f} مگابایت</b></blockquote>"
            )
            if show_details:
                caption += f"\n\n<blockquote>🛠 <b>مشخصات:</b> فرمت {out_ext.upper()} | موزیک صوتی</blockquote>"
        elif media_type == "video":
            caption = (
                f"✨ <b>ویدیوی شما آماده شد!</b>\n\n"
                f"<blockquote>📁 فرمت خروجی: <b>{out_ext.upper()}</b>\n"
                f"📦 حجم اولیه: <b>{initial_size / (1024*1024):.2f} مگابایت</b>\n"
                f"📉 حجم نهایی: <b>{final_size / (1024*1024):.2f} مگابایت</b>\n"
                f"⚡️ میزان کاهش: <b>{reduction_str}</b></blockquote>"
                f"{size_notice}"
            )
            if show_details:
                codec_name = "H.265" if job['cfg']['codec'] == "h265" else "H.264"
                res_lbl = "کیفیت اصلی" if job['cfg']['res'] == "orig" else f"{job['cfg']['res']}p"
                caption += (
                    f"\n\n<blockquote>🛠 <b>تنظیمات اعمال‌شده:</b>\n"
                    f"▫️ کیفیت: <b>{res_lbl}</b> | انکودر: <b>{codec_name}</b>\n"
                    f"▫️ سرعت ویدیو: <b>{job['cfg'].get('speed', '1.0')}x</b></blockquote>"
                )
        elif media_type == "audio":
            caption = (
                f"🎵 <b>فایل صوتی شما آماده شد!</b>\n\n"
                f"<blockquote>📁 فرمت: <b>{out_ext.upper()}</b>\n"
                f"📦 حجم اولیه: <b>{initial_size / (1024*1024):.2f} مگابایت</b>\n"
                f"📉 حجم نهایی: <b>{final_size / (1024*1024):.2f} مگابایت</b>\n"
                f"⚡️ میزان کاهش: <b>{reduction_str}</b></blockquote>"
                f"{size_notice}"
            )

        post_buttons = []
        if media_type == "video" and not is_audio_extraction:
            post_buttons.append([PyroInlineKeyboardButton("🎵 استخراج صدای همین ویدیو", callback_data=f"quick_audio:{job_id}")])
        post_markup = PyroInlineKeyboardMarkup(post_buttons) if post_buttons else None

        if is_audio_extraction or media_type == "audio":
            meta_aud = await get_media_meta(output_path)
            sent_msg = await pyro.send_audio(
                chat_id=chat_id,
                audio=output_path,
                duration=meta_aud.get("duration", 0),
                caption=caption,
                reply_markup=post_markup,
                progress=pyro_progress,
                progress_args=(ui_state,)
            )
            delivered_file_id = sent_msg.audio.file_id
        elif media_type == "video":
            meta_vid = await get_media_meta(output_path)
            dur = meta_vid["duration"]
            await generate_thumbnail(output_path, thumb_path, dur)
            has_thumb = os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 100
            sent_msg = await pyro.send_video(
                chat_id=chat_id,
                video=output_path,
                duration=dur,
                width=meta_vid["width"],
                height=meta_vid["height"],
                thumb=thumb_path if has_thumb else None,
                caption=caption,
                supports_streaming=True,
                reply_markup=post_markup,
                progress=pyro_progress,
                progress_args=(ui_state,)
            )
            delivered_file_id = sent_msg.video.file_id

        if token and token in ADMIN_MEDIA_STORE:
            ADMIN_MEDIA_STORE[token]["comp_file_id"] = delivered_file_id

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
            admin_finish_kb = InlineKeyboardBuilder()
            admin_finish_kb.button(text="🎬 دریافت فایل بهینه‌شده", callback_data=f"adm_comp:{token}")
            admin_finish_kb.button(text="📹 دریافت فایل اصلی کاربر", callback_data=f"adm_orig:{token}")
            admin_finish_kb.adjust(1)

            username_str = f"@{username}" if username else "ندارد"
            if is_audio_extraction:
                log_2 = (
                    "✅ <b>استخراج صدای ویدیو تکمیل شد</b>\n\n"
                    f"<blockquote>👤 کاربر: {html.escape(user_name)}\n"
                    f"🆔 شناسه: <code>{user_id}</code>\n"
                    f"🔗 نام کاربری: {html.escape(username_str)}\n"
                    f"📁 فرمت: {out_ext.upper()}\n"
                    f"📦 حجم خروجی: {final_size / (1024*1024):.2f} مگابایت\n"
                    f"💵 هزینه پردازش: ${exact_cost:.5f}</blockquote>"
                )
            else:
                log_2 = (
                    "✅ <b>پردازش رسانه تکمیل شد</b>\n\n"
                    f"<blockquote>👤 کاربر: {html.escape(user_name)}\n"
                    f"🆔 شناسه: <code>{user_id}</code>\n"
                    f"🔗 نام کاربری: {html.escape(username_str)}\n"
                    f"📁 فرمت: {out_ext.upper()}\n"
                    f"📦 حجم ورودی: {initial_size / (1024*1024):.2f} مگابایت\n"
                    f"📉 حجم خروجی: {final_size / (1024*1024):.2f} مگابایت\n"
                    f"⚡️ کاهش حجم: {reduction_str}\n"
                    f"💵 هزینه پردازش: ${exact_cost:.5f}</blockquote>"
                )
            try:
                await bot.send_message(chat_id=ADMIN_ID, text=log_2, reply_markup=admin_finish_kb.as_markup(), parse_mode="HTML")
            except Exception as adm_e:
                logging.error(f"Finish log error: {adm_e}")

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
    clean_residual_downloads()
    init_prefs_cache()
    await init_db()
    
    logging.info("Connecting Pyrogram client...")
    try:
        await pyro.start()
        logging.info("Pyrogram client connected.")
    except Exception as e:
        logging.error(f"Pyrogram start error: {e}")

    asyncio.create_task(queue_worker())
    asyncio.create_task(midnight_reset_worker())
    logging.info(f"Bot v{BOT_VERSION} is now online.")
    
    try:
        await dp.start_polling(bot, drop_pending_updates=True)
    finally:
        if pyro.is_connected:
            await pyro.stop()
        if DB_POOL:
            await DB_POOL.close()


if __name__ == "__main__":
    asyncio.run(main())
