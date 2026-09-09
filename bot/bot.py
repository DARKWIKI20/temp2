import os
import gc
import re
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

from aiogram import Bot, Dispatcher, F, types as aiotypes
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter, TelegramForbiddenError
from aiogram.types import FSInputFile
from pyrogram import Client as PyroClient, raw

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BOT_TOKEN = os.getenv("BOT_TOKEN", "8812733722:AAEFW8oxPPQYyqrqHGtnvS8fTpu3ATxcDbo")
ADMIN_ID = int(os.getenv("ADMIN_ID", "6616272875"))
API_ID = int(os.getenv("API_ID", "26202905"))
API_HASH = os.getenv("API_HASH", "ec9fd909b90288d01befa4f87c8d71c1")

FFMPEG_BIN = "ffmpeg"
MAX_FILE_SIZE = 2000 * 1024 * 1024
PREFS_FILE = "user_prefs.json"
USERS_FILE = "users.json"
STATS_FILE = "user_stats.json"

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


class SupportState(StatesGroup):
    waiting_for_message = State()


class AdminMessageState(StatesGroup):
    waiting_for_user_id = State()
    waiting_for_single_content = State()
    waiting_for_broadcast_content = State()
    waiting_for_custom_limit = State()


# دریافت زمان مصرف واقعی CPU از کرنل لینوکس
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


# مدیریت آمار و محدودیت‌ها
def load_stats() -> dict:
    if os.path.exists(STATS_FILE):
        try:
            with open(STATS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"daily_limit_mb": 500, "users": {}}
    return {"daily_limit_mb": 500, "users": {}}


def save_stats(stats: dict):
    try:
        with open(STATS_FILE, "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.warning(f"Failed to save stats: {e}")


def get_daily_limit_mb() -> int:
    stats = load_stats()
    return stats.get("daily_limit_mb", 500)


def set_daily_limit_mb(limit_mb: int):
    stats = load_stats()
    stats["daily_limit_mb"] = limit_mb
    save_stats(stats)


def check_and_update_daily_usage(user_id: int, file_size_mb: float) -> tuple[bool, float, int]:
    if user_id == ADMIN_ID:
        return True, 0.0, 0

    stats = load_stats()
    limit = stats.get("daily_limit_mb", 500)
    if limit == 0:  # 0 یعنی بدون محدودیت
        return True, 0.0, 0

    today_str = datetime.date.today().isoformat()
    u_key = str(user_id)
    u_data = stats.get("users", {}).get(u_key, {})

    saved_date = u_data.get("today_date", "")
    current_mb = u_data.get("today_mb", 0.0) if saved_date == today_str else 0.0

    if (current_mb + file_size_mb) > limit:
        return False, current_mb, limit

    return True, current_mb, limit


def record_job_stats(user_id: int, name: str, username: str, cost: float, file_size_mb: float, file_id: str):
    stats = load_stats()
    if "users" not in stats:
        stats["users"] = {}

    today_str = datetime.date.today().isoformat()
    u_key = str(user_id)

    if u_key not in stats["users"]:
        stats["users"][u_key] = {
            "name": name,
            "username": username,
            "total_cost": 0.0,
            "total_jobs": 0,
            "today_date": today_str,
            "today_mb": 0.0,
            "max_video": None
        }

    u = stats["users"][u_key]
    u["name"] = name
    u["username"] = username
    u["total_cost"] = round(u.get("total_cost", 0.0) + cost, 6)
    u["total_jobs"] = u.get("total_jobs", 0) + 1

    if u.get("today_date") == today_str:
        u["today_mb"] = round(u.get("today_mb", 0.0) + file_size_mb, 2)
    else:
        u["today_date"] = today_str
        u["today_mb"] = round(file_size_mb, 2)

    cur_max = u.get("max_video")
    if cur_max is None or cost > cur_max.get("cost", 0.0):
        u["max_video"] = {
            "file_id": file_id,
            "cost": round(cost, 6),
            "size_mb": round(file_size_mb, 2),
            "date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        }

    save_stats(stats)


# مدیریت لیست کاربران
def load_users() -> list:
    if os.path.exists(USERS_FILE):
        try:
            with open(USERS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    return []


def register_user(user_id: int):
    users = load_users()
    if user_id not in users:
        users.append(user_id)
        try:
            with open(USERS_FILE, "w", encoding="utf-8") as f:
                json.dump(users, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.warning(f"Failed to register user: {e}")


# مدیریت تنظیمات نمایش
def load_prefs() -> dict:
    if os.path.exists(PREFS_FILE):
        try:
            with open(PREFS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_prefs(prefs: dict):
    try:
        with open(PREFS_FILE, "w", encoding="utf-8") as f:
            json.dump(prefs, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.warning(f"Prefs save error: {e}")


def get_user_show_details(user_id: int) -> bool:
    prefs = load_prefs()
    return prefs.get(str(user_id), {}).get("show_details", True)


def set_user_show_details(user_id: int, show_details: bool):
    prefs = load_prefs()
    u_key = str(user_id)
    if u_key not in prefs:
        prefs[u_key] = {}
    prefs[u_key]["show_details"] = show_details
    save_prefs(prefs)


# متد آپلود بدون فریز
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
    builder.button(text="⚙️ تنظیمات نمایش")
    builder.button(text="📞 پیام به پشتیبانی")
    if user_id == ADMIN_ID:
        builder.button(text="👑 پنل مدیریت")
        builder.adjust(2, 1)
    else:
        builder.adjust(2)
    return builder.as_markup(resize_keyboard=True)


def get_settings_inline_keyboard(user_id: int):
    show_details = get_user_show_details(user_id)
    builder = InlineKeyboardBuilder()
    toggle_text = "حالت فعلی: خلاصه کامل جزئیات ✅" if show_details else "حالت فعلی: فقط حجم فایل‌ها 📉"
    action_text = "تغییر به: گزارش ساده" if show_details else "تغییر به: گزارش کامل همراه با مشخصات"
    builder.button(text=toggle_text, callback_data="none")
    builder.button(text=f"🔄 {action_text}", callback_data="toggle_details")
    builder.button(text="بستن منو ❌", callback_data="close_settings")
    builder.adjust(1)
    return builder.as_markup()


def get_admin_panel_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🏆 پرمصرف‌ترین کاربران (هزینه)", callback_data="admin_top_users")
    builder.button(text="⏱ تنظیم محدودیت روزانه (Daily Limit)", callback_data="admin_set_limit")
    builder.button(text="📢 ارسال پیام به همه کاربران", callback_data="admin_broadcast")
    builder.button(text="👤 ارسال پیام به کاربر خاص", callback_data="admin_send_single")
    builder.button(text="❌ بستن منو", callback_data="admin_close")
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

    b.button(text="🎬 ویدیو" + (" ✅" if mode == "video" else ""), callback_data="cfg:" + encode_cfg("video", res, codec, crf, mute, speed, "orig" if fmt not in ["mp4", "mkv", "mov"] else fmt))
    b.button(text="🎵 استخراج صدا" + (" ✅" if mode == "audio" else ""), callback_data="cfg:" + encode_cfg("audio", res, codec, crf, mute, speed, "mp3" if fmt in ["orig", "mp4", "mkv", "mov"] else fmt))

    if mode == "video":
        b.button(text=f"فرمت: اصلی ({orig_ext.upper()})" + (" ✅" if fmt == "orig" else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, crf, mute, speed, "orig"))
        b.button(text="MP4" + (" ✅" if fmt == "mp4" else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, crf, mute, speed, "mp4"))
        b.button(text="MKV" + (" ✅" if fmt == "mkv" else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, crf, mute, speed, "mkv"))
        b.button(text="MOV" + (" ✅" if fmt == "mov" else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, crf, mute, speed, "mov"))

        for r_k, r_t in [("orig", "وضوح اصلی"), ("1080", "1080p"), ("720", "720p"), ("480", "480p")]:
            b.button(text=r_t + (" ✅" if res == r_k else ""), callback_data="cfg:" + encode_cfg(mode, r_k, codec, crf, mute, speed, fmt))

        b.button(text="H.264" + (" ✅" if codec == "h264" else ""), callback_data="cfg:" + encode_cfg(mode, res, "h264", crf, mute, speed, fmt))
        b.button(text="H.265 (کم‌حجم)" + (" ✅" if codec == "h265" else ""), callback_data="cfg:" + encode_cfg(mode, res, "h265", crf, mute, speed, fmt))

        for c_k, c_t in [("light", "کاهش کم"), ("medium", "متعادل"), ("heavy", "کاهش زیاد")]:
            b.button(text=c_t + (" ✅" if crf == c_k else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, c_k, mute, speed, fmt))

        b.button(text="🔇 صدا: قطع" if mute else "🔊 صدا: وصل", callback_data="cfg:" + encode_cfg(mode, res, codec, crf, not mute, speed, fmt))
    else:
        for af_k, af_t in [("mp3", "MP3"), ("wav", "WAV (کیفیت بالا)"), ("m4a", "M4A"), ("ogg", "OGG"), ("flac", "FLAC")]:
            b.button(text=af_t + (" ✅" if fmt == af_k else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, crf, mute, speed, af_k))

    for s_k, s_t in [("1.0", "1x"), ("1.5", "1.5x"), ("2.0", "2x")]:
        b.button(text=s_t + (" ✅" if speed == s_k else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, crf, mute, s_k, fmt))

    b.button(text="🚀 شروع پردازش", callback_data=f"run:{encode_cfg(mode, res, codec, crf, mute, speed, fmt)}")
    b.button(text="❌ انصراف", callback_data="cancel_panel")

    if mode == "video":
        b.adjust(2, 4, 4, 2, 3, 1, 3, 2)
    else:
        b.adjust(2, 3, 2, 3, 2)
    return b.as_markup()


def get_cancel_keyboard(job_id: str):
    b = InlineKeyboardBuilder()
    b.button(text="❌ لغو فوری پردازش", callback_data=f"stop:{job_id}")
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
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=5.0)
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
        await asyncio.wait_for(proc.communicate(), timeout=4.0)
    except Exception:
        pass


@dp.message(CommandStart())
async def start_handler(message: aiotypes.Message, state: FSMContext):
    await state.clear()
    register_user(message.from_user.id)

    builder = InlineKeyboardBuilder()
    builder.button(text="⚙️ تنظیمات نمایش", callback_data="open_settings")
    builder.button(text="📞 پیام به پشتیبانی", callback_data="start_support")
    builder.adjust(2)

    await message.answer(
        "👋 سلام! به ربات پردازش و فشرده‌سازی ویدیو خوش آمدید.\n\n"
        "🎬 ویدیوی خود را ارسال کنید (پشتیبانی تا سقف ۲ گیگابایت).\n"
        "از گزینه‌های زیر برای تنظیمات و ارتباط با پشتیبانی استفاده کنید:",
        reply_markup=get_main_reply_keyboard(message.from_user.id)
    )
    await message.answer("📌 دسترسی سریع:", reply_markup=builder.as_markup())


# --- پنل مدیریت ادمین ---
@dp.message(Command("admin"))
@dp.message(F.text == "👑 پنل مدیریت")
async def admin_panel_handler(message: aiotypes.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.clear()
    users = load_users()
    limit = get_daily_limit_mb()
    limit_str = f"{limit} مگابایت" if limit > 0 else "نامحدود"
    await message.answer(
        f"👑 **پنل مدیریت ربات**\n\n"
        f"👥 تعداد کل کاربران ثبت‌شده: **{len(users)} نفر**\n"
        f"⏱ محدودیت روزانه فعلی: **{limit_str}**\n\n"
        f"عملیات مورد نظر را انتخاب کنید:",
        reply_markup=get_admin_panel_keyboard(),
        parse_mode="Markdown"
    )


@dp.callback_query(F.data == "admin_close")
async def close_admin_panel(callback: aiotypes.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await callback.answer()
    await callback.message.delete()


# ۱. رتبه‌بندی پرمصرف‌ترین کاربران
@dp.callback_query(F.data == "admin_top_users")
async def show_top_users(callback: aiotypes.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()

    stats = load_stats()
    users_dict = stats.get("users", {})

    if not users_dict:
        return await callback.message.answer("📊 هنوز آماری از مصرف کاربران ثبت نشده است.")

    # مرتب‌سازی بر اساس هزینه کل به صورت نزولی
    sorted_users = sorted(users_dict.items(), key=lambda item: item[1].get("total_cost", 0.0), reverse=True)

    builder = InlineKeyboardBuilder()
    text_lines = ["🏆 **رتبه‌بندی پرمصرف‌ترین کاربران (بر اساس هزینه واقعی):**\n"]

    for idx, (uid, data) in enumerate(sorted_users[:10], start=1):
        name = data.get("name", "نامشخص")
        cost = data.get("total_cost", 0.0)
        jobs = data.get("total_jobs", 0)
        text_lines.append(f"**{idx}.** {name} | هزینه: **${cost:.4f}** ({jobs} ویدیو)")
        builder.button(text=f"{idx}. {name[:12]} (${cost:.4f})", callback_data=f"adm_u_stat:{uid}")

    builder.button(text="🔙 بازگشت به پنل", callback_data="admin_back_main")
    builder.adjust(1)

    await callback.message.answer("\n".join(text_lines), reply_markup=builder.as_markup(), parse_mode="Markdown")


@dp.callback_query(F.data.startswith("adm_u_stat:"))
async def show_single_user_stat(callback: aiotypes.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    target_uid = callback.data.split(":")[1]

    stats = load_stats()
    data = stats.get("users", {}).get(target_uid)

    if not data:
        return await callback.message.answer("اطلاعات این کاربر یافت نشد.")

    name = data.get("name", "نامشخص")
    uname = data.get("username", "ندارد")
    cost = data.get("total_cost", 0.0)
    jobs = data.get("total_jobs", 0)
    today_mb = data.get("today_mb", 0.0)
    max_vid = data.get("max_video")

    text = (
        f"👤 **جزئیات مصرف کاربر:**\n\n"
        f"▫️ **نام:** {name}\n"
        f"▫️ **یوزرنیم:** {uname}\n"
        f"▫️ **آیدی عددی:** `{target_uid}`\n"
        f"▫️ **هزینه واقعی کل:** **${cost:.5f}**\n"
        f"▫️ **تعداد کل تبدیل‌ها:** {jobs} عدد\n"
        f"▫️ **مصرف امروز:** {today_mb} مگابایت\n"
    )

    builder = InlineKeyboardBuilder()
    if max_vid and max_vid.get("file_id"):
        text += (
            f"\n🔥 **اطلاعات پرمصرف‌ترین ویدیو:**\n"
            f"▫️ هزینه این ویدیو: **${max_vid.get('cost', 0):.5f}**\n"
            f"▫️ حجم اولیه: {max_vid.get('size_mb', 0)} MB\n"
            f"▫️ تاریخ: {max_vid.get('date', 'نامشخص')}"
        )
        builder.button(text="🎬 مشاهده و دریافت این ویدیو", callback_data=f"adm_get_vid:{target_uid}")

    builder.button(text="🔙 بازگشت به لیست پرمصرف‌ها", callback_data="admin_top_users")
    builder.adjust(1)

    await callback.message.answer(text, reply_markup=builder.as_markup(), parse_mode="Markdown")


@dp.callback_query(F.data.startswith("adm_get_vid:"))
async def send_max_consuming_video(callback: aiotypes.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer("در حال ارسال ویدیوی پرمصرف...")
    target_uid = callback.data.split(":")[1]

    stats = load_stats()
    max_vid = stats.get("users", {}).get(target_uid, {}).get("max_video")

    if not max_vid or not max_vid.get("file_id"):
        return await callback.message.answer("❌ ویدیویی برای این کاربر یافت نشد.")

    cap = (
        f"🎬 **پرمصرف‌ترین ویدیوی کاربر `{target_uid}`:**\n\n"
        f"💵 هزینه واقعی پردازش: **${max_vid.get('cost', 0):.5f}**\n"
        f"📦 حجم اولیه: {max_vid.get('size_mb', 0)} MB\n"
        f"📅 تاریخ: {max_vid.get('date', 'نامشخص')}"
    )

    try:
        await bot.send_video(chat_id=ADMIN_ID, video=max_vid["file_id"], caption=cap, parse_mode="Markdown")
    except Exception as e:
        try:
            await bot.send_document(chat_id=ADMIN_ID, document=max_vid["file_id"], caption=cap, parse_mode="Markdown")
        except Exception as e2:
            await callback.message.answer(f"⚠️ ارسال فایل با خطا مواجه شد (ممکن است کش تلگرام پاک شده باشد):\n`{e2}`")


# ۲. تنظیم سقف محدودیت روزانه (Daily Limit)
@dp.callback_query(F.data == "admin_set_limit")
async def show_limit_settings(callback: aiotypes.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    cur_limit = get_daily_limit_mb()
    cur_str = f"{cur_limit} مگابایت" if cur_limit > 0 else "نامحدود (بدون سقف)"

    builder = InlineKeyboardBuilder()
    builder.button(text="100 MB", callback_data="set_lim:100")
    builder.button(text="300 MB", callback_data="set_lim:300")
    builder.button(text="500 MB", callback_data="set_lim:500")
    builder.button(text="1000 MB (1GB)", callback_data="set_lim:1000")
    builder.button(text="2000 MB (2GB)", callback_data="set_lim:2000")
    builder.button(text="نامحدود ♾", callback_data="set_lim:0")
    builder.button(text="✏️ ورود عدد دلخواه", callback_data="set_lim_custom")
    builder.button(text="🔙 بازگشت به پنل", callback_data="admin_back_main")
    builder.adjust(3, 3, 1, 1)

    text = (
        f"⏱ **تنظیم سقف محدودیت روزانه کاربران (Daily Limit):**\n\n"
        f"▫️ سقف فعلی: **{cur_str}**\n\n"
        f"یک گزینه را انتخاب کنید یا عدد دلخواه وارد نمایید:"
    )
    await callback.message.answer(text, reply_markup=builder.as_markup(), parse_mode="Markdown")


@dp.callback_query(F.data.startswith("set_lim:"))
async def apply_preset_limit(callback: aiotypes.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    val = int(callback.data.split(":")[1])
    set_daily_limit_mb(val)
    val_str = f"{val} مگابایت" if val > 0 else "نامحدود"
    await callback.answer(f"سقف به {val_str} تغییر یافت.", show_alert=True)
    await show_limit_settings(callback)


@dp.callback_query(F.data == "set_lim_custom")
async def ask_custom_limit(callback: aiotypes.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    cancel_b = InlineKeyboardBuilder()
    cancel_b.button(text="❌ انصراف", callback_data="cancel_admin_action")

    await callback.message.answer(
        "✏️ لطفاً عدد سقف مجاز روزانه را به **مگابایت (MB)** ارسال کنید (مثلاً: `400` یا برای نامحدود `0`):",
        reply_markup=cancel_b.as_markup(),
        parse_mode="Markdown"
    )
    await state.set_state(AdminMessageState.waiting_for_custom_limit)


@dp.message(AdminMessageState.waiting_for_custom_limit, F.chat.id == ADMIN_ID)
async def process_custom_limit_input(message: aiotypes.Message, state: FSMContext):
    text = message.text.strip() if message.text else ""
    if not text.isdigit():
        return await message.answer("⚠️ لطفاً فقط یک عدد معتبر ارسال کنید:")

    val = int(text)
    set_daily_limit_mb(val)
    await state.clear()
    val_str = f"{val} مگابایت" if val > 0 else "نامحدود"
    await message.answer(f"✅ سقف محدودیت روزانه با موفقیت بر روی **{val_str}** تنظیم شد.", parse_mode="Markdown")


@dp.callback_query(F.data == "admin_back_main")
async def back_to_admin_main(callback: aiotypes.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    users = load_users()
    limit = get_daily_limit_mb()
    limit_str = f"{limit} مگابایت" if limit > 0 else "نامحدود"
    await callback.message.edit_text(
        f"👑 **پنل مدیریت ربات**\n\n"
        f"👥 تعداد کل کاربران ثبت‌شده: **{len(users)} نفر**\n"
        f"⏱ محدودیت روزانه فعلی: **{limit_str}**\n\n"
        f"عملیات مورد نظر را انتخاب کنید:",
        reply_markup=get_admin_panel_keyboard(),
        parse_mode="Markdown"
    )


# ۳. فرآیند ارسال به کل کاربران
@dp.callback_query(F.data == "admin_broadcast")
async def start_broadcast(callback: aiotypes.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    users = load_users()
    cancel_b = InlineKeyboardBuilder()
    cancel_b.button(text="❌ انصراف", callback_data="cancel_admin_action")

    await callback.message.answer(
        f"⚠️ **توجه:** پیام ارسالی شما **به کل کاربران ({len(users)} نفر)** فرستاده خواهد شد.\n\n"
        f"✍️ لطفاً پیام خود را (متن، عکس، ویدیو، صدا یا هر رسانه‌ای) ارسال کنید:",
        reply_markup=cancel_b.as_markup(),
        parse_mode="Markdown"
    )
    await state.set_state(AdminMessageState.waiting_for_broadcast_content)


@dp.message(AdminMessageState.waiting_for_broadcast_content, F.chat.id == ADMIN_ID)
async def process_broadcast(message: aiotypes.Message, state: FSMContext):
    users = load_users()
    await message.answer(f"⏳ در حال ارسال همگانی به {len(users)} کاربر... لطفاً صبور باشید.")

    success = 0
    failed = 0
    for uid in users:
        try:
            await message.copy_to(chat_id=uid)
            success += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1

    await state.clear()
    await message.answer(
        f"📢 **نتیجه ارسال همگانی:**\n\n"
        f"✅ ارسال موفق: **{success} کاربر**\n"
        f"❌ ناموفق (بلاک یا غیرفعال): **{failed} کاربر**\n"
        f"📊 مجموع: **{len(users)} نفر**",
        parse_mode="Markdown"
    )


# ۴. فرآیند ارسال به کاربر خاص
@dp.callback_query(F.data == "admin_send_single")
async def ask_user_id_for_single(callback: aiotypes.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    cancel_b = InlineKeyboardBuilder()
    cancel_b.button(text="❌ انصراف", callback_data="cancel_admin_action")

    await callback.message.answer(
        "👤 لطفاً **آیدی عددی (User ID)** کاربر مقصد را ارسال کنید:",
        reply_markup=cancel_b.as_markup(),
        parse_mode="Markdown"
    )
    await state.set_state(AdminMessageState.waiting_for_user_id)


@dp.message(AdminMessageState.waiting_for_user_id, F.chat.id == ADMIN_ID)
async def process_user_id_input(message: aiotypes.Message, state: FSMContext):
    text = message.text.strip() if message.text else ""
    if not text.isdigit():
        return await message.answer("⚠️ لطفاً فقط یک شناسه عددی معتبر ارسال کنید:")

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
    cancel_b.button(text="❌ انصراف", callback_data="cancel_admin_action")

    confirm_text = (
        f"🎯 **کاربر مقصد مشخص شد:**\n\n"
        f"👤 **نام:** {user_name}\n"
        f"🔗 **یوزرنیم:** {username}\n"
        f"🆔 **آیدی عددی:** `{target_id}`\n\n"
        f"✉️ این پیام **به این یوزر و اسم** ارسال می‌شود.\n"
        f"لطفاً متن، ویدیو، عکس یا ویس مورد نظر را بفرستید:"
    )
    await message.answer(confirm_text, reply_markup=cancel_b.as_markup(), parse_mode="Markdown")
    await state.set_state(AdminMessageState.waiting_for_single_content)


@dp.message(AdminMessageState.waiting_for_single_content, F.chat.id == ADMIN_ID)
async def send_single_message_to_user(message: aiotypes.Message, state: FSMContext):
    data = await state.get_data()
    target_id = data.get("target_id")
    user_name = data.get("user_name", "کاربر")

    try:
        await bot.send_message(chat_id=target_id, text="💬 **پیام از طرف مدیریت:**", parse_mode="Markdown")
        await message.copy_to(chat_id=target_id)
        await message.answer(f"✅ پیام با موفقیت برای **{user_name}** (`{target_id}`) ارسال شد.", parse_mode="Markdown")
    except TelegramForbiddenError:
        await message.answer("❌ خطا: کاربر ربات را بلاک کرده است.")
    except Exception as e:
        await message.answer(f"⚠️ ارسال پیام با خطا مواجه شد:\n`{e}`", parse_mode="Markdown")

    await state.clear()


@dp.callback_query(F.data == "cancel_admin_action")
async def cancel_admin_action(callback: aiotypes.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await callback.answer("عملیات لغو شد.")
    await callback.message.edit_text("❌ عملیات لغو شد.")


# --- مدیریت تنظیمات نمایش ---
@dp.message(F.text == "⚙️ تنظیمات نمایش")
@dp.callback_query(F.data == "open_settings")
async def show_settings_menu(event: aiotypes.Message | aiotypes.CallbackQuery):
    user_id = event.from_user.id
    register_user(user_id)
    text = (
        "⚙️ **تنظیمات نحوه نمایش گزارش پس از تحویل ویدیو:**\n\n"
        "می‌توانید مشخص کنید هنگام تحویل فایل خروجی:\n"
        "🔹 **خلاصه مشخصات کامل:** شامل فرمت خروجی، وضوح، کدک، فشرده‌سازی، صدا و سرعت باشد.\n"
        "🔹 **گزارش ساده:** صرفاً حجم اولیه، حجم خروجی و درصد کاهش حجم باشد."
    )
    kb = get_settings_inline_keyboard(user_id)
    if isinstance(event, aiotypes.CallbackQuery):
        await event.answer()
        await event.message.answer(text, reply_markup=kb, parse_mode="Markdown")
    else:
        await event.answer(text, reply_markup=kb, parse_mode="Markdown")


@dp.callback_query(F.data == "toggle_details")
async def toggle_settings_option(callback: aiotypes.CallbackQuery):
    user_id = callback.from_user.id
    current_status = get_user_show_details(user_id)
    set_user_show_details(user_id, not current_status)
    await callback.answer("تنظیمات به‌روزرسانی شد.")
    try:
        await callback.message.edit_reply_markup(reply_markup=get_settings_inline_keyboard(user_id))
    except TelegramBadRequest:
        pass


@dp.callback_query(F.data == "close_settings")
async def close_settings_menu(callback: aiotypes.CallbackQuery):
    await callback.answer()
    await callback.message.delete()


@dp.callback_query(F.data == "none")
async def no_action_callback(callback: aiotypes.CallbackQuery):
    await callback.answer()


# --- مدیریت پشتیبانی دو طرفه ---
@dp.message(F.text == "📞 پیام به پشتیبانی")
@dp.callback_query(F.data == "start_support")
async def ask_support_message(event: aiotypes.Message | aiotypes.CallbackQuery, state: FSMContext):
    register_user(event.from_user.id)
    cancel_b = InlineKeyboardBuilder()
    cancel_b.button(text="❌ انصراف", callback_data="cancel_support")

    msg_text = "✍️ پیام یا مشکل خود را به صورت کامل بنویسید و ارسال کنید:"
    if isinstance(event, aiotypes.CallbackQuery):
        await event.answer()
        await event.message.answer(msg_text, reply_markup=cancel_b.as_markup())
    else:
        await event.answer(msg_text, reply_markup=cancel_b.as_markup())

    await state.set_state(SupportState.waiting_for_message)


@dp.callback_query(F.data == "cancel_support")
async def cancel_support(callback: aiotypes.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.answer("عملیات لغو شد.")
    await callback.message.edit_text("❌ ارسال پیام به پشتیبانی لغو شد.")


@dp.message(SupportState.waiting_for_message)
async def forward_support_message(message: aiotypes.Message, state: FSMContext):
    u = message.from_user
    user_id = u.id
    name = u.full_name or "بدون نام"
    username = f"@{u.username}" if u.username else "ندارد"
    register_user(user_id)

    admin_header = (
        f"📩 **پیام جدید از کاربر به پشتیبانی:**\n\n"
        f"👤 **نام:** {name}\n"
        f"🆔 **آیدی عددی:** `{user_id}`\n"
        f"🔗 **یوزرنیم:** {username}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💡 **برای پاسخ، مستقیماً روی این پیام ریپلای کنید.**"
    )

    try:
        header_msg = await bot.send_message(chat_id=ADMIN_ID, text=admin_header, parse_mode="Markdown")
        content_msg = await message.copy_to(chat_id=ADMIN_ID)

        SUPPORT_MAP[header_msg.message_id] = user_id
        SUPPORT_MAP[content_msg.message_id] = user_id

        await message.reply("✅ پیام شما دریافت شد و برای پشتیبانی ارسال گردید.")
    except Exception as e:
        logging.error(f"Failed to forward message to admin: {e}")
        await message.reply("⚠️ متأسفانه در ارسال پیام خطایی رخ داد.")

    await state.clear()


@dp.message(F.chat.id == ADMIN_ID, F.reply_to_message)
async def handle_admin_reply(message: aiotypes.Message):
    replied = message.reply_to_message
    target_user_id = SUPPORT_MAP.get(replied.message_id)

    if not target_user_id:
        text_source = (replied.text or "") + " " + (replied.caption or "")
        match = re.search(r"آیدی عددی:\s*`?(\d+)`?", text_source)
        if match:
            target_user_id = int(match.group(1))

    if not target_user_id:
        return

    try:
        await bot.send_message(
            chat_id=target_user_id,
            text="💬 **پاسخ پشتیبانی:**",
            parse_mode="Markdown"
        )
        await message.copy_to(chat_id=target_user_id)
        await message.reply("✅ پاسخ با موفقیت برای کاربر ارسال شد (هویت شما مخفی ماند).")
    except TelegramForbiddenError:
        await message.reply("❌ خطا: کاربر ربات را مسدود کرده است.")
    except Exception as e:
        logging.error(f"Failed to send admin reply: {e}")
        await message.reply(f"⚠️ خطا در ارسال:\n`{e}`")


@dp.callback_query(F.data.startswith("err_send_vid:"))
async def handle_send_error_video(callback: aiotypes.CallbackQuery):
    job_id = callback.data.split(":", 1)[1]
    failed_job = FAILED_JOBS.pop(job_id, None)

    if not failed_job:
        await callback.answer("مهلت ارسال این ویدیو گذشته است.", show_alert=True)
        return await callback.message.edit_text("❌ مهلت ارسال ویدیوی این خطا به پایان رسیده است.")

    await callback.answer("در حال ارسال ویدیو برای تیم پشتیبانی...")
    try:
        await bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                f"🎥 **ویدیوی مربوط به خطای پردازش کاربر:**\n"
                f"👤 {failed_job['user_name']} | `{failed_job['user_id']}`"
            ),
            parse_mode="Markdown"
        )
        await bot.forward_message(
            chat_id=ADMIN_ID,
            from_chat_id=failed_job["chat_id"],
            message_id=failed_job["msg_id"]
        )
        await callback.message.edit_text(
            "✅ ویدیوی شما همراه با گزارش خطا برای پشتیبانی ارسال شد. متشکریم!"
        )
    except Exception as e:
        logging.error(f"Error forwarding failed video: {e}")
        await callback.message.edit_text("⚠️ خطا در ارسال ویدیو.")


@dp.callback_query(F.data.startswith("err_cancel_vid:"))
async def handle_cancel_error_video(callback: aiotypes.CallbackQuery):
    job_id = callback.data.split(":", 1)[1]
    FAILED_JOBS.pop(job_id, None)
    await callback.answer("انصراف ثبت شد.")
    await callback.message.edit_text("👌 ویدیو ارسال نشد. لاگ سیستمی خطا پیش از این برای ادمین ارسال گردیده است.")


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


# --- دریافت و فشرده‌سازی ویدیو با بررسی سقف روزانه ---
@dp.message(F.video | F.document)
async def handle_video(message: aiotypes.Message):
    register_user(message.from_user.id)
    video = message.video or (
        message.document if message.document and message.document.mime_type and (
            message.document.mime_type.startswith("video/") or message.document.file_name.lower().endswith((".mp4", ".mkv", ".mov", ".avi", ".webm"))
        ) else None
    )
    if not video:
        return await message.answer("⚠️ لطفاً یک فایل ویدیویی معتبر ارسال کنید.")
    if video.file_size > MAX_FILE_SIZE:
        return await message.answer("❌ حجم فایل بیشتر از سقف مجاز ۲ گیگابایت است.")

    file_size_mb = video.file_size / (1024 * 1024)
    allowed, cur_mb, limit_mb = check_and_update_daily_usage(message.from_user.id, file_size_mb)

    if not allowed:
        return await message.answer(
            f"⚠️ **محدودیت مصرف روزانه:**\n\n"
            f"سقف مجاز روزانه: **{limit_mb} مگابایت**\n"
            f"مصرف امروز شما: **{cur_mb:.1f} مگابایت**\n"
            f"حجم این فایل: **{file_size_mb:.1f} مگابایت**\n\n"
            f"امکان پردازش وجود ندارد. فردا مجدداً تلاش فرمایید.",
            parse_mode="Markdown"
        )

    orig_ext = detect_file_extension(message)
    default_cfg = {
        "mode": "video",
        "res": "720",
        "codec": "h264",
        "crf": "medium",
        "mute": False,
        "speed": "1.0",
        "fmt": "orig"
    }
    await message.reply(
        f"⚙️ **تنظیمات تبدیل و فشرده‌سازی ویدیو:**\n▫️ فرمت ورودی: **{orig_ext.upper()}**",
        reply_markup=build_config_keyboard(default_cfg, orig_ext=orig_ext)
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
    await callback.message.edit_text("❌ لغو شد.")


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

        await callback.answer("پردازش متوقف شد.")
        await callback.message.edit_text("🛑 پردازش لغو شد و نوبت صف آزاد گردید.")
    else:
        await callback.answer("پردازشی در حال اجرا نیست.", show_alert=True)


@dp.callback_query(F.data.startswith("run:"))
async def enqueue_task(callback: aiotypes.CallbackQuery):
    await callback.answer()
    cfg = decode_cfg(callback.data[4:])
    orig_msg = callback.message.reply_to_message
    if not orig_msg:
        return await callback.message.edit_text("❌ پیام ویدیوی مرجع یافت نشد.")

    video = orig_msg.video or (
        orig_msg.document if orig_msg.document and orig_msg.document.mime_type and (
            orig_msg.document.mime_type.startswith("video/") or orig_msg.document.file_name.lower().endswith((".mp4", ".mkv", ".mov", ".avi", ".webm"))
        ) else None
    )
    if not video:
        return await callback.message.edit_text("❌ ویدیویی یافت نشد.")

    orig_ext = detect_file_extension(orig_msg)
    job_id = f"{callback.message.chat.id}_{callback.message.message_id}"
    status_msg = await callback.message.edit_text(
        f"⏳ در صف انتظار سرور...\n👥 نوبت شما: **نفر {JOB_QUEUE.qsize() + 1}**",
        reply_markup=get_cancel_keyboard(job_id)
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

            admin_err_alert = (
                f"🚨 **گزارش خطای خودکار در پردازش ویدیو:**\n\n"
                f"👤 **نام کاربر:** {user_name}\n"
                f"🆔 **آیدی عددی:** `{user_id}`\n"
                f"🔗 **یوزرنیم:** {username}\n"
                f"❌ **شرح خطا:** `{str(e)[:250]}`\n\n"
                f"📋 **لاگ سیستمی:**\n"
                f"```text\n{clean_tb[:800]}\n```"
            )
            try:
                await bot.send_message(chat_id=ADMIN_ID, text=admin_err_alert, parse_mode="Markdown")
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
            err_kb.button(text="بله، ویدیو هم فرستاده شود ✅", callback_data=f"err_send_vid:{job_id}")
            err_kb.button(text="خیر، لازم نیست ❌", callback_data=f"err_cancel_vid:{job_id}")
            err_kb.adjust(1)

            user_notice = (
                "⚠️ متأسفانه در فرآیند تبدیل ویدیوی شما خطایی رخ داد.\n\n"
                "📨 **توضیحات خطا فرستاده شد.**\n\n"
                "❓ آیا می‌خواهید ویدیو هم همراهش بفرستم تا مشکل سریع‌تر حل بشه؟"
            )
            try:
                await job["status_msg"].edit_text(user_notice, reply_markup=err_kb.as_markup(), parse_mode="Markdown")
            except Exception:
                try:
                    await bot.send_message(chat_id=job["chat_id"], text=user_notice, reply_markup=err_kb.as_markup(), parse_mode="Markdown")
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
                text = f"📥 در حال دریافت فایل از تلگرام:\n{bar}"
            elif act == "encode":
                eta_val = state.get("eta")
                if state.get("file_size", 0) >= 50 * 1024 * 1024 and eta_val:
                    text = f"⚙️ در حال فشرده‌سازی و پردازش:\n{bar}\n⏱ زمان تقریبی تا پایان پردازش: **{eta_val}**"
                else:
                    text = f"⚙️ در حال فشرده‌سازی و پردازش:\n{bar}"
            elif act == "upload":
                text = f"📤 در حال ارسال به تلگرام:\n{bar}"
            else:
                text = "⏳ لطفا کمی صبر کنید..."

            if text != last_text:
                await state["status_msg"].edit_text(text, reply_markup=get_cancel_keyboard(state["job_id"]), parse_mode="Markdown")
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
                    f"🔄 در حال تلاش مجدد برای دریافت فایل ({attempt}/{max_retries})...",
                    reply_markup=get_cancel_keyboard(job["job_id"])
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
                    raise RuntimeError(
                        f"دانلود ناقص است: {downloaded_bytes / (1024*1024):.2f}MB از {initial_size / (1024*1024):.2f}MB."
                    )
            else:
                raise RuntimeError("فایل ذخیره نشد.")

        except Exception as e:
            last_err = e
            await asyncio.sleep(2)

    raise RuntimeError(f"دانلود متوقف شد:\n{last_err}")


async def process_job(job: dict):
    job_id = job["job_id"]
    cfg = job["cfg"]
    status_msg = job["status_msg"]
    mode = cfg["mode"]
    initial_size = job["file_size"]
    user_id = job.get("user_id", job["chat_id"])
    u = job.get("user")
    user_name = u.full_name if u else "کاربر"
    username = f"@{u.username}" if (u and u.username) else "ندارد"
    orig_ext = job.get("orig_ext", "mp4")
    fmt_choice = cfg.get("fmt", "orig")
    speed_factor = float(cfg.get("speed", "1.0"))

    # اندازه‌گیری دقیق منابع جهت محاسبه هزینه بدون تقریب
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
            scale = "scale=trunc(iw/2)*2:trunc(ih/2)*2" if cfg["res"] == "orig" else f"scale=-2:{cfg['res']}:flags=fast_bilinear"
            vf = [f"setpts={1.0 / speed_factor}*PTS"] if speed_factor != 1.0 else []
            vf.append(scale)

            cmd += [
                "-map", "0:v:0",
                "-c:v", v_codec,
                "-vf", ",".join(vf),
                "-crf", crf_map.get(cfg["crf"], "28"),
                "-preset", "ultrafast",
                "-pix_fmt", "yuv420p"
            ]

            if v_codec == "libx264":
                cmd += ["-x264opts", "rc-lookahead=10:sync-lookahead=0:bframes=2"]
            elif v_codec == "libx265":
                cmd += ["-x265-params", "pools=1:frame-threads=1:rc-lookahead=5:bframes=2"]

            if cfg["mute"]:
                cmd += ["-an"]
            else:
                cmd += ["-map", "0:a:0?", "-c:a", "aac", "-b:a", "128k", "-ar", "44100"]
                if speed_factor != 1.0:
                    cmd += ["-filter:a", f"atempo={speed_factor}"]

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
                                ui_state["eta"] = f"{mins} دقیقه و {secs} ثانیه"
                            else:
                                ui_state["eta"] = f"{secs} ثانیه"

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
        base_caption = (
            f"✅ پردازش انجام شد\n\n"
            f"📦 اولیه: {initial_size / (1024*1024):.2f} MB\n"
            f"📉 خروجی: {final_size / (1024*1024):.2f} MB\n"
            f"⚡ فشرده‌سازی: {reduction}%"
        )

        if show_details:
            if mode == "audio":
                summary_text = (
                    f"\n\n🛠 **مشخصات تبدیل:**\n"
                    f"▫️ فرمت خروجی: {out_ext.upper()}\n"
                    f"▫️ نوع خروجی: فایل صوتی"
                )
            else:
                res_names = {"orig": "وضوح اصلی", "1080": "1080p", "720": "720p", "480": "480p"}
                codec_names = {"h264": "H.264", "h265": "H.265 (HEVC)"}
                crf_names = {"light": "کاهش کم (کیفیت بالا)", "medium": "متعادل", "heavy": "کاهش زیاد (کم‌حجم)"}
                mute_name = "بی‌صدا 🔇" if cfg["mute"] else "همراه صدا 🔊"
                
                summary_text = (
                    f"\n\n🛠 **تنظیمات اعمال‌شده:**\n"
                    f"▫️ فرمت خروجی: {out_ext.upper()}\n"
                    f"▫️ وضوح: {res_names.get(cfg['res'], cfg['res'])}\n"
                    f"▫️ کدک: {codec_names.get(cfg['codec'], cfg['codec'])}\n"
                    f"▫️ شدت فشرده‌سازی: {crf_names.get(cfg['crf'], cfg['crf'])}\n"
                    f"▫️ وضعیت صدا: {mute_name}\n"
                    f"▫️ سرعت ویدیو: {speed_factor}x"
                )
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

        # محاسبه هزینه واقعی دلاری بر اساس تعرفه دقیق Railway
        end_cpu_sec = get_cpu_seconds()
        end_wall_time = time.time()

        cpu_used_sec = max(0.01, end_cpu_sec - start_cpu_sec)
        wall_time_sec = max(0.01, end_wall_time - start_wall_time)

        cpu_cost = cpu_used_sec * 0.00000772
        ram_cost = wall_time_sec * 0.35 * 0.00000386
        egress_gb = final_size / (1024 ** 3)
        network_cost = egress_gb * 0.05
        exact_cost = cpu_cost + ram_cost + network_cost

        # ثبت آمار مصرف کاربر و ذخیره پرمصرف‌ترین ویدیو
        record_job_stats(
            user_id=user_id,
            name=user_name,
            username=username,
            cost=exact_cost,
            file_size_mb=initial_size / (1024 * 1024),
            file_id=job["file_id"]
        )

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
    logging.info("در حال اتصال کلاینت Pyrogram...")
    try:
        await pyro.start()
        logging.info("✅ کلاینت Pyrogram با موفقیت متصل شد.")
    except Exception as e:
        logging.error(f"خطای شروع Pyrogram: {e}")

    asyncio.create_task(queue_worker())
    logging.info("✅ ربات آنلاین و آماده دریافت ویدیو است.")
    try:
        await dp.start_polling(bot, drop_pending_updates=True)
    finally:
        if pyro.is_connected:
            await pyro.stop()


if __name__ == "__main__":
    asyncio.run(main())
