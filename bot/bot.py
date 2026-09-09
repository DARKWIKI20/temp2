import os
import gc
import re
import json
import math
import types
import random
import asyncio
import logging
import traceback
import subprocess

from aiogram import Bot, Dispatcher, F, types as aiotypes
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.types import FSInputFile, ReplyKeyboardMarkup, KeyboardButton
from pyrogram import Client as PyroClient, raw

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BOT_TOKEN = os.getenv("BOT_TOKEN", "8812733722:AAEFW8oxPPQYyqrqHGtnvS8fTpu3ATxcDbo")
ADMIN_ID = int(os.getenv("ADMIN_ID", "6616272875"))
API_ID = int(os.getenv("API_ID", "26202905"))
API_HASH = os.getenv("API_HASH", "ec9fd909b90288d01befa4f87c8d71c1")

FFMPEG_BIN = "ffmpeg"
MAX_FILE_SIZE = 2000 * 1024 * 1024

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

# کلاس وضعیت برای ارسال پیام به پشتیبانی
class SupportState(StatesGroup):
    waiting_for_message = State()

# موتور آپلود ترتیبی بدون قطعی با مدیریت کاور و استخراج صدا
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


def get_main_reply_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.button(text="📞 پیام به پشتیبانی")
    return builder.as_markup(resize_keyboard=True)


def generate_progress_bar(percent: float) -> str:
    total_blocks = 12
    filled = int(round((percent / 100) * total_blocks))
    filled = min(total_blocks, max(0, filled))
    return f"[{'█' * filled}{'░' * (total_blocks - filled)}] {percent:.1f}%"


def encode_cfg(mode, res, codec, crf, mute, speed):
    return f"{mode}:{res}:{codec}:{crf}:{'1' if mute else '0'}:{speed}"


def decode_cfg(data_str):
    p = data_str.split(":")
    return {"mode": p[0], "res": p[1], "codec": p[2], "crf": p[3], "mute": p[4] == "1", "speed": p[5]}


def build_config_keyboard(cfg: dict):
    b = InlineKeyboardBuilder()
    mode, res, codec, crf, mute, speed = cfg["mode"], cfg["res"], cfg["codec"], cfg["crf"], cfg["mute"], cfg["speed"]

    b.button(text="🎬 ویدیو" + (" ✅" if mode == "video" else ""), callback_data="cfg:" + encode_cfg("video", res, codec, crf, mute, speed))
    b.button(text="🎵 استخراج MP3" + (" ✅" if mode == "mp3" else ""), callback_data="cfg:" + encode_cfg("mp3", res, codec, crf, mute, speed))

    if mode == "video":
        for r_k, r_t in [("orig", "اصلی"), ("1080", "1080p"), ("720", "720p"), ("480", "480p")]:
            b.button(text=r_t + (" ✅" if res == r_k else ""), callback_data="cfg:" + encode_cfg(mode, r_k, codec, crf, mute, speed))
        b.button(text="H.264" + (" ✅" if codec == "h264" else ""), callback_data="cfg:" + encode_cfg(mode, res, "h264", crf, mute, speed))
        b.button(text="H.265 (کم‌حجم)" + (" ✅" if codec == "h265" else ""), callback_data="cfg:" + encode_cfg(mode, res, "h265", crf, mute, speed))
        for c_k, c_t in [("light", "کاهش کم"), ("medium", "متعادل"), ("heavy", "کاهش زیاد")]:
            b.button(text=c_t + (" ✅" if crf == c_k else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, c_k, mute, speed))
        b.button(text="🔇 صدا: قطع" if mute else "🔊 صدا: وصل", callback_data="cfg:" + encode_cfg(mode, res, codec, crf, not mute, speed))
        for s_k, s_t in [("1.0", "1x"), ("1.5", "1.5x"), ("2.0", "2x")]:
            b.button(text=s_t + (" ✅" if speed == s_k else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, crf, mute, s_k))

    b.button(text="🚀 شروع پردازش", callback_data=f"run:{encode_cfg(mode, res, codec, crf, mute, speed)}")
    b.button(text="❌ انصراف", callback_data="cancel_panel")

    if mode == "video":
        b.adjust(2, 4, 2, 3, 1, 3, 2)
    else:
        b.adjust(2, 2)
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
    builder = InlineKeyboardBuilder()
    builder.button(text="📞 پیام به پشتیبانی", callback_data="start_support")

    await message.answer(
        "👋 سلام! به ربات پردازش و فشرده‌سازی ویدیو خوش آمدید.\n\n"
        "🎬 ویدیوی مورد نظر خود را ارسال کنید (پشتیبانی تا سقف ۲ گیگابایت).\n"
        "یا از گزینه‌های زیر برای ارتباط با پشتیبانی استفاده کنید:",
        reply_markup=get_main_reply_keyboard()
    )
    await message.answer("📌 منوی دسترسی سریع:", reply_markup=builder.as_markup())


# --- بخش مدیریت پشتیبانی ---
@dp.message(F.text == "📞 پیام به پشتیبانی")
@dp.callback_query(F.data == "start_support")
async def ask_support_message(event: aiotypes.Message | aiotypes.CallbackQuery, state: FSMContext):
    cancel_b = InlineKeyboardBuilder()
    cancel_b.button(text="❌ انصراف", callback_data="cancel_support")

    msg_text = "✍️ لطفاً پیام، انتقاد یا مشکل خود را به صورت کامل بنویسید و ارسال کنید:"
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

    admin_header = (
        f"📩 **پیام جدید از سمت کاربر برای پشتیبانی:**\n\n"
        f"👤 **نام:** {name}\n"
        f"🆔 **آیدی عددی:** `{user_id}`\n"
        f"🔗 **یوزرنیم:** {username}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
    )

    try:
        # ارسال پیام به ادمین همراه با مشخصات کاربر
        await bot.send_message(chat_id=ADMIN_ID, text=admin_header, parse_mode="Markdown")
        await message.forward(chat_id=ADMIN_ID)
        await message.reply("✅ پیام شما دریافت شد و برای پشتیبانی ارسال گردید. به زودی بررسی خواهد شد.")
    except Exception as e:
        logging.error(f"Failed to forward message to admin: {e}")
        await message.reply("⚠️ متأسفانه در ارسال پیام به پشتیبانی خطایی رخ داد. لطفاً بعداً تلاش کنید.")

    await state.clear()


# --- بخش پردازش ویدیو ---
@dp.message(F.video | F.document)
async def handle_video(message: aiotypes.Message):
    video = message.video or (
        message.document if message.document and message.document.mime_type and message.document.mime_type.startswith("video/") else None
    )
    if not video:
        return await message.answer("⚠️ لطفاً فایل ویدیویی معتبر ارسال کنید.")
    if video.file_size > MAX_FILE_SIZE:
        return await message.answer("❌ حجم فایل بیشتر از سقف مجاز ۲ گیگابایت است.")

    default_cfg = {"mode": "video", "res": "720", "codec": "h264", "crf": "medium", "mute": False, "speed": "1.0"}
    await message.reply("⚙️ **تنظیمات تبدیل را مشخص کنید:**", reply_markup=build_config_keyboard(default_cfg))


@dp.callback_query(F.data.startswith("cfg:"))
async def update_settings(callback: aiotypes.CallbackQuery):
    await callback.answer()
    cfg = decode_cfg(callback.data[4:])
    try:
        await callback.message.edit_reply_markup(reply_markup=build_config_keyboard(cfg))
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
        orig_msg.document if orig_msg.document and orig_msg.document.mime_type and orig_msg.document.mime_type.startswith("video/") else None
    )
    if not video:
        return await callback.message.edit_text("❌ ویدیویی یافت نشد.")

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

            err_text = (
                f"⚠️ **خطایی در اجرای عملیات رخ داد:**\n\n"
                f"❌ **شرح خطا:** `{str(e)[:200]}`\n\n"
                f"📋 **جزئیات لاگ سیستمی:**\n"
                f"```text\n{clean_tb[:1200]}\n```"
            )
            try:
                await job["status_msg"].edit_text(err_text, parse_mode="Markdown")
            except Exception:
                try:
                    await bot.send_message(chat_id=job["chat_id"], text=err_text, parse_mode="Markdown")
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
    speed_factor = float(cfg.get("speed", "1.0"))

    input_path = os.path.join(DOWNLOAD_DIR, f"in_{job_id}.mp4")
    ext = "mp3" if mode == "mp3" else "mp4"
    output_path = os.path.join(DOWNLOAD_DIR, f"out_{job_id}.{ext}")
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

        # ۱. دریافت کامل فایل
        await download_with_retry(job, input_path, ui_state, max_retries=3)

        if ACTIVE_PROCESSES[job_id]["cancelled"]:
            return

        gc.collect()

        # ۲. پردازش و فشرده‌سازی با FFmpeg
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

        if mode == "mp3":
            cmd += ["-vn", "-c:a", "libmp3lame", "-b:a", "192k", "-progress", "pipe:2", output_path]
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

            cmd += [
                "-avoid_negative_ts", "make_zero",
                "-movflags", "+faststart",
                "-progress", "pipe:2",
                output_path
            ]

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

        # ۳. دریافت متادیتا
        out_meta = await get_media_meta(output_path)
        out_dur = out_meta["duration"] or int(eff_duration)
        out_w = out_meta["width"]
        out_h = out_meta["height"]

        if mode == "video":
            await generate_thumbnail(output_path, thumb_path, out_dur)

        # ۴. آپلود ترتیبی بدون فریز و ارور
        ui_state["action"] = "upload"
        ui_state["percent"] = 0.0

        final_size = os.path.getsize(output_path)
        reduction = max(0, int(((initial_size - final_size) / initial_size) * 100))
        caption = f"✅ پردازش انجام شد\n\n📦 اولیه: {initial_size / (1024*1024):.2f} MB\n📉 خروجی: {final_size / (1024*1024):.2f} MB\n⚡ فشرده‌سازی: {reduction}%"

        has_thumb = os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 100
        chat_id = job["chat_id"]

        if mode == "mp3":
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
