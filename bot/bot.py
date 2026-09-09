import os
import re
import asyncio
import logging
import subprocess

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import CommandStart
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.types import FSInputFile
from pyrogram import Client as PyroClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BOT_TOKEN = os.getenv("BOT_TOKEN", "8812733722:AAEFW8oxPPQYyqrqHGtnvS8fTpu3ATxcDbo")
ADMIN_ID = int(os.getenv("ADMIN_ID", "6616272875"))
API_ID = int(os.getenv("API_ID", "26202905"))
API_HASH = os.getenv("API_HASH", "ec9fd909b90288d01befa4f87c8d71c1")

FFMPEG_BIN = "ffmpeg"
MAX_FILE_SIZE = 2000 * 1024 * 1024

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

pyro = PyroClient(name="bot_engine", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

JOB_QUEUE = asyncio.Queue()
ACTIVE_PROCESSES = {}
RUNNING_TASKS = {}


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
    b.button(text="🎞 گیف GIF" + (" ✅" if mode == "gif" else ""), callback_data="cfg:" + encode_cfg("gif", res, codec, crf, mute, speed))

    if mode == "video":
        for r_k, r_t in [("orig", "اصلی"), ("1080", "1080p"), ("720", "720p"), ("480", "480p")]:
            b.button(text=r_t + (" ✅" if res == r_k else ""), callback_data="cfg:" + encode_cfg(mode, r_k, codec, crf, mute, speed))
        b.button(text="H.264" + (" ✅" if codec == "h264" else ""), callback_data="cfg:" + encode_cfg(mode, res, "h264", crf, mute, speed))
        b.button(text="H.265 (کم‌حجم)" + (" ✅" if codec == "h265" else ""), callback_data="cfg:" + encode_cfg(mode, res, "h265", crf, mute, speed))
        for c_k, c_t in [("light", "کاهش کم"), ("medium", "متعادل"), ("heavy", "کاهش زیاد")]:
            b.button(text=c_t + (" ✅" if crf == c_k else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, c_k, mute, speed))
        b.button(text="🔇 صدا: قطع" if mute else "🔊 صدا: وصل", callback_data="cfg:" + encode_cfg(mode, res, codec, crf, not mute, speed))

    if mode in ["video", "gif"]:
        for s_k, s_t in [("1.0", "1x"), ("1.5", "1.5x"), ("2.0", "2x")]:
            b.button(text=s_t + (" ✅" if speed == s_k else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, crf, mute, s_k))

    b.button(text="🚀 شروع پردازش", callback_data=f"run:{encode_cfg(mode, res, codec, crf, mute, speed)}")
    b.button(text="❌ انصراف", callback_data="cancel_panel")

    if mode == "video":
        b.adjust(3, 4, 2, 3, 1, 3, 2)
    elif mode == "gif":
        b.adjust(3, 3, 2)
    else:
        b.adjust(3, 2)
    return b.as_markup()


def get_cancel_keyboard(job_id: str):
    b = InlineKeyboardBuilder()
    b.button(text="❌ لغو فوری پردازش", callback_data=f"stop:{job_id}")
    return b.as_markup()


async def get_video_duration(file_path: str) -> float:
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", file_path,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=3.0)
        val = float(stdout.decode().strip())
        if val > 0:
            return val
    except Exception:
        pass

    try:
        cmd = [FFMPEG_BIN, "-i", file_path]
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        _, stderr = await asyncio.wait_for(proc.communicate(), timeout=3.0)
        match = re.search(r"Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)", stderr.decode(errors="ignore"))
        if match:
            h, m, s = map(float, match.groups())
            return h * 3600 + m * 60 + s
    except Exception:
        pass
    return 0.0


@dp.message(CommandStart())
async def start_handler(message: types.Message):
    await message.answer("🎬 ویدیوی خود را بفرستید (پشتیبانی تا ۲ گیگابایت).")


@dp.message(F.video | F.document)
async def handle_video(message: types.Message):
    video = message.video or (
        message.document if message.document and message.document.mime_type and message.document.mime_type.startswith("video/") else None
    )
    if not video:
        return await message.answer("⚠️ لطفاً فایل ویدیویی ارسال کنید.")
    if video.file_size > MAX_FILE_SIZE:
        return await message.answer("❌ حجم فایل بیشتر از سقف مجاز ۲ گیگابایت است.")

    default_cfg = {"mode": "video", "res": "720", "codec": "h264", "crf": "medium", "mute": False, "speed": "1.0"}
    await message.reply("⚙️ **تنظیمات تبدیل را مشخص کنید:**", reply_markup=build_config_keyboard(default_cfg))


@dp.callback_query(F.data.startswith("cfg:"))
async def update_settings(callback: types.CallbackQuery):
    await callback.answer()
    cfg = decode_cfg(callback.data[4:])
    try:
        await callback.message.edit_reply_markup(reply_markup=build_config_keyboard(cfg))
    except TelegramBadRequest:
        pass


@dp.callback_query(F.data == "cancel_panel")
async def cancel_panel(callback: types.CallbackQuery):
    await callback.message.edit_text("❌ لغو شد.")


@dp.callback_query(F.data.startswith("stop:"))
async def stop_processing(callback: types.CallbackQuery):
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

        await callback.answer("پردازش بلافاصله متوقف شد.")
        await callback.message.edit_text("🛑 پردازش لغو شد و نوبت صف آزاد گردید.")
    else:
        await callback.answer("پردازشی در حال اجرا نیست.", show_alert=True)


@dp.callback_query(F.data.startswith("run:"))
async def enqueue_task(callback: types.CallbackQuery):
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
        "chat_id": callback.message.chat.id, "user": callback.from_user, "status_msg": status_msg
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
            logging.info(f"Task {job_id} cancelled cleanly.")
        except Exception as e:
            logging.error(f"Job {job_id} error: {e}", exc_info=True)
            try:
                await job["status_msg"].edit_text(f"⚠️ خطایی رخ داد: `{e}`")
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
                text = f"📥 در حال دریافت فایل:\n{bar}"
            elif act == "encode":
                text = f"⚙️ در حال فشرده‌سازی و پردازش:\n{bar}"
            elif act == "upload":
                text = f"📤 در حال ارسال به تلگرام:\n{bar}"
            else:
                text = "⏳ لطفا کمی صبر کنید..."

            if text != last_text:
                await state["status_msg"].edit_text(text, reply_markup=get_cancel_keyboard(state["job_id"]))
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

    ui_state = {"status_msg": status_msg, "job_id": job_id, "action": "download", "percent": 0.0, "done": False}
    ui_task = asyncio.create_task(ui_updater(ui_state))

    try:
        # ۱. دانلود فایل مستقیم روی دیسک
        if initial_size < 19.5 * 1024 * 1024:
            ui_state["percent"] = 50.0
            file_info = await bot.get_file(job["file_id"])
            await bot.download_file(file_info.file_path, destination=input_path)
            ui_state["percent"] = 100.0
        else:
            msg = await pyro.get_messages(chat_id=job["chat_id"], message_ids=job["msg_id"])
            await msg.download(file_name=input_path, progress=pyro_progress, progress_args=(ui_state,))

        if ACTIVE_PROCESSES[job_id]["cancelled"]:
            return

        # ۲. پیکربندی بهینه FFmpeg (مصرف حداقل رم)
        ui_state["action"] = "encode"
        ui_state["percent"] = 1.0

        duration = await get_video_duration(input_path)
        eff_duration = duration / speed_factor if speed_factor > 0 else duration

        # محدود کردن به ۱ تا ۲ ترد برای پیشگیری قطعی از کرش رم سرور
        cmd = [FFMPEG_BIN, "-y", "-i", input_path, "-threads", "2", "-max_muxing_queue_size", "512"]

        if mode == "mp3":
            cmd += ["-vn", "-c:a", "libmp3lame", "-b:a", "192k", "-progress", "pipe:2", output_path]
        elif mode == "gif":
            vf = [f"setpts={1.0 / speed_factor}*PTS"] if speed_factor != 1.0 else []
            vf += ["fps=15", "scale=480:-2:flags=lanczos"]
            cmd += ["-an", "-c:v", "libx264", "-vf", ",".join(vf), "-crf", "28", "-preset", "veryfast", "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-progress", "pipe:2", output_path]
        else:
            crf_map = {"light": "23", "medium": "28", "heavy": "34"}
            v_codec = "libx265" if cfg["codec"] == "h265" else "libx264"
            scale = "scale=trunc(iw/2)*2:trunc(ih/2)*2" if cfg["res"] == "orig" else f"scale=-2:{cfg['res']}:flags=lanczos"
            vf = [f"setpts={1.0 / speed_factor}*PTS"] if speed_factor != 1.0 else []
            vf.append(scale)
            cmd += ["-map", "0:v:0", "-c:v", v_codec, "-vf", ",".join(vf), "-crf", crf_map.get(cfg["crf"], "28"), "-preset", "veryfast", "-pix_fmt", "yuv420p", "-movflags", "+faststart"]
            if cfg["mute"]:
                cmd += ["-an"]
            else:
                cmd += ["-map", "0:a:0?", "-c:a", "aac", "-b:a", "128k", "-ac", "2", "-strict", "experimental"]
                if speed_factor != 1.0:
                    cmd += ["-filter:a", f"atempo={speed_factor}"]
            cmd += ["-progress", "pipe:2", output_path]

        proc = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        ACTIVE_PROCESSES[job_id]["proc"] = proc

        time_us_pattern = re.compile(r"out_time_us=(\d+)")
        time_str_pattern = re.compile(r"out_time=(\d+):(\d+):(\d+(?:\.\d+)?)")

        while True:
            line = await proc.stderr.readline()
            if not line:
                break
            if ACTIVE_PROCESSES[job_id]["cancelled"]:
                try: proc.kill()
                except Exception: pass
                return

            line_str = line.decode(errors="ignore").strip()
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

        await proc.wait()

        if ACTIVE_PROCESSES[job_id]["cancelled"]:
            return

        if proc.returncode != 0 or not os.path.exists(output_path):
            raise RuntimeError(f"خطای پردازشگر کد: {proc.returncode}")

        # ۳. ارسال به تلگرام
        ui_state["action"] = "upload"
        ui_state["percent"] = 25.0

        final_size = os.path.getsize(output_path)
        reduction = max(0, int(((initial_size - final_size) / initial_size) * 100))
        caption = f"✅ پردازش انجام شد\n\n📦 اولیه: {initial_size / (1024*1024):.2f} MB\n📉 خروجی: {final_size / (1024*1024):.2f} MB\n⚡ فشرده‌سازی: {reduction}%"

        if final_size < 49.5 * 1024 * 1024:
            if mode == "mp3":
                await bot.send_audio(chat_id=job["chat_id"], audio=FSInputFile(output_path), caption=caption)
            elif mode == "gif":
                await bot.send_animation(chat_id=job["chat_id"], animation=FSInputFile(output_path), caption=caption)
            else:
                await bot.send_video(chat_id=job["chat_id"], video=FSInputFile(output_path), caption=caption, supports_streaming=True)
        else:
            if mode == "mp3":
                await pyro.send_audio(chat_id=job["chat_id"], audio=output_path, caption=caption, progress=pyro_progress, progress_args=(ui_state,))
            elif mode == "gif":
                await pyro.send_animation(chat_id=job["chat_id"], animation=output_path, caption=caption, unsave=True, progress=pyro_progress, progress_args=(ui_state,))
            else:
                await pyro.send_video(chat_id=job["chat_id"], video=output_path, caption=caption, supports_streaming=True, progress=pyro_progress, progress_args=(ui_state,))

        ui_state["done"] = True
        ui_task.cancel()
        await status_msg.delete()

    finally:
        ui_state["done"] = True
        if not ui_task.done():
            ui_task.cancel()
        for p in (input_path, output_path):
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass


async def start_pyrogram():
    while True:
        try:
            await pyro.start()
            logging.info("✅ کلاینت Pyrogram متصل شد.")
            break
        except Exception as e:
            logging.warning(f"انتظار برای اتصال Pyrogram: {e}")
            await asyncio.sleep(5)


async def main():
    asyncio.create_task(start_pyrogram())
    asyncio.create_task(queue_worker())
    logging.info("✅ ربات آنلاین و آماده است.")
    try:
        await dp.start_polling(bot, drop_pending_updates=True)
    finally:
        try:
            await pyro.stop()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
