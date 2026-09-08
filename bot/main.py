import os
import re
import time
import asyncio
import logging
import aiohttp

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
WORKER_URL = os.getenv("WORKER_URL", "http://worker.railway.internal:8000")

MAX_FILE_SIZE = 2000 * 1024 * 1024

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

pyro = PyroClient(
    name="bot_engine",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

JOB_QUEUE = asyncio.Queue()
ACTIVE_PROCESSES = {}


def generate_progress_bar(percent: float) -> str:
    total_blocks = 15
    filled = int(round((percent / 100) * total_blocks))
    filled = min(total_blocks, max(0, filled))
    bar = "█" * filled + "░" * (total_blocks - filled)
    return f"[{bar}] {percent:.1f}%"


def encode_cfg(mode, res, codec, crf, mute, speed):
    m_val = "1" if mute else "0"
    return f"{mode}:{res}:{codec}:{crf}:{m_val}:{speed}"


def decode_cfg(data_str):
    parts = data_str.split(":")
    return {
        "mode": parts[0],
        "res": parts[1],
        "codec": parts[2],
        "crf": parts[3],
        "mute": parts[4] == "1",
        "speed": parts[5]
    }


def build_config_keyboard(cfg: dict):
    b = InlineKeyboardBuilder()
    mode = cfg["mode"]
    res = cfg["res"]
    codec = cfg["codec"]
    crf = cfg["crf"]
    mute = cfg["mute"]
    speed = cfg["speed"]

    b.button(text="🎬 ویدیو" + (" ✅" if mode == "video" else ""), callback_data="cfg:" + encode_cfg("video", res, codec, crf, mute, speed))
    b.button(text="🎵 استخراج MP3" + (" ✅" if mode == "mp3" else ""), callback_data="cfg:" + encode_cfg("mp3", res, codec, crf, mute, speed))
    b.button(text="🎞 گیف GIF" + (" ✅" if mode == "gif" else ""), callback_data="cfg:" + encode_cfg("gif", res, codec, crf, mute, speed))

    if mode == "video":
        for r_k, r_t in [("orig", "ابعاد اصلی"), ("1080", "1080p"), ("720", "720p"), ("480", "480p")]:
            b.button(text=r_t + (" ✅" if res == r_k else ""), callback_data="cfg:" + encode_cfg(mode, r_k, codec, crf, mute, speed))

        b.button(text="H.264" + (" ✅" if codec == "h264" else ""), callback_data="cfg:" + encode_cfg(mode, res, "h264", crf, mute, speed))
        b.button(text="H.265 (کم‌حجم‌تر)" + (" ✅" if codec == "h265" else ""), callback_data="cfg:" + encode_cfg(mode, res, "h265", crf, mute, speed))

        for c_k, c_t in [("light", "کاهش کم"), ("medium", "متعادل"), ("heavy", "کاهش شدید")]:
            b.button(text=c_t + (" ✅" if crf == c_k else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, c_k, mute, speed))

        mute_t = "🔇 صدا: قطع" if mute else "🔊 صدا: وصل"
        b.button(text=mute_t, callback_data="cfg:" + encode_cfg(mode, res, codec, crf, not mute, speed))

    if mode in ["video", "gif"]:
        for s_k, s_t in [("1.0", "1x عادی"), ("1.5", "1.5x"), ("2.0", "2x سریع")]:
            b.button(text=s_t + (" ✅" if speed == s_k else ""), callback_data="cfg:" + encode_cfg(mode, res, codec, crf, mute, s_k))

    cfg_str = encode_cfg(mode, res, codec, crf, mute, speed)
    b.button(text="🚀 ثبت در صف و شروع", callback_data=f"run:{cfg_str}")
    b.button(text="❌ لغو", callback_data="cancel_panel")

    if mode == "video":
        b.adjust(3, 4, 2, 3, 1, 3, 2)
    elif mode == "gif":
        b.adjust(3, 3, 2)
    else:
        b.adjust(3, 2)

    return b.as_markup()


def get_cancel_keyboard(job_id: str):
    b = InlineKeyboardBuilder()
    b.button(text="❌ انصراف / لغو", callback_data=f"stop:{job_id}")
    return b.as_markup()


@dp.message(CommandStart())
async def start_handler(message: types.Message):
    await message.answer("🎬 ویدیوی خود را ارسال کنید (پشتیبانی تا سقف ۲ گیگابایت).")


@dp.message(F.video | F.document)
async def handle_video(message: types.Message):
    video = message.video or (
        message.document
        if message.document and message.document.mime_type and message.document.mime_type.startswith("video/")
        else None
    )

    if not video:
        return await message.answer("⚠️ لطفاً فقط فایل ویدیویی ارسال کنید.")

    if video.file_size > MAX_FILE_SIZE:
        return await message.answer(f"❌ حجم فایل از سقف ۲ گیگابایت بیشتر است.")

    default_cfg = {
        "mode": "video",
        "res": "720",
        "codec": "h264",
        "crf": "medium",
        "mute": False,
        "speed": "1.0"
    }

    res_info = f"\n📏 **ابعاد:** `{video.width}x{video.height}`" if hasattr(video, "width") and video.width else ""

    await message.reply(
        f"⚙️ **تنظیمات پردازش:**{res_info}\nتنظیمات را مشخص کرده و روی «شروع» بزنید:",
        reply_markup=build_config_keyboard(default_cfg)
    )


@dp.callback_query(F.data.startswith("cfg:"))
async def update_settings(callback: types.CallbackQuery):
    await callback.answer()
    cfg_data = callback.data[4:]
    cfg = decode_cfg(cfg_data)
    try:
        await callback.message.edit_reply_markup(reply_markup=build_config_keyboard(cfg))
    except TelegramBadRequest:
        pass


@dp.callback_query(F.data == "cancel_panel")
async def cancel_panel(callback: types.CallbackQuery):
    await callback.message.edit_text("❌ عملیات لغو شد.")


@dp.callback_query(F.data.startswith("stop:"))
async def stop_processing(callback: types.CallbackQuery):
    job_id = callback.data.split(":")[1]
    if job_id in ACTIVE_PROCESSES:
        ACTIVE_PROCESSES[job_id]["cancelled"] = True
        await callback.answer("عملیات لغو شد.")
        await callback.message.edit_text("🛑 پردازش توسط شما متوقف شد.")
    else:
        await callback.answer("پردازشی فعال نیست.", show_alert=True)


@dp.callback_query(F.data.startswith("run:"))
async def enqueue_task(callback: types.CallbackQuery):
    await callback.answer()
    cfg_data = callback.data[4:]
    cfg = decode_cfg(cfg_data)

    orig_msg = callback.message.reply_to_message
    if not orig_msg:
        return await callback.message.edit_text("❌ ویدیوی مرجع یافت نشد. مجدداً ویدیو را ارسال کنید.")

    video = orig_msg.video or (
        orig_msg.document
        if orig_msg.document and orig_msg.document.mime_type and orig_msg.document.mime_type.startswith("video/")
        else None
    )

    if not video:
        return await callback.message.edit_text("❌ ویدیویی یافت نشد.")

    job_id = f"{callback.message.chat.id}_{callback.message.message_id}"

    status_msg = await callback.message.edit_text(
        f"⏳ در صف انتظار سرور...\n👥 نوبت شما: **نفر {JOB_QUEUE.qsize() + 1}**",
        reply_markup=get_cancel_keyboard(job_id)
    )

    job_payload = {
        "job_id": job_id,
        "cfg": cfg,
        "msg_id": orig_msg.message_id,
        "file_size": video.file_size,
        "file_id": video.file_id,
        "chat_id": callback.message.chat.id,
        "user": callback.from_user,
        "status_msg": status_msg
    }

    ACTIVE_PROCESSES[job_id] = {"cancelled": False}
    await JOB_QUEUE.put(job_payload)


async def queue_worker():
    while True:
        job = await JOB_QUEUE.get()
        job_id = job["job_id"]

        if ACTIVE_PROCESSES.get(job_id, {}).get("cancelled"):
            JOB_QUEUE.task_done()
            continue

        try:
            await process_job(job)
        except Exception as e:
            logging.error(f"Error on job {job_id}: {e}", exc_info=True)
            try:
                await job["status_msg"].edit_text("⚠️ خطایی در اجرای پردازش رخ داد.")
            except Exception:
                pass
        finally:
            ACTIVE_PROCESSES.pop(job_id, None)
            JOB_QUEUE.task_done()


async def ui_updater_task(state: dict):
    last_text = ""
    while not state.get("done", False):
        try:
            percent = min(100.0, max(0.0, state.get("percent", 0.0)))
            bar = generate_progress_bar(percent)
            
            action = state.get("action", "")
            if action == "download":
                text = f"📥 در حال دریافت فایل:\n{bar}"
            elif action == "encode":
                text = f"⚙️ در حال پردازش در ورکر مستقل...\n⏳ لطفاً شکیبا باشید (بدون افت حافظه)"
            elif action == "upload_pyro":
                text = f"📤 در حال ارسال به تلگرام (موتور MTProto):\n{bar}"
            elif action == "upload_http":
                text = f"📤 در حال ارسال فایل (موتور پرسرعت)...\n{bar}"
            else:
                text = "⏳ لطفا صبر کنید..."

            if text != last_text:
                await state["status_msg"].edit_text(text, reply_markup=get_cancel_keyboard(state["job_id"]))
                last_text = text
                
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after)
            continue
        except (TelegramBadRequest, asyncio.CancelledError):
            pass
        except Exception:
            pass
        
        await asyncio.sleep(3.5)


def py_progress_callback(current, total, state):
    if total > 0:
        state["percent"] = (current / total) * 100.0


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

    ui_state = {
        "status_msg": status_msg,
        "job_id": job_id,
        "action": "download",
        "percent": 0.0,
        "done": False
    }
    
    ui_task = asyncio.create_task(ui_updater_task(ui_state))

    try:
        # مرحله ۱: دانلود فایل
        if initial_size < 19.5 * 1024 * 1024:
            ui_state["percent"] = 50.0
            file_info = await bot.get_file(job["file_id"])
            await bot.download_file(file_info.file_path, destination=input_path)
            ui_state["percent"] = 100.0
        else:
            target_pyro_msg = await pyro.get_messages(chat_id=job["chat_id"], message_ids=job["msg_id"])
            await target_pyro_msg.download(
                file_name=input_path,
                progress=py_progress_callback,
                progress_args=(ui_state,)
            )

        if ACTIVE_PROCESSES[job_id]["cancelled"]:
            return

        # مرحله ۲: ارسال به ورکر مستقل جهت پردازش بدون اشغال رم کلاینت ربات
        ui_state["action"] = "encode"
        ui_state["percent"] = 0.0

        async with aiohttp.ClientSession() as session:
            with open(input_path, "rb") as f:
                data = aiohttp.FormData()
                data.add_field("file", f, filename="video.mp4")
                data.add_field("mode", mode)
                data.add_field("res", cfg["res"])
                data.add_field("codec", cfg["codec"])
                data.add_field("crf", cfg["crf"])
                data.add_field("mute", "1" if cfg["mute"] else "0")
                data.add_field("speed", str(speed_factor))

                async with session.post(f"{WORKER_URL}/process", data=data, timeout=aiohttp.ClientTimeout(total=2400)) as resp:
                    if resp.status != 200:
                        err_text = await resp.text()
                        logging.error(f"Worker Error: {err_text}")
                        ui_state["done"] = True
                        ui_task.cancel()
                        return await status_msg.edit_text(f"❌ پردازش در سرور ورکر با خطا مواجه شد:\n`{err_text[:200]}`")
                    
                    with open(output_path, "wb") as out_f:
                        while True:
                            chunk = await resp.content.read(1024 * 1024)
                            if not chunk:
                                break
                            out_f.write(chunk)

        if ACTIVE_PROCESSES[job_id]["cancelled"]:
            return

        final_size = os.path.getsize(output_path)
        reduction = max(0, int(((initial_size - final_size) / initial_size) * 100))

        # مرحله ۳: ارسال فایل به کاربر
        if mode == "mp3":
            caption_text = f"✅ پردازش انجام شد\n\n📦 حجم اولیه: {initial_size / (1024*1024):.2f} MB\n📉 حجم نهایی: {final_size / (1024*1024):.2f} MB\n⚡ فشرده‌سازی: {reduction}% کاهش (فرمت MP3)"
        else:
            caption_text = f"✅ پردازش انجام شد\n\n📦 حجم اولیه: {initial_size / (1024*1024):.2f} MB\n📉 حجم نهایی: {final_size / (1024*1024):.2f} MB\n⚡ فشرده‌سازی: {reduction}% کاهش (سرعت {speed_factor}x)"

        if final_size < 49.5 * 1024 * 1024:
            ui_state["action"] = "upload_http"
            ui_state["percent"] = 75.0
            
            if mode == "mp3":
                await bot.send_audio(chat_id=job["chat_id"], audio=FSInputFile(output_path), caption=caption_text)
            elif mode == "gif":
                await bot.send_animation(chat_id=job["chat_id"], animation=FSInputFile(output_path), caption=caption_text)
            else:
                await bot.send_video(chat_id=job["chat_id"], video=FSInputFile(output_path), caption=caption_text, supports_streaming=True)
                
            ui_state["percent"] = 100.0
        else:
            ui_state["action"] = "upload_pyro"
            ui_state["percent"] = 0.0
            
            if mode == "mp3":
                await pyro.send_audio(chat_id=job["chat_id"], audio=output_path, caption=caption_text, progress=py_progress_callback, progress_args=(ui_state,))
            elif mode == "gif":
                await pyro.send_animation(chat_id=job["chat_id"], animation=output_path, caption=caption_text, unsave=True, progress=py_progress_callback, progress_args=(ui_state,))
            else:
                await pyro.send_video(chat_id=job["chat_id"], video=output_path, caption=caption_text, supports_streaming=True, progress=py_progress_callback, progress_args=(ui_state,))

        ui_state["done"] = True
        ui_task.cancel()
        await status_msg.delete()

        if ADMIN_ID and job["user"].id != ADMIN_ID:
            u = job["user"]
            u_name = f"@{u.username}" if u.username else "ندارد"
            admin_text = f"🔔 لاگ موفق\n\n👤 کاربر: {u.full_name} ({u_name}) | {u.id}\n🎯 نوع: {mode.upper()}\n⚡ تغییر: {initial_size / (1024*1024):.2f} MB ← {final_size / (1024*1024):.2f} MB"
            try:
                await bot.send_message(ADMIN_ID, admin_text)
            except Exception:
                pass

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


async def start_pyrogram_safely():
    while True:
        try:
            await pyro.start()
            logging.info("✅ موتور قدرتمند Pyrogram متصل شد.")
            break
        except Exception as e:
            if "FLOOD_WAIT" in str(e).upper():
                match = re.search(r'\d+', str(e))
                wait_time = int(match.group()) if match else 60
                logging.warning(f"⚠️ محدودیت لاگین Pyrogram: {wait_time} ثانیه انتظار...")
                await asyncio.sleep(wait_time + 1)
            else:
                logging.error(f"خطا در اتصال Pyrogram: {e}")
                await asyncio.sleep(5)


async def main():
    asyncio.create_task(start_pyrogram_safely())
    asyncio.create_task(queue_worker())
    
    logging.info("✅ ربات تلگرام با معماری ۲ سرویسی فعال شد.")
    try:
        await dp.start_polling(bot, drop_pending_updates=True)
    finally:
        try:
            await pyro.stop()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())