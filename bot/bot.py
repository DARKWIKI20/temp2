import os
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
WORKER_URL = os.getenv("WORKER_URL", "http://honest-surprise.railway.internal:8000").rstrip("/")

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


@dp.message(CommandStart())
async def start_handler(message: types.Message):
    await message.answer("🎬 ویدیوی خود را بفرستید (پشتیبانی تا سقف ۲ گیگابایت).")


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
        
        # ۱. لغو بی‌درنگ تسک پایتون برای آزاد شدن صف
        task = RUNNING_TASKS.get(job_id)
        if task and not task.done():
            task.cancel()

        # ۲. ارسال فرمان کشتن FFmpeg به ورکر
        worker_task_id = ACTIVE_PROCESSES.get(job_id, {}).get("worker_task_id")
        if worker_task_id:
            async def notify_cancel():
                try:
                    async with aiohttp.ClientSession() as s:
                        await s.post(f"{WORKER_URL}/cancel/{worker_task_id}", timeout=3)
                except Exception:
                    pass
            asyncio.create_task(notify_cancel())

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

    ACTIVE_PROCESSES[job_id] = {"cancelled": False, "worker_task_id": None}
    await JOB_QUEUE.put({
        "job_id": job_id, "cfg": cfg, "msg_id": orig_msg.message_id,
        "file_size": video.file_size, "file_id": video.file_id,
        "chat_id": callback.message.chat.id, "user": callback.from_user, "status_msg": status_msg
    })


async def queue_worker():
    while True:
        job = await JOB_QUEUE.get()
        job_id = job["job_id"]

        # اگر کاربر قبل از شروع تسک، آن را لغو کرده باشد
        if ACTIVE_PROCESSES.get(job_id, {}).get("cancelled"):
            ACTIVE_PROCESSES.pop(job_id, None)
            JOB_QUEUE.task_done()
            continue

        # اجرای تسک به‌صورت کنترل‌پذیر
        task = asyncio.create_task(process_job(job))
        RUNNING_TASKS[job_id] = task

        try:
            await task
        except asyncio.CancelledError:
            logging.info(f"Task {job_id} was successfully aborted.")
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
                text = f"📥 در حال دریافت فایل از تلگرام:\n{bar}"
            elif act == "transfer":
                text = f"🔄 در حال انتقال به ورکر پردازش...\n{bar}"
            elif act == "encode":
                text = f"⚙️ در حال فشرده‌سازی و رندر:\n{bar}"
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
        # مرحله ۱: دانلود
        if initial_size < 19.5 * 1024 * 1024:
            ui_state["percent"] = 50.0
            file_info = await bot.get_file(job["file_id"])
            await bot.download_file(file_info.file_path, destination=input_path)
            ui_state["percent"] = 100.0
        else:
            msg = await pyro.get_messages(chat_id=job["chat_id"], message_ids=job["msg_id"])
            await msg.download(file_name=input_path, progress=pyro_progress, progress_args=(ui_state,))

        # مرحله ۲: انتقال به ورکر
        ui_state["action"] = "transfer"
        ui_state["percent"] = 15.0

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

                async with session.post(f"{WORKER_URL}/start", data=data, timeout=aiohttp.ClientTimeout(total=600)) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"ورکر تسک را نپذیرفت: {await resp.text()}")
                    res_json = await resp.json()
                    worker_task_id = res_json["task_id"]
                    ACTIVE_PROCESSES[job_id]["worker_task_id"] = worker_task_id

            # مرحله ۳: نظارت زنده بر رندر
            ui_state["action"] = "encode"
            ui_state["percent"] = 1.0

            while True:
                await asyncio.sleep(1.5)
                async with session.get(f"{WORKER_URL}/status/{worker_task_id}", timeout=10) as resp:
                    if resp.status == 200:
                        st = await resp.json()
                        ui_state["percent"] = st.get("percent", 1.0)
                        if st.get("status") == "done":
                            break
                        if st.get("status") == "error":
                            raise RuntimeError(f"خطای رندر در ورکر: {st.get('error')}")

            # دریافت خروجی آماده‌شده
            async with session.get(f"{WORKER_URL}/download/{worker_task_id}", timeout=aiohttp.ClientTimeout(total=1800)) as resp:
                with open(output_path, "wb") as out_f:
                    while chunk := await resp.content.read(1024 * 1024):
                        out_f.write(chunk)

        # مرحله ۴: ارسال به تلگرام
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
