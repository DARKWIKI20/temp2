import os
import shutil
import tempfile
import subprocess
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse
from starlette.background import BackgroundTasks
FFMPEG_BIN = "ffmpeg"

app = FastAPI(title="Video Processing Worker")
FFMPEG_BIN = imageio_ffmpeg.get_ffmpeg_exe()


def cleanup_files(*file_paths):
    for path in file_paths:
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except OSError:
                pass


@app.get("/")
async def health_check():
    return {"status": "ok", "service": "worker"}


@app.post("/process")
async def process_video(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    mode: str = Form("video"),
    res: str = Form("720"),
    codec: str = Form("h264"),
    crf: str = Form("medium"),
    mute: str = Form("0"),
    speed: str = Form("1.0")
):
    is_mute = mute in ["1", "true", "True"]
    speed_factor = float(speed)

    # ذخیره استریم فایل ورودی بدون مصرف رم اضافی
    with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as in_tmp:
        shutil.copyfileobj(file.file, in_tmp)
        in_path = in_tmp.name

    out_ext = ".mp3" if mode == "mp3" else ".mp4"
    out_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=out_ext)
    out_path = out_tmp.name
    out_tmp.close()

    try:
        cmd = [
            FFMPEG_BIN, "-y", "-i", in_path,
            "-threads", "2",
            "-max_muxing_queue_size", "1024"
        ]

        if mode == "mp3":
            cmd += ["-vn", "-c:a", "libmp3lame", "-b:a", "192k", out_path]

        elif mode == "gif":
            vf = []
            if speed_factor != 1.0:
                vf.append(f"setpts={1.0 / speed_factor}*PTS")
            vf += ["fps=15", "scale=480:-2:flags=lanczos"]
            cmd += [
                "-an", "-c:v", "libx264", "-vf", ",".join(vf),
                "-crf", "28", "-preset", "veryfast", "-pix_fmt", "yuv420p",
                "-movflags", "+faststart", out_path
            ]

        else:
            crf_map = {"light": "23", "medium": "28", "heavy": "34"}
            v_codec = "libx265" if codec == "h265" else "libx264"

            # محاسبه ابعاد زوج برای رفع خطای Exit Code 1
            if res == "orig":
                scale_filter = "scale=trunc(iw/2)*2:trunc(ih/2)*2"
            else:
                scale_filter = f"scale=-2:{res}:flags=lanczos"

            vf = []
            if speed_factor != 1.0:
                vf.append(f"setpts={1.0 / speed_factor}*PTS")
            vf.append(scale_filter)

            cmd += [
                "-map", "0:v:0", "-c:v", v_codec, "-vf", ",".join(vf),
                "-crf", crf_map.get(crf, "28"), "-preset", "veryfast",
                "-pix_fmt", "yuv420p", "-movflags", "+faststart"
            ]

            if is_mute:
                cmd += ["-an"]
            else:
                # استریو کردن اجباری با ۲ کانال برای جلوگیری از کرش سورس‌های چندکاناله
                cmd += [
                    "-map", "0:a:0?",
                    "-c:a", "aac", "-b:a", "128k", "-ac", "2", "-strict", "experimental"
                ]
                if speed_factor != 1.0:
                    cmd += ["-filter:a", f"atempo={speed_factor}"]

            cmd.append(out_path)

        # اجرای دستور در پروسه مجزا
        res_proc = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)

        if res_proc.returncode != 0:
            error_detail = res_proc.stderr[-400:] if res_proc.stderr else "Unknown error"
            cleanup_files(in_path, out_path)
            raise HTTPException(status_code=500, detail=f"FFmpeg Error (Code {res_proc.returncode}): {error_detail}")

        # حذف فایل‌های موقت پس از اتمام ارسال پاسخ HTTP
        background_tasks.add_task(cleanup_files, in_path, out_path)

        media_type = "audio/mpeg" if mode == "mp3" else "video/mp4"
        return FileResponse(out_path, media_type=media_type, filename=f"output{out_ext}")

    except HTTPException:
        raise
    except Exception as e:
        cleanup_files(in_path, out_path)
        raise HTTPException(status_code=500, detail=f"Server Exception: {str(e)}")
