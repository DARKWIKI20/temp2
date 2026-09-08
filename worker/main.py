import os
import re
import uuid
import asyncio
import subprocess
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse
from starlette.background import BackgroundTasks

app = FastAPI(title="Video Processing Worker")
FFMPEG_BIN = "ffmpeg"

TEMP_DIR = "temp_processing"
os.makedirs(TEMP_DIR, exist_ok=True)

TASKS = {}


def cleanup_files(*paths):
    for p in paths:
        if p and os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass


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


async def run_ffmpeg_task(task_id: str, cmd: list, in_path: str, out_path: str, duration: float):
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
        )
        TASKS[task_id]["proc"] = proc
        time_us_pattern = re.compile(r"out_time_us=(\d+)")
        time_str_pattern = re.compile(r"out_time=(\d+):(\d+):(\d+(?:\.\d+)?)")

        while True:
            line = await proc.stderr.readline()
            if not line:
                break
            if TASKS[task_id].get("cancelled"):
                proc.kill()
                cleanup_files(in_path, out_path)
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

            if current_secs is not None and duration > 0:
                pct = (current_secs / duration) * 100.0
                TASKS[task_id]["percent"] = min(99.0, max(1.0, pct))

        await proc.wait()

        if TASKS[task_id].get("cancelled"):
            cleanup_files(in_path, out_path)
            TASKS.pop(task_id, None)
            return

        if proc.returncode == 0 and os.path.exists(out_path):
            TASKS[task_id]["status"] = "done"
            TASKS[task_id]["percent"] = 100.0
        else:
            TASKS[task_id]["status"] = "error"
            TASKS[task_id]["error"] = f"FFmpeg exited with code {proc.returncode}"
    except Exception as e:
        TASKS[task_id]["status"] = "error"
        TASKS[task_id]["error"] = str(e)


@app.post("/start")
async def start_processing(
    file: UploadFile = File(...),
    mode: str = Form("video"),
    res: str = Form("720"),
    codec: str = Form("h264"),
    crf: str = Form("medium"),
    mute: str = Form("0"),
    speed: str = Form("1.0")
):
    task_id = str(uuid.uuid4())
    in_path = os.path.join(TEMP_DIR, f"{task_id}_in.mp4")
    out_ext = "mp3" if mode == "mp3" else "mp4"
    out_path = os.path.join(TEMP_DIR, f"{task_id}_out.{out_ext}")

    with open(in_path, "wb") as f:
        while chunk := await file.read(1024 * 1024):
            f.write(chunk)

    is_mute = mute in ["1", "true", "True"]
    speed_factor = float(speed)
    duration = await get_video_duration(in_path)
    eff_duration = duration / speed_factor if speed_factor > 0 else duration

    cmd = [FFMPEG_BIN, "-y", "-i", in_path, "-threads", "2", "-max_muxing_queue_size", "1024"]

    if mode == "mp3":
        cmd += ["-vn", "-c:a", "libmp3lame", "-b:a", "192k", "-progress", "pipe:2", out_path]
    elif mode == "gif":
        vf = [f"setpts={1.0 / speed_factor}*PTS"] if speed_factor != 1.0 else []
        vf += ["fps=15", "scale=480:-2:flags=lanczos"]
        cmd += ["-an", "-c:v", "libx264", "-vf", ",".join(vf), "-crf", "28", "-preset", "veryfast", "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-progress", "pipe:2", out_path]
    else:
        crf_map = {"light": "23", "medium": "28", "heavy": "34"}
        v_codec = "libx265" if codec == "h265" else "libx264"
        scale = "scale=trunc(iw/2)*2:trunc(ih/2)*2" if res == "orig" else f"scale=-2:{res}:flags=lanczos"
        vf = [f"setpts={1.0 / speed_factor}*PTS"] if speed_factor != 1.0 else []
        vf.append(scale)
        cmd += ["-map", "0:v:0", "-c:v", v_codec, "-vf", ",".join(vf), "-crf", crf_map.get(crf, "28"), "-preset", "veryfast", "-pix_fmt", "yuv420p", "-movflags", "+faststart"]
        if is_mute:
            cmd += ["-an"]
        else:
            cmd += ["-map", "0:a:0?", "-c:a", "aac", "-b:a", "128k", "-ac", "2", "-strict", "experimental"]
            if speed_factor != 1.0:
                cmd += ["-filter:a", f"atempo={speed_factor}"]
        cmd += ["-progress", "pipe:2", out_path]

    TASKS[task_id] = {
        "status": "processing",
        "percent": 1.0,
        "in_path": in_path,
        "out_path": out_path,
        "proc": None,
        "cancelled": False
    }

    asyncio.create_task(run_ffmpeg_task(task_id, cmd, in_path, out_path, eff_duration))
    return {"task_id": task_id}


@app.get("/status/{task_id}")
async def get_status(task_id: str):
    if task_id not in TASKS:
        raise HTTPException(status_code=404, detail="Task not found")
    return {
        "status": TASKS[task_id]["status"],
        "percent": TASKS[task_id]["percent"],
        "error": TASKS[task_id].get("error")
    }


@app.get("/download/{task_id}")
async def download_result(task_id: str, bg: BackgroundTasks):
    if task_id not in TASKS or TASKS[task_id]["status"] != "done":
        raise HTTPException(status_code=400, detail="File not ready")
    out_path = TASKS[task_id]["out_path"]
    in_path = TASKS[task_id]["in_path"]
    bg.add_task(cleanup_files, in_path, out_path)
    bg.add_task(TASKS.pop, task_id, None)
    return FileResponse(out_path, media_type="application/octet-stream")


@app.post("/cancel/{task_id}")
async def cancel_task(task_id: str):
    if task_id in TASKS:
        TASKS[task_id]["cancelled"] = True
        proc = TASKS[task_id].get("proc")
        if proc:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
        cleanup_files(TASKS[task_id].get("in_path"), TASKS[task_id].get("out_path"))
        TASKS.pop(task_id, None)
    return {"status": "ok"}
