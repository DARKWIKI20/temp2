FROM python:3.11-slim

WORKDIR /app

# نصب FFmpeg رسمی لینوکس همراه با تمامی کدک‌ها
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .

CMD ["python", "bot.py"]
