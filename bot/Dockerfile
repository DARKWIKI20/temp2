FROM python:3.11-slim

WORKDIR /app

# نصب وابستگی‌ها
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# کپی اسکریپت اصلی
COPY bot.py .

CMD ["python", "bot.py"]
