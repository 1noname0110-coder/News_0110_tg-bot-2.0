FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Важно: запуск через модуль, чтобы пакет app гарантированно импортировался.
CMD ["python", "-m", "app.main"]
