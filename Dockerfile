# Dockerfile for bot_i1play (paths aligned to current repo)
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Leverage Docker layer caching: install deps first
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    apt-get update && apt-get install -y --no-install-recommends \
      fonts-noto-cjk \
      fonts-noto-color-emoji \
      fonts-dejavu-core && \
    rm -rf /var/lib/apt/lists/*

# Copy project source
COPY . /app

# Optional: Cloud Run uses this port, though the bot does not listen
ENV PORT=8080

# Start via entrypoint to ensure health server starts before bot
CMD ["python", "entrypoint.py"]
