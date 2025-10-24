# Dockerfile for bot_i1play (paths aligned to current repo)
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Copy entire project
COPY . /app

# Install dependencies from project root
RUN python -m pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Optional: Cloud Run uses this port, though the bot does not listen
ENV PORT=8080

# Start via entrypoint to ensure health server starts before bot
CMD ["python", "entrypoint.py"]
