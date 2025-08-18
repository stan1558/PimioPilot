# Base image
FROM python:3.12-slim

# --- Environment hygiene ---
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Asia/Taipei

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tzdata  && rm -rf /var/lib/apt/lists/*

# --- Workdir & Python deps ---
WORKDIR /app
COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

# --- App code ---
COPY . .

# Let docker-compose set the command (see docker-compose.yml)
# CMD ["python", "fetch_taiwan_stock_yfinance.py"]
