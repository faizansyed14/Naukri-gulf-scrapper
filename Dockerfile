FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    ca-certificates \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

ENV PYTHONUNBUFFERED=1 \
    SCRAPER_HEADLESS=1 \
    CHROME_BIN=/usr/bin/chromium

CMD ["sh", "-c", "gunicorn -w 2 -k gthread --threads 8 -b 0.0.0.0:${PORT:-10000} --access-logfile - --error-logfile - app:app"]

