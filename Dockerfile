FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    tor \
    curl \
    ca-certificates \
    procps \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -s http://127.0.0.1:9050 > /dev/null || exit 1

CMD ["/app/start.sh"]