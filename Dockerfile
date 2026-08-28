FROM python:3.11-slim

WORKDIR /app

# Установка Tor и зависимостей
RUN apt-get update && apt-get install -y \
    tor \
    curl \
    ca-certificates \
    dnsmasq \
    && rm -rf /var/lib/apt/lists/*

# Копируем код проекта
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Скрипт запуска с Tor прокси
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -s http://127.0.0.1:9050 > /dev/null || exit 1

CMD ["/app/start.sh"]
