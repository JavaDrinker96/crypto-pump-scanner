#!/bin/bash

# Запуск Tor в фоне
echo "🔐 Запуск Tor прокси..."
tor &
TOR_PID=$!

# Ждем инициализации Tor
sleep 3

# Проверяем, что Tor запущен
if ! ps -p $TOR_PID > /dev/null; then
    echo "❌ Tor не запустился"
    exit 1
fi

echo "✅ Tor прокси запущен (SOCKS5 127.0.0.1:9050)"

# Запускаем приложение
python pump_scanner.py &
APP_PID=$!

# Ждем завершения приложения
wait $APP_PID
