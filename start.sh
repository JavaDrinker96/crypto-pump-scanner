#!/bin/bash

echo "🔐 Запуск Tor прокси..."

# Запускаем Tor в фоне
tor --hush &

# Ждем инициализации Tor (5 секунд)
sleep 5

echo "✅ Tor прокси запущен на SOCKS5 127.0.0.1:9050"
echo "▶️  Запуск pump_scanner..."

# Запускаем приложение
python pump_scanner.py