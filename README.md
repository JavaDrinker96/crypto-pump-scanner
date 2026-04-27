<div align="center">

# Crypto Pump Scanner

### Real-time pump detection & auto-trading for Bybit

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Bybit](https://img.shields.io/badge/Exchange-Bybit-F7A600.svg)](https://www.bybit.com)
[![ccxt](https://img.shields.io/badge/Library-ccxt-orange.svg)](https://github.com/ccxt/ccxt)

[Website](https://deepalphabot.com) · [Discord](https://discord.gg/P4yX686m) · [Telegram](https://t.me/DeepAlphaVault)

</div>

---

## What is this?

A real-time pump detection system that monitors **all 500+ Bybit USDT perpetual pairs** every 3 seconds. When it detects a pump, it automatically opens a position with multi-target take profit.

```
500+ pairs scanned every 3s
        |
   Volume spike detected (5x+ normal)
        |
   Confirmation: price +3%, RSI 60-85, 3 green candles, buy ratio >65%
        |
   LONG opened with ATR-based stop loss
        |
   TP cascade: +5% close 40% | +10% close 30% + trailing | +20% close rest
```

## Features

- **Real-time scanning** — monitors every USDT perpetual pair on Bybit, every 3 seconds
- **Smart filters** — RSI, buy ratio, consecutive candles, minimum volume — filters out fakeouts
- **Multi-target TP** — takes profit at 3 levels (+5%, +10%, +20%) with trailing stop
- **New listing detection** — monitors Bybit announcements API for new perpetual listings
- **Dump shorting** — detects exhaustion (RSI > 80, volume decline) and shorts the top
- **Circuit breaker** — stops trading after -$50 daily loss
- **Telegram alerts** — real-time notifications for every signal and trade
- **Trade log** — saves every trade to `pump_trades.json` for verification
- **Stock token blacklist** — automatically skips non-crypto tokens (TSLA, GOOGL, etc.)
- **Standalone** — runs independently, no other dependencies

## Quick Start

### 1. Clone

```bash
git clone https://github.com/stefanoviana/crypto-pump-scanner.git
cd crypto-pump-scanner
pip install -r requirements.txt
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env with your Bybit API keys:
#   BYBIT_API_KEY=your_key
#   BYBIT_API_SECRET=your_secret
```

### 3. Run

```bash
python pump_scanner.py
```

That's it. The scanner starts monitoring 500+ pairs immediately.

### Docker

```bash
cp .env.example .env   # edit with your keys
docker compose up -d
```

## How Detection Works

The scanner uses a multi-layer confirmation system to avoid fakeouts:

| Layer | Check | Threshold |
|-------|-------|-----------|
| 1. Volume | Current vs 20-period EWMA | **5x** normal volume |
| 2. Price | Price change in scan window | **+3%** minimum |
| 3. Momentum | Consecutive green candles | **3+** candles |
| 4. RSI | Relative Strength Index | **60-85** range |
| 5. Buy Ratio | Buy volume / total volume | **>65%** buy-side |
| 6. Dollar Volume | Minimum trading activity | **$500K+** daily |

Only when **all 6 layers pass** does the scanner open a trade.

## Take Profit Strategy

The scanner uses a **cascade TP** system that locks profits progressively:

```
Entry: $0.0064 (XCN pump detected)
  |
  +5%  → TP1: Close 40% of position ($0.0067)     ← Lock early profit
  |
  +10% → TP2: Close 30%, activate trailing stop    ← Let it run
  |
  +20% → TP3: Close remaining 30% ($0.0077)        ← Full exit

  Trailing stop: 3% from highest point after TP2
  Hard SL: 1.5x ATR below entry
```

## Configuration

See [`.env.example`](.env.example) for all parameters. Key settings:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `PUMP_VOL_SPIKE_MULT` | 5.0 | Volume must be 5x normal |
| `PUMP_PRICE_SPIKE_PCT` | 0.03 | Price must move +3% |
| `PUMP_LEVERAGE` | 5 | Leverage for pump trades |
| `PUMP_RISK_BUDGET_PCT` | 0.05 | 5% of equity per trade |
| `PUMP_MAX_POSITIONS` | 2 | Maximum simultaneous trades |
| `PUMP_TP1_PCT` | 0.05 | +5% first take profit |
| `PUMP_TP2_PCT` | 0.10 | +10% second take profit |
| `PUMP_TP3_PCT` | 0.20 | +20% final take profit |

## Telegram Alerts

Get real-time notifications for every pump detected and every trade:

```
PUMP LONG OPENED: XCN
Entry: $0.0064 | Qty: 70460
SL: $0.0062 | TP1: $0.0067 | TP2: $0.0070 | TP3: $0.0077
Vol ratio: 5.8x | RSI: 64 | Conf: 69%
```

Add to your `.env`:
```
TELEGRAM_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

## Use with DeepAlpha AI Bot

This scanner is part of the [DeepAlpha](https://github.com/stefanoviana/deepalpha) ecosystem. You can run it standalone or alongside the AI trading bot:

```python
from pump_scanner import create_pump_scanner_from_config

scanner = create_pump_scanner_from_config()
scanner.start()  # runs in background thread
```

The AI bot handles trend-following with 70.9% accuracy, while the pump scanner catches momentum spikes. Different strategies, same account.

**[Try DeepAlpha Cloud](https://deepalphabot.com)** — AI trades for you, 7-day free trial, no credit card.

## Risk Warning

**Trading involves significant risk of loss.** This software is provided as-is. Pump trades are inherently risky — pumps can reverse in seconds. The circuit breaker (-$50/day) limits daily losses. Only trade with money you can afford to lose.

## License

MIT License — see [LICENSE](LICENSE)

## Links

- [DeepAlpha AI Bot](https://github.com/stefanoviana/deepalpha) — AI crypto trading with 70.9% accuracy
- [DeepAlpha Cloud](https://deepalphabot.com) — Cloud-hosted AI trading, free trial
- [Discord](https://discord.gg/P4yX686m) — Community & support
- [Telegram](https://t.me/DeepAlphaVault) — Live trade signals
