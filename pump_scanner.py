"""
DeepAlpha Pump Scanner - SIMPLE VERSION
========================================

Упрощённая версия:
- Один TP (not multiple)
- Один SL (not multiple)
- TP/SL встроены в ордер (не отдельные условные ордера)
"""

import json
import logging
import os
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import requests


logger = logging.getLogger("pump_scanner")


# ============================================================================
# CONFIG
# ============================================================================

PUMP_MODE = os.getenv("PUMP_MODE", "alerts").lower().strip()
TRADING_ENABLED = os.getenv("TRADING_ENABLED", "false").lower() == "true"

BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BYBIT_TESTNET = os.getenv("BYBIT_TESTNET", "false").lower() == "true"

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Scanner
SCAN_INTERVAL_SEC = int(os.getenv("PUMP_SCAN_INTERVAL", "10"))
VOLUME_SPIKE_MULT = float(os.getenv("PUMP_VOL_SPIKE_MULT", "5.0"))
PRICE_SPIKE_PCT = float(os.getenv("PUMP_PRICE_SPIKE_PCT", "0.03"))
DUMP_PRICE_SPIKE_PCT = float(os.getenv("PUMP_DUMP_PRICE_SPIKE_PCT", "0.03"))

PRICE_WINDOW_CANDLES = int(os.getenv("PUMP_PRICE_WINDOW", "5"))
MIN_DOLLAR_VOLUME = float(os.getenv("PUMP_MIN_DOLLAR_VOL", "500000"))

# Pump filters
CONFIRM_CANDLES = int(os.getenv("PUMP_CONFIRM_CANDLES", "3"))
MIN_RSI_ENTRY = float(os.getenv("PUMP_MIN_RSI_ENTRY", "60"))
MAX_RSI_ENTRY = float(os.getenv("PUMP_MAX_RSI_ENTRY", "85"))
MIN_BUY_RATIO = float(os.getenv("PUMP_MIN_BUY_RATIO", "0.65"))

# Dump filters
DUMP_MAX_RSI = float(os.getenv("PUMP_DUMP_MAX_RSI", "45"))
DUMP_MIN_SELL_RATIO = float(os.getenv("PUMP_DUMP_MIN_SELL_RATIO", "0.65"))

# Trading
PUMP_LEVERAGE = int(os.getenv("PUMP_LEVERAGE", "5"))
PUMP_RISK_BUDGET_PCT = float(os.getenv("PUMP_RISK_BUDGET_PCT", "0.05"))
PUMP_MAX_POSITIONS = int(os.getenv("PUMP_MAX_POSITIONS", "2"))

# ⭐ ОДИН TP И ОДИН SL
PUMP_TP_PCT = float(os.getenv("PUMP_TP_PCT", "0.05"))
PUMP_SL_ATR_MULT = float(os.getenv("PUMP_SL_ATR_MULT", "1.5"))

SHORT_SL_ATR_MULT = float(os.getenv("PUMP_SHORT_SL_ATR", "2.0"))
SHORT_TP_PCT = float(os.getenv("PUMP_SHORT_TP_PCT", "0.05"))

MIN_NOTIONAL = float(os.getenv("PUMP_MIN_NOTIONAL", "5"))

# Liquidation safety
LIQ_SL_MIN_DISTANCE_PCT = float(os.getenv("PUMP_LIQ_SL_MIN_DISTANCE_PCT", "0.50"))
MAINTENANCE_MARGIN_RATE = float(os.getenv("PUMP_MAINTENANCE_MARGIN_RATE", "0.005"))
LIQ_ESTIMATE_BUFFER_PCT = float(os.getenv("PUMP_LIQ_ESTIMATE_BUFFER_PCT", "0.25"))
POSITION_SYNC_INTERVAL_SEC = float(os.getenv("PUMP_POSITION_SYNC_INTERVAL", "5"))

PUMP_COOLDOWN_SEC = int(os.getenv("PUMP_COOLDOWN_SEC", "1800"))
LISTING_CHECK_INTERVAL = int(os.getenv("PUMP_LISTING_CHECK", "60"))
TEST_ALERT = os.getenv("PUMP_TEST_ALERT", "true").lower() == "true"


# ============================================================================
# DATA
# ============================================================================

@dataclass
class PumpSignal:
    coin: str
    signal_type: str
    detected_at: float
    price_at_detection: float
    volume_ratio: float
    rsi: float
    atr: float
    confidence: float
    metadata: dict = field(default_factory=dict)


@dataclass
class PumpPosition:
    coin: str
    side: str
    entry_price: float
    quantity: float
    stop_loss: float
    take_profit: float
    opened_at: float = 0.0
    liquidation_price: float = 0.0
    last_sync_at: float = 0.0
    closed: bool = False


# ============================================================================
# SCANNER
# ============================================================================

class PumpScanner:

    def __init__(self, ccxt_client, telegram_fn=None):
        self.client = ccxt_client
        self.telegram_fn = telegram_fn

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

        self._all_symbols = []
        self.price_history = defaultdict(list)
        self.volume_baselines = defaultdict(list)

        self.cooldowns = {}
        self.pump_positions = {}

        self.known_listings = set()
        self._markets_last_loaded = 0
        self._last_signal_time = {}

        self._blacklist = {
            "TSLA", "TSM", "INTC", "HOOD", "CHIP", "OPG",
            "AAPL", "AMZN", "GOOG", "GOOGL", "MSFT", "NVDA",
            "META", "NFLX", "AMD", "COIN", "MSTR", "PLTR",
            "UBER", "SQ", "PYPL", "SHOP", "SNOW", "CRWD",
            "NET", "DDOG", "ZS", "BABA", "DIS", "BA", "JPM",
            "V", "MA", "WMT", "PFE", "KO", "PEP", "COST",
            "CSCO", "ORCL", "CRM", "ABNB", "SNAP", "PINS", "ROKU",
        }

    # ========================================================================
    # LIFECYCLE
    # ========================================================================

    def start(self):
        if self._running:
            logger.warning("PumpScanner already running")
            return

        self._load_all_symbols()

        if not self._all_symbols:
            raise RuntimeError("No Bybit USDT perpetual symbols found")

        self._running = True
        self._thread = threading.Thread(
            target=self._main_loop,
            daemon=True,
            name="PumpScanner",
        )
        self._thread.start()

        logger.info(
            "PumpScanner started | mode=%s | symbols=%d | testnet=%s",
            PUMP_MODE,
            len(self._all_symbols),
            BYBIT_TESTNET,
        )

        self._alert(
            "🟢 <b>DeepAlpha Pump Scanner ONLINE</b>\n\n"
            f"Mode: <b>{PUMP_MODE.upper()}</b>\n"
            f"Pairs: <b>{len(self._all_symbols)}</b>\n"
            f"Trading: <b>{'ON' if self._trading_allowed() else 'OFF'}</b>\n"
            f"Testnet: <b>{'YES' if BYBIT_TESTNET else 'NO'}</b>\n"
            f"Style: <b>1 TP + 1 SL per position</b>"
        )

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("PumpScanner stopped")

    # ========================================================================
    # MAIN LOOP
    # ========================================================================

    def _main_loop(self):
        listing_check_last = 0

        while self._running:
            try:
                loop_start = time.time()

                if time.time() - listing_check_last >= LISTING_CHECK_INTERVAL:
                    self._check_new_symbols()
                    listing_check_last = time.time()

                tickers = self._fetch_all_tickers()

                if not tickers:
                    time.sleep(SCAN_INTERVAL_SEC)
                    continue

                signals = self._scan_for_signals(tickers)

                for signal in signals:
                    self._process_signal(signal)

                if self._trading_allowed():
                    self._manage_positions(tickers)

                elapsed = time.time() - loop_start
                time.sleep(max(0.2, SCAN_INTERVAL_SEC - elapsed))

            except Exception as exc:
                logger.exception("PumpScanner main loop error: %s", exc)
                time.sleep(5)

    # ========================================================================
    # MARKET DATA
    # ========================================================================

    def _load_all_symbols(self):
        try:
            markets = self.client.markets or self.client.load_markets()
        except Exception as exc:
            logger.error("Failed to load Bybit markets: %s", exc)
            return

        self._all_symbols = []

        for symbol, info in markets.items():
            if (
                info.get("linear")
                and info.get("active")
                and info.get("quote") == "USDT"
                and info.get("type") == "swap"
            ):
                self._all_symbols.append(symbol)

        logger.info("Loaded %d USDT perpetuals", len(self._all_symbols))

    def _fetch_all_tickers(self):
        try:
            return self.client.fetch_tickers()
        except Exception as exc:
            logger.error("Failed to fetch tickers: %s", exc)
            return {}

    def _fetch_ohlcv(self, symbol, limit=30):
        try:
            return self.client.fetch_ohlcv(symbol, timeframe="1m", limit=limit)
        except Exception as exc:
            logger.debug("OHLCV failed for %s: %s", symbol, exc)
            return []

    # ========================================================================
    # SIGNAL DETECTION
    # ========================================================================

    def _scan_for_signals(self, tickers):
        signals = []

        for symbol, ticker in tickers.items():

            if symbol not in self._all_symbols:
                continue

            coin = symbol.split("/")[0]

            if coin in self._blacklist:
                continue

            if self._in_cooldown(coin):
                continue

            try:
                price = float(ticker.get("last") or 0)
                quote_volume = float(ticker.get("quoteVolume") or 0)

                if price <= 0 or quote_volume < MIN_DOLLAR_VOLUME:
                    continue

                self.price_history[coin].append(price)

                if len(self.price_history[coin]) > 100:
                    self.price_history[coin] = self.price_history[coin][-100:]

                prices = self.price_history[coin]

                if len(prices) < PRICE_WINDOW_CANDLES:
                    continue

                candles = self._fetch_ohlcv(symbol, 30)

                if len(candles) < 20:
                    continue

                volumes = [float(c[5]) for c in candles]
                opens = [float(c[1]) for c in candles]
                highs = [float(c[2]) for c in candles]
                lows = [float(c[3]) for c in candles]
                closes = [float(c[4]) for c in candles]

                last_volume = volumes[-1]
                baseline_volume = np.mean(volumes[:-1])

                if baseline_volume <= 0:
                    continue

                volume_ratio = last_volume / baseline_volume

                price_change = (
                    prices[-1] - prices[-PRICE_WINDOW_CANDLES]
                ) / prices[-PRICE_WINDOW_CANDLES]

                rsi = self._calc_rsi(closes)
                atr = self._calc_atr(highs, lows, closes)

                self.volume_baselines[coin] = volumes

                # PUMP
                if (
                    volume_ratio >= VOLUME_SPIKE_MULT
                    and price_change >= PRICE_SPIKE_PCT
                ):
                    signal = self._validate_pump(
                        coin=coin,
                        symbol=symbol,
                        price=price,
                        volume_ratio=volume_ratio,
                        price_change=price_change,
                        candles=candles,
                    )

                    if signal:
                        signals.append(signal)
                        continue

                # DUMP
                if (
                    volume_ratio >= VOLUME_SPIKE_MULT * 0.5
                    and price_change <= -DUMP_PRICE_SPIKE_PCT
                ):
                    signal = self._validate_dump(
                        coin=coin,
                        symbol=symbol,
                        price=price,
                        volume_ratio=volume_ratio,
                        price_change=price_change,
                        candles=candles,
                    )

                    if signal:
                        signals.append(signal)

            except Exception as exc:
                logger.debug("Scan error %s: %s", symbol, exc)

        return signals

    def _validate_pump(
        self,
        coin,
        symbol,
        price,
        volume_ratio,
        price_change,
        candles,
    ):
        opens = [float(c[1]) for c in candles]
        highs = [float(c[2]) for c in candles]
        lows = [float(c[3]) for c in candles]
        closes = [float(c[4]) for c in candles]
        volumes = [float(c[5]) for c in candles]

        green_count = sum(
            1
            for i in range(-CONFIRM_CANDLES, 0)
            if closes[i] > opens[i]
        )

        if green_count < CONFIRM_CANDLES:
            return None

        rsi = self._calc_rsi(closes)

        if rsi < MIN_RSI_ENTRY or rsi > MAX_RSI_ENTRY:
            return None

        green_volume = sum(
            volumes[i]
            for i in range(-5, 0)
            if closes[i] > opens[i]
        )

        total_volume = sum(volumes[-5:])
        buy_ratio = green_volume / max(total_volume, 1e-9)

        if buy_ratio < MIN_BUY_RATIO:
            return None

        atr = self._calc_atr(highs, lows, closes)
        confidence = self._pump_confidence(
            volume_ratio, price_change, rsi, buy_ratio
        )

        if confidence < 0.5:
            return None

        return PumpSignal(
            coin=coin,
            signal_type="pump",
            detected_at=time.time(),
            price_at_detection=price,
            volume_ratio=volume_ratio,
            rsi=rsi,
            atr=atr,
            confidence=confidence,
            metadata={
                "price_change": price_change,
                "buy_ratio": buy_ratio,
                "green_candles": green_count,
            },
        )

    def _validate_dump(
        self,
        coin,
        symbol,
        price,
        volume_ratio,
        price_change,
        candles,
    ):
        opens = [float(c[1]) for c in candles]
        highs = [float(c[2]) for c in candles]
        lows = [float(c[3]) for c in candles]
        closes = [float(c[4]) for c in candles]
        volumes = [float(c[5]) for c in candles]

        rsi = self._calc_rsi(closes)

        if rsi > DUMP_MAX_RSI:
            return None

        red_volume = sum(
            volumes[i]
            for i in range(-5, 0)
            if closes[i] < opens[i]
        )

        total_volume = sum(volumes[-5:])
        sell_ratio = red_volume / max(total_volume, 1e-9)

        if sell_ratio < DUMP_MIN_SELL_RATIO:
            return None

        red_count = sum(
            1
            for i in range(-CONFIRM_CANDLES, 0)
            if closes[i] < opens[i]
        )

        if red_count < CONFIRM_CANDLES:
            return None

        atr = self._calc_atr(highs, lows, closes)

        volume_score = min(volume_ratio / (VOLUME_SPIKE_MULT * 2), 1.0)
        price_score = min(
            abs(price_change) / (DUMP_PRICE_SPIKE_PCT * 3),
            1.0,
        )
        sell_score = min(sell_ratio / 0.8, 1.0)
        rsi_score = min(max((50 - rsi) / 30, 0), 1.0)

        confidence = (
            volume_score * 0.30
            + price_score * 0.30
            + sell_score * 0.25
            + rsi_score * 0.15
        )

        if confidence < 0.5:
            return None

        return PumpSignal(
            coin=coin,
            signal_type="dump",
            detected_at=time.time(),
            price_at_detection=price,
            volume_ratio=volume_ratio,
            rsi=rsi,
            atr=atr,
            confidence=confidence,
            metadata={
                "price_change": price_change,
                "sell_ratio": sell_ratio,
                "red_candles": red_count,
            },
        )

    # ========================================================================
    # SIGNAL PROCESSING
    # ========================================================================

    def _process_signal(self, signal):
        coin = signal.coin

        last_time = self._last_signal_time.get((coin, signal.signal_type), 0)
        if time.time() - last_time < PUMP_COOLDOWN_SEC:
            return

        self._last_signal_time[(coin, signal.signal_type)] = time.time()

        logger.info(
            "%s detected: %s | price=%.8f | volume=%.1fx | RSI=%.1f | confidence=%.0f%%",
            signal.signal_type.upper(),
            coin,
            signal.price_at_detection,
            signal.volume_ratio,
            signal.rsi,
            signal.confidence * 100,
        )

        self._send_signal_alert(signal)

        if not self._trading_allowed():
            logger.info(
                "%s %s: trade skipped — trading is disabled",
                signal.signal_type.upper(),
                coin,
            )
            return

        success, reason, details = self._execute_signal(signal)

        if success:
            return

        self._send_trade_skipped_alert(signal, reason, details)

    def _send_signal_alert(self, signal):
        coin = signal.coin

        if signal.signal_type == "pump":
            emoji = "🚀"
            title = "PUMP DETECTED"
            direction = "UP"
        else:
            emoji = "🔻"
            title = "DUMP DETECTED"
            direction = "DOWN"

        price_change = signal.metadata.get("price_change", 0)
        buy_ratio = signal.metadata.get("buy_ratio")
        sell_ratio = signal.metadata.get("sell_ratio")

        lines = [
            f"{emoji} <b>{title}</b>",
            "",
            f"🪙 <b>{coin}</b>",
            f"💰 Price: <code>{self._format_price(signal.price_at_detection)}</code>",
            f"📈 Move: <b>{price_change:+.2%}</b>",
            f"📊 Volume: <b>{signal.volume_ratio:.1f}x</b>",
            f"📉 RSI: <b>{signal.rsi:.1f}</b>",
            f"🎯 Confidence: <b>{signal.confidence:.0%}</b>",
        ]

        if buy_ratio is not None:
            lines.append(f"🟢 Buy ratio: <b>{buy_ratio:.0%}</b>")

        if sell_ratio is not None:
            lines.append(f"🔴 Sell ratio: <b>{sell_ratio:.0%}</b>")

        lines.extend([
            "",
            f"Direction: <b>{direction}</b>",
            f"Mode: <b>{PUMP_MODE.upper()}</b>",
        ])

        if not self._trading_allowed():
            lines.append("⚪ Trading: <b>OFF</b>")

        self._alert("\n".join(lines))

    def _send_trade_skipped_alert(self, signal, reason, details=None):
        details = details or {}
        emoji = "🚫"
        direction = "LONG" if signal.signal_type == "pump" else "SHORT"

        lines = [
            f"{emoji} <b>{signal.signal_type.upper()} — TRADE SKIPPED</b>",
            "",
            f"🪙 <b>{signal.coin}</b>",
            f"Direction: <b>{direction}</b>",
            f"Price: <code>{self._format_price(signal.price_at_detection)}</code>",
            "",
            f"❌ <b>Reason:</b> {reason}",
        ]

        for label, value in details.items():
            if isinstance(value, float):
                if "pct" in label.lower():
                    rendered = f"{value:.3f}%"
                else:
                    rendered = self._format_price(value)
            else:
                rendered = str(value)
            lines.append(f"{label}: <code>{rendered}</code>")

        self._alert("\n".join(lines))
        logger.warning(
            "%s %s TRADE SKIPPED: %s | %s",
            signal.signal_type.upper(),
            signal.coin,
            reason,
            details,
        )

    # ============================================================================
    # TRADING - SIMPLIFIED
    # ============================================================================

    def _trading_allowed(self):
        return (
            PUMP_MODE == "trading"
            and TRADING_ENABLED
            and bool(BYBIT_API_KEY)
            and bool(BYBIT_API_SECRET)
        )

    def _execute_signal(self, signal):
        with self._lock:
            if len(self.pump_positions) >= PUMP_MAX_POSITIONS:
                return False, "maximum number of open positions reached", {
                    "Open positions": len(self.pump_positions),
                    "Maximum": PUMP_MAX_POSITIONS,
                }

        coin = signal.coin
        symbol = f"{coin}/USDT:USDT"

        try:
            markets = self.client.markets or {}
            market = markets.get(symbol)

            if not market:
                self.client.load_markets(True)
                market = self.client.markets.get(symbol)

            if not market or not market.get("active"):
                return False, "symbol is not tradable on Bybit", {"Symbol": symbol}

            equity = self._get_equity()
            if equity <= 0:
                return False, "USDT equity is zero or unavailable", {"Equity": equity}

            if signal.signal_type == "pump":
                return self._open_long(signal, equity)

            return self._open_short(signal, equity)

        except Exception as exc:
            logger.exception("Trade execution failed for %s: %s", coin, exc)
            return False, f"trade execution error: {exc}", {}

    def _prepare_trade(self, signal, equity, side):
        symbol = f"{signal.coin}/USDT:USDT"
        price = signal.price_at_detection

        if side == "long":
            risk_fraction = PUMP_RISK_BUDGET_PCT * signal.confidence
        else:
            risk_fraction = PUMP_RISK_BUDGET_PCT * signal.confidence * 0.7

        notional = equity * risk_fraction * PUMP_LEVERAGE
        quantity = self._round_qty(symbol, notional / price)

        if quantity <= 0:
            return None, "calculated quantity is below minimum order size", {
                "Calculated notional": notional,
                "Equity": equity,
                "Minimum notional": MIN_NOTIONAL,
            }

        actual_notional = quantity * price

        market = self.client.market(symbol)
        market_cost_min = (
            market.get("limits", {})
            .get("cost", {})
            .get("min", 0)
            or 0
        )
        required_notional = max(MIN_NOTIONAL, float(market_cost_min or 0))

        if actual_notional < required_notional:
            return None, "deposit/position size is too small", {
                "Notional": actual_notional,
                "Minimum notional": required_notional,
                "Quantity": quantity,
            }

        try:
            self.client.set_leverage(PUMP_LEVERAGE, symbol)
        except Exception as exc:
            logger.warning("set_leverage failed for %s: %s", symbol, exc)

        return {
            "symbol": symbol,
            "quantity": quantity,
            "notional": actual_notional,
            "price": price,
        }, None, None

    def _maintenance_margin_rate(self, symbol, notional):
        rate = MAINTENANCE_MARGIN_RATE

        try:
            tiers = self.client.fetch_market_leverage_tiers(symbol)
            if isinstance(tiers, list):
                for tier in tiers:
                    max_notional = tier.get("maxNotional")
                    min_notional = tier.get("minNotional", 0) or 0
                    mmr = (
                        tier.get("maintenanceMarginRate")
                        or tier.get("maintenanceMargin")
                    )
                    if mmr is None:
                        continue
                    if max_notional is None or (
                        min_notional <= notional <= max_notional
                    ):
                        rate = float(mmr)
                        break
        except Exception as exc:
            logger.debug("Could not fetch leverage tiers for %s: %s", symbol, exc)

        return max(rate, 0.0)

    def _estimate_liquidation_price(self, entry_price, side, leverage, mmr):
        effective_mmr = mmr + (LIQ_ESTIMATE_BUFFER_PCT / 100.0)

        if side == "long":
            return entry_price * (1.0 - (1.0 / leverage) + effective_mmr)

        return entry_price * (1.0 + (1.0 / leverage) - effective_mmr)

    def _liq_sl_distance_pct(self, stop_loss, liquidation_price, side):
        if side == "long":
            if liquidation_price >= stop_loss:
                return -abs(
                    (liquidation_price - stop_loss)
                    / max(stop_loss, 1e-12)
                    * 100
                )
            return (
                (stop_loss - liquidation_price)
                / max(stop_loss, 1e-12)
                * 100
            )

        if liquidation_price <= stop_loss:
            return -abs(
                (stop_loss - liquidation_price)
                / max(stop_loss, 1e-12)
                * 100
            )

        return (
            (liquidation_price - stop_loss)
            / max(stop_loss, 1e-12)
            * 100
        )

    def _check_liquidation_safety(
        self,
        symbol,
        entry_price,
        stop_loss,
        side,
        notional,
    ):
        mmr = self._maintenance_margin_rate(symbol, notional)
        estimated_liq = self._estimate_liquidation_price(
            entry_price,
            side,
            PUMP_LEVERAGE,
            mmr,
        )
        distance = self._liq_sl_distance_pct(
            stop_loss,
            estimated_liq,
            side,
        )

        if distance < LIQ_SL_MIN_DISTANCE_PCT:
            return False, {
                "Entry": entry_price,
                "SL": stop_loss,
                "Estimated liquidation": estimated_liq,
                "Liq→SL distance": distance,
                "Required distance": LIQ_SL_MIN_DISTANCE_PCT,
                "MMR used": mmr,
            }

        return True, {
            "Entry": entry_price,
            "SL": stop_loss,
            "Estimated liquidation": estimated_liq,
            "Liq→SL distance": distance,
            "MMR used": mmr,
        }

    def _fetch_live_position(self, symbol, expected_side=None):
        try:
            positions = self.client.fetch_positions([symbol])
        except Exception as exc:
            logger.warning("fetch_positions failed for %s: %s", symbol, exc)
            return None

        for pos in positions or []:
            contracts = pos.get("contracts")
            try:
                contracts = abs(float(contracts or 0))
            except (TypeError, ValueError):
                contracts = 0

            if contracts <= 0:
                continue

            side = str(pos.get("side") or "").lower()
            if expected_side and side and side != expected_side:
                continue

            entry = float(pos.get("entryPrice") or 0)
            liq = float(pos.get("liquidationPrice") or 0)

            if entry > 0:
                return {
                    "entry_price": entry,
                    "liquidation_price": liq,
                    "quantity": contracts,
                    "side": side,
                    "raw": pos,
                }

        return None

    def _open_long(self, signal, equity):
        """
        ⭐ ПРОСТАЯ ВЕРСИЯ: Один ордер с встроённым TP и SL
        """
        coin = signal.coin
        symbol = f"{coin}/USDT:USDT"

        atr = signal.atr or signal.price_at_detection * 0.02
        estimated_entry = signal.price_at_detection
        stop_loss = estimated_entry - atr * PUMP_SL_ATR_MULT
        take_profit = estimated_entry * (1 + PUMP_TP_PCT)

        trade, reason, details = self._prepare_trade(signal, equity, "long")
        if not trade:
            return False, reason, details

        safe, liq_details = self._check_liquidation_safety(
            symbol,
            estimated_entry,
            stop_loss,
            "long",
            trade["notional"],
        )
        if not safe:
            return (
                False,
                "estimated liquidation price is too close to SL",
                liq_details,
            )

        try:
            # ⭐ Создать позицию с встроённым TP и SL
            order = self.client.create_market_order(
                symbol,
                "buy",
                trade["quantity"],
                params={
                    "takeProfit": {
                        "triggerPrice": take_profit,
                    },
                    "stopLoss": {
                        "triggerPrice": stop_loss,
                    },
                }
            )

            fill_price = float(
                order.get("average")
                or order.get("price")
                or estimated_entry
            )

            live = self._fetch_live_position(symbol, "long")
            if live:
                fill_price = live["entry_price"]
                actual_liq = live["liquidation_price"]
            else:
                actual_liq = 0.0

            # Пересчитываем TP/SL на основе реальной цены входа
            atr = signal.atr or fill_price * 0.02
            stop_loss = fill_price - atr * PUMP_SL_ATR_MULT
            take_profit = fill_price * (1 + PUMP_TP_PCT)

            if actual_liq > 0:
                actual_distance = self._liq_sl_distance_pct(
                    stop_loss,
                    actual_liq,
                    "long",
                )
                if actual_distance < LIQ_SL_MIN_DISTANCE_PCT:
                    logger.error(
                        "Actual liquidation is too close after long fill: %s distance=%.3f%%",
                        coin,
                        actual_distance,
                    )
                    self._emergency_close_after_failed_protection(
                        symbol,
                        "sell",
                        trade["quantity"],
                    )
                    return (
                        False,
                        "liquidation price too close to SL; position closed",
                        {
                            "Entry": fill_price,
                            "SL": stop_loss,
                            "Liquidation": actual_liq,
                            "Distance": actual_distance,
                        },
                    )

            live_qty = live["quantity"] if live else trade["quantity"]

            position = PumpPosition(
                coin=coin,
                side="long",
                entry_price=fill_price,
                quantity=live_qty,
                stop_loss=stop_loss,
                take_profit=take_profit,
                liquidation_price=actual_liq,
                opened_at=time.time(),
                last_sync_at=time.time(),
            )

            with self._lock:
                self.pump_positions[coin] = position

            logger.info(
                "LONG OPENED %s | entry=%.8f qty=%s liq=%.8f SL=%.8f TP=%.8f",
                coin,
                fill_price,
                live_qty,
                actual_liq,
                stop_loss,
                take_profit,
            )

            self._alert(
                f"🟢 <b>LONG OPENED</b>\n\n"
                f"🪙 {coin}\n"
                f"Entry: <code>{self._format_price(fill_price)}</code>\n"
                f"Qty: <code>{live_qty}</code>\n"
                f"SL: <code>{self._format_price(stop_loss)}</code>\n"
                f"TP: <code>{self._format_price(take_profit)}</code>\n"
                f"Leverage: {PUMP_LEVERAGE}x\n"
                f"TP/SL: <b>Built-in to position</b>"
            )

            return True, None, None

        except Exception as exc:
            logger.exception("Failed to open long %s: %s", coin, exc)
            try:
                self._emergency_close_after_failed_protection(
                    symbol,
                    "sell",
                    trade["quantity"],
                )
            except Exception:
                logger.exception("Emergency close failed for %s", coin)

            return False, f"order setup failed: {exc}", {}

    def _open_short(self, signal, equity):
        """
        ⭐ ПРОСТАЯ ВЕРСИЯ: Один ордер с встроённым TP и SL
        """
        coin = signal.coin
        symbol = f"{coin}/USDT:USDT"

        atr = signal.atr or signal.price_at_detection * 0.02
        estimated_entry = signal.price_at_detection
        stop_loss = estimated_entry + atr * SHORT_SL_ATR_MULT
        take_profit = estimated_entry * (1 - SHORT_TP_PCT)

        trade, reason, details = self._prepare_trade(signal, equity, "short")
        if not trade:
            return False, reason, details

        safe, liq_details = self._check_liquidation_safety(
            symbol,
            estimated_entry,
            stop_loss,
            "short",
            trade["notional"],
        )
        if not safe:
            return (
                False,
                "estimated liquidation price is too close to SL",
                liq_details,
            )

        try:
            # ⭐ Создать позицию с встроённым TP и SL
            order = self.client.create_market_order(
                symbol,
                "sell",
                trade["quantity"],
                params={
                    "takeProfit": {
                        "triggerPrice": take_profit,
                    },
                    "stopLoss": {
                        "triggerPrice": stop_loss,
                    },
                }
            )

            fill_price = float(
                order.get("average")
                or order.get("price")
                or estimated_entry
            )

            live = self._fetch_live_position(symbol, "short")
            if live:
                fill_price = live["entry_price"]
                actual_liq = live["liquidation_price"]
            else:
                actual_liq = 0.0

            # Пересчитываем TP/SL на основе реальной цены входа
            atr = signal.atr or fill_price * 0.02
            stop_loss = fill_price + atr * SHORT_SL_ATR_MULT
            take_profit = fill_price * (1 - SHORT_TP_PCT)

            if actual_liq > 0:
                actual_distance = self._liq_sl_distance_pct(
                    stop_loss,
                    actual_liq,
                    "short",
                )
                if actual_distance < LIQ_SL_MIN_DISTANCE_PCT:
                    logger.error(
                        "Actual liquidation is too close after short fill: %s distance=%.3f%%",
                        coin,
                        actual_distance,
                    )
                    self._emergency_close_after_failed_protection(
                        symbol,
                        "buy",
                        trade["quantity"],
                    )
                    return (
                        False,
                        "liquidation price too close to SL; position closed",
                        {
                            "Entry": fill_price,
                            "SL": stop_loss,
                            "Liquidation": actual_liq,
                            "Distance": actual_distance,
                        },
                    )

            live_qty = live["quantity"] if live else trade["quantity"]

            position = PumpPosition(
                coin=coin,
                side="short",
                entry_price=fill_price,
                quantity=live_qty,
                stop_loss=stop_loss,
                take_profit=take_profit,
                liquidation_price=actual_liq,
                opened_at=time.time(),
                last_sync_at=time.time(),
            )

            with self._lock:
                self.pump_positions[coin] = position

            logger.info(
                "SHORT OPENED %s | entry=%.8f qty=%s liq=%.8f SL=%.8f TP=%.8f",
                coin,
                fill_price,
                live_qty,
                actual_liq,
                stop_loss,
                take_profit,
            )

            self._alert(
                f"🔴 <b>SHORT OPENED</b>\n\n"
                f"🪙 {coin}\n"
                f"Entry: <code>{self._format_price(fill_price)}</code>\n"
                f"Qty: <code>{live_qty}</code>\n"
                f"SL: <code>{self._format_price(stop_loss)}</code>\n"
                f"TP: <code>{self._format_price(take_profit)}</code>\n"
                f"Leverage: {PUMP_LEVERAGE}x\n"
                f"TP/SL: <b>Built-in to position</b>"
            )

            return True, None, None

        except Exception as exc:
            logger.exception("Failed to open short %s: %s", coin, exc)
            try:
                self._emergency_close_after_failed_protection(
                    symbol,
                    "buy",
                    trade["quantity"],
                )
            except Exception:
                logger.exception("Emergency close failed for %s", coin)

            return False, f"order setup failed: {exc}", {}

    def _emergency_close_after_failed_protection(self, symbol, side, quantity):
        qty = self._round_qty(symbol, quantity)
        if qty > 0:
            self.client.create_market_order(
                symbol,
                side,
                qty,
                params={"reduceOnly": True},
            )

    # ============================================================================
    # POSITION MANAGEMENT - SIMPLIFIED
    # ============================================================================

    def _manage_positions(self, tickers):
        """Manage open positions and check for closes"""
        with self._lock:
            positions = list(self.pump_positions.items())

        for coin, position in positions:
            symbol = f"{coin}/USDT:USDT"

            try:
                live = self._fetch_live_position(symbol, position.side)

                if live:
                    position.quantity = live["quantity"]
                    if live["entry_price"] > 0:
                        position.entry_price = live["entry_price"]
                    if live["liquidation_price"] > 0:
                        position.liquidation_price = live["liquidation_price"]

                    if (
                        position.liquidation_price > 0
                        and time.time() - position.last_sync_at >= POSITION_SYNC_INTERVAL_SEC
                    ):
                        logger.info(
                            "%s sync | side=%s qty=%s entry=%.8f liq=%.8f",
                            coin,
                            position.side,
                            position.quantity,
                            position.entry_price,
                            position.liquidation_price,
                        )
                        position.last_sync_at = time.time()

                else:
                    # Position closed on exchange
                    logger.info(
                        "%s position closed on exchange | entry=%.8f",
                        coin,
                        position.entry_price,
                    )

                    self._alert(
                        f"⚪ <b>POSITION CLOSED</b>\n\n"
                        f"🪙 {coin}\n"
                        f"Side: {position.side}\n"
                        f"Entry: <code>{self._format_price(position.entry_price)}</code>\n"
                        f"Closed by: <b>Bybit (TP/SL)</b>"
                    )

                    with self._lock:
                        self.pump_positions.pop(coin, None)
                        self.cooldowns[coin] = time.time() + PUMP_COOLDOWN_SEC

            except Exception as exc:
                logger.exception(
                    "Position sync failed for %s: %s",
                    coin,
                    exc,
                )

    # ========================================================================
    # NEW LISTINGS
    # ========================================================================

    def _check_new_symbols(self):
        try:
            now = time.time()

            if now - self._markets_last_loaded < 300:
                return

            self.client.load_markets(True)
            self._markets_last_loaded = now

            current = set()

            for symbol, info in self.client.markets.items():
                if (
                    info.get("linear")
                    and info.get("active")
                    and info.get("quote") == "USDT"
                    and info.get("type") == "swap"
                ):
                    current.add(symbol)

            old = set(self._all_symbols)
            new_symbols = current - old

            for symbol in new_symbols:
                coin = symbol.split("/")[0]

                if coin in self._blacklist:
                    continue

                self._alert(
                    f"🆕 <b>NEW LISTING</b>\n\n"
                    f"🪙 <b>{coin}</b>\n"
                    f"Pair: <code>{symbol}</code>"
                )

            self._all_symbols = list(current)

        except Exception as exc:
            logger.debug("New symbol check failed: %s", exc)

    # ========================================================================
    # TELEGRAM
    # ========================================================================

    def _alert(self, message):
        if self.telegram_fn:
            try:
                self.telegram_fn(message)
            except Exception as exc:
                logger.error("Telegram callback failed: %s", exc)
            return

        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            logger.warning("Telegram not configured")
            return

        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

            response = requests.post(
                url,
                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=10,
            )

            if not response.ok:
                logger.error("Telegram error: %s", response.text[:500])

        except Exception as exc:
            logger.error("Telegram request failed: %s", exc)

    # ========================================================================
    # UTILITIES
    # ========================================================================

    def _in_cooldown(self, coin):
        with self._lock:
            until = self.cooldowns.get(coin, 0)

        return time.time() < until

    def _get_equity(self):
        try:
            balance = self.client.fetch_balance({"type": "contract"})
            return float(balance["total"].get("USDT", 0))
        except Exception as exc:
            logger.error("Balance fetch failed: %s", exc)
            return 0.0

    def _round_qty(self, symbol, quantity):
        try:
            market = self.client.market(symbol)

            minimum = (
                market.get("limits", {})
                .get("amount", {})
                .get("min", 0)
                or 0
            )

            quantity = float(
                self.client.amount_to_precision(
                    symbol,
                    quantity,
                )
            )

            if quantity < minimum:
                return 0.0

            return quantity

        except Exception:
            return round(quantity, 4)

    @staticmethod
    def _format_price(price):
        if price >= 1000:
            return f"{price:,.2f}"
        if price >= 1:
            return f"{price:.4f}"
        if price >= 0.01:
            return f"{price:.6f}"
        return f"{price:.8f}"

    @staticmethod
    def _pump_confidence(
        volume_ratio,
        price_change,
        rsi,
        buy_ratio,
    ):
        volume_score = min(
            volume_ratio / (VOLUME_SPIKE_MULT * 2),
            1.0,
        )

        price_score = min(
            price_change / (PRICE_SPIKE_PCT * 3),
            1.0,
        )

        rsi_score = max(
            0,
            1 - abs(rsi - 70) / 30,
        )

        buy_score = min(
            buy_ratio / 0.8,
            1.0,
        )

        return (
            volume_score * 0.30
            + price_score * 0.30
            + rsi_score * 0.20
            + buy_score * 0.20
        )

    @staticmethod
    def _calc_rsi(closes, period=14):
        if len(closes) < period + 1:
            return 50.0

        deltas = np.diff(closes[-(period + 1):])

        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)

        avg_gain = np.mean(gains)
        avg_loss = np.mean(losses)

        if avg_loss == 0:
            return 100.0

        rs = avg_gain / avg_loss

        return 100 - (100 / (1 + rs))

    @staticmethod
    def _calc_atr(highs, lows, closes, period=14):
        if len(highs) < period + 1:
            return 0.0

        trs = []

        for i in range(-period, 0):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )

            trs.append(tr)

        return float(np.mean(trs))

    def get_status(self):
        with self._lock:
            return {
                "running": self._running,
                "mode": PUMP_MODE,
                "trading_enabled": TRADING_ENABLED,
                "trading_allowed": self._trading_allowed(),
                "symbols": len(self._all_symbols),
                "positions": len(self.pump_positions),
                "cooldowns": sum(
                    1
                    for value in self.cooldowns.values()
                    if value > time.time()
                ),
            }


# ============================================================================
# FACTORY
# ============================================================================

def create_pump_scanner_from_config():
    """Create scanner according to PUMP_MODE"""

    if PUMP_MODE == "off":
        logger.info("PUMP_MODE=off — scanner disabled")
        return None

    try:
        import ccxt

        if PUMP_MODE == "alerts":
            logger.info("Creating PUBLIC Bybit client")
            client = ccxt.bybit({'enableRateLimit': True})

        elif PUMP_MODE == "trading":

            if not TRADING_ENABLED:
                logger.error("PUMP_MODE=trading but TRADING_ENABLED=false")
                client = ccxt.bybit({'enableRateLimit': True})

            elif not BYBIT_API_KEY or not BYBIT_API_SECRET:
                raise RuntimeError(
                    "PUMP_MODE=trading requires BYBIT_API_KEY and BYBIT_API_SECRET"
                )

            else:
                logger.warning(
                    "Creating AUTHENTICATED Bybit client — TRADING ENABLED=%s",
                    TRADING_ENABLED,
                )

                client = ccxt.bybit({
                    "apiKey": BYBIT_API_KEY,
                    "secret": BYBIT_API_SECRET,
                    "enableRateLimit": True,
                    "options": {
                        "defaultType": "linear",
                    },
                })

        else:
            raise ValueError(f"Unknown PUMP_MODE={PUMP_MODE}")

        if BYBIT_TESTNET:
            client.set_sandbox_mode(True)

        client.load_markets()

        scanner = PumpScanner(client)

        return scanner

    except Exception as exc:
        logger.exception("Failed to create pump scanner: %s", exc)
        return None


# ============================================================================
# STANDALONE
# ============================================================================

if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    print(
        """
╔═══════════════════════════════════════════╗
║  DeepAlpha Pump Scanner (SIMPLIFIED)      ║
║  1 TP + 1 SL Built-in to Position         ║
╚═══════════════════════════════════════════╝
"""
    )

    print(f"Mode: {PUMP_MODE}")
    print(f"Trading enabled: {TRADING_ENABLED}")

    scanner = create_pump_scanner_from_config()

    if not scanner:
        print("Scanner is disabled or failed to initialize.")
        raise SystemExit(1)

    scanner.start()

    if TEST_ALERT:
        scanner._alert(
            "🧪 <b>TEST ALERT</b>\n\n"
            "Telegram connection works.\n"
            f"Mode: <b>{PUMP_MODE.upper()}</b>\n"
            f"Style: <b>1 TP + 1 SL per position</b>"
        )

    try:
        while True:
            time.sleep(60)

    except KeyboardInterrupt:
        print("Stopping scanner...")
        scanner.stop()
