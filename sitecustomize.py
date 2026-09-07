"""Runtime safety patch for ML gating and exchange-native Bybit TP/SL."""
import logging
import os

try:
    import numpy as np
    import joblib
except Exception:
    np = None
    joblib = None

try:
    import advanced_engine
except Exception:
    logging.exception("RUNTIME PATCH | failed to import advanced_engine")
else:
    _original_init = advanced_engine.Engine.__init__
    _original_signal = advanced_engine.Engine.signal
    _original_open = advanced_engine.Engine.open

    def _model_path():
        return os.getenv("ML_MODEL_PATH", "data/models/pump_classifier.joblib")

    def _train_model(client):
        """Train a small directional classifier from recent public OHLCV data."""
        if np is None or joblib is None:
            raise RuntimeError("numpy/joblib unavailable")
        try:
            from sklearn.ensemble import RandomForestClassifier
            from sklearn.model_selection import train_test_split
            from sklearn.metrics import roc_auc_score
        except Exception as exc:
            raise RuntimeError(f"scikit-learn unavailable: {exc}")

        symbols = [s.strip() for s in os.getenv(
            "ML_TRAIN_SYMBOLS",
            "BTC/USDT:USDT,ETH/USDT:USDT,SOL/USDT:USDT,XRP/USDT:USDT,DOGE/USDT:USDT,BNB/USDT:USDT"
        ).split(",") if s.strip()]
        limit = max(300, int(os.getenv("ML_TRAIN_CANDLES", "700")))
        threshold = float(os.getenv("ML_LABEL_RETURN_PCT", "0.25")) / 100.0
        X, y = [], []

        def rsi(c, n=14):
            d = np.diff(c); g = np.maximum(d, 0); l = np.maximum(-d, 0)
            ag = g[:n].mean(); al = l[:n].mean()
            for i in range(n, len(d)):
                ag = (ag * (n - 1) + g[i]) / n
                al = (al * (n - 1) + l[i]) / n
            return 100.0 if al <= 1e-12 else 100.0 - 100.0 / (1.0 + ag / al)

        for symbol in symbols:
            try:
                candles = client.fetch_ohlcv(symbol, timeframe=os.getenv("PUMP_TIMEFRAME", "1m"), limit=limit)
                if len(candles) < 80:
                    continue
                c = np.asarray([z[4] for z in candles], dtype=float)
                h = np.asarray([z[2] for z in candles], dtype=float)
                l = np.asarray([z[3] for z in candles], dtype=float)
                o = np.asarray([z[1] for z in candles], dtype=float)
                v = np.asarray([z[5] for z in candles], dtype=float)
                for i in range(30, len(c) - 5):
                    cc = c[:i + 1]
                    vv = v[:i + 1]
                    p = float(cc[-1])
                    rr = rsi(cc)
                    vr = float(vv[-1] / max(vv[-21:-1].mean(), 1e-12))
                    m1 = float(cc[-1] / cc[-2] - 1.0)
                    m3 = float(cc[-1] / cc[-4] - 1.0)
                    m5 = float(cc[-1] / cc[-6] - 1.0)
                    tr = np.maximum(h[1:i + 1] - l[1:i + 1], np.maximum(np.abs(h[1:i + 1] - c[:i]), np.abs(l[1:i + 1] - c[:i])))
                    atr = float(tr[-14:].mean())
                    vw = sum(((candles[j][2] + candles[j][3] + candles[j][4]) / 3.0) * candles[j][5] for j in range(max(0, i - 29), i + 1)) / max(sum(candles[j][5] for j in range(max(0, i - 29), i + 1)), 1e-12)
                    vd = abs(p / vw - 1.0) * 100.0
                    flow_proxy = float(np.clip(0.5 + (o[i] and (c[i] - o[i]) / max(h[i] - l[i], 1e-12)) * 0.25, 0.0, 1.0))
                    book_proxy = float(np.clip(0.5 + (c[i] - l[i]) / max(h[i] - l[i], 1e-12) * 0.25, 0.0, 1.0))
                    spread_proxy = float(min(1.0, atr / max(p, 1e-12) * 100.0))
                    score = min(0.3 * min(vr / 5.0, 1.0) + 0.2 * min(abs(m5) / 0.05, 1.0) + 0.25 * flow_proxy + 0.15 * max((book_proxy - 0.5) * 2, 0.0) + 0.1, 1.0)
                    future = float(c[i + 5] / c[i] - 1.0)
                    base = [score, rr, vr, flow_proxy, book_proxy, vd, m5 * 100.0, atr, m1, m3, spread_proxy]
                    X.append(base + [1.0]); y.append(int(future >= threshold))
                    X.append(base + [-1.0]); y.append(int(future <= -threshold))
            except Exception:
                logging.exception("ML TRAIN | failed for %s", symbol)

        if len(X) < 500 or len(set(y)) < 2:
            raise RuntimeError(f"insufficient training data: samples={len(X)} classes={sorted(set(y))}")

        X = np.asarray(X, dtype=float); y = np.asarray(y, dtype=int)
        xa, xb, ya, yb = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y)
        model = RandomForestClassifier(
            n_estimators=250, max_depth=8, min_samples_leaf=10,
            class_weight="balanced_subsample", random_state=42, n_jobs=-1
        )
        model.fit(xa, ya)
        try:
            auc = roc_auc_score(yb, model.predict_proba(xb)[:, 1])
        except Exception:
            auc = 0.0
        path = _model_path()
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        joblib.dump({"model": model, "feature_count": 12, "label_threshold": threshold, "symbols": symbols, "auc": auc}, path)
        logging.info("ML TRAINED | samples=%s | positives=%s | auc=%.3f | path=%s", len(y), int(y.sum()), auc, path)
        return path

    def _patched_init(self, client, alert=None):
        _original_init(self, client, alert)
        self.ml_required = os.getenv("ML_REQUIRED", "true").lower() in ("1", "true", "yes", "on")
        if not os.getenv("ML_ENABLED", "true").lower() in ("1", "true", "yes", "on"):
            if self.ml_required:
                logging.error("ML BLOCKED | ML_ENABLED=false while ML_REQUIRED=true")
            return
        path = _model_path()
        if self.ml is None and os.getenv("ML_AUTO_TRAIN", "true").lower() in ("1", "true", "yes", "on"):
            try:
                _train_model(client)
                self.ml = joblib.load(path)
                logging.info("ML STATUS | enabled=true | loaded=true | source=auto-trained | path=%s", path)
            except Exception:
                logging.exception("ML AUTO TRAIN FAILED | trading will remain blocked")
        elif self.ml is not None:
            logging.info("ML STATUS | enabled=true | loaded=true | path=%s", path)
        else:
            logging.error("ML STATUS | enabled=true | loaded=false | path=%s", path)

    def _patched_signal(self, symbol):
        sig = _original_signal(self, symbol)
        if sig is None:
            return None
        if getattr(self, "ml_required", True) and getattr(self, "ml", None) is None:
            self._diag("ml_unavailable")
            logging.warning("ML BLOCK | %s | no model", symbol)
            return None
        if getattr(self, "ml_required", True) and getattr(sig, "ml_prob", 0.0) <= 0.0:
            self._diag("ml_invalid_probability")
            logging.warning("ML BLOCK | %s | invalid probability=%.4f", symbol, getattr(sig, "ml_prob", 0.0))
            return None
        return sig

    def _amount(self, symbol, value):
        return float(self.c.amount_to_precision(symbol, value))

    def _native_trading_stop(self, p, price, qty, tp_price):
        symbol = self.c.market(p.symbol).get("id") or p.symbol.replace("/", "").replace(":USDT", "")
        side_sign = 1 if p.side == "long" else -1
        params = {
            "category": "linear",
            "symbol": symbol,
            "positionIdx": 0,
            "tpslMode": "Partial",
            "takeProfit": str(tp_price),
            "stopLoss": str(p.stop),
            "tpSize": str(qty),
            "slSize": str(qty),
            "tpOrderType": "Market",
            "slOrderType": "Market",
            "tpTriggerBy": "MarkPrice",
            "slTriggerBy": "MarkPrice",
        }
        if side_sign == 1:
            if not (tp_price > price and p.stop < price):
                raise RuntimeError(f"invalid LONG protection prices entry={price} tp={tp_price} sl={p.stop}")
        else:
            if not (tp_price < price and p.stop > price):
                raise RuntimeError(f"invalid SHORT protection prices entry={price} tp={tp_price} sl={p.stop}")
        return self.c.request("v5/position/trading-stop", "private", "POST", params)

    def _protect(self, p):
        fractions = [("TP1", p.tp1, float(os.getenv("TP1_CLOSE_PCT", "0.35"))), ("TP2", p.tp2, float(os.getenv("TP2_CLOSE_PCT", "0.35"))), ("TP3", p.tp3, max(0.0, 1.0 - float(os.getenv("TP1_CLOSE_PCT", "0.35")) - float(os.getenv("TP2_CLOSE_PCT", "0.35"))))]
        orders = []
        for name, tp, fraction in fractions:
            qty = _amount(self, p.symbol, p.qty * fraction)
            if qty <= 0:
                continue
            response = _native_trading_stop(self, p, p.entry, qty, tp)
            if not isinstance(response, dict) or response.get("retCode", 0) != 0:
                raise RuntimeError(f"Bybit trading-stop failed: {response}")
            orders.append((name, qty, response.get("retCode")))
            logging.info("PROTECTION SET | symbol=%s | %s qty=%s tp=%s sl=%s", p.symbol, name, qty, tp, p.stop)
        if len(orders) != 3:
            raise RuntimeError(f"protection incomplete: {orders}")
        logging.info("PROTECTION VERIFIED | symbol=%s | side=%s | SL=%s | TP1=%s | TP2=%s | TP3=%s | mode=BybitPartial", p.symbol, p.side, p.stop, p.tp1, p.tp2, p.tp3)
        self.journal("protection", p, {"exchange_orders": orders, "mode": "bybit_trading_stop_partial"})

    def _open_with_protection(self, s):
        before = set(self.pos)
        _original_open(self, s)
        if s.symbol not in self.pos or s.symbol in before:
            return
        p = self.pos[s.symbol]
        try:
            _protect(self, p)
        except Exception:
            logging.exception("PROTECTION FAILED | symbol=%s | closing immediately", p.symbol)
            try:
                q = _amount(self, p.symbol, p.qty)
                if q > 0:
                    self.c.create_order(p.symbol, "market", "sell" if p.side == "long" else "buy", q, None, {"reduceOnly": True, "positionIdx": 0})
            finally:
                self.pos.pop(s.symbol, None)
            raise

    def _exchange_managed_only(self):
        return

    advanced_engine.Engine.__init__ = _patched_init
    advanced_engine.Engine.signal = _patched_signal
    advanced_engine.Engine.open = _open_with_protection
    advanced_engine.Engine.manage = _exchange_managed_only
    logging.info("RUNTIME PATCH | ML fail-closed + Bybit native partial TP/SL enabled")
