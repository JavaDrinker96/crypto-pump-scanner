# Research pipeline

The repository now has three research stages: market collection, backtesting and ML training. Keep live trading disabled while validating a strategy.

## 1. Collect market data

```bash
python data/market_collector.py --symbols BTC/USDT:USDT,ETH/USDT:USDT --minutes 1440 --interval 10
```

Snapshots are appended under `data/market/` and include 1m OHLCV, recent public trades and an order-book snapshot. CCXT exposes OHLCV, public trades and order books through its unified API; historical public trades generally require pagination and exchange-specific handling.

## 2. Analyze live/paper trade journal

```bash
python analytics/trade_analyzer.py --input data/trades.jsonl
```

The report contains win rate, net PnL, profit factor, expectancy, drawdown and breakdowns by side/reason/symbol.

## 3. Backtest

Export a CSV with columns `timestamp,open,high,low,close,volume`, then run:

```bash
python backtest/run_backtest.py data/BTC_USDT_1m.csv --fee 0.00055 --slippage 0.0005
```

The runner is deliberately cost-aware and chronological. It is a baseline research engine, not a claim that historical simulation predicts future returns.

## 4. Train ML filter

After at least 30 paired open/close trades have accumulated:

```bash
python ml/train.py --input data/trades.jsonl
```

The model is saved to `models/pump_classifier.joblib`. Validation uses `TimeSeriesSplit` rather than random shuffling to reduce future-data leakage. The live engine loads the model only when `ML_ENABLED=true` and rejects signals below `ML_MIN_PROBABILITY`.

Do not load untrusted joblib/model files: scikit-learn documents that joblib/pickle-based persistence can execute arbitrary code when loading a malicious artifact.

## Recommended loop

`collect -> paper/testnet -> journal -> analyze -> backtest -> walk-forward -> train ML -> testnet -> production`

Never auto-deploy optimized parameters or a newly trained model directly to a live account. A target such as 2% per day is not guaranteed and should not be used as the acceptance criterion; prefer positive expectancy, controlled drawdown and stability across unseen time periods.
