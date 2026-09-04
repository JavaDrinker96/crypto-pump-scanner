"""Production entrypoint for the advanced DeepAlpha engine."""
import os,time,logging,ccxt
from advanced_engine import Engine

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),format="%(asctime)s | %(levelname)s | %(message)s")
KEY=os.getenv("BYBIT_API_KEY",""); SECRET=os.getenv("BYBIT_API_SECRET","")
TESTNET=os.getenv("BYBIT_TESTNET","true").lower()=="true"
TRADING=os.getenv("TRADING_ENABLED","false").lower() in ("1","true","yes","on")
TG=os.getenv("TELEGRAM_TOKEN",""); CHAT=os.getenv("TELEGRAM_CHAT_ID","")

def alert(text):
    if not (TG and CHAT): return
    try:
        import requests
        requests.post(f"https://api.telegram.org/bot{TG}/sendMessage",data={"chat_id":CHAT,"text":text,"parse_mode":"HTML"},timeout=5)
    except Exception: pass

def create_pump_scanner_from_config():
    config={"enableRateLimit":True,"options":{"defaultType":"swap","adjustForTimeDifference":True}}
    if KEY and SECRET:
        config.update({"apiKey":KEY,"secret":SECRET})
    x=ccxt.bybit(config)
    if TESTNET: x.set_sandbox_mode(True)
    return x

def load_public_markets(x):
    """Load market metadata without calling Bybit private currency endpoints."""
    markets=x.fetch_markets()
    x.set_markets(markets)
    return x.markets

def main():
    x=create_pump_scanner_from_config()
    if TRADING and not (KEY and SECRET):
        raise RuntimeError("TRADING_ENABLED=true requires BYBIT_API_KEY and BYBIT_API_SECRET")
    markets=load_public_markets(x)
    symbols=[s for s,m in markets.items() if m.get("active") and m.get("linear") and m.get("swap") and m.get("quote")=="USDT"]
    mode="TRADING" if TRADING else "ALERTS-ONLY"
    alert(f"🟢 <b>DeepAlpha ONLINE</b>\nPairs: {len(symbols)}\nMode: {mode}\nTestnet: {TESTNET}")
    logging.info("DeepAlpha ONLINE | pairs=%s | mode=%s | testnet=%s",len(symbols),mode,TESTNET)
    engine=Engine(x,alert)
    while True:
        try:
            tickers=x.fetch_tickers(); ranked=[]
            minvol=float(os.getenv("PUMP_MIN_DOLLAR_VOL","5000000"))
            for s in symbols:
                t=tickers.get(s) or {}; q=float(t.get("quoteVolume") or 0); p=float(t.get("percentage") or 0)
                if q>=minvol: ranked.append((abs(p),q,s))
            ranked.sort(reverse=True)
            engine.run([z[2] for z in ranked[:80]])
        except Exception:
            logging.exception("scanner loop failed")
        time.sleep(max(1,int(os.getenv("PUMP_SCAN_INTERVAL","5"))))

if __name__=="__main__": main()
