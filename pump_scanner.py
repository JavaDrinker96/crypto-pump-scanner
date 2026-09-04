"""Production entrypoint for the advanced DeepAlpha engine."""
import os,time,logging,ccxt
from advanced_engine import Engine

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),format="%(asctime)s | %(levelname)s | %(message)s")
KEY=os.getenv("BYBIT_API_KEY",""); SECRET=os.getenv("BYBIT_API_SECRET","")
TESTNET=os.getenv("BYBIT_TESTNET","true").lower()=="true"
TG=os.getenv("TELEGRAM_TOKEN",""); CHAT=os.getenv("TELEGRAM_CHAT_ID","")

def alert(text):
    if not (TG and CHAT): return
    try:
        import requests
        requests.post(f"https://api.telegram.org/bot{TG}/sendMessage",data={"chat_id":CHAT,"text":text,"parse_mode":"HTML"},timeout=5)
    except Exception: pass

def create_pump_scanner_from_config():
    x=ccxt.bybit({"apiKey":KEY,"secret":SECRET,"enableRateLimit":True,"options":{"defaultType":"swap","adjustForTimeDifference":True}})
    if TESTNET: x.set_sandbox_mode(True)
    return Engine(x,alert)

def main():
    x=create_pump_scanner_from_config()
    markets=x.c.load_markets()
    symbols=[s for s,m in markets.items() if m.get("active") and m.get("linear") and m.get("swap") and m.get("quote")=="USDT"]
    alert(f"🟢 <b>DeepAlpha ONLINE</b>\nPairs: {len(symbols)}\nTrading: {os.getenv('TRADING_ENABLED','false')}\nTestnet: {TESTNET}")
    while True:
        try:
            # Rank liquid movers first; expensive order-book/trade calls are only made for the top set.
            tickers=x.c.fetch_tickers(); ranked=[]
            minvol=float(os.getenv("PUMP_MIN_DOLLAR_VOL","5000000"))
            for s in symbols:
                t=tickers.get(s) or {}; q=float(t.get("quoteVolume") or 0); p=float(t.get("percentage") or 0)
                if q>=minvol: ranked.append((abs(p),q,s))
            ranked.sort(reverse=True)
            x.run([z[2] for z in ranked[:80]])
        except Exception:
            logging.exception("scanner loop failed")
        time.sleep(max(1,int(os.getenv("PUMP_SCAN_INTERVAL","5"))))

if __name__=="__main__": main()
