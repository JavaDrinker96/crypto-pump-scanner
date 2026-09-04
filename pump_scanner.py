"""Production entrypoint for the advanced DeepAlpha engine."""
import os,time,logging,ccxt
from ccxt.base.errors import AuthenticationError
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
    except Exception:
        logging.exception("telegram alert failed")

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

def validate_live_credentials(x):
    """Fail closed: live trading is disabled for this process if Bybit auth is invalid."""
    if not TRADING:
        return False
    if not (KEY and SECRET):
        logging.error("LIVE BLOCKED | missing Bybit API credentials")
        return False
    try:
        x.fetch_balance({"type":"swap"})
        logging.info("LIVE AUTH OK | mainnet trading credentials accepted")
        return True
    except AuthenticationError:
        logging.error("LIVE BLOCKED | Bybit rejected API credentials (retCode 10003)")
        return False
    except Exception:
        logging.exception("LIVE BLOCKED | Bybit credential validation failed")
        return False

def main():
    x=create_pump_scanner_from_config()
    live_ready=validate_live_credentials(x)
    effective_trading=TRADING and live_ready
    markets=load_public_markets(x)
    symbols=[s for s,m in markets.items() if m.get("active") and m.get("linear") and m.get("swap") and m.get("quote")=="USDT"]
    mode="TRADING" if effective_trading else ("LIVE-BLOCKED" if TRADING else "ALERTS-ONLY")
    alert(f"🟢 <b>DeepAlpha ONLINE</b>\nPairs: {len(symbols)}\nMode: {mode}\nTestnet: {TESTNET}")
    logging.info("DeepAlpha ONLINE | pairs=%s | mode=%s | testnet=%s",len(symbols),mode,TESTNET)
    engine=Engine(x,alert)
    scan_limit=max(10,int(os.getenv("PUMP_MAX_SCAN_SYMBOLS","40")))
    interval=max(5,int(os.getenv("PUMP_SCAN_INTERVAL","15")))
    while True:
        try:
            tickers=x.fetch_tickers(params={"subType":"linear"})
            ranked=[]
            minvol=float(os.getenv("PUMP_MIN_DOLLAR_VOL","5000000"))
            for s in symbols:
                t=tickers.get(s) or {}; q=float(t.get("quoteVolume") or 0); p=float(t.get("percentage") or 0)
                if q>=minvol and t.get("last"): ranked.append((abs(p),q,s))
            ranked.sort(reverse=True); selected=[z[2] for z in ranked[:scan_limit]]
            logging.info("UNIVERSE | tickers=%s | liquid=%s | scanning=%s | top=%s",len(tickers),len(ranked),len(selected),selected[:5])
            if effective_trading:
                os.environ["TRADING_ENABLED"]="true"
            else:
                os.environ["TRADING_ENABLED"]="false"
            stats=engine.run(selected)
            logging.info("SCAN RESULT | signals=%s | errors=%s",stats.get("signals",0),stats.get("errors",0))
        except Exception:
            logging.exception("scanner loop failed")
        time.sleep(interval)

if __name__=="__main__": main()
