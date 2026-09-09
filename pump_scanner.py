"""Production entrypoint for the advanced DeepAlpha engine."""
import os
import time
import logging

# Configure logging before importing runtime patches/engine so early diagnostics are visible.
logging.basicConfig(level=os.getenv('LOG_LEVEL','INFO'),format='%(asctime)s | %(levelname)s | %(message)s',force=True)

import ccxt
try:
    import sitecustomize  # noqa: F401
except Exception:
    logging.exception('RUNTIME PATCH | explicit sitecustomize import failed')
from ccxt.base.errors import AuthenticationError
from advanced_engine import Engine
from ws_market import BybitMarketWS

KEY=os.getenv('BYBIT_API_KEY','');SECRET=os.getenv('BYBIT_API_SECRET','');TESTNET=os.getenv('BYBIT_TESTNET','true').lower()=='true';TRADING=os.getenv('TRADING_ENABLED','false').lower() in ('1','true','yes','on');TG=os.getenv('TELEGRAM_TOKEN','');CHAT=os.getenv('TELEGRAM_CHAT_ID','')

def alert(text):
    if not (TG and CHAT):
        logging.error('TELEGRAM BLOCKED | token_set=%s | chat_set=%s',bool(TG),bool(CHAT))
        return False
    try:
        import requests
        r=requests.post(f'https://api.telegram.org/bot{TG}/sendMessage',data={'chat_id':CHAT,'text':text,'parse_mode':'HTML'},timeout=10)
        if not r.ok:
            logging.error('TELEGRAM FAILED | status=%s | response=%s',r.status_code,r.text[:300])
            return False
        logging.info('TELEGRAM SENT | status=%s',r.status_code)
        return True
    except Exception:
        logging.exception('TELEGRAM REQUEST FAILED')
        return False

def create_pump_scanner_from_config():
    logging.info('EXCHANGE INIT | bybit swap | testnet=%s',TESTNET)
    x=ccxt.bybit({'enableRateLimit':True,'options':{'defaultType':'swap','adjustForTimeDifference':True},'timeout':10000})
    if KEY and SECRET:x.apiKey=KEY;x.secret=SECRET
    if TESTNET:x.set_sandbox_mode(True)
    return x

def load_public_markets(x):
    started=time.time();markets=x.fetch_markets();x.set_markets(markets);logging.info('MARKETS LOADED | count=%s | elapsed=%.1fs',len(markets),time.time()-started);return x.markets

def validate_live_credentials(x):
    if not TRADING:return False
    if not(KEY and SECRET):logging.error('LIVE BLOCKED | missing Bybit API credentials');return False
    try:x.fetch_balance({'type':'swap'});logging.info('LIVE AUTH OK | mainnet trading credentials accepted');return True
    except AuthenticationError:logging.error('LIVE BLOCKED | Bybit rejected API credentials');return False
    except Exception:logging.exception('LIVE BLOCKED | Bybit credential validation failed');return False

def main():
    logging.info('BOOT | telegram_token_set=%s | telegram_chat_set=%s | trading=%s | testnet=%s',bool(TG),bool(CHAT),TRADING,TESTNET)
    x=create_pump_scanner_from_config();live_ready=validate_live_credentials(x);effective_trading=TRADING and live_ready
    markets=load_public_markets(x)
    symbols=[s for s,m in markets.items() if m.get('active') and m.get('linear') and m.get('swap') and m.get('quote')=='USDT']
    logging.info('MARKET UNIVERSE | linear_usdt=%s',len(symbols))
    ws=BybitMarketWS(TESTNET);x.ws_market=ws;ws.start();ws.ensure_tickers(symbols)
    mode='TRADING' if effective_trading else('LIVE-BLOCKED' if TRADING else 'ALERTS-ONLY')
    alert(f'🟢 <b>DeepAlpha ONLINE</b>\nPairs: {len(symbols)}\nMode: {mode}\nTestnet: {TESTNET}')
    logging.info('DeepAlpha ONLINE | pairs=%s | mode=%s | testnet=%s',len(symbols),mode,TESTNET)
    engine=Engine(x,alert);scan_limit=max(10,int(os.getenv('PUMP_MAX_SCAN_SYMBOLS','40')));interval=max(5,int(os.getenv('PUMP_SCAN_INTERVAL','15')));minvol=float(os.getenv('PUMP_MIN_DOLLAR_VOL','5000000'))
    logging.info('SCANNER CONFIG | scan_limit=%s | interval=%ss | min_dollar_vol=%s',scan_limit,interval,minvol)
    last_heartbeat=0.0
    while True:
        loop_started=time.time()
        try:
            if not ws.is_healthy():
                logging.warning('WS UNHEALTHY | waiting for reconnect | tickers=%s',len(ws.ticker_snapshot()))
                alert('⚠️ <b>DeepAlpha WS unhealthy</b>\nWaiting for automatic reconnect.')
                time.sleep(3);continue
            snap=ws.ticker_snapshot();ranked=[]
            if len(snap)<max(10,min(100,len(symbols)//2)):
                logging.warning('WS TICKER WARMUP | received=%s/%s',len(snap),len(symbols));time.sleep(2);continue
            for s in symbols:
                z=ws.sym(s);t=snap.get(z) or {};q=float(t.get('turnover24h') or t.get('quoteVolume') or 0);pct=float(t.get('price24hPcnt') or 0)*100
                if q>=minvol and t.get('lastPrice'):ranked.append((abs(pct),q,s))
            ranked.sort(reverse=True);selected=[z[2] for z in ranked[:scan_limit]];ws.ensure_symbols(selected)
            logging.info('SCAN START | tickers=%s | liquid=%s | scanning=%s | top=%s',len(snap),len(ranked),len(selected),selected[:5])
            os.environ['TRADING_ENABLED']='true' if effective_trading else 'false'
            stats=engine.run(selected)
            elapsed=time.time()-loop_started
            logging.info('SCAN RESULT | signals=%s | errors=%s | elapsed=%.1fs',stats.get('signals',0),stats.get('errors',0),elapsed)
            now=time.time()
            if now-last_heartbeat>=300:
                alert(f'💓 <b>DeepAlpha heartbeat</b>\nScans OK\nSignals: {stats.get("signals",0)}\nErrors: {stats.get("errors",0)}')
                last_heartbeat=now
        except Exception:
            logging.exception('SCANNER LOOP FAILED | elapsed=%.1fs',time.time()-loop_started)
            alert('🔴 <b>DeepAlpha scanner error</b>\nSee Railway logs for traceback.')
        time.sleep(interval)

if __name__=='__main__':main()
