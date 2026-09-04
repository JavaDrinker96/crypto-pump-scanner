"""Collect OHLCV, public trades and order-book snapshots for research/backtesting."""
import argparse,json,os,time
import ccxt
from dotenv import load_dotenv
load_dotenv()

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--symbols',default='BTC/USDT:USDT');ap.add_argument('--minutes',type=int,default=240);ap.add_argument('--interval',type=int,default=10);ap.add_argument('--out',default='data/market');a=ap.parse_args()
    ex=ccxt.bybit({'apiKey':os.getenv('BYBIT_API_KEY',''),'secret':os.getenv('BYBIT_API_SECRET',''),'enableRateLimit':True,'options':{'defaultType':'swap'}})
    if os.getenv('BYBIT_TESTNET','true').lower() in ('1','true','yes','on'): ex.set_sandbox_mode(True)
    ex.load_markets(); symbols=[x.strip() for x in a.symbols.split(',') if x.strip()]; os.makedirs(a.out,exist_ok=True)
    end=time.time()+a.minutes*60
    while time.time()<end:
        for s in symbols:
            try:
                ts=int(time.time()*1000); ohlcv=ex.fetch_ohlcv(s,'1m',limit=120); trades=ex.fetch_trades(s,limit=100); book=ex.fetch_order_book(s,limit=50)
                rec={'ts':ts,'symbol':s,'ohlcv':ohlcv,'trades':[{'timestamp':t.get('timestamp'),'price':t.get('price'),'amount':t.get('amount'),'side':t.get('side')} for t in trades], 'book':book}
                fn=os.path.join(a.out,s.replace('/','_').replace(':','_')+'.jsonl')
                with open(fn,'a',encoding='utf8') as f:f.write(json.dumps(rec,separators=(',',':'))+'\n')
            except Exception as e: print('collector error',s,e)
        time.sleep(max(1,a.interval))
if __name__=='__main__': main()
