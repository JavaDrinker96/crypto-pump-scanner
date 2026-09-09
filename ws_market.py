import json,logging,threading,time
from collections import defaultdict,deque
import websocket

class BybitMarketWS:
 def __init__(self,testnet=False,max_candles=180):
  self.url='wss://stream-testnet.bybit.com/v5/public/linear' if testnet else 'wss://stream.bybit.com/v5/public/linear'
  self.max_candles=max_candles
  self._lock=threading.RLock();self._ws=None;self._thread=None;self._stop=False
  self._topics=set();self._ready=threading.Event();self._sub_id=0
  self._last_message=0.0;self._last_connect=0.0;self._reconnect_count=0;self._last_health_log=0.0
  self.tickers={};self.trades=defaultdict(lambda:deque(maxlen=300));self.books={};self.candles=defaultdict(lambda:deque(maxlen=max_candles));self._seeded=set()
 @staticmethod
 def sym(s):return s.replace('/','').replace(':USDT','')
 def start(self):
  if self._thread and self._thread.is_alive():return
  self._stop=False
  self._thread=threading.Thread(target=self._run,daemon=True,name='bybit-market-ws');self._thread.start();self._ready.wait(10)
  logging.info('WS START RESULT | ready=%s | thread_alive=%s',self._ready.is_set(),self._thread.is_alive())
 def stop(self):
  self._stop=True
  with self._lock:
   ws=self._ws
  if ws:
   try:ws.close()
   except Exception:logging.exception('WS CLOSE FAILED')
 def _run(self):
  backoff=2
  while not self._stop:
   try:
    self._ready.clear()
    logging.info('WS CONNECTING | url=%s',self.url)
    ws=websocket.WebSocketApp(self.url,on_open=self._on_open,on_message=self._on_message,on_error=self._on_error,on_close=self._on_close)
    self._ws=ws
    ws.run_forever(ping_interval=20,ping_timeout=10,ping_payload='ping')
    logging.warning('WS RUN_FOREVER RETURNED | stop=%s',self._stop)
   except Exception:logging.exception('WS LOOP FAILED')
   finally:
    with self._lock:self._ws=None
    self._ready.clear()
   if not self._stop:
    self._reconnect_count+=1
    logging.warning('WS RECONNECT | attempt=%s | backoff=%ss',self._reconnect_count,backoff)
    time.sleep(backoff)
    backoff=min(backoff*2,30)
  logging.info('WS LOOP STOPPED')
 def _on_open(self,ws):
  with self._lock:self._last_connect=time.time();self._last_message=time.time()
  self._ready.set()
  logging.info('WS CONNECTED | reconnect=%s | topics=%s',self._reconnect_count,len(self._topics))
  self._send(list(self._topics))
  if self._topics:logging.info('WS RESUBSCRIBE | topics=%s',len(self._topics))
  self._reconnect_count=0
 def _send(self,topics):
  if not topics:return
  for i in range(0,len(topics),100):
   self._sub_id+=1
   try:
    self._ws.send(json.dumps({'op':'subscribe','req_id':str(self._sub_id),'args':topics[i:i+100]}))
   except Exception as e:logging.warning('WS SUBSCRIBE FAILED | %s',e)
 def ensure_tickers(self,symbols):
  self.start();new=[f'tickers.{self.sym(s)}' for s in symbols if f'tickers.{self.sym(s)}' not in self._topics];self._topics.update(new)
  if new and self._ws and self._ready.is_set():self._send(new);logging.info('WS SUBSCRIBE TICKERS | count=%s',len(new))
 def ensure_symbols(self,symbols):
  self.start();new=[]
  for s in symbols:
   z=self.sym(s)
   for t in(f'publicTrade.{z}',f'orderbook.50.{z}',f'kline.1.{z}'):
    if t not in self._topics:new.append(t)
  self._topics.update(new)
  if new and self._ws and self._ready.is_set():self._send(new);logging.info('WS SUBSCRIBE SYMBOLS | count=%s',len(new))
 def _on_message(self,ws,msg):
  with self._lock:self._last_message=time.time()
  try:d=json.loads(msg);t=d.get('topic','');data=d.get('data')
  except Exception:return
  if not t or data is None:return
  try:
   with self._lock:
    if t.startswith('tickers.'):
     z=t.split('.',1)[1];incoming=data if isinstance(data,dict) else data[0]
     current=self.tickers.get(z,{})
     current.update(incoming);self.tickers[z]=current
    elif t.startswith('publicTrade.'):
     z=t.split('.',1)[1]
     for x in(data if isinstance(data,list) else[data]):self.trades[z].append(x)
    elif t.startswith('orderbook.'):
     z=t.rsplit('.',1)[1]
     b={'bids':{},'asks':{}} if d.get('type')=='snapshot' or z not in self.books else self.books[z]
     for side,key in(('b','bids'),('a','asks')):
      for row in data.get(side,[]):
       p,q=float(row[0]),float(row[1]);b[key].pop(p,None) if q<=0 else b[key].__setitem__(p,q)
     self.books[z]=b
    elif t.startswith('kline.'):
     z=t.rsplit('.',1)[1]
     for x in(data if isinstance(data,list) else[data]):
      bar=[int(x['start']),float(x['open']),float(x['high']),float(x['low']),float(x['close']),float(x['volume'])]
      if self.candles[z] and self.candles[z][-1][0]==bar[0]:self.candles[z][-1]=bar
      else:self.candles[z].append(bar)
  except Exception:logging.exception('WS MESSAGE PROCESS FAILED')
 def _on_error(self,ws,err):logging.warning('WS ERROR | %s',err)
 def _on_close(self,ws,code,msg):
  self._ready.clear()
  with self._lock:self._ws=None
  logging.warning('WS CLOSED | code=%s msg=%s',code,msg)
 def is_healthy(self,max_silence=45):
  with self._lock:
   healthy=self._ready.is_set() and self._ws is not None and self._last_message>0 and time.time()-self._last_message<=max_silence
   now=time.time()
   if now-self._last_health_log>=60:
    self._last_health_log=now;logging.info('WS HEALTH | healthy=%s | age=%.1fs | topics=%s | tickers=%s',healthy,(now-self._last_message) if self._last_message else -1,len(self._topics),len(self.tickers))
   return healthy
 def ticker_snapshot(self):
  with self._lock:return dict(self.tickers)
 def seed_ohlcv(self,symbol,fetcher,n=120):
  z=self.sym(symbol)
  with self._lock:
   if z in self._seeded and len(self.candles[z])>=30:return
  rows=fetcher(symbol,n)
  with self._lock:
   self.candles[z].clear()
   for r in rows[-self.max_candles:]:self.candles[z].append([int(r[0]),float(r[1]),float(r[2]),float(r[3]),float(r[4]),float(r[5])])
   self._seeded.add(z)
 def get_ohlcv(self,symbol,fetcher,n=120):
  z=self.sym(symbol);self.seed_ohlcv(symbol,fetcher,n)
  with self._lock:return list(self.candles[z])[-n:]
 def get_trades(self,symbol):
  z=self.sym(symbol)
  with self._lock:rows=list(self.trades[z])
  out=[]
  for x in rows:
   side=str(x.get('S') or x.get('side') or '').lower();amount=float(x.get('v') or x.get('size') or x.get('amount') or 0)
   if side in('buy','sell'):out.append({'side':side,'amount':amount})
  return out
 def get_order_book(self,symbol):
  z=self.sym(symbol)
  with self._lock:b=self.books.get(z)
  if not b:return None
  return {'bids':sorted([[p,q] for p,q in b['bids'].items()],reverse=True),'asks':sorted([[p,q] for p,q in b['asks'].items()])}
