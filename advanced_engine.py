"""Advanced risk-first Bybit signal/execution engine used by pump_scanner."""
import json,os,time
from dataclasses import dataclass,asdict
import numpy as np
try:
 import joblib
except Exception: joblib=None
F=lambda k,d:float(os.getenv(k,d)); I=lambda k,d:int(os.getenv(k,d)); B=lambda k,d:os.getenv(k,str(d)).lower() in ('1','true','yes','on')
@dataclass
class Signal:
 symbol:str; side:str; price:float; atr:float; rsi:float; vol:float; flow:float; book:float; vwap:float; move5:float; score:float; reason:str; m1:float=0.; m3:float=0.; spread:float=0.; ml_prob:float=0.
@dataclass
class Position:
 symbol:str; side:str; entry:float; qty:float; stop:float; tp1:float; tp2:float; tp3:float; risk:float; remaining:float=1.; tp1_done:bool=False; tp2_done:bool=False
class Engine:
 def __init__(self,client,alert=None):
  self.c=client;self.alert=alert;self.pos={};self.realized=0.;self.losses=0;self.halted=False;self.day=time.strftime('%Y-%m-%d',time.gmtime());self.day_start=None
  self.vol=F('PUMP_VOL_SPIKE_MULT',3);self.minvol=F('PUMP_MIN_DOLLAR_VOL',5e6);self.m1=F('PUMP_MIN_1M_MOVE_PCT',.006);self.m3=F('PUMP_MIN_3M_MOVE_PCT',.012);self.m5=F('PUMP_MAX_5M_MOVE_PCT',.045);self.brk=I('PUMP_BREAKOUT_LOOKBACK',10)
  self.rmin=F('PUMP_MIN_RSI_ENTRY',52);self.rmax=F('PUMP_MAX_RSI_ENTRY',78);self.flowmin=F('PUMP_MIN_BUY_RATIO',.58);self.bookmin=F('PUMP_MIN_BOOK_IMBALANCE',.56);self.spread=F('MAX_SPREAD_PCT',.15);self.depth=F('ORDERBOOK_DEPTH_PCT',1)
  self.risk=F('MAX_RISK_PER_TRADE_PCT',.5)/100;self.dayloss=F('MAX_DAILY_LOSS_PCT',2)/100;self.maxloss=I('MAX_CONSECUTIVE_LOSSES',3);self.maxpos=I('PUMP_MAX_POSITIONS',2);self.lev=I('PUMP_LEVERAGE',2)
  self.tp1=F('TP1_R',1);self.tp2=F('TP2_R',2);self.tp3=F('TP3_R',3.5);self.tq1=F('TP1_CLOSE_PCT',.35);self.tq2=F('TP2_CLOSE_PCT',.35);self.trail=F('TRAILING_ATR_MULT',1.5);self.sl=F('PUMP_SL_ATR_MULT',1.5);self.ssl=F('SHORT_SL_ATR_MULT',1.5)
  self.ml=None;self.ml_min=F('ML_MIN_PROBABILITY',.62)
  if B('ML_ENABLED',True) and joblib and os.path.exists(os.getenv('ML_MODEL_PATH','models/pump_classifier.joblib')):
   try:self.ml=joblib.load(os.getenv('ML_MODEL_PATH','models/pump_classifier.joblib'))
   except Exception:self.ml=None
 def ohlcv(self,s,n=120):return self.c.fetch_ohlcv(s,timeframe=os.getenv('PUMP_TIMEFRAME','1m'),limit=n)
 def rsi(self,c,n=14):
  d=np.diff(c);g=np.maximum(d,0);l=np.maximum(-d,0);ag=g[:n].mean();al=l[:n].mean()
  for i in range(n,len(d)):ag=(ag*(n-1)+g[i])/n;al=(al*(n-1)+l[i])/n
  return 100 if al<=1e-12 else 100-100/(1+ag/al)
 def atr(self,x,n=14):
  h=np.array([z[2] for z in x]);l=np.array([z[3] for z in x]);c=np.array([z[4] for z in x]);pc=c[:-1];tr=np.maximum(h[1:]-l[1:],np.maximum(abs(h[1:]-pc),abs(l[1:]-pc)));return float(tr[-n:].mean())
 def flow(self,s):
  try:t=self.c.fetch_trades(s,limit=100)
  except Exception:return .5
  b=sum(float(x.get('amount') or 0) for x in t if str(x.get('side')).lower()=='buy');a=sum(float(x.get('amount') or 0) for x in t if str(x.get('side')).lower()=='sell');return b/max(a+b,1e-12)
 def book(self,s,p):
  try:
   o=self.c.fetch_order_book(s,limit=50);b=sum(float(q)*float(px) for px,q in o.get('bids',[]) if float(px)>=p*(1-self.depth/100));a=sum(float(q)*float(px) for px,q in o.get('asks',[]) if float(px)<=p*(1+self.depth/100));bid=o.get('bids',[[p,0]])[0][0];ask=o.get('asks',[[p,0]])[0][0];return b/max(a+b,1e-12),(ask-bid)/p*100
  except Exception:return .5,999
 def signal(self,s):
  x=self.ohlcv(s);c=np.array([z[4] for z in x],float);o=np.array([z[1] for z in x]);v=np.array([z[5] for z in x]);p=float(c[-1]);a=self.atr(x);r=self.rsi(c);vr=v[-1]/max(v[-21:-1].mean(),1e-12);m1=c[-1]/c[-2]-1;m3=c[-1]/c[-4]-1;m5=c[-1]/c[-6]-1;flow=self.flow(s);book,sp=self.book(s,p);vw=sum(((z[2]+z[3]+z[4])/3)*z[5] for z in x[-30:])/max(sum(z[5] for z in x[-30:]),1e-12);vd=abs(p/vw-1)*100;green=sum(c[-2:] > o[-2:]);br=p>=max(c[-self.brk-1:-1]);
  long=br and m1>=self.m1 and m3>=self.m3 and m5<=self.m5 and vr>=self.vol and green>=2 and self.rmin<=r<=self.rmax and flow>=self.flowmin and book>=self.bookmin and vd<=F('PUMP_MAX_DISTANCE_FROM_VWAP_PCT',4.5) and sp<=self.spread
  failed=max(c[-6:])>=max(c[-self.brk-2:-2]) and p<c[-2];short=m5>=F('PUMP_MAX_ENTRY_5M_MOVE_PCT',4.5)/100 and vr>=self.vol*.8 and failed and r>=F('PUMP_DUMP_MAX_RSI',72) and flow<=F('PUMP_DUMP_MIN_SELL_RATIO',.55) and book<=1-self.bookmin and sp<=self.spread
  if not(long or short):return None
  side='long' if long else 'short';q=flow if long else 1-flow;bi=book if long else 1-book;score=min(.3*min(vr/5,1)+.2*min(abs(m5)/.05,1)+.25*q+.15*max((bi-.5)*2,0)+.1,1);mlp=0.
  if self.ml:
   try:
    feats=np.asarray([[score,r,vr,flow,book,vd,m5*100,a,m1,m3,sp]],float);mlp=float(self.ml['model'].predict_proba(feats)[0,1])
    if mlp<self.ml_min:return None
   except Exception:pass
  return Signal(s,side,p,a,r,vr,flow,book,vd,m5*100,score,'continuation' if long else 'exhaustion',m1*100,m3*100,sp,mlp)
 def equity(self):
  try:b=self.c.fetch_balance({'type':'swap'});return float((b.get('total') or {}).get('USDT') or (b.get('USDT') or {}).get('total') or 0)
  except Exception:return 0
 def open(self,s):
  if len(self.pos)>=self.maxpos:return
  e=self.equity();d=s.atr*(self.ssl if s.side=='short' else self.sl);q=e*self.risk/d if e and d else 0;m=self.c.market(s.symbol);amin=float(((m.get('limits',{}).get('amount') or {}).get('min')) or 0);q=float(self.c.amount_to_precision(s.symbol,max(q,amin)))
  if q<=0:return
  self.c.set_leverage(self.lev,s.symbol);o=self.c.create_order(s.symbol,'market','buy' if s.side=='long' else 'sell',q,None,{'positionIdx':0});en=float(o.get('average') or o.get('price') or s.price);sg=1 if s.side=='long' else -1;stop=en-sg*d;t1=en+sg*d*self.tp1;t2=en+sg*d*self.tp2;t3=en+sg*d*self.tp3;self.pos[s.symbol]=Position(s.symbol,s.side,en,q,stop,t1,t2,t3,d);self.journal('open',self.pos[s.symbol],{'signal':asdict(s)})
 def journal(self,event,p,extra=None):
  path=os.getenv('TRADE_JOURNAL_PATH','data/trades.jsonl');os.makedirs(os.path.dirname(path) or '.',exist_ok=True);open(path,'a',encoding='utf8').write(json.dumps({'ts':time.time(),'event':event,'position':asdict(p),**(extra or {})},default=str)+'\n')
 def manage(self):
  for s,p in list(self.pos.items()):
   try:px=float(self.c.fetch_ticker(s)['last']);sg=1 if p.side=='long' else -1
   except Exception:continue
   if (sg==1 and px<=p.stop) or (sg==-1 and px>=p.stop):self.close(p,'SL');continue
   if not p.tp1_done and ((sg==1 and px>=p.tp1) or (sg==-1 and px<=p.tp1)):self.partial(p,self.tq1,'TP1');p.tp1_done=True
   if not p.tp2_done and ((sg==1 and px>=p.tp2) or (sg==-1 and px<=p.tp2)):self.partial(p,self.tq2,'TP2');p.tp2_done=True
   if p.tp2_done:
    a=self.atr(self.ohlcv(s,40));p.stop=max(p.stop,px-a*self.trail) if sg==1 else min(p.stop,px+a*self.trail)
   if (sg==1 and px>=p.tp3) or (sg==-1 and px<=p.tp3):self.close(p,'TP3')
 def partial(self,p,f,reason):
  q=float(self.c.amount_to_precision(p.symbol,p.qty*f)
  )
  if q<=0:return
  self.c.create_order(p.symbol,'market','sell' if p.side=='long' else 'buy',q,None,{'reduceOnly':True,'positionIdx':0});p.remaining-=f;self.journal(reason.lower(),p,{'qty':q})
 def close(self,p,reason):
  q=float(self.c.amount_to_precision(p.symbol,p.qty*p.remaining));
  if q>0:self.c.create_order(p.symbol,'market','sell' if p.side=='long' else 'buy',q,None,{'reduceOnly':True,'positionIdx':0})
  try:px=float(self.c.fetch_ticker(p.symbol)['last']);pnl=(px-p.entry)*p.qty*(1 if p.side=='long' else -1)
  except Exception:pnl=0
  self.realized+=pnl;self.losses=self.losses+1 if pnl<0 else 0;self.journal('close',p,{'reason':reason,'pnl':pnl});self.pos.pop(p.symbol,None)
 def run(self,symbols):
  for s in symbols:
   try:
    sig=self.signal(s)
    if sig and self.alert:self.alert(f"🚨 {sig.side.upper()} {s} score={sig.score:.2f} ML={sig.ml_prob:.2f} RSI={sig.rsi:.1f} vol={sig.vol:.1f}x flow={sig.flow:.2f} book={sig.book:.2f}")
    if sig and B('TRADING_ENABLED',False) and os.getenv('PUMP_MODE','alerts')=='trading':self.open(sig)
   except Exception:pass
  if B('TRADING_ENABLED',False):self.manage()
