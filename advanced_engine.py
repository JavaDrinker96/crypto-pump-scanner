"""Advanced risk-first Bybit signal/execution engine used by pump_scanner."""
import json, os, time, logging, uuid
from dataclasses import dataclass, asdict
import numpy as np
try:
    import joblib
except Exception:
    joblib = None
try:
    from ccxt.base.errors import RateLimitExceeded, BadRequest
except Exception:
    RateLimitExceeded = Exception
    BadRequest = Exception

F=lambda k,d: float(os.getenv(k,d))
I=lambda k,d: int(os.getenv(k,d))
B=lambda k,d: os.getenv(k,str(d)).lower() in ('1','true','yes','on')

def short_exhaustion_gate(m1,m3,m5,rsi,volume_ratio,reversal,vwap_distance,vol_threshold,
                          min_move_pct=0.8,min_rsi=65,min_vwap_pct=0.5,
                          dumped_1m_pct=0.30,dumped_3m_pct=0.70):
    already_dumped=((m1<=-(dumped_1m_pct/100) and m3<=0) or
                    m3<=-(dumped_3m_pct/100) or rsi<45)
    checks={
        'prior_pump':m5>=min_move_pct/100,
        'volume':volume_ratio>=vol_threshold*.8,
        'reversal':bool(reversal),
        'rsi':rsi>=min_rsi,
        'vwap_distance':vwap_distance>=min_vwap_pct,
        'not_already_dumped':not already_dumped,
    }
    return all(bool(v) for v in checks.values()), checks

@dataclass
class Signal:
    symbol:str; side:str; price:float; atr:float; rsi:float; vol:float; flow:float; book:float; vwap:float; move5:float; score:float; reason:str; m1:float=0.; m3:float=0.; spread:float=0.; ml_prob:float=0.
@dataclass
class Position:
    symbol:str; side:str; entry:float; qty:float; stop:float; tp1:float; tp2:float; tp3:float; risk:float | None; remaining:float=1.; tp1_done:bool=False; tp2_done:bool=False

class Engine:
    def __init__(self,client,alert=None):
        self.c=client; self.alert=alert; self.pos={}; self.realized=0.; self.losses=0; self.halted=False
        self.day=time.strftime('%Y-%m-%d',time.gmtime()); self.day_start=None; self.pending={}; self.flow_cache={}; self.book_cache={}; self.flow_book_ttl=max(5,I('PUMP_FLOW_BOOK_CACHE_SEC',20)); self.diag={}
        self.vol=F('PUMP_VOL_SPIKE_MULT',1.4); self.minvol=F('PUMP_MIN_DOLLAR_VOL',5e6); self.m1=F('PUMP_MIN_1M_MOVE_PCT',.0015); self.m3=F('PUMP_MIN_3M_MOVE_PCT',.003); self.m5=F('PUMP_MAX_5M_MOVE_PCT',.060); self.brk=I('PUMP_BREAKOUT_LOOKBACK',10); self.long_momentum=I('PUMP_LONG_MIN_MOMENTUM',2)
        self.rmin=F('PUMP_MIN_RSI_ENTRY',52); self.rmax=F('PUMP_MAX_RSI_ENTRY',80); self.flowmin=F('PUMP_MIN_BUY_RATIO',.56); self.bookmin=F('PUMP_MIN_BOOK_IMBALANCE',.54); self.spread=F('MAX_SPREAD_PCT',.15); self.depth=F('ORDERBOOK_DEPTH_PCT',1)
        self.risk=F('MAX_RISK_PER_TRADE_PCT',.5)/100; self.dayloss=F('MAX_DAILY_LOSS_PCT',2)/100; self.maxloss=I('MAX_CONSECUTIVE_LOSSES',3); self.maxpos=I('PUMP_MAX_POSITIONS',2); self.lev=I('PUMP_LEVERAGE',2)
        self.tp1=F('TP1_R',1); self.tp2=F('TP2_R',2); self.tp3=F('TP3_R',3.5); self.tq1=F('TP1_CLOSE_PCT',.35); self.tq2=F('TP2_CLOSE_PCT',.35); self.trail=F('TRAILING_ATR_MULT',1.5); self.sl=F('PUMP_SL_ATR_MULT',1.8); self.ssl=F('SHORT_SL_ATR_MULT',1.5)
        self.confirm=max(1,I('SIGNAL_CONFIRM_CYCLES',2)); self.short_confirm=max(1,I('SHORT_SIGNAL_CONFIRM_CYCLES',2)); self.ml=None; self.ml_min=F('ML_MIN_PROBABILITY',.58)
        path=os.getenv('ML_MODEL_PATH','models/pump_classifier.joblib')
        if B('ML_ENABLED',True) and joblib and os.path.exists(path):
            try:self.ml=joblib.load(path)
            except Exception:logging.exception('ML model load failed')
    def _diag(self,k): self.diag[k]=self.diag.get(k,0)+1
    def ohlcv(self,s,n=120): return self.c.fetch_ohlcv(s,timeframe=os.getenv('PUMP_TIMEFRAME','1m'),limit=n)
    def rsi(self,c,n=14):
        d=np.diff(c); g=np.maximum(d,0); l=np.maximum(-d,0); ag=g[:n].mean(); al=l[:n].mean()
        for i in range(n,len(d)): ag=(ag*(n-1)+g[i])/n; al=(al*(n-1)+l[i])/n
        return 100 if al<=1e-12 else 100-100/(1+ag/al)
    def atr(self,x,n=14):
        h=np.array([z[2] for z in x]); l=np.array([z[3] for z in x]); c=np.array([z[4] for z in x]); pc=c[:-1]; tr=np.maximum(h[1:]-l[1:],np.maximum(abs(h[1:]-pc),abs(l[1:]-pc))); return float(tr[-n:].mean())
    def flow(self,s):
        now=time.time(); hit=self.flow_cache.get(s)
        if hit and now-hit[0]<self.flow_book_ttl:return hit[1]
        try:t=self.c.fetch_trades(s,limit=100)
        except RateLimitExceeded:raise
        except Exception:self._diag('flow_fetch_failed');return .5
        b=sum(float(x.get('amount') or 0) for x in t if str(x.get('side')).lower()=='buy'); a=sum(float(x.get('amount') or 0) for x in t if str(x.get('side')).lower()=='sell'); v=b/max(a+b,1e-12); self.flow_cache[s]=(now,v); return v
    def book(self,s,p):
        now=time.time(); hit=self.book_cache.get(s)
        if hit and now-hit[0]<self.flow_book_ttl:return hit[1],hit[2]
        try:
            o=self.c.fetch_order_book(s,limit=50); b=sum(float(q)*float(px) for px,q in o.get('bids',[]) if float(px)>=p*(1-self.depth/100)); a=sum(float(q)*float(px) for px,q in o.get('asks',[]) if float(px)<=p*(1+self.depth/100)); bid=o.get('bids',[[p,0]])[0][0]; ask=o.get('asks',[[p,0]])[0][0]; v=b/max(a+b,1e-12); sp=(ask-bid)/p*100; self.book_cache[s]=(now,v,sp); return v,sp
        except RateLimitExceeded:raise
        except Exception:self._diag('book_fetch_failed');return .5,999
    def signal(self,s):
        x=self.ohlcv(s)
        if len(x)<30:self._diag('insufficient_ohlcv');return None
        c=np.array([z[4] for z in x],float); o=np.array([z[1] for z in x]); v=np.array([z[5] for z in x]); p=float(c[-1]); a=self.atr(x); r=self.rsi(c); vr=v[-1]/max(v[-21:-1].mean(),1e-12); m1=c[-1]/c[-2]-1; m3=c[-1]/c[-4]-1; m5=c[-1]/c[-6]-1
        vw=sum(((z[2]+z[3]+z[4])/3)*z[5] for z in x[-30:])/max(sum(z[5] for z in x[-30:]),1e-12); vd=abs(p/vw-1)*100; green=sum(c[-2:] > o[-2:]); br=p>=max(c[-self.brk-1:-1]); failed=max(c[-6:])>=max(c[-self.brk-2:-2]) and p<c[-2]
        reversal=((c[-1]<o[-1] and c[-2]<o[-2]) or (p<c[-2] and c[-1]<c[-2]))
        short_vwap=vd>=F('PUMP_MIN_SHORT_DISTANCE_FROM_VWAP_PCT',0.5)
        checks={'breakout':br,'m1':m1>=self.m1,'m3':m3>=self.m3,'volume':vr>=self.vol,'green_2':green>=2}
        momentum=sum(bool(z) for z in checks.values())
        pre_long=momentum>=self.long_momentum and m5<=self.m5 and self.rmin<=r<=self.rmax and vd<=F('PUMP_MAX_DISTANCE_FROM_VWAP_PCT',5.0)
        short_reversal=failed or reversal
        pre_short,short_checks=short_exhaustion_gate(
            m1,m3,m5,r,vr,short_reversal,vd,self.vol,
            F('PUMP_SHORT_MIN_5M_MOVE_PCT',0.8),
            F('PUMP_SHORT_MIN_RSI',65),
            F('PUMP_SHORT_MIN_VWAP_DISTANCE_PCT',0.5),
            F('PUMP_SHORT_ALREADY_DUMPED_1M_PCT',0.30),
            F('PUMP_SHORT_ALREADY_DUMPED_3M_PCT',0.70),
        )
        if (vr>=self.vol*.8 and short_reversal) and not pre_short:
            reasons=[k for k,vv in short_checks.items() if not vv]
            logging.info('SHORT REJECT | %s | reasons=%s | rsi=%.1f | m1=%.3f%% | m3=%.3f%% | m5=%.3f%% | vwap=%.3f%%',
                         s,','.join(reasons),r,m1*100,m3*100,m5*100,vd)
        for k,vv in checks.items():
            if not vv:self._diag(f'long_fail_{k}')
        for k,vv in short_checks.items():
            if not vv:self._diag(f'short_fail_{k}')
        if not pre_long:self._diag('long_core_failed')
        if not pre_short:self._diag('short_core_failed')
        if not(pre_long or pre_short):self._diag('no_core_candidate');return None
        book,sp=self.book(s,p)
        if sp>self.spread:self._diag('spread_failed');return None
        long_book=book>=self.bookmin; short_book=book<=1-self.bookmin
        if not(long_book or short_book):self._diag('book_imbalance_failed');return None
        flow=self.flow(s); long=pre_long and flow>=self.flowmin and long_book; short=pre_short and flow<=F('PUMP_DUMP_MIN_SELL_RATIO',.55) and short_book
        if not long:self._diag('long_flow_or_book_failed')
        if not short:self._diag('short_flow_or_book_failed')
        if long and short:self._diag('signal_conflict');logging.warning('SIGNAL CONFLICT | %s',s);return None
        if not(long or short):self._diag('flow_filter_failed');return None
        side='long' if long else 'short'; q=flow if long else 1-flow; bi=book if long else 1-book; score=min(.3*min(vr/5,1)+.2*min(abs(m5)/.05,1)+.25*q+.15*max((bi-.5)*2,0)+.1,1)
        return Signal(s,side,p,a,r,vr,flow,book,vd,m5*100,score,'continuation' if long else 'exhaustion',m1*100,m3*100,sp,0.)
    def equity(self):
        try:
            b=self.c.fetch_balance({'type':'swap'}); usdt=b.get('USDT') or {}
            total=float((b.get('total') or {}).get('USDT') or usdt.get('total') or 0)
            free=float((b.get('free') or {}).get('USDT') or usdt.get('free') or 0)
            return total if total>0 else free
        except Exception as e:
            logging.warning('BALANCE READ FAILED | %s',e); return 0
    def open(self,s):
        if s.symbol in self.pos:
            logging.info('ORDER BLOCKED | position already exists | symbol=%s',s.symbol); return
        if len(self.pos)>=self.maxpos:
            logging.warning('ORDER BLOCKED | max positions reached | open=%s | max=%s | symbol=%s',len(self.pos),self.maxpos,s.symbol)
            if self.alert:self.alert(f'🟡 ORDER BLOCKED | {s.symbol} | max positions {len(self.pos)}/{self.maxpos}')
            return
        e=self.equity(); d=s.atr*(self.ssl if s.side=='short' else self.sl); q=e*self.risk/d if e and d else 0; m=self.c.market(s.symbol); amin=float(((m.get('limits',{}).get('amount') or {}).get('min')) or 0); q=float(self.c.amount_to_precision(s.symbol,max(q,amin)))
        if e<=0 or d<=0:
            logging.warning('ORDER BLOCKED | invalid sizing inputs | symbol=%s | equity=%.4f | atr=%.8f | risk_pct=%.4f',s.symbol,e,s.atr,self.risk)
            if self.alert:self.alert(f'🟡 ORDER BLOCKED | {s.symbol} | balance/ATR unavailable')
            return
        try:
            b=self.c.fetch_balance({'type':'swap'}); usdt=b.get('USDT') or {}; free=float((b.get('free') or {}).get('USDT') or usdt.get('free') or 0); total=float((b.get('total') or {}).get('USDT') or usdt.get('total') or 0)
        except Exception:
            free=total=0
        if free>0 and s.price>0 and self.lev>0:
            max_qty=free*self.lev*0.90/s.price
            if q>max_qty:
                logging.warning('ORDER RESIZE | symbol=%s | requested_qty=%s | max_qty=%s | free_usdt=%.4f | leverage=%s',s.symbol,q,max_qty,free,self.lev)
                q=float(self.c.amount_to_precision(s.symbol,max_qty))
        if q<amin or q<=0:
            logging.warning('ORDER BLOCKED | symbol=%s | qty=%s | min_qty=%s | free_usdt=%.4f',s.symbol,q,amin,free)
            if self.alert:self.alert(f'🟡 ORDER BLOCKED | {s.symbol} | insufficient margin/min qty | free={free:.2f} USDT')
            return
        side='buy' if s.side=='long' else 'sell'; logging.info('ORDER INTENT | signal=%s | order_side=%s | symbol=%s | qty=%s',s.side,side,s.symbol,q)
        try:self.c.set_leverage(self.lev,s.symbol)
        except BadRequest as e:
            if '110043' not in str(e):raise
            logging.info('LEVERAGE UNCHANGED | symbol=%s | leverage=%s',s.symbol,self.lev)
        try:o=self.c.create_order(s.symbol,'market',side,q,None,{'positionIdx':0})
        except Exception as e:
            msg=str(e)
            if '110007' in msg or 'InsufficientFunds' in msg:
                logging.warning('ORDER BLOCKED | symbol=%s | insufficient available margin | qty=%s | free_usdt=%.4f',s.symbol,q,free)
                if self.alert:self.alert(f'🟡 ORDER BLOCKED | {s.symbol} | insufficient available margin')
                return
            raise
        logging.info('ORDER EXECUTED | symbol=%s | side=%s | qty=%s | id=%s',s.symbol,side,q,o.get('id'))
        en=float(o.get('average') or o.get('price') or s.price); sg=1 if s.side=='long' else -1; stop=en-sg*d; t1=en+sg*d*self.tp1; t2=en+sg*d*self.tp2; t3=en+sg*d*self.tp3; self.pos[s.symbol]=Position(s.symbol,s.side,en,q,stop,t1,t2,t3,d)
        p=self.pos[s.symbol]
        p.trade_id=uuid.uuid4().hex
        p.signal_time=getattr(s,'signal_time',time.time()); p.order_time=time.time(); p.fill_time=time.time()
        p.entry_price=en; p.entry_qty=q
        p.tp1_price=t1; p.tp2_price=t2; p.tp3_price=t3; p.sl_price=stop
        p.tp1_fill_qty=0.0; p.tp2_fill_qty=0.0; p.tp3_fill_qty=0.0
        p.exit_price=None; p.exit_time=None; p.exit_reason=None; p.realized_pnl=None; p.fees=0.0
        p.mfe_pct=0.0; p.mae_pct=0.0; p.duration_sec=None
        p.ml_probability=s.ml_prob; p.rsi=s.rsi; p.volume_ratio=s.vol; p.flow=s.flow; p.book=s.book; p.spread=s.spread
        p.vwap_distance_pct=s.vwap; p.move1_pct=s.m1; p.move3_pct=s.m3; p.move5_pct=s.move5
        p.trade_status='OPEN'
        self.journal('TRADE_OPEN',p,{'signal':asdict(s),'order_id':o.get('id')})
    def journal(self,event,p,extra=None):
        path=os.getenv('TRADE_JOURNAL_PATH','data/trades.jsonl')
        os.makedirs(os.path.dirname(path) or '.',exist_ok=True)
        d=asdict(p)
        for k in ('trade_id','signal_time','order_time','fill_time','entry_price','entry_qty','tp1_price','tp2_price','tp3_price','sl_price','tp1_fill_qty','tp2_fill_qty','tp3_fill_qty','exit_price','exit_time','exit_reason','realized_pnl','fees','mfe_pct','mae_pct','duration_sec','ml_probability','rsi','volume_ratio','flow','book','spread','vwap_distance_pct','move1_pct','move3_pct','move5_pct','trade_status'):
            if hasattr(p,k): d[k]=getattr(p,k)
        rec={'ts':time.time(),'event':event,**d,**(extra or {})}
        line=json.dumps(rec,default=str,separators=(',',':'))
        with open(path,'a',encoding='utf8') as f: f.write(line+'\n')
        logging.info('TRADE JOURNAL | %s',line)
    def partial(self,p,f,reason):
        q=float(self.c.amount_to_precision(p.symbol,p.qty*f))
        if q>0:self.c.create_order(p.symbol,'market','sell' if p.side=='long' else 'buy',q,None,{'reduceOnly':True,'positionIdx':0});p.remaining-=f;self.journal(reason.lower(),p,{'qty':q})
    def close(self,p,reason):
        q=float(self.c.amount_to_precision(p.symbol,p.qty*p.remaining))
        if q>0:self.c.create_order(p.symbol,'market','sell' if p.side=='long' else 'buy',q,None,{'reduceOnly':True,'positionIdx':0})
        exit_time=time.time()
        try:px=float(self.c.fetch_ticker(p.symbol)['last']);pnl=(px-p.entry)*p.qty*(1 if p.side=='long' else -1)
        except Exception:px=None;pnl=0.0
        p.exit_price=px; p.exit_time=exit_time; p.exit_reason=reason; p.realized_pnl=pnl; p.duration_sec=exit_time-float(getattr(p,'fill_time',exit_time)); p.trade_status='CLOSED'
        self.realized+=pnl;self.losses=self.losses+1 if pnl<0 else 0
        self.journal('TRADE_CLOSE',p,{'reason':reason,'pnl_estimate':pnl,'pnl_source':'ticker_last_fallback'})
        self.pos.pop(p.symbol,None)

    def manage(self):pass
    def run(self,symbols):
        signals=0;errors=0;self.diag={}
        for s in symbols:
            try:
                sig=self.signal(s)
                if not sig:self.pending.pop(s,None);continue
                signals+=1; sig.signal_time=time.time(); logging.info('SIGNAL | %s %s score=%.2f rsi=%.1f vol=%.1fx flow=%.2f book=%.2f spread=%.3f%%',sig.side.upper(),s,sig.score,sig.rsi,sig.vol,sig.flow,sig.book,sig.spread)
                state=self.pending.get(s);count=(state[1]+1) if state and state[0]==sig.side else 1;self.pending[s]=(sig.side,count)
                required=self.short_confirm if sig.side=='short' else self.confirm
                logging.info('CONFIRM | %s %s %d/%d',sig.side.upper(),s,count,required)
                if self.alert:self.alert(f'🚨 {sig.side.upper()} {s} score={sig.score:.2f} CONF={count}/{required} ML={sig.ml_prob:.3f}')
                if count>=required and B('TRADING_ENABLED',False) and s not in self.pos:
                    self.open(sig);self.pending.pop(s,None)
                elif count>=required and not B('TRADING_ENABLED',False):
                    logging.info('ORDER BLOCKED | trading disabled | symbol=%s',s)
            except RateLimitExceeded:errors+=1;self._diag('rate_limit_exceeded');logging.error('RATE LIMIT | %s | ending scan cycle early',s);break
            except Exception:errors+=1;self._diag('unexpected_scan_error');logging.exception('signal scan failed for %s',s)
        top=sorted(self.diag.items(),key=lambda kv:kv[1],reverse=True)[:14]
        if top:logging.info('FILTER DIAGNOSTICS | %s',' | '.join(f'{k}={v}' for k,v in top))
        return {'signals':signals,'errors':errors}
