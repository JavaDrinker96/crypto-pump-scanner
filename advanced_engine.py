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

def volume_activity_ratio(rows, now_ms=None):
    if len(rows)<23:return 0.0
    vols=np.asarray([float(z[5]) for z in rows],float)
    baseline=float(vols[-22:-2].mean())
    if baseline<=1e-12:return 0.0
    now_ms=float(now_ms if now_ms is not None else time.time()*1000)
    candle_start=float(rows[-1][0])
    elapsed=max(.15,min(1.0,(now_ms-candle_start)/60000.0))
    projected_current=float(vols[-1])/elapsed
    return max(projected_current,float(vols[-2]))/baseline

def btc_regime_gate(ret15_pct,ret30_pct,side,long_min_15=-0.15,long_min_30=-0.05,
                    short_max_15=0.15,short_max_30=0.05):
    if side=='long':
        checks={'btc_15m':ret15_pct>=long_min_15,'btc_30m':ret30_pct>=long_min_30}
    else:
        checks={'btc_15m':ret15_pct<=short_max_15,'btc_30m':ret30_pct<=short_max_30}
    return all(checks.values()),checks

def long_continuation_gate(m1,m3,m5,rsi,volume_ratio,breakout,green_count,vwap_distance,vol_threshold,
                           min_m3_pct=0.03,min_m5_pct=0.10,min_rsi=52,max_rsi=72,
                           max_vwap_pct=3.0,max_m5_pct=6.0):
    checks={
        'volume':volume_ratio>=vol_threshold,
        'momentum_3m':m3>=min_m3_pct/100,
        'momentum_5m':m5>=min_m5_pct/100,
        'price_action':bool(breakout or green_count>=2 or m1>=min_m3_pct/200),
        'rsi':min_rsi<=rsi<=max_rsi,
        'vwap_distance':vwap_distance<=max_vwap_pct,
        'not_overextended':m5<=max_m5_pct/100,
    }
    return all(bool(v) for v in checks.values()), checks

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
    symbol:str; side:str; price:float; atr:float; rsi:float; vol:float; flow:float; book:float; vwap:float; move5:float; score:float; reason:str; m1:float=0.; m3:float=0.; spread:float=0.; ml_prob:float=0.; stop_distance:float=0.
@dataclass
class Position:
    symbol:str; side:str; entry:float; qty:float; stop:float; tp1:float; tp2:float; tp3:float; risk:float | None; remaining:float=1.; tp1_done:bool=False; tp2_done:bool=False

class Engine:
    def __init__(self,client,alert=None):
        self.c=client; self.alert=alert; self.pos={}; self.realized=0.; self.losses=0; self.halted=False
        self.day=time.strftime('%Y-%m-%d',time.gmtime()); self.day_start=None; self.pending={}; self.flow_cache={}; self.book_cache={}; self.flow_book_ttl=max(5,I('PUMP_FLOW_BOOK_CACHE_SEC',20)); self.diag={}
        self.btc_regime_cache=None; self.signal_rows_cache={}
        self.vol=F('PUMP_VOL_SPIKE_MULT',1.4); self.minvol=F('PUMP_MIN_DOLLAR_VOL',5e6); self.m1=F('PUMP_MIN_1M_MOVE_PCT',.0015); self.m3=F('PUMP_MIN_3M_MOVE_PCT',.003); self.m5=F('PUMP_MAX_5M_MOVE_PCT',.060); self.brk=I('PUMP_BREAKOUT_LOOKBACK',10); self.long_momentum=I('PUMP_LONG_MIN_MOMENTUM',2)
        self.rmin=F('PUMP_MIN_RSI_ENTRY',52); self.rmax=F('PUMP_MAX_RSI_ENTRY',80); self.flowmin=F('PUMP_MIN_BUY_RATIO',.56); self.bookmin=F('PUMP_MIN_BOOK_IMBALANCE',.54); self.spread=F('MAX_SPREAD_PCT',.15); self.depth=F('ORDERBOOK_DEPTH_PCT',1)
        self.risk=F('MAX_RISK_PER_TRADE_PCT',.5)/100; self.dayloss=F('MAX_DAILY_LOSS_PCT',2)/100; self.maxloss=I('MAX_CONSECUTIVE_LOSSES',3); self.maxpos=I('PUMP_MAX_POSITIONS',2); self.lev=I('PUMP_LEVERAGE',2)
        self.tp1=F('TP1_R',1); self.tp2=F('TP2_R',2); self.tp3=F('TP3_R',3.5); self.tq1=F('TP1_CLOSE_PCT',.35); self.tq2=F('TP2_CLOSE_PCT',.35); self.trail=F('TRAILING_ATR_MULT',1.5); self.sl=F('PUMP_SL_ATR_MULT',1.8); self.ssl=F('SHORT_SL_ATR_MULT',1.5)
        self.confirm=max(1,I('SIGNAL_CONFIRM_CYCLES',2)); self.long_confirm=max(1,I('LONG_SIGNAL_CONFIRM_CYCLES',2)); self.short_confirm=max(1,I('SHORT_SIGNAL_CONFIRM_CYCLES',2)); self.ml=None; self.ml_min=F('ML_MIN_PROBABILITY',.58)
        self.day_realized=0.0; self.day_start_equity=None; self.closed_trades=0; self.seen_execution_ids=set(); self._restore_risk_state()
        logging.info('STRATEGY CONFIG | schema=2 | long_confirm=%s | short_confirm=%s | long_vol=%.2f | long_m3_min=%.3f%% | long_m5_min=%.3f%% | long_rsi_max=%.1f | long_score_min=%.3f | btc_regime=%s | short_m5_min=%.3f%% | short_rsi_min=%.1f | risk_per_trade=%.3f%% | daily_loss_limit=%.3f%% | max_consecutive_losses=%s | leverage=%s',
                     self.long_confirm,self.short_confirm,F('PUMP_LONG_MIN_VOLUME_RATIO',1.2),F('PUMP_LONG_MIN_3M_MOVE_PCT',0.03),F('PUMP_LONG_MIN_5M_MOVE_PCT',0.10),
                     F('PUMP_LONG_MAX_RSI',72),F('PUMP_LONG_MIN_SCORE',0.0),B('BTC_REGIME_ENABLED',True),F('PUMP_SHORT_MIN_5M_MOVE_PCT',0.8),F('PUMP_SHORT_MIN_RSI',65),
                     self.risk*100,self.dayloss*100,self.maxloss,self.lev)
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
    def btc_regime(self):
        if not B('BTC_REGIME_ENABLED',True):
            return {'available':True,'ret15_pct':0.0,'ret30_pct':0.0}
        now=time.time(); hit=self.btc_regime_cache
        ttl=max(5,I('BTC_REGIME_CACHE_SEC',20))
        if hit and now-hit[0]<ttl:return hit[1]
        try:
            rows=self.ohlcv(os.getenv('BTC_REGIME_SYMBOL','BTC/USDT:USDT'),60)
            closes=np.asarray([float(z[4]) for z in rows],float)
            if len(closes)<31:raise RuntimeError('insufficient BTC candles')
            data={'available':True,'ret15_pct':(closes[-1]/closes[-16]-1)*100,
                  'ret30_pct':(closes[-1]/closes[-31]-1)*100}
            self.btc_regime_cache=(now,data)
            return data
        except Exception:
            logging.exception('BTC REGIME READ FAILED')
            return {'available':False,'ret15_pct':0.0,'ret30_pct':0.0}

    def signal(self,s):
        x=self.ohlcv(s)
        if len(x)<30:self._diag('insufficient_ohlcv');return None
        self.signal_rows_cache[s]=x
        c=np.array([z[4] for z in x],float); o=np.array([z[1] for z in x]); h=np.array([z[2] for z in x],float); l=np.array([z[3] for z in x],float); p=float(c[-1]); a=self.atr(x); r=self.rsi(c); vr=volume_activity_ratio(x); m1=c[-1]/c[-2]-1; m3=c[-1]/c[-4]-1; m5=c[-1]/c[-6]-1
        vw=sum(((z[2]+z[3]+z[4])/3)*z[5] for z in x[-30:])/max(sum(z[5] for z in x[-30:]),1e-12); vd=abs(p/vw-1)*100; green=sum(c[-2:] > o[-2:]); br=p>=max(c[-self.brk-1:-1]); failed=max(c[-6:])>=max(c[-self.brk-2:-2]) and p<c[-2]
        reversal=((c[-1]<o[-1] and c[-2]<o[-2]) or (p<c[-2] and c[-1]<c[-2]))
        short_vwap=vd>=F('PUMP_MIN_SHORT_DISTANCE_FROM_VWAP_PCT',0.5)
        pre_long,checks=long_continuation_gate(
            m1,m3,m5,r,vr,br,green,vd,F('PUMP_LONG_MIN_VOLUME_RATIO',1.2),
            F('PUMP_LONG_MIN_3M_MOVE_PCT',0.03),
            F('PUMP_LONG_MIN_5M_MOVE_PCT',0.10),
            F('PUMP_LONG_MIN_RSI',self.rmin),
            F('PUMP_LONG_MAX_RSI',72),
            F('PUMP_LONG_MAX_VWAP_DISTANCE_PCT',3.0),
            F('PUMP_LONG_MAX_5M_MOVE_PCT',self.m5*100),
        )
        if not pre_long and (vr>=self.vol*.8 or br or green>=2):
            reasons=[k for k,vv in checks.items() if not vv]
            logging.info('LONG REJECT | %s | reasons=%s | rsi=%.1f | vol=%.2fx | m1=%.3f%% | m3=%.3f%% | m5=%.3f%% | vwap=%.3f%%',
                         s,','.join(reasons),r,vr,m1*100,m3*100,m5*100,vd)
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
        side='long' if long else 'short'
        regime=self.btc_regime()
        if not regime.get('available'):
            if B('BTC_REGIME_REQUIRED',True):
                self._diag('btc_regime_unavailable'); logging.warning('BTC REGIME REJECT | %s | side=%s | unavailable',s,side); return None
        else:
            allowed,regime_checks=btc_regime_gate(
                regime['ret15_pct'],regime['ret30_pct'],side,
                F('BTC_LONG_MIN_15M_RETURN_PCT',-0.15),F('BTC_LONG_MIN_30M_RETURN_PCT',-0.05),
                F('BTC_SHORT_MAX_15M_RETURN_PCT',0.15),F('BTC_SHORT_MAX_30M_RETURN_PCT',0.05),
            )
            if B('BTC_REGIME_ENABLED',True) and not allowed:
                reasons=[k for k,vv in regime_checks.items() if not vv]
                self._diag('btc_regime_failed')
                logging.info('BTC REGIME REJECT | %s | side=%s | reasons=%s | btc15=%.3f%% | btc30=%.3f%%',
                             s,side,','.join(reasons),regime['ret15_pct'],regime['ret30_pct'])
                return None
        q=flow if long else 1-flow; bi=book if long else 1-book; score=min(.3*min(vr/5,1)+.2*min(abs(m5)/.05,1)+.25*q+.15*max((bi-.5)*2,0)+.1,1)
        long_score_min=F('PUMP_LONG_MIN_SCORE',0.0)
        if side=='long' and long_score_min>0 and score<long_score_min:
            self._diag('long_score_failed'); logging.info('LONG REJECT | %s | reasons=score | score=%.3f | min=%.3f',s,score,long_score_min); return None
        direction=1 if side=='long' else -1
        atr_distance=a*(self.sl if side=='long' else self.ssl)
        lookback=max(2,I('STRUCTURE_STOP_LOOKBACK',5))
        buffer_atr=F('STRUCTURE_STOP_BUFFER_ATR',0.20)
        if side=='long':
            structural=max(0.0,p-(float(np.min(l[-lookback-1:-1]))-a*buffer_atr))
        else:
            structural=max(0.0,(float(np.max(h[-lookback-1:-1]))+a*buffer_atr)-p)
        stop_distance=max(atr_distance,structural)
        max_stop=a*F('MAX_STOP_ATR_MULT',3.0)
        if stop_distance>max_stop:
            self._diag('structure_stop_too_wide')
            logging.info('SIGNAL REJECT | %s | side=%s | reason=structure_stop_too_wide | stop_atr=%.2f | max=%.2f',
                         s,side,stop_distance/max(a,1e-12),max_stop/max(a,1e-12))
            return None
        logging.info('SIGNAL FEATURES | %s | side=%s | score=%.3f | rsi=%.2f | vol=%.3f | flow=%.3f | book=%.3f | vwap=%.3f%% | m1=%.3f%% | m3=%.3f%% | m5=%.3f%% | atr=%.8f | stop_atr=%.2f | btc15=%.3f%% | btc30=%.3f%% | spread=%.4f%%',
                     s,side,score,r,vr,flow,book,vd,m1*100,m3*100,m5*100,a,stop_distance/max(a,1e-12),
                     regime.get('ret15_pct',0),regime.get('ret30_pct',0),sp)
        return Signal(s,side,p,a,r,vr,flow,book,vd,m5*100,score,'continuation' if long else 'exhaustion',m1*100,m3*100,sp,0.,stop_distance)
    def _journal_raw(self,event,extra=None):
        path=os.getenv('TRADE_JOURNAL_PATH','data/trades.jsonl')
        os.makedirs(os.path.dirname(path) or '.',exist_ok=True)
        rec={'ts':time.time(),'event':event,**(extra or {})}
        line=json.dumps(rec,default=str,separators=(',',':'))
        with open(path,'a',encoding='utf8') as f:f.write(line+'\n')
        logging.info('TRADE JOURNAL | %s',line)

    def _restore_risk_state(self):
        path=os.getenv('TRADE_JOURNAL_PATH','data/trades.jsonl')
        if not os.path.exists(path):return
        today=time.strftime('%Y-%m-%d',time.gmtime())
        closes=[]; open_records={}; unresolved=set()
        try:
            with open(path,'r',encoding='utf8') as f:
                for line in f:
                    try:rec=json.loads(line)
                    except Exception:continue
                    event=rec.get('event'); trade_id=str(rec.get('trade_id') or '')
                    rid=rec.get('execution_id')
                    if rid:self.seen_execution_ids.add(str(rid))
                    ts=float(rec.get('ts') or 0)
                    day=time.strftime('%Y-%m-%d',time.gmtime(ts)) if ts else ''
                    if event=='RISK_DAY_START' and day==today:
                        self.day_start_equity=float(rec.get('equity') or 0) or self.day_start_equity
                    if event=='EXECUTION_FILL' and day==today:
                        self.day_realized+=float(rec.get('net_pnl') or 0)
                    if event=='TRADE_CLOSE':
                        closes.append(float(rec.get('realized_pnl') or 0))
                        if trade_id:open_records.pop(trade_id,None); unresolved.discard(trade_id)
                    elif rec.get('schema_version')==2 and trade_id and event in ('TRADE_OPEN','PROTECTION_SET','STOP_UPDATE','POSITION_REDUCED'):
                        open_records[trade_id]=rec
                    if event=='TRADE_CLOSE_UNRESOLVED' and trade_id:
                        unresolved.add(trade_id)
            losses=0
            for pnl in reversed(closes):
                if pnl<0:losses+=1
                else:break
            self.losses=losses; self.closed_trades=len(closes)
            for trade_id,rec in open_records.items():
                try:
                    p=Position(
                        symbol=rec['symbol'],side=rec['side'],entry=float(rec.get('entry') or rec.get('entry_price')),
                        qty=float(rec.get('current_qty') or rec.get('qty') or rec.get('entry_qty')),
                        stop=float(rec.get('stop') or rec.get('sl_price') or 0),
                        tp1=float(rec.get('tp1') or rec.get('tp1_price') or 0),
                        tp2=float(rec.get('tp2') or rec.get('tp2_price') or 0),
                        tp3=float(rec.get('tp3') or rec.get('tp3_price') or 0),
                        risk=float(rec.get('risk')) if rec.get('risk') is not None else None,
                    )
                    p.trade_id=trade_id
                    p.remaining=float(rec.get('remaining') or 1.0)
                    p.tp1_done=bool(rec.get('tp1_done',False)); p.tp2_done=bool(rec.get('tp2_done',False))
                    for key in ('signal_time','order_time','fill_time','entry_price','entry_qty','tp1_price','tp2_price','tp3_price','sl_price',
                                'exit_price','exit_time','exit_reason','realized_pnl','fees','mfe_pct','mae_pct','duration_sec','ml_probability','rsi',
                                'volume_ratio','flow','book','spread','vwap_distance_pct','move1_pct','move3_pct','move5_pct','trade_status',
                                'entry_fees','exit_fees','initial_qty','current_qty','exit_filled_qty','tp_hits','stop_moved_to_be','profit_protected','trailing_armed','trailing_distance','tp_order_ids','tp_plan'):
                        if key in rec:setattr(p,key,rec.get(key))
                    p.seen_execution_ids=set()
                    self.pos[p.symbol]=p
                except Exception:
                    logging.exception('POSITION STATE RESTORE FAILED | trade_id=%s',trade_id)
            if unresolved:
                self.halted=True
                logging.error('RISK STATE RESTORED | unresolved trade closes=%s | new entries halted',len(unresolved))
            logging.info('RISK STATE RESTORED | day_realized=%.6f | day_start_equity=%s | consecutive_losses=%s | seen_executions=%s | open_positions=%s',
                         self.day_realized,self.day_start_equity,self.losses,len(self.seen_execution_ids),len(self.pos))
        except Exception:
            logging.exception('RISK STATE RESTORE FAILED')

    def _ensure_risk_day(self,equity=None):
        today=time.strftime('%Y-%m-%d',time.gmtime())
        if self.day!=today:
            self.day=today; self.day_realized=0.0; self.day_start_equity=None
        if self.day_start_equity is None and equity and equity>0:
            self.day_start_equity=float(equity)
            self._journal_raw('RISK_DAY_START',{'day':today,'equity':self.day_start_equity})

    def risk_block_reason(self,equity=None):
        if not B('CIRCUIT_BREAKER_ENABLED',True):return None
        self._ensure_risk_day(equity)
        if self.halted:return 'manual_halt'
        if self.maxloss>0 and self.losses>=self.maxloss:return f'consecutive_losses:{self.losses}/{self.maxloss}'
        if self.day_start_equity and self.dayloss>0:
            limit=self.day_start_equity*self.dayloss
            if self.day_realized<=-limit:return f'daily_loss:{self.day_realized:.6f}<=-{limit:.6f}'
        return None

    def record_exit_fill(self,net_pnl):
        self._ensure_risk_day()
        delta=float(net_pnl or 0)
        self.day_realized+=delta; self.realized+=delta

    def record_trade_close(self,realized_pnl):
        pnl=float(realized_pnl or 0); self.closed_trades+=1
        self.losses=self.losses+1 if pnl<0 else 0
        logging.info('RISK UPDATE | trade_pnl=%.6f | day_realized=%.6f | consecutive_losses=%s/%s',
                     pnl,self.day_realized,self.losses,self.maxloss)

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
        e=self.equity(); block=self.risk_block_reason(e)
        if block:
            logging.warning('ORDER BLOCKED | circuit_breaker | symbol=%s | reason=%s | day_realized=%.6f | consecutive_losses=%s',s.symbol,block,self.day_realized,self.losses)
            self._journal_raw('RISK_BLOCK',{'symbol':s.symbol,'reason':block,'day_realized':self.day_realized,'consecutive_losses':self.losses,'equity':e})
            if self.alert:self.alert(f'🛑 RISK BLOCK | {s.symbol} | {block}')
            return
        logging.info('RISK SNAPSHOT | symbol=%s | equity=%.6f | day_start_equity=%s | day_realized=%.6f | consecutive_losses=%s/%s | risk_per_trade=%.3f%%',
                     s.symbol,e,self.day_start_equity,self.day_realized,self.losses,self.maxloss,self.risk*100)
        d=float(getattr(s,'stop_distance',0) or 0) or s.atr*(self.ssl if s.side=='short' else self.sl); q=e*self.risk/d if e and d else 0; m=self.c.market(s.symbol); amin=float(((m.get('limits',{}).get('amount') or {}).get('min')) or 0); q=float(self.c.amount_to_precision(s.symbol,max(q,amin)))
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
        p.exit_price=None; p.exit_time=None; p.exit_reason=None; p.realized_pnl=0.0; p.fees=0.0; p.entry_fees=0.0; p.exit_fees=0.0; p.initial_qty=q; p.current_qty=q; p.exit_filled_qty=0.0; p.seen_execution_ids=set(); p.tp_hits=[]; p.stop_moved_to_be=False; p.profit_protected=False; p.trailing_armed=False; p.trailing_distance=0.0; p.tp_order_ids={}; p.tp_plan={}
        p.mfe_pct=0.0; p.mae_pct=0.0; p.duration_sec=None
        p.ml_probability=s.ml_prob; p.rsi=s.rsi; p.volume_ratio=s.vol; p.flow=s.flow; p.book=s.book; p.spread=s.spread
        p.vwap_distance_pct=s.vwap; p.move1_pct=s.m1; p.move3_pct=s.m3; p.move5_pct=s.move5
        p.trade_status='OPEN'
        self.journal('TRADE_OPEN',p,{'schema_version':3,'signal':asdict(s),'order_id':o.get('id')})
    def journal(self,event,p,extra=None):
        path=os.getenv('TRADE_JOURNAL_PATH','data/trades.jsonl')
        os.makedirs(os.path.dirname(path) or '.',exist_ok=True)
        d=asdict(p)
        for k in ('trade_id','signal_time','order_time','fill_time','entry_price','entry_qty','tp1_price','tp2_price','tp3_price','sl_price','tp1_fill_qty','tp2_fill_qty','tp3_fill_qty','exit_price','exit_time','exit_reason','realized_pnl','fees','mfe_pct','mae_pct','duration_sec','ml_probability','rsi','volume_ratio','flow','book','spread','vwap_distance_pct','move1_pct','move3_pct','move5_pct','trade_status','entry_fees','exit_fees','initial_qty','current_qty','exit_filled_qty','tp_hits','stop_moved_to_be','profit_protected','trailing_armed','trailing_distance','tp_order_ids','tp_plan'):
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
                required=self.short_confirm if sig.side=='short' else self.long_confirm
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
