"""Reconcile local position state with Bybit and make native TP quantities sum exactly."""
import logging, os

import sitecustomize
from advanced_engine import Engine, Position


def _size(pos):
    try:
        v=pos.get('contracts')
        if v is None:
            v=(pos.get('info') or {}).get('size')
        return abs(float(v or 0))
    except Exception:
        return 0.0


def _side(pos):
    s=str(pos.get('side') or '').lower()
    if s in ('long','short'):
        return s
    raw=str((pos.get('info') or {}).get('side') or '').lower()
    return 'short' if raw=='sell' else ('long' if raw=='buy' else 'long')


def sync(self):
    try:
        positions=self.c.fetch_positions(params={'category':'linear','settleCoin':'USDT'})
    except Exception as e:
        logging.warning('POSITION SYNC FAILED | %s',e)
        return False

    live={}
    for p in positions or []:
        symbol=p.get('symbol')
        size=_size(p)
        if symbol and size>0:
            live[symbol]=(size,_side(p),float(p.get('entryPrice') or (p.get('info') or {}).get('avgPrice') or 0))

    for symbol in list(self.pos):
        if symbol not in live:
            logging.info('POSITION SYNC | symbol=%s | exchange_size=0 | local_cleared=true',symbol)
            self.pos.pop(symbol,None)
            self.pending.pop(symbol,None)

    for symbol,(size,side,entry) in live.items():
        if symbol in self.pos:
            p=self.pos[symbol]
            old=p.qty
            p.qty=size
            p.side=side
            if old and abs(size-old)/old>0.005:
                logging.info('POSITION SYNC | symbol=%s | local_qty=%s | exchange_qty=%s',symbol,old,size)
        else:
            self.pos[symbol]=Position(symbol,side,entry,size,0,0,0,0)
            logging.warning('POSITION SYNC | external/open position detected | symbol=%s | side=%s | qty=%s',symbol,side,size)
            if self.alert:self.alert(f'🟡 POSITION SYNC | {symbol} | exchange position detected | qty={size}')
    return True


def _protect_exact(self,p):
    def req(params):
        return self.c.request('v5/position/trading-stop','private','POST',params)

    symbol=self.c.market(p.symbol).get('id') or p.symbol.replace('/','').replace(':USDT','')
    total=float(self.c.amount_to_precision(p.symbol,p.qty))
    market=self.c.market(p.symbol)
    limits=market.get('limits',{}).get('amount') or {}
    amin=float(limits.get('min') or 0)
    step=float(((market.get('info') or {}).get('lotSizeFilter') or {}).get('qtyStep') or 0)
    if step<=0: step=amin or 1.0

    def floor_step(v):
        return max(0.0, int((v/step)+1e-9)*step)

    # Allocate integer qty steps so every submitted leg is valid and the sum equals position qty.
    raw_total=float(p.qty)
    total=floor_step(raw_total)
    if total<amin:
        raise RuntimeError(f'Position qty below exchange minimum: qty={total} min={amin}')
    min_steps=max(1,int(round(amin/step)))
    total_steps=max(1,int(round(total/step)))
    q1=q2=q3=0.0
    n1=int(total_steps*float(os.getenv('TP1_CLOSE_PCT','.35')))
    n2=int(total_steps*float(os.getenv('TP2_CLOSE_PCT','.35')))
    if total_steps < 3*min_steps:
        q3=total
    else:
        n1=max(min_steps,n1); n2=max(min_steps,n2)
        if n1+n2>total_steps-min_steps:
            n1=min_steps; n2=min_steps
        n3=total_steps-n1-n2
        if n3<min_steps:
            n3=min_steps
            n2=max(min_steps,total_steps-n1-n3)
        q1=n1*step; q2=n2*step; q3=n3*step
    qsum=q1+q2+q3
    if abs(qsum-total)>step*0.01:
        raise RuntimeError(f'TP allocation mismatch: total={total} sum={qsum} step={step} min={amin}')
    logging.info('PROTECTION PLAN | symbol=%s | entry_qty=%s | min_qty=%s | qty_step=%s | TP1 qty=%s price=%s | TP2 qty=%s price=%s | TP3 qty=%s price=%s | SL=%s | sum=%s',
                 p.symbol,total,amin,step,q1,p.tp1,q2,p.tp2,q3,p.tp3,p.stop,qsum)
    legs=[('TP1',p.tp1,q1),('TP2',p.tp2,q2),('TP3',p.tp3,q3)]
    for name,tp,qty in legs:
        if qty<=0: continue
        params={'category':'linear','symbol':symbol,'positionIdx':0,'tpslMode':'Partial','takeProfit':str(tp),'stopLoss':str(p.stop),'tpSize':str(qty),'slSize':str(qty),'tpOrderType':'Market','slOrderType':'Market','tpTriggerBy':'MarkPrice','slTriggerBy':'MarkPrice'}
        r=req(params)
        if not isinstance(r,dict) or r.get('retCode',0)!=0:
            raise RuntimeError(f'Bybit TP/SL failed: {r}')
        logging.info('PROTECTION SET | %s | %s qty=%s tp=%s sl=%s',p.symbol,name,qty,tp,p.stop)
    logging.info('PROTECTION TOTAL | %s | qty=%s | tp_qty_sum=%s',p.symbol,total,sum(x[2] for x in legs))
    self.journal('protection',p,{'mode':'TP1+TP2+TP3_exchange_native_exact_qty'})


_original_run=Engine.run

def _run(self,symbols):
    sync(self)
    return _original_run(self,symbols)

Engine.run=_run
sitecustomize._protect=_protect_exact
logging.info('POSITION SYNC | exchange reconciliation enabled; exact TP quantity allocation enabled')
