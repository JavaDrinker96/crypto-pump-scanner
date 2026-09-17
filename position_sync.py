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
        positions=self.c.fetch_positions(params={'category':'linear'})
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
    amin=float(((self.c.market(p.symbol).get('limits',{}).get('amount') or {}).get('min')) or 0)
    q1=float(self.c.amount_to_precision(p.symbol,total*float(os.getenv('TP1_CLOSE_PCT','.35'))))
    q2=float(self.c.amount_to_precision(p.symbol,total*float(os.getenv('TP2_CLOSE_PCT','.35'))))
    q3=float(self.c.amount_to_precision(p.symbol,max(0,total-q1-q2)))

    # Precision rounding can otherwise leave a tiny unprotected remainder.
    # If the remainder cannot be a valid standalone TP, fold it into TP2;
    # if the position is too small for three legs, use one TP for the full size.
    if q3>0 and q3<amin:
        q2=float(self.c.amount_to_precision(p.symbol,q2+q3));q3=0
    if q2>0 and q2<amin:
        q1=float(self.c.amount_to_precision(p.symbol,q1+q2+q3));q2=q3=0
    if q1>0 and q1<amin:
        q1=total;q2=q3=0

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
