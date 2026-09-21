"""Exchange reconciliation, execution accounting and native protection management."""
import logging
import os
import time

import sitecustomize
from advanced_engine import Engine, Position
from trade_utils import allocate_tp_quantities, breakeven_stop


def _size(pos):
    try:
        value=pos.get('contracts')
        if value is None:
            value=(pos.get('info') or {}).get('size')
        return abs(float(value or 0))
    except Exception:
        return 0.0


def _side(pos):
    side=str(pos.get('side') or '').lower()
    if side in ('long','short'):
        return side
    raw=str((pos.get('info') or {}).get('side') or '').lower()
    return 'short' if raw=='sell' else ('long' if raw=='buy' else 'long')


def _market_id(client,symbol):
    return client.market(symbol).get('id') or symbol.replace('/','').replace(':USDT','')


def _set_full_stop(self,p,stop,reason):
    params={
        'category':'linear','symbol':_market_id(self.c,p.symbol),'positionIdx':0,
        'tpslMode':'Full','stopLoss':str(stop),'slOrderType':'Market','slTriggerBy':'MarkPrice',
    }
    result=self.c.request('v5/position/trading-stop','private','POST',params)
    if not isinstance(result,dict) or result.get('retCode',0)!=0:
        raise RuntimeError(f'Bybit full stop update failed: {result}')
    old=float(getattr(p,'stop',0) or 0); p.stop=float(stop); p.sl_price=float(stop)
    logging.info('STOP UPDATE | %s | reason=%s | old=%s | new=%s',p.symbol,reason,old,stop)
    self.journal('STOP_UPDATE',p,{'schema_version':2,'reason':reason,'old_stop':old,'new_stop':float(stop)})


def _create_tp_order(self,p,name,price,qty):
    if qty<=0:return None
    exit_side='sell' if p.side=='long' else 'buy'
    trigger_direction=1 if p.side=='long' else 2
    link=f"{str(getattr(p,'trade_id','trade'))[:20]}-{name.lower()}"[:36]
    params={
        'category':'linear','positionIdx':0,'reduceOnly':True,'closeOnTrigger':True,
        'triggerPrice':str(price),'triggerDirection':trigger_direction,'triggerBy':'MarkPrice',
        'orderLinkId':link,
    }
    order=self.c.create_order(p.symbol,'market',exit_side,float(qty),None,params)
    order_id=str(order.get('id') or (order.get('info') or {}).get('orderId') or '')
    logging.info('TP ORDER SET | %s | %s qty=%s trigger=%s id=%s',p.symbol,name,qty,price,order_id)
    return order_id


def _cancel_tp_orders(self,p):
    for order_id in list((getattr(p,'tp_order_ids',{}) or {}).values()):
        if not order_id:continue
        try:self.c.cancel_order(order_id,p.symbol,params={'category':'linear'})
        except Exception:logging.info('TP ORDER CLEANUP | %s | id=%s | already gone/unavailable',p.symbol,order_id)


def _protect_exact(self,p):
    market=self.c.market(p.symbol)
    limits=market.get('limits',{}).get('amount') or {}
    minimum=float(limits.get('min') or 0)
    step=float(((market.get('info') or {}).get('lotSizeFilter') or {}).get('qtyStep') or 0)
    if step<=0:step=minimum or 1.0
    total_steps=int((float(p.qty)/step)+1e-9)
    total=total_steps*step
    if total<minimum:
        raise RuntimeError(f'Position qty below exchange minimum: qty={total} min={minimum}')
    q1,q2,q3=allocate_tp_quantities(
        total,minimum,step,
        float(os.getenv('TP1_CLOSE_PCT','.35')),
        float(os.getenv('TP2_CLOSE_PCT','.35')),
    )
    logging.info('PROTECTION PLAN | symbol=%s | mode=full_sl+conditional_tp | entry_qty=%s | min_qty=%s | qty_step=%s | TP1 qty=%s price=%s | TP2 qty=%s price=%s | TP3 qty=%s price=%s | SL=%s | sum=%s',
                 p.symbol,total,minimum,step,q1,p.tp1,q2,p.tp2,q3,p.tp3,p.stop,q1+q2+q3)
    _set_full_stop(self,p,p.stop,'initial')
    ids={}
    for name,price,qty in [('TP1',p.tp1,q1),('TP2',p.tp2,q2),('TP3',p.tp3,q3)]:
        oid=_create_tp_order(self,p,name,price,qty)
        if oid:ids[name]=oid
    p.tp_order_ids=ids
    p.tp_plan={'TP1':q1,'TP2':q2,'TP3':q3}
    self.journal('PROTECTION_SET',p,{
        'schema_version':2,'mode':'full_position_sl+reduce_only_conditional_tps',
        'tp_plan':p.tp_plan,'tp_order_ids':ids,
    })


def _execution_id(trade):
    info=trade.get('info') or {}
    return str(trade.get('id') or info.get('execId') or
               f"{trade.get('order')}-{trade.get('timestamp')}-{trade.get('side')}-{trade.get('amount')}-{trade.get('price')}")


def _fee(trade):
    fee=trade.get('fee') or {}
    if fee.get('cost') is not None:
        try:return float(fee.get('cost') or 0)
        except Exception:pass
    info=trade.get('info') or {}
    for key in ('execFee','fee','execFeeV2'):
        if info.get(key) not in (None,''):
            try:return float(info.get(key) or 0)
            except Exception:pass
    return 0.0


def _classify_exit(p,trade):
    info=trade.get('info') or {}
    order_link=str(info.get('orderLinkId') or '').lower()
    raw=' '.join(str(info.get(k) or '') for k in ('stopOrderType','createType','orderType','execType','orderLinkId')).lower()
    price=float(trade.get('price') or info.get('execPrice') or 0)
    targets={'TP1':float(p.tp1 or 0),'TP2':float(p.tp2 or 0),'TP3':float(p.tp3 or 0),'SL':float(p.stop or 0)}
    for name in ('TP1','TP2','TP3'):
        if f'-{name.lower()}' in order_link:
            return name
    if 'trailingstop' in raw or 'trailing_stop' in raw:
        return 'TRAILING_SL'
    if 'stoploss' in raw or 'stop_loss' in raw:
        return 'SL'
    nearest=min((name for name,val in targets.items() if val>0),key=lambda name:abs(price-targets[name]),default='EXIT')
    if 'takeprofit' in raw or 'take_profit' in raw:
        return nearest if nearest.startswith('TP') else 'TP'
    risk=abs(float(getattr(p,'risk',0) or 0))
    if risk>0 and nearest in targets and abs(price-targets[nearest])<=risk*.65:
        return nearest
    return 'EXIT'


def _update_excursion(p,mark):
    if not mark or not getattr(p,'entry',0):return
    move=(float(mark)/float(p.entry)-1)*100
    favorable=move if p.side=='long' else -move
    adverse=-move if p.side=='long' else move
    p.mfe_pct=max(float(getattr(p,'mfe_pct',0) or 0),favorable)
    p.mae_pct=max(float(getattr(p,'mae_pct',0) or 0),adverse)


def _reconcile_executions(self,p):
    since=int((float(getattr(p,'fill_time',time.time()))-10)*1000)
    try:
        trades=self.c.fetch_my_trades(p.symbol,since=since,limit=100,params={'category':'linear'})
        if not isinstance(trades,(list,tuple)):
            logging.warning('EXECUTION RECONCILE SKIPPED | %s | unexpected response type=%s',p.symbol,type(trades).__name__)
            return 0.0
    except Exception:
        logging.exception('EXECUTION RECONCILE FAILED | %s',p.symbol)
        return 0.0
    added_exit_qty=0.0
    entry_side='buy' if p.side=='long' else 'sell'
    direction=1 if p.side=='long' else -1
    if not hasattr(p,'seen_execution_ids'):p.seen_execution_ids=set()
    if not hasattr(p,'entry_fees'):p.entry_fees=0.0
    if not hasattr(p,'exit_fees'):p.exit_fees=0.0
    if not hasattr(p,'realized_pnl') or p.realized_pnl is None:p.realized_pnl=0.0
    if not hasattr(p,'exit_filled_qty'):p.exit_filled_qty=0.0
    if not hasattr(p,'tp_hits'):p.tp_hits=[]
    for trade in sorted(trades or [],key=lambda t:float(t.get('timestamp') or 0)):
        eid=_execution_id(trade)
        if eid in self.seen_execution_ids or eid in p.seen_execution_ids:continue
        side=str(trade.get('side') or '').lower()
        qty=abs(float(trade.get('amount') or (trade.get('info') or {}).get('execQty') or 0))
        price=float(trade.get('price') or (trade.get('info') or {}).get('execPrice') or 0)
        if qty<=0 or price<=0:continue
        fee=_fee(trade)
        classification='entry' if side==entry_side else 'exit'
        reason='ENTRY'
        gross=0.0
        net=-fee
        if classification=='entry':
            p.entry_fees+=fee
        else:
            reason=_classify_exit(p,trade)
            gross=(price-float(p.entry))*qty*direction
            net=gross-fee
            p.exit_fees+=fee; p.realized_pnl+=net; p.exit_filled_qty+=qty; added_exit_qty+=qty
            if reason.startswith('TP') and reason not in p.tp_hits:p.tp_hits.append(reason)
            if reason=='TP1':p.tp1_done=True
            if reason=='TP2':p.tp2_done=True
            p.exit_reason=reason; p.exit_price=price
        self.record_exit_fill(net)
        self.seen_execution_ids.add(eid); p.seen_execution_ids.add(eid)
        self._journal_raw('EXECUTION_FILL',{
            'schema_version':2,'trade_id':getattr(p,'trade_id',None),'execution_id':eid,
            'order_id':trade.get('order') or (trade.get('info') or {}).get('orderId'),
            'symbol':p.symbol,'position_side':p.side,'execution_side':side,
            'classification':classification,'reason':reason,'qty':qty,'price':price,
            'fee':fee,'gross_pnl':gross,'net_pnl':net,
            'exchange_timestamp_ms':trade.get('timestamp'),
        })
        logging.info('EXECUTION FILL | %s | class=%s | reason=%s | qty=%s | price=%s | fee=%.8f | gross=%.8f | net=%.8f',
                     p.symbol,classification,reason,qty,price,fee,gross,net)
    return added_exit_qty


def _stop_improves(p,candidate):
    current=float(getattr(p,'stop',0) or 0)
    return candidate>current if p.side=='long' else candidate<current


def _set_native_trailing(self,p,mark,distance):
    try:
        distance=float(self.c.price_to_precision(p.symbol,distance))
    except Exception:
        distance=float(distance)
    if distance<=0:raise ValueError('trailing distance must be positive')
    params={
        'category':'linear','symbol':_market_id(self.c,p.symbol),'positionIdx':0,
        'tpslMode':'Full','stopLoss':str(p.stop),'slOrderType':'Market','slTriggerBy':'MarkPrice',
        'trailingStop':str(distance),
    }
    result=self.c.request('v5/position/trading-stop','private','POST',params)
    if not isinstance(result,dict) or result.get('retCode',0)!=0:
        raise RuntimeError(f'Bybit trailing stop update failed: {result}')
    p.trailing_armed=True; p.trailing_distance=distance
    logging.info('TRAILING ARMED | %s | mark=%s | distance=%s | atr_mult=%s | hard_stop=%s',
                 p.symbol,mark,distance,getattr(self,'trail',None),p.stop)
    self.journal('TRAILING_ARMED',p,{
        'schema_version':3,'mark':float(mark),'distance':distance,
        'atr_mult':float(getattr(self,'trail',0) or 0),'hard_stop':float(p.stop),
    })


def _manage_dynamic_protection(self,p,remaining,mark):
    if remaining<=0 or not mark:return
    direction=1 if p.side=='long' else -1
    risk=max(abs(float(getattr(p,'risk',0) or 0)),1e-12)
    current_r=direction*(float(mark)-float(p.entry))/risk

    if not getattr(p,'tp1_done',False) and not getattr(p,'profit_protected',False):
        trigger=float(os.getenv('PROFIT_PROTECT_TRIGGER_R','.50'))
        stop_r=float(os.getenv('PROFIT_PROTECT_STOP_R','-.10'))
        if current_r>=trigger:
            candidate=float(p.entry)+direction*risk*stop_r
            if _stop_improves(p,candidate):
                _set_full_stop(self,p,candidate,'profit_protect')
            p.profit_protected=True
            logging.info('PROFIT PROTECT ARMED | %s | current=%.3fR | trigger=%.3fR | stop_target=%.3fR | stop=%s',
                         p.symbol,current_r,trigger,stop_r,p.stop)
            self.journal('PROFIT_PROTECT_ARMED',p,{
                'schema_version':3,'current_r':current_r,'trigger_r':trigger,'stop_r':stop_r,
            })

    if getattr(p,'tp1_done',False) and not getattr(p,'stop_moved_to_be',False):
        new_stop=breakeven_stop(
            p.entry,p.side,p.stop,float(os.getenv('BREAKEVEN_FEE_BUFFER_PCT','.06'))
        )
        if _stop_improves(p,new_stop):
            _set_full_stop(self,p,new_stop,'tp1_breakeven')
        p.stop_moved_to_be=True
        logging.info('BREAKEVEN ARMED | %s | remaining=%s | stop=%s',p.symbol,remaining,p.stop)

    trailing_after=max(0,int(os.getenv('TRAILING_AFTER_TP','2')))
    if trailing_after and getattr(p,'tp2_done',False) and not getattr(p,'trailing_armed',False):
        rows=self.ohlcv(p.symbol,30)
        atr=float(self.atr(rows))
        distance=atr*float(os.getenv('TRAILING_ATR_MULT',str(getattr(self,'trail',1.5))))
        _set_native_trailing(self,p,mark,distance)


def _finalize_position(self,p):
    final_pnl=float(getattr(p,'realized_pnl',0) or 0)-float(getattr(p,'entry_fees',0) or 0)
    initial_qty=float(getattr(p,'initial_qty',0) or getattr(p,'entry_qty',0) or 0)
    risk_usdt=(abs(float(getattr(p,'risk',0) or 0))*initial_qty)
    r_multiple=final_pnl/risk_usdt if risk_usdt>0 else None
    p.realized_pnl=final_pnl; p.exit_time=time.time(); p.duration_sec=p.exit_time-float(getattr(p,'fill_time',p.exit_time)); p.trade_status='CLOSED'
    p.fees=float(getattr(p,'entry_fees',0) or 0)+float(getattr(p,'exit_fees',0) or 0)
    p.current_qty=0.0
    _cancel_tp_orders(self,p)
    self.record_trade_close(final_pnl)
    self.journal('TRADE_CLOSE',p,{
        'schema_version':2,'reason':getattr(p,'exit_reason',None) or 'exchange_closed',
        'realized_pnl':final_pnl,'risk_usdt':risk_usdt,'r_multiple':r_multiple,
        'entry_fees':float(getattr(p,'entry_fees',0) or 0),
        'exit_fees':float(getattr(p,'exit_fees',0) or 0),
        'exit_filled_qty':float(getattr(p,'exit_filled_qty',0) or 0),
        'tp_hits':list(getattr(p,'tp_hits',[]) or []),
        'mfe_pct':float(getattr(p,'mfe_pct',0) or 0),
        'mae_pct':float(getattr(p,'mae_pct',0) or 0),
        'pnl_source':'private_execution_history',
    })
    logging.info('TRADE CLOSED | %s | pnl=%.8f | R=%s | fees=%.8f | tp_hits=%s | mfe=%.3f%% | mae=%.3f%%',
                 p.symbol,final_pnl,'n/a' if r_multiple is None else f'{r_multiple:.3f}',
                 float(getattr(p,'entry_fees',0) or 0)+float(getattr(p,'exit_fees',0) or 0),
                 getattr(p,'tp_hits',[]),float(getattr(p,'mfe_pct',0) or 0),float(getattr(p,'mae_pct',0) or 0))


def sync(self):
    try:
        positions=self.c.fetch_positions(params={'category':'linear','settleCoin':'USDT'})
    except Exception as exc:
        logging.warning('POSITION SYNC FAILED | %s',exc)
        return False
    live={}
    for raw in positions or []:
        symbol=raw.get('symbol'); size=_size(raw)
        if symbol and size>0:
            info=raw.get('info') or {}
            mark=float(raw.get('markPrice') or info.get('markPrice') or 0)
            entry=float(raw.get('entryPrice') or info.get('avgPrice') or 0)
            stop=float(raw.get('stopLossPrice') or info.get('stopLoss') or 0)
            live[symbol]=(size,_side(raw),entry,stop,mark)

    for symbol in list(self.pos):
        p=self.pos[symbol]
        if getattr(p,'risk',None) is None:
            if symbol not in live:
                logging.warning('RECOVERED POSITION CLOSED | %s | exact PnL unavailable because original lifecycle metadata was missing',symbol)
                self._journal_raw('RECOVERED_POSITION_CLOSED',{'schema_version':2,'symbol':symbol})
                self.pos.pop(symbol,None); self.pending.pop(symbol,None)
            continue
        if symbol in live:
            size,side,entry,stop,mark=live[symbol]
            _update_excursion(p,mark)
            old=float(getattr(p,'current_qty',0) or getattr(p,'qty',0) or size)
            _reconcile_executions(self,p)
            p.current_qty=size; p.qty=size; p.side=side
            if old and abs(size-old)/old>0.005:
                logging.info('POSITION SYNC | symbol=%s | local_qty=%s | exchange_qty=%s',symbol,old,size)
                self.journal('POSITION_REDUCED',p,{'schema_version':2,'old_qty':old,'exchange_qty':size})
            _manage_dynamic_protection(self,p,size,mark)
        else:
            _reconcile_executions(self,p)
            expected=float(getattr(p,'initial_qty',0) or getattr(p,'entry_qty',0) or 0)
            filled=float(getattr(p,'exit_filled_qty',0) or 0)
            if expected>0 and filled<expected*.98:
                missing_since=float(getattr(p,'missing_since',0) or 0)
                if not missing_since:
                    p.missing_since=time.time()
                    logging.warning('POSITION CLOSED BUT EXECUTIONS PENDING | %s | reconciled_qty=%s expected=%s',symbol,filled,expected)
                    continue
                if time.time()-missing_since<30:
                    continue
                logging.error('POSITION CLOSE UNRESOLVED | %s | reconciled_qty=%s expected=%s | keeping conservative risk block',symbol,filled,expected)
                self._journal_raw('TRADE_CLOSE_UNRESOLVED',{
                    'schema_version':2,'trade_id':getattr(p,'trade_id',None),'symbol':symbol,
                    'reconciled_exit_qty':filled,'expected_qty':expected,
                })
                self.halted=True
                continue
            _finalize_position(self,p)
            logging.info('POSITION SYNC | symbol=%s | exchange_size=0 | local_cleared=true',symbol)
            self.pos.pop(symbol,None); self.pending.pop(symbol,None)

    for symbol,(size,side,entry,stop,mark) in live.items():
        if symbol in self.pos:continue
        self.pos[symbol]=Position(symbol=symbol,side=side,entry=entry,qty=size,stop=stop,tp1=0,tp2=0,tp3=0,risk=None)
        logging.warning('POSITION SYNC | external/open position detected | symbol=%s | side=%s | qty=%s',symbol,side,size)
        self._journal_raw('RECOVERED_POSITION',{'schema_version':2,'symbol':symbol,'side':side,'qty':size,'entry':entry,'stop':stop})
        if self.alert:self.alert(f'🟡 POSITION SYNC | {symbol} | exchange position detected | qty={size} | new entries paused while original risk is unknown')
    return True


_original_run=Engine.run
_original_open=Engine.open


def _open_after_sync(self,signal):
    if any(p.risk is None for p in self.pos.values()):
        logging.warning('ORDER BLOCKED | recovered position risk unknown | symbol=%s',signal.symbol)
        return
    return _original_open(self,signal)


def _run(self,symbols):
    if not sync(self):
        raise RuntimeError('Position synchronization failed; scan skipped to prevent orders on stale state')
    return _original_run(self,symbols)


Engine.run=_run
Engine.open=_open_after_sync
sitecustomize._protect=_protect_exact
logging.info('POSITION SYNC | private execution reconciliation + persistent risk + full SL/conditional TP protection enabled')
