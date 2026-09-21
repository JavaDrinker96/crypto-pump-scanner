"""Runtime patch: Bybit WS market data, pretrained Chronos filter and native protection."""
import logging
import os

try:
    from chronos_filter import AsyncChronosForecastFilter
except Exception:
    AsyncChronosForecastFilter=None

try:
    import advanced_engine
except Exception:
    logging.exception('RUNTIME PATCH | advanced_engine import failed')
else:
    _init=advanced_engine.Engine.__init__
    _signal=advanced_engine.Engine.signal
    _open=advanced_engine.Engine.open
    _ohlcv=advanced_engine.Engine.ohlcv
    _flow=advanced_engine.Engine.flow
    _book=advanced_engine.Engine.book

    def _enabled(name,default='true'):
        return os.getenv(name,default).lower() in ('1','true','yes','on')

    def _notify(self,text):
        try:
            if self.alert:self.alert(text)
        except Exception:
            logging.exception('ALERT FAILED | %s',text)

    def _patched_init(self,client,alert=None):
        _init(self,client,alert)
        self.ws=getattr(client,'ws_market',None)
        self.chronos=None
        self.chronos_required=_enabled('CHRONOS_REQUIRED',os.getenv('ML_REQUIRED','true'))
        enabled=_enabled('CHRONOS_ENABLED',os.getenv('ML_ENABLED','true'))
        if self.ws:logging.info('MARKET DATA | Bybit public WebSocket enabled')
        if enabled and AsyncChronosForecastFilter:
            try:
                self.chronos=AsyncChronosForecastFilter()
                logging.info('CHRONOS STATUS | enabled=true | mode=lazy_async | loaded=false | required=%s | model=%s | max_pending=%s',
                             self.chronos_required,self.chronos.model_id,self.chronos.max_pending)
            except Exception:
                logging.exception('CHRONOS INIT FAILED | trading candidates will be blocked when required')
        if self.chronos is None:
            logging.error('CHRONOS STATUS | enabled=%s | loaded=false | required=%s',enabled,self.chronos_required)

    def _ws_ohlcv(self,s,n=120):
        if getattr(self,'ws',None):
            return self.ws.get_ohlcv(s,lambda sym,lim:_ohlcv(self,sym,lim),n)
        return _ohlcv(self,s,n)

    def _ws_flow(self,s):
        if getattr(self,'ws',None):
            trades=self.ws.get_trades(s)
            if trades:
                buys=sum(x['amount'] for x in trades if x['side']=='buy')
                sells=sum(x['amount'] for x in trades if x['side']=='sell')
                return buys/max(buys+sells,1e-12)
        return _flow(self,s)

    def _ws_book(self,s,p):
        if getattr(self,'ws',None):
            orderbook=self.ws.get_order_book(s)
            if orderbook and orderbook.get('bids') and orderbook.get('asks'):
                depth=self.depth
                bids=sum(float(q)*float(px) for px,q in orderbook['bids'] if float(px)>=p*(1-depth/100))
                asks=sum(float(q)*float(px) for px,q in orderbook['asks'] if float(px)<=p*(1+depth/100))
                spread=(float(orderbook['asks'][0][0])-float(orderbook['bids'][0][0]))/p*100
                return bids/max(asks+bids,1e-12),spread
        return _book(self,s,p)

    def _patched_signal(self,symbol):
        sig=_signal(self,symbol)
        if sig is None:return None
        if self.chronos is None:
            if self.chronos_required:
                self._diag('chronos_unavailable')
                logging.warning('CHRONOS BLOCK | %s | side=%s | async filter unavailable',symbol,sig.side)
                self._journal_raw('MODEL_DECISION',{
                    'schema_version':3,'symbol':symbol,'side':sig.side,'model':'chronos',
                    'allowed':False,'reason':'model_unavailable',
                })
                return None
            return sig
        try:
            rows=(getattr(self,'signal_rows_cache',{}) or {}).get(symbol) or self.ohlcv(symbol,120)
            sl_mult=self.ssl if sig.side=='short' else self.sl
            result=self.chronos.request(symbol,rows,sig,sl_mult)
            state=result.get('state')
            if state in ('pending','busy'):
                self._diag('chronos_pending' if state=='pending' else 'chronos_busy')
                logging.info('CHRONOS %s | %s | side=%s | reason=%s | wait_ms=%.1f',
                             state.upper(),symbol,sig.side,result.get('reason',''),
                             float(result.get('wait_ms') or 0))
                if result.get('reason')=='scheduled':
                    self._journal_raw('MODEL_PENDING',{
                        'schema_version':3,'symbol':symbol,'side':sig.side,'model':self.chronos.model_id,
                        'reason':'scheduled','candle_ts':result.get('fingerprint',[None,None,None])[2],
                    })
                return None
            if state=='error':
                self._diag('chronos_inference_failed')
                logging.error('CHRONOS RESULT ERROR | %s | side=%s | error=%s',symbol,sig.side,result.get('error'))
                self._journal_raw('MODEL_DECISION',{
                    'schema_version':3,'symbol':symbol,'side':sig.side,'model':self.chronos.model_id,
                    'allowed':False,'reason':result.get('reason','worker_failed'),'error':result.get('error'),
                })
                return None if self.chronos_required else sig
            if state!='ready':
                self._diag('chronos_unknown_state')
                logging.warning('CHRONOS BLOCK | %s | side=%s | unexpected_state=%s',symbol,sig.side,state)
                return None if self.chronos_required else sig

            sig.ml_prob=float(result['barrier_support'])
            payload={
                'schema_version':3,'symbol':symbol,'side':sig.side,'model':result['model_id'],
                'allowed':result['allowed'],'failed':result['failed'],
                'barrier_support':result['barrier_support'],'resolved_support':result['resolved_support'],
                'direction_support':result['direction_support'],'median_mfe_r':result['median_mfe_r'],
                'median_mae_r':result['median_mae_r'],'median_terminal_r':result['median_terminal_r'],
                'latency_ms':result['latency_ms'],'queue_to_result_ms':result.get('queue_to_result_ms'),
                'horizon':result['horizon'],'context':result['context'],
            }
            self._journal_raw('MODEL_DECISION',payload)
            logging.info('CHRONOS DECISION | %s | side=%s | allow=%s | barrier=%.3f | direction=%.3f | median_mfe=%.3fR | median_mae=%.3fR | terminal=%.3fR | inference=%.1fms | total_worker=%.1fms | failed=%s',
                         symbol,sig.side,result['allowed'],result['barrier_support'],result['direction_support'],
                         result['median_mfe_r'],result['median_mae_r'],result['median_terminal_r'],
                         result['latency_ms'],float(result.get('queue_to_result_ms') or result['latency_ms']),
                         ','.join(result['failed']) or 'none')
            if not result['allowed']:
                self._diag('chronos_rejected')
                return None
            return sig
        except Exception:
            self._diag('chronos_request_failed')
            logging.exception('CHRONOS REQUEST FAILED | %s',symbol)
            self._journal_raw('MODEL_DECISION',{
                'schema_version':3,'symbol':symbol,'side':sig.side,'model':'chronos',
                'allowed':False,'reason':'request_failed',
            })
            if self.chronos_required:return None
            return sig

    def _protect(self,p):
        """Fallback protection. position_sync replaces this with exact managed protection."""
        params={
            'category':'linear',
            'symbol':self.c.market(p.symbol).get('id') or p.symbol.replace('/','').replace(':USDT',''),
            'positionIdx':0,'tpslMode':'Full','stopLoss':str(p.stop),
            'slOrderType':'Market','slTriggerBy':'MarkPrice',
        }
        result=self.c.request('v5/position/trading-stop','private','POST',params)
        if not isinstance(result,dict) or result.get('retCode',0)!=0:
            raise RuntimeError(f'Bybit fallback stop failed: {result}')
        logging.warning('PROTECTION FALLBACK | %s | only full SL installed; exact TP manager not loaded',p.symbol)

    def _open_with_protection(self,s):
        before=set(self.pos)
        _open(self,s)
        if s.symbol not in self.pos or s.symbol in before:return
        p=self.pos[s.symbol]
        try:
            _protect(self,p)
        except Exception:
            logging.exception('PROTECTION FAILED | symbol=%s | closing immediately',p.symbol)
            try:
                q=float(self.c.amount_to_precision(p.symbol,p.qty))
                if q>0:
                    self.c.create_order(
                        p.symbol,'market','sell' if p.side=='long' else 'buy',q,None,
                        {'reduceOnly':True,'positionIdx':0}
                    )
            finally:
                self.pos.pop(s.symbol,None)
            raise

    advanced_engine.Engine.__init__=_patched_init
    advanced_engine.Engine.ohlcv=_ws_ohlcv
    advanced_engine.Engine.flow=_ws_flow
    advanced_engine.Engine.book=_ws_book
    advanced_engine.Engine.signal=_patched_signal
    advanced_engine.Engine.open=_open_with_protection
    advanced_engine.Engine.manage=lambda self:None
    logging.info('RUNTIME PATCH | WS market data + lazy async Chronos-Bolt filter + fail-closed inference + native protection hook')
