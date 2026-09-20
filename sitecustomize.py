"""Runtime patch: consistent ML inference, Bybit WS market data and native protection."""
import logging
import os

try:
    import joblib
    import numpy as np
    from ml_pipeline import compatible, signal_features, train_model
except Exception:
    joblib=np=None
    compatible=signal_features=train_model=None

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

    def _path():
        return os.getenv('ML_MODEL_PATH','data/models/pump_classifier.joblib')

    def _notify(self,text):
        try:
            if self.alert:self.alert(text)
        except Exception:
            logging.exception('ML ALERT FAILED | %s',text)

    def _patched_init(self,client,alert=None):
        _init(self,client,alert)
        self.ml_required=os.getenv('ML_REQUIRED','true').lower() in ('1','true','yes','on')
        enabled=os.getenv('ML_ENABLED','true').lower() in ('1','true','yes','on')
        path=_path(); self.ws=getattr(client,'ws_market',None); self.ml=None
        if self.ws:logging.info('MARKET DATA | Bybit public WebSocket enabled')
        if enabled and joblib and compatible:
            try:
                if os.path.exists(path):
                    candidate=joblib.load(path)
                    if compatible(candidate):
                        self.ml=candidate
                    else:
                        logging.warning('ML MODEL INCOMPATIBLE | retraining required | path=%s',path)
            except Exception:
                logging.exception('ML MODEL LOAD FAILED | retraining')
            if self.ml is None and os.getenv('ML_AUTO_TRAIN','true').lower() in ('1','true','yes','on'):
                try:
                    self.ml=train_model(client,path)
                    logging.info('ML STATUS | enabled=true | loaded=true | source=auto-trained | version=%s | label=%s | path=%s',
                                 self.ml.get('feature_version'),self.ml.get('label'),path)
                except Exception:
                    logging.exception('ML AUTO TRAIN FAILED | trading remains blocked')
        if self.ml is None:
            logging.error('ML STATUS | enabled=%s | loaded=false | required=%s | path=%s',enabled,self.ml_required,path)
        else:
            logging.info('ML STATUS | enabled=true | loaded=true | version=%s | label=%s | auc=%s | precision_at_0_5=%s | samples=%s | path=%s',
                         self.ml.get('feature_version'),self.ml.get('label'),self.ml.get('auc'),
                         self.ml.get('precision_at_0_5'),self.ml.get('samples'),path)

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
        saved=self.ml
        if self.ml_required and saved is None:
            self._diag('ml_unavailable')
            logging.warning('ML BLOCK | %s | no compatible triple-barrier model',symbol)
            _notify(self,f'🟠 ML BLOCK | {symbol} | no compatible model')
            return None
        if saved is None:
            return sig
        try:
            feats=signal_features(sig)
            model=saved['model']
            prob=float(model.predict_proba(feats)[0,1]); sig.ml_prob=prob
            key='ML_MIN_PROBABILITY_LONG' if sig.side=='long' else 'ML_MIN_PROBABILITY_SHORT'
            minimum=float(os.getenv(key,'0.55'))
            logging.info('ML DECISION | %s | side=%s | probability=%.3f | min=%.3f | feature_version=%s | label=%s',
                         symbol,sig.side,prob,minimum,saved.get('feature_version'),saved.get('label'))
            if not np.isfinite(prob) or prob<minimum:
                self._diag('ml_rejected')
                logging.info('ML REJECT | %s | side=%s | probability=%.3f | min=%.3f',symbol,sig.side,prob,minimum)
                return None
            logging.info('ML ACCEPT | %s | side=%s | probability=%.3f | min=%.3f',symbol,sig.side,prob,minimum)
            _notify(self,f'✅ ML ACCEPT | {symbol} | side={sig.side.upper()} | probability={prob:.3f} | min={minimum:.3f}')
            return sig
        except Exception:
            self._diag('ml_inference_failed')
            logging.exception('ML INFERENCE FAILED | %s',symbol)
            _notify(self,f'🔴 ML INFERENCE FAILED | {symbol}')
            return None

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
    logging.info('RUNTIME PATCH | WS market data + triple-barrier ML + fail-closed inference + native protection hook')
