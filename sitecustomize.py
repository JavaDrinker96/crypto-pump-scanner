"""Runtime patch: fail-closed ML, Bybit WS market data, native TP/SL + runner trailing stop."""
import logging, os
try:
    import numpy as np, joblib
except Exception: np=joblib=None
try:
    import advanced_engine
except Exception:
    logging.exception('RUNTIME PATCH | advanced_engine import failed')
else:
    _init=advanced_engine.Engine.__init__; _signal=advanced_engine.Engine.signal; _open=advanced_engine.Engine.open
    _ohlcv=advanced_engine.Engine.ohlcv; _flow=advanced_engine.Engine.flow; _book=advanced_engine.Engine.book
    def _path(): return os.getenv('ML_MODEL_PATH','data/models/pump_classifier.joblib')
    def _train(client):
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import roc_auc_score
        symbols=[s.strip() for s in os.getenv('ML_TRAIN_SYMBOLS','BTC/USDT:USDT,ETH/USDT:USDT,SOL/USDT:USDT,XRP/USDT:USDT,DOGE/USDT:USDT,BNB/USDT:USDT').split(',') if s.strip()]
        limit=max(300,int(os.getenv('ML_TRAIN_CANDLES','700'))); threshold=float(os.getenv('ML_LABEL_RETURN_PCT','0.25'))/100; X=[]; y=[]
        def rsi(c,n=14):
            d=np.diff(c); g=np.maximum(d,0); l=np.maximum(-d,0); ag=g[:n].mean(); al=l[:n].mean()
            for j in range(n,len(d)): ag=(ag*(n-1)+g[j])/n; al=(al*(n-1)+l[j])/n
            return 100 if al<=1e-12 else 100-100/(1+ag/al)
        for s in symbols:
            try:
                rows=client.fetch_ohlcv(s,timeframe=os.getenv('PUMP_TIMEFRAME','1m'),limit=limit)
                if len(rows)<80: continue
                c=np.asarray([z[4] for z in rows],float); h=np.asarray([z[2] for z in rows],float); l=np.asarray([z[3] for z in rows],float); o=np.asarray([z[1] for z in rows],float); v=np.asarray([z[5] for z in rows],float)
                for i in range(30,len(c)-5):
                    cc=c[:i+1]; vr=v[i]/max(v[i-20:i].mean(),1e-12); m1=cc[-1]/cc[-2]-1; m3=cc[-1]/cc[-4]-1; m5=cc[-1]/cc[-6]-1; rr=rsi(cc); tr=np.maximum(h[1:i+1]-l[1:i+1],np.maximum(abs(h[1:i+1]-c[:i]),abs(l[1:i+1]-c[:i]))); atr=float(tr[-14:].mean()); lo=max(0,i-29); vw=sum(((rows[j][2]+rows[j][3]+rows[j][4])/3)*rows[j][5] for j in range(lo,i+1))/max(sum(rows[j][5] for j in range(lo,i+1)),1e-12); vd=abs(cc[-1]/vw-1)*100; rng=max(h[i]-l[i],1e-12); fp=float(np.clip(.5+((c[i]-o[i])/rng)*.25,0,1)); bp=float(np.clip(.5+((c[i]-l[i])/rng-.5)*.5,0,1)); sp=float(min(1,atr/max(c[i],1e-12)*100)); score=min(.3*min(vr/5,1)+.2*min(abs(m5)/.05,1)+.25*fp+.15*max((bp-.5)*2,0)+.1,1); base=[score,rr,vr,fp,bp,vd,m5*100,atr,m1,m3,sp]; future=c[i+5]/c[i]-1
                    X.append(base+[1.0]); y.append(int(future>=threshold)); X.append(base+[-1.0]); y.append(int(future<=-threshold))
            except Exception: logging.exception('ML TRAIN | failed for %s',s)
        if len(X)<500 or len(set(y))<2: raise RuntimeError(f'insufficient training data: samples={len(X)} classes={sorted(set(y))}')
        xa,xb,ya,yb=train_test_split(np.asarray(X,float),np.asarray(y,int),test_size=.25,random_state=42,stratify=y); model=RandomForestClassifier(n_estimators=300,max_depth=8,min_samples_leaf=10,class_weight='balanced_subsample',random_state=42,n_jobs=-1); model.fit(xa,ya); auc=roc_auc_score(yb,model.predict_proba(xb)[:,1]); path=_path(); os.makedirs(os.path.dirname(path) or '.',exist_ok=True); joblib.dump({'model':model,'feature_count':12,'label_threshold':threshold,'symbols':symbols,'auc':auc},path); logging.info('ML TRAINED | samples=%s | positives=%s | auc=%.3f | path=%s',len(y),int(np.sum(y)),auc,path)
    def _patched_init(self,client,alert=None):
        _init(self,client,alert); self.ml_required=os.getenv('ML_REQUIRED','true').lower() in ('1','true','yes','on'); enabled=os.getenv('ML_ENABLED','true').lower() in ('1','true','yes','on'); path=_path(); self.ws=getattr(client,'ws_market',None)
        if self.ws: logging.info('MARKET DATA | Bybit public WebSocket enabled')
        self.ml=None
        if enabled and joblib:
            try:
                if os.path.exists(path):
                    candidate=joblib.load(path); fc=candidate.get('feature_count') if isinstance(candidate,dict) else None
                    if fc==12: self.ml=candidate
                    else: logging.warning('ML MODEL INCOMPATIBLE | feature_count=%s expected=12 | retraining',fc)
            except Exception: logging.exception('ML model load failed; retraining')
            if self.ml is None and os.getenv('ML_AUTO_TRAIN','true').lower() in ('1','true','yes','on'):
                try: _train(client); self.ml=joblib.load(path); logging.info('ML STATUS | enabled=true | loaded=true | source=auto-trained | path=%s',path)
                except Exception: logging.exception('ML AUTO TRAIN FAILED | trading remains blocked')
        if self.ml is None: logging.error('ML STATUS | enabled=%s | loaded=false | required=%s | path=%s',enabled,self.ml_required,path)
        else: logging.info('ML STATUS | enabled=true | loaded=true | auc=%s | path=%s',self.ml.get('auc') if isinstance(self.ml,dict) else 'n/a',path)
    def _ws_ohlcv(self,s,n=120):
        if getattr(self,'ws',None): return self.ws.get_ohlcv(s,lambda sym,lim:_ohlcv(self,sym,lim),n)
        return _ohlcv(self,s,n)
    def _ws_flow(self,s):
        if getattr(self,'ws',None):
            t=self.ws.get_trades(s)
            if t:
                b=sum(x['amount'] for x in t if x['side']=='buy'); a=sum(x['amount'] for x in t if x['side']=='sell'); return b/max(a+b,1e-12)
        return _flow(self,s)
    def _ws_book(self,s,p):
        if getattr(self,'ws',None):
            o=self.ws.get_order_book(s)
            if o and o.get('bids') and o.get('asks'):
                depth=self.depth; b=sum(float(q)*float(px) for px,q in o['bids'] if float(px)>=p*(1-depth/100)); a=sum(float(q)*float(px) for px,q in o['asks'] if float(px)<=p*(1+depth/100)); return b/max(a+b,1e-12),(float(o['asks'][0][0])-float(o['bids'][0][0]))/p*100
        return _book(self,s,p)
    def _patched_signal(self,symbol):
        saved=self.ml; self.ml=None
        try: sig=_signal(self,symbol)
        finally: self.ml=saved
        if sig is None:return None
        if self.ml_required and saved is None: self._diag('ml_unavailable'); logging.warning('ML BLOCK | %s | no compatible model',symbol); return None
        try:
            side=1. if sig.side=='long' else -1.; feats=np.asarray([[sig.score,sig.rsi,sig.vol,sig.flow,sig.book,sig.vwap,sig.move5,sig.atr,sig.m1/100,sig.m3/100,sig.spread,side]],float); model=saved['model'] if isinstance(saved,dict) else saved; prob=float(model.predict_proba(feats)[0,1]); sig.ml_prob=prob; minimum=float(os.getenv('ML_MIN_PROBABILITY','0.58'))
            if not np.isfinite(prob) or prob<minimum: self._diag('ml_rejected'); logging.info('ML REJECT | %s | probability=%.3f | min=%.3f',symbol,prob,minimum); return None
            logging.info('ML ACCEPT | %s | side=%s | probability=%.3f',symbol,sig.side,prob); return sig
        except Exception: self._diag('ml_inference_failed'); logging.exception('ML INFERENCE FAILED | %s',symbol); return None
    def _protect(self,p):
        def req(params):
            return self.c.request('v5/position/trading-stop','private','POST',params)
        symbol=self.c.market(p.symbol).get('id') or p.symbol.replace('/','').replace(':USDT',''); q1=float(os.getenv('TP1_CLOSE_PCT','.35')); q2=float(os.getenv('TP2_CLOSE_PCT','.35')); q3=max(0,1-q1-q2)
        for name,tp,f in [('TP1',p.tp1,q1),('TP2',p.tp2,q2)]:
            qty=float(self.c.amount_to_precision(p.symbol,p.qty*f)); params={'category':'linear','symbol':symbol,'positionIdx':0,'tpslMode':'Partial','takeProfit':str(tp),'stopLoss':str(p.stop),'tpSize':str(qty),'slSize':str(qty),'tpOrderType':'Market','slOrderType':'Market','tpTriggerBy':'MarkPrice','slTriggerBy':'MarkPrice'}; r=req(params)
            if not isinstance(r,dict) or r.get('retCode',0)!=0: raise RuntimeError(f'Bybit TP/SL failed: {r}')
            logging.info('PROTECTION SET | %s | %s qty=%s tp=%s sl=%s',p.symbol,name,qty,tp,p.stop)
        if q3>0:
            qty=float(self.c.amount_to_precision(p.symbol,p.qty*q3)); distance=float(p.risk)*float(os.getenv('TRAILING_ATR_MULT','1.5')); active=p.tp2
            side='sell' if p.side=='long' else 'buy'; params={'category':'linear','symbol':symbol,'positionIdx':0,'side':side,'orderType':'Market','qty':str(qty),'triggerDirection':1 if p.side=='long' else 2,'triggerPrice':str(active),'triggerBy':'MarkPrice','stopOrderType':'TrailingStop','trailingStop':str(distance),'reduceOnly':True,'closeOnTrigger':True}
            r=self.c.create_order(p.symbol,'market',side,qty,None,params)
            logging.info('RUNNER TRAILING SET | %s | qty=%s | activation=%s | distance=%s | order=%s',p.symbol,qty,active,distance,r.get('id'))
        self.journal('protection',p,{'mode':'TP1+TP2+exchange_trailing_runner','runner_fraction':q3})
    def _open_with_protection(self,s):
        before=set(self.pos); _open(self,s)
        if s.symbol not in self.pos or s.symbol in before:return
        p=self.pos[s.symbol]
        try:_protect(self,p)
        except Exception:
            logging.exception('PROTECTION FAILED | symbol=%s | closing immediately',p.symbol)
            try:
                q=float(self.c.amount_to_precision(p.symbol,p.qty));
                if q>0:self.c.create_order(p.symbol,'market','sell' if p.side=='long' else 'buy',q,None,{'reduceOnly':True,'positionIdx':0})
            finally:self.pos.pop(s.symbol,None)
            raise
    advanced_engine.Engine.__init__=_patched_init; advanced_engine.Engine.ohlcv=_ws_ohlcv; advanced_engine.Engine.flow=_ws_flow; advanced_engine.Engine.book=_ws_book; advanced_engine.Engine.signal=_patched_signal; advanced_engine.Engine.open=_open_with_protection; advanced_engine.Engine.manage=lambda self:None
    logging.info('RUNTIME PATCH | WS market data + ML fail-closed + TP1/TP2 + exchange trailing runner')
