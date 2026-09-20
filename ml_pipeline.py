"""ML feature/label pipeline shared by training and live inference."""
import logging
import os

import numpy as np
import joblib

FEATURE_NAMES=(
    'rsi','volume_ratio','vwap_distance_pct','move5_pct',
    'atr_pct','move1_pct','move3_pct','side',
)
FEATURE_VERSION=2


def _rsi(closes,n=14):
    d=np.diff(closes); g=np.maximum(d,0); l=np.maximum(-d,0)
    ag=g[:n].mean(); al=l[:n].mean()
    for j in range(n,len(d)):
        ag=(ag*(n-1)+g[j])/n; al=(al*(n-1)+l[j])/n
    return 100.0 if al<=1e-12 else 100-100/(1+ag/al)


def _atr(rows,i,n=14):
    start=max(1,i-n+1); tr=[]
    for j in range(start,i+1):
        h=float(rows[j][2]); l=float(rows[j][3]); pc=float(rows[j-1][4])
        tr.append(max(h-l,abs(h-pc),abs(l-pc)))
    return float(np.mean(tr)) if tr else 0.0


def historical_features(rows,i,side):
    closes=np.asarray([float(z[4]) for z in rows[:i+1]],float)
    volumes=np.asarray([float(z[5]) for z in rows],float)
    p=float(closes[-1]); atr=_atr(rows,i)
    rsi=_rsi(closes)
    volume_ratio=float(volumes[i]/max(volumes[max(0,i-20):i].mean(),1e-12))
    m1=(closes[-1]/closes[-2]-1)*100
    m3=(closes[-1]/closes[-4]-1)*100
    m5=(closes[-1]/closes[-6]-1)*100
    lo=max(0,i-29)
    denom=max(sum(float(rows[j][5]) for j in range(lo,i+1)),1e-12)
    vwap=sum(((float(rows[j][2])+float(rows[j][3])+float(rows[j][4]))/3)*float(rows[j][5]) for j in range(lo,i+1))/denom
    vd=abs(p/vwap-1)*100
    atr_pct=atr/max(p,1e-12)*100
    return np.asarray([rsi,volume_ratio,vd,m5,atr_pct,m1,m3,1.0 if side=='long' else -1.0],float)


def signal_features(sig):
    atr_pct=float(sig.atr)/max(float(sig.price),1e-12)*100
    return np.asarray([[
        float(sig.rsi),float(sig.vol),float(sig.vwap),float(sig.move5),
        atr_pct,float(sig.m1),float(sig.m3),1.0 if sig.side=='long' else -1.0,
    ]],float)


def triple_barrier_label(rows,i,side,sl_atr_mult,tp_r=1.0,horizon=15):
    """1 when TP is reached before SL, 0 when SL is reached first, None unresolved.

    If TP and SL are both touched in the same candle the label is conservative (0)
    because candle data cannot determine intrabar ordering.
    """
    atr=_atr(rows,i)
    if atr<=0:return None
    entry=float(rows[i][4]); risk=atr*float(sl_atr_mult)
    direction=1 if side=='long' else -1
    target=entry+direction*risk*float(tp_r)
    stop=entry-direction*risk
    for j in range(i+1,min(len(rows),i+1+int(horizon))):
        high=float(rows[j][2]); low=float(rows[j][3])
        if side=='long':
            tp=high>=target; sl=low<=stop
        else:
            tp=low<=target; sl=high>=stop
        if tp and sl:return 0
        if sl:return 0
        if tp:return 1
    return None


def train_model(client,path):
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import roc_auc_score, precision_score

    default_symbols=(
        'BTC/USDT:USDT,ETH/USDT:USDT,SOL/USDT:USDT,XRP/USDT:USDT,'
        'DOGE/USDT:USDT,BNB/USDT:USDT,SUI/USDT:USDT,SEI/USDT:USDT,'
        'AR/USDT:USDT,WIF/USDT:USDT,PEPE/USDT:USDT,PUMPFUN/USDT:USDT,'
        'MSTR/USDT:USDT'
    )
    symbols=[s.strip() for s in os.getenv('ML_TRAIN_SYMBOLS',default_symbols).split(',') if s.strip()]
    limit=max(500,int(os.getenv('ML_TRAIN_CANDLES','900')))
    horizon=max(5,int(os.getenv('ML_LABEL_HORIZON_CANDLES','15')))
    tp_r=float(os.getenv('ML_LABEL_TP_R','1.0'))
    long_sl=float(os.getenv('PUMP_SL_ATR_MULT','1.8'))
    short_sl=float(os.getenv('SHORT_SL_ATR_MULT','1.5'))
    samples=[]
    for symbol in symbols:
        try:
            rows=client.fetch_ohlcv(symbol,timeframe=os.getenv('PUMP_TIMEFRAME','1m'),limit=limit)
            if len(rows)<100:continue
            for i in range(30,len(rows)-horizon):
                for side,sl_mult in (('long',long_sl),('short',short_sl)):
                    label=triple_barrier_label(rows,i,side,sl_mult,tp_r,horizon)
                    if label is None:continue
                    samples.append((int(rows[i][0]),historical_features(rows,i,side),int(label),symbol,side))
        except Exception:
            logging.exception('ML TRAIN | failed for %s',symbol)
    if len(samples)<500:
        raise RuntimeError(f'insufficient labeled ML samples: {len(samples)}')
    samples.sort(key=lambda x:x[0])
    X=np.asarray([x[1] for x in samples],float); y=np.asarray([x[2] for x in samples],int)
    split=max(1,int(len(X)*.75))
    xa,ya=X[:split],y[:split]; xb,yb=X[split:],y[split:]
    if len(set(ya))<2 or len(set(yb))<2:
        raise RuntimeError('time-series validation split has insufficient class diversity')
    model=RandomForestClassifier(
        n_estimators=400,max_depth=8,min_samples_leaf=12,
        class_weight='balanced_subsample',random_state=42,n_jobs=-1,
    )
    model.fit(xa,ya)
    prob=model.predict_proba(xb)[:,1]
    auc=float(roc_auc_score(yb,prob))
    precision=float(precision_score(yb,prob>=.5,zero_division=0))
    artifact={
        'model':model,'feature_count':len(FEATURE_NAMES),'feature_names':FEATURE_NAMES,
        'feature_version':FEATURE_VERSION,'label':'triple_barrier_tp_before_sl',
        'tp_r':tp_r,'horizon_candles':horizon,'symbols':symbols,
        'auc':auc,'precision_at_0_5':precision,'validation':'global_chronological_last_25pct',
        'samples':len(samples),'positive_rate':float(y.mean()),
    }
    os.makedirs(os.path.dirname(path) or '.',exist_ok=True)
    joblib.dump(artifact,path)
    logging.info('ML TRAINED | version=%s | samples=%s | positive_rate=%.3f | train=%s | validation=%s | auc=%.3f | precision@0.5=%.3f | symbols=%s | path=%s',
                 FEATURE_VERSION,len(y),float(y.mean()),len(ya),len(yb),auc,precision,len(symbols),path)
    return artifact


def compatible(candidate):
    return (
        isinstance(candidate,dict)
        and candidate.get('feature_version')==FEATURE_VERSION
        and candidate.get('feature_count')==len(FEATURE_NAMES)
        and candidate.get('label')=='triple_barrier_tp_before_sl'
    )
