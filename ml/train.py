"""Train a leakage-safe pump outcome classifier from paired journal events."""
import argparse,json,os
import joblib
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import TimeSeriesSplit

FEATURES=['score','rsi','vol','flow','book','vwap','move5','atr','m1','m3','spread']

def load(path):
    rows=[]
    if not os.path.exists(path): return rows
    with open(path,encoding='utf8') as f:
        for line in f:
            try: rows.append(json.loads(line))
            except Exception: pass
    return rows

def dataset(rows):
    opens={}; pairs=[]
    for r in sorted(rows,key=lambda z:z.get('ts',0)):
        p=r.get('position') or {}; key=(p.get('symbol'),p.get('entry'),p.get('qty'))
        if r.get('event')=='open' and isinstance(r.get('signal'),dict): opens[key]=r
        elif r.get('event')=='close' and key in opens:
            s=opens[key].get('signal',{}); pairs.append((float(r.get('ts',0)),[float(s.get(k,0) or 0) for k in FEATURES],int(float(r.get('pnl',0) or 0)>0)))
    pairs.sort(); return np.asarray([x[1] for x in pairs]),np.asarray([x[2] for x in pairs])

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--input',default=os.getenv('TRADE_JOURNAL_PATH','data/trades.jsonl'));ap.add_argument('--model',default=os.getenv('ML_MODEL_PATH','models/pump_classifier.joblib'));a=ap.parse_args()
    X,y=dataset(load(a.input))
    if len(y)<30: raise SystemExit(f'Need at least 30 paired closed trades; found {len(y)}')
    if len(np.unique(y))<2: raise SystemExit('Need both winning and losing examples')
    n=min(5,max(2,len(y)//20)); aucs=[]
    for tr,te in TimeSeriesSplit(n_splits=n).split(X):
        if len(np.unique(y[tr]))<2 or len(np.unique(y[te]))<2: continue
        m=RandomForestClassifier(n_estimators=400,min_samples_leaf=4,max_features='sqrt',class_weight='balanced_subsample',random_state=42,n_jobs=-1).fit(X[tr],y[tr]); aucs.append(roc_auc_score(y[te],m.predict_proba(X[te])[:,1]))
    model=RandomForestClassifier(n_estimators=500,min_samples_leaf=4,max_features='sqrt',class_weight='balanced_subsample',random_state=42,n_jobs=-1).fit(X,y)
    os.makedirs(os.path.dirname(a.model) or '.',exist_ok=True);joblib.dump({'model':model,'features':FEATURES,'cv_auc':float(np.mean(aucs)) if aucs else None,'samples':len(y)},a.model)
    print(json.dumps({'samples':len(y),'positive_rate':float(y.mean()),'cv_auc':float(np.mean(aucs)) if aucs else None,'model':a.model},indent=2))
if __name__=='__main__': main()
