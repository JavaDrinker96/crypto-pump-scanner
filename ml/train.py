"""Train a pump outcome classifier from the journal. Uses chronological validation."""
import argparse,json,os
import joblib
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report,roc_auc_score
from sklearn.model_selection import TimeSeriesSplit

FEATURES=['score','rsi','vol','flow','book','vwap','move5','atr','m1','m3','spread']

def load(path):
    out=[]
    if not os.path.exists(path): return out
    with open(path,encoding='utf8') as f:
        for line in f:
            try:
                r=json.loads(line)
                if r.get('event')=='close' and isinstance(r.get('signal'),dict): out.append(r)
            except Exception: pass
    return out

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--input',default=os.getenv('TRADE_JOURNAL_PATH','data/trades.jsonl'));ap.add_argument('--model',default=os.getenv('ML_MODEL_PATH','models/pump_classifier.joblib'));a=ap.parse_args()
    rows=load(a.input)
    if len(rows)<30: raise SystemExit(f'Need at least 30 closed trades with signal features; found {len(rows)}')
    rows.sort(key=lambda r:r.get('ts',0)); X=[];y=[]
    for r in rows:
        s=r['signal']; X.append([float(s.get(k,0) or 0) for k in FEATURES]); y.append(int(float(r.get('pnl',0) or 0)>0))
    X=np.asarray(X);y=np.asarray(y)
    if len(np.unique(y))<2: raise SystemExit('Need both winning and losing examples')
    n=min(5,max(2,len(X)//20)); tscv=TimeSeriesSplit(n_splits=n); aucs=[]
    for train,test in tscv.split(X):
        if len(np.unique(y[train]))<2: continue
        m=RandomForestClassifier(n_estimators=400,min_samples_leaf=4,max_features='sqrt',class_weight='balanced_subsample',random_state=42,n_jobs=-1)
        m.fit(X[train],y[train]); p=m.predict_proba(X[test])[:,1]
        if len(np.unique(y[test]))==2: aucs.append(roc_auc_score(y[test],p))
    model=RandomForestClassifier(n_estimators=500,min_samples_leaf=4,max_features='sqrt',class_weight='balanced_subsample',random_state=42,n_jobs=-1)
    model.fit(X,y); os.makedirs(os.path.dirname(a.model) or '.',exist_ok=True)
    joblib.dump({'model':model,'features':FEATURES,'cv_auc':float(np.mean(aucs)) if aucs else None,'samples':len(y)},a.model)
    print(json.dumps({'samples':len(y),'positive_rate':float(y.mean()),'cv_auc':float(np.mean(aucs)) if aucs else None,'model':a.model},indent=2))
if __name__=='__main__': main()
