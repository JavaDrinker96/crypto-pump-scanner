"""Simple cost-aware chronological backtest for continuation/exhaustion signals."""
import argparse,csv,json,os
import numpy as np


def load(path):
    rows=[]
    with open(path,encoding='utf8') as f:
        for r in csv.DictReader(f): rows.append([float(r[k]) for k in ('timestamp','open','high','low','close','volume')])
    return np.asarray(rows,float)

def rsi(c,n=14):
    d=np.diff(c);g=np.maximum(d,0);l=np.maximum(-d,0);ag=g[:n].mean();al=l[:n].mean()
    for i in range(n,len(d)):ag=(ag*(n-1)+g[i])/n;al=(al*(n-1)+l[i])/n
    return 100 if al<=1e-12 else 100-100/(1+ag/al)

def main():
    ap=argparse.ArgumentParser();ap.add_argument('csv');ap.add_argument('--risk',type=float,default=.005);ap.add_argument('--sl-atr',type=float,default=1.5);ap.add_argument('--tp-r',type=float,default=2.0);ap.add_argument('--fee',type=float,default=.00055);ap.add_argument('--slippage',type=float,default=.0005);a=ap.parse_args()
    x=load(a.csv); equity=1.; peak=1.; dd=0.; trades=[]; pos=None
    for i in range(30,len(x)):
        o,h,l,c,v=x[i]; hist=x[:i+1]; closes=hist[:,4]; atr=np.mean(np.maximum(hist[-14:,2]-hist[-14:,3],np.maximum(abs(hist[-14:,2]-hist[-15:-1,4]),abs(hist[-14:,3]-hist[-15:-1,4]))));
        if pos:
            side,e,stop,tp,risk=pos
            hit_sl=(side==1 and l<=stop) or (side==-1 and h>=stop); hit_tp=(side==1 and h>=tp) or (side==-1 and l<=tp)
            if hit_sl or hit_tp:
                px=stop if hit_sl else tp; gross=(px-e)/e*side; net=gross-a.fee*2-a.slippage*2; equity*=1+net; trades.append(net); pos=None
        if not pos:
            m1=c/closes[-2]-1;m3=c/closes[-4]-1;m5=c/closes[-6]-1;vol=v/max(np.mean(hist[-21:-1,5]),1e-12);rr=rsi(closes);br=c>=max(closes[-11:-1]);
            if br and m1>=.006 and m3>=.012 and m5<=.045 and vol>=3 and 52<=rr<=78:
                d=atr*a.sl_atr; pos=(1,c*(1+a.slippage),c*(1+a.slippage)-d,c*(1+a.slippage)+d*a.tp_r,equity*a.risk)
            elif m5>=.045 and vol>=2.4 and rr>=72:
                d=atr*a.sl_atr; pos=(-1,c*(1-a.slippage),c*(1-a.slippage)+d,c*(1-a.slippage)-d*a.tp_r,equity*a.risk)
        peak=max(peak,equity);dd=max(dd,(peak-equity)/peak)
    gp=sum(t for t in trades if t>0);gl=abs(sum(t for t in trades if t<0));report={'trades':len(trades),'win_rate':sum(t>0 for t in trades)/len(trades) if trades else 0,'return_pct':(equity-1)*100,'profit_factor':gp/gl if gl else None,'max_drawdown_pct':dd*100,'expectancy_pct':(sum(trades)/len(trades)*100) if trades else 0}
    print(json.dumps(report,indent=2))
if __name__=='__main__':main()
