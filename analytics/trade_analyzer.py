"""Analyze data/trades.jsonl and produce JSON/CSV-ready performance statistics."""
import argparse,json,math,os
from collections import defaultdict


def load(path):
    rows=[]
    if not os.path.exists(path): return rows
    with open(path,encoding='utf8') as f:
        for line in f:
            try: rows.append(json.loads(line))
            except Exception: pass
    return rows


def analyze(rows):
    closes=[r for r in rows if r.get('event')=='close']
    pnls=[float(r.get('pnl',0) or 0) for r in closes]
    wins=[p for p in pnls if p>0]; losses=[p for p in pnls if p<0]
    equity=0.; peak=0.; maxdd=0.; curve=[]
    for p in pnls:
        equity+=p; peak=max(peak,equity); maxdd=max(maxdd,peak-equity); curve.append(equity)
    gross_win=sum(wins); gross_loss=abs(sum(losses));
    by_reason=defaultdict(list); by_side=defaultdict(list); by_symbol=defaultdict(list)
    for r in closes:
        p=float(r.get('pnl',0) or 0); by_reason[r.get('reason','unknown')].append(p)
        pos=r.get('position') or {}; by_side[pos.get('side','unknown')].append(p); by_symbol[pos.get('symbol','unknown')].append(p)
    def group(xs):
        return {'trades':len(xs),'win_rate':sum(x>0 for x in xs)/len(xs) if xs else 0,'pnl':sum(xs),'avg':sum(xs)/len(xs) if xs else 0}
    return {'trades':len(pnls),'wins':len(wins),'losses':len(losses),'win_rate':len(wins)/len(pnls) if pnls else 0,
            'net_pnl':sum(pnls),'gross_profit':gross_win,'gross_loss':-gross_loss,
            'profit_factor':gross_win/gross_loss if gross_loss else math.inf,'expectancy':sum(pnls)/len(pnls) if pnls else 0,
            'max_drawdown':maxdd,'max_drawdown_pct':(maxdd/peak if peak>0 else 0),
            'best_trade':max(pnls) if pnls else 0,'worst_trade':min(pnls) if pnls else 0,
            'by_reason':{k:group(v) for k,v in by_reason.items()},'by_side':{k:group(v) for k,v in by_side.items()},
            'top_symbols':sorted(({'symbol':k,**group(v)} for k,v in by_symbol.items()),key=lambda x:x['pnl'],reverse=True)[:20]}


def main():
    ap=argparse.ArgumentParser();ap.add_argument('--input',default=os.getenv('TRADE_JOURNAL_PATH','data/trades.jsonl'));ap.add_argument('--output',default='data/reports/trade_report.json');a=ap.parse_args()
    rows=load(a.input); report=analyze(rows); os.makedirs(os.path.dirname(a.output) or '.',exist_ok=True)
    with open(a.output,'w',encoding='utf8') as f: json.dump(report,f,indent=2,ensure_ascii=False)
    print(json.dumps(report,indent=2,ensure_ascii=False))
if __name__=='__main__': main()
