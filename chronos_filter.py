"""Pretrained Chronos-Bolt forecast filter for trade candidates."""
import logging
import os
import time

import numpy as np


def analyze_quantile_forecast(forecast,entry,risk,side):
    """Convert Chronos marginal quantile paths into transparent support metrics.

    Quantile paths are not treated as calibrated first-passage probabilities.
    The metrics are only used as a conservative veto after structural filters.
    """
    arr=np.asarray(forecast,float)
    if arr.ndim!=2 or arr.shape[0]<3 or arr.shape[1]<1:
        raise ValueError(f'unexpected forecast shape: {arr.shape}')
    entry=float(entry); risk=max(abs(float(risk)),1e-12)
    direction=1.0 if side=='long' else -1.0
    target=entry+direction*risk
    stop=entry-direction*risk
    resolved=0; wins=0
    for path in arr:
        outcome=None
        for value in path:
            if side=='long':
                hit_tp=value>=target; hit_sl=value<=stop
            else:
                hit_tp=value<=target; hit_sl=value>=stop
            if hit_tp and hit_sl:
                outcome=0; break
            if hit_sl:
                outcome=0; break
            if hit_tp:
                outcome=1; break
        if outcome is not None:
            resolved+=1; wins+=outcome
    median=arr[arr.shape[0]//2]
    directional_r=direction*(median-entry)/risk
    terminal=arr[:,-1]
    direction_support=float(np.mean(direction*(terminal-entry)>0))
    barrier_support=float(wins/max(arr.shape[0],1))
    resolved_support=float(resolved/max(arr.shape[0],1))
    return {
        'barrier_support':barrier_support,
        'resolved_support':resolved_support,
        'direction_support':direction_support,
        'median_mfe_r':float(max(0.0,np.max(directional_r))),
        'median_mae_r':float(max(0.0,-np.min(directional_r))),
        'median_terminal_r':float(directional_r[-1]),
        'forecast_min':float(np.min(arr)),
        'forecast_max':float(np.max(arr)),
    }


def accept_forecast(metrics,min_barrier=.20,min_direction=.44,min_mfe_r=.20,max_median_mae_r=1.25):
    checks={
        'barrier_support':float(metrics['barrier_support'])>=float(min_barrier),
        'direction_support':float(metrics['direction_support'])>=float(min_direction),
        'median_mfe':float(metrics['median_mfe_r'])>=float(min_mfe_r),
        'median_mae':float(metrics['median_mae_r'])<=float(max_median_mae_r),
    }
    return all(checks.values()),checks


class ChronosForecastFilter:
    def __init__(self):
        import torch
        from chronos import BaseChronosPipeline

        self.torch=torch
        self.model_id=os.getenv('CHRONOS_MODEL_ID','amazon/chronos-bolt-tiny')
        self.context=max(32,int(os.getenv('CHRONOS_CONTEXT_LENGTH','120')))
        self.horizon=max(3,int(os.getenv('CHRONOS_FORECAST_HORIZON','15')))
        self.cache_ttl=max(1,int(os.getenv('CHRONOS_SIGNAL_CACHE_SEC','20')))
        self.cache={}
        cache_dir=os.getenv('HF_HOME','/app/data/models/huggingface')
        os.makedirs(cache_dir,exist_ok=True)
        started=time.time()
        self.pipeline=BaseChronosPipeline.from_pretrained(
            self.model_id,
            device_map='cpu',
            torch_dtype=torch.float32,
            cache_dir=cache_dir,
        )
        logging.info('CHRONOS LOADED | model=%s | context=%s | horizon=%s | device=cpu | load_sec=%.3f | cache=%s',
                     self.model_id,self.context,self.horizon,time.time()-started,cache_dir)

    def evaluate(self,symbol,rows,sig,sl_mult):
        now=time.time(); key=(symbol,sig.side)
        hit=self.cache.get(key)
        if hit and now-hit[0]<self.cache_ttl:
            return hit[1]
        closes=np.asarray([float(z[4]) for z in rows[-self.context:]],dtype=np.float32)
        if len(closes)<32:
            raise RuntimeError(f'insufficient Chronos context: {len(closes)}')
        started=time.time()
        context=self.torch.tensor(closes,dtype=self.torch.float32)
        with self.torch.no_grad():
            forecast=self.pipeline.predict(context=context,prediction_length=self.horizon)
        arr=forecast.detach().cpu().numpy()[0]
        risk=float(getattr(sig,'stop_distance',0) or 0) or float(sig.atr)*float(sl_mult)
        metrics=analyze_quantile_forecast(arr,float(sig.price),risk,sig.side)
        allowed,checks=accept_forecast(
            metrics,
            float(os.getenv('CHRONOS_MIN_BARRIER_SUPPORT','.20')),
            float(os.getenv('CHRONOS_MIN_DIRECTION_SUPPORT','.44')),
            float(os.getenv('CHRONOS_MIN_MEDIAN_MFE_R','.20')),
            float(os.getenv('CHRONOS_MAX_MEDIAN_MAE_R','1.25')),
        )
        result={
            **metrics,'allowed':bool(allowed),
            'failed':[k for k,v in checks.items() if not v],
            'latency_ms':(time.time()-started)*1000,
            'model_id':self.model_id,'horizon':self.horizon,'context':len(closes),
        }
        self.cache[key]=(now,result)
        return result
