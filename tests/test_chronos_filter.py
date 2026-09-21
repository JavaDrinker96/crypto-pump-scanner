import os
import threading
import time
import unittest
from unittest.mock import patch

import numpy as np

from chronos_filter import AsyncChronosForecastFilter, analyze_quantile_forecast, accept_forecast


class ChronosForecastAnalysisTests(unittest.TestCase):
    def test_supportive_long_forecast_passes(self):
        # 9 quantile paths x 5 future steps, entry=100, 1R=1.
        forecast=np.array([
            [99.7,99.9,100.1,100.3,100.5],
            [99.8,100.0,100.2,100.5,100.8],
            [99.9,100.1,100.4,100.8,101.1],
            [100.0,100.2,100.6,101.0,101.3],
            [100.0,100.3,100.7,101.1,101.5],
            [100.1,100.4,100.8,101.2,101.6],
            [100.1,100.5,100.9,101.3,101.8],
            [100.2,100.6,101.0,101.4,102.0],
            [100.3,100.7,101.1,101.6,102.2],
        ])
        metrics=analyze_quantile_forecast(forecast,100,1,'long')
        allowed,checks=accept_forecast(metrics,.20,.44,.20,1.25)
        self.assertTrue(allowed,checks)
        self.assertGreaterEqual(metrics['direction_support'],.44)
        self.assertGreaterEqual(metrics['median_mfe_r'],1.0)

    def test_strongly_adverse_long_forecast_is_rejected(self):
        forecast=np.array([
            [100.0,99.7,99.2,98.7,98.5],
            [100.0,99.8,99.3,98.8,98.6],
            [100.0,99.8,99.4,99.0,98.8],
            [100.0,99.9,99.5,99.1,98.9],
            [100.0,99.9,99.6,99.2,99.0],
            [100.0,100.0,99.7,99.4,99.2],
            [100.1,100.0,99.8,99.5,99.4],
            [100.1,100.1,99.9,99.6,99.5],
            [100.2,100.1,100.0,99.8,99.6],
        ])
        metrics=analyze_quantile_forecast(forecast,100,1,'long')
        allowed,checks=accept_forecast(metrics,.20,.44,.20,1.25)
        self.assertFalse(allowed)
        self.assertIn('direction_support',[k for k,v in checks.items() if not v])

    def test_short_direction_is_mirrored(self):
        forecast=np.array([
            [99.0,98.8,98.5],
            [99.2,99.0,98.7],
            [99.4,99.1,98.9],
            [99.6,99.3,99.0],
            [99.8,99.4,99.1],
            [100.0,99.6,99.3],
            [100.1,99.8,99.5],
            [100.2,100.0,99.7],
            [100.3,100.1,99.9],
        ])
        metrics=analyze_quantile_forecast(forecast,100,1,'short')
        self.assertGreater(metrics['direction_support'],.5)
        self.assertGreater(metrics['median_mfe_r'],.5)


if __name__=='__main__':
    unittest.main()


class AsyncChronosTests(unittest.TestCase):
    class Sig:
        side='long'
        price=100.0
        atr=1.0
        stop_distance=1.0

    @staticmethod
    def rows(ts=1000000):
        return [[ts+i*60000,100,101,99,100+i*.01,1000] for i in range(120)]

    def test_request_is_non_blocking_and_result_is_reused(self):
        gate=threading.Event()

        class SlowEvaluator:
            model_id='fake'
            def evaluate(self,symbol,rows,sig,sl_mult):
                gate.wait(2)
                return {
                    'allowed':True,'failed':[],'barrier_support':.5,'resolved_support':.5,
                    'direction_support':.7,'median_mfe_r':.8,'median_mae_r':.2,
                    'median_terminal_r':.4,'latency_ms':123.0,'model_id':'fake',
                    'horizon':15,'context':120,
                }

        filt=AsyncChronosForecastFilter(evaluator_factory=SlowEvaluator)
        try:
            started=time.perf_counter()
            first=filt.request('X/USDT:USDT',self.rows(),self.Sig(),1.8)
            elapsed=time.perf_counter()-started
            self.assertEqual(first['state'],'pending')
            self.assertLess(elapsed,.10)

            again=filt.request('X/USDT:USDT',self.rows(),self.Sig(),1.8)
            self.assertEqual(again['state'],'pending')
            gate.set()

            result=None
            for _ in range(100):
                result=filt.request('X/USDT:USDT',self.rows(),self.Sig(),1.8)
                if result['state']=='ready':break
                time.sleep(.01)
            self.assertEqual(result['state'],'ready')
            self.assertTrue(result['allowed'])

            reused=filt.request('X/USDT:USDT',self.rows(),self.Sig(),1.8)
            self.assertEqual(reused['state'],'ready')
        finally:
            gate.set()
            filt.close()

    def test_queue_is_bounded(self):
        gate=threading.Event()

        class SlowEvaluator:
            model_id='fake'
            def evaluate(self,symbol,rows,sig,sl_mult):
                gate.wait(2)
                return {
                    'allowed':True,'failed':[],'barrier_support':.5,'resolved_support':.5,
                    'direction_support':.7,'median_mfe_r':.8,'median_mae_r':.2,
                    'median_terminal_r':.4,'latency_ms':1.0,'model_id':'fake',
                    'horizon':15,'context':120,
                }

        with patch.dict(os.environ,{'CHRONOS_MAX_PENDING':'1'}):
            filt=AsyncChronosForecastFilter(evaluator_factory=SlowEvaluator)
            try:
                self.assertEqual(filt.request('A/USDT:USDT',self.rows(),self.Sig(),1.8)['state'],'pending')
                second=filt.request('B/USDT:USDT',self.rows(),self.Sig(),1.8)
                self.assertEqual(second['state'],'busy')
                self.assertEqual(second['reason'],'queue_full')
            finally:
                gate.set()
                filt.close()

    def test_load_failure_enters_cooldown(self):
        class BrokenEvaluator:
            def __init__(self):
                raise RuntimeError('download failed')

        with patch.dict(os.environ,{'CHRONOS_LOAD_RETRY_SEC':'300'}):
            filt=AsyncChronosForecastFilter(evaluator_factory=BrokenEvaluator)
            try:
                self.assertEqual(filt.request('A/USDT:USDT',self.rows(),self.Sig(),1.8)['state'],'pending')
                failed=None
                for _ in range(100):
                    failed=filt.request('A/USDT:USDT',self.rows(),self.Sig(),1.8)
                    if failed['state']=='error':break
                    time.sleep(.01)
                self.assertEqual(failed['state'],'error')

                next_rows=self.rows(2000000)
                cooldown=filt.request('B/USDT:USDT',next_rows,self.Sig(),1.8)
                self.assertEqual(cooldown['state'],'error')
                self.assertEqual(cooldown['reason'],'model_load_cooldown')
            finally:
                filt.close()
