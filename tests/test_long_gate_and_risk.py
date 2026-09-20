import os
import tempfile
import unittest
from unittest.mock import patch

class SyncClient:\n    ws_market=None\n    def fetch_positions(self,params=None):return []\n\n\nfrom advanced_engine import Engine, Signal, long_continuation_gate


class LongContinuationGateTests(unittest.TestCase):
    def gate(self, *, m1, m3, m5, rsi, volume, breakout=True, green=2, vwap=.5):
        return long_continuation_gate(
            m1,m3,m5,rsi,volume,breakout,green,vwap,1.4,
            min_m3_pct=.30,min_m5_pct=.40,min_rsi=52,max_rsi=78,
            max_vwap_pct=3.0,max_m5_pct=6.0,
        )

    def test_strong_continuation_passes(self):
        allowed,checks=self.gate(m1=.002,m3=.0045,m5=.008,rsi=67,volume=2.2)
        self.assertTrue(allowed,checks)

    def test_mstr_like_trade_is_rejected_for_weak_momentum(self):
        allowed,checks=self.gate(
            m1=.0004448,m3=.0006355,m5=.0014629,rsi=74.14,volume=3.98
        )
        self.assertFalse(allowed)
        self.assertFalse(checks['momentum_3m'])
        self.assertFalse(checks['momentum_5m'])

    def test_sei_like_trade_is_rejected_for_volume_and_momentum(self):
        allowed,checks=self.gate(
            m1=.0002076,m3=.0004153,m5=.0006231,rsi=61.89,volume=.0386
        )
        self.assertFalse(allowed)
        self.assertFalse(checks['volume'])

    def test_pumpfun_like_trade_is_rejected_for_volume(self):
        allowed,checks=self.gate(
            m1=.0002402,m3=.0002402,m5=.0007210,rsi=56.3,volume=.1303
        )
        self.assertFalse(allowed)
        self.assertFalse(checks['volume'])

    def test_sol_like_trade_is_rejected_for_volume_and_momentum(self):
        allowed,checks=self.gate(
            m1=.0003606,m3=.001444,m5=.002439,rsi=77.16,volume=.2876
        )
        self.assertFalse(allowed)
        self.assertFalse(checks['volume'])
        self.assertFalse(checks['momentum_3m'])
        self.assertFalse(checks['momentum_5m'])

    def test_eth_like_trade_is_rejected_as_overheated_and_weak_momentum(self):
        allowed,checks=self.gate(
            m1=.0008400,m3=.001433,m5=.001697,rsi=78.30,volume=2.75
        )
        self.assertFalse(allowed)
        self.assertFalse(checks['rsi'])


class LongConfirmationTests(unittest.TestCase):
    class StubEngine(Engine):
        def __init__(self):
            super().__init__(client=SyncClient(),alert=None)
            self.opened=[]

        def signal(self,symbol):
            return Signal(
                symbol=symbol,side='long',price=1.0,atr=.01,rsi=65,vol=2.0,
                flow=.7,book=.7,vwap=.5,move5=.8,score=.7,reason='continuation',
                m1=.2,m3=.4,spread=.01,ml_prob=.8,
            )

        def open(self,sig):
            self.opened.append(sig.symbol)

    def test_long_confirmation_has_independent_setting(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ,{
            'TRADING_ENABLED':'true','ML_ENABLED':'false',
            'TRADE_JOURNAL_PATH':os.path.join(tmp,'trades.jsonl'),
            'SIGNAL_CONFIRM_CYCLES':'1','LONG_SIGNAL_CONFIRM_CYCLES':'2',
            'SHORT_SIGNAL_CONFIRM_CYCLES':'3',
        },clear=False):
            engine=self.StubEngine()
            self.assertEqual(engine.long_confirm,2)
            engine.run(['X/USDT:USDT'])
            self.assertEqual(engine.opened,[])
            engine.run(['X/USDT:USDT'])
            self.assertEqual(engine.opened,['X/USDT:USDT'])


class CircuitBreakerTests(unittest.TestCase):
    def make_engine(self,tmp,**env):
        values={
            'ML_ENABLED':'false','TRADE_JOURNAL_PATH':os.path.join(tmp,'trades.jsonl'),
            'CIRCUIT_BREAKER_ENABLED':'true','MAX_CONSECUTIVE_LOSSES':'3',
            'MAX_DAILY_LOSS_PCT':'2',
        }
        values.update(env)
        ctx=patch.dict(os.environ,values,clear=False)
        ctx.start()
        self.addCleanup(ctx.stop)
        return Engine(object(),None)

    def test_blocks_after_consecutive_losses(self):
        with tempfile.TemporaryDirectory() as tmp:
            engine=self.make_engine(tmp)
            engine.losses=3
            self.assertIn('consecutive_losses',engine.risk_block_reason(100))

    def test_blocks_after_daily_loss_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            engine=self.make_engine(tmp)
            self.assertIsNone(engine.risk_block_reason(100))
            engine.day_realized=-2.01
            self.assertIn('daily_loss',engine.risk_block_reason(100))


if __name__=='__main__':
    unittest.main()
