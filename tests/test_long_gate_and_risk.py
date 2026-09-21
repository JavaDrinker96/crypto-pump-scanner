import os
import tempfile
import unittest
from unittest.mock import patch

class SyncClient:
    ws_market=None
    def fetch_positions(self,params=None):return []


from advanced_engine import (
    Engine, Signal, btc_regime_gate, long_continuation_gate, volume_activity_ratio
)


class LongContinuationGateTests(unittest.TestCase):
    def gate(self, *, m1, m3, m5, rsi, volume, breakout=True, green=2, vwap=.5):
        return long_continuation_gate(
            m1,m3,m5,rsi,volume,breakout,green,vwap,1.2,
            min_m3_pct=.03,min_m5_pct=.10,min_rsi=52,max_rsi=72,
            max_vwap_pct=3.0,max_m5_pct=6.0,
        )

    def test_hype_like_trade_passes(self):
        allowed,checks=self.gate(
            m1=.0004285,m3=.0007501,m5=.0020386,rsi=66.80,volume=4.75
        )
        self.assertTrue(allowed,checks)

    def test_aero_like_trade_passes_with_previous_closed_volume(self):
        allowed,checks=self.gate(
            m1=.0001501,m3=.0004505,m5=.0015033,rsi=56.92,volume=1.58
        )
        self.assertTrue(allowed,checks)

    def test_mstr_like_trade_rejected_for_overheated_rsi(self):
        allowed,checks=self.gate(
            m1=.0004448,m3=.0006355,m5=.0014629,rsi=74.14,volume=5.03
        )
        self.assertFalse(allowed)
        self.assertFalse(checks['rsi'])

    def test_sei_like_trade_rejected_for_volume_and_5m_momentum(self):
        allowed,checks=self.gate(
            m1=.0002076,m3=.0004153,m5=.0006231,rsi=61.89,volume=.874
        )
        self.assertFalse(allowed)
        self.assertFalse(checks['volume'])
        self.assertFalse(checks['momentum_5m'])

    def test_pumpfun_like_trade_rejected_for_volume_and_momentum(self):
        allowed,checks=self.gate(
            m1=.0002402,m3=.0002402,m5=.0007210,rsi=56.3,volume=.164
        )
        self.assertFalse(allowed)
        self.assertFalse(checks['volume'])
        self.assertFalse(checks['momentum_3m'])
        self.assertFalse(checks['momentum_5m'])

    def test_sol_like_trade_rejected_for_volume_and_rsi(self):
        allowed,checks=self.gate(
            m1=.0003606,m3=.001444,m5=.002439,rsi=77.16,volume=.887
        )
        self.assertFalse(allowed)
        self.assertFalse(checks['volume'])
        self.assertFalse(checks['rsi'])

    def test_eth_hot_trade_rejected_for_rsi(self):
        allowed,checks=self.gate(
            m1=.0008400,m3=.001433,m5=.001697,rsi=78.30,volume=3.20
        )
        self.assertFalse(allowed)
        self.assertFalse(checks['rsi'])

    def test_eth_weak_volume_trade_rejected(self):
        allowed,checks=self.gate(
            m1=.0000776,m3=.0009636,m5=.0018901,rsi=68.03,volume=1.106
        )
        self.assertFalse(allowed)
        self.assertFalse(checks['volume'])


class VolumeFeatureTests(unittest.TestCase):
    def test_uses_previous_closed_or_projected_current_volume(self):
        rows=[]
        for i in range(24):
            rows.append([i*60000,1,1,1,1,100.0])
        rows[-2][5]=160.0
        rows[-1][5]=30.0
        now_ms=rows[-1][0]+30000
        self.assertAlmostEqual(volume_activity_ratio(rows,now_ms),1.6)

    def test_projects_partial_current_candle(self):
        rows=[]
        for i in range(24):
            rows.append([i*60000,1,1,1,1,100.0])
        rows[-2][5]=80.0
        rows[-1][5]=60.0
        now_ms=rows[-1][0]+30000
        self.assertAlmostEqual(volume_activity_ratio(rows,now_ms),1.2)


class BtcRegimeTests(unittest.TestCase):
    def test_long_rejects_negative_30m_regime(self):
        allowed,checks=btc_regime_gate(.08,-.09,'long')
        self.assertFalse(allowed)
        self.assertFalse(checks['btc_30m'])

    def test_long_allows_hype_aero_style_regime(self):
        self.assertTrue(btc_regime_gate(-.046,.210,'long')[0])
        self.assertTrue(btc_regime_gate(.194,.051,'long')[0])

    def test_short_is_mirrored(self):
        allowed,checks=btc_regime_gate(.20,.22,'short')
        self.assertFalse(allowed)
        self.assertFalse(checks['btc_15m'])


class LongConfirmationTests(unittest.TestCase):
    class StubEngine(Engine):
        def __init__(self):
            super().__init__(client=SyncClient(),alert=None)
            self.opened=[]
            self.sequence=[]

        def signal(self,symbol):
            if self.sequence:
                return self.sequence.pop(0)
            return None

        def open(self,sig):
            self.opened.append(sig.symbol)

        @staticmethod
        def sig():
            return Signal(
                symbol='X/USDT:USDT',side='long',price=1.0,atr=.01,rsi=65,vol=2.0,
                flow=.7,book=.7,vwap=.5,move5=.8,score=.7,reason='continuation',
                m1=.2,m3=.4,spread=.01,ml_prob=.8,stop_distance=.02,
            )

    def test_long_confirmation_requires_consecutive_signals(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ,{
            'TRADING_ENABLED':'true','ML_ENABLED':'false','CHRONOS_ENABLED':'false',
            'TRADE_JOURNAL_PATH':os.path.join(tmp,'trades.jsonl'),
            'SIGNAL_CONFIRM_CYCLES':'1','LONG_SIGNAL_CONFIRM_CYCLES':'2',
            'SHORT_SIGNAL_CONFIRM_CYCLES':'3',
        },clear=False):
            engine=self.StubEngine()
            engine.sequence=[engine.sig(),None,engine.sig(),engine.sig()]
            engine.run(['X/USDT:USDT'])
            self.assertEqual(engine.opened,[])
            engine.run(['X/USDT:USDT'])
            self.assertEqual(engine.opened,[])
            engine.run(['X/USDT:USDT'])
            self.assertEqual(engine.opened,[])
            engine.run(['X/USDT:USDT'])
            self.assertEqual(engine.opened,['X/USDT:USDT'])


class CircuitBreakerTests(unittest.TestCase):
    def make_engine(self,tmp,**env):
        values={
            'ML_ENABLED':'false','CHRONOS_ENABLED':'false',
            'TRADE_JOURNAL_PATH':os.path.join(tmp,'trades.jsonl'),
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
