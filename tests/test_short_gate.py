import os
import unittest
from unittest.mock import patch

from advanced_engine import Engine, Signal, short_exhaustion_gate


class ShortExhaustionGateTests(unittest.TestCase):
    def gate(self, *, m1, m3, m5, rsi, volume=3.0, reversal=True, vwap=1.0):
        return short_exhaustion_gate(
            m1, m3, m5, rsi, volume, reversal, vwap, 1.4
        )

    def test_cross_like_reversal_passes(self):
        allowed, checks = self.gate(
            m1=-0.0041309, m3=0.0109422, m5=0.00889296,
            rsi=67.15, volume=3.57, vwap=1.44,
        )
        self.assertTrue(allowed, checks)

    def test_flock_like_chase_short_is_rejected(self):
        allowed, checks = self.gate(
            m1=-0.0037568, m3=-0.0059697, m5=-0.0126862,
            rsi=31.25, volume=3.60, vwap=1.09,
        )
        self.assertFalse(allowed)
        self.assertFalse(checks['prior_pump'])
        self.assertFalse(checks['rsi'])
        self.assertFalse(checks['not_already_dumped'])

    def test_1000pepe_like_chase_short_is_rejected(self):
        allowed, checks = self.gate(
            m1=-0.0005241, m3=-0.0070294, m5=-0.0067708,
            rsi=39.79, volume=2.64, vwap=0.506,
        )
        self.assertFalse(allowed)
        self.assertFalse(checks['prior_pump'])
        self.assertFalse(checks['rsi'])
        self.assertFalse(checks['not_already_dumped'])

    def test_coti_like_chase_short_is_rejected(self):
        allowed, checks = self.gate(
            m1=-0.0032917, m3=-0.0070888, m5=-0.0051698,
            rsi=32.91, volume=3.04, vwap=0.807,
        )
        self.assertFalse(allowed)
        self.assertFalse(checks['prior_pump'])
        self.assertFalse(checks['rsi'])
        self.assertFalse(checks['not_already_dumped'])

    def test_reversal_is_mandatory(self):
        allowed, checks = self.gate(
            m1=0.001, m3=0.010, m5=0.020,
            rsi=75.0, volume=3.0, reversal=False, vwap=2.0,
        )
        self.assertFalse(allowed)
        self.assertFalse(checks['reversal'])


class ShortConfirmationTests(unittest.TestCase):
    class StubEngine(Engine):
        def __init__(self):
            super().__init__(client=object(), alert=None)
            self.opened = []
            self.confirm = 1

        def signal(self, symbol):
            return Signal(
                symbol=symbol, side='short', price=1.0, atr=0.01,
                rsi=70.0, vol=3.0, flow=0.3, book=0.3,
                vwap=1.0, move5=1.2, score=0.7, reason='exhaustion',
                m1=-0.1, m3=0.5, spread=0.01, ml_prob=0.8,
            )

        def open(self, sig):
            self.opened.append(sig.symbol)

    def test_short_requires_two_confirmations_even_when_global_confirm_is_one(self):
        engine = self.StubEngine()
        with patch.dict(os.environ, {'TRADING_ENABLED': 'true'}):
            engine.run(['X/USDT:USDT'])
            self.assertEqual(engine.opened, [])
            engine.run(['X/USDT:USDT'])
            self.assertEqual(engine.opened, ['X/USDT:USDT'])


if __name__ == '__main__':
    unittest.main()
