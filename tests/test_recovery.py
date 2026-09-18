"""Offline regression tests; no exchange, Telegram, or model-training calls."""
import importlib
import os
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock, patch

# Optional live-service libraries are not needed for these offline scenarios.
try:
    import ccxt
except ImportError:
    ccxt = types.ModuleType('ccxt')
    base = types.ModuleType('ccxt.base')
    errors = types.ModuleType('ccxt.base.errors')
    for name in ('AuthenticationError', 'RateLimitExceeded', 'BadRequest'):
        setattr(errors, name, type(name, (Exception,), {}))
    sys.modules.update({'ccxt': ccxt, 'ccxt.base': base, 'ccxt.base.errors': errors})

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import position_sync
from advanced_engine import Engine, Position, Signal
from notification_control import IncidentAlerts

SYMBOL = 'BTC/USDT:USDT'


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {'ML_ENABLED': 'false', 'ML_AUTO_TRAIN': 'false'})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.client = Mock()
        self.client.ws_market = None
        self.client.fetch_positions.return_value = []
        self.engine = Engine(self.client, Mock())

    def live(self, **kwargs):
        data = dict(symbol=SYMBOL, contracts=2, side='long', entryPrice=100,
                    stopLossPrice=95)
        data.update(kwargs)
        self.client.fetch_positions.return_value = [data]

    def test_recovers_position_without_overwriting_native_exits(self):
        self.live()
        self.assertTrue(position_sync.sync(self.engine))
        pos = self.engine.pos[SYMBOL]
        self.assertEqual((pos.symbol, pos.side, pos.entry, pos.qty, pos.stop),
                         (SYMBOL, 'long', 100, 2, 95))
        self.assertIsNone(pos.risk)
        self.client.create_order.assert_not_called()
        self.client.request.assert_not_called()
        self.client.fetch_positions.assert_called_once_with(
            params={'category': 'linear', 'settleCoin': 'USDT'})

    def test_raw_short_position_and_missing_stop(self):
        self.live(contracts=None, side=None, entryPrice=None, stopLossPrice=None,
                  info={'size': '3', 'side': 'Sell', 'avgPrice': '101'})
        self.assertTrue(position_sync.sync(self.engine))
        pos = self.engine.pos[SYMBOL]
        self.assertEqual((pos.side, pos.qty, pos.entry, pos.stop), ('short', 3, 101, 0))
        self.assertIsNone(pos.risk)

    def test_repeated_sync_does_not_repeat_recovery_alert(self):
        self.live()
        position_sync.sync(self.engine)
        position_sync.sync(self.engine)
        self.engine.alert.assert_called_once()

    def test_existing_risk_and_targets_survive_partial_fill(self):
        pos = Position(SYMBOL, 'long', 100, 4, 95, 105, 110, 115, 5)
        self.engine.pos[SYMBOL] = pos
        self.live()
        position_sync.sync(self.engine)
        self.assertIs(self.engine.pos[SYMBOL], pos)
        self.assertEqual((pos.qty, pos.risk, pos.tp1, pos.tp2, pos.tp3), (2, 5, 105, 110, 115))

    def test_closed_position_clears_local_state(self):
        self.live()
        position_sync.sync(self.engine)
        self.engine.pending[SYMBOL] = ('long', 1)
        self.client.fetch_positions.return_value = []
        position_sync.sync(self.engine)
        self.assertEqual(self.engine.pos, {})
        self.assertEqual(self.engine.pending, {})

    def test_api_failure_preserves_positions_and_blocks_scan(self):
        self.engine.pos[SYMBOL] = Position(SYMBOL, 'long', 100, 2, 95, 105, 110, 115, 5)
        self.client.fetch_positions.side_effect = RuntimeError('exchange unavailable')
        with patch.object(position_sync, '_original_run') as scan:
            with self.assertRaisesRegex(RuntimeError, 'synchronization failed'):
                self.engine.run([SYMBOL])
            scan.assert_not_called()
        self.assertIn(SYMBOL, self.engine.pos)
        self.client.create_order.assert_not_called()

    def test_recovered_position_allows_scan_but_blocks_new_entry(self):
        self.live()
        sig = Signal('ETH/USDT:USDT', 'long', 100, 1, 60, 2, .7, .7, 1, 1, .8, 'test')
        self.engine.signal = Mock(return_value=sig)
        self.engine.confirm = 1
        with patch.dict(os.environ, {'TRADING_ENABLED': 'true'}):
            with patch.object(position_sync, '_original_open') as open_order:
                result = self.engine.run([sig.symbol])
                self.assertEqual(result, {'signals': 1, 'errors': 0})
                open_order.assert_not_called()
        self.client.create_order.assert_not_called()

    def test_normal_entry_path_is_preserved_without_unknown_risk(self):
        sig = Mock()
        with patch.object(position_sync, '_original_open', return_value='opened') as opener:
            self.assertEqual(self.engine.open(sig), 'opened')
            opener.assert_called_once_with(self.engine, sig)


class IncidentTests(unittest.TestCase):
    def setUp(self):
        self.now = 0
        self.send = Mock(return_value=True)
        self.alerts = IncidentAlerts(self.send, clock=lambda: self.now)

    def test_fifteen_second_errors_send_once_then_reminder_at_five_minutes(self):
        for self.now in range(0, 300, 15):
            self.alerts.notify('scanner', 'error')
        self.send.assert_called_once_with('error')
        self.now = 300
        self.alerts.notify('scanner', 'error')
        self.assertEqual(self.send.call_count, 2)

    def test_new_incident_after_recovery_notifies_immediately(self):
        self.alerts.notify('scanner', 'error')
        self.alerts.clear('scanner')
        self.now = 15
        self.alerts.notify('scanner', 'error')
        self.assertEqual(self.send.call_count, 2)

    def test_websocket_incident_is_independent(self):
        self.alerts.notify('scanner', 'scan error')
        self.alerts.notify('websocket', 'ws error')
        self.assertEqual(self.send.call_count, 2)

    def test_failed_telegram_attempt_is_also_throttled(self):
        self.send.return_value = False
        self.alerts.notify('scanner', 'error')
        self.now = 15
        self.alerts.notify('scanner', 'error')
        self.send.assert_called_once()

    def test_actual_scanner_loop_throttles_repeated_failures(self):
        # Import the entrypoint with only its websocket transport stubbed.
        ws_module = types.ModuleType('ws_market')
        ws_module.BybitMarketWS = Mock()
        with patch.dict(sys.modules, {'ws_market': ws_module}):
            scanner = importlib.import_module('pump_scanner')
        markets = {str(i): dict(active=True, linear=True, swap=True, quote='USDT')
                   for i in range(10)}
        ws = Mock()
        ws.is_healthy.return_value = True
        ws.sym.side_effect = lambda s: s
        ws.ticker_snapshot.return_value = {
            s: dict(turnover24h=10000000, lastPrice=100) for s in markets}
        engine = Mock()
        engine.run.side_effect = RuntimeError('test scanner failure')
        with patch.object(scanner, 'create_pump_scanner_from_config'), \
             patch.object(scanner, 'validate_live_credentials', return_value=False), \
             patch.object(scanner, 'load_public_markets', return_value=markets), \
             patch.object(scanner, 'BybitMarketWS', return_value=ws), \
             patch.object(scanner, 'Engine', return_value=engine), \
             patch.object(scanner, 'alert', return_value=True) as send, \
             patch.object(scanner.time, 'sleep', side_effect=[None, KeyboardInterrupt]), \
             patch.dict(os.environ, {}, clear=False):
            with self.assertRaises(KeyboardInterrupt):
                scanner.main()
            errors = [c for c in send.call_args_list if 'scanner error' in c.args[0]]
            self.assertEqual(len(errors), 1)
            self.assertEqual(engine.run.call_count, 2)


if __name__ == '__main__':
    unittest.main()
