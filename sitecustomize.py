"""Runtime patch: place SL/partial TP on Bybit instead of Railway polling."""
import logging

try:
    import advanced_engine
except Exception:
    logging.exception("PROTECTION PATCH | failed to import advanced_engine")
else:
    _original_open = advanced_engine.Engine.open

    def _amount(engine, symbol, value):
        return float(engine.c.amount_to_precision(symbol, value))

    def _protect(self, p):
        sg = 1 if p.side == 'long' else -1
        close_side = 'sell' if sg == 1 else 'buy'
        trigger_down = 2
        trigger_up = 1
        sl_direction = trigger_down if sg == 1 else trigger_up
        tp_direction = trigger_up if sg == 1 else trigger_down
        base = {
            'positionIdx': 0,
            'reduceOnly': True,
            'closeOnTrigger': True,
            'triggerBy': 'MarkPrice',
        }
        orders = []
        try:
            sl_qty = _amount(self, p.symbol, p.qty)
            sl = self.c.create_order(
                p.symbol, 'market', close_side, sl_qty, None,
                {**base, 'triggerPrice': p.stop, 'triggerDirection': sl_direction}
            )
            orders.append(('SL', sl.get('id')))

            parts = [
                ('TP1', p.tp1, self.tq1),
                ('TP2', p.tp2, self.tq2),
                ('TP3', p.tp3, max(0.0, 1.0 - self.tq1 - self.tq2)),
            ]
            for name, price, fraction in parts:
                qty = _amount(self, p.symbol, p.qty * fraction)
                if qty <= 0:
                    continue
                o = self.c.create_order(
                    p.symbol, 'market', close_side, qty, None,
                    {**base, 'triggerPrice': price, 'triggerDirection': tp_direction}
                )
                orders.append((name, o.get('id')))
            logging.info(
                'EXCHANGE PROTECTION | symbol=%s | side=%s | SL=%s | TP1=%s | TP2=%s | TP3=%s | orders=%s',
                p.symbol, p.side, p.stop, p.tp1, p.tp2, p.tp3, orders
            )
            self.journal('protection', p, {'exchange_orders': orders, 'mode': 'exchange_trigger_orders'})
        except Exception:
            logging.exception('EXCHANGE PROTECTION FAILED | symbol=%s | side=%s', p.symbol, p.side)
            raise

    def _open_with_exchange_protection(self, s):
        before = set(self.pos)
        _original_open(self, s)
        if s.symbol not in self.pos or s.symbol in before:
            return
        p = self.pos[s.symbol]
        try:
            _protect(self, p)
        except Exception:
            logging.error('UNPROTECTED POSITION | symbol=%s | closing immediately', s.symbol)
            try:
                q = _amount(self, p.symbol, p.qty * p.remaining)
                if q > 0:
                    self.c.create_order(
                        p.symbol, 'market',
                        'sell' if p.side == 'long' else 'buy',
                        q, None, {'reduceOnly': True, 'positionIdx': 0}
                    )
            finally:
                self.pos.pop(s.symbol, None)
            raise

    def _exchange_managed_only(self):
        # SL/TP live on Bybit. No Railway-side ticker polling or synthetic trailing SL.
        return

    advanced_engine.Engine.open = _open_with_exchange_protection
    advanced_engine.Engine.manage = _exchange_managed_only
    logging.info('PROTECTION PATCH | exchange-native SL/TP enabled')
