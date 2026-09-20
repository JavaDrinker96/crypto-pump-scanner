import os
import tempfile
import unittest
from unittest.mock import Mock, patch

from advanced_engine import Position
import position_sync


class FakeClient:
    def __init__(self):
        self.requests=[]
        self.orders=[]
        self.trades=[]
        self.cancelled=[]

    def market(self,symbol):
        return {
            'id':symbol.replace('/','').replace(':USDT',''),
            'limits':{'amount':{'min':.01}},
            'info':{'lotSizeFilter':{'qtyStep':'0.01'}},
        }

    def amount_to_precision(self,symbol,qty):
        return f'{qty:.2f}'

    def request(self,path,api,method,params):
        self.requests.append((path,api,method,params))
        return {'retCode':0}

    def create_order(self,symbol,order_type,side,qty,price,params):
        order={'id':f'order-{len(self.orders)+1}'}
        self.orders.append((symbol,order_type,side,qty,price,params,order))
        return order

    def cancel_order(self,order_id,symbol,params=None):
        self.cancelled.append((order_id,symbol,params))
        return {'id':order_id}

    def fetch_my_trades(self,symbol,since=None,limit=None,params=None):
        return list(self.trades)


class DummyEngine:
    def __init__(self,client):
        self.c=client
        self.seen_execution_ids=set()
        self.day_realized=0.0
        self.journal=Mock()
        self.raw=[]

    def _journal_raw(self,event,payload):
        self.raw.append((event,payload))

    def record_exit_fill(self,value):
        self.day_realized+=value


class ProtectionTests(unittest.TestCase):
    def test_small_position_uses_tp1_tp2_and_full_stop(self):
        client=FakeClient(); engine=DummyEngine(client)
        p=Position('ETH/USDT:USDT','long',2585.4,.02,2583.1,2587.6,2589.9,2591.1,2.3)
        p.trade_id='abc123'
        position_sync._protect_exact(engine,p)
        self.assertEqual(len(client.requests),1)
        params=client.requests[0][3]
        self.assertEqual(params['tpslMode'],'Full')
        self.assertEqual(params['stopLoss'],str(p.stop))
        self.assertEqual(len(client.orders),2)
        self.assertAlmostEqual(client.orders[0][3],.01)
        self.assertAlmostEqual(client.orders[1][3],.01)
        self.assertEqual(p.tp_plan,{'TP1':.01,'TP2':.01,'TP3':0.0})
        for order in client.orders:
            self.assertTrue(order[5]['reduceOnly'])
            self.assertTrue(order[5]['closeOnTrigger'])
            self.assertEqual(order[5]['triggerBy'],'MarkPrice')

    def test_tp1_moves_full_stop_to_break_even_buffer(self):
        client=FakeClient(); engine=DummyEngine(client)
        p=Position('X/USDT:USDT','long',100,1,98,101,102,103,2)
        p.trade_id='x'; p.tp1_done=True; p.stop_moved_to_be=False
        p.sl_price=98
        engine.journal=Mock()
        with patch.dict(os.environ,{'BREAKEVEN_FEE_BUFFER_PCT':'.06'}):
            position_sync._maybe_move_stop(engine,p,.65)
        self.assertTrue(p.stop_moved_to_be)
        self.assertAlmostEqual(p.stop,100.06)
        self.assertEqual(client.requests[-1][3]['tpslMode'],'Full')
        self.assertEqual(client.requests[-1][3]['stopLoss'],str(p.stop))


class ReconciliationTests(unittest.TestCase):
    def test_order_link_id_preserves_tp_reason_despite_slippage(self):
        p=Position('X/USDT:USDT','long',100,1,98,102,104,106,2)
        reason=position_sync._classify_exit(p,{
            'side':'sell','amount':.35,'price':103.8,
            'info':{'orderLinkId':'trade-tp1'},
        })
        self.assertEqual(reason,'TP1')

    def test_private_executions_capture_fees_and_realized_pnl(self):
        client=FakeClient(); engine=DummyEngine(client)
        p=Position('X/USDT:USDT','long',100,1,98,102,104,105,2)
        p.trade_id='trade'; p.fill_time=1000; p.entry_fees=0; p.exit_fees=0
        p.realized_pnl=0; p.exit_filled_qty=0; p.tp_hits=[]; p.seen_execution_ids=set()
        client.trades=[
            {'id':'entry','order':'e','timestamp':1000001,'side':'buy','amount':1,'price':100,'fee':{'cost':.01}},
            {'id':'tp1','order':'t1','timestamp':1001000,'side':'sell','amount':.35,'price':102,'fee':{'cost':.005},
             'info':{'orderLinkId':'trade-tp1'}},
        ]
        qty=position_sync._reconcile_executions(engine,p)
        self.assertAlmostEqual(qty,.35)
        self.assertAlmostEqual(p.entry_fees,.01)
        self.assertAlmostEqual(p.exit_fees,.005)
        self.assertAlmostEqual(p.realized_pnl,.695)
        self.assertTrue(p.tp1_done)
        self.assertIn('TP1',p.tp_hits)
        self.assertAlmostEqual(engine.day_realized,.685)
        fills=[payload for event,payload in engine.raw if event=='EXECUTION_FILL']
        self.assertEqual(len(fills),2)
        self.assertEqual(fills[1]['classification'],'exit')
        self.assertAlmostEqual(fills[1]['gross_pnl'],.7)
        self.assertAlmostEqual(fills[1]['net_pnl'],.695)


if __name__=='__main__':
    unittest.main()
