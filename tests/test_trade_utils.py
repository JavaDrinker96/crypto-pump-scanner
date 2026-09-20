import unittest

from trade_utils import allocate_tp_quantities, breakeven_stop


class TakeProfitAllocationTests(unittest.TestCase):
    def test_one_lot_goes_to_tp1_not_tp3(self):
        self.assertEqual(allocate_tp_quantities(.01,.01,.01),(.01,0.0,0.0))

    def test_two_lots_split_tp1_tp2(self):
        self.assertEqual(allocate_tp_quantities(.02,.01,.01),(.01,.01,0.0))

    def test_normal_allocation_sums_exactly(self):
        q1,q2,q3=allocate_tp_quantities(.41,.01,.01,.35,.35)
        self.assertAlmostEqual(q1,.14)
        self.assertAlmostEqual(q2,.14)
        self.assertAlmostEqual(q3,.13)
        self.assertAlmostEqual(q1+q2+q3,.41)


class BreakevenStopTests(unittest.TestCase):
    def test_long_stop_moves_above_entry_with_buffer(self):
        self.assertAlmostEqual(breakeven_stop(100,'long',98,.06),100.06)

    def test_short_stop_moves_below_entry_with_buffer(self):
        self.assertAlmostEqual(breakeven_stop(100,'short',102,.06),99.94)

    def test_never_loosens_existing_stop(self):
        self.assertEqual(breakeven_stop(100,'long',101,.06),101)
        self.assertEqual(breakeven_stop(100,'short',99,.06),99)


if __name__=='__main__':
    unittest.main()
