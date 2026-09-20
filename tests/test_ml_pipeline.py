import unittest

from ml_pipeline import FEATURE_NAMES, calibrate_threshold, triple_barrier_label


def rows_with_constant_range(count=40,price=100.0):
    rows=[]
    for i in range(count):
        rows.append([i*60000,price,price+.5,price-.5,price,1000.0])
    return rows


class TripleBarrierTests(unittest.TestCase):
    def test_long_tp_before_sl_is_positive(self):
        rows=rows_with_constant_range()
        i=20
        rows[i+1]=[(i+1)*60000,100,102.0,99.5,101,1000]
        self.assertEqual(triple_barrier_label(rows,i,'long',1.0,1.0,5),1)

    def test_long_sl_before_tp_is_negative(self):
        rows=rows_with_constant_range()
        i=20
        rows[i+1]=[(i+1)*60000,100,100.3,98.0,99,1000]
        self.assertEqual(triple_barrier_label(rows,i,'long',1.0,1.0,5),0)

    def test_same_candle_tp_and_sl_is_conservatively_negative(self):
        rows=rows_with_constant_range()
        i=20
        rows[i+1]=[(i+1)*60000,100,102.0,98.0,100,1000]
        self.assertEqual(triple_barrier_label(rows,i,'long',1.0,1.0,5),0)

    def test_short_tp_before_sl_is_positive(self):
        rows=rows_with_constant_range()
        i=20
        rows[i+1]=[(i+1)*60000,100,100.4,98.0,99,1000]
        self.assertEqual(triple_barrier_label(rows,i,'short',1.0,1.0,5),1)

    def test_feature_schema_excludes_live_only_orderflow(self):
        self.assertNotIn('flow',FEATURE_NAMES)
        self.assertNotIn('book',FEATURE_NAMES)
        self.assertEqual(len(FEATURE_NAMES),8)

    def test_threshold_calibration_chooses_target_precision(self):
        result=calibrate_threshold(
            [.51,.52,.60,.61,.70,.71],
            [0,0,1,0,1,1],
            minimum_samples=2,
            target_precision=.75,
        )
        self.assertGreaterEqual(result['precision'],.75)
        self.assertTrue(result['target_met'])
        self.assertGreaterEqual(result['threshold'],.53)


if __name__=='__main__':
    unittest.main()
