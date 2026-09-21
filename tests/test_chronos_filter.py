import unittest
import numpy as np

from chronos_filter import analyze_quantile_forecast, accept_forecast


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
