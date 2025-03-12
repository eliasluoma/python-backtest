import unittest
import pandas as pd
from datetime import datetime, timedelta
import sys
import os
import logging

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.simulation.sell_simulator import SellSimulator, calculate_trade_metrics

# Set up logging for tests
logging.basicConfig(level=logging.DEBUG)


class TestSellSimulator(unittest.TestCase):
    """Test cases for the SellSimulator class."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a simple mock buy opportunity with post-entry data
        self.sample_timestamps = [datetime.now() + timedelta(minutes=i) for i in range(50)]

        # Create mock data with an uptrend followed by a downtrend
        self.mock_market_caps = [
            100,  # Entry price
            105,
            110,
            120,
            130,
            145,
            160,
            180,
            200,
            220,  # Uptrend
            230,
            235,
            240,
            245,
            250,
            255,
            260,
            265,
            270,  # Peak
            265,
            260,
            255,
            250,
            240,
            230,
            220,
            210,
            200,  # Downtrend
            195,
            190,
            185,
            180,
            175,
            170,
            165,
            160,
            155,  # Continued downtrend
            150,
            145,
            140,
            135,
            130,
            125,
            120,
            115,
            110,  # Further drop
            105,
            100,
            95,
            90,  # Final values
        ]

        # Create a DataFrame with the mock data
        data = {
            "timestamp": self.sample_timestamps,
            "marketCap": self.mock_market_caps,
            "holderDelta5s": [3] * 50,
            "holderDelta30s": [8] * 20 + [2] * 30,  # Strong holder growth then weak
            "holderDelta60s": [15] * 20 + [4] * 30,  # Strong holder growth then weak
            "marketCapChange5s": [5] * 20 + [-2] * 30,  # Positive then negative
            "buyVolume5s": [10] * 20 + [2] * 30,  # Strong then weak
            "netVolume5s": [8] * 20 + [-1] * 30,  # Positive then negative
        }

        self.mock_df = pd.DataFrame(data)

        # Create a buy opportunity dictionary
        self.buy_opportunity = {
            "pool_address": "test_pool_123",
            "entry_price": 100,
            "entry_time": self.sample_timestamps[0].isoformat(),
            "entry_row": 0,
            "entry_metrics": {"mc_change_5s": 5, "holder_delta_30s": 8, "buy_volume_5s": 10},
            "post_entry_data": self.mock_df,
        }

        # Initialize the sell simulator with default parameters
        self.sell_simulator = SellSimulator(
            initial_investment=1.0,
            base_take_profit=1.9,  # 90% profit target
            stop_loss=0.65,  # 35% loss limit
            trailing_stop=0.9,  # 10% trailing stop
        )

    def test_initialization(self):
        """Test that the SellSimulator initializes with correct parameters."""
        self.assertEqual(self.sell_simulator.initial_investment, 1.0)
        self.assertEqual(self.sell_simulator.base_take_profit, 1.9)
        self.assertEqual(self.sell_simulator.stop_loss, 0.65)
        self.assertEqual(self.sell_simulator.trailing_stop, 0.9)
        self.assertIsNotNone(self.sell_simulator.stoploss_params)
        self.assertIsNotNone(self.sell_simulator.momentum_params)

    def test_check_momentum(self):
        """Test the momentum checking functionality."""
        # Strong momentum metrics
        strong_metrics = {"mc_change_5s": 10.0, "holder_change_30s": 30.0, "buy_volume_5s": 20.0, "net_volume_5s": 15.0}

        # Weak momentum metrics
        weak_metrics = {"mc_change_5s": 2.0, "holder_change_30s": 5.0, "buy_volume_5s": 5.0, "net_volume_5s": 1.0}

        # Test with strong momentum
        self.assertTrue(self.sell_simulator.check_momentum(strong_metrics))

        # Test with weak momentum
        self.assertFalse(self.sell_simulator.check_momentum(weak_metrics))

    def test_take_profit_exit(self):
        """Test selling at take profit with momentum loss."""
        # Modify the buy opportunity to ensure we hit the take profit condition
        # Make the price rise quickly above take profit and then start dropping
        market_caps = [
            100,  # Entry price
            150,
            180,
            200,  # Quick rise
            195,
            190,
            185,  # Start dropping (momentum loss)
            180,
            175,
            170,  # Continue dropping
        ]

        data = {
            "timestamp": self.sample_timestamps[:10],
            "marketCap": market_caps,
            "holderDelta5s": [3] * 10,
            "holderDelta30s": [8] * 3 + [2] * 7,  # Strong then weak
            "holderDelta60s": [15] * 3 + [4] * 7,  # Strong then weak
            "marketCapChange5s": [10] * 3 + [-3] * 7,  # Positive then negative
            "buyVolume5s": [15] * 3 + [3] * 7,  # Strong then weak
            "netVolume5s": [12] * 3 + [-2] * 7,  # Positive then negative
        }

        mock_df = pd.DataFrame(data)

        buy_opportunity = {
            "pool_address": "test_pool_123",
            "entry_price": 100,
            "entry_time": self.sample_timestamps[0].isoformat(),
            "entry_row": 0,
            "entry_metrics": {"mc_change_5s": 5, "holder_delta_30s": 8, "buy_volume_5s": 10},
            "post_entry_data": mock_df,
        }

        # Run the sell simulation
        result = self.sell_simulator.simulate_sell(buy_opportunity)

        # Verify the result
        self.assertIsNotNone(result)
        self.assertEqual(result["exit_reason"], "Momentum Lost + Price Drop")
        self.assertGreater(result["profit_ratio"], 1.0)  # Should have profit
        self.assertLess(result["exit_price"], 200)  # Should exit after the peak

    def test_stop_loss_exit(self):
        """Test selling at stop loss."""
        # Modify the buy opportunity to ensure we hit the stop loss condition
        # Make the price drop quickly below stop loss
        market_caps = [
            100,  # Entry price
            90,
            80,
            70,
            60,  # Quick drop below stop loss (65)
            55,
            50,
            45,
            40,
            35,  # Continue dropping
        ]

        data = {
            "timestamp": self.sample_timestamps[:10],
            "marketCap": market_caps,
            "holderDelta5s": [1] * 10,
            "holderDelta30s": [2] * 10,  # Weak growth (not enough to save from stop loss)
            "holderDelta60s": [4] * 10,  # Weak growth
            "marketCapChange5s": [-5] * 10,  # Negative
            "buyVolume5s": [3] * 10,  # Weak
            "netVolume5s": [-3] * 10,  # Negative
        }

        mock_df = pd.DataFrame(data)

        buy_opportunity = {
            "pool_address": "test_pool_123",
            "entry_price": 100,
            "entry_time": self.sample_timestamps[0].isoformat(),
            "entry_row": 0,
            "entry_metrics": {"mc_change_5s": 5, "holder_delta_30s": 8, "buy_volume_5s": 10},
            "post_entry_data": mock_df,
        }

        # Run the sell simulation
        result = self.sell_simulator.simulate_sell(buy_opportunity)

        # Verify the result
        self.assertIsNotNone(result)
        self.assertEqual(result["exit_reason"], "Stop Loss")
        self.assertLess(result["profit_ratio"], 1.0)  # Should have loss
        self.assertLessEqual(result["exit_price"], 65)  # Should exit at or below stop loss

    def test_ignore_stop_loss_with_strong_growth(self):
        """Test ignoring stop loss when there's strong holder growth."""
        # Modify the buy opportunity to ensure we would hit stop loss
        # but have strong holder growth to ignore it
        market_caps = [
            100,  # Entry price
            90,
            80,
            70,
            60,  # Quick drop below stop loss (65)
            55,
            50,
            45,
            60,
            80,  # Dip then recovery
        ]

        data = {
            "timestamp": self.sample_timestamps[:10],
            "marketCap": market_caps,
            "holderDelta5s": [5] * 10,
            "holderDelta30s": [15] * 10,  # Strong growth (should ignore stop loss)
            "holderDelta60s": [60] * 10,  # Strong growth
            "marketCapChange5s": [-5] * 5 + [10] * 5,  # Negative then positive
            "buyVolume5s": [20] * 10,  # Strong
            "netVolume5s": [15] * 10,  # Strong positive
        }

        mock_df = pd.DataFrame(data)

        buy_opportunity = {
            "pool_address": "test_pool_123",
            "entry_price": 100,
            "entry_time": self.sample_timestamps[0].isoformat(),
            "entry_row": 0,
            "entry_metrics": {"mc_change_5s": 5, "holder_delta_30s": 8, "buy_volume_5s": 10},
            "post_entry_data": mock_df,
        }

        # Run the sell simulation
        result = self.sell_simulator.simulate_sell(buy_opportunity)

        # Verify the result
        self.assertIsNotNone(result)
        # Should not exit at stop loss due to strong holder growth
        self.assertNotEqual(result["exit_reason"], "Stop Loss")
        # Should continue to end of data if strong growth persists
        self.assertEqual(result["exit_reason"], "Force Sell")

    def test_low_performance_exit(self):
        """Test low performance exit."""
        # Modify the buy opportunity to ensure we hit the low performance condition
        market_caps = [
            100,  # Entry price
            105,
            110,
            115,
            119,  # Small gain (below 1.2x)
            118,
            117,
            116,
            115,
            114,  # Slight drop with holder growth slowing
        ]

        data = {
            "timestamp": self.sample_timestamps[:10],
            "marketCap": market_caps,
            "holderDelta5s": [3] * 5 + [1] * 5,
            "holderDelta30s": [8] * 5 + [1] * 5,  # Growth slows significantly
            "holderDelta60s": [15] * 5 + [1] * 5,  # Growth slows significantly
            "marketCapChange5s": [2] * 5 + [-1] * 5,  # Positive then slightly negative
            "buyVolume5s": [10] * 5 + [3] * 5,  # Strong then weak
            "netVolume5s": [8] * 5 + [0] * 5,  # Positive then neutral
        }

        mock_df = pd.DataFrame(data)

        buy_opportunity = {
            "pool_address": "test_pool_123",
            "entry_price": 100,
            "entry_time": self.sample_timestamps[0].isoformat(),
            "entry_row": 0,
            "entry_metrics": {"mc_change_5s": 5, "holder_delta_30s": 8, "buy_volume_5s": 10},
            "post_entry_data": mock_df,
        }

        # Modify the momentum params to make the test more predictable
        sell_simulator = SellSimulator(
            initial_investment=1.0,
            base_take_profit=1.9,
            stop_loss=0.65,
            trailing_stop=0.9,
            momentum_params={
                "lp_holder_growth_threshold": 2.0,
                "mc_change_threshold": 6.0,
                "holder_change_threshold": 24.5,
                "buy_volume_threshold": 13.0,
                "net_volume_threshold": 3.0,
                "required_strong": 1.0,
            },
        )

        # Run the sell simulation
        result = sell_simulator.simulate_sell(buy_opportunity)

        # Verify the result
        self.assertIsNotNone(result)
        self.assertEqual(result["exit_reason"], "Low Performance")
        self.assertGreater(result["profit_ratio"], 1.0)  # Small profit
        self.assertLess(result["profit_ratio"], 1.2)  # But less than 20%

    def test_calculate_trade_metrics(self):
        """Test calculation of trade metrics."""
        # Create sample trade results
        trades = [
            {
                "pool_address": "pool1",
                "entry_time": "2023-01-01T12:00:00",
                "exit_time": "2023-01-01T13:00:00",
                "profit_ratio": 1.5,
                "exit_reason": "Momentum Lost + Price Drop",
            },
            {
                "pool_address": "pool2",
                "entry_time": "2023-01-02T12:00:00",
                "exit_time": "2023-01-02T12:30:00",
                "profit_ratio": 0.7,
                "exit_reason": "Stop Loss",
            },
            {
                "pool_address": "pool3",
                "entry_time": "2023-01-03T12:00:00",
                "exit_time": "2023-01-03T12:45:00",
                "profit_ratio": 1.1,
                "exit_reason": "Low Performance",
            },
            {
                "pool_address": "pool4",
                "entry_time": "2023-01-04T12:00:00",
                "exit_time": "2023-01-04T14:00:00",
                "profit_ratio": 2.0,
                "exit_reason": "Force Sell",
            },
        ]

        # Calculate metrics
        metrics = calculate_trade_metrics(trades)

        # Verify metrics
        self.assertEqual(metrics["total_trades"], 4)
        self.assertEqual(metrics["profitable_trades"], 3)
        self.assertEqual(metrics["losing_trades"], 1)
        self.assertAlmostEqual(metrics["win_rate"], 0.75)
        self.assertEqual(metrics["stoploss_trades"], 1)
        self.assertEqual(metrics["tp_trades"], 1)
        self.assertEqual(metrics["lp_trades"], 1)
        self.assertEqual(metrics["force_trades"], 1)


if __name__ == "__main__":
    unittest.main()
