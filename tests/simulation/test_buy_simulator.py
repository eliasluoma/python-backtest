import unittest
import pandas as pd
from datetime import datetime, timedelta
import sys
import os
import logging

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.simulation.buy_simulator import BuySimulator, calculate_returns, get_default_parameters

# Set up logging for tests
logging.basicConfig(level=logging.DEBUG)


class TestBuySimulator(unittest.TestCase):
    """Test cases for the BuySimulator class."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a simple mock dataset with timestamps
        self.sample_timestamps = [datetime.now() + timedelta(minutes=i) for i in range(50)]

        # Market cap values showing initial low value followed by growth
        self.mock_market_caps = [
            10000,  # Initial MC
            10500,
            11000,
            12000,
            14000,  # Gradual growth
            20000,
            30000,
            40000,
            50000,  # Accelerating growth
            70000,
            95000,
            130000,
            180000,  # Rapid growth
            200000,
            240000,
            250000,
            280000,  # Peak growth
            320000,
            340000,
            360000,
            400000,  # Continued growth but slowing
            420000,
            430000,
            435000,
            440000,  # Plateau
            445000,
            450000,
            455000,
            460000,  # Slight growth
            465000,
            470000,
            475000,
            480000,  # Continued slight growth
            485000,
            490000,
            495000,
            500000,  # Stable growth
            505000,
            510000,
            515000,
            520000,  # Final values showing stability
            525000,
            530000,
            535000,
            540000,
            545000,
            550000,
        ]

        # Create mock holder data starting with a small number and growing
        self.mock_holders = [
            5,  # Initial holders
            7,
            10,
            15,
            20,  # Early adopters
            25,
            30,
            35,
            40,  # Growing interest
            45,
            50,
            55,
            60,  # Continued growth
            65,
            70,
            75,
            80,  # Strong growth
            85,
            90,
            95,
            100,  # Peak growth
            105,
            110,
            115,
            120,  # Continued growth
        ] + [
            125
        ] * 26  # Stabilization

        # Create market cap change metrics (5s intervals)
        # Showing strong growth early, then moderation
        self.mock_mc_change_5s = [
            5.0,
            6.0,
            8.0,
            10.0,
            15.0,  # Early strong growth
            20.0,
            25.0,
            20.0,
            18.0,
            15.0,  # Peak growth
            12.0,
            10.0,
            8.0,
            6.0,
            5.0,  # Slowing growth
            4.0,
            3.0,
            2.5,
            2.0,
            1.5,  # Stabilizing
        ] + [
            1.0
        ] * 30  # Stable growth

        # Create holder delta metrics (30s intervals)
        # Showing increasing holder growth
        self.mock_holder_delta_30s = [
            2,
            3,
            5,
            7,
            10,  # Early growth
            12,
            15,
            18,
            20,
            22,  # Strong growth
            20,
            18,
            15,
            12,
            10,  # Slowing growth
            8,
            6,
            5,
            4,
            3,  # Stabilizing
        ] + [
            2
        ] * 30  # Stable growth

        # Create buy volume metrics
        self.mock_buy_volume_5s = [
            100,
            150,
            200,
            300,
            500,  # Early volume
            800,
            1200,
            1500,
            1800,
            2000,  # Peak volume
            1800,
            1600,
            1400,
            1200,
            1000,  # Declining volume
            800,
            600,
            500,
            400,
            300,  # Lower volume
        ] + [
            200
        ] * 30  # Stable volume

        # Create DataFrame with the mock data
        data = {
            "timestamp": self.sample_timestamps,
            "marketCap": self.mock_market_caps,
            "holders": self.mock_holders,
            "marketCapChange5s": self.mock_mc_change_5s,
            "holderDelta30s": self.mock_holder_delta_30s,
            "buyVolume5s": self.mock_buy_volume_5s,
            # Additional metrics that might be used
            "netVolume5s": self.mock_buy_volume_5s,  # Simplified for tests
            "buyVolumeChange": [5.0] * 50,
            "buySellRatio10s": [2.0] * 50,
            "largeBuys5s": [1] * 50,
        }

        self.mock_df = pd.DataFrame(data)

        # Initialize the buy simulator with default parameters
        self.buy_simulator = BuySimulator(
            early_mc_limit=400000,  # Set limit high enough for our test data
            min_delay=10,
            max_delay=30,
            buy_params=get_default_parameters(),
        )

    def test_initialization(self):
        """Test that the BuySimulator initializes with correct parameters."""
        self.assertEqual(self.buy_simulator.early_mc_limit, 400000)
        self.assertEqual(self.buy_simulator.min_delay, 10)
        self.assertEqual(self.buy_simulator.max_delay, 30)
        self.assertIsNotNone(self.buy_simulator.buy_params)

        # Check default parameters
        params = get_default_parameters()
        self.assertIn("mc_change_5s", params)
        self.assertIn("holder_delta_30s", params)
        self.assertIn("buy_volume_5s", params)

    def test_find_buy_opportunity_success(self):
        """Test finding a valid buy opportunity with good market conditions."""
        # Modify buy parameters to match our test data for a successful buy
        buy_params = {
            "mc_change_5s": 12.0,  # Set lower than our peak growth
            "mc_change_30s": 8.0,
            "holder_delta_30s": 10,  # Set lower than our peak holder growth
            "buy_volume_5s": 800,  # Set lower than our peak volume
            "net_volume_5s": 500,
            "buy_sell_ratio_10s": 1.5,
            "mc_growth_from_start": 0.0,  # Always pass this check
            "holder_growth_from_start": 10,  # Set lower than our growth
            "large_buy_5s": 0,
            "price_change": 0.0,  # Not used in test
        }

        # Create a simulator with our custom parameters
        simulator = BuySimulator(early_mc_limit=400000, min_delay=5, max_delay=15, buy_params=buy_params)

        # Find buy opportunity
        buy_opportunity = simulator.find_buy_opportunity(self.mock_df)

        # Verify a buy opportunity was found
        self.assertIsNotNone(buy_opportunity)
        self.assertIn("pool_address", buy_opportunity)
        self.assertIn("entry_price", buy_opportunity)
        self.assertIn("entry_time", buy_opportunity)
        self.assertIn("entry_row", buy_opportunity)
        self.assertIn("entry_metrics", buy_opportunity)
        self.assertIn("post_entry_data", buy_opportunity)

        # Check entry price is from an appropriate point in the data
        # (after min_delay but before max_delay)
        entry_row = buy_opportunity["entry_row"]
        self.assertGreaterEqual(entry_row, 5)  # min_delay
        self.assertLessEqual(entry_row, 15)  # max_delay

    def test_find_buy_opportunity_rejected(self):
        """Test rejecting a buy opportunity due to insufficient growth."""
        # Set buy parameters too high for our test data
        strict_params = {
            "mc_change_5s": 30.0,  # Higher than our peak growth
            "mc_change_30s": 50.0,
            "holder_delta_30s": 25,  # Higher than our peak holder growth
            "buy_volume_5s": 3000,  # Higher than our peak volume
            "net_volume_5s": 2500,
            "buy_sell_ratio_10s": 5.0,
            "mc_growth_from_start": 1000.0,  # Impossible to achieve
            "holder_growth_from_start": 200,  # Higher than our growth
            "large_buy_5s": 5,
            "price_change": 50.0,
        }

        # Create a simulator with our strict parameters
        simulator = BuySimulator(early_mc_limit=400000, min_delay=5, max_delay=15, buy_params=strict_params)

        # Try to find buy opportunity
        buy_opportunity = simulator.find_buy_opportunity(self.mock_df)

        # Verify no buy opportunity was found
        self.assertIsNone(buy_opportunity)

    def test_calculate_returns(self):
        """Test calculating potential returns from a buy opportunity."""
        # Create a sample buy opportunity
        entry_row = 10  # Index in our test data
        buy_opportunity = {
            "pool_address": "test_pool_123",
            "entry_price": self.mock_market_caps[entry_row],
            "entry_time": self.sample_timestamps[entry_row].isoformat(),
            "entry_row": entry_row,
            "entry_metrics": {
                "mc_change_5s": self.mock_mc_change_5s[entry_row],
                "holder_delta_30s": self.mock_holder_delta_30s[entry_row],
                "buy_volume_5s": self.mock_buy_volume_5s[entry_row],
            },
            "post_entry_data": self.mock_df.iloc[entry_row:].reset_index(drop=True),
        }

        # Calculate returns
        result = calculate_returns(buy_opportunity)

        # Verify return calculations
        self.assertIn("max_return", result)
        self.assertIn("realistic_return", result)
        self.assertIn("time_to_max", result)

        # Check if max return matches expected value
        entry_price = self.mock_market_caps[entry_row]
        max_price = max(self.mock_market_caps[entry_row:])
        expected_max_return = max_price / entry_price

        self.assertAlmostEqual(result["max_return"], expected_max_return, delta=0.01)

        # Realistic return should be less than or equal to max return
        self.assertLessEqual(result["realistic_return"], result["max_return"])

        # Time to max should be positive
        self.assertGreaterEqual(result["time_to_max"], 0)


if __name__ == "__main__":
    unittest.main()
