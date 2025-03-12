import unittest
from unittest.mock import patch
import pandas as pd
import os
import sys
from datetime import datetime, timedelta

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.data.firebase_service import FirebaseService
from src.data.data_processor import preprocess_pool_data, filter_pools
from src.simulation.buy_simulator import BuySimulator, get_default_parameters
from src.simulation.sell_simulator import SellSimulator


class TestFullSimulation(unittest.TestCase):
    """Integration test for the full simulation workflow."""

    @patch("src.data.firebase_service.FirebaseService.fetch_market_data")
    def setUp(self, mock_fetch_data):
        """Set up test environment with mock data."""
        # Create sample timestamps
        self.sample_timestamps = [
            (datetime.now() + timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%S") for i in range(100)
        ]

        # Initialize pool data
        pool_data = {
            "pool_address": "test_pool_123",
            "timestamp": self.sample_timestamps,
            "marketCap": [],
            "holders": [],
            "marketCapChange5s": [],
            "holderDelta5s": [],
            "holderDelta30s": [],
            "buyVolume5s": [],
            "netVolume5s": [],
            "buySellRatio10s": [],
        }

        # Generate sample data with a pattern that should trigger a buy and subsequent sell
        mc_base = 10000
        holder_base = 10

        # Phase 1: Initial flat period (0-19)
        for i in range(20):
            pool_data["marketCap"].append(mc_base)
            pool_data["holders"].append(holder_base)
            pool_data["marketCapChange5s"].append(0)
            pool_data["holderDelta5s"].append(0)
            pool_data["holderDelta30s"].append(0)
            pool_data["buyVolume5s"].append(100)
            pool_data["netVolume5s"].append(0)
            pool_data["buySellRatio10s"].append(1.0)

        # Phase 2: Acceleration period - should trigger buy around here (20-39)
        for i in range(20):
            growth_factor = (i + 1) * 2  # Increasing growth
            pool_data["marketCap"].append(mc_base * (1 + (growth_factor / 100)))
            pool_data["holders"].append(holder_base + i)
            pool_data["marketCapChange5s"].append(growth_factor)
            pool_data["holderDelta5s"].append(1)
            pool_data["holderDelta30s"].append(min(i, 10))  # Caps at 10
            pool_data["buyVolume5s"].append(100 + i * 50)
            pool_data["netVolume5s"].append(80 + i * 40)
            pool_data["buySellRatio10s"].append(2.0)

        # Phase 3: Strong growth period - price increases rapidly (40-59)
        mc_at_40 = pool_data["marketCap"][39]
        holders_at_40 = pool_data["holders"][39]
        for i in range(20):
            pool_data["marketCap"].append(mc_at_40 * (1.5 + (i * 0.1)))  # Rapid growth
            pool_data["holders"].append(holders_at_40 + i * 2)
            pool_data["marketCapChange5s"].append(15 - i * 0.5)  # Decreasing but still strong
            pool_data["holderDelta5s"].append(2)
            pool_data["holderDelta30s"].append(15)
            pool_data["buyVolume5s"].append(1000)
            pool_data["netVolume5s"].append(800)
            pool_data["buySellRatio10s"].append(2.5)

        # Phase 4: Decline period - should trigger sell (60-79)
        mc_at_60 = pool_data["marketCap"][59]
        holders_at_60 = pool_data["holders"][59]
        for i in range(20):
            pool_data["marketCap"].append(mc_at_60 * (1 - (i * 0.02)))  # Gradual decline
            pool_data["holders"].append(holders_at_60 + max(0, 5 - i))  # Slowing holder growth
            pool_data["marketCapChange5s"].append(-5 - i * 0.5)  # Negative and worsening
            pool_data["holderDelta5s"].append(max(0, 1 - int(i / 5)))
            pool_data["holderDelta30s"].append(max(0, 8 - int(i / 3)))  # Decreasing holder growth
            pool_data["buyVolume5s"].append(max(100, 500 - i * 25))  # Decreasing volume
            pool_data["netVolume5s"].append(-100 - i * 10)  # Negative net volume
            pool_data["buySellRatio10s"].append(max(0.5, 1.0 - i * 0.025))

        # Phase 5: Continued decline (80-99)
        mc_at_80 = pool_data["marketCap"][79]
        holders_at_80 = pool_data["holders"][79]
        for i in range(20):
            pool_data["marketCap"].append(mc_at_80 * (0.9 - (i * 0.02)))  # Continued decline
            pool_data["holders"].append(holders_at_80)  # Stagnant holder count
            pool_data["marketCapChange5s"].append(-10)
            pool_data["holderDelta5s"].append(0)
            pool_data["holderDelta30s"].append(0)
            pool_data["buyVolume5s"].append(100)
            pool_data["netVolume5s"].append(-200)
            pool_data["buySellRatio10s"].append(0.5)

        # Create the DataFrame
        self.mock_df = pd.DataFrame(pool_data)

        # Mock the fetch_market_data method to return our test data
        mock_fetch_data.return_value = self.mock_df

        # Create a pool dictionary as expected by filter_pools
        self.mock_pools = {"test_pool_123": self.mock_df}

        # Initialize components
        self.firebase_service = FirebaseService(env_file=None, credentials_json={})

        # Use parameters that will match our mock data
        custom_buy_params = get_default_parameters()
        custom_buy_params.update({"mc_change_5s": 10.0, "holder_delta_30s": 5, "buy_volume_5s": 500})

        self.buy_simulator = BuySimulator(
            early_mc_limit=1000000, min_delay=25, max_delay=35, buy_params=custom_buy_params
        )

        self.sell_simulator = SellSimulator(
            initial_investment=1.0, base_take_profit=1.5, stop_loss=0.7, trailing_stop=0.9
        )

    def test_full_simulation_flow(self):
        """Test the entire simulation workflow."""
        # Fetch data from mocked service
        data = self.firebase_service.fetch_market_data()
        self.assertFalse(data.empty)

        # Filter pools
        filtered_pools = filter_pools({"test_pool_123": data}, min_data_points=50)
        self.assertEqual(len(filtered_pools), 1)

        # Get pool data
        pool_df = filtered_pools["test_pool_123"]

        # Preprocess data
        processed_df = preprocess_pool_data(pool_df)
        self.assertGreater(len(processed_df), 0)

        # Find buy opportunity
        buy_opportunity = self.buy_simulator.find_buy_opportunity(processed_df)
        self.assertIsNotNone(buy_opportunity)

        # Verify buy opportunity details
        self.assertEqual(buy_opportunity["pool_address"], "test_pool_123")
        self.assertIn("entry_price", buy_opportunity)
        self.assertIn("entry_time", buy_opportunity)
        self.assertIn("entry_row", buy_opportunity)
        self.assertIn("entry_metrics", buy_opportunity)
        self.assertIn("post_entry_data", buy_opportunity)

        # Entry should be around the acceleration phase
        entry_row = buy_opportunity["entry_row"]
        self.assertGreaterEqual(entry_row, 25)
        self.assertLessEqual(entry_row, 35)

        # Simulate sell
        trade_result = self.sell_simulator.simulate_sell(buy_opportunity)
        self.assertIsNotNone(trade_result)

        # Verify trade result
        self.assertEqual(trade_result["pool_address"], "test_pool_123")
        self.assertIn("exit_price", trade_result)
        self.assertIn("exit_time", trade_result)
        self.assertIn("exit_reason", trade_result)
        self.assertIn("profit_ratio", trade_result)

        # Should have profitable exit
        self.assertGreater(trade_result["profit_ratio"], 1.0)


if __name__ == "__main__":
    unittest.main()
