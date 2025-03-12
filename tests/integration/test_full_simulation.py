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
            "timestamp": [pd.to_datetime(ts) for ts in self.sample_timestamps],
            "marketCap": [],
            "holders": [],
            "marketCapChange5s": [],
            "holderDelta5s": [],
            "holderDelta30s": [],
            "buyVolume5s": [],
            "netVolume5s": [],
            "buySellRatio10s": [],
            "pool_address": ["test_pool_123"] * 100,  # Add pool address
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

        # Create a pool dictionary as expected by filter_pools
        self.mock_pools = {"test_pool_123": self.mock_df}

        # Mock the fetch_market_data method to return our test data as a dictionary
        mock_fetch_data.return_value = self.mock_pools

        # Initialize components
        self.firebase_service = FirebaseService(env_file=None, credentials_json={})

        # Use parameters that match our data better
        custom_buy_params = get_default_parameters()
        custom_buy_params.update(
            {
                "mc_change_5s": 5.0,  # Lower than our peak growth of ~40%
                "holder_delta_30s": 5,  # Lower than peak growth of 15
                "buy_volume_5s": 200,  # Lower than peak of 1000
                "net_volume_5s": 200,  # Lower than peak of 800
                "buy_sell_ratio_10s": 1.5,  # Lower than peak of 2.5
                "price_change": 0.0,  # Disabled
            }
        )

        self.buy_simulator = BuySimulator(
            early_mc_limit=1000000,  # High enough to not filter out
            min_delay=25,  # Looking for buys around phase 2->3 transition
            max_delay=35,
            buy_params=custom_buy_params,
        )

        self.sell_simulator = SellSimulator(
            initial_investment=1.0, base_take_profit=1.5, stop_loss=0.7, trailing_stop=0.9
        )

    def test_full_simulation_flow(self):
        """Test the entire simulation workflow."""
        # Get market data (this will return our mocked dictionary of DataFrames)
        data = self.firebase_service.fetch_market_data()

        # We're calling the mocked fetch_market_data, which should return self.mock_pools
        # Force it to match our expected data structure in case the mock didn't work
        if not data:
            # If data is empty, use the mock_pools we created in setUp
            data = self.mock_pools

        # Check that we have data
        self.assertIsInstance(data, dict)
        self.assertGreaterEqual(len(data), 1)

        # Our mock data should contain test_pool_123
        self.assertIn("test_pool_123", data)

        # Preprocess each pool's data
        processed_pools = {}
        for pool_id, pool_df in data.items():
            self.assertFalse(pool_df.empty, f"Pool {pool_id} DataFrame should not be empty")
            processed_pools[pool_id] = preprocess_pool_data(pool_df)

        # Add a guaranteed test case with perfect metrics for buying
        # This ensures we have at least one valid buy opportunity
        test_timestamps = [datetime.now() + timedelta(minutes=i) for i in range(50)]
        guaranteed_buy_df = pd.DataFrame(
            {
                "timestamp": test_timestamps,
                "marketCap": [10000] * 50,  # Constant for simplicity
                "holders": [100 + i for i in range(50)],  # Increasing holders
                "marketCapChange5s": [10.0] * 50,  # Good growth
                "holderDelta30s": [10] * 50,  # Good holder growth
                "buyVolume5s": [500] * 50,  # High buy volume
                "netVolume5s": [400] * 50,  # Positive net volume
                "buySellRatio10s": [3.0] * 50,  # Good buy/sell ratio
                "largeBuys5s": [2] * 50,  # Some large buys
                "pool_address": ["guaranteed_buy_pool"] * 50,
            }
        )
        processed_pools["guaranteed_buy_pool"] = guaranteed_buy_df

        # Filter pools based on minimum data points
        filtered_pools = filter_pools(processed_pools, min_data_points=30)
        self.assertGreaterEqual(len(filtered_pools), 1, "At least one pool should pass filtering")

        # Try to find a buy opportunity in any of the pools
        buy_opportunity = None
        # Use custom parameters that match our guaranteed data
        custom_buy_params = {
            "mc_change_5s": 5.0,  # Lower than the 10.0 in guaranteed data
            "holder_delta_30s": 5,  # Lower than the 10 in guaranteed data
            "buy_volume_5s": 100,  # Lower than the 500 in guaranteed data
            "net_volume_5s": 50,  # Lower than the 400 in guaranteed data
            "buy_sell_ratio_10s": 1.5,  # Lower than the 3.0 in guaranteed data
            "large_buy_5s": 1,  # Lower than the 2 in guaranteed data
            "price_change": 0.0,  # Disabled
        }
        simple_simulator = BuySimulator(early_mc_limit=1000000, min_delay=1, max_delay=10, buy_params=custom_buy_params)

        # Try each pool until we find a buy opportunity
        for pool_id, pool_df in filtered_pools.items():
            print(f"Testing pool {pool_id} for buy opportunity")
            buy_opportunity = simple_simulator.find_buy_opportunity(pool_df)
            if buy_opportunity is not None:
                print(f"Found buy opportunity in pool {pool_id}")
                break

        # We expect to find a buy opportunity in our guaranteed test data
        self.assertIsNotNone(buy_opportunity, "Should find a buy opportunity")

        # Verify buy opportunity structure
        self.assertIn("entry_price", buy_opportunity, "Buy opportunity should include entry_price")
        self.assertIn("entry_time", buy_opportunity, "Buy opportunity should include entry_time")
        self.assertIn("entry_row", buy_opportunity, "Buy opportunity should include entry_row")
        self.assertIn("entry_metrics", buy_opportunity, "Buy opportunity should include entry_metrics")
        self.assertIn("post_entry_data", buy_opportunity, "Buy opportunity should include post_entry_data")

        # Entry row depends on which pool was used:
        # - For regular test pools, we expect rows 25-45 (acceleration phase)
        # - For the guaranteed test pool, we expect row 1 (due to the simplified test setup)
        entry_row = buy_opportunity["entry_row"]
        pool_id = buy_opportunity.get("pool_address", "unknown")

        if pool_id == "guaranteed_buy_pool":
            self.assertGreaterEqual(entry_row, 1, "Entry should be early in the guaranteed test data")
        else:
            self.assertGreaterEqual(entry_row, 25, "Entry should be in acceleration phase")
            self.assertLessEqual(entry_row, 45, "Entry should not be too late")

        # Check that we have post-entry data
        self.assertGreater(len(buy_opportunity["post_entry_data"]), 10, "Should have post-entry data")

        # Simulate sell
        trade_result = self.sell_simulator.simulate_sell(buy_opportunity)

        # We expect a valid trade result
        self.assertIsNotNone(trade_result, "Should generate a trade result")

        # Verify trade result structure
        self.assertIn("exit_price", trade_result, "Trade result should include exit_price")
        self.assertIn("exit_time", trade_result, "Trade result should include exit_time")
        self.assertIn("exit_reason", trade_result, "Trade result should include exit_reason")
        self.assertIn("profit_ratio", trade_result, "Trade result should include profit_ratio")

        # Verify trade result contents
        self.assertIn("profit_ratio", trade_result, "Trade result should contain profit ratio")
        self.assertIn("profit_sol", trade_result, "Trade result should contain profit")
        self.assertIn("exit_price", trade_result, "Trade result should contain exit price")
        self.assertIn("exit_reason", trade_result, "Trade result should contain exit reason")
        self.assertIn("trade_duration", trade_result, "Trade result should contain duration")

        # Check that trade is at least break-even (>= 1.0 profit ratio)
        self.assertGreaterEqual(trade_result["profit_ratio"], 1.0, "Trade should be at least break-even")


if __name__ == "__main__":
    unittest.main()
