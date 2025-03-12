import unittest
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.data.data_processor import preprocess_pool_data, filter_pools


class TestDataProcessor(unittest.TestCase):
    """Test cases for the data processor functions."""

    def setUp(self):
        """Set up test fixtures."""
        # Create sample timestamps for test data
        self.sample_timestamps = [datetime.now() + timedelta(minutes=i) for i in range(20)]

        # Create sample market data
        self.sample_data = {
            "timestamp": [ts.strftime("%Y-%m-%dT%H:%M:%S") for ts in self.sample_timestamps],
            "marketCap": [100000 + i * 10000 for i in range(20)],  # Increasing market cap
            "holders": [10 + i for i in range(20)],  # Increasing holders
            "price": [1.0 + i * 0.1 for i in range(20)],  # Increasing price
            # Add other raw metrics
            "someMetric": [5] * 20,
        }

        # Create a DataFrame with the sample data
        self.sample_df = pd.DataFrame(self.sample_data)

        # Create a dict of pool data (as would come from Firebase)
        self.sample_pools = {
            "pool1": pd.DataFrame(
                {
                    "timestamp": [ts.strftime("%Y-%m-%dT%H:%M:%S") for ts in self.sample_timestamps],
                    "marketCap": [100000 + i * 10000 for i in range(20)],
                    "holders": [10 + i for i in range(20)],
                    "pool_address": ["pool1"] * 20,  # Add pool address
                }
            ),
            "pool2": pd.DataFrame(
                {
                    "timestamp": [
                        ts.strftime("%Y-%m-%dT%H:%M:%S") for ts in self.sample_timestamps[:5]
                    ],  # Short pool (5 records)
                    "marketCap": [200000 + i * 10000 for i in range(5)],
                    "holders": [20 + i for i in range(5)],
                    "pool_address": ["pool2"] * 5,
                }
            ),
            "pool3": pd.DataFrame(
                {
                    "timestamp": [ts.strftime("%Y-%m-%dT%H:%M:%S") for ts in self.sample_timestamps],
                    "marketCap": [300000 + i * 10000 for i in range(20)],
                    "holders": [30 + i for i in range(20)],
                    "pool_address": ["pool3"] * 20,
                }
            ),
        }

    def test_preprocess_pool_data(self):
        """Test preprocessing of pool data."""
        # Preprocess the sample data
        processed_df = preprocess_pool_data(self.sample_df)

        # Check that processed data has the expected columns
        expected_columns = [
            "timestamp",
            "marketCap",
            "holders",
            "price",
            "someMetric",
            # Derived columns
            "marketCapChange5s",
            "holderDelta5s",
            "holderDelta30s",
            "holderDelta60s",
            "mcGrowthFromStart",
            "holderGrowthFromStart",
        ]

        for col in expected_columns:
            self.assertIn(col, processed_df.columns)

        # Check that the derived metrics are calculated correctly
        # Market cap change should be percentage change
        self.assertEqual(len(processed_df), len(self.sample_df))

        # Check specific metric calculations
        # Market cap percent changes should be calculated
        self.assertTrue(all(processed_df["marketCapChange5s"].iloc[1:] > 0))  # Should be positive for increasing MC

        # Holder deltas should be absolute differences
        self.assertEqual(processed_df["holderDelta5s"].iloc[5], 1)  # Each step adds 1 holder

        # Growth from start should show cumulative growth
        self.assertGreater(processed_df["mcGrowthFromStart"].iloc[-1], 0)  # Should be positive
        self.assertGreater(processed_df["holderGrowthFromStart"].iloc[-1], 0)  # Should be positive

    def test_filter_pools(self):
        """Test filtering pools based on data points."""
        # Filter pools with at least 10 data points
        filtered_pools = filter_pools(self.sample_pools, min_data_points=10)

        # Should include pool1 and pool3, but not pool2 (which has only 5 data points)
        self.assertIn("pool1", filtered_pools)
        self.assertIn("pool3", filtered_pools)
        self.assertNotIn("pool2", filtered_pools)

        # Check that pool data is preserved
        self.assertEqual(len(filtered_pools["pool1"]), 20)
        self.assertEqual(len(filtered_pools["pool3"]), 20)

        # Filter with higher threshold
        filtered_pools_strict = filter_pools(self.sample_pools, min_data_points=15)
        self.assertIn("pool1", filtered_pools_strict)
        self.assertIn("pool3", filtered_pools_strict)
        self.assertNotIn("pool2", filtered_pools_strict)

        # Filter with threshold that excludes all pools
        filtered_pools_none = filter_pools(self.sample_pools, min_data_points=25)
        self.assertEqual(len(filtered_pools_none), 0)

    def test_preprocess_with_missing_data(self):
        """Test preprocessing with missing data."""
        # Create data with some missing values
        data_with_missing = self.sample_data.copy()
        data_with_missing["holders"][5:10] = [None] * 5  # Set some holder values to None
        df_with_missing = pd.DataFrame(data_with_missing)

        # Preprocess the data with missing values
        processed_df = preprocess_pool_data(df_with_missing)

        # Check that processing completes without errors
        self.assertEqual(len(processed_df), len(df_with_missing))

        # Missing values in holders should result in NaN values in derived metrics
        # that depend on holders
        self.assertTrue(pd.isna(processed_df["holderDelta5s"].iloc[5]))

        # But other metrics should still be calculated
        self.assertFalse(pd.isna(processed_df["marketCapChange5s"].iloc[5]))


if __name__ == "__main__":
    unittest.main()
