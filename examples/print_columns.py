#!/usr/bin/env python
"""
Print all column names available in a pool's dataset
"""

import os
import sys
import logging

# Set up paths
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Import Firebase utilities
from src.data.firebase_service import FirebaseService


def main():
    # Initialize Firebase
    logger.info("Initializing Firebase connection...")
    firebase_service = FirebaseService()

    # Use a pool with sufficient data
    pool_id = "13UufXw2zaq4ffE16dcYhsCc6ZdxXXV5EwNq4mRJuT8F"

    # Fetch data for this pool
    logger.info(f"Fetching sample data for pool: {pool_id}")
    pool_data = firebase_service.fetch_market_data(
        min_data_points=5, max_pools=1, limit_per_pool=5, pool_address=pool_id
    ).get(pool_id)

    if pool_data is not None and not pool_data.empty:
        # Print all column names
        print("\n" + "=" * 80)
        print(f"ALL COLUMNS IN FIREBASE POOL DATA ({len(pool_data.columns)} total)")
        print("=" * 80 + "\n")

        # Group related columns
        grouped_columns = {}

        for col in sorted(pool_data.columns):
            # Create category based on prefix
            category = "Other"

            if col.startswith("trade_"):
                category = "Trade Data"
            elif "marketCap" in col:
                category = "Market Cap"
            elif "holder" in col.lower() or "holderDelta" in col:
                category = "Holders"
            elif "volume" in col.lower() or "buy" in col.lower():
                category = "Volume/Buys"
            elif "price" in col.lower():
                category = "Price"
            elif col in ["timestamp", "timeFromStart", "doc_id", "poolAddress"]:
                category = "Metadata"

            if category not in grouped_columns:
                grouped_columns[category] = []

            grouped_columns[category].append(col)

        # Print by category
        for category in sorted(grouped_columns.keys()):
            print(f"\n{category} ({len(grouped_columns[category])} columns):")
            print("-" * 40)
            for col in sorted(grouped_columns[category]):
                data_type = str(pool_data[col].dtype)
                example = str(pool_data[col].iloc[0])[:50] if not pool_data[col].isnull().all() else "NULL"
                print(f"- {col} ({data_type}): {example}")
    else:
        logger.error("Failed to fetch pool data")


if __name__ == "__main__":
    main()
