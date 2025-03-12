#!/usr/bin/env python
"""
Filter Pools with Required Fields

This script identifies pools that have all 61 required fields for trading strategies
and filters out pools with broken/incomplete data.
"""

import os
import sys
import logging
import json

# Set up paths
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Import Firebase utilities
from src.data.firebase_service import FirebaseService
from src.utils.firebase_utils import get_pool_ids


# Define the 63 required fields that our trading strategies use
REQUIRED_FIELDS = [
    # Market Cap Fields
    "marketCap",
    "athMarketCap",
    "minMarketCap",
    "marketCapChange5s",
    "marketCapChange10s",
    "marketCapChange30s",
    "marketCapChange60s",
    "maMarketCap10s",
    "maMarketCap30s",
    "maMarketCap60s",
    # Price Fields
    "currentPrice",
    "lastPrice",
    "priceChangePercent",
    "priceChangeFromStart",
    # Holder Fields
    "holdersCount",
    "initialHoldersCount",
    "holdersGrowthFromStart",
    "holderDelta5s",
    "holderDelta10s",
    "holderDelta30s",
    "holderDelta60s",
    # Volume Fields
    "buyVolume5s",
    "buyVolume10s",
    "netVolume5s",
    "netVolume10s",
    "totalVolume",
    # Buy Classification Fields
    "largeBuy5s",
    "largeBuy10s",
    "bigBuy5s",
    "bigBuy10s",
    "superBuy5s",
    "superBuy10s",
    # Trade Data - 5s
    "trade_last5Seconds.volume.buy",
    "trade_last5Seconds.volume.sell",
    "trade_last5Seconds.volume.bot",
    "trade_last5Seconds.tradeCount.buy.small",
    "trade_last5Seconds.tradeCount.buy.medium",
    "trade_last5Seconds.tradeCount.buy.large",
    "trade_last5Seconds.tradeCount.buy.big",
    "trade_last5Seconds.tradeCount.buy.super",
    "trade_last5Seconds.tradeCount.sell.small",
    "trade_last5Seconds.tradeCount.sell.medium",
    "trade_last5Seconds.tradeCount.sell.large",
    "trade_last5Seconds.tradeCount.sell.big",
    "trade_last5Seconds.tradeCount.sell.super",
    "trade_last5Seconds.tradeCount.bot",
    # Trade Data - 10s
    "trade_last10Seconds.volume.buy",
    "trade_last10Seconds.volume.sell",
    "trade_last10Seconds.volume.bot",
    "trade_last10Seconds.tradeCount.buy.small",
    "trade_last10Seconds.tradeCount.buy.medium",
    "trade_last10Seconds.tradeCount.buy.large",
    "trade_last10Seconds.tradeCount.buy.big",
    "trade_last10Seconds.tradeCount.buy.super",
    "trade_last10Seconds.tradeCount.sell.small",
    "trade_last10Seconds.tradeCount.sell.medium",
    "trade_last10Seconds.tradeCount.sell.large",
    "trade_last10Seconds.tradeCount.sell.big",
    "trade_last10Seconds.tradeCount.sell.super",
    "trade_last10Seconds.tradeCount.bot",
    # Metadata
    "poolAddress",
    "timeFromStart",
    "creationTime",
]


def main():
    """Filter pools that have all required fields for trading strategies"""
    # Initialize Firebase
    logger.info("Initializing Firebase connection...")
    firebase_service = FirebaseService()
    db = firebase_service.db

    if not db:
        logger.error("Failed to connect to Firebase. Exiting.")
        return

    # Create output directory if it doesn't exist
    output_dir = os.path.join(project_root, "outputs")
    os.makedirs(output_dir, exist_ok=True)

    # Get pool IDs to analyze
    logger.info("Fetching pool IDs...")

    # Use filtered pools if available (pools with sufficient data)
    filtered_pools_path = os.path.join(output_dir, "filtered_pools.json")
    if os.path.exists(filtered_pools_path):
        with open(filtered_pools_path, "r") as f:
            filtered_pools = json.load(f)
            pool_ids = [p["pool_id"] for p in filtered_pools]
            logger.info(f"Loaded {len(pool_ids)} filtered pools with sufficient data")
    else:
        # If no filtered pools file, get the first 100 pools
        max_pools = 100  # Limit to 100 pools for analysis
        pool_ids = get_pool_ids(db, limit=max_pools)
        logger.info(f"Using {len(pool_ids)} pools for analysis")

    # Check pools for required fields
    valid_pools = []
    invalid_pools = []

    logger.info(f"Checking {len(pool_ids)} pools for {len(REQUIRED_FIELDS)} required fields...")

    for i, pool_id in enumerate(pool_ids):
        logger.info(f"Checking pool {i+1}/{len(pool_ids)}: {pool_id}")

        # Fetch data for this pool
        pool_data = firebase_service.fetch_market_data(
            min_data_points=10, max_pools=1, limit_per_pool=20, pool_address=pool_id
        ).get(pool_id)

        if pool_data is not None and not pool_data.empty:
            # Check if all required fields are present
            pool_fields = set(pool_data.columns)
            missing_fields = set(REQUIRED_FIELDS) - pool_fields

            if not missing_fields:
                # Pool has all required fields
                valid_pools.append(
                    {"pool_id": pool_id, "record_count": len(pool_data), "field_count": len(pool_fields)}
                )
                logger.info(f"✓ Pool {pool_id} has all required fields")
            else:
                # Pool is missing some required fields
                invalid_pools.append(
                    {
                        "pool_id": pool_id,
                        "missing_field_count": len(missing_fields),
                        "missing_fields": list(missing_fields),
                    }
                )
                logger.info(f"✗ Pool {pool_id} is missing {len(missing_fields)} fields")
        else:
            # Failed to fetch data for this pool
            invalid_pools.append({"pool_id": pool_id, "error": "Failed to fetch data"})
            logger.info(f"✗ Pool {pool_id} - Failed to fetch data")

    # Save results
    valid_pools_file = os.path.join(output_dir, "trading_valid_pools.json")
    with open(valid_pools_file, "w") as f:
        json.dump(valid_pools, f, indent=2)

    invalid_pools_file = os.path.join(output_dir, "trading_invalid_pools.json")
    with open(invalid_pools_file, "w") as f:
        json.dump(invalid_pools, f, indent=2)

    # Print summary
    print("\n" + "=" * 80)
    print("POOL FILTERING RESULTS")
    print("=" * 80)

    print("\nRequired Fields: {}".format(len(REQUIRED_FIELDS)))
    print("Pools Analyzed: {}".format(len(pool_ids)))
    print("Valid Pools: {} ({:.1f}%)".format(len(valid_pools), len(valid_pools) / len(pool_ids) * 100))
    print("Invalid Pools: {} ({:.1f}%)".format(len(invalid_pools), len(invalid_pools) / len(pool_ids) * 100))

    print("\nTop missing fields:")
    missing_field_counts = {}
    for pool in invalid_pools:
        if "missing_fields" in pool:
            for field in pool["missing_fields"]:
                missing_field_counts[field] = missing_field_counts.get(field, 0) + 1

    if missing_field_counts:
        sorted_missing = sorted(missing_field_counts.items(), key=lambda x: x[1], reverse=True)
        for field, count in sorted_missing[:10]:  # Show top 10 missing fields
            print(
                "- {}: missing in {} pools ({:.1f}% of invalid pools)".format(
                    field, count, count / len(invalid_pools) * 100
                )
            )

    print("\nResults saved to:")
    print("- Valid pools: {}".format(valid_pools_file))
    print("- Invalid pools: {}".format(invalid_pools_file))
    print("=" * 80)


if __name__ == "__main__":
    main()
