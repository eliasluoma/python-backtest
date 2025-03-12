#!/usr/bin/env python
"""
Analyze Available Fields for Trading Strategies

This script identifies which fields are actually available in the database,
checks against our required fields, and suggests solutions for handling missing fields.
"""

import os
import sys
import logging
import json
from collections import Counter

# Set up paths
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Import Firebase utilities
from src.data.firebase_service import FirebaseService
from src.utils.firebase_utils import get_pool_ids

# Required fields from the previous script
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
    "lastPrice",  # Missing in all pools
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
    "totalVolume",  # Missing in all pools
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

# Fields we could potentially calculate from existing data
DERIVABLE_FIELDS = {
    "lastPrice": "Can be calculated by looking at previous row's currentPrice",
    "totalVolume": "Can be calculated by summing buyVolume and sellVolume, or tracking cumulative volume",
}


def analyze_pool_fields(pool_data, pool_id):
    """Analyze fields for a pool and check against required fields"""
    pool_fields = set(pool_data.columns)
    missing_fields = set(REQUIRED_FIELDS) - pool_fields
    extra_fields = pool_fields - set(REQUIRED_FIELDS)

    return {
        "pool_id": pool_id,
        "total_fields": len(pool_fields),
        "required_fields_present": len(REQUIRED_FIELDS) - len(missing_fields),
        "required_fields_missing": len(missing_fields),
        "missing_fields": list(missing_fields),
        "extra_fields": list(extra_fields),
    }


def main():
    """Analyze available fields and suggest solutions for missing fields"""
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

    # Use filtered pools if available
    filtered_pools_path = os.path.join(output_dir, "filtered_pools.json")
    if os.path.exists(filtered_pools_path):
        with open(filtered_pools_path, "r") as f:
            filtered_pools = json.load(f)
            pool_ids = [p["pool_id"] for p in filtered_pools]
            logger.info("Loaded {} filtered pools with sufficient data".format(len(pool_ids)))
    else:
        # If no filtered pools file, get 300 pools for comprehensive analysis
        max_pools = 300  # Increased from 20 to 300 for better coverage
        pool_ids = get_pool_ids(db, limit=max_pools)
        logger.info("Using {} pools for analysis".format(len(pool_ids)))

    # Analyze fields across pools
    logger.info("Analyzing fields across {} pools...".format(len(pool_ids)))

    all_fields = set()
    pool_analyses = []
    field_presence = Counter()  # Count how many pools have each field

    for i, pool_id in enumerate(pool_ids):
        logger.info("Analyzing pool {}/{}: {}".format(i + 1, len(pool_ids), pool_id))

        # Fetch data for this pool
        pool_data = firebase_service.fetch_market_data(
            min_data_points=5, max_pools=1, limit_per_pool=20, pool_address=pool_id
        ).get(pool_id)

        if pool_data is not None and not pool_data.empty:
            # Analyze fields for this pool
            pool_analysis = analyze_pool_fields(pool_data, pool_id)
            pool_analyses.append(pool_analysis)

            # Update counters
            all_fields.update(pool_data.columns)
            for field in pool_data.columns:
                field_presence[field] += 1

    # Calculate field statistics
    total_unique_fields = len(all_fields)

    # Group required fields by availability
    always_available = []
    mostly_available = []
    rarely_available = []
    never_available = []

    for field in REQUIRED_FIELDS:
        presence_pct = field_presence.get(field, 0) / len(pool_ids) * 100

        if presence_pct == 100:
            always_available.append((field, presence_pct))
        elif presence_pct >= 70:
            mostly_available.append((field, presence_pct))
        elif presence_pct > 0:
            rarely_available.append((field, presence_pct))
        else:
            never_available.append((field, presence_pct))

    # Save detailed results
    field_analysis_file = os.path.join(output_dir, "field_analysis.json")
    with open(field_analysis_file, "w") as f:
        json.dump(
            {
                "pool_analyses": pool_analyses,
                "field_presence": {field: count for field, count in field_presence.items()},
                "required_fields_availability": {
                    "always_available": [field for field, _ in always_available],
                    "mostly_available": [field for field, _ in mostly_available],
                    "rarely_available": [field for field, _ in rarely_available],
                    "never_available": [field for field, _ in never_available],
                },
            },
            f,
            indent=2,
        )

    # Print summary
    print("\n" + "=" * 80)
    print("FIELD AVAILABILITY ANALYSIS")
    print("=" * 80)

    print("\nPools analyzed: {}".format(len(pool_ids)))
    print("Total unique fields found: {}".format(total_unique_fields))
    print("Required fields: {}".format(len(REQUIRED_FIELDS)))

    print("\nREQUIRED FIELDS AVAILABILITY:")
    print("-" * 40)

    print("\nAlways Available ({} fields):".format(len(always_available)))
    for field, pct in sorted(always_available):
        print("- {} (100%)".format(field))

    print("\nMostly Available ({} fields):".format(len(mostly_available)))
    for field, pct in sorted(mostly_available, key=lambda x: x[1], reverse=True):
        print("- {} ({:.1f}%)".format(field, pct))

    print("\nRarely Available ({} fields):".format(len(rarely_available)))
    for field, pct in sorted(rarely_available, key=lambda x: x[1], reverse=True):
        print("- {} ({:.1f}%)".format(field, pct))

    print("\nNever Available ({} fields):".format(len(never_available)))
    for field, _ in sorted(never_available):
        print("- {}".format(field))

    # Suggest solutions for missing fields
    print("\nSTRATEGY RECOMMENDATIONS:")
    print("-" * 40)

    print("\n1. For always and mostly available fields:")
    print("   - Use these fields directly in your trading strategies")
    print("   - Filter pools that have these fields available")

    print("\n2. For derivable but missing fields:")
    for field, solution in DERIVABLE_FIELDS.items():
        print("   - {}: {}".format(field, solution))

    print("\n3. For other missing fields:")
    print("   - Create fallback alternatives using available data")
    print("   - Implement conditional logic in strategies to work with or without these fields")
    print("   - Consider if these fields are truly essential or can be excluded")

    print("\nBest approach:")
    print(
        "1. Focus on the {} fields that are available in most pools".format(
            len(always_available) + len(mostly_available)
        )
    )
    print("2. Derive the 2 consistently missing fields (lastPrice, totalVolume)")
    print("3. Create fallback strategies for pools missing the rare fields")

    print("\nResults saved to: {}".format(field_analysis_file))
    print("=" * 80)


if __name__ == "__main__":
    main()
