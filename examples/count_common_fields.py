#!/usr/bin/env python
"""
Count Common Fields Across Pools

This script analyzes the data fields available across multiple pools
to determine how many fields are truly common and count them accurately.
"""

import os
import sys
import logging
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


def main():
    """Analyze fields across multiple pools to count common ones"""
    # Initialize Firebase
    logger.info("Initializing Firebase connection...")
    firebase_service = FirebaseService()
    db = firebase_service.db

    if not db:
        logger.error("Failed to connect to Firebase. Exiting.")
        return

    # Get a good sample of pools
    logger.info("Fetching pool IDs...")
    sample_size = 10  # Analyze 10 pools for a faster sample
    pool_ids = get_pool_ids(db, limit=sample_size)

    if not pool_ids:
        logger.error("No pool IDs found. Exiting.")
        return

    logger.info(f"Analyzing field distribution across {len(pool_ids)} pools...")

    # Count fields across pools
    field_counts = Counter()
    all_fields = set()
    pool_field_counts = []

    for i, pool_id in enumerate(pool_ids):
        logger.info(f"Analyzing pool {i+1}/{len(pool_ids)}: {pool_id}")

        # Fetch data for this pool with fewer data points for speed
        pool_data = firebase_service.fetch_market_data(
            min_data_points=5, max_pools=1, limit_per_pool=10, pool_address=pool_id
        ).get(pool_id)

        if pool_data is not None and not pool_data.empty:
            # Get all columns for this pool
            pool_fields = set(pool_data.columns)
            all_fields.update(pool_fields)
            pool_field_counts.append(len(pool_fields))

            # Update field counts
            for field in pool_fields:
                field_counts[field] += 1

    # Analyze common fields at different thresholds
    total_fields = len(all_fields)
    logger.info(f"Found {total_fields} unique fields across all pools")

    # Calculate how many fields are present in what percentage of pools
    presence_thresholds = [100, 95, 90, 80, 70, 50, 25]

    print("\n" + "=" * 80)
    print(f"FIELD DISTRIBUTION ANALYSIS ({len(pool_ids)} POOLS)")
    print("=" * 80)

    print(f"\nTotal unique fields found: {total_fields}")
    print(f"Average fields per pool: {sum(pool_field_counts) / len(pool_field_counts):.1f}")
    print(f"Min fields in a pool: {min(pool_field_counts)}")
    print(f"Max fields in a pool: {max(pool_field_counts)}")

    print("\nCommon fields by presence threshold:")
    print("-" * 40)

    for threshold_pct in presence_thresholds:
        threshold = len(pool_ids) * threshold_pct / 100
        fields_above_threshold = [field for field, count in field_counts.items() if count >= threshold]
        print(f"Fields in at least {threshold_pct}% of pools: {len(fields_above_threshold)}")

    # Print the most common fields (top 50)
    most_common_fields = field_counts.most_common(50)

    print("\nTop 50 most common fields:")
    print("-" * 40)
    for i, (field, count) in enumerate(most_common_fields, 1):
        presence_pct = count / len(pool_ids) * 100
        print(f"{i}. {field} (present in {presence_pct:.1f}% of pools)")

    # Most likely these are the fields the user considers "common"
    # The 90% threshold is a reasonable definition of "common"
    common_threshold = 0.9 * len(pool_ids)
    common_fields = [field for field, count in field_counts.items() if count >= common_threshold]

    print(f"\nFields present in at least 90% of pools ({len(common_fields)} fields):")
    print("-" * 40)
    for field in sorted(common_fields):
        presence_pct = field_counts[field] / len(pool_ids) * 100
        print(f"- {field} ({presence_pct:.1f}%)")


if __name__ == "__main__":
    main()
