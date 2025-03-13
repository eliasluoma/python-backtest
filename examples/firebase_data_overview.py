#!/usr/bin/env python
"""
Firebase Data Overview

This script provides a comprehensive overview of the Firebase data structure in tabular format,
showing pool counts, available fields, data ranges, and basic statistics.
"""

import os
import sys
import logging
from collections import Counter, defaultdict
from datetime import datetime
from tabulate import tabulate

# Set up paths
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Import Firebase utilities
from src.data.firebase_service import FirebaseService


def main():
    """Main function to analyze and display Firebase data overview"""
    # Initialize Firebase connection
    logger.info("Initializing Firebase connection...")
    firebase_service = FirebaseService()
    db = firebase_service.db

    if not db:
        logger.error("Failed to connect to Firebase. Exiting.")
        return

    # Get all available pools
    logger.info("Fetching pool information...")
    all_pool_ids = get_pool_ids(db, limit=None)  # Get all pools
    total_pools = len(all_pool_ids)
    logger.info(f"Found a total of {total_pools} pools in the database")

    # Sample pools for detailed analysis
    sample_size = min(20, total_pools)  # Analyze up to 20 pools for performance
    sample_pools = all_pool_ids[:sample_size]

    # Collect field information across pools
    field_stats = analyze_field_distribution(firebase_service, sample_pools)

    # Collect pool statistics
    pool_stats = analyze_pool_statistics(firebase_service, sample_pools)

    # Display results in tables
    display_summary_table(total_pools, sample_size, field_stats, pool_stats)


def analyze_field_distribution(firebase_service, sample_pools):
    """Analyze the distribution of fields across sample pools"""
    field_presence = Counter()
    field_types = defaultdict(Counter)

    # Fetch data for sample pools
    logger.info(f"Analyzing field distribution across {len(sample_pools)} sample pools...")

    for i, pool_id in enumerate(sample_pools):
        logger.info(f"Fetching data for pool {i+1}/{len(sample_pools)}: {pool_id}")

        # Fetch a small sample of data for each pool (just 5 records to check structure)
        df = firebase_service.fetch_market_data(
            min_data_points=5, max_pools=1, limit_per_pool=5, pool_address=pool_id
        ).get(pool_id)

        if df is not None and not df.empty:
            # Count field presence
            for col in df.columns:
                field_presence[col] += 1

                # Determine field types
                dtype = df[col].dtype
                field_types[col][str(dtype)] += 1

    return {"field_presence": field_presence, "field_types": field_types, "total_samples": len(sample_pools)}


def analyze_pool_statistics(firebase_service, sample_pools):
    """Collect statistics about pools and their data"""
    pool_stats = []

    # Analysis parameters for more comprehensive pools
    comprehensive_analysis_pools = sample_pools[:5]  # Only do detailed analysis on 5 pools

    logger.info(f"Collecting statistics for {len(comprehensive_analysis_pools)} pools...")

    for i, pool_id in enumerate(comprehensive_analysis_pools):
        logger.info(f"Detailed analysis for pool {i+1}/{len(comprehensive_analysis_pools)}: {pool_id}")

        # Fetch more data for detailed analysis
        df = firebase_service.fetch_market_data(
            min_data_points=10,
            max_pools=1,
            limit_per_pool=100,  # Get up to 100 records for better statistics
            pool_address=pool_id,
        ).get(pool_id)

        if df is not None and not df.empty:
            # Calculate statistics
            record_count = len(df)

            # Time range
            if "timestamp" in df.columns:
                oldest = df["timestamp"].min()
                newest = df["timestamp"].max()
                time_span = newest - oldest
                time_span_hours = time_span.total_seconds() / 3600
            else:
                oldest = "N/A"
                newest = "N/A"
                time_span_hours = "N/A"

            # Key metrics if available
            market_cap_stats = {
                "min": df["marketCap"].min() if "marketCap" in df.columns else "N/A",
                "max": df["marketCap"].max() if "marketCap" in df.columns else "N/A",
                "mean": df["marketCap"].mean() if "marketCap" in df.columns else "N/A",
            }

            holders_stats = {
                "min": df["holdersCount"].min() if "holdersCount" in df.columns else "N/A",
                "max": df["holdersCount"].max() if "holdersCount" in df.columns else "N/A",
                "mean": df["holdersCount"].mean() if "holdersCount" in df.columns else "N/A",
            }

            pool_stats.append(
                {
                    "pool_id": pool_id[:10] + "...",  # Truncate for display
                    "records": record_count,
                    "oldest_data": oldest,
                    "newest_data": newest,
                    "time_span_hours": time_span_hours,
                    "market_cap_min": market_cap_stats["min"],
                    "market_cap_max": market_cap_stats["max"],
                    "holders_min": holders_stats["min"],
                    "holders_max": holders_stats["max"],
                }
            )

    return pool_stats


def display_summary_table(total_pools, sample_size, field_stats, pool_stats):
    """Display the results in tabular format"""
    # 1. Overall summary
    print("\n" + "=" * 80)
    print("FIREBASE DATA OVERVIEW")
    print("=" * 80)

    summary_table = [
        ["Total pools in database", total_pools],
        ["Pools sampled for analysis", sample_size],
        ["Timestamp of analysis", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
    ]

    print("\nGeneral Summary:")
    print(tabulate(summary_table, tablefmt="grid"))

    # 2. Field distribution
    field_presence = field_stats["field_presence"]
    total_samples = field_stats["total_samples"]

    field_rows = []
    for field, count in field_presence.most_common():
        percentage = (count / total_samples) * 100
        field_type = ", ".join(field_stats["field_types"][field].keys())
        field_rows.append([field, count, f"{percentage:.1f}%", field_type])

    print("\nField Distribution (sorted by frequency):")
    print(tabulate(field_rows, headers=["Field Name", "Presence Count", "Pools %", "Data Type(s)"], tablefmt="grid"))

    # 3. Pool statistics
    if pool_stats:
        print("\nDetailed Pool Statistics (sample):")

        # Format market cap and holders values
        for pool in pool_stats:
            for key in ["market_cap_min", "market_cap_max"]:
                if pool[key] != "N/A":
                    pool[key] = f"{float(pool[key]):.2f}"

            if pool["time_span_hours"] != "N/A":
                pool["time_span_hours"] = f"{float(pool['time_span_hours']):.2f}"

        print(
            tabulate(
                pool_stats,
                headers={
                    "pool_id": "Pool ID",
                    "records": "Records",
                    "oldest_data": "Oldest Data",
                    "newest_data": "Newest Data",
                    "time_span_hours": "Hours Span",
                    "market_cap_min": "Min MarketCap",
                    "market_cap_max": "Max MarketCap",
                    "holders_min": "Min Holders",
                    "holders_max": "Max Holders",
                },
                tablefmt="grid",
            )
        )

    # 4. Special data patterns
    nested_fields = [field for field in field_presence.keys() if "." in field]
    if nested_fields:
        print("\nNested Fields Structure:")
        for field in sorted(nested_fields):
            print(f"- {field}")

    # Summary of trading data availability
    trading_fields = [
        field for field in field_presence.keys() if any(x in field.lower() for x in ["volume", "trade", "buy", "sell"])
    ]
    if trading_fields:
        trading_count = len(trading_fields)
        print(f"\nTrading Data: {trading_count} trading-related fields available")

    # Summary conclusion
    print("\nConclusion:")
    print(f"- Database contains {total_pools} pools with varied data structure")
    print(f"- Most common fields: {', '.join([f[0] for f in field_presence.most_common(5)])}")
    print(
        f"- Average data points per pool in sample: {sum(p['records'] for p in pool_stats)/len(pool_stats) if pool_stats else 'N/A':.1f}"
    )
    print("=" * 80)


if __name__ == "__main__":
    main()
