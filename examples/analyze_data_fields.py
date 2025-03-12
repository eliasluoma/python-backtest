#!/usr/bin/env python
"""
Analyze Data Fields

This script analyzes the data fields available in our Firebase pools,
categorizes them, and provides detailed information about each field
including data types, distributions, and potential uses for trading algorithms.
"""

import os
import sys
import logging
import pandas as pd
import numpy as np
import json
from tabulate import tabulate
import matplotlib.pyplot as plt
from collections import defaultdict

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
    """Analyze data fields in Firebase pools"""
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

    # Load filtered pools if available
    filtered_pools_path = os.path.join(output_dir, "filtered_pools.json")
    if os.path.exists(filtered_pools_path):
        with open(filtered_pools_path, "r") as f:
            filtered_pools = json.load(f)
            pool_ids = [p["pool_id"] for p in filtered_pools]
            logger.info(f"Loaded {len(pool_ids)} filtered pools from {filtered_pools_path}")
    else:
        # Get pool IDs with sufficient data
        logger.info("No filtered pools file found. Fetching pool information...")
        all_pool_ids = get_pool_ids(db, limit=100)  # Limit to 100 for analysis
        pool_ids = all_pool_ids
        logger.info(f"Using {len(pool_ids)} pools for analysis")

    # Analyze a sample of pools to identify all fields
    sample_size = min(10, len(pool_ids))
    sample_pool_ids = pool_ids[:sample_size]
    logger.info(f"Analyzing {sample_size} sample pools to identify all fields...")

    # Collect field information
    field_info = analyze_field_information(firebase_service, sample_pool_ids)

    # Categorize fields
    categorized_fields = categorize_fields(field_info)

    # Generate field statistics
    field_stats = generate_field_statistics(firebase_service, sample_pool_ids, field_info)

    # Generate report
    output_file = os.path.join(output_dir, "data_fields_analysis.md")
    generate_report(field_info, categorized_fields, field_stats, output_file)

    # Display summary
    display_summary(field_info, categorized_fields)


def analyze_field_information(firebase_service, pool_ids):
    """Analyze field information from sample pools"""
    all_fields = set()
    field_counts = defaultdict(int)
    field_types = defaultdict(set)
    field_examples = defaultdict(list)

    for i, pool_id in enumerate(pool_ids):
        logger.info(f"Analyzing fields for pool {i+1}/{len(pool_ids)}: {pool_id}")

        # Fetch data for this pool with a high limit to get a good sample
        pool_data = firebase_service.fetch_market_data(
            min_data_points=10, max_pools=1, limit_per_pool=50, pool_address=pool_id
        ).get(pool_id)

        if pool_data is not None and not pool_data.empty:
            # Get all columns for this pool
            pool_fields = set(pool_data.columns)
            all_fields.update(pool_fields)

            # Update field counts
            for field in pool_fields:
                field_counts[field] += 1
                field_types[field].add(str(pool_data[field].dtype))

                # Get example values (non-null)
                if not pool_data[field].isnull().all():
                    sample_value = pool_data[field].dropna().iloc[0] if not pool_data[field].empty else None
                    if sample_value is not None:
                        field_examples[field].append(str(sample_value)[:50])  # Limit length

    # Compile field information
    field_info = []
    for field in sorted(all_fields):
        field_info.append(
            {
                "name": field,
                "count": field_counts[field],
                "presence": field_counts[field] / len(pool_ids) * 100,
                "types": sorted(field_types[field]),
                "examples": field_examples[field][:3],  # Take up to 3 examples
            }
        )

    # Sort by presence (descending)
    field_info.sort(key=lambda x: x["presence"], reverse=True)

    logger.info(f"Identified {len(field_info)} unique fields across {len(pool_ids)} pools")
    return field_info


def categorize_fields(field_info):
    """Categorize fields into logical groups"""
    categories = {
        "market_cap": ["marketCap", "athMarketCap", "minMarketCap", "maMarketCap", "marketCapChange"],
        "price": ["price", "currentPrice", "priceChange"],
        "volume": ["volume", "buyVolume", "netVolume"],
        "holders": ["holdersCount", "holderDelta", "holdersGrowth", "initialHoldersCount"],
        "trades": ["trade", "tradeCount", "bigBuy", "largeBuy", "superBuy"],
        "time": ["timestamp", "timeFromStart"],
        "metadata": ["doc_id", "poolAddress"],
    }

    categorized = {category: [] for category in categories.keys()}
    uncategorized = []

    for field in field_info:
        field_name = field["name"]
        assigned = False

        # Check if the field belongs to any category
        for category, keywords in categories.items():
            if any(keyword.lower() in field_name.lower() for keyword in keywords):
                categorized[category].append(field)
                assigned = True
                break

        if not assigned:
            uncategorized.append(field)

    # Add uncategorized as a category
    if uncategorized:
        categorized["other"] = uncategorized

    return categorized


def generate_field_statistics(firebase_service, pool_ids, field_info):
    """Generate statistics for each field"""
    # Get the top 50 most common fields
    top_fields = [field["name"] for field in field_info[:50]]

    field_stats = defaultdict(dict)

    # Sample one pool for detailed analysis
    if pool_ids:
        sample_pool_id = pool_ids[0]
        logger.info(f"Generating statistics for top fields using pool: {sample_pool_id}")

        # Fetch data for this pool
        pool_data = firebase_service.fetch_market_data(
            min_data_points=50, max_pools=1, limit_per_pool=500, pool_address=sample_pool_id
        ).get(sample_pool_id)

        if pool_data is not None and not pool_data.empty:
            for field in top_fields:
                if field in pool_data.columns:
                    # Skip non-numeric fields for statistics
                    if pd.api.types.is_numeric_dtype(pool_data[field]):
                        field_stats[field] = {
                            "mean": pool_data[field].mean(),
                            "median": pool_data[field].median(),
                            "min": pool_data[field].min(),
                            "max": pool_data[field].max(),
                            "std": pool_data[field].std(),
                            "null_percent": pool_data[field].isnull().mean() * 100,
                        }

    return field_stats


def generate_report(field_info, categorized_fields, field_stats, output_file):
    """Generate a detailed report of field analysis"""
    with open(output_file, "w") as f:
        f.write("# Data Fields Analysis\n\n")

        # Overview
        f.write("## Overview\n\n")
        f.write(f"Total unique fields identified: {len(field_info)}\n\n")

        # Field categories
        f.write("## Field Categories\n\n")
        for category, fields in categorized_fields.items():
            f.write(f"### {category.replace('_', ' ').title()} Fields ({len(fields)})\n\n")

            # Create a table for each category
            rows = []
            for field in fields:
                example = field["examples"][0] if field["examples"] else "N/A"
                rows.append([field["name"], f"{field['presence']:.1f}%", ", ".join(field["types"]), example])

            headers = ["Field Name", "Presence", "Data Types", "Example Value"]
            f.write(tabulate(rows, headers=headers, tablefmt="pipe") + "\n\n")

        # Detailed field statistics
        f.write("## Field Statistics\n\n")
        f.write("Statistics for numeric fields (from sample pool):\n\n")

        # Create table for field statistics
        rows = []
        for field, stats in field_stats.items():
            if stats:  # Skip fields without statistics
                rows.append(
                    [
                        field,
                        f"{stats.get('mean', 'N/A'):.2f}",
                        f"{stats.get('median', 'N/A'):.2f}",
                        f"{stats.get('min', 'N/A'):.2f}",
                        f"{stats.get('max', 'N/A'):.2f}",
                        f"{stats.get('std', 'N/A'):.2f}",
                        f"{stats.get('null_percent', 'N/A'):.1f}%",
                    ]
                )

        headers = ["Field", "Mean", "Median", "Min", "Max", "Std Dev", "Null %"]
        f.write(tabulate(rows, headers=headers, tablefmt="pipe") + "\n\n")

        # Potential uses for trading algorithms
        f.write("## Potential Uses for Trading Algorithms\n\n")

        f.write("### Market Cap Features\n\n")
        f.write("- `marketCap`: Current market capitalization, useful for size-based filtering\n")
        f.write("- `athMarketCap`: All-time high market cap, can indicate maximum potential\n")
        f.write("- `marketCapChange*`: Short-term market cap changes, potential for momentum signals\n")
        f.write("- `maMarketCap*`: Moving averages of market cap, useful for trend identification\n\n")

        f.write("### Volume Features\n\n")
        f.write("- `buyVolume*`: Buy volume over different time windows, indicates buying pressure\n")
        f.write("- `netVolume*`: Net volume (buys minus sells), can indicate direction\n")
        f.write("- `bigBuy*`, `largeBuy*`, `superBuy*`: Large transaction indicators, potential for whale tracking\n\n")

        f.write("### Holder Features\n\n")
        f.write("- `holdersCount`: Current number of holders, indicates distribution\n")
        f.write("- `holderDelta*`: Change in holders over time windows, indicates new investor interest\n")
        f.write("- `holdersGrowthFromStart`: Growth in holders since tracking began\n\n")

        f.write("### Trade Features\n\n")
        f.write("- Trade count fields: Indicate activity levels\n")
        f.write("- Buy vs sell trade counts: May indicate market sentiment\n\n")

        logger.info(f"Report generated and saved to {output_file}")


def display_summary(field_info, categorized_fields):
    """Display summary of field analysis"""
    print("\n" + "=" * 80)
    print("DATA FIELDS ANALYSIS SUMMARY")
    print("=" * 80)

    # Overview
    print(f"\nTotal unique fields: {len(field_info)}")

    # Field categories
    print("\nFields by Category:")
    for category, fields in categorized_fields.items():
        print(f"  - {category.replace('_', ' ').title()}: {len(fields)} fields")

    # Top 20 most common fields
    print("\nTop 20 Most Common Fields:")
    for i, field in enumerate(field_info[:20]):
        print(f"  {i+1}. {field['name']} ({field['presence']:.1f}%)")

    # Field type distribution
    type_counter = defaultdict(int)
    for field in field_info:
        for type_name in field["types"]:
            type_counter[type_name] += 1

    print("\nData Type Distribution:")
    for type_name, count in sorted(type_counter.items(), key=lambda x: x[1], reverse=True):
        print(f"  - {type_name}: {count} fields")

    print("\nDetailed Analysis:")
    print("  - Full report: outputs/data_fields_analysis.md")
    print("=" * 80)


if __name__ == "__main__":
    main()
