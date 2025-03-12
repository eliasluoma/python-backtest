#!/usr/bin/env python
"""
Filter Consistent Pools

This script identifies and filters pools with at least 10 minutes of data and
consistent data structure for use in analytics and simulations.
"""

import os
import sys
import logging
import numpy as np
import matplotlib.pyplot as plt
import json
from datetime import datetime
from tabulate import tabulate
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
    """Identify and filter pools with consistent data over at least 10 minutes"""
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

    # Get all pool IDs but limit to first 100 for quicker analysis
    logger.info("Fetching pool information...")
    all_pool_ids = get_pool_ids(db, limit=None)

    # MODIFICATION: Limit to first 100 pools for analysis
    max_pools_to_analyze = 100
    all_pool_ids = all_pool_ids[:max_pools_to_analyze]

    total_pools = len(all_pool_ids)
    logger.info(f"Found {total_pools} pools for analysis")

    # Analyze pool timespan and data consistency
    # First, get a small sample to identify common fields
    sample_size_for_fields = min(20, total_pools)
    sample_pools_for_fields = all_pool_ids[:sample_size_for_fields]

    # Get common fields across samples
    common_fields = identify_common_fields(firebase_service, sample_pools_for_fields)

    # Now analyze all pools for time span
    logger.info("Analyzing all pools for time span and data consistency...")

    # Analyze in chunks to avoid memory issues
    chunk_size = 20
    consistent_pools = []
    time_spans = []
    data_points = []

    for i in range(0, total_pools, chunk_size):
        chunk_end = min(i + chunk_size, total_pools)
        logger.info(f"Analyzing pools {i+1} to {chunk_end} out of {total_pools}...")

        pool_chunk = all_pool_ids[i:chunk_end]
        chunk_results = analyze_pools(firebase_service, pool_chunk, common_fields)

        consistent_pools.extend(chunk_results["consistent_pools"])
        time_spans.extend(chunk_results["time_spans"])
        data_points.extend(chunk_results["data_points"])

    # Generate summary statistics
    logger.info("Generating summary statistics...")

    # FIX: Handle None values properly by filtering them out first
    valid_time_spans = [ts for ts in time_spans if ts is not None]
    valid_data_points = [dp for dp in data_points if dp is not None]

    # Calculate pools with 10+ minutes of data (avoiding None comparison errors)
    pools_with_10min_data = sum(
        1 for ts, dp in zip(time_spans, data_points) if ts is not None and dp is not None and ts >= 10.0 and dp >= 10
    )

    summary_stats = {
        "total_pools": total_pools,
        "pools_with_10min_data": pools_with_10min_data,
        "pools_with_consistent_data": len(consistent_pools),
        "common_fields": common_fields,
    }

    # Add time span stats if we have valid data
    if valid_time_spans:
        summary_stats["time_span_stats"] = {
            "mean": np.mean(valid_time_spans),
            "median": np.median(valid_time_spans),
            "max": max(valid_time_spans, default=0),
            "min": min(valid_time_spans, default=0),
        }
    else:
        summary_stats["time_span_stats"] = {
            "mean": 0,
            "median": 0,
            "max": 0,
            "min": 0,
        }

    # Add data points stats if we have valid data
    if valid_data_points:
        summary_stats["data_points_stats"] = {
            "mean": np.mean(valid_data_points),
            "median": np.median(valid_data_points),
            "max": max(valid_data_points, default=0),
            "min": min(valid_data_points, default=0),
        }
    else:
        summary_stats["data_points_stats"] = {
            "mean": 0,
            "median": 0,
            "max": 0,
            "min": 0,
        }

    # Calculate points per minute for pools with data
    valid_indices = [
        i for i, (ts, dp) in enumerate(zip(time_spans, data_points)) if ts is not None and dp is not None and ts > 0
    ]

    if valid_indices:
        points_per_minute = [data_points[i] / time_spans[i] for i in valid_indices]
        summary_stats["points_per_minute"] = {
            "mean": np.mean(points_per_minute),
            "median": np.median(points_per_minute),
            "max": max(points_per_minute),
            "min": min(points_per_minute),
        }

    # Identify pools that meet the criteria
    filtered_pools = []
    for pool_id, ts, dp in zip(all_pool_ids, time_spans, data_points):
        if pool_id in consistent_pools and ts is not None and dp is not None:
            # We want at least 10 minutes of data with consistent structure
            if ts >= 10.0 and dp >= 10:
                filtered_pools.append(
                    {
                        "pool_id": pool_id,
                        "time_span_minutes": ts,
                        "data_points": dp,
                        "points_per_minute": dp / ts if ts > 0 else 0,
                    }
                )

    # Sort by time span (descending)
    filtered_pools.sort(key=lambda x: x["time_span_minutes"], reverse=True)

    # Save results to file
    save_results(filtered_pools, common_fields, summary_stats, output_dir)

    # Generate visualizations
    generate_visualizations(time_spans, data_points, filtered_pools, output_dir)

    # Display summary
    display_summary(filtered_pools, common_fields, summary_stats)

    # Update documentation
    update_documentation(common_fields, summary_stats, filtered_pools)


def identify_common_fields(firebase_service, sample_pools):
    """Identify common fields across a sample of pools"""
    logger.info(f"Identifying common fields across {len(sample_pools)} sample pools...")

    all_fields = []
    field_counts = Counter()

    for i, pool_id in enumerate(sample_pools):
        logger.info(f"Analyzing fields for pool {i+1}/{len(sample_pools)}: {pool_id}")

        # Fetch a small amount of data for this pool
        pool_data = firebase_service.fetch_market_data(
            min_data_points=5, max_pools=1, limit_per_pool=5, pool_address=pool_id
        ).get(pool_id)

        if pool_data is not None and not pool_data.empty:
            # Get all columns for this pool
            pool_fields = set(pool_data.columns)
            all_fields.append(pool_fields)

            # Update field counts
            for field in pool_fields:
                field_counts[field] += 1

    # Find fields that are present in at least 80% of the samples
    common_threshold = 0.8 * len(sample_pools)
    common_fields = [field for field, count in field_counts.items() if count >= common_threshold]

    logger.info(f"Identified {len(common_fields)} common fields present in at least 80% of sample pools")

    return common_fields


def analyze_pools(firebase_service, pool_ids, common_fields):
    """Analyze pools for time span and data consistency"""
    results = {"consistent_pools": [], "time_spans": [], "data_points": []}

    for i, pool_id in enumerate(pool_ids):
        # Fetch data for this pool with a high limit to get all available data
        pool_data = firebase_service.fetch_market_data(
            min_data_points=10, max_pools=1, limit_per_pool=1000, pool_address=pool_id
        ).get(pool_id)

        if pool_data is not None and not pool_data.empty:
            # Calculate time span
            if "timestamp" in pool_data.columns:
                min_time = pool_data["timestamp"].min()
                max_time = pool_data["timestamp"].max()
                time_span_minutes = (max_time - min_time).total_seconds() / 60
                results["time_spans"].append(time_span_minutes)

                # Count data points
                data_point_count = len(pool_data)
                results["data_points"].append(data_point_count)

                # Check for data consistency
                pool_fields = set(pool_data.columns)
                if all(field in pool_fields for field in common_fields):
                    results["consistent_pools"].append(pool_id)
            else:
                results["time_spans"].append(None)
                results["data_points"].append(None)
        else:
            results["time_spans"].append(None)
            results["data_points"].append(None)

    return results


def save_results(filtered_pools, common_fields, summary_stats, output_dir):
    """Save results to files"""
    # Save filtered pools list
    filtered_pools_file = os.path.join(output_dir, "filtered_pools.json")
    with open(filtered_pools_file, "w") as f:
        json.dump(filtered_pools, f, indent=2)
    logger.info(f"Saved filtered pools list to {filtered_pools_file}")

    # Save common fields
    common_fields_file = os.path.join(output_dir, "common_fields.json")
    with open(common_fields_file, "w") as f:
        json.dump(common_fields, f, indent=2)
    logger.info(f"Saved common fields to {common_fields_file}")

    # Save summary statistics
    summary_stats_file = os.path.join(output_dir, "pool_summary_stats.json")

    # Convert numpy values to Python native types for JSON serialization
    clean_stats = {}
    for key, value in summary_stats.items():
        if isinstance(value, dict):
            clean_stats[key] = {k: float(v) if isinstance(v, np.number) else v for k, v in value.items()}
        elif isinstance(value, np.number):
            clean_stats[key] = float(value)
        else:
            clean_stats[key] = value

    with open(summary_stats_file, "w") as f:
        json.dump(clean_stats, f, indent=2)
    logger.info(f"Saved summary statistics to {summary_stats_file}")


def generate_visualizations(time_spans, data_points, filtered_pools, output_dir):
    """Generate visualizations of pool data characteristics"""
    # Filter out None values
    valid_time_spans = [ts for ts in time_spans if ts is not None]
    valid_data_points = [dp for dp in data_points if dp is not None]

    if valid_time_spans and valid_data_points:
        # Histogram of time spans
        plt.figure(figsize=(10, 6))
        plt.hist(valid_time_spans, bins=20, alpha=0.7)
        plt.title("Distribution of Time Spans Across All Pools")
        plt.xlabel("Time Span (minutes)")
        plt.ylabel("Number of Pools")
        plt.grid(True, alpha=0.3)
        plt.axvline(x=10, color="r", linestyle="--", label="10 min threshold")
        plt.legend()
        plt.savefig(os.path.join(output_dir, "time_span_distribution.png"))
        plt.close()

        # Histogram of data points
        plt.figure(figsize=(10, 6))
        plt.hist(valid_data_points, bins=20, alpha=0.7)
        plt.title("Distribution of Data Points Across All Pools")
        plt.xlabel("Number of Data Points")
        plt.ylabel("Number of Pools")
        plt.grid(True, alpha=0.3)
        plt.savefig(os.path.join(output_dir, "data_points_distribution.png"))
        plt.close()

        # Scatter plot of time span vs data points
        plt.figure(figsize=(10, 6))
        plt.scatter(valid_time_spans, valid_data_points, alpha=0.5)
        plt.title("Time Span vs Data Points")
        plt.xlabel("Time Span (minutes)")
        plt.ylabel("Number of Data Points")
        plt.axvline(x=10, color="r", linestyle="--", label="10 min threshold")
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.savefig(os.path.join(output_dir, "time_span_vs_data_points.png"))
        plt.close()

    # Generate histogram of time spans for filtered pools
    if filtered_pools:
        filtered_time_spans = [p["time_span_minutes"] for p in filtered_pools]

        plt.figure(figsize=(10, 6))
        plt.hist(filtered_time_spans, bins=20, alpha=0.7)
        plt.title("Distribution of Time Spans for Filtered Pools")
        plt.xlabel("Time Span (minutes)")
        plt.ylabel("Number of Pools")
        plt.grid(True, alpha=0.3)
        plt.savefig(os.path.join(output_dir, "filtered_time_span_distribution.png"))
        plt.close()


def display_summary(filtered_pools, common_fields, summary_stats):
    """Display summary of findings"""
    print("\n" + "=" * 80)
    print("POOL FILTERING SUMMARY REPORT")
    print("=" * 80)

    # General statistics
    print("\nGeneral Statistics:")
    print(f"Total pools analyzed: {summary_stats['total_pools']}")
    print(f"Pools with at least 10 minutes of data: {summary_stats['pools_with_10min_data']}")
    print(f"Pools with consistent data structure: {summary_stats['pools_with_consistent_data']}")
    print(f"Pools meeting both criteria: {len(filtered_pools)}")

    # Time span statistics
    print("\nTime Span Statistics (minutes):")
    print(f"  Average: {summary_stats['time_span_stats']['mean']:.2f}")
    print(f"  Median: {summary_stats['time_span_stats']['median']:.2f}")
    print(f"  Min: {summary_stats['time_span_stats']['min']:.2f}")
    print(f"  Max: {summary_stats['time_span_stats']['max']:.2f}")

    # Data points statistics
    print("\nData Points Statistics:")
    print(f"  Average: {summary_stats['data_points_stats']['mean']:.2f}")
    print(f"  Median: {summary_stats['data_points_stats']['median']:.2f}")
    print(f"  Min: {summary_stats['data_points_stats']['min']:.2f}")
    print(f"  Max: {summary_stats['data_points_stats']['max']:.2f}")

    # Data points per minute
    if "points_per_minute" in summary_stats:
        print("\nData Points per Minute:")
        print(f"  Average: {summary_stats['points_per_minute']['mean']:.2f}")
        print(f"  Median: {summary_stats['points_per_minute']['median']:.2f}")
        print(f"  Min: {summary_stats['points_per_minute']['min']:.2f}")
        print(f"  Max: {summary_stats['points_per_minute']['max']:.2f}")

    # Show top 10 pools by time span
    if filtered_pools:
        print("\nTop 10 Pools by Time Span:")
        headers = ["Pool ID", "Time Span (min)", "Data Points", "Points/Min"]
        rows = [
            (p["pool_id"], round(p["time_span_minutes"], 2), p["data_points"], round(p["points_per_minute"], 2))
            for p in filtered_pools[:10]
        ]
        print(tabulate(rows, headers=headers, tablefmt="pipe"))

    # List common fields (abbreviated if too many)
    print(f"\nCommon Fields ({len(common_fields)}):")
    if len(common_fields) > 10:
        for field in common_fields[:10]:
            print(f"  - {field}")
        print(f"  - ... and {len(common_fields) - 10} more")
    else:
        for field in common_fields:
            print(f"  - {field}")

    # Results location
    print("\nDetailed Results:")
    print("  - Filtered pools list: outputs/filtered_pools.json")
    print("  - Common fields: outputs/common_fields.json")
    print("  - Summary statistics: outputs/pool_summary_stats.json")
    print("  - Time span distribution: outputs/time_span_distribution.png")
    print("  - Data points distribution: outputs/data_points_distribution.png")
    print("  - Time span vs data points: outputs/time_span_vs_data_points.png")
    print("  - Filtered pools time span distribution: outputs/filtered_time_span_distribution.png")

    print("\nHow to Use Filtered Pools:")
    print("  - Load the filtered_pools.json file to get a list of suitable pool IDs")
    print("  - Use these pool IDs with FirebaseService to ensure data quality")
    print("  - Ensure your analysis code handles all common fields consistently")
    print("=" * 80)


def update_documentation(common_fields, summary_stats, filtered_pools):
    """Update the documentation with filtering information"""
    # Create markdown content for filtering section
    filtering_md = """
## Data Filtering for Analytics and Simulations

For reliable analytics and trading simulations, pools should meet certain criteria regarding data quality and consistency.

### Filtering Criteria

1. **Minimum Time Span**: At least 10 minutes of data (approximately 600 data points)
2. **Data Consistency**: Must contain all common fields identified across the database
3. **Data Quality**: Should have consistent data collection frequency

### Common Fields

The following fields are present in at least 80% of pools and should be used for consistent analytics:

"""

    # Add common fields list
    for field in sorted(common_fields):
        filtering_md += f"- `{field}`\n"

    # Add statistics
    filtering_md += f"""
### Dataset Statistics

- Total pools in database: {summary_stats['total_pools']} (sample)
- Pools with at least 10 minutes of data: {summary_stats['pools_with_10min_data']}
- Pools with consistent data structure: {summary_stats['pools_with_consistent_data']}
- Pools meeting both criteria: {len(filtered_pools)}

Average data points per minute: {summary_stats.get('points_per_minute', {}).get('mean', 'N/A')}

### How to Filter Pools

To filter pools for analytics and simulations, use:

```python
# Method 1: Using filtered_pools.json
with open('outputs/filtered_pools.json', 'r') as f:
    filtered_pools = json.load(f)
    
# Get pool IDs
filtered_pool_ids = [p['pool_id'] for p in filtered_pools]

# Method 2: Using FirebaseService with filtering
firebase_service = FirebaseService()
pool_data = firebase_service.fetch_market_data(
    min_data_points=600,  # Approximately 10 minutes of data
    min_time_span_minutes=10,
    ensure_common_fields=True
)
```

For optimal simulation results, consider using pools that have at least 30 minutes of data (approximately 1800 data points).
"""

    # Save to file
    filtering_doc_path = os.path.join(project_root, "docs", "pool_filtering.md")
    with open(filtering_doc_path, "w") as f:
        f.write(filtering_md)

    logger.info(f"Updated filtering documentation at {filtering_doc_path}")


if __name__ == "__main__":
    main()
