#!/usr/bin/env python

"""
Analysis of Invalid Pools

This script analyzes pools identified as invalid in previous analysis:
1. Takes a JSON file containing invalid pool IDs as input
2. Analyzes each pool to identify specific issues
3. Generates detailed report on why pools are invalid
4. Produces statistics on common patterns among invalid pools
"""

import os
import logging
import json
import pandas as pd
from collections import Counter
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Import services
from src.data.firebase_service import FirebaseService


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


def create_field_name_mapping():
    """
    Create a mapping of field names from camelCase to snake_case and vice versa.
    This helps in checking if the same field is present but with a different naming convention.
    """
    field_mapping = {}

    # Function to convert camelCase to snake_case
    def camel_to_snake(name):
        import re

        name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()

    # Function to convert snake_case to camelCase
    def snake_to_camel(name):
        components = name.split("_")
        return components[0] + "".join(x.title() for x in components[1:])

    # Create mappings for all required fields
    for field in REQUIRED_FIELDS:
        if "." in field:  # Handle nested fields
            parts = field.split(".")
            camel_parts = [parts[0]]
            snake_parts = [camel_to_snake(parts[0])]

            for part in parts[1:]:
                camel_parts.append(part)
                snake_parts.append(camel_to_snake(part))

            # Create camelCase version (e.g., trade_last5Seconds.volumeBuy)
            camel_field = ".".join(camel_parts)
            # Create snake_case version (e.g., trade_last_5_seconds.volume_buy)
            snake_field = ".".join(snake_parts)

            field_mapping[camel_field] = snake_field
            field_mapping[snake_field] = camel_field
        else:
            field_mapping[field] = camel_to_snake(field)
            field_mapping[camel_to_snake(field)] = field

    return field_mapping


def field_exists(field, pool_fields):
    """
    Check if a field exists in the pool's fields, considering nested fields and different naming conventions.
    """
    # Direct check
    if field in pool_fields:
        return True

    # Check for parent field
    if "." in field:
        parent_field = field.split(".")[0]
        if parent_field in pool_fields and isinstance(pool_fields[parent_field], dict):
            remaining_path = ".".join(field.split(".")[1:])
            return field_exists(remaining_path, pool_fields[parent_field])

    # Check for different naming convention
    field_mapping = create_field_name_mapping()
    if field in field_mapping and field_mapping[field] in pool_fields:
        return True

    return False


def get_missing_fields(required_fields, pool_fields):
    """
    Get a list of required fields that are missing from the pool's fields.
    """
    missing_fields = []
    for field in required_fields:
        if not field_exists(field, pool_fields):
            missing_fields.append(field)
    return missing_fields


def is_parent_field_of_required_field(field, required_fields):
    """
    Check if a field is a parent field of any required field.
    """
    for required_field in required_fields:
        if "." in required_field and required_field.startswith(field + "."):
            return True
    return False


def analyze_pool_fields(pool_data, pool_id):
    """
    Analyze fields for a pool and check against required fields.
    Identifies specific issues with the pool data.
    """
    if pool_data is None or pool_data.empty:
        return {
            "pool_id": pool_id,
            "status": "error",
            "error": "No data available",
            "data_points": 0,
            "issues": ["No data available for this pool"],
            "severity": "high",
            "required_fields_missing": len(REQUIRED_FIELDS),
            "required_fields_present": 0,
            "missing_fields": REQUIRED_FIELDS,
            "extra_fields": [],
        }

    # Number of data points
    num_data_points = len(pool_data)

    # Convert the first row to dictionary to check fields
    row_dict = pool_data.iloc[0].to_dict()
    pool_fields = set(row_dict.keys())

    # Identify missing fields
    missing_fields = get_missing_fields(REQUIRED_FIELDS, row_dict)
    extra_fields = [
        f for f in pool_fields if f not in REQUIRED_FIELDS and not is_parent_field_of_required_field(f, REQUIRED_FIELDS)
    ]

    # Check for data quality issues
    issues = []
    severity = "low"

    # Check for critical missing fields
    critical_fields = [
        "marketCap",
        "currentPrice",
        "holdersCount",
        "trade_last5Seconds.volume.buy",
        "trade_last5Seconds.volume.sell",
    ]
    missing_critical = [f for f in critical_fields if f in missing_fields]
    if missing_critical:
        issues.append(f"Missing critical fields: {', '.join(missing_critical)}")
        severity = "high"

    # Check for inconsistent data types
    type_issues = []
    for column in pool_data.columns:
        if column in ["marketCap", "currentPrice"] and pool_data[column].dtype not in [float, int]:
            type_issues.append(f"{column} has non-numeric type: {pool_data[column].dtype}")
        if column == "holdersCount" and not pd.api.types.is_integer_dtype(pool_data[column].dtype):
            type_issues.append(f"{column} is not integer type: {pool_data[column].dtype}")

    if type_issues:
        issues.extend(type_issues)
        severity = max(severity, "medium")

    # Check for null values
    null_columns = pool_data.columns[pool_data.isnull().any()].tolist()
    if null_columns:
        null_pcts = {col: (pool_data[col].isnull().sum() / len(pool_data)) * 100 for col in null_columns}
        severe_nulls = [f"{col} ({null_pcts[col]:.1f}% null)" for col, pct in null_pcts.items() if pct > 20]
        if severe_nulls:
            issues.append(f"High percentage of null values in: {', '.join(severe_nulls)}")
            severity = max(severity, "high")
        else:
            issues.append(f"Some null values in: {', '.join(null_columns)}")
            severity = max(severity, "low")

    # Check for anomalies in numeric columns
    numeric_cols = pool_data.select_dtypes(include=["number"]).columns
    for col in numeric_cols:
        if col in ["marketCap", "currentPrice", "holdersCount"]:
            # Check for zeros when they shouldn't be
            zero_pct = (pool_data[col] == 0).mean() * 100
            if zero_pct > 10:
                issues.append(f"{col} contains {zero_pct:.1f}% zeros")
                severity = max(severity, "medium")

            # Check for negative values when they shouldn't be
            if col in ["marketCap", "currentPrice", "holdersCount"]:
                neg_count = (pool_data[col] < 0).sum()
                if neg_count > 0:
                    issues.append(f"{col} contains {neg_count} negative values")
                    severity = max(severity, "high")

    # Check for time consistency
    if "timeFromStart" in pool_data.columns:
        # Time should increment consistently
        time_diffs = pool_data["timeFromStart"].diff().dropna()
        if not time_diffs.empty:
            irregular_intervals = (time_diffs != time_diffs.mode()[0]).sum()
            if irregular_intervals > 0.1 * len(time_diffs):
                issues.append(f"Irregular time intervals: {irregular_intervals} out of {len(time_diffs)}")
                severity = max(severity, "medium")

    # Check for enough data points
    if num_data_points < 600:  # Less than 10 minutes of data
        issues.append(f"Insufficient data points: {num_data_points} (less than 10 minutes)")
        severity = max(severity, "medium")

    # Return the analysis
    return {
        "pool_id": pool_id,
        "status": "analyzed",
        "data_points": num_data_points,
        "issues": issues,
        "severity": severity,
        "required_fields_missing": len(missing_fields),
        "required_fields_present": len(REQUIRED_FIELDS) - len(missing_fields),
        "missing_fields": missing_fields,
        "extra_fields": extra_fields,
    }


def analyze_invalid_pools(invalid_pool_ids, max_pools=None, limit_per_pool=None):
    """
    Analyze pools previously identified as invalid.

    Args:
        invalid_pool_ids: List of pool IDs to analyze
        max_pools: Maximum number of pools to analyze (None for all)
        limit_per_pool: Maximum number of data points to fetch per pool

    Returns:
        Dictionary with analysis results
    """
    # Initialize Firebase
    logger.info("Initializing Firebase connection...")
    firebase_service = FirebaseService()
    db = firebase_service.db

    if not db:
        logger.error("Failed to connect to Firebase. Exiting.")
        return None

    logger.info("Analyzing {} invalid pools...".format(len(invalid_pool_ids)))

    # Limit the number of pools if specified
    if max_pools and len(invalid_pool_ids) > max_pools:
        logger.info("Limiting analysis to {} pools".format(max_pools))
        invalid_pool_ids = invalid_pool_ids[:max_pools]

    # Initialize containers for results
    pool_analyses = {}
    issue_counts = Counter()
    field_missing_counts = Counter()
    severity_counts = Counter()
    pools_by_severity = {"high": [], "medium": [], "low": []}
    pools_by_issue_type = {}

    # Start timer
    start_time = datetime.now()

    # Process pools in batches to avoid memory issues
    batch_size = 10
    num_batches = (len(invalid_pool_ids) + batch_size - 1) // batch_size

    for batch_idx in range(num_batches):
        batch_start = batch_idx * batch_size
        batch_end = min((batch_idx + 1) * batch_size, len(invalid_pool_ids))
        current_batch = invalid_pool_ids[batch_start:batch_end]

        logger.info(
            "Processing batch {}/{}: pools {}-{} of {}".format(
                batch_idx + 1, num_batches, batch_start + 1, batch_end, len(invalid_pool_ids)
            )
        )

        for pool_id in current_batch:
            logger.info("Analyzing pool: {}".format(pool_id))

            # Fetch data for this pool
            try:
                # Fetch more rows to do a thorough analysis (600 rows is about 10 minutes of data)
                fetch_limit = limit_per_pool or 600
                pool_data = firebase_service.fetch_market_data(
                    min_data_points=5, pool_address=pool_id, limit_per_pool=fetch_limit
                ).get(pool_id)

                # Analyze the pool
                analysis = analyze_pool_fields(pool_data, pool_id)
                pool_analyses[pool_id] = analysis

                # Update counters
                severity_counts[analysis["severity"]] += 1
                pools_by_severity[analysis["severity"]].append(pool_id)

                for issue in analysis["issues"]:
                    issue_counts[issue] += 1
                    if issue not in pools_by_issue_type:
                        pools_by_issue_type[issue] = []
                    pools_by_issue_type[issue].append(pool_id)

                for field in analysis["missing_fields"]:
                    field_missing_counts[field] += 1

            except Exception as e:
                logger.error("Error analyzing pool {}: {}".format(pool_id, str(e)))
                pool_analyses[pool_id] = {
                    "pool_id": pool_id,
                    "status": "error",
                    "error": str(e),
                    "issues": ["Exception during analysis: " + str(e)],
                    "severity": "high",
                }
                severity_counts["high"] += 1
                pools_by_severity["high"].append(pool_id)

    # Compile results
    analysis_results = {
        "analysis_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "duration_seconds": (datetime.now() - start_time).total_seconds(),
        "total_pools_analyzed": len(invalid_pool_ids),
        "summary": {
            "pools_by_severity": {
                "high": len(pools_by_severity["high"]),
                "medium": len(pools_by_severity["medium"]),
                "low": len(pools_by_severity["low"]),
            },
            "most_common_issues": issue_counts.most_common(10),
            "most_common_missing_fields": field_missing_counts.most_common(10),
        },
        "pools_by_severity": pools_by_severity,
        "pools_by_issue_type": pools_by_issue_type,
        "detailed_analyses": pool_analyses,
    }

    return analysis_results


def main():
    """
    Main function to analyze invalid pools.

    1. Loads invalid pool IDs from a file
    2. Analyzes each pool to identify issues
    3. Saves detailed report and summary statistics
    """
    import argparse

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Analyze Invalid Pools")
    parser.add_argument(
        "--input",
        "-i",
        type=str,
        default="outputs/invalid_pools.json",
        help="Path to JSON file containing invalid pool IDs",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default="outputs/invalid_pools_analysis.json",
        help="Path to save the analysis results",
    )
    parser.add_argument("--max-pools", "-m", type=int, default=None, help="Maximum number of pools to analyze")
    parser.add_argument(
        "--limit-per-pool", "-l", type=int, default=600, help="Maximum number of data points to fetch per pool"
    )
    args = parser.parse_args()

    # Check if input file exists
    if not os.path.exists(args.input):
        logger.error("Input file does not exist: {}".format(args.input))
        return

    # Load invalid pool IDs
    logger.info("Loading invalid pool IDs from: {}".format(args.input))
    with open(args.input, "r") as f:
        try:
            invalid_pool_ids = json.load(f)
            if not isinstance(invalid_pool_ids, list):
                logger.error("Input file does not contain a list of pool IDs")
                return
            logger.info("Loaded {} invalid pool IDs".format(len(invalid_pool_ids)))
        except json.JSONDecodeError:
            logger.error("Failed to parse input file as JSON")
            return

    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(args.output)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # Analyze invalid pools
    results = analyze_invalid_pools(invalid_pool_ids, max_pools=args.max_pools, limit_per_pool=args.limit_per_pool)

    if results:
        # Save results
        logger.info("Saving analysis results to: {}".format(args.output))
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)

        # Print summary
        print("\n" + "=" * 80)
        print("INVALID POOLS ANALYSIS SUMMARY")
        print("=" * 80)

        print("\nPools analyzed: {}".format(results["total_pools_analyzed"]))
        print("Analysis completed in {:.1f} seconds".format(results["duration_seconds"]))

        print("\nSEVERITY BREAKDOWN:")
        print("-" * 40)
        for severity, count in results["summary"]["pools_by_severity"].items():
            print(
                "{}: {} pools ({:.1f}%)".format(
                    severity.upper(),
                    count,
                    count / results["total_pools_analyzed"] * 100 if results["total_pools_analyzed"] > 0 else 0,
                )
            )

        print("\nTOP 5 ISSUES:")
        print("-" * 40)
        for issue, count in results["summary"]["most_common_issues"][:5]:
            print("- {}: {} pools".format(issue, count))

        print("\nTOP 5 MISSING FIELDS:")
        print("-" * 40)
        for field, count in results["summary"]["most_common_missing_fields"][:5]:
            print("- {}: missing in {} pools".format(field, count))

        print("\nResults saved to: {}".format(args.output))
        print("=" * 80)
    else:
        logger.error("Analysis failed")


if __name__ == "__main__":
    main()
