#!/usr/bin/env python
"""
Analysis of All Pools

This script analyzes all Firebase pools and produces a report:
1. How many pools have all required fields
2. How many pools have at least 600 rows (approx. 10 min)
3. How many pools have at least 1100 rows (approx. 18 min)
4. How many pools have all required fields AND at least 600 rows
5. How many pools have all required fields AND at least 1100 rows
6. Analysis of missing fields (which fields are missing and how often)
7. Analysis of extra fields (fields that are not on the required list)
8. Analysis of naming conventions (snake_case vs. camelCase)
"""

import os
import logging
import json
import re
from collections import Counter
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Import services
from src.data.firebase_service import FirebaseService
from src.utils.firebase_utils import get_pool_ids


# Constants
MINIMAL_REQUIRED_FIELDS = [
    # Market Cap Fields
    "marketCap",
    "athMarketCap",
    "minMarketCap",
    # Price Fields
    "currentPrice",
    # Holder Fields
    "holdersCount",
    "initialHoldersCount",
    # Volume Fields
    "buyVolume5s",
    "buyVolume10s",
    "netVolume5s",
    "netVolume10s",
    # Buy Classification Fields
    "bigBuy5s",
    "bigBuy10s",
    # Trade Data - 5s
    "trade_last5Seconds.volume.buy",
    "trade_last5Seconds.volume.sell",
    # Trade Data - 10s
    "trade_last10Seconds.volume.buy",
    "trade_last10Seconds.volume.sell",
    # Metadata
    "poolAddress",
    "timeFromStart",
    "creationTime",
]

# Full set of required fields
REQUIRED_FIELDS = [
    # Market Cap Fields 10
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
    # Price Fields 3
    "currentPrice",
    "priceChangePercent",
    "priceChangeFromStart",
    # Holder Fields 8
    "holdersCount",
    "initialHoldersCount",
    "holdersGrowthFromStart",
    "holderDelta5s",
    "holderDelta10s",
    "holderDelta30s",
    "holderDelta60s",
    # Volume Fields 4
    "buyVolume5s",
    "buyVolume10s",
    "netVolume5s",
    "netVolume10s",
    # Buy Classification Fields 6
    "largeBuy5s",
    "largeBuy10s",
    "bigBuy5s",
    "bigBuy10s",
    "superBuy5s",
    "superBuy10s",
    # Trade Data - 5s 14
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
    # Trade Data - 10s 14
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
    # Metadata 2
    "poolAddress",
    "timeFromStart",

    
]


def create_field_name_mapping():
    """
    Create a mapping of field names from camelCase to snake_case and vice versa.
    This helps in checking if the same field is present but with a different naming convention.
    """
    field_mapping = {}

    # Function to convert camelCase to snake_case
    def camel_to_snake(name):
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


def is_snake_case(field_name):
    """Check if a field name uses snake_case convention"""
    return "_" in field_name and not field_name.startswith("_") and not field_name.endswith("_")


def is_camel_case(field_name):
    """Check if a field name uses camelCase convention"""
    return (
        not "_" in field_name
        and not field_name.startswith(tuple("ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
        and any(c.isupper() for c in field_name)
    )


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


def analyze_pool_fields(pool_data, pool_id, required_fields=None):
    """
    Analyze fields for a pool and check against required fields.
    """
    if required_fields is None:
        required_fields = REQUIRED_FIELDS

    # Get fields from the pool data
    if pool_data is None or pool_data.empty:
        return {
            "pool_id": pool_id,
            "status": "error",
            "error": "No data available",
            "data_points": 0,
            "required_fields_present": 0,
            "required_fields_missing": len(required_fields),
            "missing_fields": required_fields,
            "extra_fields": [],
            "camel_case_count": 0,
            "snake_case_count": 0,
            "other_case_count": 0,
        }

    # Convert the first row to dictionary to check fields
    row_dict = pool_data.iloc[0].to_dict()
    pool_fields = set(row_dict.keys())

    # Identify missing and extra fields
    missing_fields = get_missing_fields(required_fields, row_dict)
    extra_fields = [
        f for f in pool_fields if f not in required_fields and not is_parent_field_of_required_field(f, required_fields)
    ]

    # Count naming conventions
    camel_case_count = sum(1 for field in pool_fields if is_camel_case(field))
    snake_case_count = sum(1 for field in pool_fields if is_snake_case(field))
    other_case_count = len(pool_fields) - camel_case_count - snake_case_count

    return {
        "pool_id": pool_id,
        "status": "success",
        "data_points": len(pool_data),
        "required_fields_present": len(required_fields) - len(missing_fields),
        "required_fields_missing": len(missing_fields),
        "missing_fields": missing_fields,
        "extra_fields": extra_fields,
        "camel_case_count": camel_case_count,
        "snake_case_count": snake_case_count,
        "other_case_count": other_case_count,
    }


def main():
    """Analyze all Firebase pools and produce a report"""
    # Initialize Firebase
    logger.info("Initializing Firebase connection...")
    firebase_service = FirebaseService()
    db = firebase_service.db

    if not db:
        logger.error("Failed to connect to Firebase. Exiting.")
        return

    # Create output directory if it doesn't exist
    output_dir = os.path.join(os.getcwd(), "outputs")
    os.makedirs(output_dir, exist_ok=True)

    # Get all pool IDs
    logger.info("Fetching all pool IDs...")
    # Get a large number of pools for comprehensive analysis
    pool_ids = get_pool_ids(db, limit=3000)  # Increased limit for better coverage
    logger.info("Found {} pools for analysis".format(len(pool_ids)))

    # Filter out any pools with a None value
    pool_ids = [pool_id for pool_id in pool_ids if pool_id]
    logger.info("After filtering out None values, {} pools remain".format(len(pool_ids)))

    # Initialize counters and containers
    valid_pools = []
    valid_pools_minimal = []
    invalid_pools = []
    pools_with_600_rows = []
    pools_with_1100_rows = []
    valid_pools_with_600_rows = []
    valid_pools_with_1100_rows = []
    valid_pools_minimal_with_600_rows = []
    all_missing_fields = Counter()
    all_extra_fields = Counter()
    camel_case_fields = 0
    snake_case_fields = 0
    other_case_fields = 0
    success_count = 0
    error_count = 0

    # Define thresholds
    rows_threshold_10min = 600  # Approx. 10 minutes of data
    rows_threshold_18min = 1100  # Approx. 18 minutes of data

    # Get start time for reporting
    start_time = datetime.now()
    logger.info("Analysis started at: {}".format(start_time.strftime("%Y-%m-%d %H:%M:%S")))

    # Process pools in batches to save memory
    batch_size = 50
    num_batches = (len(pool_ids) + batch_size - 1) // batch_size  # Ceiling division
    total_processed = 0

    all_pool_analyses = {}

    for batch_idx in range(num_batches):
        batch_start = batch_idx * batch_size
        batch_end = min((batch_idx + 1) * batch_size, len(pool_ids))
        current_batch = pool_ids[batch_start:batch_end]

        logger.info(
            "Processing batch {}/{}: pools {}-{} of {}".format(
                batch_idx + 1, num_batches, batch_start + 1, batch_end, len(pool_ids)
            )
        )

        # Fetch data for this batch
        batch_data = {}
        for pool_id in current_batch:
            logger.info("Fetching data for pool: {}".format(pool_id))
            try:
                # Fetch a sample of data (20 rows is enough for field analysis)
                pool_data = firebase_service.fetch_market_data(
                    min_data_points=5, pool_address=pool_id, limit_per_pool=20
                )
                batch_data[pool_id] = pool_data.get(pool_id)
            except Exception as e:
                logger.error("Error fetching data for pool {}: {}".format(pool_id, str(e)))
                batch_data[pool_id] = None

        # Analyze pools in this batch
        for pool_id, pool_data in batch_data.items():
            # Analyze fields
            pool_analysis = analyze_pool_fields(pool_data, pool_id)
            all_pool_analyses[pool_id] = pool_analysis

            # Update counters
            if pool_analysis["status"] == "success":
                success_count += 1

                # Check if pool has all required fields
                if pool_analysis["required_fields_missing"] == 0:
                    valid_pools.append(pool_id)
                else:
                    invalid_pools.append(pool_id)

                # Check if pool has minimal required fields
                if all(field not in pool_analysis["missing_fields"] for field in MINIMAL_REQUIRED_FIELDS):
                    valid_pools_minimal.append(pool_id)

                # Update field statistics
                for field in pool_analysis["missing_fields"]:
                    all_missing_fields[field] += 1
                for field in pool_analysis["extra_fields"]:
                    all_extra_fields[field] += 1

                # Update naming convention counts
                camel_case_fields += pool_analysis["camel_case_count"]
                snake_case_fields += pool_analysis["snake_case_count"]
                other_case_fields += pool_analysis["other_case_count"]

                # Check row thresholds
                if pool_analysis["data_points"] >= rows_threshold_10min:
                    pools_with_600_rows.append(pool_id)
                    if pool_analysis["required_fields_missing"] == 0:
                        valid_pools_with_600_rows.append(pool_id)
                    if all(field not in pool_analysis["missing_fields"] for field in MINIMAL_REQUIRED_FIELDS):
                        valid_pools_minimal_with_600_rows.append(pool_id)

                if pool_analysis["data_points"] >= rows_threshold_18min:
                    pools_with_1100_rows.append(pool_id)
                    if pool_analysis["required_fields_missing"] == 0:
                        valid_pools_with_1100_rows.append(pool_id)
            else:
                error_count += 1
                invalid_pools.append(pool_id)

            # Update total processed
            total_processed += 1
            if total_processed % 10 == 0 or total_processed == len(pool_ids):
                elapsed_time = (datetime.now() - start_time).total_seconds()
                logger.info(
                    "Processed {}/{} pools ({:.1f}%). Elapsed: {:.1f}s".format(
                        total_processed, len(pool_ids), total_processed / len(pool_ids) * 100, elapsed_time
                    )
                )

    # Get additional statistics
    pools_with_some_data = success_count
    percent_valid = success_count > 0 and (len(valid_pools) / success_count) * 100 or 0
    percent_valid_minimal = success_count > 0 and (len(valid_pools_minimal) / success_count) * 100 or 0
    percent_with_600_rows = success_count > 0 and (len(pools_with_600_rows) / success_count) * 100 or 0
    percent_with_1100_rows = success_count > 0 and (len(pools_with_1100_rows) / success_count) * 100 or 0

    # Calculate most common missing and extra fields
    most_common_missing = all_missing_fields.most_common(20)
    most_common_extra = all_extra_fields.most_common(20)

    # Compile detailed report
    report = {
        "analysis_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "summary": {
            "total_pools": len(pool_ids),
            "pools_with_some_data": pools_with_some_data,
            "pools_with_error": error_count,
            "valid_pools": len(valid_pools),
            "valid_pools_percent": percent_valid,
            "valid_pools_minimal": len(valid_pools_minimal),
            "valid_pools_minimal_percent": percent_valid_minimal,
            "pools_with_600_rows": len(pools_with_600_rows),
            "pools_with_600_rows_percent": percent_with_600_rows,
            "pools_with_1100_rows": len(pools_with_1100_rows),
            "pools_with_1100_rows_percent": percent_with_1100_rows,
            "valid_pools_with_600_rows": len(valid_pools_with_600_rows),
            "valid_pools_minimal_with_600_rows": len(valid_pools_minimal_with_600_rows),
            "valid_pools_with_1100_rows": len(valid_pools_with_1100_rows),
        },
        "field_analysis": {
            "total_required_fields": len(REQUIRED_FIELDS),
            "total_minimal_required_fields": len(MINIMAL_REQUIRED_FIELDS),
            "missing_fields_frequency": {field: count for field, count in all_missing_fields.items()},
            "most_common_missing_fields": [(field, count) for field, count in most_common_missing],
            "most_common_extra_fields": [(field, count) for field, count in most_common_extra],
        },
        "naming_conventions": {
            "camel_case_fields": camel_case_fields,
            "snake_case_fields": snake_case_fields,
            "other_case_fields": other_case_fields,
        },
        "pool_ids": {
            "valid_pools": valid_pools,
            "valid_pools_minimal": valid_pools_minimal,
            "invalid_pools": invalid_pools,
            "pools_with_600_rows": pools_with_600_rows,
            "pools_with_1100_rows": pools_with_1100_rows,
            "valid_pools_with_600_rows": valid_pools_with_600_rows,
            "valid_pools_minimal_with_600_rows": valid_pools_minimal_with_600_rows,
            "valid_pools_with_1100_rows": valid_pools_with_1100_rows,
        },
    }

    # Save detailed report
    report_file = os.path.join(output_dir, "pool_analysis_report.json")
    with open(report_file, "w") as f:
        json.dump(report, f, indent=2)

    # Save lists of pool IDs by category for easier access
    for category, pool_list in report["pool_ids"].items():
        category_file = os.path.join(output_dir, f"{category}.json")
        with open(category_file, "w") as f:
            json.dump(pool_list, f, indent=2)

    # Save detailed analysis for each pool
    pool_analyses_file = os.path.join(output_dir, "all_pool_analyses.json")
    with open(pool_analyses_file, "w") as f:
        json.dump(all_pool_analyses, f, indent=2)

    # Print summary
    print("\n" + "=" * 80)
    print("POOL ANALYSIS REPORT")
    print("=" * 80)

    print("\nSUMMARY STATISTICS:")
    print("-" * 40)
    print(f"Total pools analyzed: {len(pool_ids)}")
    print(f"Pools with some data: {pools_with_some_data} ({pools_with_some_data/len(pool_ids)*100:.1f}%)")
    print(f"Pools with errors: {error_count} ({error_count/len(pool_ids)*100:.1f}%)")
    print(f"Valid pools (all required fields): {len(valid_pools)} ({percent_valid:.1f}%)")
    print(f"Valid pools (minimal fields): {len(valid_pools_minimal)} ({percent_valid_minimal:.1f}%)")
    print(f"Pools with 600+ rows (~10 min): {len(pools_with_600_rows)} ({percent_with_600_rows:.1f}%)")
    print(f"Pools with 1100+ rows (~18 min): {len(pools_with_1100_rows)} ({percent_with_1100_rows:.1f}%)")
    print(f"Valid pools with 600+ rows: {len(valid_pools_with_600_rows)}")
    print(f"Valid pools (minimal) with 600+ rows: {len(valid_pools_minimal_with_600_rows)}")
    print(f"Valid pools with 1100+ rows: {len(valid_pools_with_1100_rows)}")

    print("\nFIELD ANALYSIS:")
    print("-" * 40)
    print(f"Required fields: {len(REQUIRED_FIELDS)}")
    print(f"Minimal required fields: {len(MINIMAL_REQUIRED_FIELDS)}")

    print("\nTop 10 Missing Fields:")
    for field, count in most_common_missing[:10]:
        print(f"- {field}: missing in {count} pools ({count/success_count*100:.1f}%)")

    print("\nNAMING CONVENTIONS:")
    print("-" * 40)
    total_fields = camel_case_fields + snake_case_fields + other_case_fields
    print(f"camelCase fields: {camel_case_fields} ({camel_case_fields/total_fields*100:.1f}%)")
    print(f"snake_case fields: {snake_case_fields} ({snake_case_fields/total_fields*100:.1f}%)")
    print(f"Other fields: {other_case_fields} ({other_case_fields/total_fields*100:.1f}%)")

    print("\nRESULTS SAVED:")
    print("-" * 40)
    print(f"Full report: {report_file}")
    print(f"All pool analyses: {pool_analyses_file}")
    print(f"Valid pools list: {os.path.join(output_dir, 'valid_pools.json')}")
    print(f"Valid pools (minimal) list: {os.path.join(output_dir, 'valid_pools_minimal.json')}")
    print(f"Invalid pools list: {os.path.join(output_dir, 'invalid_pools.json')}")
    print("=" * 80)


if __name__ == "__main__":
    main()
