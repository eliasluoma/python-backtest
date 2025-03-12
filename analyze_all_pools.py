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
import sys
import logging
import json
import re
from collections import Counter
from datetime import datetime

# Set absolute path to the root directory
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Print debugging information about the environment
print(f"Python version: {sys.version}")
print(f"Working directory: {os.getcwd()}")
print(f"sys.path: {sys.path}")
print(f"Script location: {__file__}")
print(f"Project root directory: {project_root}")

# Import Firebase utilities
try:
    from src.data.firebase_service import FirebaseService
    from src.utils.firebase_utils import get_pool_ids
    print("Firebase modules loaded successfully!")
except ImportError as e:
    logger.error(f"Firebase modules not found: {e}")
    logger.error("Make sure you run the script from the project root directory.")
    sys.exit(1)


# Define 63 required fields that our trading strategies use
REQUIRED_FIELDS = [
    # Market Cap fields
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
    # Price fields
    "currentPrice",
    "priceChangePercent",
    "priceChangeFromStart",
    # Holder fields
    "holdersCount",
    "initialHoldersCount",
    "holdersGrowthFromStart",
    "holderDelta5s",
    "holderDelta10s",
    "holderDelta30s",
    "holderDelta60s",
    # Volume fields
    "buyVolume5s",
    "buyVolume10s",
    "netVolume5s",
    "netVolume10s",
    # Buy classification fields
    "largeBuy5s",
    "largeBuy10s",
    "bigBuy5s",
    "bigBuy10s",
    "superBuy5s",
    "superBuy10s",
    # Trade data - 5s
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
    # Trade data - 10s
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
]

# Create a mapping between snake_case and camelCase field names
def create_field_name_mapping():
    """Create a mapping between snake_case and camelCase field names for trade data"""
    mappings = {}
    
    # Create mapping for trade_lastXSeconds and tradeLastXSeconds
    for field in REQUIRED_FIELDS:
        # If the field starts with "trade_last" and contains an underscore
        if field.startswith("trade_last") and "_" in field:
            # Create camelCase version
            camel_field = field.replace("trade_last", "tradeLast").replace("_", "")
            mappings[field] = camel_field
            mappings[camel_field] = field
    
    # Also add mappings for tradeLast5Seconds and tradeLast10Seconds,
    # even though they are no longer in the required_fields list
    # This helps the is_parent_field_of_required_field function
    mappings["trade_last5Seconds"] = "tradeLast5Seconds"
    mappings["tradeLast5Seconds"] = "trade_last5Seconds"
    mappings["trade_last10Seconds"] = "tradeLast10Seconds"
    mappings["tradeLast10Seconds"] = "trade_last10Seconds"
    
    # Also add creationTime and originalTimestamp
    mappings["creationTime"] = "originalTimestamp"
    mappings["originalTimestamp"] = "creationTime"
    
    return mappings

# Create field name mapping
FIELD_NAME_MAPPING = create_field_name_mapping()

def is_snake_case(field_name):
    """Check if a field name is in snake_case format"""
    return '_' in field_name


def is_camel_case(field_name):
    """Check if a field name is in camelCase format"""
    if '_' in field_name:  # If it contains underscores, it's not camelCase
        return False
    # CamelCase names typically have a mix of lowercase and uppercase letters
    return bool(re.match(r'^[a-z]+[A-Za-z0-9]*[A-Z]+[A-Za-z0-9]*$', field_name))


def field_exists(field, pool_fields):
    """
    Check if a field exists in the pool fields.
    If the field doesn't exist directly, also check alternative naming convention.
    """
    if field in pool_fields:
        return True
    
    # Check if there's an alternative name for the field
    alt_field = FIELD_NAME_MAPPING.get(field)
    if alt_field and alt_field in pool_fields:
        return True
    
    return False


def get_missing_fields(required_fields, pool_fields):
    """
    Check which required fields are missing, considering different naming conventions.
    Returns a list of missing fields.
    """
    missing = []
    for field in required_fields:
        if not field_exists(field, pool_fields):
            missing.append(field)
    return missing


def is_parent_field_of_required_field(field, required_fields):
    """
    Check if a field is a parent field of any required subfield.
    E.g., "tradeLast5Seconds" is a parent field for "trade_last5Seconds.volume.buy".
    This function considers different naming conventions (snake_case vs camelCase).
    """
    # Debug outputs only for interesting fields
    interesting_fields = ["trade_last5Seconds", "trade_last10Seconds"]
    debug_mode = field in interesting_fields
    
    if debug_mode:
        print(f"Checking if '{field}' is a parent field...")
    
    # Handle special cases - simplified mapping
    special_case_mappings = {
        "tradeLast5Seconds": "trade_last5Seconds",
        "trade_last5Seconds": "tradeLast5Seconds",
        "tradeLast10Seconds": "trade_last10Seconds",
        "trade_last10Seconds": "tradeLast10Seconds"
    }
    
    # Check if the field is a parent field of any required field
    for required in required_fields:
        # If the required field starts with this field followed by a dot
        prefix = field + "."
        if required.startswith(prefix):
            if debug_mode:
                print(f"  - FOUND: '{field}' is a parent field for '{required}'")
            return True
    
    # Check special case mappings to see if any corresponding parent field exists
    if field in special_case_mappings:
        alt_field = special_case_mappings[field]
        if debug_mode:
            print(f"  - Checking through special mapping: '{alt_field}'")
        
        for required in required_fields:
            prefix = alt_field + "."
            if required.startswith(prefix):
                if debug_mode:
                    print(f"  - FOUND SPECIAL MAPPING: '{field}' is a parent field for '{required}'")
                return True
    
    # Also check regular alternative name forms
    alt_field = FIELD_NAME_MAPPING.get(field)
    if alt_field:
        if debug_mode:
            print(f"  - Checking alternative form '{alt_field}'")
        for required in required_fields:
            prefix = alt_field + "."
            if required.startswith(prefix):
                if debug_mode:
                    print(f"  - FOUND ALTERNATIVE: '{alt_field}' is a parent field for '{required}'")
                return True
    
    if debug_mode:
        print(f"  - '{field}' IS NOT a parent field for any required field")
    return False


def main():
    """Analyze all pools for required fields and row counts"""
    # Initialize Firebase
    logger.info("Initializing Firebase connection...")
    firebase_service = FirebaseService()
    db = firebase_service.db

    if not db:
        logger.error("Failed to establish Firebase connection. Exiting.")
        return

    # Create timestamp for subfolder
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create output directory and timestamped subfolder
    output_dir = os.path.join(project_root, "outputs", f"pool_analysis_{timestamp}")
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Results will be saved to directory: {output_dir}")

    # Get all pool IDs without limitation
    logger.info("Fetching all pool IDs...")
    pool_ids = get_pool_ids(db)  # Removed limit - fetch all pools
    logger.info(f"Found {len(pool_ids)} pools to analyze")

    # Create data structures for analysis results
    valid_pools = []           # Pools with all required fields
    invalid_pools = []         # Pools missing required fields
    invalid_pools_details = [] # Detailed information about pools with missing fields
    pools_600_rows = []        # Pools with at least 600 rows
    pools_1100_rows = []       # Pools with at least 1100 rows
    valid_and_5_rows = []      # Pools with all required fields AND at least 5 rows
    valid_and_600_rows = []    # Pools with all required fields AND at least 600 rows
    valid_and_1100_rows = []   # Pools with all required fields AND at least 1100 rows
    missing_fields_count = Counter()  # Counter for missing fields
    extra_fields_count = Counter()    # Counter for extra fields
    all_pools_info = []        # All information from pool analysis
    all_fields_seen = set()    # All fields that exist
    
    # For naming convention analysis
    pools_with_snake_case = set()  # Pools with snake_case fields
    pools_with_camel_case = set()  # Pools with camelCase fields
    snake_case_fields = set()      # All snake_case fields found
    camel_case_fields = set()      # All camelCase fields found
    snake_case_field_counts = Counter()  # Counter for snake_case fields
    camel_case_field_counts = Counter()  # Counter for camelCase fields

    # Analysis statistics
    total_analyzed = 0
    total_valid = 0
    total_600_rows = 0
    total_1100_rows = 0
    total_valid_and_5 = 0
    total_valid_and_600 = 0
    total_valid_and_1100 = 0
    pool_data_counts = []      # Row counts for each pool
    
    # Counter for fields fixed by mapping
    fields_found_with_mapping = Counter()
    pools_helped_by_mapping = 0
    originally_valid_pools = 0
    mapping_improved_valid_pools = 0

    logger.info(f"Checking {len(pool_ids)} pools for {len(REQUIRED_FIELDS)} required fields...")

    # Process all pools
    for i, pool_id in enumerate(pool_ids):
        if i % 100 == 0:  # Report progress every 100 pools
            logger.info(f"Analyzing pool {i+1}/{len(pool_ids)}: {pool_id}")
        elif i % 10 == 0:  # Minor reporting every 10 pools
            print(f"Processing: {i+1}/{len(pool_ids)}", end="\r")

        # Fetch data for this pool (use small limit_per_pool since we just need to see the fields)
        # But set minimum data points low to include pools with little data
        pool_data = firebase_service.fetch_market_data(
            min_data_points=1, max_pools=1, limit_per_pool=1500, pool_address=pool_id
        ).get(pool_id)

        # Only process if we get some data
        if pool_data is not None and not pool_data.empty:
            total_analyzed += 1
            row_count = len(pool_data)
            pool_data_counts.append(row_count)
            
            # Check row counts
            has_600_rows = row_count >= 600
            has_1100_rows = row_count >= 1100
            has_5_rows = row_count >= 5
            
            if has_600_rows:
                total_600_rows += 1
                pools_600_rows.append(pool_id)
            
            if has_1100_rows:
                total_1100_rows += 1
                pools_1100_rows.append(pool_id)

            # Check if all required fields are present
            pool_fields = set(pool_data.columns)
            
            # Update the list of all seen fields
            all_fields_seen.update(pool_fields)
            
            # First check without field mapping (original way)
            original_missing_fields = set(REQUIRED_FIELDS) - pool_fields
            pool_has_all_fields_originally = len(original_missing_fields) == 0
            
            if pool_has_all_fields_originally:
                originally_valid_pools += 1
            
            # Identify missing fields using field mapping
            missing_fields = get_missing_fields(REQUIRED_FIELDS, pool_fields)
            
            # Compare original missing fields to those missing after mapping
            fields_fixed_by_mapping = list(set(original_missing_fields) - set(missing_fields))
            for field in fields_fixed_by_mapping:
                fields_found_with_mapping[field] += 1
            
            if fields_fixed_by_mapping and len(missing_fields) == 0:
                pools_helped_by_mapping += 1
            
            # Identify extra fields (those that are not required)
            # Also consider that alternatively named (but required) fields are not "extra"
            # And parent fields of required subfields are also not "extra"
            extra_fields = set()
            for field in pool_fields:
                # If the field is not required AND the field is not an alternative name for any required field
                # AND the field is not a parent field for any required field
                if (field not in REQUIRED_FIELDS and 
                    field not in FIELD_NAME_MAPPING and 
                    not is_parent_field_of_required_field(field, REQUIRED_FIELDS)):
                    extra_fields.add(field)
            
            # Collect statistics on missing fields
            for field in missing_fields:
                missing_fields_count[field] += 1
                
            # Collect statistics on extra fields
            for field in extra_fields:
                extra_fields_count[field] += 1
                
            # Collect statistics on naming conventions
            has_snake_case = False
            has_camel_case = False
            
            for field in pool_fields:
                if is_snake_case(field):
                    has_snake_case = True
                    snake_case_fields.add(field)
                    snake_case_field_counts[field] += 1
                elif is_camel_case(field):
                    has_camel_case = True
                    camel_case_fields.add(field)
                    camel_case_field_counts[field] += 1
            
            if has_snake_case:
                pools_with_snake_case.add(pool_id)
            if has_camel_case:
                pools_with_camel_case.add(pool_id)

            if not missing_fields:
                # Pool has all required fields (considering mappings)
                total_valid += 1
                valid_pools.append(pool_id)
                
                # Also check row requirements
                if has_5_rows:
                    total_valid_and_5 += 1
                    valid_and_5_rows.append(pool_id)
                
                if has_600_rows:
                    total_valid_and_600 += 1
                    valid_and_600_rows.append(pool_id)
                
                if has_1100_rows:
                    total_valid_and_1100 += 1
                    valid_and_1100_rows.append(pool_id)
                
                pool_info = {
                    "pool_id": pool_id,
                    "record_count": row_count,
                    "field_count": len(pool_fields),
                    "has_all_required_fields": True,
                    "has_600_rows": has_600_rows,
                    "has_1100_rows": has_1100_rows,
                    "extra_fields_count": len(extra_fields),
                    "extra_fields": list(extra_fields) if extra_fields else [],
                    "has_snake_case_fields": has_snake_case,
                    "has_camel_case_fields": has_camel_case,
                    "fields_fixed_by_mapping": fields_fixed_by_mapping
                }
            else:
                # Pool is missing some fields
                invalid_pools.append(pool_id)
                
                # Save detailed info about the incomplete pool
                invalid_pool_detail = {
                    "pool_id": pool_id,
                    "record_count": row_count,
                    "missing_fields": list(missing_fields)
                }
                invalid_pools_details.append(invalid_pool_detail)
                
                pool_info = {
                    "pool_id": pool_id,
                    "record_count": row_count,
                    "field_count": len(pool_fields),
                    "has_all_required_fields": False,
                    "has_600_rows": has_600_rows,
                    "has_1100_rows": has_1100_rows,
                    "missing_field_count": len(missing_fields),
                    "missing_fields": list(missing_fields),
                    "extra_fields_count": len(extra_fields),
                    "extra_fields": list(extra_fields) if extra_fields else [],
                    "has_snake_case_fields": has_snake_case,
                    "has_camel_case_fields": has_camel_case,
                    "fields_fixed_by_mapping": fields_fixed_by_mapping
                }
            
            all_pools_info.append(pool_info)

        elif pool_data is None:
            logger.info(f"✗ Pool {pool_id} - Could not fetch data")
        else:
            logger.info(f"✗ Pool {pool_id} - No data (empty DataFrame)")

    # Calculate how much mapping improved the number of valid pools
    mapping_improved_valid_pools = total_valid - originally_valid_pools

    # Save results (note that timestamp is already defined at the beginning of the function)
    valid_pools_file = os.path.join(output_dir, "valid_pools.json")
    with open(valid_pools_file, "w") as f:
        json.dump(valid_pools, f, indent=2)
        
    # Save invalid pools
    invalid_pools_file = os.path.join(output_dir, "invalid_pools.json")
    with open(invalid_pools_file, "w") as f:
        json.dump(invalid_pools, f, indent=2)
        
    # Save detailed information about invalid pools
    invalid_pools_details_file = os.path.join(output_dir, "invalid_pools_details.json")
    with open(invalid_pools_details_file, "w") as f:
        json.dump(invalid_pools_details, f, indent=2)

    pools_600_file = os.path.join(output_dir, "pools_600_rows.json") 
    with open(pools_600_file, "w") as f:
        json.dump(pools_600_rows, f, indent=2)
        
    pools_1100_file = os.path.join(output_dir, "pools_1100_rows.json")
    with open(pools_1100_file, "w") as f:
        json.dump(pools_1100_rows, f, indent=2)
        
    # Save combined results
    valid_and_5_file = os.path.join(output_dir, "valid_and_5_rows.json")
    with open(valid_and_5_file, "w") as f:
        json.dump(valid_and_5_rows, f, indent=2)
        
    valid_and_600_file = os.path.join(output_dir, "valid_and_600_rows.json")
    with open(valid_and_600_file, "w") as f:
        json.dump(valid_and_600_rows, f, indent=2)
        
    valid_and_1100_file = os.path.join(output_dir, "valid_and_1100_rows.json")
    with open(valid_and_1100_file, "w") as f:
        json.dump(valid_and_1100_rows, f, indent=2)
        
    all_pools_file = os.path.join(output_dir, "all_pools_analysis.json")
    with open(all_pools_file, "w") as f:
        json.dump(all_pools_info, f, indent=2)

    # Calculate statistics on missing fields
    total_missing_fields = len(missing_fields_count)
    most_common_missing = missing_fields_count.most_common(20)  # Top 20 missing fields
    
    missing_fields_file = os.path.join(output_dir, "missing_fields_analysis.json")
    with open(missing_fields_file, "w") as f:
        json.dump({
            "missing_fields_count": dict(missing_fields_count),
            "most_common_missing": [{"field": field, "count": count} for field, count in most_common_missing]
        }, f, indent=2)
        
    # Calculate statistics on extra fields
    total_extra_fields = len(extra_fields_count)
    most_common_extra = extra_fields_count.most_common()  # All extra fields
    
    extra_fields_file = os.path.join(output_dir, "extra_fields_analysis.json")
    with open(extra_fields_file, "w") as f:
        json.dump({
            "extra_fields_count": dict(extra_fields_count),
            "most_common_extra": [{"field": field, "count": count} for field, count in most_common_extra],
            "total_unique_fields": len(all_fields_seen),
            "all_fields": list(all_fields_seen)
        }, f, indent=2)
        
    # Save naming convention analysis
    naming_analysis_file = os.path.join(output_dir, "naming_convention_analysis.json")
    with open(naming_analysis_file, "w") as f:
        json.dump({
            "pools_with_snake_case": len(pools_with_snake_case),
            "pools_with_camel_case": len(pools_with_camel_case),
            "pools_with_both": len(pools_with_snake_case.intersection(pools_with_camel_case)),
            "pools_with_only_snake_case": len(pools_with_snake_case - pools_with_camel_case),
            "pools_with_only_camel_case": len(pools_with_camel_case - pools_with_snake_case),
            "unique_snake_case_fields": len(snake_case_fields),
            "unique_camel_case_fields": len(camel_case_fields),
            "snake_case_field_counts": dict(snake_case_field_counts.most_common()),
            "camel_case_field_counts": dict(camel_case_field_counts.most_common())
        }, f, indent=2)
        
    # Save field mapping analysis
    mapping_analysis_file = os.path.join(output_dir, "field_mapping_analysis.json")
    with open(mapping_analysis_file, "w") as f:
        json.dump({
            "fields_found_with_mapping": dict(fields_found_with_mapping),
            "pools_helped_by_mapping": pools_helped_by_mapping,
            "originally_valid_pools": originally_valid_pools,
            "mapping_improved_valid_pools": mapping_improved_valid_pools
        }, f, indent=2)

    # Print summary
    print("\n" + "=" * 80)
    print("POOL ANALYSIS RESULTS")
    print("=" * 80)
    print(f"Analyzed pools: {total_analyzed} / {len(pool_ids)}")
    print(f"Pools with all {len(REQUIRED_FIELDS)} required fields (original): {originally_valid_pools} ({originally_valid_pools/total_analyzed*100:.1f}%)")
    print(f"Pools with all {len(REQUIRED_FIELDS)} required fields (with name mapping): {total_valid} ({total_valid/total_analyzed*100:.1f}%)")
    print(f"Pools improved by name mapping: {mapping_improved_valid_pools} ({mapping_improved_valid_pools/total_analyzed*100:.1f}%)")
    print(f"Pools with at least 600 rows (~10 min of data): {total_600_rows} ({total_600_rows/total_analyzed*100:.1f}%)")
    print(f"Pools with at least 1100 rows (~18 min of data): {total_1100_rows} ({total_1100_rows/total_analyzed*100:.1f}%)")
    print(f"Pools with all required fields AND at least 5 rows: {total_valid_and_5} ({total_valid_and_5/total_analyzed*100:.1f}%)")
    print(f"Pools with all required fields AND at least 600 rows: {total_valid_and_600} ({total_valid_and_600/total_analyzed*100:.1f}%)")
    print(f"Pools with all required fields AND at least 1100 rows: {total_valid_and_1100} ({total_valid_and_1100/total_analyzed*100:.1f}%)")
    
    if pool_data_counts:
        avg_rows = sum(pool_data_counts) / len(pool_data_counts)
        min_rows = min(pool_data_counts)
        max_rows = max(pool_data_counts)
        print(f"\nRow count statistics:")
        print(f"Average: {avg_rows:.1f} rows per pool")
        print(f"Minimum: {min_rows} rows")
        print(f"Maximum: {max_rows} rows")
    
    # Print missing fields
    print("\nMost common missing fields:")
    for field, count in most_common_missing[:10]:
        print(f"- {field}: missing from {count} pools ({count/total_analyzed*100:.1f}%)")
        
    # Print extra fields
    print("\nMost common extra fields (not required):")
    for field, count in most_common_extra[:10]:
        print(f"- {field}: found in {count} pools ({count/total_analyzed*100:.1f}%)")
    
    print(f"\nFound a total of {len(all_fields_seen)} unique fields, with {len(REQUIRED_FIELDS)} required and {total_extra_fields} extra")
    
    # Print naming convention analysis
    snake_case_pool_count = len(pools_with_snake_case)
    camel_case_pool_count = len(pools_with_camel_case)
    both_styles_pool_count = len(pools_with_snake_case.intersection(pools_with_camel_case))
    only_snake_case_pool_count = len(pools_with_snake_case - pools_with_camel_case)
    only_camel_case_pool_count = len(pools_with_camel_case - pools_with_snake_case)
    
    print("\nNaming conventions:")
    print(f"- Pools with snake_case fields: {snake_case_pool_count} ({snake_case_pool_count/total_analyzed*100:.1f}%)")
    print(f"- Pools with camelCase fields: {camel_case_pool_count} ({camel_case_pool_count/total_analyzed*100:.1f}%)")
    print(f"- Pools with both naming conventions: {both_styles_pool_count} ({both_styles_pool_count/total_analyzed*100:.1f}%)")
    print(f"- Pools with only snake_case fields: {only_snake_case_pool_count} ({only_snake_case_pool_count/total_analyzed*100:.1f}%)")
    print(f"- Pools with only camelCase fields: {only_camel_case_pool_count} ({only_camel_case_pool_count/total_analyzed*100:.1f}%)")
    print(f"- Unique snake_case fields: {len(snake_case_fields)}")
    print(f"- Unique camelCase fields: {len(camel_case_fields)}")
    
    # Print field mapping analysis
    print("\nField mapping impact:")
    print(f"- Pools helped by field mapping: {pools_helped_by_mapping} ({pools_helped_by_mapping/total_analyzed*100:.1f}%)")
    print(f"- Originally valid pools: {originally_valid_pools} ({originally_valid_pools/total_analyzed*100:.1f}%)")
    print(f"- Valid pools after mapping: {total_valid} ({total_valid/total_analyzed*100:.1f}%)")
    print(f"- New valid pools due to mapping: {mapping_improved_valid_pools} (+{mapping_improved_valid_pools/originally_valid_pools*100:.1f}%)")
    
    # Print most common fields found through mapping
    print("\nMost common fields found through alternative names:")
    for field, count in fields_found_with_mapping.most_common(10):
        print(f"- {field} → {FIELD_NAME_MAPPING.get(field)}: found in {count} pools")
    
    print("\nResults saved to files:")
    print(f"- {valid_pools_file} ({len(valid_pools)} pools)")
    print(f"- {invalid_pools_file} ({len(invalid_pools)} pools)")
    print(f"- {invalid_pools_details_file} ({len(invalid_pools_details)} pool details)")
    print(f"- {pools_600_file} ({len(pools_600_rows)} pools)")
    print(f"- {pools_1100_file} ({len(pools_1100_rows)} pools)")
    print(f"- {valid_and_5_file} ({len(valid_and_5_rows)} pools)")
    print(f"- {valid_and_600_file} ({len(valid_and_600_rows)} pools)")
    print(f"- {valid_and_1100_file} ({len(valid_and_1100_rows)} pools)")
    print(f"- {all_pools_file}")
    print(f"- {missing_fields_file}")
    print(f"- {extra_fields_file}")
    print(f"- {naming_analysis_file}")
    print(f"- {mapping_analysis_file}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user.")
    except Exception as e:
        logger.exception(f"Error in analysis: {str(e)}") 