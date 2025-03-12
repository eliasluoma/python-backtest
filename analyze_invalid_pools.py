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
import sys
import logging
import json
import pandas as pd
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
    print("Firebase modules loaded successfully!")
except ImportError as e:
    logger.error(f"Firebase modules not found: {e}")
    logger.error("Make sure you run the script from the project root directory.")
    sys.exit(1)

# Define the required fields (same as in analyze_all_pools.py)
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

# Create field name mapping for alternate field names
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
    
    # Also add mappings for tradeLast5Seconds and tradeLast10Seconds
    mappings["trade_last5Seconds"] = "tradeLast5Seconds"
    mappings["tradeLast5Seconds"] = "trade_last5Seconds"
    mappings["trade_last10Seconds"] = "tradeLast10Seconds"
    mappings["tradeLast10Seconds"] = "trade_last10Seconds"
    
    # Also add creationTime and originalTimestamp
    mappings["creationTime"] = "originalTimestamp"
    mappings["originalTimestamp"] = "creationTime"
    
    return mappings

FIELD_NAME_MAPPING = create_field_name_mapping()

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
    # Handle special cases
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
            return True
    
    # Check special case mappings to see if any corresponding parent field exists
    if field in special_case_mappings:
        alt_field = special_case_mappings[field]
        for required in required_fields:
            prefix = alt_field + "."
            if required.startswith(prefix):
                return True
    
    # Also check regular alternative name forms
    alt_field = FIELD_NAME_MAPPING.get(field)
    if alt_field:
        for required in required_fields:
            prefix = alt_field + "."
            if required.startswith(prefix):
                return True
    
    return False

def analyze_pool_fields(pool_data, pool_id):
    """
    Analyze a pool's fields to identify issues
    
    Args:
        pool_data: DataFrame containing pool data
        pool_id: ID of the pool
        
    Returns:
        dict: Analysis results for this pool
    """
    pool_fields = set(pool_data.columns)
    
    # Check required fields
    missing_fields = get_missing_fields(REQUIRED_FIELDS, pool_fields)
    
    # Group missing fields by type
    missing_by_category = {}
    all_fields_by_prefix = {}
    
    # Categorize missing fields
    for field in missing_fields:
        prefix = field.split('.')[0] if '.' in field else field
        category = next((cat for cat in ['market', 'price', 'holder', 'volume', 'buy', 'trade_last5Seconds', 'trade_last10Seconds', 'poolAddress', 'timeFromStart']
                        if cat.lower() in prefix.lower()), 'other')
        
        if category not in missing_by_category:
            missing_by_category[category] = []
        missing_by_category[category].append(field)
    
    # Analyze all fields by prefix
    for field in pool_fields:
        prefix = field.split('.')[0] if '.' in field else field
        if prefix not in all_fields_by_prefix:
            all_fields_by_prefix[prefix] = []
        all_fields_by_prefix[prefix].append(field)
    
    # Check for NaN values in fields
    nan_fields = {}
    for field in pool_fields:
        nan_count = pool_data[field].isna().sum()
        if nan_count > 0:
            nan_pct = (nan_count / len(pool_data)) * 100
            nan_fields[field] = {
                "count": int(nan_count),
                "percentage": float(nan_pct)
            }
    
    # Identify fields that could be fixed by adding mapping
    potential_mapping_fixes = []
    for field in missing_fields:
        # Check if the field exists with an alternative naming convention
        alt_field = FIELD_NAME_MAPPING.get(field)
        if alt_field and alt_field in pool_fields:
            potential_mapping_fixes.append({
                "missing_field": field,
                "alternative_field": alt_field
            })
    
    # Identify if there are any parent fields for missing nested fields
    parent_fields_present = []
    for field in missing_fields:
        if '.' in field:
            parent = field.split('.')[0]
            if parent in pool_fields or any(f.startswith(parent + '.') for f in pool_fields):
                parent_fields_present.append({
                    "missing_field": field,
                    "parent_field": parent
                })
    
    # Check if the pool has alternative structure for trade data
    alternative_trade_structure = False
    if any('tradeLast5Seconds' in field for field in pool_fields) or any('tradeLast10Seconds' in field for field in pool_fields):
        alternative_trade_structure = True
    
    return {
        "pool_id": pool_id,
        "record_count": len(pool_data),
        "field_count": len(pool_fields),
        "missing_field_count": len(missing_fields),
        "missing_fields": list(missing_fields),
        "missing_by_category": missing_by_category,
        "all_fields_by_prefix": all_fields_by_prefix,
        "nan_fields": nan_fields,
        "potential_mapping_fixes": potential_mapping_fixes,
        "parent_fields_present": parent_fields_present,
        "alternative_trade_structure": alternative_trade_structure
    }

def analyze_invalid_pools(invalid_pool_ids, max_pools=None, limit_per_pool=None):
    """
    Analyze invalid pools to identify issues
    
    Args:
        invalid_pool_ids (list): List of invalid pool IDs to analyze
        max_pools (int, optional): Maximum number of pools to analyze
        limit_per_pool (int, optional): Maximum number of rows to fetch per pool
        
    Returns:
        dict: Analysis results
    """
    # Initialize Firebase
    logger.info("Initializing Firebase connection...")
    firebase_service = FirebaseService()
    db = firebase_service.db

    if not db:
        logger.error("Failed to establish Firebase connection. Exiting.")
        return {"status": "error", "message": "Failed to establish Firebase connection"}

    # Limit the number of pools if specified
    if max_pools is not None and max_pools < len(invalid_pool_ids):
        logger.info(f"Limiting analysis to {max_pools} out of {len(invalid_pool_ids)} invalid pools")
        pool_ids_to_analyze = invalid_pool_ids[:max_pools]
    else:
        pool_ids_to_analyze = invalid_pool_ids
    
    # Initialize results
    pool_analyses = []
    all_missing_fields = Counter()
    fields_with_nan = Counter()
    potential_fixes = Counter()
    parent_fields = Counter()
    
    # Process each pool
    for i, pool_id in enumerate(pool_ids_to_analyze):
        logger.info(f"Analyzing pool {i+1}/{len(pool_ids_to_analyze)}: {pool_id}")
        
        # Fetch data for this pool (limit the number of rows per pool if specified)
        limit = limit_per_pool if limit_per_pool else 100  # Use a reasonable default if not specified
        
        pool_data = firebase_service.fetch_market_data(
            min_data_points=1, max_pools=1, limit_per_pool=limit, pool_address=pool_id
        ).get(pool_id)
        
        # Only process if we get some data
        if pool_data is not None and not pool_data.empty:
            # Analyze pool fields
            analysis = analyze_pool_fields(pool_data, pool_id)
            pool_analyses.append(analysis)
            
            # Update counters
            for field in analysis["missing_fields"]:
                all_missing_fields[field] += 1
            
            for field in analysis["nan_fields"]:
                fields_with_nan[field] += 1
            
            for fix in analysis["potential_mapping_fixes"]:
                potential_fixes[fix["missing_field"]] += 1
            
            for parent in analysis["parent_fields_present"]:
                parent_fields[parent["parent_field"]] += 1
            
        elif pool_data is None:
            logger.info(f"✗ Pool {pool_id} - Could not fetch data")
        else:
            logger.info(f"✗ Pool {pool_id} - No data (empty DataFrame)")
    
    # Compile results
    most_common_missing = all_missing_fields.most_common()
    most_common_nan = fields_with_nan.most_common()
    most_common_potential_fixes = potential_fixes.most_common()
    most_common_parent_fields = parent_fields.most_common()
    
    # Check for common patterns
    pattern_results = {}
    
    # Check if pools are consistently missing the same fields
    consistent_missing = []
    for field, count in most_common_missing:
        if count == len(pool_analyses):  # Field is missing from all analyzed pools
            consistent_missing.append(field)
    
    pattern_results["consistent_missing_fields"] = consistent_missing
    
    # Check if most pools have alternative trade structure
    alt_trade_structure_count = sum(1 for analysis in pool_analyses if analysis["alternative_trade_structure"])
    pattern_results["alternative_trade_structure"] = {
        "count": alt_trade_structure_count,
        "percentage": (alt_trade_structure_count / len(pool_analyses)) * 100 if pool_analyses else 0
    }
    
    # Compile final results
    results = {
        "status": "success",
        "total_pools_analyzed": len(pool_analyses),
        "missing_fields_analysis": {
            "missing_fields_count": dict(all_missing_fields),
            "most_common_missing": [{"field": field, "count": count} for field, count in most_common_missing]
        },
        "nan_fields_analysis": {
            "nan_fields_count": dict(fields_with_nan),
            "most_common_nan": [{"field": field, "count": count} for field, count in most_common_nan]
        },
        "potential_fixes": {
            "mapping_fixes_count": dict(potential_fixes),
            "most_common_fixes": [{"field": field, "count": count} for field, count in most_common_potential_fixes]
        },
        "parent_fields_analysis": {
            "parent_fields_count": dict(parent_fields),
            "most_common_parent_fields": [{"field": field, "count": count} for field, count in most_common_parent_fields]
        },
        "pattern_analysis": pattern_results,
        "pool_analyses": pool_analyses
    }
    
    return results

def main():
    """Analyze invalid pools for specific issues"""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Analyze invalid pools for specific issues')
    parser.add_argument('--input-file', required=True, help='JSON file containing invalid pool IDs')
    parser.add_argument('--output-dir', help='Directory to save analysis results (default: outputs/invalid_pools_analysis_TIMESTAMP)')
    parser.add_argument('--max-pools', type=int, help='Maximum number of pools to analyze')
    parser.add_argument('--limit-per-pool', type=int, help='Maximum number of rows to fetch per pool', default=100)
    args = parser.parse_args()
    
    # Load invalid pool IDs from input file
    try:
        with open(args.input_file, 'r') as f:
            invalid_pool_ids = json.load(f)
            
        logger.info(f"Loaded {len(invalid_pool_ids)} invalid pool IDs from {args.input_file}")
    except Exception as e:
        logger.error(f"Failed to load invalid pool IDs from {args.input_file}: {str(e)}")
        sys.exit(1)
    
    # Create output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
        # Create timestamp for subfolder
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = os.path.join(project_root, "outputs", f"invalid_pools_analysis_{timestamp}")
    
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Results will be saved to directory: {output_dir}")
    
    # Analyze invalid pools
    results = analyze_invalid_pools(invalid_pool_ids, args.max_pools, args.limit_per_pool)
    
    # Save results
    analysis_file = os.path.join(output_dir, "invalid_pools_analysis.json")
    with open(analysis_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    # Save missing fields analysis separately for easier access
    missing_fields_file = os.path.join(output_dir, "missing_fields_analysis.json")
    with open(missing_fields_file, 'w') as f:
        json.dump(results["missing_fields_analysis"], f, indent=2)
    
    # Save potential fixes separately
    potential_fixes_file = os.path.join(output_dir, "potential_fixes.json")
    with open(potential_fixes_file, 'w') as f:
        json.dump(results["potential_fixes"], f, indent=2)
    
    # Print summary
    print("\n" + "=" * 80)
    print("INVALID POOLS ANALYSIS RESULTS")
    print("=" * 80)
    print(f"Analyzed {results['total_pools_analyzed']} out of {len(invalid_pool_ids)} invalid pools")
    
    # Print most common missing fields
    print("\nMost common missing fields:")
    for item in results["missing_fields_analysis"]["most_common_missing"][:10]:
        print(f"- {item['field']}: missing from {item['count']} pools ({item['count']/results['total_pools_analyzed']*100:.1f}%)")
    
    # Print fields with NaN values
    if results["nan_fields_analysis"]["most_common_nan"]:
        print("\nFields most commonly containing NaN values:")
        for item in results["nan_fields_analysis"]["most_common_nan"][:10]:
            print(f"- {item['field']}: NaN in {item['count']} pools ({item['count']/results['total_pools_analyzed']*100:.1f}%)")
    
    # Print potential fixes
    if results["potential_fixes"]["most_common_fixes"]:
        print("\nPotential fixes through field mapping:")
        for item in results["potential_fixes"]["most_common_fixes"][:10]:
            print(f"- {item['field']}: fixable in {item['count']} pools ({item['count']/results['total_pools_analyzed']*100:.1f}%)")
    
    # Print parent fields analysis
    if results["parent_fields_analysis"]["most_common_parent_fields"]:
        print("\nParent fields present for missing nested fields:")
        for item in results["parent_fields_analysis"]["most_common_parent_fields"][:10]:
            print(f"- {item['field']}: present in {item['count']} pools ({item['count']/results['total_pools_analyzed']*100:.1f}%)")
    
    # Print pattern analysis
    print("\nPattern analysis:")
    
    # Consistent missing fields
    if results["pattern_analysis"]["consistent_missing_fields"]:
        print(f"- {len(results['pattern_analysis']['consistent_missing_fields'])} fields consistently missing from all analyzed pools:")
        for field in results["pattern_analysis"]["consistent_missing_fields"][:10]:  # Show up to 10
            print(f"  - {field}")
        if len(results["pattern_analysis"]["consistent_missing_fields"]) > 10:
            print(f"  - ... and {len(results['pattern_analysis']['consistent_missing_fields']) - 10} more")
    else:
        print("- No fields consistently missing from all pools")
    
    # Alternative trade structure
    alt_trade = results["pattern_analysis"]["alternative_trade_structure"]
    print(f"- Alternative trade structure: found in {alt_trade['count']} pools ({alt_trade['percentage']:.1f}%)")
    
    print("\nResults saved to files:")
    print(f"- {analysis_file}")
    print(f"- {missing_fields_file}")
    print(f"- {potential_fixes_file}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user.")
    except Exception as e:
        logger.exception(f"Error in analysis: {str(e)}")
