#!/usr/bin/env python
"""
Field Naming Convention Audit

This script audits the naming conventions used in pool data from Firebase,
detecting mixed naming conventions and analyzing field availability.

It helps ensure consistency in field naming across the data pipeline.
"""

import sys
import json
import logging
import argparse
from collections import Counter, defaultdict
from typing import Dict, List, Any
from datetime import datetime

# Import utilities
from src.utils.field_utils import normalize_field_name, get_field_value

# Import required fields list
from src.analysis.pool_analyzer import REQUIRED_FIELDS

# Import services
from src.data.firebase_service import FirebaseService
from src.utils.firebase_utils import get_pool_ids

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def analyze_naming_conventions(pool_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Analyze naming conventions used in pool data.

    Args:
        pool_data: Dictionary containing pool data

    Returns:
        Dictionary with naming convention analysis results
    """
    results = {
        "snake_case_count": 0,
        "camel_case_count": 0,
        "mixed_case_count": 0,
        "unknown_case_count": 0,
        "fields_by_convention": defaultdict(list),
        "field_variants": defaultdict(set),
    }

    # Analyze each field
    for field in pool_data.keys():
        # Skip nested fields for now (they'll be flattened later)
        if isinstance(pool_data[field], dict):
            continue

        # Determine naming convention
        if "_" in field:
            convention = "snake_case"
            results["snake_case_count"] += 1
            normalized = normalize_field_name(field, target_convention="snake")
        elif field[0].islower() and any(c.isupper() for c in field):
            convention = "camelCase"
            results["camel_case_count"] += 1
            normalized = normalize_field_name(field, target_convention="snake")
        else:
            # Could be all lowercase, all uppercase, or something else
            convention = "unknown"
            results["unknown_case_count"] += 1
            normalized = field

        # Add field to results by convention
        results["fields_by_convention"][convention].append(field)

        # Track variants of the same semantic field
        if normalized != field:
            results["field_variants"][normalized].add(field)

    # Count mixed conventions
    results["mixed_case_count"] = 1 if results["snake_case_count"] > 0 and results["camel_case_count"] > 0 else 0

    return results


def check_required_fields(pool_data: Dict[str, Any], required_fields: List[str]) -> Dict[str, Any]:
    """
    Check if pool data has all required fields.

    Args:
        pool_data: Dictionary containing pool data
        required_fields: List of required field names

    Returns:
        Dictionary with field availability analysis
    """
    results = {
        "available_fields": [],
        "missing_fields": [],
        "total_required": len(required_fields),
        "available_count": 0,
        "available_percent": 0.0,
    }

    # Check each required field
    for field in required_fields:
        value = get_field_value(pool_data, field)

        if value is not None:
            results["available_fields"].append(field)
            results["available_count"] += 1
        else:
            results["missing_fields"].append(field)

    # Calculate percentage of available fields
    results["available_percent"] = (results["available_count"] / results["total_required"]) * 100

    return results


def flatten_nested_fields(pool_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten nested fields in pool data.

    Args:
        pool_data: Dictionary containing pool data

    Returns:
        Dictionary with flattened fields
    """
    flattened = {}

    def _flatten(data, prefix=""):
        for key, value in data.items():
            if isinstance(value, dict):
                _flatten(value, f"{prefix}{key}.")
            else:
                flattened[f"{prefix}{key}"] = value

    _flatten(pool_data)
    return flattened


def audit_pool_fields(pool_ids: List[str], firebase_service: FirebaseService, limit: int = 100) -> Dict[str, Any]:
    """
    Audit field naming conventions and availability across pools.

    Args:
        pool_ids: List of pool IDs to audit
        firebase_service: Firebase service instance
        limit: Maximum number of pools to audit

    Returns:
        Dictionary with audit results
    """
    results = {
        "pools_analyzed": 0,
        "fields_by_pool": {},
        "field_availability": Counter(),
        "naming_conventions": {
            "pools_with_mixed_conventions": 0,
            "camel_case_fields": set(),
            "snake_case_fields": set(),
            "unknown_case_fields": set(),
            "field_variants": defaultdict(set),
        },
        "required_fields": {
            "pools_with_all_required": 0,
            "field_availability": Counter(),
        },
    }

    # Limit number of pools
    pool_ids = pool_ids[:limit]
    total_pools = len(pool_ids)

    # Analyze each pool
    for i, pool_id in enumerate(pool_ids):
        logger.info(f"Analyzing pool {i+1}/{total_pools}: {pool_id}")

        try:
            # Get pool data
            pool_data = firebase_service.fetch_pool_data_as_dict(pool_id)

            if not pool_data:
                logger.warning(f"No data found for pool {pool_id}")
                continue

            # Take the latest data point
            if isinstance(pool_data, list):
                latest_data = pool_data[-1]
            else:
                latest_data = pool_data

            # Flatten nested fields
            flat_data = flatten_nested_fields(latest_data)

            # Analyze naming conventions
            naming_results = analyze_naming_conventions(flat_data)

            # Check required fields
            required_results = check_required_fields(flat_data, REQUIRED_FIELDS)

            # Update results
            results["pools_analyzed"] += 1

            # Track fields per pool
            results["fields_by_pool"][pool_id] = list(flat_data.keys())

            # Track field availability
            for field in flat_data.keys():
                results["field_availability"][field] += 1

            # Track naming conventions
            if naming_results["mixed_case_count"] > 0:
                results["naming_conventions"]["pools_with_mixed_conventions"] += 1

            for field in naming_results["fields_by_convention"]["camelCase"]:
                results["naming_conventions"]["camel_case_fields"].add(field)

            for field in naming_results["fields_by_convention"]["snake_case"]:
                results["naming_conventions"]["snake_case_fields"].add(field)

            for field in naming_results["fields_by_convention"]["unknown"]:
                results["naming_conventions"]["unknown_case_fields"].add(field)

            # Track field variants
            for norm_field, variants in naming_results["field_variants"].items():
                results["naming_conventions"]["field_variants"][norm_field].update(variants)

            # Track required fields
            if required_results["available_count"] == required_results["total_required"]:
                results["required_fields"]["pools_with_all_required"] += 1

            for field in required_results["available_fields"]:
                results["required_fields"]["field_availability"][field] += 1

        except Exception as e:
            logger.error(f"Error analyzing pool {pool_id}: {str(e)}")

    # Calculate percentages
    if results["pools_analyzed"] > 0:
        results["naming_conventions"]["percent_pools_with_mixed_conventions"] = (
            results["naming_conventions"]["pools_with_mixed_conventions"] / results["pools_analyzed"]
        ) * 100

        results["required_fields"]["percent_pools_with_all_required"] = (
            results["required_fields"]["pools_with_all_required"] / results["pools_analyzed"]
        ) * 100

    # For each required field, calculate availability percentage
    for field in REQUIRED_FIELDS:
        count = results["required_fields"]["field_availability"].get(field, 0)
        percent = (count / results["pools_analyzed"]) * 100 if results["pools_analyzed"] > 0 else 0
        results["required_fields"]["field_availability"][field] = {
            "count": count,
            "percent": percent,
        }

    return results


def print_audit_report(results: Dict[str, Any]) -> None:
    """
    Print audit report to console.

    Args:
        results: Dictionary with audit results
    """
    print("\n======= FIELD NAMING CONVENTION AUDIT REPORT =======\n")

    print(f"Pools analyzed: {results['pools_analyzed']}")

    print("\n--- NAMING CONVENTIONS ---")
    print(
        f"Pools with mixed naming conventions: {results['naming_conventions']['pools_with_mixed_conventions']} ({results['naming_conventions'].get('percent_pools_with_mixed_conventions', 0):.1f}%)"
    )
    print(f"Fields using camelCase: {len(results['naming_conventions']['camel_case_fields'])}")
    print(f"Fields using snake_case: {len(results['naming_conventions']['snake_case_fields'])}")
    print(f"Fields using unknown case: {len(results['naming_conventions']['unknown_case_fields'])}")

    print("\n--- FIELD VARIANTS ---")
    print("Fields with multiple naming variants:")
    for norm_field, variants in results["naming_conventions"]["field_variants"].items():
        if len(variants) > 1:
            print(f"  {norm_field}: {sorted(variants)}")

    print("\n--- REQUIRED FIELDS AVAILABILITY ---")
    print(
        f"Pools with all required fields: {results['required_fields']['pools_with_all_required']} ({results['required_fields'].get('percent_pools_with_all_required', 0):.1f}%)"
    )

    print("\nRequired fields availability (sorted by availability):")
    sorted_fields = sorted(
        [(field, data["percent"]) for field, data in results["required_fields"]["field_availability"].items()],
        key=lambda x: x[1],
    )

    for field, percent in sorted_fields:
        count = results["required_fields"]["field_availability"][field]["count"]
        print(f"  {field}: {count} pools ({percent:.1f}%)")

    print("\n--- MOST COMMON FIELDS ---")
    most_common = results["field_availability"].most_common(20)
    print("Top 20 most common fields:")
    for field, count in most_common:
        percent = (count / results["pools_analyzed"]) * 100
        print(f"  {field}: {count} pools ({percent:.1f}%)")

    print("\n--- LEAST COMMON FIELDS ---")
    least_common = results["field_availability"].most_common()[:-21:-1]
    print("Top 20 least common fields:")
    for field, count in least_common:
        percent = (count / results["pools_analyzed"]) * 100
        print(f"  {field}: {count} pools ({percent:.1f}%)")


def export_audit_results(results: Dict[str, Any], output_path: str) -> None:
    """
    Export audit results to JSON file.

    Args:
        results: Dictionary with audit results
        output_path: Path to output file
    """
    # Convert sets to lists for JSON serialization
    export_results = {
        "pools_analyzed": results["pools_analyzed"],
        "field_availability": dict(results["field_availability"]),
        "naming_conventions": {
            "pools_with_mixed_conventions": results["naming_conventions"]["pools_with_mixed_conventions"],
            "percent_pools_with_mixed_conventions": results["naming_conventions"].get(
                "percent_pools_with_mixed_conventions", 0
            ),
            "camel_case_fields": list(results["naming_conventions"]["camel_case_fields"]),
            "snake_case_fields": list(results["naming_conventions"]["snake_case_fields"]),
            "unknown_case_fields": list(results["naming_conventions"]["unknown_case_fields"]),
            "field_variants": {k: list(v) for k, v in results["naming_conventions"]["field_variants"].items()},
        },
        "required_fields": {
            "pools_with_all_required": results["required_fields"]["pools_with_all_required"],
            "percent_pools_with_all_required": results["required_fields"].get("percent_pools_with_all_required", 0),
            "field_availability": results["required_fields"]["field_availability"],
        },
        "export_timestamp": datetime.now().isoformat(),
    }

    with open(output_path, "w") as f:
        json.dump(export_results, f, indent=2)

    logger.info(f"Audit results exported to {output_path}")


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Audit field naming conventions in pool data")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of pools to audit")
    parser.add_argument("--output", type=str, default="field_audit_results.json", help="Output file path")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    # Configure logging
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        # Initialize Firebase service
        firebase_service = FirebaseService()

        # Get available pool IDs
        logger.info("Getting available pool IDs...")
        pool_ids = get_pool_ids(firebase_service)

        if not pool_ids:
            logger.error("No pools found in Firebase")
            return 1

        logger.info(f"Found {len(pool_ids)} pools in Firebase")

        # Audit pool fields
        logger.info(f"Auditing field naming conventions (limit: {args.limit} pools)...")
        results = audit_pool_fields(pool_ids, firebase_service, limit=args.limit)

        # Print report
        print_audit_report(results)

        # Export results
        export_audit_results(results, args.output)

        return 0

    except Exception as e:
        logger.error(f"Error during audit: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
