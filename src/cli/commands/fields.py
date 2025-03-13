"""
Field inspection commands for the CLI.

This module provides commands for inspecting field naming conventions and
availability across pools in Firebase and the local cache.
"""

import json
import logging
import pandas as pd
from pathlib import Path
from tabulate import tabulate

from src.data.cache_service import DataCacheService
from src.data.firebase_service import FirebaseService
from src.analysis.pool_analyzer import REQUIRED_FIELDS
from src.utils.firebase_utils import get_pool_ids

logger = logging.getLogger(__name__)


def add_fields_subparser(subparsers):
    """Add field inspection commands to the CLI."""
    fields_parser = subparsers.add_parser("fields", help="Inspect field naming conventions and availability")

    fields_subparsers = fields_parser.add_subparsers(dest="subcommand", help="Field subcommand")

    # Inspect fields command
    inspect_parser = fields_subparsers.add_parser("inspect", help="Inspect fields from Firebase or cache")
    inspect_parser.add_argument("--pool", type=str, help="Specific pool ID to inspect")
    inspect_parser.add_argument("--random", action="store_true", help="Select a random pool to inspect")
    inspect_parser.add_argument(
        "--from-cache", action="store_true", help="Inspect fields from cache instead of Firebase"
    )
    inspect_parser.add_argument(
        "--db-path",
        type=str,
        default="cache/pools.db",
        help="Path to the SQLite database file (default: cache/pools.db)",
    )
    inspect_parser.add_argument("--output-json", action="store_true", help="Output results as JSON file")
    inspect_parser.add_argument("--output-dir", type=str, default="outputs", help="Directory to save output files")

    # Count fields command
    count_parser = fields_subparsers.add_parser("count", help="Count available fields across pools")
    count_parser.add_argument("--limit", type=int, default=10, help="Maximum number of pools to analyze (default: 10)")
    count_parser.add_argument("--from-cache", action="store_true", help="Count fields from cache instead of Firebase")
    count_parser.add_argument(
        "--db-path",
        type=str,
        default="cache/pools.db",
        help="Path to the SQLite database file (default: cache/pools.db)",
    )

    # Compare fields command
    compare_parser = fields_subparsers.add_parser(
        "compare", help="Compare fields between SQL schema and REQUIRED_FIELDS"
    )
    compare_parser.add_argument(
        "--db-path",
        type=str,
        default="cache/pools.db",
        help="Path to the SQLite database file (default: cache/pools.db)",
    )


def inspect_fields_from_firebase(pool_id: str, output_json: bool = False, output_dir: str = "outputs") -> int:
    """
    Inspect fields from a specific pool in Firebase.

    Args:
        pool_id: The pool ID to inspect
        output_json: Whether to output results as JSON
        output_dir: Directory to save output files

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    logger.info(f"Inspecting fields from Firebase for pool {pool_id}")

    try:
        # Initialize Firebase service
        firebase_service = FirebaseService()

        # Fetch data for the pool
        pool_data = firebase_service.fetch_pool_data(pool_id)

        if pool_data.empty:
            logger.error(f"No data found for pool {pool_id}")
            return 1

        # Process and display field information
        return process_field_info(pool_id, pool_data, output_json, output_dir)

    except Exception as e:
        logger.error(f"Error inspecting fields from Firebase: {str(e)}")
        return 1


def inspect_fields_from_cache(
    pool_id: str, db_path: str, output_json: bool = False, output_dir: str = "outputs"
) -> int:
    """
    Inspect fields from a specific pool in the cache.

    Args:
        pool_id: The pool ID to inspect
        db_path: Path to the SQLite database
        output_json: Whether to output results as JSON
        output_dir: Directory to save output files

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    logger.info(f"Inspecting fields from cache for pool {pool_id}")

    try:
        # Initialize cache service
        cache_service = DataCacheService(db_path=db_path)

        # Fetch data for the pool
        pool_data = cache_service.get_pool_data(pool_id)

        if pool_data.empty:
            logger.error(f"No data found for pool {pool_id} in cache")
            return 1

        # Process and display field information
        return process_field_info(pool_id, pool_data, output_json, output_dir)

    except Exception as e:
        logger.error(f"Error inspecting fields from cache: {str(e)}")
        return 1


def process_field_info(
    pool_id: str, pool_data: pd.DataFrame, output_json: bool = False, output_dir: str = "outputs"
) -> int:
    """
    Process and display field information for a pool.

    Args:
        pool_id: The pool ID
        pool_data: DataFrame containing pool data
        output_json: Whether to output results as JSON
        output_dir: Directory to save output files

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        # Get the first data point to analyze fields
        sample_row = pool_data.iloc[0].to_dict()

        # Flatten any nested dictionaries
        flattened_fields = {}

        def flatten_dict(d, parent_key=""):
            for k, v in d.items():
                new_key = f"{parent_key}{k}" if parent_key else k
                if isinstance(v, dict):
                    flatten_dict(v, f"{new_key}.")
                else:
                    flattened_fields[new_key] = v

        flatten_dict(sample_row)

        # Compare with REQUIRED_FIELDS
        available_fields = set(flattened_fields.keys())
        required_fields_set = set(REQUIRED_FIELDS)

        available_required = available_fields.intersection(required_fields_set)
        missing_required = required_fields_set - available_fields
        extra_fields = available_fields - required_fields_set

        # Count naming conventions
        snake_case_count = sum(1 for field in available_fields if "_" in field)
        camel_case_count = sum(1 for field in available_fields if "_" not in field and any(c.isupper() for c in field))
        other_count = len(available_fields) - snake_case_count - camel_case_count

        # Print results
        print("\n" + "=" * 80)
        print(f"FIELD ANALYSIS FOR POOL: {pool_id}")
        print("=" * 80)

        print(f"\nTotal fields found: {len(available_fields)}")
        print(
            f"Required fields available: {len(available_required)}/{len(REQUIRED_FIELDS)} ({len(available_required)/len(REQUIRED_FIELDS)*100:.1f}%)"
        )

        print("\nNaming conventions:")
        print(f"  snake_case fields: {snake_case_count}")
        print(f"  camelCase fields: {camel_case_count}")
        print(f"  Other fields: {other_count}")

        if missing_required:
            print(f"\nMissing required fields ({len(missing_required)}):")
            for field in sorted(missing_required):
                print(f"  - {field}")

        print(f"\nAll available fields ({len(available_fields)}):")
        field_data = []
        for field in sorted(available_fields):
            field_type = type(flattened_fields[field]).__name__
            value = str(flattened_fields[field])
            if len(value) > 30:
                value = value[:27] + "..."
            is_required = "Yes" if field in required_fields_set else "No"
            naming = "snake_case" if "_" in field else "camelCase" if any(c.isupper() for c in field) else "other"
            field_data.append([field, field_type, value, is_required, naming])

        print(tabulate(field_data, headers=["Field", "Type", "Value", "Required", "Naming Convention"]))

        # If requested, save results to a JSON file
        if output_json:
            output_dir_path = Path(output_dir)
            output_dir_path.mkdir(exist_ok=True)

            result = {
                "pool_id": pool_id,
                "total_fields": len(available_fields),
                "required_fields_count": len(REQUIRED_FIELDS),
                "available_required_count": len(available_required),
                "available_required_percent": len(available_required) / len(REQUIRED_FIELDS) * 100,
                "naming_conventions": {
                    "snake_case": snake_case_count,
                    "camel_case": camel_case_count,
                    "other": other_count,
                },
                "available_fields": sorted(list(available_fields)),
                "missing_required": sorted(list(missing_required)),
                "extra_fields": sorted(list(extra_fields)),
            }

            output_file = output_dir_path / f"field_inspection_{pool_id}.json"
            with open(output_file, "w") as f:
                json.dump(result, f, indent=2)

            print(f"\nResults saved to {output_file}")

        return 0

    except Exception as e:
        logger.error(f"Error processing field information: {str(e)}")
        return 1


def count_fields_across_pools(limit: int = 10, from_cache: bool = False, db_path: str = "cache/pools.db") -> int:
    """
    Count field availability across multiple pools.

    Args:
        limit: Maximum number of pools to analyze
        from_cache: Whether to use the cache instead of Firebase
        db_path: Path to the SQLite database

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    cache_or_firebase = "cache" if from_cache else "Firebase"
    logger.info(f"Counting fields across {limit} pools (from {cache_or_firebase})")

    try:
        field_counts = {}
        pools_analyzed = 0

        if from_cache:
            # Initialize cache service
            cache_service = DataCacheService(db_path=db_path)

            # Get pool IDs from cache
            pool_ids = cache_service.get_pool_ids(limit=limit)

            for pool_id in pool_ids:
                # Fetch data for the pool
                pool_data = cache_service.get_pool_data(pool_id)

                if not pool_data.empty:
                    # Count fields in this pool
                    fields = pool_data.columns.tolist()
                    for field in fields:
                        field_counts[field] = field_counts.get(field, 0) + 1

                    pools_analyzed += 1
                    logger.info(f"Analyzed pool {pools_analyzed}/{limit}: {pool_id}")
        else:
            # Initialize Firebase service
            firebase_service = FirebaseService()

            # Get pool IDs from Firebase
            pool_ids = get_pool_ids(firebase_service, limit=limit)

            for pool_id in pool_ids:
                # Fetch data for the pool
                pool_data = firebase_service.fetch_pool_data(pool_id)

                if not pool_data.empty:
                    # Count fields in this pool
                    fields = pool_data.columns.tolist()
                    for field in fields:
                        field_counts[field] = field_counts.get(field, 0) + 1

                    pools_analyzed += 1
                    logger.info(f"Analyzed pool {pools_analyzed}/{limit}: {pool_id}")

        # Print results
        print("\n" + "=" * 80)
        print(f"FIELD AVAILABILITY ACROSS {pools_analyzed} POOLS")
        print("=" * 80)

        # Sort fields by frequency
        sorted_fields = sorted(field_counts.items(), key=lambda x: x[1], reverse=True)

        field_data = []
        for field, count in sorted_fields:
            percent = (count / pools_analyzed) * 100 if pools_analyzed > 0 else 0
            is_required = "Yes" if field in REQUIRED_FIELDS else "No"
            naming = "snake_case" if "_" in field else "camelCase" if any(c.isupper() for c in field) else "other"
            field_data.append([field, count, f"{percent:.1f}%", is_required, naming])

        print(tabulate(field_data, headers=["Field", "Count", "Availability", "Required", "Naming Convention"]))

        return 0

    except Exception as e:
        logger.error(f"Error counting fields across pools: {str(e)}")
        return 1


def compare_sqlite_required_fields(db_path: str = "cache/pools.db") -> int:
    """
    Compare fields between SQLite schema and REQUIRED_FIELDS.

    Args:
        db_path: Path to the SQLite database

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    logger.info("Comparing SQLite schema fields with REQUIRED_FIELDS")

    try:
        # Get SQLite schema
        import sqlite3

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(market_data)")
        schema_fields = [row[1] for row in cursor.fetchall()]
        conn.close()

        # Fields to ignore in comparison (JSON containers)
        ignore_fields = ["trade_data", "additional_data"]

        # Clean schema fields
        clean_schema_fields = [field for field in schema_fields if field not in ignore_fields]

        # Extract trade_data fields from REQUIRED_FIELDS
        trade_fields = [field for field in REQUIRED_FIELDS if field.startswith("trade_")]

        # Handle special case for poolAddress vs pool_id
        if "pool_id" in clean_schema_fields and "poolAddress" in REQUIRED_FIELDS:
            # Map between them for comparison
            clean_schema_fields.append("poolAddress")

        # Get overlapping and missing fields
        schema_set = set(clean_schema_fields)
        required_set = set(REQUIRED_FIELDS) - set(trade_fields)  # Exclude trade fields as they're in JSON

        in_both = schema_set.intersection(required_set)
        only_in_schema = schema_set - required_set
        only_in_required = required_set - schema_set

        # Print results
        print("\n" + "=" * 80)
        print("COMPARISON: SQLite SCHEMA vs REQUIRED_FIELDS")
        print("=" * 80)

        print("\nSummary:")
        print(f"  SQLite schema fields: {len(clean_schema_fields)}")
        print(f"  Required fields: {len(REQUIRED_FIELDS)}")
        print(f"  Fields stored in JSON: {len(trade_fields)}")
        print(f"  Fields in both: {len(in_both)}")
        print(f"  Fields only in schema: {len(only_in_schema)}")
        print(f"  Fields only in REQUIRED_FIELDS: {len(only_in_required)}")

        print("\nFields only in schema:")
        for field in sorted(only_in_schema):
            print(f"  - {field}")

        print("\nFields only in REQUIRED_FIELDS:")
        for field in sorted(only_in_required):
            print(f"  - {field}")

        print("\nFields in both:")
        for field in sorted(in_both):
            print(f"  - {field}")

        print("\nFields stored in JSON (trade_data):")
        for field in sorted(trade_fields):
            print(f"  - {field}")

        return 0

    except Exception as e:
        logger.error(f"Error comparing SQLite schema with REQUIRED_FIELDS: {str(e)}")
        return 1


def handle_fields_command(args) -> int:
    """
    Handle the fields command.

    Args:
        args: Command line arguments

    Returns:
        int: Exit code (0 for success, non-zero for error)
    """
    if not args.subcommand:
        logger.error("No subcommand specified")
        return 1

    if args.subcommand == "inspect":
        # If no pool specified and random not set, show error
        if not args.pool and not args.random:
            logger.error("Either --pool or --random must be specified")
            return 1

        # If random, get a random pool ID
        if args.random:
            try:
                pool_ids = []
                if args.from_cache:
                    cache_svc = DataCacheService(db_path=args.db_path)
                    pool_ids = cache_svc.get_pool_ids(limit=10)
                else:
                    firebase_service = FirebaseService()
                    pool_ids = get_pool_ids(firebase_service, limit=10)

                if not pool_ids:
                    logger.error("No pools found")
                    return 1

                import random

                pool_id = random.choice(pool_ids)
                logger.info(f"Randomly selected pool: {pool_id}")
            except Exception as e:
                logger.error(f"Error selecting random pool: {str(e)}")
                return 1
        else:
            pool_id = args.pool

        # Inspect from cache or Firebase
        if args.from_cache:
            return inspect_fields_from_cache(pool_id, args.db_path, args.output_json, args.output_dir)
        else:
            return inspect_fields_from_firebase(pool_id, args.output_json, args.output_dir)

    elif args.subcommand == "count":
        return count_fields_across_pools(args.limit, args.from_cache, args.db_path)

    elif args.subcommand == "compare":
        return compare_sqlite_required_fields(args.db_path)

    else:
        logger.error(f"Unknown subcommand: {args.subcommand}")
        return 1
