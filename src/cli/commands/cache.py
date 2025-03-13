"""
Cache commands for CLI interface.

This module provides CLI commands for managing the data cache,
including updating pools, clearing the cache, and showing cache status.
All field names use camelCase to match the REQUIRED_FIELDS from pool_analyzer.py.
"""

import argparse
import logging
from pathlib import Path
from typing import List, Optional

# Import service
from src.data.cache_service import DataCacheService
from src.data.firebase_service import FirebaseService

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def add_cache_subparser(subparsers):
    """Add the cache command and related subcommands to the CLI."""
    # Create cache command
    cache_parser = subparsers.add_parser("cache", help="Manage data cache")
    cache_subparsers = cache_parser.add_subparsers(dest="cache_command", help="Cache command to execute")

    # Update command
    update_parser = cache_subparsers.add_parser("update", help="Update cache")
    update_parser.add_argument(
        "--pools", "-p", nargs="+", help="Specific pools to update (if omitted, updates all pools)"
    )
    update_parser.add_argument("--recent", "-r", action="store_true", help="Update only recently active pools")
    update_parser.add_argument("--min-points", "-m", type=int, default=0, help="Minimum data points required in cache")

    # Clear command
    clear_parser = cache_subparsers.add_parser("clear", help="Clear cache")
    clear_parser.add_argument("--days", "-d", type=int, help="Clear data older than specified days")

    # Status command
    cache_subparsers.add_parser("status", help="Show cache status")

    # Backup command
    backup_parser = cache_subparsers.add_parser("backup", help="Create a backup of the cache")
    backup_parser.add_argument(
        "--output", "-o", help="Output path for backup (if omitted, creates in default location)"
    )

    # Set cache_command as the handler function
    cache_parser.set_defaults(func=handle_cache_command)

    return cache_parser


def update_all_pools(cache_service: DataCacheService, firebase_service: FirebaseService) -> bool:
    """Update all available pools in cache."""
    # Get all pool IDs from Firebase
    pool_ids = firebase_service.get_available_pools(limit=1000)

    if not pool_ids:
        logger.error("No pools found in Firebase")
        return False

    logger.info(f"Found {len(pool_ids)} pools in Firebase")

    success_count = 0
    error_count = 0

    # Update each pool
    for pool_id in pool_ids:
        try:
            # Fetch data from Firebase
            df = firebase_service.fetch_pool_data(pool_id)

            if df.empty:
                logger.warning(f"No data found for pool {pool_id}")
                continue

            # Update cache
            success = cache_service.update_pool_data(pool_id, df)

            if success:
                success_count += 1
            else:
                error_count += 1

        except Exception as e:
            logger.error(f"Error updating pool {pool_id}: {e}")
            error_count += 1

    logger.info(f"Cache update completed: {success_count} pools updated, {error_count} errors")
    return success_count > 0


def update_specific_pools(
    cache_service: DataCacheService, firebase_service: FirebaseService, pool_ids: List[str], min_data_points: int = 0
) -> bool:
    """Update specific pools."""
    if not pool_ids:
        logger.error("No pool IDs provided")
        return False

    logger.info(f"Updating {len(pool_ids)} specific pools with min data points: {min_data_points}")

    success_count = 0
    error_count = 0
    no_data_count = 0

    # Update each pool
    for pool_id in pool_ids:
        try:
            # Check if pool meets minimum data points requirement
            if min_data_points > 0:
                pool_data = cache_service.get_pool_data(pool_id)
                if len(pool_data) < min_data_points:
                    logger.info(f"Pool {pool_id} has insufficient data points ({len(pool_data)} < {min_data_points})")
                    no_data_count += 1
                    continue

            # Fetch data from Firebase
            df = firebase_service.fetch_pool_data(pool_id)

            if df.empty:
                logger.warning(f"No data found for pool {pool_id}")
                no_data_count += 1
                continue

            # Update cache
            success = cache_service.update_pool_data(pool_id, df)

            if success:
                success_count += 1
            else:
                error_count += 1

        except Exception as e:
            logger.error(f"Error updating pool {pool_id}: {e}")
            error_count += 1

    logger.info(
        f"Cache update completed: {success_count} pools updated, "
        f"{error_count} errors, {no_data_count} without sufficient data"
    )
    return success_count > 0


def update_recent_pools(cache_service: DataCacheService, firebase_service: FirebaseService) -> bool:
    """Update recently active pools."""
    # Get recent market data from Firebase (last 24 hours)
    recent_data = firebase_service.fetch_recent_market_data(hours_back=24, max_pools=100)

    if not recent_data:
        logger.error("No recent pools found in Firebase")
        return False

    logger.info(f"Found {len(recent_data)} recent pools in Firebase")

    # Extract pool IDs from the data
    pool_ids = list(recent_data.keys())

    # Use the specific pools update function
    return update_specific_pools(cache_service, firebase_service, pool_ids)


def clear_entire_cache(cache_service: DataCacheService) -> bool:
    """Clear the entire cache."""
    logger.info("Clearing entire cache")
    return cache_service.clear_cache()


def clear_old_data(cache_service: DataCacheService, days: int) -> bool:
    """Clear data older than the specified number of days."""
    logger.info(f"Clearing data older than {days} days")
    return cache_service.clear_cache(older_than_days=days)


def show_cache_status(cache_service: DataCacheService) -> bool:
    """Show the current cache status."""
    stats = cache_service.get_cache_stats()

    if stats.get("status") != "success":
        logger.error(f"Failed to get cache stats: {stats.get('message', 'Unknown error')}")
        return False

    data = stats.get("data", {})

    print("\nCache Status:")
    print(f"  Database path: {data.get('database_path')}")
    print(f"  Last update: {data.get('last_update')}")
    print(f"  Total pools: {data.get('total_pools')}")
    print(f"  Total data points: {data.get('total_data_points')}")
    print(f"  Cache size: {data.get('cache_size_mb', 0):.2f} MB")

    memory_cache = data.get("memory_cache", {})
    print(f"  Memory cache: {memory_cache.get('size', 0)} / {memory_cache.get('max_size', 0)} pools")

    # Show largest pools
    largest_pools = data.get("largest_pools", [])
    if largest_pools:
        print("\nLargest pools in cache:")
        for pool in largest_pools:
            print(f"  {pool.get('pool_id')}: {pool.get('data_points')} data points")

    return True


def backup_cache(cache_service: DataCacheService, output_path: Optional[str] = None) -> bool:
    """Create a backup of the cache."""
    logger.info(f"Creating cache backup{' to ' + output_path if output_path else ''}")
    success, backup_path = cache_service.backup_database(output_path)

    if success:
        print(f"Cache backup created at: {backup_path}")
    else:
        logger.error(f"Failed to create backup: {backup_path}")

    return success


def handle_cache_command(args: argparse.Namespace):
    """Handle cache commands based on arguments."""
    # Get cache directory
    cache_dir = Path(__file__).parent.parent.parent.parent / "cache"
    cache_dir.mkdir(exist_ok=True)

    # Create cache service
    db_path = cache_dir / "pools.db"
    schema_path = Path(__file__).parent.parent.parent / "data" / "schema.sql"

    cache_service = DataCacheService(db_path=str(db_path), schema_path=str(schema_path))

    # Handle subcommands
    if args.cache_command == "update":
        if args.pools:
            # Initialize Firebase service
            firebase_service = FirebaseService()
            return update_specific_pools(cache_service, firebase_service, args.pools, min_data_points=args.min_points)
        elif args.recent:
            # Initialize Firebase service
            firebase_service = FirebaseService()
            return update_recent_pools(cache_service, firebase_service)
        else:
            # Initialize Firebase service
            firebase_service = FirebaseService()
            return update_all_pools(cache_service, firebase_service)

    elif args.cache_command == "clear":
        if args.days:
            return clear_old_data(cache_service, args.days)
        else:
            return clear_entire_cache(cache_service)

    elif args.cache_command == "status":
        return show_cache_status(cache_service)

    elif args.cache_command == "backup":
        return backup_cache(cache_service, args.output)

    else:
        logger.error(f"Unknown cache command: {args.cache_command}")
        return False
