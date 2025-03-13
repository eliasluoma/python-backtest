"""
Cache management commands for the CLI.

This module provides commands for managing the SQLite-based cache.
"""

import logging
from typing import List

from src.data.cache_service import DataCacheService
from src.data.firebase_service import FirebaseService

logger = logging.getLogger(__name__)


def add_cache_subparser(subparsers):
    """Add cache management commands to the CLI."""
    cache_parser = subparsers.add_parser("cache", help="Manage the data cache")

    cache_subparsers = cache_parser.add_subparsers(dest="subcommand", help="Cache subcommand")

    # Update cache command
    update_parser = cache_subparsers.add_parser("update", help="Update the data cache from Firebase")
    update_parser.add_argument("--all", action="store_true", help="Update all pools")
    update_parser.add_argument("--pools", type=str, nargs="+", help="Specific pool IDs to update")
    update_parser.add_argument(
        "--limit", type=int, default=100, help="Maximum number of pools to update (default: 100)"
    )
    update_parser.add_argument(
        "--min-data-points",
        type=int,
        default=20,
        help="Minimum number of data points required for each pool (default: 20)",
    )
    update_parser.add_argument(
        "--db-path",
        type=str,
        default="cache/pools.db",
        help="Path to the SQLite database file (default: cache/pools.db)",
    )

    # Clear cache command
    clear_parser = cache_subparsers.add_parser("clear", help="Clear the data cache")
    clear_parser.add_argument("--all", action="store_true", help="Clear entire cache")
    clear_parser.add_argument("--older-than", type=int, help="Clear data older than N days")
    clear_parser.add_argument(
        "--db-path",
        type=str,
        default="cache/pools.db",
        help="Path to the SQLite database file (default: cache/pools.db)",
    )

    # Status command
    status_parser = cache_subparsers.add_parser("status", help="Show cache status")
    status_parser.add_argument(
        "--db-path",
        type=str,
        default="cache/pools.db",
        help="Path to the SQLite database file (default: cache/pools.db)",
    )

    # Backup command
    backup_parser = cache_subparsers.add_parser("backup", help="Create a backup of the cache database")
    backup_parser.add_argument("--output-path", type=str, help="Path for the backup file (default: auto-generated)")
    backup_parser.add_argument(
        "--db-path",
        type=str,
        default="cache/pools.db",
        help="Path to the SQLite database file (default: cache/pools.db)",
    )


def update_all_pools(
    cache_service: DataCacheService, firebase_service: FirebaseService, limit: int = 100, min_data_points: int = 20
) -> bool:
    """
    Update all available pools in the cache.

    Args:
        cache_service: The cache service instance
        firebase_service: The Firebase service instance
        limit: Maximum number of pools to update
        min_data_points: Minimum number of data points required for each pool

    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Updating all pools (limit: {limit}, min_data_points: {min_data_points})...")

    # Get available pools from Firebase
    pools = firebase_service.get_available_pools(limit=limit)

    if not pools:
        logger.error("No pools found in Firebase.")
        return False

    logger.info(f"Found {len(pools)} pools in Firebase.")

    return update_specific_pools(cache_service, firebase_service, pools, min_data_points)


def update_specific_pools(
    cache_service: DataCacheService, firebase_service: FirebaseService, pools: List[str], min_data_points: int = 20
) -> bool:
    """
    Update specific pools in the cache.

    Args:
        cache_service: The cache service instance
        firebase_service: The Firebase service instance
        pools: List of pool IDs to update
        min_data_points: Minimum number of data points required for each pool

    Returns:
        True if successful, False otherwise
    """
    if not pools:
        logger.error("No pools specified for update.")
        return False

    logger.info(f"Updating {len(pools)} specified pools...")

    success_count = 0
    error_count = 0

    for i, pool_id in enumerate(pools):
        logger.info(f"Updating pool {i+1}/{len(pools)}: {pool_id}")

        try:
            # Fetch data from Firebase
            pool_data = firebase_service.fetch_pool_data(pool_id)

            if pool_data.empty:
                logger.warning(f"No data found for pool {pool_id} in Firebase.")
                error_count += 1
                continue

            if len(pool_data) < min_data_points:
                logger.warning(f"Insufficient data points for pool {pool_id}: {len(pool_data)} < {min_data_points}")
                error_count += 1
                continue

            # Update cache
            result = cache_service.update_pool_data(pool_id, pool_data)

            if result:
                logger.info(f"Successfully updated pool {pool_id} with {len(pool_data)} data points.")
                success_count += 1
            else:
                logger.error(f"Failed to update pool {pool_id} in cache.")
                error_count += 1

        except Exception as e:
            logger.error(f"Error updating pool {pool_id}: {str(e)}")
            error_count += 1

    # Log summary
    logger.info(f"Update completed: {success_count} successes, {error_count} errors")

    return error_count == 0


def update_recent_pools(cache_service: DataCacheService, firebase_service: FirebaseService, limit: int = 50) -> bool:
    """
    Update recently active pools in the cache.

    Args:
        cache_service: The cache service instance
        firebase_service: The Firebase service instance
        limit: Maximum number of pools to update

    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Updating recently active pools (limit: {limit})...")

    # Fetch recent market data (last 24 hours)
    recent_data = firebase_service.fetch_recent_market_data(hours_back=24, max_pools=limit)

    if not recent_data:
        logger.error("No recent data found in Firebase.")
        return False

    logger.info(f"Found {len(recent_data)} recently active pools.")

    # Update each pool
    success_count = 0
    error_count = 0

    for pool_id, pool_data in recent_data.items():
        try:
            # Update cache
            result = cache_service.update_pool_data(pool_id, pool_data)

            if result:
                logger.info(f"Successfully updated pool {pool_id} with {len(pool_data)} data points.")
                success_count += 1
            else:
                logger.error(f"Failed to update pool {pool_id} in cache.")
                error_count += 1

        except Exception as e:
            logger.error(f"Error updating pool {pool_id}: {str(e)}")
            error_count += 1

    # Log summary
    logger.info(f"Update completed: {success_count} successes, {error_count} errors")

    return error_count == 0


def clear_entire_cache(cache_service: DataCacheService) -> bool:
    """
    Clear the entire cache.

    Args:
        cache_service: The cache service instance

    Returns:
        True if successful, False otherwise
    """
    logger.info("Clearing entire cache...")

    result = cache_service.clear_cache()

    if result:
        logger.info("Cache cleared successfully.")
    else:
        logger.error("Failed to clear cache.")

    return result


def clear_old_data(cache_service: DataCacheService, days: int) -> bool:
    """
    Clear data older than the specified number of days.

    Args:
        cache_service: The cache service instance
        days: Number of days threshold

    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Clearing data older than {days} days...")

    result = cache_service.clear_cache(older_than_days=days)

    if result:
        logger.info(f"Successfully cleared data older than {days} days.")
    else:
        logger.error(f"Failed to clear data older than {days} days.")

    return result


def show_cache_status(cache_service: DataCacheService) -> bool:
    """
    Show cache status.

    Args:
        cache_service: The cache service instance

    Returns:
        True if successful, False otherwise
    """
    logger.info("Retrieving cache status...")

    stats = cache_service.get_cache_stats()

    if stats.get("status") == "error":
        logger.error(f"Failed to get cache stats: {stats.get('message')}")
        return False

    data = stats.get("data", {})

    # Print status summary
    print("\n===== CACHE STATUS =====")
    print(f"Database path: {data.get('database_path')}")
    print(f"Last update: {data.get('last_update')}")
    print(f"Total pools: {data.get('total_pools')}")
    print(f"Total data points: {data.get('total_data_points')}")
    print(f"Cache size: {data.get('cache_size_mb', 0):.2f} MB")

    # Memory cache info
    memory_cache = data.get("memory_cache", {})
    print(f"Memory cache: {memory_cache.get('size')} items (max: {memory_cache.get('max_size')})")

    # Largest pools
    largest_pools = data.get("largest_pools", [])
    if largest_pools:
        print("\nLargest pools:")
        for i, pool in enumerate(largest_pools):
            print(f"  {i+1}. {pool.get('pool_id')}: {pool.get('data_points')} data points")

    return True


def handle_cache_command(args):
    """
    Handle cache management commands.

    Args:
        args: Command line arguments

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    # Get the database path
    db_path = args.db_path

    # Create cache service
    try:
        cache_service = DataCacheService(db_path)
    except Exception as e:
        logger.error(f"Failed to initialize cache service: {str(e)}")
        return 1

    # Handle subcommands
    if args.subcommand == "update":
        # Create Firebase service
        try:
            firebase_service = FirebaseService()
        except Exception as e:
            logger.error(f"Failed to initialize Firebase service: {str(e)}")
            return 1

        if args.all:
            # Update all pools
            result = update_all_pools(
                cache_service, firebase_service, limit=args.limit, min_data_points=args.min_data_points
            )
        elif args.pools:
            # Update specific pools
            result = update_specific_pools(
                cache_service, firebase_service, args.pools, min_data_points=args.min_data_points
            )
        else:
            # Update recently active pools
            result = update_recent_pools(cache_service, firebase_service, limit=args.limit)

        return 0 if result else 1

    elif args.subcommand == "clear":
        if args.all:
            # Clear entire cache
            result = clear_entire_cache(cache_service)
        elif args.older_than:
            # Clear old data
            result = clear_old_data(cache_service, args.older_than)
        else:
            logger.error("Please specify what to clear (--all or --older-than N)")
            return 1

        return 0 if result else 1

    elif args.subcommand == "status":
        # Show cache status
        result = show_cache_status(cache_service)
        return 0 if result else 1

    elif args.subcommand == "backup":
        # Create backup
        output_path = args.output_path if args.output_path else None
        success, backup_path = cache_service.backup_database(output_path)

        if success:
            logger.info(f"Successfully backed up database to: {backup_path}")
            return 0
        else:
            logger.error(f"Failed to create backup: {backup_path}")
            return 1

    else:
        logger.error("No cache subcommand specified")
        return 1
