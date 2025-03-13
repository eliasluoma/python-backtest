#!/usr/bin/env python
"""
Initialize SQLite Database

This script initializes the SQLite database for caching pool data.
It ensures the proper directory structure and database schema.
All field names use camelCase to match the REQUIRED_FIELDS from pool_analyzer.py.
"""

import sys
from pathlib import Path

# Add the root directory to the path
root_dir = Path(__file__).parent.parent
sys.path.append(str(root_dir))

# Import the DataCacheService
from src.data.cache_service import DataCacheService


def main():
    """Initialize the SQLite database for pool data caching."""
    # Define paths
    cache_dir = root_dir / "cache"
    db_path = cache_dir / "pools.db"
    schema_path = root_dir / "src" / "data" / "schema.sql"

    # Create cache directory if it doesn't exist
    cache_dir.mkdir(exist_ok=True)

    print(f"Initializing database at: {db_path}")
    print(f"Using schema from: {schema_path}")
    print("Using schema version 2.0 with camelCase field naming")

    try:
        # Initialize the DataCacheService which will create the database
        cache_service = DataCacheService(db_path=str(db_path), schema_path=str(schema_path))

        # Get cache stats to verify it was initialized correctly
        stats = cache_service.get_cache_stats()

        if stats.get("status") == "success":
            print("\nDatabase initialized successfully!")
            print("\nCache Statistics:")
            data = stats.get("data", {})
            print(f"  Database path: {data.get('database_path')}")
            print(f"  Last update: {data.get('last_update')}")
            print(f"  Total pools: {data.get('total_pools')}")
            print(f"  Total data points: {data.get('total_data_points')}")
            print(f"  Cache size: {data.get('cache_size_mb', 0):.2f} MB")
            return 0
        else:
            print(f"Error initializing database: {stats.get('message', 'Unknown error')}")
            return 1

    except Exception as e:
        print(f"Error initializing database: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
