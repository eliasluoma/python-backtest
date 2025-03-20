#!/usr/bin/env python
"""
Compare SQLite schema with REQUIRED_FIELDS

A simple script to compare the fields in the SQLite database schema
with the REQUIRED_FIELDS list from pool_analyzer.py
"""

import os
import sqlite3
import sys
from pathlib import Path


# Add the project root to the path
sys.path.append(str(Path(__file__).parent))

# Import the REQUIRED_FIELDS
from src.analysis.pool_analyzer import REQUIRED_FIELDS


def main(db_path="cache/pools.db"):
    """Compare SQLite schema with REQUIRED_FIELDS."""
    print(f"Using database at: {db_path}")

    # Check if database exists
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at {db_path}")
        return 1

    try:
        # Get SQLite schema
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

        # Print fields
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

        print("\nTotal SQL fields including JSON contents:")
        print(f"  {len(clean_schema_fields) + len(trade_fields)}")

        return 0

    except Exception as e:
        print(f"Error: {str(e)}")
        return 1


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "cache/pools.db"
    sys.exit(main(db_path))
