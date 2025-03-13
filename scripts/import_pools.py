#!/usr/bin/env python
"""
Import Pools from Firebase to SQLite

This script imports a specified number of pools from Firebase to a local SQLite database.
It uses the field constants defined in constants/fields.py and the updated SQLite schema.
"""

import sys
import json
import logging
import sqlite3
import time
from pathlib import Path
from datetime import datetime
import pandas as pd

# Add project root to path
root_dir = Path(__file__).parent
sys.path.append(str(root_dir))

# Import services and utilities
from src.data.firebase_service import FirebaseService
from src.utils.field_utils import normalize_dataframe_columns
from constants.fields import (
    COMPLEX_FIELDS,
    NUMERIC_FIELDS,
    INTEGER_FIELDS,
)

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("import_pools")

# Constants
NUM_POOLS_TO_IMPORT = 10
MIN_DATA_POINTS = 100
SCHEMA_FILE = root_dir / "src" / "data" / "updated_schema.sql"
DB_PATH = root_dir / "cache" / "pools.db"


class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for complex objects."""

    def default(self, obj):
        if hasattr(obj, "seconds") and hasattr(obj, "nanoseconds"):
            # Firebase Timestamp
            return datetime.fromtimestamp(obj.seconds + obj.nanoseconds / 1e9).isoformat()
        elif hasattr(obj, "isoformat"):
            # Datetime object
            return obj.isoformat()
        elif pd.api.types.is_datetime64_any_dtype(type(obj)):
            # Pandas Timestamp object
            return obj.isoformat()
        return super().default(obj)


def setup_database():
    """Set up the SQLite database with the updated schema."""
    logger.info(f"Setting up database at {DB_PATH}")

    # Make sure parent directory exists
    DB_PATH.parent.mkdir(exist_ok=True)

    # Connect to database (will create if it doesn't exist)
    conn = sqlite3.connect(str(DB_PATH))

    try:
        # Execute schema file
        with open(SCHEMA_FILE, "r") as f:
            schema_sql = f.read()
            conn.executescript(schema_sql)
            conn.commit()

        # Verify tables were created
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        logger.info(f"Database setup complete with tables: {', '.join(tables)}")

        return conn
    except Exception as e:
        logger.error(f"Error setting up database: {e}")
        if conn:
            conn.close()
        return None


def preprocess_dataframe(df):
    """Preprocess DataFrame for SQLite storage."""
    # Create a copy
    df = df.copy()

    # Handle timestamp conversion
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["timestamp"] = df["timestamp"].apply(lambda x: x.isoformat() if hasattr(x, "isoformat") else str(x))

    # Convert numeric fields properly
    for col in df.columns:
        # Skip timestamp which is already handled
        if col == "timestamp":
            continue

        # Handle complex fields first (they might be misidentified as numeric)
        if col in [field.lower() for field in [f.replace("FIELD_", "") for f in COMPLEX_FIELDS]]:
            logger.debug(f"Converting complex field {col} to JSON string")
            df[col] = df[col].apply(lambda x: json.dumps(x, cls=CustomJSONEncoder) if x is not None else None)
        # Handle numeric fields
        elif col in [field.lower() for field in [f.replace("FIELD_", "") for f in NUMERIC_FIELDS]]:
            logger.debug(f"Converting numeric field {col} to float")
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: float(x) if pd.notnull(x) else None)
        # Handle integer fields
        elif col in [field.lower() for field in [f.replace("FIELD_", "") for f in INTEGER_FIELDS]]:
            logger.debug(f"Converting integer field {col} to int")
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: int(x) if pd.notnull(x) else None)
        # Handle string fields
        else:
            logger.debug(f"Converting field {col} to string if needed")
            if df[col].dtype == "object":
                df[col] = df[col].apply(lambda x: str(x) if pd.notnull(x) and x is not None else None)

    return df


def insert_pool_data(conn, pool_id, df):
    """Insert pool data into the database."""
    if df.empty:
        logger.warning(f"Empty DataFrame for pool {pool_id}, skipping")
        return False

    # Get metadata
    min_timestamp = df["timestamp"].min() if "timestamp" in df.columns else None
    max_timestamp = df["timestamp"].max() if "timestamp" in df.columns else None
    data_points = len(df)

    try:
        # Begin transaction
        conn.execute("BEGIN TRANSACTION")

        # First, insert or update pool info
        try:
            # Check if pool exists
            cursor = conn.execute("SELECT poolAddress FROM pools WHERE poolAddress = ?", (pool_id,))
            pool_exists = cursor.fetchone() is not None

            if pool_exists:
                # Update existing pool
                conn.execute(
                    """
                    UPDATE pools SET 
                        lastUpdated = ?,
                        dataPoints = dataPoints + ?,
                        minTimestamp = MIN(minTimestamp, ?),
                        maxTimestamp = MAX(maxTimestamp, ?)
                    WHERE poolAddress = ?
                    """,
                    (
                        datetime.now().isoformat(),
                        data_points,
                        min_timestamp,
                        max_timestamp,
                        pool_id,
                    ),
                )
                logger.info(f"Updated pool info for {pool_id}")
            else:
                # Insert new pool
                conn.execute(
                    """
                    INSERT INTO pools (
                        poolAddress, creationTime, lastUpdated, 
                        dataPoints, minTimestamp, maxTimestamp, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        pool_id,
                        datetime.now().isoformat(),
                        datetime.now().isoformat(),
                        data_points,
                        min_timestamp,
                        max_timestamp,
                        "{}",
                    ),
                )
                logger.info(f"Inserted new pool {pool_id}")

        except Exception as e:
            logger.error(f"Error inserting pool info for {pool_id}: {e}")
            conn.rollback()
            return False

        # Get column names from table schema
        cursor = conn.execute("PRAGMA table_info(market_data)")
        db_columns = [row[1] for row in cursor.fetchall()]

        # Then insert market data
        success_count = 0
        error_count = 0

        for idx, row in df.iterrows():
            # Create a dictionary of field values that are in the schema
            db_fields = {}
            extra_fields = {}

            # Separate fields that are in the schema from those that are not
            for col in row.index:
                if col in db_columns:
                    # Only include non-null values
                    if pd.notnull(row[col]) and row[col] is not None:
                        db_fields[col] = row[col]
                else:
                    # Skip NaN values
                    if pd.notnull(row[col]) and row[col] is not None:
                        extra_fields[col] = row[col]

            # Convert extra fields to JSON
            additional_data = json.dumps(extra_fields, cls=CustomJSONEncoder) if extra_fields else "{}"
            db_fields["additional_data"] = additional_data

            # Create placeholders and values for SQL query
            placeholders = ", ".join(["?"] * len(db_fields))
            columns = ", ".join(db_fields.keys())
            values = list(db_fields.values())

            # Insert into database
            try:
                conn.execute(f"INSERT OR REPLACE INTO market_data ({columns}) VALUES ({placeholders})", values)
                success_count += 1
            except Exception as e:
                logger.error(f"Error inserting row {idx} for pool {pool_id}: {e}")
                error_count += 1

        # Commit transaction
        conn.commit()
        logger.info(f"Imported {success_count} rows for pool {pool_id} ({error_count} errors)")

        return success_count > 0

    except Exception as e:
        logger.error(f"Error during transaction for pool {pool_id}: {e}")
        conn.rollback()
        return False


def import_pools():
    """Import pools from Firebase to the local SQLite database."""
    logger.info(f"Starting import of {NUM_POOLS_TO_IMPORT} pools from Firebase")
    start_time = time.time()

    # Initialize Firebase service
    firebase_service = FirebaseService()

    # Setup database
    conn = setup_database()
    if not conn:
        logger.error("Failed to set up database, aborting")
        return

    try:
        # Get available pools
        pool_ids = firebase_service.get_available_pools(limit=NUM_POOLS_TO_IMPORT * 2)

        if not pool_ids:
            logger.error("No pools found in Firebase")
            return

        logger.info(f"Found {len(pool_ids)} available pools")

        # Import each pool
        imported_pools = []
        failed_pools = []

        for i, pool_id in enumerate(pool_ids[:NUM_POOLS_TO_IMPORT]):
            logger.info(f"Importing pool {i+1}/{NUM_POOLS_TO_IMPORT}: {pool_id}")

            try:
                # Fetch data from Firebase
                df = firebase_service.fetch_pool_data(pool_id)

                if df.empty:
                    logger.warning(f"No data found for pool {pool_id}")
                    failed_pools.append(pool_id)
                    continue

                # Ensure we have enough data points
                if len(df) < MIN_DATA_POINTS:
                    logger.warning(f"Insufficient data points for pool {pool_id}: {len(df)} < {MIN_DATA_POINTS}")
                    failed_pools.append(pool_id)
                    continue

                # Normalize column names to camelCase
                df = normalize_dataframe_columns(df, target_convention="camel")

                # Make sure poolAddress exists in each row (not poolId)
                if "poolId" in df.columns and "poolAddress" not in df.columns:
                    df["poolAddress"] = pool_id
                    logger.info(f"Added poolAddress column based on pool_id for {pool_id}")

                # Preprocess the DataFrame to handle SQLite data type issues
                logger.info(f"Preprocessing data for pool {pool_id}")
                df = preprocess_dataframe(df)

                # Insert data
                logger.info(f"Inserting {len(df)} data points for pool {pool_id}")
                success = insert_pool_data(conn, pool_id, df)

                if success:
                    logger.info(f"Successfully imported pool {pool_id}")
                    imported_pools.append(pool_id)
                else:
                    logger.error(f"Failed to import pool {pool_id}")
                    failed_pools.append(pool_id)

            except Exception as e:
                logger.error(f"Error processing pool {pool_id}: {e}")
                failed_pools.append(pool_id)

        # Calculate statistics
        elapsed_time = time.time() - start_time
        success_rate = len(imported_pools) / NUM_POOLS_TO_IMPORT * 100 if NUM_POOLS_TO_IMPORT > 0 else 0

        # Print summary
        logger.info("=" * 50)
        logger.info("Import Summary")
        logger.info("=" * 50)
        logger.info(f"Total pools attempted: {NUM_POOLS_TO_IMPORT}")
        logger.info(f"Successfully imported: {len(imported_pools)} ({success_rate:.1f}%)")
        logger.info(f"Failed to import: {len(failed_pools)}")
        logger.info(f"Total time: {elapsed_time:.2f} seconds")
        logger.info("=" * 50)

        if imported_pools:
            logger.info("Imported pools:")
            for i, pool_id in enumerate(imported_pools, 1):
                logger.info(f"{i}. {pool_id}")

        if failed_pools:
            logger.info("Failed pools:")
            for i, pool_id in enumerate(failed_pools, 1):
                logger.info(f"{i}. {pool_id}")

    except Exception as e:
        logger.error(f"Error during import: {e}")
    finally:
        # Close database connection
        if conn:
            conn.close()


def main():
    """Main entry point."""
    try:
        import_pools()
        logger.info("Import process completed")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)


if __name__ == "__main__":
    main()
