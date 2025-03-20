#!/usr/bin/env python
"""
Debug Pool Import

This script tests importing a single pool from Firebase to SQLite with detailed debugging.
"""

import sys
import json
import logging
import traceback
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime

# Add project root to path
root_dir = Path(__file__).parent
sys.path.append(str(root_dir))

# Import services and utilities
from src.data.firebase_service import FirebaseService
from src.utils.field_utils import normalize_dataframe_columns

# Set up logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("debug_import")

# Test pool ID - choose one that has data
TEST_POOL_ID = "12H7zN3gXRfUeu2fCQjooPAofbBL2wz7X7wKoS44oJkX"


def setup_test_db():
    """Create a simple test database with just the necessary fields"""
    db_path = root_dir / "cache" / "debug.db"

    # Delete existing DB if it exists
    if db_path.exists():
        db_path.unlink()

    # Create directory if it doesn't exist
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Create a simple schema
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS pools (
        poolAddress TEXT PRIMARY KEY,
        lastUpdated TIMESTAMP,
        dataPoints INTEGER
    )
    """
    )

    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS market_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        poolAddress TEXT NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        currentPrice REAL,
        marketCap REAL,
        holdersCount INTEGER,
        additional_data TEXT,
        FOREIGN KEY (poolAddress) REFERENCES pools(poolAddress)
    )
    """
    )

    conn.commit()
    conn.close()

    logger.info(f"Created test database at {db_path}")
    return db_path


def fetch_and_preprocess_data():
    """Fetch data from Firebase and prepare it for SQLite"""
    firebase_service = FirebaseService()
    logger.info(f"Fetching data for pool {TEST_POOL_ID}")

    # Get a limited set of data for testing
    df = firebase_service.fetch_pool_data(TEST_POOL_ID)
    df = df.head(10)  # Just use 10 rows for easier debugging

    if df.empty:
        logger.error("No data found for test pool")
        return None

    logger.info(f"Fetched {len(df)} rows of data")

    # Normalize column names to camelCase
    df = normalize_dataframe_columns(df, target_convention="camel")

    # Ensure we have required columns
    required_cols = ["timestamp", "currentPrice", "marketCap", "holdersCount"]
    for col in required_cols:
        if col not in df.columns:
            logger.warning(f"Required column '{col}' not in data")

    # Ensure timestamp is datetime
    if "timestamp" in df.columns:
        # Check if timestamp is already a datetime
        if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            # Check if it's a Firestore Timestamp
            if hasattr(df["timestamp"].iloc[0], "seconds"):
                df["timestamp"] = df["timestamp"].apply(
                    lambda x: datetime.fromtimestamp(x.seconds + x.nanoseconds / 1e9)
                )
            else:
                # Try to convert to datetime
                df["timestamp"] = pd.to_datetime(df["timestamp"])

    logger.info(f"DataFrame columns: {list(df.columns)}")
    return df


def print_df_types(df):
    """Print data types for debugging"""
    logger.info("DataFrame data types:")
    for col in df.columns:
        col_type = df[col].dtype
        sample_val = df[col].iloc[0]
        sample_type = type(sample_val).__name__
        logger.info(f"  {col}: {col_type} -> Python type: {sample_type}")


def preprocess_for_sqlite(df):
    """Process data to be SQLite-compatible"""
    df = df.copy()

    # SQLite doesn't store NaN values well, convert to None
    for col in df.columns:
        has_null = df[col].isna().any()
        if has_null:
            logger.debug(f"Column {col} has NULL values, converting to None")

    # Handle specific data types
    for col in df.columns:
        # Handle datetime columns
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            logger.debug(f"Converting datetime column {col} to ISO format strings")
            df[col] = df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)

        # Handle numeric columns
        elif pd.api.types.is_float_dtype(df[col]):
            logger.debug(f"Converting float column {col} to Python float")
            df[col] = df[col].apply(lambda x: float(x) if pd.notnull(x) else None)

        elif pd.api.types.is_integer_dtype(df[col]):
            logger.debug(f"Converting integer column {col} to Python int")
            df[col] = df[col].apply(lambda x: int(x) if pd.notnull(x) else None)

        # Handle boolean columns
        elif pd.api.types.is_bool_dtype(df[col]):
            logger.debug(f"Converting boolean column {col} to int")
            df[col] = df[col].astype(int)

        # Handle remaining object columns that might have complex types
        elif df[col].dtype == "object":
            has_complex = False
            for val in df[col].dropna():
                if not isinstance(val, (str, int, float, bool)):
                    has_complex = True
                    logger.warning(f"Column {col} has complex Python type: {type(val).__name__}")
                    break

            if has_complex:
                logger.debug(f"Converting complex objects in column {col} to JSON strings")
                df[col] = df[col].apply(lambda x: json.dumps(x, default=str) if pd.notnull(x) else None)

    return df


def import_to_sqlite(df, db_path):
    """Import the data to SQLite with detailed error handling"""
    try:
        conn = sqlite3.connect(str(db_path))

        # First, insert pool record
        logger.info("Inserting pool record")
        conn.execute(
            "INSERT INTO pools (poolAddress, lastUpdated, dataPoints) VALUES (?, ?, ?)",
            (TEST_POOL_ID, datetime.now().isoformat(), len(df)),
        )

        # Now insert market data one row at a time with detailed error handling
        logger.info("Inserting market data rows:")

        for i, row in df.iterrows():
            # Extract main columns for direct insertion
            timestamp = row.get("timestamp")
            current_price = row.get("currentPrice")
            market_cap = row.get("marketCap")
            holders_count = row.get("holdersCount")

            # Put all other columns into additional_data JSON
            additional_cols = {}
            for col in row.index:
                if col not in ["poolAddress", "timestamp", "currentPrice", "marketCap", "holdersCount"]:
                    val = row[col]
                    if pd.notnull(val):
                        additional_cols[col] = val

            # Convert additional data to JSON
            additional_data = json.dumps(additional_cols, default=str)

            try:
                logger.debug(f"Inserting row {i+1}/{len(df)}")
                conn.execute(
                    """
                    INSERT INTO market_data 
                    (poolAddress, timestamp, currentPrice, marketCap, holdersCount, additional_data)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (TEST_POOL_ID, timestamp, current_price, market_cap, holders_count, additional_data),
                )
                logger.debug(f"Row {i+1} inserted successfully")
            except Exception as e:
                logger.error(f"Error inserting row {i+1}: {e}")
                logger.error(
                    f"Row data: timestamp={timestamp}, currentPrice={current_price}, "
                    f"marketCap={market_cap}, holdersCount={holders_count}"
                )
                logger.error(f"Additional data: {additional_data[:100]}...")

                # Detailed error diagnostics
                for j, (param_name, param_val) in enumerate(
                    [
                        ("poolAddress", TEST_POOL_ID),
                        ("timestamp", timestamp),
                        ("currentPrice", current_price),
                        ("marketCap", market_cap),
                        ("holdersCount", holders_count),
                        ("additional_data", additional_data),
                    ]
                ):
                    logger.debug(f"  Parameter {j+1}: {param_name}={param_val} (type: {type(param_val).__name__})")

                # Continue with next row
                continue

        # Commit transaction
        conn.commit()
        logger.info("All data committed to the database")

    except Exception as e:
        logger.error(f"Database error: {e}")
        logger.error(traceback.format_exc())
    finally:
        if "conn" in locals():
            conn.close()


def verify_import(db_path):
    """Verify that data was imported correctly"""
    try:
        conn = sqlite3.connect(str(db_path))

        # Query pool data
        cursor = conn.execute("SELECT poolAddress, dataPoints FROM pools")
        pool_data = cursor.fetchone()
        if pool_data:
            logger.info(f"Pool record: {pool_data}")
        else:
            logger.error("No pool record found!")

        # Query market data
        cursor = conn.execute("SELECT COUNT(*) FROM market_data WHERE poolAddress = ?", (TEST_POOL_ID,))
        data_count = cursor.fetchone()[0]
        logger.info(f"Market data points: {data_count}")

        # Sample data
        if data_count > 0:
            cursor = conn.execute("SELECT timestamp, currentPrice, marketCap, holdersCount FROM market_data LIMIT 3")
            sample_data = cursor.fetchall()
            logger.info("Sample data:")
            for row in sample_data:
                logger.info(f"  {row}")

    except Exception as e:
        logger.error(f"Verification error: {e}")
    finally:
        if "conn" in locals():
            conn.close()


def main():
    """Main function"""
    logger.info("Starting debug pool import")

    try:
        # Setup test database
        db_path = setup_test_db()

        # Fetch and process data
        df = fetch_and_preprocess_data()
        if df is None:
            logger.error("Failed to fetch data, exiting")
            return

        # Print data types for debugging
        print_df_types(df)

        # Process for SQLite
        logger.info("Processing data for SQLite")
        df_sqlite = preprocess_for_sqlite(df)

        # Import to SQLite
        logger.info("Importing to SQLite")
        import_to_sqlite(df_sqlite, db_path)

        # Verify import
        logger.info("Verifying import")
        verify_import(db_path)

        logger.info("Debug import completed")

    except Exception as e:
        logger.error(f"Unhandled error: {e}")
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()
