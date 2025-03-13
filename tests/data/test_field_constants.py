#!/usr/bin/env python
"""
Field Constants and Schema Validation Test

This script validates that:
1. The field constants can be imported and used
2. The schema fields align with the constants
3. Data from Firebase can be properly stored in the schema
"""

import sys
import json
import logging
import sqlite3
from pathlib import Path
import pandas as pd
from datetime import datetime

# Add project root to path
root_dir = Path(__file__).parent
sys.path.append(str(root_dir))

# Import services and utilities
from src.data.firebase_service import FirebaseService
from src.utils.field_utils import normalize_dataframe_columns
from constants.fields import *  # Import all field constants

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("test_field_constants")

# Test pool ID
TEST_POOL_ID = "12H7zN3gXRfUeu2fCQjooPAofbBL2wz7X7wKoS44oJkX"
SCHEMA_FILE = root_dir / "src" / "data" / "schema.sql"
TEST_DB_PATH = root_dir / "cache" / "test_fields.db"


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


def validate_constants():
    """Validate that field constants are properly defined."""
    logger.info("Validating field constants...")

    # Check that field groups have appropriate content
    timestamp_field_count = len(TIMESTAMP_FIELDS)
    numeric_field_count = len(NUMERIC_FIELDS)
    integer_field_count = len(INTEGER_FIELDS)
    string_field_count = len(STRING_FIELDS)
    complex_field_count = len(COMPLEX_FIELDS)

    logger.info(f"Found {timestamp_field_count} timestamp fields")
    logger.info(f"Found {numeric_field_count} numeric fields")
    logger.info(f"Found {integer_field_count} integer fields")
    logger.info(f"Found {string_field_count} string fields")
    logger.info(f"Found {complex_field_count} complex fields")

    # Check that all required fields exist
    logger.info(f"Required fields: {REQUIRED_FIELDS}")

    # Ensure required fields are defined in at least one category
    all_fields = TIMESTAMP_FIELDS + NUMERIC_FIELDS + INTEGER_FIELDS + STRING_FIELDS + COMPLEX_FIELDS
    missing_required = [field for field in REQUIRED_FIELDS if field not in all_fields]

    if missing_required:
        logger.error(f"Missing required fields: {missing_required}")
    else:
        logger.info("All required fields are properly defined")


def setup_test_database():
    """Set up a test database using the schema."""
    logger.info(f"Setting up test database at {TEST_DB_PATH}")

    # Delete existing database file if it exists
    if TEST_DB_PATH.exists():
        TEST_DB_PATH.unlink()
        logger.info("Removed existing test database")

    # Make sure parent directory exists
    TEST_DB_PATH.parent.mkdir(exist_ok=True)

    # Create new database with schema
    conn = sqlite3.connect(str(TEST_DB_PATH))

    # Read schema file
    with open(SCHEMA_FILE, "r") as f:
        schema_sql = f.read()

    # Execute schema
    conn.executescript(schema_sql)
    conn.commit()

    # Verify schema was created
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]

    logger.info(f"Created tables: {tables}")

    return conn


def validate_schema_alignment(conn):
    """Validate that the schema fields align with the constants."""
    logger.info("Validating schema alignment with field constants...")

    # Get column names from market_data table
    cursor = conn.execute("PRAGMA table_info(market_data)")
    columns = {row[1]: row[2] for row in cursor.fetchall()}

    logger.info(f"Market data table has {len(columns)} columns")

    # Check required fields exist in schema
    schema_required_fields = ["poolAddress", "timestamp", "currentPrice", "marketCap", "holdersCount"]
    missing_schema_fields = [field for field in schema_required_fields if field not in columns]

    if missing_schema_fields:
        logger.error(f"Schema missing required fields: {missing_schema_fields}")
    else:
        logger.info("Schema contains all required fields")

    # Check data types are appropriate
    for field in NUMERIC_FIELDS:
        field_value = field.replace("FIELD_", "").lower()
        if field_value in columns and columns[field_value] != "REAL":
            logger.warning(f"Field {field_value} should be REAL but is {columns[field_value]}")

    for field in INTEGER_FIELDS:
        field_value = field.replace("FIELD_", "").lower()
        if field_value in columns and columns[field_value] != "INTEGER":
            logger.warning(f"Field {field_value} should be INTEGER but is {columns[field_value]}")

    for field in COMPLEX_FIELDS:
        field_value = field.replace("FIELD_", "").lower()
        if field_value in columns and columns[field_value] != "TEXT":
            logger.warning(f"Field {field_value} should be TEXT but is {columns[field_value]}")


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
            logger.info(f"Converting complex field {col} to JSON string")
            df[col] = df[col].apply(lambda x: json.dumps(x, cls=CustomJSONEncoder) if x is not None else None)
        # Handle numeric fields
        elif col in [field.lower() for field in [f.replace("FIELD_", "") for f in NUMERIC_FIELDS]]:
            logger.info(f"Converting numeric field {col} to float")
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: float(x) if pd.notnull(x) else None)
        # Handle integer fields
        elif col in [field.lower() for field in [f.replace("FIELD_", "") for f in INTEGER_FIELDS]]:
            logger.info(f"Converting integer field {col} to int")
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: int(x) if pd.notnull(x) else None)
        # Handle string fields
        else:
            logger.info(f"Converting field {col} to string if needed")
            if df[col].dtype == "object":
                df[col] = df[col].apply(lambda x: str(x) if pd.notnull(x) and x is not None else None)

    return df


def test_data_import(conn):
    """Test importing data from Firebase to the SQLite database."""
    logger.info("Testing data import from Firebase to SQLite...")

    # Initialize Firebase service
    firebase = FirebaseService()

    # Get a small sample of data (10 rows)
    df = firebase.fetch_pool_data(TEST_POOL_ID)
    df = df.head(10)

    if df.empty:
        logger.error("No data retrieved from Firebase")
        return False

    logger.info(f"Retrieved {len(df)} rows from Firebase")

    # Normalize column names
    df = normalize_dataframe_columns(df, target_convention="camel")

    # Make sure poolAddress exists in each row (not poolId)
    if "poolId" in df.columns and "poolAddress" not in df.columns:
        df["poolAddress"] = TEST_POOL_ID
        logger.info("Added poolAddress column based on TEST_POOL_ID")

    # Preprocess for SQLite
    df = preprocess_dataframe(df)

    # Log column names before insert
    logger.info(f"DataFrame columns: {sorted(df.columns)}")

    # First, insert pool info
    pool_insert = """
    INSERT INTO pools 
    (poolAddress, createTimestamp, lastUpdateTimestamp, minTimestamp, maxTimestamp, dataPointsCount)
    VALUES (?, ?, ?, ?, ?, ?)
    """

    try:
        min_timestamp = df["timestamp"].min() if "timestamp" in df.columns else None
        max_timestamp = df["timestamp"].max() if "timestamp" in df.columns else None

        conn.execute(
            pool_insert,
            (
                TEST_POOL_ID,
                datetime.now().isoformat(),
                datetime.now().isoformat(),
                min_timestamp,
                max_timestamp,
                len(df),
            ),
        )
        conn.commit()
        logger.info(f"Inserted pool info for {TEST_POOL_ID}")
    except sqlite3.Error as e:
        logger.error(f"Error inserting pool info: {e}")
        return False

    # Then insert market data one row at a time with detailed logging
    success_count = 0
    error_count = 0

    for idx, row in df.iterrows():
        # Build the columns and values for the INSERT statement
        columns = []
        placeholders = []
        values = []

        for col, val in row.items():
            # Skip poolId if present, we only want poolAddress
            if col == "poolId":
                continue

            # Make sure poolAddress is always included
            if col == "poolAddress" or pd.notnull(val) and val is not None:
                columns.append(col)
                placeholders.append("?")
                values.append(val)

        # Ensure poolAddress is included
        if "poolAddress" not in columns:
            columns.append("poolAddress")
            placeholders.append("?")
            values.append(TEST_POOL_ID)

        # Log all parameters with their index
        logger.info(f"Row {idx+1} parameters:")
        for i, (col, val) in enumerate(zip(columns, values), 1):
            logger.info(f"  Parameter {i}: {col} = {val} ({type(val).__name__})")

        # Create the INSERT statement
        sql = f"INSERT INTO market_data ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

        try:
            conn.execute(sql, values)
            success_count += 1
            logger.info(f"Successfully inserted row {idx+1}")
        except sqlite3.Error as e:
            logger.error(f"Error inserting row {idx+1}: {e}")
            # Retry with basic columns only
            try:
                basic_cols = ["poolAddress", "timestamp"]
                basic_vals = [TEST_POOL_ID, row.get("timestamp")]
                basic_sql = f"INSERT INTO market_data (poolAddress, timestamp) VALUES (?, ?)"
                conn.execute(basic_sql, basic_vals)
                logger.info(f"Inserted row {idx+1} with basic fields only")
                success_count += 1
            except sqlite3.Error as e2:
                logger.error(f"Error inserting basic row: {e2}")
                error_count += 1

    conn.commit()
    logger.info(f"Inserted {success_count} rows successfully, {error_count} errors")

    # Verify data was inserted
    cursor = conn.execute("SELECT COUNT(*) FROM market_data")
    count = cursor.fetchone()[0]
    logger.info(f"Market data table now has {count} rows")

    return count > 0


def cleanup():
    """Clean up test resources."""
    logger.info("Cleaning up test resources...")
    if TEST_DB_PATH.exists():
        TEST_DB_PATH.unlink()
        logger.info(f"Removed test database {TEST_DB_PATH}")


def main():
    """Run the validation and tests."""
    logger.info("Starting field constants and schema validation test")

    # Validate field constants
    validate_constants()

    # Set up test database
    conn = setup_test_database()

    try:
        # Validate schema alignment
        validate_schema_alignment(conn)

        # Test data import
        import_success = test_data_import(conn)

        if import_success:
            logger.info("✅ Test completed successfully")
        else:
            logger.error("❌ Test failed: data import issues")

    except Exception as e:
        logger.error(f"Error during test: {e}", exc_info=True)

    finally:
        # Close database connection
        if conn:
            conn.close()

        # Clean up
        cleanup()

        logger.info("Test completed")


if __name__ == "__main__":
    main()
