#!/usr/bin/env python
"""
Simple Pool Import Test

Import a single pool from Firebase to SQLite with direct control over data types.
This script will:
1. Fetch pool data from Firebase (only required fields)
2. Print the Firebase structure
3. Preprocess and import to SQLite
"""

import sys
import json
import logging
from pathlib import Path
import pandas as pd
import sqlite3  # Keep this for SQLite type checking

# Add project root to path
root_dir = Path(__file__).parent
sys.path.append(str(root_dir))

# Import services and utilities
from src.data.cache_service import DataCacheService
from src.data.firebase_service import FirebaseService
from src.utils.field_utils import normalize_dataframe_columns
from src.analysis.pool_analyzer import REQUIRED_FIELDS

# Set up logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("simple_import_test")

# Test pool ID
TEST_POOL_ID = "12H7zN3gXRfUeu2fCQjooPAofbBL2wz7X7wKoS44oJkX"
MAX_ROWS = 3  # Reduced to just 3 rows for faster debugging


def print_document_structure(doc):
    """Print the structure of a Firebase document to understand available fields"""

    def _print_dict_structure(d, prefix="", depth=0):
        if depth > 10:  # Prevent infinite recursion
            return

        for key, value in d.items():
            full_key = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict):
                print(f"{' ' * (depth * 2)}{full_key}: <dict>")
                _print_dict_structure(value, full_key, depth + 1)
            elif isinstance(value, list):
                print(f"{' ' * (depth * 2)}{full_key}: <list> (length: {len(value)})")
            else:
                type_str = type(value).__name__
                val_str = str(value)
                if len(val_str) > 30:
                    val_str = val_str[:27] + "..."
                print(f"{' ' * (depth * 2)}{full_key}: <{type_str}> = {val_str}")

    if not doc:
        logger.info("Document is empty")
        return

    logger.info("Firebase Document Structure:")
    _print_dict_structure(doc)


def flatten_trade_data(df):
    """
    Flattens the nested trade data to match SQLite schema format
    """
    logger.info("Flattening trade data to match schema format")

    new_df = df.copy()

    # Process tradeLast5Seconds
    if "tradeLast5Seconds" in df.columns:
        logger.info("Processing tradeLast5Seconds column")

        # Extract volume data
        new_df["trade_last5Seconds_volume_buy"] = df["tradeLast5Seconds"].apply(
            lambda x: float(x.get("volume", {}).get("buy", 0)) if isinstance(x, dict) else 0
        )
        new_df["trade_last5Seconds_volume_sell"] = df["tradeLast5Seconds"].apply(
            lambda x: float(x.get("volume", {}).get("sell", 0)) if isinstance(x, dict) else 0
        )
        new_df["trade_last5Seconds_volume_bot"] = df["tradeLast5Seconds"].apply(
            lambda x: float(x.get("volume", {}).get("bot", 0)) if isinstance(x, dict) else 0
        )

        # Extract trade count data
        for size in ["small", "medium", "large", "big", "super"]:
            new_df[f"trade_last5Seconds_tradeCount_buy_{size}"] = df["tradeLast5Seconds"].apply(
                lambda x: (
                    int(x.get("tradeCount", {}).get("buy", {}).get(size, 0))
                    if isinstance(x, dict) and isinstance(x.get("tradeCount", {}).get("buy", {}), dict)
                    else 0
                )
            )
            new_df[f"trade_last5Seconds_tradeCount_sell_{size}"] = df["tradeLast5Seconds"].apply(
                lambda x: (
                    int(x.get("tradeCount", {}).get("sell", {}).get(size, 0))
                    if isinstance(x, dict) and isinstance(x.get("tradeCount", {}).get("sell", {}), dict)
                    else 0
                )
            )

        # Extract bot trade count
        new_df["trade_last5Seconds_tradeCount_bot"] = df["tradeLast5Seconds"].apply(
            lambda x: int(x.get("tradeCount", {}).get("bot", 0)) if isinstance(x, dict) else 0
        )

        # Now we can drop the original column
        new_df = new_df.drop("tradeLast5Seconds", axis=1)

    # Process tradeLast10Seconds - same approach
    if "tradeLast10Seconds" in df.columns:
        logger.info("Processing tradeLast10Seconds column")

        # Extract volume data
        new_df["trade_last10Seconds_volume_buy"] = df["tradeLast10Seconds"].apply(
            lambda x: float(x.get("volume", {}).get("buy", 0)) if isinstance(x, dict) else 0
        )
        new_df["trade_last10Seconds_volume_sell"] = df["tradeLast10Seconds"].apply(
            lambda x: float(x.get("volume", {}).get("sell", 0)) if isinstance(x, dict) else 0
        )
        new_df["trade_last10Seconds_volume_bot"] = df["tradeLast10Seconds"].apply(
            lambda x: float(x.get("volume", {}).get("bot", 0)) if isinstance(x, dict) else 0
        )

        # Extract trade count data
        for size in ["small", "medium", "large", "big", "super"]:
            new_df[f"trade_last10Seconds_tradeCount_buy_{size}"] = df["tradeLast10Seconds"].apply(
                lambda x: (
                    int(x.get("tradeCount", {}).get("buy", {}).get(size, 0))
                    if isinstance(x, dict) and isinstance(x.get("tradeCount", {}).get("sell", {}), dict)
                    else 0
                )
            )
            new_df[f"trade_last10Seconds_tradeCount_sell_{size}"] = df["tradeLast10Seconds"].apply(
                lambda x: (
                    int(x.get("tradeCount", {}).get("sell", {}).get(size, 0))
                    if isinstance(x, dict) and isinstance(x.get("tradeCount", {}).get("sell", {}), dict)
                    else 0
                )
            )

        # Extract bot trade count
        new_df["trade_last10Seconds_tradeCount_bot"] = df["tradeLast10Seconds"].apply(
            lambda x: int(x.get("tradeCount", {}).get("bot", 0)) if isinstance(x, dict) else 0
        )

        # Drop the original column
        new_df = new_df.drop("tradeLast10Seconds", axis=1)

    return new_df


def convert_complex_objects_to_json(df):
    """
    Converts any complex objects (dicts, lists, etc.) in the DataFrame to JSON strings
    """
    logger.info("Converting any complex objects to JSON strings")
    new_df = df.copy()

    for col in new_df.columns:
        # Check if column contains complex objects
        complex_objects = False
        for val in new_df[col].dropna().head():
            if not isinstance(val, (int, float, str, bool, type(None))):
                complex_objects = True
                logger.info(f"Found complex object in column {col}: {type(val).__name__}")
                break

        if complex_objects:
            logger.info(f"Converting objects in column {col} to JSON strings")
            new_df[col] = new_df[col].apply(
                lambda x: json.dumps(x) if x is not None and not isinstance(x, (int, float, str, bool)) else x
            )

    return new_df


def ensure_column_types(df):
    """
    Ensures all columns have compatible SQLite types
    """
    logger.info("Ensuring all columns have compatible SQLite types")

    # Create a copy to avoid modifying the original
    new_df = df.copy()

    # Ensure timestamp is proper datetime first (this is critical)
    if "timestamp" in new_df.columns:
        new_df["timestamp"] = pd.to_datetime(new_df["timestamp"])
        # Don't convert to ISO format string - let cache_service handle it
        logger.info("Converted timestamp to datetime objects")

    # Convert all float columns to Python float (not numpy.float)
    float_cols = new_df.select_dtypes(include=["float"]).columns
    for col in float_cols:
        new_df[col] = new_df[col].apply(lambda x: float(x) if pd.notnull(x) else None)
        logger.info(f"Converted {col} to Python float")

    # Convert all integer columns to Python int (not numpy.int)
    int_cols = new_df.select_dtypes(include=["int"]).columns
    for col in int_cols:
        new_df[col] = new_df[col].apply(lambda x: int(x) if pd.notnull(x) else None)
        logger.info(f"Converted {col} to Python int")

    # Handle string columns - ensure they're Python strings
    str_cols = new_df.select_dtypes(include=["object"]).columns
    for col in str_cols:
        # Special handling for currentPrice which might be a string but should be a number
        if col == "currentPrice":
            new_df[col] = pd.to_numeric(new_df[col], errors="coerce")
            new_df[col] = new_df[col].apply(lambda x: float(x) if pd.notnull(x) else None)
            logger.info(f"Converted {col} to numeric")
        else:
            # Convert to Python string, handling None/NaN values
            new_df[col] = new_df[col].apply(lambda x: str(x) if pd.notnull(x) and x is not None else None)
            logger.info(f"Converted {col} to Python string")

    # Boolean columns to integers (0/1) as SQLite has no boolean type
    bool_cols = new_df.select_dtypes(include=["bool"]).columns
    for col in bool_cols:
        new_df[col] = new_df[col].astype(int)
        logger.info(f"Converted {col} from boolean to int")

    # Final check for any remaining complex objects by directly converting them to JSON strings
    new_df = convert_complex_objects_to_json(new_df)

    return new_df


def main():
    """Import a single pool from Firebase to SQLite"""
    logger.info("Starting simple pool import test")

    # Print the required fields that we're looking for
    logger.info(f"Required fields: {REQUIRED_FIELDS}")

    # Setup cache
    cache_path = root_dir / "cache" / "simple_test.db"
    schema_path = root_dir / "src" / "data" / "schema.sql"

    if cache_path.exists():
        logger.info(f"Removing existing cache file: {cache_path}")
        cache_path.unlink()

    logger.info(f"Initializing cache at {cache_path}")
    cache_service = DataCacheService(db_path=str(cache_path), schema_path=str(schema_path))

    # We can't patch the conn attribute directly, so we'll add our own debugging
    logger.info("Adding custom debugging for SQLite operations")

    # Enable more verbose logging in the cache_service
    logging.getLogger("src.data.cache_service").setLevel(logging.DEBUG)

    # Fetch data from Firebase
    logger.info(f"Fetching data for pool {TEST_POOL_ID}")
    firebase = FirebaseService()
    df = firebase.fetch_pool_data(TEST_POOL_ID)

    # Print the structure of the first document to understand what's available
    if not df.empty:
        # Convert first row to dictionary for structure analysis
        first_doc = df.iloc[0].to_dict()
        print_document_structure(first_doc)

        # Check which required fields are missing
        missing_fields = []
        for field in REQUIRED_FIELDS:
            # Handle nested fields
            if "." in field:
                parts = field.split(".")
                if parts[0] not in df.columns:
                    missing_fields.append(field)
            elif field not in df.columns:
                missing_fields.append(field)

        if missing_fields:
            logger.warning(f"Missing required fields: {missing_fields}")
        else:
            logger.info("All required fields are present")
    else:
        logger.error("No data retrieved from Firebase")
        return

    # Limit rows for faster testing
    df = df.head(MAX_ROWS)
    logger.info(f"Using {len(df)} rows of data")

    # Normalize column names
    df = normalize_dataframe_columns(df, target_convention="camel")

    # Process nested trade data
    df = flatten_trade_data(df)

    # Ensure proper column types
    df = ensure_column_types(df)

    # Log column information
    logger.info(f"DataFrame has {len(df.columns)} columns")
    for col in df.columns:
        sample = df[col].iloc[0] if not df.empty else None
        dtype = df[col].dtype
        logger.info(f"Column: {col}, Type: {dtype}, Sample: {str(sample)[:30]}...")

    # Import to cache
    logger.info("Importing data to cache")
    try:
        # Add debug logging for the first row to see all values
        if not df.empty:
            first_row = df.iloc[0]
            logger.debug("First row values with parameter numbers:")
            for idx, (col, val) in enumerate(first_row.items(), 1):
                logger.debug(f"Parameter {idx}: {col} = {val} ({type(val).__name__})")

            # Get a sorted list of all columns (to determine the 9th parameter)
            sorted_cols = sorted(df.columns)
            if len(sorted_cols) >= 9:
                ninth_col = sorted_cols[8]  # 0-indexed, so 8 is the 9th
                logger.debug(f"9th column (alphabetically): {ninth_col}")
                logger.debug(f"9th column values: {df[ninth_col].head(3).tolist()}")

            # Check for columns with complex object types
            complex_cols = []
            for col in df.columns:
                for val in df[col].head().dropna():
                    if not isinstance(val, (int, float, str, bool, type(None))):
                        complex_cols.append(f"{col} ({type(val).__name__})")
                        break

            if complex_cols:
                logger.warning(f"Columns with complex types: {complex_cols}")

            # Let's also explicitly log the timeFromStart column if it exists
            if "timeFromStart" in df.columns:
                logger.debug("timeFromStart samples:")
                for i, val in enumerate(df["timeFromStart"].head()):
                    logger.debug(f"  Row {i}: {val} ({type(val).__name__})")

        success = cache_service.update_pool_data(TEST_POOL_ID, df)
        if success:
            logger.info("Successfully imported data to cache")

            # Verify data was imported correctly
            logger.info("Retrieving data from cache to verify")
            cached_df = cache_service.get_pool_data(TEST_POOL_ID)
            logger.info(f"Retrieved {len(cached_df)} rows from cache")

            # Show stats
            stats = cache_service.get_cache_stats()
            if stats["status"] == "success":
                for key, value in stats["data"].items():
                    logger.info(f"Cache stat: {key} = {value}")
        else:
            logger.error("Failed to import data to cache")
    except Exception as e:
        logger.error(f"Error importing data: {e}", exc_info=True)

    # Clean up
    try:
        logger.info("Cleaning up")
        cache_service.clear_cache()
        del cache_service
        if cache_path.exists():
            cache_path.unlink()
            logger.info("Removed cache file")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")

    logger.info("Test completed")


if __name__ == "__main__":
    main()
