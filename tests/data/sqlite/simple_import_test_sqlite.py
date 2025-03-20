#!/usr/bin/env python
"""
Simple SQLite Debug Test

This script demonstrates how to handle complex data types with SQLite.
It creates a simple test database and inserts test data with various types
to identify any issues with parameter binding.
"""

import sys
import sqlite3
import logging
import pandas as pd
import json
from datetime import datetime
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("sqlite_debug")


# Custom JSON encoder to handle non-serializable objects
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, "isoformat"):  # For datetime and Timestamp objects
            return obj.isoformat()
        elif pd.isna(obj):  # For NaN/None values
            return None
        # Add other type conversions as needed
        return super().default(obj)


def ensure_column_types(df):
    """
    Ensures all columns have compatible SQLite types
    """
    logger.info("Ensuring all columns have compatible SQLite types")

    # Create a copy to avoid modifying the original
    new_df = df.copy()

    # Timestamp to string or ISO format
    if "timestamp" in new_df.columns:
        new_df["timestamp"] = new_df["timestamp"].apply(lambda x: x.isoformat() if hasattr(x, "isoformat") else str(x))
        logger.info("Converted timestamp to ISO format string")

    # Convert float columns to Python float
    float_cols = ["marketCap", "currentPrice", "buyVolume5s", "netVolume5s"]
    for col in float_cols:
        if col in new_df.columns:
            new_df[col] = new_df[col].apply(lambda x: float(x) if pd.notnull(x) else None)
            logger.info(f"Converted {col} to Python float")

    # Convert int columns to Python int
    int_cols = ["holdersCount", "initialHoldersCount", "timeFromStart"]
    for col in int_cols:
        if col in new_df.columns:
            new_df[col] = new_df[col].apply(lambda x: int(x) if pd.notnull(x) else None)
            logger.info(f"Converted {col} to Python int")

    # Convert string columns to Python string
    str_cols = ["poolAddress", "timestamp"]
    for col in str_cols:
        if col in new_df.columns:
            new_df[col] = new_df[col].apply(lambda x: str(x) if pd.notnull(x) else None)
            logger.info(f"Converted {col} to Python string")

    return new_df


def main():
    """Run SQLite test to identify parameter binding issues"""
    logger.info("Starting simple SQLite debug test")

    # Create test database
    db_path = Path("cache") / "debug_test.db"
    if db_path.exists():
        db_path.unlink()

    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Create test data
    data = {
        "poolAddress": ["test_pool"] * 5,
        "timestamp": [datetime.now()] * 5,
        "marketCap": [1000.0, 1100.0, 1200.0, 1300.0, 1400.0],
        "currentPrice": [0.1, 0.11, 0.12, 0.13, 0.14],
        "holdersCount": [100, 110, 120, 130, 140],
        "initialHoldersCount": [100] * 5,
        "buyVolume5s": [10.0, 20.0, 30.0, 40.0, 50.0],
        "netVolume5s": [5.0, 10.0, 15.0, 20.0, 25.0],
        "timeFromStart": [0, 10, 20, 30, 40],
    }

    df = pd.DataFrame(data)
    df = ensure_column_types(df)

    # Connect to database and setup schema
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS market_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        poolAddress TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        marketCap REAL,
        currentPrice REAL,
        holdersCount INTEGER,
        initialHoldersCount INTEGER,
        buyVolume5s REAL,
        netVolume5s REAL,
        timeFromStart INTEGER,
        additional_data TEXT
    )
    """
    )
    logger.info("Created test database schema")

    # Insert data row by row with detailed logging
    for idx, row in df.iterrows():
        # Prepare data and params for insert
        columns = [
            "poolAddress",
            "timestamp",
            "marketCap",
            "currentPrice",
            "holdersCount",
            "initialHoldersCount",
            "buyVolume5s",
            "netVolume5s",
            "timeFromStart",
            "additional_data",
        ]

        # In a real application, you'd gather extra fields into additional_data
        additional_data = {}  # This would hold any extra fields not in the schema

        # Convert additional_data to JSON string
        additional_data_json = json.dumps(additional_data, cls=CustomJSONEncoder)

        # Build parameters list
        params = [
            row["poolAddress"],
            row["timestamp"],
            row["marketCap"],
            row["currentPrice"],
            row["holdersCount"],
            row["initialHoldersCount"],
            row["buyVolume5s"],
            row["netVolume5s"],
            row["timeFromStart"],
            additional_data_json,
        ]

        # Log all parameters
        logger.debug(f"SQL Insert: columns = {', '.join(columns)}")
        logger.debug(f"SQL Insert: placeholders = {'?, ' * (len(columns)-1)}?")

        for i, (col, val) in enumerate(zip(columns, params), 1):
            logger.debug(f"Parameter {i}: {col} = {val} ({type(val).__name__})")

        # Build SQL and execute
        sql = f"INSERT INTO market_data ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})"
        logger.debug(f"SQL: {sql}")

        try:
            conn.execute(sql, params)
            logger.info(f"Successfully inserted row {idx+1}")
        except Exception as e:
            logger.error(f"Error inserting row {idx+1}: {e}")
            # If error occurs with parameter 9, highlight it
            if "parameter 9" in str(e):
                logger.error(f"Parameter 9 (timeFromStart) value: {params[8]}, type: {type(params[8]).__name__}")

    # Verify data was inserted
    cursor = conn.execute("SELECT COUNT(*) FROM market_data")
    count = cursor.fetchone()[0]
    logger.info(f"Database contains {count} rows after test")

    # Close and clean up
    conn.close()
    if db_path.exists():
        db_path.unlink()
        logger.info(f"Removed test database: {db_path}")

    logger.info("Test completed")


if __name__ == "__main__":
    main()
