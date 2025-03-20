#!/usr/bin/env python
"""
Analyze Pool Fields

This script imports a single pool from Firebase and analyzes its fields and data types.
The output will be used to create a constants/fields.py file.
"""

import sys
import json
from pathlib import Path
import pandas as pd

# Add project root to path
root_dir = Path(__file__).parent
sys.path.append(str(root_dir))

# Import services and utilities
from src.data.firebase_service import FirebaseService

# Set up basic logging
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("analyze_fields")

# Test pool ID
TEST_POOL_ID = "12H7zN3gXRfUeu2fCQjooPAofbBL2wz7X7wKoS44oJkX"


def analyze_field_types(df):
    """Analyze field types of the DataFrame."""
    logger.info(f"DataFrame shape: {df.shape}")

    # Print column names and data types
    logger.info("Fields and their data types:")
    for col in sorted(df.columns):
        logger.info(f"{col}: {df[col].dtype}")

    # Print sample values and their Python types for the first row
    if not df.empty:
        logger.info("\nSample values (first row):")
        first_row = df.iloc[0]
        for col in sorted(df.columns):
            val = first_row[col]
            val_str = str(val)[:100] + "..." if len(str(val)) > 100 else str(val)
            logger.info(f"{col}: {type(val).__name__} = {val_str}")

    # Check for complex types (objects, nested data, etc.)
    complex_fields = []
    for col in df.columns:
        for val in df[col].dropna().head():
            if not isinstance(val, (int, float, str, bool, type(None))):
                complex_fields.append((col, type(val).__name__))
                break

    if complex_fields:
        logger.info("\nComplex fields with non-primitive types:")
        for field, type_name in complex_fields:
            sample = df[field].dropna().iloc[0] if not df[field].dropna().empty else None
            logger.info(f"{field}: {type_name}")
            if sample and isinstance(sample, dict):
                logger.info(f"  Structure: {list(sample.keys())[:10]}")


def generate_field_constants(df):
    """Generate field constants code based on DataFrame analysis."""
    # Categorize fields
    timestamp_fields = []
    numeric_fields = []
    string_fields = []
    integer_fields = []
    complex_fields = []

    for col in sorted(df.columns):
        if "time" in col.lower() or "date" in col.lower() or "timestamp" in col.lower():
            timestamp_fields.append(col)
        elif df[col].dtype == "float64" or df[col].dtype == "float32":
            numeric_fields.append(col)
        elif df[col].dtype == "int64" or df[col].dtype == "int32":
            integer_fields.append(col)
        elif df[col].dtype == "object":
            # Check first non-null value
            val = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if val is not None:
                if isinstance(val, str):
                    string_fields.append(col)
                else:
                    complex_fields.append(col)

    # Generate field constants
    code = """\"\"\"
Field constants for Solana Trading Simulator.

This file defines the field names used in the Firebase database and SQLite cache.
These constants help maintain consistency across the codebase.
\"\"\"

# Timestamp Fields 
# These fields store time-related data (datetime objects, timestamps)
"""

    for field in timestamp_fields:
        code += f'FIELD_{field.upper()} = "{field}"\n'

    code += """
# Numeric Fields
# These fields store decimal values (prices, volumes, rates)
"""

    for field in numeric_fields:
        code += f'FIELD_{field.upper()} = "{field}"\n'

    code += """
# Integer Fields
# These fields store whole number values (counts, indices)
"""

    for field in integer_fields:
        code += f'FIELD_{field.upper()} = "{field}"\n'

    code += """
# String Fields
# These fields store text data (identifiers, names, addresses)
"""

    for field in string_fields:
        code += f'FIELD_{field.upper()} = "{field}"\n'

    code += """
# Complex Fields 
# These fields store nested data structures (dicts, lists)
"""

    for field in complex_fields:
        code += f'FIELD_{field.upper()} = "{field}"  # Requires serialization for SQLite\n'

    code += """
# Field Groups (for easier access)
TIMESTAMP_FIELDS = [
"""

    for field in timestamp_fields:
        code += f"    FIELD_{field.upper()},\n"

    code += """]

NUMERIC_FIELDS = [
"""

    for field in numeric_fields:
        code += f"    FIELD_{field.upper()},\n"

    code += """]

INTEGER_FIELDS = [
"""

    for field in integer_fields:
        code += f"    FIELD_{field.upper()},\n"

    code += """]

STRING_FIELDS = [
"""

    for field in string_fields:
        code += f"    FIELD_{field.upper()},\n"

    code += """]

COMPLEX_FIELDS = [
"""

    for field in complex_fields:
        code += f"    FIELD_{field.upper()},\n"

    code += """]

# All fields combined
ALL_FIELDS = TIMESTAMP_FIELDS + NUMERIC_FIELDS + INTEGER_FIELDS + STRING_FIELDS + COMPLEX_FIELDS

# Required fields for basic functionality
REQUIRED_FIELDS = [
    FIELD_POOLADDRESS,
    FIELD_TIMESTAMP,
    FIELD_CURRENTPRICE,
"""

    # Add some common required fields
    if "marketCap" in numeric_fields or "marketCap" in integer_fields:
        code += "    FIELD_MARKETCAP,\n"
    if "holdersCount" in numeric_fields or "holdersCount" in integer_fields:
        code += "    FIELD_HOLDERSCOUNT,\n"

    code += "]"

    return code


def main():
    """Import a pool from Firebase and analyze its fields."""
    logger.info(f"Fetching data for pool {TEST_POOL_ID}")

    # Initialize Firebase service
    firebase = FirebaseService()

    # Fetch data
    df = firebase.fetch_pool_data(TEST_POOL_ID)

    if df.empty:
        logger.error("No data retrieved from Firebase")
        return

    # Analyze field types
    analyze_field_types(df)

    # Generate field constants
    constants_code = generate_field_constants(df)

    # Create directory if it doesn't exist
    constants_dir = root_dir / "constants"
    constants_dir.mkdir(exist_ok=True)

    # Write to file
    fields_file = constants_dir / "fields.py"
    with open(fields_file, "w") as f:
        f.write(constants_code)

    logger.info(f"Field constants written to {fields_file}")


if __name__ == "__main__":
    main()
