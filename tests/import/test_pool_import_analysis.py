#!/usr/bin/env python
"""
Pool Import and Analysis Test

This script demonstrates:
1. Importing pool data from Firebase to the local SQLite cache
2. Performing analysis on the cached data
3. Cleaning up the cache after testing

All data uses camelCase field naming conventions consistent with REQUIRED_FIELDS.
"""

import sys
import json
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
import os

# Add project root to path
root_dir = Path(__file__).parent
sys.path.append(str(root_dir))

# Import services and utilities
from src.data.cache_service import DataCacheService
from src.data.firebase_service import FirebaseService
from src.utils.field_utils import normalize_dataframe_columns

# Set up logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("pool_import_test")

# Constants
NUM_POOLS_TO_IMPORT = 10
MIN_DATA_POINTS = 50


# Custom JSON encoder for handling Firestore Timestamp objects
class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles Firestore Timestamp objects."""

    def default(self, obj):
        # Check for Firebase Timestamp object
        if hasattr(obj, "seconds") and hasattr(obj, "nanoseconds"):
            # Convert to ISO format string
            return datetime.fromtimestamp(obj.seconds + obj.nanoseconds / 1e9).isoformat()
        # Handle datetime objects
        elif hasattr(obj, "isoformat"):
            return obj.isoformat()
        # Handle pandas Timestamp objects
        elif pd.api.types.is_datetime64_any_dtype(type(obj)):
            return obj.isoformat()
        # Let the base class handle anything else
        return super().default(obj)


def setup_cache():
    """Setup a temporary SQLite database for testing."""
    logger.info("Setting up cache database...")

    # Create a temporary database file
    db_path = os.path.join("cache", "test_pools.db")

    # Initialize cache service
    cache_service = DataCacheService(db_path=db_path)

    # Verify cache is empty
    stats = cache_service.get_cache_stats()
    if stats.get("status") == "success":
        data = stats.get("data", {})
        logger.info(f"Cache initialized: {data.get('total_pools')} pools, {data.get('total_data_points')} data points")

    return cache_service, db_path


def preprocess_dataframe(df):
    """Preprocess the DataFrame to handle SQLite data type issues."""
    # Create a copy to avoid modifying the original
    df = df.copy()

    # Convert any Firebase Timestamp objects to datetime
    for col in df.columns:
        if df[col].dtype == "object":
            # Check if column contains timestamp-like objects
            non_null_values = df[col].dropna()
            if len(non_null_values) > 0:
                first_value = non_null_values.iloc[0]

                # Handle Firebase Timestamp objects
                if hasattr(first_value, "seconds") and hasattr(first_value, "nanoseconds"):
                    logger.debug("Converting Firebase Timestamp column {col} to datetime".format(col=col))
                    df[col] = df[col].apply(
                        lambda x: (
                            datetime.fromtimestamp(x.seconds + x.nanoseconds / 1e9)
                            if hasattr(x, "seconds") and hasattr(x, "nanoseconds")
                            else x
                        )
                    )

    # Ensure timestamp column is properly formatted as datetime objects
    # DON'T convert to string - cache_service.py expects datetime objects
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        logger.debug("Converted timestamp to datetime, sample: {timestamp.iloc[0]}".format(timestamp=df["timestamp"]))

    # Handle complex objects that can't be stored in SQLite
    # For each column, check if it contains complex objects and convert to strings
    for col in df.columns:
        if col != "timestamp" and df[col].dtype == "object":  # Skip timestamp column
            # Sample the column for complex types (dict, list, etc.)
            has_complex_objects = False
            for val in df[col].dropna().iloc[:5]:  # Check first 5 non-null values
                if not isinstance(val, (str, int, float, bool, type(None))):
                    has_complex_objects = True
                    logger.debug(
                        "Column {col} has complex Python type: {type(val).__name__}".format(
                            col=col, type=type(val).__name__
                        )
                    )
                    break

            if has_complex_objects:
                logger.debug("Converting complex objects in column {col} to JSON strings".format(col=col))
                # Use the custom encoder to handle timestamp objects properly
                df[col] = df[col].apply(
                    lambda x: json.dumps(x, cls=CustomJSONEncoder) if pd.notnull(x) and x is not None else None
                )

    # Convert numeric types to native Python types, not strings
    # Convert all numeric columns explicitly to the right Python types
    for col in df.columns:
        if col != "timestamp":  # Skip timestamp column
            if pd.api.types.is_integer_dtype(df[col]):
                df[col] = df[col].apply(lambda x: int(x) if pd.notnull(x) else None)
                logger.debug("Converted {col} to Python int".format(col=col))
            elif pd.api.types.is_float_dtype(df[col]):
                df[col] = df[col].apply(lambda x: float(x) if pd.notnull(x) else None)
                logger.debug("Converted {col} to Python float".format(col=col))
            elif pd.api.types.is_bool_dtype(df[col]):
                df[col] = df[col].astype(int)  # SQLite has no bool type, use int
                logger.debug("Converted {col} from boolean to int".format(col=col))

    # Debug output for the first row
    if not df.empty:
        first_row = df.iloc[0]
        logger.debug("First row sample values and types:")
        for col in sorted(df.columns):
            val = first_row[col]
            logger.debug(
                "  {col}: {type(val).__name__} = {str(val)[:50]}".format(
                    col=col, type=type(val).__name__, val=str(val)[:50]
                )
            )

        # Specifically check timeFromStart
        if "timeFromStart" in df.columns:
            logger.debug("timeFromStart column info:")
            logger.debug(f"  dtype: {df['timeFromStart'].dtype}")
            logger.debug(f"  sample values: {df['timeFromStart'].head(3).tolist()}")
            logger.debug(f"  sample types: {[type(x).__name__ for x in df['timeFromStart'].head(3)]}")

    return df


def import_pools_from_firebase(cache_service):
    """Import pools from Firebase to the local cache."""
    logger.info(f"Importing {NUM_POOLS_TO_IMPORT} pools from Firebase...")

    # Initialize Firebase service
    firebase_service = FirebaseService()

    # Get available pools
    pool_ids = firebase_service.get_available_pools(limit=NUM_POOLS_TO_IMPORT * 2)

    if not pool_ids:
        logger.error("No pools found in Firebase")
        return []

    logger.info(f"Found {len(pool_ids)} available pools")

    imported_pools = []
    for i, pool_id in enumerate(pool_ids[:NUM_POOLS_TO_IMPORT]):
        logger.info(f"Importing pool {i+1}/{NUM_POOLS_TO_IMPORT}: {pool_id}")

        try:
            # Fetch data from Firebase
            df = firebase_service.fetch_pool_data(pool_id)

            if df.empty:
                logger.warning(f"No data found for pool {pool_id}")
                continue

            # Ensure we have enough data points
            if len(df) < MIN_DATA_POINTS:
                logger.warning(f"Insufficient data points for pool {pool_id}: {len(df)} < {MIN_DATA_POINTS}")
                continue

            # Normalize column names to camelCase (do this before preprocessing)
            df = normalize_dataframe_columns(df, target_convention="camel")

            # Preprocess the DataFrame to handle SQLite data type issues
            logger.info(f"Preprocessing data for pool {pool_id}")
            df = preprocess_dataframe(df)

            # Update cache
            logger.info(f"Updating cache with {len(df)} data points for pool {pool_id}")

            # For testing with fewer rows
            if len(df) > 100:
                logger.debug("Using only first 100 rows for testing to reduce errors")
                test_df = df.head(100)
            else:
                test_df = df

            success = cache_service.update_pool_data(pool_id, test_df)

            if success:
                logger.info(f"Successfully imported pool {pool_id} with {len(test_df)} data points")
                imported_pools.append(pool_id)
            else:
                logger.error(f"Failed to import pool {pool_id}")

        except Exception as e:
            logger.error(f"Error importing pool {pool_id}: {e}", exc_info=True)

    logger.info(f"Import complete: {len(imported_pools)}/{NUM_POOLS_TO_IMPORT} pools imported")
    return imported_pools


def analyze_pools(cache_service, pool_ids):
    """Perform analysis on the imported pools."""
    logger.info(f"Analyzing {len(pool_ids)} pools...")

    if not pool_ids:
        logger.warning("No pools to analyze")
        return []

    analysis_results = []
    for pool_id in pool_ids:
        logger.info(f"Analyzing pool: {pool_id}")

        try:
            # Get pool data from cache
            df = cache_service.get_pool_data(pool_id)

            if df.empty:
                logger.warning(f"No data found in cache for pool {pool_id}")
                continue

            # Basic pool info
            pool_analysis = {
                "poolAddress": pool_id,
                "dataPoints": len(df),
                "timeRange": f"{df['timestamp'].min()} to {df['timestamp'].max()}",
                "maxMarketCap": df["marketCap"].max() if "marketCap" in df.columns else None,
                "maxHolders": df["holdersCount"].max() if "holdersCount" in df.columns else None,
            }

            # Price change analysis
            price_analysis = analyze_price_change(df)
            if price_analysis.get("status") == "success":
                pool_analysis.update(price_analysis.get("data", {}))

            # Holder growth analysis
            holder_analysis = analyze_holder_growth(df)
            if holder_analysis.get("status") == "success":
                pool_analysis.update(holder_analysis.get("data", {}))

            # Trade volume analysis
            volume_analysis = analyze_trade_volume(df)
            if volume_analysis.get("status") == "success":
                pool_analysis.update(volume_analysis.get("data", {}))

            analysis_results.append(pool_analysis)
            logger.info(f"Analysis complete for pool {pool_id}")

        except Exception as e:
            logger.error(f"Error analyzing pool {pool_id}: {e}", exc_info=True)

    # Print summary analysis
    print_analysis_summary(analysis_results)

    return analysis_results


def analyze_price_change(df):
    """Analyze price changes over the available time period."""
    try:
        # Ensure we have the necessary columns
        if "currentPrice" not in df.columns:
            return {"status": "error", "message": "Missing currentPrice column"}

        # Calculate price metrics
        first_price = df["currentPrice"].iloc[0]
        last_price = df["currentPrice"].iloc[-1]
        price_change = last_price - first_price
        price_change_pct = (price_change / first_price) * 100 if first_price else 0
        max_price = df["currentPrice"].max()
        min_price = df["currentPrice"].min()

        return {
            "status": "success",
            "data": {
                "firstPrice": first_price,
                "lastPrice": last_price,
                "priceChange": price_change,
                "priceChangePct": price_change_pct,
                "maxPrice": max_price,
                "minPrice": min_price,
            },
        }
    except Exception as e:
        logger.error(f"Error analyzing price change: {e}")
        return {"status": "error", "message": str(e)}


def analyze_holder_growth(df):
    """Analyze holder growth in the pool data."""
    if "holdersCount" not in df.columns or df.empty:
        return {"status": "no_data"}

    try:
        start_holders = df["holdersCount"].iloc[0]
        end_holders = df["holdersCount"].iloc[-1]
        max_holders = df["holdersCount"].max()

        growth = end_holders - start_holders
        growth_percent = (growth / start_holders) * 100 if start_holders > 0 else 0

        return {
            "startHolders": start_holders,
            "endHolders": end_holders,
            "maxHolders": max_holders,
            "netGrowth": growth,
            "growthPercent": growth_percent,
        }
    except Exception as e:
        logger.error(f"Error analyzing holder growth: {e}")
        return {"status": "error", "message": str(e)}


def analyze_trade_volume(df):
    """Analyze trade volume in the pool data."""
    volume_cols = [col for col in df.columns if "volume" in col.lower()]

    if not volume_cols or df.empty:
        return {"status": "no_data"}

    try:
        volume_data = {}
        for col in volume_cols:
            volume_sum = df[col].sum() if pd.api.types.is_numeric_dtype(df[col]) else 0
            volume_data[col] = volume_sum

        return {
            "totalVolume": df["totalVolume"].sum() if "totalVolume" in df.columns else None,
            "buyVolume": (
                df["trade_last5Seconds_volume_buy"].sum() if "trade_last5Seconds_volume_buy" in df.columns else None
            ),
            "sellVolume": (
                df["trade_last5Seconds_volume_sell"].sum() if "trade_last5Seconds_volume_sell" in df.columns else None
            ),
            "detailedVolumes": volume_data,
        }
    except Exception as e:
        logger.error(f"Error analyzing trade volume: {e}")
        return {"status": "error", "message": str(e)}


def print_analysis_summary(analysis_results):
    """Print a summary of the analysis results."""
    if not analysis_results:
        logger.warning("No analysis results to display")
        return

    print("\n" + "=" * 80)
    print(f"ANALYSIS SUMMARY FOR {len(analysis_results)} POOLS")
    print("=" * 80)

    for i, analysis in enumerate(analysis_results):
        print(f"\nPOOL {i+1}: {analysis['poolAddress']}")
        print(f"  Data Points: {analysis['dataPoints']}")
        print(f"  Time Range: {analysis['timeRange']}")

        # Price change
        price_change = analysis.get("priceChange", {})
        if price_change.get("status") != "no_data" and "percentChange" in price_change:
            print(
                f"  Price Change: {price_change['percentChange']:.2f}% (Max Gain: {price_change.get('maxPercentGain', 0):.2f}%)"
            )

        # Holder growth
        holder_growth = analysis.get("holderGrowth", {})
        if holder_growth.get("status") != "no_data" and "netGrowth" in holder_growth:
            print(f"  Holder Growth: {holder_growth['netGrowth']} ({holder_growth.get('growthPercent', 0):.2f}%)")

        # Volume
        volume = analysis.get("tradeVolume", {})
        if volume.get("status") != "no_data" and "totalVolume" in volume:
            print(f"  Total Volume: {volume['totalVolume']}")

    print("\n" + "=" * 80)


def cleanup(cache_service, db_path):
    """Clean up the cache after testing."""
    logger.info("Cleaning up test cache...")

    try:
        # Clear all data
        cache_service.clear_cache()
        logger.info("Cache cleared")

        # Close database connection
        del cache_service

        # Delete test database file
        if Path(db_path).exists():
            Path(db_path).unlink()
            logger.info(f"Deleted test database file: {db_path}")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")


def main():
    """Main function to run the test."""
    logger.info("Starting pool import and analysis test")

    # Setup cache
    cache_service, db_path = setup_cache()

    try:
        # Import pools from Firebase
        imported_pools = import_pools_from_firebase(cache_service)

        if not imported_pools:
            logger.error("No pools were imported, aborting test")
            return

        # Analyze pools
        analyze_pools(cache_service, imported_pools)

    except Exception as e:
        logger.error(f"Error during test: {e}", exc_info=True)
    finally:
        # Clean up
        cleanup(cache_service, db_path)
        logger.info("Test completed")


if __name__ == "__main__":
    main()
