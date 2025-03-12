import os
import logging
import pandas as pd
import numpy as np
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore

logger = logging.getLogger(__name__)


def initialize_firebase():
    """Initialize Firebase connection if not already initialized."""
    try:
        if not firebase_admin._apps:
            # Check for environment variable first (recommended approach)
            key_file = os.environ.get("FIREBASE_KEY_FILE")

            # If environment variable is not set, look for credential file in various locations
            if not key_file or not os.path.exists(key_file):
                logger.warning("FIREBASE_KEY_FILE environment variable not set or invalid.")
                logger.warning("Looking for credential file in common locations.")
                logger.warning("NOTE: Storing credential files in your repo is a security risk!")
                logger.warning("Consider using environment variables instead.")

                # Try common locations
                common_locations = [
                    "credentials/firebase-credentials.json",  # Preferred location within credentials directory
                    os.path.expanduser("~/.config/firebase-credentials.json"),  # User config directory
                    "/etc/firebase-credentials.json",  # System-wide location
                ]

                for loc in common_locations:
                    if os.path.exists(loc):
                        key_file = loc
                        logger.info(f"Found Firebase credential file at: {key_file}")
                        break

                if not key_file or not os.path.exists(key_file):
                    raise FileNotFoundError(
                        "Firebase credentials not found. Please set the FIREBASE_KEY_FILE environment "
                        "variable or ensure the credential file exists in one of the expected locations."
                    )

            logger.info(f"Using Firebase credentials from: {key_file}")
            cred = credentials.Certificate(key_file)
            firebase_admin.initialize_app(cred)
            logger.info("Firebase connection initialized successfully.")
        else:
            logger.info("Firebase connection already initialized.")

        return firestore.client()
    except Exception as e:
        logger.error(f"Error initializing Firebase: {str(e)}")
        return None


def get_pool_ids(db, limit=None):
    """
    Get a list of all pool IDs from the marketContext collection.

    Args:
        db: Firestore database client
        limit: Maximum number of pool IDs to return (optional)

    Returns:
        List of pool IDs
    """
    try:
        pools_collection = db.collection("marketContext")
        if limit:
            pools = list(pools_collection.list_documents())[:limit]
        else:
            pools = list(pools_collection.list_documents())

        return [pool.id for pool in pools]
    except Exception as e:
        logger.error(f"Error getting pool IDs: {str(e)}")
        return []


def fetch_market_data_for_pool(db, pool_id, limit=100, min_data_points=20):
    """
    Fetch market data for a specific pool from the marketContexts subcollection.

    Args:
        db: Firestore database client
        pool_id: ID of the pool to fetch data for
        limit: Maximum number of data points to fetch (default: 100)
        min_data_points: Minimum number of data points required (default: 20)

    Returns:
        Pandas DataFrame with the market data, or None if insufficient data
    """
    try:
        # Get the pool document
        pool_doc = db.collection("marketContext").document(pool_id)

        # Get the marketContexts subcollection
        contexts_collection = pool_doc.collection("marketContexts")

        # Order by timestamp and limit the number of documents
        contexts = list(
            contexts_collection.order_by("timestamp", direction=firestore.Query.DESCENDING).limit(limit).stream()
        )

        if len(contexts) < min_data_points:
            logger.warning(f"Insufficient data points for pool {pool_id}: {len(contexts)} < {min_data_points}")
            return None

        # Convert to list of dictionaries
        data = []
        for doc in contexts:
            doc_data = doc.to_dict()
            doc_data["doc_id"] = doc.id
            data.append(doc_data)

        if not data:
            logger.warning(f"No data retrieved for pool {pool_id}")
            return None

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Convert Firestore timestamps to Python datetime
        if "timestamp" in df.columns:
            timestamps = []
            for ts in df["timestamp"]:
                if hasattr(ts, "seconds"):
                    timestamps.append(datetime.fromtimestamp(ts.seconds))
                else:
                    timestamps.append(ts)
            df["timestamp"] = timestamps

        # Convert string values to numeric where possible
        for col in df.columns:
            if col not in ["doc_id", "timestamp", "poolAddress", "originalTimestamp", "creationTime"]:
                try:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                except:
                    pass

        return df

    except Exception as e:
        logger.error(f"Error fetching market data for pool {pool_id}: {str(e)}")
        return None


def fetch_market_data(min_data_points=20, max_pools=10, limit_per_pool=100):
    """
    Fetch market data for multiple pools.

    Args:
        min_data_points: Minimum number of data points required per pool (default: 20)
        max_pools: Maximum number of pools to fetch data for (default: 10)
        limit_per_pool: Maximum number of data points to fetch per pool (default: 100)

    Returns:
        Dictionary mapping pool IDs to their respective DataFrames
    """
    try:
        db = initialize_firebase()
        if not db:
            return {}

        pool_ids = get_pool_ids(db, limit=max_pools)
        logger.info(f"Found {len(pool_ids)} pools, fetching data...")

        result = {}
        for pool_id in pool_ids:
            df = fetch_market_data_for_pool(db, pool_id, limit=limit_per_pool, min_data_points=min_data_points)
            if df is not None:
                result[pool_id] = df

        logger.info(f"Successfully fetched data for {len(result)} pools out of {len(pool_ids)}")
        return result

    except Exception as e:
        logger.error(f"Error fetching market data: {str(e)}")
        return {}


def extract_nested_fields(df):
    """
    Extract nested fields from the DataFrame (like tradeLast5Seconds.volume.buy).

    Args:
        df: Pandas DataFrame with nested fields

    Returns:
        DataFrame with flattened nested fields
    """

    # Function to safely access nested dictionaries
    def safe_get(d, keys, default=np.nan):
        if not isinstance(d, dict):
            return default

        current = d
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        return current

    # Define nested fields to extract
    nested_fields = [
        ("tradeLast5Seconds.volume.buy", ["tradeLast5Seconds", "volume", "buy"]),
        ("tradeLast5Seconds.volume.sell", ["tradeLast5Seconds", "volume", "sell"]),
        ("tradeLast5Seconds.tradeCount.buy.large", ["tradeLast5Seconds", "tradeCount", "buy", "large"]),
        ("tradeLast5Seconds.tradeCount.buy.medium", ["tradeLast5Seconds", "tradeCount", "buy", "medium"]),
        ("tradeLast5Seconds.tradeCount.buy.super", ["tradeLast5Seconds", "tradeCount", "buy", "super"]),
        ("tradeLast5Seconds.tradeCount.buy.small", ["tradeLast5Seconds", "tradeCount", "buy", "small"]),
        ("tradeLast5Seconds.tradeCount.sell.large", ["tradeLast5Seconds", "tradeCount", "sell", "large"]),
        ("tradeLast5Seconds.tradeCount.sell.medium", ["tradeLast5Seconds", "tradeCount", "sell", "medium"]),
        ("tradeLast5Seconds.tradeCount.sell.super", ["tradeLast5Seconds", "tradeCount", "sell", "super"]),
        ("tradeLast5Seconds.tradeCount.sell.small", ["tradeLast5Seconds", "tradeCount", "sell", "small"]),
        # Alternative naming with underscore
        ("trade_last5Seconds.volume.buy", ["trade_last5Seconds", "volume", "buy"]),
        ("trade_last5Seconds.volume.sell", ["trade_last5Seconds", "volume", "sell"]),
        ("trade_last5Seconds.tradeCount.buy.large", ["trade_last5Seconds", "tradeCount", "buy", "large"]),
    ]

    result_df = df.copy()

    # Extract each nested field
    for col_name, keys in nested_fields:
        if any(key in df.columns for key in [keys[0], keys[0].replace("_", "")]):
            result_df[col_name] = df.apply(
                lambda row: safe_get(row.get(keys[0], row.get(keys[0].replace("_", ""), {})), keys[1:]), axis=1
            )

    return result_df


def preprocess_market_data(df):
    """
    Preprocess market data DataFrame: convert types, handle missing values, etc.

    Args:
        df: Pandas DataFrame with market data

    Returns:
        Preprocessed DataFrame
    """
    if df is None or df.empty:
        return None

    # Make a copy to avoid modifying the original
    result = df.copy()

    # Extract nested fields if present
    has_nested = any("tradeLast5Seconds" in col or "trade_last5Seconds" in col for col in result.columns)
    if has_nested:
        result = extract_nested_fields(result)

    # Convert string numeric values to float
    for col in result.columns:
        if col not in ["doc_id", "timestamp", "poolAddress", "originalTimestamp", "creationTime"]:
            try:
                result[col] = pd.to_numeric(result[col], errors="coerce")
            except:
                pass

    # Sort by timestamp
    if "timestamp" in result.columns:
        result = result.sort_values("timestamp")

    # Fill NaN values with appropriate defaults
    numeric_cols = result.select_dtypes(include=["number"]).columns
    result[numeric_cols] = result[numeric_cols].fillna(0)

    return result
