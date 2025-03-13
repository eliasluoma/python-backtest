"""
Firebase service for Solana trading simulator.

This module provides functionality to connect to Firebase Firestore,
retrieve market data, and process it for backtesting.
"""

import os
import pandas as pd
import pytz  # type: ignore # Ignore "Library stubs not installed for pytz"
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import Dict, Optional, List
import logging
import time

try:
    from src.utils.firebase_utils import (
        initialize_firebase,
        fetch_market_data_for_pool,
        get_pool_ids,
        preprocess_market_data,
    )
except ImportError:
    # Handle relative import for testing
    import sys

    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    from src.utils.firebase_utils import (
        initialize_firebase,
        fetch_market_data_for_pool,
        get_pool_ids,
        preprocess_market_data,
    )

# Configure logging for this module
logger = logging.getLogger("FirebaseService")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class FirebaseService:
    """
    Professional Firebase service for Solana bot backtesting.
    Provides functionality to fetch and process market data from Firebase Firestore
    without storing data in local files.
    """

    def __init__(self, credential_path: Optional[str] = None):
        """
        Initialize FirebaseService with optional credential path.

        Args:
            credential_path: Path to Firebase credentials JSON file (optional)
        """
        # Set credential path in environment if provided
        if credential_path:
            os.environ["FIREBASE_KEY_FILE"] = credential_path

        # Load environment variables
        try:
            load_dotenv(".env.local")
            logger.info("Environment variables loaded from .env.local")
        except Exception as e:
            logger.warning(f"Could not load .env.local: {str(e)}")

        # Initialize Firebase connection using the utility function
        self.db = initialize_firebase()
        if self.db:
            logger.info("Firebase initialized successfully")
        else:
            logger.warning("Failed to initialize Firebase - some functionality may be limited")

    def fetch_market_data(
        self,
        min_data_points: int = 20,
        max_pools: int = 10,
        limit_per_pool: int = 100,
        pool_address: Optional[str] = None,
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch market data from Firebase.

        Args:
            min_data_points: Minimum number of data points required per pool
            max_pools: Maximum number of pools to fetch data for
            limit_per_pool: Maximum number of data points to fetch per pool
            pool_address: Optional specific pool address to fetch data for

        Returns:
            Dictionary mapping pool IDs to their respective DataFrames
        """
        if not self.db:
            logger.error("Firebase not initialized, cannot fetch market data")
            return {}

        start_time = time.time()

        try:
            # If specific pool is requested
            if pool_address:
                logger.info(f"Fetching data for specific pool: {pool_address}")
                df = fetch_market_data_for_pool(
                    self.db, pool_address, limit=limit_per_pool, min_data_points=min_data_points
                )

                if df is not None:
                    df = preprocess_market_data(df)
                    result = {pool_address: df}
                else:
                    result = {}
            else:
                # Fetch all pools within limits
                pool_ids = get_pool_ids(self.db, limit=max_pools)
                logger.info(f"Found {len(pool_ids)} pools, fetching data...")

                result = {}
                for pool_id in pool_ids:
                    df = fetch_market_data_for_pool(
                        self.db, pool_id, limit=limit_per_pool, min_data_points=min_data_points
                    )

                    if df is not None:
                        df = preprocess_market_data(df)
                        result[pool_id] = df

            elapsed_time = time.time() - start_time
            logger.info(f"Fetched {len(result)} pools in {elapsed_time:.2f} seconds")
            return result

        except Exception as e:
            logger.error(f"Error fetching market data: {str(e)}")
            return {}

    def fetch_recent_market_data(
        self, hours_back: int = 24, min_data_points: int = 20, max_pools: int = 10
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch recent market data within the specified time range.

        Args:
            hours_back: Number of hours to look back
            min_data_points: Minimum number of data points required per pool
            max_pools: Maximum number of pools to fetch

        Returns:
            Dictionary mapping pool IDs to their respective DataFrames
        """
        logger.info(f"Fetching recent market data from the past {hours_back} hours")

        # Fetch all data first
        all_data = self.fetch_market_data(min_data_points=min_data_points, max_pools=max_pools)

        if not all_data:
            return {}

        # Filter for recent data
        cutoff_time = datetime.now() - timedelta(hours=hours_back)

        result = {}
        for pool_id, df in all_data.items():
            if "timestamp" in df.columns:
                recent_df = df[df["timestamp"] >= cutoff_time]
                if len(recent_df) >= min_data_points:
                    result[pool_id] = recent_df

        logger.info(f"Found {len(result)} pools with recent data")
        return result

    def get_available_pools(self, limit: Optional[int] = None) -> List[str]:
        """
        Get a list of available pool IDs.

        Args:
            limit: Maximum number of pool IDs to return (None means no limit)

        Returns:
            List of pool IDs
        """
        if not self.db:
            logger.error("Firebase not initialized, cannot get available pools")
            return []

        try:
            return get_pool_ids(self.db, limit=limit)
        except Exception as e:
            logger.error(f"Error getting available pools: {str(e)}")
            return []

    def preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocess the market data for backtesting

        Args:
            df: Raw market data DataFrame

        Returns:
            Preprocessed DataFrame ready for backtesting
        """
        logger.info("Preprocessing market data...")

        # Ensure timestamp is in datetime format
        if "timestamp" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        # Sort by pool and timestamp
        df = df.sort_values(["poolAddress", "timestamp"])

        # Fill missing values with appropriate defaults
        # Note: These column names should match what's expected by simulators
        numeric_columns = [
            "marketCap",
            "holdersCount",
            "marketCapChange5s",
            "marketCapChange10s",
            "marketCapChange30s",
            "marketCapChange60s",
            "buyVolume5s",
            "netVolume5s",
            "priceChangePercent",
        ]

        # Fill NA values with 0 for numeric columns
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].fillna(0)

        # Calculate additional derived metrics if needed
        # This will be expanded based on requirements

        logger.info(f"Preprocessing complete. DataFrame shape: {df.shape}")
        return df

    def fetch_pool_data(self, pool_address: str, collection_name: str = "marketContext") -> pd.DataFrame:
        """
        Fetch data for a specific pool

        Args:
            pool_address: The pool address to fetch data for
            collection_name: The Firestore collection name for market contexts

        Returns:
            DataFrame containing pool data
        """
        logger.info(f"Fetching data for pool {pool_address}")

        if not self.db:
            raise RuntimeError("Firebase connection not initialized")

        try:
            # Get the pool document
            pool_doc = self.db.collection(collection_name).document(pool_address)

            # Fetch all market contexts for this pool
            contexts = pool_doc.collection("marketContexts").order_by("timestamp").get()

            pool_contexts = []
            for context in contexts:
                data = context.to_dict()

                # Convert string numbers to floats (same as in fetch_market_data)
                for key in [
                    "marketCap",
                    "athMarketCap",
                    "minMarketCap",
                    "maMarketCap10s",
                    "maMarketCap30s",
                    "maMarketCap60s",
                    "marketCapChange5s",
                    "marketCapChange10s",
                    "marketCapChange30s",
                    "marketCapChange60s",
                    "priceChangeFromStart",
                ]:
                    if key in data and isinstance(data[key], str):
                        try:
                            data[key] = float(data[key])
                        except (ValueError, TypeError):
                            data[key] = 0.0

                # Convert timestamp
                if "timestamp" in data:
                    try:
                        if isinstance(data["timestamp"], (int, float)):
                            data["timestamp"] = datetime.fromtimestamp(data["timestamp"] / 1000).replace(
                                tzinfo=pytz.UTC
                            )
                        elif isinstance(data["timestamp"], str):
                            data["timestamp"] = datetime.fromtimestamp(float(data["timestamp"]) / 1000).replace(
                                tzinfo=pytz.UTC
                            )
                    except Exception:
                        # Skip invalid timestamps
                        continue

                data["poolAddress"] = pool_address
                pool_contexts.append(data)

            if not pool_contexts:
                logger.warning(f"No data found for pool {pool_address}")
                return pd.DataFrame()

            # Create DataFrame
            df = pd.DataFrame(pool_contexts)
            logger.info(f"Loaded {len(df)} data points for pool {pool_address}")

            # Return preprocessed data
            return self.preprocess_data(df)

        except Exception as e:
            logger.error(f"Error fetching data for pool {pool_address}: {str(e)}")
            return pd.DataFrame()  # Return empty DataFrame on error
