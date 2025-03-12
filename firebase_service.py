import os
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import pytz
from datetime import datetime
from dotenv import load_dotenv
import json
from typing import Dict, List, Optional, Union, Any
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("FirebaseService")


class FirebaseService:
    """
    Professional Firebase service for Solana bot backtesting.
    Provides functionality to fetch and process market data from Firebase Firestore
    without storing data in local files.
    """

    def __init__(
        self, env_file: str = ".env.local", credentials_json: Optional[str] = None
    ):
        """
        Initialize the Firebase service

        Args:
            env_file: Path to .env file containing Firebase config (optional)
            credentials_json: Firebase credentials JSON string or path to file (optional)
        """
        self.db = None
        self._initialize_firebase(env_file, credentials_json)

    def _initialize_firebase(
        self, env_file: str = ".env.local", credentials_json: Optional[str] = None
    ):
        """
        Initialize Firebase connection

        Args:
            env_file: Path to .env file containing Firebase config
            credentials_json: Firebase credentials JSON string or path to file
        """
        try:
            # Load environment variables if file exists
            if os.path.exists(env_file):
                load_dotenv(env_file)
                logger.info(f"Loaded environment variables from {env_file}")

            # If Firebase app is not already initialized
            if not firebase_admin._apps:
                # Use provided JSON credentials (as string or file path)
                if credentials_json:
                    if os.path.exists(credentials_json):
                        # It's a file path
                        cred = credentials.Certificate(credentials_json)
                    else:
                        # It's a JSON string
                        try:
                            cred_dict = json.loads(credentials_json)
                            cred = credentials.Certificate(cred_dict)
                        except json.JSONDecodeError:
                            logger.error("Invalid credentials JSON string")
                            raise ValueError("Invalid credentials JSON string")
                # Try to use environment variables for credentials
                elif os.environ.get("FIREBASE_CREDENTIALS"):
                    cred_dict = json.loads(os.environ.get("FIREBASE_CREDENTIALS"))
                    cred = credentials.Certificate(cred_dict)
                # Try to use a locally stored service account file
                elif os.environ.get("FIREBASE_CREDENTIALS_FILE"):
                    cred = credentials.Certificate(
                        os.environ.get("FIREBASE_CREDENTIALS_FILE")
                    )
                else:
                    logger.error("No Firebase credentials provided")
                    raise ValueError(
                        "No Firebase credentials provided. Pass credentials_json or "
                        "set FIREBASE_CREDENTIALS or FIREBASE_CREDENTIALS_FILE environment variable."
                    )

                # Initialize the app
                firebase_admin.initialize_app(cred)
                logger.info("Firebase initialized successfully")

            # Get the Firestore client
            self.db = firestore.client()

        except Exception as e:
            logger.error(f"Error initializing Firebase: {str(e)}")
            raise

    def fetch_market_data(self, collection_name: str = "marketContext") -> pd.DataFrame:
        """
        Fetch all market data from Firebase directly into memory

        Args:
            collection_name: The Firestore collection name for market contexts

        Returns:
            DataFrame containing all market data
        """
        logger.info(f"Fetching market data from '{collection_name}' collection...")

        if not self.db:
            raise RuntimeError("Firebase connection not initialized")

        all_pool_data = []
        pool_docs = list(self.db.collection(collection_name).list_documents())
        total_pools = len(pool_docs)
        logger.info(f"Found {total_pools} pools")

        for idx, pool_doc in enumerate(pool_docs, 1):
            try:
                # Fetch all market contexts for this pool
                contexts = (
                    pool_doc.collection("marketContexts").order_by("timestamp").get()
                )

                pool_contexts = []
                for context in contexts:
                    data = context.to_dict()

                    # Convert string numbers to floats
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
                                data["timestamp"] = datetime.fromtimestamp(
                                    data["timestamp"] / 1000
                                ).replace(tzinfo=pytz.UTC)
                            elif isinstance(data["timestamp"], str):
                                data["timestamp"] = datetime.fromtimestamp(
                                    float(data["timestamp"]) / 1000
                                ).replace(tzinfo=pytz.UTC)
                        except Exception:
                            # Skip invalid timestamps
                            continue

                    data["poolAddress"] = pool_doc.id
                    pool_contexts.append(data)

                if pool_contexts:
                    df = pd.DataFrame(pool_contexts)
                    all_pool_data.append(df)
                    logger.info(
                        f"[{idx}/{total_pools}] Loaded {len(pool_contexts)} data points for pool {pool_doc.id}"
                    )

            except Exception as e:
                logger.error(f"Error processing pool {pool_doc.id}: {str(e)}")
                continue

        if not all_pool_data:
            raise ValueError("No data found in Firebase")

        # Combine all data
        final_df = pd.concat(all_pool_data, ignore_index=True)
        logger.info(
            f"Total data loaded: {len(final_df)} data points from {len(all_pool_data)} pools"
        )

        # Print statistics
        logger.info("\nDataset statistics:")
        logger.info(f"Total rows: {len(final_df)}")
        logger.info(f"Unique pools: {final_df['poolAddress'].nunique()}")

        pool_counts = final_df["poolAddress"].value_counts()
        logger.info(
            f"Data points per pool - Mean: {pool_counts.mean():.1f}, Median: {pool_counts.median():.1f}"
        )
        logger.info(
            f"Data points per pool - Min: {pool_counts.min()}, Max: {pool_counts.max()}"
        )

        return final_df

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
        if "timestamp" in df.columns and not pd.api.types.is_datetime64_any_dtype(
            df["timestamp"]
        ):
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        # Sort by pool and timestamp
        df = df.sort_values(["poolAddress", "timestamp"])

        # Add any additional preprocessing steps here

        logger.info(f"Preprocessing complete. DataFrame shape: {df.shape}")
        return df
