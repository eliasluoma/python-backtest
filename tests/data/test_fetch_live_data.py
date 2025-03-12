#!/usr/bin/env python
"""
Test script for fetching live data from Firebase.
This script will attempt to connect to Firebase using your credentials
and fetch real market data.
"""

import sys
import os
import pandas as pd
from datetime import datetime
import logging
import firebase_admin
from firebase_admin import credentials, firestore

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("FirebaseDataTest")


def fetch_pool_data_directly(db, pool_id, limit=10):
    """Fetch data directly from the marketContexts subcollection."""
    try:
        # Get the pool document
        pool_doc = db.collection("marketContext").document(pool_id)

        # Get the marketContexts subcollection
        contexts_collection = pool_doc.collection("marketContexts")

        # Fetch documents
        contexts = list(contexts_collection.limit(limit).stream())

        # Convert to list of dictionaries
        data = []
        for doc in contexts:
            item = doc.to_dict()
            item["pool_address"] = pool_id
            data.append(item)

        # Convert to DataFrame
        if data:
            df = pd.DataFrame(data)
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error fetching data for pool {pool_id}: {str(e)}")
        return pd.DataFrame()


def test_fetch_live_data():
    """Test fetching live data from Firebase."""
    logger.info("Starting test to fetch live data from Firebase")

    # Find the firebase key file
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    firebase_key_path = os.path.join(root_dir, "firebase-key.json")

    if os.path.exists(firebase_key_path):
        logger.info(f"Found Firebase key file: {firebase_key_path}")
    else:
        logger.warning(f"Firebase key file not found at: {firebase_key_path}")
        logger.info("Trying with .env.local instead")
        firebase_key_path = None

    # Initialize Firebase directly to ensure we have control
    try:
        # Initialize Firebase with the credentials
        if firebase_admin._apps:
            # Already initialized
            db = firestore.client()
            logger.info("Using existing Firebase connection")
        else:
            # Initialize new connection
            if firebase_key_path:
                cred = credentials.Certificate(firebase_key_path)
                firebase_admin.initialize_app(cred)
                db = firestore.client()
                logger.info("Firebase initialized with credentials file")
            else:
                logger.error("No valid credentials file found")
                return

        logger.info("Firebase connection established")

        # Get top 5 pool IDs from marketContext collection
        pools_collection = db.collection("marketContext")
        pools = list(pools_collection.list_documents())[:5]  # Get all documents then limit to 5

        if not pools:
            logger.warning("No pools found in marketContext collection")
            return

        logger.info(f"Found {len(pools)} pools to analyze")

        # Fetch data for each pool directly
        all_pools_data = {}
        for pool in pools:
            pool_id = pool.id
            logger.info(f"Fetching data for pool: {pool_id}")

            # Get data directly from the marketContexts subcollection
            df = fetch_pool_data_directly(db, pool_id, limit=20)

            if not df.empty:
                all_pools_data[pool_id] = df
                logger.info(f"Retrieved {len(df)} records for pool {pool_id}")
            else:
                logger.warning(f"No data found for pool {pool_id}")

        # Report on the data retrieved
        if not all_pools_data:
            logger.warning("No data was retrieved from any pools")
            return

        logger.info(f"Successfully retrieved data for {len(all_pools_data)} pools")

        # Print summary information for each pool
        for pool_id, df in all_pools_data.items():
            logger.info(f"Pool {pool_id}: {len(df)} data points")

            # Show available columns
            if not df.empty:
                # Convert timestamp if it's a Firestore timestamp
                if "timestamp" in df.columns:
                    logger.info("  - Converting timestamp column")
                    timestamps = []
                    for ts in df["timestamp"]:
                        if hasattr(ts, "seconds"):
                            timestamps.append(datetime.fromtimestamp(ts.seconds))
                        else:
                            timestamps.append(ts)
                    df["timestamp"] = timestamps

                logger.info(f"  - Available columns: {', '.join(df.columns)}")

                # Show data sample
                logger.info("  - Data sample (first 2 rows):")
                sample = df.head(2).to_dict("records")
                for i, record in enumerate(sample):
                    # Limit the output to prevent overflow
                    record_summary = {k: v for k, v in list(record.items())[:10]}
                    logger.info(f"    Row {i} (partial): {record_summary}...")
            else:
                logger.info("  - DataFrame is empty")

            # Add a separator for better readability
            logger.info("-" * 80)

    except Exception as e:
        logger.error(f"Error connecting to Firebase: {str(e)}")


if __name__ == "__main__":
    test_fetch_live_data()
