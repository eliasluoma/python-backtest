#!/usr/bin/env python3
"""
Test script for Firebase connection and pool data retrieval without CSV export
"""

import firebase_admin
from firebase_admin import credentials, firestore
import pytz
from datetime import datetime
import pandas as pd


def initialize_firebase():
    """Initialize Firebase with the correct credentials file"""
    if not firebase_admin._apps:
        try:
            # Use the correct JSON credential file
            cred = credentials.Certificate("firebase-key.json")
            firebase_admin.initialize_app(cred)
            return firestore.client()
        except Exception as e:
            print(f"Error initializing Firebase connection: {str(e)}")
            print("Make sure the firebase-key.json file exists and is valid")
            raise


def fetch_pool_data(limit_pools=None, limit_contexts_per_pool=None):
    """
    Fetch pool data from Firebase without saving to CSV

    Args:
        limit_pools: Optional limit on number of pools to fetch (for testing)
        limit_contexts_per_pool: Optional limit on contexts per pool (for testing)

    Returns:
        DataFrame containing the pool data
    """
    print("\n=== Connecting to Firebase ===")
    db = initialize_firebase()
    print("✅ Firebase connection successful!")

    print("\n=== Fetching Pool Data ===")
    market_contexts_ref = db.collection("marketContext")

    # Get all pool documents (or limited number for testing)
    pool_docs = list(market_contexts_ref.list_documents())
    if limit_pools:
        pool_docs = pool_docs[:limit_pools]

    total_pools = len(pool_docs)
    print(f"Found {total_pools} pools")

    all_pool_data = []
    for idx, pool_doc in enumerate(pool_docs, 1):
        try:
            query = pool_doc.collection("marketContexts").order_by("timestamp")
            if limit_contexts_per_pool:
                query = query.limit(limit_contexts_per_pool)

            contexts = query.get()

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

                data["poolAddress"] = pool_doc.id
                pool_contexts.append(data)

            if pool_contexts:
                df = pd.DataFrame(pool_contexts)
                all_pool_data.append(df)
                print(f"[{idx}/{total_pools}] Loaded {len(pool_contexts)} data points for pool {pool_doc.id}")

        except Exception as e:
            print(f"Error processing pool {pool_doc.id}: {str(e)}")
            continue

    if not all_pool_data:
        raise ValueError("No data found in Firebase")

    # Combine all data
    final_df = pd.concat(all_pool_data, ignore_index=True)
    print(f"\nTotal loaded: {len(final_df)} data points from {len(all_pool_data)} pools")

    return final_df


def analyze_pool_data(df):
    """Display analytics about the pool data"""
    print("\n=== Pool Data Analysis ===")
    print(f"Total records: {len(df)}")
    print(f"Unique pools: {df['poolAddress'].nunique()}")

    print("\nData points per pool:")
    pool_counts = df["poolAddress"].value_counts()
    print(f"Average: {pool_counts.mean():.1f}")
    print(f"Median: {pool_counts.median():.1f}")
    print(f"Min: {pool_counts.min()}")
    print(f"Max: {pool_counts.max()}")

    if "marketCap" in df.columns:
        print("\nMarket Cap Statistics:")
        print(f"Min: {df['marketCap'].min()}")
        print(f"Max: {df['marketCap'].max()}")
        print(f"Mean: {df['marketCap'].mean()}")

    if "timestamp" in df.columns:
        print("\nDate Range:")
        print(f"Earliest: {df['timestamp'].min()}")
        print(f"Latest: {df['timestamp'].max()}")

    # Sample data preview
    print("\nSample Data:")
    print(df.head(3))


if __name__ == "__main__":
    print("FIREBASE POOL DATA FETCHING TEST")
    print("================================")

    # For testing, limit to 5 pools with 10 context records each
    # Remove the limits for a full data fetch
    df = fetch_pool_data(limit_pools=5, limit_contexts_per_pool=10)

    # Analyze the retrieved data
    analyze_pool_data(df)

    print("\n================================")
    print("Test complete! Data loaded successfully (not saved to CSV)")

    # The data is available in the DataFrame if you want to do more with it
    print("\nThe data is available in the 'df' variable if you want to do more with it")
    print("You can access it in an interactive Python session")
