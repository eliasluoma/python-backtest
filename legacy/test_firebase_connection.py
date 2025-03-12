#!/usr/bin/env python3
"""
Test script for validating Firebase connection and data loading
"""

import os
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import pytz
from datetime import datetime

# Import only the data loading function
from fetch_firebase_data import load_market_contexts_to_csv


def initialize_firebase_test():
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


def test_firebase_connection():
    """Test basic Firebase connectivity"""
    print("\n=== Testing Firebase Connection ===")
    try:
        db = initialize_firebase_test()
        print("✅ Firebase connection successful!")

        # Test if we can access the marketContext collection
        market_contexts_ref = db.collection("marketContext")
        pool_docs = list(market_contexts_ref.limit(3).list_documents())

        if pool_docs:
            print(f"✅ Successfully queried Firebase. Found {len(pool_docs)} pool document(s).")
            print(f"Example pool address: {pool_docs[0].id}")
        else:
            print("⚠️ Connection works but no pool documents found in marketContext collection.")

        return True
    except Exception as e:
        print(f"❌ Firebase connection failed: {str(e)}")
        return False


def test_data_loading():
    """Test loading data with different parameters"""
    print("\n=== Testing Data Loading ===")
    print("NOTE: This test uses the fetch_firebase_data.py module which may use a different credentials file.")
    print("Consider updating the original file if tests fail.")

    # Test 1: Load with cache if available
    print("\nTest 1: Loading with cache (if available)")
    try:
        df1 = load_market_contexts_to_csv(output_file="test_pool_data.csv", use_cache=True)
        print(
            f"✅ Successfully loaded {len(df1)} records from {'cache' if os.path.exists('test_pool_data.csv') else 'Firebase'}"
        )
    except Exception as e:
        print(f"❌ Failed to load data with cache: {str(e)}")

    # Test 2: Force reload from Firebase
    print("\nTest 2: Force reload from Firebase")
    try:
        df2 = load_market_contexts_to_csv(output_file="test_pool_data_force.csv", use_cache=False)
        print(f"✅ Successfully loaded {len(df2)} records directly from Firebase")
    except Exception as e:
        print(f"❌ Failed to force reload from Firebase: {str(e)}")

    # Test 3: Load limited data for quick test
    print("\nTest 3: Analyzing loaded data")
    try:
        if "df2" in locals() and not df2.empty:
            # Show some basic analysis
            num_pools = df2["poolAddress"].nunique()
            print(f"✅ Data contains {num_pools} unique pools")

            # Check for key columns
            expected_columns = ["timestamp", "marketCap", "poolAddress"]
            missing_columns = [col for col in expected_columns if col not in df2.columns]

            if missing_columns:
                print(f"⚠️ Missing expected columns: {missing_columns}")
            else:
                print("✅ All expected columns found")

            # Check data date range
            if "timestamp" in df2.columns:
                # Convert to datetime if needed
                if df2["timestamp"].dtype == "object":
                    df2["timestamp"] = pd.to_datetime(df2["timestamp"], format="mixed", utc=True)

                min_date = df2["timestamp"].min()
                max_date = df2["timestamp"].max()
                print(f"✅ Data date range: {min_date} to {max_date}")
    except Exception as e:
        print(f"❌ Failed to analyze data: {str(e)}")


if __name__ == "__main__":
    print("FIREBASE CONNECTION AND DATA LOADING TEST")
    print("========================================")

    # Test Firebase connection
    connection_success = test_firebase_connection()

    # Only continue with data loading tests if connection successful
    if connection_success:
        test_data_loading()
    else:
        print("\n❌ Skipping data loading tests due to connection failure")

    print("\n========================================")
    print("Test complete!")
