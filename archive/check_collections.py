#!/usr/bin/env python
"""
Script to examine Firestore collections and subcollections
"""

import sys
import os
from pprint import pprint

# Import FirebaseService
try:
    from src.data.firebase_service import FirebaseService
except ImportError:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from src.data.firebase_service import FirebaseService


def main():
    """Main function to examine Firestore collections"""
    print("Initializing Firebase connection...")
    fs = FirebaseService()

    # Get all collections
    print("\nListing all top-level collections:")
    collections = [c.id for c in fs.db.collections()]
    print(collections)

    # Check marketContext collection
    print("\nExamining marketContext collection...")
    market_docs = list(fs.db.collection("marketContext").limit(5).stream())
    print(f"Found {len(market_docs)} documents in marketContext collection")

    # Try to get available pools
    print("\nTrying to get available pools...")
    try:
        available_pools = fs.get_available_pools(limit=5)
        print(f"Found {len(available_pools)} pools")
        if available_pools:
            print("Sample pool IDs:")
            for pool_id in available_pools[:5]:
                print(f"  - {pool_id}")

            # Try to directly access subcollections for a pool
            if available_pools:
                sample_pool = available_pools[0]
                print(f"\nChecking for subcollections under pool {sample_pool}...")
                # This approach might work even if the parent document doesn't exist
                subcoll_path = f"marketContext/{sample_pool}/marketContexts"
                subcoll_docs = list(fs.db.collection(subcoll_path).limit(5).stream())
                print(f"Found {len(subcoll_docs)} documents in {subcoll_path}")

                if subcoll_docs:
                    print("Sample document fields:")
                    pprint(subcoll_docs[0].to_dict())
    except Exception as e:
        print(f"Error getting available pools: {e}")

    # Check marketContextStatus collection
    print("\nExamining marketContextStatus collection...")
    status_docs = list(fs.db.collection("marketContextStatus").limit(5).stream())
    print(f"Found {len(status_docs)} documents in marketContextStatus collection")
    if status_docs:
        print("Sample document fields:")
        pprint(status_docs[0].to_dict())

    # Check one of the backup collections
    backup_collection = "backup_marketContext_20250320_104135"
    print(f"\nExamining backup collection {backup_collection}...")
    backup_docs = list(fs.db.collection(backup_collection).limit(5).stream())
    print(f"Found {len(backup_docs)} documents in {backup_collection}")
    if backup_docs:
        for doc in backup_docs[:2]:
            print(f"Document ID: {doc.id}")
            if doc.id != "backup_summary":  # Skip the summary document
                subcoll_path = f"{backup_collection}/{doc.id}/marketContexts"
                subcoll_docs = list(fs.db.collection(subcoll_path).limit(5).stream())
                print(f"Found {len(subcoll_docs)} documents in {subcoll_path}")


if __name__ == "__main__":
    main()
