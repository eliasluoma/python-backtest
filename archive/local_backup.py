#!/usr/bin/env python
"""
Script to back up Firebase pool data to local JSON files as quickly as possible.

This script downloads all marketContext data and saves it to local JSON files,
with each pool getting its own folder and each marketContexts document saved as JSON.

Usage:
    python local_backup.py
"""

import os
import json
import time
import logging
import argparse
import concurrent.futures
from datetime import datetime
from typing import List, Optional

# Import FirebaseService
try:
    from src.data.firebase_service import FirebaseService
except ImportError:
    import sys

    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from src.data.firebase_service import FirebaseService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"firebase_local_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
    ],
)
logger = logging.getLogger("firebase_local_backup")


class LocalBackup:
    """Class for backing up Firebase pools data to local JSON files."""

    def __init__(self, credential_path: Optional[str] = None):
        """Initialize with Firebase credentials."""
        self.firebase = FirebaseService(credential_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.backup_dir = os.path.join("backup", f"firebase_backup_{timestamp}")
        if not os.path.exists(self.backup_dir):
            os.makedirs(self.backup_dir)
        logger.info(f"Created backup directory: {self.backup_dir}")

    def backup_pool(self, pool_id: str) -> int:
        """
        Backup a single pool's data to a local folder.

        Args:
            pool_id: ID of the pool to backup

        Returns:
            Number of documents backed up for this pool
        """
        pool_dir = os.path.join(self.backup_dir, pool_id)
        if not os.path.exists(pool_dir):
            os.makedirs(pool_dir)

        # Get the main pool document
        source_doc = self.firebase.db.collection("marketContext").document(pool_id).get()
        if source_doc.exists:
            with open(os.path.join(pool_dir, "pool_data.json"), "w") as f:
                json.dump(source_doc.to_dict(), f)

        # Get all documents from marketContexts subcollection
        subcoll_path = f"marketContext/{pool_id}/marketContexts"
        subcoll_ref = self.firebase.db.collection(subcoll_path)

        # Create a new batch query to avoid timeouts
        batch_size = 500
        subcoll_docs = []
        batch_query = subcoll_ref.limit(batch_size)

        # Use batched queries to get all documents
        docs_counter = 0

        # Get the first batch
        batch_results = list(batch_query.stream())
        subcoll_docs.extend(batch_results)
        docs_counter += len(batch_results)

        # If we got a full batch, there might be more documents
        while len(batch_results) == batch_size:
            # Get the last document from the previous batch
            last_doc = batch_results[-1]
            # Set up a new query starting after the last document
            batch_query = subcoll_ref.start_after(last_doc).limit(batch_size)
            batch_results = list(batch_query.stream())
            subcoll_docs.extend(batch_results)
            docs_counter += len(batch_results)

        if docs_counter > 0:
            # Create marketContexts subdirectory
            contexts_dir = os.path.join(pool_dir, "marketContexts")
            if not os.path.exists(contexts_dir):
                os.makedirs(contexts_dir)

            # Save each document to a separate file
            for doc in subcoll_docs:
                filename = f"{doc.id}.json"
                with open(os.path.join(contexts_dir, filename), "w") as f:
                    json.dump(doc.to_dict(), f)

        logger.info(f"Backed up pool {pool_id}: {docs_counter} documents")
        return docs_counter

    def backup_pools_parallel(self, pool_ids: List[str], max_workers: int = 10) -> bool:
        """
        Backup multiple pools in parallel using a thread pool.

        Args:
            pool_ids: List of pool IDs to backup
            max_workers: Maximum number of concurrent workers

        Returns:
            True if backup succeeded
        """
        start_time = time.time()
        total_docs = 0
        successful_pools = 0
        failed_pools = 0

        logger.info(f"Starting parallel backup of {len(pool_ids)} pools using {max_workers} workers")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all pool backup tasks
            future_to_pool = {executor.submit(self.backup_pool, pool_id): pool_id for pool_id in pool_ids}

            # Process results as they complete
            for i, future in enumerate(concurrent.futures.as_completed(future_to_pool)):
                pool_id = future_to_pool[future]
                try:
                    docs_count = future.result()
                    total_docs += docs_count
                    successful_pools += 1

                    # Progress update
                    progress = (i + 1) / len(pool_ids)
                    elapsed = time.time() - start_time
                    remaining = (elapsed / progress) - elapsed if progress > 0 else 0
                    docs_per_sec = total_docs / elapsed if elapsed > 0 else 0

                    logger.info(
                        f"Progress: {progress:.1%} - Completed pool {pool_id} - "
                        f"{successful_pools}/{len(pool_ids)} pools - "
                        f"{total_docs} total documents - "
                        f"Speed: {docs_per_sec:.1f} docs/s - "
                        f"ETA: {remaining:.1f}s remaining"
                    )

                except Exception as e:
                    logger.error(f"Error backing up pool {pool_id}: {str(e)}")
                    failed_pools += 1

        # Save summary
        total_time = time.time() - start_time
        docs_per_second = total_docs / total_time if total_time > 0 else 0

        summary = {
            "timestamp": datetime.now().isoformat(),
            "backup_directory": self.backup_dir,
            "pools_processed": len(pool_ids),
            "successful_pools": successful_pools,
            "failed_pools": failed_pools,
            "total_documents": total_docs,
            "execution_time_seconds": total_time,
            "documents_per_second": docs_per_second,
        }

        # Save summary to a JSON file
        with open(os.path.join(self.backup_dir, "backup_summary.json"), "w") as f:
            json.dump(summary, f, indent=2)

        logger.info(f"Backup completed in {total_time:.2f}s")
        logger.info(f"Performance: {docs_per_second:.2f} docs/s")
        logger.info(f"Results: {successful_pools} pools with {total_docs} documents")
        logger.info(f"Backup directory: {self.backup_dir}")

        return failed_pools == 0

    def backup_all_pools(self, max_workers: int = 10) -> bool:
        """
        Backup all available pools to local JSON files.

        Args:
            max_workers: Maximum number of concurrent workers

        Returns:
            True if backup succeeded
        """
        # Get all available pool IDs
        all_pools = self.firebase.get_available_pools(limit=None)
        total_pools = len(all_pools)

        if total_pools == 0:
            logger.error("No pools found to backup. Aborting.")
            return False

        logger.info(f"Found {total_pools} pools to backup")

        # Backup all pools in parallel
        return self.backup_pools_parallel(all_pools, max_workers)


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Backup Firebase pools data to local JSON files")
    parser.add_argument("--credential-path", help="Path to Firebase credentials JSON file")
    parser.add_argument("--workers", type=int, default=10, help="Number of parallel workers")

    args = parser.parse_args()

    backup = LocalBackup(credential_path=args.credential_path)
    success = backup.backup_all_pools(max_workers=args.workers)

    exit(0 if success else 1)


if __name__ == "__main__":
    main()
