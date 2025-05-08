#!/usr/bin/env python
"""
Script to backup all data from Firestore marketContext collection.

This script creates a backup of all pools in the marketContext collection,
including their marketContexts subcollections. It works even when parent documents
don't exist, using pool IDs from the get_available_pools method.

Usage:
    python backup_firebase_pools.py

Optional arguments:
    --source-collection: Name of the source collection (default: "marketContext")
    --backup-name: Custom name for backup collection (default: "backup_marketContext_<timestamp>")
    --credential-path: Path to Firebase credentials file
    --batch-size: Number of pools to process in each batch (default: 10)
    --context-batch-size: Number of context documents to write in each batch (default: 300)
"""

import os
import time
import argparse
import logging
from datetime import datetime
from typing import Optional

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
        logging.FileHandler(f"firebase_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
    ],
)
logger = logging.getLogger("firebase_backup")


class FirebaseBackup:
    """Class for backing up Firebase pools data."""

    def __init__(self, credential_path: Optional[str] = None):
        """Initialize with Firebase credentials."""
        self.firebase = FirebaseService(credential_path)

    def backup_entire_collection(
        self,
        source_collection: str = "marketContext",
        backup_name: Optional[str] = None,
        batch_size: int = 10,
        context_batch_size: int = 300,
    ) -> bool:
        """
        Create a backup of all pools and their marketContexts subcollections.

        This works even when parent documents don't exist, using pool IDs from get_available_pools.

        Args:
            source_collection: Name of the source collection (default: "marketContext")
            backup_name: Custom name for backup collection (if None, uses timestamp)
            batch_size: Number of pools to process in each batch
            context_batch_size: Number of context documents to write in each batch

        Returns:
            True if backup was successful, False otherwise
        """
        # Generate backup collection name with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_collection_name = (
            f"{backup_name}_{timestamp}" if backup_name else f"backup_{source_collection}_{timestamp}"
        )

        logger.info(f"Starting backup of {source_collection} to {backup_collection_name}...")
        start_time = time.time()

        # Statistics tracking
        copied_main_docs = 0
        copied_subcoll_docs = 0
        failed_docs = 0

        try:
            # Get all available pool IDs using the provided method
            # This works even when parent documents don't exist in the collection
            all_pools = self.firebase.get_available_pools(limit=None)
            total_pools = len(all_pools)

            if total_pools == 0:
                logger.error("No pools found to backup. Aborting.")
                return False

            logger.info(f"Found {total_pools} pools to backup")

            # Process pools in batches
            for i in range(0, total_pools, batch_size):
                batch_start = time.time()
                current_batch = all_pools[i : i + batch_size]
                logger.info(
                    f"Processing batch {i//batch_size + 1}/{(total_pools-1)//batch_size + 1}: {len(current_batch)} pools"
                )

                for pool_id in current_batch:
                    try:
                        # Create the target document for this pool
                        target_doc = self.firebase.db.collection(backup_collection_name).document(pool_id)

                        # Try to get the source document (it may not exist)
                        source_doc = self.firebase.db.collection(source_collection).document(pool_id).get()

                        # Copy document data if it exists, otherwise create an empty document
                        if source_doc.exists:
                            target_doc.set(source_doc.to_dict())
                        else:
                            # Create an empty document with just the pool ID
                            target_doc.set({"poolAddress": pool_id})

                        copied_main_docs += 1

                        # Access the marketContexts subcollection
                        subcoll_path = f"{source_collection}/{pool_id}/marketContexts"
                        logger.info(f"Accessing marketContexts for pool {pool_id}")

                        try:
                            # Get all documents from the subcollection
                            subcoll_ref = self.firebase.db.collection(subcoll_path)
                            subcoll_docs = list(subcoll_ref.stream())

                            if subcoll_docs:
                                doc_count = len(subcoll_docs)
                                logger.info(f"Found {doc_count} marketContexts documents for pool {pool_id}")

                                # Process in smaller batches
                                for j in range(0, doc_count, context_batch_size):
                                    current_contexts = subcoll_docs[j : j + context_batch_size]
                                    logger.info(
                                        f"Processing batch {j//context_batch_size + 1}/{(doc_count-1)//context_batch_size + 1} "
                                        f"for pool {pool_id} ({len(current_contexts)} documents)"
                                    )

                                    # Create batch for efficient writing
                                    write_batch = self.firebase.db.batch()

                                    for context_doc in current_contexts:
                                        # Create reference to target context document
                                        target_context_ref = target_doc.collection("marketContexts").document(
                                            context_doc.id
                                        )
                                        write_batch.set(target_context_ref, context_doc.to_dict())

                                    # Commit the batch
                                    write_batch.commit()
                                    copied_subcoll_docs += len(current_contexts)
                                    logger.info(
                                        f"Committed {len(current_contexts)} context documents for pool {pool_id}"
                                    )

                                    # Give Firebase a short break
                                    time.sleep(0.1)
                            else:
                                logger.warning(f"No marketContexts documents found for pool {pool_id}")
                        except Exception as e:
                            logger.error(f"Error copying marketContexts for pool {pool_id}: {str(e)}")
                            failed_docs += 1

                    except Exception as e:
                        logger.error(f"Error processing pool {pool_id}: {str(e)}")
                        failed_docs += 1

                # Log batch performance
                batch_time = time.time() - batch_start
                docs_per_sec = (len(current_batch) + copied_subcoll_docs) / batch_time if batch_time > 0 else 0
                logger.info(f"Batch completed in {batch_time:.2f}s ({docs_per_sec:.2f} docs/s)")

                # Overall progress update
                elapsed = time.time() - start_time
                progress = (i + len(current_batch)) / total_pools
                remaining = (elapsed / progress) - elapsed if progress > 0 else 0

                logger.info(
                    f"Progress: {progress:.1%} - Pools: {copied_main_docs}/{total_pools} - "
                    f"Context docs: {copied_subcoll_docs} - "
                    f"Time: {elapsed:.1f}s elapsed, ~{remaining:.1f}s remaining"
                )

                # Add a delay between batches
                time.sleep(0.5)

            # Save summary
            total_time = time.time() - start_time
            total_docs = copied_main_docs + copied_subcoll_docs
            docs_per_second = total_docs / total_time if total_time > 0 else 0

            summary = {
                "timestamp": datetime.now().isoformat(),
                "source_collection": source_collection,
                "pool_documents_copied": copied_main_docs,
                "context_documents_copied": copied_subcoll_docs,
                "total_documents_copied": total_docs,
                "failed_operations": failed_docs,
                "execution_time_seconds": total_time,
            }

            # Save summary to Firestore
            self.firebase.db.collection(backup_collection_name).document("backup_summary").set(summary)

            logger.info(f"Backup completed in {total_time:.2f}s")
            logger.info(f"Performance: {docs_per_second:.2f} docs/s")
            logger.info(f"Results: {copied_main_docs} pools with {copied_subcoll_docs} context documents")
            logger.info(f"Backup collection: '{backup_collection_name}'")

            return failed_docs == 0

        except Exception as e:
            logger.error(f"Error during backup: {str(e)}")
            return False


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Backup all data from Firestore marketContext collection")
    parser.add_argument("--source-collection", default="marketContext", help="Name of the source collection")
    parser.add_argument("--backup-name", help="Custom name for the backup collection")
    parser.add_argument("--credential-path", help="Path to Firebase credentials JSON file")
    parser.add_argument("--batch-size", type=int, default=10, help="Number of pools to process in each batch")
    parser.add_argument("--context-batch-size", type=int, default=300, help="Number of context documents per batch")

    args = parser.parse_args()

    backup = FirebaseBackup(credential_path=args.credential_path)

    success = backup.backup_entire_collection(
        source_collection=args.source_collection,
        backup_name=args.backup_name,
        batch_size=args.batch_size,
        context_batch_size=args.context_batch_size,
    )

    exit(0 if success else 1)


if __name__ == "__main__":
    main()
