#!/usr/bin/env python
"""
Script to analyze and clean up Firebase pools data that:
1. Have fewer than 600 rows/seconds of data
2. Don't have volume data in the expected format
3. Backs up data before deletion

Usage:
    python clean_firebase_pools.py --analyze     # Just analyze, don't delete anything
    python clean_firebase_pools.py --cleanup     # Run the cleanup operation
    python clean_firebase_pools.py --backup      # Only perform a backup
    python clean_firebase_pools.py --check-ref   # Check reference pools structure
    python clean_firebase_pools.py --backup --firestore-backup  # Use faster Firestore-to-Firestore backup
    python clean_firebase_pools.py --backup-collection  # Backup entire collection at once (fastest)

The --firestore-backup flag can be used with both --backup and --cleanup operations
to create backups directly within Firestore instead of downloading data locally.

The --backup-collection option is the fastest way to backup all data since it copies
the entire collection at once instead of processing pools individually.
"""

import os
import json
import time
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# Import FirebaseService from the project
try:
    from src.data.firebase_service import FirebaseService
except ImportError:
    import sys

    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    from src.data.firebase_service import FirebaseService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"firebase_cleanup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
    ],
)
logger = logging.getLogger("firebase_cleanup")


class FirebasePoolCleaner:
    """Class for analyzing and cleaning Firebase pools data."""

    def __init__(self, credential_path: Optional[str] = None):
        """Initialize the cleaner with Firebase credentials."""
        self.firebase = FirebaseService(credential_path)
        self.backup_dir = os.path.join(os.getcwd(), "firebase_backup", datetime.now().strftime("%Y%m%d_%H%M%S"))

        # Create backup directory if it doesn't exist
        if not os.path.exists(self.backup_dir):
            os.makedirs(self.backup_dir)
            logger.info(f"Created backup directory: {self.backup_dir}")

        # Reference pools from check_fields.py
        self.ref_pools = {
            "pool1_format": "2vpAeyJCX7Wi93cXLuSaZYZb78JGSCjYML345jW3DUN2",
            "pool2_format": "12H7zN3gXRfUeu2fCQjooPAofbBL2wz7X7wKoS44oJkX",
        }

    def analyze_reference_pools(self) -> None:
        """
        Analyze the reference pools to understand their structure.
        This helps in understanding what to look for when cleaning/importing data.
        """
        logger.info("Analyzing reference pools structure...")

        results = {}

        for pool_type, pool_id in self.ref_pools.items():
            logger.info(f"Analyzing {pool_type} pool: {pool_id}")

            try:
                # Fetch pool data
                pool_data = self.firebase.fetch_pool_data(pool_id)

                if pool_data.empty:
                    logger.warning(f"No data found for {pool_type} pool {pool_id}")
                    continue

                # Get data shape
                data_points = len(pool_data)
                fields_count = len(pool_data.columns)

                # Get the first row
                first_row = pool_data.iloc[0].to_dict()

                # Get the flattened row
                flattened_row = self.firebase.prepare_for_database(first_row)
                fields_count_flat = len(flattened_row)

                # Check for volume data in different formats
                volume_fields = []

                # Check for flattened format (trade_last5Seconds_volume_buy)
                for col in pool_data.columns:
                    if "volume" in col.lower() and ("buy" in col.lower() or "sell" in col.lower()):
                        volume_fields.append(col)

                # Check for nested format
                nested_volume_fields = []
                if "tradeLast5Seconds" in first_row and isinstance(first_row["tradeLast5Seconds"], dict):
                    if "volume" in first_row["tradeLast5Seconds"]:
                        nested_volume_fields.append("tradeLast5Seconds.volume")

                if "tradeLast10Seconds" in first_row and isinstance(first_row["tradeLast10Seconds"], dict):
                    if "volume" in first_row["tradeLast10Seconds"]:
                        nested_volume_fields.append("tradeLast10Seconds.volume")

                # Store results
                results[pool_type] = {
                    "pool_id": pool_id,
                    "data_points": data_points,
                    "fields_count": fields_count,
                    "fields_count_flattened": fields_count_flat,
                    "volume_fields": volume_fields,
                    "nested_volume_fields": nested_volume_fields,
                    "sample_fields": list(pool_data.columns[:20]),  # First 20 fields
                    "sample_fields_flattened": list(flattened_row.keys())[:20],  # First 20 flattened fields
                }

                # Search for indicators of data quality
                has_timestamp = "timestamp" in pool_data.columns
                has_market_cap = any("marketCap" in col for col in pool_data.columns)

                results[pool_type]["has_timestamp"] = has_timestamp
                results[pool_type]["has_market_cap"] = has_market_cap

                logger.info(f"Pool {pool_id} ({pool_type}) analysis complete:")
                logger.info(f"  - Data points: {data_points}")
                logger.info(f"  - Fields: {fields_count} (original), {fields_count_flat} (flattened)")
                logger.info(
                    f"  - Volume fields: {len(volume_fields)} (flattened), {len(nested_volume_fields)} (nested)"
                )

            except Exception as e:
                logger.error(f"Error analyzing reference pool {pool_id}: {e}")
                results[pool_type] = {"error": str(e)}

        # Save results to file
        with open(os.path.join(self.backup_dir, "reference_pools_analysis.json"), "w") as f:
            json.dump(results, f, indent=2)

        logger.info(
            f"Reference pools analysis complete. Results saved to {self.backup_dir}/reference_pools_analysis.json"
        )

        # Print summary of key differences
        logger.info("\n=== REFERENCE POOLS STRUCTURE COMPARISON ===")
        if "pool1_format" in results and "pool2_format" in results:
            p1 = results["pool1_format"]
            p2 = results["pool2_format"]

            if isinstance(p1, dict) and isinstance(p2, dict) and "error" not in p1 and "error" not in p2:
                logger.info(
                    f"Pool 1 format: {p1.get('fields_count', 'N/A')} fields ({p1.get('fields_count_flattened', 'N/A')} flattened)"
                )
                logger.info(
                    f"Pool 2 format: {p2.get('fields_count', 'N/A')} fields ({p2.get('fields_count_flattened', 'N/A')} flattened)"
                )

                # Compare volume field structure
                logger.info("\nVolume data structure:")
                logger.info(f"Pool 1 volume fields: {p1.get('volume_fields', [])}")
                logger.info(f"Pool 1 nested volume: {p1.get('nested_volume_fields', [])}")
                logger.info(f"Pool 2 volume fields: {p2.get('volume_fields', [])}")
                logger.info(f"Pool 2 nested volume: {p2.get('nested_volume_fields', [])}")

                # Identify common and different fields
                if "sample_fields" in p1 and "sample_fields" in p2:
                    p1_fields = set(p1["sample_fields"])
                    p2_fields = set(p2["sample_fields"])
                    common_fields = p1_fields.intersection(p2_fields)
                    p1_only = p1_fields - p2_fields
                    p2_only = p2_fields - p1_fields

                    logger.info("\nCommon fields: ")
                    for field in sorted(common_fields):
                        logger.info(f"  - {field}")

                    logger.info("\nPool 1 specific fields: ")
                    for field in sorted(p1_only):
                        logger.info(f"  - {field}")

                    logger.info("\nPool 2 specific fields: ")
                    for field in sorted(p2_only):
                        logger.info(f"  - {field}")

    def analyze_pools(self) -> Tuple[Dict[str, int], List[str], List[str], List[str]]:
        """
        Analyze all pools in Firebase to identify which ones should be deleted.

        Returns:
            Tuple containing:
            - Dictionary mapping pool IDs to their data point counts
            - List of pool IDs with less than 600 data points
            - List of pool IDs with no volume data
            - List of pool IDs that should be kept
        """
        logger.info("Starting pool analysis...")

        # Get all available pools
        all_pools = self.firebase.get_available_pools(limit=None)
        logger.info(f"Found {len(all_pools)} pools in Firebase")

        # Get the number of data points for each pool
        pools_counts = self.firebase.get_pools_datapoints_counts(all_pools)
        logger.info(f"Retrieved data point counts for {len(pools_counts)} pools")

        # Identify pools with less than 600 data points
        low_data_pools = [pool_id for pool_id, count in pools_counts.items() if count < 600]
        logger.info(f"Found {len(low_data_pools)} pools with less than 600 data points")

        # Identify pools with no volume data
        no_volume_pools = []
        volume_check_limit = min(100, len(all_pools))  # Limit checks to avoid timeout

        logger.info(f"Checking volume data for {volume_check_limit} pools...")
        for i, pool_id in enumerate(all_pools[:volume_check_limit]):
            if i % 10 == 0:  # Log progress every 10 pools
                logger.info(f"Checking volume data: {i}/{volume_check_limit} pools")

            try:
                # Get a sample of data for this pool to check for volume data
                pool_data = self.firebase.fetch_pool_data(pool_id, collection_name="marketContext")

                # Skip if no data returned
                if pool_data.empty:
                    no_volume_pools.append(pool_id)
                    continue

                # Check for volume data in different formats
                has_volume = False

                # Check flattened format (trade_last5Seconds_volume_buy)
                volume_cols = [
                    col
                    for col in pool_data.columns
                    if ("volume" in col.lower() and ("buy" in col.lower() or "sell" in col.lower()))
                ]

                # If no volume columns found in flattened format, check nested format
                if not volume_cols:
                    # Check for tradeLast5Seconds or tradeLast10Seconds nested structures
                    if "tradeLast5Seconds" in pool_data.columns or "tradeLast10Seconds" in pool_data.columns:
                        # Sample first row to check if volume data exists in nested structure
                        first_row = pool_data.iloc[0].to_dict()

                        # Check tradeLast5Seconds
                        if (
                            "tradeLast5Seconds" in first_row
                            and isinstance(first_row["tradeLast5Seconds"], dict)
                            and "volume" in first_row["tradeLast5Seconds"]
                        ):
                            has_volume = True

                        # Check tradeLast10Seconds
                        if (
                            "tradeLast10Seconds" in first_row
                            and isinstance(first_row["tradeLast10Seconds"], dict)
                            and "volume" in first_row["tradeLast10Seconds"]
                        ):
                            has_volume = True
                else:
                    has_volume = True

                if not has_volume:
                    no_volume_pools.append(pool_id)
                    logger.debug(f"Pool {pool_id} has no volume data")

            except Exception as e:
                logger.error(f"Error checking volume data for pool {pool_id}: {e}")
                # Add to no_volume_pools if we couldn't verify
                no_volume_pools.append(pool_id)

        logger.info(f"Found {len(no_volume_pools)} pools with no volume data")

        # Identify pools to keep (not in either removal list)
        pools_to_keep = [
            pool_id
            for pool_id in all_pools
            if pool_id not in low_data_pools and pool_id not in no_volume_pools[:volume_check_limit]
        ]

        logger.info(f"Analysis complete. Found {len(pools_to_keep)} pools to keep.")

        return pools_counts, low_data_pools, no_volume_pools, pools_to_keep

    def backup_pools(self, pool_ids: List[str]) -> bool:
        """
        Backup pool data before deletion.

        Args:
            pool_ids: List of pool IDs to backup

        Returns:
            True if backup was successful, False otherwise
        """
        logger.info(f"Starting backup of {len(pool_ids)} pools...")

        success_count = 0
        error_count = 0

        # Backup pools data in batches to avoid memory issues
        batch_size = 20
        for i in range(0, len(pool_ids), batch_size):
            batch = pool_ids[i : i + batch_size]
            logger.info(f"Backing up batch {i//batch_size + 1}: {len(batch)} pools")

            for pool_id in batch:
                try:
                    # Fetch pool data
                    pool_data = self.firebase.fetch_pool_data(pool_id)

                    if not pool_data.empty:
                        # Convert to JSON and save to file
                        json_data = pool_data.to_json(orient="records", date_format="iso")

                        # Sanitize pool_id for filename
                        safe_pool_id = "".join(c if c.isalnum() else "_" for c in pool_id)

                        # Save to backup file
                        backup_file = os.path.join(self.backup_dir, f"{safe_pool_id}.json")
                        with open(backup_file, "w") as f:
                            f.write(json_data)

                        logger.debug(f"Successfully backed up pool {pool_id} ({len(pool_data)} records)")
                        success_count += 1
                    else:
                        logger.warning(f"No data to backup for pool {pool_id}")
                        error_count += 1

                except Exception as e:
                    logger.error(f"Error backing up pool {pool_id}: {e}")
                    error_count += 1

            # Small delay between batches to avoid overloading Firebase
            time.sleep(1)

        # Save summary information about the backup
        summary = {
            "timestamp": datetime.now().isoformat(),
            "total_pools": len(pool_ids),
            "successful_backups": success_count,
            "failed_backups": error_count,
            "pool_ids": pool_ids,
        }

        with open(os.path.join(self.backup_dir, "backup_summary.json"), "w") as f:
            json.dump(summary, f, indent=2)

        logger.info(f"Backup completed. {success_count} successful, {error_count} failed.")
        return error_count == 0

    def delete_pools(self, pool_ids: List[str], dry_run: bool = True) -> int:
        """
        Delete pools from Firebase.

        Args:
            pool_ids: List of pool IDs to delete
            dry_run: If True, don't actually delete, just simulate

        Returns:
            Number of successfully deleted pools
        """
        if dry_run:
            logger.info(f"DRY RUN: Would delete {len(pool_ids)} pools")
            return 0

        logger.info(f"Starting deletion of {len(pool_ids)} pools...")

        success_count = 0
        error_count = 0

        # Delete in batches to avoid overloading Firebase
        batch_size = 10
        for i in range(0, len(pool_ids), batch_size):
            batch = pool_ids[i : i + batch_size]
            logger.info(f"Deleting batch {i//batch_size + 1}: {len(batch)} pools")

            for pool_id in batch:
                try:
                    # Get reference to the pool document
                    pool_doc = self.firebase.db.collection("marketContext").document(pool_id)

                    # Get all documents from the marketContexts subcollection
                    subcollection = pool_doc.collection("marketContexts")
                    docs = list(subcollection.stream())

                    # Delete each document in the subcollection
                    for doc in docs:
                        doc.reference.delete()

                    # Then delete the pool document itself
                    pool_doc.delete()

                    logger.info(f"Successfully deleted pool {pool_id} with {len(docs)} data points")
                    success_count += 1

                except Exception as e:
                    logger.error(f"Error deleting pool {pool_id}: {e}")
                    error_count += 1

            # Small delay between batches to avoid overloading Firebase
            time.sleep(2)

        logger.info(f"Deletion completed. {success_count} successful, {error_count} failed.")
        return success_count

    def backup_pools_in_firestore(self, pool_ids: List[str], backup_collection_name: str = "backup_pools") -> bool:
        """
        Create a faster backup by copying pools directly within Firestore.
        This is much faster than downloading data locally.

        Args:
            pool_ids: List of pool IDs to backup
            backup_collection_name: Name of the collection to store backups in

        Returns:
            True if backup was successful, False otherwise
        """
        logger.info(f"Starting Firestore-to-Firestore backup of {len(pool_ids)} pools...")

        # Add timestamp to backup collection name for versioning
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_collection_name = f"{backup_collection_name}_{timestamp}"

        success_count = 0
        error_count = 0
        start_time = time.time()
        total_documents = 0

        # Process pools in batches to avoid overloading Firebase
        batch_size = 50  # Increased from 20 for faster processing
        for i in range(0, len(pool_ids), batch_size):
            batch_start_time = time.time()
            pool_batch = pool_ids[i : i + batch_size]
            logger.info(
                f"Backing up batch {i//batch_size + 1}/{(len(pool_ids)-1)//batch_size + 1}: {len(pool_batch)} pools"
            )

            batch_documents = 0
            for pool_id in pool_batch:
                try:
                    # Get reference to the source pool document
                    source_doc = self.firebase.db.collection("marketContext").document(pool_id)
                    source_doc_data = source_doc.get()

                    # Skip if source document doesn't exist
                    if not source_doc_data.exists:
                        logger.warning(f"Source pool {pool_id} doesn't exist, skipping backup")
                        continue

                    # Create the backup document with the same ID
                    target_doc = self.firebase.db.collection(backup_collection_name).document(pool_id)

                    # Copy main document data
                    if source_doc_data.exists:
                        target_doc.set(source_doc_data.to_dict() or {})

                    # Get all documents from marketContexts subcollection
                    contexts_collection = source_doc.collection("marketContexts")
                    contexts = list(contexts_collection.stream())

                    # Create a batch for efficient writing
                    write_batch = self.firebase.db.batch()
                    batch_count = 0
                    batch_limit = 500  # Firestore limit for batch operations

                    # Create the target subcollection
                    for context in contexts:
                        # Create reference to target document in subcollection
                        target_context_doc = target_doc.collection("marketContexts").document(context.id)

                        # Add to batch
                        write_batch.set(target_context_doc, context.to_dict())
                        batch_count += 1

                        # If batch is full, commit it and create a new one
                        if batch_count >= batch_limit:
                            write_batch.commit()
                            write_batch = self.firebase.db.batch()
                            batch_count = 0

                    # Commit any remaining operations in the batch
                    if batch_count > 0:
                        write_batch.commit()

                    batch_documents += len(contexts) + 1  # +1 for the pool document itself
                    total_documents += len(contexts) + 1

                    logger.info(f"Backed up pool {pool_id} with {len(contexts)} data points")
                    success_count += 1

                except Exception as e:
                    logger.error(f"Error backing up pool {pool_id} to Firestore: {e}")
                    error_count += 1

            # Calculate and log batch performance metrics
            batch_time = time.time() - batch_start_time
            docs_per_second = batch_documents / batch_time if batch_time > 0 else 0
            logger.info(f"Batch completed in {batch_time:.2f}s ({docs_per_second:.2f} docs/s)")

            # Progress update
            elapsed = time.time() - start_time
            progress = (i + len(pool_batch)) / len(pool_ids)
            estimated_total = elapsed / progress if progress > 0 else 0
            remaining = estimated_total - elapsed

            logger.info(
                f"Progress: {progress:.1%} - {success_count}/{len(pool_ids)} pools - "
                + f"Time: {elapsed:.1f}s elapsed, ~{remaining:.1f}s remaining"
            )

            # Small delay between batches to avoid overloading Firebase
            time.sleep(1)

        # Save summary information about the backup
        summary = {
            "timestamp": datetime.now().isoformat(),
            "total_pools": len(pool_ids),
            "successful_backups": success_count,
            "failed_backups": error_count,
            "total_documents": total_documents,
            "execution_time_seconds": time.time() - start_time,
            "pool_ids": pool_ids,
        }

        # Save summary to Firestore
        try:
            self.firebase.db.collection(backup_collection_name).document("backup_summary").set(summary)
            logger.info(f"Backup summary saved to {backup_collection_name}/backup_summary")
        except Exception as e:
            logger.error(f"Error saving backup summary: {e}")

        # Final performance summary
        total_time = time.time() - start_time
        pools_per_second = success_count / total_time if total_time > 0 else 0
        docs_per_second = total_documents / total_time if total_time > 0 else 0

        logger.info(f"Firestore backup completed in {total_time:.2f}s")
        logger.info(f"Performance: {pools_per_second:.2f} pools/s, {docs_per_second:.2f} docs/s")
        logger.info(
            f"Backup collection: '{backup_collection_name}' ({success_count} pools, {total_documents} documents)"
        )
        logger.info(f"Results: {success_count} successful, {error_count} failed")

        return error_count == 0

    def backup_entire_collection(
        self, collection_name: str = "marketContext", backup_collection_name: str = "backup_marketContext"
    ) -> bool:
        """
        Create a backup of the entire marketContext collection with all marketContexts subcollections.
        Uses direct paths instead of collections() method which might not work properly.

        Args:
            collection_name: Name of the collection to backup (default: "marketContext")
            backup_collection_name: Name of the backup collection (a timestamp will be appended)

        Returns:
            True if backup was successful, False otherwise
        """
        # Add timestamp to backup collection name for versioning
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_collection_name = f"{backup_collection_name}_{timestamp}"

        logger.info(f"Starting DIRECT backup of {collection_name} collection to {backup_collection_name}...")
        start_time = time.time()

        # Statistics tracking
        copied_main_docs = 0
        copied_subcoll_docs = 0
        failed_docs = 0

        try:
            # Get all top-level documents in the source collection
            source_collection = self.firebase.db.collection(collection_name)
            all_docs = list(source_collection.stream())
            total_pools = len(all_docs)
            logger.info(f"Found {total_pools} pool documents in {collection_name} collection")

            # Process pool documents in batches
            batch_size = 10  # Use smaller batches to avoid timeouts
            for i in range(0, total_pools, batch_size):
                batch_start = time.time()
                current_batch = all_docs[i : i + batch_size]
                logger.info(
                    f"Processing batch {i//batch_size + 1}/{(total_pools-1)//batch_size + 1}: {len(current_batch)} pools"
                )

                for pool_doc in current_batch:
                    pool_id = pool_doc.id

                    try:
                        # Copy the main pool document
                        logger.info(f"Copying main document for pool {pool_id}")
                        target_doc = self.firebase.db.collection(backup_collection_name).document(pool_id)
                        target_doc.set(pool_doc.to_dict() or {})
                        copied_main_docs += 1

                        # Directly access the marketContexts subcollection - we know it exists
                        subcoll_path = f"{collection_name}/{pool_id}/marketContexts"
                        logger.info(f"Accessing marketContexts for pool {pool_id} at path {subcoll_path}")

                        # We access the subcollection directly
                        subcoll_ref = self.firebase.db.collection(subcoll_path)
                        subcoll_docs = list(subcoll_ref.stream())

                        if subcoll_docs:
                            doc_count = len(subcoll_docs)
                            logger.info(f"Found {doc_count} marketContexts documents for pool {pool_id}")

                            # Process in smaller batches to avoid write errors
                            context_batch_size = 300  # Firestore has a limit of 500 per batch, be conservative
                            for j in range(0, doc_count, context_batch_size):
                                current_contexts = subcoll_docs[j : j + context_batch_size]
                                logger.info(
                                    f"Processing context batch {j//context_batch_size + 1}/{(doc_count-1)//context_batch_size + 1} for pool {pool_id}"
                                )

                                # Create a batch for efficient writing
                                write_batch = self.firebase.db.batch()

                                for context_doc in current_contexts:
                                    # Create reference to the target context document
                                    target_context_ref = target_doc.collection("marketContexts").document(
                                        context_doc.id
                                    )
                                    write_batch.set(target_context_ref, context_doc.to_dict())

                                # Commit this batch of context documents
                                write_batch.commit()
                                copied_subcoll_docs += len(current_contexts)
                                logger.info(f"Committed {len(current_contexts)} context documents for pool {pool_id}")

                                # Give Firebase a short break
                                time.sleep(0.1)
                        else:
                            logger.warning(f"No marketContexts documents found for pool {pool_id}")

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
                    + f"Context docs: {copied_subcoll_docs} - "
                    + f"Time: {elapsed:.1f}s elapsed, ~{remaining:.1f}s remaining"
                )

                # Add a delay between batches
                time.sleep(0.5)

            # Save summary
            total_time = time.time() - start_time
            total_docs = copied_main_docs + copied_subcoll_docs
            docs_per_second = total_docs / total_time if total_time > 0 else 0

            summary = {
                "timestamp": datetime.now().isoformat(),
                "source_collection": collection_name,
                "pool_documents_copied": copied_main_docs,
                "context_documents_copied": copied_subcoll_docs,
                "total_documents_copied": total_docs,
                "failed_operations": failed_docs,
                "execution_time_seconds": total_time,
            }

            # Save summary to Firestore
            self.firebase.db.collection(backup_collection_name).document("backup_summary").set(summary)

            logger.info(f"Direct backup completed in {total_time:.2f}s")
            logger.info(f"Performance: {docs_per_second:.2f} docs/s")
            logger.info(f"Results: {copied_main_docs} pools with {copied_subcoll_docs} context documents")
            logger.info(f"Backup collection: '{backup_collection_name}'")

            return failed_docs == 0

        except Exception as e:
            logger.error(f"Error during direct backup: {str(e)}")
            return False

    def run_cleanup(
        self,
        analyze_only: bool = False,
        backup_only: bool = False,
        check_ref: bool = False,
        firestore_backup: bool = False,
    ) -> None:
        """
        Run the full cleanup process.

        Args:
            analyze_only: If True, only analyze but don't delete
            backup_only: If True, only backup all pools without deletion
            check_ref: If True, only analyze reference pools
            firestore_backup: If True, use faster Firestore-to-Firestore backup
        """
        try:
            # If check_ref flag is set, only analyze reference pools
            if check_ref:
                self.analyze_reference_pools()
                return

            # For backup-only with Firestore backup, we can skip the analysis phase entirely
            if backup_only and firestore_backup:
                logger.info("Starting fast Firestore-to-Firestore backup (skipping analysis phase)...")
                # Get all available pools directly
                all_pools = self.firebase.get_available_pools(limit=None)
                logger.info(f"Found {len(all_pools)} pools in Firebase, starting direct backup...")
                # Use faster Firestore-to-Firestore backup
                self.backup_pools_in_firestore(all_pools)
                return

            # Get analysis data
            pools_counts, low_data_pools, no_volume_pools, pools_to_keep = self.analyze_pools()

            # Print summary
            logger.info("===== ANALYSIS SUMMARY =====")
            logger.info(f"Total pools: {len(pools_counts)}")
            logger.info(f"Pools with <600 data points: {len(low_data_pools)}")
            logger.info(f"Pools with no volume data: {len(no_volume_pools)}")
            logger.info(f"Pools to keep: {len(pools_to_keep)}")

            # Determine pools to delete
            pools_to_delete = list(set(low_data_pools) | set(no_volume_pools))
            logger.info(f"Total pools to delete: {len(pools_to_delete)}")

            # Write analysis to files
            with open(os.path.join(self.backup_dir, "analysis_all_pools.json"), "w") as f:
                json.dump(pools_counts, f, indent=2)

            with open(os.path.join(self.backup_dir, "analysis_low_data_pools.json"), "w") as f:
                json.dump({pid: pools_counts.get(pid, 0) for pid in low_data_pools}, f, indent=2)

            with open(os.path.join(self.backup_dir, "analysis_no_volume_pools.json"), "w") as f:
                json.dump({pid: pools_counts.get(pid, 0) for pid in no_volume_pools}, f, indent=2)

            with open(os.path.join(self.backup_dir, "analysis_pools_to_keep.json"), "w") as f:
                json.dump({pid: pools_counts.get(pid, 0) for pid in pools_to_keep}, f, indent=2)

            # If analyze_only, stop here
            if analyze_only:
                logger.info("Analysis completed. No cleanup performed.")
                return

            # If backup_only, backup all pools and stop
            if backup_only:
                all_pools = list(pools_counts.keys())
                logger.info(f"Backing up all {len(all_pools)} pools...")

                if firestore_backup:
                    # Use faster Firestore-to-Firestore backup
                    self.backup_pools_in_firestore(all_pools)
                else:
                    # Use standard local backup
                    self.backup_pools(all_pools)
                return

            # Otherwise, continue with backup and deletion

            # Backup pools to be deleted
            logger.info(f"Backing up {len(pools_to_delete)} pools before deletion...")

            if firestore_backup:
                # Use faster Firestore-to-Firestore backup for pools to be deleted
                backup_success = self.backup_pools_in_firestore(
                    pools_to_delete, backup_collection_name="deletion_backup_pools"
                )
            else:
                # Use standard local backup
                backup_success = self.backup_pools(pools_to_delete)

            if not backup_success:
                logger.warning("Backup had errors. Aborting deletion to be safe.")
                return

            # Confirmation prompt
            user_input = input(f"About to delete {len(pools_to_delete)} pools. Type 'yes' to confirm: ")
            if user_input.lower() != "yes":
                logger.info("Deletion cancelled by user.")
                return

            # Delete pools
            deleted_count = self.delete_pools(pools_to_delete, dry_run=False)
            logger.info(f"Cleanup completed. Deleted {deleted_count} pools.")

        except Exception as e:
            logger.error(f"Error during cleanup process: {e}")
            raise


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Analyze and clean up Firebase pools data")
    parser.add_argument("--analyze", action="store_true", help="Only analyze, don't delete anything")
    parser.add_argument("--cleanup", action="store_true", help="Run the cleanup operation")
    parser.add_argument("--backup", action="store_true", help="Only perform a backup of all pools")
    parser.add_argument("--check-ref", action="store_true", help="Check reference pools structure")
    parser.add_argument(
        "--firestore-backup",
        action="store_true",
        help="Use faster Firestore-to-Firestore backup (much faster than local backup)",
    )
    parser.add_argument(
        "--backup-collection",
        action="store_true",
        help="Backup the entire marketContext collection at once (fastest option)",
    )
    parser.add_argument(
        "--collection-name", default="marketContext", help="Name of the collection to backup (default: marketContext)"
    )
    parser.add_argument("--credential-path", help="Path to Firebase credentials JSON file")

    args = parser.parse_args()

    # If no arguments provided, default to analyze mode
    if not (args.analyze or args.cleanup or args.backup or args.check_ref or args.backup_collection):
        args.analyze = True
        logger.info("No mode specified, defaulting to analyze-only mode")

    cleaner = FirebasePoolCleaner(credential_path=args.credential_path)

    # Handle backup of entire collection
    if args.backup_collection:
        logger.info("Running backup of entire collection...")
        cleaner.backup_entire_collection(collection_name=args.collection_name)
        return

    # Handle other modes
    if args.check_ref:
        cleaner.run_cleanup(check_ref=True)
    elif args.analyze:
        cleaner.run_cleanup(analyze_only=True)
    elif args.backup:
        cleaner.run_cleanup(backup_only=True, firestore_backup=args.firestore_backup)
    elif args.cleanup:
        cleaner.run_cleanup(firestore_backup=args.firestore_backup)


if __name__ == "__main__":
    main()
