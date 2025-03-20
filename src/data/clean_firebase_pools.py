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
"""

import os
import json
import time
import logging
import argparse
import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple, Set, Optional

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

    def run_cleanup(self, analyze_only: bool = False, backup_only: bool = False) -> None:
        """
        Run the full cleanup process.

        Args:
            analyze_only: If True, only analyze but don't delete
            backup_only: If True, only backup all pools without deletion
        """
        try:
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
                self.backup_pools(all_pools)
                return

            # Otherwise, continue with backup and deletion

            # Backup pools to be deleted
            logger.info(f"Backing up {len(pools_to_delete)} pools before deletion...")
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
    parser.add_argument("--credential-path", help="Path to Firebase credentials JSON file")

    args = parser.parse_args()

    # If no arguments provided, default to analyze mode
    if not (args.analyze or args.cleanup or args.backup):
        args.analyze = True
        logger.info("No mode specified, defaulting to analyze-only mode")

    cleaner = FirebasePoolCleaner(credential_path=args.credential_path)

    if args.analyze:
        cleaner.run_cleanup(analyze_only=True)
    elif args.backup:
        cleaner.run_cleanup(backup_only=True)
    elif args.cleanup:
        cleaner.run_cleanup()


if __name__ == "__main__":
    main()
