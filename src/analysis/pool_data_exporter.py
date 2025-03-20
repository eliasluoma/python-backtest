#!/usr/bin/env python

"""
Export Pool Data to JSON Files

This script exports data from specified Firebase pools to JSON files:
1. Export data from pools listed in a specified JSON file (e.g., invalid_pools.json)
2. Save each pool's data as a separate JSON file
3. Optionally limit the number of rows exported per pool
"""

import os
import logging
import json
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Import services
from src.data.firebase_service import FirebaseService


def export_pool_data(pool_ids, output_dir, max_rows_per_pool=None):
    """
    Export data from specified pools to JSON files.

    Args:
        pool_ids: List of pool IDs to export
        output_dir: Directory to save the JSON files
        max_rows_per_pool: Maximum number of rows to export per pool (None for all)

    Returns:
        Dictionary with export results
    """
    # Initialize Firebase
    logger.info("Initializing Firebase connection...")
    firebase_service = FirebaseService()
    db = firebase_service.db

    if not db:
        logger.error("Failed to connect to Firebase. Exiting.")
        return None

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Initialize containers for results
    results = {
        "total_pools": len(pool_ids),
        "successful_exports": 0,
        "failed_exports": 0,
        "exported_pools": [],
        "failed_pools": [],
        "export_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    # Start timer
    start_time = datetime.now()

    # Process pools in batches to avoid memory issues
    batch_size = 5
    num_batches = (len(pool_ids) + batch_size - 1) // batch_size

    logger.info("Exporting data for {} pools...".format(len(pool_ids)))

    for batch_idx in range(num_batches):
        batch_start = batch_idx * batch_size
        batch_end = min((batch_idx + 1) * batch_size, len(pool_ids))
        current_batch = pool_ids[batch_start:batch_end]

        logger.info(
            "Processing batch {}/{}: pools {}-{} of {}".format(
                batch_idx + 1, num_batches, batch_start + 1, batch_end, len(pool_ids)
            )
        )

        # Fetch data for this batch
        for pool_id in current_batch:
            try:
                logger.info("Fetching data for pool: {}".format(pool_id))

                # Use limit_per_pool parameter if max_rows_per_pool is specified
                limit = max_rows_per_pool if max_rows_per_pool is not None else None
                pool_data = firebase_service.fetch_market_data(
                    min_data_points=1, pool_address=pool_id, limit_per_pool=limit
                ).get(pool_id)

                if pool_data is None or pool_data.empty:
                    logger.warning("No data available for pool: {}".format(pool_id))
                    results["failed_exports"] += 1
                    results["failed_pools"].append({"pool_id": pool_id, "reason": "No data available"})
                    continue

                # Create a filename for this pool
                # Use just the first 10 characters of the pool ID to keep filenames shorter
                short_pool_id = pool_id[:10]
                output_file = os.path.join(output_dir, f"pool_{short_pool_id}.json")

                # Convert DataFrame to JSON and save
                logger.info("Saving data for pool {} ({} rows) to {}".format(pool_id, len(pool_data), output_file))

                # Reset index to make sure row numbers start from 0
                pool_data.reset_index(drop=True, inplace=True)

                # Properly handle non-serializable objects
                # Convert numpy.int64, numpy.float64, etc. to native Python types
                pool_json = pool_data.to_dict(orient="records")

                # Add a metadata section to the JSON for easier identification
                json_data = {
                    "metadata": {
                        "pool_id": pool_id,
                        "export_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "row_count": len(pool_data),
                        "columns": list(pool_data.columns),
                    },
                    "data": pool_json,
                }

                # Save to file
                with open(output_file, "w") as f:
                    json.dump(json_data, f, indent=2)

                results["successful_exports"] += 1
                results["exported_pools"].append({"pool_id": pool_id, "rows": len(pool_data), "file": output_file})

            except Exception as e:
                logger.error("Error exporting pool {}: {}".format(pool_id, str(e)))
                results["failed_exports"] += 1
                results["failed_pools"].append({"pool_id": pool_id, "reason": str(e)})

    # Update results with timing information
    duration = (datetime.now() - start_time).total_seconds()
    results["duration_seconds"] = duration

    # Create a summary file
    summary_file = os.path.join(output_dir, "export_summary.json")
    with open(summary_file, "w") as f:
        json.dump(results, f, indent=2)

    logger.info("Export completed in {:.1f} seconds".format(duration))
    logger.info("Summary saved to: {}".format(summary_file))

    return results


def main():
    """
    Main function to export pool data.

    1. Loads pool IDs from a file
    2. Exports data for each pool to a JSON file
    3. Saves a summary of the export process
    """
    import argparse

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Export Pool Data to JSON Files")
    parser.add_argument("--input", "-i", type=str, required=True, help="Path to JSON file containing pool IDs")
    parser.add_argument(
        "--output-dir", "-o", type=str, default="outputs/exported_pools", help="Directory to save the exported data"
    )
    parser.add_argument("--max-rows", "-m", type=int, default=None, help="Maximum number of rows to export per pool")
    args = parser.parse_args()

    # Check if input file exists
    if not os.path.exists(args.input):
        logger.error("Input file does not exist: {}".format(args.input))
        return

    # Load pool IDs
    logger.info("Loading pool IDs from: {}".format(args.input))
    with open(args.input, "r") as f:
        try:
            pool_ids = json.load(f)
            if not isinstance(pool_ids, list):
                logger.error("Input file does not contain a list of pool IDs")
                return
            logger.info("Loaded {} pool IDs".format(len(pool_ids)))
        except json.JSONDecodeError:
            logger.error("Failed to parse input file as JSON")
            return

    # Export pool data
    export_results = export_pool_data(pool_ids, args.output_dir, max_rows_per_pool=args.max_rows)

    if export_results:
        # Print summary
        print("\n" + "=" * 80)
        print("POOL DATA EXPORT SUMMARY")
        print("=" * 80)

        print("\nPools processed: {}".format(export_results["total_pools"]))
        print("Successful exports: {}".format(export_results["successful_exports"]))
        print("Failed exports: {}".format(export_results["failed_exports"]))
        print("Total data rows exported: {}".format(sum(pool["rows"] for pool in export_results["exported_pools"])))
        print("Export completed in {:.1f} seconds".format(export_results["duration_seconds"]))

        print("\nResults saved to: {}".format(args.output_dir))
        print("Summary file: {}".format(os.path.join(args.output_dir, "export_summary.json")))
        print("=" * 80)
    else:
        logger.error("Export failed")


if __name__ == "__main__":
    main()
