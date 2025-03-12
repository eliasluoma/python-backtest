#!/usr/bin/env python

"""
Export Pool Data to JSON Files

This script exports data from specified Firebase pools to JSON files:
1. Export data from pools listed in a specified JSON file (e.g., invalid_pools.json)
2. Save each pool's data as a separate JSON file
3. Optionally limit the number of rows exported per pool
"""

import os
import sys
import logging
import json
import pandas as pd
from datetime import datetime

# Set absolute path to the root directory
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Print debugging information about the environment
print(f"Python version: {sys.version}")
print(f"Working directory: {os.getcwd()}")
print(f"sys.path: {sys.path}")
print(f"Script location: {__file__}")
print(f"Project root directory: {project_root}")

# Import Firebase utilities
try:
    from src.data.firebase_service import FirebaseService
    from src.utils.firebase_utils import get_pool_ids
    print("Firebase modules loaded successfully!")
except ImportError as e:
    logger.error(f"Firebase modules not found: {e}")
    logger.error("Make sure you run the script from the project root directory.")
    sys.exit(1)


def export_pool_data(pool_ids, output_dir, max_rows_per_pool=None):
    """
    Export data from specified pools to JSON files
    
    Args:
        pool_ids (list): List of pool IDs to export
        output_dir (str): Directory to save the exported data
        max_rows_per_pool (int, optional): Maximum number of rows to export per pool
    
    Returns:
        dict: Summary of export operation
    """
    # Initialize Firebase
    logger.info("Initializing Firebase connection...")
    firebase_service = FirebaseService()
    db = firebase_service.db

    if not db:
        logger.error("Failed to establish Firebase connection. Exiting.")
        return {"status": "error", "message": "Failed to establish Firebase connection"}

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Data will be exported to directory: {output_dir}")

    # Initialize counters
    exported_count = 0
    failed_count = 0
    total_rows_exported = 0
    export_summary = []

    # Process all pools
    for i, pool_id in enumerate(pool_ids):
        logger.info(f"Exporting pool {i+1}/{len(pool_ids)}: {pool_id}")

        # Fetch data for this pool
        limit_per_pool = max_rows_per_pool if max_rows_per_pool else 5000  # Use a reasonable default if not specified
        
        pool_data = firebase_service.fetch_market_data(
            min_data_points=1, max_pools=1, limit_per_pool=limit_per_pool, pool_address=pool_id
        ).get(pool_id)

        # Only process if we get some data
        if pool_data is not None and not pool_data.empty:
            row_count = len(pool_data)
            
            # Create a sanitized filename (replace any invalid filename characters)
            filename = f"{pool_id.replace('/', '_').replace(':', '_')}.json"
            output_file = os.path.join(output_dir, filename)
            
            try:
                # Convert DataFrame to dict for JSON serialization
                pool_data_dict = pool_data.to_dict(orient='records')
                
                # Save to JSON file
                with open(output_file, 'w') as f:
                    json.dump(pool_data_dict, f, indent=2)
                
                logger.info(f"✓ Pool {pool_id} - Exported {row_count} rows to {filename}")
                
                exported_count += 1
                total_rows_exported += row_count
                
                # Add to summary
                export_summary.append({
                    "pool_id": pool_id,
                    "filename": filename,
                    "rows_exported": row_count,
                    "status": "success"
                })
                
            except Exception as e:
                logger.error(f"✗ Pool {pool_id} - Failed to export: {str(e)}")
                failed_count += 1
                
                # Add to summary
                export_summary.append({
                    "pool_id": pool_id,
                    "status": "error",
                    "error": str(e)
                })
                
        elif pool_data is None:
            logger.info(f"✗ Pool {pool_id} - Could not fetch data")
            failed_count += 1
            
            # Add to summary
            export_summary.append({
                "pool_id": pool_id,
                "status": "error",
                "error": "Could not fetch data"
            })
            
        else:
            logger.info(f"✗ Pool {pool_id} - No data (empty DataFrame)")
            failed_count += 1
            
            # Add to summary
            export_summary.append({
                "pool_id": pool_id,
                "status": "error",
                "error": "No data (empty DataFrame)"
            })

    # Create export summary result
    result = {
        "status": "success",
        "total_pools": len(pool_ids),
        "exported_count": exported_count,
        "failed_count": failed_count,
        "total_rows_exported": total_rows_exported,
        "export_summary": export_summary
    }
    
    # Save summary to file
    summary_file = os.path.join(output_dir, "export_summary.json")
    with open(summary_file, 'w') as f:
        json.dump(result, f, indent=2)
    
    logger.info(f"Export summary saved to: {summary_file}")
    
    return result


def main():
    """Export pool data to JSON files"""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Export pool data to JSON files')
    parser.add_argument('--input-file', required=True, help='JSON file containing pool IDs to export')
    parser.add_argument('--output-dir', help='Directory to save exported data (default: outputs/exported_pools_TIMESTAMP)')
    parser.add_argument('--max-rows', type=int, help='Maximum number of rows to export per pool')
    args = parser.parse_args()
    
    # Load pool IDs from input file
    try:
        with open(args.input_file, 'r') as f:
            pool_ids = json.load(f)
            
        logger.info(f"Loaded {len(pool_ids)} pool IDs from {args.input_file}")
    except Exception as e:
        logger.error(f"Failed to load pool IDs from {args.input_file}: {str(e)}")
        sys.exit(1)
    
    # Create output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
        # Create timestamp for subfolder
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = os.path.join(project_root, "outputs", f"exported_pools_{timestamp}")
    
    # Export pool data
    result = export_pool_data(pool_ids, output_dir, args.max_rows)
    
    # Print summary
    print("\n" + "=" * 80)
    print("POOL DATA EXPORT RESULTS")
    print("=" * 80)
    print(f"Total pools: {result['total_pools']}")
    print(f"Successfully exported: {result['exported_count']} pools")
    print(f"Failed to export: {result['failed_count']} pools")
    print(f"Total rows exported: {result['total_rows_exported']}")
    print(f"Export directory: {output_dir}")
    print(f"Export summary file: {os.path.join(output_dir, 'export_summary.json')}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExport interrupted by user.")
    except Exception as e:
        logger.exception(f"Error in export: {str(e)}") 