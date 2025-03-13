#!/usr/bin/env python

"""
Analysis CLI

This script provides a command-line interface to the analysis tools.
It allows running different analysis operations from a single entry point.
"""

import os
import argparse
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Import analysis modules
from src.analysis.pool_analyzer import main as analyze_all_pools
from src.analysis.invalid_pools_analyzer import main as analyze_invalid_pools
from src.analysis.pool_data_exporter import main as export_pool_data


def main():
    """
    Main CLI function that dispatches to the appropriate analysis tool.
    """
    parser = argparse.ArgumentParser(description="Trading Pool Analysis Tools")
    subparsers = parser.add_subparsers(dest="command", help="Analysis command to run")

    # Analyze All Pools Command
    subparsers.add_parser("analyze-all", help="Analyze all pools")

    # Analyze Invalid Pools Command
    invalid_parser = subparsers.add_parser("analyze-invalid", help="Analyze invalid pools")
    invalid_parser.add_argument(
        "--input",
        "-i",
        type=str,
        default="outputs/invalid_pools.json",
        help="Path to JSON file containing invalid pool IDs",
    )
    invalid_parser.add_argument(
        "--output",
        "-o",
        type=str,
        default="outputs/invalid_pools_analysis.json",
        help="Path to save the analysis results",
    )
    invalid_parser.add_argument("--max-pools", "-m", type=int, default=None, help="Maximum number of pools to analyze")
    invalid_parser.add_argument(
        "--limit-per-pool", "-l", type=int, default=600, help="Maximum number of data points to fetch per pool"
    )

    # Export Pool Data Command
    export_parser = subparsers.add_parser("export", help="Export pool data to JSON files")
    export_parser.add_argument("--input", "-i", type=str, required=True, help="Path to JSON file containing pool IDs")
    export_parser.add_argument(
        "--output-dir", "-o", type=str, default="outputs/exported_pools", help="Directory to save the exported data"
    )
    export_parser.add_argument(
        "--max-rows", "-m", type=int, default=None, help="Maximum number of rows to export per pool"
    )

    # Parse arguments
    args = parser.parse_args()

    # Create outputs directory if it doesn't exist
    os.makedirs("outputs", exist_ok=True)

    # Execute the selected command
    if args.command == "analyze-all":
        print("Running analysis of all pools...")
        analyze_all_pools()
    elif args.command == "analyze-invalid":
        print("Running analysis of invalid pools...")
        # We need to simulate command line arguments for the analyze_invalid_pools function
        import sys

        sys.argv = [
            "invalid_pools_analyzer.py",
            "--input",
            args.input,
            "--output",
            args.output,
        ]
        if args.max_pools:
            sys.argv.extend(["--max-pools", str(args.max_pools)])
        if args.limit_per_pool:
            sys.argv.extend(["--limit-per-pool", str(args.limit_per_pool)])
        analyze_invalid_pools()
    elif args.command == "export":
        print("Running export of pool data...")
        # We need to simulate command line arguments for the export_pool_data function
        import sys

        sys.argv = [
            "pool_data_exporter.py",
            "--input",
            args.input,
            "--output-dir",
            args.output_dir,
        ]
        if args.max_rows:
            sys.argv.extend(["--max-rows", str(args.max_rows)])
        export_pool_data()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
