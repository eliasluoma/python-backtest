"""
Export command for the Solana Trading Simulator CLI.

This module provides command-line interface for exporting market data.
"""

import argparse
import logging
import os
import sys

logger = logging.getLogger(__name__)

# Import the export function
from src.analysis.pool_data_exporter import main as export_pool_data


def add_export_subparser(subparsers: argparse._SubParsersAction) -> None:
    """
    Add the export subparser to the main parser.

    Args:
        subparsers: Subparsers object from the main parser
    """
    export_parser = subparsers.add_parser("export", help="Export pool data to JSON files")

    export_parser.add_argument("--input", "-i", type=str, required=True, help="Path to JSON file containing pool IDs")
    export_parser.add_argument(
        "--output-dir", "-o", type=str, default="outputs/exported_pools", help="Directory to save the exported data"
    )
    export_parser.add_argument(
        "--max-rows", "-m", type=int, default=None, help="Maximum number of rows to export per pool"
    )
    export_parser.add_argument(
        "--format", "-f", type=str, choices=["json", "csv"], default="json", help="Output format for exported data"
    )


def handle_export_command(args: argparse.Namespace) -> int:
    """
    Handle the export command.

    Args:
        args: Command line arguments

    Returns:
        int: Exit code (0 for success, non-zero for errors)
    """
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)

    try:
        logger.info(f"Exporting pool data from {args.input} to {args.output_dir}...")

        # We need to simulate command line arguments for the export_pool_data function
        sys.argv = [
            "pool_data_exporter.py",
            "--input",
            args.input,
            "--output-dir",
            args.output_dir,
        ]

        if args.max_rows:
            sys.argv.extend(["--max-rows", str(args.max_rows)])

        # Call the export function
        export_pool_data()

        logger.info("Export completed successfully")
        return 0

    except Exception as e:
        logger.error(f"Error exporting data: {str(e)}")
        return 1
