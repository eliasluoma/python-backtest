"""
Analysis command for the Solana Trading Simulator CLI.

This module provides command-line interface for analyzing market data.
"""

import argparse
import logging
import os

logger = logging.getLogger(__name__)

# Import the analysis functions
from src.analysis.pool_analyzer import main as analyze_all_pools
from src.analysis.invalid_pools_analyzer import main as analyze_invalid_pools


def add_analyze_subparser(subparsers: argparse._SubParsersAction) -> None:
    """
    Add the analysis subparser to the main parser.

    Args:
        subparsers: Subparsers object from the main parser
    """
    analyze_parser = subparsers.add_parser("analyze", help="Analyze market data")

    # Create subcommands for different types of analysis
    analyze_subparsers = analyze_parser.add_subparsers(dest="subcommand", help="Analysis subcommand to run")

    # All pools analysis
    all_parser = analyze_subparsers.add_parser("all", help="Analyze all pools")
    all_parser.add_argument("--output-prefix", type=str, default="pool_analysis", help="Prefix for output files")

    # Invalid pools analysis
    invalid_parser = analyze_subparsers.add_parser("invalid", help="Analyze invalid pools")
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


def handle_analyze_command(args: argparse.Namespace) -> int:
    """
    Handle the analyze command.

    Args:
        args: Command line arguments

    Returns:
        int: Exit code (0 for success, non-zero for errors)
    """
    # Create outputs directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)

    if not hasattr(args, "subcommand") or not args.subcommand:
        logger.error("No analysis subcommand specified")
        return 1

    try:
        # Execute the selected subcommand
        if args.subcommand == "all":
            logger.info("Running analysis of all pools...")
            analyze_all_pools()
            return 0
        elif args.subcommand == "invalid":
            logger.info("Running analysis of invalid pools...")
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
            return 0
        else:
            logger.error(f"Unknown analysis subcommand: {args.subcommand}")
            return 1
    except Exception as e:
        logger.error(f"Error in analysis: {str(e)}")
        return 1
