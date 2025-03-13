#!/usr/bin/env python3
"""
Solana Trading Simulator CLI

This is the main entry point for the Solana Trading Simulator's command-line interface.
It provides a unified interface to all functionality, including:
- Running trading simulations
- Analyzing market data
- Exporting and visualizing results
"""

import os
import sys
import argparse
import logging
from typing import Optional, List, Dict, Any

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("SolanaSimulator")

# Import command modules
from src.cli.commands.simulate import add_simulate_subparser, handle_simulate_command
from src.cli.commands.analyze import add_analyze_subparser, handle_analyze_command
from src.cli.commands.export import add_export_subparser, handle_export_command
from src.cli.commands.visualize import add_visualize_subparser, handle_visualize_command


def create_parser() -> argparse.ArgumentParser:
    """
    Create the main argument parser for the Solana Trading Simulator CLI.

    Returns:
        argparse.ArgumentParser: The configured argument parser
    """
    parser = argparse.ArgumentParser(
        description="Solana Trading Simulator - A backtesting framework for Solana trading strategies",
        epilog="For more information, see the documentation at docs/",
    )

    # Add version information
    parser.add_argument("--version", action="version", version="Solana Trading Simulator v0.1.0")

    # Common options for all commands
    parser.add_argument(
        "--verbose", "-v", action="count", default=0, help="Increase verbosity (can be used multiple times)"
    )
    parser.add_argument("--quiet", "-q", action="store_true", help="Suppress non-error messages")
    parser.add_argument("--credentials", type=str, help="Firebase credentials JSON file or string")
    parser.add_argument("--env-file", type=str, default=".env.local", help="Path to .env file")
    parser.add_argument("--output-dir", type=str, default="outputs", help="Directory to save output files")

    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Add subparsers for each command
    add_simulate_subparser(subparsers)
    add_analyze_subparser(subparsers)
    add_export_subparser(subparsers)
    add_visualize_subparser(subparsers)

    return parser


def configure_logging(args: argparse.Namespace) -> None:
    """
    Configure logging based on verbosity level.

    Args:
        args: Command line arguments
    """
    log_level = logging.WARNING  # Default

    if args.quiet:
        log_level = logging.ERROR
    elif args.verbose == 1:
        log_level = logging.INFO
    elif args.verbose >= 2:
        log_level = logging.DEBUG

    logging.getLogger().setLevel(log_level)
    logger.debug(f"Log level set to {logging.getLevelName(log_level)}")


def ensure_output_directory(output_dir: str) -> None:
    """
    Ensure the output directory exists.

    Args:
        output_dir: Path to the output directory
    """
    os.makedirs(output_dir, exist_ok=True)
    logger.debug(f"Output directory ensured: {output_dir}")


def main() -> int:
    """
    Main entry point for the Solana Trading Simulator CLI.

    Returns:
        int: Exit code (0 for success, non-zero for errors)
    """
    parser = create_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Configure logging based on verbosity
    configure_logging(args)

    # Ensure output directory exists
    ensure_output_directory(args.output_dir)

    try:
        # Dispatch to the appropriate command handler
        if args.command == "simulate":
            return handle_simulate_command(args)
        elif args.command == "analyze":
            return handle_analyze_command(args)
        elif args.command == "export":
            return handle_export_command(args)
        elif args.command == "visualize":
            return handle_visualize_command(args)
        else:
            logger.error(f"Unknown command: {args.command}")
            return 1
    except Exception as e:
        logger.error(f"Error executing command: {e}", exc_info=args.verbose >= 2)
        return 1


if __name__ == "__main__":
    sys.exit(main())
