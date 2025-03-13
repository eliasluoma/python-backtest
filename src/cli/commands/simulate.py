"""
Simulation command for the Solana Trading Simulator CLI.

This module provides command-line interface for running trading simulations.
"""

import argparse
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Import the required modules
from firebase_service import FirebaseService
from part1_buy_simulation import BuySimulator, preprocess_pool_data
from part2_sell_simulation import SellSimulator


def add_simulate_subparser(subparsers: argparse._SubParsersAction) -> None:
    """
    Add the simulation subparser to the main parser.

    Args:
        subparsers: Subparsers object from the main parser
    """
    simulate_parser = subparsers.add_parser("simulate", help="Run a trading simulation")

    # Data parameters
    data_group = simulate_parser.add_argument_group("Data Parameters")
    data_group.add_argument("--max-pools", type=int, default=10, help="Maximum number of pools to analyze")
    data_group.add_argument("--min-data-points", type=int, default=100, help="Minimum data points required per pool")

    # Buy parameters
    buy_group = simulate_parser.add_argument_group("Buy Parameters")
    buy_group.add_argument(
        "--early-mc-limit", type=float, default=400000, help="Market cap threshold for early filtering"
    )
    buy_group.add_argument("--min-delay", type=int, default=60, help="Minimum delay in seconds")
    buy_group.add_argument("--max-delay", type=int, default=200, help="Maximum delay in seconds")
    buy_group.add_argument("--mc-change-5s", type=float, default=5.0, help="Market cap change 5s threshold")
    buy_group.add_argument("--holder-delta-30s", type=int, default=20, help="Holder delta 30s threshold")
    buy_group.add_argument("--buy-volume-5s", type=float, default=5.0, help="Buy volume 5s threshold")

    # Sell parameters
    sell_group = simulate_parser.add_argument_group("Sell Parameters")
    sell_group.add_argument("--take-profit", type=float, default=1.9, help="Take profit multiplier")
    sell_group.add_argument("--stop-loss", type=float, default=0.65, help="Stop loss multiplier")
    sell_group.add_argument("--trailing-stop", type=float, default=0.9, help="Trailing stop multiplier")
    sell_group.add_argument("--skip-sell", action="store_true", help="Skip sell simulation")


def handle_simulate_command(args: argparse.Namespace) -> int:
    """
    Handle the simulate command.

    Args:
        args: Command line arguments

    Returns:
        int: Exit code (0 for success, non-zero for errors)
    """
    logger.info("Starting simulation...")

    # Extract buy parameters
    buy_params = {
        "mc_change_5s": args.mc_change_5s,
        "holder_delta_30s": args.holder_delta_30s,
        "buy_volume_5s": args.buy_volume_5s,
    }

    # Extract sell parameters
    sell_params = {
        "take_profit": args.take_profit,
        "stop_loss": args.stop_loss,
        "trailing_stop": args.trailing_stop,
    }

    try:
        # Create and run simulation
        from run_simulation import BacktestRunner

        runner = BacktestRunner(
            firebase_credentials=args.credentials,
            env_file=args.env_file,
            early_mc_limit=args.early_mc_limit,
            min_delay=args.min_delay,
            max_delay=args.max_delay,
        )

        runner.run_simulation(
            buy_params=buy_params,
            sell_params=None if args.skip_sell else sell_params,
            max_pools=args.max_pools,
        )

        logger.info("Simulation completed successfully")
        return 0

    except Exception as e:
        logger.error(f"Simulation failed: {str(e)}")
        return 1
