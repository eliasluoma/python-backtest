#!/usr/bin/env python3
"""
Main simulation script for the Solana Trading Strategy Simulator.

This script runs backtesting simulations for Solana tokens using
Firebase market data to identify optimal buy and sell parameters.
"""

import os
import sys
import argparse
import logging
from datetime import datetime
import pandas as pd
from typing import Dict, Optional, List, Tuple

# Add src directory to path for importing our modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.data.firebase_service import FirebaseService
from src.data.data_processor import preprocess_pool_data, filter_pools
from src.simulation.buy_simulator import BuySimulator, calculate_returns
from src.simulation.sell_simulator import SellSimulator, calculate_trade_metrics

# We'll implement these in future steps
# from src.utils.visualization import plot_results


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Solana Trading Strategy Simulator")

    # Firebase options
    parser.add_argument("--credentials", type=str, help="Path to Firebase credentials JSON file")
    parser.add_argument("--env-file", type=str, default=".env.local", help="Path to .env file")

    # Simulation options
    parser.add_argument("--max-pools", type=int, help="Maximum number of pools to analyze (for testing)")
    parser.add_argument("--early-mc-limit", type=float, default=400000, help="Market cap limit for early filtering")
    parser.add_argument("--min-delay", type=int, default=60, help="Minimum delay in seconds before buy")
    parser.add_argument("--max-delay", type=int, default=200, help="Maximum delay in seconds for buy")

    # Buy parameters
    parser.add_argument("--mc-change-5s", type=float, help="Market cap change in 5s threshold")
    parser.add_argument("--holder-delta-30s", type=float, help="Holder delta in 30s threshold")
    parser.add_argument("--buy-volume-5s", type=float, help="Buy volume in 5s threshold")

    # Sell parameters
    parser.add_argument("--take-profit", type=float, default=1.9, help="Take profit multiplier (1.9 = +90%)")
    parser.add_argument("--stop-loss", type=float, default=0.65, help="Stop loss multiplier (0.65 = -35%)")
    parser.add_argument(
        "--trailing-stop", type=float, default=0.9, help="Trailing stop percentage (0.9 = 10% below peak)"
    )

    # Output options
    parser.add_argument("--output-dir", type=str, default="results", help="Directory to save results")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("--skip-sell", action="store_true", help="Skip sell simulation (buy opportunities only)")

    return parser.parse_args()


def setup_buy_params(args) -> Dict[str, float]:
    """Setup buy parameters from command line arguments."""
    # Start with default parameters
    from src.simulation.buy_simulator import get_default_parameters

    buy_params = get_default_parameters()

    # Override with command line arguments if provided
    if args.mc_change_5s is not None:
        buy_params["mc_change_5s"] = args.mc_change_5s
    if args.holder_delta_30s is not None:
        buy_params["holder_delta_30s"] = args.holder_delta_30s
    if args.buy_volume_5s is not None:
        buy_params["buy_volume_5s"] = args.buy_volume_5s

    logger.info("Using buy parameters:")
    for param, value in buy_params.items():
        logger.info(f"  {param}: {value}")

    return buy_params


def run_simulation(args) -> Tuple[List[Dict], Optional[List[Dict]], pd.DataFrame]:
    """Run the simulation with the given arguments."""
    # Set log level based on verbose flag
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Initialize Firebase connection
    logger.info("Initializing Firebase connection...")
    firebase_service = FirebaseService(env_file=args.env_file, credentials_json=args.credentials)

    # Fetch market data
    logger.info("Fetching market data from Firebase...")
    df = firebase_service.fetch_market_data(limit_pools=args.max_pools)

    if df.empty:
        logger.error("No data retrieved from Firebase")
        return [], None, df

    # Filter pools with sufficient data
    logger.info("Filtering pools with sufficient data...")
    valid_pools = filter_pools(df, min_data_points=args.max_delay + 50)

    if not valid_pools:
        logger.error("No valid pools found after filtering")
        return [], None, df

    # Setup buy parameters
    buy_params = setup_buy_params(args)

    # Initialize buy simulator
    logger.info("Initializing buy simulator...")
    buy_simulator = BuySimulator(
        early_mc_limit=args.early_mc_limit, min_delay=args.min_delay, max_delay=args.max_delay, buy_params=buy_params
    )

    # Find buy opportunities
    logger.info(f"Finding buy opportunities in {len(valid_pools)} pools...")
    buy_opportunities = []

    for pool_address, pool_df in valid_pools.items():
        try:
            # Preprocess pool data
            processed_df = preprocess_pool_data(pool_df)

            # Find buy opportunity
            buy_opportunity = buy_simulator.find_buy_opportunity(processed_df)

            if buy_opportunity:
                # Calculate potential returns
                buy_opportunity = calculate_returns(buy_opportunity)
                buy_opportunities.append(buy_opportunity)
                logger.info(f"Found buy opportunity in pool {pool_address}")
        except Exception as e:
            logger.error(f"Error processing pool {pool_address}: {str(e)}")

    logger.info(f"Found {len(buy_opportunities)} buy opportunities")

    # Run sell simulation if not skipped
    trades = None
    if not args.skip_sell and buy_opportunities:
        logger.info("Initializing sell simulator...")
        sell_simulator = SellSimulator(
            initial_investment=1.0,
            base_take_profit=args.take_profit,
            stop_loss=args.stop_loss,
            trailing_stop=args.trailing_stop,
        )

        logger.info("Simulating sell strategies...")
        trades = []

        for buy_opportunity in buy_opportunities:
            trade_result = sell_simulator.simulate_sell(buy_opportunity)
            if trade_result:
                trades.append(trade_result)

        logger.info(f"Completed {len(trades)} trade simulations")

        # Calculate and log trade metrics
        if trades:
            metrics = calculate_trade_metrics(trades)
            logger.info("\n=== TRADE METRICS ===")
            logger.info(f"Total trades: {metrics['total_trades']}")
            logger.info(f"Win rate: {metrics['win_rate']*100:.1f}%")
            logger.info(f"Average profit per trade: {metrics['avg_profit_per_trade']*100:.1f}%")
            logger.info(f"Average hold time: {metrics['avg_hold_time']:.1f} minutes")
            logger.info(
                f"Stop loss exits: {metrics['stoploss_trades']} ({metrics['stoploss_trades']/metrics['total_trades']*100 if metrics['total_trades'] > 0 else 0:.1f}%)"
            )
            logger.info(
                f"Take profit exits: {metrics['tp_trades']} ({metrics['tp_trades']/metrics['total_trades']*100 if metrics['total_trades'] > 0 else 0:.1f}%)"
            )
            logger.info(
                f"Low performance exits: {metrics['lp_trades']} ({metrics['lp_trades']/metrics['total_trades']*100 if metrics['total_trades'] > 0 else 0:.1f}%)"
            )
            logger.info(
                f"Force sells: {metrics['force_trades']} ({metrics['force_trades']/metrics['total_trades']*100 if metrics['total_trades'] > 0 else 0:.1f}%)"
            )

    # Save results
    save_results(buy_opportunities, trades, args.output_dir)

    return buy_opportunities, trades, df


def save_results(buy_opportunities: List[Dict], trades: Optional[List[Dict]], output_dir: str):
    """Save simulation results to files."""
    if not buy_opportunities:
        logger.warning("No buy opportunities to save")
        return

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Create a timestamp for filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Save buy opportunities to CSV
    try:
        # Extract key metrics for CSV
        metrics = []
        for opp in buy_opportunities:
            metrics.append(
                {
                    "pool_address": opp["pool_address"],
                    "entry_time": opp["entry_time"],
                    "entry_price": opp["entry_price"],
                    "max_return": opp.get("max_return", 0),
                    "realistic_return": opp.get("realistic_return", 0),
                    "time_to_max": opp.get("time_to_max", 0),
                }
            )

        metrics_df = pd.DataFrame(metrics)
        metrics_file = os.path.join(output_dir, f"buy_opportunities_{timestamp}.csv")
        metrics_df.to_csv(metrics_file, index=False)
        logger.info(f"Saved buy opportunities to {metrics_file}")

        # Save trade results if available
        if trades:
            trades_df = pd.DataFrame(trades)
            trades_file = os.path.join(output_dir, f"trade_results_{timestamp}.csv")
            trades_df.to_csv(trades_file, index=False)
            logger.info(f"Saved trade results to {trades_file}")

        # Calculate summary statistics
        if metrics:
            logger.info("\n=== BUY OPPORTUNITIES SUMMARY ===")
            logger.info(f"Total buy opportunities: {len(metrics)}")

            # Return distribution
            returns = [m["realistic_return"] for m in metrics]
            logger.info(f"Average return: {sum(returns)/len(returns):.2f}x")
            logger.info(f"Median return: {pd.Series(returns).median():.2f}x")
            logger.info(f"Max return: {max(returns):.2f}x")
            logger.info(f"Min return: {min(returns):.2f}x")

            # Time to max distribution
            times = [m["time_to_max"] for m in metrics]
            logger.info(f"Average time to max: {sum(times)/len(times):.1f} minutes")
            logger.info(f"Median time to max: {pd.Series(times).median():.1f} minutes")

            # Return distribution by ranges
            ranges = [
                (0, 1, "under 1x"),
                (1, 1.8, "1x to 1.8x"),
                (1.8, 3, "1.8x to 3x"),
                (3, 5, "3x to 5x"),
                (5, 10, "5x to 10x"),
                (10, float("inf"), "over 10x"),
            ]

            logger.info("\n=== RETURN DISTRIBUTION ===")
            for min_ret, max_ret, label in ranges:
                count = sum(1 for r in returns if min_ret <= r < max_ret)
                percentage = count / len(returns) * 100
                logger.info(f"{label}: {count} ({percentage:.1f}%)")

    except Exception as e:
        logger.error(f"Error saving results: {str(e)}")


if __name__ == "__main__":
    # Parse arguments
    args = parse_args()

    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)

    # Get current timestamp for the log filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/simulation_{timestamp}.log"

    # Set up logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )

    logger = logging.getLogger("SimulationRunner")

    logger.info("Starting Solana Trading Strategy Simulator")

    try:
        # Run simulation
        buy_opportunities, trades, market_data = run_simulation(args)

        if buy_opportunities:
            logger.info("Simulation completed successfully")

            if trades:
                logger.info(f"Completed {len(trades)} trade simulations")
            elif not args.skip_sell:
                logger.warning("No trades were successfully simulated")

        else:
            logger.warning("Simulation completed but no buy opportunities found")

    except Exception as e:
        logger.error(f"Error running simulation: {str(e)}")
        sys.exit(1)

    logger.info("Simulation finished")
