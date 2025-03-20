"""
Simulation command for the Solana Trading Simulator CLI.

This module provides command-line interface for running trading simulations.
"""

import argparse
import logging
import traceback
import pandas as pd
import warnings

logger = logging.getLogger(__name__)

# Import the required modules
from src.data.cache_service import DataCacheService

# Suppress pandas warnings for timestamp conversions
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)


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
    data_group.add_argument("--use-local-db", action="store_true", help="Use local SQLite database instead of Firebase")
    data_group.add_argument(
        "--db-path", type=str, default="cache/pools.db", help="Path to the SQLite database (when using local DB)"
    )
    data_group.add_argument(
        "--schema-path", type=str, default="src/data/schema.sql", help="Path to the SQLite schema (when using local DB)"
    )

    # Buy parameters
    buy_group = simulate_parser.add_argument_group("Buy Parameters")
    buy_group.add_argument(
        "--early-mc-limit", type=float, default=10000000, help="Market cap threshold for early filtering"
    )
    buy_group.add_argument("--min-delay", type=int, default=60, help="Minimum delay (in data points) for scanning")
    buy_group.add_argument("--max-delay", type=int, default=200, help="Maximum delay (in data points) for scanning")
    buy_group.add_argument("--mc-change-5s", type=float, default=5.0, help="Market cap change threshold (5s window)")
    buy_group.add_argument("--holder-delta-30s", type=float, default=10.0, help="Holder change threshold (30s window)")
    buy_group.add_argument("--buy-volume-5s", type=float, default=5.0, help="Buy volume threshold (5s window)")

    # Sell parameters
    sell_group = simulate_parser.add_argument_group("Sell Parameters")
    sell_group.add_argument(
        "--take-profit", type=float, default=1.9, help="Take profit multiplier (e.g., 1.9 = 90% profit)"
    )
    sell_group.add_argument(
        "--stop-loss", type=float, default=0.65, help="Stop loss multiplier (e.g., 0.65 = 35% loss)"
    )
    sell_group.add_argument(
        "--trailing-stop", type=float, default=0.9, help="Trailing stop multiplier (e.g., 0.9 = 10% from peak)"
    )
    sell_group.add_argument("--skip-sell", action="store_true", help="Skip sell simulation (for buy testing)")

    # Output parameters
    output_group = simulate_parser.add_argument_group("Output Parameters")
    output_group.add_argument("--plot", action="store_true", help="Generate and save price charts")
    output_group.add_argument("--plot-dir", type=str, default="plots", help="Directory to save price charts")
    output_group.add_argument("--save-results", action="store_true", help="Save simulation results to a file")
    output_group.add_argument("--results-file", type=str, default="results.json", help="Path to save results JSON")

    # Set the default function
    simulate_parser.set_defaults(func=simulate_command)


def simulate_command(args) -> int:
    """
    Execute the simulate command with the provided arguments.

    Args:
        args: Command line arguments

    Returns:
        int: Exit code (0 for success, non-zero for errors)
    """
    logger.info("Starting trading simulation...")

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

    # Check if we should use local database
    if args.use_local_db:
        return run_simulation_with_local_db(args, buy_params, sell_params)
    else:
        try:
            # Import here to avoid circular imports
            from src.simulation.backtest_runner import BacktestRunner

            # For Firebase simulation, use BacktestRunner
            # Note: BacktestRunner currently expects positional arguments
            runner = BacktestRunner(
                args.max_pools,
                args.min_data_points,
                args.mc_change_5s,  # Pass the raw value instead of the dict
                args.holder_delta_30s,  # Pass the raw value instead of the dict
                args.early_mc_limit,
                args.min_delay,
                args.max_delay,
            )

            runner.run_simulation()
            return 0
        except Exception as e:
            logger.error(f"Simulation failed: {str(e)}")
            return 1


def run_simulation_with_local_db(args, buy_params, sell_params) -> int:
    """
    Run simulation using local SQLite database.

    Args:
        args: Command line arguments
        buy_params: Buy parameters dictionary
        sell_params: Sell parameters dictionary

    Returns:
        int: Return code (0 for success, non-zero for errors)
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting simulation...")
    logger.info(f"Using local database at {args.db_path}")

    try:
        # Initialize caching service
        cache_service = DataCacheService(args.db_path)

        # Set up simulation classes
        from src.simulation.buy_simulator import BuySimulator
        from src.simulation.sell_simulator import SellSimulator

        # Flag to use all pools instead of specific test pools
        use_specific_pools = False
        test_pool_ids = [
            "12H7zN3gXRfUeu2fCQjooPAofbBL2wz7X7wKoS44oJkX",  # Stop loss test
            "12QspooeZFsA4d41KtLz4p3e8YyLzPxG4bShsUCBbEgU",  # Try another pool
            "12YyqbMeNdmdqh29k17zHF3MagzKuTDySKZwyUnwfzVx",  # One more pool
        ]

        # Initialize simulators
        buy_simulator = BuySimulator(
            early_mc_limit=args.early_mc_limit,
            min_delay=args.min_delay,
            max_delay=args.max_delay,
            buy_params=buy_params,
        )

        sell_simulator = SellSimulator(
            base_take_profit=sell_params["take_profit"],
            stop_loss=sell_params["stop_loss"],
            trailing_stop=sell_params["trailing_stop"],
        )

        # Collection of buy opportunities and trade results
        buy_opportunities = []
        trade_results = []

        # Use the specific test pools if enabled
        pool_ids = test_pool_ids if use_specific_pools else cache_service.get_pool_ids()

        # Limit the number of pools to process if max_pools is specified
        if args.max_pools and len(pool_ids) > args.max_pools:
            logger.info(f"Limiting to {args.max_pools} pools (from {len(pool_ids)} available)")
            pool_ids = pool_ids[: args.max_pools]
        else:
            logger.info(f"Processing {len(pool_ids)} pools")

        # Log timestamp handling message once
        logger.info("Converting timestamps - this may take a moment...")

        # Process each pool
        for pool_id in pool_ids:
            logger.info(f"Processing pool {pool_id}")
            try:
                # Directly query the SQLite database for market data
                import sqlite3

                # Connect to the SQLite database
                conn = sqlite3.connect(args.db_path)

                # Query the market data for this pool
                query = f"""
                SELECT * FROM market_data 
                WHERE poolAddress = '{pool_id}'
                ORDER BY timestamp ASC
                """

                # Load the data into a pandas DataFrame
                df = pd.read_sql_query(query, conn)
                conn.close()

                # Handle timestamp format conversion if needed
                if "timestamp" in df.columns:
                    try:
                        # Try to convert timestamps to pandas datetime with a more flexible approach
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore")
                            df["timestamp"] = pd.to_datetime(df["timestamp"], format="mixed", utc=True)
                    except:
                        # More graceful error handling without long error messages
                        logger.warning(f"Timestamp conversion issue for pool {pool_id} - trying fallback method")

                        # Try to handle inconsistent timestamp formats with a fallback method
                        invalid_formats = []
                        for i, ts in enumerate(df["timestamp"]):
                            try:
                                pd.to_datetime(ts)
                            except:
                                invalid_formats.append(i)

                        if invalid_formats:
                            # Just log the count without detailed messages
                            logger.warning(f"Dropping {len(invalid_formats)} rows with invalid timestamps")
                            df = df.drop(invalid_formats)
                            with warnings.catch_warnings():
                                warnings.simplefilter("ignore")
                                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

                logger.info(f"Successfully loaded {len(df)} rows of market data for pool {pool_id}")

                if df.empty or len(df) < args.min_delay + 10:
                    logger.warning(f"Skipping pool {pool_id}: insufficient data (only {len(df)} rows)")
                    continue

                # Preprocess pool data
                from src.simulation.backtest_runner import preprocess_pool_data

                df = preprocess_pool_data(df)

                # Run buy simulation for this pool
                buy_opportunity = buy_simulator.find_buy_opportunity(df)

                if buy_opportunity:
                    buy_opportunity["pool_address"] = pool_id  # Ensure pool ID is included
                    buy_opportunities.append(buy_opportunity)
                    logger.info(f"Found buy opportunity for pool {pool_id}")
                else:
                    logger.info(f"No buy opportunity found for pool {pool_id}")
            except Exception as e:
                logger.error(f"Error processing pool {pool_id}: {str(e)}")
                logger.error(traceback.format_exc())
                continue

        logger.info(f"Buy simulation complete. Found {len(buy_opportunities)} opportunities.")

        # Skip sell simulation if requested
        if args.skip_sell:
            logger.info("Skipping sell simulation as requested.")
            return 0

        # Run sell simulation on buy opportunities
        logger.info("Running sell simulation...")
        for buy_opportunity in buy_opportunities:
            try:
                trade_result = sell_simulator.simulate_sell(buy_opportunity)
                if trade_result:
                    trade_results.append(trade_result)
                else:
                    logger.warning(f"Sell simulation failed for pool {buy_opportunity['pool_address']}")
            except Exception as e:
                logger.error(f"Error in sell simulation for pool {buy_opportunity['pool_address']}: {str(e)}")
                logger.error(traceback.format_exc())

        logger.info(f"Sell simulation complete. Processed {len(trade_results)} trades.")

        # Calculate summary statistics
        calculate_and_display_stats(trade_results)

        return 0

    except Exception as e:
        logger.error(f"Local simulation failed: {str(e)}")
        logger.error(traceback.format_exc())
        return 1


def calculate_and_display_stats(trade_results, metrics=None):
    """Calculate and display trading statistics"""
    logger = logging.getLogger(__name__)

    if not trade_results:
        logger.warning("No trades to analyze")
        return

    if metrics is None:
        metrics = {}

    # Initialize metrics with default values
    default_metrics = {
        "total_trades": len(trade_results),
        "profitable_trades": 0,
        "profit_percentages": [],
        "loss_percentages": [],
        "durations": [],
        "exit_reasons": {},
        "total_profit": 0.0,
    }

    # Update metrics with default values if not present
    for key, value in default_metrics.items():
        metrics.setdefault(key, value)

    # Calculate statistics
    for result in trade_results:
        # Extract profit percentage from profit_ratio (SellSimulator uses profit_ratio)
        profit_percentage = (result.get("profit_ratio", 1.0) - 1.0) * 100

        if profit_percentage > 0:
            metrics["profitable_trades"] += 1
            metrics["profit_percentages"].append(profit_percentage)
        else:
            metrics["loss_percentages"].append(profit_percentage)

        metrics["total_profit"] += profit_percentage

        # Extract duration from trade_duration (in seconds from SellSimulator)
        if "trade_duration" in result:
            # Convert seconds to minutes
            duration_minutes = result["trade_duration"] / 60
            metrics["durations"].append(duration_minutes)

        # Count exit reasons
        exit_reason = result.get("exit_reason", "Unknown")
        metrics["exit_reasons"][exit_reason] = metrics["exit_reasons"].get(exit_reason, 0) + 1

    # Calculate derived metrics
    metrics["win_percentage"] = (
        (metrics["profitable_trades"] / metrics["total_trades"] * 100) if metrics["total_trades"] > 0 else 0
    )
    metrics["average_profit"] = (
        sum(metrics["profit_percentages"]) / len(metrics["profit_percentages"]) if metrics["profit_percentages"] else 0
    )
    metrics["average_loss"] = (
        sum(metrics["loss_percentages"]) / len(metrics["loss_percentages"]) if metrics["loss_percentages"] else 0
    )
    metrics["max_profit"] = max(metrics["profit_percentages"]) if metrics["profit_percentages"] else 0
    metrics["max_loss"] = min(metrics["loss_percentages"]) if metrics["loss_percentages"] else 0
    metrics["win_loss_ratio"] = (
        len(metrics["profit_percentages"]) / len(metrics["loss_percentages"])
        if len(metrics["loss_percentages"]) > 0
        else 0
    )
    metrics["profit_factor"] = (
        abs(sum(metrics["profit_percentages"]) / sum(metrics["loss_percentages"]))
        if sum(metrics["loss_percentages"]) != 0
        else 0
    )
    metrics["average_duration"] = sum(metrics["durations"]) / len(metrics["durations"]) if metrics["durations"] else 0

    # Display results
    logger.info("\n=== SIMULATION RESULTS ===")
    logger.info(f"Total trades: {metrics['total_trades']}")
    logger.info(f"Profitable trades: {metrics['profitable_trades']} ({metrics['win_percentage']:.1f}%)")
    logger.info(f"Average profit: {metrics['total_profit'] / metrics['total_trades']:.2f}%")

    if metrics["profit_percentages"]:
        logger.info(f"Average profit (winning trades): {metrics['average_profit']:.2f}%")

    if metrics["loss_percentages"]:
        logger.info(f"Average loss (losing trades): {metrics['average_loss']:.2f}%")

    logger.info(f"Max profit: {metrics['max_profit']:.2f}%")
    logger.info(f"Max loss: {metrics['max_loss']:.2f}%")
    logger.info(f"Win/loss ratio: {metrics['win_loss_ratio']:.2f}")
    logger.info(f"Profit factor: {metrics['profit_factor']:.2f}")
    logger.info(f"Average trade duration: {metrics['average_duration']:.1f} minutes")

    # Display exit reasons if available
    if metrics["exit_reasons"]:
        logger.info("\nExit reasons:")
        for reason, count in metrics["exit_reasons"].items():
            percentage = (count / metrics["total_trades"]) * 100
            logger.info(f"  {reason}: {count} ({percentage:.1f}%)")

    # Generate and display detailed trading log
    if trade_results:
        logger.info("\n=== DETAILED TRADING LOG ===")
        logger.info(
            f"{'Pool Address':<45} | {'Buy Time':<25} | {'Sell Time':<25} | {'Buy MC':<15} | {'Sell MC':<15} | {'Profit %':<10} | {'Exit Reason':<20}"
        )
        logger.info(f"{'-'*45} | {'-'*25} | {'-'*25} | {'-'*15} | {'-'*15} | {'-'*10} | {'-'*20}")

        # Sort trades by profit percentage
        sorted_trades = sorted(trade_results, key=lambda x: x.get("profit_ratio", 1.0) - 1.0, reverse=True)

        for trade in sorted_trades:
            pool_addr = trade.get("pool_address", "Unknown")
            buy_time = str(trade.get("entry_time", "Unknown"))
            sell_time = str(trade.get("exit_time", "Unknown"))
            buy_mc = f"{trade.get('entry_price', 0):.2f}"
            sell_mc = f"{trade.get('exit_price', 0):.2f}"
            profit_pct = f"{(trade.get('profit_ratio', 1.0) - 1.0) * 100:.2f}%"
            exit_reason = trade.get("exit_reason", "Unknown")

            logger.info(
                f"{pool_addr:<45} | {buy_time:<25} | {sell_time:<25} | {buy_mc:<15} | {sell_mc:<15} | {profit_pct:<10} | {exit_reason:<20}"
            )

    return metrics
