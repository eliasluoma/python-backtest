"""
Simulation command for the Solana Trading Simulator CLI.

This module provides command-line interface for running trading simulations.
"""

import argparse
import logging
import traceback
import pandas as pd
import warnings
import os
import multiprocessing
import concurrent.futures
import sqlite3
import time
from datetime import datetime
import json
import csv

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
    data_group.add_argument(
        "--max-pools", type=int, default=None, help="Maximum number of pools to analyze (None for unlimited)"
    )
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
    buy_group.add_argument(
        "--min-net-volume-5s",
        type=float,
        default=0.0,
        help="Minimum net volume (buy-sell) threshold (5s window, default: 0.0)",
    )

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
    sell_group.add_argument(
        "--stop-loss-confirmation",
        type=int,
        default=6,
        help="Number of consecutive data points below stop loss to trigger sell (default: 6)",
    )
    sell_group.add_argument("--skip-sell", action="store_true", help="Skip sell simulation (for buy testing)")

    # Output parameters
    output_group = simulate_parser.add_argument_group("Output Parameters")
    output_group.add_argument("--plot", action="store_true", help="Generate and save price charts")
    output_group.add_argument("--plot-dir", type=str, default="plots", help="Directory to save price charts")
    output_group.add_argument("--save-results", action="store_true", help="Save simulation results to a file")
    output_group.add_argument("--results-file", type=str, default="results.json", help="Path to save results JSON")
    output_group.add_argument(
        "--output-format", type=str, default="json", help="Output format for results ('json' or 'csv')"
    )

    # Grid testing parameters
    grid_group = simulate_parser.add_argument_group("Grid Testing Parameters")
    grid_group.add_argument("--grid-test", action="store_true", help="Enable grid testing for parameter optimization")
    grid_group.add_argument(
        "--test-params",
        type=str,
        choices=["all", "take-profit", "stop-loss", "low-performance", "trailing-stop", "stop-loss-confirmation"],
        default="all",
        help="Parameters to test in grid testing",
    )
    grid_group.add_argument(
        "--take-profit-values",
        type=str,
        default="1.9",
        help="Comma-separated take profit values to test (e.g., '1.5,1.7,1.9,2.1')",
    )
    grid_group.add_argument(
        "--stop-loss-values",
        type=str,
        default="0.65",
        help="Comma-separated stop loss values to test (e.g., '0.5,0.6,0.65,0.7')",
    )
    grid_group.add_argument(
        "--trailing-stop-values",
        type=str,
        default="0.9",
        help="Comma-separated trailing stop values to test (e.g., '0.85,0.9,0.95')",
    )
    grid_group.add_argument(
        "--stop-loss-confirmation-values",
        type=str,
        default="6",
        help="Comma-separated stop loss confirmation count values to test (e.g., '1,3,6,9')",
    )
    grid_group.add_argument(
        "--lp-threshold-values",
        type=str,
        default="2.5",
        help="Comma-separated low performance threshold values to test (e.g., '-2.5,0.0,2.5')",
    )
    grid_group.add_argument(
        "--max-combinations", type=int, default=100, help="Maximum number of parameter combinations to test"
    )
    grid_group.add_argument(
        "--grid-pools-limit",
        type=int,
        default=9999999,
        help="Maximum number of pools to test in grid testing (to speed up testing)",
    )

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

    # Check if grid testing is enabled
    if args.grid_test:
        return run_grid_testing(args)

    # Extract buy parameters
    buy_params = {
        "mc_change_5s": args.mc_change_5s,
        "holder_delta_30s": args.holder_delta_30s,
        "buy_volume_5s": args.buy_volume_5s,
        "min_net_volume_5s": args.min_net_volume_5s if hasattr(args, "min_net_volume_5s") else 0.0,
    }

    # Extract sell parameters
    sell_params = {
        "take_profit": args.take_profit,
        "stop_loss": args.stop_loss,
        "trailing_stop": args.trailing_stop,
    }

    # Add stoploss parameters if provided
    if hasattr(args, "stop_loss_confirmation"):
        # Hae ensin oletusparametrit
        from src.simulation.sell_simulator import get_default_stoploss_params

        stoploss_params = get_default_stoploss_params()
        # Ylikirjoita vain tarvittava parametri
        stoploss_params["stop_loss_confirmation_count"] = args.stop_loss_confirmation
        sell_params["stoploss_params"] = stoploss_params

    # Check if we should use local database
    if args.use_local_db:
        return run_simulation_with_local_db(args, buy_params, sell_params)
    else:
        try:
            # Import here to avoid circular imports
            from src.simulation.backtest_runner import BacktestRunner

            # For Firebase simulation, use BacktestRunner
            # Note: BacktestRunner currently expects positional arguments
            # BacktestRunner will handle None value for max_pools correctly based on implementation
            runner = BacktestRunner(
                args.max_pools,  # None means all pools will be processed
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


def process_single_pool(pool_data):
    """
    Process a single pool in a separate process.

    Args:
        pool_data: Dictionary containing parameters needed for pool processing

    Returns:
        Dictionary with buy opportunity if found, None otherwise
    """
    pool_id = pool_data["pool_id"]
    db_path = pool_data["db_path"]
    min_delay = pool_data["min_delay"]
    buy_simulator = pool_data["buy_simulator"]

    try:
        # Create a new logger for this process to avoid shared resource issues
        process_logger = logging.getLogger(f"PoolProcessor-{os.getpid()}")
        process_logger.setLevel(logging.INFO)

        # Directly query the SQLite database for market data
        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)

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
                # Käytä yksinkertaista ja luotettavaa tapaa aikaleiman muuntamiseen
                # ISO 8601 -formaatti toimii suoraan ilman format-parametria
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
                # Pudota rivit, joissa on NaT (Not a Time) arvoja
                invalid_count = df["timestamp"].isna().sum()
                if invalid_count > 0:
                    process_logger.warning(f"Dropping {invalid_count} rows with invalid timestamps")
                    df = df.dropna(subset=["timestamp"])
            except Exception as e:
                process_logger.warning(f"Error converting timestamps for pool {pool_id}: {str(e)}")
                # Jos kaikki muu epäonnistuu, käytä viimeisenä keinona aikaleiman poistamista kokonaan
                if "timestamp" in df.columns:
                    process_logger.warning("Unable to convert timestamps, dropping timestamp column")
                    df = df.drop(columns=["timestamp"])

        process_logger.info(f"Successfully loaded {len(df)} rows of market data for pool {pool_id}")

        if df.empty or len(df) < min_delay + 10:
            process_logger.warning(f"Skipping pool {pool_id}: insufficient data (only {len(df)} rows)")
            return {"pool_id": pool_id, "buy_opportunity": None, "error": None}

        # Preprocess pool data
        from src.simulation.backtest_runner import preprocess_pool_data

        # Lisätään tietotyyppien automaattinen muunnos: muunnetaan merkkijonot numeroiksi
        # ennen kuin simulaatiokoodia ajetaan
        numeric_columns = [
            # Markkinakapitalisaation kentät
            "marketCap",
            "athMarketCap",
            "minMarketCap",
            "marketCapChange5s",
            "marketCapChange10s",
            "marketCapChange30s",
            "marketCapChange60s",
            "maMarketCap10s",
            "maMarketCap30s",
            "maMarketCap60s",
            # Hinnan kentät
            "currentPrice",
            "priceChangePercent",
            "priceChangeFromStart",
            # Volyymin kentät
            "buyVolume5s",
            "buyVolume10s",
            "netVolume5s",
            "netVolume10s",
            # Trade data kentät (5s)
            "trade_last5Seconds_volume_buy",
            "trade_last5Seconds_volume_sell",
            "trade_last5Seconds_volume_bot",
            # Trade data kentät (10s)
            "trade_last10Seconds_volume_buy",
            "trade_last10Seconds_volume_sell",
            "trade_last10Seconds_volume_bot",
            # Muut numeeriset kentät, joita mahdollisesti käytetään laskennassa
            "buySellRatio5s",
            "buySellRatio10s",
            "largeBuys5s",
            "bigBuys5s",
            "superBuys5s",
            "largeBuys10s",
            "bigBuys10s",
            "superBuys10s",
            "holdersGrowthFromStart",
            "totalVolume",
        ]

        for col in numeric_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                except Exception as e:
                    process_logger.warning(f"Virhe muunnettaessa saraketta {col} numeeriseksi: {str(e)}")

        df = preprocess_pool_data(df)

        # Run buy simulation for this pool
        buy_opportunity = buy_simulator.find_buy_opportunity(df)

        if buy_opportunity:
            buy_opportunity["pool_address"] = pool_id  # Ensure pool ID is included
            process_logger.info(f"Found buy opportunity for pool {pool_id}")
            return {"pool_id": pool_id, "buy_opportunity": buy_opportunity, "error": None}
        else:
            process_logger.info(f"No buy opportunity found for pool {pool_id}")
            return {"pool_id": pool_id, "buy_opportunity": None, "error": None}
    except Exception as e:
        error_msg = f"Error processing pool {pool_id}: {str(e)}"
        return {"pool_id": pool_id, "buy_opportunity": None, "error": error_msg}


def process_sell(buy_opp, sell_simulator):
    """
    Process sell simulation for a single buy opportunity in a separate process.

    Args:
        buy_opp: Buy opportunity dictionary
        sell_simulator: SellSimulator instance to use

    Returns:
        Dictionary with trade result if successful, None otherwise
    """
    try:
        return sell_simulator.simulate_sell(buy_opp)
    except Exception as e:
        logger.error(f"Error in sell simulation for pool {buy_opp['pool_address']}: {str(e)}")
        return None


def process_sell_wrapper(args):
    """
    Wrapper function to unpack arguments for process_sell.
    This is necessary because multiprocessing cannot pickle lambda functions.

    Args:
        args: Tuple containing (buy_opp, sell_simulator)

    Returns:
        Result from process_sell
    """
    buy_opp, sell_simulator = args
    return process_sell(buy_opp, sell_simulator)


def run_simulation_with_local_db(args, buy_params, sell_params) -> int:
    """
    Run simulation using local SQLite database.

    Args:
        args: Command line arguments
        buy_params: Buy parameters dictionary
        sell_params: Sell parameters dictionary

    Returns:
        Dict or int: Dictionary of metrics if successful, int error code otherwise
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
            "2oGVitjzxUECEsfBxXdTobkekAkSStbTJtpNJoqSNfgd",  # Käyttäjän pyytämä pooli
        ]

        # Initialize simulators
        buy_simulator = BuySimulator(
            early_mc_limit=args.early_mc_limit,
            min_delay=args.min_delay,
            max_delay=args.max_delay,
            buy_params=buy_params,
        )

        # Handle momentum parameters if provided
        momentum_params = sell_params.get("momentum_params")

        sell_simulator = SellSimulator(
            base_take_profit=sell_params["take_profit"],
            stop_loss=sell_params["stop_loss"],
            trailing_stop=sell_params["trailing_stop"],
            stoploss_params=sell_params.get("stoploss_params"),
            momentum_params=momentum_params,
        )

        # Collection of buy opportunities and trade results
        buy_opportunities = []
        trade_results = []

        # Use the specific test pools if enabled
        pool_ids = (
            test_pool_ids
            if use_specific_pools
            else cache_service.get_pool_ids(limit=args.max_pools if args.max_pools else 999999999)
        )

        # Store the total number of pools for the summary
        total_pools = len(pool_ids)

        # Limit the number of pools to process if max_pools is specified
        if args.max_pools and len(pool_ids) > args.max_pools:
            logger.info(f"Limiting to {args.max_pools} pools (from {len(pool_ids)} available)")
            pool_ids = pool_ids[: args.max_pools]
        else:
            logger.info(f"Processing all {len(pool_ids)} pools")

        # Log timestamp handling message once
        logger.info("Converting timestamps - this may take a moment...")

        # Calculate number of workers (use 90% of available cores)
        num_cores = multiprocessing.cpu_count()
        num_workers = max(1, int(num_cores * 0.9))
        logger.info(f"Using {num_workers} workers out of {num_cores} available cores (90%)")

        # Prepare pool data for parallel processing
        pool_data_list = []
        for pool_id in pool_ids:
            pool_data_list.append(
                {
                    "pool_id": pool_id,
                    "db_path": args.db_path,
                    "min_delay": args.min_delay,
                    "buy_simulator": buy_simulator,
                }
            )

        # Timestamp for measuring performance
        start_time = time.time()

        # Process pools in parallel using ProcessPoolExecutor
        with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
            # Submit jobs
            futures = [executor.submit(process_single_pool, pool_data) for pool_data in pool_data_list]

            # Track progress
            total_pools = len(pool_ids)
            completed = 0

            # Process results as they complete
            for future in concurrent.futures.as_completed(futures):
                completed += 1
                # Show progress periodically
                if completed % max(1, total_pools // 20) == 0 or completed == total_pools:
                    logger.info(
                        f"Progress: {completed}/{total_pools} pools processed ({completed/total_pools*100:.1f}%)"
                    )

                try:
                    result = future.result()
                    if result["error"]:
                        logger.error(result["error"])
                    elif result["buy_opportunity"]:
                        buy_opportunities.append(result["buy_opportunity"])
                except Exception as e:
                    logger.error(f"Error in worker process: {str(e)}")

        # Calculate execution time
        execution_time = time.time() - start_time
        logger.info(f"Buy simulation completed in {execution_time:.2f} seconds")
        logger.info(f"Buy simulation complete. Found {len(buy_opportunities)} opportunities.")

        # Skip sell simulation if requested
        if args.skip_sell:
            logger.info("Skipping sell simulation as requested.")
            return 0

        # Run sell simulation on buy opportunities
        logger.info("Running sell simulation...")

        # Sell simulation can also be parallelized
        with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
            # Prepare arguments for process_sell function (needs both buy_opportunity and sell_simulator)
            sell_tasks = [(buy_opp, sell_simulator) for buy_opp in buy_opportunities]

            # Submit all buy opportunities for sell simulation using process_sell_wrapper instead of lambda
            sell_results = list(executor.map(process_sell_wrapper, sell_tasks))

            # Filter out None results and add valid trades to results
            trade_results = [result for result in sell_results if result is not None]

        logger.info(f"Sell simulation complete. Processed {len(trade_results)} trades.")

        # Calculate summary statistics
        metrics = calculate_and_display_stats(trade_results, output_format=args.output_format, total_pools=total_pools)

        # When in grid testing mode, return metrics instead of exit code
        if getattr(args, "grid_test", False):
            return metrics

        return 0

    except Exception as e:
        logger.error(f"Local simulation failed: {str(e)}")
        logger.error(traceback.format_exc())
        return 1


def calculate_and_display_stats(trade_results, metrics=None, output_format="json", total_pools=0):
    """
    Calculate and display trading statistics

    Args:
        trade_results: List of trade result dictionaries
        metrics: Optional pre-defined metrics dictionary
        output_format: Format to save results ('json' or 'csv')
        total_pools: Total number of pools that were simulated

    Returns:
        Dictionary of calculated metrics
    """
    logger = logging.getLogger(__name__)

    if not trade_results:
        logger.warning("No trades to analyze")
        return

    if metrics is None:
        metrics = {}

    # Initialize metrics with default values
    default_metrics = {
        "total_pools": total_pools,
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

    # Calculate Solana-based investment results
    metrics["initial_investment_sol"] = metrics["total_trades"]  # 1 SOL per trade

    # Calculate final amounts for each trade (assuming 1 SOL initial investment)
    sol_amounts = []
    for result in trade_results:
        profit_ratio = result.get("profit_ratio", 1.0)
        sol_amounts.append(profit_ratio)  # Final SOL amount after trade

    metrics["final_amounts_sol"] = sol_amounts
    metrics["total_final_amount_sol"] = sum(sol_amounts)
    metrics["profit_loss_sol"] = metrics["total_final_amount_sol"] - metrics["initial_investment_sol"]
    metrics["roi_percentage"] = (
        (metrics["profit_loss_sol"] / metrics["initial_investment_sol"] * 100)
        if metrics["initial_investment_sol"] > 0
        else 0
    )

    # Display results in the requested order
    logger.info("\n=== SIMULATION RESULTS ===")

    # Display results in the order specified by the user
    logger.info(f"Total pools simulated: {metrics['total_pools']}")
    logger.info(f"Total trades: {metrics['total_trades']}")
    logger.info(f"Total investment: {metrics['initial_investment_sol']:.2f} SOL")
    logger.info(f"Total final amount: {metrics['total_final_amount_sol']:.3f} SOL")

    profit_loss_text = "Profit" if metrics["profit_loss_sol"] >= 0 else "Loss"
    logger.info(f"{profit_loss_text}: {metrics['profit_loss_sol']:.3f} SOL")
    logger.info(f"ROI percentage: {metrics['roi_percentage']:.2f}%")
    logger.info("-" * 50)

    # Display additional statistics
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

    # Save results to file based on output format
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Create results directory if it doesn't exist
    results_dir = os.path.join("src", "simulation", "results")
    if not os.path.exists(results_dir):
        try:
            os.makedirs(results_dir)
            logger.info(f"Created results directory at {results_dir}")
        except Exception as e:
            logger.error(f"Error creating results directory: {str(e)}")
            results_dir = "."  # Fall back to current directory if can't create results dir

    # Make sure we have detailed trade data for exports
    export_data = {"summary": metrics, "trades": trade_results}

    # Poistetaan pyydetyt tiedot myös JSON-tiedostosta
    if "profit_percentages" in export_data["summary"]:
        del export_data["summary"]["profit_percentages"]
    if "loss_percentages" in export_data["summary"]:
        del export_data["summary"]["loss_percentages"]
    if "durations" in export_data["summary"]:
        del export_data["summary"]["durations"]
    if "final_amounts_sol" in export_data["summary"]:
        del export_data["summary"]["final_amounts_sol"]

    if output_format.lower() == "json":
        output_file = os.path.join(results_dir, f"simulation_results_{timestamp}.json")
        logger.info(f"\nSaving results to {output_file}")
        try:
            with open(output_file, "w") as f:
                json.dump(export_data, f, indent=2, default=str)
            logger.info(f"Results successfully saved to {output_file}")
        except Exception as e:
            logger.error(f"Error saving JSON results: {str(e)}")

    elif output_format.lower() == "csv":
        # Save summary to CSV
        summary_file = os.path.join(results_dir, f"simulation_summary_{timestamp}.csv")
        trades_file = os.path.join(results_dir, f"simulation_trades_{timestamp}.csv")

        logger.info(f"\nSaving summary to {summary_file}")
        logger.info(f"Saving detailed trades to {trades_file}")

        try:
            # Save summary metrics
            with open(summary_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Metric", "Value"])
                for key, value in metrics.items():
                    if key not in ["profit_percentages", "loss_percentages", "durations", "exit_reasons"]:
                        writer.writerow([key, value])

                # Add exit reasons as separate rows
                for reason, count in metrics.get("exit_reasons", {}).items():
                    writer.writerow([f"exit_reason_{reason}", count])

            # Save detailed trade data
            if trade_results:
                try:
                    # Get all possible keys from trade_results
                    all_keys = set()
                    for trade in trade_results:
                        all_keys.update(trade.keys())

                    with open(trades_file, "w", newline="") as f:
                        writer = csv.DictWriter(f, fieldnames=list(all_keys))
                        writer.writeheader()
                        for trade in trade_results:
                            # Convert non-serializable objects to strings
                            row = {
                                k: str(v) if not isinstance(v, (int, float, str, bool, type(None))) else v
                                for k, v in trade.items()
                            }
                            writer.writerow(row)
                except Exception as e:
                    logger.error(f"Error saving detailed trades CSV: {str(e)}")

            logger.info(f"Results successfully saved to CSV files")
        except Exception as e:
            logger.error(f"Error saving CSV results: {str(e)}")

    else:
        logger.warning(f"Unsupported output format: {output_format}. Results not saved to file.")

    return metrics


def run_grid_testing(args) -> int:
    """
    Run grid testing with various parameter combinations.

    Args:
        args: Command-line arguments

    Returns:
        int: Exit code (0 for success, non-zero for errors)
    """
    logger.info("Starting grid testing for parameter optimization...")

    # Parse parameter values from command line arguments
    take_profit_values = [float(x) for x in args.take_profit_values.split(",")]
    stop_loss_values = [float(x) for x in args.stop_loss_values.split(",")]
    trailing_stop_values = [float(x) for x in args.trailing_stop_values.split(",")]
    lp_threshold_values = [float(x) for x in args.lp_threshold_values.split(",")]
    stop_loss_confirmation_values = [int(x) for x in args.stop_loss_confirmation_values.split(",")]

    # Log parameter values
    logger.info(f"Take profit values: {take_profit_values}")
    logger.info(f"Stop loss values: {stop_loss_values}")
    logger.info(f"Trailing stop values: {trailing_stop_values}")
    logger.info(f"Low performance threshold values: {lp_threshold_values}")
    logger.info(f"Stop loss confirmation values: {stop_loss_confirmation_values}")

    # Determine which parameters to test
    test_params = args.test_params.lower()

    # Generate parameter combinations based on selected test parameters
    param_combinations = []

    # If only one value is provided for all parameters, simply test that combination
    if (
        len(take_profit_values) == 1
        and len(stop_loss_values) == 1
        and len(trailing_stop_values) == 1
        and len(lp_threshold_values) == 1
        and len(stop_loss_confirmation_values) == 1
        and test_params == "all"
    ):

        param_combinations.append(
            {
                "base_take_profit": take_profit_values[0],
                "stop_loss": stop_loss_values[0],
                "trailing_stop": trailing_stop_values[0],
                "momentum_params": {"lp_holder_growth_threshold": lp_threshold_values[0]},
                "stoploss_params": {"stop_loss_confirmation_count": stop_loss_confirmation_values[0]},
            }
        )

        logger.info(f"Testing single parameter combination with all values provided")

    # Otherwise, generate combinations based on selected test parameters
    else:
        # For each parameter type, use either specified values or default value
        if test_params == "take-profit" or test_params == "all":
            for tp in take_profit_values:
                # Use default values for other parameters if testing only this one
                sl = stop_loss_values[0]
                ts = trailing_stop_values[0]
                lp = lp_threshold_values[0]
                slc = stop_loss_confirmation_values[0]

                param_combinations.append(
                    {
                        "base_take_profit": tp,
                        "stop_loss": sl,
                        "trailing_stop": ts,
                        "momentum_params": {"lp_holder_growth_threshold": lp},
                        "stoploss_params": {"stop_loss_confirmation_count": slc},
                    }
                )

        if test_params == "stop-loss" or test_params == "all":
            for sl in stop_loss_values:
                # Skip if already added in take-profit test with default values
                if test_params == "all" and len(take_profit_values) == 1 and len(stop_loss_values) > 1:
                    tp = take_profit_values[0]
                    ts = trailing_stop_values[0]
                    lp = lp_threshold_values[0]
                    slc = stop_loss_confirmation_values[0]

                    param_combinations.append(
                        {
                            "base_take_profit": tp,
                            "stop_loss": sl,
                            "trailing_stop": ts,
                            "momentum_params": {"lp_holder_growth_threshold": lp},
                            "stoploss_params": {"stop_loss_confirmation_count": slc},
                        }
                    )

        if test_params == "trailing-stop" or test_params == "all":
            for ts in trailing_stop_values:
                # Skip if already added in previous tests with default values
                if test_params == "all" and len(trailing_stop_values) > 1:
                    tp = take_profit_values[0]
                    sl = stop_loss_values[0]
                    lp = lp_threshold_values[0]
                    slc = stop_loss_confirmation_values[0]

                    param_combinations.append(
                        {
                            "base_take_profit": tp,
                            "stop_loss": sl,
                            "trailing_stop": ts,
                            "momentum_params": {"lp_holder_growth_threshold": lp},
                            "stoploss_params": {"stop_loss_confirmation_count": slc},
                        }
                    )

        if test_params == "low-performance" or test_params == "all":
            for lp in lp_threshold_values:
                # Korjattu ehto: Lisää parametriyhdistelmät aina low-performance testeille
                # ja myös "all"-testeille jos lp_threshold_values listassa on useita arvoja
                if test_params == "low-performance" or (test_params == "all" and len(lp_threshold_values) > 1):
                    tp = take_profit_values[0]
                    sl = stop_loss_values[0]
                    ts = trailing_stop_values[0]
                    slc = stop_loss_confirmation_values[0]

                    param_combinations.append(
                        {
                            "base_take_profit": tp,
                            "stop_loss": sl,
                            "trailing_stop": ts,
                            "momentum_params": {"lp_holder_growth_threshold": lp},
                            "stoploss_params": {"stop_loss_confirmation_count": slc},
                        }
                    )

        if test_params == "stop-loss-confirmation" or test_params == "all":
            for slc in stop_loss_confirmation_values:
                # Skip if already added in previous tests with default values
                if test_params == "stop-loss-confirmation" or (
                    test_params == "all" and len(stop_loss_confirmation_values) > 1
                ):
                    tp = take_profit_values[0]
                    sl = stop_loss_values[0]
                    ts = trailing_stop_values[0]
                    lp = lp_threshold_values[0]

                    param_combinations.append(
                        {
                            "base_take_profit": tp,
                            "stop_loss": sl,
                            "trailing_stop": ts,
                            "momentum_params": {"lp_holder_growth_threshold": lp},
                            "stoploss_params": {"stop_loss_confirmation_count": slc},
                        }
                    )

        # Handle multi-parameter testing (if multiple parameters have multiple values and test_params is "all")
        if (
            test_params == "all"
            and sum(
                [
                    len(take_profit_values) > 1,
                    len(stop_loss_values) > 1,
                    len(trailing_stop_values) > 1,
                    len(lp_threshold_values) > 1,
                    len(stop_loss_confirmation_values) > 1,
                ]
            )
            > 1
        ):
            logger.info("Multiple parameters have multiple values. Creating a limited set of combinations.")

            # Clear existing combinations to avoid duplication
            param_combinations = []

            # For true grid testing, we'd do a cartesian product of all parameters,
            # but that can explode quickly, so we'll use a more selective approach:

            # Generate a limited number of combinations
            for tp_idx, tp in enumerate(take_profit_values):
                for sl_idx, sl in enumerate(stop_loss_values):
                    for ts_idx, ts in enumerate(trailing_stop_values):
                        for lp_idx, lp in enumerate(lp_threshold_values):
                            for slc_idx, slc in enumerate(stop_loss_confirmation_values):
                                # Create full grid for up to max_combinations
                                if len(param_combinations) < args.max_combinations:
                                    param_combinations.append(
                                        {
                                            "base_take_profit": tp,
                                            "stop_loss": sl,
                                            "trailing_stop": ts,
                                            "momentum_params": {"lp_holder_growth_threshold": lp},
                                            "stoploss_params": {"stop_loss_confirmation_count": slc},
                                        }
                                    )

            logger.info(f"Created {len(param_combinations)} parameter combinations for multi-parameter testing")

    # If there are no combinations, add default values
    if not param_combinations:
        param_combinations.append(
            {
                "base_take_profit": take_profit_values[0],
                "stop_loss": stop_loss_values[0],
                "trailing_stop": trailing_stop_values[0],
                "momentum_params": {"lp_holder_growth_threshold": lp_threshold_values[0]},
                "stoploss_params": {"stop_loss_confirmation_count": stop_loss_confirmation_values[0]},
            }
        )

    # Limit number of combinations if needed
    if len(param_combinations) > args.max_combinations:
        logger.warning(
            f"Limiting to {args.max_combinations} parameter combinations (from {len(param_combinations)} possible)"
        )
        param_combinations = param_combinations[: args.max_combinations]

    logger.info(f"Testing {len(param_combinations)} parameter combinations")

    # Log unique values for each parameter being tested
    tp_values = sorted(set(combo["base_take_profit"] for combo in param_combinations))
    sl_values = sorted(set(combo["stop_loss"] for combo in param_combinations))
    ts_values = sorted(set(combo["trailing_stop"] for combo in param_combinations))
    lp_values = sorted(set(combo["momentum_params"]["lp_holder_growth_threshold"] for combo in param_combinations))
    slc_values = sorted(set(combo["stoploss_params"]["stop_loss_confirmation_count"] for combo in param_combinations))

    logger.info(f"Unique parameter values in test:")
    logger.info(f"Take profit values: {tp_values}")
    logger.info(f"Stop loss values: {sl_values}")
    logger.info(f"Trailing stop values: {ts_values}")
    logger.info(f"Low performance threshold values: {lp_values}")
    logger.info(f"Stop loss confirmation values: {slc_values}")

    # Store results for each parameter combination
    grid_results = []

    # Buy parameters remain the same for all tests
    buy_params = {
        "mc_change_5s": args.mc_change_5s,
        "holder_delta_30s": args.holder_delta_30s,
        "buy_volume_5s": args.buy_volume_5s,
        "min_net_volume_5s": args.min_net_volume_5s if hasattr(args, "min_net_volume_5s") else 0.0,
    }

    # Limit number of pools for grid testing to speed up process
    max_pools = args.grid_pools_limit if args.grid_pools_limit else args.max_pools

    # Modified args for grid testing
    grid_args = argparse.Namespace(**vars(args))
    grid_args.max_pools = max_pools
    grid_args.save_results = True

    # Run simulation with each parameter combination
    for i, params in enumerate(param_combinations):
        logger.info(f"\n[{i+1}/{len(param_combinations)}] Testing parameters: {params}")

        # Set sell parameters for this run
        grid_args.take_profit = params["base_take_profit"]
        grid_args.stop_loss = params["stop_loss"]
        grid_args.trailing_stop = params["trailing_stop"]
        grid_args.stop_loss_confirmation = params["stoploss_params"]["stop_loss_confirmation_count"]

        # Generate timestamp for this run
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        grid_args.results_file = f"grid_test_results_{timestamp}.json"

        # Set low performance threshold
        lp_threshold = params["momentum_params"]["lp_holder_growth_threshold"]

        # Create sell params
        sell_params = {
            "take_profit": params["base_take_profit"],
            "stop_loss": params["stop_loss"],
            "trailing_stop": params["trailing_stop"],
            "stoploss_params": {
                "stop_loss_confirmation_count": params["stoploss_params"]["stop_loss_confirmation_count"]
            },
        }

        # Add momentum parameters with lp_threshold
        from src.simulation.sell_simulator import get_default_momentum_params, get_default_stoploss_params

        momentum_params = get_default_momentum_params()
        momentum_params["lp_holder_growth_threshold"] = lp_threshold

        # Käytä oletusarvoisia stop loss -parametreja ja korvaa vain stop_loss_confirmation_count
        stoploss_params = get_default_stoploss_params()
        stoploss_params["stop_loss_confirmation_count"] = params["stoploss_params"]["stop_loss_confirmation_count"]
        sell_params["stoploss_params"] = stoploss_params

        # Create run params for this test
        run_params = {
            "buy_params": buy_params,
            "sell_params": sell_params,
            "momentum_params": momentum_params,
            "lp_threshold": lp_threshold,
            "stop_loss_confirmation": params["stoploss_params"]["stop_loss_confirmation_count"],
        }

        try:
            # Log the parameters we're testing
            logger.info(f"Running simulation with:")
            logger.info(f"  take_profit={params['base_take_profit']}")
            logger.info(f"  stop_loss={params['stop_loss']}")
            logger.info(f"  trailing_stop={params['trailing_stop']}")
            logger.info(f"  lp_threshold={lp_threshold}")
            logger.info(f"  stop_loss_confirmation={params['stoploss_params']['stop_loss_confirmation_count']}")

            # Use local DB for grid testing (more efficient)
            grid_args.use_local_db = True
            grid_args.grid_test = True

            # Add momentum params to sell_params
            sell_params_with_momentum = dict(sell_params)
            sell_params_with_momentum["momentum_params"] = momentum_params

            # Run simulation and get results
            result = run_simulation_with_local_db(grid_args, buy_params, sell_params_with_momentum)

            # Store metrics if simulation was successful
            if isinstance(result, dict):
                metrics = result

                # Add results to grid_results
                test_result = {
                    "parameters": params,
                    "timestamp": timestamp,
                    "result_file": grid_args.results_file,
                    "metrics": {
                        "total_trades": metrics.get("total_trades", 0),
                        "profitable_trades": metrics.get("profitable_trades", 0),
                        "win_percentage": metrics.get("win_percentage", 0),
                        "average_profit": metrics.get("average_profit", 0),
                        "average_loss": metrics.get("average_loss", 0),
                        "max_profit": metrics.get("max_profit", 0),
                        "max_loss": metrics.get("max_loss", 0),
                        "profit_factor": metrics.get("profit_factor", 0),
                        "roi_percentage": metrics.get("roi_percentage", 0),
                        "initial_investment_sol": metrics.get("initial_investment_sol", 0),
                        "total_final_amount_sol": metrics.get("total_final_amount_sol", 0),
                        "profit_loss_sol": metrics.get("profit_loss_sol", 0),
                        "exit_reasons": metrics.get("exit_reasons", {}),
                    },
                }

                grid_results.append(test_result)
                logger.info(
                    f"Run completed with ROI: {metrics.get('roi_percentage', 0):.2f}%, "
                    f"Win rate: {metrics.get('win_percentage', 0):.2f}%"
                )
            else:
                logger.warning(f"Simulation run failed for parameters: {params}")

        except Exception as e:
            logger.error(f"Error during grid test with parameters {params}: {str(e)}")
            logger.error(traceback.format_exc())

    # Generate grid test summary
    grid_summary_file = os.path.join(
        "src", "simulation", "results", f"grid_test_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )

    # Sort results by ROI percentage for final report and summary
    if grid_results:
        # Sort by ROI percentage (descending)
        sorted_results = sorted(
            grid_results,
            key=lambda x: (
                x["metrics"]["roi_percentage"] if "metrics" in x and "roi_percentage" in x["metrics"] else -float("inf")
            ),
            reverse=True,
        )

        # Get top 5 results for the summary
        top_results = []
        for i, result in enumerate(sorted_results[:5]):
            params = result["parameters"]
            metrics = result["metrics"]

            # Create simplified summary for the top results
            top_result = {
                "rank": i + 1,
                "parameters": {
                    "take_profit": params["base_take_profit"],
                    "stop_loss": params["stop_loss"],
                    "trailing_stop": params["trailing_stop"],
                    "lp_threshold": params["momentum_params"]["lp_holder_growth_threshold"],
                    "stop_loss_confirmation": params["stoploss_params"]["stop_loss_confirmation_count"],
                },
                "metrics": {
                    "initial_investment_sol": metrics.get("initial_investment_sol", 0),
                    "total_final_amount_sol": metrics.get("total_final_amount_sol", 0),
                    "profit_loss_sol": metrics.get("profit_loss_sol", 0),
                    "roi_percentage": metrics.get("roi_percentage", 0),
                    "win_percentage": metrics.get("win_percentage", 0),
                    "total_trades": metrics.get("total_trades", 0),
                },
            }
            top_results.append(top_result)

        # Add top results to the summary
        summary_data = {
            "grid_parameters": {
                "test_params": test_params,
                "take_profit_values": take_profit_values,
                "stop_loss_values": stop_loss_values,
                "trailing_stop_values": trailing_stop_values,
                "lp_threshold_values": lp_threshold_values,
                "stop_loss_confirmation_values": stop_loss_confirmation_values,
            },
            "top_results": top_results,
            "results": grid_results,
        }

        try:
            with open(grid_summary_file, "w") as f:
                json.dump(summary_data, f, indent=2, default=str)
            logger.info(f"Grid test summary saved to {grid_summary_file}")
        except Exception as e:
            logger.error(f"Error saving grid test summary: {str(e)}")

        # Display top 5 results
        logger.info("\n=== GRID TEST RESULTS (TOP 5) ===")

        for top_result in top_results:
            rank = top_result["rank"]
            params = top_result["parameters"]
            metrics = top_result["metrics"]

            logger.info(f"\n#{rank}: ROI: {metrics['roi_percentage']:.2f}%")
            logger.info(
                f"Parameters: take_profit={params['take_profit']}, stop_loss={params['stop_loss']}, "
                f"trailing_stop={params['trailing_stop']}, lp_threshold={params['lp_threshold']}, "
                f"stop_loss_confirmation={params['stop_loss_confirmation']}"
            )

            # Display the key metrics that user requested
            logger.info(f"Initial investment: {metrics['initial_investment_sol']:.2f} SOL")
            logger.info(f"Total final amount: {metrics['total_final_amount_sol']:.2f} SOL")
            logger.info(f"Profit/Loss: {metrics['profit_loss_sol']:.2f} SOL")
            logger.info(f"ROI percentage: {metrics['roi_percentage']:.2f}%")
            logger.info(f"Win rate: {metrics['win_percentage']:.2f}%")

        logger.info(f"\nFull results available in: {grid_summary_file}")

    return 0
