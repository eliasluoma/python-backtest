#!/usr/bin/env python3
"""
Backtest Runner for Solana Trading Simulator

This module provides the main class for running backtesting simulations
with Solana trading data. It orchestrates the buy and sell simulation processes.
"""

import logging
import pandas as pd
from typing import Dict, Optional, List

from src.data.firebase_service import FirebaseService
from src.simulation.buy_simulator import BuySimulator
from src.simulation.sell_simulator import SellSimulator
from src.utils.firebase_utils import preprocess_market_data

# Configure logging
logger = logging.getLogger(__name__)


def calculate_percentage_change(series, window=5):
    """
    Calculate the percentage change over a window of data points.

    Args:
        series: Pandas Series containing numeric values
        window: Number of data points to consider for the change

    Returns:
        Series with percentage changes
    """
    return series.pct_change(periods=window) * 100


def calculate_absolute_change(series, window=5):
    """
    Calculate the absolute change over a window of data points.

    Args:
        series: Pandas Series containing numeric values
        window: Number of data points to consider for the change

    Returns:
        Series with absolute changes
    """
    return series.diff(periods=window)


def preprocess_pool_data(df):
    """
    Preprocess the pool data by ensuring all required columns exist and are properly formatted.

    Args:
        df: DataFrame with raw pool data

    Returns:
        DataFrame with preprocessed data
    """
    # Ensure that we have a DataFrame to work with
    if df is None or df.empty:
        return pd.DataFrame()

    # Convert timestamp to datetime if it exists
    if "timestamp" in df.columns:
        # Try to handle various timestamp formats
        try:
            df["timestamp"] = pd.to_datetime(df["timestamp"], format="mixed")
        except Exception as e:
            logger.warning(f"Error converting timestamps with mixed format: {str(e)}")
            # Try with ISO format
            try:
                df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601")
            except Exception as e:
                logger.warning(f"Error converting timestamps with ISO8601 format: {str(e)}")
                # Fall back to the most flexible approach
                try:
                    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
                    # Drop rows with invalid timestamps
                    df = df.dropna(subset=["timestamp"])
                except Exception as e:
                    logger.error(f"Failed to parse timestamps: {str(e)}")
                    return pd.DataFrame()

    # Ensure the DataFrame is sorted by timestamp
    if "timestamp" in df.columns:
        df = df.sort_values("timestamp")

    # Calculate additional metrics if needed
    metrics_to_calculate = []

    if "marketCap" in df.columns:
        # Calculate market cap changes
        if "marketCapChange5s" not in df.columns:
            metrics_to_calculate.append("marketCapChange5s")
            df["marketCapChange5s"] = calculate_percentage_change(df["marketCap"], window=5)

        if "marketCapChange30s" not in df.columns:
            metrics_to_calculate.append("marketCapChange30s")
            df["marketCapChange30s"] = calculate_percentage_change(df["marketCap"], window=30)

    if "holders" in df.columns:
        # Calculate holder changes
        if "holderDelta5s" not in df.columns:
            metrics_to_calculate.append("holderDelta5s")
            df["holderDelta5s"] = calculate_absolute_change(df["holders"], window=5)

        if "holderDelta30s" not in df.columns:
            metrics_to_calculate.append("holderDelta30s")
            df["holderDelta30s"] = calculate_absolute_change(df["holders"], window=30)

        if "holderDelta60s" not in df.columns:
            metrics_to_calculate.append("holderDelta60s")
            df["holderDelta60s"] = calculate_absolute_change(df["holders"], window=60)

    # If we calculated any new metrics, log them
    if metrics_to_calculate:
        logger.debug(f"Calculated additional metrics: {', '.join(metrics_to_calculate)}")

    return df


class BacktestRunner:
    """
    Runs backtesting simulations using data directly from Firebase without saving local files.
    """

    def __init__(
        self,
        firebase_credentials: Optional[str] = None,
        env_file: str = ".env.local",
        early_mc_limit: float = 400000,
        min_delay: int = 60,
        max_delay: int = 200,
    ):
        """
        Initialize the backtest runner

        Args:
            firebase_credentials: Firebase credentials JSON string or path to file
            env_file: Path to .env file containing Firebase config
            early_mc_limit: Market cap limit for early filtering
            min_delay: Minimum delay for buy simulation in seconds
            max_delay: Maximum delay for buy simulation in seconds
        """
        # Firebase service receives credential info through environment variables
        # loaded from the env_file. The credentials parameter is not directly used.
        self.firebase_service = FirebaseService()

        # Load environment variables if env_file is provided
        if env_file:
            from dotenv import load_dotenv

            load_dotenv(env_file)

        # If credentials are directly provided, set the environment variable
        if firebase_credentials:
            import os

            os.environ["FIREBASE_CREDENTIALS"] = firebase_credentials

        self.early_mc_limit = early_mc_limit
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.buy_opportunities: List[Dict] = []
        self.trade_results: List[Dict] = []

    def run_simulation(
        self,
        buy_params: Optional[Dict] = None,
        sell_params: Optional[Dict] = None,
        stoploss_params: Optional[Dict] = None,
        momentum_params: Optional[Dict] = None,
        max_pools: Optional[int] = None,
    ):
        """
        Run the full simulation process

        Args:
            buy_params: Parameters for buy strategy
            sell_params: Parameters for sell strategy
            stoploss_params: Parameters for stoploss strategy
            momentum_params: Parameters for momentum strategy
            max_pools: Maximum number of pools to analyze (for testing)
        """
        try:
            # Fetch data from Firebase
            logger.info("Starting data fetch from Firebase...")
            df = self.firebase_service.fetch_market_data()

            # Preprocess data
            df = preprocess_market_data(df)

            # Get unique pools
            unique_pools = df["poolAddress"].unique()
            logger.info(f"Found {len(unique_pools)} unique pools")

            if max_pools:
                unique_pools = unique_pools[:max_pools]
                logger.info(f"Limited to {max_pools} pools for testing")

            # Initialize simulators
            buy_simulator = BuySimulator(
                early_mc_limit=self.early_mc_limit,
                min_delay=self.min_delay,
                max_delay=self.max_delay,
                buy_params=buy_params,
            )

            sell_simulator = SellSimulator(
                stoploss_params=stoploss_params,
                momentum_params=momentum_params,
                **({} if sell_params is None else sell_params),
            )

            # Run buy simulation
            logger.info("Running buy simulation...")
            for pool_addr in unique_pools:
                pool_df = df[df["poolAddress"] == pool_addr].copy()

                # Skip pools with insufficient data
                if len(pool_df) < self.max_delay + 10:
                    logger.debug(f"Skipping pool {pool_addr}: insufficient data")
                    continue

                # Preprocess pool data
                pool_df = preprocess_pool_data(pool_df)

                # Run buy simulation for this pool
                buy_opportunity = buy_simulator.find_buy_opportunity(pool_df)

                if buy_opportunity:
                    self.buy_opportunities.append(buy_opportunity)
                    logger.info(f"Found buy opportunity for pool {pool_addr}")

            logger.info(f"Buy simulation complete. Found {len(self.buy_opportunities)} opportunities.")

            # Run sell simulation
            if self.buy_opportunities:
                logger.info("Running sell simulation...")
                for buy_opportunity in self.buy_opportunities:
                    sell_result = sell_simulator.simulate_sell(buy_opportunity)
                    if sell_result:
                        self.trade_results.append(sell_result)

                logger.info(f"Sell simulation complete. Processed {len(self.trade_results)} trades.")

                # Calculate summary statistics
                if self.trade_results:
                    self._calculate_statistics()
            else:
                logger.warning("No buy opportunities found. Skipping sell simulation.")

        except Exception as e:
            logger.error(f"Error in simulation: {str(e)}")
            raise

    def _calculate_statistics(self):
        """Calculate and display summary statistics for the trades"""
        try:
            profits = [result["profit_ratio"] for result in self.trade_results]

            total_trades = len(profits)
            winning_trades = sum(1 for p in profits if p > 1.0)
            losing_trades = total_trades - winning_trades

            win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0

            avg_profit = sum([(p - 1.0) * 100 for p in profits]) / total_trades if total_trades > 0 else 0

            logger.info("\n=== SIMULATION RESULTS ===")
            logger.info(f"Total trades: {total_trades}")
            logger.info(f"Winning trades: {winning_trades} ({win_rate:.2f}%)")
            logger.info(f"Losing trades: {losing_trades}")
            logger.info(f"Average profit: {avg_profit:.2f}%")

            # Print top 5 most profitable trades
            if self.trade_results:
                sorted_results = sorted(self.trade_results, key=lambda x: x["profit_ratio"], reverse=True)

                logger.info("\nTop 5 profitable trades:")
                for i, trade in enumerate(sorted_results[:5], 1):
                    profit_pct = (trade["profit_ratio"] - 1.0) * 100
                    logger.info(
                        f"{i}. Pool: {trade['pool_address']} - "
                        f"Profit: {profit_pct:.2f}% - "
                        f"Reason: {trade['exit_reason']}"
                    )

        except Exception as e:
            logger.error(f"Error calculating statistics: {str(e)}")
