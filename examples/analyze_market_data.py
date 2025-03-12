#!/usr/bin/env python
"""
Analyze Market Data Example

This script demonstrates how to use the Firebase utilities to fetch and analyze market data.
It includes data fetching, basic statistical analysis, and visualization.
"""

import os
import sys
import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# Set up paths
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Import Firebase utilities
from src.utils.firebase_utils import initialize_firebase, fetch_market_data_for_pool, get_pool_ids
from src.data.firebase_service import FirebaseService


def main():
    """Main function to analyze market data"""
    # Initialize Firebase and fetch data
    logger.info("Initializing Firebase and fetching market data...")

    # Using the FirebaseService class
    firebase_service = FirebaseService()

    # Fetch data with lower minimum data points to get more pools
    market_data = firebase_service.fetch_market_data(
        min_data_points=10,  # Lower minimum to get more data
        max_pools=5,  # Limit to 5 pools for this example
        limit_per_pool=100,  # Get up to 100 data points per pool
    )

    if not market_data:
        logger.error("No market data retrieved. Exiting.")
        return

    logger.info(f"Successfully fetched data for {len(market_data)} pools")

    # Analyze each pool
    for pool_id, df in market_data.items():
        logger.info(f"\n{'='*50}\nAnalyzing pool: {pool_id}\n{'='*50}")
        analyze_pool_data(pool_id, df)

    logger.info("Analysis complete")


def analyze_pool_data(pool_id, df):
    """
    Analyze the data for a specific pool

    Args:
        pool_id: The ID of the pool
        df: DataFrame containing the pool's data
    """
    # Basic data exploration
    logger.info(f"Data shape: {df.shape}")
    logger.info(f"Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")

    # Check available columns
    logger.info(f"Available columns: {', '.join(df.columns)}")

    # Basic statistics for key metrics
    numeric_columns = ["marketCap", "holdersCount", "priceChangePercent"]
    numeric_columns = [col for col in numeric_columns if col in df.columns]

    if numeric_columns:
        logger.info("\nKey metrics statistics:")
        logger.info(df[numeric_columns].describe().to_string())

    # Plot time series for key metrics
    plot_time_series(pool_id, df)

    # Analyze market cap changes
    analyze_market_cap_changes(pool_id, df)

    # Analyze holder growth
    analyze_holder_growth(pool_id, df)

    # Analyze trading patterns
    analyze_trading_patterns(pool_id, df)


def plot_time_series(pool_id, df):
    """Plot time series for key metrics"""
    if len(df) < 2:
        logger.warning("Not enough data points for time series plot")
        return

    # Create a figure with subplots
    fig, axes = plt.subplots(3, 1, figsize=(12, 15), sharex=True)
    fig.suptitle(f"Market Data Analysis for Pool {pool_id}", fontsize=16)

    # Plot market cap
    if "marketCap" in df.columns:
        axes[0].plot(df["timestamp"], df["marketCap"], "b-", label="Market Cap")
        axes[0].set_title("Market Cap Over Time")
        axes[0].set_ylabel("Market Cap")
        axes[0].grid(True)
        axes[0].legend()

    # Plot holders count
    if "holdersCount" in df.columns:
        axes[1].plot(df["timestamp"], df["holdersCount"], "g-", label="Holders Count")
        axes[1].set_title("Holders Count Over Time")
        axes[1].set_ylabel("Number of Holders")
        axes[1].grid(True)
        axes[1].legend()

    # Plot price change percent
    if "priceChangePercent" in df.columns:
        axes[2].plot(df["timestamp"], df["priceChangePercent"], "r-", label="Price Change %")
        axes[2].set_title("Price Change Percentage Over Time")
        axes[2].set_ylabel("Price Change %")
        axes[2].set_xlabel("Time")
        axes[2].grid(True)
        axes[2].legend()

    plt.tight_layout()
    plt.subplots_adjust(top=0.95)

    # Save the figure
    save_dir = os.path.join(project_root, "outputs")
    os.makedirs(save_dir, exist_ok=True)
    plt.savefig(os.path.join(save_dir, f"pool_{pool_id}_time_series.png"))
    logger.info(f"Time series plot saved to outputs/pool_{pool_id}_time_series.png")
    plt.close()


def analyze_market_cap_changes(pool_id, df):
    """Analyze market cap changes over different time intervals"""
    if len(df) < 10:
        logger.warning("Not enough data points for market cap change analysis")
        return

    # Check which market cap change columns are available
    change_columns = [col for col in df.columns if "marketCapChange" in col]

    if not change_columns:
        logger.warning("No market cap change columns found in data")
        return

    logger.info("\nMarket Cap Change Analysis:")
    for col in change_columns:
        if col in df.columns:
            mean_change = df[col].mean()
            median_change = df[col].median()
            max_change = df[col].max()
            min_change = df[col].min()

            logger.info(
                f"{col}: Mean={mean_change:.2f}, Median={median_change:.2f}, "
                f"Max={max_change:.2f}, Min={min_change:.2f}"
            )

    # Plot market cap changes
    fig, ax = plt.subplots(figsize=(12, 8))

    for col in change_columns:
        if col in df.columns:
            ax.plot(df["timestamp"], df[col], label=col)

    ax.set_title(f"Market Cap Changes for Pool {pool_id}")
    ax.set_xlabel("Time")
    ax.set_ylabel("Market Cap Change")
    ax.grid(True)
    ax.legend()

    # Save the figure
    save_dir = os.path.join(project_root, "outputs")
    os.makedirs(save_dir, exist_ok=True)
    plt.savefig(os.path.join(save_dir, f"pool_{pool_id}_market_cap_changes.png"))
    logger.info(f"Market cap changes plot saved to outputs/pool_{pool_id}_market_cap_changes.png")
    plt.close()


def analyze_holder_growth(pool_id, df):
    """Analyze holder growth over time"""
    if "holdersCount" not in df.columns or len(df) < 2:
        logger.warning("Holder count data not available for analysis")
        return

    # Calculate holder growth metrics
    initial_holders = df["holdersCount"].iloc[0]
    final_holders = df["holdersCount"].iloc[-1]
    total_growth = final_holders - initial_holders
    growth_pct = (total_growth / initial_holders * 100) if initial_holders > 0 else 0

    logger.info("\nHolder Growth Analysis:")
    logger.info(f"Initial holders: {initial_holders}")
    logger.info(f"Final holders: {final_holders}")
    logger.info(f"Total growth: {total_growth} holders ({growth_pct:.2f}%)")

    # Calculate growth rate
    if len(df) > 1 and "timestamp" in df.columns:
        time_diff = (df["timestamp"].iloc[-1] - df["timestamp"].iloc[0]).total_seconds() / 3600  # hours
        growth_rate = total_growth / time_diff if time_diff > 0 else 0

        logger.info(f"Growth rate: {growth_rate:.2f} holders per hour")

    # Check which holder delta columns are available
    delta_columns = [col for col in df.columns if "holderDelta" in col]

    if delta_columns:
        logger.info("\nHolder Delta Analysis:")
        for col in delta_columns:
            if col in df.columns:
                mean_delta = df[col].mean()
                positive_pct = (df[col] > 0).mean() * 100

                logger.info(f"{col}: Mean={mean_delta:.2f}, Positive={positive_pct:.2f}%")


def analyze_trading_patterns(pool_id, df):
    """Analyze trading patterns using volume and trade count data"""
    # Check for trading volume columns
    volume_columns = [col for col in df.columns if "Volume" in col]
    trade_count_columns = [col for col in df.columns if "Buy" in col and not "Volume" in col]

    if not volume_columns and not trade_count_columns:
        logger.warning("No trading data available for analysis")
        return

    logger.info("\nTrading Pattern Analysis:")

    # Volume analysis
    if volume_columns:
        logger.info("Volume Analysis:")
        for col in volume_columns:
            if col in df.columns:
                mean_vol = df[col].mean()
                max_vol = df[col].max()

                logger.info(f"{col}: Mean={mean_vol:.4f}, Max={max_vol:.4f}")

    # Trade count analysis
    if trade_count_columns:
        logger.info("\nTrade Count Analysis:")
        for col in trade_count_columns:
            if col in df.columns:
                total_count = df[col].sum()
                max_count = df[col].max()
                non_zero_pct = (df[col] > 0).mean() * 100

                logger.info(f"{col}: Total={total_count:.0f}, Max={max_count:.0f}, " f"Non-zero={non_zero_pct:.2f}%")

    # Buy/sell ratio analysis for each time window
    if "buyVolume5s" in df.columns and "netVolume5s" in df.columns:
        # Calculate sell volume: buyVolume - netVolume
        df["sellVolume5s"] = df["buyVolume5s"] - df["netVolume5s"]

        # Calculate buy/sell ratio where sell volume is not zero
        valid_rows = df["sellVolume5s"] > 0
        if valid_rows.any():
            buy_sell_ratio = df.loc[valid_rows, "buyVolume5s"] / df.loc[valid_rows, "sellVolume5s"]
            mean_ratio = buy_sell_ratio.mean()
            median_ratio = buy_sell_ratio.median()

            logger.info(f"\nBuy/Sell Ratio (5s): Mean={mean_ratio:.2f}, Median={median_ratio:.2f}")


if __name__ == "__main__":
    main()
