"""
Data processor for Solana trading simulator.

This module provides functionality to preprocess and enrich market data
for backtesting trading strategies.
"""

import pandas as pd
import numpy as np
from typing import Optional, Dict, List
import logging

# Configure logging
logger = logging.getLogger("DataProcessor")


def preprocess_pool_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess pool data for backtesting.

    Args:
        df: DataFrame containing pool data

    Returns:
        Preprocessed DataFrame
    """
    logger.info(f"Preprocessing pool data with {len(df)} records")

    if df.empty:
        logger.warning("Empty DataFrame provided for preprocessing")
        return df

    # Make a copy to avoid modifying the original
    df = df.copy()

    # Sort by timestamp
    df = df.sort_values("timestamp")

    # Calculate additional metrics that might be needed for the simulations
    try:
        df = calculate_derived_metrics(df)
    except Exception as e:
        logger.error(f"Error calculating derived metrics: {str(e)}")

    return df


def calculate_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate derived metrics needed for trading strategies.

    Args:
        df: DataFrame containing pool data

    Returns:
        DataFrame with additional metrics
    """
    # Ensure required columns exist
    required_columns = ["marketCap", "timestamp", "poolAddress"]
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Required column '{col}' not found in pool data")

    # Calculate market cap change percentages for different time windows
    # These are calculated as percent changes over time windows
    for window in [5, 10, 30, 60]:
        col_name = f"marketCapChange{window}s"
        if col_name not in df.columns:
            try:
                df[col_name] = calculate_metric_change(df, "marketCap", window)
            except Exception as e:
                logger.warning(f"Could not calculate {col_name}: {str(e)}")
                df[col_name] = 0.0

    # Calculate holder changes for different time windows
    if "holdersCount" in df.columns:
        for window in [5, 10, 30, 60]:
            col_name = f"holderDelta{window}s"
            if col_name not in df.columns:
                try:
                    df[col_name] = calculate_holder_delta(df, window)
                except Exception as e:
                    logger.warning(f"Could not calculate {col_name}: {str(e)}")
                    df[col_name] = 0

    # Calculate buy/sell volume metrics if transaction data is available
    if "buyVolume5s" not in df.columns and "buyTxns" in df.columns:
        try:
            df["buyVolume5s"] = calculate_buy_volume(df, 5)
        except Exception as e:
            logger.warning(f"Could not calculate buyVolume5s: {str(e)}")
            df["buyVolume5s"] = 0.0

    # Calculate net volume (buy - sell) if possible
    if "netVolume5s" not in df.columns and "buyVolume5s" in df.columns and "sellVolume5s" in df.columns:
        df["netVolume5s"] = df["buyVolume5s"] - df["sellVolume5s"]

    # Calculate large buys if not already present
    if "largeBuy5s" not in df.columns and "buyVolume5s" in df.columns:
        # A large buy is defined as > 0.5 SOL in this example
        # This threshold should be configurable
        df["largeBuy5s"] = (df["buyVolume5s"] > 0.5).astype(int)

    return df


def calculate_metric_change(df: pd.DataFrame, metric: str, window: int) -> pd.Series:
    """
    Calculate percentage change in a metric over a time window.

    Args:
        df: DataFrame containing pool data
        metric: Metric column name
        window: Time window in seconds

    Returns:
        Series containing percentage changes
    """
    # Use shift to compare with previous rows
    return ((df[metric] / df[metric].shift(window)) - 1) * 100


def calculate_holder_delta(df: pd.DataFrame, window: int) -> pd.Series:
    """
    Calculate absolute change in holders over a time window.

    Args:
        df: DataFrame containing pool data
        window: Time window in seconds

    Returns:
        Series containing holder deltas
    """
    return df["holdersCount"] - df["holdersCount"].shift(window)


def calculate_buy_volume(df: pd.DataFrame, window: int) -> pd.Series:
    """
    Calculate buy volume over a time window.

    Args:
        df: DataFrame containing pool data
        window: Time window in seconds

    Returns:
        Series containing buy volumes
    """
    # This is a simplified implementation and should be adapted based on actual data structure
    if "buyTxns" in df.columns and "buyVolume" in df.columns:
        # If transaction data is available in columns
        return df["buyVolume"]

    # Default implementation if specific columns are not available
    return pd.Series([0.0] * len(df), index=df.index)


def filter_pools(df: pd.DataFrame, min_data_points: int = 100) -> Dict[str, pd.DataFrame]:
    """
    Filter and group pools with sufficient data.

    Args:
        df: DataFrame containing market data for multiple pools
        min_data_points: Minimum number of data points required for a pool

    Returns:
        Dictionary of DataFrames grouped by pool address
    """
    logger.info(f"Filtering pools with minimum {min_data_points} data points")

    # Group by pool address
    grouped = df.groupby("poolAddress")

    # Filter pools with sufficient data
    valid_pools = {}
    for pool_addr, pool_df in grouped:
        if len(pool_df) >= min_data_points:
            valid_pools[pool_addr] = pool_df.copy()
            logger.debug(f"Pool {pool_addr} passed filter with {len(pool_df)} data points")
        else:
            logger.debug(f"Pool {pool_addr} filtered out with only {len(pool_df)} data points")

    logger.info(f"Retained {len(valid_pools)} pools out of {len(grouped)} after filtering")
    return valid_pools
