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
    Preprocess pool data to calculate derived metrics.

    Args:
        df: DataFrame containing market data for a single pool

    Returns:
        DataFrame with additional derived metrics
    """
    logger.info(f"Preprocessing pool data with {len(df)} records")

    try:
        # Make a copy to avoid modifying the original
        df = df.copy()

        # Ensure standardized column naming
        if "poolAddress" in df.columns and "pool_address" not in df.columns:
            df["pool_address"] = df["poolAddress"]
        elif "pool_address" in df.columns and "poolAddress" not in df.columns:
            df["poolAddress"] = df["pool_address"]

        # Ensure timestamp is in datetime format
        if "timestamp" in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
                df["timestamp"] = pd.to_datetime(df["timestamp"], format="mixed", utc=True)

            # Sort by timestamp to ensure proper calculation of changes
            df = df.sort_values("timestamp")

        # Calculate market cap changes
        if "marketCap" in df.columns:
            # Percentage changes over different time windows
            df["marketCapChange5s"] = df["marketCap"].pct_change() * 100
            df["marketCapChange30s"] = df["marketCap"].pct_change(6) * 100  # Assuming 5s intervals
            df["marketCapChange60s"] = df["marketCap"].pct_change(12) * 100

            # Growth from the start of the data
            first_mc = df["marketCap"].iloc[0] if not df.empty else 0
            df["mcGrowthFromStart"] = ((df["marketCap"] - first_mc) / first_mc) * 100 if first_mc > 0 else 0

        # Calculate holder changes
        if "holders" in df.columns:
            # Absolute changes over different time windows
            df["holderDelta5s"] = df["holders"].diff()
            df["holderDelta30s"] = df["holders"].diff(6)  # Assuming 5s intervals
            df["holderDelta60s"] = df["holders"].diff(12)

            # Growth from the start of the data
            first_holders = df["holders"].iloc[0] if not df.empty else 0
            df["holderGrowthFromStart"] = df["holders"] - first_holders

        return df

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


def filter_pools(pools, min_data_points=100):
    """
    Filter pools based on minimum number of data points.

    Args:
        pools: Dict of DataFrames or a single DataFrame with 'pool_address' or 'poolAddress' column
        min_data_points: Minimum number of data points required per pool

    Returns:
        Dict of filtered pool DataFrames
    """
    logger.info(f"Filtering pools with minimum {min_data_points} data points")

    filtered_pools = {}

    # Handle case where input is a dict of DataFrames
    if isinstance(pools, dict):
        for pool_id, pool_df in pools.items():
            if len(pool_df) >= min_data_points:
                filtered_pools[pool_id] = pool_df
            else:
                logger.debug(f"Pool {pool_id} filtered out: only {len(pool_df)} data points")

        return filtered_pools

    # Handle case where input is a single DataFrame
    elif isinstance(pools, pd.DataFrame):
        # Check which column name is used
        pool_col = None
        if "pool_address" in pools.columns:
            pool_col = "pool_address"
        elif "poolAddress" in pools.columns:
            pool_col = "poolAddress"
        else:
            logger.error("No pool address column found in DataFrame")
            return {}

        # Group by pool address
        grouped = pools.groupby(pool_col)

        for pool_id, group in grouped:
            if len(group) >= min_data_points:
                filtered_pools[pool_id] = group
            else:
                logger.debug(f"Pool {pool_id} filtered out: only {len(group)} data points")

        return filtered_pools

    else:
        logger.error(f"Unsupported input type: {type(pools)}")
        return {}
