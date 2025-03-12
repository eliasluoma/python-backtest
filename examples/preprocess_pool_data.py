#!/usr/bin/env python
"""
Preprocess Pool Data for Trading Strategies

This script enhances the pool data by:
1. Adding derived fields that are missing (lastPrice, totalVolume)
2. Creating fallbacks for trade data fields
3. Building a consistent dataset that can be used by all trading strategies
"""

import os
import sys
import logging
import json
from pathlib import Path

# Set up paths
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Import Firebase utilities
from src.data.firebase_service import FirebaseService
from src.utils.firebase_utils import get_pool_ids

# Fields that are always available in pools
RELIABLE_FIELDS = [
    # Market Cap Fields
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
    # Price Fields
    "currentPrice",
    "priceChangePercent",
    "priceChangeFromStart",
    # Holder Fields
    "holdersCount",
    "initialHoldersCount",
    "holdersGrowthFromStart",
    "holderDelta5s",
    "holderDelta10s",
    "holderDelta30s",
    "holderDelta60s",
    # Volume Fields
    "buyVolume5s",
    "buyVolume10s",
    "netVolume5s",
    "netVolume10s",
    # Buy Classification Fields
    "largeBuy5s",
    "largeBuy10s",
    "bigBuy5s",
    "bigBuy10s",
    "superBuy5s",
    "superBuy10s",
    # Metadata
    "poolAddress",
    "timeFromStart",
]

# Fields we need to derive
DERIVED_FIELDS = [
    "lastPrice",  # Previous row's currentPrice
    "totalVolume",  # Cumulative sum of volume
]

# Trade data fields that might be missing
TRADE_DATA_FIELDS = [
    # 5s trade data
    "trade_last5Seconds.volume.buy",
    "trade_last5Seconds.volume.sell",
    "trade_last5Seconds.volume.bot",
    "trade_last5Seconds.tradeCount.buy.small",
    "trade_last5Seconds.tradeCount.buy.medium",
    "trade_last5Seconds.tradeCount.buy.large",
    "trade_last5Seconds.tradeCount.buy.big",
    "trade_last5Seconds.tradeCount.buy.super",
    "trade_last5Seconds.tradeCount.sell.small",
    "trade_last5Seconds.tradeCount.sell.medium",
    "trade_last5Seconds.tradeCount.sell.large",
    "trade_last5Seconds.tradeCount.sell.big",
    "trade_last5Seconds.tradeCount.sell.super",
    "trade_last5Seconds.tradeCount.bot",
    # 10s trade data
    "trade_last10Seconds.volume.buy",
    "trade_last10Seconds.volume.sell",
    "trade_last10Seconds.volume.bot",
    "trade_last10Seconds.tradeCount.buy.small",
    "trade_last10Seconds.tradeCount.buy.medium",
    "trade_last10Seconds.tradeCount.buy.large",
    "trade_last10Seconds.tradeCount.buy.big",
    "trade_last10Seconds.tradeCount.buy.super",
    "trade_last10Seconds.tradeCount.sell.small",
    "trade_last10Seconds.tradeCount.sell.medium",
    "trade_last10Seconds.tradeCount.sell.large",
    "trade_last10Seconds.tradeCount.sell.big",
    "trade_last10Seconds.tradeCount.sell.super",
    "trade_last10Seconds.tradeCount.bot",
]


def add_derived_fields(df):
    """Add derived fields to the dataframe"""
    # Last price (the previous price data point)
    df["lastPrice"] = df["currentPrice"].shift(1)
    # For the first row, use the same value as currentPrice
    df.loc[df.index[0], "lastPrice"] = df.loc[df.index[0], "currentPrice"]

    # Total cumulative volume
    # Calculate based on net volume per time step
    df["volumePerStep"] = df["netVolume5s"].abs()  # Use absolute value of net volume
    df["totalVolume"] = df["volumePerStep"].cumsum()
    df.drop("volumePerStep", axis=1, inplace=True)

    return df


def add_fallback_trade_fields(df):
    """Add fallback values for missing trade data fields"""

    # 5-second trade data fallbacks
    if "trade_last5Seconds.volume.buy" not in df.columns:
        df["trade_last5Seconds.volume.buy"] = df["buyVolume5s"]

    if "trade_last5Seconds.volume.sell" not in df.columns:
        # Derive sell volume from buy and net
        df["trade_last5Seconds.volume.sell"] = df["buyVolume5s"] - df["netVolume5s"]

    # Ensure sell volume is never negative
    if "trade_last5Seconds.volume.sell" in df.columns:
        df["trade_last5Seconds.volume.sell"] = df["trade_last5Seconds.volume.sell"].clip(lower=0)

    # Bot volume fallback (typically very small)
    if "trade_last5Seconds.volume.bot" not in df.columns:
        df["trade_last5Seconds.volume.bot"] = 0

    # Trade count fallbacks for 5s
    for trade_type in ["buy", "sell"]:
        for size in ["small", "medium", "large", "big", "super"]:
            field = f"trade_last5Seconds.tradeCount.{trade_type}.{size}"
            if field not in df.columns:
                # Set fallback values - use zero for sell, proportional values for buy
                if trade_type == "sell":
                    df[field] = 0
                else:
                    # Derive simple values from the buy classification fields
                    if size == "small":
                        df[field] = 1  # Always assume at least one small buy
                    elif size == "medium":
                        df[field] = 0
                    elif size == "large":
                        df[field] = df["largeBuy5s"]
                    elif size == "big":
                        df[field] = df["bigBuy5s"]
                    elif size == "super":
                        df[field] = df["superBuy5s"]

    # 10-second trade data fallbacks (similar approach)
    if "trade_last10Seconds.volume.buy" not in df.columns:
        df["trade_last10Seconds.volume.buy"] = df["buyVolume10s"]

    if "trade_last10Seconds.volume.sell" not in df.columns:
        df["trade_last10Seconds.volume.sell"] = df["buyVolume10s"] - df["netVolume10s"]

    # Ensure sell volume is never negative
    if "trade_last10Seconds.volume.sell" in df.columns:
        df["trade_last10Seconds.volume.sell"] = df["trade_last10Seconds.volume.sell"].clip(lower=0)

    # Bot volume fallback (typically very small)
    if "trade_last10Seconds.volume.bot" not in df.columns:
        df["trade_last10Seconds.volume.bot"] = 0

    # Trade count fallbacks for 10s
    for trade_type in ["buy", "sell"]:
        for size in ["small", "medium", "large", "big", "super"]:
            field = f"trade_last10Seconds.tradeCount.{trade_type}.{size}"
            if field not in df.columns:
                # Set fallback values - use zero for sell, proportional values for buy
                if trade_type == "sell":
                    df[field] = 0
                else:
                    # Derive simple values from the buy classification fields
                    if size == "small":
                        df[field] = 2  # Assume more small buys in 10s window
                    elif size == "medium":
                        df[field] = 0
                    elif size == "large":
                        df[field] = df["largeBuy10s"]
                    elif size == "big":
                        df[field] = df["bigBuy10s"]
                    elif size == "super":
                        df[field] = df["superBuy10s"]

    # Bot trade counts
    if "trade_last5Seconds.tradeCount.bot" not in df.columns:
        df["trade_last5Seconds.tradeCount.bot"] = 0

    if "trade_last10Seconds.tradeCount.bot" not in df.columns:
        df["trade_last10Seconds.tradeCount.bot"] = 0

    return df


def add_creation_time(df, pool_id):
    """Add creation time field if missing"""
    if "creationTime" not in df.columns:
        # Get the earliest timeFromStart as a proxy for creation time
        earliest_time = df["timeFromStart"].min()
        df["creationTime"] = earliest_time
        logger.info(f"Added derived creationTime for pool {pool_id}")

    return df


def preprocess_pool_data(df, pool_id):
    """Apply all preprocessing steps to pool data"""
    if df is None or df.empty:
        logger.warning(f"No data for pool {pool_id}")
        return None

    logger.info(f"Preprocessing data for pool {pool_id}")

    # Add derived fields
    df = add_derived_fields(df)

    # Add fallback trade data
    df = add_fallback_trade_fields(df)

    # Add creation time if missing
    df = add_creation_time(df, pool_id)

    logger.info(f"Preprocessing complete for pool {pool_id}: {len(df)} rows, {len(df.columns)} columns")

    return df


def main():
    """Preprocess pool data for trading strategies"""
    # Initialize Firebase
    logger.info("Initializing Firebase connection...")
    firebase_service = FirebaseService()
    db = firebase_service.db

    if not db:
        logger.error("Failed to connect to Firebase. Exiting.")
        return

    # Create output directory if it doesn't exist
    output_dir = Path(project_root) / "outputs" / "processed_pools"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Get pool IDs to analyze
    logger.info("Fetching pool IDs...")

    # Use filtered pools if available
    filtered_pools_path = Path(project_root) / "outputs" / "filtered_pools.json"
    if filtered_pools_path.exists():
        with filtered_pools_path.open("r") as f:
            filtered_pools = json.load(f)
            pool_ids = [p["pool_id"] for p in filtered_pools]
            logger.info(f"Loaded {len(pool_ids)} filtered pools with sufficient data")
    else:
        # If no filtered pools file, get pools with sufficient data points
        max_pools = 20  # Limit for initial processing
        pool_ids = get_pool_ids(db, limit=max_pools)
        logger.info(f"Using {len(pool_ids)} pools for processing")

    # Process each pool
    processed_pools = []
    skipped_pools = []

    for i, pool_id in enumerate(pool_ids):
        logger.info(f"Processing pool {i+1}/{len(pool_ids)}: {pool_id}")

        # Fetch data for this pool
        pool_data = firebase_service.fetch_market_data(
            min_data_points=20, max_pools=1, limit_per_pool=100, pool_address=pool_id
        ).get(pool_id)

        if pool_data is not None and not pool_data.empty:
            # Process the pool data
            processed_data = preprocess_pool_data(pool_data, pool_id)

            if processed_data is not None:
                # Save processed data to file
                output_file = output_dir / f"{pool_id}.csv"
                processed_data.to_csv(output_file)
                processed_pools.append({"pool_id": pool_id, "rows": len(processed_data), "file_path": str(output_file)})
        else:
            skipped_pools.append(pool_id)
            logger.warning(f"Skipped pool {pool_id}: No data available")

    # Save list of processed pools
    processed_pools_file = Path(project_root) / "outputs" / "processed_pools.json"
    with processed_pools_file.open("w") as f:
        json.dump(processed_pools, f, indent=2)

    # Print summary
    print("\n" + "=" * 80)
    print("POOL DATA PREPROCESSING RESULTS")
    print("=" * 80)

    print(f"\nTotal pools: {len(pool_ids)}")
    print(f"Successfully processed: {len(processed_pools)}")
    print(f"Skipped: {len(skipped_pools)}")

    print(f"\nProcessed data saved to: {output_dir}")
    print(f"Processed pools list saved to: {processed_pools_file}")

    print("\nThe processed data includes:")
    print("1. All 32 reliable fields that exist in the original data")
    print("2. Derived fields (lastPrice, totalVolume)")
    print("3. Fallback values for trade data")
    print("4. Added creationTime if missing")

    print("\nYou can now use this processed data for your trading strategies!")
    print("=" * 80)


if __name__ == "__main__":
    main()
