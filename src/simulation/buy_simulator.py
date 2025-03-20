"""
Buy Simulator for Solana trading strategy.

This module provides a simulator for finding optimal buy entry points
in Solana trading pools based on various market metrics.
"""

import pandas as pd
import logging
from typing import Dict, Optional
from datetime import datetime

# Configure logging
logger = logging.getLogger("BuySimulator")


def get_default_parameters() -> Dict[str, float]:
    """
    Get default parameters for buy strategy.

    Returns:
        Dictionary of parameter names and threshold values
    """
    return {
        "price_change": 1.0,
        "mc_change_5s": 5.0,
        "mc_change_30s": 10.0,
        "holder_delta_30s": 20,
        "buy_volume_5s": 5.0,
        "net_volume_5s": 0.0,
        "buy_sell_ratio_10s": 1.5,
        "mc_growth_from_start": 10.0,
        "holder_growth_from_start": 20,
        "large_buy_5s": 1,
    }


class BuySimulator:
    """
    Simulator for finding optimal buy entry points in Solana token pools.
    """

    def __init__(
        self,
        early_mc_limit: float = 400000,  # Market cap limit for early filtering
        min_delay: int = 60,  # Minimum delay in seconds
        max_delay: int = 200,  # Maximum delay in seconds
        buy_params: Optional[Dict[str, float]] = None,
    ):
        """
        Initialize the buy simulator.

        Args:
            early_mc_limit: Market cap limit for early filtering (e.g., 400k)
            min_delay: Minimum delay in seconds before considering a buy
            max_delay: Maximum delay in seconds to look for buy opportunities
            buy_params: Custom buy parameters (thresholds)
        """
        self.buy_params = buy_params if buy_params is not None else get_default_parameters()
        self.early_mc_limit = early_mc_limit
        self.min_delay = min_delay
        self.max_delay = max_delay

        logger.info(
            f"Initialized BuySimulator with early_mc_limit={early_mc_limit}, "
            f"min_delay={min_delay}, max_delay={max_delay}"
        )
        logger.debug(f"Buy parameters: {self.buy_params}")

    def check_buy_conditions(
        self,
        metrics: Dict[str, float],
        initial_metrics: Dict[str, float],
        pool_data: pd.DataFrame,
    ) -> bool:
        """
        Check if current metrics meet buying conditions.

        Args:
            metrics: Dictionary of current market metrics
            initial_metrics: Dictionary of initial/baseline metrics
            pool_data: DataFrame with pool data up to current point

        Returns:
            Boolean indicating whether buying conditions are met
        """
        try:
            logger.debug(f"Checking buy conditions with: {metrics}")
            logger.debug(f"Initial metrics: {initial_metrics}")

            # Track metrics that passed checks
            passed_metrics = []

            # Check each metric against its threshold
            for metric_name, threshold in self.buy_params.items():
                # Skip price_change check if not available (special case)
                if metric_name == "price_change" and "price_change" not in metrics:
                    logger.debug(f"Skipping {metric_name} check - not in metrics")
                    continue

                # Choose where to look for the metric
                actual_value = None

                # Try to get from current metrics first
                if metric_name in metrics:
                    actual_value = metrics[metric_name]
                # Try alternate names (for backward compatibility)
                elif metric_name == "mc_change_5s" and "marketCapChange5s" in metrics:
                    actual_value = metrics["marketCapChange5s"]
                elif metric_name == "holder_delta_30s" and "holderDelta30s" in metrics:
                    actual_value = metrics["holderDelta30s"]
                elif metric_name == "buy_volume_5s" and "buyVolume5s" in metrics:
                    actual_value = metrics["buyVolume5s"]
                elif metric_name == "net_volume_5s" and "netVolume5s" in metrics:
                    actual_value = metrics["netVolume5s"]
                elif metric_name == "buy_sell_ratio_10s" and "buySellRatio10s" in metrics:
                    actual_value = metrics["buySellRatio10s"]
                elif metric_name == "large_buy_5s" and "largeBuys5s" in metrics:
                    actual_value = metrics["largeBuys5s"]
                # Try to get from initial metrics for growth checks
                elif metric_name in initial_metrics:
                    actual_value = initial_metrics[metric_name]

                # If we still don't have a value, try calculated metrics
                if actual_value is None:
                    if metric_name == "mc_growth_from_start" and "marketCap" in pool_data.columns:
                        # Calculate growth from start
                        start_mc = pool_data["marketCap"].iloc[0]
                        current_mc = pool_data["marketCap"].iloc[-1]
                        if start_mc > 0:
                            actual_value = ((current_mc / start_mc) - 1) * 100
                    elif metric_name == "holder_growth_from_start" and "holders" in pool_data.columns:
                        # Calculate holder growth
                        start_holders = pool_data["holders"].iloc[0]
                        current_holders = pool_data["holders"].iloc[-1]
                        actual_value = current_holders - start_holders

                # If we still don't have a value, log a warning and continue
                if actual_value is None:
                    if not hasattr(self, f"_warned_{metric_name}"):
                        logger.warning(f"Metric {metric_name} not found in data")
                        setattr(self, f"_warned_{metric_name}", True)
                    continue

                # Check if the metric meets the threshold
                if actual_value < threshold:
                    logger.debug(f"Failed check: {metric_name} = {actual_value} < {threshold}")
                    return False

                passed_metrics.append(f"{metric_name}={actual_value:.2f}")

            # All checks passed (or skipped)
            if passed_metrics:
                logger.debug(f"All checks passed: {', '.join(passed_metrics)}")
                return True
            else:
                logger.warning("No metrics were checked - all were missing")
                return False

        except Exception as e:
            logger.error(f"Error in check_buy_conditions: {str(e)}")
            return False

    def find_buy_opportunity(self, pool_data: pd.DataFrame) -> Optional[Dict]:
        """
        Find a buy opportunity in the given pool data.

        Args:
            pool_data: DataFrame containing pool data

        Returns:
            Dictionary with buy opportunity details if found, None otherwise
        """
        try:
            if pool_data.empty:
                logger.warning("Empty pool data provided")
                return None

            # Make sure data is sorted by timestamp
            pool_data = pool_data.sort_values("timestamp")

            # Get pool address - handle both column name formats
            if "poolAddress" in pool_data.columns:
                pool_address = pool_data["poolAddress"].iloc[0]
                address_column = "poolAddress"
            elif "pool_address" in pool_data.columns:
                pool_address = pool_data["pool_address"].iloc[0]
                address_column = "pool_address"
            else:
                logger.error("No pool address column found in data")
                return None

            logger.info(f"Analyzing pool {pool_address} with {len(pool_data)} data points")

            # Log columns available for debugging

            # Check minimum data requirements
            if len(pool_data) < self.max_delay + 10:
                logger.warning(f"Insufficient data points: {len(pool_data)} (need at least {self.max_delay + 10})")
                return None

            # Apply early market cap filter if enabled
            if self.early_mc_limit > 0:
                # Find the index for data around 10 seconds after launch
                # Most pools have data every 2-3 seconds, so we'll use index 4-5 to be approximately 10 seconds
                mc_check_index = min(5, len(pool_data) - 1)  # Use index 5 or last row if data is shorter

                # Get market cap at this time point
                early_mc = pool_data["marketCap"].iloc[mc_check_index]

                if early_mc > self.early_mc_limit:
                    logger.warning(
                        f"Pool {pool_address} exceeds early market cap limit: {early_mc:.0f} > {self.early_mc_limit:.0f}"
                    )
                    return None
                logger.info(f"Pool passes early MC filter: {early_mc:.0f} <= {self.early_mc_limit:.0f}")

            # Wait for minimum delay before considering buying
            if self.min_delay > 0:
                window_start = self.min_delay
            else:
                window_start = 0

            # Only consider buy up to max_delay
            window_end = min(self.max_delay, len(pool_data) - 10)  # Leave at least 10 rows for post-analysis

            if window_start >= window_end:
                logger.warning(f"Buy window is empty: {window_start} to {window_end}")
                return None

            logger.info(f"Scanning for buy opportunities between rows {window_start} and {window_end}")

            # Scan through the window for buy opportunities
            for i in range(window_start, window_end):
                current_row = pool_data.iloc[i]

                # Get current price metrics
                current_metrics = {
                    "mc_change_5s": current_row.get("marketCapChange5s", 0),
                    "mc_change_30s": current_row.get("marketCapChange30s", 0),
                    "holder_delta_30s": current_row.get("holderDelta30s", 0),
                    "buy_volume_5s": current_row.get("buyVolume5s", 0),
                    "net_volume_5s": current_row.get("netVolume5s", 0),
                    "buy_sell_ratio_10s": current_row.get("buySellRatio10s", 1.0),
                    "large_buy_5s": current_row.get("largeBuys5s", 0),
                }

                # Get metrics from the beginning of data for growth checks
                initial_row = pool_data.iloc[0]
                initial_metrics = {
                    "mc_growth_from_start": (
                        (current_row["marketCap"] / initial_row["marketCap"] - 1) * 100
                        if "marketCap" in initial_row and initial_row["marketCap"] > 0
                        else 0
                    ),
                    "holder_growth_from_start": (
                        current_row["holders"] - initial_row["holders"] if "holders" in initial_row else 0
                    ),
                }

                # Check if this point meets buy criteria
                if self.check_buy_conditions(current_metrics, initial_metrics, pool_data.iloc[: i + 1]):
                    # Calculate the entry price (market cap)
                    entry_price = current_row["marketCap"]
                    entry_time = (
                        current_row["timestamp"].isoformat()
                        if isinstance(current_row["timestamp"], datetime)
                        else str(current_row["timestamp"])
                    )

                    # Create the buy opportunity record
                    buy_opportunity = {
                        address_column: pool_address,  # Use the correct column name
                        "entry_price": entry_price,
                        "entry_time": entry_time,
                        "entry_row": i,
                        "entry_metrics": {**current_metrics, **initial_metrics},
                        "post_entry_data": pool_data.iloc[i:].reset_index(drop=True),
                    }

                    logger.info(f"Buy opportunity found at row {i} with price {entry_price:.2f}")
                    return buy_opportunity

            logger.info("No buy opportunities found in this pool")
            return None

        except Exception as e:
            logger.error(f"Error in find_buy_opportunity: {str(e)}")
            return None


def calculate_returns(buy_opportunity: Dict) -> Dict:
    """
    Calculate theoretical returns for a buy opportunity.

    Args:
        buy_opportunity: Buy opportunity object

    Returns:
        Updated buy opportunity with return metrics
    """
    if not buy_opportunity or "post_entry_data" not in buy_opportunity:
        return buy_opportunity

    try:
        entry_price = buy_opportunity["entry_price"]
        post_entry_data = buy_opportunity["post_entry_data"]

        if post_entry_data.empty:
            return buy_opportunity

        # Calculate maximum potential return
        max_price = post_entry_data["marketCap"].max()
        max_return = max_price / entry_price

        # Calculate more realistic returns (e.g., exit at 80% of max)
        realistic_return = max_return * 0.8

        # Find time to reach maximum price
        max_idx = post_entry_data["marketCap"].idxmax()
        time_to_max = post_entry_data.loc[max_idx, "timestamp"] - pd.to_datetime(buy_opportunity["entry_time"])

        # Add metrics to buy opportunity
        buy_opportunity["max_price"] = max_price
        buy_opportunity["max_return"] = max_return
        buy_opportunity["realistic_return"] = realistic_return
        buy_opportunity["time_to_max"] = time_to_max.total_seconds() / 60  # in minutes

        logger.info(
            f"Calculated returns for pool {buy_opportunity['pool_address']}: "
            f"Max return: {max_return:.2f}x, Time to max: {buy_opportunity['time_to_max']:.1f} min"
        )

        return buy_opportunity

    except Exception as e:
        logger.error(f"Error calculating returns: {str(e)}")
        return buy_opportunity
