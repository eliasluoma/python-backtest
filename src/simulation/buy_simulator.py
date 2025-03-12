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
        Check if buy conditions are met for a given set of metrics.

        Args:
            metrics: Current metrics to evaluate
            initial_metrics: Metrics at the start of the pool
            pool_data: Complete pool data

        Returns:
            True if conditions are met, False otherwise
        """
        try:
            # Early market cap filter
            if len(pool_data) > 10:
                mc_at_10s = pool_data.iloc[10]["marketCap"]
                if mc_at_10s > self.early_mc_limit:
                    logger.debug(f"Pool rejected: MC at 10s ({mc_at_10s:.2f}) > limit ({self.early_mc_limit})")
                    return False

            # Calculate growth from start if not already in metrics
            if "mc_growth_from_start" not in metrics:
                initial_mc = initial_metrics.get("marketCap", 0)
                current_mc = metrics.get("mc_at_delay", 0)
                if initial_mc > 0:
                    mc_growth = ((current_mc / initial_mc) - 1) * 100
                    metrics["mc_growth_from_start"] = mc_growth
                else:
                    metrics["mc_growth_from_start"] = 0

            if "holder_growth_from_start" not in metrics:
                initial_holders = initial_metrics.get("holdersCount", 0)
                current_holders = metrics.get("holders_at_delay", 0)
                holder_growth = current_holders - initial_holders
                metrics["holder_growth_from_start"] = holder_growth

            # Count how many conditions are met
            conditions_met = 0
            conditions_total = 0

            for param, threshold in self.buy_params.items():
                conditions_total += 1
                if param in metrics:
                    if metrics[param] > threshold:
                        conditions_met += 1
                        logger.debug(f"Condition {param} met: {metrics[param]:.2f} > {threshold}")
                    else:
                        logger.debug(f"Condition {param} not met: {metrics[param]:.2f} <= {threshold}")
                else:
                    logger.warning(f"Metric {param} not found in data")

            # Require a minimum number of conditions to be met (configurable)
            required_conditions = max(1, int(conditions_total * 0.7))  # At least 70% of conditions

            if conditions_met >= required_conditions:
                logger.info(f"Buy conditions met: {conditions_met}/{conditions_total} parameters exceed thresholds")
                return True
            else:
                logger.debug(
                    f"Buy conditions not met: only {conditions_met}/{conditions_total} parameters exceed thresholds"
                )
                return False

        except Exception as e:
            logger.error(f"Error checking buy conditions: {str(e)}")
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

            # Get pool address
            pool_address = pool_data["poolAddress"].iloc[0]
            logger.info(f"Analyzing pool {pool_address} with {len(pool_data)} data points")

            # Check minimum data requirements
            if len(pool_data) < self.max_delay + 10:
                logger.warning(
                    f"Insufficient data for pool {pool_address}: {len(pool_data)} points < {self.max_delay + 10} required"
                )
                return None

            # Store initial metrics
            initial_metrics = {
                "marketCap": pool_data.iloc[0]["marketCap"],
                "holdersCount": pool_data.iloc[0]["holdersCount"] if "holdersCount" in pool_data.columns else 0,
            }

            logger.debug(
                f"Initial metrics: MC={initial_metrics['marketCap']:.2f}, Holders={initial_metrics['holdersCount']}"
            )

            # Check each potential buy point within the delay window
            for delay in range(self.min_delay, min(self.max_delay + 1, len(pool_data))):
                try:
                    # Prepare current metrics
                    current_metrics = {
                        "mc_at_delay": pool_data.iloc[delay]["marketCap"],
                        "holders_at_delay": (
                            pool_data.iloc[delay]["holdersCount"] if "holdersCount" in pool_data.columns else 0
                        ),
                    }

                    # Add all available metrics
                    for col in pool_data.columns:
                        if col in [
                            "marketCapChange5s",
                            "marketCapChange10s",
                            "marketCapChange30s",
                            "holderDelta5s",
                            "holderDelta30s",
                            "holderDelta60s",
                            "buyVolume5s",
                            "netVolume5s",
                            "priceChangePercent",
                            "largeBuy5s",
                        ]:
                            current_metrics[col.lower()] = pool_data.iloc[delay][col]

                    # Map column names to our parameter names
                    metric_mapping = {
                        "marketcapchange5s": "mc_change_5s",
                        "marketcapchange30s": "mc_change_30s",
                        "holderdelta30s": "holder_delta_30s",
                        "pricepercent": "price_change",
                    }

                    # Apply mapping
                    for old_name, new_name in metric_mapping.items():
                        if old_name in current_metrics:
                            current_metrics[new_name] = current_metrics.pop(old_name)

                    # Check if buy conditions are met
                    if self.check_buy_conditions(current_metrics, initial_metrics, pool_data[: delay + 1]):
                        # Found buy opportunity
                        entry_time = pool_data.iloc[delay]["timestamp"]
                        entry_price = pool_data.iloc[delay]["marketCap"]

                        logger.info(f"Buy opportunity found in pool {pool_address} at delay {delay}")
                        logger.info(f"Entry price: {entry_price:.2f}, Entry time: {entry_time}")

                        # Get post-entry data for later sell simulation
                        post_entry_data = pool_data.iloc[delay:].copy()

                        # Create buy opportunity object
                        buy_opportunity = {
                            "pool_address": pool_address,
                            "entry_row": delay,
                            "entry_time": str(entry_time),
                            "entry_price": entry_price,
                            "entry_metrics": current_metrics,
                            "initial_metrics": initial_metrics,
                            "post_entry_data": post_entry_data,
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }

                        return buy_opportunity

                except Exception as e:
                    logger.error(f"Error processing delay {delay}: {str(e)}")
                    continue

            logger.debug(f"No buy opportunity found for pool {pool_address}")
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
