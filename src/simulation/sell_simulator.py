"""
Sell Simulator for Solana trading strategy.

This module provides a simulator for finding optimal sell points (exits)
in Solana trading pools based on various market metrics and momentum indicators.
"""

import pandas as pd
import logging
from typing import Dict, Optional, List

# Configure logging
logger = logging.getLogger("SellSimulator")


def get_default_stoploss_params() -> Dict[str, float]:
    """
    Get default stoploss parameters for sell strategy.

    Returns:
        Dictionary of stoploss parameter names and values
    """
    return {
        "holder_growth_30s_strong": 10.0,
        "holder_growth_60s_strong": 50.0,
        "holder_growth_30s_moderate": 20.0,
        "holder_growth_60s_moderate": 30.0,
        "buy_volume_moderate": 15.0,
        "mc_drop_limit": -40.0,
    }


def get_default_momentum_params() -> Dict[str, float]:
    """
    Get default momentum parameters for sell strategy.

    Returns:
        Dictionary of momentum parameter names and values
    """
    return {
        "mc_change_threshold": 6.0,
        "holder_change_threshold": 24.5,
        "buy_volume_threshold": 13.0,
        "net_volume_threshold": 3.0,
        "required_strong": 1.0,
        "lp_holder_growth_threshold": 0.0,
    }


class SellSimulator:
    """
    Simulator for finding optimal sell (exit) points in Solana token pools.
    """

    def __init__(
        self,
        initial_investment: float = 1.0,
        base_take_profit: float = 1.9,
        stop_loss: float = 0.65,
        trailing_stop: float = 0.9,
        stoploss_params: Optional[Dict[str, float]] = None,
        momentum_params: Optional[Dict[str, float]] = None,
    ):
        """
        Initialize the sell simulator.

        Args:
            initial_investment: Initial investment amount in SOL
            base_take_profit: Base take profit multiplier (e.g., 1.9 = +90%)
            stop_loss: Stop loss multiplier (e.g., 0.65 = -35%)
            trailing_stop: Trailing stop percentage (e.g., 0.9 = 10% below peak)
            stoploss_params: Custom stoploss parameters
            momentum_params: Custom momentum parameters
        """
        self.initial_investment = initial_investment
        self.base_take_profit = base_take_profit
        self.stop_loss = stop_loss
        self.trailing_stop = trailing_stop

        # Use provided parameters or defaults
        self.stoploss_params = stoploss_params if stoploss_params is not None else get_default_stoploss_params()
        self.momentum_params = momentum_params if momentum_params is not None else get_default_momentum_params()

        logger.info(
            f"Initialized SellSimulator with take_profit={base_take_profit}, "
            f"stop_loss={stop_loss}, trailing_stop={trailing_stop}"
        )
        logger.debug(f"Stoploss parameters: {self.stoploss_params}")
        logger.debug(f"Momentum parameters: {self.momentum_params}")

    def check_momentum(self, metrics: Dict[str, float], momentum_params: Optional[Dict[str, float]] = None) -> bool:
        """
        Check if momentum is still strong based on current metrics.

        Args:
            metrics: Current market metrics
            momentum_params: Optional custom momentum parameters

        Returns:
            True if momentum is still strong, False otherwise
        """
        try:
            # Use provided parameters or instance parameters
            if momentum_params is None:
                momentum_params = self.momentum_params

            momentum_score = 0
            required_strong = momentum_params["required_strong"]

            # Market cap growth is still good
            if metrics.get("mc_change_5s", 0) > momentum_params["mc_change_threshold"]:
                momentum_score += 1
                logger.debug(
                    f"MC change is strong: {metrics.get('mc_change_5s', 0):.2f} > {momentum_params['mc_change_threshold']}"
                )

            # Holders are still growing
            if metrics.get("holder_change_30s", 0) > momentum_params["holder_change_threshold"]:
                momentum_score += 1
                logger.debug(
                    f"Holder growth is strong: {metrics.get('holder_change_30s', 0)} > {momentum_params['holder_change_threshold']}"
                )

            # Volume remains high
            if metrics.get("buy_volume_5s", 0) > momentum_params["buy_volume_threshold"]:
                momentum_score += 1
                logger.debug(
                    f"Buy volume is strong: {metrics.get('buy_volume_5s', 0):.2f} > {momentum_params['buy_volume_threshold']}"
                )

            # Net volume is clearly positive
            if metrics.get("net_volume_5s", 0) > momentum_params["net_volume_threshold"]:
                momentum_score += 1
                logger.debug(
                    f"Net volume is strong: {metrics.get('net_volume_5s', 0):.2f} > {momentum_params['net_volume_threshold']}"
                )

            # Check if momentum is strong enough
            is_strong = momentum_score >= required_strong

            if is_strong:
                logger.debug(f"Momentum still strong with score {momentum_score}/{4}")
            else:
                logger.debug(f"Momentum weakening with score {momentum_score}/{4}")

            return is_strong

        except Exception as e:
            logger.error(f"Error checking momentum: {str(e)}")
            return False  # Default to no strong momentum on error

    def simulate_sell(self, buy_opportunity: Dict) -> Optional[Dict]:
        """
        Simulate sell strategy on a given buy opportunity.

        Args:
            buy_opportunity: Dictionary containing buy opportunity details

        Returns:
            Dictionary with trade result details if successful, None otherwise
        """
        try:
            # Extract buy opportunity details
            pool_address = buy_opportunity["pool_address"]
            entry_price = buy_opportunity["entry_price"]
            entry_time = buy_opportunity["entry_time"]
            entry_row = buy_opportunity["entry_row"]
            entry_metrics = buy_opportunity["entry_metrics"]

            # Get post-entry data for simulation
            pool_data = buy_opportunity["post_entry_data"]

            if len(pool_data) < 10:
                logger.warning(f"Insufficient data for sell simulation on pool {pool_address}")
                return None

            logger.info(f"Simulating sell for pool: {pool_address}")
            logger.info(f"Entry price: {entry_price:.2f}, Entry time: {entry_time}")

            max_profit = 0
            max_price = entry_price
            exit_reason = ""
            current_metrics = {}

            # Track position through the data
            for index in range(len(pool_data)):
                try:
                    # Get current price and time
                    current_price = pool_data.iloc[index]["marketCap"]
                    current_time = pd.to_datetime(pool_data.iloc[index]["timestamp"])
                    profit_ratio = current_price / entry_price

                    # Update maximum price and profit
                    max_price = max(max_price, current_price)
                    max_profit = max(max_profit, profit_ratio)

                    # Collect current metrics
                    try:
                        current_metrics = {
                            "mc_change_5s": pool_data.iloc[index].get("marketCapChange5s", 0),
                            "holder_change_5s": pool_data.iloc[index].get("holderDelta5s", 0),
                            "holder_change_30s": pool_data.iloc[index].get("holderDelta30s", 0),
                            "holder_change_60s": pool_data.iloc[index].get("holderDelta60s", 0),
                            "buy_volume_5s": pool_data.iloc[index].get("buyVolume5s", 0),
                            "net_volume_5s": pool_data.iloc[index].get("netVolume5s", 0),
                            "price_change": pool_data.iloc[index].get("priceChangePercent", 0),
                        }
                    except Exception as e:
                        logger.warning(f"Error collecting metrics at index {index}: {str(e)}")
                        # Set defaults for missing metrics
                        for key in [
                            "mc_change_5s",
                            "holder_change_5s",
                            "holder_change_30s",
                            "holder_change_60s",
                            "buy_volume_5s",
                            "net_volume_5s",
                            "price_change",
                        ]:
                            if key not in current_metrics:
                                current_metrics[key] = 0

                    # Check sell conditions
                    # 1. Take profit condition
                    if profit_ratio >= self.base_take_profit:
                        try:
                            # Check if momentum is still strong
                            momentum_strong = self.check_momentum(current_metrics, self.momentum_params)

                            # Check if price has dropped from peak
                            price_dropped = current_price < max_price * self.trailing_stop

                            # Sell only if momentum is weak AND price has dropped significantly from peak
                            if not momentum_strong and price_dropped:
                                exit_reason = "Momentum Lost + Price Drop"
                                logger.info(f"Take profit triggered: {exit_reason}")
                                break
                        except Exception as e:
                            logger.error(f"Error in take profit logic: {str(e)}")
                            continue

                    # 2. Low Performance condition (new exit type)
                    # Sell if holder growth has slowed significantly but before price drops significantly
                    elif profit_ratio < 1.2 and profit_ratio > self.stop_loss:
                        try:
                            # Get Low Performance threshold from momentum parameters
                            lp_holder_growth_threshold = self.momentum_params.get("lp_holder_growth_threshold", 2.0)

                            # Check holder growth slowdown
                            holder_growth_30s = current_metrics.get("holder_change_30s", 0)
                            holder_growth_60s = current_metrics.get("holder_change_60s", 0)

                            # Sell if holder growth has slowed below threshold
                            if (
                                holder_growth_30s < lp_holder_growth_threshold
                                and holder_growth_60s < lp_holder_growth_threshold * 2
                            ):
                                exit_reason = "Low Performance"
                                logger.info("Low performance exit triggered: holder growth slowed")
                                break
                        except Exception as e:
                            logger.error(f"Error in low performance logic: {str(e)}")
                            continue

                    # 3. Stop Loss condition
                    elif profit_ratio <= self.stop_loss:
                        try:
                            # Check if we should ignore stop loss due to strong holder growth
                            holder_30s = current_metrics.get("holder_change_30s", 0)
                            holder_60s = current_metrics.get("holder_change_60s", 0)
                            buy_volume = current_metrics.get("buy_volume_5s", 0)

                            # Strong holder growth - don't sell
                            if (
                                holder_30s > self.stoploss_params["holder_growth_30s_strong"]
                                and holder_60s > self.stoploss_params["holder_growth_60s_strong"]
                                and buy_volume > self.stoploss_params["buy_volume_moderate"]
                            ):
                                logger.debug("Ignoring stop loss due to strong holder growth")
                                continue

                            # Moderate holder growth - don't sell
                            if (
                                holder_30s > self.stoploss_params["holder_growth_30s_moderate"]
                                and holder_60s > self.stoploss_params["holder_growth_60s_moderate"]
                                and buy_volume > self.stoploss_params["buy_volume_moderate"]
                            ):
                                logger.debug("Ignoring stop loss due to moderate holder growth")
                                continue

                            exit_reason = "Stop Loss"
                            logger.info(f"Stop loss triggered at price ratio {profit_ratio:.2f}")
                            break
                        except Exception as e:
                            logger.error(f"Error in stop loss logic: {str(e)}")
                            continue

                    # 4. Force sell at end of data
                    elif index == len(pool_data) - 1:
                        exit_reason = "Force Sell"
                        logger.info("Forced sell at end of data")
                        break

                except Exception as e:
                    logger.error(f"Error processing position at index {index}: {str(e)}")
                    continue

            # If no exit reason was set, force sell at the end
            if not exit_reason:
                exit_reason = "Force Sell"
                index = len(pool_data) - 1
                current_price = pool_data.iloc[index]["marketCap"]
                current_time = pd.to_datetime(pool_data.iloc[index]["timestamp"])
                profit_ratio = current_price / entry_price
                logger.warning("No exit conditions met, forcing sell at end of data")

            # Calculate profit
            profit_sol = (profit_ratio - 1) * self.initial_investment

            # Analyze what happened after exit (for validation)
            post_exit_max_ratio = 1.0
            time_to_max = 0

            try:
                if index + 1 < len(pool_data):
                    post_exit_window = min(index + 300, len(pool_data))  # Look ahead up to 300 data points
                    post_exit_prices = pool_data.iloc[index + 1 : post_exit_window]["marketCap"]

                    if not post_exit_prices.empty:
                        max_post_price = post_exit_prices.max()
                        post_exit_max_ratio = max_post_price / current_price
                        time_to_max_idx = post_exit_prices.idxmax()
                        time_to_max = time_to_max_idx - index if isinstance(time_to_max_idx, int) else 0
            except Exception as e:
                logger.error(f"Error in post-exit analysis: {str(e)}")

            # Create trade result object
            trade_result = {
                "pool_address": pool_address,
                "entry_time": entry_time,
                "entry_price": entry_price,
                "exit_time": str(pool_data.iloc[index]["timestamp"]),
                "exit_price": current_price,
                "exit_reason": exit_reason,
                "profit_ratio": profit_ratio,
                "max_profit": max_profit,
                "trade_duration": (current_time - pd.to_datetime(entry_time)).total_seconds(),
                "investment_sol": self.initial_investment,
                "profit_sol": profit_sol,
                "entry_metrics": entry_metrics,
                "exit_metrics": current_metrics,
                "post_exit_max_ratio": post_exit_max_ratio,
                "post_exit_max_time": time_to_max,
                "entry_row": entry_row,
                "exit_row": index,
                # Quality metrics
                "max_x_after_stoploss": post_exit_max_ratio if exit_reason == "Stop Loss" else None,
                "max_x_after_tp": post_exit_max_ratio if exit_reason == "Momentum Lost + Price Drop" else None,
                "max_x_after_lp": post_exit_max_ratio if exit_reason == "Low Performance" else None,
                "stoploss_quality": (
                    "Good"
                    if exit_reason == "Stop Loss" and post_exit_max_ratio < 2.0
                    else "Bad" if exit_reason == "Stop Loss" else None
                ),
                "tp_quality": (
                    "Good"
                    if exit_reason == "Momentum Lost + Price Drop" and post_exit_max_ratio < 1.5
                    else "Bad" if exit_reason == "Momentum Lost + Price Drop" else None
                ),
                "lp_quality": (
                    "Good"
                    if exit_reason == "Low Performance" and post_exit_max_ratio < 1.5
                    else "Bad" if exit_reason == "Low Performance" else None
                ),
            }

            # Log the trade summary
            self._log_trade_summary(trade_result)

            return trade_result

        except Exception as e:
            logger.error(f"Critical error in simulate_sell: {str(e)}")
            return None

    def _log_trade_summary(self, trade_result: Dict):
        """
        Log a summary of the trade result.

        Args:
            trade_result: Dictionary containing trade result details
        """
        profit_sol = trade_result["profit_sol"]
        total_sol = trade_result["investment_sol"] + profit_sol
        profit_percent = (trade_result["profit_ratio"] - 1) * 100

        logger.info("=== TRADE SUMMARY ===")
        logger.info(f"Pool: {trade_result['pool_address']}")
        logger.info(f"Investment: {trade_result['investment_sol']:.2f} SOL")
        logger.info(f"Profit: {profit_sol:.3f} SOL ({profit_percent:.1f}%)")
        logger.info(f"Final amount: {total_sol:.3f} SOL")
        logger.info(f"Exit reason: {trade_result['exit_reason']}")
        logger.info(f"Trade duration: {trade_result['trade_duration'] / 60:.1f} minutes")

        # Log exit metrics
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Exit metrics:")
            for key, value in trade_result["exit_metrics"].items():
                logger.debug(f"  {key}: {value}")

            logger.debug("Post-exit analysis:")
            logger.debug(f"  Max price after exit: {trade_result['post_exit_max_ratio']:.2f}x current price")
            logger.debug(f"  Time to max price: {trade_result['post_exit_max_time']} data points")


def calculate_trade_metrics(trades: List[Dict]) -> Dict[str, float]:
    """
    Calculate aggregate metrics for a list of trades.

    Args:
        trades: List of trade result dictionaries

    Returns:
        Dictionary of metrics and their values
    """
    if not trades:
        logger.warning("No trades provided for metrics calculation")
        return {}

    # Calculate basic statistics
    total_trades = len(trades)
    profitable_trades = [t for t in trades if t["profit_ratio"] > 1.0]
    win_rate = len(profitable_trades) / total_trades if total_trades > 0 else 0

    # Calculate total profit
    total_profit_ratio = sum(t["profit_ratio"] - 1.0 for t in trades)
    avg_profit_per_trade = total_profit_ratio / total_trades if total_trades > 0 else 0

    # Calculate average hold time
    hold_times = [
        (pd.to_datetime(t["exit_time"]) - pd.to_datetime(t["entry_time"])).total_seconds() / 60 for t in trades
    ]
    avg_hold_time = sum(hold_times) / len(hold_times) if hold_times else 0

    # Count exit types
    stoploss_trades = len([t for t in trades if t["exit_reason"] == "Stop Loss"])
    tp_trades = len([t for t in trades if t["exit_reason"] == "Momentum Lost + Price Drop"])
    lp_trades = len([t for t in trades if t["exit_reason"] == "Low Performance"])
    force_trades = len([t for t in trades if t["exit_reason"] == "Force Sell"])

    # Create metrics dictionary
    metrics = {
        "total_trades": total_trades,
        "profitable_trades": len(profitable_trades),
        "losing_trades": total_trades - len(profitable_trades),
        "win_rate": win_rate,
        "total_profit_ratio": total_profit_ratio,
        "avg_profit_per_trade": avg_profit_per_trade,
        "avg_hold_time": avg_hold_time,
        "stoploss_trades": stoploss_trades,
        "tp_trades": tp_trades,
        "lp_trades": lp_trades,
        "force_trades": force_trades,
    }

    return metrics
