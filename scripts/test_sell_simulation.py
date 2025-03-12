#!/usr/bin/env python3
"""
Test script for the SellSimulator with mock data.

This script creates mock market data that simulates a typical token price movement,
then applies the SellSimulator to generate and analyze trade results.
"""

import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import logging

# Add src directory to path for importing our modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.simulation.sell_simulator import SellSimulator

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("SellSimulatorTest")


def create_mock_data(num_points=200, volatility=0.05):
    """
    Create mock market data with a realistic price pattern.

    Args:
        num_points: Number of data points to generate
        volatility: Volatility factor for price movements

    Returns:
        DataFrame with mock market data
    """
    # Create timestamps
    timestamps = [(datetime.now() + timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%S") for i in range(num_points)]

    # Generate price pattern:
    # 1. Initial flat period
    # 2. Rapid growth
    # 3. Peak and initial pullback
    # 4. Consolidation
    # 5. Secondary growth
    # 6. Decline

    # Base price and market cap
    initial_mc = 100000

    # Initialize arrays
    market_cap = []
    holders = []

    # Phase 1: Initial flat (20% of points)
    flat_period = int(num_points * 0.2)
    for i in range(flat_period):
        # Slight random movement around base
        market_cap.append(initial_mc * (1 + np.random.normal(0, volatility * 0.2)))
        holders.append(10 + i % 3)  # Minimal holder growth

    # Phase 2: Rapid growth (25% of points)
    growth_period = int(num_points * 0.25)
    growth_start_mc = market_cap[-1]
    for i in range(growth_period):
        # Exponential growth with some noise
        growth_factor = 1.1 + (i / growth_period) * 0.5  # Increasing growth rate
        market_cap.append(growth_start_mc * growth_factor * (1 + np.random.normal(0, volatility)))
        holders.append(10 + flat_period // 3 + i)  # Accelerating holder growth

    # Phase 3: Peak and initial pullback (10% of points)
    peak_period = int(num_points * 0.1)
    peak_start_mc = market_cap[-1]
    for i in range(peak_period):
        # First half continues up, second half starts pullback
        if i < peak_period // 2:
            adjustment = 1.05 * (1 + np.random.normal(0, volatility))
        else:
            adjustment = 0.95 * (1 + np.random.normal(0, volatility))
        market_cap.append(peak_start_mc * adjustment)
        holders.append(10 + flat_period // 3 + growth_period + min(i, peak_period // 2))

    # Phase 4: Consolidation (15% of points)
    consolidation_period = int(num_points * 0.15)
    consolidation_start_mc = market_cap[-1]
    for i in range(consolidation_period):
        # Sideways movement with volatility
        market_cap.append(consolidation_start_mc * (1 + np.random.normal(0, volatility * 0.5)))
        holders.append(10 + flat_period // 3 + growth_period + peak_period // 2 + i % 5)

    # Phase 5: Secondary growth (15% of points)
    second_growth_period = int(num_points * 0.15)
    second_growth_start_mc = market_cap[-1]
    for i in range(second_growth_period):
        # Moderate growth with some noise
        growth_factor = 1.05 + (i / second_growth_period) * 0.2
        market_cap.append(second_growth_start_mc * growth_factor * (1 + np.random.normal(0, volatility)))
        holders.append(10 + flat_period // 3 + growth_period + peak_period // 2 + consolidation_period // 5 + i // 2)

    # Phase 6: Decline (remaining points)
    remaining = num_points - len(market_cap)
    decline_start_mc = market_cap[-1]
    for i in range(remaining):
        # Accelerating decline
        decline_factor = 1.0 - (i / remaining) * 0.4
        market_cap.append(decline_start_mc * decline_factor * (1 + np.random.normal(0, volatility)))
        # Holders stabilize during decline
        holders.append(holders[-1] + max(0, 3 - i // 10))

    # Generate other metrics based on price movement
    mc_change_5s = [0]
    holder_delta_30s = [0]
    buy_volume_5s = []

    # Calculate derived metrics
    for i in range(1, len(market_cap)):
        # Market cap change (percentage)
        mc_change = ((market_cap[i] - market_cap[i - 1]) / market_cap[i - 1]) * 100
        mc_change_5s.append(mc_change)

        # Holder change (absolute)
        holder_delta = holders[i] - holders[max(0, i - 6)]
        holder_delta_30s.append(holder_delta)

        # Buy volume (correlated with price movement)
        if mc_change > 0:
            buy_volume = 100 + mc_change * 50 * (1 + np.random.normal(0, 0.3))
        else:
            buy_volume = max(10, 100 + mc_change * 20 * (1 + np.random.normal(0, 0.3)))
        buy_volume_5s.append(buy_volume)

    # Ensure first point has buy volume
    buy_volume_5s.insert(0, 100)

    # Create DataFrame
    data = {
        "timestamp": timestamps,
        "marketCap": market_cap,
        "holders": holders,
        "marketCapChange5s": mc_change_5s,
        "holderDelta30s": holder_delta_30s,
        "buyVolume5s": buy_volume_5s,
        # Add other required metrics
        "holderDelta5s": holder_delta_30s,  # Simplification
        "holderDelta60s": [x * 2 for x in holder_delta_30s],  # Simplification
        "netVolume5s": [x * 0.8 for x in buy_volume_5s],  # Simplification
        "buySellRatio10s": [max(0.5, 1 + mc / 5) for mc in mc_change_5s],  # Correlated with price
        "pool_address": ["test_pool"] * num_points,
    }

    return pd.DataFrame(data)


def simulate_trades(df, sell_params=None):
    """
    Run the sell simulation with different parameter sets.

    Args:
        df: DataFrame with market data
        sell_params: Dictionary of parameter sets to test

    Returns:
        Dictionary of trade results for each parameter set
    """
    if sell_params is None:
        # Default set of parameters to test
        sell_params = {
            "default": {"base_take_profit": 1.9, "stop_loss": 0.65, "trailing_stop": 0.9},
            "aggressive": {"base_take_profit": 1.5, "stop_loss": 0.8, "trailing_stop": 0.95},
            "conservative": {"base_take_profit": 2.5, "stop_loss": 0.5, "trailing_stop": 0.8},
        }

    # Find a good entry point (around 25% into the data)
    entry_index = int(len(df) * 0.25)
    entry_price = df.iloc[entry_index]["marketCap"]
    entry_time = df.iloc[entry_index]["timestamp"]

    # Prepare post-entry data
    post_entry_data = df.iloc[entry_index:].reset_index(drop=True)

    # Create buy opportunity
    buy_opportunity = {
        "pool_address": "test_pool",
        "entry_price": entry_price,
        "entry_time": entry_time,
        "entry_row": entry_index,
        "entry_metrics": {
            "mc_change_5s": df.iloc[entry_index]["marketCapChange5s"],
            "holder_delta_30s": df.iloc[entry_index]["holderDelta30s"],
            "buy_volume_5s": df.iloc[entry_index]["buyVolume5s"],
        },
        "post_entry_data": post_entry_data,
    }

    # Run simulations with different parameters
    results = {}
    for name, params in sell_params.items():
        logger.info(f"Running simulation with {name} parameters")
        sell_simulator = SellSimulator(
            initial_investment=1.0,
            base_take_profit=params["base_take_profit"],
            stop_loss=params["stop_loss"],
            trailing_stop=params["trailing_stop"],
        )

        trade = sell_simulator.simulate_sell(buy_opportunity)
        if trade:
            results[name] = trade
            logger.info(f"{name} - Profit: {trade['profit_ratio']:.2f}x, Exit: {trade['exit_reason']}")
        else:
            logger.warning(f"{name} - Simulation failed")

    return results


def plot_results(df, trades):
    """
    Plot market data and trade results.

    Args:
        df: DataFrame with market data
        trades: Dictionary of trade results
    """
    plt.figure(figsize=(12, 8))

    # Plot market cap
    plt.subplot(2, 1, 1)
    plt.plot(df["marketCap"], label="Market Cap")

    # Plot entry and exit points
    colors = {"default": "green", "aggressive": "red", "conservative": "blue"}

    for name, trade in trades.items():
        entry_row = trade["entry_row"]
        exit_row = trade["exit_row"] + entry_row  # Adjust for post_entry_data indexing

        # Plot entry point
        plt.scatter(
            entry_row,
            df.iloc[entry_row]["marketCap"],
            marker="^",
            color=colors.get(name, "purple"),
            s=100,
            label=f"{name} Entry",
        )

        # Plot exit point
        plt.scatter(
            exit_row,
            df.iloc[exit_row]["marketCap"],
            marker="v",
            color=colors.get(name, "purple"),
            s=100,
            label=f'{name} Exit ({trade["exit_reason"]})',
        )

    plt.title("Market Cap with Entry and Exit Points")
    plt.ylabel("Market Cap")
    plt.legend()
    plt.grid(True)

    # Plot supporting metrics
    plt.subplot(2, 1, 2)
    plt.plot(df["marketCapChange5s"], label="MC Change 5s (%)")
    plt.plot(df["holderDelta30s"], label="Holder Delta 30s")
    plt.plot(df["buyVolume5s"] / 100, label="Buy Volume / 100")

    plt.title("Supporting Metrics")
    plt.xlabel("Time")
    plt.legend()
    plt.grid(True)

    plt.tight_layout()
    plt.savefig("sell_simulation_results.png")
    logger.info("Results plot saved to sell_simulation_results.png")
    plt.show()


def print_trade_comparison(trades):
    """
    Print a comparison of trade results.

    Args:
        trades: Dictionary of trade results
    """
    if not trades:
        logger.warning("No trade results to compare")
        return

    print("\n" + "=" * 80)
    print("TRADE RESULT COMPARISON")
    print("=" * 80)

    # Headers
    headers = ["Strategy", "Profit", "Exit Reason", "Max Profit", "Hold Time (min)", "Quality"]
    print(f"{headers[0]:<15} {headers[1]:<10} {headers[2]:<20} {headers[3]:<10} {headers[4]:<15} {headers[5]:<10}")
    print("-" * 80)

    # Data
    for name, trade in trades.items():
        profit = f"{(trade['profit_ratio'] - 1) * 100:.1f}%"
        max_profit = f"{(trade['max_profit'] - 1) * 100:.1f}%"
        hold_time = f"{trade['trade_duration'] / 60:.1f}"

        # Determine quality based on how close to max profit
        efficiency = trade["profit_ratio"] / trade["max_profit"] if trade["max_profit"] > 1 else 0
        if efficiency > 0.9:
            quality = "Excellent"
        elif efficiency > 0.7:
            quality = "Good"
        elif efficiency > 0.5:
            quality = "Fair"
        else:
            quality = "Poor"

        print(f"{name:<15} {profit:<10} {trade['exit_reason']:<20} {max_profit:<10} {hold_time:<15} {quality:<10}")

    print("=" * 80)


def main():
    """Run the sell simulator test."""
    logger.info("Creating mock market data")
    df = create_mock_data(num_points=200)

    logger.info("Running sell simulations")
    trades = simulate_trades(df)

    logger.info("Plotting results")
    plot_results(df, trades)

    logger.info("Comparing trade results")
    print_trade_comparison(trades)


if __name__ == "__main__":
    main()
