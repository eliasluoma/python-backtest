#!/usr/bin/env python
"""
Firebase Data Summary Report

This script generates a focused summary report of the Firebase data,
highlighting key metrics relevant for trading simulation and analysis.
"""

import os
import sys
import logging
import matplotlib.pyplot as plt
import numpy as np

# Set up paths
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Import Firebase utilities
from src.data.firebase_service import FirebaseService
from src.utils.firebase_utils import get_pool_ids


def main():
    """Generate a focused summary report of the Firebase data"""
    # Initialize Firebase
    logger.info("Initializing Firebase connection...")
    firebase_service = FirebaseService()
    db = firebase_service.db

    if not db:
        logger.error("Failed to connect to Firebase. Exiting.")
        return

    # Create output directory if it doesn't exist
    output_dir = os.path.join(project_root, "outputs")
    os.makedirs(output_dir, exist_ok=True)

    # Get all pool IDs
    logger.info("Fetching pool information...")
    all_pool_ids = get_pool_ids(db, limit=None)
    total_pools = len(all_pool_ids)
    logger.info(f"Found {total_pools} pools in the database")

    # Sample pools for analysis
    sample_size = min(50, total_pools)  # Analyze up to 50 pools
    sample_pools = all_pool_ids[:sample_size]

    # Collect data for summary report
    summary_data = collect_summary_data(firebase_service, sample_pools)

    # Generate report
    generate_summary_report(total_pools, sample_size, summary_data, output_dir)


def collect_summary_data(firebase_service, sample_pools):
    """Collect data for summary report"""
    logger.info(f"Analyzing {len(sample_pools)} sample pools...")

    # Data collection structures
    summary_data = {
        "market_caps": [],
        "holders_counts": [],
        "data_point_counts": [],
        "time_spans": [],
        "buy_volumes": [],
        "price_changes": [],
        "has_trade_data": [],
        "avg_mc_growth_rates": [],
    }

    for i, pool_id in enumerate(sample_pools):
        logger.info(f"Analyzing pool {i+1}/{len(sample_pools)}: {pool_id}")

        # Fetch data for this pool
        pool_data = firebase_service.fetch_market_data(
            min_data_points=10, max_pools=1, limit_per_pool=100, pool_address=pool_id
        ).get(pool_id)

        if pool_data is not None and not pool_data.empty:
            # Count data points
            summary_data["data_point_counts"].append(len(pool_data))

            # Market cap statistics
            if "marketCap" in pool_data.columns:
                market_cap = pool_data["marketCap"].mean()
                summary_data["market_caps"].append(market_cap)

                # Calculate market cap growth rate
                if len(pool_data) > 1:
                    mc_sorted = pool_data.sort_values("timestamp")
                    if "marketCap" in mc_sorted.columns and mc_sorted["marketCap"].iloc[0] > 0:
                        mc_start = mc_sorted["marketCap"].iloc[0]
                        mc_end = mc_sorted["marketCap"].iloc[-1]
                        growth_rate = (mc_end - mc_start) / mc_start * 100
                        summary_data["avg_mc_growth_rates"].append(growth_rate)

            # Holders count
            if "holdersCount" in pool_data.columns:
                holders_count = pool_data["holdersCount"].mean()
                summary_data["holders_counts"].append(holders_count)

            # Time span calculation
            if "timestamp" in pool_data.columns:
                min_time = pool_data["timestamp"].min()
                max_time = pool_data["timestamp"].max()
                time_span_hours = (max_time - min_time).total_seconds() / 3600
                summary_data["time_spans"].append(time_span_hours)

            # Buy volume
            if "buyVolume5s" in pool_data.columns:
                buy_volume = pool_data["buyVolume5s"].mean()
                summary_data["buy_volumes"].append(buy_volume)

            # Price change
            if "priceChangePercent" in pool_data.columns:
                price_change = pool_data["priceChangePercent"].mean()
                summary_data["price_changes"].append(price_change)

            # Check if has trade data
            has_trade_data = any("trade" in col.lower() for col in pool_data.columns)
            summary_data["has_trade_data"].append(has_trade_data)

    return summary_data


def generate_summary_report(total_pools, sample_size, summary_data, output_dir):
    """Generate the summary report with statistics and charts"""
    # Print header
    print("\n" + "=" * 80)
    print("FIREBASE DATA SUMMARY REPORT")
    print("=" * 80)

    # General statistics
    print("\nGeneral Statistics:")
    print(f"Total pools in database: {total_pools}")
    print(f"Sample analyzed: {sample_size} pools")

    # Data points statistics
    data_points = summary_data["data_point_counts"]
    if data_points:
        print("\nData Points per Pool:")
        print(f"  Average: {np.mean(data_points):.1f}")
        print(f"  Median: {np.median(data_points):.1f}")
        print(f"  Min: {min(data_points)}")
        print(f"  Max: {max(data_points)}")

        # Generate histogram of data points
        plt.figure(figsize=(10, 6))
        plt.hist(data_points, bins=10, alpha=0.7)
        plt.title("Distribution of Data Points per Pool")
        plt.xlabel("Number of Data Points")
        plt.ylabel("Number of Pools")
        plt.grid(True, alpha=0.3)
        plt.savefig(os.path.join(output_dir, "data_points_distribution.png"))
        plt.close()

        print("  [Chart saved to outputs/data_points_distribution.png]")

    # Market cap statistics
    market_caps = summary_data["market_caps"]
    if market_caps:
        # Remove outliers for better visualization (top 5%)
        mc_for_viz = sorted(market_caps)[:]
        if len(mc_for_viz) > 20:  # Only if we have enough data points
            mc_for_viz = mc_for_viz[: int(len(mc_for_viz) * 0.95)]

        print("\nMarket Cap Statistics:")
        print(f"  Average: {np.mean(market_caps):.2f}")
        print(f"  Median: {np.median(market_caps):.2f}")
        print(f"  Min: {min(market_caps):.2f}")
        print(f"  Max: {max(market_caps):.2f}")

        # Generate histogram of market caps
        plt.figure(figsize=(10, 6))
        plt.hist(mc_for_viz, bins=10, alpha=0.7)
        plt.title("Distribution of Market Caps (excluding outliers)")
        plt.xlabel("Market Cap")
        plt.ylabel("Number of Pools")
        plt.grid(True, alpha=0.3)
        plt.savefig(os.path.join(output_dir, "market_cap_distribution.png"))
        plt.close()

        print("  [Chart saved to outputs/market_cap_distribution.png]")

    # Holders count statistics
    holders_counts = summary_data["holders_counts"]
    if holders_counts:
        # Remove outliers for better visualization (top 5%)
        hc_for_viz = sorted(holders_counts)[:]
        if len(hc_for_viz) > 20:  # Only if we have enough data points
            hc_for_viz = hc_for_viz[: int(len(hc_for_viz) * 0.95)]

        print("\nHolders Count Statistics:")
        print(f"  Average: {np.mean(holders_counts):.1f}")
        print(f"  Median: {np.median(holders_counts):.1f}")
        print(f"  Min: {min(holders_counts):.1f}")
        print(f"  Max: {max(holders_counts):.1f}")

        # Generate histogram of holders counts
        plt.figure(figsize=(10, 6))
        plt.hist(hc_for_viz, bins=10, alpha=0.7)
        plt.title("Distribution of Holders Counts (excluding outliers)")
        plt.xlabel("Holders Count")
        plt.ylabel("Number of Pools")
        plt.grid(True, alpha=0.3)
        plt.savefig(os.path.join(output_dir, "holders_distribution.png"))
        plt.close()

        print("  [Chart saved to outputs/holders_distribution.png]")

    # Time span statistics
    time_spans = summary_data["time_spans"]
    if time_spans:
        print("\nTime Span Statistics (hours):")
        print(f"  Average: {np.mean(time_spans):.2f}")
        print(f"  Median: {np.median(time_spans):.2f}")
        print(f"  Min: {min(time_spans):.2f}")
        print(f"  Max: {max(time_spans):.2f}")

    # Buy volume statistics
    buy_volumes = summary_data["buy_volumes"]
    if buy_volumes:
        print("\nBuy Volume Statistics:")
        print(f"  Average: {np.mean(buy_volumes):.4f}")
        print(f"  Median: {np.median(buy_volumes):.4f}")
        print(f"  Min: {min(buy_volumes):.4f}")
        print(f"  Max: {max(buy_volumes):.4f}")

    # Price change statistics
    price_changes = summary_data["price_changes"]
    if price_changes:
        print("\nPrice Change Percentage Statistics:")
        print(f"  Average: {np.mean(price_changes):.2f}%")
        print(f"  Median: {np.median(price_changes):.2f}%")
        print(f"  Min: {min(price_changes):.2f}%")
        print(f"  Max: {max(price_changes):.2f}%")

    # Market cap growth rate statistics
    growth_rates = summary_data["avg_mc_growth_rates"]
    if growth_rates:
        print("\nMarket Cap Growth Rate Statistics (%):")
        print(f"  Average: {np.mean(growth_rates):.2f}%")
        print(f"  Median: {np.median(growth_rates):.2f}%")
        print(f"  Min: {min(growth_rates):.2f}%")
        print(f"  Max: {max(growth_rates):.2f}%")

        # Count positive vs negative growth
        positive_growth = sum(1 for rate in growth_rates if rate > 0)
        percentage_positive = positive_growth / len(growth_rates) * 100
        print(f"  Positive Growth: {positive_growth} pools ({percentage_positive:.1f}%)")

        # Generate histogram of growth rates
        plt.figure(figsize=(10, 6))
        plt.hist(growth_rates, bins=10, alpha=0.7)
        plt.title("Distribution of Market Cap Growth Rates")
        plt.xlabel("Growth Rate (%)")
        plt.ylabel("Number of Pools")
        plt.grid(True, alpha=0.3)
        plt.axvline(x=0, color="r", linestyle="--", alpha=0.7)
        plt.savefig(os.path.join(output_dir, "growth_rate_distribution.png"))
        plt.close()

        print("  [Chart saved to outputs/growth_rate_distribution.png]")

    # Trade data availability
    has_trade_data = summary_data["has_trade_data"]
    if has_trade_data:
        trade_data_count = sum(has_trade_data)
        trade_data_percent = trade_data_count / len(has_trade_data) * 100
        print(f"\nTrade Data Available: {trade_data_count} pools ({trade_data_percent:.1f}%)")

    # Summary
    print("\nSummary for Trading Simulation:")
    print(f"- Database contains {total_pools} pools")
    print(f"- Average {np.mean(data_points):.1f} data points per pool")
    print(f"- Data typically spans {np.median(time_spans):.2f} hours per pool")

    if growth_rates:
        print(f"- {percentage_positive:.1f}% of pools show positive market cap growth")

    if has_trade_data:
        print(f"- {trade_data_percent:.1f}% of pools have detailed trading data")

    print("\nRecommendations for Trading Simulation:")
    print("- Consider using pools with at least 50 data points for more reliable backtesting")
    print("- Focus on pools with complete trading data for accurate volume-based strategies")
    print("- For holder growth strategies, filter pools with consistent holder count increases")
    print("=" * 80)


if __name__ == "__main__":
    main()
