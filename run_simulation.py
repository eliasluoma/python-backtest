#!/usr/bin/env python3
import argparse
import logging
import json
import sys
from typing import Dict, Optional, List

from firebase_service import FirebaseService
from part1_buy_simulation import BuySimulator, preprocess_pool_data
from part2_sell_simulation import SellSimulator

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("SimulationRunner")


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
        self.firebase_service = FirebaseService(env_file, firebase_credentials)
        self.early_mc_limit = early_mc_limit
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.buy_opportunities = []
        self.trade_results = []

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
            df = self.firebase_service.preprocess_data(df)

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

            logger.info(
                f"Buy simulation complete. Found {len(self.buy_opportunities)} opportunities."
            )

            # Run sell simulation
            if self.buy_opportunities:
                logger.info("Running sell simulation...")
                for buy_opportunity in self.buy_opportunities:
                    sell_result = sell_simulator.simulate_sell(buy_opportunity)
                    if sell_result:
                        self.trade_results.append(sell_result)

                logger.info(
                    f"Sell simulation complete. Processed {len(self.trade_results)} trades."
                )

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

            avg_profit = (
                sum([(p - 1.0) * 100 for p in profits]) / total_trades
                if total_trades > 0
                else 0
            )

            logger.info("\n=== SIMULATION RESULTS ===")
            logger.info(f"Total trades: {total_trades}")
            logger.info(f"Winning trades: {winning_trades} ({win_rate:.2f}%)")
            logger.info(f"Losing trades: {losing_trades}")
            logger.info(f"Average profit: {avg_profit:.2f}%")

            # Print top 5 most profitable trades
            if self.trade_results:
                sorted_results = sorted(
                    self.trade_results, key=lambda x: x["profit_ratio"], reverse=True
                )

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


def main():
    """Main entry point for the simulation runner"""
    parser = argparse.ArgumentParser(description="Run Solana bot backtest simulations")

    parser.add_argument(
        "--credentials", type=str, help="Firebase credentials JSON file or string"
    )
    parser.add_argument(
        "--env-file", type=str, default=".env.local", help="Path to .env file"
    )
    parser.add_argument(
        "--max-pools", type=int, help="Maximum number of pools to analyze (for testing)"
    )
    parser.add_argument(
        "--early-mc-limit",
        type=float,
        default=400000,
        help="Market cap limit for early filtering",
    )
    parser.add_argument(
        "--min-delay", type=int, default=60, help="Minimum delay for buy simulation"
    )
    parser.add_argument(
        "--max-delay", type=int, default=200, help="Maximum delay for buy simulation"
    )

    args = parser.parse_args()

    try:
        # Create and run simulation
        runner = BacktestRunner(
            firebase_credentials=args.credentials,
            env_file=args.env_file,
            early_mc_limit=args.early_mc_limit,
            min_delay=args.min_delay,
            max_delay=args.max_delay,
        )

        runner.run_simulation(max_pools=args.max_pools)

        logger.info("Simulation completed successfully")
        return 0

    except Exception as e:
        logger.error(f"Simulation failed: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
