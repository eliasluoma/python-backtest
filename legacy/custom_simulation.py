#!/usr/bin/env python3
"""
Custom Simulation Example

This script demonstrates how to use the Firebase service to run
a custom simulation with specific parameters.
"""

import logging
import sys
from firebase_service import FirebaseService
from run_simulation import BacktestRunner

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("CustomSimulation")


def main():
    """Run a custom simulation with specific parameters"""

    # Set these variables to match your Firebase credentials
    firebase_credentials = None  # Path to your credentials file or JSON string
    env_file = ".env.local"  # Path to your .env file

    # Example: Aggressive buy parameters for high growth tokens
    buy_params = {
        "mc_growth_from_start": 150,  # Higher market cap growth threshold
        "holder_growth_from_start": 80,  # Higher holder growth threshold
        "holder_delta_30s": 100,  # More aggressive holder increase
        "mc_change_5s": 6,  # Steeper market cap growth
        "buy_volume_5s": 18,  # Higher buying volume
        "large_buy_5s": 2,  # Require more large buys
        "net_volume_5s": 6,  # Higher net volume
        "price_change": 4.0,  # Larger price change
        "mc_change_30s": 55,  # Steeper market cap growth over 30s
        "buy_sell_ratio_10s": 1.5,  # Higher buy/sell ratio
    }

    # Example: Conservative sell parameters to secure profits
    sell_params = {
        "initial_investment": 1.0,  # 1 SOL investment
        "base_take_profit": 1.6,  # Take profit at 1.6x (instead of 1.9x)
        "stop_loss": 0.75,  # Higher stop loss to minimize losses
        "trailing_stop": 0.92,  # Tighter trailing stop
    }

    # Custom stoploss parameters
    stoploss_params = {
        "holder_growth_30s_strong": 8.0,  # Lower threshold
        "holder_growth_60s_strong": 40.0,  # Lower threshold
        "holder_growth_30s_moderate": 15.0,  # Lower threshold
        "holder_growth_60s_moderate": 25.0,  # Lower threshold
        "buy_volume_moderate": 12.0,  # Lower threshold
        "mc_drop_limit": -35.0,  # Less tolerant of MC drops
    }

    # Custom momentum parameters
    momentum_params = {
        "mc_change_threshold": 5.0,  # Lower threshold
        "holder_change_threshold": 20.0,  # Lower threshold
        "buy_volume_threshold": 12.0,  # Lower threshold
        "net_volume_threshold": 2.5,  # Lower threshold
        "required_strong": 1.0,  # Keep same
        "lp_holder_growth_threshold": 0.0,  # Keep same
    }

    try:
        logger.info("Starting custom simulation with specific parameters")

        # Create the backtest runner
        runner = BacktestRunner(
            firebase_credentials=firebase_credentials,
            env_file=env_file,
            early_mc_limit=500000,  # Higher MC limit
            min_delay=30,  # Shorter minimum delay
            max_delay=180,  # Shorter maximum delay
        )

        # Run the simulation with custom parameters
        runner.run_simulation(
            buy_params=buy_params,
            sell_params=sell_params,
            stoploss_params=stoploss_params,
            momentum_params=momentum_params,
            max_pools=20,  # Limit to 20 pools for faster testing
        )

        logger.info("Custom simulation completed!")
        return 0

    except Exception as e:
        logger.error(f"Custom simulation failed: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
