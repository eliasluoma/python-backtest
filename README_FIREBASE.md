# Firebase-Based Backtesting Service for Solana Bot

This document provides instructions for using the professional Firebase service we've created for backtesting the Solana trading bot.

## Overview

We've created a modular Firebase service that loads data directly into memory without storing local files. The service includes:

1. `firebase_service.py` - A professional service class for connecting to Firebase and fetching data
2. `run_simulation.py` - A script that leverages the Firebase service to run backtests directly in memory

## Setup Instructions

### 1. Firebase Credentials

You need Firebase credentials to connect to your Firestore database. There are multiple ways to provide these:

#### Option A: Using a Service Account JSON File

If you have a Firebase service account JSON file:

```bash
# Run the simulation with a credentials file
python run_simulation.py --credentials path/to/firebase-credentials.json
```

#### Option B: Using Environment Variables

Set your Firebase credentials in the `.env.local` file:

```
# In .env.local
FIREBASE_CREDENTIALS={"type":"service_account","project_id":"your-project",...}
# OR
FIREBASE_CREDENTIALS_FILE=/path/to/credentials.json
```

### 2. Running Simulations

Basic usage:

```bash
# Run with default parameters
python run_simulation.py

# Limit the number of pools (for testing)
python run_simulation.py --max-pools 10

# Adjust simulation parameters
python run_simulation.py --early-mc-limit 500000 --min-delay 30 --max-delay 180
```

## Advanced Usage

### Custom Backtest Parameters

You can create a custom script to run simulations with specific parameters:

```python
from firebase_service import FirebaseService
from run_simulation import BacktestRunner

# Custom buy parameters
buy_params = {
    'mc_growth_from_start': 100,
    'holder_growth_from_start': 50,
    'holder_delta_30s': 80,
    'mc_change_5s': 5,
    'buy_volume_5s': 15,
    'large_buy_5s': 1,
}

# Custom sell parameters
sell_params = {
    'initial_investment': 1.0,
    'base_take_profit': 2.0,
    'stop_loss': 0.7,
    'trailing_stop': 0.85,
}

# Initialize and run backtest
runner = BacktestRunner(firebase_credentials="path/to/credentials.json")
runner.run_simulation(
    buy_params=buy_params,
    sell_params=sell_params,
    max_pools=20
)
```

### Viewing Results

The simulation results are displayed in the console log, including:

- Total number of trades
- Win rate
- Average profit
- Top 5 most profitable trades

## Troubleshooting

### No Firebase Connection

If you see "Error initializing Firebase", check that:

1. Your Firebase credentials are valid
2. You have proper permissions to access the Firestore database
3. The collection and document structure matches what the code expects

### Insufficient Data

If the simulation finds no opportunities, check that:

1. You have sufficient data points for each pool
2. The buy parameters are not too restrictive
3. The data structure in Firestore matches what the code expects

## Performance Considerations

- The service loads all data into memory, which can be RAM-intensive for large datasets
- For very large datasets, consider adding options to limit the time range or number of data points

## Technical Notes

- All timestamps are converted to UTC for consistent calculations
- The service preprocesses the data to ensure proper formats and sorting
- Exception handling is implemented throughout to provide clear error messages 