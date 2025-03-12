# Firebase Setup Guide

This guide provides instructions for using Firebase with the Solana Trading Strategy Simulator.

## Overview

The simulator integrates with Firebase Firestore to fetch market data for backtesting. This modular approach loads data directly into memory without storing local files, using:

1. `src/data/firebase_service.py` - Professional service class for connecting to Firebase and fetching data
2. `scripts/run_simulation.py` - Main script that leverages the Firebase service for backtesting

## Setup Instructions

### 1. Firebase Credentials

You need Firebase credentials to connect to your Firestore database. There are multiple ways to provide these:

#### Option A: Using a Service Account JSON File (Recommended)

If you have a Firebase service account JSON file:

```bash
# Run the simulation with a credentials file
python scripts/run_simulation.py --credentials path/to/firebase-credentials.json
```

#### Option B: Using Environment Variables

Set your Firebase credentials in the `.env.local` file:

```
# In .env.local
FIREBASE_CREDENTIALS={"type":"service_account","project_id":"your-project",...}
# OR
FIREBASE_CREDENTIALS_FILE=/path/to/credentials.json
```

Then run the simulation with the env file:

```bash
python scripts/run_simulation.py --env-file .env.local
```

### 2. Firebase Data Structure

The simulator expects the following data structure in your Firestore database:

- Collection: `pools`
  - Documents: Pool addresses (e.g., "0x1234...")
    - Each document should contain:
      - Market data points with timestamps
      - Metrics like market cap, holders, volumes, etc.

## Log Files

The simulator generates log files during execution which are stored in the `logs` directory. These logs contain detailed information about the simulation process, including:

- Firebase connection status
- Data processing steps
- Buy and sell decisions
- Performance metrics

Log files are named with a timestamp (e.g., `simulation_20250312_141247.log`).

## Legacy Scripts

For reference, an older script for fetching Firebase data (`fetch_firebase_data.py`) has been moved to the `legacy` folder. This script directly exports data to CSV files rather than using the in-memory approach of the current implementation.

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