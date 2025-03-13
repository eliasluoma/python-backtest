# Solana Trading Simulator

This project is a backtesting framework for Solana trading strategies. It can fetch market data from Firebase, process it, analyze it, and simulate trading strategies.

## Features

- **Firebase Integration**: Fetch market data directly from Firebase Firestore.
- **Data Processing**: Clean, process, and analyze market data.
- **Backtesting**: Test trading strategies on historical data.
- **Visualization**: Generate charts and metrics for market analysis.

## Setup

### Prerequisites

- Python 3.8+
- pip
- Firebase account with Firestore database

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/solana-trading-simulator.git
   cd solana-trading-simulator
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up Firebase credentials:
   
   **IMPORTANT: Never commit credential files to your repository!**
   
   The recommended way to set up Firebase credentials is through environment variables:
   ```bash
   export FIREBASE_KEY_FILE=/path/to/your/firebase-credentials.json
   ```
   
   Alternatively, you can store the credentials file in one of these secure locations:
   - `credentials/firebase-credentials.json` (create a credentials directory that is git-ignored)
   - `~/.config/firebase-credentials.json` (user config directory)
   - `/etc/firebase-credentials.json` (system-wide location)
   
   Make sure to add any credential files to `.gitignore` to prevent accidentally committing them.

### Configuration

Create a `.env.local` file in the project root with the following variables (optional):
```
FIREBASE_KEY_FILE=path/to/your/firebase-key.json
MIN_DATA_POINTS=20
MAX_POOLS=10
```

## Usage

### Fetching Data from Firebase

```python
from src.data.firebase_service import FirebaseService

# Initialize Firebase service
firebase_service = FirebaseService()

# Fetch market data
market_data = firebase_service.fetch_market_data(
    min_data_points=20,  # Minimum data points required per pool
    max_pools=10,        # Maximum number of pools to fetch
    limit_per_pool=100   # Maximum number of data points per pool
)

# Fetch data for a specific pool
pool_address = "12QspooeZFsA4d41KtLz4p3e8YyLzPxG4bShsUCBbEgU"
pool_data = firebase_service.fetch_market_data(pool_address=pool_address)

# Fetch recent market data (last 24 hours)
recent_data = firebase_service.fetch_recent_market_data(hours_back=24)
```

### Pool Analysis Tools

The project includes several analysis tools for examining trading pools:

#### Analyzing All Pools

This tool analyzes all pools in Firebase and produces a comprehensive report:

```bash
python -m src.analysis.cli analyze-all
```

The report includes:
- How many pools have all required fields
- How many pools have at least 600 rows (approx. 10 min of data)
- How many pools have at least 1100 rows (approx. 18 min of data)
- Analysis of missing fields (which fields are missing and how often)
- Analysis of naming conventions (snake_case vs. camelCase)

Analysis results are saved in the `outputs` directory.

#### Analyzing Invalid Pools

This tool performs detailed analysis of pools identified as invalid:

```bash
python -m src.analysis.cli analyze-invalid --input outputs/invalid_pools.json
```

Parameters:
- `--input`: Path to JSON file containing invalid pool IDs
- `--output`: Path to save the analysis results
- `--max-pools`: Maximum number of pools to analyze
- `--limit-per-pool`: Maximum number of data points to fetch per pool

The analysis identifies specific issues with each pool and produces statistics on common problems.

#### Exporting Pool Data

This tool exports data from specified pools to JSON files:

```bash
python -m src.analysis.cli export --input outputs/valid_pools.json
```

Parameters:
- `--input`: Path to JSON file containing pool IDs (required)
- `--output-dir`: Directory to save the exported data
- `--max-rows`: Maximum number of rows to export per pool

Each pool's data is saved as a separate JSON file with metadata.

### Analyzing Market Data

You can run the market data analysis script to analyze and visualize the Firebase data:

```bash
python examples/analyze_market_data.py
```

This will:
1. Fetch data from Firebase
2. Generate statistical summaries
3. Create visualization charts in the `outputs` directory

### Data Structure

The Firebase data is structured as follows:

- **marketContext**: Main collection containing pool documents
  - Each pool document has a unique ID (pool address)
  - **marketContexts**: Subcollection with time-series data points

For a detailed explanation of the data schema, see the [Firebase Data Schema Documentation](docs/firebase_data_schema.md).

## Project Structure

```
solana-trading-simulator/
├── docs/
│   └── firebase_data_schema.md  # Data schema documentation
├── examples/
│   └── analyze_market_data.py   # Example script for data analysis
├── outputs/                     # Output directory for charts and results
├── src/
│   ├── data/
│   │   └── firebase_service.py  # Firebase service for fetching data
│   ├── utils/
│   │   └── firebase_utils.py    # Utility functions for Firebase
│   ├── simulation/             # Simulation modules
│   └── strategies/             # Trading strategy implementations
├── tests/
│   └── data/
│       └── test_firebase_service.py  # Tests for Firebase service
├── .env.local                  # Local environment variables
├── firebase-key.json           # Firebase credentials (not in repo)
├── requirements.txt            # Project dependencies
└── README.md                   # Project documentation
```

## Testing

Run all tests with:

```bash
python tests/run_all_tests.py
```

To run specific tests:

```bash
python -m unittest tests/data/test_firebase_service.py
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 