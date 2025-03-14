# Solana Trading Strategy Simulator

A backtesting framework for Solana token trading strategies using market data from Firebase.

## Project Structure

```
python-backtest/
├── legacy/                  # Legacy scripts (for reference)
├── scripts/                 # Executable scripts
│   ├── run_simulation.py    # Main simulation script
│   └── test_sell_simulation.py # Sell strategy test script
├── src/                     # Source code
│   ├── config/              # Configuration settings
│   ├── data/                # Data services
│   │   ├── data_processor.py
│   │   └── firebase_service.py
│   ├── simulation/          # Simulation modules
│   │   ├── buy_simulator.py
│   │   └── sell_simulator.py
│   └── utils/               # Utility functions
├── tests/                   # Test suite
│   ├── data/                # Data service tests
│   ├── integration/         # Integration tests
│   ├── simulation/          # Simulator tests
│   └── run_all_tests.py     # Test runner
├── docs/                    # Documentation
└── fetch_firebase_data.py   # Data retrieval script
```

## Features

- Buy strategy simulation based on market metrics
- Sell strategy simulation with configurable take-profit and stop-loss
- Firebase integration for retrieving market data
- Comprehensive test suite
- Custom market data visualization

## Getting Started

### Prerequisites

- Python 3.8+
- Firebase credentials (JSON key file)

### Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd python-backtest
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Set up Firebase credentials:
   ```
   cp path/to/your-firebase-key.json firebase-key.json
   ```

### Running Simulations

Run a basic simulation:

```bash
python scripts/run_simulation.py --credentials firebase-key.json
```

Additional options:

```bash
# Run with verbose output
python scripts/run_simulation.py --credentials firebase-key.json --verbose

# Limit number of pools to analyze
python scripts/run_simulation.py --credentials firebase-key.json --max-pools 5

# Customize sell strategy parameters
python scripts/run_simulation.py --credentials firebase-key.json --take-profit 2.0 --stop-loss 0.7 --trailing-stop 0.85
```

### Running Tests

Run all tests:

```bash
python tests/run_all_tests.py
```

## License

This project is licensed under the MIT License - see the LICENSE file for details. 