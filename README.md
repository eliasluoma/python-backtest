# Solana Trading Strategy Simulator

A backtesting framework for Solana token trading strategies using market data from Firebase.

## Overview

This project provides a comprehensive framework for simulating and optimizing trading strategies for Solana tokens. It uses market data to identify optimal buy and sell parameters based on configurable metrics.

### Key Features

- Buy strategy simulation based on market metrics
- Sell strategy simulation with configurable take-profit and stop-loss
- Firebase integration for retrieving market data
- Comprehensive test suite
- Custom market data visualization

## Quick Start

### Prerequisites

- Python 3.8+
- Firebase credentials (JSON key file)

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd python-backtest

# Install dependencies
pip install -r requirements.txt

# Set up Firebase credentials
cp path/to/your-firebase-key.json firebase-key.json
```

### Basic Usage

```bash
# Run a basic simulation
python scripts/run_simulation.py --credentials firebase-key.json

# Test sell strategies with simulated data
python scripts/test_sell_simulation.py

# Run all tests
python tests/run_all_tests.py
```

## Documentation

For detailed documentation, please see the [docs folder](./docs):

- [Complete User Guide](./docs/index.md)
- [Buy Simulator Documentation](./docs/buy_simulator.md)
- [Sell Simulator Documentation](./docs/sell_simulator.md)
- [Simulation Guide](./docs/simulation_guide.md)

## License

This project is licensed under the MIT License - see the LICENSE file for details. 