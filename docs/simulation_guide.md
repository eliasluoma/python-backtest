# Simulation Guide

This guide provides detailed instructions for running different types of simulations with the Solana Trading Strategy Simulator.

## Basic Simulation

The simplest way to run a simulation is using the main script with default parameters:

```bash
python scripts/run_simulation.py --credentials firebase-key.json
```

This will:
1. Connect to Firebase using your credentials
2. Fetch market data for Solana tokens
3. Run the buy and sell simulations
4. Output results to the console

## Parameter Customization

You can customize various aspects of the simulation by providing command line arguments:

### Data Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--credentials` | Path to Firebase credentials JSON file | Required |
| `--env-file` | Path to .env file for additional configuration | None |
| `--max-pools` | Maximum number of pools to analyze | 10 |
| `--min-data-points` | Minimum data points required per pool | 100 |

### Buy Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--early-mc-limit` | Market cap threshold for early filtering | 400000 |
| `--min-delay` | Minimum delay in seconds | 60 |
| `--max-delay` | Maximum delay in seconds | 200 |
| `--mc-change-5s` | Market cap change 5s threshold | 5.0 |
| `--holder-delta-30s` | Holder delta 30s threshold | 20 |
| `--buy-volume-5s` | Buy volume 5s threshold | 5.0 |

### Sell Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--take-profit` | Take profit multiplier | 1.9 |
| `--stop-loss` | Stop loss multiplier | 0.65 |
| `--trailing-stop` | Trailing stop multiplier | 0.9 |
| `--skip-sell` | Skip sell simulation | False |

### Output Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--output-dir` | Directory for saving results | `results` |
| `--verbose` | Enable verbose output | False |

## Example Commands

Here are some examples of common simulation scenarios:

### Quick Test with Limited Data

```bash
python scripts/run_simulation.py --credentials firebase-key.json --max-pools 3 --verbose
```

### Optimized for High Profit

```bash
python scripts/run_simulation.py --credentials firebase-key.json --take-profit 2.5 --stop-loss 0.7 --trailing-stop 0.85
```

### Conservative Buy Strategy

```bash
python scripts/run_simulation.py --credentials firebase-key.json --mc-change-5s 8.0 --holder-delta-30s 30 --buy-volume-5s 8.0
```

### Aggressive Buy Strategy

```bash
python scripts/run_simulation.py --credentials firebase-key.json --mc-change-5s 3.0 --holder-delta-30s 15 --buy-volume-5s 3.0
```

### Buy-Only Analysis

```bash
python scripts/run_simulation.py --credentials firebase-key.json --skip-sell
```

## Testing Sell Strategies

For in-depth analysis of sell strategies using simulated data:

```bash
python scripts/test_sell_simulation.py
```

This script:
1. Generates realistic market data with different phases
2. Tests multiple sell strategies with different parameters
3. Creates visualizations of entry/exit points
4. Compares performance of different strategies

## Understanding Results

After running a simulation, you'll get several outputs:

### Console Output

The console will display:
- Number of pools analyzed
- Buy opportunities found
- Trade results with:
  - Win rate
  - Average profit
  - Average hold time
  - Exit reason distribution

### Generated Files

By default, the simulation saves:

1. `results/buy_opportunities_{timestamp}.json` - All buy opportunities
2. `results/trades_{timestamp}.json` - All trade results (if sell simulation was run)
3. `results/summary_{timestamp}.json` - Summary metrics
4. `simulation_{timestamp}.log` - Detailed log of the simulation run

## Performance Analysis

The trade metrics provide insights into strategy performance:

- **Win Rate**: Percentage of trades that were profitable
- **Average Profit**: Average profit across all trades
- **Max Profit**: Maximum profit achieved
- **Average Hold Time**: Average duration of trades
- **Exit Reason Distribution**: How trades were exited (take profit, stop loss, etc.)

## Troubleshooting

### Common Issues

- **Firebase Connection Errors**: Check your credentials and internet connection
- **Missing Market Data**: Ensure your Firebase database has the expected structure
- **No Buy Opportunities**: Try adjusting buy parameters to be less restrictive
- **Poor Sell Performance**: Adjust take profit and stop loss parameters

### Debug Mode

For detailed debugging information, use:

```bash
python scripts/run_simulation.py --credentials firebase-key.json --verbose
``` 