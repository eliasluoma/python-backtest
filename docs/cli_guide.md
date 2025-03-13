# Unified CLI Guide

This guide provides detailed documentation for the Solana Trading Simulator's command-line interface (CLI).

## Overview

The Solana Trading Simulator provides a unified command-line interface for all functionality, making it easy to access all features from a single entry point. The CLI follows a command-subcommand structure with consistent parameter naming across all commands.

## Usage

The basic syntax for the CLI is:

```bash
python -m src.cli.main [command] [subcommand] [options]
```

## Global Options

These options are available for all commands:

| Option | Description |
|--------|-------------|
| `--verbose`, `-v` | Increase verbosity (can be used multiple times for more detail) |
| `--quiet`, `-q` | Suppress non-error messages |
| `--credentials` | Firebase credentials JSON file or string |
| `--env-file` | Path to .env file (default: .env.local) |
| `--output-dir` | Directory to save output files (default: outputs) |
| `--version` | Show version information and exit |

## Commands

The following commands are available:

- [`simulate`](#simulate-command): Run a trading simulation
- [`analyze`](#analyze-command): Analyze market data
- [`export`](#export-command): Export pool data to JSON files
- [`visualize`](#visualize-command): Generate visualizations from data
- [`cache`](#cache-command): Manage the local data cache

### Simulate Command

The `simulate` command runs a trading simulation with configurable parameters:

```bash
python -m src.cli.main simulate [options]
```

#### Options

**Data Parameters:**

| Option | Description | Default |
|--------|-------------|---------|
| `--max-pools` | Maximum number of pools to analyze | 10 |
| `--min-data-points` | Minimum data points required per pool | 100 |

**Buy Parameters:**

| Option | Description | Default |
|--------|-------------|---------|
| `--early-mc-limit` | Market cap threshold for early filtering | 400000 |
| `--min-delay` | Minimum delay in seconds | 60 |
| `--max-delay` | Maximum delay in seconds | 200 |
| `--mc-change-5s` | Market cap change 5s threshold | 5.0 |
| `--holder-delta-30s` | Holder delta 30s threshold | 20 |
| `--buy-volume-5s` | Buy volume 5s threshold | 5.0 |

**Sell Parameters:**

| Option | Description | Default |
|--------|-------------|---------|
| `--take-profit` | Take profit multiplier | 1.9 |
| `--stop-loss` | Stop loss multiplier | 0.65 |
| `--trailing-stop` | Trailing stop multiplier | 0.9 |
| `--skip-sell` | Skip sell simulation | false |

#### Examples

Run a simulation with default parameters:
```bash
python -m src.cli.main simulate --credentials firebase-key.json
```

Run with custom buy parameters:
```bash
python -m src.cli.main simulate --credentials firebase-key.json --mc-change-5s 8.0 --holder-delta-30s 30 --buy-volume-5s 8.0
```

Run with custom sell parameters:
```bash
python -m src.cli.main simulate --credentials firebase-key.json --take-profit 2.5 --stop-loss 0.7 --trailing-stop 0.85
```

Run a buy-only simulation:
```bash
python -m src.cli.main simulate --credentials firebase-key.json --skip-sell
```

### Analyze Command

The `analyze` command provides tools for analyzing market data:

```bash
python -m src.cli.main analyze [subcommand] [options]
```

#### Subcommands

**all**: Analyze all pools
```bash
python -m src.cli.main analyze all [options]
```

Options:
| Option | Description | Default |
|--------|-------------|---------|
| `--output-prefix` | Prefix for output files | pool_analysis |

**invalid**: Analyze invalid pools
```bash
python -m src.cli.main analyze invalid [options]
```

Options:
| Option | Description | Default |
|--------|-------------|---------|
| `--input`, `-i` | Path to JSON file containing invalid pool IDs | outputs/invalid_pools.json |
| `--output`, `-o` | Path to save the analysis results | outputs/invalid_pools_analysis.json |
| `--max-pools`, `-m` | Maximum number of pools to analyze | None (all pools) |
| `--limit-per-pool`, `-l` | Maximum number of data points to fetch per pool | 600 |

#### Examples

Analyze all pools:
```bash
python -m src.cli.main analyze all
```

Analyze invalid pools:
```bash
python -m src.cli.main analyze invalid --input outputs/invalid_pools.json --max-pools 5
```

### Export Command

The `export` command exports pool data to JSON or CSV files:

```bash
python -m src.cli.main export [options]
```

#### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--input`, `-i` | Path to JSON file containing pool IDs | (required) |
| `--output-dir`, `-o` | Directory to save the exported data | outputs/exported_pools |
| `--max-rows`, `-m` | Maximum number of rows to export per pool | None (all rows) |
| `--format`, `-f` | Output format (json or csv) | json |

#### Examples

Export pool data to JSON:
```bash
python -m src.cli.main export --input outputs/valid_pools.json
```

Export with row limit and specific output format:
```bash
python -m src.cli.main export --input outputs/valid_pools.json --max-rows 1000 --format csv
```

### Visualize Command

The `visualize` command generates visualizations from data:

```bash
python -m src.cli.main visualize [subcommand] [options]
```

#### Subcommands

**market**: Visualize market data
```bash
python -m src.cli.main visualize market [options]
```

Options:
| Option | Description | Default |
|--------|-------------|---------|
| `--input`, `-i` | Path to market data JSON file or directory | (required) |
| `--output-dir`, `-o` | Directory to save visualizations | outputs/visualizations/market |
| `--pools`, `-p` | Specific pool IDs to visualize | None (all pools) |
| `--metrics`, `-m` | Metrics to visualize | marketCap, holdersCount, priceChangePercent |

**results**: Visualize simulation results
```bash
python -m src.cli.main visualize results [options]
```

Options:
| Option | Description | Default |
|--------|-------------|---------|
| `--input`, `-i` | Path to simulation results JSON file | (required) |
| `--output-dir`, `-o` | Directory to save visualizations | outputs/visualizations/results |
| `--type`, `-t` | Type of visualization to generate (summary, detailed, all) | all |

**compare**: Compare different strategies
```bash
python -m src.cli.main visualize compare [options]
```

Options:
| Option | Description | Default |
|--------|-------------|---------|
| `--input`, `-i` | Paths to multiple simulation results JSON files | (required) |
| `--labels`, `-l` | Labels for each strategy | None (uses filenames) |
| `--output-dir`, `-o` | Directory to save visualizations | outputs/visualizations/comparison |

#### Examples

Visualize market data:
```bash
python -m src.cli.main visualize market --input outputs/exported_pools/pool_data.json
```

Visualize simulation results:
```bash
python -m src.cli.main visualize results --input outputs/trades_2023-03-15.json
```

Compare strategies:
```bash
python -m src.cli.main visualize compare --input outputs/strategy1.json outputs/strategy2.json --labels "Aggressive" "Conservative"
```

### Cache Command

The `cache` command provides functionality for managing the local SQLite cache:

```bash
python -m src.cli.main cache [subcommand] [options]
```

#### Subcommands

**import**: Import pools from Firebase to local cache
```bash
python -m src.cli.main cache import [options]
```

Options:
| Option | Description | Default |
|--------|-------------|---------|
| `--pools`, `-p` | Specific pools to import | None (imports pools up to the limit) |
| `--limit`, `-l` | Maximum number of pools to import | None (imports all available pools) |
| `--min-points`, `-m` | Minimum data points required for a pool to be imported | 600 (10 minutes) |
| `--schema`, `-s` | Path to schema file | updated_schema.sql |

**update**: Update cache with latest data
```bash
python -m src.cli.main cache update [options]
```

Options:
| Option | Description | Default |
|--------|-------------|---------|
| `--pools`, `-p` | Specific pools to update | None (updates all pools) |
| `--recent`, `-r` | Update only recently active pools | False |
| `--min-points`, `-m` | Minimum data points required in cache | 0 |

**clear**: Clear the cache
```bash
python -m src.cli.main cache clear [options]
```

Options:
| Option | Description | Default |
|--------|-------------|---------|
| `--days`, `-d` | Clear data older than specified days | None (clears entire cache) |

**status**: Show cache status
```bash
python -m src.cli.main cache status
```

**backup**: Create a backup of the cache
```bash
python -m src.cli.main cache backup [options]
```

Options:
| Option | Description | Default |
|--------|-------------|---------|
| `--output`, `-o` | Output path for backup | None (creates in default location) |

#### Examples

Import all available pools with at least 600 data points:
```bash
python -m src.cli.main cache import
```

Import specific pools:
```bash
python -m src.cli.main cache import --pools pool1 pool2 pool3
```

Import a limited number of pools:
```bash
python -m src.cli.main cache import --limit 20 --min-points 1000
```

Update cache with latest data for all pools:
```bash
python -m src.cli.main cache update
```

Show cache status:
```bash
python -m src.cli.main cache status
```

Clear entire cache:
```bash
python -m src.cli.main cache clear
```

## Exit Codes

The CLI uses the following exit codes:

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | General error |
| 2 | Invalid arguments |

## Environment Variables

The CLI respects the following environment variables:

| Variable | Description |
|----------|-------------|
| FIREBASE_KEY_FILE | Path to Firebase credentials file |

These can also be set in the `.env.local` file. 