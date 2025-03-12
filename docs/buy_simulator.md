# Buy Simulator Documentation

The `BuySimulator` is a core component responsible for identifying potential entry points for trades based on configurable market metrics and parameters.

## Overview

The Buy Simulator analyzes market data to identify optimal entry points based on a set of predefined criteria. It evaluates various market metrics such as:

- Market cap changes
- Holder growth rates
- Buy volume metrics
- Price momentum indicators

## Usage

```python
from src.simulation.buy_simulator import BuySimulator, get_default_parameters

# Initialize with default or custom parameters
buy_simulator = BuySimulator(
    early_mc_limit=400000,  # Market cap filter for early elimination
    min_delay=60,           # Minimum time delay for entry after pattern detection
    max_delay=200,          # Maximum time delay for entry after pattern detection
    buy_params=get_default_parameters()  # Parameters for buy conditions
)

# Find buy opportunities in processed market data
buy_opportunity = buy_simulator.find_buy_opportunity(processed_data_df)

if buy_opportunity:
    print(f"Found entry point at price: {buy_opportunity['entry_price']}")
    # Process the buy opportunity further...
else:
    print("No suitable entry points found")
```

## Configuration Parameters

### Core Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `early_mc_limit` | Market cap threshold for early filtering | 400000 |
| `min_delay` | Minimum time delay (in seconds) before entry | 60 |
| `max_delay` | Maximum time delay (in seconds) after pattern detection | 200 |

### Buy Condition Parameters

These parameters determine the thresholds for various market metrics that must be met for a buy opportunity:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `mc_change_5s` | Market cap change over 5 seconds (%) | 5.0 |
| `mc_change_30s` | Market cap change over 30 seconds (%) | 8.0 |
| `holder_delta_30s` | Holder growth over 30 seconds | 20 |
| `buy_volume_5s` | Buy volume over 5 seconds | 5.0 |
| `net_volume_5s` | Net volume over 5 seconds | 0.0 |
| `buy_sell_ratio_10s` | Buy/sell ratio over 10 seconds | 1.5 |
| `mc_growth_from_start` | Market cap growth from pool launch (%) | 50.0 |
| `holder_growth_from_start` | Holder growth from pool launch | 25 |
| `large_buy_5s` | Number of large buys in last 5 seconds | 0 |
| `price_change` | Minimum price change threshold (%) | 2.0 |

## Algorithm

The buy simulator uses the following algorithm:

1. **Early Filtering**: Eliminate pools with market cap above threshold
2. **Pattern Detection**: Identify market patterns that meet buy criteria
3. **Delay Application**: Apply time delay to avoid false signals
4. **Entry Selection**: Select optimal entry point based on metrics
5. **Opportunity Creation**: Format buy opportunity with entry details and post-entry data

## Return Value

When a buy opportunity is found, the simulator returns a dictionary containing:

```python
{
    "pool_address": "...",           # Pool identifier
    "entry_price": 100000,           # Market cap at entry
    "entry_time": "2023-01-01T...",  # Timestamp of entry
    "entry_row": 42,                 # Row index in the dataframe
    "entry_metrics": {               # Key metrics at entry
        "mc_change_5s": 6.5,
        "holder_delta_30s": 25,
        "buy_volume_5s": 8.2
        # ...other metrics
    },
    "post_entry_data": pd.DataFrame  # Data after entry point
}
```

## Additional Functions

The module includes additional utility functions:

- `get_default_parameters()`: Returns default parameter set
- `calculate_returns(buy_opportunity)`: Calculates potential returns for a buy opportunity 