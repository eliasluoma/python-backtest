# Sell Simulator Documentation

The `SellSimulator` is responsible for determining optimal exit points for trades based on configurable parameters and market conditions.

## Overview

The Sell Simulator analyzes market data after entry to identify the best time to exit a position based on multiple strategies:

- Take profit targets
- Stop loss protection
- Trailing stop mechanism
- Momentum-based exits
- Performance evaluation

## Usage

```python
from src.simulation.sell_simulator import SellSimulator

# Initialize with custom parameters
sell_simulator = SellSimulator(
    initial_investment=1.0,     # Initial investment amount
    base_take_profit=1.9,       # Take profit target (1.9 = 90% profit)
    stop_loss=0.65,             # Stop loss level (0.65 = 35% loss)
    trailing_stop=0.9           # Trailing stop (0.9 = 10% from peak)
)

# Simulate sell strategy on a buy opportunity
trade_result = sell_simulator.simulate_sell(buy_opportunity)

if trade_result:
    print(f"Exit price: {trade_result['exit_price']}")
    print(f"Profit: {(trade_result['profit_ratio'] - 1) * 100:.2f}%")
    print(f"Exit reason: {trade_result['exit_reason']}")
```

## Configuration Parameters

### Core Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `initial_investment` | Initial investment amount | 1.0 |
| `base_take_profit` | Take profit multiplier (1.9 = 90% profit target) | 1.9 |
| `stop_loss` | Stop loss multiplier (0.65 = 35% max loss) | 0.65 |
| `trailing_stop` | Trailing stop multiplier (0.9 = 10% from peak) | 0.9 |

### Momentum Parameters

The `momentum_params` dictionary controls the thresholds for evaluating whether price momentum is still strong:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `lp_holder_growth_threshold` | LP token holder growth threshold | 1.0 |
| `mc_change_threshold` | Market cap change threshold | 6.0 |
| `holder_change_threshold` | Holder change threshold | 24.5 |
| `buy_volume_threshold` | Buy volume threshold | 13.0 |
| `net_volume_threshold` | Net volume threshold | 3.0 |
| `required_strong` | Required number of strong metrics | 1.0 |

### Stop Loss Parameters

The `stoploss_params` dictionary controls stop loss behavior:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `ignore_sl_holder_growth` | Holder growth rate to ignore stop loss | 12.5 |
| `max_time_after_peak` | Maximum time to wait after peak (seconds) | 300 |
| `underperform_threshold` | Performance threshold for early exit | 1.2 |
| `underperform_max_time` | Maximum time to wait before underperform exit (seconds) | 180 |

## Exit Strategies

The Sell Simulator implements several exit strategies:

1. **Take Profit**: Exit when price reaches target and momentum starts declining
2. **Stop Loss**: Exit when price falls below stop loss level
3. **Trailing Stop**: Exit when price falls below trailing stop from peak
4. **Low Performance Exit**: Exit if performance is below threshold after a specified time
5. **Force Exit**: Exit at the end of available data if no other exit is triggered

## Algorithm

The sell simulation algorithm follows these steps:

1. **Initialize**: Set up parameters and starting conditions
2. **Iterate Data**: Process each market data point after entry
3. **Track Performance**: Monitor price relative to entry and peak
4. **Check Exits**: Evaluate exit conditions at each point
5. **Apply Strategy**: Execute the appropriate exit strategy
6. **Calculate Results**: Compute profit/loss and record exit details

## Return Value

When a trade is completed, the simulator returns a dictionary containing:

```python
{
    "pool_address": "...",           # Pool identifier
    "entry_price": 100000,           # Market cap at entry
    "entry_time": "2023-01-01T...",  # Timestamp of entry
    "exit_price": 150000,            # Market cap at exit
    "exit_time": "2023-01-01T...",   # Timestamp of exit
    "exit_row": 67,                  # Row index of exit
    "exit_reason": "Take Profit",    # Reason for exit
    "profit_ratio": 1.5,             # Profit ratio (1.5 = 50% profit)
    "max_profit": 1.8,               # Maximum potential profit seen
    "trade_duration": 3600,          # Duration in seconds
    "peak_to_exit_time": 120         # Time from peak to exit in seconds
}
```

## Additional Functions

The module includes useful utility functions:

- `calculate_trade_metrics(trades)`: Calculates aggregate metrics from a list of trades
- `get_default_stoploss_parameters()`: Returns default stop loss parameters 
- `get_default_momentum_parameters()`: Returns default momentum parameters 