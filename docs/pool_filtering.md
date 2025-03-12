
## Data Filtering for Analytics and Simulations

For reliable analytics and trading simulations, pools should meet certain criteria regarding data quality and consistency.

### Filtering Criteria

1. **Minimum Time Span**: At least 10 minutes of data (approximately 600 data points)
2. **Data Consistency**: Must contain all common fields identified across the database
3. **Data Quality**: Should have consistent data collection frequency

### Common Fields

The following fields are present in at least 80% of pools and should be used for consistent analytics:

- `athMarketCap`
- `bigBuy10s`
- `bigBuy5s`
- `buyVolume10s`
- `buyVolume5s`
- `currentPrice`
- `doc_id`
- `holderDelta10s`
- `holderDelta30s`
- `holderDelta5s`
- `holderDelta60s`
- `holdersCount`
- `holdersGrowthFromStart`
- `initialHoldersCount`
- `largeBuy10s`
- `largeBuy5s`
- `maMarketCap10s`
- `maMarketCap30s`
- `maMarketCap60s`
- `marketCap`
- `marketCapChange10s`
- `marketCapChange30s`
- `marketCapChange5s`
- `marketCapChange60s`
- `minMarketCap`
- `netVolume10s`
- `netVolume5s`
- `poolAddress`
- `priceChangeFromStart`
- `priceChangePercent`
- `superBuy10s`
- `superBuy5s`
- `timeFromStart`
- `timestamp`

### Dataset Statistics

- Total pools in database: 100 (sample)
- Pools with at least 10 minutes of data: 60
- Pools with consistent data structure: 97
- Pools meeting both criteria: 60

Average data points per minute: 56.800482044795636

### How to Filter Pools

To filter pools for analytics and simulations, use:

```python
# Method 1: Using filtered_pools.json
with open('outputs/filtered_pools.json', 'r') as f:
    filtered_pools = json.load(f)
    
# Get pool IDs
filtered_pool_ids = [p['pool_id'] for p in filtered_pools]

# Method 2: Using FirebaseService with filtering
firebase_service = FirebaseService()
pool_data = firebase_service.fetch_market_data(
    min_data_points=600,  # Approximately 10 minutes of data
    min_time_span_minutes=10,
    ensure_common_fields=True
)
```

For optimal simulation results, consider using pools that have at least 30 minutes of data (approximately 1800 data points).
