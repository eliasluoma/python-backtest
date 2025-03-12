# Firebase Data Schema Documentation

This document outlines the structure of the Firebase database used in the Solana Trading Simulator project, including the collections, documents, and data fields.

## Database Structure

The Firebase database consists of the following main collections:

- `marketContext`: Contains documents for each pool being tracked
- `marketContextStatus`: Contains status information about the market context data collection

Each pool document in the `marketContext` collection contains a subcollection:

- `marketContexts`: Time-series data about the pool's market context at different points in time

## Pool Data Structure

Each pool is identified by its address (e.g., `12QspooeZFsA4d41KtLz4p3e8YyLzPxG4bShsUCBbEgU`). Pool data is stored in the `marketContexts` subcollection, with each document representing a snapshot of the pool's state at a specific time.

### Key Fields in Pool Data Documents

#### Market Cap and Price Data

| Field Name | Type | Description |
|------------|------|-------------|
| `marketCap` | String/Number | Current market capitalization of the pool |
| `athMarketCap` | String/Number | All-time high market capitalization |
| `minMarketCap` | String/Number | Minimum market capitalization observed |
| `currentPrice` | Number | Current price of the token |
| `priceChangePercent` | Number | Percentage change in price (from start) |
| `priceChangeFromStart` | String/Number | Price change amount from the start |

#### Moving Averages and Change Metrics

| Field Name | Type | Description |
|------------|------|-------------|
| `maMarketCap10s` | String/Number | 10-second moving average of market cap |
| `maMarketCap30s` | String/Number | 30-second moving average of market cap |
| `maMarketCap60s` | String/Number | 60-second moving average of market cap |
| `marketCapChange5s` | String/Number | 5-second change in market cap |
| `marketCapChange10s` | String/Number | 10-second change in market cap |
| `marketCapChange30s` | String/Number | 30-second change in market cap |
| `marketCapChange60s` | String/Number | 60-second change in market cap |

#### Holder Metrics

| Field Name | Type | Description |
|------------|------|-------------|
| `holdersCount` | Number | Current number of token holders |
| `initialHoldersCount` | Number | Initial number of token holders |
| `holdersGrowthFromStart` | Number | Growth in holder count from start |
| `holderDelta5s` | Number | 5-second change in holder count |
| `holderDelta10s` | Number | 10-second change in holder count |
| `holderDelta30s` | Number | 30-second change in holder count |
| `holderDelta60s` | Number | 60-second change in holder count |

#### Volume and Trade Metrics

| Field Name | Type | Description |
|------------|------|-------------|
| `buyVolume5s` | Number | Buy volume in the last 5 seconds |
| `buyVolume10s` | Number | Buy volume in the last 10 seconds |
| `netVolume5s` | Number | Net volume (buy-sell) in the last 5 seconds |
| `netVolume10s` | Number | Net volume (buy-sell) in the last 10 seconds |
| `largeBuy5s` | Number | Count of large buys in the last 5 seconds |
| `largeBuy10s` | Number | Count of large buys in the last 10 seconds |
| `superBuy5s` | Number | Count of super buys in the last 5 seconds |
| `superBuy10s` | Number | Count of super buys in the last 10 seconds |
| `bigBuy5s` | Number | Count of big buys in the last 5 seconds |
| `bigBuy10s` | Number | Count of big buys in the last 10 seconds |

#### Trade Details Nested Structure

Some pools have detailed trade information organized in nested objects:

**tradeLast5Seconds**:
```json
{
  "volume": {
    "buy": "X.XX",
    "sell": "X.XX",
    "bot": "0.00000000"
  },
  "tradeCount": {
    "buy": {
      "large": 0,
      "medium": 0,
      "super": 0,
      "small": 0,
      "big": 0
    },
    "bot": 0,
    "sell": {
      "large": 0,
      "medium": 0,
      "super": 0,
      "small": 0,
      "big": 0
    }
  }
}
```

**tradeLast10Seconds**: Similar structure as `tradeLast5Seconds` but for a 10-second window.

#### Other Fields

| Field Name | Type | Description |
|------------|------|-------------|
| `timestamp` | Timestamp | When the data was recorded |
| `poolAddress` | String | Address of the pool |
| `timeFromStart` | Number | Time elapsed since tracking started (in intervals) |
| `originalTimestamp` | Timestamp | Original timestamp when data was first recorded |
| `creationTime` | Timestamp | When the pool was created |

## Alternative Field Structure

Some pools have a different field naming convention, especially for trade data, using underscores instead of camel case:

- `trade_last5Seconds.volume.buy` vs `tradeLast5Seconds.volume.buy` 
- `trade_last10Seconds.tradeCount.buy.medium` vs `tradeLast10Seconds.tradeCount.buy.medium`

## Using the Firebase Data

### Accessing the Data

To access the data, you need to:

1. Initialize Firebase with the correct credentials
2. Access the marketContext collection 
3. Get the pool document by its address
4. Access the marketContexts subcollection
5. Query the documents with appropriate filters

Example code:
```python
# Get the pool document
pool_doc = db.collection("marketContext").document(pool_id)
        
# Get the marketContexts subcollection
contexts_collection = pool_doc.collection("marketContexts")
        
# Fetch documents
contexts = list(contexts_collection.limit(20).stream())
```

### Working with Timestamps

The timestamps in the database are Firestore Timestamp objects. When converting to a DataFrame, you'll need to convert these to Python datetime objects:

```python
# Convert Firestore timestamps to Python datetime
if 'timestamp' in df.columns:
    timestamps = []
    for ts in df['timestamp']:
        if hasattr(ts, 'seconds'):
            timestamps.append(datetime.fromtimestamp(ts.seconds))
        else:
            timestamps.append(ts)
    df['timestamp'] = timestamps
```

## Data Analysis Considerations

When analyzing this data:

1. **Time Series Analysis**: The data is time-series in nature, with multiple snapshots taken at regular intervals.
2. **Market Cap Trends**: Look at the various market cap change metrics to identify trends.
3. **Holder Growth**: Analyze holder growth as an indicator of token popularity.
4. **Trading Volume**: Buy/sell volumes and ratios can indicate market sentiment.
5. **Large Transactions**: Pay attention to `largeBuy5s`, `superBuy5s`, etc. as they may indicate whale activity.

## Common Challenges

1. **Data Consistency**: Fields may have different formats or naming conventions across pools.
2. **Missing Data**: Some pools may have missing fields or incomplete data.
3. **Data Type Conversion**: Many fields are stored as strings but represent numeric values.
4. **Timestamp Handling**: Timestamps need proper conversion to be useful in pandas DataFrames. 