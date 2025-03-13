# SQLite Cache for Pool Data

This guide explains the SQLite-based caching system used to store and retrieve pool data efficiently for both Python and TypeScript applications.

## Overview

The cache system uses SQLite as a shared data layer that can be accessed by both the Python simulation code and TypeScript applications. This approach provides:

1. Fast local access to pool data without hitting Firebase limits
2. Reduced latency for simulation and analysis tools
3. Common data format used across language boundaries
4. Efficient storage with proper indexing for time-series data
5. In-memory caching in each language for maximum performance

## Architecture

![Cache Architecture](../assets/cache_architecture.png)

The architecture consists of:

1. **SQLite Database**: The core shared component accessible by both Python and TypeScript
2. **Python Cache Service**: Handles data retrieval, updates, and memory caching in Python
3. **TypeScript Applications**: Read data from the SQLite database directly
4. **In-Memory Cache**: Each language maintains its own in-memory cache for frequently accessed data

## Database Schema

The SQLite database schema is defined in `src/data/schema.sql` and includes:

- **pools**: Metadata about each pool (creation time, data points, time range, etc.)
- **market_data**: The actual time-series data for each pool
- **schema_version**: Tracks the schema version for future upgrades
- **cache_stats**: Statistics about the cache for monitoring

The database includes proper indexes for efficient querying by pool ID, timestamp, and other frequently filtered fields.

## Python Implementation

The Python implementation is in `src/data/cache_service.py` and provides:

```python
# Example usage
from src.data.cache_service import DataCacheService

# Initialize the cache service
cache_service = DataCacheService(db_path="cache/pools.db")

# Get data for a specific pool
pool_data = cache_service.get_pool_data("pool_123")

# Get data for a specific time range
from datetime import datetime, timedelta
start_time = datetime.now() - timedelta(days=7)
end_time = datetime.now()
recent_data = cache_service.get_pool_data("pool_123", min_timestamp=start_time, max_timestamp=end_time)

# Update pool data (e.g., after fetching from Firebase)
cache_service.update_pool_data("pool_123", new_data_df)

# Filter pools by criteria
active_pools = cache_service.filter_pools(min_data_points=100, min_market_cap=1000000)

# Get cache statistics
stats = cache_service.get_cache_stats()
```

The Python cache service includes:

1. **Memory Caching**: Frequently accessed pools are cached in memory
2. **Automatic Column Standardization**: Converts between camelCase and snake_case
3. **JSON Field Handling**: Nested fields are stored as JSON and expanded when retrieved
4. **Batch Processing**: Data is inserted in batches for better performance

## TypeScript Access

In the TypeScript project, you should create a similar cache service. The key requirements are:

1. **Use a SQLite library**: [`better-sqlite3`](https://github.com/JoshuaWise/better-sqlite3) is recommended for Node.js
2. **Create a common configuration**: Ensure both projects use the same file path

Example TypeScript implementation (for the other repository):

```typescript
// Example TypeScript implementation
import Database from 'better-sqlite3';
import { LRUCache } from 'lru-cache';

interface PoolData {
  poolId: string;
  timestamp: Date;
  marketCap: number;
  currentPrice: number;
  // ... other fields
}

class DataCacheService {
  private db: Database.Database;
  private memoryCache: LRUCache<string, PoolData[]>;
  
  constructor(dbPath: string, memoryCacheSize: number = 100) {
    this.db = new Database(dbPath, { readonly: true });
    this.memoryCache = new LRUCache({ max: memoryCacheSize });
  }
  
  getPoolData(poolId: string, minTimestamp?: Date, maxTimestamp?: Date): PoolData[] {
    // Generate cache key
    const cacheKey = `${poolId}_${minTimestamp?.toISOString() || ''}_${maxTimestamp?.toISOString() || ''}`;
    
    // Check memory cache first
    const cachedData = this.memoryCache.get(cacheKey);
    if (cachedData) {
      return cachedData;
    }
    
    // Build query with parameters
    let query = `
      SELECT * FROM market_data
      WHERE pool_id = ?
    `;
    const params: any[] = [poolId];
    
    if (minTimestamp) {
      query += ' AND timestamp >= ?';
      params.push(minTimestamp.toISOString());
    }
    
    if (maxTimestamp) {
      query += ' AND timestamp <= ?';
      params.push(maxTimestamp.toISOString());
    }
    
    query += ' ORDER BY timestamp';
    
    // Execute query
    const stmt = this.db.prepare(query);
    const rows = stmt.all(...params);
    
    // Process results
    const result = rows.map(row => {
      // Parse JSON fields
      let tradeData = {};
      let additionalData = {};
      
      if (row.trade_data) {
        try {
          tradeData = JSON.parse(row.trade_data);
        } catch (e) {
          console.warn(`Invalid JSON in trade_data for pool ${poolId}`);
        }
      }
      
      if (row.additional_data) {
        try {
          additionalData = JSON.parse(row.additional_data);
        } catch (e) {
          console.warn(`Invalid JSON in additional_data for pool ${poolId}`);
        }
      }
      
      // Convert to camelCase and proper types
      return {
        poolId: row.pool_id,
        timestamp: new Date(row.timestamp),
        marketCap: row.market_cap,
        currentPrice: row.current_price,
        lastPrice: row.last_price,
        // ... other fields
        ...this.flattenObject(tradeData, 'trade_'),
        ...additionalData
      };
    });
    
    // Cache result
    this.memoryCache.set(cacheKey, result);
    
    return result;
  }
  
  // Helper to flatten nested objects with prefix
  private flattenObject(obj: any, prefix: string = ''): any {
    const result: any = {};
    
    for (const key in obj) {
      if (typeof obj[key] === 'object' && obj[key] !== null) {
        const nestedObj = this.flattenObject(obj[key], `${prefix}${key}.`);
        Object.assign(result, nestedObj);
      } else {
        result[`${prefix}${key}`] = obj[key];
      }
    }
    
    return result;
  }
  
  // Other methods (getPoolIds, filterPools, etc.)
}
```

## Cache Management

### Updating the Cache

The SQLite database should be updated from the Python side:

```python
# Example of updating the cache from Firebase
from src.data.firebase_service import FirebaseService
from src.data.cache_service import DataCacheService

def update_cache():
    firebase = FirebaseService()
    cache = DataCacheService("cache/pools.db")
    
    # Get pools needing update
    pools = firebase.get_available_pools(limit=100)
    
    for pool_id in pools:
        # Fetch from Firebase
        pool_data = firebase.fetch_pool_data(pool_id)
        
        # Update cache
        if not pool_data.empty:
            cache.update_pool_data(pool_id, pool_data)
```

### CLI Commands

The Python project includes CLI commands for cache management:

```bash
# Update cache with latest data
python -m src.cli cache update --all

# Update specific pools
python -m src.cli cache update --pools pool1 pool2 pool3

# Clear old data (older than 30 days)
python -m src.cli cache clear --older-than 30

# Check cache status
python -m src.cli cache status
```

### Synchronization Between Projects

Since TypeScript and Python applications access the same SQLite file:

1. Avoid writing to the database from TypeScript (read-only access)
2. Use the Python CLI to update the cache before running TypeScript applications
3. Use a timestamp file to check if the cache is fresh:

```typescript
// TypeScript code to check cache freshness
import * as fs from 'fs';

async function isCacheFresh(): Promise<boolean> {
  try {
    const lastUpdateText = await fs.promises.readFile('cache/last_update.txt', 'utf8');
    const lastUpdate = new Date(lastUpdateText);
    const now = new Date();
    
    // Cache is considered fresh if updated within the last hour
    const oneHour = 60 * 60 * 1000;
    return (now.getTime() - lastUpdate.getTime()) < oneHour;
  } catch (error) {
    console.warn('Could not read cache timestamp file');
    return false;
  }
}
```

## Cross-Platform Considerations

1. **File Paths**: Use relative paths or configuration to ensure both applications use the same database file
2. **Schema Changes**: Use the schema_version table to handle schema migrations
3. **File Locking**: SQLite handles read concurrency well, but be careful with simultaneous writes
4. **Data Types**: Ensure consistent handling of timestamps, numeric types, and JSON data

## Performance Optimization

To get the best performance:

1. **Preload Common Pools**: Load frequently used pools into memory at startup
2. **Index Properly**: The database includes indexes on pool_id, timestamp, and other fields
3. **Batch Operations**: Insert data in batches (as done in the Python implementation)
4. **Query Only What You Need**: Filter data by time range if you don't need the full history
5. **Periodically Vacuum**: Run `VACUUM` command after deleting large amounts of data

## Troubleshooting

Common issues and solutions:

1. **Database Locked**: This typically happens when trying to write while another process is writing. Add retry logic or implement proper synchronization.
2. **Missing Data**: Ensure the cache is updated before use. Check if data exists in Firebase.
3. **Performance Issues**: Check your queries are using indexes. Consider adding more indexes if needed.
4. **Disk Space**: Monitor cache size with `cache_stats` table. Clear old data regularly.

## Next Steps

1. Implement the TypeScript cache service in your TypeScript project
2. Create automation to keep the cache updated
3. Monitor cache performance and size
4. Add metrics to track cache hit rates 