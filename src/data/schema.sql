-- SQLite Schema for Pool Data Caching
-- This schema defines the structure for caching pool data
-- across Python and TypeScript applications

-- Enable foreign keys
PRAGMA foreign_keys = ON;

-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_version (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    version INTEGER NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

-- Pools metadata
CREATE TABLE IF NOT EXISTS pools (
    pool_id TEXT PRIMARY KEY,
    creation_time TIMESTAMP NOT NULL,
    last_updated TIMESTAMP NOT NULL,
    data_points INTEGER NOT NULL,
    min_timestamp TIMESTAMP NOT NULL,
    max_timestamp TIMESTAMP NOT NULL,
    metadata TEXT -- JSON string with additional metadata
);

-- Market data
CREATE TABLE IF NOT EXISTS market_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pool_id TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    
    -- Core market metrics
    market_cap REAL,
    current_price REAL,
    last_price REAL,
    
    -- Market cap metrics
    ath_market_cap REAL,
    min_market_cap REAL,
    ma_market_cap_10s REAL,
    ma_market_cap_30s REAL,
    ma_market_cap_60s REAL,
    
    -- Market cap changes
    market_cap_change_5s REAL,
    market_cap_change_10s REAL,
    market_cap_change_30s REAL,
    market_cap_change_60s REAL,
    
    -- Price metrics
    price_change_percent REAL,
    price_change_from_start REAL,
    
    -- Holder metrics
    holders_count INTEGER,
    initial_holders_count INTEGER,
    holders_growth_from_start REAL,
    holder_delta_5s INTEGER,
    holder_delta_10s INTEGER,
    holder_delta_30s INTEGER,
    holder_delta_60s INTEGER,
    
    -- Volume metrics
    buy_volume_5s REAL,
    buy_volume_10s REAL,
    net_volume_5s REAL,
    net_volume_10s REAL,
    total_volume REAL,
    
    -- Buy classification metrics
    large_buy_5s INTEGER,
    large_buy_10s INTEGER,
    big_buy_5s INTEGER,
    big_buy_10s INTEGER,
    super_buy_5s INTEGER,
    super_buy_10s INTEGER,
    
    -- Metadata
    time_from_start INTEGER,
    
    -- Trade data (stored as JSON to handle nested structure)
    trade_data TEXT,  -- JSON containing all trade_last5Seconds and trade_last10Seconds data
    
    -- Additional fields as JSON for flexible schema
    additional_data TEXT,  -- JSON containing any other fields
    
    -- Constraints
    FOREIGN KEY (pool_id) REFERENCES pools(pool_id) ON DELETE CASCADE,
    UNIQUE(pool_id, timestamp)
);

-- Indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_market_data_pool_id ON market_data(pool_id);
CREATE INDEX IF NOT EXISTS idx_market_data_timestamp ON market_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_market_data_pool_timestamp ON market_data(pool_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_market_data_market_cap ON market_data(market_cap);
CREATE INDEX IF NOT EXISTS idx_market_data_holders_count ON market_data(holders_count);

-- Cache management table
CREATE TABLE IF NOT EXISTS cache_stats (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_global_update TIMESTAMP,
    total_pools INTEGER,
    total_data_points INTEGER,
    cache_size_bytes INTEGER
);

-- Initial schema version
INSERT OR IGNORE INTO schema_version (id, version, updated_at) 
VALUES (1, 1, datetime('now'));

-- Initial cache stats
INSERT OR IGNORE INTO cache_stats (id, last_global_update, total_pools, total_data_points, cache_size_bytes)
VALUES (1, datetime('now'), 0, 0, 0); 