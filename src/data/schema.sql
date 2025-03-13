-- Solana Trading Simulator - SQLite Schema v2
-- This schema is designed for SQLite and defines the database structure for the trading simulator.
-- The schema is separated into tables for pools, market data, and metadata.

-- Enable foreign keys
PRAGMA foreign_keys = ON;

-- Store schema version
PRAGMA user_version = 2;

-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_version (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    version INTEGER NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

-- Pools table
-- Stores basic pool information
CREATE TABLE IF NOT EXISTS pools (
    poolAddress TEXT PRIMARY KEY,
    creationTime TIMESTAMP NOT NULL,
    lastUpdated TIMESTAMP NOT NULL,
    dataPoints INTEGER NOT NULL,
    minTimestamp TIMESTAMP NOT NULL,
    maxTimestamp TIMESTAMP NOT NULL,
    metadata TEXT -- JSON string with additional metadata
);

-- Market data
CREATE TABLE IF NOT EXISTS market_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    poolAddress TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    
    -- Market Cap Fields - 10 fields - Muutetaan string-tyyppisiksi
    marketCap TEXT,             -- String-tyyppinen Firebasessa
    athMarketCap TEXT,          -- String-tyyppinen Firebasessa
    minMarketCap TEXT,          -- String-tyyppinen Firebasessa
    marketCapChange5s TEXT,     -- String-tyyppinen Firebasessa
    marketCapChange10s TEXT,    -- String-tyyppinen Firebasessa
    marketCapChange30s TEXT,    -- String-tyyppinen Firebasessa
    marketCapChange60s TEXT,    -- String-tyyppinen Firebasessa
    maMarketCap10s TEXT,        -- String-tyyppinen Firebasessa
    maMarketCap30s TEXT,        -- String-tyyppinen Firebasessa
    maMarketCap60s TEXT,        -- String-tyyppinen Firebasessa
    
    -- Price Fields - 3 fields
    currentPrice TEXT,           -- String-tyyppinen Firebasessa
    priceChangePercent REAL,     -- Number-tyyppinen Firebasessa
    priceChangeFromStart TEXT,   -- String-tyyppinen Firebasessa
    
    -- Holder Fields - 8 fields
    holdersCount INTEGER,
    initialHoldersCount INTEGER,
    holdersGrowthFromStart REAL,
    holderDelta5s INTEGER,
    holderDelta10s INTEGER,
    holderDelta30s INTEGER,
    holderDelta60s INTEGER,
    
    -- Volume Fields - 4 fields
    buyVolume5s REAL,
    buyVolume10s REAL,
    netVolume5s REAL,
    netVolume10s REAL,
    
    -- Buy Classification Fields - 6 fields
    largeBuy5s INTEGER,
    largeBuy10s INTEGER,
    bigBuy5s INTEGER,
    bigBuy10s INTEGER,
    superBuy5s INTEGER,
    superBuy10s INTEGER,
    
    -- Trade Data - 5s - 14 fields
    trade_last5Seconds_volume_buy TEXT,      -- String-tyyppinen Firebasessa
    trade_last5Seconds_volume_sell TEXT,     -- String-tyyppinen Firebasessa 
    trade_last5Seconds_volume_bot TEXT,      -- String-tyyppinen Firebasessa
    trade_last5Seconds_tradeCount_buy_small INTEGER,
    trade_last5Seconds_tradeCount_buy_medium INTEGER,
    trade_last5Seconds_tradeCount_buy_large INTEGER,
    trade_last5Seconds_tradeCount_buy_big INTEGER,
    trade_last5Seconds_tradeCount_buy_super INTEGER,
    trade_last5Seconds_tradeCount_sell_small INTEGER,
    trade_last5Seconds_tradeCount_sell_medium INTEGER,
    trade_last5Seconds_tradeCount_sell_large INTEGER,
    trade_last5Seconds_tradeCount_sell_big INTEGER,
    trade_last5Seconds_tradeCount_sell_super INTEGER,
    trade_last5Seconds_tradeCount_bot INTEGER,
    
    -- Trade Data - 10s - 14 fields
    trade_last10Seconds_volume_buy TEXT,     -- String-tyyppinen Firebasessa
    trade_last10Seconds_volume_sell TEXT,    -- String-tyyppinen Firebasessa
    trade_last10Seconds_volume_bot TEXT,     -- String-tyyppinen Firebasessa
    trade_last10Seconds_tradeCount_buy_medium INTEGER,
    trade_last10Seconds_tradeCount_buy_large INTEGER,
    trade_last10Seconds_tradeCount_buy_big INTEGER,
    trade_last10Seconds_tradeCount_buy_super INTEGER,
    trade_last10Seconds_tradeCount_buy_small INTEGER,
    trade_last10Seconds_tradeCount_sell_small INTEGER,
    trade_last10Seconds_tradeCount_sell_medium INTEGER,
    trade_last10Seconds_tradeCount_sell_large INTEGER,
    trade_last10Seconds_tradeCount_sell_big INTEGER,
    trade_last10Seconds_tradeCount_sell_super INTEGER,
    trade_last10Seconds_tradeCount_bot INTEGER,
    
    -- Metadata - 2 fields
    timeFromStart INTEGER,
    totalVolume REAL, -- Extra field not in main REQUIRED_FIELDS list but needed
    
    -- Additional fields as JSON for future flexibility
    additional_data TEXT,  -- JSON containing any other fields that may be added in the future
    
    -- Constraints
    FOREIGN KEY (poolAddress) REFERENCES pools(poolAddress) ON DELETE CASCADE,
    UNIQUE(poolAddress, timestamp)
);

-- Analytics table (for storing calculated metrics/statistics)
CREATE TABLE IF NOT EXISTS analytics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    poolAddress TEXT NOT NULL,
    calculationTimestamp TEXT NOT NULL,
    metricName TEXT NOT NULL,
    metricValue REAL,
    metadata TEXT, -- JSON string for additional context
    FOREIGN KEY (poolAddress) REFERENCES pools(poolAddress),
    UNIQUE (poolAddress, metricName, calculationTimestamp)
);

-- Indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_market_data_pool_id ON market_data(poolAddress);
CREATE INDEX IF NOT EXISTS idx_market_data_timestamp ON market_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_market_data_pool_timestamp ON market_data(poolAddress, timestamp);
CREATE INDEX IF NOT EXISTS idx_market_data_market_cap ON market_data(marketCap);
CREATE INDEX IF NOT EXISTS idx_market_data_holders_count ON market_data(holdersCount);

-- Cache metadata table (replaces the older cache_stats table)
CREATE TABLE IF NOT EXISTS cache_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT UNIQUE,
    value TEXT,
    lastUpdated TEXT
);

-- Initial schema version
INSERT OR IGNORE INTO schema_version (id, version, updated_at) 
VALUES (1, 2, datetime('now'));

-- Initial cache metadata
INSERT OR IGNORE INTO cache_metadata (key, value, lastUpdated) 
VALUES ('schema_version', '2', datetime('now'));

INSERT OR IGNORE INTO cache_metadata (key, value, lastUpdated) 
VALUES ('created_at', datetime('now'), datetime('now'));

-- For backward compatibility
CREATE TABLE IF NOT EXISTS cache_stats (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_global_update TIMESTAMP,
    total_pools INTEGER,
    total_data_points INTEGER,
    cache_size_bytes INTEGER
);

-- Initial cache stats (if table doesn't exist yet)
INSERT OR IGNORE INTO cache_stats (id, last_global_update, total_pools, total_data_points, cache_size_bytes)
VALUES (1, datetime('now'), 0, 0, 0); 