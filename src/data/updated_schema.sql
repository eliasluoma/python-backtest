-- Solana Trading Simulator - SQLite Schema v2
-- This schema is designed for SQLite and defines the database structure for the trading simulator.
-- The schema is separated into tables for pools, market data, and metadata.

-- Enable foreign keys
PRAGMA foreign_keys = ON;

-- Store schema version
PRAGMA user_version = 2;

-- Pools table
-- Stores basic pool information
CREATE TABLE IF NOT EXISTS pools (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    poolAddress TEXT NOT NULL UNIQUE,
    createTimestamp TEXT,
    lastUpdateTimestamp TEXT,
    minTimestamp TEXT,
    maxTimestamp TEXT,
    dataPointsCount INTEGER DEFAULT 0,
    metadata TEXT -- JSON string for additional metadata
);

-- Market data table
-- Stores time-series data for each pool
CREATE TABLE IF NOT EXISTS market_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    poolAddress TEXT NOT NULL,
    timestamp TEXT NOT NULL, -- UTC ISO format string
    
    -- Price and market cap (numeric fields)
    currentPrice REAL,
    marketCap REAL,
    athMarketCap REAL,
    minMarketCap REAL,
    
    -- Market cap moving averages
    maMarketCap10s REAL,
    maMarketCap30s REAL, 
    maMarketCap60s REAL,
    
    -- Market cap changes
    marketCapChange5s REAL,
    marketCapChange10s REAL,
    marketCapChange30s REAL,
    marketCapChange60s REAL,
    
    -- Price changes
    priceChangeFromStart REAL,
    priceChangePercent REAL,
    
    -- Holder metrics
    holdersCount INTEGER,
    initialHoldersCount INTEGER,
    holdersGrowthFromStart INTEGER,
    
    -- Holder change metrics
    holderDelta5s INTEGER,
    holderDelta10s INTEGER,
    holderDelta30s INTEGER,
    holderDelta60s INTEGER,
    
    -- Volume metrics
    buyVolume5s REAL,
    buyVolume10s REAL,
    netVolume5s REAL,
    netVolume10s REAL,
    
    -- Trade counts by size
    bigBuy5s INTEGER,
    bigBuy10s INTEGER,
    largeBuy5s INTEGER,
    largeBuy10s INTEGER,
    superBuy5s INTEGER,
    superBuy10s INTEGER,
    
    -- Time reference 
    timeFromStart INTEGER,
    
    -- Complex data (serialized as JSON)
    tradeLast5Seconds TEXT, -- JSON string for nested trade data
    tradeLast10Seconds TEXT, -- JSON string for nested trade data
    
    -- Additional data not covered by specific columns
    additional_data TEXT, -- JSON string for any other fields
    
    -- Create index and foreign key constraint
    FOREIGN KEY (poolAddress) REFERENCES pools(poolAddress),
    UNIQUE (poolAddress, timestamp)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_market_data_pooladdress ON market_data(poolAddress);
CREATE INDEX IF NOT EXISTS idx_market_data_timestamp ON market_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_market_data_pooladdress_timestamp ON market_data(poolAddress, timestamp);

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

-- Cache metadata table 
-- Tracks when cache was last updated, version info, etc.
CREATE TABLE IF NOT EXISTS cache_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT UNIQUE,
    value TEXT,
    lastUpdated TEXT
);

-- Insert initial metadata
INSERT OR IGNORE INTO cache_metadata (key, value, lastUpdated) 
VALUES ('schema_version', '2', datetime('now'));

INSERT OR IGNORE INTO cache_metadata (key, value, lastUpdated) 
VALUES ('created_at', datetime('now'), datetime('now')); 