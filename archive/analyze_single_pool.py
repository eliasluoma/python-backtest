#!/usr/bin/env python3
"""
Analyze a single pool's market cap data in detail
"""

import sqlite3
import pandas as pd
import logging
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def analyze_pool(db_path: str, pool_id: str):
    """
    Analyze a single pool's market cap data in detail
    
    Args:
        db_path: Path to the SQLite database
        pool_id: Pool address to analyze
    """
    try:
        conn = sqlite3.connect(db_path)
        
        # Get table schema
        schema_query = "PRAGMA table_info(market_data)"
        columns = pd.read_sql_query(schema_query, conn)
        logger.info(f"Market data table columns: {', '.join(columns['name'].tolist())}")
        
        # Get market data for this pool
        market_data_query = f"""
        SELECT * FROM market_data 
        WHERE poolAddress = '{pool_id}'
        ORDER BY timestamp ASC
        """
        
        df = pd.read_sql_query(market_data_query, conn)
        
        if df.empty:
            logger.warning(f"No data found for pool {pool_id}")
            return
            
        # Basic stats
        logger.info(f"Found {len(df)} data points for pool {pool_id}")
        logger.info(f"First timestamp: {df['timestamp'].iloc[0]}")
        logger.info(f"Last timestamp: {df['timestamp'].iloc[-1]}")
        
        # Get numeric columns
        numeric_columns = ['marketCap', 'athMarketCap', 'price', 'volume']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Market cap analysis
        logger.info(f"Market cap range: ${df['marketCap'].min():.2f} to ${df['marketCap'].max():.2f}")
        
        # Calculate multiplier from initial market cap (at 10s)
        if len(df) >= 10:
            initial_marketcap = df.iloc[9]['marketCap']
            logger.info(f"Initial market cap (10s): ${initial_marketcap:.2f}")
            
            # Find max market cap
            max_marketcap_row = df.loc[df['marketCap'].idxmax()]
            max_marketcap = max_marketcap_row['marketCap']
            max_marketcap_time = max_marketcap_row['timestamp']
            multiplier = max_marketcap / initial_marketcap
            logger.info(f"Max market cap: ${max_marketcap:.2f} at {max_marketcap_time} (x{multiplier:.2f})")
            
            # Show top 10 market cap values
            logger.info("Top 10 market cap values:")
            top_marketcaps = df.sort_values(by='marketCap', ascending=False).head(10)
            for i, (_, row) in enumerate(top_marketcaps.iterrows()):
                market_cap = row['marketCap']
                timestamp = row['timestamp']
                multiplier_at_point = market_cap / initial_marketcap
                logger.info(f"  {i+1}. ${market_cap:.2f} at {timestamp} (x{multiplier_at_point:.2f})")
        else:
            logger.warning(f"Not enough data points for pool {pool_id}")
            
    except Exception as e:
        logger.error(f"Error analyzing pool {pool_id}: {e}")

def main():
    """Main function to analyze a single pool"""
    parser = argparse.ArgumentParser(description='Analyze a single pool in detail')
    parser.add_argument('--db-path', type=str, default='cache/pools.db',
                      help='Path to the SQLite database')
    parser.add_argument('--pool-id', type=str, required=True,
                      help='Pool address to analyze')
    
    args = parser.parse_args()
    
    logger.info(f"Analyzing pool {args.pool_id} using database {args.db_path}")
    analyze_pool(args.db_path, args.pool_id)

if __name__ == "__main__":
    main() 