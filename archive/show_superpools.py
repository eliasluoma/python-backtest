#!/usr/bin/env python3
"""
Show Superpools

Prints the addresses of superpools (pools with extremely high multipliers)
"""

import sqlite3
import pandas as pd
import numpy as np
import logging
import argparse
from typing import List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_definitely_buy_pools(db_path: str, min_marketcap: float = 40000, 
                           min_multiplier: float = 7.0, max_time_point: int = 600,
                           excluded_pools: List[str] = None) -> pd.DataFrame:
    """
    Get data for all pools that qualify as "Definitely Buy" (reach at least min_multiplier returns).
    
    Args:
        db_path: Path to the SQLite database
        min_marketcap: Minimum market cap to be considered at 10s
        min_multiplier: Minimum multiplier to qualify as "Definitely Buy"
        max_time_point: Maximum time point to be analyzed
        excluded_pools: List of pool addresses to exclude from analysis
        
    Returns:
        DataFrame containing "Definitely Buy" pools data
    """
    try:
        conn = sqlite3.connect(db_path)
        
        # Get all pools
        pool_query = "SELECT poolAddress FROM pools"
        pool_ids = pd.read_sql_query(pool_query, conn)['poolAddress'].tolist()
        
        # Filter out excluded pools
        if excluded_pools:
            original_count = len(pool_ids)
            pool_ids = [pid for pid in pool_ids if pid not in excluded_pools]
            excluded_count = original_count - len(pool_ids)
            logger.info(f"Excluded {excluded_count} pools from analysis")
            
        definitely_buy_pools_data = []
        
        for idx, pool_id in enumerate(pool_ids):
            if (idx + 1) % 100 == 0:
                logger.info(f"Processing pool {idx+1}/{len(pool_ids)}")
                
            # Get market data for this pool
            market_data_query = f"""
            SELECT * FROM market_data 
            WHERE poolAddress = '{pool_id}'
            ORDER BY timestamp ASC
            """
            
            df = pd.read_sql_query(market_data_query, conn)
            
            # Convert numeric columns to float
            numeric_columns = ['marketCap', 'athMarketCap', 'minMarketCap', 'price', 'volume']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Skip pools with insufficient data
            if len(df) < max_time_point:
                continue
                
            # Get market cap at 10s
            if len(df) <= 9:  # Index 9 corresponds to 10s (0-indexed)
                continue
                
            # Skip pools with market cap below threshold at 10s
            initial_marketcap = float(df.iloc[9]['marketCap'])
            if initial_marketcap < min_marketcap:
                continue
            
            # Calculate ATH market cap
            ath_marketcap = df['marketCap'].max()
            
            # Calculate final multiplier (from 10s to ATH)
            final_multiplier = ath_marketcap / initial_marketcap
            
            # Only keep pools that qualify as "Definitely Buy"
            if final_multiplier < min_multiplier:
                continue
                
            # Add pool data
            pool_data = {
                'pool_id': pool_id,
                'initial_marketcap': initial_marketcap,
                'ath_marketcap': ath_marketcap,
                'final_multiplier': final_multiplier
            }
            
            definitely_buy_pools_data.append(pool_data)
        
        conn.close()
        
        return pd.DataFrame(definitely_buy_pools_data)
    
    except Exception as e:
        logger.error(f"Error fetching pool data: {e}")
        return pd.DataFrame()

def main():
    """Main function to identify superpools"""
    parser = argparse.ArgumentParser(description='Identify superpools')
    parser.add_argument('--db-path', type=str, default='cache/pools.db',
                       help='Path to the SQLite database')
    parser.add_argument('--min-marketcap', type=float, default=40000,
                       help='Minimum market cap at first time point (10s)')
    parser.add_argument('--min-multiplier', type=float, default=7.0,
                       help='Minimum multiplier to qualify as a "Definitely Buy" pool')
    parser.add_argument('--superpool-threshold', type=float, default=140000000.0,
                       help='Threshold for superpools (final multiplier)')
    parser.add_argument('--excluded-pools', type=str, nargs='+',
                       help='List of pool addresses to exclude from analysis')
    
    args = parser.parse_args()
    
    logger.info(f"Starting analysis using database: {args.db_path}")
    
    # Get "Definitely Buy" pools data
    df = get_definitely_buy_pools(
        db_path=args.db_path,
        min_marketcap=args.min_marketcap,
        min_multiplier=args.min_multiplier,
        excluded_pools=args.excluded_pools
    )
    
    if len(df) == 0:
        logger.error("No 'Definitely Buy' pools found with the given criteria.")
        return
    
    # Identify superpools
    superpools = df[df['final_multiplier'] >= args.superpool_threshold]
    
    logger.info(f"\nFound {len(superpools)} superpools with multiplier >= {args.superpool_threshold:.2f}x:")
    
    # Show superpools sorted by multiplier (descending)
    superpools_sorted = superpools.sort_values(by='final_multiplier', ascending=False)
    
    for i, (_, pool) in enumerate(superpools_sorted.iterrows()):
        logger.info(f"{i+1}. Pool address: {pool['pool_id']}")
        logger.info(f"   Final multiplier: {pool['final_multiplier']:.2f}x")
        logger.info(f"   Initial market cap: ${pool['initial_marketcap']:.2f}")
        logger.info(f"   ATH market cap: ${pool['ath_marketcap']:.2f}")
        logger.info(f"   URL: https://solscan.io/token/{pool['pool_id']}\n")

if __name__ == "__main__":
    main() 