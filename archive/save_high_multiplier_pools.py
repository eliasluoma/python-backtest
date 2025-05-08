#!/usr/bin/env python3
"""
Save High Multiplier Pools

Saves pools with multiplier above threshold to a text file
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
                           min_multiplier: float = 7.0, max_time_point: int = 600) -> pd.DataFrame:
    """
    Get data for all pools that qualify as "Definitely Buy" (reach at least min_multiplier returns).
    
    Args:
        db_path: Path to the SQLite database
        min_marketcap: Minimum market cap to be considered at 10s
        min_multiplier: Minimum multiplier to qualify as "Definitely Buy"
        max_time_point: Maximum time point to be analyzed
        
    Returns:
        DataFrame containing "Definitely Buy" pools data
    """
    try:
        conn = sqlite3.connect(db_path)
        
        # Get all pools
        pool_query = "SELECT poolAddress FROM pools"
        pool_ids = pd.read_sql_query(pool_query, conn)['poolAddress'].tolist()
            
        definitely_buy_pools_data = []
        
        logger.info(f"Processing {len(pool_ids)} pools...")
        
        # Use a more efficient method to process pools in batches
        batch_size = 100
        for i in range(0, len(pool_ids), batch_size):
            batch_pool_ids = pool_ids[i:i+batch_size]
            placeholders = ','.join(['?'] * len(batch_pool_ids))
            
            # Get market data for this batch of pools
            market_data_query = f"""
            SELECT * FROM market_data 
            WHERE poolAddress IN ({placeholders})
            ORDER BY poolAddress, timestamp ASC
            """
            
            df = pd.read_sql_query(market_data_query, conn, params=batch_pool_ids)
            
            # Convert numeric columns to float
            numeric_columns = ['marketCap', 'athMarketCap', 'minMarketCap', 'price', 'volume']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Process each pool in the batch
            for pool_id in batch_pool_ids:
                pool_data = df[df['poolAddress'] == pool_id]
                
                # Skip pools with insufficient data
                if len(pool_data) < max_time_point:
                    continue
                    
                # Get market cap at 10s
                if len(pool_data) <= 9:  # Index 9 corresponds to 10s (0-indexed)
                    continue
                    
                # Skip pools with market cap below threshold at 10s
                initial_marketcap = float(pool_data.iloc[9]['marketCap'])
                if initial_marketcap < min_marketcap:
                    continue
                
                # Calculate ATH market cap
                ath_marketcap = pool_data['marketCap'].max()
                
                # Calculate final multiplier (from 10s to ATH)
                final_multiplier = ath_marketcap / initial_marketcap
                
                # Only keep pools that qualify as "Definitely Buy"
                if final_multiplier < min_multiplier:
                    continue
                    
                # Add pool data
                pool_data_dict = {
                    'pool_id': pool_id,
                    'initial_marketcap': initial_marketcap,
                    'ath_marketcap': ath_marketcap,
                    'final_multiplier': final_multiplier
                }
                
                definitely_buy_pools_data.append(pool_data_dict)
            
            logger.info(f"Processed {min(i+batch_size, len(pool_ids))}/{len(pool_ids)} pools")
        
        conn.close()
        
        return pd.DataFrame(definitely_buy_pools_data)
    
    except Exception as e:
        logger.error(f"Error fetching pool data: {e}")
        return pd.DataFrame()

def save_pools_to_file(pools_df: pd.DataFrame, threshold: float, output_file: str):
    """
    Save pools with multiplier above threshold to a text file
    
    Args:
        pools_df: DataFrame with pool data
        threshold: Minimum multiplier threshold
        output_file: Output file path
    """
    # Filter pools based on threshold
    high_multiplier_pools = pools_df[pools_df['final_multiplier'] >= threshold]
    
    # Sort by multiplier (descending)
    sorted_pools = high_multiplier_pools.sort_values(by='final_multiplier', ascending=False)
    
    logger.info(f"Found {len(sorted_pools)} pools with multiplier >= {threshold:.2f}x")
    
    # Save to file
    with open(output_file, 'w') as f:
        f.write(f"# Pools with multiplier >= {threshold}x\n")
        f.write("# Format: rank. pool_address, multiplier, initial_marketcap, ath_marketcap\n\n")
        
        for i, (_, pool) in enumerate(sorted_pools.iterrows()):
            f.write(f"{i+1}. {pool['pool_id']}, {pool['final_multiplier']:.2f}x, ${pool['initial_marketcap']:.2f}, ${pool['ath_marketcap']:.2f}\n")
    
    logger.info(f"Saved {len(sorted_pools)} pools to {output_file}")

def main():
    """Main function to save high multiplier pools"""
    parser = argparse.ArgumentParser(description='Save pools with high multiplier to a file')
    parser.add_argument('--db-path', type=str, default='cache/pools.db',
                       help='Path to the SQLite database')
    parser.add_argument('--min-marketcap', type=float, default=40000,
                       help='Minimum market cap at first time point (10s)')
    parser.add_argument('--min-multiplier', type=float, default=7.0,
                       help='Minimum multiplier to qualify as a "Definitely Buy" pool')
    parser.add_argument('--threshold', type=float, default=1000.0,
                       help='Threshold for high multiplier pools')
    parser.add_argument('--output-file', type=str, default='high_multiplier_pools.txt',
                       help='Output file path')
    
    args = parser.parse_args()
    
    logger.info(f"Starting analysis using database: {args.db_path}")
    logger.info(f"Looking for pools with multiplier >= {args.threshold:.2f}x")
    
    # Get "Definitely Buy" pools data
    df = get_definitely_buy_pools(
        db_path=args.db_path,
        min_marketcap=args.min_marketcap,
        min_multiplier=args.min_multiplier
    )
    
    if len(df) == 0:
        logger.error("No 'Definitely Buy' pools found with the given criteria.")
        return
    
    # Save pools to file
    save_pools_to_file(
        pools_df=df,
        threshold=args.threshold,
        output_file=args.output_file
    )
    
    logger.info("Done!")

if __name__ == "__main__":
    main() 