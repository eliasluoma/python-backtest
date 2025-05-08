#!/usr/bin/env python3
"""
Analyze pools with more realistic multipliers by filtering out extreme spikes
"""

import sqlite3
import pandas as pd
import numpy as np
import logging
import argparse
from typing import List, Dict, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_realistic_multipliers(db_path: str, min_marketcap: float = 40000, 
                             min_multiplier: float = 10.0,
                             max_spike_percent: float = 1000.0) -> pd.DataFrame:
    """
    Get data for pools with realistic multipliers by filtering out extreme spikes.
    
    Args:
        db_path: Path to the SQLite database
        min_marketcap: Minimum market cap to be considered at 10s
        min_multiplier: Minimum multiplier to qualify
        max_spike_percent: Maximum allowed percent increase between consecutive points
        
    Returns:
        DataFrame containing pool data with realistic multipliers
    """
    try:
        conn = sqlite3.connect(db_path)
        
        # Get all pools
        pool_query = "SELECT poolAddress FROM pools"
        pool_ids = pd.read_sql_query(pool_query, conn)['poolAddress'].tolist()
            
        pools_data = []
        
        logger.info(f"Analyzing {len(pool_ids)} pools for realistic multipliers...")
        
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
            
            # Skip pools with insufficient data
            if len(df) < 10:  # Need at least 10 seconds of data
                continue
                
            # Convert numeric columns to float
            df['marketCap'] = pd.to_numeric(df['marketCap'], errors='coerce')
            
            # Calculate percent changes
            df['pct_change'] = df['marketCap'].pct_change() * 100
            
            # Get market cap at 10s
            initial_marketcap = float(df.iloc[9]['marketCap'])
            if initial_marketcap < min_marketcap:
                continue
            
            # Filter out unrealistic spikes
            # First identify rows where the percent change exceeds the threshold
            spike_indices = df[abs(df['pct_change']) > max_spike_percent].index.tolist()
            
            # Create a copy of market cap data
            clean_market_cap = df['marketCap'].copy()
            
            # For each spike, replace with the previous value
            for idx in spike_indices:
                if idx > 0:  # Skip the first row
                    clean_market_cap.iloc[idx] = clean_market_cap.iloc[idx-1]
            
            # Calculate ATH market cap from cleaned data
            ath_marketcap = clean_market_cap.max()
            
            # Calculate final multiplier (from 10s to ATH)
            final_multiplier = ath_marketcap / initial_marketcap
            
            # Track how much cleaning occurred
            raw_ath = df['marketCap'].max()
            raw_multiplier = raw_ath / initial_marketcap
            
            # Only keep pools with acceptable multiplier
            if final_multiplier < min_multiplier:
                continue
                
            # Add pool data
            pools_data.append({
                'pool_id': pool_id,
                'initial_marketcap': initial_marketcap,
                'ath_marketcap': ath_marketcap,
                'final_multiplier': final_multiplier,
                'raw_ath': raw_ath,
                'raw_multiplier': raw_multiplier,
                'spikes_cleaned': len(spike_indices)
            })
        
        conn.close()
        
        # Convert to DataFrame and sort by multiplier
        pools_df = pd.DataFrame(pools_data)
        if len(pools_df) > 0:
            pools_df = pools_df.sort_values(by='final_multiplier', ascending=False)
            
        logger.info(f"Found {len(pools_df)} pools with realistic multipliers >= {min_multiplier}x")
        return pools_df
    
    except Exception as e:
        logger.error(f"Error analyzing pools: {e}")
        return pd.DataFrame()

def analyze_and_save_results(pools_df: pd.DataFrame, output_file: str):
    """
    Analyze pool data with realistic multipliers and save to file
    
    Args:
        pools_df: DataFrame containing pool data with realistic multipliers
        output_file: Output file path
    """
    if len(pools_df) == 0:
        logger.warning("No pools to analyze")
        return
    
    with open(output_file, 'w') as f:
        f.write(f"# Realistic Multiplier Pools Analysis\n")
        f.write(f"# Total pools found: {len(pools_df)}\n\n")
        
        f.write("## Summary Statistics\n")
        f.write(f"- Average realistic multiplier: {pools_df['final_multiplier'].mean():.2f}x\n")
        f.write(f"- Median realistic multiplier: {pools_df['final_multiplier'].median():.2f}x\n")
        f.write(f"- Max realistic multiplier: {pools_df['final_multiplier'].max():.2f}x\n")
        f.write(f"- Pools with cleaned spikes: {len(pools_df[pools_df['spikes_cleaned'] > 0])} ({pools_df[pools_df['spikes_cleaned'] > 0].shape[0]/pools_df.shape[0]*100:.1f}%)\n")
        f.write(f"- Average number of spikes cleaned: {pools_df['spikes_cleaned'].mean():.1f}\n\n")
        
        f.write("## Pool Details\n")
        for i, (_, pool) in enumerate(pools_df.iterrows()):
            pool_id = pool['pool_id']
            initial_mc = pool['initial_marketcap']
            ath_mc = pool['ath_marketcap']
            multiplier = pool['final_multiplier']
            raw_ath = pool['raw_ath']
            raw_multiplier = pool['raw_multiplier']
            spikes_cleaned = pool['spikes_cleaned']
            
            f.write(f"### {i+1}. Pool: {pool_id}\n")
            f.write(f"   Initial Market Cap: ${initial_mc:.2f}\n")
            f.write(f"   ATH Market Cap (Cleaned): ${ath_mc:.2f}\n")
            f.write(f"   Realistic Multiplier: {multiplier:.2f}x\n")
            
            if spikes_cleaned > 0:
                f.write(f"   Raw ATH Market Cap: ${raw_ath:.2f}\n")
                f.write(f"   Raw Multiplier: {raw_multiplier:.2f}x\n")
                f.write(f"   Spikes Cleaned: {spikes_cleaned}\n")
            
            f.write("\n")
            
            # Log progress
            if (i+1) % 10 == 0 or (i+1) == len(pools_df):
                logger.info(f"Processed {i+1}/{len(pools_df)} pools")
    
    logger.info(f"Analysis complete. Results saved to {output_file}")

def main():
    """Main function to analyze pools with realistic multipliers"""
    parser = argparse.ArgumentParser(description='Analyze pools with realistic multipliers')
    parser.add_argument('--db-path', type=str, default='cache/pools.db',
                       help='Path to the SQLite database')
    parser.add_argument('--min-marketcap', type=float, default=40000,
                       help='Minimum market cap at first time point (10s)')
    parser.add_argument('--min-multiplier', type=float, default=10.0,
                       help='Minimum multiplier to qualify')
    parser.add_argument('--max-spike-percent', type=float, default=1000.0,
                       help='Maximum allowed percent increase between consecutive points')
    parser.add_argument('--output-file', type=str, default='realistic_multipliers_analysis.txt',
                       help='Output file path')
    
    args = parser.parse_args()
    
    logger.info(f"Starting analysis using database: {args.db_path}")
    logger.info(f"Parameters: min_marketcap=${args.min_marketcap}, min_multiplier={args.min_multiplier}x, max_spike_percent={args.max_spike_percent}%")
    
    # Get pools with realistic multipliers
    pools_df = get_realistic_multipliers(
        db_path=args.db_path,
        min_marketcap=args.min_marketcap,
        min_multiplier=args.min_multiplier,
        max_spike_percent=args.max_spike_percent
    )
    
    if len(pools_df) == 0:
        logger.error("No pools with realistic multipliers found.")
        return
    
    # Analyze and save results
    analyze_and_save_results(
        pools_df=pools_df,
        output_file=args.output_file
    )

if __name__ == "__main__":
    main() 