#!/usr/bin/env python3
"""
Pool MarketCap Analysis

Analyzes pools with high multipliers and shows top market cap values
"""

import sqlite3
import pandas as pd
import numpy as np
import logging
import argparse
import multiprocessing
import os
from functools import partial
from typing import List, Dict, Tuple, Any
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def filter_absolute_spikes(df: pd.DataFrame, max_multiplier: float = 80.0) -> pd.DataFrame:
    """
    Filter out market cap values that exceed max_multiplier times the reference market cap.
    Reference is the 20th market cap value (index 19) or the first one if less data available.
    
    Args:
        df: DataFrame containing timestamp and marketCap columns
        max_multiplier: Maximum allowed multiplier from reference market cap (absolute limit)
        
    Returns:
        DataFrame with excessive values filtered out
    """
    if len(df) < 2:
        return df
    
    # Make a copy to avoid modifying the original
    filtered_df = df.copy()
    
    # Convert timestamps to datetime objects if they're strings
    if isinstance(filtered_df['timestamp'].iloc[0], str):
        filtered_df['timestamp'] = pd.to_datetime(filtered_df['timestamp'])
    
    # Get reference market cap (using the 20th value as reference, or first if fewer points)
    try:
        ref_index = min(19, len(filtered_df) - 1)  # Use 20th value (index 19) if available
        reference_marketcap = float(filtered_df.iloc[ref_index]['marketCap'])
        
        logger.info(f"ABSOLUTE FILTERING: Reference market cap: ${reference_marketcap:.2f} (from index {ref_index})")
        
        # Create a mask for rows to keep
        keep_rows = np.ones(len(filtered_df), dtype=bool)
        
        # Filter out any value that exceeds max_multiplier times the reference value
        removed_count = 0
        for i in range(len(filtered_df)):
            try:
                current_cap = float(filtered_df.iloc[i]['marketCap'])
                current_multiplier = current_cap / reference_marketcap
                
                if current_multiplier > max_multiplier:
                    keep_rows[i] = False
                    removed_count += 1
                    logger.debug(f"Removed absolute spike: {current_multiplier:.2f}x at {filtered_df.iloc[i]['timestamp']} " +
                               f"(value: ${current_cap:.2f}, reference: ${reference_marketcap:.2f})")
            except Exception as e:
                logger.warning(f"Error processing row {i} for absolute multiplier check: {e}")
        
        # Return filtered DataFrame
        filtered_result = filtered_df[keep_rows].reset_index(drop=True)
        if removed_count > 0:
            logger.info(f"ABSOLUTE FILTERING: Removed {removed_count} data points exceeding {max_multiplier}x multiplier out of {len(filtered_df)} records")
        return filtered_result
        
    except Exception as e:
        logger.warning(f"Error in absolute filtering: {e}")
        return df

def filter_relative_spikes(df: pd.DataFrame, max_multiplier: float = 20.0, normal_level_index: int = 6) -> pd.DataFrame:
    """
    Filter out market cap values that exceed max_multiplier times the normal level.
    Normal level is defined as the Nth largest market cap value (where N is normal_level_index+1).
    
    Args:
        df: DataFrame containing timestamp and marketCap columns
        max_multiplier: Maximum allowed multiplier from normal level (relative limit)
        normal_level_index: Index for defining normal level (0-based, so 6 means 7th largest value)
        
    Returns:
        DataFrame with excessive values filtered out
    """
    if len(df) < 2:
        return df
    
    # Make a copy to avoid modifying the original
    filtered_df = df.copy()
    
    # Convert timestamps to datetime objects if they're strings
    if isinstance(filtered_df['timestamp'].iloc[0], str):
        filtered_df['timestamp'] = pd.to_datetime(filtered_df['timestamp'])
    
    # Get normal level as the Nth largest market cap value
    try:
        # Make sure marketCap is numeric
        filtered_df['marketCap'] = pd.to_numeric(filtered_df['marketCap'], errors='coerce')
        
        # Sort values in descending order
        sorted_caps = filtered_df['marketCap'].sort_values(ascending=False)
        
        # Log sorted values for debugging
        logger.debug(f"Sorted market caps (top 10): {sorted_caps.head(10).tolist()}")
        
        # Check if we have enough data points
        if len(sorted_caps) <= normal_level_index:
            # If we don't have enough data points, use the last one (smallest value)
            normal_level = sorted_caps.iloc[-1]
            logger.warning(f"Not enough data points for normal level index {normal_level_index}, using smallest value")
        else:
            # Get the Nth largest value (e.g., 7th largest if normal_level_index=6)
            normal_level = sorted_caps.iloc[normal_level_index]
        
        logger.info(f"RELATIVE FILTERING: Normal level: ${normal_level:.2f} (from {normal_level_index+1}. largest value)")
        
        # If normal level is too small or zero, use a fallback
        if normal_level < 1000:
            logger.warning(f"Normal level (${normal_level:.2f}) is too small, using $1000 as minimum")
            normal_level = 1000
        
        # Create a mask for rows to keep
        keep_rows = np.ones(len(filtered_df), dtype=bool)
        
        # Filter out any value that exceeds max_multiplier times the normal level
        removed_count = 0
        for i in range(len(filtered_df)):
            try:
                current_cap = float(filtered_df.iloc[i]['marketCap'])
                current_multiplier = current_cap / normal_level
                
                if current_multiplier > max_multiplier:
                    keep_rows[i] = False
                    removed_count += 1
                    logger.debug(f"Removed relative spike: ${current_cap:.2f} at {filtered_df.iloc[i]['timestamp']} " +
                               f"({current_multiplier:.2f}x normal level of ${normal_level:.2f})")
            except Exception as e:
                logger.warning(f"Error processing row {i} for relative value check: {e}")
        
        # Return filtered DataFrame
        filtered_result = filtered_df[keep_rows].reset_index(drop=True)
        if removed_count > 0:
            logger.info(f"RELATIVE FILTERING: Removed {removed_count} data points exceeding {max_multiplier}x the normal level out of {len(filtered_df)} records")
        return filtered_result
        
    except Exception as e:
        logger.warning(f"Error in relative filtering: {e}")
        return df

def filter_spikes(df: pd.DataFrame, absolute_max: float = 80.0, relative_max: float = 3.0, normal_level_index: int = 6) -> pd.DataFrame:
    """
    Apply two-stage filtering:
    1. First remove all values exceeding absolute_max times the reference value (20th second)
    2. Then remove all values exceeding relative_max times the normal level (Nth largest value)
    
    Args:
        df: DataFrame containing timestamp and marketCap columns
        absolute_max: Maximum allowed multiplier from reference market cap (absolute limit)
        relative_max: Maximum allowed multiplier from normal level (relative limit)
        normal_level_index: Index for defining normal level (0-based, so 6 means 7th largest value)
        
    Returns:
        DataFrame with excessive values filtered out
    """
    # Log initial count
    initial_count = len(df)
    logger.info(f"TWO-STAGE FILTERING: Starting with {initial_count} data points")
    
    # First apply absolute filtering (based on 20th second value)
    filtered_df = filter_absolute_spikes(df, max_multiplier=absolute_max)
    
    # Log intermediate results
    after_absolute_count = len(filtered_df)
    logger.info(f"TWO-STAGE FILTERING: After absolute filtering ({absolute_max}x): {after_absolute_count} data points remaining")
    
    # Then apply relative filtering (based on Nth largest value)
    filtered_df = filter_relative_spikes(filtered_df, max_multiplier=relative_max, normal_level_index=normal_level_index)
    
    # Log final results
    final_count = len(filtered_df)
    logger.info(f"TWO-STAGE FILTERING: After relative filtering ({relative_max}x): {final_count} data points remaining (removed {initial_count - final_count} total)")
    
    return filtered_df

def process_single_pool(pool_id: str, db_path: str, min_marketcap: float = 40000, 
                        min_multiplier: float = 7.0) -> Dict[str, Any]:
    """
    Process a single pool and check if it meets the multiplier criteria.
    This function is designed to be used with multiprocessing.
    
    Args:
        pool_id: Pool address to analyze
        db_path: Path to the SQLite database
        min_marketcap: Minimum market cap to be considered at 20s
        min_multiplier: Minimum multiplier to qualify
        
    Returns:
        Dictionary with pool data if it qualifies, None otherwise
    """
    try:
        # Create a connection for this process
        conn = sqlite3.connect(db_path)
        
        # Get market data for this pool
        market_data_query = f"""
        SELECT timestamp, marketCap FROM market_data 
        WHERE poolAddress = '{pool_id}'
        ORDER BY timestamp ASC
        """
        
        df = pd.read_sql_query(market_data_query, conn)
        conn.close()
        
        # Convert marketCap to float
        df['marketCap'] = pd.to_numeric(df['marketCap'], errors='coerce')
        
        # Skip pools with insufficient data
        if len(df) < 20:  # Need at least 20 seconds of data
            return None
            
        # Get market cap at 20s (index 19)
        initial_marketcap = float(df.iloc[19]['marketCap'])
        if initial_marketcap < min_marketcap:
            return None
        
        # Kaksivaiheinen suodatus: ensin absoluuttinen 80x raja, sitten suhteellinen 3x raja
        filtered_df = filter_spikes(df, absolute_max=80.0, relative_max=3.0, normal_level_index=6)
        
        # Jos suodatus poisti kaikki tiedot, ohita tämä pooli
        if len(filtered_df) < 20:
            return None
            
        # Tarkista filtteröinnin jälkeen, että referenssi aikaleima on vielä saatavilla
        # (jos ei, käytä suodatetun dataframen ensimmäistä arvoa)
        if len(filtered_df) <= 19:
            initial_marketcap = float(filtered_df.iloc[0]['marketCap'])
        else:
            initial_marketcap = float(filtered_df.iloc[19]['marketCap'])
            
        if initial_marketcap < min_marketcap:
            return None
        
        # Calculate ATH market cap - käytä suodatettua dataa!
        ath_marketcap = filtered_df['marketCap'].max()
        
        # Calculate final multiplier (from 20s to ATH) - käyttäen suodatettua dataa
        final_multiplier = ath_marketcap / initial_marketcap
        
        # Only keep pools with high multiplier - tarkista vielä kerran että multiplier on sallituissa rajoissa
        if final_multiplier < min_multiplier:
            return None
            
        # Return pool data as dictionary
        return {
            'pool_id': pool_id,
            'initial_marketcap': initial_marketcap,
            'ath_marketcap': ath_marketcap,
            'final_multiplier': final_multiplier
        }
    
    except Exception as e:
        logger.error(f"Error processing pool {pool_id}: {e}")
        return None

def get_high_multiplier_pools_parallel(db_path: str, min_marketcap: float = 40000, 
                                       min_multiplier: float = 7.0) -> pd.DataFrame:
    """
    Get data for all pools that have at least min_multiplier returns using parallel processing.
    
    Args:
        db_path: Path to the SQLite database
        min_marketcap: Minimum market cap to be considered at 20s
        min_multiplier: Minimum multiplier to qualify
        
    Returns:
        DataFrame containing high multiplier pools data
    """
    try:
        # Get all pool IDs first
        conn = sqlite3.connect(db_path)
        pool_query = "SELECT poolAddress FROM pools"
        pool_ids = pd.read_sql_query(pool_query, conn)['poolAddress'].tolist()
        conn.close()
        
        logger.info(f"Analyzing {len(pool_ids)} pools for multipliers >= {min_multiplier}x...")
        
        # Determine CPU count
        cpu_count = multiprocessing.cpu_count()
        # Use 90% of available CPUs but at least 1
        num_workers = max(1, int(cpu_count * 0.9))
        logger.info(f"Using {num_workers} CPU cores out of {cpu_count} available")
        
        # Create process pool
        with multiprocessing.Pool(processes=num_workers) as pool:
            # Create partial function with fixed parameters
            process_pool_partial = partial(
                process_single_pool, 
                db_path=db_path, 
                min_marketcap=min_marketcap, 
                min_multiplier=min_multiplier
            )
            
            # Process pools in chunks for better progress reporting
            chunk_size = 100
            high_multiplier_pools_data = []
            
            for i in range(0, len(pool_ids), chunk_size):
                chunk = pool_ids[i:i+chunk_size]
                logger.info(f"Processing chunk {i//chunk_size + 1}/{(len(pool_ids) + chunk_size - 1)//chunk_size}...")
                
                # Process chunk in parallel
                chunk_results = pool.map(process_pool_partial, chunk)
                
                # Filter out None results and add valid results to the list
                valid_results = [result for result in chunk_results if result is not None]
                high_multiplier_pools_data.extend(valid_results)
                
                logger.info(f"Found {len(valid_results)} qualifying pools in current chunk, total: {len(high_multiplier_pools_data)}")
        
        # Convert to DataFrame and sort by multiplier
        pools_df = pd.DataFrame(high_multiplier_pools_data)
        if len(pools_df) > 0:
            pools_df = pools_df.sort_values(by='final_multiplier', ascending=False)
            
        logger.info(f"Total found: {len(pools_df)} pools with multiplier >= {min_multiplier}x")
        return pools_df
    
    except Exception as e:
        logger.error(f"Error analyzing pools in parallel: {e}")
        return pd.DataFrame()

def get_top_marketcaps(db_path: str, pool_id: str, top_n: int = 10) -> List[Tuple[str, float]]:
    """
    Get the top N market cap values for a specific pool
    
    Args:
        db_path: Path to the SQLite database
        pool_id: Pool address to analyze
        top_n: Number of top market cap values to return
        
    Returns:
        List of (timestamp, marketcap) tuples sorted by marketcap (descending)
    """
    try:
        conn = sqlite3.connect(db_path)
        
        # Get market data for this pool
        market_data_query = f"""
        SELECT timestamp, marketCap FROM market_data 
        WHERE poolAddress = '{pool_id}'
        """
        
        df = pd.read_sql_query(market_data_query, conn)
        conn.close()
        
        if not df.empty:
            # Varmistetaan että kaikki marketCap-arvot ovat float-tyyppisiä
            df['marketCap'] = pd.to_numeric(df['marketCap'], errors='coerce')
            
            # Log info about data before filtering
            logger.debug(f"Pool {pool_id}: Found {len(df)} records")
            logger.debug(f"Min marketCap: {df['marketCap'].min()}, Max marketCap: {df['marketCap'].max()}")
            
            # Kaksivaiheinen suodatus: ensin absoluuttinen 80x raja, sitten suhteellinen 3x raja
            df = filter_spikes(df, absolute_max=80.0, relative_max=3.0, normal_level_index=6)
            
            # Log info after filtering
            if not df.empty:
                logger.debug(f"After filtering - Min marketCap: {df['marketCap'].min()}, Max marketCap: {df['marketCap'].max()}")
            
            # Järjestetään numerisesti, ei merkkijonopohjaisesti
            df = df.sort_values(by='marketCap', ascending=False)
            
            # Get top N rows
            top_df = df.head(top_n)
            
            # Convert to list of tuples, varmistetaan float-tyyppi
            result = [(str(row['timestamp']), float(row['marketCap'])) 
                      for _, row in top_df.iterrows()]
                
            # Varmistetaan, että löydettiin arvot, jotka vastaavat korkea kerrointa
            max_marketcap = df['marketCap'].max()
            if len(result) > 0 and max_marketcap not in [cap for _, cap in result]:
                logger.warning(f"Warning: The maximum market cap value {max_marketcap} "
                              f"was not included in top {top_n} values. Adding it explicitly.")
                
                # Find the row with the maximum value
                max_row = df[df['marketCap'] == max_marketcap].iloc[0]
                # Add to beginning of results
                result.insert(0, (str(max_row['timestamp']), float(max_marketcap)))
                # Trim result if needed to maintain top_n size
                if len(result) > top_n:
                    result = result[:top_n]
                
            return result
        else:
            logger.warning(f"No data found for pool {pool_id}")
            return []
            
    except Exception as e:
        logger.error(f"Error getting top market caps for pool {pool_id}: {e}")
        return []

def analyze_and_save_results(db_path: str, pools_df: pd.DataFrame, output_file: str, top_n: int = 10):
    """
    Analyze each high multiplier pool and save results to file
    
    Args:
        db_path: Path to the SQLite database
        pools_df: DataFrame containing pool data
        output_file: Output file path
        top_n: Number of top market cap values to analyze
    """
    if len(pools_df) == 0:
        logger.warning("No pools to analyze")
        return
    
    with open(output_file, 'w') as f:
        f.write(f"# High Multiplier Pools Analysis\n")
        f.write(f"# Total pools found: {len(pools_df)}\n\n")
        
        for i, (_, pool) in enumerate(pools_df.iterrows()):
            pool_id = pool['pool_id']
            multiplier = pool['final_multiplier']
            initial_marketcap = pool['initial_marketcap']
            ath_marketcap = pool['ath_marketcap']
            
            # Get top market cap values
            top_marketcaps = get_top_marketcaps(db_path, pool_id, top_n)
            
            # Find ATH record if available
            ath_timestamp = "Unknown"
            # Fetch timestamp for ATH value
            if len(top_marketcaps) > 0:
                # Check if ATH value matches any in top_marketcaps
                for timestamp, marketcap in top_marketcaps:
                    if abs(marketcap - ath_marketcap) / ath_marketcap < 0.0001:  # Almost equal, accounting for floating point precision
                        ath_timestamp = timestamp
                        break
            
            # Write pool info
            f.write(f"## {i+1}. Pool: {pool_id}\n")
            f.write(f"   Multiplier: {multiplier:.2f}x\n")
            f.write(f"   Initial Market Cap: ${initial_marketcap:.2f}\n")
            f.write(f"   Highest Market Cap: ${ath_marketcap:.2f} at {ath_timestamp} (x{multiplier:.2f})\n")
            f.write(f"   Top {len(top_marketcaps)} Market Cap Values:\n")
            
            # Write top market caps - show timestamp as is
            for j, data in enumerate(top_marketcaps):
                timestamp, marketcap = data
                multiplier_at_point = marketcap / initial_marketcap
                f.write(f"     {j+1}. ${marketcap:.2f} at {timestamp} (x{multiplier_at_point:.2f})\n")
            
            f.write("\n")
            
            # Log progress
            if (i+1) % 10 == 0 or (i+1) == len(pools_df):
                logger.info(f"Analyzed {i+1}/{len(pools_df)} pools")
    
    logger.info(f"Analysis complete. Results saved to {output_file}")

def main():
    """Main function to analyze high multiplier pools"""
    parser = argparse.ArgumentParser(description='Analyze pools with high multipliers')
    parser.add_argument('--db-path', type=str, default='cache/pools.db',
                       help='Path to the SQLite database')
    parser.add_argument('--min-marketcap', type=float, default=40000,
                       help='Minimum market cap at first time point (20s)')
    parser.add_argument('--min-multiplier', type=float, default=7.0,
                       help='Minimum multiplier to qualify')
    parser.add_argument('--top-n', type=int, default=10,
                       help='Number of top market cap values to analyze per pool')
    parser.add_argument('--output-file', type=str, default='high_multiplier_pools_analysis.txt',
                       help='Output file path')
    
    args = parser.parse_args()
    
    logger.info(f"Starting analysis using database: {args.db_path}")
    
    # Get high multiplier pools with parallel processing
    pools_df = get_high_multiplier_pools_parallel(
        db_path=args.db_path,
        min_marketcap=args.min_marketcap,
        min_multiplier=args.min_multiplier
    )
    
    if len(pools_df) == 0:
        logger.error("No high multiplier pools found.")
        return
    
    # Analyze and save results
    analyze_and_save_results(
        db_path=args.db_path,
        pools_df=pools_df,
        output_file=args.output_file,
        top_n=args.top_n
    )

if __name__ == "__main__":
    # Safety for Windows multiprocessing
    multiprocessing.freeze_support()
    main() 