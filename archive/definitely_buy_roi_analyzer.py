#!/usr/bin/env python3
"""
Definitely Buy ROI Analyzer

Analyzes the SOL-denominated return on investment for "Definitely Buy" pools at different time points.
Shows how delaying purchase affects potential returns in terms of SOL amount.
"""

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import logging
import argparse
import os
from typing import Dict, List, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_definitely_buy_pools(db_path: str, min_marketcap: float = 40000, min_multiplier: float = 7.0) -> pd.DataFrame:
    """
    Get pools that would be classified as "Definitely Buy" (with high return potential).
    
    Args:
        db_path: Path to the SQLite database
        min_marketcap: Minimum market cap at the first time point (10s)
        min_multiplier: Minimum ATH/initial marketcap multiplier to qualify as "Definitely Buy"
        
    Returns:
        DataFrame containing pool data
    """
    try:
        conn = sqlite3.connect(db_path)
        
        # Get all pools
        pool_query = "SELECT poolAddress FROM pools"
        pool_ids = pd.read_sql_query(pool_query, conn)['poolAddress'].tolist()
        
        definitely_buy_pools = []
        
        # Define more detailed time points for analysis
        time_points = [10, 15, 20, 25, 30, 40, 50, 60, 90, 120, 150, 180, 240, 300, 360, 420, 480, 540, 600]
        
        for pool_id in pool_ids:
            # Get market data for this pool
            market_data_query = f"""
            SELECT * FROM market_data 
            WHERE poolAddress = '{pool_id}'
            ORDER BY timestamp ASC
            """
            
            df = pd.read_sql_query(market_data_query, conn)
            
            # Convert numeric columns to float
            numeric_columns = ['marketCap', 'athMarketCap', 'minMarketCap']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Skip pools with insufficient data (less than 600 rows)
            if len(df) < 600:
                continue
                
            # Get market cap at 10s
            if len(df) <= 9:  # Index 9 corresponds to 10s (0-indexed)
                continue
                
            initial_marketcap = float(df.iloc[9]['marketCap'])
            
            # Skip pools with market cap below threshold
            if initial_marketcap < min_marketcap:
                continue
                
            # Get ATH market cap
            ath_marketcap = float(df['athMarketCap'].iloc[-1])
            
            # Calculate multiplier
            multiplier = ath_marketcap / initial_marketcap
            
            # Check if this is a "Definitely Buy" pool
            if multiplier >= min_multiplier:
                # Add relevant data to results
                pool_data = {
                    'pool_id': pool_id,
                    'initial_marketcap': initial_marketcap,
                    'ath_marketcap': ath_marketcap,
                    'multiplier': multiplier
                }
                
                # Add market cap at different time points
                for t in time_points:
                    if len(df) > t-1:
                        pool_data[f'marketcap_{t}s'] = float(df.iloc[t-1]['marketCap'])
                    else:
                        break  # Skip this pool if we don't have data for all time points
                
                # Only add if we have data for all time points
                if len(pool_data) == 4 + len(time_points):  # 4 base fields + time points
                    definitely_buy_pools.append(pool_data)
        
        conn.close()
        
        # Convert to DataFrame
        return pd.DataFrame(definitely_buy_pools)
    
    except Exception as e:
        logger.error(f"Error fetching definitely buy pools: {e}")
        return pd.DataFrame()

def calculate_sol_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the SOL returns for each pool at different time points.
    
    Args:
        df: DataFrame containing pool data
        
    Returns:
        DataFrame with SOL returns added
    """
    # Add SOL investment and return columns
    initial_investment = 1.0  # 1 SOL
    
    # Get all time points from column names
    time_points = []
    for col in df.columns:
        if col.startswith('marketcap_') and col.endswith('s'):
            # Extract the time point from column name (e.g., 'marketcap_10s' -> 10)
            time_point = int(col.replace('marketcap_', '').replace('s', ''))
            time_points.append(time_point)
    
    time_points.sort()
    
    for t in time_points:
        # Calculate how much we could buy at this time point with our initial investment
        df[f'token_amount_{t}s'] = initial_investment / df[f'marketcap_{t}s']
        
        # Calculate how much SOL we would get when selling at ATH
        df[f'sol_return_{t}s'] = df[f'token_amount_{t}s'] * df['ath_marketcap']
        
        # Calculate ROI as a multiple of initial investment
        df[f'roi_{t}s'] = df[f'sol_return_{t}s'] / initial_investment
    
    return df

def analyze_roi_decay(df: pd.DataFrame, output_path_prefix: str = 'definitely_buy_roi'):
    """
    Analyze how ROI decays over time and generate visualizations.
    
    Args:
        df: DataFrame containing pool data with calculated returns
        output_path_prefix: Prefix for output files
    """
    # Get all time points from column names
    time_points = []
    for col in df.columns:
        if col.startswith('roi_') and col.endswith('s'):
            # Extract the time point from column name (e.g., 'roi_10s' -> 10)
            time_point = int(col.replace('roi_', '').replace('s', ''))
            time_points.append(time_point)
    
    time_points.sort()
    
    # Calculate statistics
    stats = {}
    
    # Average ROI at each time point
    for t in time_points:
        stats[f'avg_roi_{t}s'] = df[f'roi_{t}s'].mean()
        stats[f'median_roi_{t}s'] = df[f'roi_{t}s'].median()
        stats[f'min_roi_{t}s'] = df[f'roi_{t}s'].min()
        stats[f'max_roi_{t}s'] = df[f'roi_{t}s'].max()
    
    # Calculate percentage of original ROI retained at each time point
    for t in time_points[1:]:  # Skip first time point (10s) as it's the baseline
        stats[f'avg_pct_retained_{t}s'] = (stats[f'avg_roi_{t}s'] / stats[f'avg_roi_10s']) * 100
        stats[f'median_pct_retained_{t}s'] = (stats[f'median_roi_{t}s'] / stats[f'median_roi_10s']) * 100
    
    # Relative to t=10s, percentage of pools where ROI improves
    for t in time_points[1:]:
        improved = (df[f'roi_{t}s'] > df['roi_10s']).sum()
        stats[f'pct_pools_improved_{t}s'] = (improved / len(df)) * 100
    
    # Display statistics
    logger.info(f"Analysis for {len(df)} 'Definitely Buy' pools:")
    
    logger.info("\nSOL ROI Statistics:")
    logger.info(f"Time point\tAvg ROI\t\tMedian ROI\tMin ROI\t\tMax ROI")
    for t in time_points:
        logger.info(f"{t}s\t\t{stats[f'avg_roi_{t}s']:.2f}x\t\t{stats[f'median_roi_{t}s']:.2f}x\t\t{stats[f'min_roi_{t}s']:.2f}x\t\t{stats[f'max_roi_{t}s']:.2f}x")
    
    logger.info("\nPercentage of Original ROI (at 10s) Retained:")
    logger.info(f"Time point\tAvg % Retained\tMedian % Retained")
    for t in time_points[1:]:
        logger.info(f"{t}s\t\t{stats[f'avg_pct_retained_{t}s']:.2f}%\t\t{stats[f'median_pct_retained_{t}s']:.2f}%")
    
    logger.info("\nPercentage of Pools Where ROI Improves Compared to 10s:")
    for t in time_points[1:]:
        logger.info(f"{t}s: {stats[f'pct_pools_improved_{t}s']:.2f}%")
    
    # Generate visualizations
    plt.figure(figsize=(16, 10))
    
    # Plot 1: Average and median ROI over time
    plt.subplot(2, 1, 1)
    avg_roi = [stats[f'avg_roi_{t}s'] for t in time_points]
    median_roi = [stats[f'median_roi_{t}s'] for t in time_points]
    
    plt.plot(time_points, avg_roi, 'o-', color='blue', label='Average ROI')
    plt.plot(time_points, median_roi, 'o-', color='green', label='Median ROI')
    plt.title('ROI Over Time for "Definitely Buy" Pools')
    plt.xlabel('Time Point (seconds)')
    plt.ylabel('Return Multiple (x)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    
    # Plot 2: Percentage of original ROI retained
    plt.subplot(2, 1, 2)
    avg_retained = [100] + [stats[f'avg_pct_retained_{t}s'] for t in time_points[1:]]
    median_retained = [100] + [stats[f'median_pct_retained_{t}s'] for t in time_points[1:]]
    
    plt.plot(time_points, avg_retained, 'o-', color='blue', label='Average % Retained')
    plt.plot(time_points, median_retained, 'o-', color='green', label='Median % Retained')
    plt.axhline(y=100, color='red', linestyle='--', alpha=0.7)
    plt.title('Percentage of Original ROI (at 10s) Retained Over Time')
    plt.xlabel('Time Point (seconds)')
    plt.ylabel('Percentage (%)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    
    plt.tight_layout()
    plt.savefig(f"{output_path_prefix}_over_time.png")
    logger.info(f"ROI over time plot saved to {output_path_prefix}_over_time.png")
    
    # Plot detailed view of retained percentage (focusing on median)
    plt.figure(figsize=(16, 8))
    plt.plot(time_points, median_retained, 'o-', color='green', linewidth=2, markersize=8)
    plt.axhline(y=100, color='red', linestyle='--', alpha=0.7)
    plt.axhline(y=90, color='orange', linestyle='--', alpha=0.7)
    plt.axhline(y=80, color='yellow', linestyle='--', alpha=0.7)
    
    # Add annotations for local minima
    median_array = np.array(median_retained)
    for i in range(1, len(median_array)-1):
        if median_array[i] < median_array[i-1] and median_array[i] < median_array[i+1]:
            plt.annotate(f'{median_array[i]:.1f}%', 
                        xy=(time_points[i], median_array[i]),
                        xytext=(time_points[i], median_array[i]-5),
                        arrowprops=dict(facecolor='black', shrink=0.05, width=1.5, headwidth=8),
                        ha='center', fontsize=10)
    
    plt.title('Median Percentage of Original ROI Retained Over Time')
    plt.xlabel('Time Point (seconds)')
    plt.ylabel('Percentage of Original ROI (%)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(f"{output_path_prefix}_median_retained.png")
    logger.info(f"Median retained percentage plot saved to {output_path_prefix}_median_retained.png")
    
    # Plot individual pool ROIs (on a separate figure to avoid clutter)
    plt.figure(figsize=(16, 8))
    for i, row in df.iterrows():
        roi_values = [row[f'roi_{t}s'] for t in time_points]
        plt.plot(time_points, roi_values, 'o-', alpha=0.3)
    
    # Add average line
    plt.plot(time_points, avg_roi, 'o-', color='red', linewidth=3, label='Average ROI')
    plt.plot(time_points, median_roi, 'o-', color='black', linewidth=3, label='Median ROI')
    
    plt.title('Individual Pool ROI Over Time')
    plt.xlabel('Time Point (seconds)')
    plt.ylabel('Return Multiple (x)')
    plt.yscale('log')  # Use log scale for better visualization
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    
    plt.tight_layout()
    plt.savefig(f"{output_path_prefix}_individual_pools.png")
    logger.info(f"Individual pool ROI plot saved to {output_path_prefix}_individual_pools.png")
    
    # Save detailed results to CSV
    df.to_csv(f"{output_path_prefix}_detailed.csv", index=False)
    logger.info(f"Detailed results saved to {output_path_prefix}_detailed.csv")

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Analyze ROI decay for "Definitely Buy" pools')
    parser.add_argument('--db-path', type=str, default='cache/pools.db',
                       help='Path to the SQLite database')
    parser.add_argument('--min-marketcap', type=float, default=40000,
                       help='Minimum market cap at first time point (10s)')
    parser.add_argument('--min-multiplier', type=float, default=7.0,
                       help='Minimum multiplier to qualify as a "Definitely Buy" pool')
    parser.add_argument('--output-prefix', type=str, default='definitely_buy_roi',
                       help='Prefix for output files')
    
    args = parser.parse_args()
    
    logger.info(f"Starting analysis using database: {args.db_path}")
    logger.info(f"Minimum market cap: {args.min_marketcap}")
    logger.info(f"Minimum multiplier for 'Definitely Buy': {args.min_multiplier}")
    
    # Get "Definitely Buy" pools
    df = get_definitely_buy_pools(
        db_path=args.db_path,
        min_marketcap=args.min_marketcap,
        min_multiplier=args.min_multiplier
    )
    
    if len(df) == 0:
        logger.error("No 'Definitely Buy' pools found with the given criteria.")
        return
    
    logger.info(f"Found {len(df)} 'Definitely Buy' pools.")
    
    # Calculate SOL returns
    df = calculate_sol_returns(df)
    
    # Analyze ROI decay
    analyze_roi_decay(df, output_path_prefix=args.output_prefix)

if __name__ == "__main__":
    main() 