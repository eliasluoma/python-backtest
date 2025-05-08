#!/usr/bin/env python3
"""
Potential Growth Analyzer

Analyzes pools and calculates potential growth based on ATH market cap values.
"""

import sqlite3
import pandas as pd
import logging
from collections import Counter
import numpy as np
import matplotlib.pyplot as plt
import os
from typing import Dict, List, Tuple
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_pool_ids(db_path: str, limit: int = None) -> List[str]:
    """
    Get all pool IDs from the database.
    
    Args:
        db_path: Path to the SQLite database
        limit: Maximum number of pools to return
    
    Returns:
        List of pool IDs
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        if limit:
            cursor.execute("SELECT poolAddress FROM pools LIMIT ?", (limit,))
        else:
            cursor.execute("SELECT poolAddress FROM pools")
            
        pool_ids = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        return pool_ids
    except Exception as e:
        logger.error(f"Error fetching pool IDs: {e}")
        return []

def get_pool_data(db_path: str, pool_id: str) -> pd.DataFrame:
    """
    Get market data for a specific pool.
    
    Args:
        db_path: Path to the SQLite database
        pool_id: Pool ID to fetch data for
    
    Returns:
        DataFrame containing pool data
    """
    try:
        conn = sqlite3.connect(db_path)
        
        query = f"""
        SELECT * FROM market_data 
        WHERE poolAddress = '{pool_id}'
        ORDER BY timestamp ASC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Convert string marketcap fields to numeric
        numeric_columns = [
            'marketCap', 'athMarketCap', 'minMarketCap'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    except Exception as e:
        logger.error(f"Error fetching data for pool {pool_id}: {e}")
        return pd.DataFrame()

def analyze_potential_growth(db_path: str, max_pools: int = None) -> Dict:
    """
    Analyze potential growth for all pools based on ATH market cap.
    
    Args:
        db_path: Path to the SQLite database
        max_pools: Maximum number of pools to analyze
    
    Returns:
        Dictionary with results grouped by multiplier categories
    """
    pool_ids = get_pool_ids(db_path, max_pools)
    logger.info(f"Analyzing {len(pool_ids)} pools")
    
    # Results dictionary to store multipliers
    results = []
    
    # Tracking variables for filtering stats
    total_pools_checked = 0
    pools_filtered_by_min_data = 0
    pools_filtered_by_min_marketcap = 0
    
    # Process each pool
    for i, pool_id in enumerate(pool_ids):
        if (i + 1) % 10 == 0:
            logger.info(f"Progress: {i + 1}/{len(pool_ids)} pools processed")
        
        try:
            # Get pool data
            df = get_pool_data(db_path, pool_id)
            total_pools_checked += 1
            
            # Skip pools with insufficient data
            if len(df) < 11:  # We need at least 11 rows (0-indexed, so row 10 is the 11th row)
                pools_filtered_by_min_data += 1
                continue
            
            # Get 10th row (index 9) market cap and ATH market cap
            tenth_row_marketcap = df.iloc[9]['marketCap']
            tenth_row_ath_marketcap = df.iloc[9]['athMarketCap']
            
            # Skip pools with market cap less than 40,000 at 10 seconds
            if tenth_row_marketcap < 40000:
                pools_filtered_by_min_marketcap += 1
                continue
            
            # Get last row ATH market cap
            last_row_ath_marketcap = df.iloc[-1]['athMarketCap']
            
            # Calculate potential multiplier only if ATH market cap is higher than 10th row market cap
            if last_row_ath_marketcap > tenth_row_marketcap:
                potential_multiplier = last_row_ath_marketcap / tenth_row_marketcap
                results.append({
                    'pool_id': pool_id,
                    'tenth_row_marketcap': tenth_row_marketcap,
                    'tenth_row_ath_marketcap': tenth_row_ath_marketcap,
                    'last_row_ath_marketcap': last_row_ath_marketcap,
                    'potential_multiplier': potential_multiplier
                })
        except Exception as e:
            logger.error(f"Error analyzing pool {pool_id}: {e}")
    
    # Categorize results by multiplier ranges
    multiplier_categories = {
        'less_than_2x': 0,
        '3x': 0, '4x': 0, '5x': 0, '6x': 0, '7x': 0, '8x': 0, '9x': 0,
        '10x': 0, '11x': 0, '12x': 0, '13x': 0, '14x': 0, '15x': 0,
        '17x': 0, '18x': 0, '19x': 0, '20x': 0,
        'more_than_20x': 0
    }
    
    total_pools_with_potential = len(results)
    
    for result in results:
        multiplier = result['potential_multiplier']
        
        if multiplier < 2:
            multiplier_categories['less_than_2x'] += 1
        elif multiplier >= 2 and multiplier < 3:
            multiplier_categories['3x'] += 1
        elif multiplier >= 3 and multiplier < 4:
            multiplier_categories['4x'] += 1
        elif multiplier >= 4 and multiplier < 5:
            multiplier_categories['5x'] += 1
        elif multiplier >= 5 and multiplier < 6:
            multiplier_categories['6x'] += 1
        elif multiplier >= 6 and multiplier < 7:
            multiplier_categories['7x'] += 1
        elif multiplier >= 7 and multiplier < 8:
            multiplier_categories['8x'] += 1
        elif multiplier >= 8 and multiplier < 9:
            multiplier_categories['9x'] += 1
        elif multiplier >= 9 and multiplier < 10:
            multiplier_categories['10x'] += 1
        elif multiplier >= 10 and multiplier < 11:
            multiplier_categories['11x'] += 1
        elif multiplier >= 11 and multiplier < 12:
            multiplier_categories['12x'] += 1
        elif multiplier >= 12 and multiplier < 13:
            multiplier_categories['13x'] += 1
        elif multiplier >= 13 and multiplier < 14:
            multiplier_categories['14x'] += 1
        elif multiplier >= 14 and multiplier < 15:
            multiplier_categories['15x'] += 1
        elif multiplier >= 15 and multiplier < 17: # Skip 16x, go from 15x to 17x
            multiplier_categories['15x'] += 1  # Count in 15x category
        elif multiplier >= 17 and multiplier < 18:
            multiplier_categories['17x'] += 1
        elif multiplier >= 18 and multiplier < 19:
            multiplier_categories['18x'] += 1
        elif multiplier >= 19 and multiplier < 20:
            multiplier_categories['19x'] += 1
        elif multiplier >= 20 and multiplier < 21:
            multiplier_categories['20x'] += 1
        else:  # multiplier >= 21
            multiplier_categories['more_than_20x'] += 1
    
    # Calculate percentages
    percentages = {}
    for category, count in multiplier_categories.items():
        if total_pools_with_potential > 0:
            percentages[category] = (count / total_pools_with_potential) * 100
        else:
            percentages[category] = 0
    
    return {
        'total_pools_analyzed': len(pool_ids),
        'pools_with_potential': total_pools_with_potential,
        'multiplier_counts': multiplier_categories,
        'multiplier_percentages': percentages,
        'detailed_results': results,
        'pools_filtered_by_min_data': pools_filtered_by_min_data,
        'pools_filtered_by_min_marketcap': pools_filtered_by_min_marketcap
    }

def plot_results(results: Dict, output_path: str = 'potential_growth.png'):
    """
    Plot the results as a bar chart
    
    Args:
        results: Results dictionary from analyze_potential_growth
        output_path: Path to save the plot
    """
    categories = []
    counts = []
    
    for category, count in results['multiplier_counts'].items():
        # Clean up category names for display
        if category == 'less_than_2x':
            display_name = '<2x'
        elif category == 'more_than_20x':
            display_name = '>20x'
        else:
            display_name = category
        
        categories.append(display_name)
        counts.append(count)
    
    plt.figure(figsize=(12, 8))
    bars = plt.bar(categories, counts)
    
    # Add count labels on top of bars
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                 f'{int(height)}',
                 ha='center', va='bottom', rotation=0)
    
    plt.title('Potential Growth Distribution')
    plt.xlabel('Multiplier Range')
    plt.ylabel('Number of Pools')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Save the plot
    plt.savefig(output_path)
    logger.info(f"Plot saved to {output_path}")

def display_results(results: Dict):
    """
    Display results to the console
    
    Args:
        results: Results dictionary from analyze_potential_growth
    """
    logger.info(f"Analysis complete!")
    logger.info(f"Total pools analyzed: {results['total_pools_analyzed']}")
    logger.info(f"Pools with growth potential: {results['pools_with_potential']}")
    
    # Display filter stats if available
    if 'pools_filtered_by_min_data' in results:
        logger.info(f"Pools filtered due to insufficient data: {results['pools_filtered_by_min_data']}")
    if 'pools_filtered_by_min_marketcap' in results:
        logger.info(f"Pools filtered due to minimum marketcap requirement (< 40,000): {results['pools_filtered_by_min_marketcap']}")
    
    logger.info("\nResults by multiplier category:")
    
    # Display in order from less than 2x to more than 20x
    ordered_categories = [
        'less_than_2x',
        '3x', '4x', '5x', '6x', '7x', '8x', '9x',
        '10x', '11x', '12x', '13x', '14x', '15x',
        '17x', '18x', '19x', '20x',
        'more_than_20x'
    ]
    
    for category in ordered_categories:
        count = results['multiplier_counts'][category]
        percentage = results['multiplier_percentages'][category]
        
        if category == 'less_than_2x':
            display_name = 'Less than 2x'
        elif category == 'more_than_20x':
            display_name = 'More than 20x'
        else:
            display_name = f'{category} range'
        
        logger.info(f"{display_name}: {count} pools ({percentage:.1f}%)")

def main():
    """Main function to run the analysis"""
    parser = argparse.ArgumentParser(description='Analyze potential growth based on ATH market cap')
    parser.add_argument('--db-path', type=str, default='cache/pools.db', 
                        help='Path to the SQLite database')
    parser.add_argument('--max-pools', type=int, default=None,
                        help='Maximum number of pools to analyze')
    parser.add_argument('--plot', action='store_true',
                        help='Generate a plot of the results')
    parser.add_argument('--output', type=str, default='potential_growth.png',
                        help='Path to save the plot')
    
    args = parser.parse_args()
    
    logger.info(f"Starting analysis using database: {args.db_path}")
    logger.info(f"Analyzing a maximum of {args.max_pools if args.max_pools else 'all'} pools")
    
    # Run analysis
    results = analyze_potential_growth(args.db_path, args.max_pools)
    
    # Display results
    display_results(results)
    
    # Generate plot if requested
    if args.plot:
        plot_results(results, args.output)

if __name__ == "__main__":
    main() 