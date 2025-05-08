#!/usr/bin/env python3
"""
Time Window Analyzer for "Definitely Buy" Pools

Analyzes how timing affects the ability to identify high-potential pools, with focus on
eliminating outlier effects and tracking how poor pools drop out over time.
"""

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import logging
import argparse
import os
from typing import Dict, List, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_all_pools_data(db_path: str, min_marketcap: float = 40000, max_time_point: int = 600) -> pd.DataFrame:
    """
    Get data for all pools that have sufficient data.
    
    Args:
        db_path: Path to the SQLite database
        min_marketcap: Minimum market cap to be considered at 10s
        max_time_point: Maximum time point to be analyzed
        
    Returns:
        DataFrame containing all pools data
    """
    try:
        conn = sqlite3.connect(db_path)
        
        # Get all pools
        pool_query = "SELECT poolAddress FROM pools"
        pool_ids = pd.read_sql_query(pool_query, conn)['poolAddress'].tolist()
        
        all_pools_data = []
        
        # Define time points for analysis
        time_points = [10, 15, 20, 25, 30, 40, 50, 60, 90, 120, 150, 180, 240, 300, 360, 420, 480, 540, 600]
        
        # Filter out time points beyond the max_time_point
        time_points = [t for t in time_points if t <= max_time_point]
        
        logger.info(f"Analyzing {len(pool_ids)} pools with time points: {time_points}")
        
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
            numeric_columns = ['marketCap', 'athMarketCap', 'minMarketCap']
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
                
            # Get ATH market cap from the data
            ath_marketcap = float(df['athMarketCap'].iloc[-1])
            
            # Calculate final multiplier (from 10s to ATH)
            final_multiplier = ath_marketcap / initial_marketcap
            
            # Add pool data
            pool_data = {
                'pool_id': pool_id,
                'initial_marketcap': initial_marketcap,
                'ath_marketcap': ath_marketcap,
                'final_multiplier': final_multiplier
            }
            
            # Add market cap at each time point
            for t in time_points:
                if t-1 < len(df):
                    time_marketcap = float(df.iloc[t-1]['marketCap'])
                    pool_data[f'marketcap_{t}s'] = time_marketcap
                    
                    # Add the ATH at this point in time
                    # (what would be known as the ATH at this time)
                    if t-1 >= 0:
                        time_ath = float(df.iloc[:(t-1)+1]['athMarketCap'].max())
                        pool_data[f'ath_{t}s'] = time_ath
                        
                        # Calculate multiplier at this time point
                        # This shows what the pool's potential looked like at time t
                        time_multiplier = time_ath / initial_marketcap
                        pool_data[f'multiplier_{t}s'] = time_multiplier
            
            all_pools_data.append(pool_data)
        
        conn.close()
        
        # Convert to DataFrame
        return pd.DataFrame(all_pools_data)
    
    except Exception as e:
        logger.error(f"Error fetching pool data: {e}")
        return pd.DataFrame()

def analyze_time_windows(df: pd.DataFrame, min_multiplier: float = 7.0, outlier_percentile: float = 95.0) -> Dict:
    """
    Analyze how the identification of high-potential pools changes over time.
    
    Args:
        df: DataFrame containing all pools data
        min_multiplier: Minimum multiplier to be considered "Definitely Buy"
        outlier_percentile: Percentile to cut off outliers (superpools)
        
    Returns:
        Dictionary with analysis results
    """
    # Get all time points from column names
    time_points = []
    for col in df.columns:
        if col.startswith('multiplier_') and col.endswith('s'):
            # Extract the time point from column name (e.g., 'multiplier_10s' -> 10)
            time_point = int(col.replace('multiplier_', '').replace('s', ''))
            time_points.append(time_point)
    
    time_points.sort()
    
    # Identify "Definitely Buy" pools based on final result
    final_definitely_buy = df[df['final_multiplier'] >= min_multiplier].copy()
    
    # Identify outliers based on percentile of final_multiplier
    outlier_threshold = np.percentile(final_definitely_buy['final_multiplier'], outlier_percentile)
    normal_definitely_buy = final_definitely_buy[final_definitely_buy['final_multiplier'] < outlier_threshold]
    superpools = final_definitely_buy[final_definitely_buy['final_multiplier'] >= outlier_threshold]
    
    logger.info(f"Total pools analyzed: {len(df)}")
    logger.info(f"Total 'Definitely Buy' pools: {len(final_definitely_buy)} (multiplier >= {min_multiplier})")
    logger.info(f"Normal 'Definitely Buy' pools: {len(normal_definitely_buy)} (multiplier < {outlier_threshold})")
    logger.info(f"Superpools: {len(superpools)} (multiplier >= {outlier_threshold})")
    
    # Initialize results
    results = {
        'time_points': time_points,
        'total_pools': len(df),
        'total_definitely_buy': len(final_definitely_buy),
        'normal_definitely_buy': len(normal_definitely_buy),
        'superpools': len(superpools),
        'outlier_threshold': outlier_threshold,
        'accuracy_stats': {},
        'dropout_stats': {},
        'timing_stats': {}
    }
    
    # For each time point, analyze how well we can identify the true "Definitely Buy" pools
    for t in time_points:
        # How many pools would be identified as "Definitely Buy" at this time point
        identified_at_t = df[df[f'multiplier_{t}s'] >= min_multiplier]
        
        # How many of the true "Definitely Buy" pools are identified at this time point
        true_positives = len(set(identified_at_t['pool_id']) & set(final_definitely_buy['pool_id']))
        
        # How many pools misclassified as "Definitely Buy" at this time point
        false_positives = len(identified_at_t) - true_positives
        
        # How many true "Definitely Buy" pools missed at this time point
        false_negatives = len(final_definitely_buy) - true_positives
        
        # How many pools correctly identified as not "Definitely Buy"
        true_negatives = len(df) - len(identified_at_t) - false_negatives
        
        # Calculate accuracy metrics
        if true_positives + false_positives > 0:
            precision = true_positives / (true_positives + false_positives)
        else:
            precision = 0
            
        if true_positives + false_negatives > 0:
            recall = true_positives / (true_positives + false_negatives)
        else:
            recall = 0
            
        if precision + recall > 0:
            f1_score = 2 * (precision * recall) / (precision + recall)
        else:
            f1_score = 0
            
        accuracy = (true_positives + true_negatives) / len(df)
        
        # Store accuracy stats
        results['accuracy_stats'][t] = {
            'identified': len(identified_at_t),
            'true_positives': true_positives,
            'false_positives': false_positives,
            'false_negatives': false_negatives,
            'true_negatives': true_negatives,
            'precision': precision,
            'recall': recall,
            'f1_score': f1_score,
            'accuracy': accuracy
        }
        
        # Now analyze dropout of poor pools
        # A poor pool is defined as one with a final_multiplier < 2.0
        poor_pools = df[df['final_multiplier'] < 2.0]
        
        # How many poor pools would be identified as "Definitely Buy" at this time point
        poor_identified_at_t = poor_pools[poor_pools[f'multiplier_{t}s'] >= min_multiplier]
        
        # Calculate dropout rate
        poor_dropout_rate = 1 - (len(poor_identified_at_t) / len(poor_pools)) if len(poor_pools) > 0 else 0
        
        # Store dropout stats
        results['dropout_stats'][t] = {
            'total_poor_pools': len(poor_pools),
            'poor_identified': len(poor_identified_at_t),
            'poor_dropout_rate': poor_dropout_rate
        }
        
        # Analyze timing statistics for normal "Definitely Buy" pools
        # For each normal "Definitely Buy" pool, what's the multiplier at this time point
        if len(normal_definitely_buy) > 0:
            multipliers_at_t = normal_definitely_buy[f'multiplier_{t}s']
            avg_multiplier = multipliers_at_t.mean()
            median_multiplier = multipliers_at_t.median()
            min_multiplier_at_t = multipliers_at_t.min()
            max_multiplier_at_t = multipliers_at_t.max()
            
            # Calculate percentage of final potential visible at this time
            pct_of_final = (multipliers_at_t / normal_definitely_buy['final_multiplier']) * 100
            avg_pct_of_final = pct_of_final.mean()
            median_pct_of_final = pct_of_final.median()
            
            # How many normal "Definitely Buy" pools would be identified at this time
            normal_identified_at_t = normal_definitely_buy[normal_definitely_buy[f'multiplier_{t}s'] >= min_multiplier]
            normal_identified_rate = len(normal_identified_at_t) / len(normal_definitely_buy)
            
            results['timing_stats'][t] = {
                'avg_multiplier': avg_multiplier,
                'median_multiplier': median_multiplier,
                'min_multiplier': min_multiplier_at_t,
                'max_multiplier': max_multiplier_at_t,
                'avg_pct_of_final': avg_pct_of_final,
                'median_pct_of_final': median_pct_of_final,
                'normal_identified': len(normal_identified_at_t),
                'normal_identified_rate': normal_identified_rate
            }
    
    return results

def plot_analysis_results(results: Dict, output_prefix: str = 'time_window'):
    """
    Generate plots to visualize the analysis results.
    
    Args:
        results: Results dictionary from analyze_time_windows
        output_prefix: Prefix for output files
    """
    time_points = results['time_points']
    
    # Plot 1: Accuracy metrics over time
    plt.figure(figsize=(16, 8))
    
    precision = [results['accuracy_stats'][t]['precision'] * 100 for t in time_points]
    recall = [results['accuracy_stats'][t]['recall'] * 100 for t in time_points]
    f1_score = [results['accuracy_stats'][t]['f1_score'] * 100 for t in time_points]
    
    plt.plot(time_points, precision, 'o-', color='blue', linewidth=2, label='Precision')
    plt.plot(time_points, recall, 'o-', color='green', linewidth=2, label='Recall')
    plt.plot(time_points, f1_score, 'o-', color='red', linewidth=2, label='F1 Score')
    
    plt.title('Accuracy of "Definitely Buy" Pool Identification Over Time')
    plt.xlabel('Time Point (seconds)')
    plt.ylabel('Percentage (%)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{output_prefix}_accuracy.png")
    logger.info(f"Accuracy plot saved to {output_prefix}_accuracy.png")
    
    # Plot 2: Poor pools dropout rate
    plt.figure(figsize=(16, 8))
    
    dropout_rate = [results['dropout_stats'][t]['poor_dropout_rate'] * 100 for t in time_points]
    
    plt.plot(time_points, dropout_rate, 'o-', color='purple', linewidth=2, markersize=8)
    
    # Add annotations for key points
    for i, t in enumerate(time_points):
        if i > 0 and dropout_rate[i] > dropout_rate[i-1] + 5:  # Significant increase
            plt.annotate(f'{dropout_rate[i]:.1f}%', 
                        xy=(t, dropout_rate[i]),
                        xytext=(t, dropout_rate[i]+5),
                        arrowprops=dict(facecolor='black', shrink=0.05, width=1.5, headwidth=8),
                        ha='center', fontsize=10)
    
    plt.title('Poor Pools Dropout Rate Over Time')
    plt.xlabel('Time Point (seconds)')
    plt.ylabel('Dropout Rate (%)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(f"{output_prefix}_dropout.png")
    logger.info(f"Dropout plot saved to {output_prefix}_dropout.png")
    
    # Plot 3: Percentage of final potential visible
    plt.figure(figsize=(16, 8))
    
    avg_pct = [results['timing_stats'][t]['avg_pct_of_final'] for t in time_points]
    median_pct = [results['timing_stats'][t]['median_pct_of_final'] for t in time_points]
    
    plt.plot(time_points, avg_pct, 'o-', color='blue', linewidth=2, label='Average')
    plt.plot(time_points, median_pct, 'o-', color='green', linewidth=2, label='Median')
    plt.axhline(y=100, color='red', linestyle='--', alpha=0.7)
    
    # Add annotations for local minima
    median_array = np.array(median_pct)
    for i in range(1, len(median_array)-1):
        if median_array[i] < median_array[i-1] and median_array[i] < median_array[i+1]:
            plt.annotate(f'{median_array[i]:.1f}%', 
                        xy=(time_points[i], median_array[i]),
                        xytext=(time_points[i], median_array[i]-5),
                        arrowprops=dict(facecolor='black', shrink=0.05, width=1.5, headwidth=8),
                        ha='center', fontsize=10)
    
    plt.title('Percentage of Final Potential Visible at Each Time Point (Excluding Superpools)')
    plt.xlabel('Time Point (seconds)')
    plt.ylabel('Percentage of Final Potential (%)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{output_prefix}_potential.png")
    logger.info(f"Potential plot saved to {output_prefix}_potential.png")
    
    # Plot 4: Identification rate of normal "Definitely Buy" pools
    plt.figure(figsize=(16, 8))
    
    identification_rate = [results['timing_stats'][t]['normal_identified_rate'] * 100 for t in time_points]
    
    plt.plot(time_points, identification_rate, 'o-', color='orange', linewidth=2, markersize=8)
    
    # Add annotations for key points
    identification_array = np.array(identification_rate)
    for i in range(1, len(identification_array)-1):
        if (identification_array[i] < identification_array[i-1] and 
            identification_array[i] < identification_array[i+1]):
            plt.annotate(f'{identification_array[i]:.1f}%', 
                        xy=(time_points[i], identification_array[i]),
                        xytext=(time_points[i], identification_array[i]-5),
                        arrowprops=dict(facecolor='black', shrink=0.05, width=1.5, headwidth=8),
                        ha='center', fontsize=10)
    
    plt.title('Identification Rate of Normal "Definitely Buy" Pools Over Time')
    plt.xlabel('Time Point (seconds)')
    plt.ylabel('Identification Rate (%)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(f"{output_prefix}_identification.png")
    logger.info(f"Identification rate plot saved to {output_prefix}_identification.png")
    
    # Plot 5: Combined insights
    plt.figure(figsize=(16, 12))
    
    plt.subplot(3, 1, 1)
    plt.plot(time_points, recall, 'o-', color='green', linewidth=2, label='Recall (True Positive Rate)')
    plt.plot(time_points, dropout_rate, 'o-', color='purple', linewidth=2, label='Poor Pools Dropout Rate')
    plt.title('Recall vs Poor Pools Dropout Rate')
    plt.ylabel('Percentage (%)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    
    plt.subplot(3, 1, 2)
    plt.plot(time_points, median_pct, 'o-', color='blue', linewidth=2, label='Median % of Final Potential')
    plt.axhline(y=100, color='red', linestyle='--', alpha=0.7)
    plt.axhline(y=80, color='orange', linestyle='--', alpha=0.7)
    plt.title('Median Percentage of Final Potential Visible')
    plt.ylabel('Percentage (%)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    
    plt.subplot(3, 1, 3)
    plt.plot(time_points, identification_rate, 'o-', color='orange', linewidth=2, 
             label='Identification Rate')
    plt.title('Identification Rate of Normal "Definitely Buy" Pools')
    plt.xlabel('Time Point (seconds)')
    plt.ylabel('Percentage (%)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    
    plt.tight_layout()
    plt.savefig(f"{output_prefix}_combined.png")
    logger.info(f"Combined insights plot saved to {output_prefix}_combined.png")
    
    # Plot 6: Raw counts
    plt.figure(figsize=(16, 8))
    
    total_identified = [results['accuracy_stats'][t]['identified'] for t in time_points]
    true_positives = [results['accuracy_stats'][t]['true_positives'] for t in time_points]
    false_positives = [results['accuracy_stats'][t]['false_positives'] for t in time_points]
    
    plt.bar(time_points, total_identified, color='blue', alpha=0.7, label='Total Identified')
    plt.bar(time_points, true_positives, color='green', alpha=0.7, label='True Positives')
    plt.bar(time_points, false_positives, color='red', alpha=0.7, label='False Positives')
    
    plt.axhline(y=results['total_definitely_buy'], color='black', linestyle='--', 
                label=f'Total "Definitely Buy" Pools ({results["total_definitely_buy"]})')
    
    plt.title('Raw Counts of Pool Identification')
    plt.xlabel('Time Point (seconds)')
    plt.ylabel('Number of Pools')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{output_prefix}_counts.png")
    logger.info(f"Raw counts plot saved to {output_prefix}_counts.png")

def main():
    """Main function to run the analysis"""
    parser = argparse.ArgumentParser(description='Analyze time windows for pool identification')
    parser.add_argument('--db-path', type=str, default='cache/pools.db',
                       help='Path to the SQLite database')
    parser.add_argument('--min-marketcap', type=float, default=40000,
                       help='Minimum market cap at first time point (10s)')
    parser.add_argument('--min-multiplier', type=float, default=7.0,
                       help='Minimum multiplier to qualify as a "Definitely Buy" pool')
    parser.add_argument('--outlier-percentile', type=float, default=95.0,
                       help='Percentile to cut off outliers (superpools)')
    parser.add_argument('--max-time-point', type=int, default=600,
                       help='Maximum time point to analyze (in seconds)')
    parser.add_argument('--output-prefix', type=str, default='time_window',
                       help='Prefix for output files')
    
    args = parser.parse_args()
    
    logger.info(f"Starting analysis using database: {args.db_path}")
    logger.info(f"Minimum market cap: {args.min_marketcap}")
    logger.info(f"Minimum multiplier for 'Definitely Buy': {args.min_multiplier}")
    logger.info(f"Outlier percentile: {args.outlier_percentile}")
    logger.info(f"Maximum time point: {args.max_time_point}")
    
    # Get all pools data
    df = get_all_pools_data(
        db_path=args.db_path,
        min_marketcap=args.min_marketcap,
        max_time_point=args.max_time_point
    )
    
    if len(df) == 0:
        logger.error("No pools found with the given criteria.")
        return
    
    logger.info(f"Found {len(df)} pools for analysis.")
    
    # Analyze time windows
    results = analyze_time_windows(
        df=df,
        min_multiplier=args.min_multiplier,
        outlier_percentile=args.outlier_percentile
    )
    
    # Plot analysis results
    plot_analysis_results(results, output_prefix=args.output_prefix)
    
    # Output summary to console
    logger.info("\nSummary of analysis:")
    logger.info(f"Total pools analyzed: {results['total_pools']}")
    logger.info(f"Total 'Definitely Buy' pools: {results['total_definitely_buy']} (multiplier >= {args.min_multiplier})")
    logger.info(f"Normal 'Definitely Buy' pools: {results['normal_definitely_buy']} (multiplier < {results['outlier_threshold']:.2f})")
    logger.info(f"Superpools: {results['superpools']} (multiplier >= {results['outlier_threshold']:.2f})")
    
    logger.info("\nAccuracy metrics by time point:")
    for t in results['time_points']:
        logger.info(f"{t}s: Precision: {results['accuracy_stats'][t]['precision']*100:.1f}% | " + 
                    f"Recall: {results['accuracy_stats'][t]['recall']*100:.1f}% | " +
                    f"F1: {results['accuracy_stats'][t]['f1_score']*100:.1f}%")
    
    logger.info("\nPoor pools dropout rate by time point:")
    for t in results['time_points']:
        logger.info(f"{t}s: {results['dropout_stats'][t]['poor_dropout_rate']*100:.1f}%")
    
    logger.info("\nPercentage of final potential visible by time point:")
    for t in results['time_points']:
        logger.info(f"{t}s: Avg: {results['timing_stats'][t]['avg_pct_of_final']:.1f}% | " + 
                    f"Median: {results['timing_stats'][t]['median_pct_of_final']:.1f}%")
    
    logger.info("\nIdentification rate of normal 'Definitely Buy' pools by time point:")
    for t in results['time_points']:
        logger.info(f"{t}s: {results['timing_stats'][t]['normal_identified_rate']*100:.1f}%")

if __name__ == "__main__":
    main() 