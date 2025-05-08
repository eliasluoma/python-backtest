#!/usr/bin/env python3
"""
Delay Impact Analyzer

Analyzes how delaying the purchase affects potential returns.
This helps to determine the optimal time window for buying pools.
"""

import sqlite3
import pandas as pd
import logging
import numpy as np
import matplotlib.pyplot as plt
import os
from typing import Dict, List, Tuple
import argparse
import csv

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
        
        # Convert string numeric fields to actual numeric types
        numeric_columns = [
            'marketCap', 'athMarketCap', 'minMarketCap', 
            'marketCapChange5s', 'marketCapChange10s', 'marketCapChange30s', 'marketCapChange60s',
            'maMarketCap10s', 'maMarketCap30s', 'maMarketCap60s',
            'currentPrice', 'priceChangeFromStart',
            'trade_last5Seconds_volume_buy', 'trade_last5Seconds_volume_sell', 'trade_last5Seconds_volume_bot',
            'trade_last10Seconds_volume_buy', 'trade_last10Seconds_volume_sell', 'trade_last10Seconds_volume_bot'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    except Exception as e:
        logger.error(f"Error fetching data for pool {pool_id}: {e}")
        return pd.DataFrame()

def analyze_delay_impact(db_path: str, time_points: List[int], min_marketcap: float = 40000, max_pools: int = None) -> Dict:
    """
    Analyze the impact of delaying purchase on potential returns.
    
    Args:
        db_path: Path to the SQLite database
        time_points: List of time points (in seconds/rows) to analyze
        min_marketcap: Minimum market cap to consider at first time point
        max_pools: Maximum number of pools to analyze
        
    Returns:
        Dictionary with analysis results
    """
    pool_ids = get_pool_ids(db_path, max_pools)
    logger.info(f"Analyzing delay impact for {len(pool_ids)} pools")
    
    # Sort time points to ensure they're in ascending order
    time_points = sorted(time_points)
    
    # Track results for each pool and time point
    results = []
    
    # Tracking variables for filtering stats
    total_pools_checked = 0
    pools_filtered_by_min_data = 0
    pools_filtered_by_min_marketcap = 0
    
    # Define category thresholds for final returns
    category_thresholds = {
        'do_not_buy': 3.0,    # Less than 3x return
        'maybe_buy': 7.0,     # Between 3x and 7x return
        'definitely_buy': float('inf')  # More than 7x return
    }
    
    # Process each pool
    for i, pool_id in enumerate(pool_ids):
        if (i + 1) % 10 == 0:
            logger.info(f"Progress: {i + 1}/{len(pool_ids)} pools processed")
        
        try:
            # Get pool data
            df = get_pool_data(db_path, pool_id)
            total_pools_checked += 1
            
            # Skip pools with insufficient data
            max_time_point = max(time_points)
            if len(df) <= max_time_point:
                pools_filtered_by_min_data += 1
                continue
            
            # Get market cap at first time point
            first_time_point = time_points[0]
            first_marketcap = df.iloc[first_time_point-1]['marketCap']  # -1 because index is 0-based
            
            # Skip pools with market cap less than minimum at first time point
            if first_marketcap < min_marketcap:
                pools_filtered_by_min_marketcap += 1
                continue
            
            # Get last row ATH market cap (maximum possible gain reference)
            last_row_ath_marketcap = df.iloc[-1]['athMarketCap']
            
            # Skip if final ATH is not higher than initial marketcap (no potential gain)
            if last_row_ath_marketcap <= first_marketcap:
                continue
            
            # Calculate potential returns at each time point
            pool_result = {'pool_id': pool_id}
            
            # Calculate maximum possible return (from first time point)
            max_potential_return = last_row_ath_marketcap / first_marketcap
            pool_result['max_potential_return'] = max_potential_return
            
            # Determine pool category based on max potential return
            if max_potential_return < category_thresholds['do_not_buy']:
                pool_category = 'do_not_buy'
            elif max_potential_return < category_thresholds['maybe_buy']:
                pool_category = 'maybe_buy'
            else:
                pool_category = 'definitely_buy'
            
            pool_result['category'] = pool_category
            
            # Calculate returns at each time point
            for time_point in time_points:
                if time_point <= len(df):
                    time_marketcap = df.iloc[time_point-1]['marketCap']
                    time_potential_return = last_row_ath_marketcap / time_marketcap
                    
                    # Calculate percentage of maximum return still available
                    if max_potential_return > 0:
                        percent_of_max_return = (time_potential_return / max_potential_return) * 100
                    else:
                        percent_of_max_return = 0
                    
                    pool_result[f'marketcap_at_{time_point}s'] = time_marketcap
                    pool_result[f'potential_return_at_{time_point}s'] = time_potential_return
                    pool_result[f'percent_of_max_return_at_{time_point}s'] = percent_of_max_return
            
            # Add additional early indicators to analyze correlation
            if first_time_point < len(df):
                # Add key performance indicators at the first time point
                first_point_data = df.iloc[first_time_point-1]
                
                # Market cap changes
                pool_result['marketcap_change_5s_at_first'] = first_point_data.get('marketCapChange5s', None)
                pool_result['marketcap_change_10s_at_first'] = first_point_data.get('marketCapChange10s', None)
                pool_result['marketcap_change_30s_at_first'] = first_point_data.get('marketCapChange30s', None)
                
                # Holder metrics
                pool_result['holders_count_at_first'] = first_point_data.get('holdersCount', None)
                pool_result['holder_delta_5s_at_first'] = first_point_data.get('holderDelta5s', None)
                pool_result['holder_delta_10s_at_first'] = first_point_data.get('holderDelta10s', None)
                
                # Volume metrics
                pool_result['buy_volume_5s_at_first'] = first_point_data.get('buyVolume5s', None)
                pool_result['buy_volume_10s_at_first'] = first_point_data.get('buyVolume10s', None)
                pool_result['net_volume_5s_at_first'] = first_point_data.get('netVolume5s', None)
                
                # Buy classification
                pool_result['large_buy_5s_at_first'] = first_point_data.get('largeBuy5s', None)
                pool_result['big_buy_5s_at_first'] = first_point_data.get('bigBuy5s', None)
                pool_result['super_buy_5s_at_first'] = first_point_data.get('superBuy5s', None)
            
            results.append(pool_result)
            
        except Exception as e:
            logger.error(f"Error analyzing pool {pool_id}: {e}")
    
    # Calculate average percent of max return for each category and time point
    summary = {}
    categories = ['do_not_buy', 'maybe_buy', 'definitely_buy', 'overall']
    
    for category in categories:
        summary[category] = {}
        
        if category == 'overall':
            category_results = results  # All results
        else:
            category_results = [r for r in results if r.get('category') == category]
        
        if not category_results:
            continue
            
        # Count pools in this category
        summary[category]['count'] = len(category_results)
        
        # Calculate average max potential return
        max_returns = [r.get('max_potential_return', 0) for r in category_results]
        summary[category]['avg_max_potential_return'] = np.mean(max_returns)
        
        # Calculate statistics for each time point
        for time_point in time_points:
            percent_key = f'percent_of_max_return_at_{time_point}s'
            return_key = f'potential_return_at_{time_point}s'
            
            percent_values = [r.get(percent_key, 0) for r in category_results if percent_key in r]
            return_values = [r.get(return_key, 0) for r in category_results if return_key in r]
            
            if percent_values:
                summary[category][f'avg_{percent_key}'] = np.mean(percent_values)
                summary[category][f'median_{percent_key}'] = np.median(percent_values)
                
            if return_values:
                summary[category][f'avg_{return_key}'] = np.mean(return_values)
                summary[category][f'median_{return_key}'] = np.median(return_values)
    
    # Calculate correlation between early indicators and max potential return
    correlations = {}
    early_indicators = [
        'marketcap_change_5s_at_first', 'marketcap_change_10s_at_first', 'marketcap_change_30s_at_first',
        'holders_count_at_first', 'holder_delta_5s_at_first', 'holder_delta_10s_at_first',
        'buy_volume_5s_at_first', 'buy_volume_10s_at_first', 'net_volume_5s_at_first',
        'large_buy_5s_at_first', 'big_buy_5s_at_first', 'super_buy_5s_at_first'
    ]
    
    for indicator in early_indicators:
        valid_results = [(r.get(indicator, None), r.get('max_potential_return', None)) 
                        for r in results 
                        if indicator in r and r.get(indicator) is not None and r.get('max_potential_return') is not None]
        
        if valid_results:
            x = [v[0] for v in valid_results]
            y = [v[1] for v in valid_results]
            
            if len(x) > 1 and len(set(x)) > 1:  # Need at least 2 different values for correlation
                corr = np.corrcoef(x, y)[0, 1]
                correlations[indicator] = corr
    
    return {
        'time_points': time_points,
        'total_pools_analyzed': len(pool_ids),
        'pools_with_data': total_pools_checked - pools_filtered_by_min_data - pools_filtered_by_min_marketcap,
        'pools_filtered_by_min_data': pools_filtered_by_min_data,
        'pools_filtered_by_min_marketcap': pools_filtered_by_min_marketcap,
        'category_counts': {
            'do_not_buy': len([r for r in results if r.get('category') == 'do_not_buy']),
            'maybe_buy': len([r for r in results if r.get('category') == 'maybe_buy']),
            'definitely_buy': len([r for r in results if r.get('category') == 'definitely_buy'])
        },
        'detailed_results': results,
        'summary': summary,
        'early_indicator_correlations': correlations
    }

def plot_delay_impact(results: Dict, output_path: str = 'delay_impact.png'):
    """
    Plot the impact of delay on potential returns for each category.
    
    Args:
        results: Results dictionary from analyze_delay_impact
        output_path: Path to save the plot
    """
    time_points = results['time_points']
    summary = results['summary']
    
    plt.figure(figsize=(14, 10))
    
    # Plot 1: Percentage of maximum return over time by category
    plt.subplot(2, 1, 1)
    
    categories = ['do_not_buy', 'maybe_buy', 'definitely_buy', 'overall']
    colors = ['red', 'orange', 'green', 'blue']
    
    for i, category in enumerate(categories):
        if category in summary:
            values = [summary[category].get(f'avg_percent_of_max_return_at_{t}s', 100) for t in time_points]
            plt.plot(time_points, values, marker='o', color=colors[i], label=category.replace('_', ' ').title())
    
    plt.title('Percentage of Maximum Return Available Over Time')
    plt.xlabel('Time Point (seconds/rows)')
    plt.ylabel('Percentage of Maximum Return (%)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    
    # Plot 2: Actual potential returns over time by category
    plt.subplot(2, 1, 2)
    
    for i, category in enumerate(categories):
        if category in summary:
            values = [summary[category].get(f'avg_potential_return_at_{t}s', 0) for t in time_points]
            plt.plot(time_points, values, marker='o', color=colors[i], label=category.replace('_', ' ').title())
    
    plt.title('Potential Return Multiple Over Time')
    plt.xlabel('Time Point (seconds/rows)')
    plt.ylabel('Return Multiple (x)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    
    plt.tight_layout()
    plt.savefig(output_path)
    logger.info(f"Delay impact plot saved to {output_path}")
    
    # Create correlation plot
    if 'early_indicator_correlations' in results and results['early_indicator_correlations']:
        plot_correlations(results['early_indicator_correlations'], 
                          output_path=output_path.replace('.png', '_correlations.png'))

def plot_correlations(correlations: Dict, output_path: str = 'correlations.png'):
    """
    Plot the correlations between early indicators and maximum potential return.
    
    Args:
        correlations: Dictionary of correlation values
        output_path: Path to save the plot
    """
    # Sort correlations by absolute value
    sorted_correlations = sorted(correlations.items(), key=lambda x: abs(x[1]), reverse=True)
    
    indicators = [item[0].replace('_at_first', '').replace('_', ' ').title() for item in sorted_correlations]
    values = [item[1] for item in sorted_correlations]
    
    plt.figure(figsize=(12, 8))
    
    # Create horizontal bar chart
    bars = plt.barh(indicators, values, color=['green' if v > 0 else 'red' for v in values])
    
    # Add value labels
    for bar in bars:
        width = bar.get_width()
        plt.text(width * 1.01, bar.get_y() + bar.get_height()/2, 
                 f'{width:.3f}', va='center')
    
    plt.title('Correlation Between Early Indicators and Maximum Potential Return')
    plt.xlabel('Correlation Coefficient')
    plt.axvline(x=0, color='black', linestyle='-', alpha=0.3)
    plt.grid(True, linestyle='--', axis='x', alpha=0.7)
    
    plt.tight_layout()
    plt.savefig(output_path)
    logger.info(f"Correlation plot saved to {output_path}")

def save_to_csv(results: Dict, output_path: str = 'delay_analysis.csv'):
    """
    Save detailed results to a CSV file for further analysis.
    
    Args:
        results: Results dictionary from analyze_delay_impact
        output_path: Path to save the CSV file
    """
    if not results['detailed_results']:
        logger.warning("No detailed results to save to CSV")
        return
    
    try:
        # Get all possible fields from results
        all_fields = set()
        for result in results['detailed_results']:
            all_fields.update(result.keys())
        
        # Order fields logically
        priority_fields = ['pool_id', 'category', 'max_potential_return']
        time_point_fields = []
        indicator_fields = []
        
        for field in all_fields:
            if field in priority_fields:
                continue
            elif 'at_' in field and any(f'_{t}s' in field for t in results['time_points']):
                time_point_fields.append(field)
            else:
                indicator_fields.append(field)
        
        # Sort time point fields by time point and then by field type
        def get_time_point(field):
            try:
                # Extract digits from the last part of the field name
                digit_part = ''.join(filter(str.isdigit, field.split('_')[-1]))
                if digit_part:
                    return int(digit_part)
                return 0
            except (ValueError, IndexError):
                return 0
                
        def get_field_type(field):
            try:
                return field.split('_at_')[0]
            except (ValueError, IndexError):
                return field
                
        time_point_fields.sort(key=lambda x: (get_time_point(x), get_field_type(x)))
        
        # Combine all fields in logical order
        ordered_fields = priority_fields + time_point_fields + indicator_fields
        
        # Write to CSV
        with open(output_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=ordered_fields)
            writer.writeheader()
            for result in results['detailed_results']:
                writer.writerow(result)
        
        logger.info(f"Detailed results saved to {output_path}")
    
    except Exception as e:
        logger.error(f"Error saving to CSV: {e}")

def display_results(results: Dict):
    """
    Display analysis results to the console.
    
    Args:
        results: Results dictionary from analyze_delay_impact
    """
    time_points = results['time_points']
    
    logger.info(f"Analysis complete!")
    logger.info(f"Total pools analyzed: {results['total_pools_analyzed']}")
    logger.info(f"Pools with sufficient data: {results['pools_with_data']}")
    logger.info(f"Pools filtered due to insufficient data: {results['pools_filtered_by_min_data']}")
    logger.info(f"Pools filtered due to minimum marketcap requirement: {results['pools_filtered_by_min_marketcap']}")
    
    logger.info("\nPool categories:")
    for category, count in results['category_counts'].items():
        logger.info(f"  {category.replace('_', ' ').title()}: {count} pools")
    
    logger.info("\nDelay impact summary:")
    categories = ['do_not_buy', 'maybe_buy', 'definitely_buy', 'overall']
    
    for category in categories:
        if category in results['summary']:
            logger.info(f"\n{category.replace('_', ' ').title()} ({results['summary'][category].get('count', 0)} pools):")
            logger.info(f"  Average maximum potential return: {results['summary'][category].get('avg_max_potential_return', 0):.2f}x")
            
            logger.info("  Percentage of maximum return available:")
            for time_point in time_points:
                percent_key = f'avg_percent_of_max_return_at_{time_point}s'
                if percent_key in results['summary'][category]:
                    logger.info(f"    At {time_point}s: {results['summary'][category][percent_key]:.1f}%")
            
            logger.info("  Average potential return multiple:")
            for time_point in time_points:
                return_key = f'avg_potential_return_at_{time_point}s'
                if return_key in results['summary'][category]:
                    logger.info(f"    At {time_point}s: {results['summary'][category][return_key]:.2f}x")
    
    if 'early_indicator_correlations' in results and results['early_indicator_correlations']:
        logger.info("\nTop correlations with maximum potential return:")
        sorted_correlations = sorted(
            results['early_indicator_correlations'].items(), 
            key=lambda x: abs(x[1]), 
            reverse=True
        )
        
        for indicator, correlation in sorted_correlations[:10]:  # Show top 10
            logger.info(f"  {indicator.replace('_at_first', '').replace('_', ' ').title()}: {correlation:.3f}")

def main():
    """Main function to run the analysis"""
    parser = argparse.ArgumentParser(description='Analyze how delaying purchases affects potential returns')
    parser.add_argument('--db-path', type=str, default='cache/pools.db', 
                        help='Path to the SQLite database')
    parser.add_argument('--max-pools', type=int, default=None,
                        help='Maximum number of pools to analyze')
    parser.add_argument('--min-marketcap', type=float, default=40000,
                        help='Minimum market cap at first time point')
    parser.add_argument('--time-points', type=str, default='10,20,30,60,120,180',
                        help='Comma-separated list of time points (in seconds/rows) to analyze')
    parser.add_argument('--plot', action='store_true',
                        help='Generate plots of the results')
    parser.add_argument('--output-prefix', type=str, default='delay_analysis',
                        help='Prefix for output files')
    parser.add_argument('--save-csv', action='store_true',
                        help='Save detailed results to CSV')
    
    args = parser.parse_args()
    
    # Parse time points
    time_points = [int(t) for t in args.time_points.split(',')]
    
    logger.info(f"Starting analysis using database: {args.db_path}")
    logger.info(f"Analyzing maximum of {args.max_pools if args.max_pools else 'all'} pools")
    logger.info(f"Minimum market cap: {args.min_marketcap}")
    logger.info(f"Time points: {time_points}")
    
    # Run analysis
    results = analyze_delay_impact(
        db_path=args.db_path,
        time_points=time_points,
        min_marketcap=args.min_marketcap,
        max_pools=args.max_pools
    )
    
    # Display results
    display_results(results)
    
    # Generate plots if requested
    if args.plot:
        plot_delay_impact(results, output_path=f"{args.output_prefix}_plot.png")
    
    # Save to CSV if requested
    if args.save_csv:
        save_to_csv(results, output_path=f"{args.output_prefix}.csv")

if __name__ == "__main__":
    main() 