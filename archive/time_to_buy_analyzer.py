#!/usr/bin/env python3
"""
Time to Buy Analyzer for "Definitely Buy" Pools

First identifies pools that eventually reach at least 7x returns (Definitely Buy),
then analyzes how much potential return is lost if purchase is delayed to later time points.
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

def get_definitely_buy_pools(db_path: str, min_marketcap: float = 40000, 
                           min_multiplier: float = 7.0, max_time_point: int = 600,
                           max_multiplier: float = 1000.0,
                           max_marketcap: float = 50000000.0,
                           spike_threshold: float = 5.0,
                           window_size: int = 5,
                           use_median_filter: bool = True,
                           detect_down_spikes: bool = True,
                           excluded_pools: List[str] = None) -> pd.DataFrame:
    """
    Get data for all pools that qualify as "Definitely Buy" (reach at least min_multiplier returns).
    
    Args:
        db_path: Path to the SQLite database
        min_marketcap: Minimum market cap to be considered at 10s
        min_multiplier: Minimum multiplier to qualify as "Definitely Buy"
        max_time_point: Maximum time point to be analyzed
        max_multiplier: Maximum reasonable multiplier (used for logging unrealistic values, not filtering)
        max_marketcap: Maximum reasonable market cap in USD (used for logging unrealistic values, not filtering)
        spike_threshold: Threshold for detecting unrealistic spikes (e.g., 5.0 means 500% increase)
        window_size: Size of the window for rolling median calculation
        use_median_filter: Whether to use median filter for additional spike detection
        detect_down_spikes: Whether to detect and clean downward spikes as well
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
        
        # Define time points for analysis
        time_points = [10, 15, 20, 25, 30, 40, 50, 60, 90, 120, 150, 180, 240, 300, 360, 420, 480, 540, 600]
        
        # Filter out time points beyond the max_time_point
        time_points = [t for t in time_points if t <= max_time_point]
        
        logger.info(f"Analyzing {len(pool_ids)} pools with time points: {time_points}")
        logger.info(f"Analysis criteria: min_marketcap=${min_marketcap}, " + 
                   f"min_multiplier={min_multiplier}x, spike_threshold={spike_threshold}x")
        logger.info(f"Spike detection: window_size={window_size}, use_median_filter={use_median_filter}, " +
                   f"detect_down_spikes={detect_down_spikes}")
        logger.info(f"Unrealistic value thresholds (for reporting only): max_multiplier={max_multiplier}x, " +
                   f"max_marketcap=${max_marketcap}")
        
        pools_processed = 0
        pools_with_unrealistic_marketcap = []
        pools_with_unrealistic_multiplier = []
        pools_with_sufficient_data = 0
        pools_with_spikes = []
        
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
            
            pools_with_sufficient_data += 1
                
            # Skip pools with market cap below threshold at 10s
            initial_marketcap = float(df.iloc[9]['marketCap'])
            if initial_marketcap < min_marketcap:
                continue
            
            # Clean market cap data by removing unrealistic spikes using multiple techniques
            cleaned_marketcap = df['marketCap'].copy()
            spikes_detected = False
            spike_details = []
            
            # Calculate rolling median if using median filter
            if use_median_filter and len(df) >= window_size:
                rolling_median = cleaned_marketcap.rolling(window=window_size, center=True).median()
                # Fill NaN values at the edges with nearest non-NaN values
                rolling_median = rolling_median.fillna(method='ffill').fillna(method='bfill')
            else:
                rolling_median = None
                
            # First pass: Detect and clean individual spikes
            for i in range(1, len(df) - 1):
                current_value = cleaned_marketcap.iloc[i]
                prev_value = cleaned_marketcap.iloc[i-1]
                next_value = cleaned_marketcap.iloc[i+1]
                
                # Skip NaN values
                if pd.isna(current_value) or pd.isna(prev_value) or pd.isna(next_value):
                    continue
                
                # Check if this is a spike based on neighbors
                is_up_spike = (current_value > prev_value * spike_threshold and 
                               current_value > next_value * spike_threshold)
                
                is_down_spike = False
                if detect_down_spikes:
                    # Check for downward spikes (value much lower than neighbors)
                    is_down_spike = (current_value * spike_threshold < prev_value and 
                                     current_value * spike_threshold < next_value)
                
                # Check against rolling median if enabled
                is_median_outlier = False
                if use_median_filter and rolling_median is not None:
                    median_at_point = rolling_median.iloc[i]
                    if not pd.isna(median_at_point):
                        is_median_outlier = (current_value > median_at_point * spike_threshold) or \
                                            (detect_down_spikes and current_value * spike_threshold < median_at_point)
                
                # If any spike detection method triggers, clean the spike
                if is_up_spike or is_down_spike or is_median_outlier:
                    # Use the most appropriate replacement value
                    if is_median_outlier and rolling_median is not None and not pd.isna(rolling_median.iloc[i]):
                        # Use rolling median as replacement
                        replacement_value = rolling_median.iloc[i]
                    else:
                        # Use average of neighbors
                        replacement_value = (prev_value + next_value) / 2
                    
                    spike_details.append({
                        'time_index': i,
                        'original_value': current_value,
                        'replacement_value': replacement_value,
                        'ratio_to_prev': current_value / prev_value,
                        'ratio_to_next': current_value / next_value,
                        'is_up_spike': is_up_spike,
                        'is_down_spike': is_down_spike,
                        'is_median_outlier': is_median_outlier
                    })
                    
                    cleaned_marketcap.iloc[i] = replacement_value
                    spikes_detected = True
            
            # Second pass: Detect consecutive spikes or sudden trend changes
            if len(cleaned_marketcap) >= 3:
                # Calculate percentage changes between consecutive points
                pct_changes = cleaned_marketcap.pct_change().abs()
                
                # Get the median percentage change (excluding zeros)
                non_zero_changes = pct_changes[pct_changes > 0]
                if len(non_zero_changes) > 0:
                    median_pct_change = non_zero_changes.median()
                    
                    # Look for sequences of abnormal changes
                    abnormal_threshold = median_pct_change * 10  # 10x the median change
                    
                    for i in range(1, len(cleaned_marketcap) - 1):
                        if i not in [s['time_index'] for s in spike_details]:  # Skip already identified spikes
                            current_change = pct_changes.iloc[i]
                            if current_change > abnormal_threshold:
                                # Check if the next point also has a large change in the opposite direction
                                if i + 1 < len(pct_changes) and pct_changes.iloc[i+1] > abnormal_threshold:
                                    # This might be part of a spike sequence
                                    current_value = cleaned_marketcap.iloc[i]
                                    
                                    # Find nearest stable points
                                    left_idx = i - 1
                                    while left_idx >= 0 and pct_changes.iloc[left_idx] > abnormal_threshold:
                                        left_idx -= 1
                                    
                                    right_idx = i + 1
                                    while right_idx < len(pct_changes) and pct_changes.iloc[right_idx] > abnormal_threshold:
                                        right_idx += 1
                                    
                                    # Only proceed if we found stable points on both sides
                                    if left_idx >= 0 and right_idx < len(cleaned_marketcap):
                                        left_value = cleaned_marketcap.iloc[left_idx]
                                        right_value = cleaned_marketcap.iloc[right_idx]
                                        
                                        # Linear interpolation between stable points
                                        total_distance = right_idx - left_idx
                                        position = i - left_idx
                                        replacement_value = left_value + (right_value - left_value) * (position / total_distance)
                                        
                                        spike_details.append({
                                            'time_index': i,
                                            'original_value': current_value,
                                            'replacement_value': replacement_value,
                                            'is_trend_anomaly': True
                                        })
                                        
                                        cleaned_marketcap.iloc[i] = replacement_value
                                        spikes_detected = True
            
            if spikes_detected:
                pools_with_spikes.append({
                    'pool_id': pool_id,
                    'num_spikes': len(spike_details),
                    'spike_details': spike_details
                })
                
            # Calculate the ATH market cap from the cleaned data
            ath_marketcap = cleaned_marketcap.max()
            
            # Log pools with unrealistically high market cap, but don't filter them out
            if ath_marketcap > max_marketcap:
                pools_with_unrealistic_marketcap.append({
                    'pool_id': pool_id,
                    'ath_marketcap': ath_marketcap,
                    'ratio_to_max': ath_marketcap / max_marketcap
                })
            
            # Calculate final multiplier (from 10s to ATH)
            final_multiplier = ath_marketcap / initial_marketcap
            
            # Log pools with unrealistically high multiplier, but don't filter them out
            if final_multiplier > max_multiplier:
                pools_with_unrealistic_multiplier.append({
                    'pool_id': pool_id,
                    'final_multiplier': final_multiplier,
                    'initial_marketcap': initial_marketcap,
                    'ath_marketcap': ath_marketcap,
                    'ratio_to_max': final_multiplier / max_multiplier
                })
            
            # Only keep pools that qualify as "Definitely Buy"
            if final_multiplier < min_multiplier:
                continue
            
            pools_processed += 1
                
            # This pool is a "Definitely Buy" pool
            # Add pool data
            pool_data = {
                'pool_id': pool_id,
                'initial_marketcap': initial_marketcap,
                'ath_marketcap': ath_marketcap,
                'final_multiplier': final_multiplier,
                'had_spikes': spikes_detected,
                'num_spikes': len(spike_details) if spikes_detected else 0,
                'unrealistic_marketcap': ath_marketcap > max_marketcap,
                'unrealistic_multiplier': final_multiplier > max_multiplier
            }
            
            # Add market cap at each time point
            for t in time_points:
                if t-1 < len(df):
                    # Market cap at this time point (use cleaned value)
                    time_index = t-1
                    time_marketcap = float(cleaned_marketcap.iloc[time_index])
                    pool_data[f'marketcap_{t}s'] = time_marketcap
                    
                    # Calculate what the multiplier would be if purchased at this time point
                    # This is ATH market cap / market cap at time t
                    purchase_multiplier = ath_marketcap / time_marketcap
                    
                    # Don't cap the multiplier, but log it if it's unrealistic
                    pool_data[f'purchase_multiplier_{t}s'] = purchase_multiplier
                    
                    # Calculate percentage of optimal return retained
                    # If purchased at 10s, the return is final_multiplier
                    # If purchased at time t, the return is purchase_multiplier
                    # The percentage retained is (purchase_multiplier / final_multiplier) * 100
                    pct_retained = (purchase_multiplier / final_multiplier) * 100
                    pool_data[f'pct_retained_{t}s'] = pct_retained
                    
                    # Also store whether we gained or lost by waiting
                    gain_by_waiting = purchase_multiplier > final_multiplier
                    pool_data[f'gain_by_waiting_{t}s'] = gain_by_waiting
            
            definitely_buy_pools_data.append(pool_data)
        
        conn.close()
        
        logger.info(f"Processed {pools_with_sufficient_data} pools with sufficient data")
        logger.info(f"Detected and cleaned spikes in {len(pools_with_spikes)} pools")
        logger.info(f"Identified {len(pools_with_unrealistic_marketcap)} pools with unrealistic market cap (>{max_marketcap})")
        logger.info(f"Identified {len(pools_with_unrealistic_multiplier)} pools with unrealistic multiplier (>{max_multiplier})")
        logger.info(f"Final 'Definitely Buy' pools for analysis: {pools_processed}")
        
        # Log details of pools with spikes
        if len(pools_with_spikes) > 0:
            logger.info("\nPools with spikes:")
            for i, pool_data in enumerate(pools_with_spikes[:10]):  # Limit to first 10 for brevity
                pool_id = pool_data['pool_id']
                num_spikes = pool_data['num_spikes']
                logger.info(f"{i+1}. Pool {pool_id}: {num_spikes} spikes detected")
                # Log first 3 spikes at most
                try:
                    for j, spike in enumerate(pool_data['spike_details'][:3]):
                        spike_type = []
                        if 'is_up_spike' in spike and spike['is_up_spike']:
                            spike_type.append("up spike")
                        if 'is_down_spike' in spike and spike['is_down_spike']:
                            spike_type.append("down spike")
                        if 'is_median_outlier' in spike and spike['is_median_outlier']:
                            spike_type.append("median outlier")
                        if 'is_trend_anomaly' in spike and spike['is_trend_anomaly']:
                            spike_type.append("trend anomaly")
                        
                        spike_type_str = ", ".join(spike_type) if spike_type else "unknown"
                        
                        # Construct log message based on available fields
                        log_msg = f"   Spike {j+1}: Index {spike['time_index']}, " + \
                                 f"Value: ${spike['original_value']:.2f} → ${spike['replacement_value']:.2f}"
                        
                        # Add ratio information if available
                        if 'ratio_to_prev' in spike and 'ratio_to_next' in spike:
                            if spike['ratio_to_prev'] is not None and spike['ratio_to_next'] is not None:
                                log_msg += f", Ratio to prev: {spike['ratio_to_prev']:.2f}x, " + \
                                          f"Ratio to next: {spike['ratio_to_next']:.2f}x"
                        
                        # Add spike type
                        log_msg += f" (Type: {spike_type_str})"
                        
                        logger.info(log_msg)
                except Exception as e:
                    logger.warning(f"   Could not log spike details for pool {pool_id}: {str(e)}")
            if len(pools_with_spikes) > 10:
                logger.info(f"   ... and {len(pools_with_spikes) - 10} more pools with spikes")
                
        # Log details of pools with unrealistic market cap
        if len(pools_with_unrealistic_marketcap) > 0:
            logger.info("\nPools with unrealistic market cap:")
            for i, pool_data in enumerate(pools_with_unrealistic_marketcap[:10]):  # Limit to first 10 for brevity
                logger.info(f"{i+1}. Pool {pool_data['pool_id']}: " +
                           f"ATH Market Cap: ${pool_data['ath_marketcap']:.2f}, " +
                           f"{pool_data['ratio_to_max']:.2f}x above threshold")
            if len(pools_with_unrealistic_marketcap) > 10:
                logger.info(f"   ... and {len(pools_with_unrealistic_marketcap) - 10} more pools with unrealistic market cap")
                
        # Log details of pools with unrealistic multiplier
        if len(pools_with_unrealistic_multiplier) > 0:
            logger.info("\nPools with unrealistic multiplier:")
            for i, pool_data in enumerate(pools_with_unrealistic_multiplier[:10]):  # Limit to first 10 for brevity
                logger.info(f"{i+1}. Pool {pool_data['pool_id']}: " +
                           f"Multiplier: {pool_data['final_multiplier']:.2f}x, " +
                           f"Initial: ${pool_data['initial_marketcap']:.2f}, " +
                           f"ATH: ${pool_data['ath_marketcap']:.2f}, " +
                           f"{pool_data['ratio_to_max']:.2f}x above threshold")
            if len(pools_with_unrealistic_multiplier) > 10:
                logger.info(f"   ... and {len(pools_with_unrealistic_multiplier) - 10} more pools with unrealistic multiplier")
        
        # Convert to DataFrame
        return pd.DataFrame(definitely_buy_pools_data)
    
    except Exception as e:
        logger.error(f"Error fetching pool data: {e}")
        return pd.DataFrame()

def analyze_purchase_timing(df: pd.DataFrame, outlier_percentile: float = 95.0) -> Dict:
    """
    Analyze how purchase timing affects returns for "Definitely Buy" pools.
    
    Args:
        df: DataFrame containing "Definitely Buy" pools data
        outlier_percentile: Percentile to cut off outliers (superpools)
        
    Returns:
        Dictionary with analysis results
    """
    # Get all time points from column names
    time_points = []
    for col in df.columns:
        if col.startswith('purchase_multiplier_') and col.endswith('s'):
            # Extract the time point from column name (e.g., 'purchase_multiplier_10s' -> 10)
            time_point = int(col.replace('purchase_multiplier_', '').replace('s', ''))
            time_points.append(time_point)
    
    time_points.sort()
    
    # Count pools with special characteristics
    pools_with_spikes = df[df['had_spikes'] == True]
    pools_with_unrealistic_marketcap = df[df['unrealistic_marketcap'] == True]
    pools_with_unrealistic_multiplier = df[df['unrealistic_multiplier'] == True]
    
    logger.info(f"\nPool characteristics in the analyzed set:")
    logger.info(f"Pools with cleaned spikes: {len(pools_with_spikes)} ({len(pools_with_spikes)/len(df)*100:.1f}%)")
    logger.info(f"Pools with unrealistic market cap: {len(pools_with_unrealistic_marketcap)} ({len(pools_with_unrealistic_marketcap)/len(df)*100:.1f}%)")
    logger.info(f"Pools with unrealistic multiplier: {len(pools_with_unrealistic_multiplier)} ({len(pools_with_unrealistic_multiplier)/len(df)*100:.1f}%)")
    
    # Identify outliers based on percentile of final_multiplier
    outlier_threshold = np.percentile(df['final_multiplier'], outlier_percentile)
    normal_pools = df[df['final_multiplier'] < outlier_threshold]
    superpools = df[df['final_multiplier'] >= outlier_threshold]
    
    logger.info(f"Total 'Definitely Buy' pools: {len(df)}")
    logger.info(f"Normal 'Definitely Buy' pools: {len(normal_pools)} (multiplier < {outlier_threshold:.2f})")
    logger.info(f"Superpools: {len(superpools)} (multiplier >= {outlier_threshold:.2f})")
    
    # Initialize results
    results = {
        'time_points': time_points,
        'total_pools': len(df),
        'normal_pools': len(normal_pools),
        'superpools': len(superpools),
        'outlier_threshold': outlier_threshold,
        'roi_stats': {},
        'timing_stats': {}
    }
    
    # For each time point, analyze the ROI if purchased at that time
    for t in time_points:
        # Get purchase multiplier at this time point
        multiplier_col = f'purchase_multiplier_{t}s'
        pct_retained_col = f'pct_retained_{t}s'
        gain_by_waiting_col = f'gain_by_waiting_{t}s'
        
        # Normal pools stats
        normal_multipliers = normal_pools[multiplier_col]
        normal_pct_retained = normal_pools[pct_retained_col]
        normal_gain_by_waiting = normal_pools[gain_by_waiting_col].sum() / len(normal_pools)
        
        # Calculate ROI statistics for normal pools
        results['roi_stats'][t] = {
            'avg_multiplier': normal_multipliers.mean(),
            'median_multiplier': normal_multipliers.median(),
            'min_multiplier': normal_multipliers.min(),
            'max_multiplier': normal_multipliers.max(),
            'avg_pct_retained': normal_pct_retained.mean(),
            'median_pct_retained': normal_pct_retained.median(),
            'min_pct_retained': normal_pct_retained.min(),
            'max_pct_retained': normal_pct_retained.max(),
            'pct_pools_gained': normal_gain_by_waiting * 100  # percentage of pools that gained by waiting
        }
        
        # Analyze distribution of percentage retained
        pct_retained_bins = [0, 20, 40, 60, 80, 90, 95, 100, float('inf')]
        pct_retained_counts = []
        
        for i in range(len(pct_retained_bins) - 1):
            lower = pct_retained_bins[i]
            upper = pct_retained_bins[i+1]
            count = len(normal_pools[(normal_pct_retained >= lower) & (normal_pct_retained < upper)])
            pct_retained_counts.append(count)
        
        # Calculate percentages
        pct_retained_pcts = [count / len(normal_pools) * 100 for count in pct_retained_counts]
        
        results['timing_stats'][t] = {
            'pct_retained_bins': pct_retained_bins[:-1],  # Exclude the infinity bin
            'pct_retained_counts': pct_retained_counts,
            'pct_retained_pcts': pct_retained_pcts
        }
    
    return results

def plot_analysis_results(results: Dict, output_prefix: str = 'time_to_buy'):
    """
    Generate plots to visualize the analysis results.
    
    Args:
        results: Results dictionary from analyze_purchase_timing
        output_prefix: Prefix for output files
    """
    time_points = results['time_points']
    
    # Plot 1: ROI Multipliers over time
    plt.figure(figsize=(16, 8))
    
    avg_multiplier = [results['roi_stats'][t]['avg_multiplier'] for t in time_points]
    median_multiplier = [results['roi_stats'][t]['median_multiplier'] for t in time_points]
    
    plt.plot(time_points, avg_multiplier, 'o-', color='blue', linewidth=2, label='Average Multiplier')
    plt.plot(time_points, median_multiplier, 'o-', color='green', linewidth=2, label='Median Multiplier')
    
    # Add annotations for local minima in median
    median_array = np.array(median_multiplier)
    for i in range(1, len(median_array)-1):
        if median_array[i] < median_array[i-1] and median_array[i] < median_array[i+1]:
            plt.annotate(f'{median_array[i]:.1f}x', 
                        xy=(time_points[i], median_array[i]),
                        xytext=(time_points[i], median_array[i]-1),
                        arrowprops=dict(facecolor='black', shrink=0.05, width=1.5, headwidth=8),
                        ha='center', fontsize=10)
    
    plt.title('Return Multiplier if Purchased at Different Time Points')
    plt.xlabel('Purchase Time (seconds)')
    plt.ylabel('Return Multiplier (x)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{output_prefix}_multiplier.png")
    logger.info(f"Multiplier plot saved to {output_prefix}_multiplier.png")
    
    # Plot 2: Percentage of optimal return retained
    plt.figure(figsize=(16, 8))
    
    avg_pct_retained = [results['roi_stats'][t]['avg_pct_retained'] for t in time_points]
    median_pct_retained = [results['roi_stats'][t]['median_pct_retained'] for t in time_points]
    
    plt.plot(time_points, avg_pct_retained, 'o-', color='blue', linewidth=2, label='Average % Retained')
    plt.plot(time_points, median_pct_retained, 'o-', color='green', linewidth=2, label='Median % Retained')
    plt.axhline(y=100, color='red', linestyle='--', alpha=0.7, label='Optimal (Purchase at 10s)')
    plt.axhline(y=80, color='orange', linestyle='--', alpha=0.7, label='80% Retained')
    
    # Add annotations for significant drops
    median_retained_array = np.array(median_pct_retained)
    for i in range(1, len(median_retained_array)-1):
        if (median_retained_array[i] < median_retained_array[i-1] - 5 and 
            median_retained_array[i] < median_retained_array[i+1]):
            plt.annotate(f'{median_retained_array[i]:.1f}%', 
                        xy=(time_points[i], median_retained_array[i]),
                        xytext=(time_points[i], median_retained_array[i]-5),
                        arrowprops=dict(facecolor='black', shrink=0.05, width=1.5, headwidth=8),
                        ha='center', fontsize=10)
    
    plt.title('Percentage of Optimal Return Retained if Purchased Later')
    plt.xlabel('Purchase Time (seconds)')
    plt.ylabel('Percentage of Optimal Return (%)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{output_prefix}_pct_retained.png")
    logger.info(f"Percentage retained plot saved to {output_prefix}_pct_retained.png")
    
    # Plot 3: Percentage of pools that gained by waiting
    plt.figure(figsize=(16, 8))
    
    pct_pools_gained = [results['roi_stats'][t]['pct_pools_gained'] for t in time_points]
    
    plt.plot(time_points, pct_pools_gained, 'o-', color='purple', linewidth=2, markersize=8)
    
    # Add annotations for local maxima
    pct_gained_array = np.array(pct_pools_gained)
    for i in range(1, len(pct_gained_array)-1):
        if pct_gained_array[i] > pct_gained_array[i-1] and pct_gained_array[i] > pct_gained_array[i+1]:
            plt.annotate(f'{pct_gained_array[i]:.1f}%', 
                        xy=(time_points[i], pct_gained_array[i]),
                        xytext=(time_points[i], pct_gained_array[i]+3),
                        arrowprops=dict(facecolor='black', shrink=0.05, width=1.5, headwidth=8),
                        ha='center', fontsize=10)
    
    plt.title('Percentage of Pools That Gained by Waiting')
    plt.xlabel('Purchase Time (seconds)')
    plt.ylabel('Percentage of Pools (%)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(f"{output_prefix}_pct_gained.png")
    logger.info(f"Percentage gained plot saved to {output_prefix}_pct_gained.png")
    
    # Plot 4: Distribution of percentage retained at selected time points
    selected_times = [10, 60, 180, 360, 600]
    selected_times = [t for t in selected_times if t in time_points]
    
    plt.figure(figsize=(16, 10))
    
    for i, t in enumerate(selected_times):
        plt.subplot(len(selected_times), 1, i+1)
        
        bins = results['timing_stats'][t]['pct_retained_bins']
        pcts = results['timing_stats'][t]['pct_retained_pcts']
        
        plt.bar(bins, pcts, width=15, alpha=0.7)
        plt.title(f'Distribution of % Retained at {t}s')
        plt.ylabel('% of Pools')
        plt.grid(True, linestyle='--', alpha=0.5)
        
        # Add percentages as text
        for j, pct in enumerate(pcts):
            plt.text(bins[j] + 7.5, pct + 1, f'{pct:.1f}%', ha='center')
    
    plt.xlabel('Percentage of Optimal Return Retained')
    plt.tight_layout()
    plt.savefig(f"{output_prefix}_distribution.png")
    logger.info(f"Distribution plot saved to {output_prefix}_distribution.png")
    
    # Plot 5: Combined insights
    plt.figure(figsize=(16, 12))
    
    plt.subplot(3, 1, 1)
    plt.plot(time_points, median_multiplier, 'o-', color='green', linewidth=2, label='Median Multiplier')
    plt.title('Median Return Multiplier if Purchased Later')
    plt.ylabel('Multiplier (x)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    
    plt.subplot(3, 1, 2)
    plt.plot(time_points, median_pct_retained, 'o-', color='blue', linewidth=2, label='Median % Retained')
    plt.axhline(y=100, color='red', linestyle='--', alpha=0.7)
    plt.axhline(y=80, color='orange', linestyle='--', alpha=0.7)
    plt.title('Median Percentage of Optimal Return Retained')
    plt.ylabel('Percentage (%)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    
    plt.subplot(3, 1, 3)
    plt.plot(time_points, pct_pools_gained, 'o-', color='purple', linewidth=2, 
             label='% Pools Gained by Waiting')
    plt.title('Percentage of Pools That Gained by Waiting')
    plt.xlabel('Purchase Time (seconds)')
    plt.ylabel('Percentage (%)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    
    plt.tight_layout()
    plt.savefig(f"{output_prefix}_combined.png")
    logger.info(f"Combined insights plot saved to {output_prefix}_combined.png")

def main():
    """Main function to run the analysis"""
    parser = argparse.ArgumentParser(description='Analyze purchase timing for "Definitely Buy" pools')
    parser.add_argument('--db-path', type=str, default='cache/pools.db',
                       help='Path to the SQLite database')
    parser.add_argument('--min-marketcap', type=float, default=40000,
                       help='Minimum market cap at first time point (10s)')
    parser.add_argument('--min-multiplier', type=float, default=7.0,
                       help='Minimum multiplier to qualify as a "Definitely Buy" pool')
    parser.add_argument('--max-multiplier', type=float, default=1000.0,
                       help='Maximum realistic multiplier to consider (filter out unrealistic values)')
    parser.add_argument('--max-marketcap', type=float, default=50000000.0,
                       help='Maximum realistic market cap in USD (filter out unrealistic values)')
    parser.add_argument('--spike-threshold', type=float, default=5.0,
                       help='Threshold for detecting unrealistic spikes (5.0 = 500% increase)')
    parser.add_argument('--window-size', type=int, default=5,
                       help='Window size for rolling median calculation')
    parser.add_argument('--use-median-filter', action='store_true',
                       help='Use median filter for additional spike detection')
    parser.add_argument('--detect-down-spikes', action='store_true',
                       help='Detect and clean downward spikes as well')
    parser.add_argument('--outlier-percentile', type=float, default=95.0,
                       help='Percentile to cut off outliers (superpools)')
    parser.add_argument('--max-time-point', type=int, default=600,
                       help='Maximum time point to analyze (in seconds)')
    parser.add_argument('--output-prefix', type=str, default='time_to_buy',
                       help='Prefix for output files')
    parser.add_argument('--excluded-pools', type=str, nargs='+',
                       help='List of pool addresses to exclude from analysis')
    
    args = parser.parse_args()
    
    logger.info(f"Starting analysis using database: {args.db_path}")
    logger.info(f"Minimum market cap: {args.min_marketcap}")
    logger.info(f"Minimum multiplier for 'Definitely Buy': {args.min_multiplier}")
    logger.info(f"Maximum multiplier (realism filter): {args.max_multiplier}")
    logger.info(f"Maximum market cap (realism filter): {args.max_marketcap}")
    logger.info(f"Spike threshold: {args.spike_threshold}x (detects {args.spike_threshold * 100}% changes)")
    logger.info(f"Window size for rolling median: {args.window_size}")
    logger.info(f"Use median filter: {args.use_median_filter}")
    logger.info(f"Detect down spikes: {args.detect_down_spikes}")
    logger.info(f"Outlier percentile: {args.outlier_percentile}")
    logger.info(f"Maximum time point: {args.max_time_point}")
    
    if args.excluded_pools:
        logger.info(f"Excluding {len(args.excluded_pools)} pools from analysis:")
        for pool in args.excluded_pools:
            logger.info(f"  - {pool}")
    
    # Get "Definitely Buy" pools data
    df = get_definitely_buy_pools(
        db_path=args.db_path,
        min_marketcap=args.min_marketcap,
        min_multiplier=args.min_multiplier,
        max_time_point=args.max_time_point,
        max_multiplier=args.max_multiplier,
        max_marketcap=args.max_marketcap,
        spike_threshold=args.spike_threshold,
        window_size=args.window_size,
        use_median_filter=args.use_median_filter,
        detect_down_spikes=args.detect_down_spikes,
        excluded_pools=args.excluded_pools
    )
    
    if len(df) == 0:
        logger.error("No 'Definitely Buy' pools found with the given criteria.")
        return
    
    logger.info(f"Found {len(df)} 'Definitely Buy' pools (>={args.min_multiplier}x) for analysis.")
    
    # Analyze purchase timing
    results = analyze_purchase_timing(
        df=df,
        outlier_percentile=args.outlier_percentile
    )
    
    # Plot analysis results
    plot_analysis_results(results, output_prefix=args.output_prefix)
    
    # Output summary to console
    logger.info("\nSummary of analysis:")
    logger.info(f"Total 'Definitely Buy' pools: {results['total_pools']}")
    logger.info(f"Normal pools: {results['normal_pools']} (multiplier < {results['outlier_threshold']:.2f})")
    logger.info(f"Superpools: {results['superpools']} (multiplier >= {results['outlier_threshold']:.2f})")
    
    logger.info("\nReturn multiplier by purchase time:")
    for t in results['time_points']:
        logger.info(f"{t}s: Avg: {results['roi_stats'][t]['avg_multiplier']:.2f}x | " + 
                    f"Median: {results['roi_stats'][t]['median_multiplier']:.2f}x")
    
    logger.info("\nPercentage of optimal return retained by purchase time:")
    for t in results['time_points']:
        logger.info(f"{t}s: Avg: {results['roi_stats'][t]['avg_pct_retained']:.1f}% | " + 
                    f"Median: {results['roi_stats'][t]['median_pct_retained']:.1f}%")
    
    logger.info("\nPercentage of pools that gained by waiting:")
    for t in results['time_points']:
        logger.info(f"{t}s: {results['roi_stats'][t]['pct_pools_gained']:.1f}%")
    
    # Show top 3 best performing pools (highest final multiplier)
    logger.info("\nTop 3 best performing pools:")
    top_pools = df.sort_values(by='final_multiplier', ascending=False).head(3)
    for i, (_, pool) in enumerate(top_pools.iterrows()):
        logger.info(f"#{i+1}: Pool address: {pool['pool_id']}, " + 
                    f"Final multiplier: {pool['final_multiplier']:.2f}x, " +
                    f"Initial market cap: ${pool['initial_marketcap']:.2f}, " +
                    f"ATH market cap: ${pool['ath_marketcap']:.2f}")
        
        # Show how this pool performed at different time points
        logger.info(f"  Performance at different time points:")
        for t in results['time_points']:
            if f'purchase_multiplier_{t}s' in pool:
                multiplier = pool[f'purchase_multiplier_{t}s']
                pct_retained = pool[f'pct_retained_{t}s']
                logger.info(f"  {t}s: Multiplier: {multiplier:.2f}x, " +
                           f"% of optimal return: {pct_retained:.1f}%")

if __name__ == "__main__":
    main() 