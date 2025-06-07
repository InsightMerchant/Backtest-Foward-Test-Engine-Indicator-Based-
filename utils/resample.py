# utils/resample.py
import pandas as pd
from typing import Dict

def resample_data(dataframes: Dict[str, pd.DataFrame], interval: str) -> Dict[str, pd.DataFrame]:
    """
    Resample kline data to the specified interval.
    
    Args:
        dataframes (Dict[str, pd.DataFrame]): Dictionary of symbol DataFrames
        interval (str): Target interval (e.g., '1h', '2h', '4h', '8h', '12h', '1d')
    
    Returns:
        Dict[str, pd.DataFrame]: Resampled DataFrames
    """
    valid_intervals = ['1h', '2h', '4h', '8h', '12h', '1d']
    if interval not in valid_intervals:
        raise ValueError(f"Interval must be one of {valid_intervals}")

    resampled_data = {}
    for symbol, df in dataframes.items():
        if df.empty:
            print(f"Warning: Empty DataFrame for {symbol}")
            continue
        
        # Resampling rules
        resample_rule = interval
        agg_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }
        
        try:
            # Resample the DataFrame
            resampled_df = df.resample(resample_rule).agg(agg_dict).dropna()
            resampled_data[symbol] = resampled_df
        except Exception as e:
            print(f"Error resampling {symbol} to {interval}: {e}")
    
    return resampled_data