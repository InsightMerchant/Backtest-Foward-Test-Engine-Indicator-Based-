# utils/data_loader.py
import pandas as pd
import os
from typing import List, Dict

def load_kline_data(candle_folder: str, symbols: List[str], interval: str = '1h') -> Dict[str, pd.DataFrame]:
    """
    Load kline data from CSV files in the candle folder for specified symbols.
    
    Args:
        candle_folder (str): Path to the candle folder (e.g., 'candle')
        symbols (List[str]): List of symbols (e.g., ['BTCUSDT', 'ETHUSDT'])
        interval (str): Interval of the data (e.g., '1h')
    
    Returns:
        Dict[str, pd.DataFrame]: Dictionary mapping symbols to their DataFrames
    """
    dataframes = {}
    for symbol in symbols:
        csv_path = os.path.join(candle_folder, f'{symbol}_{interval}_binance.csv')
        if not os.path.exists(csv_path):
            print(f"Warning: CSV file for {symbol} ({csv_path}) not found")
            continue
        try:
            df = pd.read_csv(csv_path, parse_dates=['datetime'])
            df.set_index('datetime', inplace=True)
            dataframes[symbol] = df[['open', 'high', 'low', 'close', 'volume']]
        except Exception as e:
            print(f"Error loading {csv_path}: {e}")
    return dataframes