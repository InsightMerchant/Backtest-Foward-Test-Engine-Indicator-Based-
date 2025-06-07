import pandas as pd
from binance.client import Client
import time
from datetime import datetime
import pytz
import os
from tqdm import tqdm

# Binance API credentials
api_key = ''
api_secret = ''
client = Client(api_key, api_secret)

# Configuration
symbols = ['BTCUSDT']
interval = Client.KLINE_INTERVAL_1HOUR
limit = 1000
start_time_str = "2022-01-01 00:00:00"
end_time_str = "2025-06-01 00:00:00"

# Helper functions
def str_to_timestamp(time_str):
    dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
    dt = pytz.utc.localize(dt)
    return int(dt.timestamp() * 1000)

def timestamp_to_datetime(timestamp):
    return datetime.fromtimestamp(timestamp / 1000.0, tz=pytz.utc)

def get_kline_time(client, symbol, interval, start_time, end_time, limit):
    klines = []
    start_ts = start_time
    total_hours = (end_time - start_time) / (1000 * 60 * 60)
    total_batches = total_hours / limit
    with tqdm(total=total_batches, desc=f"Fetching {symbol} klines", leave=False) as pbar:
        while start_ts < end_time:
            try:
                batch = client.get_klines(
                    symbol=symbol,
                    interval=interval,
                    startTime=start_ts,
                    endTime=end_time,
                    limit=limit
                )
                if not batch:
                    break
                klines.extend(batch)
                start_ts = batch[-1][0] + 1
                pbar.update(1)
                time.sleep(0.1)
            except Exception as e:
                print(f"Error fetching klines for {symbol}: {e}")
                time.sleep(1)
                continue
    return klines

# Convert start and end times to timestamps
start_time = str_to_timestamp(start_time_str)
end_time = str_to_timestamp(end_time_str)

# Collect data for all symbols
dataframes = {}
for symbol in tqdm(symbols, desc="Processing symbols"):
    klines = get_kline_time(client, symbol, interval, start_time, end_time, limit)
    kline_data = [{
        'datetime': timestamp_to_datetime(kline[0]),
        'open': float(kline[1]),
        'high': float(kline[2]),
        'low': float(kline[3]),
        'close': float(kline[4]),
        'volume': float(kline[5])
    } for kline in klines]
    dataframes[symbol] = pd.DataFrame(kline_data)

# Create the 'candle' folder
candle_folder = 'candle'
os.makedirs(candle_folder, exist_ok=True)

# Save each symbol's DataFrame to a separate CSV
for symbol in symbols:
    csv_filename = os.path.join(candle_folder, f'{symbol}_{interval}_binance.csv')
    symbol_df = dataframes[symbol][['datetime', 'open', 'high', 'low', 'close', 'volume']]
    symbol_df.to_csv(csv_filename, index=False)
    print(f'Saved {csv_filename}')
