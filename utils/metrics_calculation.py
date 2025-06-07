# utils/metrics_calculation.py
import numpy as np
import pandas as pd
import re
from typing import Dict, Optional, Tuple, List

def get_periods_per_year(interval: str) -> float:
    """
    Calculate the number of periods per year based on the interval.
    
    Args:
        interval (str): Time interval (e.g., '1h', '2h', '1d')
    
    Returns:
        float: Number of periods per year
    """
    m = re.match(r"(\d+)\s*(h|d)", interval, re.IGNORECASE)
    if not m:
        raise ValueError(f"Invalid interval format: {interval}")
    num, unit = m.groups()
    num = int(num)
    unit = unit.lower()
    if unit == "h":
        return 8760 / num  # 8760 hours in a non-leap year
    if unit == "d":
        return 365 / num   # 365 days in a non-leap year
    return 1

def calculate_returns_from_signals(df: pd.DataFrame, signals: np.ndarray) -> Tuple[pd.Series, List[bool]]:
    """
    Calculate returns and trade outcomes based on buy/sell signals.
    
    Args:
        df (pd.DataFrame): DataFrame with 'close' column
        signals (np.ndarray): Array of signals (1 = buy, -1 = sell, 0 = hold)
    
    Returns:
        Tuple[pd.Series, List[bool]]: Series of returns and list of trade profitability (True if positive)
    """
    position = 0
    returns = []
    trade_outcomes = []  # Track if each trade was profitable
    entry_price = 0
    
    for i in range(1, len(df)):
        if signals[i] == 1 and position == 0:  # Buy
            position = 1
            entry_price = df['close'].iloc[i]
        elif signals[i] == -1 and position == 1:  # Sell
            position = 0
            exit_price = df['close'].iloc[i]
            trade_return = (exit_price - entry_price) / entry_price
            returns.append(trade_return)
            trade_outcomes.append(trade_return > 0)  # True if profitable
    
    return (pd.Series(returns, index=df.index[1:len(returns) + 1]) if returns else pd.Series(dtype=float), 
            trade_outcomes)

def calculate_metrics(df: pd.DataFrame, interval: str, signals: np.ndarray, 
                     annualization_factor: Optional[float] = None) -> Dict:
    """
    Compute performance metrics based on price data and signals.
    Returns a dict suitable for JSON serialization.
    
    Args:
        df (pd.DataFrame): DataFrame with 'datetime' and 'close' columns
        interval (str): Time interval (e.g., '1h', '2h', '1d')
        signals (np.ndarray): Array of signals (1 = buy, -1 = sell, 0 = hold)
        annualization_factor (float, optional): Factor for annualizing metrics
    
    Returns:
        Dict: Performance metrics including win rate
    """
    # Ensure datetime column exists
    df = df.copy()
    if df.index.name != "datetime" or not np.issubdtype(df.index.dtype, np.datetime64):
        df["datetime"] = pd.to_datetime(df["datetime"])
        df.set_index("datetime", inplace=True)
    
    # Calculate returns and trade outcomes from signals
    returns, trade_outcomes = calculate_returns_from_signals(df, signals)
    
    # Set annualization factor
    periods_per_year = get_periods_per_year(interval)
    if annualization_factor is None:
        annualization_factor = np.sqrt(periods_per_year)

    # Sharpe Ratio
    pnl_std = returns.std() if len(returns) else 0
    mean_return = returns.mean() if len(returns) else 0
    sharpe = (mean_return / pnl_std * annualization_factor) if pnl_std else 0

    # Cumulative and drawdowns
    cumu = returns.cumsum()
    running_max = cumu.cummax()
    drawdowns = cumu - running_max
    max_dd = drawdowns.min() if len(drawdowns) else 0

    # Drawdown dates
    dd_end_idx = drawdowns.idxmin() if len(drawdowns) else df.index[0]
    dd_end_loc = df.index.get_loc(dd_end_idx)
    dd_end = df.index[dd_end_loc]

    if dd_end_loc > 0:
        pre = running_max.iloc[:dd_end_loc] == cumu.iloc[:dd_end_loc]
        if pre.any():
            start_label = pre[pre].index[-1]
            start_loc = df.index.get_loc(start_label)
            dd_start = df.index[start_loc]
        else:
            dd_start = df.index[0]
    else:
        dd_start = dd_end

    post = cumu.iloc[dd_end_loc:]
    recover = post[post >= running_max.iloc[dd_end_loc]]
    dd_recover = recover.index[0] if len(recover) else dd_end
    mdd_days = (dd_recover - dd_start).total_seconds() / 86400 if dd_start and dd_recover else 0

    # Sortino Ratio
    downside = returns[returns < 0]
    ds_std = downside.std() if len(downside) else 0
    sortino = (mean_return / ds_std * annualization_factor) if ds_std else 0

    # Calmar Ratio
    calmar = (mean_return * periods_per_year / abs(max_dd)) if max_dd else 0
    ann_return = mean_return * periods_per_year
    num_trades = len(returns)  # Number of completed trades
    total_ret = returns.sum()
    tpi = num_trades / len(df) if len(df) else 0
    sr_cr = sharpe / calmar if calmar else 0

    # Win Rate
    win_trades = sum(1 for outcome in trade_outcomes if outcome) if trade_outcomes else 0
    win_rate = (win_trades / num_trades * 100) if num_trades else 0.0

    return {
        "SR": round(sharpe, 4),
        "CR": round(calmar, 4),
        "MDD": round(max_dd, 4),
        "sortino_ratio": round(sortino, 4),
        "AR": round(ann_return, 4),
        "num_of_trades": float(num_trades),
        "TR": round(total_ret, 4),
        "trades_per_interval": round(tpi, 4),
        "MDD_MAX_DURATION_IN_DAY": round(mdd_days, 4),
        "SR_CR": round(sr_cr, 4),
        "win_rate": round(win_rate, 2),  # Win rate in percentage
        "backtest_start_date": df.index.min().isoformat(),
        "backtest_end_date": df.index.max().isoformat(),
    }