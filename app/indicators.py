"""
Quantitative indicator calculations using pure pandas.

Indicators computed:
  - ATR (14-day)     : Average True Range in absolute dollar terms
  - ATR%             : ATR as a percentage of closing price  (target > 8%)
  - RVOL             : Today's volume / 20-day average volume (target > 2.0)
  - SMA-20           : 20-day Simple Moving Average of close
  - Market Regime    : Bullish if SPY & QQQ close > their own SMA-20
"""

import pandas as pd
import numpy as np


def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Compute the Average True Range over *period* days.

    Expects columns: high, low, close (sorted by date ascending).
    Returns a Series of the same length (first *period* rows will be NaN).
    """
    high = df["high"]
    low = df["low"]
    prev_close = df["close"].shift(1)

    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr = tr.rolling(window=period, min_periods=period).mean()
    return atr


def compute_atr_pct(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """ATR as a percentage of the closing price.  Weekly projection = ATR/close * sqrt(5) * 100."""
    atr = compute_atr(df, period)
    # Project daily ATR to a weekly (5-day) move using sqrt-of-time scaling
    atr_pct = (atr / df["close"]) * np.sqrt(5) * 100
    return atr_pct


def compute_rvol(df: pd.DataFrame, period: int = 20) -> pd.Series:
    """Relative Volume = today's volume / 20-day rolling average volume."""
    avg_vol = df["volume"].rolling(window=period, min_periods=period).mean()
    rvol = df["volume"] / avg_vol
    return rvol


def compute_rsi(df: pd.DataFrame, period: int = 2) -> pd.Series:
    """
    Compute the Relative Strength Index over *period* days.

    Uses the Wilder smoothing method (exponential moving average).
    Default period=2 for mean-reversion setups (Larry Connors RSI-2).
    """
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)

    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi


def compute_sma(df: pd.DataFrame, column: str = "close", period: int = 20) -> pd.Series:
    """Simple Moving Average over *period* days."""
    return df[column].rolling(window=period, min_periods=period).mean()


def compute_adv(df: pd.DataFrame, period: int = 20) -> pd.Series:
    """Average Daily Volume over *period* days."""
    return df["volume"].rolling(window=period, min_periods=period).mean()


def compute_vol_scaled_size(
    atr_pct_series: pd.Series,
    target_risk: float = 0.01,
    min_size: float = 0.05,
    max_size: float = 0.20,
) -> pd.Series:
    """
    Position size as fraction of equity, scaled inversely to ATR%.

    Equal-risk sizing: position_pct = target_risk / (ATR% / 100)
    Capped between min_size (5%) and max_size (20%).
    """
    raw = target_risk / (atr_pct_series / 100.0)
    return raw.clip(min_size, max_size)


def add_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Attach all indicator columns to a single-ticker OHLCV DataFrame (in-place).

    Expected input columns: open, high, low, close, volume
    Added columns:          atr_14, atr_pct, rvol, sma_20, adv_20,
                            rsi_14, sma_50, high_52w, pct_from_52w_high, return_5d
    """
    df = df.sort_values("date").reset_index(drop=True)
    df["atr_14"] = compute_atr(df)
    df["atr_pct"] = compute_atr_pct(df)
    df["rvol"] = compute_rvol(df)
    df["sma_20"] = compute_sma(df)
    df["adv_20"] = compute_adv(df)
    # Momentum-specific indicators
    df["rsi_14"] = compute_rsi(df, period=14)
    df["sma_50"] = compute_sma(df, period=50)
    df["high_52w"] = df["high"].rolling(window=252, min_periods=50).max()
    df["pct_from_52w_high"] = (df["close"] / df["high_52w"] - 1) * 100
    df["return_5d"] = df["close"].pct_change(5) * 100
    return df


# ------------------------------------------------------------------
# Market Regime (SPY / QQQ)
# ------------------------------------------------------------------

def check_market_regime(spy_df: pd.DataFrame, qqq_df: pd.DataFrame) -> dict:
    """
    Determine the current market regime.

    Returns:
        {
            "spy_above_sma20": bool,
            "qqq_above_sma20": bool,
            "regime": "Bullish" | "Bearish" | "Mixed",
        }
    """
    spy_sma = compute_sma(spy_df, "close", 20)
    qqq_sma = compute_sma(qqq_df, "close", 20)

    spy_above = bool(spy_df["close"].iloc[-1] > spy_sma.iloc[-1])
    qqq_above = bool(qqq_df["close"].iloc[-1] > qqq_sma.iloc[-1])

    if spy_above and qqq_above:
        regime = "Bullish"
    elif not spy_above and not qqq_above:
        regime = "Bearish"
    else:
        regime = "Mixed"

    return {
        "spy_above_sma20": spy_above,
        "qqq_above_sma20": qqq_above,
        "regime": regime,
    }
