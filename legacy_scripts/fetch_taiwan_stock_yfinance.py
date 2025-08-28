import os
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import yfinance as yf
import psycopg2
from psycopg2.extras import execute_values

DB_HOST = os.getenv("DB_HOST", "timescaledb")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "marketdata")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
TZ = os.getenv("TZ", "Asia/Taipei")

TICKER = os.getenv("TICKER", "2330.TW")

def _conn():
    last_err = None
    for i in range(12):
        try:
            return psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                user=DB_USER, password=DB_PASSWORD, connect_timeout=5
            )
        except Exception as e:
            last_err = e
            print(f"[DB] connect failed ({i+1}/12): {e}")
            time.sleep(2)
    raise RuntimeError(f"DB connection failed: {last_err}")

def _normalize(df: pd.DataFrame, symbol: str, src_interval: str) -> pd.DataFrame:
    """
    Normalize the yfinance DataFrame to the `tw_ticks` columns:
    ts, symbol, open, high, low, close, volume, dividends, stock_splits, src_interval
    and ensure `ts` is UTC tz-aware.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=[
            "ts","symbol","open","high","low","close","volume","dividends","stock_splits","src_interval"
        ])

    df = df.copy()
    df = df.rename_axis("ts").reset_index()
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")

    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    # Common yfinance columns: open, high, low, close, volume, dividends, stock_splits
    for col in ["open","high","low","close","dividends","stock_splits"]:
        if col not in df.columns:
            df[col] = None

    for col in ["open","high","low","close","dividends","stock_splits"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = pd.to_numeric(df.get("volume"), errors="coerce").fillna(0).astype("Int64")

    df["symbol"] = symbol
    df["src_interval"] = src_interval
    return df[["ts","symbol","open","high","low","close","volume","dividends","stock_splits","src_interval"]]

def _to_python_scalars(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # NaN/NaT → None
    df = df.where(pd.notnull(df), None)

    for c in ["open","high","low","close","dividends","stock_splits"]:
        if c in df.columns:
            df[c] = df[c].apply(lambda v: float(v) if v is not None else None)

    if "volume" in df.columns:
        df["volume"] = df["volume"].apply(lambda v: int(v) if v is not None else None)

    if "ts" in df.columns:
        df["ts"] = df["ts"].apply(lambda v: v.to_pydatetime() if hasattr(v, "to_pydatetime") else v)

    return df

def upsert_ticks(conn, df: pd.DataFrame) -> int:
    """Batch upsert into `tw_ticks` with the primary key `(symbol, ts)`."""
    if df is None or df.empty:
        return 0
    sql = """
        INSERT INTO tw_ticks
            (ts, symbol, "open", high, low, "close", volume, dividends, stock_splits, src_interval)
        VALUES %s
        ON CONFLICT (symbol, ts) DO UPDATE SET
            "open" = EXCLUDED."open",
            high = EXCLUDED.high,
            low  = EXCLUDED.low,
            "close" = EXCLUDED."close",
            volume = EXCLUDED.volume,
            dividends = EXCLUDED.dividends,
            stock_splits = EXCLUDED.stock_splits,
            src_interval = EXCLUDED.src_interval;
    """
    df = _to_python_scalars(df)
    rows = list(df.itertuples(index=False, name=None))
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    return len(rows)

def fetch_and_store(symbol: str) -> None:
    """Fetch the last 6 months of daily candlesticks and today’s 5-minute bars, and write them to the DB."""
    t = yf.Ticker(symbol)

    daily = t.history(period="6mo", interval="1d", auto_adjust=False)
    daily_df = _normalize(daily, symbol, "1d")

    intra = t.history(period="1d", interval="5m", auto_adjust=False)
    intra_df = _normalize(intra, symbol, "5m")

    with _conn() as conn:
        n1 = upsert_ticks(conn, daily_df)
        n2 = upsert_ticks(conn, intra_df)
        print(f"[{symbol}] upsert daily(1d): {n1} rows, intraday(5m): {n2} rows")

def main():
    print(f"🏁 start fetch for {TICKER}")
    fetch_and_store(TICKER)
    print("✅ done")

if __name__ == "__main__":
    main()
