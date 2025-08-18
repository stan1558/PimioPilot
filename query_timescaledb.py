import os
import argparse
import pandas as pd
import psycopg2

DB_HOST = os.getenv("DB_HOST", "timescaledb")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "marketdata")
DB_USER = os.getenv("DB_USER", "appuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "apppass")

def query(symbol: str, start: str, end: str, interval: str | None):
    sql = [
        "SELECT ts, symbol, \"open\", high, low, \"close\", volume, dividends, stock_splits, src_interval",
        "FROM tw_ticks",
        "WHERE symbol = %s AND ts >= %s AND ts < %s"
    ]
    params = [symbol, start, end]
    if interval:
        sql.append("AND src_interval = %s")
        params.append(interval)
    sql.append("ORDER BY ts ASC")
    sql_text = "\n".join(sql)

    with psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    ) as conn:
        return pd.read_sql(sql_text, conn, params=params)

def main():
    ap = argparse.ArgumentParser(description="Query ticks from TimescaleDB")
    ap.add_argument("symbol", help="e.g. 2330.TW")
    ap.add_argument("--start", required=True, help="inclusive start (YYYY-MM-DD or ISO 8601)")
    ap.add_argument("--end", required=True, help="exclusive end (YYYY-MM-DD or ISO 8601)")
    ap.add_argument("--interval", choices=["1d","5m"], help="filter by source interval")
    ap.add_argument("--csv", help="save results to CSV path")
    args = ap.parse_args()

    df = query(args.symbol, args.start, args.end, args.interval)
    if df.empty:
        print("no rows")
        return

    print(df.to_string(index=False, max_rows=60))
    if args.csv:
        df.to_csv(args.csv, index=False, encoding="utf-8")
        print(f"saved to {args.csv}")

if __name__ == "__main__":
    main()
