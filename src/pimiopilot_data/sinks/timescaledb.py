from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Iterable, Any
import os
import psycopg2
import psycopg2.extras
import pandas as pd

@dataclass
class TSConfig:
    dsn: Optional[str] = None
    host: Optional[str] = None
    dbname: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    port: int = 5432
    table: str = "tw_ticks"

    @classmethod
    def from_env(cls) -> "TSConfig":
        return cls(
            dsn=os.getenv("DB_DSN"),
            host=os.getenv("DB_HOST"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=int(os.getenv("DB_PORT", "5432")),
            table=os.getenv("DB_TABLE", "tw_ticks"),
        )

def _connect(cfg: TSConfig):
    if cfg.dsn:
        return psycopg2.connect(cfg.dsn)
    return psycopg2.connect(
        host=cfg.host, dbname=cfg.dbname, user=cfg.user, password=cfg.password, port=cfg.port
    )

_COLS = ["symbol","ts","open","high","low","close","adj_close","volume","src_interval","dividends","stock_splits"]

def _iter_rows(df: pd.DataFrame, interval: str) -> Iterable[tuple[Any, ...]]:
    # Ensure required columns exist; missing ones become None
    cols_present = {c for c in df.columns}
    for _, row in df.iterrows():
        yield (
            row.get("symbol"),
            pd.to_datetime(row.get("ts")).to_pydatetime() if row.get("ts") is not None else None,
            row.get("open"),
            row.get("high"),
            row.get("low"),
            row.get("close"),
            row.get("adj_close") if "adj_close" in cols_present else None,
            int(row.get("volume")) if pd.notna(row.get("volume")) else None,
            interval,
            row.get("dividends") if "dividends" in cols_present else None,
            row.get("stock_splits") if "stock_splits" in cols_present else None,
        )

def upsert_prices(df: pd.DataFrame, *, interval: str, cfg: Optional[TSConfig] = None) -> int:
    """Bulk upsert price rows into TimescaleDB.
    Returns number of rows attempted.
    """
    if cfg is None:
        cfg = TSConfig.from_env()
    if df.empty:
        return 0

    # Build SQL
    cols_sql = ",".join(_COLS)
    placeholders = ",".join(["%s"] * len(_COLS))
    update_cols = ["open","high","low","close","adj_close","volume","src_interval","dividends","stock_splits"]
    update_sql = ",".join([f'{c}=EXCLUDED.{c}' for c in update_cols])

    sql = f"""    INSERT INTO {cfg.table} ({cols_sql})
    VALUES %s
    ON CONFLICT (symbol, ts) DO UPDATE SET
      {update_sql};
    """

    rows = list(_iter_rows(df, interval))
    with _connect(cfg) as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, rows, template=f"({placeholders})", page_size=1000)
    return len(rows)
