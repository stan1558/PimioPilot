from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Iterable
import os
import psycopg2
import psycopg2.extras
import pandas as pd

@dataclass
class DBConn:
    host: Optional[str] = None
    dbname: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    port: int = 5432
    dsn: Optional[str] = None

    @staticmethod
    def from_env() -> "DBConn":
        return DBConn(
            dsn=os.getenv("DB_DSN"),
            host=os.getenv("DB_HOST", "timescaledb"),
            dbname=os.getenv("DB_NAME", "marketdata"),
            user=os.getenv("DB_USER", "appuser"),
            password=os.getenv("DB_PASSWORD", "apppass"),
            port=int(os.getenv("DB_PORT", "5432")),
        )

def _connect(cfg: DBConn):
    if cfg.dsn:
        return psycopg2.connect(cfg.dsn)
    return psycopg2.connect(
        host=cfg.host, dbname=cfg.dbname, user=cfg.user, password=cfg.password, port=cfg.port
    )

_ALLOWED_COLUMNS = {
    "ts","symbol","open","high","low","close","adj_close","volume","dividends","stock_splits","src_interval"
}

def build_sql(spec: dict) -> tuple[str, list]:
    cols = spec.get("columns")
    if not cols:
        cols = sorted(_ALLOWED_COLUMNS)
    else:
        unknown = [c for c in cols if c not in _ALLOWED_COLUMNS]
        if unknown:
            raise ValueError(f"Unknown columns in query: {unknown}")

    placeholders = []
    where = []

    # symbols (ARRAY ANY)
    symbols = spec["symbols"]
    where.append("symbol = ANY(%s)")
    placeholders.append(symbols)

    # time range
    tr = spec["time_range"]
    where.append("ts >= %s AND ts < %s")
    placeholders.extend([tr["start"], tr["end"]])

    # intervals (ARRAY ANY)
    intervals = spec.get("intervals")
    if intervals:
        where.append("src_interval = ANY(%s)")
        placeholders.append(intervals)

    # extra filters (trusted subset provided by user)
    for filt in spec.get("filters", []) or []:
        where.append(f"({filt})")

    order_by = spec.get("order_by") or ["ts ASC"]
    order_sql = ", ".join(order_by)

    limit = spec.get("limit")
    limit_sql = f" LIMIT {int(limit)}" if limit else ""

    sql = f"""
    SELECT {", ".join(cols)}
    FROM tw_ticks
    WHERE {' AND '.join(where)}
    ORDER BY {order_sql}
    {limit_sql}
    """.strip()

    return sql, placeholders

def _maybe_debug(sql: str, params: list):
    if os.getenv("PPDATA_DEBUG") in ("1", "true", "True", "yes", "on"):
        # Concise and readable version
        print("[PPDATA_DEBUG] SQL:", sql.replace("\n", " "), flush=True)
        print("[PPDATA_DEBUG] params:", params, flush=True)

def query_to_dataframe(spec: dict, conn: Optional[DBConn] = None, chunk_rows: Optional[int] = None) -> pd.DataFrame:
    """Non-streaming query using psycopg2 cursor (avoid pandas.read_sql DBAPI quirks)."""
    sql, params = build_sql(spec)
    _maybe_debug(sql, params)
    conn = conn or DBConn.from_env()
    with _connect(conn) as c:
        with c.cursor() as cur:
            cur.execute(sql, params)
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)

def iter_query_chunks(spec: dict, conn: Optional[DBConn] = None, chunksize: int = 100_000) -> Iterable[pd.DataFrame]:
    """Server-side cursor to stream large results in chunks."""
    sql, params = build_sql(spec)
    _maybe_debug(sql, params)
    conn = conn or DBConn.from_env()
    with _connect(conn) as c:
        with c.cursor(name="pp_stream", cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.itersize = chunksize
            cur.execute(sql, params)
            while True:
                rows = cur.fetchmany(chunksize)
                if not rows:
                    break
                yield pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])
