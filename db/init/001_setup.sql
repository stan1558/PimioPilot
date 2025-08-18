CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Store both daily and minute-level bars in a single table; use `src_interval` to indicate the source granularity (e.g., '1d', '5m').
CREATE TABLE IF NOT EXISTS tw_ticks (
  ts timestamptz NOT NULL,
  symbol text NOT NULL,
  "open" numeric,
  high numeric,
  low numeric,
  "close" numeric,
  volume bigint,
  dividends numeric,
  stock_splits numeric,
  src_interval text,
  PRIMARY KEY (symbol, ts)
);

SELECT create_hypertable('tw_ticks', 'ts', if_not_exists => TRUE);

-- Query using the common index (by symbol + time in descending order)
CREATE INDEX IF NOT EXISTS idx_tw_ticks_symbol_ts_desc ON tw_ticks (symbol, ts DESC);
