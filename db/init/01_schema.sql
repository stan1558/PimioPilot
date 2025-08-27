CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS tw_ticks (
  symbol        text        NOT NULL,
  ts            timestamptz NOT NULL,
  open          double precision,
  high          double precision,
  low           double precision,
  close         double precision,
  adj_close     double precision,
  volume        bigint,
  src_interval  text        NOT NULL,
  dividends     double precision,
  stock_splits  double precision,
  PRIMARY KEY (symbol, ts)
);

SELECT create_hypertable('tw_ticks','ts', if_not_exists => true, chunk_time_interval => interval '7 days');
CREATE INDEX IF NOT EXISTS idx_tw_ticks_symbol_ts_desc ON tw_ticks (symbol, ts DESC);
CREATE INDEX IF NOT EXISTS idx_tw_ticks_interval      ON tw_ticks (src_interval);
