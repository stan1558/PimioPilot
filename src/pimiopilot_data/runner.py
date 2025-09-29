from __future__ import annotations
import json, time, os
from pathlib import Path
from types import SimpleNamespace
from typing import Tuple
import pandas as pd
from datetime import datetime
import pytz
from dateutil.relativedelta import relativedelta

from .models import Job, RangeSpec, OutputSpec, YFOpts, RetentionSpec
from .io.ndjson_logger import NDJSONLogger
from .io.csv_writer import write_csv

from .io.parquet_writer import write_parquet
from .io.manifest import stable_spec, spec_hash, write_manifest
from .io.json_validator import validate_json

from .fetchers import yf_client
from .sinks.timescaledb import upsert_prices, TSConfig, purge_older_than

def _parse_relative(spec: str):
    unit = spec[-1]
    value = int(spec[:-1])
    if unit == "d":
        return relativedelta(days=value)
    elif unit == "w":
        return relativedelta(weeks=value)
    elif unit == "m":
        return relativedelta(months=value)
    elif unit == "y":
        return relativedelta(years=value)
    else:
        raise ValueError(f"Unsupported relative unit: {unit}")

def resolve_date_range(rng: RangeSpec, tz: str = "Asia/Taipei") -> tuple[str, str]:
    tzinfo = pytz.timezone(tz)
    today = datetime.now(tzinfo).date()
    if rng.relative:
        end = today
        start = today - _parse_relative(rng.relative)
        return start.isoformat(), end.isoformat()
    else:
        return rng.start, rng.end or today.isoformat()

def _materialize_job(raw: dict) -> Job:
    rng = RangeSpec(**raw["range"])
    outs = OutputSpec(**raw["outputs"])
    yfopts = YFOpts(**raw.get("yfinance_options", {}))
    retention = RetentionSpec(**raw.get("retention", {})) if "retention" in raw else None
    job = Job(
        task_id=raw["task_id"],
        source=raw["source"],
        symbols=raw["symbols"],
        interval=raw["interval"],
        range=rng,
        outputs=outs,
        yfinance_options=yfopts,
        retention=retention,
        raw=raw,
    )
    return job

def run_job(validated_cfg: dict, schema_version: str = "2025-08-24") -> Tuple[dict, pd.DataFrame]:
    job = _materialize_job(validated_cfg)
    out_dir = Path(job.outputs.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    log_path = out_dir / "logs.ndjson"
    summary_path = out_dir / "summary.json"
    logger = NDJSONLogger(log_path)
    t0 = time.time()

    start, end = resolve_date_range(job.range)

    logger.log("job_start", task_id=job.task_id, symbols=job.symbols, interval=job.interval, start=start, end=end)

    # Fetch data
    df = yf_client.fetch(
        job.symbols,
        interval=job.interval,
        start=start,
        end=end,
        options={
            "auto_adjust": job.yfinance_options.auto_adjust,
            "actions": job.yfinance_options.actions,
            "prepost": job.yfinance_options.prepost,
            "threads": job.yfinance_options.threads,
        },
    )
    logger.log("fetch_done", rows=int(df.shape[0]), cols=int(df.shape[1]))

    # Optional CSV output
    csv_path: str | None = None
    if job.outputs.write_csv:
        csv_path = write_csv(df, job.outputs.out_dir, job.outputs.csv_filename, job.outputs.csv_fields)
        logger.log("csv_written", path=csv_path)

    # TimescaleDB upsert
    upsert_rows = 0
    try:
        cfg = TSConfig.from_env()
        upsert_rows = upsert_prices(df, interval=job.interval, cfg=cfg)
        logger.log("timescaledb_upsert_done", rows=upsert_rows, table=cfg.table, db=cfg.dbname or "dsn")
    except Exception as e:
        logger.log("timescaledb_upsert_error", error=str(e))
        # Propagate to mark job as failed
        raise

    deleted_rows = 0
    if job.retention and job.retention.delete_older_than:
        cutoff = None
        val = job.retention.delete_older_than
        if val and (val[-1] in "dwmy"):
            tzinfo = pytz.timezone("Asia/Taipei")
            today = datetime.now(tzinfo).date()
            cutoff = today - _parse_relative(val)
            cutoff = cutoff.isoformat()
        else:
            cutoff = val
        try:
            cfg = TSConfig.from_env()
            deleted_rows = purge_older_than(cfg, cutoff, job.symbols)
            logger.log("retention_delete_done", cutoff=cutoff, rows=deleted_rows)
        except Exception as e:
            logger.log("retention_delete_error", error=str(e))

    elapsed = time.time() - t0
    summary = {
        "schema": {"job_config_schema": "schemas/job.schema.json", "version": schema_version},
        "task": {"task_id": job.task_id, "source": job.source, "symbols": job.symbols, "interval": job.interval, "range": {"start": start, "end": end}},
        "artifacts": {"log_ndjson": str(log_path), "csv": csv_path, "rows": int(df.shape[0]), "cols": int(df.shape[1]), "out_dir": str(out_dir)},
        "db": {"table": cfg.table, "upserted": upsert_rows, "deleted": deleted_rows},
        "timing": {"seconds": round(elapsed, 3)},
        "status": "ok",
    }
    Path(summary_path).write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.log("job_end", seconds=elapsed)

    return summary, df

_YF_RENAME = {
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Adj Close": "adj_close",
    "AdjClose": "adj_close",
    "Adj_Close": "adj_close",
    "adjclose": "adj_close",
    "Volume": "volume",
}


def _normalize_candle_df(df: pd.DataFrame, symbol: str, assume_no_adjust: bool=False) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()

    rename_map = {c: _YF_RENAME.get(c, c) for c in df.columns}
    df.rename(columns=rename_map, inplace=True)

    if "ts" not in df.columns:
        if isinstance(df.index, pd.DatetimeIndex):
            df["ts"] = df.index.tz_convert("UTC").view("int64") // 10**9 if df.index.tz is not None else df.index.tz_localize("UTC").view("int64") // 10**9
        else:
            pass

    required = {"open","high","low","close","volume"}
    missing_basic = sorted(list(required - set(df.columns)))
    if missing_basic:
        raise RuntimeError(f"Missing basic OHLCV columns: {missing_basic}")

    if "adj_close" not in df.columns or df["adj_close"].isna().all():
        df["adj_close"] = df["close"]
        if not assume_no_adjust:
            pass

    if "symbol" not in df.columns:
        df["symbol"] = symbol

    cols = ["ts","symbol","open","high","low","close","volume","adj_close"]
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA

    return df[cols]

def _to_namespace(obj):
    if isinstance(obj, dict):
        return SimpleNamespace(**{k: _to_namespace(v) for k, v in obj.items()})
    if isinstance(obj, list):
        return [_to_namespace(v) for v in obj]
    return obj

def run_job(job):
    if isinstance(job, dict):
        job = _to_namespace(job)

    out_dir = Path(job.outputs.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    logger = NDJSONLogger(out_dir / (job.outputs.logs_filename or "logs.ndjson"))
    logger.log("job.start", task_id=job.task_id, source=job.source)

    start, end = resolve_date_range(job.range, tz="Asia/Taipei")
    # Fetch
    df = None
    if job.source == "yfinance":
        df = yf_client.fetch(job.symbols, interval=job.interval, start=start, end=end, options=job.yfinance_options.__dict__)
        symbol = job.symbols[0]
        df = _normalize_candle_df(df, symbol, assume_no_adjust=False)
    else:
        raise ValueError(f"Unsupported source: {job.source}")

    # Normalize columns to CandleV1
    cols = ["ts","symbol","open","high","low","close","volume","adj_close"]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing columns for CandleV1: {missing}")

    # Write parquet + metadata
    meta = {
        "pimiopilot.schema_version": "CandleV1",
        "pimiopilot.ts_tz": "UTC",
        "pimiopilot.interval": job.interval,
        "pimiopilot.adjustment": "auto" if job.yfinance_options.auto_adjust else "none",
        "pimiopilot.source": job.source,
    }
    parquet_path = None
    if job.outputs.write_parquet:
        parquet_path = write_parquet(df[cols], out_dir, job.outputs.parquet_filename, metadata=meta, fields=cols)

    # Build manifest
    spec = stable_spec({
        "source": job.source,
        "symbols": job.symbols,
        "interval": job.interval,
        "range": job.range.__dict__,
        "yfinance_options": job.yfinance_options.__dict__,
    })
    shash = spec_hash(spec)
    manifest = {
        "spec_hash": shash,
        "source": job.source,
        "symbols": job.symbols,
        "range": {"start": start, "end": end, "relative": job.range.relative},
        "interval": job.interval,
        "timezone": "Asia/Taipei",
        "adjustment": "auto" if job.yfinance_options.auto_adjust else "none",
        "schema_version": "CandleV1",
        "created_at": datetime.now(pytz.UTC).isoformat(),
        "artifacts": {
            "parquet": str(Path(job.outputs.parquet_filename)),
            "logs": str(Path(job.outputs.logs_filename)),
        },
        "source_options": job.yfinance_options.__dict__,
    }
    logs_path = out_dir / (job.outputs.logs_filename or "logs.ndjson")
    manifest_path = out_dir / (job.outputs.manifest_filename or "manifest.json")

    # Validate manifest before write
    validate_json(manifest, Path("schemas/manifest.schema.json"))
    write_manifest(manifest_path, manifest, schema_path=Path("schemas/manifest.schema.json"))

    rows = len(df) if df is not None else 0
    logger.log("job.end", task_id=job.task_id, rows=rows)
    summary = {
        "status": "ok",
        "task_id": getattr(job, "task_id", None),
        "source": getattr(job, "source", None),
        "interval": getattr(job, "interval", None),
        "out_dir": str(out_dir),
        "artifacts": {
            "out_dir": str(out_dir),
            "parquet": str(parquet_path) if parquet_path else None,
            "manifest": str(manifest_path),
            "logs": str(logs_path),
            "rows": rows
        },
        "parquet": str(parquet_path) if parquet_path else None,
        "manifest": str(manifest_path),
        "logs": str(logs_path),
        "rows": rows

    }
    return summary
