from __future__ import annotations
import json, time, os
from pathlib import Path
from typing import Tuple
import pandas as pd
from datetime import datetime
import pytz
from dateutil.relativedelta import relativedelta

from .models import Job, RangeSpec, OutputSpec, YFOpts, RetentionSpec
from .io.ndjson_logger import NDJSONLogger
from .io.csv_writer import write_csv
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
