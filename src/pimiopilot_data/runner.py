from __future__ import annotations
import json, time
from pathlib import Path
from typing import Tuple
import pandas as pd

from .models import Job, RangeSpec, OutputSpec, YFOpts
from .io.ndjson_logger import NDJSONLogger
from .io.csv_writer import write_csv
from .fetchers import yf_client

def _materialize_job(raw: dict) -> Job:
    rng = RangeSpec(**raw["range"])
    outs = OutputSpec(**raw["outputs"])
    yfopts = YFOpts(**raw.get("yfinance_options", {}))
    job = Job(
        task_id=raw["task_id"],
        source=raw["source"],
        symbols=raw["symbols"],
        interval=raw["interval"],
        range=rng,
        outputs=outs,
        yfinance_options=yfopts,
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

    logger.log("job_start", task_id=job.task_id, symbols=job.symbols, interval=job.interval)

    df = yf_client.fetch(
        job.symbols,
        interval=job.interval,
        start=job.range.start,
        end=job.range.end,
        options={
            "auto_adjust": job.yfinance_options.auto_adjust,
            "actions": job.yfinance_options.actions,
            "prepost": job.yfinance_options.prepost,
            "threads": job.yfinance_options.threads,
        },
    )
    logger.log("fetch_done", rows=int(df.shape[0]), cols=int(df.shape[1]))

    csv_path: str | None = None
    if job.outputs.write_csv:
        csv_path = write_csv(df, job.outputs.out_dir, job.outputs.csv_filename, job.outputs.csv_fields)
        logger.log("csv_written", path=csv_path)

    elapsed = time.time() - t0
    summary = {
        "schema": {"job_config_schema": "schemas/job.schema.json", "version": schema_version},
        "task": {"task_id": job.task_id, "source": job.source, "symbols": job.symbols, "interval": job.interval, "range": {"start": job.range.start, "end": job.range.end}},
        "artifacts": {"log_ndjson": str(log_path), "csv": csv_path, "rows": int(df.shape[0]), "cols": int(df.shape[1]), "out_dir": str(out_dir)},
        "timing": {"seconds": round(elapsed, 3)},
        "status": "ok",
    }
    Path(summary_path).write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.log("job_end", seconds=elapsed)

    return summary, df
