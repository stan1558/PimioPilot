from __future__ import annotations
import os, time, json, math, shutil
from pathlib import Path
from typing import Tuple, List, Optional
import pandas as pd

from .validator import load_and_validate
from .io.ndjson_logger import NDJSONLogger
from .io.csv_writer import write_csv
from .queries import query_to_dataframe, iter_query_chunks
from .timeutil import parse_relative_range

def _default_filename(spec: dict) -> str:
    syms = "-".join(sorted(spec["symbols"]))[:40].replace("/","_")
    start = spec["time_range"]["start"].replace(":","").replace("-","").replace("T","").replace("Z","")
    end = spec["time_range"]["end"].replace(":","").replace("-","").replace("T","").replace("Z","")
    return f"q_{syms}_{start}_{end}"

def run_query(spec: dict, schema_version: str = "1") -> Tuple[dict, Optional[pd.DataFrame]]:
    """Execute a DB query spec and materialize outputs. Returns (summary, df_if_small)."""
    t0 = time.time()

    out_cfg = spec["output"]
    out_dir = Path(out_cfg["path"])
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- relative support: expand to start/end if time_range.relative is provided ---
    tr = spec.get("time_range") or {}
    rel = tr.get("relative")
    if rel:
        intervals = spec.get("intervals") or []
        start_iso, end_iso = parse_relative_range(rel, intervals=intervals)
        tr["start"], tr["end"] = start_iso, end_iso
        spec["time_range"] = tr

    base = out_cfg.get("filename") or _default_filename(spec)
    fmt = out_cfg["format"]
    include_header = bool(out_cfg.get("include_header", True))
    chunk_size = out_cfg.get("chunk_size")
    if chunk_size is not None:
        chunk_size = int(chunk_size)

    # log path lives beside result file
    log_path = out_dir / f"{base}.log.ndjson"
    manifest_path = out_dir / f"{base}.manifest.json"
    logger = NDJSONLogger(log_path)

    logger.log("query_start", spec=spec)

    rows_written = 0
    file_path = None

    if fmt == "csv":
        # streaming write if chunk_size provided, else single shot
        csv_path = out_dir / (base + ".csv")
        file_path = csv_path
        if chunk_size:
            # stream
            mode = "w"
            header = include_header
            for chunk in iter_query_chunks(spec, chunksize=chunk_size):
                if chunk.empty:
                    continue
                chunk.to_csv(csv_path, index=False, mode=mode, header=header)
                rows_written += len(chunk)
                mode = "a"
                header = False
        else:
            df = query_to_dataframe(spec)
            if not df.empty:
                df.to_csv(csv_path, index=False, header=include_header)
                rows_written = len(df)
            else:
                # still create empty file with header if requested
                df.head(0).to_csv(csv_path, index=False, header=include_header)
                rows_written = 0

    elif fmt == "ndjson":
        nd_path = out_dir / (base + ".ndjson")
        file_path = nd_path
        with nd_path.open("w", encoding="utf-8") as f:
            for chunk in iter_query_chunks(spec, chunksize=chunk_size or 100_000):
                if chunk.empty:
                    continue
                for rec in chunk.to_dict(orient="records"):
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                rows_written += len(chunk)

    elif fmt == "parquet":
        # non-streaming parquet (fast & typed). If chunk_size given, collect small batches.
        pq_path = out_dir / (base + ".parquet")
        file_path = pq_path
        if chunk_size:
            # accumulate chunks in memory cautiously
            frames: List[pd.DataFrame] = []
            n = 0
            for chunk in iter_query_chunks(spec, chunksize=chunk_size):
                if not chunk.empty:
                    frames.append(chunk)
                    rows_written += len(chunk)
                    n += 1
            df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            df.to_parquet(pq_path, index=False)
        else:
            df = query_to_dataframe(spec)
            rows_written = len(df)
            df.to_parquet(pq_path, index=False)
    else:
        raise ValueError(f"Unsupported output.format: {fmt}")

    elapsed = round(time.time() - t0, 3)
    summary = {
        "schema": {"query_config_schema": "schemas/query.schema.json", "version": schema_version},
        "query": {
            "symbols": spec["symbols"],
            "intervals": spec.get("intervals"),
            "time_range": spec["time_range"],
            "columns": spec.get("columns"),
        },
        "artifacts": {
            "result": str(file_path) if file_path else None,
            "log_ndjson": str(log_path),
            "manifest": str(manifest_path),
            "rows": rows_written,
            "out_dir": str(out_dir),
        },
        "timing": {"seconds": elapsed},
        "status": "ok"
    }

    Path(manifest_path).write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.log("query_end", seconds=elapsed, rows=rows_written)

    # For convenience: if result is small and format is parquet/csv w/o streaming, return df; else None
    df_out = None

    # normalize artifacts to fetch-style names ===
    # Target layout:
    #   <out_dir>/<run_dir>/
    #       data.csv
    #       logs.ndjson
    #       summary.json
    def _normalize_artifacts_for_fetch_style(summary: dict, spec: dict) -> dict:
        artifacts = summary.get("artifacts", {})
        out_dir = Path(artifacts.get("out_dir") or spec["output"]["path"]).resolve()
        base = spec["output"].get("filename")
        if base:
            base = Path(base).stem  # strip any accidental extension
        else:
            base = _default_filename(spec)
        run_dir = out_dir / base
        run_dir.mkdir(parents=True, exist_ok=True)

        # Legacy flat files
        legacy_csv = out_dir / f"{base}.csv"
        legacy_log = out_dir / f"{base}.log.ndjson"
        legacy_manifest = out_dir / f"{base}.manifest.json"

        # Fetch-style destinations
        dst_csv = run_dir / "data.csv"
        dst_log = run_dir / "logs.ndjson"
        dst_summary = run_dir / "summary.json"

        # Move legacy into new locations if they exist
        try:
            if legacy_csv.exists():
                shutil.move(str(legacy_csv), str(dst_csv))
            if legacy_log.exists():
                shutil.move(str(legacy_log), str(dst_log))
            if legacy_manifest.exists():
                shutil.move(str(legacy_manifest), str(dst_summary))
        except Exception:
            # best-effort; don't fail overall query
            pass

        # Overwrite artifacts to point to fetch-style only
        new_artifacts = {
            "out_dir": str(run_dir),
            "csv": str(dst_csv) if dst_csv.exists() else artifacts.get("csv"),
            "log_ndjson": str(dst_log) if dst_log.exists() else artifacts.get("log_ndjson"),
            "result": str(dst_summary) if dst_summary.exists() else artifacts.get("result"),
            "rows": artifacts.get("rows"),
        }
        summary["artifacts"] = new_artifacts
        return summary

    if fmt in ("parquet", "csv") and not (chunk_size):
        try:
            # Re-read quickly only for users who call via API and want a DF
            if fmt == "parquet":
                df_out = pd.read_parquet(file_path)
            else:
                df_out = pd.read_csv(file_path)
        except Exception:
            df_out = None

    try:
        summary = _normalize_artifacts_for_fetch_style(summary, spec)
    except Exception:
        pass

    # Write the normalized summary to its final path (fetch-style)
    final_summary_path = Path(summary["artifacts"]["result"])  # should be <run-dir>/summary.json
    final_summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    return summary, df_out
