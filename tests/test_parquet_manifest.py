import json, os
from pathlib import Path
from pimiopilot_data.models import Job, RangeSpec, OutputSpec, YFOpts
from pimiopilot_data.runner import run_job

def test_parquet_manifest_end_to_end(tmp_path):
    job = Job(
        task_id="t1",
        source="yfinance",
        symbols=["2330.TW"],
        interval="1d",
        range=RangeSpec(relative="1m"),
        outputs=OutputSpec(out_dir=str(tmp_path), write_parquet=True, parquet_filename="data.parquet", manifest_filename="manifest.json", logs_filename="logs.ndjson"),
        yfinance_options=YFOpts(auto_adjust=True, actions=False, prepost=False, threads="auto"),
    )
    res = run_job(job)
    assert Path(res["parquet"]).exists()
    m = json.loads(Path(tmp_path/"manifest.json").read_text(encoding="utf-8"))
    assert m["schema_version"] == "CandleV1"
    assert m["artifacts"]["parquet"] == "data.parquet"
    # Determinism: run again and compare manifest hash
    res2 = run_job(job)
    m2 = json.loads(Path(tmp_path/"manifest.json").read_text(encoding="utf-8"))
    assert m["spec_hash"] == m2["spec_hash"]