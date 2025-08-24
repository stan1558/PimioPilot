from __future__ import annotations
import argparse
from pathlib import Path
from .validator import load_and_validate
from .runner import run_job

def main():
    ap = argparse.ArgumentParser(description="PimioPilot Data Module — Fetch & Store API")
    sub = ap.add_subparsers(dest="cmd", required=True)

    runp = sub.add_parser("run", help="Run a fetch job from YAML config")
    runp.add_argument("--config", required=True, help="Path to job YAML")
    runp.add_argument("--schema", default=str(Path("schemas") / "job.schema.json"), help="Path to JSON Schema")

    args = ap.parse_args()
    if args.cmd == "run":
        cfg = load_and_validate(args.config, args.schema)
        summary, _ = run_job(cfg)
        import json
        print(json.dumps({"status": summary["status"], "rows": summary["artifacts"]["rows"], "out": summary["artifacts"]["out_dir"]}))

if __name__ == "__main__":
    main()
