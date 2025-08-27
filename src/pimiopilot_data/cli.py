from __future__ import annotations
import argparse
from pathlib import Path
import json

from .validator import load_and_validate
from .runner import run_job
from .query_runner import run_query

def main():
    ap = argparse.ArgumentParser(description="PimioPilot Data Module — Fetch & Query")
    sub = ap.add_subparsers(dest="cmd", required=True)

    # Fetch subcommand (existing)
    runp = sub.add_parser("run", help="Run a fetch job from YAML config")
    runp.add_argument("--config", required=True, help="Path to job YAML")
    runp.add_argument("--schema", default=str(Path("schemas") / "job.schema.json"), help="Path to JSON Schema")

    # Query subcommand (new)
    q = sub.add_parser("query", help="Run a DB query from YAML/JSON spec")
    q.add_argument("--config", required=True, help="Path to query YAML/JSON")
    q.add_argument("--schema", default=str(Path("schemas") / "query.schema.json"), help="Path to JSON Schema")

    args = ap.parse_args()

    if args.cmd == "run":
        cfg = load_and_validate(args.config, args.schema)
        summary, _ = run_job(cfg)
        print(json.dumps({
            "status": summary["status"],
            "rows": summary["artifacts"]["rows"],
            "out": summary["artifacts"]["out_dir"]
        }, ensure_ascii=False))

    elif args.cmd == "query":
        cfg = load_and_validate(args.config, args.schema)
        summary, _ = run_query(cfg)
        print(json.dumps({
            "status": summary["status"],
            "rows": summary["artifacts"]["rows"],
            "result": summary["artifacts"]["result"],
            "out": summary["artifacts"]["out_dir"]
        }, ensure_ascii=False))

if __name__ == "__main__":
    main()
