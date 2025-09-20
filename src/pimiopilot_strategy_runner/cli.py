from __future__ import annotations
import sys, json, argparse, pandas as pd
from pimiopilot_strategy_runner.runner import StrategyRunner, StrategyRef

def main():
    ap = argparse.ArgumentParser(description="Run a strategy over input CSV")
    ap.add_argument("--module", required=True, help="Strategy module, e.g. pimiopilot_strategies.sma_crossover")
    ap.add_argument("--params", default="{}", help="JSON dict of params")
    ap.add_argument("--mode", default="batch", choices=["batch","online"])
    ap.add_argument("--csv", required=True, help="Input CSV with at least columns: ts, open, high, low, close, volume")
    ap.add_argument("--log", default="out/strategy_runner.log", help="NDJSON log path")
    args = ap.parse_args()

    params = json.loads(args.params)
    ref = StrategyRef(module=args.module, params=params)
    df = pd.read_csv(args.csv)
    r = StrategyRunner(ref, log_path=args.log)
    out = r.run(df, mode=args.mode)
    print(json.dumps(out, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
