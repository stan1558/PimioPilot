from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, Callable, Optional, Literal
from importlib import import_module
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import time, json

# Reuse NDJSON logger from data module if available
try:
    from pimiopilot_data.io.ndjson_logger import NDJSONLogger
except Exception:
    class NDJSONLogger:  # fallback
        def __init__(self, path: str | Path):
            self.path = Path(path)
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text("", encoding="utf-8")
        def log(self, event: str, **fields):
            rec = {"ts": time.time(), "event": event} | fields
            with self.path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(rec) + "\n")

SCHEMA_VERSION = "1.0"

@dataclass(frozen=True)
class StrategyRef:
    module: str   # e.g., "pimiopilot_strategies.sma_crossover"
    params: Dict[str, Any]

class StrategyRunner:
    def __init__(self, strat: StrategyRef, log_path: str | Path = "out/strategy_runner.log"):
        self.ref = strat
        self.logger = NDJSONLogger(log_path)

        self.logger.log("runner_init", module=strat.module, params=strat.params)

        mod = import_module(self.ref.module)
        if not hasattr(mod, "build_strategy"):
            raise ImportError(f"strategy module {self.ref.module} missing build_strategy()")
        self.strategy = mod.build_strategy(self.ref.params)
        self.logger.log("strategy_loaded", module=self.ref.module)

    def run(self, df: pd.DataFrame, mode: Literal["batch","online"]="batch") -> Dict[str, Any]:
        t0 = time.time()
        self.logger.log("run_start", mode=mode, rows=int(df.shape[0]), cols=int(df.shape[1]))

        if mode == "online":
            # online: only use available data "up to now", same interface to strategy
            # The strategy itself decides how to interpret the full df for an online tick
            pass  # no special slicing by default

        out = self.strategy.generate_signal(df)

        # Normalize: ensure 'input' exists for downstream consumers/tests
        if isinstance(out, dict) and "input" not in out:
            out = {**out, "input": {"rows": int(df.shape[0]), "cols": int(df.shape[1])}}

        elapsed = round(time.time() - t0, 6)
        self.logger.log("run_end", seconds=elapsed, output_keys=list(out.keys()))
        # normalize minimal envelope
        return {
            "schema": {"runner_schema": "schemas/strategy_runner.schema.json", "version": SCHEMA_VERSION},
            "strategy_output": out,
            "runner": {"module": self.ref.module, "mode": mode},
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "status": "ok",
        }
