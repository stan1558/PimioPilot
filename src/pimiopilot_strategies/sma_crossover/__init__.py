import pandas as pd
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

class SMACrossover:
    """Reference strategy implementing the PimioPilot interface."""
    def __init__(self, fast: int = 10, slow: int = 30):
        if fast <= 0 or slow <= 0 or fast >= slow:
            raise ValueError("invalid window sizes: expect 0 < fast < slow")
        self.fast, self.slow = fast, slow

    def _to_utc_iso(self, ts):
        ts = pd.to_datetime(ts)
        if getattr(ts, "tzinfo", None) is None or ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return ts.isoformat()

    def generate_signal(self, data: pd.DataFrame, *, as_of: Optional[str] = None, context: Optional[Dict[str, Any]] = None):
        required = {"ts", "close"}
        missing = required - set(map(str, data.columns))
        if missing:
            raise ValueError(f"missing columns: {missing}")

        df = data.copy()
        if "symbol" not in df.columns:
            df["symbol"] = (context or {}).get("symbol", "UNKNOWN")

        df = df.sort_values(["symbol", "ts"])

        rows: List[Dict[str, Any]] = []
        for sym, g in df.groupby("symbol", sort=False):
            fast_ma = g["close"].rolling(self.fast, min_periods=1).mean()
            slow_ma = g["close"].rolling(self.slow, min_periods=1).mean()
            diff = fast_ma - slow_ma
            action = diff.apply(lambda x: "BUY" if x > 0 else ("SELL" if x < 0 else "HOLD"))
            out = g.assign(action=action)
            for r in out.itertuples(index=False):
                rows.append({
                    "ts": self._to_utc_iso(getattr(r, "ts")),
                    "symbol": getattr(r, "symbol", sym),
                    "action": getattr(r, "action"),
                })

        return {
            "schema_version": "1.0",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "as_of": as_of,
            "signals": rows,
            "metadata": {"strategy": "sma_crossover", "fast": self.fast, "slow": self.slow},
        }

def build_strategy(config: Optional[Dict[str, Any]] = None) -> "SMACrossover":
    cfg = config or {}
    return SMACrossover(fast=int(cfg.get("fast", 10)), slow=int(cfg.get("slow", 30)))
