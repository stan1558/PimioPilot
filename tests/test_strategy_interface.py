import sys, pathlib
import pandas as pd
from datetime import datetime, timezone, timedelta
from importlib import import_module

# Ensure repo root is importable
ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SCHEMA_VERSION = "1.0"

def _mk_df():
    ts0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    rows = []
    for i in range(50):
        for sym, base in [("2330.TW", 100.0), ("2317.TW", 60.0)]:
            rows.append({
                "ts": ts0 + timedelta(minutes=i),
                "symbol": sym,
                "open": base,
                "high": base + 1,
                "low": base - 1,
                "close": base + i * 0.1,
                "volume": 1000 + i,
            })
    return pd.DataFrame(rows)

def test_build_and_generate():
    mod = import_module("pimiopilot_strategies.sma_crossover")
    strat = mod.build_strategy({"fast": 5, "slow": 10})
    out = strat.generate_signal(_mk_df())
    assert out["schema_version"] == SCHEMA_VERSION
    assert isinstance(out["signals"], list) and len(out["signals"]) > 0
    row = out["signals"][0]
    assert {"ts", "symbol"}.issubset(row.keys())

def test_missing_columns_raises():
    mod = import_module("pimiopilot_strategies.sma_crossover")
    strat = mod.build_strategy({})
    df = _mk_df().drop(columns=["close"])  # Missing required field
    import pytest
    with pytest.raises(ValueError):
        strat.generate_signal(df)

def test_deterministic():
    mod = import_module("pimiopilot_strategies.sma_crossover")
    strat = mod.build_strategy({"fast": 5, "slow": 10})
    df = _mk_df()
    out1 = strat.generate_signal(df)
    out2 = strat.generate_signal(df)

    # Ignore non-deterministic timestamp fields
    out1.pop("generated_at", None)
    out2.pop("generated_at", None)

    assert out1 == out2
