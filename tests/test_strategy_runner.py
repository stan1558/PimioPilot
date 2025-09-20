import sys, pathlib
import pandas as pd
from datetime import datetime, timezone, timedelta

# Ensure repo root is importable
ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

def _mk_df(n=40):
    ts0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    rows = []
    close = 100.0
    for i in range(n):
        ts = ts0 + timedelta(minutes=i)
        close += (1 if i % 7 < 3 else -1) * 0.5  # some oscillation
        rows.append({"ts": ts.isoformat(), "open": close-0.2, "high": close+0.4, "low": close-0.6, "close": close, "volume": 100+i})
    return pd.DataFrame(rows)

def test_runner_batch_and_online(tmp_path):
    from pimiopilot_strategy_runner.runner import StrategyRunner, StrategyRef

    ref = StrategyRef(module="pimiopilot_strategies.sma_crossover", params={"fast":5, "slow":10})
    log_path = tmp_path / "runner.log"
    r = StrategyRunner(ref, log_path=log_path)

    df = _mk_df(60)

    out_batch = r.run(df, mode="batch")
    assert out_batch["status"] == "ok"
    assert out_batch["runner"]["mode"] == "batch"
    assert "strategy_output" in out_batch
    assert out_batch["strategy_output"]["input"]["rows"] == len(df)

    out_online = r.run(df.tail(15), mode="online")
    assert out_online["status"] == "ok"
    assert out_online["runner"]["mode"] == "online"

    # check log exists and non-empty
    text = log_path.read_text(encoding="utf-8")
    assert "runner_init" in text and "run_start" in text and "run_end" in text
