from __future__ import annotations
from typing import List, Dict, Any
import pandas as pd
import yfinance as yf

_VALID_INTERVALS = {"1d", "1h", "30m", "15m", "5m", "1m"}

def fetch(symbols: List[str], *, interval: str, start: str, end: str | None, options: Dict[str, Any]) -> pd.DataFrame:
    if interval not in _VALID_INTERVALS:
        raise ValueError(f"Unsupported interval: {interval}")
    df = yf.download(
        tickers=symbols,
        start=start,
        end=end,
        interval=interval,
        auto_adjust=bool(options.get("auto_adjust", True)),
        actions=bool(options.get("actions", False)),
        prepost=bool(options.get("prepost", False)),
        threads=options.get("threads", "auto"),
        progress=False,
        group_by="ticker",
    )
    if isinstance(df.columns, pd.MultiIndex):
        frames = []
        for sym in symbols:
            if sym not in df.columns.levels[0]:
                continue
            sub = df[sym].reset_index().rename(columns=str)
            sub.insert(0, "symbol", sym)
            frames.append(sub)
        out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    else:
        out = df.reset_index().rename(columns=str)
        out.insert(0, "symbol", symbols[0] if symbols else "")
    rename_map = {
        "Open": "open","High": "high","Low": "low","Close": "close",
        "Adj Close": "adj_close","Volume": "volume","Date": "ts","Datetime": "ts",
    }
    out = out.rename(columns=rename_map)
    if "ts" not in out.columns:
        raise RuntimeError("Missing timestamp column after normalization")
    out = out.sort_values(["symbol","ts"]).reset_index(drop=True)
    return out
