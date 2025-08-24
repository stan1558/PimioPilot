from __future__ import annotations
from pathlib import Path
import pandas as pd
from typing import Optional, List

def write_csv(df: pd.DataFrame, out_dir: str | Path, filename: str, fields: Optional[List[str]] = None) -> str:
    out_path = Path(out_dir) / filename
    out_path.parent.mkdir(parents=True, exist_ok=True)
    to_write = df[fields] if fields else df
    to_write.to_csv(out_path, index=False)
    return str(out_path)
