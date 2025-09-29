from __future__ import annotations
from pathlib import Path
from typing import Dict, Optional, List
import pandas as pd

def write_parquet(df: pd.DataFrame, out_dir: str | Path, filename: str, metadata: Optional[Dict[str, str]] = None, fields: Optional[List[str]] = None) -> str:
    out_path = Path(out_dir) / filename
    out_path.parent.mkdir(parents=True, exist_ok=True)
    to_write = df[fields] if fields else df
    # Normalize dtypes
    if "ts" in to_write.columns:
        to_write = to_write.copy()
        to_write["ts"] = pd.to_datetime(to_write["ts"], utc=True)
    # Use pyarrow if available, else fastparquet fallback
    engine = "pyarrow"
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        table = pa.Table.from_pandas(to_write)
        # attach file metadata
        if metadata:
            existing = table.schema.metadata or {}
            merged = existing | {k.encode(): str(v).encode() for k, v in metadata.items()}
            table = table.replace_schema_metadata(merged)
        pq.write_table(table, out_path)
    except Exception:
        try:
            engine = "fastparquet"
            to_write.to_parquet(out_path, engine="fastparquet")
        except Exception as e:
            raise RuntimeError(f"Failed writing parquet with both pyarrow and fastparquet: {e}")
    return str(out_path)