from __future__ import annotations
import json, time
from pathlib import Path
from typing import Any, Dict

class NDJSONLogger:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("", encoding="utf-8")
    def log(self, event: str, **fields: Dict[str, Any]) -> None:
        rec = {"ts": time.time(), "event": event} | fields
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
