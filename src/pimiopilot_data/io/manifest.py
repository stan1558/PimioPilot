from __future__ import annotations
import json, hashlib, time
from pathlib import Path
from typing import Any, Dict

from jsonschema import validate

def stable_spec(obj: Dict[str, Any]) -> Dict[str, Any]:
    # Minimize non-deterministic fields
    keep = {k: obj[k] for k in ["source","symbols","interval","range","yfinance_options"] if k in obj}
    return keep

def spec_hash(spec: Dict[str, Any]) -> str:
    payload = json.dumps(spec, sort_keys=True, ensure_ascii=False, separators=(",",":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def write_manifest(path: str | Path, manifest: Dict[str, Any], *, schema_path: str | Path) -> str:
    # Validate against schema
    schema = json.loads(Path(schema_path).read_text(encoding="utf-8"))
    validate(instance=manifest, schema=schema)
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(p)