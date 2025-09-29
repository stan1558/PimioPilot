from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict
from jsonschema import validate, Draft202012Validator

def validate_json(data: Dict[str, Any], schema_path: str | Path) -> None:
    schema = json.loads(Path(schema_path).read_text(encoding="utf-8"))
    validate(instance=data, schema=schema)