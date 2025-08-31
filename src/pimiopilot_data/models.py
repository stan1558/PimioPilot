from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

@dataclass
class RangeSpec:
    start: Optional[str] = None
    end: Optional[str] = None
    relative: Optional[str] = None

@dataclass
class RetentionSpec:
    delete_older_than: Optional[str] = None

@dataclass
class OutputSpec:
    out_dir: str
    write_csv: bool = False
    csv_filename: str = "data.csv"
    csv_fields: Optional[List[str]] = None

@dataclass
class YFOpts:
    auto_adjust: bool = True
    actions: bool = False
    prepost: bool = False
    threads: str | int = "auto"

@dataclass
class Job:
    task_id: str
    source: str
    symbols: List[str]
    interval: str
    range: RangeSpec
    outputs: OutputSpec
    yfinance_options: YFOpts = field(default_factory=YFOpts)
    retention: Optional[RetentionSpec] = None
    raw: Dict[str, Any] = field(default_factory=dict)
