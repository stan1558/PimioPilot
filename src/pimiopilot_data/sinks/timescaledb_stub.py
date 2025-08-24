"""TimescaleDB sink (placeholder)."""
from dataclasses import dataclass

@dataclass
class TSConfig:
    dsn: str
    table: str = "tw_ticks"

def upsert_prices_placeholder(*args, **kwargs) -> None:
    raise NotImplementedError("TimescaleDB sink not implemented yet")
