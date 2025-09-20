from __future__ import annotations
from typing import Protocol, Dict, Any
import pandas as pd

class Strategy(Protocol):
    def generate_signal(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Given a price DataFrame with at least columns ['ts','open','high','low','close','volume'],
        return a dict signal payload, deterministic for the same input+params.
        """
        ...
