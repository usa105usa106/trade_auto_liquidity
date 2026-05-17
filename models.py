from dataclasses import dataclass, field
import time

@dataclass
class Candidate:
    symbol: str
    side: str
    strategy: str
    confidence: float
    futures_price: float
    score_details: dict = field(default_factory=dict)
    mirror_used: bool = False
    session: str = "NORMAL"

@dataclass
class TradePlan:
    symbol: str
    side: str
    order_type: str
    qty: float
    entry_price: float
    stop_price: float
    take_price: float
    risk_pct: float
    confidence: float
    strategy: str
    mirror_used: bool = False
    session: str = "NORMAL"
    max_open_positions: int = 999
    created_at: float = field(default_factory=time.time)
