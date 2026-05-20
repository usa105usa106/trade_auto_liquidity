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
    planned_notional_usdt: float = 0.0
    expected_margin_usdt: float = 0.0
    max_margin_per_position_usdt: float = 0.0
    leverage: int = 0
    liquidation_stop_mode: bool = False
    liquidation_buffer_pct: float = 0.0
    liquidation_target_distance_pct: float = 0.0
    liquidity_retest_rr: float = 0.0
    liquidity_retest_zone_low: float = 0.0
    liquidity_retest_zone_high: float = 0.0
    liquidity_retest_reason: str = ""
    created_at: float = field(default_factory=time.time)
