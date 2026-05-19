from dataclasses import dataclass
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

MSK = ZoneInfo("Europe/Moscow")

@dataclass
class SessionState:
    name: str
    risk_multiplier: float
    max_positions_multiplier: float
    long_risk_multiplier: float
    short_risk_multiplier: float
    long_confidence_penalty: float
    short_confidence_bonus: float
    spread_tighten: float
    short_bias_active: bool
    reason: str

class SessionEngine:
    def __init__(
        self,
        enabled=True,
        asia_open="03:00",
        america_open="16:30",
        window_minutes=240,
        america_short_bias_enabled=True,
    ):
        self.enabled = enabled
        self.asia_open = self._parse(asia_open)
        self.america_open = self._parse(america_open)
        self.window_minutes = int(window_minutes)
        self.america_short_bias_enabled = america_short_bias_enabled

    def _parse(self, s: str) -> time:
        hh, mm = s.split(":")
        return time(int(hh), int(mm), tzinfo=MSK)

    def _in_window(self, now: datetime, start: time) -> bool:
        start_dt = datetime.combine(now.date(), start).astimezone(MSK)
        return start_dt <= now <= start_dt + timedelta(minutes=self.window_minutes)

    def get_state(self, now: datetime | None = None) -> SessionState:
        now = (now or datetime.now(MSK)).astimezone(MSK)
        if not self.enabled:
            return SessionState("OFF",1,1,1,1,0,0,1,False,"Session filter disabled")
        if self._in_window(now, self.america_open):
            if self.america_short_bias_enabled:
                return SessionState("AMERICA_OPEN_SHORT_BIAS",1,1,0.5,1.0,15,5,0.8,True,"America first 4h, short bias active")
            return SessionState("AMERICA_OPEN",0.5,0.5,0.5,0.5,10,0,0.8,False,"America first 4h")
        if self._in_window(now, self.asia_open):
            return SessionState("ASIA_OPEN",0.75,0.75,0.75,0.75,5,0,0.85,False,"Asia first 4h")
        return SessionState("NORMAL",1,1,1,1,0,0,1,False,"Normal session")

    def apply(self, candidate: dict, settings: dict, now: datetime | None = None) -> dict:
        state = self.get_state(now)
        c = dict(candidate)
        side = str(c.get("side","")).upper()
        risk = float(settings.get("risk_pct", 0.005))
        max_pos = int(settings.get("max_open_positions", 5))
        min_conf = float(c.get("min_confidence", 70))
        conf = float(c.get("confidence", 0))

        strategy = str(c.get("strategy") or c.get("effective_strategy_mode") or "").lower()
        liquidity_retest = strategy == "liquidity_retest"
        if side == "SHORT":
            c["risk_pct"] = risk * state.short_risk_multiplier
            c["confidence"] = conf + state.short_confidence_bonus
            c["max_open_positions"] = max_pos if state.short_bias_active else max(1, int(max_pos * state.max_positions_multiplier))
            if liquidity_retest and state.short_bias_active:
                # v0082: US open dump bias supports short retests after upward liquidity grabs.
                c["confidence"] = float(c.get("confidence", conf)) + 4
                c["liquidity_retest_bias"] = "US_OPEN_SHORT_PRIORITY"
        elif side == "LONG":
            c["risk_pct"] = risk * state.long_risk_multiplier
            c["confidence"] = conf - state.long_confidence_penalty
            c["max_open_positions"] = max(1, int(max_pos * (0.5 if state.short_bias_active else state.max_positions_multiplier)))
            min_conf += 10 if state.short_bias_active else 0
            if liquidity_retest and state.short_bias_active:
                # Keep America bias: during 16:30-20:30 MSK longs need stronger reclaim.
                c["confidence"] = float(c.get("confidence", conf)) - 5
                min_conf += 5
                c["liquidity_retest_bias"] = "US_OPEN_LONG_STRICT"
        else:
            c["risk_pct"] = risk * state.risk_multiplier
            c["max_open_positions"] = max(1, int(max_pos * state.max_positions_multiplier))

        c["session"] = state.name
        c["session_reason"] = state.reason
        c["min_confidence"] = min_conf
        c["allowed_by_session"] = c["confidence"] >= min_conf
        c["spread_limit_multiplier"] = state.spread_tighten
        return c
