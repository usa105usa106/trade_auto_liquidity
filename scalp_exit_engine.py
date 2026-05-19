import os


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


class ScalpExitPolicy:
    """Small deterministic policy for local scalp exits.

    All percentages are raw price percentages, not leverage-adjusted. The policy
    is deliberately conservative: it improves exits without removing fixed TP/SL.
    Exchange TP/SL may still exist; these rules are for local bot monitoring and
    LOCAL BOT PROTECTED fallback.
    """

    def __init__(self):
        self.enabled = _bool_env("SCALP_EXIT_ENABLED", True)
        self.breakeven_trigger_pct = float(os.getenv("BREAKEVEN_TRIGGER_PCT", "0.12"))
        self.breakeven_offset_pct = float(os.getenv("BREAKEVEN_OFFSET_PCT", "0.01"))
        self.trailing_enabled = _bool_env("SCALP_TRAILING_ENABLED", True)
        self.trailing_start_pct = float(os.getenv("SCALP_TRAILING_START_PCT", "0.18"))
        self.trailing_giveback_pct = float(os.getenv("SCALP_TRAILING_GIVEBACK_PCT", "0.08"))
        self.time_min_sec = int(float(os.getenv("SMART_TIME_STOP_MIN_SEC", "45")))
        self.time_extend_profit_pct = float(os.getenv("SMART_TIME_STOP_EXTEND_PROFIT_PCT", "0.08"))
        self.time_max_extend_sec = int(float(os.getenv("SMART_TIME_STOP_MAX_EXTEND_SEC", "180")))
        self.stale_pnl_abs_pct = float(os.getenv("SMART_TIME_STOP_STALE_ABS_PCT", "0.04"))

    @staticmethod
    def pnl_pct(side: str, entry: float, price: float) -> float:
        if entry <= 0 or price <= 0:
            return 0.0
        return (price - entry) / entry * 100.0 if str(side).upper() == "LONG" else (entry - price) / entry * 100.0

    def breakeven_stop(self, side: str, entry: float) -> float:
        if entry <= 0:
            return 0.0
        off = self.breakeven_offset_pct / 100.0
        return entry * (1 + off) if str(side).upper() == "LONG" else entry * (1 - off)

    def should_move_breakeven(self, pos: dict, price: float) -> tuple[bool, float, float]:
        if not self.enabled:
            return False, 0.0, 0.0
        side = str(pos.get("side") or "").upper()
        entry = float(pos.get("entry_price") or 0)
        stop = float(pos.get("stop_price") or 0)
        pnl = self.pnl_pct(side, entry, price)
        if pnl < self.breakeven_trigger_pct or entry <= 0:
            return False, pnl, 0.0
        new_stop = self.breakeven_stop(side, entry)
        if side == "LONG" and (not stop or stop < new_stop):
            return True, pnl, new_stop
        if side == "SHORT" and (not stop or stop > new_stop):
            return True, pnl, new_stop
        return False, pnl, new_stop

    def update_best_pnl(self, pos: dict, pnl: float) -> bool:
        best = float(pos.get("best_pnl_pct") or -999.0)
        if pnl > best:
            pos["best_pnl_pct"] = pnl
            return True
        if "best_pnl_pct" not in pos:
            pos["best_pnl_pct"] = pnl
            return True
        return False

    def trailing_exit_reason(self, pos: dict, pnl: float) -> str | None:
        if not (self.enabled and self.trailing_enabled):
            return None
        best = float(pos.get("best_pnl_pct") or pnl)
        if best >= self.trailing_start_pct and (best - pnl) >= self.trailing_giveback_pct and pnl > 0:
            return "trailing_scalp_exit"
        return None

    def time_stop_reason(self, pos: dict, pnl: float, age_sec: float, base_time_stop_sec: int) -> str | None:
        if not self.enabled:
            return "time_stop" if age_sec >= base_time_stop_sec else None
        if age_sec < self.time_min_sec:
            return None
        # Flat/no-progress scalp: exit before the hard timeout if price is going nowhere.
        if age_sec >= max(self.time_min_sec, base_time_stop_sec * 0.5) and abs(pnl) <= self.stale_pnl_abs_pct:
            return "smart_time_stop_stale"
        # If it is working and not retracing, allow a limited extension instead of cutting too early.
        if age_sec >= base_time_stop_sec and pnl >= self.time_extend_profit_pct:
            if age_sec < base_time_stop_sec + self.time_max_extend_sec:
                return None
            return "smart_time_stop_extended"
        if age_sec >= base_time_stop_sec:
            return "time_stop"
        return None
