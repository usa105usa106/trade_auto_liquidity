import os
from models import TradePlan


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


class TradePlanner:
    def __init__(self):
        self.default_equity = float(os.getenv("DEFAULT_EQUITY_USDT", "1000"))
        self.min_order_usdt = float(os.getenv("MIN_ORDER_USDT", "5"))
        self.max_order_usdt = float(os.getenv("MAX_ORDER_USDT", "100"))
        self.tp_atr_mult = float(os.getenv("TP_ATR_MULT", "2.2"))
        self.sl_atr_mult = float(os.getenv("SL_ATR_MULT", "1.2"))
        self.min_tp_pct = float(os.getenv("MIN_TP_PCT", "0.25"))
        self.max_tp_pct = float(os.getenv("MAX_TP_PCT", "1.20"))
        self.min_sl_pct = float(os.getenv("MIN_SL_PCT", "0.15"))
        self.max_sl_pct = float(os.getenv("MAX_SL_PCT", "0.60"))

    @staticmethod
    def _bool_setting(settings: dict, key: str, default: bool = True) -> bool:
        raw = settings.get(key, os.getenv(key.upper(), default))
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}

    def make_plan(self, candidate: dict, settings: dict, equity_usdt: float | None = None) -> TradePlan | None:
        price = float(candidate.get("futures_price") or 0)
        side = str(candidate.get("side", "")).upper()
        if price <= 0 or side not in {"LONG", "SHORT"}:
            return None

        equity = float(equity_usdt or self.default_equity)
        risk_pct = float(candidate.get("risk_pct", settings.get("risk_pct", 0.005)))
        atr_pct = float(candidate.get("atr_pct") or 0.25)
        sl_pct = clamp(atr_pct * self.sl_atr_mult, self.min_sl_pct, self.max_sl_pct)
        tp_pct = clamp(atr_pct * self.tp_atr_mult, self.min_tp_pct, self.max_tp_pct)
        max_positions = max(1, int(candidate.get("max_open_positions", settings.get("max_open_positions", 5)) or 5))
        leverage = max(1, int(float(settings.get("mexc_order_leverage", os.getenv("MEXC_ORDER_LEVERAGE", "5")) or 5)))

        risk_usdt = max(0.0, equity * risk_pct)
        stop_distance = price * (sl_pct / 100.0)
        if stop_distance <= 0:
            return None

        # Classic risk sizing: position notional based on loss at SL.
        risk_qty = risk_usdt / stop_distance
        risk_notional = risk_qty * price

        # v0064 safety: fixed margin allocation per slot.
        # Example: 50 USDT balance / 5 max positions = 10 USDT max margin.
        # With 5x leverage, max notional for one trade = 50 USDT.
        max_margin_per_position = equity / max_positions if max_positions > 0 else equity
        max_notional_by_margin = max_margin_per_position * leverage

        notional_ceiling = self.max_order_usdt
        if self._bool_setting(settings, "margin_allocation_enabled", True):
            notional_ceiling = min(notional_ceiling, max_notional_by_margin)

        if notional_ceiling < self.min_order_usdt:
            # Account is too small for the configured number of slots/leverage/min order.
            return None

        notional = clamp(risk_notional, self.min_order_usdt, notional_ceiling)
        qty = notional / price
        expected_margin = notional / leverage if leverage > 0 else notional

        if side == "LONG":
            stop = price * (1 - sl_pct / 100.0)
            take = price * (1 + tp_pct / 100.0)
        else:
            stop = price * (1 + sl_pct / 100.0)
            take = price * (1 - tp_pct / 100.0)

        strategy = str(candidate.get("strategy", "momentum")).lower()
        order_type = "market" if strategy == "momentum" else "limit"
        return TradePlan(
            symbol=candidate["symbol"],
            side=side,
            order_type=order_type,
            qty=qty,
            entry_price=price,
            stop_price=stop,
            take_price=take,
            risk_pct=risk_pct,
            confidence=float(candidate.get("confidence", 0)),
            strategy=strategy,
            mirror_used=bool(candidate.get("mirror_used", False)),
            session=str(candidate.get("session", "NORMAL")),
            max_open_positions=max_positions,
            planned_notional_usdt=notional,
            expected_margin_usdt=expected_margin,
            max_margin_per_position_usdt=max_margin_per_position,
            leverage=leverage,
        )
