
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
        risk_usdt = max(0.0, equity * risk_pct)
        stop_distance = price * (sl_pct / 100.0)
        if stop_distance <= 0: return None
        qty = risk_usdt / stop_distance
        notional = clamp(qty * price, self.min_order_usdt, self.max_order_usdt)
        qty = notional / price
        if side == "LONG":
            stop = price * (1 - sl_pct / 100.0); take = price * (1 + tp_pct / 100.0)
        else:
            stop = price * (1 + sl_pct / 100.0); take = price * (1 - tp_pct / 100.0)
        strategy = str(candidate.get("strategy", "momentum")).lower()
        order_type = "market" if strategy == "momentum" else "limit"
        return TradePlan(symbol=candidate["symbol"], side=side, order_type=order_type, qty=qty, entry_price=price, stop_price=stop, take_price=take, risk_pct=risk_pct, confidence=float(candidate.get("confidence",0)), strategy=strategy, mirror_used=bool(candidate.get("mirror_used", False)), session=str(candidate.get("session", "NORMAL")), max_open_positions=int(candidate.get("max_open_positions", settings.get("max_open_positions", 5))))
