from dataclasses import dataclass

@dataclass
class SpotConfirmationResult:
    confirmed: bool
    score_delta: float
    reason: str

class SpotConfirmationEngine:
    def __init__(self, enabled=True, min_volume_ratio=1.15, min_move_pct=0.05, max_divergence_pct=0.35):
        self.enabled = enabled
        self.min_volume_ratio = min_volume_ratio
        self.min_move_pct = min_move_pct
        self.max_divergence_pct = max_divergence_pct

    def apply(self, candidate: dict, spot_data: dict | None) -> dict:
        c = dict(candidate)
        if not self.enabled:
            c["spot_confirmation"] = "OFF"
            c["spot_confirmed"] = True
            c["spot_reason"] = "Spot confirmation disabled"
            return c
        if not spot_data:
            c["confidence"] = float(c.get("confidence",0)) - 8
            c["spot_confirmation"] = "WEAK"
            c["spot_confirmed"] = False
            c["spot_reason"] = "Spot data unavailable"
            return c

        futures_price = float(c.get("futures_price", 0) or 0)
        spot_price = float(spot_data.get("spot_price", 0) or 0)
        vol_now = float(spot_data.get("spot_volume_now", 0) or 0)
        vol_avg = float(spot_data.get("spot_volume_avg", 1) or 1)
        move = float(spot_data.get("spot_price_change_pct", 0) or 0)
        side = str(c.get("side")).upper()

        if futures_price <= 0 or spot_price <= 0:
            c["confidence"] -= 10
            c["spot_confirmation"] = "WEAK"
            c["spot_confirmed"] = False
            c["spot_reason"] = "Invalid spot data"
            return c

        divergence = abs((futures_price - spot_price)/spot_price)*100
        volume_ratio = vol_now / vol_avg if vol_avg else 0
        direction_ok = move >= self.min_move_pct if side == "LONG" else move <= -self.min_move_pct
        volume_ok = volume_ratio >= self.min_volume_ratio
        strategy = str(c.get("strategy") or "").lower()
        spot_ob_imb = float(spot_data.get("spot_orderbook_imbalance", 0) or 0)
        spot_delta_ratio = float(spot_data.get("spot_delta_ratio", 0) or 0)
        if strategy == "orderflow_impulse":
            direction_ok = (move >= 0 and spot_delta_ratio > 0 and spot_ob_imb > 0) if side == "LONG" else (move <= 0 and spot_delta_ratio < 0 and spot_ob_imb < 0)
            volume_ok = volume_ratio >= max(self.min_volume_ratio, 1.5)

        c["spot_volume_ratio"] = volume_ratio
        c["spot_move_pct"] = move
        c["spot_orderbook_imbalance"] = spot_ob_imb
        c["spot_delta_ratio"] = spot_delta_ratio
        c["spot_delta_usdt"] = float(spot_data.get("spot_delta_usdt", 0) or 0)
        c["spot_buy_volume_usdt"] = float(spot_data.get("spot_buy_volume_usdt", 0) or 0)
        c["spot_sell_volume_usdt"] = float(spot_data.get("spot_sell_volume_usdt", 0) or 0)
        c["spot_bid_depth_usdt"] = float(spot_data.get("spot_bid_depth_usdt", 0) or 0)
        c["spot_ask_depth_usdt"] = float(spot_data.get("spot_ask_depth_usdt", 0) or 0)
        c["futures_spot_divergence_pct"] = divergence
        # Keep the actual Binance/MEXC spot orderflow metrics inside score_details too.
        # TradePlanner persists score_details into the position, so Telegram open
        # messages and /log can prove that this mode used real spot delta, real
        # executed volume and real orderbook imbalance instead of only a marker.
        if strategy == "orderflow_impulse":
            details = dict(c.get("score_details") or {})
            details.update({
                "spot_source": spot_data.get("spot_source"),
                "spot_move_pct": round(move, 4),
                "spot_volume_ratio": round(volume_ratio, 4),
                "spot_orderbook_imbalance": round(spot_ob_imb, 5),
                "spot_delta_ratio": round(spot_delta_ratio, 5),
                "spot_delta_usdt": round(float(spot_data.get("spot_delta_usdt", 0) or 0), 4),
                "spot_buy_volume_usdt": round(float(spot_data.get("spot_buy_volume_usdt", 0) or 0), 4),
                "spot_sell_volume_usdt": round(float(spot_data.get("spot_sell_volume_usdt", 0) or 0), 4),
                "spot_bid_depth_usdt": round(float(spot_data.get("spot_bid_depth_usdt", 0) or 0), 4),
                "spot_ask_depth_usdt": round(float(spot_data.get("spot_ask_depth_usdt", 0) or 0), 4),
                "futures_spot_divergence_pct": round(divergence, 5),
            })
            c["score_details"] = details

        if divergence > self.max_divergence_pct:
            c["confidence"] -= 12
            c["spot_confirmation"] = "DIVERGENCE"
            c["spot_confirmed"] = False
            c["spot_reason"] = "Futures/spot divergence too high"
        elif direction_ok and volume_ok:
            c["confidence"] += 8
            c["spot_confirmation"] = "CONFIRMED"
            c["spot_confirmed"] = True
            c["spot_reason"] = "Spot confirms futures signal" if strategy != "orderflow_impulse" else "Binance spot delta/orderbook/volume confirms orderflow"
        else:
            c["confidence"] -= 6
            c["spot_confirmation"] = "WEAK"
            c["spot_confirmed"] = False
            c["spot_reason"] = "Spot confirmation weak" if strategy != "orderflow_impulse" else "Binance spot orderflow weak/misaligned"
        return c
