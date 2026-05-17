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

        c["spot_volume_ratio"] = volume_ratio
        c["spot_move_pct"] = move
        c["futures_spot_divergence_pct"] = divergence

        if divergence > self.max_divergence_pct:
            c["confidence"] -= 12
            c["spot_confirmation"] = "DIVERGENCE"
            c["spot_confirmed"] = False
            c["spot_reason"] = "Futures/spot divergence too high"
        elif direction_ok and volume_ok:
            c["confidence"] += 8
            c["spot_confirmation"] = "CONFIRMED"
            c["spot_confirmed"] = True
            c["spot_reason"] = "Spot confirms futures signal"
        else:
            c["confidence"] -= 6
            c["spot_confirmation"] = "WEAK"
            c["spot_confirmed"] = False
            c["spot_reason"] = "Spot confirmation weak"
        return c
