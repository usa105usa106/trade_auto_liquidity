from statistics import mean


def _pct(a: float, b: float) -> float:
    return ((a - b) / b * 100.0) if b else 0.0


def _ema(values: list[float], period: int) -> float:
    if not values:
        return 0.0
    k = 2 / (period + 1)
    e = float(values[0])
    for v in values[1:]:
        e = float(v) * k + e * (1 - k)
    return e


class RegimeEngine:
    """
    Market-regime detector used by the live scanner.

    Inputs are intentionally simple and exchange-agnostic: OHLCV candles and/or
    ticker breadth. The output is consumed by Scanner + AdaptiveEngine so the
    bot does not merely store a regime flag; it changes strategy selection,
    candidate count, and universe size.
    """

    def detect(self, volatility: float, trend_strength: float, chop: float) -> str:
        if volatility >= 1.5:
            return "HIGH_VOLATILITY"
        if trend_strength >= 0.7 and chop <= 0.45:
            return "TRENDING"
        if chop >= 0.70:
            return "CHOPPY"
        return "LOW_VOLATILITY"

    def detect_from_candles(self, candles: list[list[float]]) -> dict:
        if not candles or len(candles) < 30:
            return {"regime": "LOW_VOLATILITY", "volatility": 0.0, "trend_strength": 0.0, "chop": 1.0}

        closes = [float(c[4]) for c in candles if len(c) >= 5]
        highs = [float(c[2]) for c in candles if len(c) >= 5]
        lows = [float(c[3]) for c in candles if len(c) >= 5]
        if len(closes) < 30:
            return {"regime": "LOW_VOLATILITY", "volatility": 0.0, "trend_strength": 0.0, "chop": 1.0}

        last_close = closes[-1]
        window = min(48, len(closes) - 1)
        ranges_pct = [((highs[-i] - lows[-i]) / closes[-i] * 100.0) for i in range(1, window + 1) if closes[-i]]
        volatility = mean(ranges_pct) if ranges_pct else 0.0

        fast = _ema(closes[-50:], 12)
        slow = _ema(closes[-80:], 26) if len(closes) >= 80 else _ema(closes, 26)
        ema_gap_pct = abs(_pct(fast, slow))
        directional_move = abs(_pct(closes[-1], closes[-window]))
        total_path = sum(abs(_pct(closes[i], closes[i - 1])) for i in range(len(closes) - window + 1, len(closes)))
        efficiency = min(1.0, directional_move / total_path) if total_path else 0.0
        trend_strength = min(1.0, (ema_gap_pct / 0.8) * 0.55 + efficiency * 0.45)
        chop = max(0.0, min(1.0, 1.0 - efficiency))

        regime = self.detect(volatility=volatility, trend_strength=trend_strength, chop=chop)
        return {
            "regime": regime,
            "volatility": round(volatility, 4),
            "trend_strength": round(trend_strength, 4),
            "chop": round(chop, 4),
            "efficiency": round(efficiency, 4),
        }

    def detect_from_tickers(self, tickers: dict) -> dict:
        rows = []
        for sym, t in (tickers or {}).items():
            if "USDT" not in str(sym):
                continue
            try:
                pct = abs(float(t.get("percentage") or 0.0))
                qv = float(t.get("quoteVolume") or 0.0)
            except Exception:
                continue
            if qv > 0:
                rows.append((pct, qv))
        if not rows:
            return {"regime": "LOW_VOLATILITY", "volatility": 0.0, "trend_strength": 0.0, "chop": 1.0, "breadth_count": 0}
        rows.sort(reverse=True)
        top = rows[: min(50, len(rows))]
        avg_abs_change = mean(p for p, _ in top)
        # Breadth proxy: many symbols moving hard usually means high-volatility regime.
        volatility = min(3.0, avg_abs_change / 2.0)
        trend_strength = min(1.0, avg_abs_change / 6.0)
        chop = max(0.0, min(1.0, 1.0 - trend_strength))
        regime = self.detect(volatility=volatility, trend_strength=trend_strength, chop=chop)
        return {
            "regime": regime,
            "volatility": round(volatility, 4),
            "trend_strength": round(trend_strength, 4),
            "chop": round(chop, 4),
            "breadth_count": len(rows),
        }

    def weights(self, regime: str) -> dict:
        if regime == "TRENDING":
            return {"momentum": 0.65, "pullback": 0.25, "reversal": 0.10}
        if regime == "CHOPPY":
            return {"momentum": 0.15, "pullback": 0.25, "reversal": 0.60}
        if regime == "HIGH_VOLATILITY":
            return {"momentum": 0.45, "pullback": 0.35, "reversal": 0.20}
        return {"momentum": 0.30, "pullback": 0.50, "reversal": 0.20}
