import math
from statistics import mean

def pct(a: float, b: float) -> float:
    if not b:
        return 0.0
    return (a - b) / b * 100.0

def clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))

def ema(values: list[float], period: int) -> float:
    if not values:
        return 0.0
    k = 2 / (period + 1)
    e = values[0]
    for v in values[1:]:
        e = v * k + e * (1 - k)
    return e

def atr_pct(candles: list[list[float]], period: int = 14) -> float:
    """
    candles: [timestamp, open, high, low, close, volume]
    returns ATR as percent of close
    """
    if len(candles) < period + 2:
        return 0.0
    trs = []
    prev_close = candles[-period-1][4]
    for c in candles[-period:]:
        high, low, close = float(c[2]), float(c[3]), float(c[4])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
        prev_close = close
    last_close = float(candles[-1][4])
    return (mean(trs) / last_close * 100.0) if last_close else 0.0

class SignalEngine:
    """
    Real futures-first signal engine.

    Generates trade candidates from real OHLCV + ticker/orderbook metrics:
    - Momentum breakout
    - Pullback reclaim
    - Reversal / liquidity sweep

    It does NOT fabricate trades. If market data is missing or confidence is low,
    it returns no candidate.
    """

    def __init__(
        self,
        min_confidence: float = 70.0,
        volume_spike_mult: float = 1.8,
        breakout_lookback: int = 20,
        momentum_threshold_pct: float = 0.18,
        max_candidates_per_cycle: int = 8,
    ):
        self.min_confidence = float(min_confidence)
        self.volume_spike_mult = float(volume_spike_mult)
        self.breakout_lookback = int(breakout_lookback)
        self.momentum_threshold_pct = float(momentum_threshold_pct)
        self.max_candidates_per_cycle = int(max_candidates_per_cycle)

    def analyze_symbol(
        self,
        symbol: str,
        candles: list[list[float]],
        ticker: dict | None = None,
        orderbook: dict | None = None,
        preferred_strategy: str = "hybrid",
    ) -> dict | None:
        if not candles or len(candles) < max(30, self.breakout_lookback + 5):
            return None

        closes = [float(c[4]) for c in candles]
        highs = [float(c[2]) for c in candles]
        lows = [float(c[3]) for c in candles]
        vols = [float(c[5]) for c in candles]
        last = candles[-1]
        prev = candles[-2]
        open_, high, low, close, vol = map(float, [last[1], last[2], last[3], last[4], last[5]])
        prev_close = float(prev[4])

        vol_avg = mean(vols[-21:-1]) if len(vols) >= 22 else mean(vols[:-1])
        vol_ratio = vol / vol_avg if vol_avg else 0.0
        move_1m = pct(close, prev_close)
        move_5m = pct(close, closes[-6]) if len(closes) >= 6 else 0.0
        ema_fast = ema(closes[-30:], 9)
        ema_slow = ema(closes[-60:], 21) if len(closes) >= 60 else ema(closes, 21)
        trend_up = ema_fast > ema_slow
        trend_down = ema_fast < ema_slow
        atrp = atr_pct(candles)

        prior_high = max(highs[-self.breakout_lookback-1:-1])
        prior_low = min(lows[-self.breakout_lookback-1:-1])
        breakout_up = close > prior_high
        breakout_down = close < prior_low

        orderbook_metrics = self._orderbook_metrics(orderbook)
        if not orderbook_metrics["valid"]:
            return None
        spread_pct = orderbook_metrics["spread_pct"]
        depth_usdt = orderbook_metrics["depth_usdt"]
        imbalance = orderbook_metrics["imbalance"]

        candidates = []
        if preferred_strategy in {"hybrid", "momentum"}:
            candidates += self._momentum(symbol, close, move_1m, move_5m, vol_ratio, breakout_up, breakout_down, trend_up, trend_down, atrp, spread_pct, depth_usdt, imbalance)
        if preferred_strategy in {"hybrid", "pullback"}:
            candidates += self._pullback(symbol, close, candles, trend_up, trend_down, vol_ratio, atrp, spread_pct, depth_usdt, imbalance)
        if preferred_strategy in {"hybrid", "reversal"}:
            candidates += self._reversal(symbol, close, candles, prior_high, prior_low, vol_ratio, atrp, spread_pct, depth_usdt, imbalance)

        candidates = [c for c in candidates if c["confidence"] >= self.min_confidence]
        if not candidates:
            return None
        candidates.sort(key=lambda x: x["confidence"], reverse=True)
        return candidates[0]

    def _base(self, symbol, side, strategy, price, confidence, spread_pct, depth_usdt, atrp, details):
        expected_slippage_pct = self._estimate_slippage_pct(spread_pct, depth_usdt)
        return {
            "symbol": symbol,
            "side": side,
            "strategy": strategy,
            "confidence": round(clamp(confidence), 2),
            "futures_price": float(price),
            "spread_pct": float(spread_pct),
            "expected_slippage_pct": float(expected_slippage_pct),
            "depth_usdt": float(depth_usdt),
            "atr_pct": float(atrp),
            "min_confidence": self.min_confidence,
            "score_details": details,
        }

    def _momentum(self, symbol, close, move_1m, move_5m, vol_ratio, breakout_up, breakout_down, trend_up, trend_down, atrp, spread_pct, depth_usdt, imbalance):
        out = []
        long_score = 45
        long_score += min(20, max(0, move_1m) * 35)
        long_score += min(15, max(0, move_5m) * 8)
        long_score += min(15, max(0, vol_ratio - 1) * 8)
        long_score += 10 if breakout_up else 0
        long_score += 8 if trend_up else 0
        long_score += 6 if imbalance > 0.12 else 0
        long_score -= 8 if spread_pct > 0.12 else 0

        short_score = 45
        short_score += min(20, abs(min(0, move_1m)) * 35)
        short_score += min(15, abs(min(0, move_5m)) * 8)
        short_score += min(15, max(0, vol_ratio - 1) * 8)
        short_score += 10 if breakout_down else 0
        short_score += 8 if trend_down else 0
        short_score += 6 if imbalance < -0.12 else 0
        short_score -= 8 if spread_pct > 0.12 else 0

        if move_1m >= self.momentum_threshold_pct and vol_ratio >= self.volume_spike_mult:
            out.append(self._base(symbol, "LONG", "momentum", close, long_score, spread_pct, depth_usdt, atrp, {
                "move_1m": move_1m, "move_5m": move_5m, "vol_ratio": vol_ratio, "breakout": breakout_up, "imbalance": imbalance
            }))
        if move_1m <= -self.momentum_threshold_pct and vol_ratio >= self.volume_spike_mult:
            out.append(self._base(symbol, "SHORT", "momentum", close, short_score, spread_pct, depth_usdt, atrp, {
                "move_1m": move_1m, "move_5m": move_5m, "vol_ratio": vol_ratio, "breakout": breakout_down, "imbalance": imbalance
            }))
        return out

    def _pullback(self, symbol, close, candles, trend_up, trend_down, vol_ratio, atrp, spread_pct, depth_usdt, imbalance):
        out = []
        closes = [float(c[4]) for c in candles]
        highs = [float(c[2]) for c in candles]
        lows = [float(c[3]) for c in candles]
        last_low = float(candles[-1][3])
        last_high = float(candles[-1][2])
        ema9 = ema(closes[-30:], 9)
        ema21 = ema(closes[-60:], 21) if len(closes) >= 60 else ema(closes, 21)

        # pullback reclaim long: trend up, candle dipped below ema9/ema21 area and closed back above ema9
        if trend_up and last_low <= ema9 and close > ema9 and vol_ratio >= 1.1:
            score = 58 + min(15, (vol_ratio - 1) * 8) + (8 if imbalance > 0 else 0) + min(10, atrp * 2)
            out.append(self._base(symbol, "LONG", "pullback", close, score, spread_pct, depth_usdt, atrp, {
                "ema9": ema9, "ema21": ema21, "vol_ratio": vol_ratio, "reclaim": True, "imbalance": imbalance
            }))

        if trend_down and last_high >= ema9 and close < ema9 and vol_ratio >= 1.1:
            score = 58 + min(15, (vol_ratio - 1) * 8) + (8 if imbalance < 0 else 0) + min(10, atrp * 2)
            out.append(self._base(symbol, "SHORT", "pullback", close, score, spread_pct, depth_usdt, atrp, {
                "ema9": ema9, "ema21": ema21, "vol_ratio": vol_ratio, "reclaim": True, "imbalance": imbalance
            }))
        return out

    def _reversal(self, symbol, close, candles, prior_high, prior_low, vol_ratio, atrp, spread_pct, depth_usdt, imbalance):
        out = []
        last = candles[-1]
        open_, high, low, close_ = map(float, [last[1], last[2], last[3], last[4]])
        rng = max(1e-12, high - low)
        upper_wick = (high - max(open_, close_)) / rng
        lower_wick = (min(open_, close_) - low) / rng
        swept_high = high > prior_high and close_ < prior_high
        swept_low = low < prior_low and close_ > prior_low

        if swept_high and upper_wick >= 0.35 and vol_ratio >= 1.4:
            score = 62 + min(15, (vol_ratio - 1) * 8) + (8 if imbalance < 0 else 0) + min(8, upper_wick*10)
            out.append(self._base(symbol, "SHORT", "reversal", close_, score, spread_pct, depth_usdt, atrp, {
                "sweep": "high", "upper_wick": upper_wick, "vol_ratio": vol_ratio, "imbalance": imbalance
            }))

        if swept_low and lower_wick >= 0.35 and vol_ratio >= 1.4:
            score = 62 + min(15, (vol_ratio - 1) * 8) + (8 if imbalance > 0 else 0) + min(8, lower_wick*10)
            out.append(self._base(symbol, "LONG", "reversal", close_, score, spread_pct, depth_usdt, atrp, {
                "sweep": "low", "lower_wick": lower_wick, "vol_ratio": vol_ratio, "imbalance": imbalance
            }))
        return out

    def _orderbook_metrics(self, orderbook):
        if not orderbook:
            return {"valid": False, "spread_pct": 999.0, "depth_usdt": 0.0, "imbalance": 0.0}
        bids = orderbook.get("bids") or []
        asks = orderbook.get("asks") or []
        if not bids or not asks:
            return {"valid": False, "spread_pct": 999.0, "depth_usdt": 0.0, "imbalance": 0.0}
        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        mid = (best_bid + best_ask) / 2
        spread_pct = (best_ask - best_bid) / mid * 100 if mid else 999
        bid_depth = sum(float(p)*float(q) for p,q,*_ in bids[:10])
        ask_depth = sum(float(p)*float(q) for p,q,*_ in asks[:10])
        total = bid_depth + ask_depth
        imbalance = (bid_depth - ask_depth) / total if total else 0.0
        return {"valid": True, "spread_pct": spread_pct, "depth_usdt": total, "imbalance": imbalance}

    def _estimate_slippage_pct(self, spread_pct, depth_usdt):
        if depth_usdt <= 0:
            return 999.0
        depth_penalty = 0.02 if depth_usdt > 100000 else 0.05 if depth_usdt > 25000 else 0.15
        return spread_pct / 2 + depth_penalty
