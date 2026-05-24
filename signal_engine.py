import math
import os
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
        self.weak_momentum_filter_enabled = str(os.getenv("WEAK_MOMENTUM_FILTER_ENABLED", "true")).lower() in {"1", "true", "yes", "on"}
        self.momentum_min_5m_confirm_pct = float(os.getenv("MOMENTUM_MIN_5M_CONFIRM_PCT", "0.05"))
        self.momentum_min_imbalance_abs = float(os.getenv("MOMENTUM_MIN_IMBALANCE_ABS", "0.02"))
        self.momentum_max_spread_pct = float(os.getenv("MOMENTUM_MAX_SPREAD_PCT", "0.12"))
        self.liquidity_retest_zone_tolerance_pct = float(os.getenv("LIQUIDITY_RETEST_ZONE_TOLERANCE_PCT", "0.08"))
        self.liquidity_retest_min_sweep_wick = float(os.getenv("LIQUIDITY_RETEST_MIN_SWEEP_WICK", "0.25"))
        self.liquidity_retest_min_reclaim_pct = float(os.getenv("LIQUIDITY_RETEST_MIN_RECLAIM_PCT", "0.04"))
        self.liquidity_retest_min_displacement_pct = float(os.getenv("LIQUIDITY_RETEST_MIN_DISPLACEMENT_PCT", "0.10"))
        self.liquidity_retest_min_displacement_body = float(os.getenv("LIQUIDITY_RETEST_MIN_DISPLACEMENT_BODY", "0.55"))
        self.liquidity_retest_min_volume_ratio = float(os.getenv("LIQUIDITY_RETEST_MIN_VOLUME_RATIO", "1.15"))
        self.liquidity_retest_max_spread_pct = float(os.getenv("LIQUIDITY_RETEST_MAX_SPREAD_PCT", "0.18"))
        self.liquidity_retest_min_target_rr = float(os.getenv("LIQUIDITY_RETEST_MIN_TARGET_RR", "1.8"))
        self.liquidity_retest_min_retest_rejection_wick = float(os.getenv("LIQUIDITY_RETEST_MIN_RETEST_REJECTION_WICK", "0.25"))
        self.liquidity_retest_min_zone_quality = float(os.getenv("LIQUIDITY_RETEST_MIN_ZONE_QUALITY", "2.0"))
        self.liquidity_retest_mtf_enabled = str(os.getenv("LIQUIDITY_RETEST_MTF_ENABLED", "true")).lower() in {"1", "true", "yes", "on"}
        self.liquidity_retest_min_mtf_score = float(os.getenv("LIQUIDITY_RETEST_MIN_MTF_SCORE", "-0.25"))
        self.liquidity_retest_require_clean_path = str(os.getenv("LIQUIDITY_RETEST_REQUIRE_CLEAN_PATH", "false")).lower() in {"1", "true", "yes", "on"}
        self.liquidity_retest_quality_mode = str(os.getenv("LIQUIDITY_RETEST_QUALITY_MODE", "a_plus") or "a_plus").lower()
        self.quick_bounce_drop_4h_pct = float(os.getenv("QUICK_BOUNCE_DROP_4H_PCT", "5.0"))
        self.quick_bounce_pump_4h_pct = float(os.getenv("QUICK_BOUNCE_PUMP_4H_PCT", "5.0"))
        self.quick_bounce_reversal_pct = float(os.getenv("QUICK_BOUNCE_REVERSAL_PCT", "1.0"))
        self.quick_bounce_min_volume_ratio = float(os.getenv("QUICK_BOUNCE_MIN_VOLUME_RATIO", "1.15"))
        self.quick_bounce_max_spread_pct = float(os.getenv("QUICK_BOUNCE_MAX_SPREAD_PCT", "0.30"))
        self.quick_bounce_min_24h_volume_usdt = float(os.getenv("QUICK_BOUNCE_MIN_24H_VOLUME_USDT", "20000000"))
        self.quick_bounce_btc_filter_enabled = str(os.getenv("QUICK_BOUNCE_BTC_FILTER_ENABLED", "true")).lower() in {"1", "true", "yes", "on"}
        self.quick_bounce_btc_max_drop_1h_pct = float(os.getenv("QUICK_BOUNCE_BTC_MAX_DROP_1H_PCT", "2.0"))
        self.quick_bounce_btc_max_pump_1h_pct = float(os.getenv("QUICK_BOUNCE_BTC_MAX_PUMP_1H_PCT", "2.0"))

    @staticmethod
    def _truthy(value, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _float_setting(settings: dict, key: str, current: float) -> float:
        try:
            return float(settings.get(key, current))
        except Exception:
            return current

    def configure_from_settings(self, settings: dict | None = None) -> None:
        """Apply runtime /set values before each scan cycle.

        v0084: several signal thresholds were stored in SQLite by /set but the
        signal engine kept reading only process ENV, so changed values did not
        affect live scanning until restart. This keeps menu /set behavior real.
        """
        settings = settings or {}
        self.weak_momentum_filter_enabled = self._truthy(settings.get("weak_momentum_filter_enabled"), self.weak_momentum_filter_enabled)
        self.momentum_min_5m_confirm_pct = self._float_setting(settings, "momentum_min_5m_confirm_pct", self.momentum_min_5m_confirm_pct)
        self.momentum_min_imbalance_abs = self._float_setting(settings, "momentum_min_imbalance_abs", self.momentum_min_imbalance_abs)
        self.momentum_max_spread_pct = self._float_setting(settings, "momentum_max_spread_pct", self.momentum_max_spread_pct)
        self.liquidity_retest_zone_tolerance_pct = self._float_setting(settings, "liquidity_retest_zone_tolerance_pct", self.liquidity_retest_zone_tolerance_pct)
        self.liquidity_retest_min_sweep_wick = self._float_setting(settings, "liquidity_retest_min_sweep_wick", self.liquidity_retest_min_sweep_wick)
        self.liquidity_retest_min_reclaim_pct = self._float_setting(settings, "liquidity_retest_min_reclaim_pct", self.liquidity_retest_min_reclaim_pct)
        self.liquidity_retest_min_displacement_pct = self._float_setting(settings, "liquidity_retest_min_displacement_pct", self.liquidity_retest_min_displacement_pct)
        self.liquidity_retest_min_displacement_body = self._float_setting(settings, "liquidity_retest_min_displacement_body", self.liquidity_retest_min_displacement_body)
        self.liquidity_retest_min_volume_ratio = self._float_setting(settings, "liquidity_retest_min_volume_ratio", self.liquidity_retest_min_volume_ratio)
        self.liquidity_retest_max_spread_pct = self._float_setting(settings, "liquidity_retest_max_spread_pct", self.liquidity_retest_max_spread_pct)
        self.liquidity_retest_min_target_rr = self._float_setting(settings, "liquidity_retest_min_target_rr", self.liquidity_retest_min_target_rr)
        self.liquidity_retest_min_retest_rejection_wick = self._float_setting(settings, "liquidity_retest_min_retest_rejection_wick", self.liquidity_retest_min_retest_rejection_wick)
        self.liquidity_retest_min_zone_quality = self._float_setting(settings, "liquidity_retest_min_zone_quality", self.liquidity_retest_min_zone_quality)
        self.liquidity_retest_mtf_enabled = self._truthy(settings.get("liquidity_retest_mtf_enabled"), self.liquidity_retest_mtf_enabled)
        self.liquidity_retest_min_mtf_score = self._float_setting(settings, "liquidity_retest_min_mtf_score", self.liquidity_retest_min_mtf_score)
        self.liquidity_retest_require_clean_path = self._truthy(settings.get("liquidity_retest_require_clean_path"), self.liquidity_retest_require_clean_path)
        self.liquidity_retest_quality_mode = str(settings.get("liquidity_retest_quality_mode", self.liquidity_retest_quality_mode) or "a_plus").lower()
        self.quick_bounce_drop_4h_pct = self._float_setting(settings, "quick_bounce_drop_4h_pct", self.quick_bounce_drop_4h_pct)
        self.quick_bounce_pump_4h_pct = self._float_setting(settings, "quick_bounce_pump_4h_pct", self.quick_bounce_pump_4h_pct)
        self.quick_bounce_reversal_pct = self._float_setting(settings, "quick_bounce_reversal_pct", self.quick_bounce_reversal_pct)
        self.quick_bounce_min_volume_ratio = self._float_setting(settings, "quick_bounce_min_volume_ratio", self.quick_bounce_min_volume_ratio)
        self.quick_bounce_max_spread_pct = self._float_setting(settings, "quick_bounce_max_spread_pct", self.quick_bounce_max_spread_pct)
        self.quick_bounce_min_24h_volume_usdt = self._float_setting(settings, "quick_bounce_min_24h_volume_usdt", self.quick_bounce_min_24h_volume_usdt)
        self.quick_bounce_btc_filter_enabled = self._truthy(settings.get("quick_bounce_btc_filter_enabled"), self.quick_bounce_btc_filter_enabled)
        self.quick_bounce_btc_max_drop_1h_pct = self._float_setting(settings, "quick_bounce_btc_max_drop_1h_pct", self.quick_bounce_btc_max_drop_1h_pct)
        self.quick_bounce_btc_max_pump_1h_pct = self._float_setting(settings, "quick_bounce_btc_max_pump_1h_pct", self.quick_bounce_btc_max_pump_1h_pct)
        self._apply_liquidity_retest_quality_profile()

    def _apply_liquidity_retest_quality_profile(self) -> None:
        """Runtime liquidity_retest quality presets.

        a_plus = old strict behavior. normal/aggressive only relax thresholds;
        they do not reduce the scanned universe.
        """
        mode = str(getattr(self, "liquidity_retest_quality_mode", "a_plus") or "a_plus").lower().replace("+", "_plus").replace("-", "_")
        if mode in {"a", "a_plus", "aplus", "strict"}:
            return
        if mode == "normal":
            self.liquidity_retest_min_target_rr = min(self.liquidity_retest_min_target_rr, 1.45)
            self.liquidity_retest_min_zone_quality = min(self.liquidity_retest_min_zone_quality, 1.45)
            self.liquidity_retest_min_sweep_wick = min(self.liquidity_retest_min_sweep_wick, 0.18)
            self.liquidity_retest_min_reclaim_pct = min(self.liquidity_retest_min_reclaim_pct, 0.025)
            self.liquidity_retest_min_retest_rejection_wick = min(self.liquidity_retest_min_retest_rejection_wick, 0.16)
            self.liquidity_retest_min_displacement_pct = min(self.liquidity_retest_min_displacement_pct, 0.07)
            self.liquidity_retest_min_displacement_body = min(self.liquidity_retest_min_displacement_body, 0.42)
            self.liquidity_retest_min_volume_ratio = min(self.liquidity_retest_min_volume_ratio, 1.05)
            self.liquidity_retest_min_mtf_score = min(self.liquidity_retest_min_mtf_score, -0.45)
            return
        if mode == "aggressive":
            self.liquidity_retest_min_target_rr = min(self.liquidity_retest_min_target_rr, 1.20)
            self.liquidity_retest_min_zone_quality = min(self.liquidity_retest_min_zone_quality, 1.05)
            self.liquidity_retest_min_sweep_wick = min(self.liquidity_retest_min_sweep_wick, 0.12)
            self.liquidity_retest_min_reclaim_pct = min(self.liquidity_retest_min_reclaim_pct, 0.015)
            self.liquidity_retest_min_retest_rejection_wick = min(self.liquidity_retest_min_retest_rejection_wick, 0.10)
            self.liquidity_retest_min_displacement_pct = min(self.liquidity_retest_min_displacement_pct, 0.04)
            self.liquidity_retest_min_displacement_body = min(self.liquidity_retest_min_displacement_body, 0.30)
            self.liquidity_retest_min_volume_ratio = min(self.liquidity_retest_min_volume_ratio, 0.95)
            self.liquidity_retest_min_mtf_score = min(self.liquidity_retest_min_mtf_score, -0.70)
            self.liquidity_retest_require_clean_path = False

    def _liquidity_mode(self) -> str:
        return str(getattr(self, "liquidity_retest_quality_mode", "a_plus") or "a_plus").lower().replace("+", "_plus").replace("-", "_")

    def _liq_allows_partial_reclaim(self) -> bool:
        return self._liquidity_mode() in {"normal", "aggressive"}

    def _liq_allows_soft_structure(self) -> bool:
        return self._liquidity_mode() == "aggressive"

    def analyze_symbol(
        self,
        symbol: str,
        candles: list[list[float]],
        ticker: dict | None = None,
        orderbook: dict | None = None,
        preferred_strategy: str = "hybrid",
        mtf_candles: dict | None = None,
        market_context: dict | None = None,
    ) -> dict | None:
        self.last_reject_reason = "-"
        if not candles or len(candles) < max(30, self.breakout_lookback + 5):
            self.last_reject_reason = "not enough candles"
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
            self.last_reject_reason = "invalid orderbook"
            return None
        spread_pct = orderbook_metrics["spread_pct"]
        depth_usdt = orderbook_metrics["depth_usdt"]
        imbalance = orderbook_metrics["imbalance"]

        candidates = []
        if preferred_strategy in {"hybrid", "all", "momentum"}:
            candidates += self._momentum(symbol, close, move_1m, move_5m, vol_ratio, breakout_up, breakout_down, trend_up, trend_down, atrp, spread_pct, depth_usdt, imbalance)
        if preferred_strategy in {"hybrid", "all", "pullback"}:
            candidates += self._pullback(symbol, close, candles, trend_up, trend_down, vol_ratio, atrp, spread_pct, depth_usdt, imbalance)
        if preferred_strategy in {"hybrid", "all", "reversal"}:
            candidates += self._reversal(symbol, close, candles, prior_high, prior_low, vol_ratio, atrp, spread_pct, depth_usdt, imbalance)
        # v0082: Liquidity Retest is intentionally separate from hybrid/all.
        # It is not a scalp mode; user enables it explicitly for SMC-style tests.
        if preferred_strategy == "liquidity_retest":
            candidates += self._liquidity_retest(symbol, close, candles, vol_ratio, atrp, spread_pct, depth_usdt, imbalance)
        if preferred_strategy == "quick_bounce":
            candles_1h = (mtf_candles or {}).get("1h") if isinstance(mtf_candles, dict) else None
            candidates += self._quick_bounce(symbol, close, candles, vol_ratio, atrp, spread_pct, depth_usdt, imbalance, candles_1h=candles_1h, ticker=ticker, market_context=market_context)

        raw_candidates = list(candidates)
        candidates = [c for c in candidates if c["confidence"] >= self.min_confidence]
        if not candidates:
            if raw_candidates:
                best = max(raw_candidates, key=lambda x: float(x.get("confidence", 0)))
                self.last_reject_reason = f"confidence {float(best.get('confidence', 0)):.1f} < {self.min_confidence:.1f}"
            elif preferred_strategy == "liquidity_retest":
                self.last_reject_reason = getattr(self, "last_liquidity_retest_reject_reason", "liquidity_retest filters: no valid sweep/reclaim/retest/RR")
            elif preferred_strategy == "quick_bounce":
                self.last_reject_reason = getattr(self, "last_quick_bounce_reject_reason", "quick_bounce filters: no anomaly + reversal")
            else:
                self.last_reject_reason = "no strategy setup"
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

        long_ok = move_1m >= self.momentum_threshold_pct and vol_ratio >= self.volume_spike_mult
        short_ok = move_1m <= -self.momentum_threshold_pct and vol_ratio >= self.volume_spike_mult
        if self.weak_momentum_filter_enabled:
            # v0081: suppress weak momentum scalps. A 1m impulse alone is often
            # noise; require 5m direction confirmation, acceptable spread and at
            # least mild top-of-book support unless there is a clean breakout.
            long_ok = long_ok and move_5m >= self.momentum_min_5m_confirm_pct and spread_pct <= self.momentum_max_spread_pct and (breakout_up or imbalance >= self.momentum_min_imbalance_abs)
            short_ok = short_ok and move_5m <= -self.momentum_min_5m_confirm_pct and spread_pct <= self.momentum_max_spread_pct and (breakout_down or imbalance <= -self.momentum_min_imbalance_abs)
        if long_ok:
            out.append(self._base(symbol, "LONG", "momentum", close, long_score, spread_pct, depth_usdt, atrp, {
                "move_1m": move_1m, "move_5m": move_5m, "vol_ratio": vol_ratio, "breakout": breakout_up, "imbalance": imbalance, "weak_filter": self.weak_momentum_filter_enabled
            }))
        if short_ok:
            out.append(self._base(symbol, "SHORT", "momentum", close, short_score, spread_pct, depth_usdt, atrp, {
                "move_1m": move_1m, "move_5m": move_5m, "vol_ratio": vol_ratio, "breakout": breakout_down, "imbalance": imbalance, "weak_filter": self.weak_momentum_filter_enabled
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


    def _swing_points(self, highs, lows, left: int = 2, right: int = 2):
        swing_highs = []
        swing_lows = []
        n = len(highs)
        for i in range(left, max(left, n - right)):
            h = highs[i]
            l = lows[i]
            if all(h >= highs[j] for j in range(i-left, i+right+1) if j != i):
                swing_highs.append((i, h))
            if all(l <= lows[j] for j in range(i-left, i+right+1) if j != i):
                swing_lows.append((i, l))
        return swing_highs, swing_lows

    def _last_opposite_order_block(self, candles, start_idx: int, end_idx: int, side: str):
        """Return OB candle zone before displacement.

        For LONG we want the last bearish candle before bullish displacement.
        For SHORT we want the last bullish candle before bearish displacement.
        """
        step_range = range(max(1, end_idx - 6), max(1, start_idx - 1), -1)
        for j in step_range:
            o, h, l, c = map(float, [candles[j][1], candles[j][2], candles[j][3], candles[j][4]])
            if side == "LONG" and c < o:
                return {"index": j, "low": l, "high": o, "kind": "bullish_order_block"}
            if side == "SHORT" and c > o:
                return {"index": j, "low": o, "high": h, "kind": "bearish_order_block"}
        return None

    def _fvg_zone(self, candles, displacement_idx: int, side: str):
        """Simple 3-candle fair value gap/imbalance detector around displacement."""
        if displacement_idx < 2 or displacement_idx >= len(candles):
            return None
        prev2 = candles[displacement_idx - 2]
        cur = candles[displacement_idx]
        p2_h, p2_l = float(prev2[2]), float(prev2[3])
        cur_h, cur_l = float(cur[2]), float(cur[3])
        if side == "LONG" and cur_l > p2_h:
            return {"low": p2_h, "high": cur_l, "kind": "bullish_fvg"}
        if side == "SHORT" and cur_h < p2_l:
            return {"low": cur_h, "high": p2_l, "kind": "bearish_fvg"}
        return None

    def _rr_from_structure(self, *, side: str, score_bits: float, volume_ratio: float, displacement_pct: float, has_fvg: bool, bos_strength: float, bias: float = 0.0, zone_quality: float = 0.0, mtf_score: float = 0.0, clean_path: bool = True) -> float:
        strength = (
            score_bits
            + max(0.0, volume_ratio - 1.0) * 0.8
            + max(0.0, displacement_pct) * 3.0
            + max(0.0, bos_strength) * 2.0
            + (0.7 if has_fvg else 0.0)
            + max(0.0, zone_quality) * 0.35
            + max(0.0, mtf_score) * 0.8
            + (0.4 if clean_path else -0.8)
            + bias
        )
        if strength >= 5.4:
            return 4.0
        if strength >= 3.2:
            return 3.0
        return 2.0

    def _aggregate_candles(self, candles, group: int = 5):
        out = []
        if group <= 1:
            return candles
        for i in range(0, len(candles), group):
            chunk = candles[i:i + group]
            if len(chunk) < group:
                continue
            ts = chunk[0][0]
            o = float(chunk[0][1])
            h = max(float(c[2]) for c in chunk)
            l = min(float(c[3]) for c in chunk)
            c = float(chunk[-1][4])
            v = sum(float(c[5]) for c in chunk)
            out.append([ts, o, h, l, c, v])
        return out

    def _mtf_bias_score(self, candles, side: str) -> float:
        agg = self._aggregate_candles(candles, 5)
        if len(agg) < 8:
            return 0.0
        closes = [float(c[4]) for c in agg]
        highs = [float(c[2]) for c in agg]
        lows = [float(c[3]) for c in agg]
        ef = ema(closes[-12:], 3)
        es = ema(closes[-24:], 6) if len(closes) >= 24 else ema(closes, 6)
        last = closes[-1]
        prev_high = max(highs[-8:-1])
        prev_low = min(lows[-8:-1])
        score = 0.0
        if side == "LONG":
            score += 0.6 if ef >= es else -0.6
            score += 0.4 if last >= es else -0.2
            score += 0.5 if last > prev_high else 0.0
            score -= 0.7 if last < prev_low else 0.0
        else:
            score += 0.6 if ef <= es else -0.6
            score += 0.4 if last <= es else -0.2
            score += 0.5 if last < prev_low else 0.0
            score -= 0.7 if last > prev_high else 0.0
        return score

    def _zone_quality(self, *, zone_src: dict, fvg: dict | None, displacement_pct: float, volume_ratio: float, bos_strength: float, sweep_wick: float, retest_wick: float) -> float:
        q = 0.0
        kind = str((zone_src or {}).get("kind", ""))
        if "order_block" in kind:
            q += 1.0
        if fvg:
            q += 0.8
        q += min(1.0, max(0.0, displacement_pct) * 2.5)
        q += min(0.8, max(0.0, volume_ratio - 1.0) * 0.7)
        q += min(0.7, max(0.0, bos_strength) * 2.0)
        q += min(0.5, max(0.0, sweep_wick) * 0.7)
        q += min(0.5, max(0.0, retest_wick) * 0.7)
        return q

    def _zone_intact_between(self, candles, start_idx: int, end_idx: int, side: str, zone_low: float, zone_high: float) -> bool:
        if end_idx <= start_idx:
            return True
        for c in candles[start_idx:end_idx]:
            h, l, cl = float(c[2]), float(c[3]), float(c[4])
            if side == "LONG" and cl < zone_low and l < zone_low:
                return False
            if side == "SHORT" and cl > zone_high and h > zone_high:
                return False
        return True

    def _retest_rejection_wick(self, candle, side: str) -> float:
        o, h, l, c = map(float, [candle[1], candle[2], candle[3], candle[4]])
        rng = max(1e-12, h - l)
        if side == "LONG":
            return (min(o, c) - l) / rng
        return (h - max(o, c)) / rng

    def _clean_path_to_target(self, *, side: str, close: float, target: float, highs: list[float], lows: list[float]) -> bool:
        if target <= 0:
            return False
        if side == "LONG":
            if target <= close:
                return False
            blockers = [h for h in highs[-8:-1] if close < h < target]
            return len(blockers) <= 2
        if target >= close:
            return False
        blockers = [l for l in lows[-8:-1] if target < l < close]
        return len(blockers) <= 2


    def _quick_bounce(self, symbol, close, candles, vol_ratio, atrp, spread_pct, depth_usdt, imbalance, candles_1h=None, ticker=None, market_context=None):
        """Fast anomaly bounce mode, v0227.

        1h candles detect the abnormal 4-hour move; 15m candles confirm the
        first bounce/pullback. BTC, 24h volume and spread filters protect the
        mode from broad market dumps and illiquid symbols.
        """
        out = []
        self.last_quick_bounce_reject_reason = "not enough candles"
        if len(candles) < 12:
            return out
        candles_1h = candles_1h or candles
        if len(candles_1h) < 5:
            return out
        if spread_pct > self.quick_bounce_max_spread_pct:
            self.last_quick_bounce_reject_reason = f"spread high {spread_pct:.3f}% > {self.quick_bounce_max_spread_pct:.3f}%"
            return out

        quote_vol = 0.0
        if isinstance(ticker, dict):
            for key in ("quoteVolume", "quote_volume", "amount24", "volume24"):
                try:
                    quote_vol = float(ticker.get(key) or 0)
                    if quote_vol > 0:
                        break
                except Exception:
                    pass
        if quote_vol and quote_vol < self.quick_bounce_min_24h_volume_usdt:
            self.last_quick_bounce_reject_reason = f"24h volume low {quote_vol:.0f} < {self.quick_bounce_min_24h_volume_usdt:.0f}"
            return out

        btc_change_1h = None
        if isinstance(market_context, dict):
            try:
                btc_change_1h = float(market_context.get("btc_change_1h_pct"))
            except Exception:
                btc_change_1h = None
        btc_allows_long = True
        btc_allows_short = True
        if self.quick_bounce_btc_filter_enabled and btc_change_1h is not None:
            btc_allows_long = btc_change_1h > -abs(self.quick_bounce_btc_max_drop_1h_pct)
            btc_allows_short = btc_change_1h < abs(self.quick_bounce_btc_max_pump_1h_pct)

        opens15 = [float(c[1]) for c in candles]
        highs15 = [float(c[2]) for c in candles]
        lows15 = [float(c[3]) for c in candles]
        closes15 = [float(c[4]) for c in candles]
        vols15 = [float(c[5]) for c in candles]

        highs1h = [float(c[2]) for c in candles_1h]
        lows1h = [float(c[3]) for c in candles_1h]
        closes1h = [float(c[4]) for c in candles_1h]
        lookback_1h = min(len(candles_1h), 5)  # 4 completed hours + current hour.
        start_1h = closes1h[-lookback_1h]
        local_low_1h = min(lows1h[-lookback_1h:])
        local_high_1h = max(highs1h[-lookback_1h:])
        move_4h_pct = pct(close, start_1h)

        confirm_15m = min(len(candles), 8)  # last ~2h for first bounce/pullback.
        local_low_15m = min(lows15[-confirm_15m:])
        local_high_15m = max(highs15[-confirm_15m:])
        bounce_from_low = pct(close, local_low_15m) if local_low_15m > 0 else 0.0
        pullback_from_high = pct(local_high_15m, close) if close > 0 else 0.0
        last_green = closes15[-1] > opens15[-1] and closes15[-1] > closes15[-2]
        last_red = closes15[-1] < opens15[-1] and closes15[-1] < closes15[-2]
        recent_vol = mean(vols15[-4:]) if len(vols15) >= 4 else vols15[-1]
        base_vol = mean(vols15[-20:-4]) if len(vols15) >= 24 else mean(vols15[:-4] or vols15)
        local_vol_ratio = (recent_vol / base_vol) if base_vol else vol_ratio
        if local_vol_ratio < self.quick_bounce_min_volume_ratio:
            self.last_quick_bounce_reject_reason = f"volume low {local_vol_ratio:.2f} < {self.quick_bounce_min_volume_ratio:.2f}"

        long_ok = (btc_allows_long and
                   move_4h_pct <= -abs(self.quick_bounce_drop_4h_pct) and
                   bounce_from_low >= self.quick_bounce_reversal_pct and
                   last_green and local_vol_ratio >= self.quick_bounce_min_volume_ratio)
        short_ok = (btc_allows_short and
                    move_4h_pct >= abs(self.quick_bounce_pump_4h_pct) and
                    pullback_from_high >= self.quick_bounce_reversal_pct and
                    last_red and local_vol_ratio >= self.quick_bounce_min_volume_ratio)
        if long_ok:
            score = 70 + min(12, abs(move_4h_pct) * 1.2) + min(8, bounce_from_low * 2.0) + min(6, max(0, local_vol_ratio - 1) * 4) + (3 if imbalance > 0 else 0)
            out.append(self._base(symbol, "LONG", "quick_bounce", close, score, spread_pct, depth_usdt, atrp, {
                "setup": "1h_drop_15m_bounce", "move_4h_pct": round(move_4h_pct, 3), "bounce_from_low_pct": round(bounce_from_low, 3),
                "volume_ratio": round(local_vol_ratio, 3), "quote_volume_24h": round(quote_vol, 2), "btc_change_1h_pct": btc_change_1h,
                "anomaly_tf": "1h", "confirm_tf": "15m", "tp_pct": 2.5, "sl_pct": 1.5,
            }))
        if short_ok:
            score = 70 + min(12, abs(move_4h_pct) * 1.2) + min(8, pullback_from_high * 2.0) + min(6, max(0, local_vol_ratio - 1) * 4) + (3 if imbalance < 0 else 0)
            out.append(self._base(symbol, "SHORT", "quick_bounce", close, score, spread_pct, depth_usdt, atrp, {
                "setup": "1h_pump_15m_pullback", "move_4h_pct": round(move_4h_pct, 3), "pullback_from_high_pct": round(pullback_from_high, 3),
                "volume_ratio": round(local_vol_ratio, 3), "quote_volume_24h": round(quote_vol, 2), "btc_change_1h_pct": btc_change_1h,
                "anomaly_tf": "1h", "confirm_tf": "15m", "tp_pct": 2.5, "sl_pct": 1.5,
            }))
        if not out:
            btc_note = f" btc1h={btc_change_1h:+.2f}%" if btc_change_1h is not None else ""
            self.last_quick_bounce_reject_reason = (
                f"no 1h/15m reversal move4h={move_4h_pct:+.2f}% bounce15={bounce_from_low:.2f}% "
                f"pullback15={pullback_from_high:.2f}% vol={local_vol_ratio:.2f} green={last_green} red={last_red}{btc_note}"
            )
        return out

    def _liquidity_retest(self, symbol, close, candles, vol_ratio, atrp, spread_pct, depth_usdt, imbalance):
        """Video-style SMC Liquidity Retest mode.

        Required sequence:
        1) liquidity sweep/grab of a recent swing high/low;
        2) reclaim/rejection back into range;
        3) displacement candle and BOS/CHOCH confirmation;
        4) order-block or FVG zone construction;
        5) current candle retests that zone and rejects from it;
        6) TP is adaptive 2R/3R/4R, optionally capped by nearest liquidity target.

        This stays manual-only and does not reuse the ultra-scalp momentum exits.
        """
        out = []
        if len(candles) < 50:
            return out
        highs = [float(c[2]) for c in candles]
        lows = [float(c[3]) for c in candles]
        closes = [float(c[4]) for c in candles]
        opens = [float(c[1]) for c in candles]
        vols = [float(c[5]) for c in candles]
        avg_vol = mean(vols[-41:-1]) if len(vols) >= 42 else mean(vols[:-1])
        now = candles[-1]
        now_o, now_h, now_l, now_c, now_v = map(float, [now[1], now[2], now[3], now[4], now[5]])

        tolerance_pct = self.liquidity_retest_zone_tolerance_pct
        min_sweep_wick = self.liquidity_retest_min_sweep_wick
        min_reclaim_pct = self.liquidity_retest_min_reclaim_pct
        min_displacement_pct = self.liquidity_retest_min_displacement_pct
        min_displacement_body = self.liquidity_retest_min_displacement_body
        min_volume = self.liquidity_retest_min_volume_ratio
        max_spread = self.liquidity_retest_max_spread_pct
        min_target_rr = self.liquidity_retest_min_target_rr
        min_retest_wick = self.liquidity_retest_min_retest_rejection_wick
        min_zone_quality = self.liquidity_retest_min_zone_quality
        mode = self._liquidity_mode()
        allow_partial_reclaim = mode in {"normal", "aggressive"}
        soft_structure = mode == "aggressive"
        self.last_liquidity_retest_reject_reason = "no sweep"
        if spread_pct > max_spread:
            self.last_liquidity_retest_reject_reason = f"spread high {spread_pct:.3f}% > {max_spread:.3f}%"
            return out

        swing_highs, swing_lows = self._swing_points(highs, lows, 2, 2)
        tol = close * tolerance_pct / 100.0
        # Current candle must be the retest/rejection candle, not the sweep candle.
        search_from = max(24, len(candles) - 22)
        search_to = len(candles) - 3

        for sweep_idx in range(search_from, search_to + 1):
            o, h, l, cl, v = map(float, [candles[sweep_idx][1], candles[sweep_idx][2], candles[sweep_idx][3], candles[sweep_idx][4], candles[sweep_idx][5]])
            rng = max(1e-12, h - l)
            upper_wick = (h - max(o, cl)) / rng
            lower_wick = (min(o, cl) - l) / rng
            local_vol_ratio = v / avg_vol if avg_vol else vol_ratio
            if local_vol_ratio < min_volume:
                continue
            prior_sw_highs = [(i, x) for i, x in swing_highs if sweep_idx - 35 <= i < sweep_idx]
            prior_sw_lows = [(i, x) for i, x in swing_lows if sweep_idx - 35 <= i < sweep_idx]
            ref_high = prior_sw_highs[-1][1] if prior_sw_highs else max(highs[sweep_idx-20:sweep_idx])
            ref_low = prior_sw_lows[-1][1] if prior_sw_lows else min(lows[sweep_idx-20:sweep_idx])

            # LONG: take sell-side liquidity, reclaim, bullish displacement/BOS, retest demand OB/FVG.
            long_sweep = l < ref_low and lower_wick >= min_sweep_wick
            long_reclaim = cl > ref_low and pct(cl, ref_low) >= min_reclaim_pct
            if allow_partial_reclaim and long_sweep and not long_reclaim:
                long_reclaim = cl >= ref_low - tol and now_c >= ref_low - tol
            if long_sweep:
                self.last_liquidity_retest_reject_reason = "no reclaim"
            if long_sweep and long_reclaim:
                self.last_liquidity_retest_reject_reason = "no BOS/displacement"
                bos_level = max(highs[max(0, sweep_idx-12):sweep_idx+1])
                disp_idx = None
                for j in range(sweep_idx + 1, len(candles) - 1):
                    jo, jh, jl, jc, jv = map(float, [candles[j][1], candles[j][2], candles[j][3], candles[j][4], candles[j][5]])
                    body_ratio = abs(jc - jo) / max(1e-12, jh - jl)
                    disp = pct(jc, cl)
                    strict_bos = jc > bos_level and disp >= min_displacement_pct and body_ratio >= min_displacement_body
                    soft_bos = soft_structure and jc > cl and disp >= (min_displacement_pct * 0.55) and body_ratio >= (min_displacement_body * 0.70)
                    if strict_bos or soft_bos:
                        disp_idx = j
                        break
                if disp_idx is None:
                    continue
                displacement = pct(closes[disp_idx], cl)
                bos_strength = pct(closes[disp_idx], bos_level)
                ob = self._last_opposite_order_block(candles, sweep_idx, disp_idx, "LONG")
                fvg = self._fvg_zone(candles, disp_idx, "LONG")
                zone_src = ob or fvg
                if ob and fvg:
                    # prefer the deeper demand zone if both exist; it matches OB retest logic from the video.
                    zone_src = ob
                if not zone_src:
                    zone_src = {"low": l, "high": min(o, cl, ref_low), "kind": "sweep_wick_zone"}
                zone_low, zone_high = float(zone_src["low"]), float(zone_src["high"])
                if zone_high <= zone_low:
                    zone_low, zone_high = min(l, ref_low), max(min(o, cl), ref_low)
                retest = now_l <= zone_high + tol and now_c >= zone_low and now_c > now_o and now_c >= zone_high - tol
                structure_ok = now_c > zone_low and now_c > ref_low
                liquidity_target = max(highs[disp_idx:len(candles)-1]) if disp_idx < len(candles) - 1 else bos_level
                target_rr = 0.0
                if close > zone_low:
                    risk = close - zone_low
                    target_rr = (liquidity_target - close) / risk if risk > 0 else 0.0
                retest_wick = self._retest_rejection_wick(now, "LONG")
                zone_intact = self._zone_intact_between(candles, disp_idx + 1, len(candles) - 1, "LONG", zone_low, zone_high)
                mtf_score = self._mtf_bias_score(candles, "LONG") if self.liquidity_retest_mtf_enabled else 0.0
                clean_path = self._clean_path_to_target(side="LONG", close=close, target=liquidity_target, highs=highs, lows=lows)
                zone_quality = self._zone_quality(zone_src=zone_src, fvg=fvg, displacement_pct=displacement, volume_ratio=local_vol_ratio, bos_strength=bos_strength, sweep_wick=lower_wick, retest_wick=retest_wick)
                if not retest:
                    self.last_liquidity_retest_reject_reason = "no retest"
                elif target_rr < min_target_rr:
                    self.last_liquidity_retest_reject_reason = f"RR low {target_rr:.2f} < {min_target_rr:.2f}"
                elif retest_wick < min_retest_wick:
                    self.last_liquidity_retest_reject_reason = f"retest wick low {retest_wick:.2f} < {min_retest_wick:.2f}"
                elif zone_quality < min_zone_quality:
                    self.last_liquidity_retest_reject_reason = f"zone quality low {zone_quality:.2f} < {min_zone_quality:.2f}"
                elif mtf_score < self.liquidity_retest_min_mtf_score:
                    self.last_liquidity_retest_reject_reason = f"MTF weak {mtf_score:.2f} < {self.liquidity_retest_min_mtf_score:.2f}"
                elif self.liquidity_retest_require_clean_path and not clean_path:
                    self.last_liquidity_retest_reject_reason = "clean path absent"
                if (
                    retest and structure_ok and target_rr >= min_target_rr
                    and retest_wick >= min_retest_wick
                    and (zone_intact or soft_structure)
                    and zone_quality >= min_zone_quality
                    and mtf_score >= self.liquidity_retest_min_mtf_score
                    and (clean_path or not self.liquidity_retest_require_clean_path or soft_structure)
                ):
                    rr = self._rr_from_structure(side="LONG", score_bits=1.0, volume_ratio=local_vol_ratio, displacement_pct=displacement, has_fvg=bool(fvg), bos_strength=bos_strength, bias=(0.5 if imbalance > 0 else 0.0), zone_quality=zone_quality, mtf_score=mtf_score, clean_path=clean_path)
                    score = 67 + min(12, max(0, local_vol_ratio - 1) * 6) + min(10, max(0, displacement) * 7) + min(8, max(0, bos_strength) * 8) + (5 if fvg else 0) + (4 if imbalance > 0 else 0) + min(6, zone_quality * 1.5) + min(4, max(0, mtf_score) * 3)
                    out.append(self._base(symbol, "LONG", "liquidity_retest", close, score, spread_pct, depth_usdt, atrp, {
                        "setup": "sell_side_sweep_bos_ob_fvg_retest", "sweep_index": sweep_idx, "displacement_index": disp_idx,
                        "bos_level": bos_level, "choch": True, "zone_low": zone_low, "zone_high": zone_high,
                        "zone_type": zone_src.get("kind", "zone"), "fvg_low": (fvg or {}).get("low"), "fvg_high": (fvg or {}).get("high"),
                        "sweep_wick": lower_wick, "retest_rejection_wick": retest_wick, "vol_ratio": local_vol_ratio, "displacement_pct": displacement,
                        "bos_strength_pct": bos_strength, "zone_quality": zone_quality, "zone_intact": zone_intact, "mtf_score": mtf_score, "clean_path": clean_path,
                        "liquidity_target": liquidity_target, "target_rr": target_rr,
                        "adaptive_rr": rr, "rr_reason": "sell-side sweep + BOS/CHOCH + demand OB/FVG retest + MTF/context confirmation"
                    }))

            # SHORT: take buy-side liquidity, reject, bearish displacement/BOS, retest supply OB/FVG.
            short_sweep = h > ref_high and upper_wick >= min_sweep_wick
            short_reclaim = cl < ref_high and pct(ref_high, cl) >= min_reclaim_pct
            if allow_partial_reclaim and short_sweep and not short_reclaim:
                short_reclaim = cl <= ref_high + tol and now_c <= ref_high + tol
            if short_sweep:
                self.last_liquidity_retest_reject_reason = "no reclaim"
            if short_sweep and short_reclaim:
                self.last_liquidity_retest_reject_reason = "no BOS/displacement"
                bos_level = min(lows[max(0, sweep_idx-12):sweep_idx+1])
                disp_idx = None
                for j in range(sweep_idx + 1, len(candles) - 1):
                    jo, jh, jl, jc, jv = map(float, [candles[j][1], candles[j][2], candles[j][3], candles[j][4], candles[j][5]])
                    body_ratio = abs(jc - jo) / max(1e-12, jh - jl)
                    disp = pct(cl, jc)
                    strict_bos = jc < bos_level and disp >= min_displacement_pct and body_ratio >= min_displacement_body
                    soft_bos = soft_structure and jc < cl and disp >= (min_displacement_pct * 0.55) and body_ratio >= (min_displacement_body * 0.70)
                    if strict_bos or soft_bos:
                        disp_idx = j
                        break
                if disp_idx is None:
                    continue
                displacement = pct(cl, closes[disp_idx])
                bos_strength = pct(bos_level, closes[disp_idx])
                ob = self._last_opposite_order_block(candles, sweep_idx, disp_idx, "SHORT")
                fvg = self._fvg_zone(candles, disp_idx, "SHORT")
                zone_src = ob or fvg
                if ob and fvg:
                    zone_src = ob
                if not zone_src:
                    zone_src = {"low": max(o, cl, ref_high), "high": h, "kind": "sweep_wick_zone"}
                zone_low, zone_high = float(zone_src["low"]), float(zone_src["high"])
                if zone_high <= zone_low:
                    zone_low, zone_high = min(max(o, cl), ref_high), max(h, ref_high)
                retest = now_h >= zone_low - tol and now_c <= zone_high and now_c < now_o and now_c <= zone_low + tol
                structure_ok = now_c < zone_high and now_c < ref_high
                liquidity_target = min(lows[disp_idx:len(candles)-1]) if disp_idx < len(candles) - 1 else bos_level
                target_rr = 0.0
                if zone_high > close:
                    risk = zone_high - close
                    target_rr = (close - liquidity_target) / risk if risk > 0 else 0.0
                retest_wick = self._retest_rejection_wick(now, "SHORT")
                zone_intact = self._zone_intact_between(candles, disp_idx + 1, len(candles) - 1, "SHORT", zone_low, zone_high)
                mtf_score = self._mtf_bias_score(candles, "SHORT") if self.liquidity_retest_mtf_enabled else 0.0
                clean_path = self._clean_path_to_target(side="SHORT", close=close, target=liquidity_target, highs=highs, lows=lows)
                zone_quality = self._zone_quality(zone_src=zone_src, fvg=fvg, displacement_pct=displacement, volume_ratio=local_vol_ratio, bos_strength=bos_strength, sweep_wick=upper_wick, retest_wick=retest_wick)
                if not retest:
                    self.last_liquidity_retest_reject_reason = "no retest"
                elif target_rr < min_target_rr:
                    self.last_liquidity_retest_reject_reason = f"RR low {target_rr:.2f} < {min_target_rr:.2f}"
                elif retest_wick < min_retest_wick:
                    self.last_liquidity_retest_reject_reason = f"retest wick low {retest_wick:.2f} < {min_retest_wick:.2f}"
                elif zone_quality < min_zone_quality:
                    self.last_liquidity_retest_reject_reason = f"zone quality low {zone_quality:.2f} < {min_zone_quality:.2f}"
                elif mtf_score < self.liquidity_retest_min_mtf_score:
                    self.last_liquidity_retest_reject_reason = f"MTF weak {mtf_score:.2f} < {self.liquidity_retest_min_mtf_score:.2f}"
                elif self.liquidity_retest_require_clean_path and not clean_path:
                    self.last_liquidity_retest_reject_reason = "clean path absent"
                if (
                    retest and structure_ok and target_rr >= min_target_rr
                    and retest_wick >= min_retest_wick
                    and (zone_intact or soft_structure)
                    and zone_quality >= min_zone_quality
                    and mtf_score >= self.liquidity_retest_min_mtf_score
                    and (clean_path or not self.liquidity_retest_require_clean_path or soft_structure)
                ):
                    rr = self._rr_from_structure(side="SHORT", score_bits=1.0, volume_ratio=local_vol_ratio, displacement_pct=displacement, has_fvg=bool(fvg), bos_strength=bos_strength, bias=(0.5 if imbalance < 0 else 0.0), zone_quality=zone_quality, mtf_score=mtf_score, clean_path=clean_path)
                    score = 67 + min(12, max(0, local_vol_ratio - 1) * 6) + min(10, max(0, displacement) * 7) + min(8, max(0, bos_strength) * 8) + (5 if fvg else 0) + (4 if imbalance < 0 else 0) + min(6, zone_quality * 1.5) + min(4, max(0, mtf_score) * 3)
                    out.append(self._base(symbol, "SHORT", "liquidity_retest", close, score, spread_pct, depth_usdt, atrp, {
                        "setup": "buy_side_sweep_bos_ob_fvg_retest", "sweep_index": sweep_idx, "displacement_index": disp_idx,
                        "bos_level": bos_level, "choch": True, "zone_low": zone_low, "zone_high": zone_high,
                        "zone_type": zone_src.get("kind", "zone"), "fvg_low": (fvg or {}).get("low"), "fvg_high": (fvg or {}).get("high"),
                        "sweep_wick": upper_wick, "retest_rejection_wick": retest_wick, "vol_ratio": local_vol_ratio, "displacement_pct": displacement,
                        "bos_strength_pct": bos_strength, "zone_quality": zone_quality, "zone_intact": zone_intact, "mtf_score": mtf_score, "clean_path": clean_path,
                        "liquidity_target": liquidity_target, "target_rr": target_rr,
                        "adaptive_rr": rr, "rr_reason": "buy-side sweep + BOS/CHOCH + supply OB/FVG retest + MTF/context confirmation"
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
