
from __future__ import annotations

import time
from typing import Any

from ai_scalping_engine import AIScalpDecision, AIScalpingEngine, _f, _r, _depth_usdt_within


def _norm_symbol(x: str) -> str:
    s = str(x or "").upper().replace("_", "/")
    if ":" not in s and s.endswith("/USDT"):
        s += ":USDT"
    return s


class BoostScalpingEngine:
    """v0174 aggressive boost autopilot scanner.

    It does NOT rely on AI. First it asks MEXC for account-specific 0-fee
    futures symbols, then ranks only active/liquid markets. If 0-fee cannot be
    verified, it returns WAIT by default.
    """

    def __init__(self):
        self._base = AIScalpingEngine()
        self._fee_cache: tuple[float, list[str]] = (0.0, [])

    async def zero_fee_symbols(self, exchange_client, settings: dict) -> list[str]:
        ttl = 600.0
        now = time.time()
        if self._fee_cache[1] and now - self._fee_cache[0] < ttl:
            return self._fee_cache[1]
        max_symbols = int(float(settings.get("boost_max_symbols_scan", 300) or 300))
        allow_fb = str(settings.get("boost_allow_fee_fallback", False)).lower() in {"1", "true", "yes", "on"}
        manual = str(settings.get("boost_zero_fee_symbols") or "")
        try:
            syms = await exchange_client.mexc_verified_zero_fee_symbols(max_symbols=max_symbols, allow_fallback=allow_fb, manual_symbols=manual)
        except Exception:
            syms = []
        self._fee_cache = (now, [_norm_symbol(s) for s in syms][:max_symbols])
        return self._fee_cache[1]

    async def _snapshot(self, exchange_client, symbol: str) -> dict:
        m = await self._base.market_snapshot(exchange_client, symbol, quality_mode=False)
        try:
            t = await exchange_client.fetch_ticker(symbol)
            qv = _f(t.get("quoteVolume") or t.get("quoteVolume24h") or (t.get("info") or {}).get("amount24") or (t.get("info") or {}).get("volume24"), 0.0)
        except Exception:
            qv = 0.0
        m["quote_volume_usdt"] = _r(qv, 2)
        return m

    def _side_and_score(self, market: dict, settings: dict) -> dict:
        price = _f(market.get("price"), 0.0)
        if price <= 0:
            return {"ok": False, "reason": "no price"}
        spread = _f(market.get("spread_pct"), 999.0)
        max_spread = _f(settings.get("boost_max_spread_pct"), 0.08)
        if spread > max_spread:
            return {"ok": False, "reason": f"spread {spread:.3f}% > {max_spread:.3f}%"}
        vol = _f(market.get("quote_volume_usdt"), 0.0)
        min_vol = _f(settings.get("boost_min_quote_volume_usdt"), 5000000.0)
        if vol and vol < min_vol:
            return {"ok": False, "reason": f"volume {vol:.0f} < {min_vol:.0f}"}
        atr = _f(market.get("atr_1m_pct"), 0.0)
        min_atr = _f(settings.get("boost_min_atr_pct"), 0.08)
        if atr < min_atr:
            return {"ok": False, "reason": f"ATR {atr:.3f}% < {min_atr:.3f}%"}

        spot_bid = _f(market.get("spot_bid_depth_usdt"), 0.0)
        spot_ask = _f(market.get("spot_ask_depth_usdt"), 0.0)
        ratio_min = _f(settings.get("boost_spot_imbalance_ratio"), 2.0)
        long_ratio = spot_bid / max(1.0, spot_ask)
        short_ratio = spot_ask / max(1.0, spot_bid)
        r1 = _f(market.get("ret_1m_pct"), 0.0)
        r3 = _f(market.get("ret_3m_pct"), 0.0)
        min_momo = _f(settings.get("boost_futures_momentum_min_pct"), 0.03)
        max_against = _f(settings.get("boost_futures_max_against_pct"), 0.01)

        side = None
        ratio = 0.0
        momo = 0.0
        if long_ratio >= ratio_min and r1 >= -max_against and r3 >= min_momo:
            side = "LONG"; ratio = long_ratio; momo = max(0.0, r3)
        if short_ratio >= ratio_min and r1 <= max_against and r3 <= -min_momo:
            s_score = short_ratio * 12.0 + abs(r3) * 90.0 + atr * 20.0
            l_score = ratio * 12.0 + momo * 90.0 + atr * 20.0 if side else -1
            if s_score > l_score:
                side = "SHORT"; ratio = short_ratio; momo = abs(r3)
        if side is None:
            return {"ok": False, "reason": f"no aligned boost impulse: bid/ask={long_ratio:.2f} ask/bid={short_ratio:.2f} r1={r1:.3f}% r3={r3:.3f}%"}
        strength = max(0.0, min(1.0, ((min(ratio, 4.0) - ratio_min) / max(0.1, 4.0 - ratio_min)) * 0.55 + min(1.0, momo / max(min_momo * 4.0, 0.01)) * 0.45))
        score = ratio * 12.0 + momo * 90.0 + atr * 20.0 - spread * 80.0
        return {"ok": True, "side": side, "score": score, "strength": strength, "ratio": ratio, "momo": momo, "reason": f"{side} ratio={ratio:.2f} r1={r1:.3f}% r3={r3:.3f}% atr={atr:.3f}%"}

    async def decide(self, exchange_client, settings: dict) -> AIScalpDecision:
        symbols = await self.zero_fee_symbols(exchange_client, settings)
        if not symbols:
            return AIScalpDecision(ok=True, decision="WAIT", confidence=0.0, reason="BOOST: no API-verified 0-fee futures symbols", model="boost_zero_fee_scanner", market={"markets": []})
        best = None
        checked = []
        max_scan = int(float(settings.get("boost_max_symbols_scan", 300) or 300))
        for sym in symbols[:max_scan]:
            try:
                market = await self._snapshot(exchange_client, sym)
                res = self._side_and_score(market, settings)
                checked.append({"symbol": sym, "ok": res.get("ok"), "reason": res.get("reason"), "score": res.get("score", 0), **{k: market.get(k) for k in ("price","spread_pct","atr_1m_pct","ret_1m_pct","ret_3m_pct","quote_volume_usdt")}})
                if res.get("ok") and (best is None or _f(res.get("score"), 0) > _f(best[0].get("score"), 0)):
                    best = (res, market)
            except Exception as e:
                checked.append({"symbol": sym, "ok": False, "reason": str(e)[:120]})
        if not best:
            why = "; ".join(f"{c.get('symbol')}:{c.get('reason')}" for c in checked[:5])
            return AIScalpDecision(ok=True, decision="WAIT", confidence=0.0, reason=("BOOST wait: " + why)[:220], model="boost_zero_fee_scanner", market={"markets": checked})
        res, market = best
        market["boost_score"] = _r(_f(res.get("score"), 0.0), 4)
        market["boost_strength"] = _r(_f(res.get("strength"), 0.0), 4)
        conf = max(0.72, min(0.96, 0.72 + _f(res.get("strength"), 0.0) * 0.24))
        return AIScalpDecision(ok=True, symbol=market.get("symbol"), decision=res.get("side"), confidence=conf, reason=res.get("reason"), model="boost_zero_fee_scanner", market={"markets": [market], "checked": checked}, tp_strength=_f(res.get("strength"), 0.0))

    def _auto_leverage(self, market: dict, strength: float, settings: dict) -> int:
        min_lev = max(1, int(float(settings.get("boost_min_leverage", 10) or 10)))
        max_lev = max(min_lev, int(float(settings.get("boost_max_leverage", 50) or 50)))
        if str(settings.get("boost_auto_leverage", True)).lower() not in {"1", "true", "yes", "on"}:
            return max(1, int(float(settings.get("mexc_order_leverage", min_lev) or min_lev)))
        atr = max(0.01, _f(market.get("atr_1m_pct"), 0.10))
        # More strength -> more leverage; more volatility -> lower safe cap.
        strength_lev = int(min_lev + max(0.0, min(1.0, strength)) * (max_lev - min_lev))
        vol_cap = int(max(min_lev, min(max_lev, 4.0 / atr)))  # ATR 0.10%=40x cap, 0.20%=20x, 0.50%=8x
        return max(min_lev, min(max_lev, strength_lev, vol_cap))

    def make_candidate(self, decision: AIScalpDecision, settings: dict) -> dict | None:
        if not decision.ok or decision.decision not in {"LONG", "SHORT"} or not decision.symbol:
            return None
        market = ((decision.market or {}).get("markets") or [{}])[0]
        price = _f(market.get("price"), 0.0)
        if price <= 0:
            return None
        min_tp = max(0.01, _f(settings.get("boost_min_tp_pct"), 0.08))
        max_tp = max(min_tp, _f(settings.get("boost_max_tp_pct"), 0.18))
        strength = max(0.0, min(1.0, _f(decision.tp_strength, 0.0)))
        tp = min_tp + (max_tp - min_tp) * strength
        sl = tp * max(1.0, _f(settings.get("boost_sl_tp_multiplier"), 1.15))
        leverage = self._auto_leverage(market, strength, settings)
        full_bank = str(settings.get("boost_use_full_bank_per_trade", True)).lower() in {"1", "true", "yes", "on"}
        margin_pct = 1.0 if full_bank else _f(settings.get("boost_balance_share"), 0.10)
        risk_pct = max(0.001, min(1.0, _f(settings.get("boost_risk_pct_per_trade"), 0.12)))
        clean_market = {k: v for k, v in market.items() if not str(k).startswith("_")}
        return {
            "symbol": decision.symbol,
            "side": decision.decision,
            "strategy": "boost_scalping",
            "confidence": decision.confidence,
            "futures_price": price,
            "atr_pct": max(0.01, _f(market.get("atr_1m_pct"), 0.10)),
            "ai_scalping_tp_pct": _r(tp, 4),
            "ai_scalping_sl_pct": _r(sl, 4),
            "risk_pct": risk_pct,
            "trade_margin_pct": margin_pct,
            "leverage": leverage,
            "max_open_positions": 1,
            "score_details": {"boost_reason": decision.reason, "boost_model": decision.model, "boost_score": _r(_f(market.get("boost_score"), 0.0), 4), "boost_strength": _r(_f(market.get("boost_strength"), strength), 4), **clean_market},
        }
