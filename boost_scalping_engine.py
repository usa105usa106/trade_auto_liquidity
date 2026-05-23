
from __future__ import annotations

import time
import json
import asyncio
from typing import Any

from ai_scalping_engine import AIScalpDecision, AIScalpingEngine, _f, _r, _depth_usdt_within
from debug_log import log_event


def _blocked_symbols_from_settings(settings: dict) -> set[str]:
    """Return normalized symbols temporarily blacklisted by BOOST failover.

    Stored format is JSON: {"BTCUSDT": {"until": 123, "reason": "..."}}.
    Expired entries are ignored here; cleanup is handled by main/storage.
    """
    raw = settings.get("boost_blocked_symbols_json") or "{}"
    now = time.time()
    try:
        data = json.loads(str(raw)) if raw else {}
    except Exception:
        return set()
    out = set()
    if not isinstance(data, dict):
        return out
    for sym, meta in data.items():
        try:
            until = float((meta or {}).get("until") or 0) if isinstance(meta, dict) else 0.0
        except Exception:
            until = 0.0
        if until and until > now:
            out.add(_norm_symbol(sym))
    return out


def _norm_symbol(x: str) -> str:
    s = str(x or "").strip().upper().replace("_", "/")
    # Accept easy Telegram input/storage formats: BTC, BTCUSDT, BTC_USDT, BTC/USDT.
    if s and "/" not in s and not s.endswith("USDT"):
        s = s + "/USDT"
    elif s.endswith("USDT") and "/" not in s and len(s) > 4:
        s = s[:-4] + "/USDT"
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
        self._scan_cursor: int = 0
        self._hot_cache: tuple[float, list[str], list[dict]] = (0.0, [], [])

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
        min_vol = _f(settings.get("boost_min_quote_volume_usdt"), 3000000.0)
        if vol <= 0 or vol < min_vol:
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
        min_momo = _f(settings.get("boost_futures_momentum_min_pct"), 0.045)
        max_against = _f(settings.get("boost_futures_max_against_pct"), 0.01)
        # Live edge gate: do not scalp a move that is smaller than spread + likely
        # slippage. 0-fee does not mean 0-cost; bad spread/latency turns paper
        # micro-profits into live losses.
        slip_buf = _f(settings.get("boost_live_slippage_buffer_pct"), 0.035)
        min_edge = max(min_momo, spread * _f(settings.get("boost_spread_edge_mult"), 2.8) + slip_buf)

        side = None
        ratio = 0.0
        momo = 0.0
        if long_ratio >= ratio_min and r1 >= -max_against and r3 >= min_edge:
            side = "LONG"; ratio = long_ratio; momo = max(0.0, r3)
        if short_ratio >= ratio_min and r1 <= max_against and r3 <= -min_edge:
            s_score = short_ratio * 12.0 + abs(r3) * 90.0 + atr * 20.0
            l_score = ratio * 12.0 + momo * 90.0 + atr * 20.0 if side else -1
            if s_score > l_score:
                side = "SHORT"; ratio = short_ratio; momo = abs(r3)
        if side is None:
            return {"ok": False, "reason": f"no live edge: need≈{min_edge:.3f}% spread={spread:.3f}% bid/ask={long_ratio:.2f} ask/bid={short_ratio:.2f} r1={r1:.3f}% r3={r3:.3f}%"}
        strength = max(0.0, min(1.0, ((min(ratio, 4.0) - ratio_min) / max(0.1, 4.0 - ratio_min)) * 0.55 + min(1.0, momo / max(min_momo * 4.0, 0.01)) * 0.45))
        score = ratio * 12.0 + momo * 90.0 + atr * 20.0 - spread * 80.0
        return {"ok": True, "side": side, "score": score, "strength": strength, "ratio": ratio, "momo": momo, "reason": f"{side} ratio={ratio:.2f} r1={r1:.3f}% r3={r3:.3f}% atr={atr:.3f}%"}

    async def _fast_contract_tickers(self, exchange_client) -> list[dict]:
        """Read all MEXC futures tickers in one public request when native client is available."""
        try:
            if getattr(exchange_client, "exchange_id", "") == "mexc" and getattr(exchange_client, "exchange", None) is None and hasattr(exchange_client, "_mexc_public"):
                resp = await asyncio.wait_for(exchange_client._mexc_public("GET", "/api/v1/contract/ticker"), timeout=4.0)
                data = resp.get("data") if isinstance(resp, dict) else resp
                if isinstance(data, list):
                    return [x for x in data if isinstance(x, dict)]
                if isinstance(data, dict):
                    return [data]
        except Exception as e:
            log_event("boost_hotlist_error", stage="all_tickers", ok=False, error=str(e)[:300])
        return []

    def _ticker_symbol_key(self, row: dict) -> str:
        raw = row.get("symbol") or row.get("contract") or row.get("id") or row.get("symbolName") or ""
        return _norm_symbol(str(raw))

    def _ticker_rank_score(self, row: dict) -> dict:
        last = _f(row.get("lastPrice") or row.get("last") or row.get("fairPrice") or row.get("indexPrice"), 0.0)
        high = _f(row.get("high24Price") or row.get("high24") or row.get("high"), 0.0)
        low = _f(row.get("low24Price") or row.get("low24") or row.get("low"), 0.0)
        bid = _f(row.get("bid1") or row.get("bid") or row.get("bidPrice"), last)
        ask = _f(row.get("ask1") or row.get("ask") or row.get("askPrice"), last)
        qv = _f(row.get("amount24") or row.get("volume24") or row.get("quoteVolume") or row.get("turnover24") or row.get("holdVol"), 0.0)
        # MEXC sometimes gives riseFallRate as fraction (0.012 = 1.2%). Keep both cases safe.
        r = _f(row.get("riseFallRate") or row.get("changeRate") or row.get("rate"), 0.0)
        change_pct = r * 100.0 if abs(r) <= 2 else r
        range_pct = ((high - low) / last * 100.0) if last > 0 and high > low else 0.0
        spread_pct = ((ask - bid) / last * 100.0) if last > 0 and ask > 0 and bid > 0 else 0.0
        vol_score = min(25.0, max(0.0, qv) ** 0.25 / 8.0) if qv > 0 else 0.0
        score = abs(change_pct) * 2.5 + range_pct * 3.0 + vol_score - spread_pct * 60.0
        return {"score": _r(score, 4), "change_pct": _r(change_pct, 4), "range_pct": _r(range_pct, 4), "quote_volume_usdt": _r(qv, 2), "spread_pct": _r(spread_pct, 4), "last": last}

    def _hunter_score(self, market: dict, base: dict, settings: dict) -> dict:
        """v0203 HUNTER gate: trade only abnormal momentum, not ordinary chop.

        Returns ok=False for noisy moves even when the old BOOST gate would enter.
        The goal is to reduce live overtrading/slippage losses: wait most of the
        time, attack only when impulse + acceleration + depth imbalance agree.
        """
        if str(settings.get("boost_hunter_mode", True)).lower() not in {"1", "true", "yes", "on"}:
            return base
        if not base.get("ok"):
            return base
        side = str(base.get("side") or "")
        r1 = _f(market.get("ret_1m_pct"), 0.0)
        r3 = _f(market.get("ret_3m_pct"), 0.0)
        atr = _f(market.get("atr_1m_pct"), 0.0)
        spread = _f(market.get("spread_pct"), 999.0)
        ratio = _f(base.get("ratio"), 0.0)
        vol = _f(market.get("quote_volume_usdt"), 0.0)
        min_r3 = _f(settings.get("boost_hunter_min_move_3m_pct"), 0.22)
        min_accel = _f(settings.get("boost_hunter_min_accel_pct"), 0.05)
        min_score = _f(settings.get("boost_hunter_min_score"), 105.0)
        max_wick = _f(settings.get("boost_hunter_max_wick_pct"), 0.42)
        # acceleration: last minute must confirm the 3m move, not fade against it.
        if side == "LONG":
            move = r3
            accel = r1 - (r3 / 3.0)
            same_dir = r1 > 0 and r3 > 0
        else:
            move = abs(r3)
            accel = abs(r1) - (abs(r3) / 3.0)
            same_dir = r1 < 0 and r3 < 0
        if not same_dir:
            return {**base, "ok": False, "reason": f"HUNTER no-trade: no same-dir confirmation r1={r1:.3f}% r3={r3:.3f}%"}
        if move < min_r3:
            return {**base, "ok": False, "reason": f"HUNTER no-trade: move {move:.3f}% < {min_r3:.3f}%"}
        if accel < min_accel:
            return {**base, "ok": False, "reason": f"HUNTER no-trade: accel {accel:.3f}% < {min_accel:.3f}%"}
        # Wick/fade proxy: if 1m contribution is too small vs 3m, impulse may be old and decaying.
        wick_proxy = 1.0 - min(1.0, abs(r1) / max(0.001, abs(r3)))
        if wick_proxy > max_wick:
            return {**base, "ok": False, "reason": f"HUNTER no-trade: momentum decay/wick proxy {wick_proxy:.2f} > {max_wick:.2f}"}
        # Score rewards acceleration and high volume, punishes spread heavily.
        vol_bonus = min(18.0, (max(0.0, vol) ** 0.25) / 6.0) if vol > 0 else 0.0
        hunter_score = _f(base.get("score"), 0.0) + move * 110.0 + max(0.0, accel) * 160.0 + ratio * 8.0 + vol_bonus - spread * 160.0
        if hunter_score < min_score:
            return {**base, "ok": False, "reason": f"HUNTER no-trade: score {hunter_score:.1f} < {min_score:.1f} move={move:.3f}% accel={accel:.3f}%"}
        strength = max(_f(base.get("strength"), 0.0), min(1.0, (hunter_score - min_score) / max(1.0, _f(settings.get("boost_hunter_extreme_score"), 145.0) - min_score)))
        return {**base, "ok": True, "score": hunter_score, "strength": strength, "hunter_score": hunter_score, "accel": accel, "move": move, "reason": f"HUNTER {side} score={hunter_score:.1f} move={move:.3f}% accel={accel:.3f}% ratio={ratio:.2f} atr={atr:.3f}%"}

    async def _hot_symbols(self, exchange_client, settings: dict, symbols: list[str]) -> tuple[list[str], list[dict]]:
        """Every 5 minutes choose the hottest zero-fee symbols, then deep-scan only this hotlist every 1-3s."""
        now = time.time()
        refresh_sec = max(30.0, float(settings.get("boost_hotlist_refresh_sec", 300) or 300))
        hot_n = max(5, int(float(settings.get("boost_hotlist_size", 30) or 30)))
        max_symbols = int(float(settings.get("boost_max_symbols_scan", len(symbols)) or len(symbols)))
        universe = [_norm_symbol(x) for x in symbols[:max_symbols]]
        allowed = set(universe)
        if self._hot_cache[1] and now - self._hot_cache[0] < refresh_sec:
            return [s for s in self._hot_cache[1] if s in allowed][:hot_n], self._hot_cache[2]
        rows = await self._fast_contract_tickers(exchange_client)
        ranked = []
        for row in rows:
            sym = self._ticker_symbol_key(row)
            if sym not in allowed:
                continue
            metr = self._ticker_rank_score(row)
            if metr.get("last", 0) <= 0:
                continue
            ranked.append({"symbol": sym, **metr})
        ranked = sorted(ranked, key=lambda x: _f(x.get("score"), 0.0), reverse=True)
        hot = [r["symbol"] for r in ranked[:hot_n]]
        if not hot:
            # If bulk tickers failed, do not go random-small forever: sweep the full 126 in controlled chunks.
            hot = universe[:hot_n]
            ranked = [{"symbol": x, "score": 0, "reason": "fallback_universe"} for x in hot]
        self._hot_cache = (now, hot, ranked[:hot_n])
        log_event("boost_hotlist_refresh", stage="hotlist", ok=True, universe=len(universe), hot=len(hot), top=ranked[:5])
        return hot, ranked[:hot_n]

    async def decide(self, exchange_client, settings: dict) -> AIScalpDecision:
        symbols = await self.zero_fee_symbols(exchange_client, settings)
        blocked = _blocked_symbols_from_settings(settings)
        if blocked:
            symbols = [s for s in symbols if _norm_symbol(s) not in blocked]
        if not symbols:
            return AIScalpDecision(ok=True, decision="WAIT", confidence=0.0, reason="BOOST: no tradable 0-fee futures symbols after blacklist/filter", model="boost_hunter_autopilot", market={"markets": []})

        hot_symbols, hot_ranked = await self._hot_symbols(exchange_client, settings, symbols)
        total = len(hot_symbols)
        if total <= 0:
            return AIScalpDecision(ok=True, decision="WAIT", confidence=0.0, reason="BOOST: hotlist empty", model="boost_hunter_autopilot", market={"markets": [], "universe_total": len(symbols)})

        # Fast loop: every cycle deep-check the hottest active coins, not a random 15-25.
        min_checks = max(1, int(float(settings.get("boost_min_checked_per_cycle", 12) or 12)))
        max_checks = max(min_checks, int(float(settings.get("boost_max_checked_per_cycle", min(30, total)) or min(30, total))))
        check_count = min(total, max_checks)
        start = self._scan_cursor % max(1, total)
        scan_symbols = (hot_symbols[start:] + hot_symbols[:start])[:check_count]
        self._scan_cursor = (start + check_count) % max(1, total)

        best = None
        checked = []
        log_event("boost_deep_scan_start", stage="deep_scan", ok=True, hot_total=total, check_count=check_count, symbols=scan_symbols[:40])
        async def check_one(sym: str):
            timeout = max(0.3, float(settings.get("boost_symbol_snapshot_timeout_sec", 0.9) or 0.9))
            market = await asyncio.wait_for(self._snapshot(exchange_client, sym), timeout=timeout)
            res = self._side_and_score(market, settings)
            res = self._hunter_score(market, res, settings)
            return sym, market, res

        # Parallel scan is critical for 1-3 second BOOST loops. Limit concurrency to avoid API ban.
        conc = max(1, min(10, int(float(settings.get("boost_scan_concurrency", 6) or 6))))
        sem = asyncio.Semaphore(conc)
        async def guarded(sym: str):
            async with sem:
                try:
                    return await check_one(sym)
                except Exception as e:
                    return sym, None, {"ok": False, "reason": str(e)[:120]}
        results = await asyncio.gather(*(guarded(s) for s in scan_symbols))
        for sym, market, res in results:
            if market:
                checked.append({"symbol": sym, "ok": res.get("ok"), "reason": res.get("reason"), "score": res.get("score", 0), "hunter_score": res.get("hunter_score"), "accel": res.get("accel"), **{k: market.get(k) for k in ("price","spread_pct","atr_1m_pct","ret_1m_pct","ret_3m_pct","quote_volume_usdt")}})
            else:
                checked.append({"symbol": sym, "ok": False, "reason": res.get("reason")})
            if market and res.get("ok") and (best is None or _f(res.get("score"), 0) > _f(best[0].get("score"), 0)):
                best = (res, market)
        ok_candidates = sorted([c for c in checked if c.get("ok")], key=lambda c: _f(c.get("score"), 0.0), reverse=True)[:20]
        log_event("boost_deep_scan_done", stage="deep_scan", ok=True, checked=len(checked), candidates=len(ok_candidates), best=(ok_candidates[0] if ok_candidates else None))
        if not best:
            why = "; ".join(f"{c.get('symbol')}:{c.get('reason')}" for c in checked[:5])
            return AIScalpDecision(ok=True, decision="WAIT", confidence=0.0, reason=("HUNTER no-trade: " + why)[:260], model="boost_hunter_autopilot", market={"markets": checked, "checked": checked, "hotlist": hot_ranked, "universe_total": len(symbols), "loaded": len(symbols), "hot_total": total, "ai_candidates": 0})
        res, market = best
        market["boost_score"] = _r(_f(res.get("score"), 0.0), 4)
        market["boost_strength"] = _r(_f(res.get("strength"), 0.0), 4)
        conf = max(0.72, min(0.96, 0.72 + _f(res.get("strength"), 0.0) * 0.24))
        return AIScalpDecision(ok=True, symbol=market.get("symbol"), decision=res.get("side"), confidence=conf, reason=res.get("reason"), model="boost_hunter_autopilot", market={"markets": [market], "checked": checked, "hotlist": hot_ranked, "universe_total": len(symbols), "loaded": len(symbols), "hot_total": total, "ai_candidates": len(ok_candidates), "top_candidates": ok_candidates}, tp_strength=_f(res.get("strength"), 0.0))

    def _auto_leverage(self, market: dict, strength: float, settings: dict) -> int:
        min_lev = max(1, int(float(settings.get("boost_min_leverage", 10) or 10)))
        max_lev = max(min_lev, int(float(settings.get("boost_max_leverage", 50) or 50)))
        if str(settings.get("boost_auto_leverage", True)).lower() not in {"1", "true", "yes", "on"}:
            return max(1, int(float(settings.get("mexc_order_leverage", min_lev) or min_lev)))
        atr = max(0.01, _f(market.get("atr_1m_pct"), 0.10))
        r3 = abs(_f(market.get("ret_3m_pct"), 0.0))
        # BOOST uses 30x-50x by default. Strength/impulse raises leverage; extreme ATR
        # only prevents jumping to max, it must not silently fall back to 10x.
        impulse_boost = min(1.0, r3 / max(0.05, _f(settings.get("boost_futures_momentum_min_pct"), 0.015) * 8.0))
        blended = max(0.0, min(1.0, strength * 0.70 + impulse_boost * 0.30))
        strength_lev = int(round(min_lev + blended * (max_lev - min_lev)))
        if atr >= 0.45:
            strength_lev = min(strength_lev, max(min_lev, int(max_lev * 0.70)))
        elif atr >= 0.25:
            strength_lev = min(strength_lev, max(min_lev, int(max_lev * 0.85)))
        return max(min_lev, min(max_lev, strength_lev))

    def make_candidate(self, decision: AIScalpDecision, settings: dict) -> dict | None:
        if not decision.ok or decision.decision not in {"LONG", "SHORT"} or not decision.symbol:
            return None
        market = ((decision.market or {}).get("markets") or [{}])[0]
        price = _f(market.get("price"), 0.0)
        if price <= 0:
            return None
        min_tp = max(0.01, _f(settings.get("boost_min_tp_pct"), 0.18))
        max_tp = max(min_tp, _f(settings.get("boost_max_tp_pct"), 0.55))
        strength = max(0.0, min(1.0, _f(decision.tp_strength, 0.0)))
        spread = max(0.0, _f(market.get("spread_pct"), 0.0))
        atr = max(0.01, _f(market.get("atr_1m_pct"), 0.10))
        # TP must cover spread + expected live slippage. Strong impulse waits longer.
        edge_tp = spread * _f(settings.get("boost_tp_spread_mult"), 3.2) + _f(settings.get("boost_live_slippage_buffer_pct"), 0.035)
        atr_tp = atr * _f(settings.get("boost_tp_atr_mult"), 0.70)
        raw_tp = min_tp + (max_tp - min_tp) * strength
        tp = min(max_tp, max(min_tp, edge_tp, atr_tp, raw_tp))
        if str(settings.get("boost_hunter_mode", True)).lower() in {"1", "true", "yes", "on"}:
            # HUNTER lets strong impulses breathe more; exits can still be earlier by momentum decay.
            tp = min(max_tp, max(tp, atr * 0.95, spread * 4.5 + _f(settings.get("boost_live_slippage_buffer_pct"), 0.035)))
        # Emergency stop only. It is wider than TP so the bot does not harvest
        # normal noise as repeated losses. Local/rotation logic still refuses
        # to close minus positions for convenience.
        sl = max(tp * max(1.0, _f(settings.get("boost_sl_tp_multiplier"), 2.40)), atr * 1.60, spread * 5.0)
        leverage = self._auto_leverage(market, strength, settings)
        full_bank = str(settings.get("boost_use_full_bank_per_trade", False)).lower() in {"1", "true", "yes", "on"}
        margin_pct = 1.0 if full_bank else max(0.05, min(0.50, _f(settings.get("boost_trade_margin_pct"), 0.35)))
        risk_pct = max(0.001, min(0.20, _f(settings.get("boost_risk_pct_per_trade"), 0.035)))
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
