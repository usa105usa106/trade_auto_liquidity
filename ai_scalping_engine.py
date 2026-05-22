from __future__ import annotations

import asyncio
import json
import math
import os
import re
import time
from dataclasses import dataclass
from typing import Any

import aiohttp

from models import TradePlan

# BTC_ETH_AI_SCALP_DIRECTION | Return one strict JSON object only | bot TP before bot SL | Do not force trades | max_output_tokens
from openai_signal_engine import OPENAI_RESPONSES_URL, OPENAI_CHAT_URL, active_model, openai_key


@dataclass
class AIScalpDecision:
    ok: bool
    symbol: str = ""
    decision: str = "WAIT"  # LONG | SHORT | WAIT
    confidence: float = 0.0
    reason: str = ""
    error: str = ""
    raw: str = ""
    model: str = ""
    market: dict | None = None
    cached: bool = False
    confidence_inferred: bool = False
    tp_strength: float = 0.0  # 0..1, AI/local strength used to select TP inside BTC/ETH range



def _b(settings: dict, key: str, default: bool = False) -> bool:
    v = (settings or {}).get(key, default)
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in {"1", "true", "yes", "on"}

def _f(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _r(v: Any, n: int = 6) -> float:
    try:
        return round(float(v), n)
    except Exception:
        return 0.0


def _clean_reason(text: Any, limit: int = 160) -> str:
    """Compact human-readable AI/local reason for Telegram logs.

    This is local formatting only; it never calls OpenAI and never spends tokens.
    It also repairs common malformed/truncated model outputs such as:
    BTC:WAIT "confidence":0.92,"reason":"mixed momentum".
    """
    raw = str(text or "").strip()
    if not raw:
        return ""
    m = re.search(r'"reason"\s*:\s*"([^"{}]{0,220})', raw, flags=re.I)
    if m:
        raw = m.group(1)
    raw = re.sub(r'^[,\s:"{}\[\]]+', '', raw)
    raw = re.sub(r'"?\s*,?\s*"?(?:confidence|symbol|decision)"?\s*[:=].*$', '', raw, flags=re.I)
    raw = raw.replace('\\n', ' ').replace('\n', ' ')
    raw = re.sub(r'\s+', ' ', raw).strip(' ;,.-–—')
    return raw[:limit]



def _ema(values: list[float], period: int) -> float:
    vals = [float(x) for x in values if x is not None]
    if not vals:
        return 0.0
    k = 2.0 / (period + 1.0)
    ema = vals[0]
    for x in vals[1:]:
        ema = x * k + ema * (1.0 - k)
    return ema


def _rsi(closes: list[float], period: int = 14) -> float:
    if len(closes) <= period:
        return 50.0
    gains, losses = [], []
    for a, b in zip(closes[-period-1:-1], closes[-period:]):
        d = b - a
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss <= 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _atr_pct(candles: list[list[Any]], period: int = 14) -> float:
    if len(candles) < 2:
        return 0.2
    trs = []
    prev_close = _f(candles[0][4])
    for c in candles[1:]:
        high, low, close = _f(c[2]), _f(c[3]), _f(c[4])
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
        prev_close = close
    last_close = _f(candles[-1][4])
    atr = sum(trs[-period:]) / max(1, min(period, len(trs)))
    return (atr / last_close * 100.0) if last_close > 0 else 0.2


def _local_scalp_setup(candles: list[list[Any]], side: str, price: float, atr_pct: float) -> dict:
    """Detect a tiny BTC/ETH scalp setup before asking AI to trade.

    This is intentionally deterministic and cheap: the AI may confirm direction,
    but the bot must first see liquidity interaction near the edge of the recent
    range. It rejects mid-range direction guesses.
    """
    side = str(side or "").upper()
    if len(candles) < 24 or price <= 0:
        return {"valid": False, "reason": "setup: not enough candles", "score": 0.0}
    prev = candles[-24:-4]
    recent = candles[-4:]
    prev_high = max(_f(c[2]) for c in prev)
    prev_low = min(_f(c[3]) for c in prev)
    range_pct = ((prev_high - prev_low) / price * 100.0) if price > 0 else 0.0
    if range_pct <= 0:
        return {"valid": False, "reason": "setup: invalid range", "score": 0.0}
    pos = (price - prev_low) / max(1e-12, prev_high - prev_low)
    min_range = max(0.06, min(0.22, atr_pct * 1.35))
    if range_pct < min_range:
        return {"valid": False, "reason": f"setup: range {range_pct:.3f}% < {min_range:.3f}%", "score": 0.0, "range_pct": range_pct, "range_pos": pos}

    last_close = _f(candles[-1][4])
    last_high = max(_f(c[2]) for c in recent)
    last_low = min(_f(c[3]) for c in recent)
    ret3 = ((_f(candles[-1][4]) - _f(candles[-4][4])) / _f(candles[-4][4]) * 100.0) if _f(candles[-4][4]) else 0.0
    vol_prev = sum(_f(c[5]) for c in candles[-24:-4]) / max(1, len(candles[-24:-4]))
    vol_recent = sum(_f(c[5]) for c in recent) / max(1, len(recent))
    vol_ratio = vol_recent / max(1e-12, vol_prev) if vol_prev > 0 else 1.0
    buffer = max(price * 0.00008, price * (atr_pct / 100.0) * 0.12)

    if side == "LONG":
        swept = last_low < (prev_low - buffer)
        reclaimed = last_close > (prev_low + buffer * 0.25)
        near_edge = pos <= 0.40
        impulse_ok = ret3 >= -max(0.04, atr_pct * 0.7)
        swing_stop = min(last_low, prev_low)
        target = prev_high
        stop_pct = ((price - swing_stop) / price * 100.0) if swing_stop > 0 else 0.0
        target_pct = ((target - price) / price * 100.0) if target > price else 0.0
    elif side == "SHORT":
        swept = last_high > (prev_high + buffer)
        reclaimed = last_close < (prev_high - buffer * 0.25)
        near_edge = pos >= 0.60
        impulse_ok = ret3 <= max(0.04, atr_pct * 0.7)
        swing_stop = max(last_high, prev_high)
        target = prev_low
        stop_pct = ((swing_stop - price) / price * 100.0) if swing_stop > 0 else 0.0
        target_pct = ((price - target) / price * 100.0) if target < price else 0.0
    else:
        return {"valid": False, "reason": "setup: no side", "score": 0.0}

    rr = target_pct / max(0.01, stop_pct) if target_pct > 0 else 0.0
    # v0145 aggressive BTC/ETH micro-scalp gate: sweep/reclaim is a bonus,
    # not mandatory. Small 0% fee scalps can enter on fresh momentum + acceptable
    # room because TP is only 0.08-0.16% and SL is fixed at TP*2.
    momentum_abs = abs(ret3)
    min_momentum = max(0.025, atr_pct * 0.28)
    momentum_ok = momentum_abs >= min_momentum
    volume_ok = vol_ratio >= 0.92
    rr_min = 0.45
    context_ok = bool(swept or reclaimed or near_edge or momentum_ok)
    score = 0.0
    score += 22.0 if swept else 0.0
    score += 18.0 if reclaimed else 0.0
    score += 12.0 if near_edge else 0.0
    score += 14.0 if impulse_ok else 0.0
    score += 16.0 if momentum_ok else 0.0
    score += min(10.0, max(0.0, vol_ratio - 0.85) * 12.0)
    score += min(8.0, max(0.0, rr - rr_min) * 10.0)
    valid = bool(context_ok and impulse_ok and volume_ok and rr >= rr_min)
    parts = []
    if not context_ok: parts.append("no context")
    if not swept: parts.append("no sweep")
    if not reclaimed: parts.append("no reclaim")
    if not near_edge: parts.append("mid ok")
    if not momentum_ok: parts.append("weak momentum")
    if not impulse_ok: parts.append("bad impulse")
    if not volume_ok: parts.append(f"vol {vol_ratio:.2f} < 0.92")
    if rr < rr_min: parts.append(f"RR {rr:.2f} < {rr_min:.2f}")
    return {
        "valid": valid,
        "reason": "setup ok" if valid else "setup: " + ", ".join(parts),
        "score": round(score, 2),
        "range_high": _r(prev_high, 4),
        "range_low": _r(prev_low, 4),
        "range_pct": _r(range_pct, 4),
        "range_pos": _r(pos, 4),
        "swept": swept,
        "reclaimed": reclaimed,
        "vol_ratio": _r(vol_ratio, 3),
        "swing_stop": _r(swing_stop, 4),
        "target": _r(target, 4),
        "structure_sl_pct": _r(stop_pct, 4),
        "structure_tp_pct": _r(target_pct, 4),
        "structure_rr": _r(rr, 3),
    }


def _quality_score(market: dict, setup: dict, side: str) -> dict:
    side = str(side or "").upper()
    score = 0.0
    reasons = []
    atr = _f(market.get("atr_1m_pct"), 0.0)
    spread = _f(market.get("spread_pct"), 0.0)
    ret5 = _f(market.get("ret_5m_pct"), 0.0)
    imb = _f(market.get("imbalance"), 0.0)
    e1 = market.get("ema9_gt_ema21_1m")
    e5 = market.get("ema9_gt_ema21_5m")
    e15 = market.get("ema9_gt_ema21_15m")
    score += min(20.0, max(0.0, atr) * 220.0)
    score += max(0.0, 15.0 - spread * 180.0)
    score += min(20.0, max(0.0, _f(setup.get("score"), 0.0)) * 0.25)
    score += min(15.0, abs(ret5) * 130.0)
    if (side == "LONG" and e1 is True) or (side == "SHORT" and e1 is False):
        score += 8.0
    else:
        reasons.append("1m bias weak")
    if (side == "LONG" and e5 is True) or (side == "SHORT" and e5 is False):
        score += 10.0
    else:
        reasons.append("5m bias weak")
    if e15 is not None:
        if (side == "LONG" and e15 is True) or (side == "SHORT" and e15 is False):
            score += 7.0
        else:
            reasons.append("15m conflict")
    if (side == "LONG" and imb > -0.15) or (side == "SHORT" and imb < 0.15):
        score += 5.0
    else:
        reasons.append("orderbook against")
    return {"score": round(max(0.0, min(100.0, score)), 2), "notes": "; ".join(reasons[:3])}




def _depth_usdt_within(ob: dict, price: float, side: str, window_pct: float = 0.15, fallback_levels: int = 10) -> float:
    rows = ob.get("bids") if side == "bid" else ob.get("asks")
    rows = rows or []
    if price <= 0:
        return sum(_f(p) * _f(q) for p, q, *_ in rows[:fallback_levels])
    if side == "bid":
        lo, hi = price * (1.0 - window_pct / 100.0), price
    else:
        lo, hi = price, price * (1.0 + window_pct / 100.0)
    total = 0.0
    for p, q, *_ in rows:
        pp, qq = _f(p), _f(q)
        if lo <= pp <= hi:
            total += pp * qq
    if total <= 0:
        total = sum(_f(p) * _f(q) for p, q, *_ in rows[:fallback_levels])
    return total


def _spot_orderbook_bias(market: dict, settings: dict) -> dict:
    """Direction from SPOT liquidity only. No AI, no sweep logic."""
    bid_depth = _f(market.get("spot_bid_depth_usdt"), 0.0)
    ask_depth = _f(market.get("spot_ask_depth_usdt"), 0.0)
    ratio_min = _f(settings.get("ai_scalping_spot_imbalance_ratio"), _f(os.getenv("AI_SCALPING_SPOT_IMBALANCE_RATIO"), 1.8))
    ratio_min = max(1.05, ratio_min)
    if bid_depth <= 0 or ask_depth <= 0:
        return {"side": "WAIT", "ratio": 0.0, "reason": "spot depth missing"}
    bid_ask = bid_depth / max(1e-12, ask_depth)
    ask_bid = ask_depth / max(1e-12, bid_depth)
    if bid_ask >= ratio_min:
        return {"side": "LONG", "ratio": _r(bid_ask, 3), "reason": f"spot bid/ask {bid_ask:.2f} >= {ratio_min:.2f}"}
    if ask_bid >= ratio_min:
        return {"side": "SHORT", "ratio": _r(ask_bid, 3), "reason": f"spot ask/bid {ask_bid:.2f} >= {ratio_min:.2f}"}
    return {"side": "WAIT", "ratio": _r(max(bid_ask, ask_bid), 3), "reason": f"spot imbalance {max(bid_ask, ask_bid):.2f} < {ratio_min:.2f}"}


def _futures_micro_momentum(market: dict, side: str, settings: dict) -> dict:
    """Tiny futures confirmation after spot orderbook chose direction.

    This is intentionally simple and fast: it only checks that futures price is
    not moving against the spot bias and preferably has a tiny push in the same
    direction.
    """
    side = str(side or "").upper()
    r1 = _f(market.get("ret_1m_pct"), 0.0)
    r3 = _f(market.get("ret_3m_pct"), 0.0)
    atr = _f(market.get("atr_1m_pct"), 0.0)
    min_move = _f(settings.get("ai_scalping_futures_momentum_min_pct"), _f(os.getenv("AI_SCALPING_FUTURES_MOMENTUM_MIN_PCT"), 0.015))
    max_against = _f(settings.get("ai_scalping_futures_max_against_pct"), _f(os.getenv("AI_SCALPING_FUTURES_MAX_AGAINST_PCT"), 0.035))
    min_move = max(0.0, min_move)
    max_against = max(0.0, max_against)
    if side == "LONG":
        hard_against = r1 < -max_against and r3 < -max_against
        ok = (not hard_against) and (r1 >= min_move or r3 >= min_move or (r1 >= 0 and r3 >= 0))
        same_move = max(r1, r3)
    elif side == "SHORT":
        hard_against = r1 > max_against and r3 > max_against
        ok = (not hard_against) and (r1 <= -min_move or r3 <= -min_move or (r1 <= 0 and r3 <= 0))
        same_move = max(-r1, -r3)
    else:
        return {"ok": False, "strength": 0.0, "reason": "no side"}
    strength = 0.0
    if ok:
        strength = min(1.0, max(0.15, (same_move + min_move) / max(0.04, atr * 0.7, min_move * 4)))
    reason = f"futures r1={r1:.3f}% r3={r3:.3f}%" + (" ok" if ok else " against/weak")
    return {"ok": bool(ok), "strength": _r(strength, 4), "reason": reason}

def _compact_ai_setup_features(market: dict, long_setup: dict, short_setup: dict) -> dict:
    """Build a tiny ready-feature payload for OpenAI.

    The model receives facts only, not candles. Local code already calculated
    liquidity sweep/reclaim/range/ATR/RR, so AI spends tokens on validation
    instead of rediscovering the setup from raw OHLCV.
    """
    def pack(side: str, setup: dict) -> dict:
        return {
            "side": side,
            "ok": 1 if setup.get("valid") else 0,
            "score": _r(setup.get("score"), 1),
            "reason": str(setup.get("reason") or "")[:48],
            "sweep": 1 if setup.get("swept") else 0,
            "reclaim": 1 if setup.get("reclaimed") else 0,
            "edge": "low" if side == "LONG" else "high",
            "rp": _r(setup.get("range_pos"), 3),
            "rr": _r(setup.get("structure_rr"), 2),
            "sl": _r(setup.get("structure_sl_pct"), 3),
            "tp": _r(setup.get("structure_tp_pct"), 3),
            "vol": _r(setup.get("vol_ratio"), 2),
        }

    candidates = []
    if long_setup.get("valid"):
        candidates.append(pack("LONG", long_setup))
    if short_setup.get("valid"):
        candidates.append(pack("SHORT", short_setup))

    def align(side: str) -> int:
        e1 = market.get("ema9_gt_ema21_1m")
        e5 = market.get("ema9_gt_ema21_5m")
        e15 = market.get("ema9_gt_ema21_15m")
        if side == "LONG":
            vals = [e1 is True, e5 is True]
            if e15 is not None:
                vals.append(e15 is True)
        else:
            vals = [e1 is False, e5 is False]
            if e15 is not None:
                vals.append(e15 is False)
        return int(sum(1 for x in vals if x))

    for c in candidates:
        q = _quality_score(market, long_setup if c["side"] == "LONG" else short_setup, c["side"])
        c["q"] = _r(q.get("score"), 1)
        c["mtf"] = align(c["side"])
        if q.get("notes"):
            c["warn"] = str(q.get("notes"))[:42]

    return {
        "p": market.get("price", 0),
        "atr": market.get("atr_1m_pct", 0),
        "sp": market.get("spread_pct", 0),
        "r1": market.get("ret_1m_pct", 0),
        "r5": market.get("ret_5m_pct", 0),
        "rsi": market.get("rsi_1m", 50),
        "imb": market.get("imbalance", 0),
        "e1": 1 if market.get("ema9_gt_ema21_1m") else 0,
        "e5": 1 if market.get("ema9_gt_ema21_5m") else 0,
        "e15": (1 if market.get("ema9_gt_ema21_15m") else 0) if market.get("ema9_gt_ema21_15m") is not None else None,
        "cand": candidates,
    }

def _extract_text(data: dict) -> str:
    chunks = []
    def walk(obj):
        if isinstance(obj, str):
            chunks.append(obj)
        elif isinstance(obj, dict):
            if obj.get("type") in {"output_text", "text"} and isinstance(obj.get("text"), str):
                chunks.append(obj["text"])
            if isinstance(obj.get("content"), str):
                chunks.append(obj["content"])
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    walk(v)
        elif isinstance(obj, list):
            for v in obj:
                walk(v)
    walk(data.get("output", []))
    if data.get("choices"):
        try:
            walk(data["choices"][0].get("message", {}).get("content", ""))
        except Exception:
            pass
    return "\n".join(x.strip() for x in chunks if x and x.strip()).strip()


def parse_ai_scalp_decision(text: str, allowed_symbols: list[str], model: str) -> AIScalpDecision:
    raw = (text or "").strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.I).strip()
    cleaned = re.sub(r"\s*```$", "", cleaned).strip()
    data = None
    try:
        data = json.loads(cleaned)
    except Exception:
        m = re.search(r"\{.*\}", cleaned, flags=re.S)
        if m:
            try:
                data = json.loads(m.group(0))
            except Exception:
                data = None
    if not isinstance(data, dict):
        # v0148: strict AI JSON. Never infer LONG/SHORT from plain text and
        # never inject fallback confidence such as 0.72. A malformed response is
        # a WAIT/reject so the bot only enters on deterministic JSON.
        return AIScalpDecision(ok=False, symbol=allowed_symbols[0] if allowed_symbols else "", decision="WAIT", confidence=0.0, error="AI did not return strict JSON", reason="malformed AI JSON", raw=raw[:1000], model=model)
    symbol = str(data.get("symbol") or "").upper().replace("_", "").replace("/", "").replace(":USDT", "")
    norm_allowed = {s.upper().replace("_", "").replace("/", "").replace(":USDT", ""): s for s in allowed_symbols}
    if symbol not in norm_allowed:
        return AIScalpDecision(ok=True, symbol="", decision="WAIT", confidence=0.0, reason="AI symbol not allowed", raw=raw[:1000], model=model)
    decision = str(data.get("decision") or data.get("side") or "WAIT").upper().strip()
    if decision not in {"LONG", "SHORT", "WAIT"}:
        decision = "WAIT"
    inferred = False
    reason = _clean_reason(data.get("reason") or "", 180)
    if data.get("confidence") is None:
        if decision in {"LONG", "SHORT"}:
            return AIScalpDecision(ok=True, symbol=norm_allowed.get(symbol, ""), decision="WAIT", confidence=0.0, reason=("AI JSON missing confidence; " + reason).strip(" ;")[:220], raw=raw[:1000], model=model, confidence_inferred=False, tp_strength=0.0)
        conf = 0.0
    else:
        conf = _f(data.get("confidence"), 0.0)
        if conf > 1:
            conf /= 100.0
    if data.get("tp_strength") is None and decision in {"LONG", "SHORT"}:
        return AIScalpDecision(ok=True, symbol=norm_allowed.get(symbol, ""), decision="WAIT", confidence=0.0, reason=("AI JSON missing tp_strength; " + reason).strip(" ;")[:220], raw=raw[:1000], model=model, confidence_inferred=False, tp_strength=0.0)
    tp_strength = _f(data.get("tp_strength"), conf)
    if tp_strength > 1:
        tp_strength /= 100.0
    return AIScalpDecision(
        ok=True,
        symbol=norm_allowed[symbol],
        decision=decision,
        confidence=max(0.0, min(1.0, conf)),
        reason=reason,
        raw=raw[:1000],
        model=model,
        confidence_inferred=inferred,
        tp_strength=max(0.0, min(1.0, tp_strength)),
    )




def ai_scalp_json_schema(symbol: str) -> dict:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "symbol": {"type": "string", "enum": [symbol]},
            "decision": {"type": "string", "enum": ["LONG", "SHORT", "WAIT"]},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "tp_strength": {"type": "number", "minimum": 0, "maximum": 1},
            "reason": {"type": "string", "maxLength": 80},
        },
        "required": ["symbol", "decision", "confidence", "tp_strength", "reason"],
    }


def _json_format_payload(symbol: str) -> dict:
    return {
        "type": "json_schema",
        "name": "ai_scalp_decision",
        "strict": True,
        "schema": ai_scalp_json_schema(symbol),
    }


def _json_object_payload() -> dict:
    return {"type": "json_object"}

class AIScalpingEngine:
    """Low-token BTC/ETH direction selector. Bot keeps sizing, TP/SL and execution."""

    def __init__(self):
        self._sem = asyncio.Semaphore(1)
        self._decision_cache: dict[str, tuple[float, AIScalpDecision]] = {}

    @staticmethod
    def symbols(settings: dict) -> list[str]:
        raw = str(settings.get("ai_scalping_symbols") or os.getenv("AI_SCALPING_SYMBOLS", "BTC_USDT,ETH_USDT"))
        out = []
        for x in raw.split(","):
            s = x.strip()
            if not s:
                continue
            u = s.upper().replace("_", "/")
            if "/" not in u:
                u = u.replace("USDT", "/USDT")
            if ":" not in u:
                u = u + ":USDT"
            if u.startswith(("BTC/USDT", "ETH/USDT")) and u not in out:
                out.append(u)
        return out or ["BTC/USDT:USDT", "ETH/USDT:USDT"]

    async def market_snapshot(self, exchange_client, symbol: str, quality_mode: bool = False) -> dict:
        c1 = await exchange_client.fetch_ohlcv(symbol, timeframe="1m", limit=60)
        c5 = await exchange_client.fetch_ohlcv(symbol, timeframe="5m", limit=60)
        c15 = await exchange_client.fetch_ohlcv(symbol, timeframe="15m", limit=50) if quality_mode else []
        ticker = await exchange_client.fetch_ticker(symbol)
        ob = await exchange_client.fetch_order_book(symbol, limit=10)
        try:
            spot_ob = await exchange_client.fetch_spot_order_book(symbol, limit=50)
        except Exception as e:
            spot_ob = {"bids": [], "asks": [], "error": str(e)[:120]}
        closes1 = [_f(c[4]) for c in c1]
        closes5 = [_f(c[4]) for c in c5]
        closes15 = [_f(c[4]) for c in c15]
        price = _f(ticker.get("last") or ticker.get("close") or (closes1[-1] if closes1 else 0))
        bid = _f(ticker.get("bid"), price)
        ask = _f(ticker.get("ask"), price)
        spread_pct = ((ask - bid) / price * 100.0) if price > 0 and ask > bid else 0.0
        bid_depth = sum(_f(p) * _f(q) for p, q, *_ in (ob.get("bids") or [])[:5])
        ask_depth = sum(_f(p) * _f(q) for p, q, *_ in (ob.get("asks") or [])[:5])
        spot_window = _f(os.getenv("AI_SCALPING_SPOT_DEPTH_WINDOW_PCT"), 0.15)
        spot_mid = price
        if (spot_ob.get("bids") or []) and (spot_ob.get("asks") or []):
            spot_mid = (_f(spot_ob["bids"][0][0]) + _f(spot_ob["asks"][0][0])) / 2.0
        spot_bid_depth = _depth_usdt_within(spot_ob, spot_mid, "bid", spot_window, 10)
        spot_ask_depth = _depth_usdt_within(spot_ob, spot_mid, "ask", spot_window, 10)
        ret_1m = ((closes1[-1] - closes1[-2]) / closes1[-2] * 100.0) if len(closes1) > 2 and closes1[-2] else 0.0
        ret_3m = ((closes1[-1] - closes1[-4]) / closes1[-4] * 100.0) if len(closes1) > 4 and closes1[-4] else 0.0
        ret_5m = ((closes1[-1] - closes1[-6]) / closes1[-6] * 100.0) if len(closes1) > 6 and closes1[-6] else 0.0
        ema9 = _ema(closes1[-40:], 9)
        ema21 = _ema(closes1[-50:], 21)
        ema5_9 = _ema(closes5[-40:], 9)
        ema5_21 = _ema(closes5[-50:], 21)
        ema15_9 = _ema(closes15[-40:], 9) if closes15 else 0.0
        ema15_21 = _ema(closes15[-50:], 21) if closes15 else 0.0
        ema_gap_5m_pct = (abs(ema5_9 - ema5_21) / price * 100.0) if price > 0 else 0.0
        return {
            "symbol": symbol,
            "price": _r(price, 4),
            "_candles1": c1,
            "ret_1m_pct": _r(ret_1m, 4),
            "ret_3m_pct": _r(ret_3m, 4),
            "ret_5m_pct": _r(ret_5m, 4),
            "rsi_1m": _r(_rsi(closes1, 14), 2),
            "ema9_1m": _r(ema9, 4),
            "ema21_1m": _r(ema21, 4),
            "ema9_gt_ema21_1m": ema9 > ema21,
            "ema9_gt_ema21_5m": ema5_9 > ema5_21,
            "ema9_gt_ema21_15m": (ema15_9 > ema15_21) if closes15 else None,
            "ema_gap_5m_pct": _r(ema_gap_5m_pct, 4),
            "atr_1m_pct": _r(_atr_pct(c1, 14), 4),
            "spread_pct": _r(spread_pct, 4),
            "top5_bid_depth_usdt": _r(bid_depth, 2),
            "top5_ask_depth_usdt": _r(ask_depth, 2),
            "imbalance": _r((bid_depth - ask_depth) / max(1.0, bid_depth + ask_depth), 4),
            "spot_bid_depth_usdt": _r(spot_bid_depth, 2),
            "spot_ask_depth_usdt": _r(spot_ask_depth, 2),
            "spot_imbalance": _r((spot_bid_depth - spot_ask_depth) / max(1.0, spot_bid_depth + spot_ask_depth), 4),
            "spot_depth_error": spot_ob.get("error", ""),
        }

    async def decide(self, exchange_client, settings: dict) -> AIScalpDecision:
        """Backward-compatible selector: choose one allowed symbol.

        v0099 uses decide_symbol() in the trading loop so BTC and ETH can run
        independently. Keep this method for older tests/integrations.
        """
        allowed = self.symbols(settings)
        for symbol in allowed:
            dec = await self.decide_symbol(exchange_client, settings, symbol)
            if dec.ok and dec.decision in {"LONG", "SHORT"}:
                return dec
        return dec if 'dec' in locals() else AIScalpDecision(ok=True, decision="WAIT", reason="no symbols", model=active_model(settings))

    async def decide_symbol(self, exchange_client, settings: dict, symbol: str) -> AIScalpDecision:
        """Ask OpenAI for exactly one symbol only, with token guards.

        v0115 fixes token overuse: no schema retry chain by default, no raw candles,
        no history/stats, local reject before AI when data is invalid, and per-symbol
        WAIT cache so BTC/ETH are not re-asked every scan while the market is unchanged.
        """
        allowed_all = self.symbols(settings)
        norm = {x.upper().replace("_", "/"): x for x in allowed_all}
        sym = str(symbol or "").upper().replace("_", "/")
        if ":" not in sym and sym.endswith("/USDT"):
            sym += ":USDT"
        allowed = [norm.get(sym, symbol if symbol in allowed_all else allowed_all[0])]
        allowed_symbol = allowed[0]
        base_key = allowed_symbol.split(":")[0].replace("/", "").upper()
        model = active_model(settings)
        quality_mode = _b(settings, "ai_scalping_quality_filters_enabled", False)
        # v0128: OpenAI is required only in quality mode. In normal mode the
        # deterministic local liquidity setup gate can open trades without AI,
        # which avoids half-day stalls and cuts token usage to zero for normal scalping.
        key = openai_key(settings)
        if quality_mode and not key:
            return AIScalpDecision(ok=False, symbol=allowed_symbol, decision="WAIT", error="OpenAI API key missing", model=model)

        cooldown = int(_f(settings.get("ai_scalping_ai_cooldown_sec"), 60))
        now = time.time()
        cached = self._decision_cache.get(base_key)
        if cooldown > 0 and cached and now - cached[0] < cooldown:
            old = cached[1]
            return AIScalpDecision(
                ok=old.ok, symbol=old.symbol, decision=old.decision, confidence=old.confidence,
                reason=f"cached {int(cooldown - (now - cached[0]))}s: {old.reason}"[:220],
                raw=old.raw, model=old.model, market=old.market, cached=True, tp_strength=old.tp_strength,
            )

        try:
            market = await self.market_snapshot(exchange_client, allowed_symbol, quality_mode=quality_mode)
        except Exception as e:
            market = {"symbol": allowed_symbol, "error": str(e)[:160]}
        markets = [market]
        max_spread = _f(settings.get("ai_scalping_max_spread_pct"), _f(settings.get("max_spread_pct"), 0.20))

        # Do not spend tokens when local market data is unusable or spread is already too high.
        if market.get("error") or _f(market.get("price"), 0.0) <= 0:
            return AIScalpDecision(ok=True, symbol=allowed_symbol, decision="WAIT", confidence=0.0, reason="local: no reliable market data", model=model, market={"markets": markets})
        if _f(market.get("spread_pct"), 0.0) > max_spread:
            dec = AIScalpDecision(ok=True, symbol=allowed_symbol, decision="WAIT", confidence=0.0, reason="local: spread too high", model=model, market={"markets": markets})
            self._decision_cache[base_key] = (now, dec)
            return dec

        # v0152 REAL orderbook scalp path:
        # 1) SPOT orderbook chooses LONG/SHORT bias.
        # 2) FUTURES micro momentum confirms price is not moving against it.
        # 3) OpenAI is optional and can only reject; it no longer chooses direction.
        ob_bias = _spot_orderbook_bias(market, settings)
        if ob_bias.get("side") not in {"LONG", "SHORT"}:
            dec = AIScalpDecision(ok=True, symbol=allowed_symbol, decision="WAIT", confidence=0.0, reason="local: " + str(ob_bias.get("reason", "no spot bias"))[:190], model="spot_orderbook_bias", market={"markets": markets})
            self._decision_cache[base_key] = (now, dec)
            return dec

        side = str(ob_bias.get("side"))
        mom = _futures_micro_momentum(market, side, settings)
        if not mom.get("ok"):
            reason = f"local: {side} spot bias but {mom.get('reason', 'no futures momentum')}"
            dec = AIScalpDecision(ok=True, symbol=allowed_symbol, decision="WAIT", confidence=0.0, reason=reason[:220], model="spot_orderbook_bias", market={"markets": markets})
            self._decision_cache[base_key] = (now, dec)
            return dec

        min_conf = _f(settings.get("ai_scalping_min_confidence"), 0.52)
        # Use the existing global AI button as the on/off switch for optional confirmation.
        ai_confirm_enabled = bool(key) and _b(settings, "openai_analysis_enabled", False) and _b(settings, "ai_scalping_ai_entry_filter_enabled", True)
        spot_ratio = _f(ob_bias.get("ratio"), 0.0)
        mom_strength = _f(mom.get("strength"), 0.0)
        local_strength = max(0.0, min(1.0, (min(spot_ratio, 3.0) - 1.0) / 2.0 * 0.65 + mom_strength * 0.35))
        local_conf = max(min_conf, min(0.92, 0.50 + local_strength * 0.42))

        if not ai_confirm_enabled:
            reason = f"spot {side}: {ob_bias.get('reason')}; {mom.get('reason')}"
            return AIScalpDecision(ok=True, symbol=allowed_symbol, decision=side, confidence=local_conf, reason=reason[:180], raw="spot_orderbook_no_ai", model="spot_orderbook", market={"markets": markets}, tp_strength=local_strength)

        # Optional anti-fake AI prompt. Direction is already fixed by code.
        # AI may only return the same side or WAIT; if it returns the opposite side,
        # parse_ai_scalp_decision accepts it but we reject it below before candidate creation.
        prompt_obj = {
            "s": base_key,
            "side": side,
            "min": min_conf,
            "spot": {"bid": _r(market.get("spot_bid_depth_usdt"), 0), "ask": _r(market.get("spot_ask_depth_usdt"), 0), "ratio": _r(spot_ratio, 3)},
            "fut": {"r1": market.get("ret_1m_pct", 0), "r3": market.get("ret_3m_pct", 0), "sp": market.get("spread_pct", 0), "atr": market.get("atr_1m_pct", 0)},
        }
        prompt = json.dumps(prompt_obj, separators=(",", ":"), ensure_ascii=False)
        system = (
            'BTC/ETH micro-scalp anti-fake filter. JSON only. '
            'Direction is already chosen by SPOT orderbook. Do NOT choose another side. '
            'Reject only if flat, late entry, exhaustion candle, spread spike, or futures momentum is fake. '
            'Return {"symbol":"BTCUSDT|ETHUSDT","decision":"SAME_SIDE_OR_WAIT","confidence":0-1,"tp_strength":0-1,"reason":"max8w"}.'
        )
        timeout_sec = float(settings.get("openai_timeout_sec", os.getenv("OPENAI_TIMEOUT_SEC", "12")) or 12)
        headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
        async with self._sem:
            try:
                timeout = aiohttp.ClientTimeout(total=timeout_sec)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    # v0115: one tiny request by default. JSON mode and fallback are optional
                    # because rejected response_format calls were doubling token spend.
                    chat_body = {
                        "model": model,
                        "messages": [{"role": "system", "content": system}, {"role": "user", "content": prompt}],
                    }
                    if _b(settings, "ai_scalping_json_mode_enabled", True):
                        chat_body["response_format"] = _json_object_payload()
                    if str(model).lower().startswith(("gpt-5", "o1", "o3", "o4")):
                        chat_body["max_completion_tokens"] = 40
                    else:
                        chat_body["temperature"] = 0
                        chat_body["max_tokens"] = 40
                    async with session.post(OPENAI_CHAT_URL, headers=headers, json=chat_body) as r:
                        txt = await r.text()
                        if r.status == 200:
                            parsed = _extract_text(json.loads(txt))
                            dec = parse_ai_scalp_decision(parsed, allowed, model)
                            dec.market = {"markets": markets}
                            if dec.ok and dec.decision in {"LONG", "SHORT"} and dec.decision != side:
                                dec = AIScalpDecision(ok=True, symbol=allowed_symbol, decision="WAIT", confidence=0.0, reason=f"AI tried {dec.decision}, spot side is {side}", raw=parsed[:1000], model=model, market={"markets": markets})
                            if dec.ok and dec.decision == "WAIT":
                                self._decision_cache[base_key] = (time.time(), dec)
                            return dec
                        first_error = txt[:240]

                    if not _b(settings, "ai_scalping_openai_fallback_enabled", False):
                        return AIScalpDecision(ok=False, symbol=allowed_symbol, decision="WAIT", error=f"OpenAI error chat={first_error}", model=model, market={"markets": markets})

                    fallback_body = dict(chat_body)
                    fallback_body.pop("response_format", None)
                    async with session.post(OPENAI_CHAT_URL, headers=headers, json=fallback_body) as r2:
                        txt2 = await r2.text()
                        if r2.status == 200:
                            parsed2 = _extract_text(json.loads(txt2))
                            dec = parse_ai_scalp_decision(parsed2, allowed, model)
                            dec.market = {"markets": markets}
                            if dec.ok and dec.decision == "WAIT":
                                self._decision_cache[base_key] = (time.time(), dec)
                            return dec
                        return AIScalpDecision(ok=False, symbol=allowed_symbol, decision="WAIT", error=f"OpenAI error chat={first_error}; fallback={r2.status}: {txt2[:160]}", model=model, market={"markets": markets})
            except Exception as e:
                return AIScalpDecision(ok=False, symbol=allowed_symbol, decision="WAIT", error=f"OpenAI unavailable: {e}", model=model, market={"markets": markets})

    def candidate_reject_reason(self, decision: AIScalpDecision, settings: dict) -> str:
        """Return exact local reject reason without extra OpenAI calls."""
        if not decision.ok:
            return decision.error or "AI unavailable"
        if decision.decision not in {"LONG", "SHORT"}:
            return f"AI decision {decision.decision}"
        if not decision.symbol:
            return "missing symbol"
        quality_mode = _b(settings, "ai_scalping_quality_filters_enabled", False)
        min_conf = _f(settings.get("ai_scalping_min_confidence"), 0.52)
        if quality_mode:
            min_conf = max(min_conf, _f(settings.get("ai_scalping_quality_min_confidence"), 0.72))
        if decision.confidence < min_conf:
            return f"confidence {decision.confidence:.2f} < {min_conf:.2f}"
        market = {}
        for m in ((decision.market or {}).get("markets") or []):
            if m.get("symbol") == decision.symbol:
                market = m
                break
        price = _f(market.get("price"), 0.0)
        if price <= 0:
            return "no reliable price"
        max_spread = _f(settings.get("ai_scalping_max_spread_pct"), _f(settings.get("max_spread_pct"), 0.20))
        spread = _f(market.get("spread_pct"), 0.0)
        if spread > max_spread:
            return f"spread {spread:.3f}% > max {max_spread:.3f}%"
        ob_bias = _spot_orderbook_bias(market, settings)
        if ob_bias.get("side") != decision.decision:
            return f"spot bias {ob_bias.get('side')} != {decision.decision}: {ob_bias.get('reason')}"
        mom = _futures_micro_momentum(market, decision.decision, settings)
        if not mom.get("ok"):
            return str(mom.get("reason") or "futures momentum rejected")
        if quality_mode:
            min_atr = _f(settings.get("ai_scalping_quality_min_atr_pct"), 0.035)
            min_gap = _f(settings.get("ai_scalping_quality_min_ema_gap_pct"), 0.015)
            min_ret5 = _f(settings.get("ai_scalping_quality_min_ret_5m_abs_pct"), 0.035)
            atr = _f(market.get("atr_1m_pct"), 0.0)
            gap = _f(market.get("ema_gap_5m_pct"), 0.0)
            ret5 = abs(_f(market.get("ret_5m_pct"), 0.0))
            if atr < min_atr:
                return f"quality ATR {atr:.3f}% < {min_atr:.3f}%"
            if gap < min_gap and ret5 < min_ret5:
                return f"quality chop: ema_gap {gap:.3f}% < {min_gap:.3f}% and ret5 {ret5:.3f}% < {min_ret5:.3f}%"
        return "unknown local reject"

    def make_candidate(self, decision: AIScalpDecision, settings: dict) -> dict | None:
        if not decision.ok or decision.decision not in {"LONG", "SHORT"} or not decision.symbol:
            return None
        quality_mode = _b(settings, "ai_scalping_quality_filters_enabled", False)
        min_conf = _f(settings.get("ai_scalping_min_confidence"), 0.52)
        if quality_mode:
            min_conf = max(min_conf, _f(settings.get("ai_scalping_quality_min_confidence"), 0.72))
        if decision.confidence < min_conf:
            return None
        market = {}
        for m in ((decision.market or {}).get("markets") or []):
            if m.get("symbol") == decision.symbol:
                market = m
                break
        price = _f(market.get("price"), 0.0)
        if price <= 0:
            return None
        max_spread = _f(settings.get("ai_scalping_max_spread_pct"), _f(settings.get("max_spread_pct"), 0.20))
        if _f(market.get("spread_pct"), 0.0) > max_spread:
            return None
        ob_bias = _spot_orderbook_bias(market, settings)
        if ob_bias.get("side") != decision.decision:
            return None
        mom = _futures_micro_momentum(market, decision.decision, settings)
        if not mom.get("ok"):
            return None
        if quality_mode:
            min_atr = _f(settings.get("ai_scalping_quality_min_atr_pct"), 0.035)
            min_gap = _f(settings.get("ai_scalping_quality_min_ema_gap_pct"), 0.015)
            min_ret5 = _f(settings.get("ai_scalping_quality_min_ret_5m_abs_pct"), 0.035)
            atr = _f(market.get("atr_1m_pct"), 0.0)
            gap = _f(market.get("ema_gap_5m_pct"), 0.0)
            ret5 = abs(_f(market.get("ret_5m_pct"), 0.0))
            if atr < min_atr:
                return None
            if gap < min_gap and ret5 < min_ret5:
                return None
        clean_market = {k: v for k, v in market.items() if not str(k).startswith("_")}
        atr = max(0.01, _f(market.get("atr_1m_pct"), 0.18))
        # v0143 ETH/BTC micro-scalp brackets. TP is chosen by AI/local setup
        # strength inside the symbol range; SL is ALWAYS 2x TP. No separate random SL.
        base = str(decision.symbol or "").upper()
        if base.startswith("BTC"):
            min_tp_pct = _f(settings.get("ai_scalping_btc_min_tp_pct"), _f(os.getenv("AI_SCALPING_BTC_MIN_TP_PCT"), 0.08))
            max_tp_pct = _f(settings.get("ai_scalping_btc_max_tp_pct"), _f(os.getenv("AI_SCALPING_BTC_MAX_TP_PCT"), 0.12))
        elif base.startswith("ETH"):
            min_tp_pct = _f(settings.get("ai_scalping_eth_min_tp_pct"), _f(os.getenv("AI_SCALPING_ETH_MIN_TP_PCT"), 0.10))
            max_tp_pct = _f(settings.get("ai_scalping_eth_max_tp_pct"), _f(os.getenv("AI_SCALPING_ETH_MAX_TP_PCT"), 0.16))
        else:
            min_tp_pct = _f(settings.get("ai_scalping_min_tp_pct"), _f(os.getenv("AI_SCALPING_MIN_TP_PCT"), 0.08))
            max_tp_pct = _f(settings.get("ai_scalping_max_tp_pct"), _f(os.getenv("AI_SCALPING_MAX_TP_PCT"), 0.14))
        min_tp_pct = max(0.01, min_tp_pct)
        max_tp_pct = max(min_tp_pct, max_tp_pct)
        spot_ratio = _f(ob_bias.get("ratio"), 0.0)
        local_strength = max(0.0, min(1.0, (min(spot_ratio, 3.0) - 1.0) / 2.0 * 0.65 + _f(mom.get("strength"), 0.0) * 0.35))
        strength = _f(getattr(decision, "tp_strength", 0.0), local_strength)
        if strength <= 0:
            strength = local_strength
        strength = max(0.0, min(1.0, strength))
        tp_pct = min_tp_pct + (max_tp_pct - min_tp_pct) * strength
        sl_mult = max(1.0, _f(settings.get("ai_scalping_sl_tp_multiplier"), _f(os.getenv("AI_SCALPING_SL_TP_MULTIPLIER"), 2.0)))
        sl_pct = tp_pct * sl_mult
        return {
            "symbol": decision.symbol,
            "side": decision.decision,
            "strategy": "ai_scalping",
            "confidence": decision.confidence,
            "futures_price": price,
            "atr_pct": atr,
            "ai_scalping_tp_pct": _r(tp_pct, 4),
            "ai_scalping_sl_pct": _r(sl_pct, 4),
            "ai_scalping_tp_strength": _r(strength, 4),
            "risk_pct": _f(settings.get("risk_pct"), 0.005),
            "score_details": {"ai_reason": decision.reason, "ai_model": decision.model, "spot_bias": ob_bias, "futures_momentum": mom, **clean_market},
        }
