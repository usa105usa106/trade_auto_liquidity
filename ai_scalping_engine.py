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
    score = 0.0
    score += 30.0 if swept else 0.0
    score += 25.0 if reclaimed else 0.0
    score += 15.0 if near_edge else 0.0
    score += 10.0 if impulse_ok else 0.0
    score += min(10.0, max(0.0, vol_ratio - 1.0) * 8.0)
    score += min(10.0, max(0.0, rr - 0.8) * 8.0)
    valid = bool(swept and reclaimed and near_edge and impulse_ok and rr >= 0.85)
    parts = []
    if not swept: parts.append("no sweep")
    if not reclaimed: parts.append("no reclaim")
    if not near_edge: parts.append("mid range")
    if not impulse_ok: parts.append("bad impulse")
    if rr < 0.85: parts.append(f"RR {rr:.2f} < 0.85")
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
        # Some providers/models may still return plain text or malformed JSON
        # despite strict instructions, e.g. "WAIT No reliable market data",
        # "LONG confidence 0.82 ...", or a truncated object containing only
        # decision text. Do not convert LONG/SHORT without a parsed confidence
        # to 0.00: that creates false local rejects. Use a conservative fallback
        # and mark it in the reason/log so the operator can see what happened.
        plain = re.sub(r"^```(?:json|text)?\s*", "", raw, flags=re.I).strip()
        plain = re.sub(r"\s*```$", "", plain).strip()
        m_dec = re.search(r"\b(LONG|SHORT|WAIT)\b", plain, flags=re.I)
        if m_dec:
            decision = m_dec.group(1).upper()
            m_conf = re.search(r"(?:confidence|conf)\s*[:=]?\s*(0(?:\.\d+)?|1(?:\.0+)?|\d{1,3}(?:\.\d+)?)", plain, flags=re.I)
            inferred = False
            if m_conf:
                conf = _f(m_conf.group(1), 0.0)
                if conf > 1:
                    conf /= 100.0
            else:
                # Missing confidence must not become 0.00 for LONG/SHORT.
                # 0.72 is intentionally conservative: it passes normal mode
                # and meets the default quality gate, but is still visible in logs.
                conf = 0.72 if decision in {"LONG", "SHORT"} else 0.65
                inferred = True
            m_reason = re.search(r'"reason"\s*:\s*"([^"{}]{0,220})', plain, flags=re.I)
            reason = _clean_reason(m_reason.group(1) if m_reason else plain[m_dec.end():], 180)
            if inferred:
                reason = ("confidence missing -> fallback %.2f; %s" % (conf, reason)).strip(" ;")[:220]
            # In decide_symbol() there is exactly one allowed symbol, so use it.
            symbol = allowed_symbols[0] if allowed_symbols else ""
            return AIScalpDecision(ok=True, symbol=symbol, decision=decision, confidence=max(0.0, min(1.0, conf)), reason=reason, raw=raw[:1000], model=model, confidence_inferred=inferred)
        return AIScalpDecision(ok=False, error="AI did not return JSON", raw=raw[:1000], model=model)
    symbol = str(data.get("symbol") or "").upper().replace("_", "").replace("/", "").replace(":USDT", "")
    norm_allowed = {s.upper().replace("_", "").replace("/", "").replace(":USDT", ""): s for s in allowed_symbols}
    if symbol not in norm_allowed:
        return AIScalpDecision(ok=True, symbol="", decision="WAIT", confidence=0.0, reason="AI symbol not allowed", raw=raw[:1000], model=model)
    decision = str(data.get("decision") or data.get("side") or "WAIT").upper().strip()
    if decision not in {"LONG", "SHORT", "WAIT"}:
        decision = "WAIT"
    inferred = False
    if data.get("confidence") is None:
        conf = 0.72 if decision in {"LONG", "SHORT"} else 0.65
        inferred = True
    else:
        conf = _f(data.get("confidence"), 0.0)
        if conf > 1:
            conf /= 100.0
    reason = _clean_reason(data.get("reason") or "", 180)
    if inferred:
        reason = ("confidence missing -> fallback %.2f; %s" % (conf, reason)).strip(" ;")[:220]
    return AIScalpDecision(
        ok=True,
        symbol=norm_allowed[symbol],
        decision=decision,
        confidence=max(0.0, min(1.0, conf)),
        reason=reason,
        raw=raw[:1000],
        model=model,
        confidence_inferred=inferred,
    )




def ai_scalp_json_schema(symbol: str) -> dict:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "symbol": {"type": "string", "enum": [symbol]},
            "decision": {"type": "string", "enum": ["LONG", "SHORT", "WAIT"]},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "reason": {"type": "string", "maxLength": 80},
        },
        "required": ["symbol", "decision", "confidence", "reason"],
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
        closes1 = [_f(c[4]) for c in c1]
        closes5 = [_f(c[4]) for c in c5]
        closes15 = [_f(c[4]) for c in c15]
        price = _f(ticker.get("last") or ticker.get("close") or (closes1[-1] if closes1 else 0))
        bid = _f(ticker.get("bid"), price)
        ask = _f(ticker.get("ask"), price)
        spread_pct = ((ask - bid) / price * 100.0) if price > 0 and ask > bid else 0.0
        bid_depth = sum(_f(p) * _f(q) for p, q, *_ in (ob.get("bids") or [])[:5])
        ask_depth = sum(_f(p) * _f(q) for p, q, *_ in (ob.get("asks") or [])[:5])
        ret_1m = ((closes1[-1] - closes1[-2]) / closes1[-2] * 100.0) if len(closes1) > 2 and closes1[-2] else 0.0
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
                raw=old.raw, model=old.model, market=old.market, cached=True,
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

        atr_pct = _f(market.get("atr_1m_pct"), 0.0)
        price = _f(market.get("price"), 0.0)
        long_setup = _local_scalp_setup(market.get("_candles1") or [], "LONG", price, atr_pct)
        short_setup = _local_scalp_setup(market.get("_candles1") or [], "SHORT", price, atr_pct)

        # Do not spend OpenAI tokens when the deterministic liquidity setup gate
        # has no valid LONG/SHORT candidate. This is the main token saver.
        if not long_setup.get("valid") and not short_setup.get("valid"):
            reason = f"local: no setup L={long_setup.get('reason','')}; S={short_setup.get('reason','')}"
            dec = AIScalpDecision(ok=True, symbol=allowed_symbol, decision="WAIT", confidence=0.0, reason=reason[:220], model=model, market={"markets": markets})
            self._decision_cache[base_key] = (now, dec)
            return dec

        # v0128 normal mode: no OpenAI approval required. Pick the best valid
        # local setup and return a synthetic decision that still passes through
        # make_candidate(), planner, TP/SL, spread and quality-score checks.
        if not quality_mode:
            valid_setups = []
            if long_setup.get("valid"):
                ql = _quality_score(market, long_setup, "LONG")
                valid_setups.append(("LONG", long_setup, ql))
            if short_setup.get("valid"):
                qs = _quality_score(market, short_setup, "SHORT")
                valid_setups.append(("SHORT", short_setup, qs))
            if not valid_setups:
                dec = AIScalpDecision(ok=True, symbol=allowed_symbol, decision="WAIT", confidence=0.0, reason="local: no valid setup", model="local_setup_gate", market={"markets": markets})
                self._decision_cache[base_key] = (now, dec)
                return dec
            side, setup, q = max(valid_setups, key=lambda x: (_f(x[2].get("score"), 0.0), _f(x[1].get("structure_rr"), 0.0), _f(x[1].get("score"), 0.0)))
            min_conf = _f(settings.get("ai_scalping_min_confidence"), 0.58)
            conf = max(min_conf, min(0.90, max(_f(q.get("score"), 0.0), _f(setup.get("score"), 0.0)) / 100.0))
            reason = f"local setup {side}: score={_f(setup.get('score'),0):.1f} q={_f(q.get('score'),0):.1f} rr={_f(setup.get('structure_rr'),0):.2f}"
            return AIScalpDecision(ok=True, symbol=allowed_symbol, decision=side, confidence=conf, reason=reason[:180], raw="local_setup_gate_no_ai", model="local_setup_gate", market={"markets": markets})

        feature_payload = _compact_ai_setup_features(market, long_setup, short_setup)
        min_conf = _f(settings.get("ai_scalping_quality_min_confidence" if quality_mode else "ai_scalping_min_confidence"), 0.72 if quality_mode else 0.58)
        prompt_obj = {
            "s": base_key,
            "mode": "quality" if quality_mode else "normal",
            "min": min_conf,
            "task": "validate one listed liquidity scalp candidate; choose only candidate side or WAIT",
            "x": feature_payload,
        }
        prompt = json.dumps(prompt_obj, separators=(",", ":"), ensure_ascii=False)
        system = 'JSON only. You are a setup validator, not a direction predictor. Use only x.cand sides; if target path/MTF/chop unclear return WAIT. Return {"symbol":"BTCUSDT|ETHUSDT","decision":"LONG|SHORT|WAIT","confidence":0-1,"reason":"short"}.'
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
                    if _b(settings, "ai_scalping_json_mode_enabled", False):
                        chat_body["response_format"] = _json_object_payload()
                    if str(model).lower().startswith(("gpt-5", "o1", "o3", "o4")):
                        chat_body["max_completion_tokens"] = 32
                    else:
                        chat_body["temperature"] = 0.1
                        chat_body["max_tokens"] = 32
                    async with session.post(OPENAI_CHAT_URL, headers=headers, json=chat_body) as r:
                        txt = await r.text()
                        if r.status == 200:
                            parsed = _extract_text(json.loads(txt))
                            dec = parse_ai_scalp_decision(parsed, allowed, model)
                            dec.market = {"markets": markets}
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
        min_conf = _f(settings.get("ai_scalping_min_confidence"), 0.58)
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
        setup = _local_scalp_setup(market.get("_candles1") or [], decision.decision, price, _f(market.get("atr_1m_pct"), 0.0))
        if not setup.get("valid"):
            return str(setup.get("reason") or "setup rejected")
        q = _quality_score(market, setup, decision.decision)
        min_q = _f(settings.get("ai_scalping_setup_min_quality_score"), 58.0)
        if q.get("score", 0.0) < min_q:
            return f"quality score {q.get('score', 0):.1f} < {min_q:.1f}: {q.get('notes') or 'weak setup'}"
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
        min_conf = _f(settings.get("ai_scalping_min_confidence"), 0.58)
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
        setup = _local_scalp_setup(market.get("_candles1") or [], decision.decision, price, _f(market.get("atr_1m_pct"), 0.0))
        if not setup.get("valid"):
            return None
        q = _quality_score(market, setup, decision.decision)
        min_q = _f(settings.get("ai_scalping_setup_min_quality_score"), 58.0)
        if q.get("score", 0.0) < min_q:
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
        raw_sl = max(_f(setup.get("structure_sl_pct"), 0.0), atr * 1.15)
        # Cap absurd wick stops, but keep them wider than the old fixed micro-stop.
        sl_pct = max(0.16, min(0.75, raw_sl))
        rr_target = 1.35 if q.get("score", 0) < 72 else 1.55
        raw_tp = max(_f(setup.get("structure_tp_pct"), 0.0), sl_pct * rr_target)
        tp_pct = max(0.18, min(1.10, raw_tp))
        return {
            "symbol": decision.symbol,
            "side": decision.decision,
            "strategy": "ai_scalping",
            "confidence": decision.confidence,
            "futures_price": price,
            "atr_pct": atr,
            "ai_scalping_tp_pct": _r(tp_pct, 4),
            "ai_scalping_sl_pct": _r(sl_pct, 4),
            "risk_pct": _f(settings.get("risk_pct"), 0.005),
            "score_details": {"ai_reason": decision.reason, "ai_model": decision.model, "setup": setup, "quality_score": q, **clean_market},
        }
