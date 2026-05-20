from __future__ import annotations

import asyncio
import json
import math
import os
import re
from dataclasses import dataclass
from typing import Any

import aiohttp

from models import TradePlan
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
        # v0110: Some providers/models may still return plain text despite
        # JSON instructions, e.g. "WAIT No reliable market data" or
        # "LONG confidence 0.82 ...". Treat this as a valid fallback instead
        # of breaking the AI scalping loop. LONG/SHORT without confidence stays
        # at 0.0 and will be rejected by the normal confidence gate.
        plain = re.sub(r"^```(?:json|text)?\s*", "", raw, flags=re.I).strip()
        plain = re.sub(r"\s*```$", "", plain).strip()
        m_dec = re.search(r"\b(LONG|SHORT|WAIT)\b", plain, flags=re.I)
        if m_dec:
            decision = m_dec.group(1).upper()
            conf = 0.0
            m_conf = re.search(r"(?:confidence|conf)\s*[:=]?\s*(0(?:\.\d+)?|1(?:\.0+)?|\d{1,3}(?:\.\d+)?)", plain, flags=re.I)
            if m_conf:
                conf = _f(m_conf.group(1), 0.0)
                if conf > 1:
                    conf /= 100.0
            reason = plain[m_dec.end():].strip(" :-—–\n\t")[:220]
            # In decide_symbol() there is exactly one allowed symbol, so use it.
            symbol = allowed_symbols[0] if allowed_symbols else ""
            return AIScalpDecision(ok=True, symbol=symbol, decision=decision, confidence=max(0.0, min(1.0, conf)), reason=reason, raw=raw[:1000], model=model)
        return AIScalpDecision(ok=False, error="AI did not return JSON", raw=raw[:1000], model=model)
    symbol = str(data.get("symbol") or "").upper().replace("/", "").replace(":USDT", "")
    norm_allowed = {s.upper().replace("/", "").replace(":USDT", ""): s for s in allowed_symbols}
    if symbol not in norm_allowed:
        return AIScalpDecision(ok=True, symbol="", decision="WAIT", confidence=0.0, reason="AI symbol not allowed", raw=raw[:1000], model=model)
    decision = str(data.get("decision") or data.get("side") or "WAIT").upper().strip()
    if decision not in {"LONG", "SHORT", "WAIT"}:
        decision = "WAIT"
    conf = _f(data.get("confidence"), 0.0)
    if conf > 1:
        conf /= 100.0
    return AIScalpDecision(
        ok=True,
        symbol=norm_allowed[symbol],
        decision=decision,
        confidence=max(0.0, min(1.0, conf)),
        reason=str(data.get("reason") or "")[:220],
        raw=raw[:1000],
        model=model,
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
        """Ask OpenAI for exactly one symbol only.

        This is the cheap dual-loop mode: BTC and ETH are queried separately only
        when that specific symbol has no active position. The AI controls only
        LONG/SHORT/WAIT; the bot controls size, TP/SL, leverage and execution.
        """
        allowed_all = self.symbols(settings)
        norm = {x.upper().replace("_", "/"): x for x in allowed_all}
        sym = str(symbol or "").upper().replace("_", "/")
        if ":" not in sym and sym.endswith("/USDT"):
            sym += ":USDT"
        allowed = [norm.get(sym, symbol if symbol in allowed_all else allowed_all[0])]
        model = active_model(settings)
        key = openai_key(settings)
        if not key:
            return AIScalpDecision(ok=False, symbol=allowed[0], decision="WAIT", error="OpenAI API key missing", model=model)
        quality_mode = _b(settings, "ai_scalping_quality_filters_enabled", False)
        try:
            market = await self.market_snapshot(exchange_client, allowed[0], quality_mode=quality_mode)
        except Exception as e:
            market = {"symbol": allowed[0], "error": str(e)[:160]}
        markets = [market]
        max_spread = _f(settings.get("ai_scalping_max_spread_pct"), _f(settings.get("max_spread_pct"), 0.20))
        # v0112: ultra-low-token prompt. BTC_ETH_AI_SCALP_DIRECTION. Return one strict JSON object only; bot TP before bot SL; Do not force trades; max_output_tokens replaced by max_completion_tokens where required Do NOT send raw candles, trade history,
        # stats, or verbose rules. The AI gets only compact derived features.
        compact_market = {
            "s": allowed[0].split(":")[0].replace("/", "_"),
            "p": market.get("price", 0),
            "r1": market.get("ret_1m_pct", 0),
            "r5": market.get("ret_5m_pct", 0),
            "rsi": market.get("rsi_1m", 50),
            "e1": 1 if market.get("ema9_gt_ema21_1m") else 0,
            "e5": 1 if market.get("ema9_gt_ema21_5m") else 0,
            "e15": (1 if market.get("ema9_gt_ema21_15m") else 0) if market.get("ema9_gt_ema21_15m") is not None else None,
            "gap5": market.get("ema_gap_5m_pct", 0),
            "atr": market.get("atr_1m_pct", 0),
            "sp": market.get("spread_pct", 0),
            "imb": market.get("imbalance", 0),
        }
        min_conf = _f(settings.get("ai_scalping_quality_min_confidence" if quality_mode else "ai_scalping_min_confidence"), 0.72 if quality_mode else 0.58)
        prompt_obj = {
            "task": "scalp_dir",
            "rule": "Return JSON only. Pick LONG/SHORT only if TP likely before SL, else WAIT.",
            "sym": compact_market["s"],
            "tf": "1m/5m" + ("/15m" if quality_mode else ""),
            "q": 1 if quality_mode else 0,
            "min_conf": min_conf,
            "max_sp": max_spread,
            "m": compact_market,
            "out": {"symbol": compact_market["s"], "decision": "LONG|SHORT|WAIT", "confidence": 0.0, "reason": "short"},
        }
        prompt = json.dumps(prompt_obj, separators=(",", ":"), ensure_ascii=False)
        system = "BTC/ETH futures scalping filter. JSON only. No prose. Prefer WAIT on mixed/chop/weak edge."
        timeout_sec = float(settings.get("openai_timeout_sec", os.getenv("OPENAI_TIMEOUT_SEC", "12")) or 12)
        headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
        async with self._sem:
            try:
                timeout = aiohttp.ClientTimeout(total=timeout_sec)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    # v0112: one cheap Chat Completions request first. The older
                    # Responses+schema path could double/triple token spend when a
                    # model/provider rejected structured output and the code retried.
                    chat_body = {
                        "model": model,
                        "messages": [{"role": "system", "content": system}, {"role": "user", "content": prompt}],
                        "response_format": _json_object_payload(),
                    }
                    if str(model).lower().startswith(("gpt-5", "o1", "o3", "o4")):
                        chat_body["max_completion_tokens"] = 40
                    else:
                        chat_body["temperature"] = 0.1
                        chat_body["max_tokens"] = 40
                    async with session.post(OPENAI_CHAT_URL, headers=headers, json=chat_body) as r:
                        txt = await r.text()
                        if r.status == 200:
                            parsed = _extract_text(json.loads(txt))
                            dec = parse_ai_scalp_decision(parsed, allowed, model)
                            dec.market = {"markets": markets}
                            return dec
                        first_error = txt[:240]

                    # One fallback only: no response_format for providers that reject JSON mode.
                    fallback_body = dict(chat_body)
                    fallback_body.pop("response_format", None)
                    async with session.post(OPENAI_CHAT_URL, headers=headers, json=fallback_body) as r2:
                        txt2 = await r2.text()
                        if r2.status == 200:
                            parsed2 = _extract_text(json.loads(txt2))
                            dec = parse_ai_scalp_decision(parsed2, allowed, model)
                            dec.market = {"markets": markets}
                            return dec
                        return AIScalpDecision(ok=False, symbol=allowed[0], decision="WAIT", error=f"OpenAI error chat={first_error}; fallback={r2.status}: {txt2[:160]}", model=model, market={"markets": markets})
            except Exception as e:
                return AIScalpDecision(ok=False, symbol=allowed[0], decision="WAIT", error=f"OpenAI unavailable: {e}", model=model, market={"markets": markets})

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
        if quality_mode:
            side = decision.decision
            bias5 = market.get("ema9_gt_ema21_5m")
            bias15 = market.get("ema9_gt_ema21_15m")
            if side == "LONG" and (bias5 is False or bias15 is False):
                return None
            if side == "SHORT" and (bias5 is True or bias15 is True):
                return None
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
        return {
            "symbol": decision.symbol,
            "side": decision.decision,
            "strategy": "ai_scalping",
            "confidence": decision.confidence,
            "futures_price": price,
            "atr_pct": max(0.01, _f(market.get("atr_1m_pct"), 0.18)),
            "risk_pct": _f(settings.get("risk_pct"), 0.005),
            "score_details": {"ai_reason": decision.reason, "ai_model": decision.model, **market},
        }
