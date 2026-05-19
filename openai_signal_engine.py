from __future__ import annotations

import asyncio
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict

import aiohttp


OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
OPENAI_CHAT_URL = "https://api.openai.com/v1/chat/completions"

MODEL_CHOICES = [
    "gpt-5.4-mini",
    "gpt-4o-mini",
    "gpt-5.5",
    "gpt-5.5-pro",
    "gpt-4.1",
]
DEFAULT_MODEL = "gpt-5.4-mini"
STRENGTH_CHOICES = {"weak", "medium", "strong"}
CONFIDENCE_FLOOR = {"weak": 0.55, "medium": 0.65, "strong": 0.75}


@dataclass
class AIVerdict:
    ok: bool
    approved: bool
    confidence: float = 0.0
    reason: str = ""
    raw: str = ""
    error: str = ""
    mode: str = "medium"
    model: str = DEFAULT_MODEL


def bool_setting(settings: dict, key: str, default: bool = False) -> bool:
    raw = settings.get(key, default)
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def parse_bool_value(raw: Any, default: bool = False) -> bool:
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return default
    if isinstance(raw, (int, float)):
        return bool(raw)
    text = str(raw).strip().lower()
    if text in {"1", "true", "yes", "on", "approve", "approved"}:
        return True
    if text in {"0", "false", "no", "off", "reject", "rejected"}:
        return False
    return default


def openai_key(settings: dict) -> str:
    key = str(settings.get("openai_api_key") or "").strip()
    if key:
        return key
    if bool_setting(settings, "openai_env_fallback", True):
        return str(os.getenv("OPENAI_API_KEY", "")).strip()
    return ""


def active_model(settings: dict) -> str:
    model = str(settings.get("openai_model") or os.getenv("OPENAI_MODEL", DEFAULT_MODEL) or DEFAULT_MODEL).strip()
    return model if model in MODEL_CHOICES else DEFAULT_MODEL


def active_strength(settings: dict) -> str:
    mode = str(settings.get("openai_check_strength") or os.getenv("OPENAI_CHECK_STRENGTH", "medium") or "medium").strip().lower()
    return mode if mode in STRENGTH_CHOICES else "medium"


def _compact_float(v: Any, digits: int = 6) -> Any:
    try:
        return round(float(v), digits)
    except Exception:
        return v


def _pct_dist(entry: float, level: float, side: str, kind: str) -> float:
    try:
        entry = float(entry)
        level = float(level)
        if entry <= 0 or level <= 0:
            return 0.0
        side = str(side).upper()
        if kind == "tp":
            return ((level - entry) / entry * 100.0) if side == "LONG" else ((entry - level) / entry * 100.0)
        return ((entry - level) / entry * 100.0) if side == "LONG" else ((level - entry) / entry * 100.0)
    except Exception:
        return 0.0


def _pick(details: dict, keys: list[str]) -> dict:
    out = {}
    for k in keys:
        if k in details:
            out[k] = _compact_float(details.get(k), 6)
    return out


def compact_signal_payload(candidate: dict, plan: Any, settings: dict) -> dict:
    details = candidate.get("score_details") or {}
    if not isinstance(details, dict):
        details = {}
    symbol = getattr(plan, "symbol", candidate.get("symbol"))
    side = getattr(plan, "side", candidate.get("side"))
    strategy = str(getattr(plan, "strategy", candidate.get("strategy")) or "").lower()
    entry = float(getattr(plan, "entry_price", 0) or 0)
    sl = float(getattr(plan, "stop_price", 0) or 0)
    tp = float(getattr(plan, "take_price", 0) or 0)
    sl_pct = max(0.0, _pct_dist(entry, sl, side, "sl"))
    tp_pct = max(0.0, _pct_dist(entry, tp, side, "tp"))
    rr_actual = tp_pct / sl_pct if sl_pct > 0 else 0.0

    common_details = [
        "vol_ratio", "volume_ratio", "atr_pct", "spread_pct", "imbalance",
        "move_1m", "move_5m", "momentum_5m_pct", "breakout", "weak_filter",
        "ema9", "ema21", "reclaim", "upper_wick", "lower_wick", "sweep",
    ]
    liquidity_details = [
        "setup", "rr_reason", "adaptive_rr", "target_rr", "zone_low", "zone_high", "zone_type",
        "zone_quality", "zone_intact", "mtf_score", "bos", "choch", "bos_level", "bos_strength_pct",
        "displacement_pct", "displacement_body", "volume_ratio", "vol_ratio", "rejection_wick",
        "retest_rejection_wick", "clean_path", "liquidity_target", "sweep", "sweep_wick",
        "sweep_index", "displacement_index", "reclaim_pct", "fvg_low", "fvg_high", "imbalance",
    ]
    selected_details = _pick(details, liquidity_details if strategy == "liquidity_retest" else common_details)

    return {
        "symbol": symbol,
        "side": side,
        "strategy": strategy,
        "bot_confidence": _compact_float(candidate.get("confidence", getattr(plan, "confidence", 0)), 4),
        "session": candidate.get("session", getattr(plan, "session", "NORMAL")),
        "market": {
            "spread_pct": _compact_float(candidate.get("spread_pct", details.get("spread_pct", 0)), 5),
            "expected_slippage_pct": _compact_float(candidate.get("expected_slippage_pct", 0), 5),
            "depth_usdt_top10": _compact_float(candidate.get("depth_usdt", 0), 2),
            "atr_pct": _compact_float(candidate.get("atr_pct", details.get("atr_pct", 0)), 5),
        },
        "spot": {
            "enabled": bool_setting(settings, "spot_confirmation_enabled", True),
            "confirmed": bool(candidate.get("spot_confirmed", True)),
            "state": candidate.get("spot_confirmation", "OFF"),
            "reason": str(candidate.get("spot_reason", ""))[:120],
        },
        "plan_fixed_by_bot": {
            "entry": _compact_float(entry, 8),
            "sl": _compact_float(sl, 8),
            "tp": _compact_float(tp, 8),
            "sl_pct": _compact_float(sl_pct, 4),
            "tp_pct": _compact_float(tp_pct, 4),
            "rr": _compact_float(rr_actual or getattr(plan, "liquidity_retest_rr", 0), 3),
            "notional_usdt": _compact_float(getattr(plan, "planned_notional_usdt", 0), 2),
            "order_type": getattr(plan, "order_type", ""),
        },
        "details": selected_details,
    }


def build_scalp_prompt(payload: dict, strength: str) -> tuple[str, str, int]:
    strict = {
        "weak": "Reject only obvious bad scalp entries: late move, chop/range, weak volume, bad spread/slippage, or momentum already faded.",
        "medium": "Approve only if fresh momentum, acceptable spread/slippage, volume support, not late, and enough TP room after fees/spread.",
        "strong": "Approve only A-quality scalp continuation: fresh impulse, good volume, no chop, no obvious wall/resistance/support directly ahead, clean risk after fees.",
    }[strength]
    system = (
        "You are a crypto futures SCALP trade validator. The bot already calculated entry/SL/TP; "
        "do not change levels. Return JSON only. No markdown."
    )
    prompt = {
        "task": "approve_or_reject_scalp_signal",
        "strictness": strength,
        "decision_rule": strict,
        "focus": ["fresh_momentum", "not_late", "volume", "spread_slippage", "chop_filter", "tp_room_after_costs", "side_matches_orderbook"],
        "reject_if": ["late_entry", "momentum_faded", "range_chop", "weak_volume", "spread_or_slippage_too_high", "bad_rr_after_fees", "opposite_orderbook_pressure"],
        "reply_schema": {"approve": True, "confidence": 0.0, "reason": "max 12 words"},
        "signal": payload,
    }
    max_tokens = 110 if strength == "weak" else 150 if strength == "medium" else 210
    return system, json.dumps(prompt, separators=(",", ":"), ensure_ascii=False), max_tokens


def build_liquidity_retest_prompt(payload: dict, strength: str) -> tuple[str, str, int]:
    strict = {
        "weak": "Approve reasonable sweep-retest setups; reject if sweep/reclaim/retest is missing, zone is broken, or RR/target path is poor.",
        "medium": "Approve only coherent SMC setups: sweep, reclaim, BOS/CHOCH or displacement, valid OB/FVG zone, intact retest, rejection wick, clean target room.",
        "strong": "Be strict like a discretionary SMC trader: reject dirty structure, fake BOS, mitigated/broken zone, weak rejection, poor MTF/context, no clean liquidity target, or forced late entry.",
    }[strength]
    system = (
        "You validate crypto futures SMC liquidity retest setups: liquidity sweep -> reclaim -> displacement/BOS/CHOCH -> OB/FVG zone -> retest/rejection. "
        "The bot already calculated entry/SL/TP/RR; do not invent levels. Return JSON only. No markdown."
    )
    prompt = {
        "task": "approve_or_reject_liquidity_retest_signal",
        "strictness": strength,
        "decision_rule": strict,
        "must_validate": [
            "real_liquidity_sweep", "reclaim_after_sweep", "BOS_or_CHOCH", "displacement_strength",
            "OB_or_FVG_zone_quality", "zone_not_broken_before_retest", "clean_retest_not_breakdown",
            "rejection_wick", "MTF_context", "clean_path_to_liquidity_target", "RR_2_to_4_valid"
        ],
        "reject_if": [
            "zone_broken_or_mitigated", "no_retest", "weak_rejection", "fake_BOS", "late_entry",
            "target_too_close", "dirty_structure", "MTF_conflict", "spread_too_high"
        ],
        "reply_schema": {"approve": True, "confidence": 0.0, "rr_ok": True, "reason": "max 14 words"},
        "signal": payload,
    }
    max_tokens = 160 if strength == "weak" else 230 if strength == "medium" else 310
    return system, json.dumps(prompt, separators=(",", ":"), ensure_ascii=False), max_tokens


def build_prompt(candidate: dict, plan: Any, settings: dict) -> tuple[str, str, int, str]:
    strength = active_strength(settings)
    payload = compact_signal_payload(candidate, plan, settings)
    strategy = str(payload.get("strategy") or "").lower()
    if strategy == "liquidity_retest":
        system, prompt, max_tokens = build_liquidity_retest_prompt(payload, strength)
    else:
        system, prompt, max_tokens = build_scalp_prompt(payload, strength)
    return system, prompt, max_tokens, strength


def _extract_text(data: Dict[str, Any]) -> str:
    if isinstance(data, str):
        return data.strip()
    if not isinstance(data, dict):
        return ""
    if isinstance(data.get("output_text"), str) and data["output_text"].strip():
        return data["output_text"].strip()
    chunks: list[str] = []
    def walk(obj: Any):
        if isinstance(obj, str):
            chunks.append(obj)
        elif isinstance(obj, dict):
            typ = str(obj.get("type", ""))
            if typ in {"output_text", "text"} and isinstance(obj.get("text"), str):
                chunks.append(obj["text"])
            elif isinstance(obj.get("content"), str):
                chunks.append(obj["content"])
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    walk(v)
        elif isinstance(obj, list):
            for v in obj:
                walk(v)
    walk(data.get("output", []))
    choices = data.get("choices") or []
    if choices:
        try:
            walk(choices[0].get("message", {}).get("content", ""))
        except Exception:
            pass
    return "\n".join(x.strip() for x in chunks if x and x.strip()).strip()


def parse_verdict(text: str, model: str, mode: str) -> AIVerdict:
    raw = (text or "").strip()
    if not raw:
        return AIVerdict(ok=False, approved=False, error="empty AI response", model=model, mode=mode)
    cleaned = raw.strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.I).strip()
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
    if isinstance(data, dict):
        approved = parse_bool_value(data.get("approve", data.get("approved", False)), False)
        try:
            conf = float(data.get("confidence", 0) or 0)
            if conf > 1:
                conf /= 100.0
        except Exception:
            conf = 0.0
        return AIVerdict(ok=True, approved=approved, confidence=max(0.0, min(1.0, conf)), reason=str(data.get("reason", ""))[:240], raw=raw[:1000], model=model, mode=mode)
    upper = cleaned.upper()
    approved = "APPROVE" in upper and "REJECT" not in upper
    rejected = "REJECT" in upper
    conf = 0.0
    cm = re.search(r"(\d{1,3})(?:\.\d+)?\s*%", cleaned)
    if cm:
        conf = min(1.0, float(cm.group(1)) / 100.0)
    return AIVerdict(ok=True, approved=approved and not rejected, confidence=conf, reason=cleaned[:180], raw=raw[:1000], model=model, mode=mode)


def enforce_confidence_floor(verdict: AIVerdict, strength: str) -> AIVerdict:
    floor = CONFIDENCE_FLOOR.get(strength, CONFIDENCE_FLOOR["medium"])
    if verdict.ok and verdict.approved and verdict.confidence < floor:
        reason = verdict.reason or "AI confidence below mode threshold"
        return AIVerdict(ok=True, approved=False, confidence=verdict.confidence, reason=f"low confidence < {floor:.2f}: {reason}"[:240], raw=verdict.raw, model=verdict.model, mode=verdict.mode)
    return verdict


class OpenAISignalEngine:
    def __init__(self):
        self._sem = asyncio.Semaphore(max(1, int(os.getenv("OPENAI_MAX_CONCURRENT", "1") or 1)))

    async def validate(self, candidate: dict, plan: Any, settings: dict) -> AIVerdict:
        if not bool_setting(settings, "openai_analysis_enabled", False):
            return AIVerdict(ok=True, approved=True, reason="AI disabled", model=active_model(settings), mode=active_strength(settings))
        key = openai_key(settings)
        model = active_model(settings)
        system, prompt, max_tokens, strength = build_prompt(candidate, plan, settings)
        if not key:
            if bool_setting(settings, "openai_fail_open", False):
                return AIVerdict(ok=False, approved=True, error="OpenAI key missing; fail-open", model=model, mode=strength)
            return AIVerdict(ok=False, approved=False, error="OpenAI API key missing", model=model, mode=strength)
        timeout_sec = float(settings.get("openai_timeout_sec", os.getenv("OPENAI_TIMEOUT_SEC", "12")) or 12)
        headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
        async with self._sem:
            try:
                timeout = aiohttp.ClientTimeout(total=timeout_sec)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    body = {"model": model, "input": prompt, "instructions": system, "max_output_tokens": max_tokens}
                    if str(model).lower().startswith(("gpt-5", "o1", "o3", "o4")):
                        body["reasoning"] = {"effort": "low" if strength == "weak" else "medium" if strength == "medium" else "high"}
                    async with session.post(OPENAI_RESPONSES_URL, headers=headers, json=body) as r:
                        txt = await r.text()
                        if r.status == 200:
                            try:
                                parsed = _extract_text(json.loads(txt))
                            except Exception:
                                parsed = txt
                            verdict = enforce_confidence_floor(parse_verdict(parsed, model, strength), strength)
                            if verdict.ok:
                                return verdict
                        responses_error = txt[:400]
                    chat_body = {"model": model, "messages": [{"role": "system", "content": system}, {"role": "user", "content": prompt}]}
                    if str(model).lower().startswith(("gpt-5", "o1", "o3", "o4")):
                        chat_body["max_completion_tokens"] = max_tokens
                    else:
                        chat_body["temperature"] = 0.1
                        chat_body["max_tokens"] = max_tokens
                    async with session.post(OPENAI_CHAT_URL, headers=headers, json=chat_body) as r2:
                        txt2 = await r2.text()
                        if r2.status == 200:
                            try:
                                parsed2 = _extract_text(json.loads(txt2))
                            except Exception:
                                parsed2 = txt2
                            return enforce_confidence_floor(parse_verdict(parsed2, model, strength), strength)
                        return AIVerdict(ok=False, approved=False, error=f"OpenAI error responses={responses_error}; chat={r2.status}: {txt2[:300]}", model=model, mode=strength)
            except Exception as e:
                if bool_setting(settings, "openai_fail_open", False):
                    return AIVerdict(ok=False, approved=True, error=f"OpenAI unavailable fail-open: {e}", model=model, mode=strength)
                return AIVerdict(ok=False, approved=False, error=f"OpenAI unavailable: {e}", model=model, mode=strength)
