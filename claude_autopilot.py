from __future__ import annotations

import asyncio
import base64
import json
import mimetypes
import os
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Awaitable

import aiohttp

from claude_runtime_logger import claude_log_event


CLAUDE_SONNET_46 = "claude-sonnet-4-6"
CLAUDE_OPUS_48 = "claude-opus-4-8"
CLAUDE_ALLOWED_MODELS = {CLAUDE_SONNET_46, CLAUDE_OPUS_48}
CLAUDE_API_URL = os.getenv("ANTHROPIC_API_URL", "https://api.anthropic.com/v1/messages")
CLAUDE_API_VERSION = os.getenv("ANTHROPIC_VERSION", "2023-06-01")

MSK = timezone(timedelta(hours=3))


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or default)
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or default)
    except Exception:
        return default


def _claude_pricing_per_mtok(model: str) -> dict[str, float]:
    """Return approximate USD per 1M tokens for log-only cost estimates.

    Can be overridden by env without changing trading logic:
    CLAUDE_INPUT_USD_PER_MTOK, CLAUDE_OUTPUT_USD_PER_MTOK,
    CLAUDE_CACHE_CREATE_USD_PER_MTOK, CLAUDE_CACHE_READ_USD_PER_MTOK.
    """
    model = normalize_claude_model(model)
    if model == CLAUDE_OPUS_48:
        defaults = {"input": 15.0, "output": 75.0, "cache_create": 18.75, "cache_read": 1.5}
    else:
        defaults = {"input": 3.0, "output": 15.0, "cache_create": 3.75, "cache_read": 0.3}
    return {
        "input": _safe_float(os.getenv("CLAUDE_INPUT_USD_PER_MTOK"), defaults["input"]),
        "output": _safe_float(os.getenv("CLAUDE_OUTPUT_USD_PER_MTOK"), defaults["output"]),
        "cache_create": _safe_float(os.getenv("CLAUDE_CACHE_CREATE_USD_PER_MTOK"), defaults["cache_create"]),
        "cache_read": _safe_float(os.getenv("CLAUDE_CACHE_READ_USD_PER_MTOK"), defaults["cache_read"]),
    }


def estimate_claude_cost_usd(model: str, usage: dict[str, Any] | None) -> dict[str, Any]:
    usage = usage or {}
    rates = _claude_pricing_per_mtok(model)
    input_tokens = _safe_int(usage.get("input_tokens"))
    output_tokens = _safe_int(usage.get("output_tokens"))
    cache_create = _safe_int(usage.get("cache_creation_input_tokens"))
    cache_read = _safe_int(usage.get("cache_read_input_tokens"))
    input_usd = input_tokens / 1_000_000 * rates["input"]
    output_usd = output_tokens / 1_000_000 * rates["output"]
    cache_create_usd = cache_create / 1_000_000 * rates["cache_create"]
    cache_read_usd = cache_read / 1_000_000 * rates["cache_read"]
    total = input_usd + output_usd + cache_create_usd + cache_read_usd
    return {
        "model": normalize_claude_model(model),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_creation_input_tokens": cache_create,
        "cache_read_input_tokens": cache_read,
        "rates_usd_per_mtok": rates,
        "input_usd": round(input_usd, 6),
        "output_usd": round(output_usd, 6),
        "cache_creation_usd": round(cache_create_usd, 6),
        "cache_read_usd": round(cache_read_usd, 6),
        "total_usd_estimate": round(total, 6),
        "note": "approximate log estimate from API usage tokens; billing console is source of truth",
    }


def msk_stamp() -> str:
    return datetime.now(MSK).strftime("%H%M_%d%m")


def normalize_claude_model(model: str | None) -> str:
    raw = str(model or "").strip()
    if raw in CLAUDE_ALLOWED_MODELS:
        return raw
    aliases = {
        "sonnet": CLAUDE_SONNET_46,
        "sonnet4.6": CLAUDE_SONNET_46,
        "sonnet-4.6": CLAUDE_SONNET_46,
        "claude-sonnet": CLAUDE_SONNET_46,
        "opus": CLAUDE_OPUS_48,
        "opus4.8": CLAUDE_OPUS_48,
        "opus-4.8": CLAUDE_OPUS_48,
        "claude-opus": CLAUDE_OPUS_48,
    }
    return aliases.get(raw.lower(), CLAUDE_SONNET_46)


def claude_model_label(model: str | None) -> str:
    m = normalize_claude_model(model)
    if m == CLAUDE_OPUS_48:
        return "Claude Opus 4.8"
    return "Claude Sonnet 4.6"


def schedule_label(value: str | None) -> str:
    v = str(value or "off").lower()
    return {
        "off": "выкл",
        "4h": "4H свеча +1м МСК",
        "1h": "1H свеча +1м МСК",
        "2h": "каждые 2 часа",
    }.get(v, v)


def next_schedule_run(schedule: str | None, last_ts: float | None = None, now: datetime | None = None) -> datetime | None:
    schedule = str(schedule or "off").lower()
    now = now or datetime.now(MSK)
    if schedule == "off":
        return None
    base = now.replace(second=0, microsecond=0)
    if schedule == "1h":
        candidate = base.replace(minute=1)
        if candidate <= now:
            candidate = candidate + timedelta(hours=1)
        return candidate
    if schedule == "4h":
        # MEXC/crypto 4H candle closes in MSK at 03/07/11/15/19/23; run +1m.
        close_hours = [3, 7, 11, 15, 19, 23]
        for day_add in (0, 1):
            d = (now + timedelta(days=day_add)).date()
            for h in close_hours:
                candidate = datetime(d.year, d.month, d.day, h, 1, tzinfo=MSK)
                if candidate > now:
                    return candidate
        return now + timedelta(hours=4)
    if schedule == "2h":
        if last_ts:
            candidate = datetime.fromtimestamp(float(last_ts), tz=MSK) + timedelta(hours=2)
            if candidate > now:
                return candidate.replace(second=0, microsecond=0)
        return (now + timedelta(hours=2)).replace(second=0, microsecond=0)
    return None


def schedule_due(schedule: str | None, last_ts: float | None = None, now: datetime | None = None) -> bool:
    schedule = str(schedule or "off").lower()
    if schedule == "off":
        return False
    now = now or datetime.now(MSK)
    last_ts = float(last_ts or 0)
    # Prevent duplicate runs inside the same minute/window.
    if last_ts and (time.time() - last_ts) < 55:
        return False
    if schedule == "1h":
        return now.minute == 1 and now.second < 40
    if schedule == "4h":
        return now.hour in {3, 7, 11, 15, 19, 23} and now.minute == 1 and now.second < 40
    if schedule == "2h":
        return not last_ts or (time.time() - last_ts) >= 7200
    return False


def safe_setup_filename(stamp: str | None = None) -> str:
    return f"setup-{stamp or msk_stamp()}.txt"


def _read_zip_text(zf: zipfile.ZipFile, name: str, default: str = "") -> str:
    try:
        return zf.read(name).decode("utf-8-sig", errors="replace")
    except KeyError:
        return default


def _symbol_aliases(sym: str) -> set[str]:
    raw = str(sym or "").strip().lower()
    aliases = {raw}
    if raw.endswith("_usdt"):
        aliases.add(raw[:-5])
    aliases.add(raw.replace("_", ""))
    aliases.add(raw.replace("_usdt", ""))
    return {x for x in aliases if x}


def _chart_sort_key(name: str, selected: list[str]) -> tuple[int, int, str]:
    lower = name.lower()
    stem = Path(name).stem.lower()
    base = Path(name).name

    # Context charts must always go first. Current pack filenames are usually
    # btc_4h.png / eth_1h.png, not btc_usdt_4h.png, so match both forms.
    if re.search(r"(^|[_\-])btc($|[_\-])", stem) or "btcusdt" in lower or "btc_usdt" in lower:
        sym_rank = -2
    elif re.search(r"(^|[_\-])eth($|[_\-])", stem) or "ethusdt" in lower or "eth_usdt" in lower:
        sym_rank = -1
    else:
        sym_rank = 999
        for i, sym in enumerate(selected):
            for alias in _symbol_aliases(sym):
                if re.search(rf"(^|[_\-]){re.escape(alias)}($|[_\-])", stem) or alias in stem.replace("_", ""):
                    sym_rank = i
                    break
            if sym_rank == i:
                break

    if re.search(r"(^|[_\-])4h($|[_\-])", stem):
        tf_rank = 0
    elif re.search(r"(^|[_\-])1h($|[_\-])", stem):
        tf_rank = 1
    elif "15m" in lower or "15min" in lower:
        tf_rank = 2
    else:
        tf_rank = 9
    return (sym_rank, tf_rank, base)


def _content_text(text: str) -> dict[str, str]:
    return {"type": "text", "text": text}


def _content_image(media_type: str, b64: str) -> dict[str, Any]:
    return {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": b64}}


async def build_claude_messages_from_scan_pack(zip_path: str | Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Turn one ChatGPT scan ZIP into a structured Claude multimodal request.

    External interface stays one ZIP file; internally we feed Claude the same
    contents in a deterministic order: task -> manifest -> log -> BTC/ETH charts
    -> selected candidate charts. This prevents random file order and avoids
    asking Claude to infer ZIP contents.
    """
    zip_path = Path(zip_path)
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        task = _read_zip_text(zf, "task.txt")
        manifest_text = _read_zip_text(zf, "manifest.json")
        log_text = _read_zip_text(zf, "log.txt")
        try:
            manifest = json.loads(manifest_text or "{}")
        except Exception:
            manifest = {}
        selected = [str(x) for x in (manifest.get("selected_symbols") or [])]
        image_names = [n for n in names if n.lower().endswith((".png", ".jpg", ".jpeg", ".webp"))]
        image_names = sorted(image_names, key=lambda n: _chart_sort_key(n, selected))

        content: list[dict[str, Any]] = []
        content.append(_content_text(
            "SYSTEM ORDER FOR CLAUDE AUTOPILOT — OUTPUT BUDGET IS STRICT:\n"
            "Follow task.txt as the highest-priority trading instruction. "
            "Analyze privately/silently in this order: task.txt -> manifest.json -> log.txt -> BTC/ETH charts -> candidate charts. "
            "Do NOT write reasoning, chain-of-thought, explanation, summary, markdown, code fences, comments, or preface. "
            "Return ONLY the clean bot-ready setup.txt v1.6 JSON object. "
            "The first character of your answer must be { and the last character must be }. "
            "The bot will save your response as setup-HHMM_DDMM.txt automatically."
        ))
        content.append(_content_text("=== task.txt ===\n" + task[:120_000]))
        content.append(_content_text("=== manifest.json ===\n" + manifest_text[:80_000]))
        content.append(_content_text("=== log.txt ===\n" + log_text[:180_000]))
        content.append(_content_text("=== CHARTS START ===\nCharts are supplied in deterministic order. First BTC/ETH context, then each selected symbol according to manifest timeframes. Use filenames/timeframes exactly as supplied."))
        for n in image_names:
            raw = zf.read(n)
            media_type = mimetypes.guess_type(n)[0] or "image/png"
            if media_type == "image/jpg":
                media_type = "image/jpeg"
            content.append(_content_text(f"CHART_FILE: {Path(n).name}"))
            content.append(_content_image(media_type, base64.b64encode(raw).decode("ascii")))
        content.append(_content_text(
            "FINAL OUTPUT CONTRACT: Return only valid JSON for setup.txt. "
            "No reasoning text. No analysis text. No explanations. No markdown. "
            "setup_version must be 1.6. Use exactly up to 3 trades. "
            "All prices must be normal decimal numbers, never scientific notation."
        ))
        image_sizes = {}
        for n in image_names:
            try:
                image_sizes[Path(n).name] = len(zf.read(n))
            except Exception:
                image_sizes[Path(n).name] = -1
        image_total_bytes = sum(v for v in image_sizes.values() if isinstance(v, int) and v > 0)
        image_timeframes = sorted({
            ("15m" if ("15m" in Path(n).stem.lower() or "15min" in Path(n).stem.lower()) else "1h" if re.search(r"(^|[_\-])1h($|[_\-])", Path(n).stem.lower()) else "4h" if re.search(r"(^|[_\-])4h($|[_\-])", Path(n).stem.lower()) else "unknown")
            for n in image_names
        })
        meta = {
            "zip_path": str(zip_path),
            "zip_size_bytes": zip_path.stat().st_size if zip_path.exists() else 0,
            "task_len": len(task),
            "log_len": len(log_text),
            "manifest_len": len(manifest_text),
            "text_total_chars": len(task) + len(log_text) + len(manifest_text),
            "task_sent_chars": min(len(task), 120_000),
            "log_sent_chars": min(len(log_text), 180_000),
            "manifest_sent_chars": min(len(manifest_text), 80_000),
            "image_count": len(image_names),
            "image_names": [Path(n).name for n in image_names],
            "image_timeframes": image_timeframes,
            "image_sizes": image_sizes,
            "image_total_bytes": image_total_bytes,
            "image_total_kb": round(image_total_bytes / 1024, 2),
            "selected_symbols": selected,
            "selected_count": len(selected),
            "content_blocks": len(content),
            "content_text_blocks": sum(1 for x in content if x.get("type") == "text"),
            "content_image_blocks": sum(1 for x in content if x.get("type") == "image"),
        }
        claude_log_event("claude_pack_prepared_for_api", **meta)
        return [{"role": "user", "content": content}], meta


async def call_claude_for_setup(
    zip_path: str | Path,
    *,
    api_key: str,
    model: str = CLAUDE_SONNET_46,
    max_tokens: int = 6000,
    temperature: float = 0.2,
    timeout_sec: int = 240,
    progress: Callable[[str], Awaitable[None]] | None = None,
) -> tuple[str, dict[str, Any]]:
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY missing")
    model = normalize_claude_model(model)
    if progress:
        await progress("готовлю task/log/manifest/графики для Claude...")
    messages, meta = await build_claude_messages_from_scan_pack(zip_path)
    system_prompt = (
        "You are a strict trading setup JSON generator. "
        "Think silently only. Never reveal reasoning or analysis. "
        "Output only one valid JSON object that matches setup.txt v1.6. "
        "No markdown, no code fences, no preface, no suffix. "
        "The response must start with { and end with }."
    )
    payload: dict[str, Any] = {
        "model": model,
        "max_tokens": int(max_tokens or 6000),
        "system": system_prompt,
        "messages": messages,
    }
    # Opus 4.8 rejects non-default sampling parameters on the current Anthropic API.
    # Sonnet accepts temperature and benefits from low randomness for strict JSON.
    if model != CLAUDE_OPUS_48:
        payload["temperature"] = float(temperature)
    headers = {
        "x-api-key": api_key,
        "anthropic-version": CLAUDE_API_VERSION,
        "content-type": "application/json",
    }
    payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    payload_bytes = len(payload_json.encode("utf-8"))
    meta["payload_bytes"] = payload_bytes
    meta["payload_mb"] = round(payload_bytes / 1024 / 1024, 3)
    meta["request_started_ts"] = datetime.now(timezone.utc).isoformat()
    if progress:
        await progress(f"отправляю в {claude_model_label(model)}...")
    claude_log_event(
        "claude_api_request_prepared",
        api_url=CLAUDE_API_URL,
        api_version=CLAUDE_API_VERSION,
        model=model,
        model_label=claude_model_label(model),
        max_tokens=max_tokens,
        output_contract="json_only_no_reasoning",
        temperature=(temperature if model != CLAUDE_OPUS_48 else "default_for_opus"),
        timeout_sec=timeout_sec,
        image_count=meta.get("image_count"),
        image_timeframes=meta.get("image_timeframes"),
        image_total_bytes=meta.get("image_total_bytes"),
        image_total_kb=meta.get("image_total_kb"),
        selected_symbols=meta.get("selected_symbols"),
        selected_count=meta.get("selected_count"),
        zip_path=meta.get("zip_path"),
        zip_size_bytes=meta.get("zip_size_bytes"),
        text_total_chars=meta.get("text_total_chars"),
        task_sent_chars=meta.get("task_sent_chars"),
        log_sent_chars=meta.get("log_sent_chars"),
        manifest_sent_chars=meta.get("manifest_sent_chars"),
        content_blocks=meta.get("content_blocks"),
        content_text_blocks=meta.get("content_text_blocks"),
        content_image_blocks=meta.get("content_image_blocks"),
        payload_bytes=payload_bytes,
        payload_mb=meta.get("payload_mb"),
    )
    claude_log_event("claude_api_http_start", model=model, api_url=CLAUDE_API_URL, timeout_sec=timeout_sec)
    timeout = aiohttp.ClientTimeout(total=float(timeout_sec))
    started = time.time()
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.post(CLAUDE_API_URL, headers=headers, json=payload) as resp:
                body = await resp.text()
                http_status = int(resp.status)
                elapsed_sec = round(time.time() - started, 3)
                response_headers = {
                    k: v for k, v in dict(resp.headers).items()
                    if k.lower().startswith("anthropic") or k.lower().startswith("x-ratelimit") or k.lower() in {"request-id", "retry-after", "content-type"}
                }
                claude_log_event(
                    "claude_api_http_response",
                    http_status=http_status,
                    elapsed_sec=elapsed_sec,
                    response_bytes=len(body.encode("utf-8")),
                    response_headers=response_headers,
                    body_preview=body[:20000],
                )
                if resp.status >= 400:
                    claude_log_event("claude_api_http_error_body", http_status=http_status, elapsed_sec=elapsed_sec, body_preview=body[:20000])
                    raise RuntimeError(f"Claude API HTTP {resp.status}: {body[:1200]}")
                data = json.loads(body)
        except Exception as e:
            claude_log_event("claude_api_http_error", error=repr(e), model=model, api_url=CLAUDE_API_URL, elapsed_sec=round(time.time() - started, 3))
            raise
    parts = []
    for item in data.get("content") or []:
        if isinstance(item, dict) and item.get("type") == "text":
            parts.append(str(item.get("text") or ""))
    text = "\n".join(parts).strip()
    if not text:
        claude_log_event("claude_api_empty_text", response_id=data.get("id"), usage=data.get("usage") or {})
        raise RuntimeError("Claude returned empty text")
    meta["model"] = model
    meta["usage"] = data.get("usage") or {}
    meta["cost_estimate"] = estimate_claude_cost_usd(model, meta["usage"])
    meta["response_id"] = data.get("id")
    meta["response_type"] = data.get("type")
    meta["stop_reason"] = data.get("stop_reason")
    meta["stop_sequence"] = data.get("stop_sequence")
    meta["http_status"] = http_status
    meta["response_body_preview"] = body[:20000]
    meta["content_block_count"] = len(data.get("content") or [])
    claude_log_event(
        "claude_usage",
        model=model,
        response_id=meta.get("response_id"),
        usage=meta.get("usage"),
        cost_estimate=meta.get("cost_estimate"),
        stop_reason=meta.get("stop_reason"),
    )
    claude_log_event(
        "claude_api_text_extracted",
        model=model,
        response_id=meta.get("response_id"),
        usage=meta.get("usage"),
        cost_estimate=meta.get("cost_estimate"),
        raw_len=len(text or ""),
        raw_preview=text[:4000],
        content_block_count=meta.get("content_block_count"),
    )
    return text, meta


def save_claude_setup_text(text: str, out_dir: str | Path | None = None, stamp: str | None = None) -> str:
    out_dir = Path(out_dir or os.getenv("CLAUDE_SETUP_DIR", os.getenv("CHATGPT_LOG_DIR", "/tmp")))
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / safe_setup_filename(stamp)
    path.write_text(text.strip() + "\n", encoding="utf-8")
    return str(path)
