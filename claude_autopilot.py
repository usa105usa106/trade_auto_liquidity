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


CLAUDE_SONNET_46 = "claude-sonnet-4-6"
CLAUDE_OPUS_48 = "claude-opus-4-8"
CLAUDE_ALLOWED_MODELS = {CLAUDE_SONNET_46, CLAUDE_OPUS_48}
CLAUDE_API_URL = os.getenv("ANTHROPIC_API_URL", "https://api.anthropic.com/v1/messages")
CLAUDE_API_VERSION = os.getenv("ANTHROPIC_VERSION", "2023-06-01")

MSK = timezone(timedelta(hours=3))


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
            "SYSTEM ORDER FOR CLAUDE AUTOPILOT:\n"
            "Follow task.txt as the highest-priority trading instruction. "
            "Analyze in this order: task.txt -> manifest.json -> log.txt -> BTC/ETH charts -> candidate charts. "
            "Return ONLY the clean bot-ready setup.txt v1.6 JSON content. "
            "No Markdown, no code fences, no explanations, no comments before or after JSON. "
            "The bot will save your response as setup-HHMM_DDMM.txt automatically."
        ))
        content.append(_content_text("=== task.txt ===\n" + task[:120_000]))
        content.append(_content_text("=== manifest.json ===\n" + manifest_text[:80_000]))
        content.append(_content_text("=== log.txt ===\n" + log_text[:180_000]))
        content.append(_content_text("=== CHARTS START ===\nCharts are supplied in deterministic order. First BTC/ETH context, then each selected symbol 4H -> 1H -> 15m."))
        for n in image_names:
            raw = zf.read(n)
            media_type = mimetypes.guess_type(n)[0] or "image/png"
            if media_type == "image/jpg":
                media_type = "image/jpeg"
            content.append(_content_text(f"CHART_FILE: {Path(n).name}"))
            content.append(_content_image(media_type, base64.b64encode(raw).decode("ascii")))
        content.append(_content_text(
            "FINAL REMINDER: Return only valid JSON for setup.txt. "
            "setup_version must be 1.6. Use exactly up to 3 trades. "
            "All prices must be normal decimal numbers, never scientific notation."
        ))
        meta = {
            "zip_path": str(zip_path),
            "task_len": len(task),
            "log_len": len(log_text),
            "image_count": len(image_names),
            "image_names": [Path(n).name for n in image_names],
            "selected_symbols": selected,
        }
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
    payload: dict[str, Any] = {
        "model": model,
        "max_tokens": int(max_tokens or 6000),
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
    if progress:
        await progress(f"отправляю в {claude_model_label(model)}...")
    timeout = aiohttp.ClientTimeout(total=float(timeout_sec))
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(CLAUDE_API_URL, headers=headers, json=payload) as resp:
            body = await resp.text()
            http_status = int(resp.status)
            if resp.status >= 400:
                raise RuntimeError(f"Claude API HTTP {resp.status}: {body[:1200]}")
            data = json.loads(body)
    parts = []
    for item in data.get("content") or []:
        if isinstance(item, dict) and item.get("type") == "text":
            parts.append(str(item.get("text") or ""))
    text = "\n".join(parts).strip()
    if not text:
        raise RuntimeError("Claude returned empty text")
    meta["model"] = model
    meta["usage"] = data.get("usage") or {}
    meta["response_id"] = data.get("id")
    meta["http_status"] = http_status
    meta["response_body_preview"] = body[:12000]
    meta["content_block_count"] = len(data.get("content") or [])
    return text, meta


def save_claude_setup_text(text: str, out_dir: str | Path | None = None, stamp: str | None = None) -> str:
    out_dir = Path(out_dir or os.getenv("CLAUDE_SETUP_DIR", os.getenv("CHATGPT_LOG_DIR", "/tmp")))
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / safe_setup_filename(stamp)
    path.write_text(text.strip() + "\n", encoding="utf-8")
    return str(path)
