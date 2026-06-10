import asyncio
import json
import math
import os
import re
import time
import zipfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

from models import TradePlan
from execution_engine import ExecutionEngine
from debug_log import log_event

CHATGPT_MODE_KEYS = {
    "quick_bounce_enabled": False,
    "impulse_dump_enabled": False,
    "orderflow_impulse_enabled": False,
    "cascade_hunter_enabled": False,
    "strongest_coin_enabled": False,
    "knife_reversal_enabled": False,
    "multi_strategy_enabled": False,
    "boost_enabled": False,
    "boost_parallel_scan_enabled": False,
    "ai_scalping_enabled": False,
    "btc_ai_autopilot_enabled": False,
    # ChatGPT Mode disables other ENTRY strategies, but live trading must stay ON
    # because setup.txt is meant to place real MEXC orders.
    "live_trading": True,
}

# Simple mandatory stop-risk corridor for ChatGPT-generated setup.txt.
# With the user's default 10% margin per trade and 10x leverage, the notional
# is approximately 100% of the deposit, so stop distance in % is also the
# estimated deposit risk in % for one trade.
CHATGPT_MIN_STOP_DISTANCE_PCT = 1.0
CHATGPT_MAX_STOP_DISTANCE_PCT = 5.0
CHATGPT_SETUP_VERSION = "1.6"
# ChatGPT Mode separates pending LIMIT slots from real open position slots.
# Old pending LIMITs are always cancelled when a new setup is imported.
# Up to 3 fresh pending limits can be placed, while up to 6 real ChatGPT
# positions may be open after previous limits have filled.
CHATGPT_MAX_PENDING_LIMITS = 3
CHATGPT_MAX_OPEN_POSITIONS = 6
# Backward-compatible names used in logs/older code paths.
CHATGPT_MAX_ACTIVE_TRADES = CHATGPT_MAX_PENDING_LIMITS
CHATGPT_MAX_TOTAL_SLOTS = CHATGPT_MAX_OPEN_POSITIONS
CHATGPT_LIMIT_TTL_MINUTES = 120
CHATGPT_MONITOR_INTERVAL_SEC = 30

# MEXC regional restrictions: tokenized stock/stock-index contracts can be
# blocked in the user's region and must never be scanned or traded in
# ChatGPT Setup Mode. Example: MSFTSTOCK_USDT / STXSTOCKUSDT.
CHATGPT_BLOCKED_SYMBOL_SUBSTRINGS = ("STOCK",)


def _symbol_compact(sym: Any) -> str:
    return str(sym or "").upper().replace("/", "").replace("_", "").replace(":", "")


def is_chatgpt_blocked_symbol(sym: Any) -> bool:
    compact = _symbol_compact(sym)
    return any(x in compact for x in CHATGPT_BLOCKED_SYMBOL_SUBSTRINGS)


def filter_chatgpt_symbols(symbols: list[str]) -> tuple[list[str], list[str]]:
    kept, blocked = [], []
    for sym in symbols or []:
        if is_chatgpt_blocked_symbol(sym):
            blocked.append(sym)
        else:
            kept.append(sym)
    return kept, blocked


def mexc_native_symbol(sym: Any) -> str:
    """Return clean MEXC futures symbol for logs/setup: BTC_USDT, ETH_USDT.

    Scanner may receive ccxt symbols like BTC/USDT:USDT.  ChatGPT mode
    intentionally writes only native MEXC contract ids to log.txt and setup.txt
    to avoid symbol-format confusion in the trading module.
    """
    raw = str(sym or "").strip().upper()
    if raw.endswith(":USDT"):
        raw = raw[:-5]
    raw = raw.replace("-", "_")
    if "/" in raw:
        base, quote = raw.split("/", 1)
        quote = quote.split(":", 1)[0] or "USDT"
        raw = f"{base}_{quote}"
    elif raw.endswith("USDT") and "_" not in raw and len(raw) > 4:
        raw = f"{raw[:-4]}_USDT"
    raw = raw.replace("__", "_").strip("_ /:-")
    return raw


def mexc_native_symbols(symbols: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for sym in symbols or []:
        native = mexc_native_symbol(sym)
        if not native or native in seen:
            continue
        seen.add(native)
        out.append(native)
    return out


# Optional display names are informational only. Trading must always use the
# exact MEXC native symbol from log.txt/manifest.json (for example NVDA_USDT),
# never a long human name like NVIDIA_USDT.
CHATGPT_SYMBOL_DISPLAY_NAMES = {
    "NVDA": "NVIDIA",
    "TSLA": "Tesla",
    "AAPL": "Apple",
    "MSFT": "Microsoft",
    "AMZN": "Amazon",
    "GOOGL": "Google",
    "META": "Meta",
    "XAU": "Gold",
    "XAUT": "Tether Gold",
    "GOLD": "Gold",
    "SILVER": "Silver",
    "COPPER": "Copper",
    "OIL": "Oil",
    "USOIL": "US Oil",
    "UKOIL": "UK Oil",
    "XPD": "Palladium",
}


def chatgpt_symbol_display_name(sym: Any) -> str:
    native = mexc_native_symbol(sym)
    base = native[:-5] if native.endswith("_USDT") else native.split("_", 1)[0]
    return CHATGPT_SYMBOL_DISPLAY_NAMES.get(base, base)


def _chatgpt_pack_allowed_symbols_from_manifest(manifest: Any) -> set[str]:
    if not isinstance(manifest, dict):
        return set()
    symbols: set[str] = set()
    for sym in manifest.get("selected_symbols") or []:
        native = mexc_native_symbol(sym)
        if native:
            symbols.add(native)
    # BTC/ETH are context charts and may be valid setup candidates too.
    for sym in ("BTC_USDT", "ETH_USDT"):
        symbols.add(sym)
    return symbols


async def build_chatgpt_runtime_manifest_from_mexc(storage, exchange_client, source: str = "accept_setup") -> dict:
    """Build a fast runtime symbol manifest from live MEXC futures markets.

    Used by the manual "Accept setup" button after redeploy or without a
    fresh ChatGPT scan pack. This is intentionally NOT a top-200 scan: no
    OHLCV, no scoring, no charts. It only loads tradable futures symbols so
    setup validation can check exact MEXC symbols without requiring Railway
    volume/persistent scan-pack files.
    """
    chatgpt_log_event("setup_runtime_manifest_build_start", source=source)
    try:
        ex = getattr(exchange_client, "exchange", None)
        markets = getattr(ex, "markets", None) if ex is not None else None
        if ex is not None and not markets:
            chatgpt_log_event("setup_runtime_manifest_load_markets_start", source=source)
            await asyncio.wait_for(ex.load_markets(), timeout=float(os.getenv("CHATGPT_RUNTIME_MANIFEST_LOAD_MARKETS_TIMEOUT", "12") or 12))
            chatgpt_log_event("setup_runtime_manifest_load_markets_done", source=source, markets=len(getattr(ex, "markets", {}) or {}))
    except Exception as e:
        chatgpt_log_event("setup_runtime_manifest_load_markets_error", source=source, error=repr(e))

    raw_symbols = []
    try:
        if hasattr(exchange_client, "futures_market_symbols"):
            raw_symbols = list(exchange_client.futures_market_symbols() or [])
    except Exception as e:
        chatgpt_log_event("setup_runtime_manifest_futures_symbols_error", source=source, error=repr(e))
        raw_symbols = []

    native_symbols = mexc_native_symbols(raw_symbols)
    # Keep only USDT contracts; block only symbols containing STOCK.
    native_symbols = [s for s in native_symbols if s.endswith("_USDT")]
    allowed, blocked = filter_chatgpt_symbols(native_symbols)
    # De-duplicate preserving order.
    selected_symbols = mexc_native_symbols(allowed)

    chatgpt_log_event(
        "setup_runtime_manifest_symbols_loaded",
        source=source,
        raw_count=len(raw_symbols),
        native_count=len(native_symbols),
        allowed_count=len(selected_symbols),
        blocked_count=len(blocked),
        blocked_sample=",".join(blocked[:20]),
        allowed_sample=",".join(selected_symbols[:30]),
    )
    if len(selected_symbols) < int(os.getenv("CHATGPT_RUNTIME_MANIFEST_MIN_SYMBOLS", "20") or 20):
        chatgpt_log_event("setup_runtime_manifest_incomplete", source=source, allowed_count=len(selected_symbols), sample=",".join(selected_symbols[:20]))
        raise ValueError(
            f"не смог быстро создать runtime manifest MEXC symbols: найдено только {len(selected_symbols)} symbols. "
            "Попробуй нажать «Принять setup» ещё раз или запусти ChatGPT Scan Mode."
        )

    manifest = {
        "pack_type": "CHATGPT_RUNTIME_SYMBOL_MANIFEST",
        "source": source,
        "created_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "bot_version": os.getenv("BOT_VERSION", "410_plus_full"),
        "symbol_guard_mode": "runtime_mexc_symbols",
        "selected_count": len(selected_symbols),
        "selected_symbols": selected_symbols,
        "blocked_symbol_substrings": list(CHATGPT_BLOCKED_SYMBOL_SUBSTRINGS),
        "note": "Fast manifest from live MEXC futures symbols; no scan/log/charts/score.",
    }
    await storage.set("chatgpt_last_scan_manifest", manifest, bump_revision=False)
    await storage.set("chatgpt_last_scan_manifest_source", source, bump_revision=False)
    chatgpt_log_event(
        "setup_runtime_manifest_saved",
        source=source,
        selected_count=len(selected_symbols),
        sample=",".join(selected_symbols[:30]),
    )
    return manifest


CHATGPT_RUNTIME_LOG_PATH = Path(os.getenv("CHATGPT_RUNTIME_LOG_PATH", "/tmp/chatgpt_mode_runtime.log"))
CHATGPT_RUNTIME_LOG_MAX_BYTES = int(os.getenv("CHATGPT_RUNTIME_LOG_MAX_BYTES", "3000000") or 3000000)
CHATGPT_RUNTIME_LOG_FIELD_MAX_CHARS = int(os.getenv("CHATGPT_RUNTIME_LOG_FIELD_MAX_CHARS", "30000") or 30000)


def chatgpt_runtime_log_path() -> str:
    CHATGPT_RUNTIME_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    return str(CHATGPT_RUNTIME_LOG_PATH)


def chatgpt_log_event(event: str, **fields) -> None:
    """Append a detailed, human-readable ChatGPT-mode runtime log line.

    This file is returned by /log_chatgpt, so errors in scan/setup execution can
    be diagnosed from Telegram without Railway shell access.
    """
    try:
        CHATGPT_RUNTIME_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        if CHATGPT_RUNTIME_LOG_PATH.exists() and CHATGPT_RUNTIME_LOG_PATH.stat().st_size > CHATGPT_RUNTIME_LOG_MAX_BYTES:
            rotated = CHATGPT_RUNTIME_LOG_PATH.with_suffix(CHATGPT_RUNTIME_LOG_PATH.suffix + ".1")
            try:
                if rotated.exists():
                    rotated.unlink()
                CHATGPT_RUNTIME_LOG_PATH.rename(rotated)
            except Exception:
                CHATGPT_RUNTIME_LOG_PATH.unlink(missing_ok=True)
        safe = {}
        for k, v in fields.items():
            try:
                txt = str(v)
            except Exception:
                txt = repr(v)
            safe[k] = txt[:CHATGPT_RUNTIME_LOG_FIELD_MAX_CHARS]
        line = json.dumps({"ts": _now_utc(), "event": event, **safe}, ensure_ascii=False)
        with CHATGPT_RUNTIME_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def tail_chatgpt_runtime_log(max_lines: int = 80) -> str:
    try:
        path = Path(chatgpt_runtime_log_path())
        if not path.exists():
            return "chatgpt runtime log is empty"
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join(lines[-max_lines:]) or "chatgpt runtime log is empty"
    except Exception as e:
        return f"failed to read chatgpt runtime log: {e}"


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _chatgpt_display_tz() -> timezone:
    # Telegram users read ChatGPT Mode status in local MSK time.
    # Keep runtime logs in UTC, but never show UTC in the live monitor card.
    try:
        hours = int(os.getenv("CHATGPT_DISPLAY_TZ_OFFSET_HOURS", "3"))
    except Exception:
        hours = 3
    return timezone(timedelta(hours=hours))


def _now_chatgpt_display() -> str:
    return datetime.now(_chatgpt_display_tz()).strftime("%Y-%m-%d %H:%M:%S МСК")


def _now_chatgpt_display_short() -> str:
    return datetime.now(_chatgpt_display_tz()).strftime("%H:%M МСК")


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v in (None, ""):
            return default
        if isinstance(v, str):
            v = v.strip().replace(",", ".")
            if v.endswith("%"):
                v = v[:-1].strip()
            if v == "":
                return default
        return float(v)
    except Exception:
        return default


def _truthy(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on", "да"}


def _fmt(v: Any, digits: int = 8) -> str:
    try:
        f = float(v)
        if abs(f) >= 100:
            return f"{f:.4f}".rstrip("0").rstrip(".")
        return f"{f:.{digits}f}".rstrip("0").rstrip(".")
    except Exception:
        return str(v)


def _is_mexc_rate_limit_error(e: Exception) -> bool:
    txt = repr(e).lower() + " " + str(e).lower()
    return "requests are too frequent" in txt or "code\":510" in txt or "code 510" in txt


async def _mexc_call(label: str, coro_factory, symbol: str = "", retries: int | None = None):
    """Small throttle/retry wrapper for MEXC public API during ChatGPT scan."""
    retries = int(os.getenv("CHATGPT_SCAN_RETRIES", "2") if retries is None else retries)
    base_delay = float(os.getenv("CHATGPT_SCAN_REQUEST_DELAY_SEC", "0.12") or 0.12)
    rate_sleep = float(os.getenv("CHATGPT_SCAN_RATE_LIMIT_SLEEP_SEC", "2.0") or 2.0)
    last_exc = None
    for attempt in range(retries + 1):
        if base_delay > 0:
            await asyncio.sleep(base_delay)
        try:
            return await coro_factory()
        except Exception as e:
            last_exc = e
            if _is_mexc_rate_limit_error(e) and attempt < retries:
                wait = rate_sleep * (attempt + 1)
                chatgpt_log_event("scan_rate_limit_retry", label=label, symbol=symbol, attempt=attempt + 1, sleep_sec=wait, error=repr(e))
                await asyncio.sleep(wait)
                continue
            raise
    raise last_exc


def _pct(a: float, b: float) -> float:
    return ((a - b) / b * 100.0) if b else 0.0


def _stop_distance_pct(entry: float, stop: float) -> float:
    return abs(float(entry) - float(stop)) / float(entry) * 100.0 if entry else 0.0




async def _fetch_chatgpt_current_price(exchange_client, symbol: str) -> float:
    """Return the best available current price for a setup safety gate.

    v0391: the setup entry safety check must not skip a good trade after one
    transient MEXC 510 rate-limit response.  We retry only the live ticker read
    with fixed backoff 2s -> 5s, then fail closed.  No cached price fallback is
    used here because stale prices can make TP2/SL safety checks misleading.
    """
    waits = [0.0, 2.0, 5.0]
    last_exc = None
    chatgpt_log_event("setup_price_check_start", symbol=symbol, attempts=len(waits), retry_schedule_sec="2,5", cache_fallback="off")
    for idx, wait in enumerate(waits, start=1):
        if wait > 0:
            chatgpt_log_event("setup_price_check_retry_sleep", symbol=symbol, attempt=idx, sleep_sec=wait)
            await asyncio.sleep(wait)
        try:
            chatgpt_log_event("setup_price_check_fetch_start", symbol=symbol, attempt=idx)
            ticker = await exchange_client.fetch_ticker(symbol)
            info = ticker.get("info") if isinstance(ticker, dict) else {}
            candidates = []
            if isinstance(ticker, dict):
                candidates += [
                    ticker.get("mark"),
                    ticker.get("markPrice"),
                    ticker.get("last"),
                    ticker.get("close"),
                    ticker.get("bid"),
                    ticker.get("ask"),
                ]
            if isinstance(info, dict):
                candidates += [
                    info.get("markPrice"),
                    info.get("fairPrice"),
                    info.get("indexPrice"),
                    info.get("lastPrice"),
                    info.get("last"),
                    info.get("close"),
                    info.get("bid1"),
                    info.get("ask1"),
                ]
            for raw in candidates:
                price = _safe_float(raw)
                if price > 0:
                    chatgpt_log_event("setup_price_check_ok", symbol=symbol, attempt=idx, current_price=price)
                    return price
            raise ValueError("ticker has no usable current price")
        except Exception as e:
            last_exc = e
            is_rate_limit = _is_mexc_rate_limit_error(e)
            chatgpt_log_event(
                "setup_price_check_fetch_error",
                symbol=symbol,
                attempt=idx,
                will_retry=bool(is_rate_limit and idx < len(waits)),
                rate_limit=bool(is_rate_limit),
                error=repr(e),
            )
            if is_rate_limit and idx < len(waits):
                continue
            break
    chatgpt_log_event("setup_price_check_failed", symbol=symbol, attempts=len(waits), error=repr(last_exc))
    raise last_exc if last_exc is not None else RuntimeError("price check failed")


def _chatgpt_stop_breached(direction: str, current_price: float, stop_price: float) -> bool:
    """True when market already moved beyond the setup SL.

    LONG is invalid if current <= stop.  SHORT is invalid if current >= stop.
    In this state placing the old LIMIT is unsafe because the setup risk model
    is already broken before entry.
    """
    d = str(direction or "").upper()
    if d == "LONG":
        return current_price <= stop_price
    if d == "SHORT":
        return current_price >= stop_price
    return False


def _chatgpt_tp2_already_touched(direction: str, current_price: float, tp2_price: float) -> bool:
    """True when the market already reached TP2 before entry placement.

    LONG is stale if current >= TP2. SHORT is stale if current <= TP2.
    TP1 touch alone is allowed: otherwise normal pullback setups are skipped too
    aggressively. TP2 touch means the setup has already played out enough that
    a fresh entry should wait for a new setup.
    """
    d = str(direction or "").upper()
    if tp2_price <= 0:
        return False
    if d == "LONG":
        return current_price >= tp2_price
    if d == "SHORT":
        return current_price <= tp2_price
    return False


def _chatgpt_tp1_already_touched(direction: str, current_price: float, tp1_price: float) -> bool:
    """Compatibility wrapper kept for older tests/imports.

    New ChatGPT Mode safety uses TP2, not TP1. This helper retains the old TP1
    comparison only for code that imports it directly; execution paths do not
    call it for setup-entry rejection anymore.
    """
    d = str(direction or "").upper()
    if tp1_price <= 0:
        return False
    if d == "LONG":
        return current_price >= tp1_price
    if d == "SHORT":
        return current_price <= tp1_price
    return False


def _closes(candles: list) -> list[float]:
    out = []
    for r in candles or []:
        try:
            out.append(float(r[4]))
        except Exception:
            pass
    return out


def _volumes(candles: list) -> list[float]:
    out = []
    for r in candles or []:
        try:
            out.append(float(r[5]))
        except Exception:
            pass
    return out


def _ma(values: list[float], n: int) -> float:
    if len(values) < n:
        return 0.0
    return sum(values[-n:]) / n


def _rsi(values: list[float], n: int = 14) -> float:
    if len(values) <= n:
        return 0.0
    gains, losses = [], []
    for i in range(-n, 0):
        d = values[i] - values[i - 1]
        gains.append(max(d, 0.0)); losses.append(max(-d, 0.0))
    ag = sum(gains) / n
    al = sum(losses) / n
    if al == 0:
        return 100.0 if ag > 0 else 50.0
    rs = ag / al
    return 100.0 - (100.0 / (1.0 + rs))


def _ema(values: list[float], n: int) -> float:
    if not values:
        return 0.0
    k = 2.0 / (n + 1.0)
    e = values[0]
    for x in values[1:]:
        e = x * k + e * (1.0 - k)
    return e


def _macd(values: list[float]) -> tuple[float, float, float]:
    if len(values) < 35:
        return 0.0, 0.0, 0.0
    macd_series = []
    for i in range(26, len(values) + 1):
        sub = values[:i]
        macd_series.append(_ema(sub, 12) - _ema(sub, 26))
    signal = _ema(macd_series, 9)
    macd = macd_series[-1]
    return macd, signal, macd - signal


def _orderbook_stats(ob: dict, price: float) -> dict:
    bids = ob.get("bids") or []
    asks = ob.get("asks") or []
    bid_qty = sum(_safe_float(x[1]) for x in bids[:20])
    ask_qty = sum(_safe_float(x[1]) for x in asks[:20])
    total = bid_qty + ask_qty
    bid_pct = (bid_qty / total * 100.0) if total else 0.0
    ask_pct = (ask_qty / total * 100.0) if total else 0.0
    big_bid = max(bids[:20], key=lambda x: _safe_float(x[1]), default=[0, 0])
    big_ask = max(asks[:20], key=lambda x: _safe_float(x[1]), default=[0, 0])
    spread = 0.0
    try:
        spread = (float(asks[0][0]) - float(bids[0][0])) / price * 100.0 if price and asks and bids else 0.0
    except Exception:
        pass
    return {"bid_pct": bid_pct, "ask_pct": ask_pct, "big_bid": big_bid, "big_ask": big_ask, "spread_pct": spread}


def _score_symbol(price: float, tf: dict, ob: dict) -> tuple[int, int, str]:
    long_score = 50
    short_score = 50
    notes = []
    for name, data in tf.items():
        ma7, ma25, ma99 = data.get("ma7", 0), data.get("ma25", 0), data.get("ma99", 0)
        rsi = data.get("rsi", 0)
        hist = data.get("macd_hist", 0)
        if price > ma7 > 0:
            long_score += 3; short_score -= 2
        else:
            short_score += 2
        if price > ma25 > 0:
            long_score += 4; short_score -= 3
        else:
            short_score += 3
        if ma7 > ma25 > 0:
            long_score += 3
        if ma7 < ma25 and ma25 > 0:
            short_score += 3
        if hist > 0:
            long_score += 3
        elif hist < 0:
            short_score += 3
        if 42 <= rsi <= 62:
            long_score += 1
        if rsi >= 70:
            short_score += 3; notes.append(f"{name} RSI high")
        if rsi <= 30:
            long_score += 3; notes.append(f"{name} RSI low")
    bid_pct = ob.get("bid_pct", 0)
    if bid_pct >= 60:
        long_score += 5
    if bid_pct <= 40:
        short_score += 5
    long_score = max(0, min(100, int(round(long_score))))
    short_score = max(0, min(100, int(round(short_score))))
    return long_score, short_score, "; ".join(notes[:3]) or "-"


async def disable_other_modes(storage) -> None:
    for k, v in CHATGPT_MODE_KEYS.items():
        try:
            await storage.set(k, v, bump_revision=False)
        except TypeError:
            await storage.set(k, v)
        except Exception:
            pass
    try:
        await storage.set("chatgpt_setup_mode", True, bump_revision=False)
        await storage.set("chatgpt_waiting_setup", True, bump_revision=False)
        await storage.set("settings_revision", int(_safe_float(await storage.get("settings_revision", 1), 1)) + 1, bump_revision=False)
    except Exception:
        pass




def _parse_chatgpt_log_candidates(log_text: str, limit: int = 15) -> list[dict]:
    """Pick strongest non-STOCK candidates only from the TOP-200 scan section.

    This intentionally uses the existing log scores instead of inventing a new
    strategy: candidates are sorted by max(LONG, SHORT), then by volume, then by
    the weaker opposite score.  STOCK contracts are excluded before selection.

    Important: BTC/ETH market-context blocks appear before the full scan and may
    have slightly different scores.  For ChatGPT Scan Pack we must select the
    top-15 strictly from the TOP-200 FULL SCAN section, so context blocks are
    stripped before parsing candidates.
    """
    out: list[dict] = []
    text = str(log_text or "")
    marker = "=== TOP-200 FULL SCAN ==="
    if marker in text:
        text = text.split(marker, 1)[1]
    text = re.split(r"\n\n(?:=== TASK ===|CHATGPT_SCAN_PACK_NOTE:)", text, maxsplit=1)[0]
    blocks = re.split(r"\n---\n", text)
    for block in blocks:
        m = re.search(r"^SYMBOL:\s*([A-Z0-9_:/.-]+)", block, re.M)
        if not m:
            continue
        symbol = mexc_native_symbol(m.group(1))
        if not symbol or is_chatgpt_blocked_symbol(symbol):
            continue
        sm = re.search(r"SCORES:\s*LONG\s*=\s*(\d+)\s*/\s*100\s+SHORT\s*=\s*(\d+)\s*/\s*100", block, re.I)
        if not sm:
            continue
        long_score = int(sm.group(1)); short_score = int(sm.group(2))
        vm = re.search(r"QUOTE_VOL:\s*([0-9.eE+-]+)", block)
        quote_vol = _safe_float(vm.group(1) if vm else 0)
        spm = re.search(r"spread=([0-9.]+)%", block)
        spread = _safe_float(spm.group(1) if spm else 0)
        strength = max(long_score, short_score)
        # Soft penalty for wide spread, without changing the user's score model.
        score_adj = strength - min(15.0, spread * 20.0)
        out.append({
            "symbol": symbol,
            "display_name": chatgpt_symbol_display_name(symbol),
            "long_score": long_score,
            "short_score": short_score,
            "strength": strength,
            "score_adj": score_adj,
            "quote_vol": quote_vol,
            "spread_pct": spread,
        })
    # de-dupe preserving best occurrence
    best: dict[str, dict] = {}
    for row in out:
        sym = row["symbol"]
        if sym not in best or (row["score_adj"], row["quote_vol"]) > (best[sym]["score_adj"], best[sym]["quote_vol"]):
            best[sym] = row
    rows = list(best.values())
    rows.sort(key=lambda r: (r["score_adj"], r["strength"], r["quote_vol"]), reverse=True)
    return rows[: int(limit)]


def _chatgpt_tf_to_exchange(tf: str) -> str:
    t = str(tf or "").lower().strip()
    if t in {"15min", "15m", "15"}:
        return "15m"
    if t in {"1h", "1hour", "60m"}:
        return "1h"
    if t in {"4h", "4hour", "240m"}:
        return "4h"
    return t or "4h"


def _chatgpt_tf_to_filename(tf: str) -> str:
    t = _chatgpt_tf_to_exchange(tf)
    return "15min" if t == "15m" else t


def _chatgpt_chart_filename(symbol: str, tf: str) -> str:
    base = mexc_native_symbol(symbol).replace("_USDT", "").replace("_", "").lower()
    return f"{base}_{_chatgpt_tf_to_filename(tf)}.png"


def _chatgpt_prepare_chart_df(candles: list, tail: int = 120):
    import pandas as pd
    df = pd.DataFrame(candles or [], columns=["ts", "open", "high", "low", "close", "volume"])
    if df.empty:
        return df
    df["dt"] = pd.to_datetime(df.ts.astype(float), unit="ms", errors="coerce")
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = df[c].astype(float)
    df["MA7"] = df.close.rolling(7).mean()
    df["MA25"] = df.close.rolling(25).mean()
    df["MA99"] = df.close.rolling(99).mean()
    ema12 = df.close.ewm(span=12, adjust=False).mean()
    ema26 = df.close.ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["Signal"] = df.MACD.ewm(span=9, adjust=False).mean()
    df["Hist"] = df.MACD - df.Signal
    # RSI(14) over full series, then crop.
    delta = df.close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss.replace(0, float("nan"))
    df["RSI"] = 100.0 - (100.0 / (1.0 + rs))
    df["RSI"] = df["RSI"].fillna(50.0)
    return df.tail(tail).reset_index(drop=True)


def _render_chatgpt_candidate_chart(symbol: str, timeframe: str, candles: list, meta: dict, out_path: Path) -> str:
    """Render raw candidate chart for ChatGPT Scan Mode, without ENTRY/SL/TP."""
    import numpy as np
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    from matplotlib.ticker import FuncFormatter

    df = _chatgpt_prepare_chart_df(candles, tail=120)
    if df.empty or len(df) < 30:
        raise ValueError(f"not enough candles for {symbol} {timeframe}: {len(df)}")

    bg = "#0f1722"; grid = "#263241"; txt = "#d5dde8"
    green = "#21c087"; red = "#f6465d"; orange = "#f59e0b"; blue = "#3b82f6"; purple = "#a855f7"
    fig = plt.figure(figsize=(12.8, 7.2), dpi=100)
    gs = fig.add_gridspec(3, 1, height_ratios=[5.2, 1.2, 1.5], hspace=0.06)
    ax = fig.add_subplot(gs[0]); av = fig.add_subplot(gs[1], sharex=ax); am = fig.add_subplot(gs[2], sharex=ax)
    fig.patch.set_facecolor(bg)
    for a in (ax, av, am):
        a.set_facecolor(bg); a.grid(True, color=grid, alpha=0.42, linewidth=0.8)
        a.tick_params(colors=txt, labelsize=9); a.yaxis.tick_right()
        for sp in a.spines.values(): sp.set_color(grid)

    x = np.arange(len(df)); w = 0.58
    min_body = max(float(df.close.iloc[-1]) * 0.00005, max((float(df.high.max())-float(df.low.min()))*0.001, 1e-12))
    for i, r in enumerate(df.itertuples()):
        col = green if r.close >= r.open else red
        ax.vlines(i, r.low, r.high, color=col, linewidth=1.0, alpha=0.95)
        body_low = min(r.open, r.close); body_h = max(abs(r.close - r.open), min_body)
        ax.add_patch(Rectangle((i - w/2, body_low), w, body_h, facecolor=col, edgecolor=col, linewidth=0.55))

    ax.plot(x, df.MA7, color=blue, linewidth=1.25, label=f"MA7 {df.MA7.iloc[-1]:.8g}")
    ax.plot(x, df.MA25, color=orange, linewidth=1.25, label=f"MA25 {df.MA25.iloc[-1]:.8g}")
    if not np.isnan(df.MA99.iloc[-1]):
        ax.plot(x, df.MA99, color=purple, linewidth=1.35, label=f"MA99 {df.MA99.iloc[-1]:.8g}")

    last = _safe_float(meta.get("last_price"), float(df.close.iloc[-1]))
    all_price_levels = [float(df.low.min()), float(df.high.max()), last]
    high24 = _safe_float(meta.get("high_24h")); low24 = _safe_float(meta.get("low_24h"))
    if high24 > 0: all_price_levels.append(high24)
    if low24 > 0: all_price_levels.append(low24)
    ymin, ymax = min(all_price_levels), max(all_price_levels)
    pad = max((ymax - ymin) * 0.12, max(last * 0.003, 1e-12))
    ax.set_ylim(ymin - pad, ymax + pad); ax.set_xlim(-1, len(df) + 12)
    ax.axhline(last, color=txt, linestyle=":", linewidth=1.05, alpha=0.75)
    ax.text(len(df)+0.4, last, f"LAST {last:.8g}", color=txt, va="center", fontsize=9,
            bbox=dict(boxstyle="round,pad=0.25", facecolor="#111827", edgecolor=txt, alpha=0.75))

    lookback = min(len(df), 6 if _chatgpt_tf_to_exchange(timeframe) == "4h" else 24)
    if lookback > 0:
        df24 = df.iloc[-lookback:].copy()
        hi_idx = int(df24["high"].astype(float).idxmax()); lo_idx = int(df24["low"].astype(float).idxmin())
        hi = float(df.at[hi_idx, "high"]); lo = float(df.at[lo_idx, "low"])
        ax.axvspan(len(df)-lookback-0.5, len(df)-0.5, color="#94a3b8", alpha=0.045, zorder=0)
        ax.scatter([hi_idx], [hi], marker="^", s=76, color="#f8fafc", edgecolor="#0b111c", linewidth=0.8, zorder=8)
        ax.scatter([lo_idx], [lo], marker="v", s=76, color="#f8fafc", edgecolor="#0b111c", linewidth=0.8, zorder=8)
        ax.annotate(f"▲ HIGH {hi:.8g}", xy=(hi_idx, hi), xytext=(0, 14), textcoords="offset points", color="#f8fafc", ha="center", fontsize=8,
                    bbox=dict(boxstyle="round,pad=0.16", facecolor="#0b111c", edgecolor="#94a3b8", alpha=0.82))
        ax.annotate(f"▼ LOW {lo:.8g}", xy=(lo_idx, lo), xytext=(0, -18), textcoords="offset points", color="#f8fafc", ha="center", va="top", fontsize=8,
                    bbox=dict(boxstyle="round,pad=0.16", facecolor="#0b111c", edgecolor="#94a3b8", alpha=0.82))

    rsi = float(df.RSI.iloc[-1]) if "RSI" in df else 0.0
    long_score = meta.get("long_score", "-"); short_score = meta.get("short_score", "-")
    ax.set_title(f"{mexc_native_symbol(symbol)} · MEXC Futures · {_chatgpt_tf_to_exchange(timeframe).upper()} · Last {last:.8g} · RSI {rsi:.1f} · LONG {long_score} · SHORT {short_score}",
                 color=txt, loc="left", fontsize=12.5, fontweight="bold")
    ax.legend(loc="upper left", frameon=False, labelcolor=txt, fontsize=8)

    cols = [green if c >= o else red for o, c in zip(df.open, df.close)]
    av.bar(x, df.volume, color=cols, alpha=0.62, width=w)
    def _compact_volume_formatter(value, _pos=None):
        value = float(value or 0); sign = "-" if value < 0 else ""; value = abs(value)
        if value >= 1_000_000_000: return f"{sign}{value/1_000_000_000:.1f}B"
        if value >= 1_000_000: return f"{sign}{value/1_000_000:.0f}M"
        if value >= 1_000: return f"{sign}{value/1_000:.0f}K"
        return f"{sign}{value:.0f}"
    av.yaxis.set_major_formatter(FuncFormatter(_compact_volume_formatter))
    vol24 = _safe_float(meta.get("quote_volume"))
    vol_ratio = _safe_float(meta.get("vol_ratio"))
    av.text(0.01, 0.86, f"MEXC VOL 24H {_compact_volume_formatter(vol24)} | ratio {vol_ratio:.2f}x", transform=av.transAxes,
            color=txt, fontsize=9, fontweight="bold", bbox=dict(boxstyle="round,pad=0.20", facecolor="#111827", edgecolor=grid, alpha=0.76), va="top", ha="left")

    hcols = [green if h >= 0 else red for h in df.Hist]
    am.bar(x, df.Hist, color=hcols, alpha=0.72, width=w)
    am.plot(x, df.MACD, color=blue, linewidth=1.15, label="MACD")
    am.plot(x, df.Signal, color=orange, linewidth=1.15, label="Signal")
    am.axhline(0, color=txt, alpha=0.45, linewidth=0.8)
    am.legend(loc="upper left", frameon=False, labelcolor=txt, fontsize=8)
    step = max(10, len(df)//6); ticks = list(range(0, len(df), step))
    am.set_xticks(ticks); am.set_xticklabels([df.dt.iloc[i].strftime("%m-%d %H:%M") for i in ticks], color=txt)
    plt.setp(ax.get_xticklabels(), visible=False); plt.setp(av.get_xticklabels(), visible=False)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, facecolor=fig.get_facecolor(), dpi=100)
    plt.close(fig)
    return str(out_path)


CHATGPT_SCAN_TASK_TEXT = """CHATGPT_TASK

1. Прочитай log.txt, manifest.json и все PNG-графики из папки screenshots.
2. Используй графики как raw/candidate charts. На них не должно быть заранее заданных ENTRY/SL/TP.
3. Сначала оцени BTC/ETH market context по btc_4h/btc_1h/eth_4h/eth_1h.
4. Перед анализом графиков перепроверь по log.txt, насколько правильно бот отобрал top-15 монет.
5. Если из твоего независимого отбора по full top-200 log совпадает хотя бы top-10 с selected_symbols из manifest.json/графиками, доверяй этому пакету скринов и переходи к анализу по графикам.
6. Если совпало меньше 10 монет из 15, не делай setup сразу. Верни список недостающих/лучших монет и попроси новый pack.
7. Проверь manifest.json: expected_png_count, actual_png_count, missing_charts, generation_errors.
8. Если missing_charts/generation_errors пустые или не мешают анализу — продолжай.
9. Если у выбранной для сделки монеты нет полного набора 4h/1h/15min или график нечитаемый — не выбирай эту монету для setup.
10. Запрещены только instruments/symbols, где в symbol/name есть substring STOCK в любом регистре. Пример запрещённого: JPMSTOCK_USDT.
11. Акции/stock-токены без substring STOCK, металлы, commodities и индексы разрешены. Не блокируй NVDA_USDT, TSLA_USDT, AAPL_USDT, XAU, XAUT, GOLD, SILVER, COPPER, OIL, XPD и похожие symbols только из-за категории инструмента.
12. SYMBOL RULE: используй только exact symbols из log.txt / manifest.json. Не переименовывай биржевые тикеры в длинные названия. Если manifest говорит NVDA_USDT, в setup должен быть NVDA_USDT, не NVIDIA_USDT. display_name является только информацией и не должен использоваться как order symbol.
13. Цель режима — выбрать 3 лучшие сделки. Не душить количество сетапов: сделки лимитные, живут около 2 часов, поэтому старайся заполнить 3 слота качественными идеями.
14. Сохраняй цель 3 сделки. Если рынок сложный, не сокращай количество автоматически, а выбирай более аккуратные входы от уровней/ретестов; NO_TRADE только если реально нет ни одного приемлемого setup.
15. Если сделки есть — верни setup-HHMM_DDMM.txt версии 1.6.

CRITICAL OUTPUT RULES:
- Финальный setup нужно вернуть только прикреплённым .txt файлом.
- Нельзя писать setup обычным текстом в сообщении.
- Нельзя писать setup в Markdown.
- Нельзя писать setup в json/code block.
- Нельзя использовать старый plain-text формат VERSION=1.6 / [TRADE_1].
- Нужен только файл с чистым JSON object.
- Файл должен называться строго: setup-HHMM_DDMM.txt.
- PRICE FORMAT RULE: все price values (entry, stop_loss, take_profits.price) пиши только обычным десятичным числом, без scientific notation / экспоненциальной записи. Пример: 0.0000242, не 2.42e-05.
- Если не можешь создать файл, прямо напиши: не могу создать файл, и НЕ выдавай setup текстом.

SETUP FORMAT STRICT:
- Только чистый JSON object.
- Файл должен начинаться с { и заканчиваться }.
- Все сделки должны быть внутри массива trades.
- Не использовать TRADE_1 / TRADE_2 / TRADE_3.
- entry — только одно число.
- stop_loss — только одно число.
- take_profits — массив из 3 объектов price/size_percent.

REQUIRED TOP LEVEL JSON:
{
  "setup_version": "1.6",
  "mode": "AUTO_OPEN",
  "exchange": "MEXC_FUTURES",
  "margin_mode": "ISOLATED",
  "default_margin_percent_per_trade": 10,
  "default_leverage": 10,
  "verdict": "TRADE" или "NO_TRADE",
  "blocked_symbol_substrings": ["STOCK"],
  "symbol_format": "MEXC_NATIVE_UNDERSCORE",
  "trades": []
}

TRADE OBJECT FORMAT:
{
  "symbol": "BTC_USDT",
  "direction": "LONG или SHORT",
  "order_type": "LIMIT или MARKET",
  "entry": 0.0,
  "stop_loss": 0.0,
  "take_profits": [
    {"price": 0.0, "size_percent": 35},
    {"price": 0.0, "size_percent": 35},
    {"price": 0.0, "size_percent": "REMAINDER"}
  ],
  "cancel_if_not_filled_minutes": 120,
  "cancel_if_tp2_before_entry": true,
  "invalidation": "Краткая отмена идеи.",
  "comment": "Краткая причина сделки.",
  "risk": {
    "stop_distance_percent": 0.0,
    "estimated_deposit_risk_percent": 0.0
  }
}

MARKET PHASE / ANTI-STOP RULES:
- Перед выбором 3 сделок сначала обязательно классифицируй BTC/ETH по 1H и 4H в одну из фаз:
  1) TRENDING_DOWN / трендовое падение продолжается — структура ещё bearish, откаты слабые, продавец контролирует рынок. SHORT разрешён, но предпочтительно от отката/ретеста выше.
  2) LATE_DUMP / поздний слив — уже прошли длинные красные свечи, цена около локальных low/поддержки, движение выглядит перепроданным, есть замедление или первые выкупы. Fresh SHORT у дна запрещён; ищи retest SHORT выше или LONG от поддержки с реакцией.
  3) RELIEF_BOUNCE / отскок после падения — рынок отскакивает после слива. Не догоняй LONG в середине отскока; ищи LONG от отката в поддержку или SHORT от сильного сопротивления.
  4) CHOP_RANGE / грязная пила или диапазон — нет чистого направления, цена ходит внутри диапазона. Ищи 3 лучшие LIMIT-идеи от границ: LONG от поддержки, SHORT от сопротивления; не входи посередине.
  5) TRENDING_UP / трендовый рост продолжается — структура bullish, откаты выкупаются, покупатель контролирует рынок. LONG разрешён, но предпочтительно от отката/ретеста поддержки.
  6) LATE_PUMP / поздний рост — уже прошли длинные зелёные свечи, цена около локальных high/сопротивления, движение выглядит перегретым, есть замедление или отказ. Fresh LONG на хаях запрещён; ищи retest LONG ниже или SHORT от сопротивления с реакцией.
- 3 сделки остаются целевым форматом режима. Не переходи автоматически в 1-2 сделки только потому, что рынок сложный; лучше сделай входы аккуратнее.
- Не давай SHORT просто потому, что весь рынок красный. Сначала пойми: падение продолжается или это уже поздняя фаза у поддержки.
- Не шорти у самого дна после сильного пролива. Если BTC/ETH уже сильно пролились, цена около локальных low/поддержки, видны замедление/выкуп/перепроданность — fresh SHORT внизу плохой.
- После сильного падения SHORT предпочтительно брать от отката/ретеста выше в сопротивление, а не после длинной красной свечи и не на пробое локального low внизу.
- Если цена пришла к сильной поддержке и есть реакция: выкуп, замедление падения, удержание уровня, reclaim или монета держится сильнее BTC/ETH — LONG получает приоритет.
- Не давай LONG просто потому, что весь рынок зелёный. Сначала пойми: рост продолжается или это поздняя фаза у сопротивления.
- Не лонгуй на самом хае после сильного пампа. Если цена около сильного сопротивления и есть отказ/слабость/замедление роста — SHORT получает приоритет.
- В грязной пиле/диапазоне не входи посередине диапазона. Ищи 3 лучшие идеи от границ: LONG от поддержки, SHORT от сопротивления.
- Сделка от уровня имеет приоритет только если есть реакция от уровня. Простого касания поддержки/сопротивления недостаточно.
- MARKET-ордера не менять и не запрещать: оставь как было. MARKET можно использовать для сильных A+ setup, если это реально нужно, чтобы не упустить движение. Эти правила не должны ломать MARKET-логику.

MARKET MAKER / CHESS THINKING RULE:
- Перед финальным выбором учитывай, что рынок может заманивать толпу в очевидное направление: fake breakout, fake breakdown, stop hunt, sweep liquidity, резкий вынос за high/low с возвратом обратно.
- Не выбирай сделку только потому, что направление выглядит слишком очевидным. Если всем визуально хочется SHORT, проверь, не находится ли цена у поддержки/после выноса вниз, где маркетмейкер может собирать ликвидность перед отскоком. Если всем визуально хочется LONG, проверь, не находится ли цена у сопротивления/после выноса вверх.
- Думай как в шахматах на несколько ходов вперёд: сначала сформулируй основной сценарий, затем обязательно оспорь его противоположным сценарием.
- Для каждого финального setup мысленно проверь: почему LONG, а не SHORT? почему SHORT, а не LONG? где будет ошибка идеи? где толпа может попасть в ловушку?
- Выбирай не самый прямолинейный ход, а самый умный setup с лучшим соотношением: фаза BTC/ETH + уровень + реакция + риск/прибыль + вероятность, что вход не является ловушкой.
- Это мягкое правило анализа, а не запрет на сделки. Цель остаётся — выбрать 3 лучшие сделки, не ломая MARKET-ордера и текущую логику режима.

ORDER TYPE RULE:
- По умолчанию используй order_type: "LIMIT".
- Используй order_type: "MARKET" только для очень сильных A+ setup, где 4H/1H/15min подтверждают направление, цена не находится после позднего вертикального импульса, риск до stop_loss приемлемый, а R/R до TP2 минимум 1:2.
- Если есть сомнения по точке входа — используй LIMIT.
- Не используй MARKET, если цена уже далеко ушла от оптимального входа или находится у локального high/low после резкого движения.

STOP/RISK RULE:
- Стоп должен быть не меньше 1% от entry.
- Если расчетный stop_loss получается ближе 1% от entry, расширь stop_loss минимум до 1%.
- Максимальный stop_distance_percent — 5%.
- Если для нормальной структуры нужен стоп больше 5%, не бери эту сделку.
- risk.stop_distance_percent должен соответствовать расстоянию от entry до stop_loss.

TAKE PROFIT RULE:
- Take profits не считать механически от размера стопа.
- TP ставить по структуре графика: ближайшие уровни ликвидности, high/low, зоны сопротивления/поддержки, MA/диапазоны и реальные цели движения.
- Если структурные TP не дают адекватный risk/reward хотя бы до TP2, сделку не брать.

EXECUTION SAFETY RULES:
- После TP1 бот переносит stop_loss в breakeven.
- До входа бот НЕ отменяет сделку из-за касания TP1.
- До входа бот отменяет/не выставляет сделку, если цена уже дошла до TP2 или прошла stop_loss.
- Trailing: OFF. Scalp exit: OFF.

ANALYSIS RULES:
- Сначала оцени BTC/ETH market context.
- Потом сравни top-15 кандидатов по 4H/1H/15min.
- Не входи в поздний вертикальный памп без ретеста.
- Не шорти сильную альту без слома структуры.
- Если BTC падает, BTC.D падает, а альты держатся лучше — отдавай приоритет strong-alt long-кандидатам, а не шортам по альтам.
- Если рынок грязный или риск/прибыль плохой — не входи посередине диапазона; ищи лучшие лимитные идеи от поддержки/сопротивления. NO_TRADE используй только если реально нет приемлемого setup.
- Цель режима — до 3 лучших сделок, с фокусом на качество входа, а не на автоматический отказ от сделок.

FINAL SELF-CHECK BEFORE SENDING FILE:
1. Файл прикреплён как .txt, а не написан текстом в сообщении.
2. Имя файла похоже на setup-0059_0106.txt.
3. В файле чистый JSON, начинается с { и заканчивается }.
4. setup_version ровно "1.6".
5. В trades максимум 3 сделки.
6. Нет symbols с substring STOCK.
7. Каждый symbol точно совпадает с symbol из log.txt/manifest.json; не использовать длинные названия вместо тикеров.
8. У каждой сделки order_type LIMIT или MARKET.
9. У каждой сделки take_profits ровно в формате 35 / 35 / REMAINDER через ключ size_percent.
10. Стоп каждой сделки от 1% до 5%.
11. Все цены записаны обычными десятичными числами без e-05 / scientific notation.
12. Если условий нет, лучше дай NO_TRADE, чем кривой setup."""


async def build_chatgpt_scan_pack(exchange_client, scanner, settings: dict, ws_supervisor=None, limit: int = 200, storage=None) -> str:
    """Build a full ChatGPT Scan Mode ZIP: log.txt + task.txt + manifest + charts."""
    started = time.time()
    _phase_t0 = started
    chatgpt_log_event("scan_pack_start", limit=limit, top=os.getenv("CHATGPT_SCAN_PACK_TOP", "15"), chart_workers=os.getenv("CHATGPT_CHART_CONCURRENCY", os.getenv("CHATGPT_SCAN_CONCURRENCY", "3")))
    log_path = await build_chatgpt_log(exchange_client, scanner, settings, ws_supervisor=ws_supervisor, limit=limit)
    _timing_build_log_sec = round(time.time() - _phase_t0, 2)
    _phase_t0 = time.time()
    raw_log_text = Path(log_path).read_text(encoding="utf-8", errors="replace")
    selected_rows = _parse_chatgpt_log_candidates(raw_log_text, limit=int(os.getenv("CHATGPT_SCAN_PACK_TOP", "15") or 15))
    _timing_select_sec = round(time.time() - _phase_t0, 2)
    # The legacy log still contains manual-screenshot instructions for old flow.
    # For ZIP pack mode, task.txt is the source of truth, so keep the market data
    # and replace the legacy instruction block with a short pointer.
    log_text = re.split(r"\n\n(?:=== TASK ===\n)?ЗАДАЧА ДЛЯ CHATGPT MODE:", raw_log_text, maxsplit=1)[0].rstrip() + "\n\nCHATGPT_SCAN_PACK_NOTE:\nUse task.txt and manifest.json from this ZIP as the active instructions for this pack.\n"
    selected_symbols = [r["symbol"] for r in selected_rows]
    chatgpt_log_event(
        "scan_pack_top15_selected",
        selected_count=len(selected_symbols),
        symbols=",".join(selected_symbols),
        rows=json.dumps(selected_rows, ensure_ascii=False)[:1800],
    )
    meta_by_symbol = {r["symbol"]: r for r in selected_rows}
    # Market context must always exist, but it must not duplicate selected charts.
    required_context = [("BTC_USDT", "4h"), ("BTC_USDT", "1h"), ("ETH_USDT", "4h"), ("ETH_USDT", "1h")]
    chart_jobs: list[tuple[str, str]] = []
    seen_files: set[str] = set()
    for sym in selected_symbols:
        for tf in ("4h", "1h", "15m"):
            fn = _chatgpt_chart_filename(sym, tf)
            if fn not in seen_files:
                seen_files.add(fn); chart_jobs.append((sym, tf))
    for sym, tf in required_context:
        fn = _chatgpt_chart_filename(sym, tf)
        if fn not in seen_files:
            seen_files.add(fn); chart_jobs.append((sym, tf))

    _timing_jobs_prepare_sec = round(time.time() - _phase_t0, 2)
    chatgpt_log_event(
        "scan_pack_chart_jobs_prepared",
        jobs=len(chart_jobs),
        expected_png_count=len(seen_files),
        selected_count=len(selected_symbols),
        context=",".join([f"{s}:{t}" for s, t in required_context]),
        sample_files=",".join(sorted(list(seen_files))[:20]),
    )

    stamp = datetime.now().strftime("%H%M_%d%m")
    log_dir = Path(os.getenv("CHATGPT_LOG_DIR", "/tmp"))
    work = log_dir / f"chatgpt_scan_pack_work_{stamp}_{int(time.time())}"
    zip_path = log_dir / f"chatgpt_scan_pack-{stamp}.zip"
    screens = work / "screenshots"
    screens.mkdir(parents=True, exist_ok=True)
    (work / "log.txt").write_text(log_text, encoding="utf-8")
    (work / "task.txt").write_text(CHATGPT_SCAN_TASK_TEXT, encoding="utf-8")

    workers = int(os.getenv("CHATGPT_CHART_CONCURRENCY", os.getenv("CHATGPT_SCAN_CONCURRENCY", "3")) or 3)
    sem = asyncio.Semaphore(max(1, workers))
    missing: list[str] = []
    errors: list[str] = []

    async def _render_one(sym: str, tf: str):
        native = mexc_native_symbol(sym)
        ex_tf = _chatgpt_tf_to_exchange(tf)
        filename = _chatgpt_chart_filename(native, ex_tf)
        out = screens / filename
        try:
            chatgpt_log_event("scan_pack_chart_start", symbol=native, timeframe=ex_tf, file=filename)
            async with sem:
                candles = await _mexc_call(f"chart_fetch_ohlcv_{ex_tf}", lambda: exchange_client.fetch_ohlcv(native, timeframe=ex_tf, limit=160), symbol=native)
                ticker = await _mexc_call("chart_fetch_ticker", lambda: exchange_client.fetch_ticker(native), symbol=native)
            closes = _closes(candles); vols = _volumes(candles)
            vol_now = vols[-1] if vols else 0.0
            vol_avg = (sum(vols[-21:-1]) / 20.0) if len(vols) >= 21 else 0.0
            meta = dict(meta_by_symbol.get(native) or {})
            meta.update({
                "last_price": _safe_float((ticker or {}).get("last") or (ticker or {}).get("close")),
                "quote_volume": _safe_float((ticker or {}).get("quoteVolume") or ((ticker or {}).get("info") or {}).get("volume24")),
                "high_24h": _safe_float((ticker or {}).get("high") or ((ticker or {}).get("info") or {}).get("high24Price")),
                "low_24h": _safe_float((ticker or {}).get("low") or ((ticker or {}).get("info") or {}).get("low24Price")),
                "vol_ratio": (vol_now / vol_avg) if vol_avg else 0.0,
            })
            _render_chatgpt_candidate_chart(native, ex_tf, candles, meta, out)
            try:
                size_kb = round(out.stat().st_size / 1024, 1)
                chatgpt_log_event("scan_pack_chart_done", symbol=native, timeframe=ex_tf, file=filename, candles=len(candles or []), size_kb=size_kb, resolution="1280x720")
                if out.stat().st_size > int(os.getenv("CHATGPT_MAX_PNG_SIZE_KB", "900")) * 1024:
                    chatgpt_log_event("scan_pack_chart_large", file=filename, size_kb=size_kb)
            except Exception:
                pass
        except Exception as e:
            missing.append(filename)
            errors.append(f"{filename}: {str(e)[:240]}")
            chatgpt_log_event("scan_pack_chart_error", symbol=native, timeframe=ex_tf, file=filename, error=repr(e))

    _phase_t0 = time.time()
    await asyncio.gather(*(_render_one(sym, tf) for sym, tf in chart_jobs))
    _timing_charts_sec = round(time.time() - _phase_t0, 2)
    png_files = sorted([p for p in screens.glob("*.png")])
    try:
        from config import VERSION as _bot_code_version
    except Exception:
        _bot_code_version = "410_plus_full"
    manifest = {
        "pack_type": "CHATGPT_SCAN_MODE",
        "created_utc": _now_utc(),
        "bot_version": _bot_code_version,
        "scan_limit": int(limit),
        "selected_count": len(selected_symbols),
        "selected_symbols": selected_symbols,
        "selected_rows": selected_rows,
        "timeframes": ["4h", "1h", "15min"],
        "required_context": ["btc_4h", "btc_1h", "eth_4h", "eth_1h"],
        "blocked_symbol_substrings": list(CHATGPT_BLOCKED_SYMBOL_SUBSTRINGS),
        "chart_source": "python_ohlcv_mexc",
        "chart_resolution": "1280x720",
        "chart_dpi": 100,
        "candles_per_chart": 120,
        "ohlcv_fetch_limit": 160,
        "expected_png_count": len(seen_files),
        "actual_png_count": len(png_files),
        "missing_charts": sorted(set(missing)),
        "generation_errors": errors,
        "max_png_size_kb_target": 600,
        "timing_sec": {
            "build_log_full_scan": _timing_build_log_sec,
            "parse_select_top": _timing_select_sec,
            "prepare_chart_jobs": _timing_jobs_prepare_sec,
            "render_fetch_all_charts": _timing_charts_sec,
            "total_until_manifest": round(time.time() - started, 2),
        },
        "elapsed_sec": round(time.time() - started, 2),
    }
    _phase_t0 = time.time()
    (work / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    _timing_manifest_write_sec = round(time.time() - _phase_t0, 2)
    if storage is not None:
        try:
            await storage.set("chatgpt_last_scan_manifest", manifest, bump_revision=False)
            await storage.set("chatgpt_last_scan_allowed_symbols", sorted(_chatgpt_pack_allowed_symbols_from_manifest(manifest)), bump_revision=False)
            chatgpt_log_event("scan_pack_allowed_symbols_saved", count=len(_chatgpt_pack_allowed_symbols_from_manifest(manifest)))
        except Exception as e:
            chatgpt_log_event("scan_pack_allowed_symbols_save_error", error=repr(e))
    chatgpt_log_event("scan_pack_manifest_written", expected=manifest["expected_png_count"], actual=manifest["actual_png_count"], missing=len(manifest["missing_charts"]), errors=len(manifest["generation_errors"]), resolution=manifest["chart_resolution"])
    _phase_t0 = time.time()
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for rel in ("log.txt", "task.txt", "manifest.json"):
            z.write(work / rel, rel)
        for p in png_files:
            z.write(p, f"screenshots/{p.name}")
    _timing_zip_write_sec = round(time.time() - _phase_t0, 2)
    try:
        zip_size_kb = round(zip_path.stat().st_size / 1024, 1)
    except Exception:
        zip_size_kb = 0
    chatgpt_log_event("scan_pack_zip_written", zip_path=str(zip_path), zip_size_kb=zip_size_kb, png_count=len(png_files), zip_write_sec=_timing_zip_write_sec)
    chatgpt_log_event(
        "scan_pack_timing",
        build_log_full_scan_sec=_timing_build_log_sec,
        parse_select_top_sec=_timing_select_sec,
        prepare_chart_jobs_sec=_timing_jobs_prepare_sec,
        render_fetch_all_charts_sec=_timing_charts_sec,
        manifest_write_sec=_timing_manifest_write_sec,
        zip_write_sec=_timing_zip_write_sec,
        total_sec=round(time.time() - started, 2),
        chart_workers=workers,
    )
    chatgpt_log_event("scan_pack_done", zip_path=str(zip_path), selected=len(selected_symbols), expected=len(seen_files), actual=len(png_files), missing=len(missing), elapsed_sec=round(time.time() - started, 2))
    return str(zip_path)

async def build_chatgpt_log(exchange_client, scanner, settings: dict, ws_supervisor=None, limit: int = 200) -> str:
    """Run one top-N scan and write a ChatGPT-ready log.txt."""
    scan_started_at = time.time()
    workers = int(os.getenv("CHATGPT_SCAN_CONCURRENCY", "3") or 3)
    chatgpt_log_event("scan_start", limit=limit, workers=workers, retries=os.getenv("CHATGPT_SCAN_RETRIES", "2"))
    work = Path(os.getenv("CHATGPT_LOG_DIR", "/tmp"))
    work.mkdir(parents=True, exist_ok=True)
    path = work / f"chatgpt_market_log_{int(time.time())}.txt"

    scan_settings = dict(settings or {})
    scan_settings["universe_mode"] = f"top-{int(limit)}"
    # Load extra symbols first, then remove region-blocked STOCK contracts, so
    # the final scan can still contain up to the requested top-N tradable coins.
    scan_settings["max_symbols"] = int(os.getenv("CHATGPT_SCAN_PREFILTER_LIMIT", str(max(int(limit) * 2, int(limit)))))
    _phase_t0 = time.time()
    await scanner.refresh_symbols(exchange_client, scan_settings, ws_supervisor)
    _timing_refresh_symbols_sec = round(time.time() - _phase_t0, 2)
    _phase_t0 = time.time()
    raw_symbols = list(dict.fromkeys(getattr(scanner, "hot_symbols", []) or []))
    filtered_symbols, blocked_symbols = filter_chatgpt_symbols(raw_symbols)
    symbols = mexc_native_symbols(filtered_symbols)[:int(limit)]
    _timing_symbol_filter_sec = round(time.time() - _phase_t0, 2)
    chatgpt_log_event(
        "scan_symbols_loaded",
        raw_count=len(raw_symbols),
        blocked_count=len(blocked_symbols),
        blocked_sample=",".join(blocked_symbols[:30]),
        count=len(symbols),
        sample=",".join(symbols[:10]),
    )

    async def one_symbol(sym: str) -> str:
        native_sym = mexc_native_symbol(sym)
        if is_chatgpt_blocked_symbol(native_sym):
            chatgpt_log_event("scan_symbol_blocked_stock", symbol=native_sym)
            return f"SYMBOL: {native_sym}\nSKIPPED: REGION_BLOCKED_STOCK_CONTRACT"
        try:
            # MEXC returns code 510 if we hit public endpoints too fast.
            # Keep calls sequential + throttled; command itself runs in background.
            ticker = await _mexc_call("fetch_ticker", lambda: exchange_client.fetch_ticker(native_sym), symbol=native_sym)
            ob = await _mexc_call("fetch_order_book", lambda: exchange_client.fetch_order_book(native_sym, limit=20), symbol=native_sym)
            price = _safe_float(ticker.get("last") or ticker.get("close"))
            tf_data = {}
            for tf in ("15m", "1h", "4h"):
                candles = await _mexc_call(f"fetch_ohlcv_{tf}", lambda tf=tf: exchange_client.fetch_ohlcv(native_sym, timeframe=tf, limit=120), symbol=native_sym)
                closes = _closes(candles)
                vols = _volumes(candles)
                macd, sig, hist = _macd(closes)
                high = max([float(x[2]) for x in candles[-40:]], default=0.0) if candles else 0.0
                low = min([float(x[3]) for x in candles[-40:]], default=0.0) if candles else 0.0
                vol_now = vols[-1] if vols else 0.0
                vol_avg = (sum(vols[-21:-1]) / 20.0) if len(vols) >= 21 else 0.0
                tf_data[tf] = {
                    "ma7": _ma(closes, 7), "ma25": _ma(closes, 25), "ma99": _ma(closes, 99),
                    "rsi": _rsi(closes), "macd": macd, "macd_signal": sig, "macd_hist": hist,
                    "vol_ratio": (vol_now / vol_avg) if vol_avg else 0.0,
                    "high40": high, "low40": low,
                }
            obs = _orderbook_stats(ob, price)
            long_score, short_score, note = _score_symbol(price, tf_data, obs)
            pct24 = _safe_float(ticker.get("percentage") or (ticker.get("info") or {}).get("riseFallRate"))
            if abs(pct24) <= 1.0:
                pct24 *= 100.0
            lines = [
                f"SYMBOL: {native_sym}",
                f"PRICE: {_fmt(price)} | 24H_CHANGE_PCT: {_fmt(pct24, 4)} | QUOTE_VOL: {_fmt(ticker.get('quoteVolume') or 0, 2)}",
                f"ORDERBOOK: bid={obs['bid_pct']:.1f}% ask={obs['ask_pct']:.1f}% spread={obs['spread_pct']:.3f}% | big_bid={_fmt(obs['big_bid'][0])}/{_fmt(obs['big_bid'][1],2)} | big_ask={_fmt(obs['big_ask'][0])}/{_fmt(obs['big_ask'][1],2)}",
            ]
            for tf, d in tf_data.items():
                lines.append(
                    f"{tf.upper()}: MA7={_fmt(d['ma7'])} MA25={_fmt(d['ma25'])} MA99={_fmt(d['ma99'])} "
                    f"RSI={d['rsi']:.1f} MACD_HIST={_fmt(d['macd_hist'], 8)} VOL_RATIO={d['vol_ratio']:.2f} "
                    f"HIGH40={_fmt(d['high40'])} LOW40={_fmt(d['low40'])}"
                )
            lines.append(f"SCORES: LONG={long_score}/100 SHORT={short_score}/100 NOTE={note}")
            return "\n".join(lines)
        except Exception as e:
            chatgpt_log_event("scan_symbol_error", symbol=native_sym, error=repr(e))
            return f"SYMBOL: {native_sym}\nERROR: {str(e)[:240]}"

    # v22: keep the same scan depth, but use three cautious workers by default.
    # This should reduce a ~9 minute scan toward ~3-5 minutes without removing
    # funding/orderbook/candles/RSI/MACD data. MEXC code=510 is still handled
    # inside _mexc_call with retry/backoff.
    sem = asyncio.Semaphore(workers)
    async def guarded(sym):
        async with sem:
            return await one_symbol(sym)
    _phase_t0 = time.time()
    blocks = await asyncio.gather(*(guarded(s) for s in symbols), return_exceptions=True)
    _timing_workers_scan_sec = round(time.time() - _phase_t0, 2)
    ok_blocks = 0
    err_blocks = 0
    for b in blocks:
        txt = str(b)
        if isinstance(b, Exception) or "\nERROR:" in txt:
            err_blocks += 1
        else:
            ok_blocks += 1
    chatgpt_log_event("scan_workers_done", workers=workers, symbols=len(symbols), ok=ok_blocks, errors=err_blocks, worker_scan_sec=_timing_workers_scan_sec, elapsed_sec=round(time.time() - scan_started_at, 2))

    btc = ""
    eth = ""
    _phase_t0 = time.time()
    try:
        btc = await one_symbol("BTC_USDT")
    except Exception:
        pass
    _timing_btc_context_sec = round(time.time() - _phase_t0, 2)
    _phase_t0 = time.time()
    try:
        eth = await one_symbol("ETH_USDT")
    except Exception:
        pass
    _timing_eth_context_sec = round(time.time() - _phase_t0, 2)

    # Keep the legacy log task synchronized with the active ZIP task.txt instructions.
    task = CHATGPT_SCAN_TASK_TEXT.replace("CHATGPT_TASK", "ЗАДАЧА ДЛЯ CHATGPT MODE:", 1)

    header = [
        "CHATGPT MARKET SCAN LOG",
        f"CREATED_UTC: {_now_utc()}",
        f"EXCHANGE: {getattr(exchange_client, 'exchange_id', 'mexc')}",
        f"SCAN_LIMIT: {limit}",
        f"SCANNER_SOURCE: {getattr(scanner, 'last_scan_source', '-')}",
        f"SELECTED_SYMBOLS: {len(symbols)}",
        "",
        "=== MARKET CONTEXT: BTC ===",
        btc or "BTC context unavailable",
        "",
        "=== MARKET CONTEXT: ETH ===",
        eth or "ETH context unavailable",
        "",
        "=== TOP-200 FULL SCAN ===",
    ]
    body = []
    for b in blocks:
        if isinstance(b, Exception):
            body.append(f"ERROR_BLOCK: {str(b)[:240]}")
        else:
            body.append(str(b))
    text = "\n".join(header + ["\n---\n".join(body), "", "=== TASK ===", task, ""])
    _phase_t0 = time.time()
    path.write_text(text, encoding="utf-8-sig")
    _timing_log_write_sec = round(time.time() - _phase_t0, 2)
    chatgpt_log_event(
        "scan_timing",
        refresh_symbols_sec=_timing_refresh_symbols_sec,
        symbol_filter_sec=_timing_symbol_filter_sec,
        worker_scan_sec=_timing_workers_scan_sec,
        btc_context_sec=_timing_btc_context_sec,
        eth_context_sec=_timing_eth_context_sec,
        log_write_sec=_timing_log_write_sec,
        total_sec=round(time.time() - scan_started_at, 2),
        workers=workers,
        symbols=len(symbols),
    )
    chatgpt_log_event("scan_log_created", path=str(path), symbols=len(symbols), bytes=len(text.encode("utf-8-sig")))
    return str(path)


def extract_setup_json(text: str) -> dict:
    chatgpt_log_event("setup_extract_start", text_bytes=len(str(text or "").encode("utf-8", errors="ignore")))
    raw = str(text or "").replace("\ufeff", "").strip()

    # Allow the historical wrapper used in some ChatGPT answers, but still
    # require the payload itself to be a JSON object.
    if "===== setup.txt =====" in raw:
        raw = raw.split("===== setup.txt =====", 1)[1].split("===== end setup.txt =====", 1)[0].strip()

    # Robustly unwrap common markdown fences if a model accidentally added them.
    if raw.startswith("```"):
        lines = raw.splitlines()
        if lines and lines[0].strip().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        raw = "\n".join(lines).strip()
        if raw.lower().startswith("json\n"):
            raw = raw.split("\n", 1)[1].strip()

    # Primary path: pure JSON file.
    if raw.startswith("{") and raw.endswith("}"):
        data = json.loads(raw)
    else:
        # Fallback: extract the first balanced top-level JSON object from text.
        # This handles accidental captions around JSON without accepting YAML/
        # key=value pseudo-setup formats.
        start = raw.find("{")
        if start < 0:
            raise ValueError(
                "setup.txt должен содержать чистый JSON object: файл начинается с { и заканчивается }. "
                "Сейчас в файле не найден символ {"
            )
        depth = 0
        in_str = False
        esc = False
        end = -1
        for i in range(start, len(raw)):
            ch = raw[i]
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    in_str = False
                continue
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        if end < start:
            raise ValueError("setup.txt содержит незакрытый JSON object: не найден корректный закрывающий }")
        data = json.loads(raw[start:end + 1])

    if not isinstance(data, dict):
        raise ValueError("setup JSON must be an object")
    chatgpt_log_event("setup_extract_ok", setup_version=data.get("setup_version"), trades_count=len(data.get("trades") or []), verdict=data.get("verdict"))
    return data

def validate_setup(data: dict) -> list[dict]:
    chatgpt_log_event("setup_validate_start", setup_version=(data or {}).get("setup_version") if isinstance(data, dict) else None)
    if not isinstance(data, dict):
        raise ValueError("setup JSON must be an object")
    setup_version = str(data.get("setup_version") or "").strip()
    if setup_version != CHATGPT_SETUP_VERSION:
        raise ValueError(
            f"неподдерживаемая версия setup_version={setup_version or 'MISSING'}. "
            f"Нужна версия: {CHATGPT_SETUP_VERSION}. Старые setup-файлы не поддерживаются, чтобы не ломать ChatGPT Mode."
        )
    trades = data.get("trades") or []
    if not isinstance(trades, list):
        raise ValueError("trades must be a list")
    data["_requested_trades_total"] = len(trades)
    max_trades = CHATGPT_MAX_ACTIVE_TRADES
    if len(trades) > max_trades:
        raise ValueError(f"too many trades: max {max_trades} for ChatGPT Mode")
    scan_pack_allowed_symbols = {mexc_native_symbol(x) for x in (data.get("_scan_pack_allowed_symbols") or []) if mexc_native_symbol(x)}
    if scan_pack_allowed_symbols:
        chatgpt_log_event("setup_symbol_guard_loaded", count=len(scan_pack_allowed_symbols), sample=",".join(sorted(scan_pack_allowed_symbols)[:20]))
    duplicate_skipped: list[dict] = []
    seen_setup_symbols: set[str] = set()
    valid_until = str(data.get("valid_until_utc") or "").replace("UTC", "").strip()
    if valid_until:
        try:
            dt = datetime.fromisoformat(valid_until)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) > dt.astimezone(timezone.utc):
                raise ValueError("setup expired")
        except ValueError as e:
            if "expired" in str(e):
                raise
    out = []
    for i, t in enumerate(trades, start=1):
        if not isinstance(t, dict):
            raise ValueError(f"TRADE_{i}: must be object")
        raw_symbol = str(t.get("symbol") or "").strip().upper()
        if is_chatgpt_blocked_symbol(raw_symbol):
            raise ValueError(f"TRADE_{i}: symbol {raw_symbol} is region-blocked because it contains STOCK")
        symbol = mexc_native_symbol(raw_symbol)
        if is_chatgpt_blocked_symbol(symbol):
            raise ValueError(f"TRADE_{i}: symbol {symbol} is region-blocked because it contains STOCK")
        if scan_pack_allowed_symbols and symbol not in scan_pack_allowed_symbols:
            chatgpt_log_event("setup_symbol_rejected_not_in_scan_pack", index=i, raw_symbol=raw_symbol, normalized_symbol=symbol, allowed_sample=",".join(sorted(scan_pack_allowed_symbols)[:30]))
            raise ValueError(
                f"TRADE_{i}: symbol {symbol} is not in latest ChatGPT scan pack. "
                "Use exact MEXC symbols from log.txt/manifest.json; do not rename tickers, e.g. use NVDA_USDT not NVIDIA_USDT."
            )
        if symbol in seen_setup_symbols:
            reason = f"дубль в setup-файле: {symbol}; повторная сделка пропущена"
            duplicate_skipped.append({"symbol": symbol, "ok": False, "reason": reason, "skipped_duplicate_in_setup": True})
            chatgpt_log_event("setup_trade_skipped_duplicate_in_setup", index=i, symbol=symbol, reason=reason)
            continue
        seen_setup_symbols.add(symbol)
        direction = str(t.get("direction") or "").upper()
        order_type = str(t.get("order_type") or "").upper()
        if direction not in {"LONG", "SHORT"}:
            raise ValueError(f"TRADE_{i}: direction must be LONG/SHORT")
        if order_type not in {"MARKET", "LIMIT"}:
            raise ValueError(f"TRADE_{i}: order_type must be MARKET/LIMIT")
        entry = _safe_float(t.get("entry") if order_type == "LIMIT" else (t.get("entry_reference") or t.get("entry")))
        stop = _safe_float(t.get("stop_loss"))
        if entry <= 0 or stop <= 0:
            raise ValueError(f"TRADE_{i}: entry and stop_loss are required")
        if direction == "LONG" and stop >= entry:
            raise ValueError(f"TRADE_{i}: LONG stop must be below entry")
        if direction == "SHORT" and stop <= entry:
            raise ValueError(f"TRADE_{i}: SHORT stop must be above entry")
        stop_dist = _stop_distance_pct(entry, stop)
        if stop_dist < CHATGPT_MIN_STOP_DISTANCE_PCT:
            raise ValueError(f"TRADE_{i}: stop distance {stop_dist:.2f}% is below minimum {CHATGPT_MIN_STOP_DISTANCE_PCT:.2f}%")
        if stop_dist > CHATGPT_MAX_STOP_DISTANCE_PCT:
            raise ValueError(f"TRADE_{i}: stop distance {stop_dist:.2f}% is above maximum {CHATGPT_MAX_STOP_DISTANCE_PCT:.2f}%")
        tps = t.get("take_profits") or []
        if not tps:
            raise ValueError(f"TRADE_{i}: take_profits required")
        if not isinstance(tps, list):
            raise ValueError(f"TRADE_{i}: take_profits must be a list")

        def _tp_raw_size(tp: dict, idx: int, total: int) -> Any:
            # ChatGPT иногда пишет size / percent / size_pct вместо size_percent.
            # Торговую логику не меняем — только нормализуем входной setup v1.6.
            for key in ("size_percent", "size_pct", "percent", "size", "qty_percent"):
                if key in tp:
                    return tp.get(key)
            # Если TP3 без размера, но первые TP уже числовые — считаем остатком.
            if idx == total - 1 and total >= 2:
                return "REMAINDER"
            return None

        def _is_remainder(v: Any) -> bool:
            if not isinstance(v, str):
                return False
            return v.strip().upper() in {"REMAINDER", "REMAINING", "REST", "ОСТАТОК"}

        has_remainder = False
        numeric_total = 0.0
        normalized_sizes: list[float | str] = []
        for idx, x in enumerate(tps):
            if not isinstance(x, dict):
                raise ValueError(f"TRADE_{i}: invalid TP row")
            raw_size = _tp_raw_size(x, idx, len(tps))
            if _is_remainder(raw_size):
                if idx != len(tps) - 1:
                    raise ValueError(f"TRADE_{i}: REMAINDER is allowed only on the last TP")
                has_remainder = True
                normalized_sizes.append("REMAINDER")
            else:
                size = _safe_float(raw_size)
                normalized_sizes.append(size)
                numeric_total += size

        if has_remainder:
            if numeric_total <= 0 or numeric_total >= 100.0:
                raise ValueError(f"TRADE_{i}: numeric TP sizes before REMAINDER must be >0 and <100")
        else:
            if abs(numeric_total - 100.0) > 0.5:
                raise ValueError(f"TRADE_{i}: TP sizes must sum to 100 or last TP must be REMAINDER")

        clean_tps = []
        for idx, tp in enumerate(tps):
            p = _safe_float(tp.get("price") if isinstance(tp, dict) else 0)
            s: float | str = normalized_sizes[idx]
            if p <= 0 or (s != "REMAINDER" and float(s) <= 0):
                raise ValueError(f"TRADE_{i}: invalid TP")
            if direction == "LONG" and p <= entry:
                raise ValueError(f"TRADE_{i}: LONG TP must be above entry")
            if direction == "SHORT" and p >= entry:
                raise ValueError(f"TRADE_{i}: SHORT TP must be below entry")
            clean_tps.append({"price": p, "size_percent": s})
        ct = dict(t)
        risk = dict(ct.get("risk") or {}) if isinstance(ct.get("risk") or {}, dict) else {}
        risk["stop_distance_percent"] = round(stop_dist, 4)
        risk["estimated_deposit_risk_percent"] = round(stop_dist, 4)
        risk["rule"] = "STOP_DISTANCE_MUST_BE_1_TO_5_PERCENT"
        ct.update({"symbol": symbol, "direction": direction, "order_type": order_type, "entry": entry, "stop_loss": stop, "take_profits": clean_tps, "risk": risk})
        chatgpt_log_event("setup_trade_validated", index=i, symbol=symbol, direction=direction, order_type=order_type, entry=entry, stop_loss=stop, stop_distance_pct=round(stop_dist, 4), tps=len(clean_tps))
        out.append(ct)
    data["_duplicate_skipped"] = duplicate_skipped
    chatgpt_log_event("setup_validate_ok", trades=len(out), duplicates=len(duplicate_skipped))
    return out


async def _cancel_all_old_pending_limits(storage, exec_engine, setup_symbols: set[str] | None = None) -> list[dict]:
    """Cancel all stale ChatGPT orders before applying a new setup.

    Hard rule for V30:
    - real open positions are never touched;
    - TP/SL of real open positions are never touched;
    - all old pending entry orders are cancelled;
    - orphan TP/SL/plan orders whose symbol has no real open ChatGPT position
      are treated as garbage and cancelled too.

    This fixes the case where global sync reported pending=0, but MEXC later
    rejected TON/HBAR with "open order exists on exchange" because orphan
    plan/stop orders were still live for those symbols.
    """
    cancelled: list[dict] = []
    local_pending_ids: set[str] = set()
    setup_symbols = {mexc_native_symbol(s) for s in (setup_symbols or set()) if mexc_native_symbol(s)}

    # 1) Read cache only after exchange-first reconcile has already rebuilt it.
    # It is used here only to identify order IDs and real open symbols, never to
    # make pre-sync slot decisions.
    try:
        positions = await storage.positions()
    except Exception as e:
        chatgpt_log_event("cancel_old_pending_load_error", error=str(e))
        positions = []

    live_symbols: set[str] = set()
    for pos in positions or []:
        if str(pos.get("strategy") or "").lower() == "chatgpt_setup" and str(pos.get("status") or "").lower() in {"open", "closing"}:
            sym_live = mexc_native_symbol(pos.get("symbol"))
            if sym_live:
                live_symbols.add(sym_live)
    chatgpt_log_event("cancel_old_pending_live_symbols", symbols=sorted(live_symbols), count=len(live_symbols))

    for pos in positions or []:
        sym = str(pos.get("symbol") or "").upper()
        strategy = str(pos.get("strategy") or "").lower()
        status = str(pos.get("status") or "").lower()
        order_type = str(pos.get("order_type") or "").lower()
        if strategy == "chatgpt_setup" and status == "pending" and order_type == "limit":
            oid = str(pos.get("order_id") or "").strip()
            if oid:
                local_pending_ids.add(oid)
            try:
                chatgpt_log_event("cancel_old_pending_local_start", symbol=sym, order_id=oid, old_entry=pos.get("entry_price"))
                res = await exec_engine.cancel_entry(pos, live=True, reason="chatgpt_new_setup_cancel_old_pending")
                ok = bool((res or {}).get("ok"))
                chatgpt_log_event("cancel_old_pending_local_done", symbol=sym, order_id=oid, ok=ok, result=res)
                cancelled.append({"source": "local", "symbol": sym, "order_id": oid, "ok": ok, "reason": "entry", "result": res})
            except Exception as e:
                chatgpt_log_event("cancel_old_pending_local_error", symbol=sym, order_id=oid, error=str(e))
                cancelled.append({"source": "local", "symbol": sym, "order_id": oid, "ok": False, "reason": "entry", "error": str(e)})
        elif strategy == "chatgpt_setup":
            chatgpt_log_event("cancel_old_pending_skip", symbol=sym, status=status, strategy=strategy, order_type=order_type)

    # 2) Cancel exchange-only ChatGPT entry orders. These can survive redeploy or
    # local state mismatch. Do not cancel TP/SL/reduce-only orders.
    try:
        chatgpt_log_event("cancel_old_pending_exchange_fetch_start")
        orders = await exec_engine.exchange_client.fetch_open_orders()
        chatgpt_log_event("cancel_old_pending_exchange_found", count=len(orders or []))
    except Exception as e:
        chatgpt_log_event("cancel_old_pending_exchange_fetch_error", error=str(e))
        orders = []

    def _order_symbol(o: dict) -> str:
        info = o.get("info") if isinstance(o.get("info"), dict) else {}
        return mexc_native_symbol(o.get("symbol") or info.get("symbol") or info.get("contract") or "")

    def _order_id(o: dict) -> str:
        info = o.get("info") if isinstance(o.get("info"), dict) else {}
        return str(o.get("id") or info.get("orderId") or info.get("id") or info.get("planOrderId") or "").strip()

    def _is_chatgpt_entry_order(o: dict) -> bool:
        info = o.get("info") if isinstance(o.get("info"), dict) else {}
        ext = str(info.get("externalOid") or info.get("clientOrderId") or o.get("clientOrderId") or "")
        oid = _order_id(o)
        reduce_only = str(o.get("reduceOnly") or info.get("reduceOnly") or "").lower() in {"1", "true", "yes"}
        prot = str(info.get("_protection_kind") or "").lower()
        src = str(info.get("_source_endpoint") or "").lower()
        if reduce_only or prot in {"tp", "sl"} or "stoporder" in src or "planorder" in src:
            return False
        is_normal_open_order_endpoint = "order/list/open_orders" in src or src == ""
        return ext.startswith("bot_entry_") or (oid and oid in local_pending_ids) or is_normal_open_order_endpoint

    exchange_cancelled_ids: set[str] = set()
    cancel_candidates: list[tuple[dict, str, str, str]] = []
    for o in orders or []:
        sym = _order_symbol(o)
        oid = _order_id(o)
        info = o.get("info") if isinstance(o.get("info"), dict) else {}
        is_entry = _is_chatgpt_entry_order(o)
        # Only remove orphan planorders for symbols that are being reused by the NEW setup.
        # Global orphan cleanup is dangerous: if exchange position sync misses a live position
        # for a moment, the bot can delete its real SL/TP. Entry limits are still always removed.
        is_orphan = bool(sym and sym in setup_symbols and sym not in live_symbols)
        should_cancel = bool(is_entry or is_orphan)
        # Do not spam runtime logs with every untouched order. Log only actions.
        if should_cancel:
            chatgpt_log_event(
                "cancel_old_pending_exchange_order_seen",
                symbol=sym,
                order_id=oid,
                source=info.get("_source_endpoint"),
                kind=info.get("_protection_kind"),
                is_entry=is_entry,
                is_orphan=is_orphan,
                should_cancel=should_cancel,
            )
        if not should_cancel or not sym or not oid or oid in exchange_cancelled_ids:
            continue
        exchange_cancelled_ids.add(oid)
        cancel_candidates.append((o, sym, oid, "entry" if is_entry else "orphan"))

    async def _cancel_one_exchange_order(item: tuple[dict, str, str, str]) -> dict:
        _o, sym, oid, reason = item
        try:
            chatgpt_log_event("cancel_old_pending_exchange_order_cancel_start", symbol=sym, order_id=oid, reason=reason)
            res = await exec_engine.exchange_client.cancel_order(oid, sym)
            chatgpt_log_event("cancel_old_pending_exchange_order_cancel_ok", symbol=sym, order_id=oid, result=res)
            return {"source": "exchange", "symbol": sym, "order_id": oid, "ok": True, "reason": reason, "result": res}
        except Exception as e:
            chatgpt_log_event("cancel_old_pending_exchange_order_cancel_error", symbol=sym, order_id=oid, error=str(e))
            return {"source": "exchange", "symbol": sym, "order_id": oid, "ok": False, "reason": reason, "error": str(e)}

    if cancel_candidates:
        # MEXC can rate-limit, so keep it parallel but bounded. This prevents a
        # 6-order stale TP/SL cleanup from taking ~30 seconds sequentially.
        sem = asyncio.Semaphore(int(os.getenv("CHATGPT_CANCEL_CONCURRENCY", "3") or 4))

        async def _bounded_cancel(item):
            async with sem:
                return await _cancel_one_exchange_order(item)

        results = await asyncio.gather(*[_bounded_cancel(x) for x in cancel_candidates], return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                cancelled.append({"source": "exchange", "ok": False, "error": str(r)})
            else:
                cancelled.append(r)

    # 3) Verify after cancel.
    try:
        await asyncio.sleep(float(os.getenv("CHATGPT_CANCEL_VERIFY_DELAY_SEC", "0.8") or 0.8))
        verify = await exec_engine.exchange_client.fetch_open_orders()
        leftovers = []
        for o in verify or []:
            sym = _order_symbol(o)
            # V39: verify only what we were allowed to cancel.
            # Old V38 logic treated ANY plan/open order for a non-live symbol as a
            # leftover, including unrelated symbols outside the new setup. That could
            # abort setup after a successful cleanup and could also make the bot look
            # inconsistent: cancel logic was setup-scoped, verify logic was global.
            # Entry orders are still checked globally; orphan planorders are checked
            # only for symbols from the new setup and only when no real position is
            # live for that symbol.
            is_setup_scoped_orphan = bool(sym and sym in setup_symbols and sym not in live_symbols)
            if _is_chatgpt_entry_order(o) or is_setup_scoped_orphan:
                leftovers.append({"symbol": sym, "order_id": _order_id(o)})
        if leftovers:
            chatgpt_log_event("cancel_old_pending_verify_still_exists_error", leftovers=leftovers[:20], count=len(leftovers))
            for item in leftovers:
                cancelled.append({"source": "verify", "symbol": item.get("symbol"), "order_id": item.get("order_id"), "ok": False, "error": "order_still_open_after_cancel"})
        else:
            chatgpt_log_event("cancel_old_pending_verify_after_cancel_ok")
    except Exception as e:
        chatgpt_log_event("cancel_old_pending_verify_after_cancel_error", error=str(e))

    return cancelled



def _chatgpt_order_symbol(o: dict) -> str:
    info = o.get("info") if isinstance(o.get("info"), dict) else {}
    return mexc_native_symbol(o.get("symbol") or info.get("symbol") or info.get("contract") or "")


def _chatgpt_order_id(o: dict) -> str:
    info = o.get("info") if isinstance(o.get("info"), dict) else {}
    return str(o.get("id") or info.get("orderId") or info.get("id") or info.get("planOrderId") or "").split(":", 1)[0].strip()


def _chatgpt_is_entry_order(o: dict, known_order_ids: set[str] | None = None) -> bool:
    """True only for ChatGPT entry LIMIT orders, never TP/SL/reduce-only.

    This is intentionally exchange-first: it reads the live order row and only
    uses known_order_ids after a live exchange snapshot has already been fetched.
    """
    known_order_ids = known_order_ids or set()
    info = o.get("info") if isinstance(o.get("info"), dict) else {}
    ext = str(info.get("externalOid") or info.get("clientOrderId") or o.get("clientOrderId") or "")
    oid = _chatgpt_order_id(o)
    reduce_only = str(o.get("reduceOnly") or info.get("reduceOnly") or "").lower() in {"1", "true", "yes"}
    prot = str(info.get("_protection_kind") or "").lower()
    src = str(info.get("_source_endpoint") or "").lower()
    if reduce_only or prot in {"tp", "sl"} or "stoporder" in src or "planorder" in src:
        return False
    # Exchange-source-of-truth mode: any active normal open-order row is an entry
    # candidate.  This intentionally catches old ChatGPT orders after redeploy
    # even when MEXC/ccxt did not return externalOid/clientOrderId.
    is_normal_open_order_endpoint = "order/list/open_orders" in src or src == ""
    return ext.startswith("bot_entry_") or (oid and oid in known_order_ids) or is_normal_open_order_endpoint


def _chatgpt_exchange_position_qty(row: dict) -> float:
    for key in ("contracts", "amount", "positionAmt"):
        try:
            v = row.get(key)
            if v not in (None, ""):
                q = abs(float(v))
                if q > 0:
                    return q
        except Exception:
            pass
    info = row.get("info") if isinstance(row.get("info"), dict) else {}
    for key in ("holdVol", "vol", "positionVol", "positionAmt", "amount", "contracts"):
        try:
            v = info.get(key)
            if v not in (None, ""):
                q = abs(float(v))
                if q > 0:
                    return q
        except Exception:
            pass
    return 0.0


def _chatgpt_exchange_position_symbol(row: dict) -> str:
    info = row.get("info") if isinstance(row.get("info"), dict) else {}
    return mexc_native_symbol(row.get("symbol") or row.get("mexc_symbol") or info.get("symbol") or info.get("contract") or "")


def _chatgpt_exchange_position_side(row: dict) -> str:
    side = str(row.get("side") or "").lower()
    info = row.get("info") if isinstance(row.get("info"), dict) else {}
    raw = str(info.get("positionType") or info.get("holdSide") or info.get("side") or side).lower()
    if side in {"short", "sell"} or raw in {"2", "short", "sell"} or "short" in raw:
        return "SHORT"
    return "LONG"


def _chatgpt_exchange_position_entry(row: dict) -> float:
    for key in ("entryPrice", "entry_price", "average"):
        try:
            v = row.get(key)
            if v not in (None, ""):
                f = float(v)
                if f > 0:
                    return f
        except Exception:
            pass
    info = row.get("info") if isinstance(row.get("info"), dict) else {}
    for key in ("holdAvgPrice", "openAvgPrice", "entryPrice", "avgPrice"):
        try:
            v = info.get(key)
            if v not in (None, ""):
                f = float(v)
                if f > 0:
                    return f
        except Exception:
            pass
    return 0.0


def _chatgpt_exchange_position_opened_at(row: dict) -> float:
    info = row.get("info") if isinstance(row.get("info"), dict) else {}
    for key in ("createTime", "openTime", "updateTime"):
        try:
            v = info.get(key)
            if v not in (None, ""):
                ts = float(v)
                return ts / 1000.0 if ts > 10_000_000_000 else ts
        except Exception:
            pass
    return time.time()


def _chatgpt_exchange_position_to_local(row: dict) -> dict | None:
    sym = _chatgpt_exchange_position_symbol(row)
    qty = _chatgpt_exchange_position_qty(row)
    if not sym or qty <= 0:
        return None
    entry = _chatgpt_exchange_position_entry(row)
    return {
        "symbol": sym,
        "side": _chatgpt_exchange_position_side(row),
        "status": "open",
        "entry_price": entry,
        "qty": qty,
        "stop_price": 0.0,
        "take_price": 0.0,
        "strategy": "chatgpt_setup",
        "order_id": str((row.get("info") or {}).get("positionId") or row.get("id") or ""),
        "opened_at": _chatgpt_exchange_position_opened_at(row),
        "chatgpt_exchange_reconciled": True,
        "exchange_source_of_truth": True,
        "raw_exchange_position": row,
    }


async def _fetch_exchange_chatgpt_snapshot(exchange_client, known_order_ids: set[str] | None = None) -> dict:
    """Fetch live MEXC state before setup handling.

    No local position cache is read here. This is the hard source of truth after
    redeploy/restart and prevents stale SQLite rows from duplicating slots.
    """
    known_order_ids = known_order_ids or set()
    chatgpt_log_event("exchange_first_snapshot_start")
    positions: list[dict] = []
    orders: list[dict] = []
    try:
        raw_positions = await exchange_client.fetch_positions()
        for row in raw_positions or []:
            rec = _chatgpt_exchange_position_to_local(row)
            if rec:
                positions.append(rec)
        chatgpt_log_event("exchange_first_positions_loaded", count=len(positions), symbols=[p.get("symbol") for p in positions])
    except Exception as e:
        chatgpt_log_event("exchange_first_positions_error", error=str(e))
        raise
    try:
        raw_orders = await exchange_client.fetch_open_orders()
        for o in raw_orders or []:
            if _chatgpt_is_entry_order(o, known_order_ids):
                orders.append(o)
        chatgpt_log_event("exchange_first_pending_loaded", count=len(orders), symbols=[_chatgpt_order_symbol(o) for o in orders])
    except Exception as e:
        chatgpt_log_event("exchange_first_pending_error", error=str(e))
        raise
    return {"positions": positions, "pending_orders": orders}


async def _reconcile_chatgpt_state_from_exchange(storage, exchange_client) -> dict:
    """Rebuild ChatGPT local cache from live exchange state.

    Important invariant for setup imports:
    - before this function completes, setup code must not use local cache;
    - after this function completes, local cache may be used because stale
      ChatGPT rows have been removed and live exchange rows have been written.
    """
    # The only local read before the exchange snapshot is order IDs to help
    # identify old bot_entry rows on exchanges that omit externalOid in wrappers.
    # Counts and slot decisions never use this pre-sync cache.
    known_order_ids: set[str] = set()
    try:
        for pos in await storage.positions():
            if str(pos.get("strategy") or "").lower() == "chatgpt_setup":
                oid = str(pos.get("order_id") or "").split(":", 1)[0].strip()
                if oid:
                    known_order_ids.add(oid)
    except Exception as e:
        chatgpt_log_event("exchange_first_known_ids_cache_error", error=str(e))
    snapshot = await _fetch_exchange_chatgpt_snapshot(exchange_client, known_order_ids)

    live_symbols = {p.get("symbol") for p in snapshot.get("positions", []) if p.get("symbol")}
    live_order_ids = {_chatgpt_order_id(o) for o in snapshot.get("pending_orders", []) if _chatgpt_order_id(o)}

    # Now the exchange snapshot is in hand; it is safe to clean stale local
    # ChatGPT rows and rebuild cache from live rows.
    removed = []
    try:
        for pos in await storage.positions():
            if str(pos.get("strategy") or "").lower() != "chatgpt_setup":
                continue
            sym = mexc_native_symbol(pos.get("symbol"))
            oid = str(pos.get("order_id") or "").split(":", 1)[0].strip()
            status = str(pos.get("status") or "").lower()
            if status == "pending":
                keep = bool(oid and oid in live_order_ids)
            else:
                keep = bool(sym and sym in live_symbols)
            if not keep and sym:
                await storage.remove_position(sym)
                removed.append({"symbol": sym, "order_id": oid, "status": status})
    except Exception as e:
        chatgpt_log_event("exchange_first_cleanup_error", error=str(e))

    upserted_positions = []
    for rec in snapshot.get("positions", []) or []:
        try:
            await storage.upsert_position(rec)
            upserted_positions.append(rec.get("symbol"))
        except Exception as e:
            chatgpt_log_event("exchange_first_upsert_position_error", symbol=rec.get("symbol"), error=str(e))

    upserted_pending = []
    for o in snapshot.get("pending_orders", []) or []:
        oid = _chatgpt_order_id(o)
        sym = _chatgpt_order_symbol(o)
        if not sym or not oid:
            continue
        info = o.get("info") if isinstance(o.get("info"), dict) else {}
        side_raw = str(o.get("side") or info.get("side") or "").lower()
        side = "SHORT" if side_raw in {"3", "sell", "short"} else "LONG"
        rec = {
            "symbol": sym,
            "side": side,
            "status": "pending",
            "order_type": "limit",
            "entry_price": _safe_float(o.get("price") or info.get("price")),
            "qty": _safe_float(o.get("amount") or info.get("vol")),
            "stop_price": 0.0,
            "take_price": 0.0,
            "strategy": "chatgpt_setup",
            "order_id": oid,
            "opened_at": time.time(),
            "chatgpt_exchange_reconciled": True,
            "exchange_source_of_truth": True,
            "raw_exchange_order": o,
        }
        try:
            await storage.upsert_position(rec)
            upserted_pending.append(sym)
        except Exception as e:
            chatgpt_log_event("exchange_first_upsert_pending_error", symbol=sym, order_id=oid, error=str(e))

    result = {
        "ok": True,
        "positions": snapshot.get("positions", []),
        "pending_orders": snapshot.get("pending_orders", []),
        "removed_stale": removed,
        "upserted_positions": upserted_positions,
        "upserted_pending": upserted_pending,
    }
    chatgpt_log_event(
        "exchange_first_reconcile_done",
        positions=len(result["positions"]),
        pending=len(result["pending_orders"]),
        removed=len(removed),
        upserted_positions=upserted_positions,
        upserted_pending=upserted_pending,
    )
    return result

async def _count_open_chatgpt_positions(storage) -> tuple[int, list[dict]]:
    """Count real active ChatGPT positions only.

    Pending LIMITs are not counted as position slots. They are cancelled before
    each new setup import and are governed separately by
    CHATGPT_MAX_PENDING_LIMITS.
    """
    active: list[dict] = []
    try:
        positions = await storage.positions()
    except Exception as e:
        chatgpt_log_event("slot_count_load_error", error=str(e))
        return 0, active

    for pos in positions or []:
        strategy = str(pos.get("strategy") or "").lower()
        status = str(pos.get("status") or "open").lower()
        if strategy != "chatgpt_setup":
            continue
        if status in {"open", "closing"}:
            active.append(pos)
        elif status == "pending":
            chatgpt_log_event("slot_count_pending_ignored_after_cancel", symbol=pos.get("symbol"), order_id=pos.get("order_id"), status=status)
    chatgpt_log_event("slot_count_open_positions", count=len(active), symbols=[p.get("symbol") for p in active])
    return len(active), active


def _chatgpt_tp1_reached(pos: dict) -> bool:
    """Best-effort TP1 marker for rotation safety.

    If TP1 has already been reached and the stop was moved to breakeven, the
    position is protected and should not be rotated out automatically.
    """
    for key in (
        "chatgpt_tp1_breakeven_done",
        "breakeven_moved",
        "tp1_hit",
        "partial_taken",
        "partial_take_done",
    ):
        if _truthy(pos.get(key), False):
            return True
    return False


def _chatgpt_position_pnl_pct_from_price(pos: dict, price: float) -> float:
    try:
        entry = float(pos.get("entry_price") or 0)
        if entry <= 0 or price <= 0:
            return 0.0
        side = str(pos.get("side") or "").upper()
        if side == "LONG":
            return (price - entry) / entry * 100.0
        return (entry - price) / entry * 100.0
    except Exception:
        return 0.0


async def _select_worst_chatgpt_rotation_position(open_positions: list[dict], exchange_client) -> dict | None:
    """Select at most one worst ChatGPT position for rotation.

    Candidates: TP1 not reached. Rank by worst PnL first, oldest as tie-break.
    """
    candidates: list[dict] = []
    for pos in open_positions or []:
        if _chatgpt_tp1_reached(pos):
            chatgpt_log_event("rotation_candidate_skip_tp1_reached", symbol=pos.get("symbol"), opened_at=pos.get("opened_at"))
            continue
        sym = str(pos.get("symbol") or "")
        price = 0.0
        try:
            ticker = await exchange_client.fetch_ticker(sym)
            price = _safe_float(ticker.get("last") or ticker.get("close") or ticker.get("bid") or ticker.get("ask"))
        except Exception as e:
            chatgpt_log_event("rotation_candidate_price_error", symbol=sym, error=str(e))
        pnl = _chatgpt_position_pnl_pct_from_price(pos, price)
        try:
            opened = float(pos.get("opened_at") or 0)
        except Exception:
            opened = 0.0
        row = dict(pos)
        row["_rotation_current_price"] = price
        row["_rotation_pnl_pct"] = pnl
        row["_rotation_opened_at"] = opened
        candidates.append(row)

    chatgpt_log_event(
        "rotation_candidates_found",
        count=len(candidates),
        candidates=[{
            "symbol": c.get("symbol"),
            "pnl_pct": round(float(c.get("_rotation_pnl_pct") or 0), 4),
            "opened_at": c.get("opened_at"),
        } for c in candidates[:20]],
    )
    if not candidates:
        return None
    candidates.sort(key=lambda c: (float(c.get("_rotation_pnl_pct") or 0), float(c.get("_rotation_opened_at") or 0)))
    worst = candidates[0]
    chatgpt_log_event(
        "rotation_worst_selected",
        symbol=worst.get("symbol"),
        pnl_pct=round(float(worst.get("_rotation_pnl_pct") or 0), 4),
        opened_at=worst.get("opened_at"),
        current_price=worst.get("_rotation_current_price"),
    )
    return worst


async def _rotate_one_chatgpt_position(storage, exec_engine: ExecutionEngine, open_positions: list[dict]) -> dict:
    """Close exactly one worst ChatGPT position when 6/6 positions are full.

    Rotation is allowed only on a new setup import and only when all 6 open
    position slots are occupied. If closing fails, no new setup limit is placed.
    """
    chatgpt_log_event("rotation_check_start", open_positions=len(open_positions), max_open_positions=CHATGPT_MAX_OPEN_POSITIONS)
    worst = await _select_worst_chatgpt_rotation_position(open_positions, exec_engine.exchange_client)
    if not worst:
        chatgpt_log_event("rotation_no_candidate", reason="all_positions_after_tp1_or_no_candidates")
        return {"ok": False, "rotated": False, "reason": "no_rotation_candidate"}
    try:
        chatgpt_log_event("rotation_close_start", symbol=worst.get("symbol"), pnl_pct=worst.get("_rotation_pnl_pct"), opened_at=worst.get("opened_at"))
        res = await exec_engine.close_position(worst, reason="chatgpt_rotation_new_setup", live=True)
        ok = bool((res or {}).get("ok"))
        if ok:
            chatgpt_log_event("rotation_close_ok", symbol=worst.get("symbol"), result=res)
            return {"ok": True, "rotated": True, "symbol": worst.get("symbol"), "pnl_pct": worst.get("_rotation_pnl_pct"), "result": res}
        chatgpt_log_event("rotation_close_error", symbol=worst.get("symbol"), result=res)
        return {"ok": False, "rotated": False, "symbol": worst.get("symbol"), "reason": "close_failed", "result": res}
    except Exception as e:
        chatgpt_log_event("rotation_close_error", symbol=worst.get("symbol"), error=str(e))
        return {"ok": False, "rotated": False, "symbol": worst.get("symbol"), "reason": f"close_exception: {e}"}


def _balance_total_usdt(balance: dict) -> float:
    for key in ("USDT", "usdt"):
        try:
            item = (balance.get("total") or {}).get(key)
            if item:
                return float(item)
        except Exception:
            pass
    try:
        return float((balance.get("info") or {}).get("totalWalletBalance") or 0)
    except Exception:
        return 0.0




def _chatgpt_protection_kind(o: dict) -> str:
    info = o.get("info") if isinstance(o.get("info"), dict) else {}
    kind = str(info.get("_protection_kind") or "").lower()
    typ = str(o.get("type") or info.get("orderType") or info.get("type") or "").lower()
    src = str(info.get("_source_endpoint") or "").lower()
    txt = " ".join(str(info.get(k) or "").lower() for k in ("externalOid", "clientOrderId", "side", "orderType", "triggerType", "type"))
    if kind in {"tp", "sl"}:
        return kind
    # V32: ChatGPT protection is placed through MEXC planorder/place with
    # externalOid bot_tp_* / bot_sl_*. MEXC list endpoints often return orderType=5
    # for both TP and SL, so side/orderType alone cannot classify them. The bot
    # must read the explicit externalOid marker first, otherwise real TP orders are
    # shown as missing and the monitor enters false LOCAL PROTECTION MODE.
    if "bot_tp" in txt or "chatgpt_tp" in txt or "takeprofit" in txt or "take_profit" in txt or "tp" == typ or "tp" in typ:
        return "tp"
    if "bot_sl" in txt or "chatgpt_sl" in txt or "stoploss" in txt or "stop_loss" in txt or "sl" == typ or "sl" in typ:
        return "sl"
    # If no bot marker survived in the MEXC row, use the plan trigger direction
    # as a best-effort classifier: side=4 closes LONG, triggerType=1 is TP
    # (price >= trigger), triggerType=2 is SL. side=2 closes SHORT, inverse.
    if "planorder" in src or "stoporder" in src:
        side = str(info.get("side") or o.get("side") or "")
        trig = str(info.get("triggerType") or info.get("trigger_type") or "")
        if side == "4":
            if trig == "1":
                return "tp"
            if trig == "2":
                return "sl"
        if side == "2":
            if trig == "2":
                return "tp"
            if trig == "1":
                return "sl"
    return ""


def _chatgpt_group_protection(positions: list[dict], all_orders: list[dict]) -> tuple[list[str], list[str]]:
    protected, emergency = [], []
    by_symbol: dict[str, set[str]] = {}
    for o in all_orders or []:
        sym = _chatgpt_order_symbol(o)
        if not sym:
            continue
        kind = _chatgpt_protection_kind(o)
        if kind:
            by_symbol.setdefault(sym, set()).add(kind)
    for p in positions or []:
        sym = mexc_native_symbol(p.get("symbol"))
        kinds = by_symbol.get(sym, set())
        if "sl" in kinds and "tp" in kinds:
            protected.append(sym)
        else:
            missing = []
            if "sl" not in kinds:
                missing.append("SL")
            if "tp" not in kinds:
                missing.append("TP")
            emergency.append(f"{sym} — {'/'.join(missing)} missing")
    return protected, emergency




def _chatgpt_cleanup_summary(items: list[dict] | None) -> dict:
    """Summarize setup cleanup in user-facing terms.

    entry = normal pending entry LIMIT orders.
    orphan = old conditional/plan orders for setup symbols that have no live
    position anymore.  These are garbage TP/SL leftovers, not real лимитки.
    """
    summary = {
        "entry_found": 0,
        "entry_cancelled": 0,
        "entry_left": 0,
        "orphan_found": 0,
        "orphan_cancelled": 0,
        "orphan_left": 0,
        "other_failed": 0,
    }
    seen: set[tuple[str, str, str]] = set()
    for x in items or []:
        reason = str(x.get("reason") or "entry").lower()
        if reason not in {"entry", "orphan"}:
            reason = "entry" if str(x.get("source") or "").lower() in {"local", "exchange"} else "other"
        key = (reason, str(x.get("symbol") or ""), str(x.get("order_id") or ""))
        if key in seen:
            continue
        seen.add(key)
        ok = bool(x.get("ok"))
        if reason == "orphan":
            summary["orphan_found"] += 1
            if ok:
                summary["orphan_cancelled"] += 1
            else:
                summary["orphan_left"] += 1
        elif reason == "entry":
            summary["entry_found"] += 1
            if ok:
                summary["entry_cancelled"] += 1
            else:
                summary["entry_left"] += 1
        else:
            if not ok:
                summary["other_failed"] += 1
    summary["total_found"] = summary["entry_found"] + summary["orphan_found"]
    summary["total_cancelled"] = summary["entry_cancelled"] + summary["orphan_cancelled"]
    summary["total_left"] = summary["entry_left"] + summary["orphan_left"] + summary["other_failed"]
    return summary

async def build_chatgpt_monitor_text(storage, exchange_client, setup_result: dict | None = None) -> str:
    """Build one exchange-source-of-truth monitor card for Telegram.

    Counts and protection state are fetched from MEXC every time. Local cache is
    not used as source of truth for positions/orders. The last setup execution
    result is persisted only for the user-facing monitor summary, so periodic
    refreshes do not revert to "ожидаю setup-файл" and do not lose the cleanup /
    skipped reasons after the file has already been processed.
    """
    if setup_result is None:
        try:
            waiting_for_new_setup = bool(await storage.get("chatgpt_waiting_setup", False))
        except Exception:
            waiting_for_new_setup = False
        if not waiting_for_new_setup:
            try:
                saved = await storage.get("chatgpt_last_setup_result", None)
                if isinstance(saved, dict) and saved.get("_monitor_persist"):
                    setup_result = saved
            except Exception:
                setup_result = None
    known: set[str] = set()
    try:
        for pos in await storage.positions():
            if str(pos.get("strategy") or "").lower() == "chatgpt_setup":
                oid = str(pos.get("order_id") or "").split(":", 1)[0].strip()
                if oid:
                    known.add(oid)
    except Exception:
        pass
    snap = await _fetch_exchange_chatgpt_snapshot(exchange_client, known)
    positions = snap.get("positions") or []
    pending = snap.get("pending_orders") or []
    try:
        all_orders = await exchange_client.fetch_open_orders()
    except Exception:
        all_orders = []
    protected, emergency = _chatgpt_group_protection(positions, all_orders)
    res = setup_result or {}
    opened = res.get("opened") if isinstance(res.get("opened"), list) else []
    placed = len([x for x in opened if x.get("ok")])
    skipped = [x for x in opened if not x.get("ok")]
    requested = res.get("requested_trades", res.get("limits_to_place", "-"))
    cancelled = res.get("cancelled_pending_count", 0) if setup_result is not None else 0
    cleanup = res.get("cleanup_summary") or {"entry_found": cancelled, "entry_cancelled": cancelled, "entry_left": 0, "orphan_found": 0, "orphan_cancelled": 0, "orphan_left": 0}
    setup_status = "setup-файл обработан" if setup_result is not None and bool(res.get("ok", True)) else ("setup-файл НЕ исполнен" if setup_result is not None else "ожидаю setup-файл")
    setup_time = str(res.get("setup_installed_at") or res.get("setup_received_at") or "-")
    lines = [
        "🤖 ChatGPT Mode Monitor",
        f"✅ {setup_status}" if setup_result is not None and bool(res.get("ok", True)) else f"❌ {setup_status}" if setup_result is not None else f"📥 {setup_status}",
        "🔄 биржа синхронизирована",
        f"📊 открытые позиции: {len(positions)}/{CHATGPT_MAX_OPEN_POSITIONS}",
        f"📌 pending лимитки: {len(pending)}/{CHATGPT_MAX_PENDING_LIMITS}",
        "",
        "🛡 полная защита:",
    ]
    if protected:
        lines += [f"• {x} — SL + TP" for x in protected]
    else:
        lines.append("• нет подтверждённых")
    lines += ["", "🚨 аварийные позиции:"]
    if emergency:
        lines += [f"• {x}" for x in emergency]
    else:
        lines.append("• нет")
    if setup_result is not None:
        lines += [
            "",
            "🧹 очистка перед setup:",
            f"• entry-лимитки: найдено {cleanup.get('entry_found', 0)}, снято {cleanup.get('entry_cancelled', cancelled)}, осталось {cleanup.get('entry_left', 0)}",
            f"• старые условные ордера без позиции: найдено {cleanup.get('orphan_found', 0)}, снято {cleanup.get('orphan_cancelled', 0)}, осталось {cleanup.get('orphan_left', 0)}",
            f"📄 к установке из setup: {requested}/{CHATGPT_MAX_PENDING_LIMITS}",
            f"✅ поставлено новых входов: {placed}",
            f"❌ пропущено: {len(skipped)}",
        ]
        if skipped:
            for row in skipped[:6]:
                reason = row.get("reason") or (row.get("result") or {}).get("reason") or "unknown"
                rlow = str(reason).lower()
                if "duplicate order id" in rlow or "2042" in rlow:
                    reason = "entry не поставлена: duplicate clientOrderId, повтор не помог"
                elif "symbol locked: chatgpt_new_setup_cancel_old_pending" in rlow:
                    reason = "entry не поставлена: старый pending снят, но символ был ошибочно заблокирован"
                lines.append(f"• {row.get('symbol')}: {reason}")
        else:
            lines.append("• нет")
        if res.get("message"):
            lines.append(f"status: {res.get('message')}")
    lines += [
        "",
        "🔁 сопровождение включено",
        f"⏳ LIMIT живут {CHATGPT_LIMIT_TTL_MINUTES} мин, MARKET исполняется сразу, установка: {setup_time}",
        f"⏱ проверка каждые {CHATGPT_MONITOR_INTERVAL_SEC} сек",
        f"🕒 последнее обновление: {_now_chatgpt_display()}",
    ]
    return "\n".join(lines)[:3900]

async def execute_setup(storage, exchange_client, setup: dict) -> dict:
    chatgpt_log_event("setup_execute_start")
    try:
        guard_enabled = str(os.getenv("CHATGPT_SETUP_SYMBOL_GUARD_ENABLED", "true")).lower() in {"1", "true", "yes", "on"}
        if guard_enabled and isinstance(setup, dict):
            manifest = await storage.get("chatgpt_last_scan_manifest", {})
            source = (manifest or {}).get("source") if isinstance(manifest, dict) else None
            guard_mode = (manifest or {}).get("symbol_guard_mode") if isinstance(manifest, dict) else None
            selected_count = len((manifest or {}).get("selected_symbols") or []) if isinstance(manifest, dict) else 0
            allowed = sorted(_chatgpt_pack_allowed_symbols_from_manifest(manifest))
            if allowed:
                setup["_scan_pack_allowed_symbols"] = allowed
                chatgpt_log_event(
                    "setup_symbol_guard_enabled",
                    source=source or "scan_pack",
                    guard_mode=guard_mode or "scan_pack_selected_symbols",
                    selected_count=selected_count,
                    count=len(allowed),
                    sample=",".join(allowed[:30]),
                )
            else:
                chatgpt_log_event("setup_symbol_guard_no_manifest")
        else:
            chatgpt_log_event("setup_symbol_guard_disabled")
    except Exception as e:
        chatgpt_log_event("setup_symbol_guard_error", error=repr(e))
    trades = validate_setup(setup)
    if not trades:
        chatgpt_log_event("setup_execute_no_trade")
        return {"ok": True, "opened": [], "message": "NO_TRADE"}
    # V28 hard rule: after redeploy/restart, never trust local SQLite/cache for
    # ChatGPT slots until a live exchange-first reconciliation has completed.
    # This prevents stale local rows from duplicating slots and prevents missing
    # live positions (for example SKYAI) from being counted as 0/6.
    exec_engine = ExecutionEngine(storage, exchange_client)
    reconcile = await _reconcile_chatgpt_state_from_exchange(storage, exchange_client)

    bal = await exchange_client.fetch_balance()
    equity = _balance_total_usdt(bal)
    if equity <= 0:
        equity = float(os.getenv("DEFAULT_EQUITY_USDT", "100") or 100)
    margin_pct = float(setup.get("default_margin_percent_per_trade") or 10) / 100.0
    leverage = int(float(setup.get("default_leverage") or 10))
    opened = []
    live = True
    cancelled_pending = await _cancel_all_old_pending_limits(storage, exec_engine, {str(t.get("symbol") or "") for t in trades})
    cleanup_summary = _chatgpt_cleanup_summary(cancelled_pending)
    if cancelled_pending:
        chatgpt_log_event("setup_cancelled_old_pending_limits", items=cancelled_pending)

    # v0391: when a new setup replaces an old pending LIMIT for the same symbol,
    # the cancel monitor can briefly create a `limit_canceled` lock. That lock is
    # correct for normal expired/cancelled orders, but it must not block the fresh
    # replacement inside this same setup cycle.  Clear only successful ENTRY
    # cleanup locks for symbols that are present in the new setup.  Do not clear
    # restricted/exchange-error locks such as mexc_opening_restricted_8950.
    replacement_unlocked_symbols: set[str] = set()
    for item in cancelled_pending or []:
        try:
            sym = mexc_native_symbol(item.get("symbol"))
            is_ok = bool(item.get("ok"))
            is_entry_cleanup = str(item.get("reason") or "").lower() == "entry"
            if is_ok and is_entry_cleanup and sym and sym in {mexc_native_symbol(t.get("symbol")) for t in trades}:
                if hasattr(storage, "clear_lock"):
                    await storage.clear_lock(sym)
                else:
                    await storage.set_lock(sym, 0, "chatgpt_replacement_unlock")
                replacement_unlocked_symbols.add(sym)
                chatgpt_log_event(
                    "setup_symbol_unlock_after_cleanup",
                    symbol=sym,
                    reason="replacing_old_pending_from_new_setup",
                    source=item.get("source"),
                    order_id=item.get("order_id"),
                )
        except Exception as e:
            chatgpt_log_event("setup_symbol_unlock_after_cleanup_error", item=item, error=str(e))

    chatgpt_log_event("setup_cleanup_summary", **cleanup_summary)
    cancel_failed = [x for x in (cancelled_pending or []) if not bool(x.get("ok"))]
    if cancel_failed:
        # Hard safety gate: never place new setup limits while an old ChatGPT
        # entry limit is still open or its cancellation was not verified.
        chatgpt_log_event("setup_abort_old_pending_cancel_failed", failed=cancel_failed[:20], count=len(cancel_failed))
        return {"ok": False, "opened": [], "message": "OLD_PENDING_CANCEL_FAILED", "failed_cancelled_pending": cancel_failed, "cleanup_summary": cleanup_summary, "reconcile": reconcile}

    # Re-sync after cancellation. From here local cache may be used because it is
    # rebuilt from real-time exchange state, not from stale pre-deploy data.
    reconcile_after_cancel = await _reconcile_chatgpt_state_from_exchange(storage, exchange_client)

    open_slots, open_positions = await _count_open_chatgpt_positions(storage)

    rotation_result: dict | None = None
    limits_to_place = min(CHATGPT_MAX_PENDING_LIMITS, len(trades))

    if open_slots >= CHATGPT_MAX_OPEN_POSITIONS:
        # Full book: rotate exactly one worst ChatGPT position and then allow
        # exactly one new pending limit from the setup. Never rotate more than
        # one position per setup file.
        rotation_result = await _rotate_one_chatgpt_position(storage, exec_engine, open_positions)
        if not rotation_result.get("ok"):
            chatgpt_log_event("setup_abort_rotation_failed", rotation=rotation_result)
            return {
                "ok": False,
                "opened": [],
                "message": "ROTATION_FAILED",
                "rotation": rotation_result,
                "open_positions": open_slots,
                "max_open_positions": CHATGPT_MAX_OPEN_POSITIONS,
                "reconcile": reconcile_after_cancel,
            }
        limits_to_place = 1
        # Re-count after successful close so duplicate-symbol filtering below sees
        # the updated local state.
        open_slots, open_positions = await _count_open_chatgpt_positions(storage)
    else:
        free_position_capacity = max(0, CHATGPT_MAX_OPEN_POSITIONS - open_slots)
        limits_to_place = min(CHATGPT_MAX_PENDING_LIMITS, free_position_capacity, len(trades))

    original_requested_trades = int(setup.get("_requested_trades_total") or len(trades))
    open_symbols = {mexc_native_symbol(p.get("symbol")) for p in (open_positions or []) if p.get("symbol")}
    filtered_trades = []
    skipped_open_symbol = []
    preplace_skipped: list[dict] = list(setup.get("_duplicate_skipped") or [])
    for t in trades:
        sym = mexc_native_symbol(t.get("symbol"))
        if sym in open_symbols:
            skipped_open_symbol.append(sym)
            reason = f"уже есть открытая позиция: {sym}"
            preplace_skipped.append({"symbol": sym, "ok": False, "reason": reason, "skipped_existing_position": True})
            chatgpt_log_event("setup_trade_skipped_existing_position", symbol=sym, reason=reason)
            continue
        filtered_trades.append(t)
    trades = filtered_trades

    requested_trades = original_requested_trades
    if limits_to_place <= 0:
        chatgpt_log_event(
            "setup_no_limit_capacity",
            max_open_positions=CHATGPT_MAX_OPEN_POSITIONS,
            max_pending_limits=CHATGPT_MAX_PENDING_LIMITS,
            open_positions=open_slots,
            skipped_open_symbol=skipped_open_symbol,
        )
        no_slot_reason = f"нет свободного общего слота: {open_slots}/{CHATGPT_MAX_TOTAL_SLOTS}"
        capacity_skipped = [
            {"symbol": mexc_native_symbol(t.get("symbol")), "ok": False, "reason": no_slot_reason, "skipped_no_total_slot": True}
            for t in trades
        ]
        return {
            "ok": True,
            "opened": preplace_skipped + capacity_skipped,
            "message": "NO_LIMIT_CAPACITY",
            "rotation": rotation_result,
            "open_positions": open_slots,
            "max_open_positions": CHATGPT_MAX_OPEN_POSITIONS,
            "skipped_open_symbol": skipped_open_symbol,
            "reconcile": reconcile_after_cancel,
            "requested_trades": requested_trades,
            "cleanup_summary": cleanup_summary,
        }
    if len(trades) > limits_to_place:
        skipped = trades[limits_to_place:]
        no_slot_reason = f"нет свободного общего слота: {CHATGPT_MAX_TOTAL_SLOTS}/{CHATGPT_MAX_TOTAL_SLOTS}"
        preplace_skipped.extend([
            {"symbol": mexc_native_symbol(t.get("symbol")), "ok": False, "reason": no_slot_reason, "skipped_no_total_slot": True}
            for t in skipped
        ])
        trades = trades[:limits_to_place]
        chatgpt_log_event(
            "setup_trades_trimmed_to_limit_capacity",
            max_open_positions=CHATGPT_MAX_OPEN_POSITIONS,
            max_pending_limits=CHATGPT_MAX_PENDING_LIMITS,
            open_positions=open_slots,
            limits_to_place=limits_to_place,
            requested_trades=requested_trades,
            accepted_trades=len(trades),
            skipped_symbols=[t.get("symbol") for t in skipped],
            skipped_reason=no_slot_reason,
            skipped_open_symbol=skipped_open_symbol,
        )

    chatgpt_log_event("setup_execute_balance", equity=equity, margin_pct=margin_pct, leverage=leverage, trades=len(trades), requested_trades=requested_trades, cancelled_pending=len(cancelled_pending), open_positions=open_slots, limits_to_place=limits_to_place, max_pending_limits=CHATGPT_MAX_PENDING_LIMITS, max_open_positions=CHATGPT_MAX_OPEN_POSITIONS, rotation=rotation_result)

    def _build_plan(t: dict) -> TradePlan:
        entry = float(t["entry"])
        notional = equity * margin_pct * leverage
        qty = notional / entry
        tps = t["take_profits"]
        final_tp = float(tps[-1]["price"])
        return TradePlan(
            symbol=t["symbol"],
            side=t["direction"],
            order_type=str(t["order_type"]).lower(),
            qty=qty,
            entry_price=entry,
            stop_price=float(t["stop_loss"]),
            take_price=final_tp,
            risk_pct=0.0,
            confidence=float(t.get("confidence") or 0),
            strategy="chatgpt_setup",
            max_open_positions=CHATGPT_MAX_OPEN_POSITIONS,
            planned_notional_usdt=notional,
            expected_margin_usdt=equity * margin_pct,
            max_margin_per_position_usdt=equity * margin_pct,
            leverage=leverage,
            signal_details={
                "chatgpt_take_profits": tps,
                "invalidation": str(t.get("invalidation") or ""),
                "comment": str(t.get("comment") or ""),
                "cancel_if_not_filled_minutes": CHATGPT_LIMIT_TTL_MINUTES,
                "cancel_if_tp2_before_entry": _truthy(t.get("cancel_if_tp2_before_entry", t.get("cancel_if_tp1_before_entry", True))),
                "cancel_if_stop_before_entry": _truthy(t.get("cancel_if_stop_before_entry", False)),
                "max_price_deviation_percent": float(t.get("max_price_deviation_percent") or 0),
                "risk": t.get("risk") or {},
                "trade_management": {
                    "trailing_enabled": False,
                    "scalp_exit_enabled": False,
                    "paper_fill_enabled": False,
                    "breakeven_after_tp1_only": True,
                    "breakeven_price": "ENTRY",
                },
            },
        )

    async def _place_one_setup_trade(t: dict) -> dict:
        plan = _build_plan(t)
        entry = float(plan.entry_price)
        final_tp = float(plan.take_price)
        stop_price = float(plan.stop_price)
        tp_rows = plan.signal_details.get("chatgpt_take_profits") or []
        tp1_price = _safe_float((tp_rows[0] or {}).get("price") if tp_rows else 0)
        tp2_price = _safe_float((tp_rows[1] or {}).get("price") if isinstance(tp_rows, list) and len(tp_rows) > 1 else 0)
        # v0381 safety gate: before entry placement, reject only if the setup SL
        # has already been crossed OR TP2 has already been reached. TP1 touch is
        # allowed, because otherwise normal pullback setups are skipped too often.
        try:
            cur = await _fetch_chatgpt_current_price(exchange_client, plan.symbol)
            if _chatgpt_stop_breached(plan.side, cur, stop_price):
                side_word = "ниже" if str(plan.side).upper() == "LONG" else "выше"
                reason = f"цена уже {side_word} стопа: current={_fmt(cur)}, stop={_fmt(stop_price)}; лимитка не выставлена в целях безопасности"
                chatgpt_log_event(
                    "setup_trade_rejected_stop_already_breached",
                    symbol=plan.symbol,
                    side=plan.side,
                    order_type=plan.order_type,
                    current_price=cur,
                    entry=entry,
                    stop=stop_price,
                    tp1=tp1_price,
                    tp2=tp2_price,
                    reason=reason,
                )
                return {"symbol": plan.symbol, "side": plan.side, "order_type": plan.order_type, "entry": entry, "ok": False, "reason": reason, "safety_stop_breached": True, "current_price": cur, "stop": stop_price, "tp1": tp1_price, "tp2": tp2_price}
            if _chatgpt_tp2_already_touched(plan.side, cur, tp2_price):
                side_word = "выше TP2" if str(plan.side).upper() == "LONG" else "ниже TP2"
                reason = f"цена уже {side_word}: current={_fmt(cur)}, tp2={_fmt(tp2_price)}; лимитка не выставлена в целях безопасности"
                chatgpt_log_event(
                    "setup_trade_rejected_tp2_already_touched",
                    symbol=plan.symbol,
                    side=plan.side,
                    order_type=plan.order_type,
                    current_price=cur,
                    entry=entry,
                    stop=stop_price,
                    tp1=tp1_price,
                    tp2=tp2_price,
                    reason=reason,
                )
                return {"symbol": plan.symbol, "side": plan.side, "order_type": plan.order_type, "entry": entry, "ok": False, "reason": reason, "safety_tp2_touched": True, "current_price": cur, "stop": stop_price, "tp1": tp1_price, "tp2": tp2_price}
        except Exception as e:
            reason = f"не смог проверить текущую цену перед входом: {e}; лимитка не выставлена в целях безопасности"
            chatgpt_log_event("setup_trade_rejected_price_safety_check_failed", symbol=plan.symbol, side=plan.side, entry=entry, stop=stop_price, tp1=tp1_price, tp2=tp2_price, error=str(e))
            return {"symbol": plan.symbol, "side": plan.side, "order_type": plan.order_type, "entry": entry, "ok": False, "reason": reason, "price_safety_check_failed": True}

        # Market stale-price guard. LIMIT entries are already handled by expiry/TP-before-entry rules.
        if plan.order_type == "market":
            max_dev = float(plan.signal_details.get("max_price_deviation_percent") or 0.7)
            try:
                ticker = await exchange_client.fetch_ticker(plan.symbol)
                cur = _safe_float(ticker.get("last") or ticker.get("close"))
                if cur > 0 and abs(cur - entry) / entry * 100.0 > max_dev:
                    reason = f"market price deviation > {max_dev}%"
                    chatgpt_log_event("setup_trade_rejected", symbol=plan.symbol, reason=reason, current_price=cur, entry=entry)
                    return {"symbol": plan.symbol, "ok": False, "reason": reason}
            except Exception as e:
                reason = f"ticker check failed: {e}"
                chatgpt_log_event("setup_trade_rejected", symbol=plan.symbol, reason=reason)
                return {"symbol": plan.symbol, "ok": False, "reason": reason}

        chatgpt_log_event("setup_trade_place_start", symbol=plan.symbol, side=plan.side, order_type=plan.order_type, entry=entry, qty=plan.qty, stop=plan.stop_price, final_tp=final_tp)
        res = await exec_engine.place_entry(plan, live=live)
        reason_text = str((res or {}).get("reason") or "").lower()

        # v0391 second safety net: if the position monitor created a `limit_canceled`
        # lock after cleanup but before this entry placement, clear it and retry once
        # only for symbols that were successfully cancelled for replacement in this
        # setup cycle.  This is the real fix for: old ONDO pending cancelled -> new
        # ONDO setup skipped as `symbol locked: limit_canceled`.
        if (
            not bool((res or {}).get("ok"))
            and plan.symbol in replacement_unlocked_symbols
            and "symbol locked" in reason_text
            and "limit_canceled" in reason_text
        ):
            chatgpt_log_event(
                "setup_symbol_lock_cleared_for_replacement",
                symbol=plan.symbol,
                first_reason=(res or {}).get("reason"),
                action="clear_lock_and_retry_once",
            )
            try:
                if hasattr(storage, "clear_lock"):
                    await storage.clear_lock(plan.symbol)
                else:
                    await storage.set_lock(plan.symbol, 0, "chatgpt_replacement_unlock_retry")
                res = await exec_engine.place_entry(plan, live=live)
                reason_text = str((res or {}).get("reason") or "").lower()
                chatgpt_log_event("setup_symbol_lock_retry_after_cleanup_result", symbol=plan.symbol, result=res)
            except Exception as e:
                res = {"ok": False, "reason": f"replacement unlock retry failed: {e}"}
                reason_text = str(res.get("reason") or "").lower()
                chatgpt_log_event("setup_symbol_lock_retry_after_cleanup_error", symbol=plan.symbol, error=str(e))

        if (not bool((res or {}).get("ok"))) and "open" in reason_text and "order" in reason_text and "exchange" in reason_text:
            chatgpt_log_event("setup_trade_open_order_conflict_retry_start", symbol=plan.symbol, first_result=res)
            cleanup = await exec_engine.cancel_same_symbol_stale_orders(plan.symbol, reason="chatgpt_setup_retry_open_order_exists")
            if cleanup.get("ok"):
                res2 = await exec_engine.place_entry(plan, live=live)
                chatgpt_log_event("setup_trade_open_order_conflict_retry_result", symbol=plan.symbol, cleanup=cleanup, retry_result=res2)
                res = res2
            else:
                chatgpt_log_event("setup_trade_open_order_conflict_cleanup_failed", symbol=plan.symbol, cleanup=cleanup)
                res = {"ok": False, "reason": "open order exists on exchange; same-symbol cleanup failed", "cleanup": cleanup, "first_result": res}
        ok_flag = bool((res or {}).get("ok"))
        reason_value = (res or {}).get("reason") or (res or {}).get("error") or ""
        chatgpt_log_event("setup_trade_place_result", symbol=plan.symbol, ok=ok_flag, reason=reason_value, result=res)
        return {"symbol": plan.symbol, "side": plan.side, "order_type": plan.order_type, "entry": entry, "ok": ok_flag, "reason": reason_value, "result": res}

    # Pre-placement skips (existing open position / no total slot) must remain
    # visible in the final monitor instead of disappearing from the report.
    opened.extend(preplace_skipped)

    # v0385 SAFETY FIX: ChatGPT setup entries are private trading operations,
    # so place them strictly one-by-one.  Parallel entry placement caused MEXC
    # private requests (leverage/order/margin/protection) to overlap and return
    # misleading Duplicate order ID / lock states.  Scanning can stay parallel,
    # but setup execution must be deterministic.
    if trades:
        chatgpt_log_event("setup_trade_place_sequence_start", trades=len(trades), symbols=[t.get("symbol") for t in trades])
        for idx, t in enumerate(trades, start=1):
            sym = str(t.get("symbol") or "")
            try:
                chatgpt_log_event("setup_trade_place_sequence_item_start", index=idx, symbol=sym)
                r = await _place_one_setup_trade(t)
                opened.append(r)
                chatgpt_log_event("setup_trade_place_sequence_item_done", index=idx, symbol=sym, result=r)
                # Give MEXC a short breath between private trading operations so
                # order/leverage/protection requests do not collide internally.
                await asyncio.sleep(float(os.getenv("CHATGPT_SETUP_SEQUENCE_DELAY_SEC", "1.2")))
            except Exception as e:
                chatgpt_log_event("setup_trade_place_exception", symbol=sym, error=str(e))
                opened.append({"symbol": sym, "ok": False, "reason": str(e)})
                await asyncio.sleep(float(os.getenv("CHATGPT_SETUP_SEQUENCE_DELAY_SEC", "1.2")))
        chatgpt_log_event("setup_trade_place_sequence_done", opened=opened)
    placed_count = len([x for x in opened if isinstance(x, dict) and bool(x.get("ok"))])
    skipped_rows = [x for x in opened if isinstance(x, dict) and not bool(x.get("ok"))]
    chatgpt_log_event("setup_execute_done", opened=opened, rotation=rotation_result)
    chatgpt_log_event(
        "setup_execute_summary",
        requested_trades=requested_trades,
        limits_to_place=limits_to_place,
        placed=placed_count,
        skipped=len(skipped_rows),
        skipped_rows=skipped_rows[:20],
        cleanup_summary=cleanup_summary,
        open_positions=open_slots,
        max_open_positions=CHATGPT_MAX_OPEN_POSITIONS,
        max_pending_limits=CHATGPT_MAX_PENDING_LIMITS,
    )
    return {
        "ok": True,
        "opened": opened,
        "rotation": rotation_result,
        "cancelled_pending_count": len([x for x in (cancelled_pending or []) if bool(x.get("ok"))]),
        "cleanup_summary": cleanup_summary,
        "open_positions": open_slots,
        "max_open_positions": CHATGPT_MAX_OPEN_POSITIONS,
        "max_pending_limits": CHATGPT_MAX_PENDING_LIMITS,
        "limits_to_place": limits_to_place,
        "requested_trades": requested_trades,
        "reconcile": reconcile_after_cancel,
    }
