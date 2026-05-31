import asyncio
import json
import math
import os
import time
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


CHATGPT_RUNTIME_LOG_PATH = Path(os.getenv("CHATGPT_RUNTIME_LOG_PATH", "/tmp/chatgpt_mode_runtime.log"))
CHATGPT_RUNTIME_LOG_MAX_BYTES = int(os.getenv("CHATGPT_RUNTIME_LOG_MAX_BYTES", "700000") or 700000)


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
            safe[k] = txt[:2000]
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

    For stale LIMIT setups, the dangerous case is a fast move through the
    planned stop before the bot places the entry order.  We prefer mark/index
    prices when MEXC exposes them, then last/close/bid/ask.  Any failure is
    surfaced to the caller so the setup row can be skipped safely instead of
    placing a blind order.
    """
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
            return price
    raise ValueError("ticker has no usable current price")


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


def _chatgpt_tp1_already_touched(direction: str, current_price: float, tp1_price: float) -> bool:
    """True when the market already reached TP1 before entry placement.

    LONG is stale if current >= TP1.  SHORT is stale if current <= TP1.
    If TP1 has already been touched, risk/reward is no longer the setup that
    ChatGPT approved, so the LIMIT must be skipped for safety.
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
    await scanner.refresh_symbols(exchange_client, scan_settings, ws_supervisor)
    raw_symbols = list(dict.fromkeys(getattr(scanner, "hot_symbols", []) or []))
    filtered_symbols, blocked_symbols = filter_chatgpt_symbols(raw_symbols)
    symbols = mexc_native_symbols(filtered_symbols)[:int(limit)]
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
    blocks = await asyncio.gather(*(guarded(s) for s in symbols), return_exceptions=True)
    ok_blocks = 0
    err_blocks = 0
    for b in blocks:
        txt = str(b)
        if isinstance(b, Exception) or "\nERROR:" in txt:
            err_blocks += 1
        else:
            ok_blocks += 1
    chatgpt_log_event("scan_workers_done", workers=workers, symbols=len(symbols), ok=ok_blocks, errors=err_blocks, elapsed_sec=round(time.time() - scan_started_at, 2))

    btc = ""
    eth = ""
    try:
        btc = await one_symbol("BTC_USDT")
    except Exception:
        pass
    try:
        eth = await one_symbol("ETH_USDT")
    except Exception:
        pass

    task = 'ЗАДАЧА ДЛЯ CHATGPT MODE:\n\nТы анализируешь торговый log.txt для выбора сделок на MEXC Futures.\n\nВАЖНО:\nНе проси сразу 15m / 1H / 4H по всем монетам.\nНе проси 20–30 скриншотов сразу.\nСкриншоты браузером бот пока НЕ делает.\nСначала нужен только 4H.\n\nЗАПРЕЩЁННЫЕ ИНСТРУМЕНТЫ:\nНе рассматривай и не добавляй в setup любые инструменты, где в символе есть STOCK.\nПримеры: MSFTSTOCK_USDT, DELLSTOCK_USDT, IBMSTOCK_USDT, SPCXSTOCK_USDT.\nТакие инструменты регионально блокируются и не могут быть открыты ботом.\n\nПОРЯДОК РАБОТЫ:\n\n1. Проанализируй весь log.txt.\n   Используй все метрики скана: 15m / 1H / 4H, объём, ликвидность,\n   RSI, MACD, MA7/MA25/MA99, orderbook, движение, силу/слабость,\n   риск перегрева и общий фон BTC/ETH.\n   Символы со STOCK не рассматривать.\n\n2. Выбери 10 лучших инструментов-кандидатов для ручной проверки по графикам.\n   В эти 10 инструментов нельзя включать символы со STOCK.\n\n3. Сначала попроси у пользователя только 10 скриншотов 4H:\n   по одному 4H-графику на каждый выбранный инструмент.\n\n4. После получения 4H-графиков оставь 5 лучших кандидатов\n   и попроси по ним графики на 1H:\n   по одному 1H-графику на каждый из 5 кандидатов.\n\n5. По графикам 1H выбери 3 лучшие монеты для setup.\n\n6. 15m проси только если точка входа по 1H неясная,\n   максимум по 1–2 монетам.\n   15m нужен только для уточнения входа, а не для отбора всех кандидатов.\n\n7. После финального выбора ОБЯЗАТЕЛЬНО верни готовый setup как прикреплённый .txt файл.\n   Не пиши setup просто текстом в сообщении.\n   Не пиши setup в Markdown.\n   Не пиши setup в ```json блоке.\n   Нужно именно создать и приложить файл.\n\n   Имя файла строго:\n   setup-HHMM_DDMM.txt\n\n   Пример имени:\n   setup-0059_0106.txt\n\n8. Внутри setup-файла должен быть ЧИСТЫЙ JSON object.\n   Файл должен начинаться с { и заканчиваться }.\n   Не используй Markdown, не используй ```json, не используй поясняющий текст до или после JSON.\n   В файле не должно быть строк вида setup_version: 1.6 вне JSON.\n\n   Обязательные поля верхнего уровня:\n   "setup_version": "1.6"\n   "mode": "AUTO_OPEN"\n   "exchange": "MEXC_FUTURES"\n   "margin_mode": "ISOLATED"\n   "default_margin_percent_per_trade": 10\n   "default_leverage": 10\n   "verdict": "TRADE" или "NO_TRADE"\n   "blocked_symbol_substrings": ["STOCK"]\n   "symbol_format": "MEXC_NATIVE_UNDERSCORE"\n   "trades": максимум 3 сделки.\n\n   Минимальный пример структуры файла:\n   {\n     "setup_version": "1.6",\n     "mode": "AUTO_OPEN",\n     "exchange": "MEXC_FUTURES",\n     "margin_mode": "ISOLATED",\n     "default_margin_percent_per_trade": 10,\n     "default_leverage": 10,\n     "verdict": "TRADE",\n     "blocked_symbol_substrings": ["STOCK"],\n     "symbol_format": "MEXC_NATIVE_UNDERSCORE",\n     "trades": []\n   }\n\n9. По каждой сделке обязательно указать:\n   symbol\n   direction: LONG или SHORT\n   order_type: LIMIT\n   entry\n   stop_loss\n   take_profits:\n     TP1: 35%\n     TP2: 35%\n     TP3: REMAINDER\n   cancel_if_not_filled_minutes: 120\n   cancel_if_tp1_before_entry: true\n   invalidation\n   comment\n   risk.stop_distance_percent\n   risk.estimated_deposit_risk_percent\n\n10. ВАЖНО ПО STOP_LOSS:\n\n   ChatGPT сам рассчитывает entry, stop_loss и take_profits.\n   Сделки выставляются лимитными ордерами, не рыночными.\n\n   Stop_loss должен быть структурным и находиться в диапазоне:\n   минимум 1% от entry,\n   максимум 5% от entry.\n\n   Если структурный стоп получается меньше 1%, не используй микростоп.\n   Расширь stop_loss до логичного уровня, чтобы расстояние было не меньше 1%.\n\n   Если структурный стоп получается больше 5%, не давай эту сделку в setup.\n\n   Take_profits НЕ пересчитываются от округлённого или расширенного stop_loss.\n   TP1 / TP2 / TP3 выбираются по графику, уровням, ликвидности и структуре рынка.\n\n   Схема фиксации:\n   TP1: 35%\n   TP2: 35%\n   TP3: REMAINDER\n\n   После TP1 бот переносит stop_loss в breakeven.\n   Trailing: OFF.\n   Scalp exit: OFF.\n\n11. Старые версии setup не использовать.\n   Бот принимает только setup_version "1.6".\n   Не использовать setup_version "1.4", "1.5", "2.3" и любые другие версии.\n\n12. Если нет 3 качественных сделок, не выдумывай.\n   Лучше дай 1–2 сделки или verdict: "NO_TRADE".\n'.strip()

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
    path.write_text(text, encoding="utf-8-sig")
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

        has_remainder = False
        numeric_total = 0.0
        for idx, x in enumerate(tps):
            if not isinstance(x, dict):
                raise ValueError(f"TRADE_{i}: invalid TP row")
            raw_size = x.get("size_percent")
            if isinstance(raw_size, str) and raw_size.strip().upper() == "REMAINDER":
                if idx != len(tps) - 1:
                    raise ValueError(f"TRADE_{i}: REMAINDER is allowed only on the last TP")
                has_remainder = True
            else:
                numeric_total += _safe_float(raw_size)

        if has_remainder:
            if numeric_total <= 0 or numeric_total >= 100.0:
                raise ValueError(f"TRADE_{i}: numeric TP sizes before REMAINDER must be >0 and <100")
        else:
            if abs(numeric_total - 100.0) > 0.5:
                raise ValueError(f"TRADE_{i}: TP sizes must sum to 100 or last TP must be REMAINDER")

        clean_tps = []
        for idx, tp in enumerate(tps):
            p = _safe_float(tp.get("price") if isinstance(tp, dict) else 0)
            raw_size = tp.get("size_percent") if isinstance(tp, dict) else 0
            if isinstance(raw_size, str) and raw_size.strip().upper() == "REMAINDER":
                s: float | str = "REMAINDER"
            else:
                s = _safe_float(raw_size)
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
        sem = asyncio.Semaphore(int(os.getenv("CHATGPT_CANCEL_CONCURRENCY", "3") or 3))

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
            f"✅ поставлено новых лимиток: {placed}",
            f"❌ пропущено: {len(skipped)}",
        ]
        if skipped:
            for row in skipped[:6]:
                reason = row.get("reason") or (row.get("result") or {}).get("reason") or "unknown"
                lines.append(f"• {row.get('symbol')}: {reason}")
        else:
            lines.append("• нет")
        if res.get("message"):
            lines.append(f"status: {res.get('message')}")
    lines += [
        "",
        "🔁 сопровождение включено",
        f"⏳ лимитки живут {CHATGPT_LIMIT_TTL_MINUTES} мин, установка: {setup_time}",
        f"⏱ проверка каждые {CHATGPT_MONITOR_INTERVAL_SEC} сек",
        f"🕒 последнее обновление: {_now_chatgpt_display()}",
    ]
    return "\n".join(lines)[:3900]

async def execute_setup(storage, exchange_client, setup: dict) -> dict:
    chatgpt_log_event("setup_execute_start")
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
                "cancel_if_tp1_before_entry": _truthy(t.get("cancel_if_tp1_before_entry", False)),
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
        # V38 hard safety gate: before entry placement, reject stale setups if
        # either the setup SL has already been crossed OR TP1 has already been
        # reached.  In both cases the original risk/reward is broken before the
        # limit order even exists.
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
                    reason=reason,
                )
                return {"symbol": plan.symbol, "side": plan.side, "order_type": plan.order_type, "entry": entry, "ok": False, "reason": reason, "safety_stop_breached": True, "current_price": cur, "stop": stop_price, "tp1": tp1_price}
            if _chatgpt_tp1_already_touched(plan.side, cur, tp1_price):
                side_word = "выше TP1" if str(plan.side).upper() == "LONG" else "ниже TP1"
                reason = f"цена уже {side_word}: current={_fmt(cur)}, tp1={_fmt(tp1_price)}; лимитка не выставлена в целях безопасности"
                chatgpt_log_event(
                    "setup_trade_rejected_tp1_already_touched",
                    symbol=plan.symbol,
                    side=plan.side,
                    order_type=plan.order_type,
                    current_price=cur,
                    entry=entry,
                    stop=stop_price,
                    tp1=tp1_price,
                    reason=reason,
                )
                return {"symbol": plan.symbol, "side": plan.side, "order_type": plan.order_type, "entry": entry, "ok": False, "reason": reason, "safety_tp1_touched": True, "current_price": cur, "stop": stop_price, "tp1": tp1_price}
        except Exception as e:
            reason = f"не смог проверить текущую цену перед входом: {e}; лимитка не выставлена в целях безопасности"
            chatgpt_log_event("setup_trade_rejected_price_safety_check_failed", symbol=plan.symbol, side=plan.side, entry=entry, stop=stop_price, tp1=tp1_price, error=str(e))
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
        chatgpt_log_event("setup_trade_place_result", symbol=plan.symbol, ok=bool((res or {}).get("ok")), result=res)
        return {"symbol": plan.symbol, "side": plan.side, "order_type": plan.order_type, "entry": entry, "ok": bool((res or {}).get("ok")), "result": res}

    # Pre-placement skips (existing open position / no total slot) must remain
    # visible in the final monitor instead of disappearing from the report.
    opened.extend(preplace_skipped)

    # V34 SPEED FIX: place up to three setup LIMITs concurrently after one
    # exchange-first sync/cleanup pass.  Per-symbol locks in ExecutionEngine
    # still prevent duplicates; this removes the old one-by-one delay.
    if trades:
        chatgpt_log_event("setup_trade_place_batch_start", trades=len(trades), symbols=[t.get("symbol") for t in trades])
        batch_results = await asyncio.gather(*[_place_one_setup_trade(t) for t in trades], return_exceptions=True)
        for t, r in zip(trades, batch_results):
            if isinstance(r, Exception):
                sym = str(t.get("symbol") or "")
                chatgpt_log_event("setup_trade_place_exception", symbol=sym, error=str(r))
                opened.append({"symbol": sym, "ok": False, "reason": str(r)})
            else:
                opened.append(r)
        chatgpt_log_event("setup_trade_place_batch_done", opened=opened)
    chatgpt_log_event("setup_execute_done", opened=opened, rotation=rotation_result)
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
