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
CHATGPT_SETUP_VERSION = "1.5"
CHATGPT_MAX_ACTIVE_TRADES = 3
CHATGPT_MAX_TOTAL_SLOTS = 3  # open ChatGPT positions + new pending limits must not exceed this

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

    task = """
TASK FOR CHATGPT:
Ты получил log.txt от Telegram-бота со сканом топ-200 монет MEXC Futures.

ОБЩИЙ ПРОЦЕСС:
1. Бот собирает данные по MEXC Futures API.
2. Пользователь отправляет этот log.txt в ChatGPT.
3. ChatGPT сначала выбирает 10 монет-кандидатов.
4. Пользователь присылает только 10 скриншотов 4H по этим 10 монетам.
5. ChatGPT по 4H оставляет 5 лучших монет.
6. Пользователь присылает ещё 10 скриншотов: 15m и 1H по этим 5 монетам.
7. Только после этого ChatGPT выбирает максимум 3 лучшие сделки и возвращает текстовый вердикт + готовый setup.txt файлом/блоком JSON.
8. По умолчанию максимум 3 сделки; не расширяй setup.txt до 5/10 без отдельной просьбы пользователя.
9. Важно: у бота есть 3 общих ChatGPT-слота: реальные открытые ChatGPT-позиции + новые pending LIMIT-ордера вместе не должны превышать 3. Pending LIMIT проверяются не чаще одного раза в 10 секунд, чтобы не долбить MEXC.

ЭТАП 1 ПОСЛЕ log.txt:
1. Проанализируй общий рынок BTC/ETH и все монеты из лога.
2. Отбери ровно 10 лучших кандидатов, если достойных меньше — напиши меньше и объясни почему.
3. Раздели их на LONG / SHORT / WAIT / NO TRADE.
4. Попроси у пользователя ТОЛЬКО 4H-скриншоты по этим 10 монетам.
5. НЕ проси сразу 15m и 1H.
6. НЕ давай финальные сделки и НЕ создавай setup.txt на этом этапе.
7. НЕ рассматривай монеты/контракты, где в названии есть STOCK: они заблокированы по региону и торговать их нельзя.

ЭТАП 2 ПОСЛЕ 10 СКРИНШОТОВ 4H:
1. Проанализируй 4H-структуру выбранных 10 монет.
2. Оставь 5 лучших монет.
3. Попроси у пользователя только 15m и 1H скриншоты по этим 5 монетам.
4. НЕ создавай setup.txt до получения 15m и 1H по этим 5 монетам.

ЭТАП 3 ПОСЛЕ ДОПОЛНИТЕЛЬНЫХ 10 СКРИНШОТОВ 15m/1H:
1. Выбери максимум 3 лучшие сделки.
2. Дай короткий текстовый вердикт.
3. ОБЯЗАТЕЛЬНО верни готовый setup-файл в JSON-формате так, чтобы пользователь мог сразу скачать файл и отправить его боту.
4. Файл создавай с уникальным именем: setup-HHMM_DDMM.txt, например setup-1445_3105.txt. Не используй одно и то же setup.txt, чтобы Telegram не путал старые файлы. Бот принимает setup.txt, setup-1.txt, setup-2.txt, setup-HHMM_DDMM.txt и любые .txt с корректным JSON setup.
5. В setup.txt каждая сделка должна содержать: symbol, direction, order_type, entry или entry_reference, stop_loss, take_profits [{price,size_percent}], invalidation, comment, risk.stop_distance_percent. Для третьего тейка используй size_percent="REMAINDER", а не 30.
6. Для ChatGPT Mode запрещены trailing/scalp exits и paper_fill: LIMIT-сделка считается открытой только после реального fill на MEXC.
7. SYMBOL всегда указывай в MEXC-native формате: BTC_USDT, ETH_USDT, BCH_USDT. Не используй BTC/USDT:USDT.
8. Для LIMIT добавь cancel_if_not_filled_minutes и cancel_if_tp1_before_entry=true.
9. Для MARKET добавь max_price_deviation_percent.
10. Не указывай размер позиции в монетах: бот сам использует 10% депозита на каждую сделку, 10x leverage, isolated.
11. Если хороших сделок нет, верни trades: [] и verdict: NO_TRADE.
12. В setup.txt всегда ставь max_active_trades=3.
13. Если часть старых ChatGPT-сделок уже стала реальными позициями, новые лимитки ставятся только на свободные слоты.

SETUP FILE NAMING RULE FOR CHATGPT:
1. Финальный файл всегда прикладывай как setup-HHMM_DDMM.txt, где HHMM — текущее время, DDMM — сегодняшняя дата.
2. Пример: setup-1445_3105.txt.
3. В ответе дай именно файл для скачивания, не только JSON-блок.

SCREENSHOT RULES FOR CHATGPT:
1. После log.txt проси только 4H по 10 монетам.
2. После 4H отбери 5 монет.
3. По 5 монетам проси только 15m и 1H.
4. Итого максимум 20 скриншотов: 10 скринов 4H + 10 скринов 15m/1H.
5. Не проси 30 скриншотов сразу.

BLOCKED SYMBOL RULES FOR CHATGPT:
1. Любой symbol, где есть substring STOCK, запрещён: MSFTSTOCK_USDT, STXSTOCKUSDT и любые аналоги.
2. Такие монеты не выбирай в кандидаты, не запрашивай по ним скриншоты и не добавляй в setup.txt.
3. Если они попали в лог ошибочно — игнорируй их как NO_TRADE / REGION_BLOCKED.

RISK RULES FOR CHATGPT:
1. Все стопы рассчитывает ChatGPT.
2. Стоп должен быть структурным: за поддержкой для LONG или за сопротивлением для SHORT.
3. Расстояние от ENTRY до STOP_LOSS должно быть от 1% до 5%.
4. Если нормальный структурный стоп получается меньше 1% — расширь стоп минимум до 1%.
5. Если нормальный структурный стоп получается больше 5% — не давай такую сделку, пометь NO_TRADE.
6. В setup.txt обязательно укажи risk.stop_distance_percent и risk.estimated_deposit_risk_percent.
7. Бот по умолчанию использует 10% депозита на сделку, 10x leverage, isolated; при таком режиме stop_distance_percent примерно равен риску по депозиту в процентах.

TRADE MANAGEMENT RULES FOR CHATGPT MODE:
1. trailing_enabled=false.
2. scalp_exit_enabled=false.
3. paper_fill_enabled=false.
4. breakeven_after_tp1_only=true.
5. LIMIT сначала PENDING_LIMIT, SL/TP ставятся только после реального fill на MEXC.
6. После исполнения TP1 бот должен перенести STOP_LOSS по остатку позиции в безубыток: breakeven_price=ENTRY.
7. В take_profits используй схему: TP1=35%, TP2=35%, TP3=size_percent="REMAINDER". Третий тейк закрывает весь остаток позиции после округлений MEXC.

NEW SETUP REPLACEMENT RULES:
1. При загрузке нового setup.txt бот отменяет все старые pending LIMIT-ордера ChatGPT Mode, которые ещё не исполнены.
2. Открытые позиции бот не трогает.
3. После отмены старых pending-лимиток бот считает реальные открытые ChatGPT-позиции.
4. Свободные слоты = 3 - количество открытых ChatGPT-позиций.
5. Бот ставит из нового setup.txt только столько новых лимиток, сколько есть свободных слотов.
6. Если уже открыто 3 ChatGPT-позиции, новые лимитки не ставятся.
7. Открытые позиции бот не трогает.
8. По умолчанию максимум 3 сделки и максимум 3 общих ChatGPT-слота.

СТРОГИЙ ФОРМАТ setup.txt:
{
  "setup_version": "1.4",
  "mode": "AUTO_OPEN",
  "exchange": "MEXC_FUTURES",
  "margin_mode": "ISOLATED",
  "default_margin_percent_per_trade": 10,
  "default_leverage": 10,
  "max_active_trades": 3,
  "valid_until_utc": "YYYY-MM-DD HH:MM:SS",
  "verdict": "...",
  "risk_rules": {
    "min_stop_distance_percent": 1.0,
    "max_stop_distance_percent": 5.0
  },
  "trade_management": {
    "trailing_enabled": false,
    "scalp_exit_enabled": false,
    "paper_fill_enabled": false,
    "breakeven_after_tp1_only": true,
    "breakeven_price": "ENTRY"
  },
  "blocked_symbol_substrings": ["STOCK"],
  "symbol_format": "MEXC_NATIVE_UNDERSCORE",
  "trades": []
}
""".strip()

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
    path.write_text(text, encoding="utf-8")
    chatgpt_log_event("scan_log_created", path=str(path), symbols=len(symbols), bytes=len(text.encode("utf-8")))
    return str(path)


def extract_setup_json(text: str) -> dict:
    chatgpt_log_event("setup_extract_start", text_bytes=len(str(text or "").encode("utf-8", errors="ignore")))
    raw = str(text or "").strip()
    if "===== setup.txt =====" in raw:
        raw = raw.split("===== setup.txt =====", 1)[1].split("===== end setup.txt =====", 1)[0].strip()
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw.lower().startswith("json"):
            raw = raw[4:].strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start < 0 or end < start:
        raise ValueError("setup.txt must contain JSON object")
    data = json.loads(raw[start:end + 1])
    chatgpt_log_event("setup_extract_ok", setup_version=data.get("setup_version"), trades_count=len(data.get("trades") or []), verdict=data.get("verdict"))
    return data


def validate_setup(data: dict) -> list[dict]:
    chatgpt_log_event("setup_validate_start", setup_version=(data or {}).get("setup_version") if isinstance(data, dict) else None)
    if not isinstance(data, dict):
        raise ValueError("setup JSON must be an object")
    if str(data.get("setup_version")) not in {"1.0", "1.1", "1.2", "1.3", "1.4"}:
        raise ValueError("unsupported setup_version")
    trades = data.get("trades") or []
    if not isinstance(trades, list):
        raise ValueError("trades must be a list")
    max_trades = CHATGPT_MAX_ACTIVE_TRADES
    if len(trades) > max_trades:
        raise ValueError(f"too many trades: max {max_trades} for ChatGPT Mode")
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
    chatgpt_log_event("setup_validate_ok", trades=len(out))
    return out


async def _cancel_all_old_pending_limits(storage, exec_engine) -> list[dict]:
    """Cancel all old ChatGPT pending LIMITs before applying a new setup.

    v22 hardens cancellation. We cancel both:
    - local DB pending ChatGPT LIMIT entries;
    - exchange-only open entry orders created by ChatGPT mode (externalOid/client id starts with bot_entry_)
      even if the local DB missed them after redeploy/restart.

    Real open positions are never touched.
    """
    cancelled: list[dict] = []
    local_pending_ids: set[str] = set()

    # 1) Cancel local pending rows first.
    try:
        positions = await storage.positions()
    except Exception as e:
        chatgpt_log_event("cancel_old_pending_load_error", error=str(e))
        positions = []

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
                cancelled.append({"source": "local", "symbol": sym, "order_id": oid, "ok": ok, "result": res})
            except Exception as e:
                chatgpt_log_event("cancel_old_pending_local_error", symbol=sym, order_id=oid, error=str(e))
                cancelled.append({"source": "local", "symbol": sym, "order_id": oid, "ok": False, "error": str(e)})
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
        if reduce_only or prot in {"tp", "sl"}:
            return False
        return ext.startswith("bot_entry_") or (oid and oid in local_pending_ids)

    exchange_cancelled_ids: set[str] = set()
    for o in orders or []:
        if not _is_chatgpt_entry_order(o):
            continue
        sym = _order_symbol(o)
        oid = _order_id(o)
        if not sym or not oid or oid in exchange_cancelled_ids:
            continue
        try:
            chatgpt_log_event("cancel_old_pending_exchange_order_cancel_start", symbol=sym, order_id=oid)
            res = await exec_engine.exchange_client.cancel_order(oid, sym)
            exchange_cancelled_ids.add(oid)
            chatgpt_log_event("cancel_old_pending_exchange_order_cancel_ok", symbol=sym, order_id=oid, result=res)
            cancelled.append({"source": "exchange", "symbol": sym, "order_id": oid, "ok": True, "result": res})
        except Exception as e:
            chatgpt_log_event("cancel_old_pending_exchange_order_cancel_error", symbol=sym, order_id=oid, error=str(e))
            cancelled.append({"source": "exchange", "symbol": sym, "order_id": oid, "ok": False, "error": str(e)})

    # 3) Verify after cancel.
    try:
        await asyncio.sleep(float(os.getenv("CHATGPT_CANCEL_VERIFY_DELAY_SEC", "0.8") or 0.8))
        verify = await exec_engine.exchange_client.fetch_open_orders()
        leftovers = []
        for o in verify or []:
            if _is_chatgpt_entry_order(o):
                leftovers.append({"symbol": _order_symbol(o), "order_id": _order_id(o)})
        if leftovers:
            chatgpt_log_event("cancel_old_pending_verify_still_exists_error", leftovers=leftovers[:20], count=len(leftovers))
            for item in leftovers:
                cancelled.append({"source": "verify", "symbol": item.get("symbol"), "order_id": item.get("order_id"), "ok": False, "error": "order_still_open_after_cancel"})
        else:
            chatgpt_log_event("cancel_old_pending_verify_after_cancel_ok")
    except Exception as e:
        chatgpt_log_event("cancel_old_pending_verify_after_cancel_error", error=str(e))

    return cancelled

async def _count_open_chatgpt_slots(storage) -> tuple[int, list[dict]]:
    """Count real active ChatGPT positions after old pending limits are cancelled.

    Pending LIMIT entries are not counted here because execute_setup cancels old
    pending ChatGPT limits before calling this helper. We count only open/closing
    positions so a fresh setup can fill remaining free slots up to 3 total.
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


async def execute_setup(storage, exchange_client, setup: dict) -> dict:
    chatgpt_log_event("setup_execute_start")
    trades = validate_setup(setup)
    if not trades:
        chatgpt_log_event("setup_execute_no_trade")
        return {"ok": True, "opened": [], "message": "NO_TRADE"}
    bal = await exchange_client.fetch_balance()
    equity = _balance_total_usdt(bal)
    if equity <= 0:
        equity = float(os.getenv("DEFAULT_EQUITY_USDT", "100") or 100)
    margin_pct = float(setup.get("default_margin_percent_per_trade") or 10) / 100.0
    leverage = int(float(setup.get("default_leverage") or 10))
    exec_engine = ExecutionEngine(storage, exchange_client)
    opened = []
    live = True
    cancelled_pending = await _cancel_all_old_pending_limits(storage, exec_engine)
    if cancelled_pending:
        chatgpt_log_event("setup_cancelled_old_pending_limits", items=cancelled_pending)
    cancel_failed = [x for x in (cancelled_pending or []) if not bool(x.get("ok"))]
    if cancel_failed:
        # Hard safety gate: never place new setup limits while an old ChatGPT
        # entry limit is still open or its cancellation was not verified.
        chatgpt_log_event("setup_abort_old_pending_cancel_failed", failed=cancel_failed[:20], count=len(cancel_failed))
        return {"ok": False, "opened": [], "message": "OLD_PENDING_CANCEL_FAILED", "failed_cancelled_pending": cancel_failed}

    open_slots, open_positions = await _count_open_chatgpt_slots(storage)
    free_slots = max(0, CHATGPT_MAX_TOTAL_SLOTS - open_slots)
    requested_trades = len(trades)
    if free_slots <= 0:
        chatgpt_log_event(
            "setup_no_free_slots",
            max_total_slots=CHATGPT_MAX_TOTAL_SLOTS,
            open_slots=open_slots,
            open_symbols=[p.get("symbol") for p in open_positions],
            requested_trades=requested_trades,
        )
        return {"ok": True, "opened": [], "message": "NO_FREE_CHATGPT_SLOTS", "open_slots": open_slots, "max_total_slots": CHATGPT_MAX_TOTAL_SLOTS}
    if len(trades) > free_slots:
        skipped = trades[free_slots:]
        trades = trades[:free_slots]
        chatgpt_log_event(
            "setup_trades_trimmed_to_free_slots",
            max_total_slots=CHATGPT_MAX_TOTAL_SLOTS,
            open_slots=open_slots,
            free_slots=free_slots,
            requested_trades=requested_trades,
            accepted_trades=len(trades),
            skipped_symbols=[t.get("symbol") for t in skipped],
        )

    chatgpt_log_event("setup_execute_balance", equity=equity, margin_pct=margin_pct, leverage=leverage, trades=len(trades), requested_trades=requested_trades, cancelled_pending=len(cancelled_pending), open_slots=open_slots, free_slots=free_slots, max_active_trades=CHATGPT_MAX_ACTIVE_TRADES, max_total_slots=CHATGPT_MAX_TOTAL_SLOTS)
    for t in trades:
        entry = float(t["entry"])
        notional = equity * margin_pct * leverage
        qty = notional / entry
        tps = t["take_profits"]
        # take_price is the farthest/final TP; protection branch reads all TPs from signal_details.
        final_tp = float(tps[-1]["price"])
        plan = TradePlan(
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
            max_open_positions=CHATGPT_MAX_ACTIVE_TRADES,
            planned_notional_usdt=notional,
            expected_margin_usdt=equity * margin_pct,
            max_margin_per_position_usdt=equity * margin_pct,
            leverage=leverage,
            signal_details={
                "chatgpt_take_profits": tps,
                "invalidation": str(t.get("invalidation") or ""),
                "comment": str(t.get("comment") or ""),
                "cancel_if_not_filled_minutes": int(float(t.get("cancel_if_not_filled_minutes") or 0)),
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
        # Market stale-price guard.
        if plan.order_type == "market":
            max_dev = float(plan.signal_details.get("max_price_deviation_percent") or 0.7)
            try:
                ticker = await exchange_client.fetch_ticker(plan.symbol)
                cur = _safe_float(ticker.get("last") or ticker.get("close"))
                if cur > 0 and abs(cur - entry) / entry * 100.0 > max_dev:
                    reason = f"market price deviation > {max_dev}%"
                    chatgpt_log_event("setup_trade_rejected", symbol=plan.symbol, reason=reason, current_price=cur, entry=entry)
                    opened.append({"symbol": plan.symbol, "ok": False, "reason": reason})
                    continue
            except Exception as e:
                reason = f"ticker check failed: {e}"
                chatgpt_log_event("setup_trade_rejected", symbol=plan.symbol, reason=reason)
                opened.append({"symbol": plan.symbol, "ok": False, "reason": reason})
                continue
        chatgpt_log_event("setup_trade_place_start", symbol=plan.symbol, side=plan.side, order_type=plan.order_type, entry=entry, qty=qty, stop=plan.stop_price, final_tp=final_tp)
        res = await exec_engine.place_entry(plan, live=live)
        chatgpt_log_event("setup_trade_place_result", symbol=plan.symbol, ok=bool(res.get("ok")), result=res)
        opened.append({"symbol": plan.symbol, "side": plan.side, "order_type": plan.order_type, "entry": entry, "ok": bool(res.get("ok")), "result": res})
    chatgpt_log_event("setup_execute_done", opened=opened)
    return {"ok": True, "opened": opened}
