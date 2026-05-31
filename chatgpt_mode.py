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
    "live_trading": False,
}

# Simple mandatory stop-risk corridor for ChatGPT-generated setup.txt.
# With the user's default 10% margin per trade and 10x leverage, the notional
# is approximately 100% of the deposit, so stop distance in % is also the
# estimated deposit risk in % for one trade.
CHATGPT_MIN_STOP_DISTANCE_PCT = 1.0
CHATGPT_MAX_STOP_DISTANCE_PCT = 5.0
CHATGPT_SETUP_VERSION = "1.1"

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
    chatgpt_log_event("scan_start", limit=limit)
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
    symbols = filtered_symbols[:int(limit)]
    chatgpt_log_event(
        "scan_symbols_loaded",
        raw_count=len(raw_symbols),
        blocked_count=len(blocked_symbols),
        blocked_sample=",".join(blocked_symbols[:30]),
        count=len(symbols),
        sample=",".join(symbols[:10]),
    )

    async def one_symbol(sym: str) -> str:
        if is_chatgpt_blocked_symbol(sym):
            chatgpt_log_event("scan_symbol_blocked_stock", symbol=sym)
            return f"SYMBOL: {sym}\nSKIPPED: REGION_BLOCKED_STOCK_CONTRACT"
        try:
            ticker, ob = await asyncio.gather(
                exchange_client.fetch_ticker(sym),
                exchange_client.fetch_order_book(sym, limit=20),
            )
            price = _safe_float(ticker.get("last") or ticker.get("close"))
            tf_data = {}
            for tf in ("15m", "1h", "4h"):
                candles = await exchange_client.fetch_ohlcv(sym, timeframe=tf, limit=120)
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
                f"SYMBOL: {sym}",
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
            chatgpt_log_event("scan_symbol_error", symbol=sym, error=repr(e))
            return f"SYMBOL: {sym}\nERROR: {str(e)[:240]}"

    sem = asyncio.Semaphore(int(os.getenv("CHATGPT_SCAN_CONCURRENCY", "4") or 4))
    async def guarded(sym):
        async with sem:
            return await one_symbol(sym)
    blocks = await asyncio.gather(*(guarded(s) for s in symbols), return_exceptions=True)

    btc = ""
    eth = ""
    try:
        btc = await one_symbol("BTC/USDT:USDT")
    except Exception:
        pass
    try:
        eth = await one_symbol("ETH/USDT:USDT")
    except Exception:
        pass

    task = """
TASK FOR CHATGPT:
Ты получил log.txt от Telegram-бота со сканом топ-200 монет.

ЭТАП 1:
1. Проанализируй общий рынок BTC/ETH и все монеты из лога.
2. Отбери 5–10 лучших кандидатов.
3. Раздели их на LONG / SHORT / WAIT / NO TRADE.
4. Скажи пользователю, какие скриншоты нужны по каждой выбранной монете: обычно 15m / 1H / 4H, 1D только если нужен среднесрок.
5. НЕ рассматривай монеты/контракты, где в названии есть STOCK: они заблокированы по региону и торговать их нельзя.
6. На этом этапе НЕ давай финальные сделки без скриншотов.

ЭТАП 2 ПОСЛЕ СКРИНШОТОВ:
1. Выбери максимум 3 лучшие сделки.
2. Дай текстовый вердикт и ОБЯЗАТЕЛЬНО верни один блок setup.txt в JSON-формате.
3. В setup.txt каждая сделка должна содержать: symbol, direction, order_type, entry или entry_reference, stop_loss, take_profits [{price,size_percent}], invalidation, comment, risk.stop_distance_percent.
4. Для LIMIT добавь cancel_if_not_filled_minutes и cancel_if_tp1_before_entry=true.
5. Для MARKET добавь max_price_deviation_percent.
6. Не указывай размер позиции в монетах: бот сам использует 10% депозита на каждую сделку, 10x leverage, isolated.
7. Если хороших сделок нет, верни trades: [] и verdict: NO_TRADE.

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

СТРОГИЙ ФОРМАТ setup.txt:
{
  "setup_version": "1.1",
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
  "blocked_symbol_substrings": ["STOCK"],
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
    if str(data.get("setup_version")) not in {"1.0", "1.1"}:
        raise ValueError("unsupported setup_version")
    trades = data.get("trades") or []
    if not isinstance(trades, list):
        raise ValueError("trades must be a list")
    if len(trades) > int(data.get("max_active_trades") or 3):
        raise ValueError("too many trades")
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
        symbol = raw_symbol.replace("_", "/")
        if symbol.endswith("USDT") and "/" not in symbol:
            symbol = symbol[:-4] + "/USDT:USDT"
        elif symbol.endswith("/USDT"):
            symbol = symbol + ":USDT"
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
        total = sum(_safe_float(x.get("size_percent")) for x in tps if isinstance(x, dict))
        if abs(total - 100.0) > 0.5:
            raise ValueError(f"TRADE_{i}: TP sizes must sum to 100")
        clean_tps = []
        for tp in tps:
            p = _safe_float(tp.get("price") if isinstance(tp, dict) else 0)
            s = _safe_float(tp.get("size_percent") if isinstance(tp, dict) else 0)
            if p <= 0 or s <= 0:
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
    chatgpt_log_event("setup_execute_balance", equity=equity, margin_pct=margin_pct, leverage=leverage, trades=len(trades))
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
            max_open_positions=int(setup.get("max_active_trades") or 3),
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
