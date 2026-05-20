import os, time, asyncio, logging
import aiohttp
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
import psutil

from config import TELEGRAM_TOKEN, ADMIN_IDS, VERSION, DEFAULT_EXCHANGE
from storage import Storage
from keyboard import MAIN_MENU, settings_menu, choices_menu, api_menu, openai_menu, ai_stats_menu, format_duration_seconds
from adaptive_engine import AdaptiveEngine
from mirror_engine import MirrorEngine
from session_engine import SessionEngine
from spot_confirmation_engine import SpotConfirmationEngine
from risk_engine import RiskEngine
from exchange_client import ExchangeClient
from execution_engine import ExecutionEngine
from sync_engine import SyncEngine
from recovery_engine import RecoveryEngine
from protection_engine import ProtectionEngine
from scanner import Scanner
from production_gate import ProductionGate
from ws_engine import WebSocketSupervisor, futures_source_from_mode
from trade_planner import TradePlanner
from openai_signal_engine import OpenAISignalEngine
from ai_scalping_engine import AIScalpingEngine
from ai_stats import AIStatsManager
from position_manager import PositionManager
from chart_renderer import render_trade_setup_chart

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bot")

storage = Storage()
scanner = Scanner()
ai_signal_engine = OpenAISignalEngine()
ai_scalping_engine = AIScalpingEngine()
ai_stats_manager = AIStatsManager(storage)
running = False
# New-entry switch is intentionally separate from the loop switch.
# /stop pauses entries but keeps the position manager alive; /panic stops the loop.
entries_enabled = False
started_at = time.time()
exchange_client = None
ws_supervisor = None
trading_task = None

def admin_id_list() -> list[str]:
    return [x.strip() for x in str(ADMIN_IDS or os.getenv("ADMIN_IDS", "")).split(",") if x.strip()]

def first_admin_id() -> str:
    ids = admin_id_list()
    return ids[0] if ids else ""

def allowed(update: Update) -> bool:
    # Fail closed: if ADMIN_IDS is not configured, nobody can control
    # the bot from Telegram. This prevents accidental public access on Railway/VPS.
    ids = set(admin_id_list())
    if not ids:
        return False
    uid = update.effective_user.id if update.effective_user else None
    return str(uid) in ids

def scanner_market_data_fresh(max_age_sec: int = 900) -> bool:
    """Return True when the scanner has a usable recent universe/cycle.

    WebSocket is an acceleration source, not a hard execution dependency.
    If WS briefly disconnects but the scanner has recently loaded symbols and
    completed a scan cycle without a large error burst, entries may continue
    through REST/fallback market data instead of being blocked by stale WS state.
    """
    try:
        age = time.time() - float(getattr(scanner, "last_refresh", 0) or 0)
        loaded = len(getattr(scanner, "hot_symbols", []) or [])
        scanned = int(getattr(scanner, "last_cycle_scanned", 0) or 0)
        errors = int(getattr(scanner, "last_cycle_errors", 0) or 0)
        source = str(getattr(scanner, "last_scan_source", "") or "")
        return (
            loaded > 0
            and source not in {"", "init"}
            and age <= max_age_sec
            and (scanned == 0 or errors <= max(3, int(scanned * 0.35)))
        )
    except Exception:
        return False

async def notify_admin(app, text: str, min_interval_sec: int = 0, key: str = "notify") -> None:
    chat_id = first_admin_id()
    if not chat_id:
        return
    now = time.time()
    if min_interval_sec:
        last_key = f"last_{key}"
        last = float(app.bot_data.get(last_key, 0) or 0)
        if now - last < min_interval_sec:
            return
        app.bot_data[last_key] = now
    try:
        await app.bot.send_message(chat_id=chat_id, text=text)
    except Exception as e:
        log.warning("telegram notification failed: %s", e)

async def send_trade_chart(app, ex, plan, settings: dict) -> None:
    """Send one clear setup chart after an auto-trade is opened.

    Charts are generated only when trade_charts_enabled is ON and only for the
    final opened trade. This avoids scanner spam and does not spend OpenAI
    tokens. If matplotlib/candles are unavailable, trading continues silently.
    """
    if not bool(settings.get("trade_charts_enabled", False)):
        return
    chat_id = first_admin_id()
    if not chat_id:
        return
    try:
        tf = str(settings.get("trade_chart_timeframe", os.getenv("TRADE_CHART_TIMEFRAME", "1m")) or "1m")
        limit = int(float(settings.get("trade_chart_candle_limit", os.getenv("TRADE_CHART_CANDLE_LIMIT", "120")) or 120))
        candles = await ex.fetch_ohlcv(plan.symbol, timeframe=tf, limit=max(60, min(limit, 240)))
        chart_path = await asyncio.to_thread(render_trade_setup_chart, plan.symbol, candles, plan)
        if not chart_path:
            return
        caption = (
            "📊 Trade setup chart\n"
            f"{plan.symbol} {plan.side} | {plan.strategy}\n"
            f"Entry {plan.entry_price:.8g} | SL {plan.stop_price:.8g} | TP {plan.take_price:.8g}"
        )
        with open(chart_path, "rb") as img:
            await app.bot.send_photo(chat_id=chat_id, photo=img, caption=caption)
        try:
            os.remove(chart_path)
        except Exception:
            pass
    except Exception as e:
        log.warning("trade chart send failed for %s: %s", getattr(plan, "symbol", "-"), e)

async def send_or_edit_ai_decision(app, text: str, message_id: int | None = None) -> int | None:
    """Send or edit one Telegram AI decision message.

    Used only for UI visibility. It never performs another OpenAI request,
    so it does not spend extra AI tokens.
    """
    chat_id = first_admin_id()
    if not chat_id:
        return message_id
    try:
        if message_id:
            await app.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
            return message_id
        msg = await app.bot.send_message(chat_id=chat_id, text=text)
        return getattr(msg, "message_id", None)
    except Exception as e:
        log.warning("AI decision Telegram update failed: %s", e)
        if message_id:
            try:
                msg = await app.bot.send_message(chat_id=chat_id, text=text)
                return getattr(msg, "message_id", message_id)
            except Exception as e2:
                log.warning("AI decision Telegram resend failed: %s", e2)
        return message_id

def ai_verdict_is_important(verdict) -> bool:
    """Minimal OFF visibility: show only AI operational problems, not normal rejects."""
    if verdict is None:
        return False
    error = str(getattr(verdict, "error", "") or "").strip()
    if error:
        return True
    return not bool(getattr(verdict, "ok", True))

def format_ai_minimal_issue(plan, verdict) -> str:
    symbol = getattr(plan, "symbol", "-")
    side = getattr(plan, "side", "-")
    strategy = getattr(plan, "strategy", "-")
    reason = (getattr(verdict, "error", "") or getattr(verdict, "reason", "") or "AI check problem").strip()
    if len(reason) > 180:
        reason = reason[:177] + "..."
    return (
        "⚠️ AI check issue\n"
        f"{symbol} {side} | {strategy}\n"
        f"Model: {getattr(verdict, 'model', '-')} | Mode: {getattr(verdict, 'mode', '-')}\n"
        f"Reason: {reason}"
    )

def _short_reason(text: str, max_len: int = 110) -> str:
    text = str(text or "-").replace("\n", " ").strip()
    if not text:
        return "-"
    # Hide low-level internals from the normal live scanner card. Full details
    # remain available in /status and logs.
    noisy_parts = [
        "WS:", "pending=", "dropped=", "slowdown=", "Markets:",
        "Execution data", "Source:", "requested:", "filtered=",
    ]
    if any(part.lower() in text.lower() for part in noisy_parts):
        text = "working"
    if len(text) > max_len:
        text = text[: max_len - 3] + "..."
    return text

def _scan_status_text(settings: dict, status: str = "scanning", last_signal: str | None = None, last_decision: str | None = None) -> str:
    """Clean user-facing scanner card.

    v0095 intentionally keeps Telegram simple. Technical counters such as raw
    WS queue, source internals, market totals and slowdown remain in /status,
    but the live scanner card should answer only: what mode is running, how
    much was scanned, was a setup found, and what the next action is.
    """
    signal = _short_reason(last_signal or scanner.last_signal_summary or "none", 120)
    decision = _short_reason(last_decision or scanner.last_reject_reason or "-", 130)
    status_label = str(status or "scanning").replace("_", " ")
    mode = str(settings.get("strategy_mode", "hybrid"))
    ai_mode = mode.lower() == "ai_scalping"
    # v0114: AI BTC/ETH scalping is a separate two-symbol loop, not the legacy
    # adaptive scanner. Do not show stale effective strategy/universe counters
    # from a previous hybrid/reversal scan.
    if ai_mode:
        effective = mode
        universe = "BTC/ETH only"
        checked = 2
        errors = 0
        loaded = 2
    else:
        effective = str(scanner.last_effective_strategy or mode)
        universe = str(settings.get("universe_mode", "adaptive"))
        checked = int(getattr(scanner, "last_cycle_scanned", 0) or 0)
        errors = int(getattr(scanner, "last_cycle_errors", 0) or 0)
        loaded = len(getattr(scanner, "hot_symbols", []) or [])
    scan_every = format_duration_seconds(settings.get("scan_interval_sec", 5))
    icon = "🟢" if status_label in {"scanning", "universe ready"} else ("🟡" if "blocked" not in status_label and "paused" not in status_label else "🛑")
    lines = [
        f"🔎 Scanner: {icon} {status_label}",
        f"📈 Mode: {mode}" + (f" → {effective}" if effective and effective != mode else ""),
        f"🌐 Universe: {universe} | loaded {loaded}",
        f"✅ Checked: {checked}" + (f" | errors {errors}" if errors else ""),
        f"🎯 Last setup: {signal}",
        f"🧠 Decision: {decision}",
        f"⏱ Next cycle pause: {scan_every}",
        f"🕒 {time.strftime('%H:%M:%S')}",
    ]
    if (not ai_mode) and scanner.last_refresh_error:
        lines.append(f"⚠️ Source issue: {_short_reason(scanner.last_refresh_error, 90)}")
    return "\n".join(lines)

async def update_scanner_status(app, settings: dict, status: str = "scanning", last_signal: str | None = None, last_decision: str | None = None, force: bool = False) -> None:
    chat_id = first_admin_id()
    if not chat_id:
        return
    now = time.time()
    # Do not edit Telegram too often; it can rate-limit noisy loops.
    if not force and now - float(app.bot_data.get("scanner_status_last_edit", 0) or 0) < 5:
        return
    text = _scan_status_text(settings, status, last_signal, last_decision)
    msg_id = app.bot_data.get("scanner_status_message_id")
    try:
        if msg_id:
            await app.bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text)
        else:
            msg = await app.bot.send_message(chat_id=chat_id, text=text)
            app.bot_data["scanner_status_message_id"] = msg.message_id
        app.bot_data["scanner_status_last_edit"] = now
    except Exception as e:
        # If the old message cannot be edited, create a new live-status message once.
        log.warning("scanner status edit failed: %s", e)
        try:
            msg = await app.bot.send_message(chat_id=chat_id, text=text)
            app.bot_data["scanner_status_message_id"] = msg.message_id
            app.bot_data["scanner_status_last_edit"] = now
        except Exception as e2:
            log.warning("scanner status send failed: %s", e2)

async def create_fresh_scanner_status(app, settings: dict, status: str = "waiting next scan") -> None:
    """Create a new scanner live-status message at the bottom of the chat.

    Telegram edits do not move an old message down. When the user presses Run,
    we intentionally create a fresh message and then all scanner updates edit
    this newest message id.
    """
    chat_id = first_admin_id()
    if not chat_id:
        return
    text = _scan_status_text(settings, status)
    try:
        msg = await app.bot.send_message(chat_id=chat_id, text=text)
        app.bot_data["scanner_status_message_id"] = msg.message_id
        app.bot_data["scanner_status_last_edit"] = time.time()
    except Exception as e:
        log.warning("fresh scanner status send failed: %s", e)


def trigger_scan_now(app, reason: str = "manual") -> None:
    """Wake the trading loop immediately instead of waiting scan_interval_sec.

    Long scan presets (15m/30m/1h/4h) are useful for liquidity_retest, but
    pressing Run or changing important scanner settings must start/restart a
    cycle right away. The event is in bot_data so command/callback handlers can
    wake the loop without touching global task state.
    """
    try:
        ev = app.bot_data.get("scan_wakeup_event")
        if ev is None:
            ev = asyncio.Event()
            app.bot_data["scan_wakeup_event"] = ev
        app.bot_data["scan_wakeup_reason"] = reason
        ev.set()
    except Exception as e:
        log.debug("scan wakeup failed: %s", e)


async def sleep_until_next_scan(app, seconds: int | float) -> None:
    """Sleep between full scanner cycles, but allow Run/settings to wake it."""
    try:
        delay = max(0.0, float(seconds))
    except Exception:
        delay = 5.0
    ev = app.bot_data.get("scan_wakeup_event")
    if ev is None:
        ev = asyncio.Event()
        app.bot_data["scan_wakeup_event"] = ev
    ev.clear()
    try:
        await asyncio.wait_for(ev.wait(), timeout=delay)
    except asyncio.TimeoutError:
        return
    finally:
        ev.clear()

def _api_creds(settings: dict) -> tuple[str, str]:
    # Telegram-saved credentials have priority. Environment variables remain a fallback
    # for server-side deployment. Secrets are never printed back to chat.
    api_key = str(settings.get("mexc_api_key") or os.getenv("MEXC_API_KEY", "") or "").strip()
    api_secret = str(settings.get("mexc_api_secret") or os.getenv("MEXC_API_SECRET", "") or "").strip()
    return api_key, api_secret


def apply_mexc_runtime_env(settings: dict) -> None:
    """Apply Telegram/DB MEXC order settings to this Railway process.

    Railway environment variables cannot be changed from inside the bot, so these
    values are stored in SQLite and mirrored into os.environ. ExchangeClient reads
    them at order time, which allows changing them from Telegram without editing
    Railway Variables.
    """
    os.environ["MEXC_ORDER_LEVERAGE"] = str(settings.get("mexc_order_leverage", os.getenv("MEXC_ORDER_LEVERAGE", "5")) or "5")
    os.environ["MEXC_ORDER_OPEN_TYPE"] = str(settings.get("mexc_order_open_type", os.getenv("MEXC_ORDER_OPEN_TYPE", "1")) or "1")
    os.environ["MEXC_RECV_WINDOW"] = str(settings.get("mexc_recv_window", os.getenv("MEXC_RECV_WINDOW", "20000")) or "20000")


def mexc_order_settings_text(settings: dict) -> str:
    return (
        "⚙️ MEXC order settings\n"
        f"Leverage: {settings.get('mexc_order_leverage', os.getenv('MEXC_ORDER_LEVERAGE', '5'))}x\n"
        f"Open type: {settings.get('mexc_order_open_type', os.getenv('MEXC_ORDER_OPEN_TYPE', '1'))} "
        "(1 isolated, 2 cross)\n"
        f"recvWindow: {settings.get('mexc_recv_window', os.getenv('MEXC_RECV_WINDOW', '20000'))} ms\n\n"
        "Commands:\n"
        "/leverage 5\n"
        "/open_type 1\n"
        "/recv_window 20000"
    )

def _position_contract_fields(pos: dict) -> tuple[float, float]:
    """Return (contracts, contract_size) for MEXC futures rows when present."""
    info = pos.get("info") if isinstance(pos.get("info"), dict) else {}
    raw = pos.get("exchange_contracts") or pos.get("contracts")
    if raw in (None, ""):
        raw = info.get("holdVol") or info.get("vol") or info.get("positionVol")
    try:
        contracts = abs(float(raw or 0))
    except Exception:
        contracts = 0.0
    cs = pos.get("contractSize") or pos.get("contract_size") or info.get("contractSize") or info.get("contract_size")
    try:
        contract_size = float(cs or 0)
    except Exception:
        contract_size = 0.0
    # Hard fallback for the major MEXC contracts used by AI scalping.
    symbol_key = str(pos.get("mexc_symbol") or pos.get("symbol") or info.get("symbol") or "").upper().replace("/", "_").replace(":USDT", "")
    if contract_size <= 0:
        if "BTC_USDT" in symbol_key or "BTCUSDT" in symbol_key:
            contract_size = 0.0001
        elif "ETH_USDT" in symbol_key or "ETHUSDT" in symbol_key:
            contract_size = 0.01
    return contracts, contract_size

def _position_base_qty(pos: dict) -> float:
    """Base coin quantity for display/notional; never show MEXC contracts as BTC/ETH."""
    contracts, cs = _position_contract_fields(pos)
    if contracts > 0 and cs > 0:
        return contracts * cs
    for key in ("amount", "qty", "size"):
        try:
            value = pos.get(key)
            if value not in (None, ""):
                return abs(float(value))
        except Exception:
            pass
    return 0.0

def _position_money_fields(pos: dict) -> tuple[float, float, int, str]:
    entry = float(pos.get("entry_price") or pos.get("entryPrice") or 0)
    qty = _position_base_qty(pos)
    notional = float(pos.get("notional_usdt") or (abs(entry * qty) if entry and qty else 0))
    leverage = int(float(pos.get("leverage") or os.getenv("MEXC_ORDER_LEVERAGE", "5") or 5))
    margin_type = str(pos.get("margin_type") or ("isolated" if int(float(os.getenv("MEXC_ORDER_OPEN_TYPE", "1") or 1)) == 1 else "cross"))
    margin = float(pos.get("estimated_margin_usdt") or pos.get("expected_margin_usdt") or (notional / leverage if leverage > 0 else notional))
    return notional, margin, leverage, margin_type



def _estimate_exchange_position_margin(positions: list, default_leverage: int = 5) -> tuple[float, int]:
    """Estimate live position margin from exchange position rows.

    MEXC sometimes returns USDT.positionMargin as 0 while open_positions
    still contains live isolated positions. For Telegram diagnostics we should
    not display a clean 0 in that case; estimate notional / leverage from
    the actual exchange rows.
    """
    total = 0.0
    count = 0
    for pos in positions or []:
        try:
            info = pos.get("info") or {} if isinstance(pos, dict) else {}
            qty = 0.0
            for key in ("amount", "qty", "size", "contracts", "holdVol", "vol"):
                v = pos.get(key) if isinstance(pos, dict) else None
                if v not in (None, ""):
                    qty = abs(float(v))
                    if qty > 0:
                        break
            price = 0.0
            for key in ("entryPrice", "entry_price", "markPrice", "holdAvgPrice", "openAvgPrice"):
                v = pos.get(key) if isinstance(pos, dict) else None
                if v not in (None, "", 0, "0"):
                    price = abs(float(v))
                    if price > 0:
                        break
            if price <= 0 and isinstance(info, dict):
                for key in ("openAvgPrice", "holdAvgPrice", "entryPrice", "price", "markPrice"):
                    v = info.get(key)
                    if v not in (None, "", 0, "0"):
                        price = abs(float(v))
                        if price > 0:
                            break
            lev = default_leverage
            if isinstance(info, dict):
                for key in ("leverage", "level"):
                    v = info.get(key)
                    if v not in (None, "", 0, "0"):
                        lev = max(1, int(float(v)))
                        break
            if qty > 0 and price > 0:
                total += qty * price / max(1, lev)
                count += 1
        except Exception:
            continue
    return total, count

def _rr_from_levels(side: str, entry: float, sl: float, tp: float) -> float:
    try:
        if str(side).upper() == "SHORT":
            risk = max(0.0, sl - entry)
            reward = max(0.0, entry - tp)
        else:
            risk = max(0.0, entry - sl)
            reward = max(0.0, tp - entry)
        return reward / risk if risk > 0 else 0.0
    except Exception:
        return 0.0

def _fmt_price(v: float) -> str:
    try:
        return f"{float(v):.8f}".rstrip("0").rstrip(".")
    except Exception:
        return str(v)

def format_position_opened(plan, placed: dict, live: bool, ai_verdict=None) -> str:
    pos = placed.get("position") if isinstance(placed, dict) else None
    pos = pos if isinstance(pos, dict) else plan.__dict__
    entry = float(pos.get("entry_price") or plan.entry_price)
    stop = float(pos.get("stop_price") or plan.stop_price)
    take = float(pos.get("take_price") or plan.take_price)
    qty = float(pos.get("qty") or plan.qty)
    notional, margin, leverage, margin_type = _position_money_fields(pos)
    coin = str(plan.symbol).split("/")[0]
    rr = _rr_from_levels(str(plan.side), entry, stop, take)
    lines = [
        "🟢 Сделка открыта",
        f"🪙 {plan.symbol}",
        f"📈 Direction: {plan.side}",
        f"🧠 Strategy: {plan.strategy}",
        f"💵 Entry: {_fmt_price(entry)}",
        f"🛑 Stop: {_fmt_price(stop)}",
        f"🎯 Take: {_fmt_price(take)}",
    ]
    if rr > 0:
        lines.append(f"📐 RR: {rr:.2f}R")
    lines.extend([
        f"📦 Qty: {qty:.6f} {coin} / {notional:.2f} USDT",
        f"⚙️ Leverage: {leverage}x | {margin_type}",
        f"💰 Margin: {margin_type} / ~{margin:.2f} USDT",
        f"🟣 Live: {'ON' if live else 'OFF'}",
    ])
    if ai_verdict is not None:
        try:
            conf = float(getattr(ai_verdict, "confidence", 0.0) or 0.0)
            reason = str(getattr(ai_verdict, "reason", "") or "").strip()
            if len(reason) > 90:
                reason = reason[:87] + "..."
            lines.append(f"🤖 AI: approved {conf:.0%}" + (f" — {reason}" if reason else ""))
        except Exception:
            pass
    if str(plan.strategy).lower() == "liquidity_retest":
        lines.append("🏃 Runner: managed by liquidity retest rules")
    return "\n".join(lines)




def _bool_setting(settings: dict, key: str, default: bool = False) -> bool:
    raw = settings.get(key, default)
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}

def format_ai_decision(plan, verdict, stage: str = "done") -> str:
    """Short Telegram-only AI decision message.

    This does not create another OpenAI request and does not spend extra tokens;
    it only formats the already requested verdict.
    """
    symbol = getattr(plan, "symbol", "-")
    side = getattr(plan, "side", "-")
    strategy = getattr(plan, "strategy", "-")
    if stage == "start":
        return (
            "🤖 AI analysis started\n"
            f"{symbol} {side}\n"
            f"Strategy: {strategy}\n"
            f"Model: {getattr(verdict, 'model', '-')} | Mode: {getattr(verdict, 'mode', '-')}"
        )
    approved = bool(getattr(verdict, "approved", False))
    icon = "✅" if approved else "❌"
    status = "approved" if approved else "rejected"
    conf = float(getattr(verdict, "confidence", 0.0) or 0.0)
    reason = (getattr(verdict, "reason", "") or getattr(verdict, "error", "") or "no reason").strip()
    if len(reason) > 160:
        reason = reason[:157] + "..."
    return (
        f"{icon} AI {status} setup\n"
        f"{symbol} {side}\n"
        f"Strategy: {strategy}\n"
        f"Model: {getattr(verdict, 'model', '-')} | Mode: {getattr(verdict, 'mode', '-')}\n"
        f"Confidence: {conf:.2f}\n"
        f"Reason: {reason}"
    )

def format_position_event(ev: dict) -> str:
    symbol = ev.get("symbol", "-")
    typ = ev.get("type", "event")
    result = ev.get("result") if isinstance(ev.get("result"), dict) else {}
    reason_map = {
        "tp": "take profit",
        "sl": "stop loss",
        "time_stop": "time stop",
        "trailing_exit": "trailing scalp exit",
        "limit_timeout": "limit timeout",
        "limit_filled": "limit filled",
        "limit_canceled": "limit canceled",
        "limit_cancelled": "limit canceled",
        "limit_rejected": "limit rejected",
        "limit_expired": "limit expired",
        "breakeven": "breakeven moved",
        "protection_failed": "protection failed",
        "protection_local": "exchange protection failed; bot monitors TP/SL locally",
    }
    label = reason_map.get(str(typ), str(typ))
    lines = [f"📌 Position event", f"{symbol}: {label}"]
    if "pnl_usdt" in result or "pnl_pct" in result:
        try:
            pnl_usdt = float(result.get("pnl_usdt") or 0)
            pnl_pct = float(result.get("pnl_pct") or 0)
            sign = "+" if pnl_usdt >= 0 else ""
            lines.append(f"PnL: {sign}{pnl_usdt:.4f} USDT ({sign}{pnl_pct:.2f}%)")
        except Exception:
            pass
    reason = str(result.get("reason") or "")
    noisy = ("2009" in reason or "1001" in reason or "2015" in reason or "nonexistent or closed" in reason.lower() or "hidden margin" in reason.lower() or "contract does not exist" in reason.lower() or "precision error" in reason.lower() or "HTTP 200" in reason)
    if reason and not noisy:
        lines.append(f"Reason: {reason}")
    elif reason and noisy:
        # Legacy suppressed text marker for tests: exchange already flat / close confirmed
        # v0079: keep Telegram clean. MEXC 2009/hidden-margin/HTTP details are
        # normal post-close settlement noise and must not be shown to the user.
        pass
    return "\n".join(lines)


async def fetch_public_ip(use_proxy: bool = False, proxy_url: str = "", timeout_sec: int = 10) -> dict:
    """Return public IP info for direct or proxied HTTP path.

    Supports HTTP/HTTPS proxies through aiohttp's proxy argument and SOCKS
    proxies through aiohttp-socks. This is used by /balance and /proxy test so
    the user can see the real Railway/VPS IP and the proxy exit IP separately.
    """
    test_url = os.getenv("PROXY_TEST_URL", "https://api.ipify.org?format=json")
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    connector = None
    proxy_arg = None
    try:
        if use_proxy and proxy_url:
            from urllib.parse import urlparse
            scheme = urlparse(proxy_url).scheme.lower()
            if scheme.startswith("socks"):
                try:
                    from aiohttp_socks import ProxyConnector
                except Exception as dep_err:
                    raise RuntimeError(f"SOCKS proxy requires aiohttp-socks: {dep_err}")
                connector = ProxyConnector.from_url(proxy_url)
            else:
                proxy_arg = proxy_url
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            async with session.get(test_url, proxy=proxy_arg) as resp:
                body = await resp.text()
                status_code = resp.status
        ip = ""
        try:
            import json
            data = json.loads(body)
            ip = str(data.get("ip") or data.get("origin") or "")
        except Exception:
            ip = body.strip()[:120]
        return {"ok": 200 <= status_code < 300, "status": status_code, "ip": ip or "unknown", "error": ""}
    except Exception as e:
        return {"ok": False, "status": None, "ip": "unavailable", "error": str(e)[:240]}

def mask_secret(value: str) -> str:
    value = str(value or "")
    if not value:
        return "missing"
    if len(value) <= 8:
        return "saved"
    return f"{value[:4]}...{value[-4:]}"

async def reset_exchange() -> None:
    global exchange_client
    if exchange_client:
        try:
            await exchange_client.close()
        except Exception:
            pass
    exchange_client = None

async def reset_ws() -> None:
    global ws_supervisor
    if ws_supervisor:
        try:
            await ws_supervisor.stop()
        except Exception:
            pass
    ws_supervisor = None

async def reset_market_runtime() -> None:
    # Force scanner/ws to rebind after source/proxy/ws settings change.
    await reset_ws()
    scanner.last_refresh = 0
    scanner.last_scan_source = "reset"
    scanner.last_refresh_error = ""
    scanner.last_reject_reason = "settings changed; scanner will refresh"

async def get_exchange(settings: dict):
    global exchange_client
    api_key, api_secret = _api_creds(settings)
    proxy_enabled = bool(settings.get("proxy_enabled", False))
    proxy_url = str(settings.get("proxy_url", ""))
    desired_signature = (DEFAULT_EXCHANGE, proxy_url, proxy_enabled, api_key, bool(api_secret))
    if exchange_client and getattr(exchange_client, "_bot_signature", None) == desired_signature:
        return exchange_client
    if exchange_client:
        try:
            await exchange_client.close()
        except Exception:
            pass
    exchange_client = await ExchangeClient(DEFAULT_EXCHANGE, proxy_url, proxy_enabled).init(api_key, api_secret)
    exchange_client._bot_signature = desired_signature
    return exchange_client

async def get_ws(settings: dict):
    global ws_supervisor
    enabled = bool(settings.get("ws_enabled", True))
    venue = futures_source_from_mode(str(settings.get("scan_market_source", "mexc_binance")))
    proxy_enabled = bool(settings.get("proxy_enabled", False))
    proxy_url = str(settings.get("proxy_url", ""))
    ws_update_throttle_ms = int(settings.get("ws_update_throttle_ms", os.getenv("WS_UPDATE_THROTTLE_MS", "500")) or 500)
    ws_max_updates_per_batch = int(settings.get("ws_max_updates_per_batch", os.getenv("WS_MAX_UPDATES_PER_BATCH", "1000")) or 1000)
    ws_queue_limit = int(settings.get("ws_queue_limit", os.getenv("WS_QUEUE_LIMIT", "2000")) or 2000)
    ws_adaptive_slowdown_threshold = int(settings.get("ws_adaptive_slowdown_threshold", os.getenv("WS_ADAPTIVE_SLOWDOWN_THRESHOLD", "1000")) or 1000)
    ws_stale_sec = int(settings.get("ws_stale_sec", os.getenv("WS_STALE_SEC", "20")) or 20)
    desired_signature = (enabled, venue, proxy_enabled, proxy_url, ws_update_throttle_ms, ws_max_updates_per_batch, ws_queue_limit, ws_adaptive_slowdown_threshold, ws_stale_sec)
    current_signature = getattr(ws_supervisor, "_bot_signature", None) if ws_supervisor else None
    if ws_supervisor and current_signature == desired_signature:
        return ws_supervisor
    if ws_supervisor:
        await ws_supervisor.stop()
    ws_supervisor = WebSocketSupervisor(
        proxy_url=proxy_url,
        proxy_enabled=proxy_enabled,
        enabled=enabled,
        venue=venue,
        update_throttle_ms=ws_update_throttle_ms,
        max_updates_per_batch=ws_max_updates_per_batch,
        queue_limit=ws_queue_limit,
        adaptive_slowdown_threshold=ws_adaptive_slowdown_threshold,
        stale_sec=ws_stale_sec,
    )
    ws_supervisor._bot_signature = desired_signature
    return ws_supervisor

async def reply(update: Update, text: str, **kwargs):
    if update.message:
        return await update.message.reply_text(text, **kwargs)
    elif update.callback_query:
        return await update.callback_query.message.reply_text(text, **kwargs)
    return None

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    await reply(update, f"🤖 Liquidity Bot v{VERSION}\nГлавное меню:", reply_markup=MAIN_MENU)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    await reply(update, f"""
🤖 Liquidity Bot v{VERSION}

Команды:
/start - меню
/help - помощь
/run - запустить торговлю
/stop - остановить новые входы
/panic - закрыть позиции и отменить ордера
/status - статус
/ping - отклик ms, RAM, uptime, открыто сейчас и общий счётчик открытий
/balance - futures balance + IP/proxy; если MEXC margin=0 при открытых позициях, показывает estimated margin
/positions - локальные + реальные позиции MEXC + protection mode
/open_orders - обычные + plan/stop/TP-SL ордера MEXC
/cancel_all - отменить normal/plan/stop/TP-SL ордера MEXC, включая ghost/frozen orders
/close_all - закрыть реальные позиции, отменить все ордера, затем сверить balance/cache
/stats - статистика сделок
/ai_stats - меню статистики AI BTC/ETH scalping
/ai_stats_current - текущая AI session статистика
/ai_stats_lifetime - lifetime AI статистика
/ai_stats_reset - сбросить текущую AI session
/sync - синхронизация позиций/ордеров
/sync_positions - подтянуть реальные позиции MEXC в бота
/recovery - восстановить позиции MEXC после рестарта и проверить TP/SL
/mexc_debug_state [SYMBOL] - raw debug MEXC positions/orders/symbol variants

Note: /positions checks MEXC exchange-first; /open_orders scans normal + plan + stop + TP/SL endpoints. If exchange TP/SL is missing, local monitor protects positions kept in bot cache.
/proxy on|off|test|set URL
/api status|set KEY SECRET|clear|test - API биржи через чат
/openai status|set KEY|clear|test - OpenAI ключ для ИИ проверки
AI scalping loop: кнопка 🤖 AI BTC/ETH scalping или /set strategy_mode ai_scalping. BTC и ETH независимы: AI-запрос только по символу без открытой позиции. После live-входа биржевые TP/SL обязательны; если MEXC не подтвердил защиту после retry, позиция аварийно закрывается. Доп. фильтры качества включаются отдельно: ai_scalping_quality_filters_enabled.
/mexc_settings - показать MEXC параметры ордера
/leverage 5 - плечо MEXC futures
/open_type 1 - 1 isolated, 2 cross
/recv_window 20000 - окно timestamp для MEXC
/set margin_allocation_enabled true|false - делить баланс по слотам
/set auto_close_on_protection_failed true|false - авто-закрытие если биржевые TP/SL не подтвердились после входа
/set require_exchange_protection true|false - требовать exchange TP/SL
/set key value - ручная настройка

Ключевые настройки:
live_trading, risk_pct, max_open_positions, scan_interval_sec, scanner_concurrency,
ws_update_throttle_ms, ws_max_updates_per_batch, ws_queue_limit,
symbol_refresh_sec, universe_mode, strategy_mode, mirror_mode,
spot_confirmation_enabled, session_filter_enabled, america_short_bias_enabled, ws_enabled,
mexc_order_leverage, mexc_order_open_type, mexc_recv_window, margin_allocation_enabled, require_exchange_protection, auto_close_on_protection_failed, total_positions_opened, ai_scalping_session_id, ai_scalping_session_reset_at,
ai_scalping_symbols, ai_scalping_min_confidence, ai_scalping_tp_pct, ai_scalping_sl_pct, ai_scalping_btc_tp_pct, ai_scalping_btc_sl_pct, ai_scalping_eth_tp_pct, ai_scalping_eth_sl_pct, ai_scalping_max_spread_pct, ai_scalping_quality_filters_enabled, ai_scalping_quality_min_confidence, ai_scalping_quality_cooldown_sec, ai_scalping_quality_min_atr_pct, ai_scalping_quality_min_ema_gap_pct, ai_scalping_quality_min_ret_5m_abs_pct, ai_scalping_ai_cooldown_sec, ai_scalping_openai_fallback_enabled, ai_scalping_json_mode_enabled, ai_scalping_liquidation_stop_mode, ai_scalping_liq_margin_pct, ai_scalping_liq_buffer_pct, ai_scalping_liq_max_leverage,
scan_market_source = binance_binance | mexc_mexc | mexc_binance.

По умолчанию: mexc_binance = MEXC фьючи скан + Binance spot подтверждение.
""".strip(), reply_markup=MAIN_MENU)

async def run_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Regression marker: global running, trading_task
    global running, entries_enabled, trading_task
    if not allowed(update): return
    lock = context.application.bot_data.get("trading_start_lock")
    if lock is None:
        lock = asyncio.Lock()
        context.application.bot_data["trading_start_lock"] = lock
    already_running = False
    async with lock:
        if trading_task and not trading_task.done():
            running = True
            entries_enabled = True
            already_running = True
            context.application.bot_data["recovery_checked_for_run"] = False
        else:
            running = True
            entries_enabled = True
            context.application.bot_data["recovery_checked_for_run"] = False
            trading_task = context.application.create_task(trading_loop(context.application))

    # v0079: one Run press must create exactly one Telegram message. Earlier
    # versions replied "started" and then immediately sent a separate scanner
    # status card, which looked like duplicate start. The reply itself becomes
    # the editable scanner-status message.
    settings = await storage.all_settings()
    status = "already running; scan requested now" if already_running else "started; scan requested now"
    header = "🟢 Bot already running" if already_running else "🟢 Bot started"
    msg = await reply(update, f"{header}\n" + _scan_status_text(settings, status=status), reply_markup=MAIN_MENU)
    if msg is not None:
        context.application.bot_data["scanner_status_message_id"] = msg.message_id
        context.application.bot_data["scanner_status_last_edit"] = time.time()
    trigger_scan_now(context.application, reason="run_button")

async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global running, entries_enabled, trading_task
    if not allowed(update): return
    entries_enabled = False
    # Keep the loop alive when it is already running so open positions continue
    # through TP/SL/trailing/local exits. If the bot was fully stopped, do not
    # start a background scanner just because /stop was pressed.
    if trading_task and not trading_task.done():
        running = True
        status = "Position manager remains active."
    else:
        running = False
        status = "No active trading loop was running."
    await reply(update, "🟡 New entries stopped\n" + status + "\nUse /panic only for full emergency stop.", reply_markup=MAIN_MENU)

async def panic_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global running, entries_enabled
    if not allowed(update): return
    entries_enabled = False
    running = False
    settings = await storage.all_settings()
    live = bool(settings.get("live_trading", False))
    closed_local = 0
    closed_external = 0
    failures = []
    ex = None
    try:
        ex = await get_exchange(settings)
    except Exception as e:
        if live:
            failures.append(f"exchange init: {e}")
    exec_engine = ExecutionEngine(storage, ex)

    # Canceling orders must not block emergency position closing.
    if live and ex:
        try:
            await ex.cancel_all_orders()
        except Exception as e:
            failures.append(f"cancel_all_orders: {e}")

    # Always close local tracked positions. In paper mode this removes SQLite
    # positions and records a trade; in live mode it also sends reduce-only close orders.
    local_positions = await storage.positions()
    local_symbols = {p.get("symbol") for p in local_positions}
    for p in local_positions:
        res = await exec_engine.close_position(p, "panic", live=live, exit_price=p.get("entry_price"))
        if res.get("ok"):
            closed_local += 1
        else:
            failures.append(f"local {p.get('symbol')}: {res.get('reason')}")

    # Live-only: also close positions that exist on the exchange but are missing locally.
    native_close_res = None
    if live and ex:
        try:
            exchange_positions = await ex.fetch_positions()
            for p in exchange_positions or []:
                qty = exec_engine.exchange_position_qty(p)
                symbol = p.get("symbol") or (p.get("info") or {}).get("symbol")
                if qty <= 0 or symbol in local_symbols:
                    continue
                res = await exec_engine.close_exchange_position(p, "panic_external")
                if res.get("ok"):
                    closed_external += 1
                else:
                    failures.append(f"exchange {symbol}: {res.get('reason')}")
        except Exception as e:
            failures.append(f"fetch/close exchange positions: {e}")
        # Final emergency fallback: native MEXC close_all only if we did not
        # already close listed positions. Suppress harmless MEXC 2009 responses
        # meaning the position is already gone.
        if closed_local == 0 and closed_external == 0 and hasattr(ex, "mexc_close_all_positions_native"):
            try:
                native_close_res = await ex.mexc_close_all_positions_native()
            except Exception as e:
                if "2009" not in str(e) and "nonexistent or closed" not in str(e).lower():
                    failures.append(f"native_close_all: {e}")
                else:
                    native_close_res = {"ok": True, "skipped": "already closed"}

    text = (
        "🚨 PANIC MODE\n"
        "Trading disabled. Close workflow executed.\n"
        f"Tracked positions closed: {closed_local}\n"
        f"Exchange-only positions closed: {closed_external}\n"
        f"Native close_all: {str(native_close_res)[:180] if native_close_res is not None else '-'}"
    )
    if failures:
        text += "\n⚠️ Failures:\n" + "\n".join(failures[:10])
    await reply(update, text, reply_markup=MAIN_MENU)

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    s = await storage.all_settings()
    positions = await storage.positions()
    ws = await get_ws(s)
    ws_text = ws.status_text()
    text = f"""
📊 Status v{VERSION}

Running: {running}
New entries: {entries_enabled}
Live: {s.get('live_trading')}
Strategy: {s.get('strategy_mode')}
Effective strategy: {scanner.last_effective_strategy}
Strategy reason: {scanner.last_strategy_reason}
Universe: {s.get('universe_mode')}
Risk: {float(s.get('risk_pct',0))*100:.2f}%
Max positions: {s.get('max_open_positions')}
Margin allocation: {s.get('margin_allocation_enabled', True)}
Scan: {format_duration_seconds(s.get('scan_interval_sec', 5))}
Concurrency: {s.get('scanner_concurrency', 5)} | last scanned={scanner.last_cycle_scanned} | errors={scanner.last_cycle_errors} | slowdown={scanner.last_slowdown_sec}s
Refresh: {s.get('symbol_refresh_sec')}s
Mirror: {s.get('mirror_mode')}
Spot confirmation: {s.get('spot_confirmation_enabled')}
Фьючи/Спот source: {s.get('scan_market_source', 'mexc_binance')}
Session filter: {s.get('session_filter_enabled')}
America short bias: {s.get('america_short_bias_enabled')}
Open positions: {len(positions)}
Revision: {s.get('settings_revision')}
Recovery: {context.application.bot_data.get('last_recovery_status', context.application.bot_data.get('startup_recovery_error', '-'))}
Scan source: {scanner.last_scan_source}
Markets total: {scanner.last_total_markets}
Markets available: {scanner.last_available_markets}
Markets filtered: {scanner.last_filtered_markets}
Symbols requested: {scanner.last_requested_symbols}
Symbols loaded: {len(scanner.hot_symbols)}
Last signal: {scanner.last_signal_summary}
Last decision: {scanner.last_reject_reason}
Scanner error: {scanner.last_refresh_error or '-'}

{ws_text}
""".strip()
    await reply(update, text, reply_markup=MAIN_MENU)

async def ping_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    t0 = time.perf_counter()
    proc = psutil.Process()
    ram = proc.memory_info().rss / 1024 / 1024
    uptime = int(time.time() - started_at)
    total_opened = 0
    local_open = 0
    exchange_open = "n/a"
    try:
        total_opened = int(await storage.get("total_positions_opened", 0) or 0)
        local_open = len([p for p in await storage.positions() if str(p.get("status", "open")).lower() in {"open", "pending"}])
    except Exception:
        pass
    try:
        s = await storage.all_settings()
        ex = await get_exchange(s)
        exec_engine = ExecutionEngine(storage, ex)
        positions = await ex.fetch_positions()
        exchange_open = len([p for p in (positions or []) if exec_engine.exchange_position_qty(p) > 0])
    except Exception:
        exchange_open = "n/a"
    response_ms = (time.perf_counter() - t0) * 1000
    await reply(
        update,
        f"🏓 Pong\n"
        f"Version: {VERSION}\n"
        f"Response: {response_ms:.0f} ms\n"
        f"RAM: {ram:.1f} MB\n"
        f"Uptime: {uptime}s\n"
        f"Open now: local {local_open} | exchange {exchange_open}\n"
        f"Total positions opened: {total_opened}",
        reply_markup=MAIN_MENU,
    )

async def balance_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    s = await storage.all_settings()
    proxy_enabled = bool(s.get("proxy_enabled", False))
    proxy_url = str(s.get("proxy_url", "") or "")

    direct_ip = await fetch_public_ip(use_proxy=False)
    proxy_ip = await fetch_public_ip(use_proxy=True, proxy_url=proxy_url) if proxy_enabled and proxy_url else {"ok": False, "ip": "not configured", "error": "proxy off or missing"}

    balance_error = ""
    free = total = "n/a"
    try:
        ex = await get_exchange(s)
        bal = await ex.fetch_balance()
        usdt = bal.get("USDT", {}) if isinstance(bal, dict) else {}
        free = usdt.get("free", "n/a") if isinstance(usdt, dict) else "n/a"
        total = usdt.get("total", "n/a") if isinstance(usdt, dict) else "n/a"
        used = usdt.get("used", "n/a") if isinstance(usdt, dict) else "n/a"
        position_margin = usdt.get("positionMargin", "n/a") if isinstance(usdt, dict) else "n/a"
        frozen_balance = usdt.get("frozenBalance", "n/a") if isinstance(usdt, dict) else "n/a"
        unrealized = usdt.get("unrealized", "n/a") if isinstance(usdt, dict) else "n/a"
        # MEXC can report positionMargin=0 even while open_positions returns
        # live positions. Show a safe effective margin instead of misleading 0.
        try:
            raw_pm = float(position_margin or 0)
        except Exception:
            raw_pm = 0.0
        exchange_positions = []
        try:
            if hasattr(ex, "_mexc_fetch_positions"):
                exchange_positions = await ex._mexc_fetch_positions()
            else:
                exchange_positions = await ex.fetch_positions()
        except Exception:
            exchange_positions = []
        est_pm, est_count = _estimate_exchange_position_margin(
            exchange_positions,
            int(float(s.get("mexc_order_leverage") or os.getenv("MEXC_ORDER_LEVERAGE", "5") or 5)),
        )
        if raw_pm <= 0 and est_pm > 0:
            position_margin = f"~{est_pm:.6f} estimated ({est_count} open position; MEXC raw=0)"
    except Exception as e:
        balance_error = str(e)[:240]
        used = position_margin = frozen_balance = unrealized = "n/a"

    proxy_line = f"{proxy_ip.get('ip')}" if proxy_ip.get("ok") else f"{proxy_ip.get('ip')} ({proxy_ip.get('error')})"
    direct_line = f"{direct_ip.get('ip')}" if direct_ip.get("ok") else f"{direct_ip.get('ip')} ({direct_ip.get('error')})"
    text = (
        "💰 Futures Balance\n"
        f"USDT free: {free}\n"
        f"USDT total: {total}\n"
        f"USDT used: {used}\n"
        f"Position margin: {position_margin}\n"
        f"Frozen balance: {frozen_balance}\n"
        f"Unrealized PnL: {unrealized}\n"
        f"Balance error: {balance_error or '-'}\n\n"
        "🌍 IP diagnostics\n"
        f"Direct IP: {direct_line}\n"
        f"Proxy enabled: {proxy_enabled}\n"
        f"Proxy IP: {proxy_line}"
    )
    await reply(update, text, reply_markup=MAIN_MENU)



def _position_identity_keys(pos: dict, ex=None) -> set[str]:
    """Return stable MEXC-style keys for matching local and exchange rows."""
    keys: set[str] = set()

    def add(v):
        if v in (None, ""):
            return
        try:
            if ex and hasattr(ex, "_mexc_normalize_contract_id"):
                k = ex._mexc_normalize_contract_id(v)
            else:
                k = str(v).upper().replace('/USDT:USDT', '_USDT').replace('/', '_').replace('-', '_')
                if k.endswith('USDT') and '_' not in k:
                    k = k[:-4] + '_USDT'
            if k:
                keys.add(k)
        except Exception:
            pass

    for key in ("symbol", "mexc_symbol", "contract"):
        add(pos.get(key))
    for v in pos.get("symbol_variants") or []:
        add(v)
    info = pos.get("info") or {}
    if isinstance(info, dict):
        for key in ("symbol", "contract"):
            add(info.get(key))
    return keys


def _dedupe_exchange_positions(rows: list[dict], ex=None) -> list[dict]:
    """Collapse duplicate MEXC rows returned by several native endpoints."""
    out: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for row in rows or []:
        keys = sorted(_position_identity_keys(row, ex))
        info = row.get("info") or {}
        raw_side = ""
        if isinstance(info, dict):
            raw_side = str(info.get("positionType") or info.get("holdSide") or info.get("side") or "")
        side = str(row.get("side") or raw_side or "").lower()
        if raw_side in {"1"}:
            side = "long"
        elif raw_side in {"2"}:
            side = "short"
        ident = (keys[0] if keys else str(row.get("symbol") or ""), side)
        if ident in seen:
            continue
        seen.add(ident)
        out.append(row)
    return out


async def _hidden_margin_present(ex) -> bool:
    """Protect against MEXC returning empty position rows while balance still shows margin."""
    try:
        bal = await ex.fetch_balance()
        usdt = (bal or {}).get("USDT", {}) if isinstance(bal, dict) else {}
        used = float(usdt.get("used") or ((bal or {}).get("used", {}) or {}).get("USDT") or 0)
        pm = float(usdt.get("positionMargin") or usdt.get("position_margin") or 0)
        upnl = float(usdt.get("unrealized") or 0)
        return used > 0.5 or pm > 0.5 or abs(upnl) > 0.01
    except Exception:
        return False

def _exchange_position_text(p: dict) -> str:
    info = p.get("info", {}) if isinstance(p.get("info"), dict) else {}
    symbol = p.get("symbol") or info.get("symbol") or "-"
    side = str(p.get("side") or info.get("side") or "-").upper()
    qty = _position_base_qty(p)
    contracts, contract_size = _position_contract_fields(p)
    entry = 0.0
    for key in ("entryPrice", "entry_price", "average"):
        try:
            value = p.get(key)
            if value not in (None, "") and float(value) > 0:
                entry = float(value); break
        except Exception:
            pass
    if entry <= 0:
        for key in ("holdAvgPrice", "openAvgPrice", "entryPrice"):
            try:
                value = info.get(key)
                if value not in (None, "") and float(value) > 0:
                    entry = float(value); break
            except Exception:
                pass
    notional = abs(qty * entry) if qty and entry else 0.0
    coin = str(symbol).split('/')[0]
    extra = f"contracts={contracts:.0f} | " if contracts > 0 and contract_size > 0 else ""
    return f"{symbol} {side} exchange\n{extra}Qty={qty:.8f} {coin} / {notional:.2f} USDT | entry={entry:.8f}"

async def positions_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    s = await storage.all_settings()
    local = await storage.positions()
    exchange_positions = []
    exchange_error = ""
    reconcile_notes = []
    # v0072: /positions reconciles exchange-first before rendering. It still must NOT depend on live_trading/Run. This removes
    # stale local rows and imports real exchange-only rows so the screen no
    # longer shows a mixed/duplicated position state after restart/close.
    api_key, api_secret = _api_creds(s)
    ex = None
    if api_key and api_secret:
        try:
            ex = await get_exchange(s)
            raw_positions = await ex.fetch_positions() or []
            exec_engine = ExecutionEngine(storage, ex)
            exchange_positions = _dedupe_exchange_positions([p for p in raw_positions if exec_engine.exchange_position_qty(p) > 0], ex)

            exchange_keys = set()
            for ep in exchange_positions:
                exchange_keys |= _position_identity_keys(ep, ex)

            hidden_margin = False
            if not exchange_positions:
                hidden_margin = await _hidden_margin_present(ex)

            # Import/update real exchange positions into local cache without
            # creating extra protection orders from a read-only /positions call.
            if exchange_positions:
                rec = await RecoveryEngine(storage, ex, exec_engine).recover(reattach=True)
                if rec.get("restored") or rec.get("updated"):
                    reconcile_notes.append(f"reconciled local cache: +{rec.get('restored', 0)} restored, {rec.get('updated', 0)} updated")

            # Remove local open rows that no longer exist on MEXC. Do not prune
            # if MEXC hides rows while balance still shows margin/PnL.
            if not hidden_margin:
                removed = 0
                for lp in list(await storage.positions()):
                    status = str(lp.get("status") or "").lower()
                    if status not in {"open", "closing"}:
                        continue
                    lk = _position_identity_keys(lp, ex)
                    if not (lk & exchange_keys):
                        await storage.remove_position(lp.get("symbol"))
                        removed += 1
                if removed:
                    reconcile_notes.append(f"removed stale local rows: {removed}")
            local = await storage.positions()
        except Exception as e:
            exchange_error = str(e)[:220]
    if not local and not exchange_positions:
        text = "📈 Positions: none"
        # MEXC can occasionally return an empty positions list while account
        # assets still show positionMargin/used/unrealized PnL. Surface that
        # clearly instead of pretending the account is clean.
        try:
            api_key2, api_secret2 = _api_creds(s)
            if api_key2 and api_secret2:
                ex2 = await get_exchange(s)
                bal = await ex2.fetch_balance()
                usdt = (bal or {}).get("USDT", {}) if isinstance(bal, dict) else {}
                used = float(usdt.get("used") or ((bal or {}).get("used", {}) or {}).get("USDT") or 0)
                pm = float(usdt.get("positionMargin") or usdt.get("position_margin") or 0)
                frozen = float(usdt.get("frozenBalance") or 0)
                upnl = float(usdt.get("unrealized") or 0)
                if used > 0.5 or pm > 0.5 or abs(upnl) > 0.01:
                    text = (
                        "📈 Positions: ⚠️ hidden exchange margin\n⚠️ Hidden MEXC margin detected"
                        f"\nUsed: {used:.4f} USDT"
                        f"\nPosition margin: {pm:.4f} USDT"
                        f"\nFrozen: {frozen:.4f} USDT"
                        f"\nUnrealized PnL: {upnl:.4f} USDT"
                        "\n\nMEXC did not return a position row, but account assets show live margin/PnL."
                        "\nThis is NOT flat. Do not start new trading until it is closed."
                        "\nUse /close_all to send native MEXC close-all, then /balance."
                    )
        except Exception as e:
            text += f"\nHidden-margin check failed: {str(e)[:160]}"
        if exchange_error:
            text += f"\nExchange sync error: {exchange_error}"
        await reply(update, text, reply_markup=MAIN_MENU); return
    lines = ["📈 Positions"]
    if reconcile_notes:
        lines.append("\nSync cleanup: " + "; ".join(reconcile_notes))
    if local:
        lines.append("\nLocal bot state:")
        for p in local:
            notional, margin, leverage, margin_type = _position_money_fields(p)
            coin = str(p.get('symbol', '')).split('/')[0]
            warn = ""
            status = p.get("protection_status") or ("EXCHANGE PROTECTED" if p.get("protection_mode") == "exchange" else "LOCAL BOT PROTECTED" if p.get("protection_mode") == "local_monitoring" else "UNKNOWN")
            if status == "EXCHANGE PROTECTED":
                warn = "\n🛡️ Protection: EXCHANGE PROTECTED (TP/SL confirmed on MEXC)"
            elif status == "LOCAL BOT PROTECTED" or p.get("protection_mode") == "local_monitoring" or p.get("protection_warning"):
                warn = "\n🟡 Protection: LOCAL BOT PROTECTED (bot monitors TP/SL/time-stop locally)"
            else:
                warn = f"\nℹ️ Protection: {status}"
            details = []
            if status == "EXCHANGE PROTECTED" and (p.get("tp_exists") is not None or p.get("sl_exists") is not None):
                details.append(f"TP={'yes' if p.get('tp_exists') else 'no'} SL={'yes' if p.get('sl_exists') else 'no'}")
            # Do not leak raw exchange HTTP/precision/contract messages into Telegram.
            # They are stored in raw position data for diagnostics, while UI stays clean.
            if details:
                warn += "\n" + " | ".join(details)
            lines.append(
                f"{p.get('symbol')} {p.get('side')} {p.get('status')} "
                f"entry={p.get('entry_price')} SL={p.get('stop_price')} TP={p.get('take_price')}\n"
                f"Qty={_position_base_qty(p):.8f} {coin} / {notional:.2f} USDT | "
                f"Lev={leverage}x | Margin={margin_type} ~{margin:.2f} USDT" + warn
            )
    if exchange_positions:
        local_keys = set()
        try:
            ex_for_variants = await get_exchange(s)
            for lp in local:
                for v in (lp.get("symbol_variants") or []):
                    local_keys.add(ex_for_variants._mexc_normalize_contract_id(v))
                if lp.get("symbol"):
                    for v in ex_for_variants.mexc_symbol_variants(lp.get("symbol")):
                        local_keys.add(ex_for_variants._mexc_normalize_contract_id(v))
                if lp.get("mexc_symbol"):
                    local_keys.add(ex_for_variants._mexc_normalize_contract_id(lp.get("mexc_symbol")))
        except Exception:
            local_keys = {p.get("symbol") for p in local}
        lines.append("\nExchange real positions:")
        for p in exchange_positions:
            p_keys = set()
            try:
                ex_for_variants = await get_exchange(s)
                for v in (p.get("symbol_variants") or []):
                    p_keys.add(ex_for_variants._mexc_normalize_contract_id(v))
                p_keys.add(ex_for_variants._mexc_normalize_contract_id(p.get("symbol")))
                p_keys.add(ex_for_variants._mexc_normalize_contract_id(p.get("mexc_symbol")))
                info = p.get("info") or {}
                if isinstance(info, dict):
                    p_keys.add(ex_for_variants._mexc_normalize_contract_id(info.get("symbol")))
                    p_keys.add(ex_for_variants._mexc_normalize_contract_id(info.get("contract")))
            except Exception:
                p_keys = {p.get("symbol")}
            prefix = "✅ synced" if (p_keys & local_keys) else "⚠️ exchange-only"
            lines.append(prefix + " " + _exchange_position_text(p))
    if exchange_error:
        lines.append(f"\nExchange sync error: {exchange_error}")
    await reply(update, "\n".join(lines), reply_markup=MAIN_MENU)

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    trades = await storage.trade_rows()
    stats = AdaptiveEngine().calc_stats(trades)
    n = stats["normal"]; m = stats["mirror"]
    text = f"""
📉 Stats

Trades: {len(trades)}
Normal PF: {n['profit_factor']:.2f}
Normal WR: {n['winrate']:.1f}%
Normal Expectancy: {n['expectancy']:.4f}

Mirror PF: {m['profit_factor']:.2f}
Mirror WR: {m['winrate']:.1f}%
Mirror Expectancy: {m['expectancy']:.4f}
""".strip()
    await reply(update, text, reply_markup=MAIN_MENU)



async def ai_stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    s = await storage.all_settings()
    rev = int(s.get("settings_revision", 1))
    cur = await ai_stats_manager.summary("current")
    await reply(update, AIStatsManager.format(cur), reply_markup=ai_stats_menu(rev))

async def ai_stats_current_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    await reply(update, AIStatsManager.format(await ai_stats_manager.summary("current")), reply_markup=MAIN_MENU)

async def ai_stats_lifetime_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    await reply(update, AIStatsManager.format(await ai_stats_manager.summary("lifetime")), reply_markup=MAIN_MENU)

async def ai_stats_reset_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    sid = await ai_stats_manager.reset_session()
    await reply(update, f"♻ AI scalping session reset\nNew session ID: {sid}", reply_markup=MAIN_MENU)

async def recovery_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    s = await storage.all_settings()
    try:
        ex = await get_exchange(s)
        exec_engine = ExecutionEngine(storage, ex)
        report = await RecoveryEngine(storage, ex, exec_engine).recover(reattach=True)
        lines = ["🛟 Recovery engine"]
        for k, v in report.items():
            lines.append(f"{k}: {v}")
        await reply(update, "\n".join(lines), reply_markup=MAIN_MENU)
    except Exception as e:
        await reply(update, f"🛟 Recovery failed: {e}", reply_markup=MAIN_MENU)

async def sync_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    s = await storage.all_settings()
    try:
        ex = await get_exchange(s)
        report = await SyncEngine(storage, ex).sync(protect=True)
        await reply(update, "🔄 Sync\n" + "\n".join(f"{k}: {v}" for k,v in report.items()), reply_markup=MAIN_MENU)
    except Exception as e:
        await reply(update, f"🔄 Sync failed: {e}", reply_markup=MAIN_MENU)

async def sync_positions_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    s = await storage.all_settings()
    try:
        ex = await get_exchange(s)
        report = await SyncEngine(storage, ex).sync(protect=True)
        await reply(update, "🔄 Sync positions done\n" + "\n".join(f"{k}: {v}" for k,v in report.items()), reply_markup=MAIN_MENU)
    except Exception as e:
        await reply(update, f"🔄 Sync positions failed: {e}", reply_markup=MAIN_MENU)

def order_display_line(o: dict) -> str:
    info = o.get('info') if isinstance(o.get('info'), dict) else {}
    src = str(info.get('_source_endpoint') or '-').replace('/api/v1/private/', '')
    kind = str(info.get('_protection_kind') or '').upper()
    typ = str(o.get('type') or 'unknown')
    label = kind or typ
    trigger = o.get('price')
    amount = o.get('amount')
    remaining = o.get('remaining')
    return (
        f"{o.get('symbol')} {label} {o.get('side')} "
        f"id={o.get('id') or '-'} trigger/price={trigger} "
        f"amount={amount} remaining={remaining} src={src} "
        f"client={o.get('clientOrderId') or '-'}"
    )

async def open_orders_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    s = await storage.all_settings()
    try:
        ex = await get_exchange(s)
        orders = await ex.fetch_open_orders()
        positions = []
        try:
            exec_engine = ExecutionEngine(storage, ex)
            positions = [p for p in (await ex.fetch_positions() or []) if exec_engine.exchange_position_qty(p) > 0]
        except Exception:
            positions = []
        if not orders:
            msg = "📋 Open orders: none"
            if positions:
                msg += "\n⚠️ Есть реальные позиции, но exchange TP/SL/open orders не найдены. Бот будет вести TP/SL локальным мониторингом, если позиция есть в local cache. Для аварийного выхода: /close_all или Panic."
            await reply(update, msg, reply_markup=MAIN_MENU); return
        lines = [f"📋 Open orders/protection: {len(orders)}"]
        for o in orders[:30]:
            lines.append(order_display_line(o))
        await reply(update, "\n".join(lines), reply_markup=MAIN_MENU)
    except Exception as e:
        await reply(update, f"📋 Open orders failed: {e}", reply_markup=MAIN_MENU)

async def cancel_all_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    s = await storage.all_settings()
    try:
        ex = await get_exchange(s)
        res = await ex.cancel_all_orders()
        await reply(update, f"🧹 Cancel all orders sent\n{str(res)[:1200]}", reply_markup=MAIN_MENU)
    except Exception as e:
        await reply(update, f"🧹 Cancel all failed: {e}", reply_markup=MAIN_MENU)

async def close_all_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    s = await storage.all_settings()
    try:
        ex = await get_exchange(s)
        exec_engine = ExecutionEngine(storage, ex)
        positions = [p for p in (await ex.fetch_positions() or []) if exec_engine.exchange_position_qty(p) > 0]
        failures = []
        closed = 0
        for p in positions:
            res = await exec_engine.close_exchange_position(p, "manual_close_all")
            if res.get("ok"):
                closed += 1
            else:
                failures.append(f"{p.get('symbol')}: {res.get('reason')}")
        native_res = None
        cancel_res = None
        try:
            cancel_res = await ex.cancel_all_orders()
        except Exception as e:
            failures.append(f"cancel_all: {e}")
        # Extra safety: call native close_all only when nothing was listed and
        # closed manually. If listed positions were already closed, MEXC often
        # returns code 2009 (Position is nonexistent or closed), which is not a
        # real failure and only confuses the operator.
        if closed == 0 and hasattr(ex, "mexc_close_all_positions_native"):
            try:
                native_res = await ex.mexc_close_all_positions_native()
            except Exception as e:
                if "2009" not in str(e) and "nonexistent or closed" not in str(e).lower():
                    failures.append(f"native_close_all: {e}")
                else:
                    native_res = {"ok": True, "skipped": "already closed"}
        # v0067: clear local cache only after balance confirms there is no hidden
        # position margin left. Previously the bot could erase local state while
        # MEXC still showed used/positionMargin > 0.
        local_cache_cleared = False
        post_pm = post_used = None
        try:
            await asyncio.sleep(float(os.getenv("POST_CLOSE_BALANCE_CHECK_DELAY_SEC", "0.8")))
            bal = await ex.fetch_balance()
            usdt = (bal or {}).get("USDT", {}) if isinstance(bal, dict) else {}
            post_pm = float(usdt.get("positionMargin") or usdt.get("position_margin") or 0)
            post_used = float(usdt.get("used") or ((bal or {}).get("used", {}) or {}).get("USDT") or 0)
            try:
                post_positions = [p for p in (await ex.fetch_positions() or []) if exec_engine.exchange_position_qty(p) > 0]
            except Exception:
                post_positions = []
            # Balance.used/frozen can stay non-zero because of leftover orders.
            # Local position cache must follow real exchange positions, not stale
            # frozen margin, otherwise the bot monitors ghosts after /close_all.
            if not post_positions:
                for lp in await storage.positions():
                    try:
                        await storage.remove_position(lp.get("symbol"))
                    except Exception:
                        pass
                local_cache_cleared = True
            else:
                local_cache_cleared = False
                failures.append(f"positions still open after close_all: {len(post_positions)}")
        except Exception as e:
            failures.append(f"post-close balance check: {e}")
        await reply(update, f"🧯 Close all sent\nListed positions closed: {closed}\nCancel all: {str(cancel_res)[:220] if cancel_res is not None else '-'}\nNative close_all: {str(native_res)[:300] if native_res is not None else '-'}\nPost used: {post_used if post_used is not None else '-'}\nPost position margin: {post_pm if post_pm is not None else '-'}\nLocal cache cleared: {'yes' if local_cache_cleared else 'no'}\nFailures: {[f for f in failures[:5] if 'hidden margin' not in str(f).lower()] if failures else '-'}", reply_markup=MAIN_MENU)
    except Exception as e:
        await reply(update, f"🧯 Close all failed: {e}", reply_markup=MAIN_MENU)

async def mexc_debug_state_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    s = await storage.all_settings()
    symbol = context.args[0] if context.args else None
    try:
        ex = await get_exchange(s)
        if not hasattr(ex, "mexc_debug_state"):
            await reply(update, "MEXC debug is not available for this exchange", reply_markup=MAIN_MENU); return
        report = await ex.mexc_debug_state(symbol)
        lines = ["🧪 MEXC raw state debug"]
        if symbol:
            lines.append(f"Symbol: {symbol}")
            lines.append("Variants: " + ", ".join(report.get("variants") or [])[:500])
        shown = 0
        for item in (report.get("endpoints") or [])[:24]:
            ep = item.get("endpoint")
            q = item.get("query")
            if item.get("error"):
                continue
            sample = str(item.get("sample"))
            if len(sample) > 260:
                sample = sample[:260] + "..."
            lines.append(f"{ep} {q}: rows={item.get('rows')} base={item.get('base')} sample={sample}")
            shown += 1
        if shown == 0:
            lines.append("No debug rows returned from supported MEXC endpoints")
        await reply(update, "\n".join(lines)[:3900], reply_markup=MAIN_MENU)
    except Exception as e:
        await reply(update, f"🧪 MEXC debug failed: {e}", reply_markup=MAIN_MENU)

async def settings_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    s = await storage.all_settings()
    rev = int(s.get("settings_revision", 1))
    await reply(update, "⚙️ Settings", reply_markup=settings_menu(rev, s))

async def mexc_settings_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    s = await storage.all_settings()
    apply_mexc_runtime_env(s)
    await reply(update, mexc_order_settings_text(s), reply_markup=MAIN_MENU)


async def leverage_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    if not context.args:
        await reply(update, "Usage: /leverage 5", reply_markup=MAIN_MENU); return
    try:
        value = int(float(context.args[0]))
        if value < 1 or value > 200:
            raise ValueError("leverage must be 1..200")
        await storage.set("mexc_order_leverage", value)
        os.environ["MEXC_ORDER_LEVERAGE"] = str(value)
        await reset_exchange()
        await reply(update, f"✅ MEXC leverage saved: {value}x", reply_markup=MAIN_MENU)
    except Exception as e:
        await reply(update, f"❌ /leverage error: {e}", reply_markup=MAIN_MENU)


async def open_type_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    if not context.args:
        await reply(update, "Usage: /open_type 1  (1 isolated, 2 cross)", reply_markup=MAIN_MENU); return
    try:
        value = int(float(context.args[0]))
        if value not in {1, 2}:
            raise ValueError("use 1 for isolated or 2 for cross")
        await storage.set("mexc_order_open_type", value)
        os.environ["MEXC_ORDER_OPEN_TYPE"] = str(value)
        await reset_exchange()
        label = "isolated" if value == 1 else "cross"
        await reply(update, f"✅ MEXC open type saved: {value} ({label})", reply_markup=MAIN_MENU)
    except Exception as e:
        await reply(update, f"❌ /open_type error: {e}", reply_markup=MAIN_MENU)


async def recv_window_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    if not context.args:
        await reply(update, "Usage: /recv_window 20000", reply_markup=MAIN_MENU); return
    try:
        value = int(float(context.args[0]))
        if value < 5000 or value > 60000:
            raise ValueError("recv_window must be 5000..60000 ms")
        await storage.set("mexc_recv_window", value)
        os.environ["MEXC_RECV_WINDOW"] = str(value)
        await reset_exchange()
        await reply(update, f"✅ MEXC recvWindow saved: {value} ms", reply_markup=MAIN_MENU)
    except Exception as e:
        await reply(update, f"❌ /recv_window error: {e}", reply_markup=MAIN_MENU)

async def openai_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    args = context.args or []
    s = await storage.all_settings()
    cmd = args[0].lower() if args else "status"
    if cmd == "status":
        key_saved = bool(s.get("openai_api_key"))
        env_fb = bool(s.get("openai_env_fallback", True))
        env_key = bool(os.getenv("OPENAI_API_KEY", "").strip())
        await reply(update,
            "🤖 OpenAI AI Analysis\n"
            f"Enabled: {bool(s.get('openai_analysis_enabled', False))}\n"
            f"Model: {s.get('openai_model', 'gpt-5.4-mini')}\n"
            f"Strength: {s.get('openai_check_strength', 'medium')}\n"
            f"Show decisions: {'detailed' if bool(s.get('openai_show_decisions', False)) else 'minimal issues only'}\n"
            f"API key saved: {key_saved}\n"
            f"ENV fallback: {env_fb} ({'present' if env_key else 'missing'})\n\n"
            "Use /openai set OPENAI_API_KEY or the Settings → ИИ анализ menu.",
            reply_markup=MAIN_MENU)
        return
    if cmd == "set" and len(args) >= 2:
        key = " ".join(args[1:]).strip()
        if not key.startswith("sk-") and len(key) < 20:
            await reply(update, "❌ This does not look like an OpenAI API key.", reply_markup=MAIN_MENU)
            return
        await storage.set("openai_api_key", key)
        await reply(update, "✅ OpenAI API key saved", reply_markup=MAIN_MENU)
        return
    if cmd == "clear":
        await storage.set("openai_api_key", "")
        await reply(update, "🗑 OpenAI API key cleared", reply_markup=MAIN_MENU)
        return
    if cmd == "test":
        from openai_signal_engine import openai_key
        key_ok = bool(openai_key(s))
        await reply(update, "✅ OpenAI key available" if key_ok else "❌ OpenAI key missing", reply_markup=MAIN_MENU)
        return
    await reply(update, "Usage: /openai status|set KEY|clear|test", reply_markup=MAIN_MENU)

async def api_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    s = await storage.all_settings()
    if not context.args or context.args[0].lower() in {"status", "show"}:
        api_key, api_secret = _api_creds(s)
        source = "Telegram settings" if s.get("mexc_api_key") and s.get("mexc_api_secret") else "Railway/env fallback" if api_key and api_secret else "not configured"
        await reply(update, (
            "🔐 API status\n"
            f"Exchange: {DEFAULT_EXCHANGE.upper()} futures\n"
            f"Source: {source}\n"
            f"Key: {mask_secret(api_key)}\n"
            f"Secret: {mask_secret(api_secret)}\n\n"
            "Команды:\n"
            "/api set API_KEY API_SECRET — сохранить ключи в боте\n"
            "/api test — проверить подключение к бирже\n"
            "/api clear — удалить ключи из SQLite"
        ), reply_markup=MAIN_MENU)
        return
    cmd = context.args[0].lower()
    if cmd == "set":
        if len(context.args) < 3:
            await reply(update, "Usage: /api set API_KEY API_SECRET", reply_markup=MAIN_MENU)
            return
        await storage.set("mexc_api_key", context.args[1])
        await storage.set("mexc_api_secret", context.args[2])
        await reset_exchange()
        await reply(update, f"✅ API saved\nKey: {mask_secret(context.args[1])}\nSecret: {mask_secret(context.args[2])}\n\nТеперь можно /api test", reply_markup=MAIN_MENU)
        return
    if cmd == "clear":
        await storage.set("mexc_api_key", "")
        await storage.set("mexc_api_secret", "")
        await reset_exchange()
        await reply(update, "🗑 API keys cleared from bot storage", reply_markup=MAIN_MENU)
        return
    if cmd == "test":
        s = await storage.all_settings()
        api_key, api_secret = _api_creds(s)
        if not api_key or not api_secret:
            await reply(update, "❌ API missing. Use /api set API_KEY API_SECRET", reply_markup=MAIN_MENU)
            return
        try:
            ex = await get_exchange(s)
            bal = await ex.fetch_balance()
            usdt = bal.get("USDT", {}) if isinstance(bal, dict) else {}
            free = usdt.get("free", "n/a") if isinstance(usdt, dict) else "n/a"
            total = usdt.get("total", "n/a") if isinstance(usdt, dict) else "n/a"
            await reply(update, f"✅ API test OK\nUSDT free: {free}\nUSDT total: {total}", reply_markup=MAIN_MENU)
        except Exception as e:
            await reply(update, f"❌ API test failed: {e}", reply_markup=MAIN_MENU)
        return
    await reply(update, "Unknown API command. Use /api status|set|clear|test", reply_markup=MAIN_MENU)

async def set_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    if len(context.args) < 2:
        await reply(update, "Usage: /set key value", reply_markup=MAIN_MENU); return
    key, value = context.args[0], " ".join(context.args[1:])
    allowed_keys = {
        "live_trading", "universe_mode", "max_symbols", "scan_interval_sec", "scanner_concurrency", "scanner_error_slowdown_threshold", "scanner_slowdown_max_sec", "symbol_refresh_sec",
        "max_open_positions", "risk_pct", "strategy_mode", "auto_strategy_adaptation",
        "regime_adaptation", "mirror_mode", "spot_confirmation_enabled", "scan_market_source",
        "session_filter_enabled", "america_short_bias_enabled", "max_spread_pct", "max_slippage_pct",
        "min_depth_usdt", "max_daily_loss_pct", "max_consecutive_losses", "cooldown_after_close_sec",
        "limit_timeout_sec", "proxy_enabled", "proxy_url", "mexc_api_key", "mexc_api_secret",
        "ws_enabled", "ws_stale_sec", "ws_update_throttle_ms", "ws_max_updates_per_batch", "ws_queue_limit", "ws_adaptive_slowdown_threshold",
        "mexc_order_leverage", "mexc_order_open_type", "mexc_recv_window",
        "margin_allocation_enabled", "require_exchange_protection", "auto_close_on_protection_failed",
        "breakeven_trigger_pct", "breakeven_offset_pct", "scalp_exit_enabled", "scalp_trailing_enabled",
        "scalp_trailing_start_pct", "scalp_trailing_giveback_pct", "smart_time_stop_min_sec",
        "smart_time_stop_stale_abs_pct", "smart_time_stop_extend_profit_pct", "smart_time_stop_max_extend_sec",
        "liquidity_retest_default_rr", "liquidity_retest_sl_buffer_pct", "liquidity_retest_time_stop_sec", "liquidity_retest_min_displacement_pct", "liquidity_retest_min_displacement_body", "liquidity_retest_min_volume_ratio", "liquidity_retest_min_target_rr", "liquidity_retest_zone_tolerance_pct", "liquidity_retest_min_sweep_wick", "liquidity_retest_min_reclaim_pct", "liquidity_retest_max_spread_pct", "liquidity_retest_min_retest_rejection_wick", "liquidity_retest_min_zone_quality", "liquidity_retest_mtf_enabled", "liquidity_retest_min_mtf_score", "liquidity_retest_require_clean_path",
        "weak_momentum_filter_enabled", "momentum_min_5m_confirm_pct", "momentum_min_imbalance_abs", "momentum_max_spread_pct",
        "openai_analysis_enabled", "openai_model", "openai_check_strength", "openai_api_key",
        "openai_env_fallback", "openai_timeout_sec", "openai_fail_open", "openai_show_decisions",
        "trade_charts_enabled", "liquidity_runner_enabled",
        "ai_scalping_symbols", "ai_scalping_min_confidence", "ai_scalping_tp_pct", "ai_scalping_sl_pct", "ai_scalping_btc_tp_pct", "ai_scalping_btc_sl_pct", "ai_scalping_eth_tp_pct", "ai_scalping_eth_sl_pct", "ai_scalping_max_spread_pct", "ai_scalping_quality_filters_enabled", "ai_scalping_quality_min_confidence", "ai_scalping_quality_cooldown_sec", "ai_scalping_quality_min_atr_pct", "ai_scalping_quality_min_ema_gap_pct", "ai_scalping_quality_min_ret_5m_abs_pct", "ai_scalping_ai_cooldown_sec", "ai_scalping_openai_fallback_enabled", "ai_scalping_json_mode_enabled", "ai_scalping_liquidation_stop_mode", "ai_scalping_liq_margin_pct", "ai_scalping_liq_buffer_pct", "ai_scalping_liq_max_leverage",
    }
    if key not in allowed_keys:
        await reply(update, f"❌ Setting is not allowed through /set: {key}", reply_markup=MAIN_MENU)
        return
    if value.lower() in {"true","false","on","off"}:
        parsed = value.lower() in {"true","on"}
    else:
        try: parsed = float(value) if "." in value else int(value)
        except Exception: parsed = value
    if key == "openai_model" and str(parsed) not in {"gpt-5.4-mini", "gpt-4o-mini", "gpt-5.5", "gpt-5.5-pro", "gpt-4.1"}:
        await reply(update, "❌ Unknown OpenAI model. Use the ИИ анализ menu.", reply_markup=MAIN_MENU)
        return
    if key == "openai_check_strength" and str(parsed).lower() not in {"weak", "medium", "strong"}:
        await reply(update, "❌ OpenAI strength must be weak, medium, or strong.", reply_markup=MAIN_MENU)
        return
    await storage.set(key, parsed)
    # v0104: /set strategy_mode ai_scalping must be a real mode switch, not
    # only a marker. It automatically enables OpenAI and the BTC/ETH scalping
    # defaults, matching the main menu button.
    if key == "strategy_mode" and str(parsed).lower() == "ai_scalping":
        ai_defaults = {
            "openai_analysis_enabled": True,
            "openai_show_decisions": True,
            "ai_scalping_symbols": "BTC_USDT,ETH_USDT",
            "ai_scalping_btc_tp_pct": 0.18,
            "ai_scalping_btc_sl_pct": 0.26,
            "ai_scalping_eth_tp_pct": 0.22,
            "ai_scalping_eth_sl_pct": 0.32,
            "max_open_positions": 2,
            "auto_strategy_adaptation": False,
            "regime_adaptation": False,
            "liquidity_runner_enabled": False,
            "spot_confirmation_enabled": False,
            "session_filter_enabled": False,
        }
        for k2, v2 in ai_defaults.items():
            await storage.set(k2, v2, bump_revision=False)
    if key in {"mexc_api_key", "mexc_api_secret", "proxy_url", "proxy_enabled", "mexc_order_leverage", "mexc_order_open_type", "mexc_recv_window", "margin_allocation_enabled", "require_exchange_protection", "auto_close_on_protection_failed"}:
        new_settings = await storage.all_settings()
        apply_mexc_runtime_env(new_settings)
        await reset_exchange()
    if key in {"proxy_url", "proxy_enabled", "scan_market_source", "ws_enabled", "ws_stale_sec", "ws_update_throttle_ms", "ws_max_updates_per_batch", "ws_queue_limit", "ws_adaptive_slowdown_threshold"}:
        await reset_market_runtime()
    shown = mask_secret(str(parsed)) if key in {"mexc_api_key", "mexc_api_secret", "openai_api_key", "proxy_url"} else parsed
    if key in {"scan_interval_sec", "scanner_concurrency", "strategy_mode", "universe_mode", "max_symbols", "scan_market_source", "spot_confirmation_enabled", "session_filter_enabled", "america_short_bias_enabled", "openai_analysis_enabled", "openai_check_strength", "openai_model", "ai_scalping_symbols", "ai_scalping_min_confidence", "ai_scalping_tp_pct", "ai_scalping_sl_pct", "ai_scalping_btc_tp_pct", "ai_scalping_btc_sl_pct", "ai_scalping_eth_tp_pct", "ai_scalping_eth_sl_pct", "ai_scalping_max_spread_pct", "ai_scalping_quality_filters_enabled", "ai_scalping_quality_min_confidence", "ai_scalping_quality_cooldown_sec", "ai_scalping_quality_min_atr_pct", "ai_scalping_quality_min_ema_gap_pct", "ai_scalping_quality_min_ret_5m_abs_pct"}:
        trigger_scan_now(context.application, reason=f"setting:{key}")
    await reply(update, f"✅ Saved\n{key} = {shown}", reply_markup=MAIN_MENU)

async def proxy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    if not context.args:
        await reply(update, "Usage: /proxy on|off|set URL|test", reply_markup=MAIN_MENU); return
    cmd = context.args[0].lower()
    if cmd == "on":
        await storage.set("proxy_enabled", True)
        await reset_exchange()
        await reset_market_runtime()
        await reply(update, "🌐 Proxy enabled\nExchange/WebSocket will reconnect with proxy.", reply_markup=MAIN_MENU)
    elif cmd == "off":
        await storage.set("proxy_enabled", False)
        await reset_exchange()
        await reset_market_runtime()
        await reply(update, "🌐 Proxy disabled\nExchange/WebSocket will reconnect directly.", reply_markup=MAIN_MENU)
    elif cmd == "set" and len(context.args) >= 2:
        await storage.set("proxy_url", context.args[1])
        await reset_exchange()
        await reset_market_runtime()
        await reply(update, "🌐 Proxy URL saved\nUse /proxy on, then /proxy test.", reply_markup=MAIN_MENU)
    elif cmd == "test":
        s = await storage.all_settings()
        proxy_enabled = bool(s.get("proxy_enabled", False))
        proxy_url = str(s.get("proxy_url", "") or "")
        # fetch_public_ip uses aiohttp.ClientSession internally, reads PROXY_TEST_URL, and supports HTTP/SOCKS proxy paths.
        direct_ip = await fetch_public_ip(use_proxy=False)
        proxy_ip = await fetch_public_ip(use_proxy=True, proxy_url=proxy_url) if proxy_enabled and proxy_url else {"ok": False, "ip": "not configured", "error": "proxy off or missing"}
        text = (
            "🌐 Proxy/IP test\n"
            f"Direct IP: {direct_ip.get('ip')}" + (f" ({direct_ip.get('error')})" if direct_ip.get('error') else "") + "\n"
            f"Proxy enabled: {proxy_enabled}\n"
            f"Proxy IP: {proxy_ip.get('ip')}" + (f" ({proxy_ip.get('error')})" if proxy_ip.get('error') else "") + "\n"
            f"Proxy OK: {bool(proxy_ip.get('ok'))}"
        )
        await reply(update, text, reply_markup=MAIN_MENU)
    else:
        await reply(update, "Unknown proxy command", reply_markup=MAIN_MENU)


async def ai_scalping_toggle_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    s = await storage.all_settings()
    enabled = str(s.get("strategy_mode", "hybrid")).lower() == "ai_scalping"
    if enabled:
        updates = {
            "strategy_mode": "hybrid",
            "auto_strategy_adaptation": True,
            "regime_adaptation": True,
            "spot_confirmation_enabled": True,
            "session_filter_enabled": True,
            "openai_analysis_enabled": False,
        }
        for k, v in updates.items():
            await storage.set(k, v, bump_revision=False)
        await storage.set("settings_revision", int(s.get("settings_revision", 1) or 1) + 1, bump_revision=False)
        trigger_scan_now(context.application, reason="ai_scalping:off")
        await reply(update, "○ AI BTC/ETH scalping OFF\nРежим возвращён на hybrid adaptive.", reply_markup=MAIN_MENU)
        return

    updates = {
        "strategy_mode": "ai_scalping",
        "ai_scalping_symbols": "BTC_USDT,ETH_USDT",
        "ai_scalping_btc_tp_pct": 0.18,
        "ai_scalping_btc_sl_pct": 0.26,
        "ai_scalping_eth_tp_pct": 0.22,
        "ai_scalping_eth_sl_pct": 0.32,
        "max_open_positions": 2,
        "scan_interval_sec": 60,
        "auto_strategy_adaptation": False,
        "regime_adaptation": False,
        "liquidity_runner_enabled": False,
        "mirror_mode": "off",
        "spot_confirmation_enabled": False,
        "session_filter_enabled": False,
        "america_short_bias_enabled": False,
        "openai_analysis_enabled": True,
        "openai_show_decisions": True,
    }
    for k, v in updates.items():
        await storage.set(k, v, bump_revision=False)
    await storage.set("settings_revision", int(s.get("settings_revision", 1) or 1) + 1, bump_revision=False)
    scanner.last_refresh = 0
    trigger_scan_now(context.application, reason="ai_scalping:on")
    await reply(
        update,
        "✅ AI BTC/ETH scalping ON\n"
        "Включено: только BTC/ETH, независимый AI-запрос по каждому символу после закрытия именно его позиции.\n"
        "Отключено: scanner strategies, spot/session filters, mirror, regime/adaptive strategy.",
        reply_markup=MAIN_MENU,
    )

async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    text = update.message.text
    mapping = {
        "▶️ Run": run_cmd, "⏹ Stop": stop_cmd, "📊 Status": status_cmd, "🚨 Panic": panic_cmd,
        "📈 Positions": positions_cmd, "📉 Stats": stats_cmd, "💰 Balance": balance_cmd,
        "🏓 Ping": ping_cmd, "⚙️ Settings": settings_cmd, "🔐 API": api_cmd, "📊 AI Stats": ai_stats_cmd, "🤖 AI BTC/ETH scalping": ai_scalping_toggle_cmd, "⚙️ MEXC": mexc_settings_cmd,
    }
    fn = mapping.get(text)
    if fn: await fn(update, context)
    else: await reply(update, "Неизвестная команда. Нажми /help.", reply_markup=MAIN_MENU)

async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not allowed(update):
        await q.answer("Access denied", show_alert=True)
        return
    await q.answer()
    data = q.data.split(":")
    s = await storage.all_settings()
    current_rev = int(s.get("settings_revision", 1))
    try:
        rev = int(data[-1])
    except Exception:
        rev = current_rev
    if rev != current_rev and data[0] != "menu":
        await q.edit_message_text("⚠️ Старое меню. Открой Settings заново.")
        return

    if data[0] == "toggle":
        key = data[1]
        new_value = not bool(s.get(key, False))
        await storage.set(key, new_value)
        if key in {"ws_enabled", "proxy_enabled", "scan_market_source"}:
            await reset_market_runtime()
        new_settings = await storage.all_settings()
        new_rev = int(new_settings.get("settings_revision", current_rev + 1))
        if key in {"live_trading", "spot_confirmation_enabled", "session_filter_enabled", "america_short_bias_enabled", "openai_analysis_enabled", "ws_enabled", "ai_scalping_quality_filters_enabled", "ai_scalping_openai_fallback_enabled", "ai_scalping_json_mode_enabled", "ai_scalping_liquidation_stop_mode"}:
            trigger_scan_now(context.application, reason=f"toggle:{key}")
        await q.edit_message_text(f"✅ {key} = {new_value}\n\n⚙️ Settings", reply_markup=settings_menu(new_rev, new_settings))
    elif data[0] == "set":
        key, value = data[1], data[2]
        parsed = value
        try:
            parsed = float(value) if "." in value else int(value)
        except ValueError:
            parsed = value
        await storage.set(key, parsed)
        if key in {"scan_market_source", "ws_enabled", "proxy_url", "proxy_enabled", "ws_stale_sec", "ws_update_throttle_ms", "ws_max_updates_per_batch", "ws_queue_limit", "ws_adaptive_slowdown_threshold"}:
            await reset_market_runtime()
        if key in {"universe_mode", "max_symbols", "scan_market_source"}:
            scanner.last_refresh = 0
            scanner.last_reject_reason = "universe settings changed; refresh queued"
        new_settings = await storage.all_settings()
        new_rev = int(new_settings.get("settings_revision", current_rev + 1))
        if key in {"scan_interval_sec", "scanner_concurrency", "strategy_mode", "universe_mode", "max_symbols", "scan_market_source", "symbol_refresh_sec", "openai_model", "openai_check_strength"}:
            trigger_scan_now(context.application, reason=f"menu:{key}")
        # Stay inside the same submenu so the selected value is immediately visible with ✅.
        if key == "universe_mode":
            await q.edit_message_text("🌐 Universe", reply_markup=choices_menu("universe_mode", [("Top-50","top-50"),("Top-100","top-100"),("Top-200","top-200"),("Top-300","top-300"),("Adaptive","adaptive")], new_rev, new_settings.get("universe_mode")))
        elif key == "strategy_mode":
            await q.edit_message_text("📈 Strategy", reply_markup=choices_menu("strategy_mode", [("Momentum","momentum"),("Pullback","pullback"),("Reversal","reversal"),("Liquidity Retest","liquidity_retest"),("AI BTC/ETH scalp","ai_scalping"),("Hybrid adaptive","hybrid"),("All strategies","all")], new_rev, new_settings.get("strategy_mode")))
        elif key == "scan_market_source":
            await q.edit_message_text("📡 Фьючи | Спот", reply_markup=choices_menu("scan_market_source", [("Binance фьючи + Binance спот","binance_binance"),("MEXC фьючи + MEXC спот","mexc_mexc"),("MEXC фьючи + Binance спот","mexc_binance")], new_rev, new_settings.get("scan_market_source", "mexc_binance")))
        elif key == "scan_interval_sec":
            await q.edit_message_text("⏱ Scan speed", reply_markup=choices_menu("scan_interval_sec", [("3s","3"),("5s default","5"),("10s","10"),("30s","30"),("1m","60"),("5m","300"),("15m","900"),("30m","1800"),("1h","3600"),("4h","14400")], new_rev, new_settings.get("scan_interval_sec")))
        elif key == "scanner_concurrency":
            await q.edit_message_text("🧵 Scanner concurrency", reply_markup=choices_menu("scanner_concurrency", [("3 requests","3"),("5 requests","5"),("8 requests","8"),("12 requests","12")], new_rev, new_settings.get("scanner_concurrency", 5)))
        elif key == "ws_update_throttle_ms":
            await q.edit_message_text("🌊 WS throttle", reply_markup=choices_menu("ws_update_throttle_ms", [("250ms","250"),("500ms","500"),("1000ms","1000"),("1500ms","1500")], new_rev, new_settings.get("ws_update_throttle_ms", 500)))
        elif key == "symbol_refresh_sec":
            await q.edit_message_text("🔄 Refresh", reply_markup=choices_menu("symbol_refresh_sec", [("60s","60"),("180s","180"),("300s","300"),("600s","600"),("1200s","1200")], new_rev, new_settings.get("symbol_refresh_sec")))
        elif key == "risk_pct":
            await q.edit_message_text("📊 Risk", reply_markup=choices_menu("risk_pct", [("0.25%","0.0025"),("0.50%","0.005"),("1%","0.01"),("3%","0.03"),("5%","0.05")], new_rev, new_settings.get("risk_pct")))
        elif key == "max_open_positions":
            await q.edit_message_text("🔥 Max positions", reply_markup=choices_menu("max_open_positions", [("1","1"),("2","2"),("3","3"),("5","5"),("10","10"),("15","15"),("20","20")], new_rev, new_settings.get("max_open_positions")))
        elif key == "mirror_mode":
            await q.edit_message_text("🪞 Mirror", reply_markup=choices_menu("mirror_mode", [("OFF","off"),("ON","on"),("AUTO","auto")], new_rev, new_settings.get("mirror_mode")))
        elif key == "openai_model":
            await q.edit_message_text("🧠 OpenAI model", reply_markup=choices_menu("openai_model", [("gpt-5.4-mini default","gpt-5.4-mini"),("gpt-4o-mini","gpt-4o-mini"),("gpt-5.5","gpt-5.5"),("gpt-5.5-pro","gpt-5.5-pro"),("gpt-4.1","gpt-4.1")], new_rev, new_settings.get("openai_model", "gpt-5.4-mini")))
        elif key == "openai_check_strength":
            await q.edit_message_text("🛡 OpenAI check strength", reply_markup=choices_menu("openai_check_strength", [("Weak","weak"),("Medium default","medium"),("Strong","strong")], new_rev, new_settings.get("openai_check_strength", "medium")))
        else:
            await q.edit_message_text(f"✅ {key} = {parsed}\n\n⚙️ Settings", reply_markup=settings_menu(new_rev, new_settings))
    elif data[0] == "api":
        action = data[1] if len(data) > 1 else "status"
        if action == "clear":
            await storage.set("mexc_api_key", "")
            await storage.set("mexc_api_secret", "")
            await reset_exchange()
            new_settings = await storage.all_settings()
            new_rev = int(new_settings.get("settings_revision", current_rev + 1))
            await q.edit_message_text("🗑 API keys cleared from bot storage", reply_markup=api_menu(new_rev, new_settings))
        elif action == "test":
            api_key, api_secret = _api_creds(s)
            if not api_key or not api_secret:
                await q.edit_message_text("❌ API missing. Use /api set API_KEY API_SECRET", reply_markup=api_menu(current_rev, s))
            else:
                try:
                    ex = await get_exchange(s)
                    bal = await ex.fetch_balance()
                    usdt = bal.get("USDT", {}) if isinstance(bal, dict) else {}
                    free = usdt.get("free", "n/a") if isinstance(usdt, dict) else "n/a"
                    total = usdt.get("total", "n/a") if isinstance(usdt, dict) else "n/a"
                    await q.edit_message_text(f"✅ API test OK\nUSDT free: {free}\nUSDT total: {total}", reply_markup=api_menu(current_rev, s))
                except Exception as e:
                    await q.edit_message_text(f"❌ API test failed: {e}", reply_markup=api_menu(current_rev, s))
        else:
            await q.edit_message_text("🔐 API menu\nUse /api set API_KEY API_SECRET to save keys.", reply_markup=api_menu(current_rev, s))
    elif data[0] == "aistats":
        action = data[1] if len(data) > 1 else "current"
        if action == "reset":
            sid = await ai_stats_manager.reset_session()
            new_settings = await storage.all_settings()
            new_rev = int(new_settings.get("settings_revision", current_rev + 1))
            await q.edit_message_text(f"♻ AI scalping session reset\nNew session ID: {sid}", reply_markup=ai_stats_menu(new_rev))
        elif action == "lifetime":
            await q.edit_message_text(AIStatsManager.format(await ai_stats_manager.summary("lifetime")), reply_markup=ai_stats_menu(current_rev))
        else:
            await q.edit_message_text(AIStatsManager.format(await ai_stats_manager.summary("current")), reply_markup=ai_stats_menu(current_rev))
    elif data[0] == "openai":
        action = data[1] if len(data) > 1 else "status"
        if action == "clear":
            await storage.set("openai_api_key", "")
            new_settings = await storage.all_settings()
            new_rev = int(new_settings.get("settings_revision", current_rev + 1))
            await q.edit_message_text("🗑 OpenAI API key cleared", reply_markup=openai_menu(new_rev, new_settings))
        else:
            await q.edit_message_text("🔑 OpenAI API key\nUse: /openai set YOUR_OPENAI_API_KEY", reply_markup=openai_menu(current_rev, s))
    elif data[0] == "noop":
        await q.answer("Use /api set API_KEY API_SECRET", show_alert=True)
    elif data[0] == "menu":
        name = data[1]
        rev = current_rev
        if name == "settings":
            await q.edit_message_text("⚙️ Settings", reply_markup=settings_menu(rev, s))
        elif name == "universe":
            await q.edit_message_text("🌐 Universe", reply_markup=choices_menu("universe_mode", [("Top-50","top-50"),("Top-100","top-100"),("Top-200","top-200"),("Top-300","top-300"),("Adaptive","adaptive")], rev, s.get("universe_mode")))
        elif name == "strategy":
            await q.edit_message_text("📈 Strategy", reply_markup=choices_menu("strategy_mode", [("Momentum","momentum"),("Pullback","pullback"),("Reversal","reversal"),("Liquidity Retest","liquidity_retest"),("AI BTC/ETH scalp","ai_scalping"),("Hybrid adaptive","hybrid"),("All strategies","all")], rev, s.get("strategy_mode")))
        elif name == "marketsource":
            await q.edit_message_text("📡 Фьючи | Спот", reply_markup=choices_menu("scan_market_source", [("Binance фьючи + Binance спот","binance_binance"),("MEXC фьючи + MEXC спот","mexc_mexc"),("MEXC фьючи + Binance спот","mexc_binance")], rev, s.get("scan_market_source", "mexc_binance")))
        elif name == "scan":
            await q.edit_message_text("⏱ Scan speed", reply_markup=choices_menu("scan_interval_sec", [("3s","3"),("5s default","5"),("10s","10"),("30s","30"),("1m","60"),("5m","300"),("15m","900"),("30m","1800"),("1h","3600"),("4h","14400")], rev, s.get("scan_interval_sec")))
        elif name == "concurrency":
            await q.edit_message_text("🧵 Scanner concurrency", reply_markup=choices_menu("scanner_concurrency", [("3 requests","3"),("5 requests","5"),("8 requests","8"),("12 requests","12")], rev, s.get("scanner_concurrency", 5)))
        elif name == "wsthrottle":
            await q.edit_message_text("🌊 WS throttle", reply_markup=choices_menu("ws_update_throttle_ms", [("250ms","250"),("500ms","500"),("1000ms","1000"),("1500ms","1500")], rev, s.get("ws_update_throttle_ms", 500)))
        elif name == "refresh":
            await q.edit_message_text("🔄 Refresh", reply_markup=choices_menu("symbol_refresh_sec", [("60s","60"),("180s","180"),("300s","300"),("600s","600"),("1200s","1200")], rev, s.get("symbol_refresh_sec")))
        elif name == "risk":
            await q.edit_message_text("📊 Risk", reply_markup=choices_menu("risk_pct", [("0.25%","0.0025"),("0.50%","0.005"),("1%","0.01"),("3%","0.03"),("5%","0.05")], rev, s.get("risk_pct")))
        elif name == "maxpos":
            await q.edit_message_text("🔥 Max positions", reply_markup=choices_menu("max_open_positions", [("1","1"),("2","2"),("3","3"),("5","5"),("10","10"),("15","15"),("20","20")], rev, s.get("max_open_positions")))
        elif name == "mirror":
            await q.edit_message_text("🪞 Mirror", reply_markup=choices_menu("mirror_mode", [("OFF","off"),("ON","on"),("AUTO","auto")], rev, s.get("mirror_mode")))
        elif name == "openai":
            await q.edit_message_text("🤖 ИИ анализ OpenAI", reply_markup=openai_menu(rev, s))
        elif name == "openai_model":
            await q.edit_message_text("🧠 OpenAI model", reply_markup=choices_menu("openai_model", [("gpt-5.4-mini default","gpt-5.4-mini"),("gpt-4o-mini","gpt-4o-mini"),("gpt-5.5","gpt-5.5"),("gpt-5.5-pro","gpt-5.5-pro"),("gpt-4.1","gpt-4.1")], rev, s.get("openai_model", "gpt-5.4-mini")))
        elif name == "openai_strength":
            await q.edit_message_text("🛡 OpenAI check strength", reply_markup=choices_menu("openai_check_strength", [("Weak","weak"),("Medium default","medium"),("Strong","strong")], rev, s.get("openai_check_strength", "medium")))
        elif name == "api":
            await q.edit_message_text("🔐 API menu\nUse /api set API_KEY API_SECRET to save keys.", reply_markup=api_menu(rev, s))


async def get_last_price(ex, symbol: str) -> float:
    settings = await storage.all_settings()
    ws = await get_ws(settings)
    cached = None
    if bool(settings.get("ws_enabled", True)) and ws and ws.healthy():
        cached = await ws.ticker(symbol, max_age_sec=float(settings.get("ws_stale_sec", 10)))
    if cached and cached.get("last"):
        return float(cached["last"])
    ticker = await ex.fetch_ticker(symbol)
    return float(ticker.get("last") or ticker.get("close") or ticker.get("bid") or ticker.get("ask") or 0)

async def fetch_spot_data_for_candidate(ex, candidate: dict, settings: dict | None = None) -> dict | None:
    symbol = candidate.get("symbol")
    settings = settings or {}
    mode = str(settings.get("scan_market_source", "mexc_binance") or "mexc_binance").lower()
    spot_source = "binance" if mode.endswith("binance") else "mexc"
    spot_symbol = str(symbol or "").split(":", 1)[0]
    if not spot_symbol:
        return None

    client = None
    try:
        import ccxt.async_support as ccxt
        proxy_enabled = bool(settings.get("proxy_enabled", False))
        proxy_url = str(settings.get("proxy_url", "") or "")
        cfg = {"enableRateLimit": True}
        if proxy_enabled and proxy_url:
            cfg["proxies"] = {"http": proxy_url, "https": proxy_url}
            cfg["aiohttp_proxy"] = proxy_url
        if spot_source == "binance":
            client = ccxt.binance(cfg)
        else:
            cfg["options"] = {"defaultType": "spot"}
            client = ccxt.mexc(cfg)
        await client.load_markets()
        candles = await client.fetch_ohlcv(spot_symbol, timeframe="1m", limit=25)
        ticker = await client.fetch_ticker(spot_symbol)

        if not candles or len(candles) < 5:
            return None
        vols = [float(c[5]) for c in candles]
        closes = [float(c[4]) for c in candles]
        avg = sum(vols[:-1]) / max(1, len(vols[:-1]))
        move = (closes[-1] - closes[-2]) / closes[-2] * 100 if closes[-2] else 0
        return {
            "spot_source": spot_source,
            "spot_price": float(ticker.get("last") or closes[-1]),
            "spot_volume_now": vols[-1],
            "spot_volume_avg": avg,
            "spot_price_change_pct": move,
        }
    except Exception as e:
        log.debug("%s spot confirmation data failed for %s: %s", spot_source, symbol, e)
        return None
    finally:
        if client:
            try:
                await client.close()
            except Exception:
                pass

async def ensure_recovery_before_entries(app, settings: dict, ex, exec_engine, live: bool) -> tuple[bool, str]:
    """Run one recovery/sync check before allowing entries after /run.

    Live: reconcile MEXC positions and reattach protection once per /run.
    Paper: record a no-op report so tests/status can confirm the pre-entry
    recovery checkpoint was reached without requiring exchange state.
    """
    if app.bot_data.get("recovery_checked_for_run"):
        return True, str(app.bot_data.get("last_recovery_status") or "already checked")
    if not live:
        report = {"mode": "paper", "status": "ok", "note": "no exchange recovery required"}
        app.bot_data["last_recovery_report"] = report
        app.bot_data["last_recovery_status"] = "paper recovery checkpoint ok"
        app.bot_data["recovery_checked_for_run"] = True
        return True, app.bot_data["last_recovery_status"]
    api_key, api_secret = _api_creds(settings)
    if not (api_key and api_secret):
        return False, "live recovery blocked: missing MEXC API keys"
    try:
        report = await RecoveryEngine(storage, ex, exec_engine).recover(reattach=True)
        app.bot_data["last_recovery_report"] = report
        app.bot_data["last_recovery_status"] = (
            f"live recovery ok: restored={report.get('restored', 0)} "
            f"updated={report.get('updated', 0)} warnings={len(report.get('warnings', []) or [])}"
        )
        app.bot_data["recovery_checked_for_run"] = True
        return True, app.bot_data["last_recovery_status"]
    except Exception as e:
        app.bot_data["last_recovery_error"] = str(e)[:300]
        return False, f"live recovery failed: {e}"

async def account_equity_usdt(ex, default: float = 1000.0) -> float:
    try:
        bal = await ex.fetch_balance()
        usdt = bal.get("USDT", {}) if isinstance(bal, dict) else {}
        total = usdt.get("total") if isinstance(usdt, dict) else None
        free = usdt.get("free") if isinstance(usdt, dict) else None
        return float(total or free or default)
    except Exception as e:
        log.debug("balance fetch failed, using default equity: %s", e)
        return float(default)

async def trading_loop(app):
    global running, entries_enabled, trading_task
    try:
        while running:
            try:
                settings = await storage.all_settings()
                ai_mode = str(settings.get("strategy_mode", "hybrid")).lower() == "ai_scalping"
                live = bool(settings.get("live_trading", False))
                ex = await get_exchange(settings)
                ws = await get_ws(settings)

                if bool(settings.get("ws_enabled", True)) and not ws.status.running:
                    await ws.start()

                exec_engine = ExecutionEngine(storage, ex)
                pos_manager = PositionManager(storage, exec_engine)

                # 1) Position management ALWAYS runs first and is never blocked by entry gates.
                events = await pos_manager.manage(lambda symbol: get_last_price(ex, symbol), live)
                for ev in events:
                    if ev.get("type") not in {"pending_sync_warning", "price_error"}:
                        await notify_admin(app, format_position_event(ev), key="position_event")

                # 2) Refresh symbol universe for legacy scanner modes only.
                # v0114: AI BTC/ETH scalping must not run the adaptive universe
                # scanner at all. It uses direct BTC_USDT/ETH_USDT market data and
                # should not emit websocket empty-cache errors from the legacy scanner.
                if not ai_mode and time.time() - scanner.last_refresh > int(settings.get("symbol_refresh_sec", 300)):
                    await update_scanner_status(app, settings, status="refreshing universe", force=True)
                    await scanner.refresh_symbols(ex, settings, ws_supervisor=ws)
                    if scanner.last_refresh_error:
                        scanner.last_reject_reason = f"universe refresh failed; using cached symbols={len(scanner.hot_symbols)}"
                        # Important exchange/source errors are still sent separately, but rate-limited.
                        await notify_admin(
                            app,
                            f"⚠️ Ошибка скана фьючерсов: {scanner.last_refresh_error}\nИспользуется прошлый список монет: {len(scanner.hot_symbols)}",
                            min_interval_sec=300,
                            key="scan_refresh_error",
                        )
                    else:
                        scanner.last_reject_reason = "universe refreshed"
                    await update_scanner_status(app, settings, status="universe ready", force=True)
                elif ai_mode:
                    # Clear stale legacy scanner state so status is not displayed as
                    # ai_scalping -> reversal/adaptive loaded 110.
                    scanner.last_effective_strategy = "ai_scalping"
                    scanner.last_refresh_error = ""
                    scanner.last_cycle_scanned = 2
                    scanner.last_cycle_errors = 0

                # 3) Manual new-entry gate. /stop pauses entries while keeping
                # position management alive; /panic is the full loop stop.
                if not entries_enabled:
                    scanner.last_reject_reason = "new entries paused by /stop; managing open positions"
                    await update_scanner_status(app, settings, status="entries paused", force=True)
                    await sleep_until_next_scan(app, int(settings.get("scan_interval_sec", 5)))
                    continue

                # 3b) One recovery checkpoint before NEW entries after each /run.
                recovery_ok, recovery_reason = await ensure_recovery_before_entries(app, settings, ex, exec_engine, live)
                if not recovery_ok:
                    scanner.last_reject_reason = recovery_reason
                    await update_scanner_status(app, settings, status="recovery blocked", force=True)
                    await notify_admin(app, f"🛟 Recovery blocked new entries: {recovery_reason}", min_interval_sec=300, key="recovery_blocked")
                    await sleep_until_next_scan(app, int(settings.get("scan_interval_sec", 5)))
                    continue

                # 4) Risk gate for NEW entries only. Use real account equity where available.
                risk = RiskEngine(storage)
                equity = await account_equity_usdt(ex, float(os.getenv("DEFAULT_EQUITY_USDT", "1000")))
                ok, reason = await risk.allow_new_trades(settings, equity=equity)
                if not ok:
                    scanner.last_reject_reason = f"risk blocked: {reason}"
                    await update_scanner_status(app, settings, status="risk paused", force=True)
                    await notify_admin(app, f"🛑 Новые входы на паузе: {reason}", min_interval_sec=300, key="risk_paused")
                    await sleep_until_next_scan(app, int(settings.get("scan_interval_sec", 5)))
                    continue

                # 5) Infrastructure gate for NEW entries only.
                ws_enabled = bool(settings.get("ws_enabled", True))
                # WS is an acceleration source, not a hard entry dependency.
                # If WS is temporarily stale but scanner market data is fresh or REST fallback
                # has been used recently, entries must not be blocked by WS health alone.
                market_data_ok = True if ai_mode else scanner_market_data_fresh(max_age_sec=max(900, int(settings.get("symbol_refresh_sec", 300)) * 3))
                if not market_data_ok:
                    scanner.last_reject_reason = (
                        f"market data blocked: stale/weak scanner data "
                        f"source={scanner.last_scan_source} symbols={len(scanner.hot_symbols)} "
                        f"scanned={scanner.last_cycle_scanned} errors={scanner.last_cycle_errors}"
                    )
                    await update_scanner_status(app, settings, status="market data blocked", force=True)
                    await notify_admin(
                        app,
                        "⚠️ Новые входы заблокированы: market data stale/weak. "
                        "Открытые позиции продолжают сопровождаться.",
                        min_interval_sec=300,
                        key="market_data_blocked",
                    )
                    await sleep_until_next_scan(app, int(settings.get("scan_interval_sec", 5)))
                    continue
                # WS health is advisory only. Even if the websocket reconnects, the
                # scanner must keep cycling and entries may proceed when REST/cache
                # scanner data is fresh. Never pass a false WS gate downstream.
                ws_healthy = True
                api_key, api_secret = _api_creds(settings)
                api_ready = bool(api_key and api_secret) if live else True
                sync_ok = True
                if live:
                    try:
                        await ex.fetch_balance()
                    except Exception as e:
                        log.warning("live balance/API probe failed: %s", e)
                        sync_ok = False

                if live:
                    gate_ok, gate_reason = ProductionGate().validate_for_live(settings, api_ready=api_ready, ws_healthy=ws_healthy, sync_ok=sync_ok)
                else:
                    gate_ok, gate_reason = ProductionGate().validate_for_paper(settings, ws_healthy=ws_healthy)
                if not gate_ok:
                    scanner.last_reject_reason = f"gate blocked: {gate_reason}"
                    await update_scanner_status(app, settings, status="entries blocked", force=True)
                    if "websocket" not in str(gate_reason).lower():
                        await notify_admin(app, f"⚠️ Входы заблокированы: {gate_reason}", min_interval_sec=300, key="gate_blocked")
                    await sleep_until_next_scan(app, int(settings.get("scan_interval_sec", 5)))
                    continue

                # 6) AI BTC/ETH dual scalping loop. In this mode the bot does not run the
                # legacy signal scanner. BTC and ETH are independent: if BTC is open,
                # ETH can still ask AI and trade; if ETH is open, BTC can still trade.
                # A new AI request is made only for a symbol with no active local/exchange position.
                if ai_mode:
                    symbols = ai_scalping_engine.symbols(settings)

                    def _base(sym: str) -> str:
                        x = str(sym or "").upper().replace("_", "/")
                        if x.startswith("BTC"):
                            return "BTC"
                        if x.startswith("ETH"):
                            return "ETH"
                        return x.split("/")[0].split("_")[0].split(":")[0]

                    active_bases = set()
                    try:
                        for p in await storage.positions():
                            if str(p.get("status", "open")).lower() in {"open", "pending"}:
                                active_bases.add(_base(p.get("symbol")))
                    except Exception as e:
                        scanner.last_reject_reason = f"AI scalp local position check warning: {e}"
                    exchange_active_count = 0
                    try:
                        raw_pos = await ex.fetch_positions(symbols)
                        for p in (raw_pos or []):
                            if exec_engine.exchange_position_qty(p) > 0:
                                active_bases.add(_base(p.get("symbol") or (p.get("info") or {}).get("symbol")))
                                exchange_active_count += 1
                    except Exception as e:
                        scanner.last_reject_reason = f"AI scalp exchange position check warning: {e}"

                    opened = []
                    waited = []
                    errors = []
                    for symbol in symbols:
                        b = _base(symbol)
                        if b in active_bases:
                            waited.append(f"{b}:active")
                            continue

                        if bool(settings.get("ai_scalping_quality_filters_enabled", False)):
                            try:
                                cd = int(float(settings.get("ai_scalping_quality_cooldown_sec", 45) or 45))
                                if cd > 0:
                                    now_ts = time.time()
                                    recent = [t for t in await storage.trade_rows(since=now_ts - cd) if str(t.get("strategy", "")).lower() == "ai_scalping" and _base(t.get("symbol")) == b]
                                    if recent:
                                        left = max(1, int(cd - (now_ts - max(float(t.get("ts_close") or now_ts) for t in recent))))
                                        waited.append(f"{b}:cooldown {left}s")
                                        continue
                            except Exception as e:
                                waited.append(f"{b}:cooldown check warning {e}")

                        decision = await ai_scalping_engine.decide_symbol(ex, settings, symbol)
                        if not decision.ok:
                            errors.append(f"{b}:{decision.error or 'AI unavailable'}")
                            continue
                        reason_short = str(decision.reason or "no edge").strip()
                        if len(reason_short) > 90:
                            reason_short = reason_short[:87] + "..."
                        if decision.decision == "WAIT":
                            waited.append(f"{b}:WAIT {decision.confidence:.2f} — {reason_short}")
                            # v0115: cached WAIT avoids repeat OpenAI calls. Do not inflate WAIT stats on cached loops.
                            if not getattr(decision, "cached", False):
                                try:
                                    await ai_stats_manager.record_wait(symbol, decision.reason or "no edge", decision.confidence, decision.model)
                                except Exception:
                                    pass
                            continue
                        cand = ai_scalping_engine.make_candidate(decision, settings)
                        if not cand:
                            try:
                                why = ai_scalping_engine.candidate_reject_reason(decision, settings)
                            except Exception as e:
                                why = f"local reject reason error: {e}"
                            waited.append(f"{b}:reject {decision.decision}: {why}")
                            continue
                        plan = TradePlanner().make_plan(cand, settings, equity_usdt=equity)
                        if plan:
                            try:
                                plan.session = f"ai_scalping_session_{int(await storage.get('ai_scalping_session_id', 1) or 1)}"
                            except Exception:
                                plan.session = "ai_scalping"
                        if not plan:
                            waited.append(f"{b}:planner reject {decision.decision}")
                            continue
                        try:
                            placed = await exec_engine.place_entry(plan, live)
                        except Exception as e:
                            errors.append(f"{b}:execution exception {e}")
                            continue
                        if placed.get("ok"):
                            active_bases.add(b)
                            opened.append(f"{plan.symbol} {plan.side} conf={decision.confidence:.2f}")
                            await notify_admin(
                                app,
                                (
                                    f"🤖 AI scalp opened\n"
                                    f"{plan.symbol} {plan.side}\n"
                                    f"Model: {decision.model}\n"
                                    f"Conf: {decision.confidence:.2f}\n"
                                    f"Reason: {decision.reason or '-'}\n"
                                    f"TP: {plan.take_price:.6g}\n"
                                    f"SL: {plan.stop_price:.6g}"
                                ),
                                key=f"ai_scalp_opened_{b}",
                            )
                            await send_trade_chart(app, ex, plan, settings)
                        else:
                            waited.append(f"{b}:execution rejected {placed.get('reason', 'unknown')}")

                    if opened:
                        scanner.last_signal_summary = "AI scalp opened: " + "; ".join(opened)
                        scanner.last_reject_reason = "Dual loop: next AI request per symbol after that symbol closes"
                        await update_scanner_status(app, settings, status="ai scalp opened", force=True)
                    else:
                        scanner.last_signal_summary = f"AI scalp dual: active={sorted(active_bases)} exchange={exchange_active_count}"
                        msg = "; ".join(waited + errors) or "no action"
                        scanner.last_reject_reason = msg[:500]
                        await update_scanner_status(app, settings, status="ai scalp wait", force=bool(errors))
                        if errors:
                            await notify_admin(app, f"⚠️ AI scalping skipped: {scanner.last_reject_reason}", min_interval_sec=120, key="ai_scalp_error")
                    await sleep_until_next_scan(app, int(settings.get("scan_interval_sec", 5)))
                    continue

                # 6) Candidate pipeline: detect market regime -> choose effective strategy ->
                # scan futures signals -> mirror -> session -> spot -> filters -> plan -> execute.
                trades = await storage.trade_rows()
                adaptive = AdaptiveEngine()
                adaptive_stats = adaptive.calc_stats(trades)
                regime_info = await scanner.detect_regime(ex, settings)
                base_strategy_mode = str(settings.get("strategy_mode", "hybrid")).lower()
                effective_strategy = adaptive.choose_strategy(
                    base_mode=base_strategy_mode,
                    trades=trades,
                    regime=str(regime_info.get("regime", "LOW_VOLATILITY")),
                    enabled=bool(settings.get("auto_strategy_adaptation", True)),
                )
                scanner.last_effective_strategy = effective_strategy
                if base_strategy_mode == "all":
                    scanner.last_strategy_reason = "mode=ALL: scanning momentum+pullback+reversal (liquidity_retest is manual-only)"
                elif base_strategy_mode == "hybrid":
                    scanner.last_strategy_reason = f"mode=HYBRID, regime={regime_info.get('regime', 'LOW_VOLATILITY')}"
                else:
                    scanner.last_strategy_reason = f"manual mode={base_strategy_mode}"
                effective_settings = dict(settings)
                effective_settings["market_regime"] = regime_info.get("regime", "LOW_VOLATILITY")
                effective_settings["market_regime_info"] = regime_info
                effective_settings["effective_strategy_mode"] = effective_strategy
                candidates = await scanner.candidates(ex, effective_settings)
                if scanner.last_slowdown_sec:
                    scanner.last_reject_reason = f"scanner adaptive slowdown {scanner.last_slowdown_sec}s after {scanner.last_cycle_errors} errors"
                    await update_scanner_status(app, settings, status="scanner slowdown", force=True)
                    await asyncio.sleep(scanner.last_slowdown_sec)
                if candidates:
                    top = candidates[0]
                    scanner.last_signal_summary = (
                        f"futures candidate pending spot: {top.get('symbol')} {top.get('side')} "
                        f"conf={top.get('confidence')} strategy={top.get('strategy', effective_strategy)} "
                        f"mode={effective_strategy} count={len(candidates)}"
                    )
                else:
                    scanner.last_signal_summary = "none"
                    scanner.last_reject_reason = "no candidates passed signal engine"
                    await update_scanner_status(app, settings, status="scanning")

                opened_this_cycle = False
                for cand in candidates:
                    original_symbol = cand.get("symbol")
                    cand = MirrorEngine(str(settings.get("mirror_mode", "off"))).apply(cand, adaptive_stats)
                    cand = SessionEngine(
                        enabled=bool(settings.get("session_filter_enabled", True)),
                        america_short_bias_enabled=bool(settings.get("america_short_bias_enabled", True)),
                        window_minutes=240,
                    ).apply(cand, settings)

                    spot_enabled = bool(settings.get("spot_confirmation_enabled", True))
                    spot_data = await fetch_spot_data_for_candidate(ex, cand, settings) if spot_enabled else None
                    cand = SpotConfirmationEngine(enabled=spot_enabled).apply(cand, spot_data)
                    cand["strategy_mode"] = base_strategy_mode
                    cand["effective_strategy_mode"] = effective_strategy

                    if not cand.get("allowed_by_session", True):
                        scanner.last_reject_reason = f"{original_symbol}: session filter blocked"
                        continue
                    if spot_enabled and not cand.get("spot_confirmed", True):
                        scanner.last_reject_reason = (
                            f"{original_symbol}: spot confirmation failed "
                            f"({cand.get('spot_confirmation', 'WEAK')}: {cand.get('spot_reason', '-')})"
                        )
                        continue
                    scanner.last_signal_summary = (
                        f"{cand.get('symbol')} {cand.get('side')} conf={cand.get('confidence')} "
                        f"strategy={cand.get('strategy', effective_strategy)} mode={effective_strategy} "
                        f"spot={cand.get('spot_confirmation', 'OFF')}"
                    )
                    mf_ok, mf_reason = risk.market_filters(cand, settings)
                    if not mf_ok:
                        scanner.last_reject_reason = f"{original_symbol}: market filter blocked: {mf_reason}"
                        continue

                    plan = TradePlanner().make_plan(cand, settings, equity_usdt=equity)
                    if not plan:
                        scanner.last_reject_reason = f"{original_symbol}: planner returned no trade"
                        continue

                    ai_enabled = bool(settings.get("openai_analysis_enabled", False))
                    ai_show = bool(settings.get("openai_show_decisions", False))
                    ai_message_id = None
                    if ai_enabled and ai_show:
                        from openai_signal_engine import active_model, active_strength
                        preview = type("AIPreview", (), {"model": active_model(settings), "mode": active_strength(settings)})()
                        ai_message_id = await send_or_edit_ai_decision(
                            app,
                            format_ai_decision(plan, preview, stage="start"),
                            message_id=None,
                        )
                    ai_verdict = await ai_signal_engine.validate(cand, plan, settings)
                    if ai_enabled:
                        if ai_show:
                            ai_message_id = await send_or_edit_ai_decision(
                                app,
                                format_ai_decision(plan, ai_verdict, stage="done"),
                                message_id=ai_message_id,
                            )
                        elif ai_verdict_is_important(ai_verdict):
                            await notify_admin(
                                app,
                                format_ai_minimal_issue(plan, ai_verdict),
                                min_interval_sec=30,
                                key="ai_minimal_issue",
                            )
                        if not ai_verdict.approved:
                            scanner.last_reject_reason = (
                                f"{plan.symbol}: OpenAI rejected "
                                f"model={ai_verdict.model} mode={ai_verdict.mode} "
                                f"conf={ai_verdict.confidence:.2f} "
                                f"reason={ai_verdict.reason or ai_verdict.error or 'no reason'}"
                            )
                            continue
                        cand["openai_approved"] = True
                        cand["openai_confidence"] = ai_verdict.confidence
                        cand["openai_reason"] = ai_verdict.reason

                    try:
                        placed = await exec_engine.place_entry(plan, live)
                    except Exception as e:
                        scanner.last_reject_reason = f"{plan.symbol}: execution exception: {e}"
                        continue
                    if placed.get("ok"):
                        scanner.last_reject_reason = f"opened {plan.symbol} {plan.side}"
                        opened_this_cycle = True
                        await update_scanner_status(app, settings, status="position opened", force=True)
                        await notify_admin(
                            app,
                            format_position_opened(plan, placed, live, ai_verdict if ai_enabled else None),
                            key="position_opened",
                        )
                        await send_trade_chart(app, ex, plan, settings)
                    else:
                        scanner.last_reject_reason = f"{plan.symbol}: execution rejected: {placed.get('reason', 'unknown')}"

                if candidates and not opened_this_cycle:
                    await update_scanner_status(app, settings, status="signal rejected", force=True)
                elif candidates:
                    await update_scanner_status(app, settings, status="scanning")

                await sleep_until_next_scan(app, int(settings.get("scan_interval_sec", 5)))
            except Exception as e:
                log.exception("trading loop error: %s", e)
                await asyncio.sleep(5)
    finally:
        trading_task = None

async def on_startup(app):
    await storage.init()
    settings = await storage.all_settings()
    apply_mexc_runtime_env(settings)
    app.bot_data.setdefault("trading_start_lock", asyncio.Lock())
    app.bot_data.setdefault("scan_wakeup_event", asyncio.Event())
    # v0071: real startup recovery. If Railway restarts while MEXC positions
    # remain open, rebuild local state from exchange positions and reattach
    # protection/local monitoring before the scanner starts opening new trades.
    try:
        if str(settings.get("live_trading", False)).lower() in {"1", "true", "yes", "on"}:
            api_key, api_secret = _api_creds(settings)
            if api_key and api_secret:
                ex = await get_exchange(settings)
                exec_engine = ExecutionEngine(storage, ex)
                report = await RecoveryEngine(storage, ex, exec_engine).recover(
                    reattach=str(os.getenv("RECOVERY_REATTACH_PROTECTION", "true")).lower() in {"1", "true", "yes", "on"}
                )
                app.bot_data["startup_recovery_report"] = report
    except Exception as e:
        app.bot_data["startup_recovery_error"] = str(e)

def build_app():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).post_init(on_startup).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("run", run_cmd))
    app.add_handler(CommandHandler("stop", stop_cmd))
    app.add_handler(CommandHandler("panic", panic_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("ping", ping_cmd))
    app.add_handler(CommandHandler("balance", balance_cmd))
    app.add_handler(CommandHandler("positions", positions_cmd))
    app.add_handler(CommandHandler("mexc_debug_state", mexc_debug_state_cmd))
    app.add_handler(CommandHandler("open_orders", open_orders_cmd))
    app.add_handler(CommandHandler("cancel_all", cancel_all_cmd))
    app.add_handler(CommandHandler("close_all", close_all_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("ai_stats", ai_stats_cmd))
    app.add_handler(CommandHandler("ai_stats_current", ai_stats_current_cmd))
    app.add_handler(CommandHandler("ai_stats_lifetime", ai_stats_lifetime_cmd))
    app.add_handler(CommandHandler("ai_stats_reset", ai_stats_reset_cmd))
    app.add_handler(CommandHandler("sync", sync_cmd))
    app.add_handler(CommandHandler("sync_positions", sync_positions_cmd))
    app.add_handler(CommandHandler("recovery", recovery_cmd))
    app.add_handler(CommandHandler("settings", settings_cmd))
    app.add_handler(CommandHandler("mexc_settings", mexc_settings_cmd))
    app.add_handler(CommandHandler("leverage", leverage_cmd))
    app.add_handler(CommandHandler("open_type", open_type_cmd))
    app.add_handler(CommandHandler("recv_window", recv_window_cmd))
    app.add_handler(CommandHandler("set", set_cmd))
    app.add_handler(CommandHandler("proxy", proxy_cmd))
    app.add_handler(CommandHandler("api", api_cmd))
    app.add_handler(CommandHandler("openai", openai_cmd))
    app.add_handler(CallbackQueryHandler(callback_router))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))
    return app

if __name__ == "__main__":
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN is required")
    build_app().run_polling()
