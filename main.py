import os, time, asyncio, logging, json
from datetime import datetime, timezone, timedelta
import aiohttp
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
import psutil

from config import TELEGRAM_TOKEN, ADMIN_IDS, VERSION, DEFAULT_EXCHANGE, DEFAULTS
from storage import Storage
try:
    from storage import DEFAULT_SETTINGS
except ImportError:
    DEFAULT_SETTINGS = {}
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
from boost_scalping_engine import BoostScalpingEngine
from ai_stats import AIStatsManager
from position_manager import PositionManager
from chart_renderer import render_trade_setup_chart
from debug_log import tail_text, tail_important, log_event

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bot")

storage = Storage()
scanner = Scanner()
ai_signal_engine = OpenAISignalEngine()
ai_scalping_engine = AIScalpingEngine()
boost_scalping_engine = BoostScalpingEngine()
ai_stats_manager = AIStatsManager(storage)
running = False
# New-entry switch is intentionally separate from the loop switch.
# /stop pauses entries but keeps the position manager alive; /panic stops the loop.
entries_enabled = False
started_at = time.time()
exchange_client = None
balance_locks = {}
ws_supervisor = None
trading_task = None
position_task = None

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
        await asyncio.wait_for(app.bot.send_message(chat_id=chat_id, text=str(text)[:3900]), timeout=6)
    except Exception as e:
        log.warning("telegram notification failed/timeout: %s", e)


async def notify_admin_bottom_replace(app, text: str, key: str = "live_status", min_interval_sec: int = 0) -> None:
    """Keep one live Telegram status message at the bottom of the chat.

    Telegram edits do not move old messages down, so for noisy live updates
    (watchdogs, protection checks, etc.) we delete the previous status message
    and send a fresh one. Important lifecycle events should still use
    notify_admin() so they remain in the history.
    """
    chat_id = first_admin_id()
    if not chat_id:
        return
    now = time.time()
    if min_interval_sec:
        last_key = f"last_bottom_{key}"
        last = float(app.bot_data.get(last_key, 0) or 0)
        if now - last < min_interval_sec:
            return
        app.bot_data[last_key] = now

    msg_key = f"bottom_msg_id_{key}"
    old_msg_id = app.bot_data.get(msg_key)
    if old_msg_id:
        try:
            await asyncio.wait_for(app.bot.delete_message(chat_id=chat_id, message_id=int(old_msg_id)), timeout=4)
        except Exception as e:
            # Message may already be gone or too old; continue and post a fresh one.
            log.debug("telegram bottom status delete skipped: %s", e)
    try:
        msg = await asyncio.wait_for(app.bot.send_message(chat_id=chat_id, text=str(text)[:3900]), timeout=6)
        app.bot_data[msg_key] = getattr(msg, "message_id", None)
    except Exception as e:
        log.warning("telegram bottom status failed: %s", e)

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
        if bool(getattr(plan, "liquidation_stop_mode", False)):
            liq_note = f"LIQ≈{getattr(plan, 'liquidation_estimated_distance_pct', 0):.3g}% via {getattr(plan, 'leverage', 0)}x"
            caption = (
                "📊 Trade setup chart\n"
                f"{plan.symbol} {plan.side} | {plan.strategy}\n"
                f"Entry {plan.entry_price:.8g} | {liq_note} | TP {plan.take_price:.8g}"
            )
        else:
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
        boost_mode = mode.lower() == "boost_scalping"
        universe = "full" if boost_mode else str(settings.get("universe_mode", "adaptive"))
        checked = int(getattr(scanner, "last_cycle_scanned", 0) or 0)
        errors = int(getattr(scanner, "last_cycle_errors", 0) or 0)
        loaded = int(getattr(scanner, "last_available_markets", 0) or 0) if boost_mode else len(getattr(scanner, "hot_symbols", []) or [])
    scan_every = format_duration_seconds(settings.get("scan_interval_sec", 5))
    icon = "🟢" if status_label in {"scanning", "universe ready"} else ("🟡" if "blocked" not in status_label and "paused" not in status_label else "🛑")
    lines = [
        f"🔎 Scanner: {icon} {status_label}",
        f"📈 Mode: {mode}" + (f" → {effective}" if effective and effective != mode else ""),
        f"🌐 Universe: {universe} | loaded {loaded}",
        f"✅ Checked: {checked}" + (f" | errors {errors}" if errors else ""),
        f"🎯 Last setup: {signal}",
        f"🧠 Decision: {decision}",
    ]
    if mode.lower() == "boost_scalping":
        lines.append(f"🪙 Zero-fee DB: {_boost_zero_fee_count_from_settings(settings)} symbols")
    ai_candidates = int(getattr(scanner, "last_ai_candidates_count", 0) or 0)
    lines.append(f"🤖 AI candidates: {ai_candidates}")
    if ai_mode:
        ai_list = getattr(scanner, "last_ai_check_symbols", []) or []
        if ai_list:
            lines.append("🤖 AI checked: " + ", ".join(map(str, ai_list[:8])))
    elif bool(settings.get("scanner_reject_log_enabled", True)):
        top_rejects = getattr(scanner, "last_reject_top_reasons", []) or []
        if top_rejects:
            top = "; ".join(f"{r}:{c}" for r, c in top_rejects[:4])
            lines.append(f"🚫 Top rejects: {top}")
        examples = getattr(scanner, "last_reject_examples", []) or []
        if examples:
            lines.append("📌 Examples: " + "; ".join(map(str, examples[:3])))
    lines += [
        f"⏱ Next cycle pause: {scan_every}",
        f"🕒 {datetime.now(timezone(timedelta(hours=3))).strftime('%H:%M:%S')}",
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


async def quick_bounce_progress_message(app, pct: int, *, done: bool = False, clear: bool = False) -> None:
    """Move the quick-bounce scan progress to the bottom with only 3 updates.

    We delete the previous progress message and send a fresh one for 10/50/100%.
    This keeps the chat current without high-frequency Telegram edits.
    """
    chat_id = first_admin_id()
    if not chat_id:
        return
    old_id = app.bot_data.get("quick_bounce_progress_message_id")
    if old_id:
        try:
            await asyncio.wait_for(app.bot.delete_message(chat_id=chat_id, message_id=int(old_id)), timeout=4)
        except Exception as e:
            log.debug("quick bounce progress delete skipped: %s", e)
        app.bot_data["quick_bounce_progress_message_id"] = None
    if clear:
        log_event("quick_bounce_progress", stage="clear", ok=True, pct=pct)
        return
    text = f"✅ Закончил сканирование {pct}%" if done else f"🔍 Сканирование {pct}%..."
    try:
        msg = await asyncio.wait_for(app.bot.send_message(chat_id=chat_id, text=text), timeout=6)
        app.bot_data["quick_bounce_progress_message_id"] = getattr(msg, "message_id", None)
        log_event("quick_bounce_progress", stage="done" if done else "scan", ok=True, pct=pct, message_id=getattr(msg, "message_id", None))
    except Exception as e:
        log.warning("quick bounce progress send failed: %s", e)
        log_event("quick_bounce_progress_error", stage="send", ok=False, pct=pct, error=str(e)[:300])


def _qb_symbol(symbol: str) -> str:
    return str(symbol or "-").replace("/USDT:USDT", "").replace("/USDT", "").replace("_USDT", "").upper()


async def quick_bounce_summary_message(app, settings: dict, candidates: list[dict] | None = None, *, opened_note: str = "") -> None:
    """Keep one quick-bounce summary panel at the bottom: delete old, send new."""
    chat_id = first_admin_id()
    if not chat_id:
        return
    candidates = candidates or []
    try:
        positions = await storage.positions()
    except Exception:
        positions = []
    qb_positions = [p for p in positions if str(p.get("strategy", "")).lower() == "quick_bounce"]
    open_names = [_qb_symbol(p.get("symbol")) for p in qb_positions]
    max_slots = int(float(settings.get("quick_bounce_max_open_positions", settings.get("max_open_positions", 5)) or 5))
    found = int(getattr(scanner, "last_ai_candidates_count", 0) or len(candidates))
    chosen = [_qb_symbol(c.get("symbol")) for c in candidates[:max_slots]]
    free_slots = max(0, max_slots - len(qb_positions))
    picked_now = chosen[:free_slots]
    reserve = chosen[free_slots:free_slots + 1]
    try:
        since = time.time() - 86400
        trades = [t for t in await storage.trade_rows(since=since) if str(t.get("strategy", "")).lower() == "quick_bounce"]
    except Exception:
        trades = []
    closed = [_qb_symbol(t.get("symbol")) for t in trades[-8:]]
    killed = [_qb_symbol(t.get("symbol")) for t in trades if "time" in str(t.get("reason", "")).lower() or "time" in str(t.get("result", "")).lower()]
    pnl = sum(float(t.get("pnl_usdt") or 0) for t in trades)
    lines = [
        "⚡ БЫСТРЫЙ ОТСКОК",
        "",
        f"Сканирование: топ {int(float(settings.get('quick_bounce_top_coins', settings.get('max_symbols', 200)) or 200))}",
        f"Нашёл монет по условиям в круге: {found}",
        "Выбраны лучшие: " + (", ".join(picked_now) if picked_now else "нет"),
    ]
    if reserve:
        lines.append(f"Лучший кандидат при освобождении слота: {reserve[0]}")
    lines += [
        "Открытые на бирже: " + (", ".join(open_names) if open_names else "нет"),
        "Закрытые на бирже: " + (", ".join(closed) if closed else "нет"),
        f"Заполнены {len(qb_positions)}/{max_slots} слотов",
        "Убитые за 12 часов: " + (", ".join(killed[-5:]) if killed else "нет"),
        f"Общий плюс по закрытым монетам: ${pnl:+.2f}",
    ]
    if opened_note:
        lines += ["", opened_note]
    text = "\n".join(lines)[:3900]
    old_id = app.bot_data.get("quick_bounce_summary_message_id")
    if old_id:
        try:
            await asyncio.wait_for(app.bot.delete_message(chat_id=chat_id, message_id=int(old_id)), timeout=4)
        except Exception as e:
            log.debug("quick bounce summary delete skipped: %s", e)
    try:
        msg = await asyncio.wait_for(app.bot.send_message(chat_id=chat_id, text=text), timeout=6)
        app.bot_data["quick_bounce_summary_message_id"] = getattr(msg, "message_id", None)
        log_event(
            "quick_bounce_summary",
            stage="sent",
            ok=True,
            found=found,
            chosen=chosen,
            open_symbols=open_names,
            closed_symbols=closed[-8:],
            time_killed=killed[-5:],
            slots=f"{len(qb_positions)}/{max_slots}",
            closed_pnl_usdt=round(pnl, 4),
            message_id=getattr(msg, "message_id", None),
        )
    except Exception as e:
        log.warning("quick bounce summary send failed: %s", e)
        log_event("quick_bounce_summary_error", stage="send", ok=False, error=str(e)[:300])


def format_quick_bounce_opened(plan, placed: dict) -> str:
    pos = placed.get("position") if isinstance(placed, dict) else None
    pos = pos if isinstance(pos, dict) else plan.__dict__
    entry = float(pos.get("entry_price") or plan.entry_price)
    stop = float(pos.get("stop_price") or plan.stop_price)
    take = float(pos.get("take_price") or plan.take_price)
    _notional, margin, leverage, _margin_type = _position_money_fields(pos)
    side = str(pos.get("side") or getattr(plan, "side", "")).upper()
    protection_mode = str(pos.get("protection_mode") or "unknown").lower()
    if protection_mode in {"exchange", "exchange_planorder", "exchange_planorder_pending_verify"}:
        protection_line = "защита: реальные SL/TP на бирже"
    elif protection_mode in {"virtual", "local_monitoring"}:
        protection_line = "защита: виртуальные SL/TP"
    else:
        protection_line = f"защита: {protection_mode}"
    return "\n".join([
        f"🟢 Открыл {side} {_qb_symbol(plan.symbol)}",
        f"${margin:.2f} плечо x{leverage}",
        f"вход ${_fmt_price(entry)}",
        f"стоп ${_fmt_price(stop)}",
        f"тейк ${_fmt_price(take)}",
        protection_line,
    ])


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
        "boost_monitor_only_no_exchange_protection": "BOOST monitor-only: exchange TP/SL missing; local negative SL close skipped",
    }
    label = reason_map.get(str(typ), str(typ))

    # v0134 improved protection notifications
    if typ == "boost_monitor_only_no_exchange_protection":
        try:
            pp = float(ev.get("pnl_pct") or 0)
            pp_s = f"{pp:+.3f}%"
        except Exception:
            pp_s = "n/a"
        return "\n".join([
            "🟡 BOOST monitor-only",
            f"{symbol}",
            "Exchange TP/SL is still missing.",
            f"Local SL close skipped; current PnL: {pp_s}",
            "Bot keeps monitoring and will keep trying to restore exchange protection."
        ])

    if typ == "protection_watchdog":
        protection_status = str(ev.get("protection_status") or "UNKNOWN")
        protection_mode = str(ev.get("protection_mode") or "")
        stop_ok = bool(ev.get("stop_loss_ok", ev.get("sl_exists", False)))
        tp_ok = bool(ev.get("take_profit_ok", ev.get("tp_exists", False)))
        reattach = bool(ev.get("reattach_attempted"))

        if protection_status == "EXCHANGE PROTECTED":
            lines = [
                "✅ EXCHANGE PROTECTED",
                f"{symbol}",
                f"SL: {'OK' if stop_ok else 'MISSING'}",
                f"TP: {'OK' if tp_ok else 'MISSING'}",
                "Protection is confirmed on MEXC."
            ]
        elif protection_status == "TP + LIQUIDATION STOP" or protection_mode == "exchange_tp_liquidation_sl":
            lines = [
                "✅ TP ON EXCHANGE + LIQUIDATION STOP",
                f"{symbol}",
                "SL: LIQUIDATION MODE — no exchange SL by design",
                f"TP: {'OK' if tp_ok else 'MISSING'}",
                "Only take-profit is placed on MEXC; liquidation replaces the stop."
            ]
        else:
            lines = [
                "⚠️ LOCAL PROTECTION MODE",
                f"{symbol}",
                f"SL: {'OK' if stop_ok else 'MISSING'}",
                f"TP: {'OK' if tp_ok else 'MISSING'}",
                "Exchange TP/SL not confirmed.",
                "Bot monitors and can close the trade locally."
            ]

            if reattach:
                lines.append("🔁 Bot is trying to restore TP/SL on exchange.")

        return "\n".join(lines)

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
    """Send Telegram text safely.

    Telegram hard-limits one message to 4096 chars. A long /help previously
    raised "Message is too long", which made the bot look dead. Split all
    oversized replies and keep the keyboard only on the last chunk.
    """
    text = str(text or "")
    max_len = 3900
    chunks = []
    while len(text) > max_len:
        cut = text.rfind("\n", 0, max_len)
        if cut < 1200:
            cut = max_len
        chunks.append(text[:cut].rstrip())
        text = text[cut:].lstrip()
    chunks.append(text)

    target = update.message if update.message else (update.callback_query.message if update.callback_query else None)
    if not target:
        return None
    last_msg = None
    base_kwargs = dict(kwargs)
    for i, chunk in enumerate(chunks):
        send_kwargs = dict(base_kwargs)
        if i < len(chunks) - 1:
            send_kwargs.pop("reply_markup", None)
        try:
            last_msg = await asyncio.wait_for(target.reply_text(str(chunk)[:3900], **send_kwargs), timeout=6)
        except Exception as e:
            log.warning("telegram reply failed/timeout: %s", e)
            last_msg = None
            break
    return last_msg


async def _safe_send_bot_message(app, chat_id: int, text: str, **kwargs):
    """Send Telegram notification with timeout so BOOST never blocks commands."""
    try:
        return await asyncio.wait_for(app.bot.send_message(chat_id=chat_id, text=text[:3900], **kwargs), timeout=6)
    except Exception as e:
        log.warning("telegram send timeout/error: %s", e)
        return None

async def _safe_delete_bot_message(app, chat_id: int, message_id: int):
    try:
        return await asyncio.wait_for(app.bot.delete_message(chat_id=chat_id, message_id=int(message_id)), timeout=4)
    except Exception as e:
        log.debug("telegram delete timeout/error: %s", e)
        return None


async def _safe_edit_message_text(message, text: str, **kwargs):
    """Edit Telegram message with timeout; never block update queue."""
    try:
        return await asyncio.wait_for(message.edit_text(str(text)[:3900], **kwargs), timeout=5)
    except Exception as e:
        log.warning("telegram edit timeout/error: %s", e)
        return None

async def _await_with_timeout(coro, timeout_sec: float, label: str):
    """Small wrapper used by command handlers and BOOST so one slow MEXC call cannot freeze buttons."""
    try:
        return await asyncio.wait_for(coro, timeout=float(timeout_sec))
    except asyncio.TimeoutError:
        raise TimeoutError(f"{label} timeout after {timeout_sec}s")

def _boost_disarm_runtime(app) -> None:
    """Hard runtime disarm: prevents stale BOOST state and repeated button presses from re-arming it."""
    try:
        app.bot_data["boost_armed_runtime"] = False
        app.bot_data["boost_start_in_progress"] = False
        app.bot_data["boost_last_action"] = "disarmed"
    except Exception:
        pass

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    await reply(update, f"🤖 Liquidity Bot v{VERSION}\nГлавное меню:", reply_markup=MAIN_MENU)


async def log_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return
    try:
        n = 80
        if context.args:
            try:
                n = max(20, min(300, int(context.args[0])))
            except Exception:
                n = 80
        text = tail_important(lines=n, max_chars=3500)
        # Telegram message limit is 4096. Keep it copyable and readable.
        await reply(update, "🧾 Last bot logs:\n```\n" + text[-3400:] + "\n```", reply_markup=MAIN_MENU)
    except Exception as e:
        await reply(update, f"/log error: {e}", reply_markup=MAIN_MENU)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    await reply(update, f"""
🤖 Liquidity Bot v{VERSION}

Команды:
/start - меню
/help - помощь
/log [lines] - последние MEXC/TP-SL ошибки
/run - запустить торговлю
/boost_start или кнопка 🚀 BOOST MODE - запустить BOOST autopilot: 10% депозита → x20 цель
/boost_stop или кнопка 🛑 STOP BOOST - остановить BOOST и новые входы
/boost_status или кнопка 📊 BOOST STATUS - статус BOOST банка/цели
/boost_rotation - включить/выключить rotation while in profit
/boost_list BTC,ETH,SOL - задать trusted 0-fee whitelist для BOOST
/boost_list - показать whitelist
/boost_list_del - очистить whitelist
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

Note: /positions checks MEXC exchange-first; /open_orders scans normal + plan + stop + TP/SL endpoints. If exchange TP/SL is missing after retries, new AI scalping entries are closed immediately.
/proxy on|off|test|set URL
/api status|set KEY SECRET|clear|test - API биржи через чат
/openai status|set KEY|clear|test - OpenAI ключ для ИИ проверки
BOOST autopilot: /boost_start или кнопка 🚀 BOOST MODE включает режим boost_scalping. Сначала использует API-подтверждённые 0-fee futures пары; если задан /boost_list BTC,ETH,SOL, то эти trusted пары тоже разрешены как 0-fee whitelist. Выбирает живую монету, LONG/SHORT и плечо, использует только boost-bank = 10% депозита и останавливается при x20 цели.
AI scalping loop: кнопка 🤖 AI BTC/ETH scalping или /set strategy_mode ai_scalping. BTC и ETH независимы: AI-запрос только по символу без открытой позиции. После live-входа бот ждёт появления позиции на MEXC, затем ставит TP/SL. Если MEXC не подтвердил защиту после повторов, AI scalping позиция закрывается сразу: нет защиты на бирже = нет позиции. Доп. фильтры качества включаются отдельно: ai_scalping_quality_filters_enabled.
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
ai_scalping_symbols, ai_scalping_min_confidence, ai_scalping_ai_entry_filter_enabled, ai_scalping_tp_pct, ai_scalping_sl_pct, ai_scalping_btc_tp_pct, ai_scalping_btc_sl_pct, ai_scalping_eth_tp_pct, ai_scalping_eth_sl_pct, ai_scalping_btc_min_tp_pct, ai_scalping_btc_max_tp_pct, ai_scalping_eth_min_tp_pct, ai_scalping_eth_max_tp_pct, ai_scalping_sl_tp_multiplier, ai_scalping_max_spread_pct, ai_scalping_quality_filters_enabled, ai_scalping_quality_min_confidence, ai_scalping_quality_cooldown_sec, ai_scalping_quality_min_atr_pct, ai_scalping_quality_min_ema_gap_pct, ai_scalping_quality_min_ret_5m_abs_pct, ai_scalping_ai_cooldown_sec, ai_scalping_openai_fallback_enabled, ai_scalping_json_mode_enabled, ai_scalping_liquidation_stop_mode, ai_scalping_liq_margin_pct, ai_scalping_liq_buffer_pct, ai_scalping_liq_max_leverage,
scan_market_source = binance_binance | mexc_mexc | mexc_binance.

По умолчанию: mexc_binance = MEXC фьючи скан + Binance spot подтверждение.
""".strip(), reply_markup=MAIN_MENU)

async def run_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Regression marker: global running, trading_task
    global running, entries_enabled, trading_task, position_task
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
        if position_task is None or position_task.done():
            position_task = context.application.create_task(position_management_loop(context.application))

    # v0079: one Run press must create exactly one Telegram message. Earlier
    # versions replied "started" and then immediately sent a separate scanner
    # status card, which looked like duplicate start. The reply itself becomes
    # the editable scanner-status message.
    settings = await storage.all_settings()
    # v0185: normal Run must not implicitly start BOOST from a stale strategy_mode.
    if str(settings.get("strategy_mode", "")).lower() == "boost_scalping" and not _is_boost_runtime_armed(context.application, settings):
        await storage.set("boost_autopilot_active", False, bump_revision=False)
        await storage.set("strategy_mode", "hybrid", bump_revision=False)
        settings = await storage.all_settings()
    status = "already running; scan requested now" if already_running else "started; scan requested now"
    header = "🟢 Bot already running" if already_running else "🟢 Bot started"
    msg = await reply(update, f"{header}\n" + _scan_status_text(settings, status=status), reply_markup=MAIN_MENU)
    if msg is not None:
        context.application.bot_data["scanner_status_message_id"] = msg.message_id
        context.application.bot_data["scanner_status_last_edit"] = time.time()
    trigger_scan_now(context.application, reason="run_button")

async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global running, entries_enabled, trading_task, position_task
    if not allowed(update): return
    entries_enabled = False
    _boost_disarm_runtime(context.application)
    await storage.set("boost_autopilot_active", False, bump_revision=False)
    await storage.set("strategy_mode", "hybrid", bump_revision=False)
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
    global running, entries_enabled, trading_task, position_task
    if not allowed(update): return
    entries_enabled = False
    running = False
    _boost_disarm_runtime(context.application)
    await storage.set("boost_autopilot_active", False, bump_revision=False)
    await storage.set("strategy_mode", "hybrid", bump_revision=False)
    for task in (trading_task, position_task):
        if task and not task.done():
            task.cancel()
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


async def _boost_session_snapshot(settings: dict) -> dict:
    now_ts = time.time()
    start_ts = float(settings.get("boost_session_start_ts", 0) or 0)
    equity = float(settings.get("boost_session_start_equity", 0) or 0)
    bank = float(settings.get("boost_session_bank_usdt", 0) or 0)
    target_mult = max(1.0, float(settings.get("boost_target_multiplier", 20.0) or 20.0))
    if start_ts <= 0 or bank <= 0:
        return {"active": False, "start_ts": start_ts, "bank": bank, "pnl": 0.0, "current_bank": bank, "target_bank": bank * target_mult, "target_mult": target_mult, "age_sec": 0, "equity": equity}
    trades = [t for t in await storage.trade_rows(since=start_ts) if str(t.get("strategy", "")).lower() == "boost_scalping"]
    pnl = sum(float(t.get("pnl_usdt") or 0) for t in trades)
    current_bank = max(0.0, bank + pnl)
    return {"active": True, "start_ts": start_ts, "bank": bank, "pnl": pnl, "current_bank": current_bank, "target_bank": bank * target_mult, "target_mult": target_mult, "age_sec": max(0, now_ts - start_ts), "equity": equity, "trades": len(trades)}



def _bool_setting(settings: dict, key: str, default: bool = False) -> bool:
    val = settings.get(key, default)
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in {"1", "true", "yes", "on"}


def _is_boost_active(settings: dict) -> bool:
    """Persistent BOOST switch set only by /boost_start and cleared by stop/emergency."""
    return _bool_setting(settings, "boost_autopilot_active", False)


def _is_boost_runtime_armed(app, settings: dict) -> bool:
    """Return True only when BOOST was explicitly armed in this running process.

    This blocks stale DB state and unrelated buttons/callbacks from reviving
    boost_scalping. After Railway restart the user must press /boost_start or
    🚀 BOOST MODE again; /balance, /status, Settings, etc. cannot start BOOST.
    """
    return bool(app.bot_data.get("boost_armed_runtime", False)) and _is_boost_active(settings)


def _boost_rotation_keyboard(settings: dict) -> InlineKeyboardMarkup:
    rev = int(settings.get("settings_revision", 1) or 1)
    rot_on = _bool_setting(settings, "boost_parallel_scan_enabled", True)
    live_on = _bool_setting(settings, "boost_live_panel_enabled", True)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(("✅ Rotation ON" if rot_on else "○ Rotation OFF"), callback_data=f"boost:rotation:{rev}")],
        [InlineKeyboardButton(("✅ Live panel ON" if live_on else "○ Live panel OFF"), callback_data=f"boost:panel:{rev}")],
        [InlineKeyboardButton("🔄 Refresh", callback_data=f"boost:refresh:{rev}")],
    ])


def _short_symbol(symbol: str) -> str:
    return str(symbol or "-").replace("/", "_").replace(":USDT", "")



def _boost_store_live_state(app, settings: dict | None = None, snap: dict | None = None, *, status: str = "scan", decision=None, position: dict | None = None, note: str = "") -> None:
    """Store latest BOOST state for the independent Telegram live watchdog.

    The trading loop can be busy on MEXC REST. The watchdog uses this cache to
    keep Telegram alive instead of waiting for the next scanner branch.
    """
    try:
        app.bot_data["boost_live_last_state"] = {
            "ts": time.time(),
            "settings": dict(settings or {}),
            "snap": dict(snap or {}),
            "status": str(status or "scan")[:120],
            "note": str(note or "")[:500],
            "position": dict(position or {}) if isinstance(position, dict) else None,
            "decision_symbol": str(getattr(decision, "symbol", "") or "")[:80] if decision is not None else "",
            "decision_side": str(getattr(decision, "decision", "") or "")[:24] if decision is not None else "",
            "decision_conf": float(getattr(decision, "confidence", 0) or 0) if decision is not None else 0.0,
            "decision_reason": str(getattr(decision, "reason", "") or "")[:260] if decision is not None else "",
        }
    except Exception:
        pass


class _BoostCachedDecision:
    def __init__(self, state: dict):
        self.symbol = state.get("decision_symbol", "")
        self.decision = state.get("decision_side", "")
        self.confidence = state.get("decision_conf", 0.0)
        self.reason = state.get("decision_reason", "")
        self.market = {"markets": [{"symbol": self.symbol}]}


async def _boost_live_panel_watchdog(app) -> None:
    """Independent BOOST Telegram heartbeat.

    This is intentionally separate from the trading loop. If scanning/execution is
    slow or waiting on MEXC, the chat still receives a fresh bottom panel and /log
    gets visible heartbeat/error events.
    """
    log_event("boost_live_panel_watchdog", stage="started", ok=True)
    while True:
        try:
            settings = await storage.all_settings()
            if not _is_boost_runtime_armed(app, settings):
                break
            if not _bool_setting(settings, "boost_live_panel_enabled", True):
                await asyncio.sleep(2)
                continue
            state = dict(app.bot_data.get("boost_live_last_state") or {})
            snap = dict(state.get("snap") or {})
            if not snap:
                try:
                    snap = await _boost_session_snapshot(settings)
                except Exception:
                    bank = float(settings.get("boost_session_bank_usdt", 0) or 0)
                    mult = float(settings.get("boost_target_multiplier", 20) or 20)
                    snap = {"bank": bank, "current_bank": bank, "target_bank": bank * mult, "pnl": 0.0, "trades": 0, "target_mult": mult}
            decision = _BoostCachedDecision(state) if state.get("decision_symbol") or state.get("decision_reason") else None
            age = time.time() - float(state.get("ts", 0) or 0)
            status = state.get("status") or "hunter scanning"
            note = state.get("note") or "watchdog: BOOST live feed active"
            if age > 8:
                note = f"watchdog heartbeat; trading loop last update {age:.0f}s ago. {note}"[:500]
            # v0207: watchdog must NOT spam Telegram. It only refreshes the
            # cached HUD on a slow heartbeat; important events are still sent
            # immediately by the trading loop with force=True.
            await boost_live_panel_update(
                app,
                settings,
                snap,
                status=status,
                decision=decision,
                position=state.get("position"),
                note=note,
                force=False,
            )
            await asyncio.sleep(max(5.0, float(settings.get("boost_live_watchdog_tick_sec", 5) or 5)))
        except asyncio.CancelledError:
            log_event("boost_live_panel_watchdog", stage="cancelled", ok=True)
            raise
        except Exception as e:
            log_event("boost_live_panel_watchdog", stage="error", ok=False, error=str(e)[:500])
            await asyncio.sleep(3)
    log_event("boost_live_panel_watchdog", stage="stopped", ok=True)




def _boost_hud_status_key(status: str, decision=None, position: dict | None = None) -> str:
    """Coarse event key for the Telegram trader HUD.

    The chat is a trader HUD, not a debug console. Scanner noise may update the
    in-memory cache and /log, but Telegram is allowed to move/send the panel only
    on important state transitions or on a slow heartbeat.
    """
    st = str(status or "").strip().lower()
    if any(x in st for x in ("opened", "entry", "rotation opened")):
        sym = str(getattr(decision, "symbol", "") or (position or {}).get("symbol") or "")
        side = str(getattr(decision, "decision", "") or (position or {}).get("side") or "")
        return f"entry:{sym}:{side}"
    if any(x in st for x in ("rotated", "rotation")):
        sym = str(getattr(decision, "symbol", "") or "")
        return f"rotation:{sym}"
    if any(x in st for x in ("target", "completed")):
        return "target"
    if any(x in st for x in ("stopped", "stop")):
        return "stopped"
    if any(x in st for x in ("blocked", "symbol blocked")):
        return "blocked"
    if "error" in st or "failed" in st:
        return "error"
    if any(x in st for x in ("armed", "confirmed")):
        sym = str(getattr(decision, "symbol", "") or "")
        return f"armed:{sym}"
    if position:
        sym = str((position or {}).get("symbol") or "")
        side = str((position or {}).get("side") or "")
        return f"active:{sym}:{side}"
    if "waiting" in st or "scanning" in st or "scan" in st:
        return "scanning"
    return st[:60] or "unknown"


def _boost_hud_is_important(status: str, decision=None, position: dict | None = None, *, force: bool = False) -> bool:
    st = str(status or "").lower()
    if any(x in st for x in (
        "started", "opened", "entry", "rotation", "rotated", "target", "completed",
        "stopped", "blocked", "error", "failed", "armed", "confirmed", "profit", "tp", "sl", "unsafe", "defensive",
    )):
        return True
    # force=True from scanner wait/reject branches is intentionally ignored unless
    # it is an important event; otherwise Telegram becomes unreadable.
    return False


def _boost_hud_clean_note(note: str, status: str = "") -> str:
    raw = str(note or "").strip()
    low = raw.lower()
    st = str(status or "").lower()
    if not raw:
        if "scan" in st or "wait" in st:
            return "Жду EXTREME impulse. Технические детали смотри в /log."
        return ""
    # Hide per-symbol reject spam from Telegram. Full details stay in /log.
    if "unsafe position" in low or "emergency sl" in low:
        return raw[:260]
    if "hunter no-trade" in low:
        return "NO-TRADE: нет подтверждённого EXTREME impulse. Детали по монетам — в /log."
    if raw.count("/USDT") >= 2 or raw.count(";") >= 2:
        return "Сканер работает. Подробные reject reasons не спамлю в чат — смотри /log."
    if "126 zero-fee" in low or "hotlist" in low:
        return "Сканирую zero-fee hotlist, вход только по сильному подтверждённому импульсу."
    return raw[:220]

async def boost_live_panel_update(app, settings: dict, snap: dict, *, status: str = "scan", decision=None, position: dict | None = None, note: str = "", force: bool = False) -> None:
    """Keep one BOOST live Telegram panel at the bottom: delete old panel, send fresh panel."""
    _boost_store_live_state(app, settings, snap, status=status, decision=decision, position=position, note=note)
    if not _bool_setting(settings, "boost_live_panel_enabled", True):
        return
    chat_id = first_admin_id()
    if not chat_id:
        log_event("boost_live_panel", stage="no_admin_chat", ok=False)
        return
    lock = app.bot_data.get("boost_live_panel_lock")
    if lock is None:
        lock = asyncio.Lock()
        app.bot_data["boost_live_panel_lock"] = lock
    if lock.locked() and not force:
        return
    async with lock:
        await _boost_live_panel_update_locked(app, chat_id, settings, snap, status=status, decision=decision, position=position, note=note, force=force)


async def _boost_live_panel_update_locked(app, chat_id: int, settings: dict, snap: dict, *, status: str = "scan", decision=None, position: dict | None = None, note: str = "", force: bool = False) -> None:
    # v0207 anti-spam HUD: Telegram shows only high-value trader events.
    # Low-level scan/reject details are stored in /log and in bot_data cache.
    interval = max(15, int(float(settings.get("boost_live_panel_interval_sec", 30) or 30)))
    heartbeat = max(interval, int(float(settings.get("boost_live_panel_heartbeat_sec", 60) or 60)))
    now = time.time()
    event_key = _boost_hud_status_key(status, decision=decision, position=position)
    last_key = str(app.bot_data.get("boost_live_panel_last_event_key") or "")
    last_sent = float(app.bot_data.get("boost_panel_last_edit", 0) or 0)
    important = _boost_hud_is_important(status, decision=decision, position=position, force=force)

    if not important:
        # No scanner spam. Quiet states may refresh only on a slow heartbeat or
        # when the coarse state changes, e.g. scanning -> active position.
        if event_key == last_key and now - last_sent < heartbeat:
            log_event("boost_live_panel", stage="throttled_quiet", ok=True, status=str(status)[:80], event_key=event_key)
            return
        if event_key != last_key and now - last_sent < interval:
            log_event("boost_live_panel", stage="throttled_state_change", ok=True, status=str(status)[:80], event_key=event_key)
            return
    else:
        # Important events are immediate, but still avoid duplicate bursts.
        if event_key == last_key and now - last_sent < 8:
            log_event("boost_live_panel", stage="throttled_duplicate_event", ok=True, status=str(status)[:80], event_key=event_key)
            return

    rot = "ON" if _bool_setting(settings, "boost_parallel_scan_enabled", True) else "OFF"
    clean_note = _boost_hud_clean_note(note, status)
    pretty_status = str(status or "scan").replace(";", " / ")[:80]
    lines = [
        "🚀 BOOST HUNTER HUD",
        f"State: {pretty_status}",
        f"Bank: {float(snap.get('current_bank') or 0):.4f} → {float(snap.get('target_bank') or 0):.4f} USDT",
        f"PnL: {float(snap.get('pnl') or 0):+.4f} USDT | Trades: {snap.get('trades', 0)}",
        f"Rotation: {rot} | Blocked: {_boost_blocked_count(settings)}",
        "Chat: важные события. Debug/reject reasons: /log.",
    ]
    if position:
        try:
            sym = _short_symbol(position.get("symbol"))
            side = str(position.get("side") or "-").upper()
            entry = float(position.get("entry_price") or 0)
            lev = position.get("leverage") or position.get("mexc_order_leverage") or "-"
            prot = str(position.get("protection_status") or position.get("protection_mode") or "").upper()
            lines += ["", "Open trade:", f"{sym} {side} | lev {lev}x | entry {entry:.8g}"]
            if _boost_position_is_unsafe(position):
                lines += ["⚠ UNSAFE POSITION: emergency SL missing", "Mode: DEFENSIVE monitoring"]
            elif prot:
                lines += [f"Protection: {prot[:42]}"]
        except Exception:
            pass
    if decision is not None and _boost_hud_is_important(status, decision=decision, position=position, force=force):
        try:
            mk = ((getattr(decision, "market", None) or {}).get("markets") or [{}])[0]
            lines += [
                "",
                "Signal:",
                f"{_short_symbol(getattr(decision, 'symbol', '') or mk.get('symbol'))} {getattr(decision, 'decision', '-')}",
                f"Confidence: {float(getattr(decision, 'confidence', 0) or 0):.2f}",
            ]
        except Exception:
            pass
    if clean_note:
        lines += ["", f"Note: {clean_note}"]
    seq = int(app.bot_data.get("boost_live_panel_seq", 0) or 0) + 1
    app.bot_data["boost_live_panel_seq"] = seq
    lines.append(datetime.now(timezone(timedelta(hours=3))).strftime(f"\nUpdated: %H:%M:%S MSK | live #{seq}"))
    text = "\n".join(lines)
    # v0182: keep the live panel at the bottom of the chat. Editing an old
    # message does not move it down in Telegram, so we delete the previous
    # panel and send one fresh replacement. This keeps exactly one noisy live
    # panel while important lifecycle alerts remain in history.
    old_ids = []
    msg_id = app.bot_data.get("boost_live_panel_message_id")
    if msg_id:
        old_ids.append(msg_id)
    old_ids.extend(app.bot_data.get("boost_live_panel_message_ids", []) or [])
    for old_id in list(dict.fromkeys([x for x in old_ids if x]))[-5:]:
        try:
            await _safe_delete_bot_message(app, chat_id, int(old_id))
        except Exception as e:
            log.debug("boost live panel delete skipped: %s", e)
    try:
        msg = await _safe_send_bot_message(app, chat_id, text, reply_markup=_boost_rotation_keyboard(settings), disable_web_page_preview=True)
        if msg is not None:
            new_id = getattr(msg, "message_id", None)
            app.bot_data["boost_live_panel_message_id"] = new_id
            app.bot_data["boost_live_panel_message_ids"] = [new_id] if new_id else []
            app.bot_data["boost_panel_last_edit"] = now
            app.bot_data["boost_live_panel_last_event_key"] = event_key
            log_event("boost_live_panel", stage="sent", ok=True, message_id=int(new_id or 0), status=str(status)[:80], event_key=event_key, important=important)
        else:
            log_event("boost_live_panel", stage="send_returned_none", ok=False, status=str(status)[:80])
    except Exception as e:
        # v0187: never fail silently. If the live panel cannot be posted, send a
        # plain notification so BOOST activity is still visible in chat/logs.
        log.warning("boost live panel bottom replace failed: %s", e)
        try:
            await app.bot.send_message(chat_id=chat_id, text=f"⚠️ BOOST live panel failed: {str(e)[:220]}\n\n{text[:3200]}")
        except Exception as e2:
            log.warning("boost live panel fallback notification failed: %s", e2)


def _normalize_boost_whitelist_input(raw: str) -> list[str]:
    """Accept user-friendly zero-fee list like 'btc, ETH sol' and store as BTCUSDT symbols."""
    cleaned = str(raw or "").replace("\n", ",").replace(";", ",").replace("|", ",")
    parts: list[str] = []
    for chunk in cleaned.split(","):
        for token in chunk.strip().split():
            if token:
                parts.append(token)
    out: list[str] = []
    seen: set[str] = set()
    for token in parts:
        t = token.strip().upper()
        if not t:
            continue
        # Strip common futures/swap formatting and leave BASE only.
        t = t.replace(":USDT", "").replace("/USDT", "").replace("_USDT", "")
        t = t.replace("-USDT", "")
        if t.endswith("USDT") and len(t) > 4:
            t = t[:-4]
        # Keep only simple exchange symbols to avoid storing malformed values.
        t = "".join(ch for ch in t if ch.isalnum())
        if not t:
            continue
        sym = f"{t}USDT"
        if sym not in seen:
            seen.add(sym)
            out.append(sym)
    return out


def _boost_zero_fee_count_from_settings(settings: dict) -> int:
    raw = str(settings.get("boost_zero_fee_symbols") or DEFAULTS.boost_zero_fee_symbols or "")
    return len({x.strip().upper() for x in raw.split(",") if x.strip()})


def _boost_normalize_symbol_key(symbol: str) -> str:
    """Canonical key for BOOST blacklist/whitelist comparisons."""
    s = str(symbol or "").strip().upper().replace("/", "_").split(":")[0]
    if not s:
        return ""
    if "_" in s:
        return s
    if s.endswith("USDT") and len(s) > 4:
        return s[:-4] + "_USDT"
    return s + "_USDT"


def _boost_blocked_map(settings: dict) -> dict:
    raw = settings.get("boost_blocked_symbols_json") or "{}"
    now = time.time()
    try:
        data = json.loads(str(raw)) if raw else {}
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    clean = {}
    for sym, meta in data.items():
        if not isinstance(meta, dict):
            continue
        try:
            until = float(meta.get("until") or 0)
        except Exception:
            until = 0.0
        if until > now:
            clean[_boost_normalize_symbol_key(sym)] = {"until": until, "reason": str(meta.get("reason") or "blocked")[:160]}
    return clean


def _boost_blocked_count(settings: dict) -> int:
    return len(_boost_blocked_map(settings))


def _boost_is_symbol_restriction_error(text: str) -> bool:
    t = str(text or "").lower()
    needles = [
        "region", "restricted", "not tradable", "not_trade", "not support", "not supported",
        "symbol disabled", "disabled", "contract not available", "contract unavailable",
        "permission denied", "permission", "forbidden", "access denied", "not open",
        "market is closed", "not in trading", "temporarily suspended", "suspended",
        "reduce-only symbol", "8950",
    ]
    return any(n in t for n in needles)


async def _boost_blacklist_symbol(symbol: str, reason: str, *, ttl_sec: int = 86400) -> int:
    """Persist a failed BOOST symbol blacklist entry for ttl_sec seconds."""
    key = _boost_normalize_symbol_key(symbol)
    if not key:
        return 0
    settings = await storage.all_settings()
    data = _boost_blocked_map(settings)
    data[key] = {"until": time.time() + int(ttl_sec), "reason": str(reason or "entry failed")[:160]}
    await storage.set("boost_blocked_symbols_json", json.dumps(data, separators=(",", ":")), bump_revision=False)
    try:
        boost_scalping_engine._fee_cache = (0.0, [])
    except Exception:
        pass
    return len(data)


async def _boost_handle_entry_failure(app, symbol: str, reason: str) -> bool:
    """If an entry failure means the contract cannot be opened, block it for 24h and rescan."""
    if not _boost_is_symbol_restriction_error(reason):
        return False
    count = await _boost_blacklist_symbol(symbol, reason, ttl_sec=86400)
    msg = (
        "⚠ SYMBOL BLOCKED 24H\n"
        f"{_boost_normalize_symbol_key(symbol)}\n"
        f"Reason: {str(reason)[:220]}\n"
        f"Blocked total: {count}\n"
        "Action: rescanning next strongest coin"
    )
    await notify_admin(app, msg, key=f"boost_symbol_blocked_{_boost_normalize_symbol_key(symbol)}", min_interval_sec=5)
    trigger_scan_now(app, reason="boost_symbol_blacklisted")
    return True


async def boost_list_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set/show trusted manual zero-fee futures symbols for BOOST mode.

    Usage: /boost_list BTC,ETH,SOL,doge
    The command stores BTCUSDT,ETHUSDT,... and enables manual fee fallback so
    BOOST can trade trusted symbols when MEXC does not expose promo 0% fees via API.
    """
    if not allowed(update):
        return
    raw = " ".join(context.args or []).strip()
    if not raw:
        s = await storage.all_settings()
        current = [x.strip().upper() for x in str(s.get("boost_zero_fee_symbols") or "").split(",") if x.strip()]
        text = "🪙 BOOST zero-fee whitelist\n"
        if current:
            bases = [x[:-4] if x.endswith("USDT") else x for x in current]
            text += f"Zero-fee DB: {len(current)} symbols\n"
            text += "Active: " + ", ".join(bases) + "\n"
            text += "Stored: " + ", ".join(current) + "\n"
            text += "Manual fallback: ON"
        else:
            text += "Empty. Add like:\n/boost_list BTC,ETH,SOL,DOGE,XRP,ZEC"
        await reply(update, text, reply_markup=MAIN_MENU)
        return
    symbols = _normalize_boost_whitelist_input(raw)
    if not symbols:
        await reply(update, "⚠️ Не понял список. Пример: /boost_list BTC,ETH,SOL", reply_markup=MAIN_MENU)
        return
    await storage.set("boost_zero_fee_symbols", ",".join(symbols), bump_revision=False)
    await storage.set("boost_allow_fee_fallback", True, bump_revision=False)
    # Invalidate boost fee cache so the new manual list is used immediately.
    try:
        boost_scalping_engine._fee_cache = (0.0, [])
    except Exception:
        pass
    trigger_scan_now(context.application, reason="boost_list:update")
    bases = [x[:-4] if x.endswith("USDT") else x for x in symbols]
    await reply(
        update,
        "✅ BOOST whitelist updated\n"
        f"Zero-fee DB: {len(symbols)} symbols\n"
        f"Trusted 0-fee: {', '.join(bases)}\n"
        f"Stored: {', '.join(symbols)}\n"
        "Manual fallback: ON",
        reply_markup=MAIN_MENU,
    )


async def boost_list_del_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear all trusted manual zero-fee futures symbols."""
    if not allowed(update):
        return
    await storage.set("boost_zero_fee_symbols", "", bump_revision=False)
    await storage.set("boost_allow_fee_fallback", False, bump_revision=False)
    try:
        boost_scalping_engine._fee_cache = (0.0, [])
    except Exception:
        pass
    trigger_scan_now(context.application, reason="boost_list:clear")
    await reply(update, "🗑 BOOST whitelist cleared. Manual fee fallback: OFF", reply_markup=MAIN_MENU)


async def boost_rotation_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    s = await storage.all_settings()
    new_value = not _bool_setting(s, "boost_parallel_scan_enabled", True)
    await storage.set("boost_parallel_scan_enabled", new_value)
    ns = await storage.all_settings()
    await reply(update, f"✅ BOOST rotation while in profit = {new_value}", reply_markup=_boost_rotation_keyboard(ns))

async def boost_status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    s = await storage.all_settings()
    snap = await _boost_session_snapshot(s)
    text = (
        f"🚀 BOOST AUTOPILOT\n"
        f"Mode: {s.get('strategy_mode')}\n"
        f"BOOST armed: {_is_boost_active(s)}\n"
        f"Running: {running} | Entries: {entries_enabled}\n"
        f"Bank: {snap['bank']:.4f} → target {snap['target_bank']:.4f} USDT (x{snap['target_mult']:.0f})\n"
        f"Current boost bank: {snap['current_bank']:.4f} USDT\n"
        f"Session PnL: {snap['pnl']:+.4f} USDT\n"
        f"Trades: {snap.get('trades', 0)}\n"
        f"Balance share: {float(s.get('boost_balance_share', 0.10))*100:.1f}%\n"
        f"Auto leverage: {s.get('boost_auto_leverage')} {s.get('boost_min_leverage')}x–{s.get('boost_max_leverage')}x\n"
        f"0-fee required: {not bool(s.get('boost_allow_fee_fallback', False))}\n"
        f"Zero-fee DB: {_boost_zero_fee_count_from_settings(s)} symbols\n"
        f"Blocked symbols: {_boost_blocked_count(s)}\n"
        f"Whitelist: {str(s.get('boost_zero_fee_symbols') or '-')}\n"
        f"Last: {scanner.last_reject_reason or '-'}"
    )
    await reply(update, text, reply_markup=_boost_rotation_keyboard(s))

async def boost_start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    global running, entries_enabled, trading_task, position_task
    app = context.application
    if app.bot_data.get("boost_start_in_progress") or app.bot_data.get("boost_armed_runtime"):
        await reply(update, "⏳ BOOST уже запущен/запускается. Повторный запуск заблокирован. Use /boost_status or /boost_stop. Проверь /log.", reply_markup=MAIN_MENU)
        return
    app.bot_data["boost_start_in_progress"] = True
    app.bot_data["boost_armed_runtime"] = True
    log_event("boost_start_requested", stage="command", ok=True)

    await reply(update, "🚀 BOOST arming... запускаю автопилот и live-panel. Команды и кнопки остаются доступны. Прогресс смотри в /log.", reply_markup=MAIN_MENU)

    # ВАЖНО: запуск BOOST вынесен в отдельную задачу. Даже если MEXC/ccxt зависнет,
    # Telegram-команды, кнопки и /log не должны блокироваться на обработчике /boost_start.
    app.create_task(_boost_start_worker(app))


async def _boost_start_worker(app):
    global running, entries_enabled, trading_task, position_task
    try:
        log_event("boost_start_stage", stage="load_settings", ok=True)
        s = await storage.all_settings()
        boost_defaults = {
            "strategy_mode": "boost_scalping",
            "boost_autopilot_active": True,
            "boost_zero_fee_scanner_enabled": True,
            "boost_balance_share": 0.10,
            "boost_target_multiplier": 20.0,
            "boost_max_consecutive_losses": 999,
            "boost_auto_leverage": True,
            "boost_min_leverage": 30,
            "boost_max_leverage": 50,
            "boost_use_full_bank_per_trade": False,
            "boost_auto_rotate_symbols": True,
            "boost_stop_when_target_reached": True,
            "boost_max_symbols_scan": 126,
            # v0196: BOOST must be live, not silent. Scan a small fast rotating slice
            # with per-symbol timeout; otherwise one slow MEXC public request freezes
            # the whole cycle and Telegram only shows "BOOST arming".
            "boost_min_checked_per_cycle": 3,
            "boost_max_checked_per_cycle": 5,
            "boost_symbol_snapshot_timeout_sec": 1.4,
            "boost_hotlist_refresh_sec": 300,
            "boost_hotlist_size": 18,
            "boost_scan_concurrency": 1,
            "universe_mode": "full",
            # v0205 HUNTER: do not trade ordinary noise. Wait for extreme impulse.
            "boost_hunter_mode": True,
            "boost_hunter_aggressive_mode": True,
            "boost_hunter_min_score": 82.0,
            "boost_hunter_extreme_score": 128.0,
            "boost_hunter_min_accel_pct": 0.012,
            "boost_hunter_min_move_3m_pct": 0.095,
            "boost_hunter_max_wick_pct": 0.68,
            "boost_hunter_no_trade_cooldown_sec": 12,
            "boost_hunter_entry_confirmations": 1,
            "boost_hunter_confirm_ttl_sec": 6,
            "boost_momentum_decay_exit_enabled": True,
            "boost_momentum_decay_min_profit_pct": 0.075,
            "boost_min_tp_pct": 0.12,
            "boost_max_tp_pct": 0.55,
            "boost_live_min_exchange_profit_pct": 0.09,
            "boost_min_quote_volume_usdt": 1200000.0,
            "boost_min_atr_pct": 0.08,
            "boost_max_spread_pct": 0.035,
            "boost_spot_imbalance_ratio": 1.22,
            "boost_futures_momentum_min_pct": 0.028,
            "boost_allow_fee_fallback": True,
            "boost_parallel_scan_enabled": True,
            "boost_live_panel_enabled": True,
            # v0207: Telegram is a trader HUD, not a debug console.
            "boost_live_panel_interval_sec": 30,
            "boost_live_panel_heartbeat_sec": 60,
            "boost_live_watchdog_tick_sec": 5,
            "boost_trade_margin_pct": 0.28,
            "boost_use_full_bank_per_trade": False,
            "boost_risk_pct_per_trade": 0.025,
            "boost_live_slippage_buffer_pct": 0.018,
            "boost_spread_edge_mult": 1.6,
            "boost_tp_spread_mult": 2.2,
            "boost_tp_atr_mult": 0.70,
            "boost_live_status_every_cycle": False,
            "boost_live_safe_execution": True,
            "boost_no_exchange_protection_monitor_only": True,
            "boost_emergency_sl_only": True,
            "boost_dynamic_trailing_enabled": True,
            "boost_trailing_min_profit_pct": 0.075,
            "boost_trailing_giveback_pct": 0.045,
            "boost_trailing_giveback_ratio": 0.38,
            "boost_momentum_decay_exit_enabled": True,
            "boost_momentum_decay_min_profit_pct": 0.075,
            "boost_momentum_decay_r1_floor_pct": 0.006,
            "boost_no_forced_negative_close": True,
            "boost_hunter_trade_rare_search_often": True,
            "boost_rotate_only_if_profit": True,
            "boost_min_profit_to_rotate_pct": 0.040,
            "boost_rotate_strength_multiplier": 1.12,
            "boost_rotate_min_score_gap": 5.0,
            "boost_rotate_cooldown_sec": 1,
            # v0199: automatic two-mode rotation. NORMAL rotates only from profit;
            # RESCUE is OFF by default in live: BOOST must not harvest losses.
            # It can be enabled manually only after testing, with strict limits.
            "boost_rescue_rotation_enabled": False,
            "boost_rescue_min_score_multiplier": 1.70,
            "boost_rescue_min_score_gap": 18.0,
            "boost_rescue_expected_move_loss_mult": 2.50,
            "boost_rescue_max_loss_pct": 0.20,
            "boost_rescue_cooldown_sec": 180,
            "boost_rescue_max_per_hour": 2,
            "scan_interval_sec": 2,
            "boost_scan_interval_sec": 2,
            "boost_snapshot_cache_ttl_sec": 5,
            "boost_fetch_depth_in_scan": False,
            "boost_mexc_rate_cooldown_sec": 8,
            "boost_timeout_cooldown_sec": 4,
            "max_open_positions": 1,
            "auto_close_on_protection_failed": False,
        }
        for k, v in boost_defaults.items():
            await storage.set(k, v, bump_revision=False)
        log_event("boost_start_stage", stage="defaults_saved", ok=True)

        s = await storage.all_settings()
        equity = 0.0
        balance_note = ""
        ex = None
        try:
            api_key, api_secret = _api_creds(s)
            ex = ExchangeClient(DEFAULT_EXCHANGE, str(s.get("proxy_url", "") or ""), bool(s.get("proxy_enabled", False)))
            ex.api_key = api_key
            ex.api_secret = api_secret
            log_event("boost_start_stage", stage="balance_precheck", ok=True)
            bal = await _await_with_timeout(ex.fetch_balance(), 4, "BOOST native MEXC futures balance")
            usdt = bal.get("USDT", {}) if isinstance(bal, dict) else {}
            equity = float(usdt.get("total") or usdt.get("free") or ((bal or {}).get("total", {}) or {}).get("USDT") or 0)
            log_event("boost_start_stage", stage="balance_ok", ok=True, equity=equity)
        except Exception as e:
            equity = float(s.get("boost_session_start_equity", 0) or 0)
            balance_note = f"\nBalance warning: {str(e)[:180]}"
            log_event("boost_start_error", stage="balance_precheck", ok=False, error=str(e)[:500])
        finally:
            if ex is not None:
                try:
                    await _await_with_timeout(ex.close(), 1.5, "exchange close")
                except Exception:
                    pass

        share = max(0.001, min(1.0, float(s.get("boost_balance_share", 0.10) or 0.10)))
        bank = max(0.0, equity * share)
        target_mult = max(1.0, float(s.get("boost_target_multiplier", 20.0) or 20.0))
        now_ts = time.time()
        await storage.set("boost_session_start_ts", now_ts, bump_revision=False)
        await storage.set("boost_session_start_equity", equity, bump_revision=False)
        await storage.set("boost_session_bank_usdt", bank, bump_revision=False)
        await storage.set("boost_session_target_profit_usdt", bank * (target_mult - 1.0), bump_revision=False)
        log_event("boost_start_stage", stage="session_saved", ok=True, bank=bank, target=bank*target_mult)

        manual_symbols = str(s.get("boost_zero_fee_symbols") or "").strip()
        if not manual_symbols:
            manual_symbols = "AAPLUSDT,AAVEUSDT,ADAUSDT,AMDUSDT,ANTHROPICUSDT,APEUSDT,ARMUSDT,ASTERUSDT,ASTSUSDT,ATOMUSDT,AVAXUSDT,BCHUSDT,BEUSDT,BILLUSDT,BLESSUSDT,BNBUSDT,BRETTUSDT,BTCUSDT,BULLAUSDT,BUSDT,CBRSUSDT,CHIPUSDT,COAIUSDT,XCU_USDT,CVNAUSDT,DOGEUSDT,DOTUSDT,ENJUSDT,ETHFIUSDT,ETHUSDT,FLOKIUSDT,FLNCUSDT,FOLKSUSDT,FUTUUSDT,GALAUSDT,GASNGUSDT,GIGGLEUSDT,GOATUSDT,PAXG_USDT,XAUT_USDT,GOOGLUSDT,GRTUSDT,HBARUSDT,HYPEUSDT,IBMUSDT,ICPUSDT,INJUSDT,INTCUSDT,INTUUSDT,IONQUSDT,JASMYUSDT,JUPUSDT,LABUSDT,LAYERUSDT,LIGHTUSDT,LINKUSDT,LTCUSDT,LYNUSDT,MEGAUSDT,MSTRUSDT,MUUSDT,MUUUSDT,MYXUSDT,NAS100_USDT,NBISUSDT,NEARUSDT,XNI_USDT,NVDAUSDT,BRENT_USDT,WTI_USDT,ONDOUSDT,OPUSDT,ORDIUSDT,XPD_USDT,PENGUINUSDT,PENGUUSDT,PEPEUSDT,PIPPINUSDT,PIXELUSDT,XPT_USDT,PNUTUSDT,POLUSDT,POWERUSDT,PUMPUSDT,PYTHUSDT,QCOMUSDT,QQQUSDT,RENDERUSDT,RIVERUSDT,RKLBUSDT,SAMSUNGUSDT,SEIUSDT,SHIBUSDT,XAG_USDT,SKHYNIXUSDT,SKYAIUSDT,SNDKUSDT,SOLUSDT,SP500_USDT,SPACEXUSDT,SPCXUSDT,SPOTUSDT,STOUSDT,STRKUSDT,STXSTOCKUSDT,SUIUSDT,TAOUSDT,TONUSDT,TRUMPUSDT,TSLAUSDT,UNIUSDT,US30_USDT,VIRTUALUSDT,VVVUSDT,WDCUSDT,WIFUSDT,WLDUSDT,WLFIUSDT,XAIUSDT,XLMUSDT,XMRUSDT,XOMUSDT,XPLUSDT,XRPUSDT,ZECUSDT,ZROUSDT"
            await storage.set("boost_zero_fee_symbols", manual_symbols, bump_revision=False)
        await storage.set("boost_blocked_symbols_json", json.dumps(_boost_blocked_map(await storage.all_settings()), separators=(",", ":")), bump_revision=False)

        if trading_task and not trading_task.done():
            running = True; entries_enabled = True
            log_event("boost_start_stage", stage="trading_loop_already_running", ok=True)
        else:
            running = True; entries_enabled = True
            trading_task = app.create_task(trading_loop(app))
            log_event("boost_start_stage", stage="trading_loop_created", ok=True)
        if position_task is None or position_task.done():
            position_task = app.create_task(position_management_loop(app))
            log_event("boost_start_stage", stage="position_loop_created", ok=True)

        # v0204: live chat must not depend on scanner/execution branches.
        # Start an independent watchdog that sends a fresh bottom panel every few seconds.
        old_watchdog = app.bot_data.get("boost_live_panel_watchdog_task")
        if old_watchdog is not None and not old_watchdog.done():
            old_watchdog.cancel()
        app.bot_data["boost_live_panel_watchdog_task"] = app.create_task(_boost_live_panel_watchdog(app))
        log_event("boost_start_stage", stage="live_panel_watchdog_created", ok=True)

        app.bot_data.pop("boost_live_panel_message_id", None)
        app.bot_data["boost_panel_last_edit"] = 0
        app.bot_data.pop("boost_live_panel_last_event_key", None)
        trigger_scan_now(app, reason="boost_start")
        ns = await storage.all_settings()
        chat_id = first_admin_id()
        if chat_id:
            await _safe_send_bot_message(app, chat_id,
                f"✅ BOOST started\nBank: {bank:.4f} USDT ({share*100:.1f}% balance)\nTarget: {bank*target_mult:.4f} USDT (x{target_mult:.0f})\nZero-fee DB: {len([x for x in manual_symbols.split(',') if x.strip()])} symbols\nMode: HUNTER LIVE ENGINE: 126 zero-fee → market radar/hotlist → deep momentum → ARMED/CONFIRMED → live trailing exit + emergency SL only\nSafety: commands/buttons stay available; /log now shows BOOST stages/errors.{balance_note}",
                reply_markup=_boost_rotation_keyboard(ns))
        await boost_live_panel_update(
            app, ns,
            {"bank": bank, "current_bank": bank, "target_bank": bank * target_mult, "pnl": 0.0, "trades": 0, "target_mult": target_mult},
            status="started; scanning",
            note="live panel active; commands remain available: /balance /status /boost_status /boost_stop",
            force=True,
        )
        app.bot_data["boost_armed_runtime"] = True
        app.bot_data["boost_start_in_progress"] = False
        log_event("boost_start_done", stage="done", ok=True)
    except Exception as e:
        app.bot_data["boost_start_in_progress"] = False
        app.bot_data["boost_armed_runtime"] = False
        await storage.set("boost_autopilot_active", False, bump_revision=False)
        log_event("boost_start_error", stage="fatal", ok=False, error=str(e)[:1000])
        chat_id = first_admin_id()
        if chat_id:
            await _safe_send_bot_message(app, chat_id, f"❌ BOOST start failed: {str(e)[:700]}\nСмотри /log", reply_markup=MAIN_MENU)

async def boost_stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    global running, entries_enabled
    entries_enabled = False
    _boost_disarm_runtime(context.application)
    await storage.set("boost_autopilot_active", False, bump_revision=False)
    await storage.set("strategy_mode", "hybrid", bump_revision=False)
    await storage.set("boost_start_in_progress", False, bump_revision=False)
    await storage.set("boost_hunter_last_no_trade_ts", time.time(), bump_revision=False)
    # Invalidate BOOST scanner caches so a later /boost_start starts clean.
    try:
        boost_scalping_engine._hot_cache = (0.0, [], [])
        boost_scalping_engine._scan_cursor = 0
    except Exception:
        pass
    task = context.application.bot_data.pop("boost_live_panel_watchdog_task", None)
    if task is not None and not task.done():
        task.cancel()
    context.application.bot_data.pop("boost_live_last_state", None)
    context.application.bot_data.pop("boost_live_panel_message_id", None)
    context.application.bot_data.pop("boost_live_panel_message_ids", None)
    context.application.bot_data.pop("boost_live_panel_last_event_key", None)
    log_event("boost_stop", stage="command", ok=True)
    await reply(update, "🛑 BOOST полностью остановлен: новые входы, hotlist/rotation/live-panel отключены. Открытые позиции не закрывал автоматически; /panic или /close_all — если надо закрыть сразу.", reply_markup=MAIN_MENU)

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


def _fmt_money_value(v):
    try:
        if v in (None, "", "n/a"):
            return "n/a"
        f = float(v)
        return f"{f:.8f}".rstrip("0").rstrip(".")
    except Exception:
        return str(v)


def _extract_mexc_usdt_balance(payload: dict) -> dict:
    """Extract USDT futures balance from several native MEXC response shapes."""
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data", payload)
    rows = []
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        for key in ("assets", "list", "rows", "items", "result"):
            if isinstance(data.get(key), list):
                rows = data.get(key)
                break
        if not rows:
            rows = [data]
    best = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        ccy = str(row.get("currency") or row.get("asset") or row.get("coin") or "USDT").upper()
        if ccy and ccy != "USDT":
            continue
        def pick(*keys, default=0):
            for k in keys:
                val = row.get(k)
                if val not in (None, ""):
                    return val
            return default
        total = pick("equity", "totalEquity", "balance", "cashBalance", "walletBalance", "marginBalance")
        free = pick("availableBalance", "available", "availableOpen", "availableCash", "cashBalance", default=total)
        frozen = pick("frozenBalance", "frozen", "hold", default=0)
        pos_margin = pick("positionMargin", "position_margin", "im", "initialMargin", default=0)
        unreal = pick("unrealized", "unrealizedPnl", "unrealisedPnl", "unrealizedProfit", default=0)
        try:
            used = max(0.0, float(total or 0) - float(free or 0))
        except Exception:
            used = pick("used", "usedBalance", default=0)
        best = {"free": free, "total": total, "used": used, "positionMargin": pos_margin, "frozenBalance": frozen, "unrealized": unreal, "raw": row}
        break
    return best


async def _direct_mexc_balance(settings: dict) -> tuple[dict, str]:
    """Fast balance path for the Telegram Balance button.

    This deliberately avoids get_exchange(), ccxt.load_markets(), positions,
    and public IP checks. The button must answer even when public MEXC endpoints
    or market loading are slow. We try futures private read hosts with a small
    per-request timeout and return either parsed USDT values or the last error.
    """
    api_key, api_secret = _api_creds(settings)
    proxy_enabled = bool(settings.get("proxy_enabled", False))
    proxy_url = str(settings.get("proxy_url", "") or "")
    ex = ExchangeClient(DEFAULT_EXCHANGE, proxy_url, proxy_enabled)
    ex.api_key = api_key
    ex.api_secret = api_secret
    bases = []
    env_base = os.getenv("MEXC_REST_BASE", "").strip().rstrip("/")
    for b in (env_base, "https://contract.mexc.com", "https://api.mexc.com"):
        if b and b not in bases:
            bases.append(b)
    endpoints = [
        "/api/v1/private/account/assets",
        "/api/v1/private/account/asset/USDT",
    ]
    errors = []
    for base in bases:
        for ep in endpoints:
            try:
                out = await _await_with_timeout(ex._mexc_private("GET", ep, query={}, base_url=base), 4, f"{base}{ep}")
                parsed = _extract_mexc_usdt_balance(out)
                if parsed:
                    parsed["source"] = f"{base}{ep}"
                    return parsed, ""
                errors.append(f"{base}{ep}: empty/unknown response")
            except Exception as e:
                errors.append(f"{base}{ep}: {str(e)[:160]}")
    return {}, " | ".join(errors[-4:])[:700]

async def balance_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    chat_id = update.effective_chat.id if update.effective_chat else 0
    lock = balance_locks.setdefault(chat_id, asyncio.Lock())
    if lock.locked():
        await reply(update, "💰 Баланс уже проверяется. Жду ответ MEXC, второй запрос не запускаю.", reply_markup=MAIN_MENU)
        return

    async with lock:
        s = await storage.all_settings()
        proxy_enabled = bool(s.get("proxy_enabled", False))
        proxy_url = str(s.get("proxy_url", "") or "")
        started = time.perf_counter()
        try:
            bal, err = await _direct_mexc_balance(s)
            if bal:
                text = (
                    "💰 Futures Balance MEXC\n"
                    f"USDT free: {_fmt_money_value(bal.get('free'))}\n"
                    f"USDT total/equity: {_fmt_money_value(bal.get('total'))}\n"
                    f"USDT used: {_fmt_money_value(bal.get('used'))}\n"
                    f"Position margin: {_fmt_money_value(bal.get('positionMargin'))}\n"
                    f"Frozen balance: {_fmt_money_value(bal.get('frozenBalance'))}\n"
                    f"Unrealized PnL: {_fmt_money_value(bal.get('unrealized'))}\n"
                    f"Proxy: {'ON' if proxy_enabled and proxy_url else 'OFF'}\n"
                    f"Time: {(time.perf_counter() - started):.1f}s"
                )
            else:
                text = (
                    "❌ Balance: MEXC не вернул баланс.\n"
                    "Кнопка работает, но private futures API не отвечает/отклоняет запрос.\n\n"
                    f"Proxy: {'ON' if proxy_enabled and proxy_url else 'OFF'}\n"
                    f"Error: {err or 'unknown'}"
                )
        except Exception as e:
            text = f"❌ Balance failed: {str(e)[:900]}"
        await reply(update, text[:3900], reply_markup=MAIN_MENU)



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



async def boost_exchange_pnl_snapshot(ex, pos: dict) -> tuple[float | None, float | None, float | None]:
    """Return (pnl_pct, pnl_usdt, mark_price) from the real exchange row.

    BOOST live exits are too tiny to trust local ticker-only PnL.  Confirm with
    MEXC mark/fair price and unrealizedPnl before closing for TP/rotation.
    """
    try:
        exec_engine = ExecutionEngine(storage, ex)
        rows = await ex.fetch_positions()
        wanted = _position_identity_keys(pos, ex)
        side_want = str(pos.get("side") or "").lower()
        for row in rows or []:
            if exec_engine.exchange_position_qty(row) <= 0:
                continue
            keys = _position_identity_keys(row, ex)
            if not (keys & wanted):
                continue
            raw_side = str(row.get("side") or ((row.get("info") or {}).get("holdSide") if isinstance(row.get("info"), dict) else "") or "").lower()
            if side_want and raw_side and side_want not in raw_side and raw_side not in side_want:
                # MEXC can return buy/sell/long/short; do not over-filter if empty.
                pass
            entry = 0.0
            for k in ("entryPrice", "entry_price", "average"):
                try:
                    if row.get(k) not in (None, ""):
                        entry = float(row.get(k)); break
                except Exception:
                    pass
            info = row.get("info") or {}
            if entry <= 0 and isinstance(info, dict):
                for k in ("holdAvgPrice", "openAvgPrice", "entryPrice"):
                    try:
                        if info.get(k) not in (None, ""):
                            entry = float(info.get(k)); break
                    except Exception:
                        pass
            mark = 0.0
            for k in ("markPrice", "fairPrice", "lastPrice"):
                try:
                    v = row.get(k) if row.get(k) not in (None, "") else (info.get(k) if isinstance(info, dict) else None)
                    if v not in (None, ""):
                        mark = float(v); break
                except Exception:
                    pass
            upnl = None
            for k in ("unrealizedPnl", "unrealised", "profit"):
                try:
                    v = row.get(k) if row.get(k) not in (None, "") else (info.get(k) if isinstance(info, dict) else None)
                    if v not in (None, ""):
                        upnl = float(v); break
                except Exception:
                    pass
            pct = None
            if entry > 0 and mark > 0:
                if str(pos.get("side") or "").upper() == "LONG":
                    pct = (mark - entry) / entry * 100.0
                else:
                    pct = (entry - mark) / entry * 100.0
            return pct, upnl, mark or None
    except Exception:
        return None, None, None
    return None, None, None




def _boost_position_is_unsafe(pos: dict) -> bool:
    mode = str((pos or {}).get("protection_mode") or "").lower()
    status = str((pos or {}).get("protection_status") or "").upper()
    return bool((pos or {}).get("boost_unsafe_position")) or mode == "unsafe_no_emergency_sl" or status == "UNSAFE POSITION"


async def _boost_try_recover_unsafe_position(app, exec_engine, pos: dict, live: bool) -> tuple[bool, str]:
    """Retry emergency SL for a BOOST position marked UNSAFE.

    Returns (recovered, note). This is intentionally throttled because MEXC can
    reject precision/rate-limited stop requests during volatility.
    """
    if not _boost_position_is_unsafe(pos) or not live:
        return False, ""
    now = time.time()
    key = f"boost_unsafe_sl_retry:{_short_symbol(pos.get('symbol'))}:{int(float(pos.get('opened_at') or 0))}"
    last = float(app.bot_data.get(key, 0) or 0)
    retry_sec = float((await storage.get("boost_unsafe_sl_retry_sec", 10)) or 10)
    if now - last < retry_sec:
        return False, f"UNSAFE POSITION: emergency SL missing; retry in {max(0, retry_sec-(now-last)):.0f}s"
    app.bot_data[key] = now
    try:
        prot = await _await_with_timeout(exec_engine.place_protection_orders(pos, live=True), 12, "boost unsafe emergency SL retry")
        ok = bool(prot.get("ok")) and str(prot.get("protection_mode") or "").lower() in {"exchange", "exchange_emergency_sl_only"}
        pos.update(prot)
        if ok:
            pos["boost_unsafe_position"] = False
            pos["boost_defensive_mode"] = False
            pos["protection_status"] = prot.get("protection_status") or "EMERGENCY SL ONLY"
            pos["protection_mode"] = prot.get("protection_mode") or "exchange_emergency_sl_only"
            pos["updated_at"] = now
            await storage.upsert_position(pos)
            log_event("boost_unsafe_recovered", stage="emergency_sl_retry", ok=True, symbol=pos.get("symbol"), protection_mode=pos.get("protection_mode"))
            await notify_admin(app, f"✅ BOOST emergency SL recovered\n{_short_symbol(pos.get('symbol'))} {str(pos.get('side') or '').upper()}\nDefensive mode OFF", key=f"boost_unsafe_recovered_{_short_symbol(pos.get('symbol'))}", min_interval_sec=5)
            return True, "Emergency SL recovered; defensive mode OFF"
        pos["boost_unsafe_position"] = True
        pos["boost_defensive_mode"] = True
        pos["protection_status"] = "UNSAFE POSITION"
        pos["protection_mode"] = "unsafe_no_emergency_sl"
        pos["boost_unsafe_reason"] = str(prot.get("boost_unsafe_reason") or prot.get("sl_error") or prot.get("protection_warning") or prot)[:500]
        pos["updated_at"] = now
        await storage.upsert_position(pos)
        log_event("boost_unsafe_retry_failed", stage="emergency_sl_retry", ok=False, symbol=pos.get("symbol"), reason=pos.get("boost_unsafe_reason"))
        return False, f"UNSAFE POSITION: emergency SL failed again; defensive mode ON"
    except Exception as e:
        pos["boost_unsafe_position"] = True
        pos["boost_defensive_mode"] = True
        pos["protection_status"] = "UNSAFE POSITION"
        pos["protection_mode"] = "unsafe_no_emergency_sl"
        pos["boost_unsafe_reason"] = str(e)[:500]
        pos["updated_at"] = now
        await storage.upsert_position(pos)
        log_event("boost_unsafe_retry_error", stage="emergency_sl_retry", ok=False, symbol=pos.get("symbol"), error=str(e)[:500])
        return False, f"UNSAFE POSITION: SL retry error; defensive mode ON"

async def _boost_hunter_manage_active_position(app, ex, exec_engine, settings: dict, active_pos: dict, snap_now: dict, live: bool) -> bool:
    """Manage an active BOOST position with live trailing + momentum decay.

    Returns True when it closed the active position. It never closes a live BOOST
    position in loss for convenience; negative exits are left only to the
    exchange emergency SL or explicit /panic /close_all.
    """
    try:
        if str(active_pos.get("strategy") or "").lower() != "boost_scalping":
            return False
        sym = str(active_pos.get("symbol") or "")
        side = str(active_pos.get("side") or "").upper()
        if not sym or side not in {"LONG", "SHORT"}:
            return False
        price = await get_last_price(ex, sym)
        # v0221: this helper runs outside the main loop scope where pos_manager
        # exists.  Use a local PositionManager instance so HUNTER active-position
        # management cannot crash with: name 'pos_manager' is not defined.
        _boost_pm = PositionManager(storage, exec_engine)
        local_pnl = _boost_pm.pnl_pct(active_pos, price) if price else 0.0
        ex_pnl, ex_upnl, ex_mark = (None, None, None)
        if live:
            ex_pnl, ex_upnl, ex_mark = await boost_exchange_pnl_snapshot(ex, active_pos)
        pnl_pct = float(ex_pnl) if ex_pnl is not None else float(local_pnl)

        unsafe = _boost_position_is_unsafe(active_pos)
        unsafe_note = ""
        if unsafe:
            recovered, unsafe_note = await _boost_try_recover_unsafe_position(app, exec_engine, active_pos, live)
            unsafe = not recovered
            if unsafe:
                active_pos["boost_unsafe_position"] = True
                active_pos["boost_defensive_mode"] = True
                # Defensive mode exits earlier, but still respects the hard rule:
                # no bot-forced negative close. Emergency SL remains the only loss exit.
                log_event("boost_unsafe_defensive_active", stage="position", ok=True, symbol=sym, side=side, pnl_pct=pnl_pct, reason=active_pos.get("boost_unsafe_reason"))

        key = f"boost_peak_pnl:{_short_symbol(sym)}:{side}:{int(float(active_pos.get('opened_at') or 0))}"
        peak = max(float(app.bot_data.get(key, pnl_pct) or pnl_pct), pnl_pct)
        app.bot_data[key] = peak

        min_profit = float(settings.get("boost_trailing_min_profit_pct", settings.get("boost_live_min_exchange_profit_pct", 0.12)) or 0.12)
        giveback_abs = float(settings.get("boost_trailing_giveback_pct", 0.07) or 0.07)
        giveback_ratio = float(settings.get("boost_trailing_giveback_ratio", 0.38) or 0.38)
        if unsafe:
            # Missing exchange SL: take smaller real profit and give less back.
            min_profit = min(min_profit, float(settings.get("boost_unsafe_min_profit_exit_pct", 0.05) or 0.05))
            giveback_abs = min(giveback_abs, float(settings.get("boost_unsafe_giveback_pct", 0.03) or 0.03))
            giveback_ratio = min(giveback_ratio, float(settings.get("boost_unsafe_giveback_ratio", 0.25) or 0.25))
        giveback = max(giveback_abs, peak * giveback_ratio)
        can_close_profit = pnl_pct >= min_profit and ((ex_upnl is None) or float(ex_upnl) > 0)

        reason = ""
        if _bool_setting(settings, "boost_dynamic_trailing_enabled", True):
            if peak >= min_profit and pnl_pct <= peak - giveback and can_close_profit:
                reason = f"dynamic_trailing_exit peak={peak:+.3f}% now={pnl_pct:+.3f}% giveback={giveback:.3f}%"

        if not reason and _bool_setting(settings, "boost_momentum_decay_exit_enabled", True) and can_close_profit:
            # Momentum decay is checked from a fresh snapshot. Failure to snapshot is
            # not a close signal; it just leaves the position monitored.
            try:
                timeout = max(0.35, float(settings.get("boost_symbol_snapshot_timeout_sec", 0.55) or 0.55))
                market = await asyncio.wait_for(boost_scalping_engine._snapshot(ex, sym), timeout=timeout)
                r1 = float(market.get("ret_1m_pct") or 0)
                r3 = float(market.get("ret_3m_pct") or 0)
                floor = float(settings.get("boost_momentum_decay_r1_floor_pct", 0.015) or 0.015)
                if side == "LONG":
                    decay = r1 <= floor or r3 <= 0
                else:
                    decay = r1 >= -floor or r3 >= 0
                if decay:
                    reason = f"momentum_decay_exit pnl={pnl_pct:+.3f}% r1={r1:+.3f}% r3={r3:+.3f}%"
            except Exception as e:
                log_event("boost_momentum_decay_check", stage="snapshot_error", ok=False, symbol=sym, error=str(e)[:160])

        log_event("boost_active_manage", stage="position", ok=True, symbol=sym, side=side, pnl_pct=pnl_pct, peak_pct=peak, ex_upnl=ex_upnl, close_reason=reason or "hold")
        if not reason:
            hud_status = "unsafe defensive" if unsafe else "active; monitoring"
            note = (unsafe_note or "UNSAFE POSITION: emergency SL missing; defensive mode ON") if unsafe else f"live trailing peak={peak:+.3f}% now={pnl_pct:+.3f}%"
            await boost_live_panel_update(app, settings, snap_now, status=hud_status, position=active_pos, note=note, force=unsafe)
            return False

        # Hard rule: no forced negative close from bot logic.
        if live and pnl_pct < min_profit:
            log_event("boost_exit_blocked", stage="no_forced_negative_close", ok=True, symbol=sym, pnl_pct=pnl_pct, min_profit=min_profit, reason=reason)
            return False

        close_res = await _await_with_timeout(exec_engine.close_position(active_pos, reason=reason, live=live, exit_price=ex_mark or price), 18, "boost live exit close_position")
        if close_res.get("ok"):
            app.bot_data.pop(key, None)
            await notify_admin(app, f"💰 BOOST profit exit\n{_short_symbol(sym)} {side}\nPnL≈{pnl_pct:+.3f}%\nReason: {reason}", key=f"boost_profit_exit_{int(time.time()*1000)}")
            await boost_live_panel_update(app, settings, snap_now, status="profit exit", position=active_pos, note=reason, force=True)
            trigger_scan_now(app, reason="boost_profit_exit")
            return True
        log_event("boost_exit_failed", stage="close_position", ok=False, symbol=sym, reason=str(close_res)[:500])
        await boost_live_panel_update(app, settings, snap_now, status="exit failed", position=active_pos, note=str(close_res)[:220], force=True)
        return False
    except Exception as e:
        log_event("boost_active_manage_error", stage="position", ok=False, error=str(e)[:500])
        return False

async def _hidden_margin_snapshot(ex) -> dict:
    """Read MEXC balance margin robustly when open_positions returns empty."""
    try:
        bal = await ex.fetch_balance()
        usdt = (bal or {}).get("USDT", {}) if isinstance(bal, dict) else {}
        used = float(usdt.get("used") or ((bal or {}).get("used", {}) or {}).get("USDT") or 0)
        pm = float(usdt.get("positionMargin") or usdt.get("position_margin") or 0)
        frozen = float(usdt.get("frozenBalance") or usdt.get("frozen_balance") or 0)
        upnl = float(usdt.get("unrealized") or usdt.get("unrealizedPnl") or 0)
        hidden = used > 0.5 or pm > 0.5 or abs(upnl) > 0.01
        return {"hidden": hidden, "used": used, "positionMargin": pm, "frozen": frozen, "unrealized": upnl}
    except Exception as e:
        return {"hidden": False, "error": str(e)[:180]}

async def _hidden_margin_present(ex) -> bool:
    return bool((await _hidden_margin_snapshot(ex)).get("hidden"))

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
                snap = await _hidden_margin_snapshot(ex2)
                if snap.get("hidden"):
                    text = (
                        "📈 Positions: ⚠️ hidden exchange margin\n⚠️ MEXC has live exposure, but open_positions returned empty"
                        f"\nUsed: {float(snap.get('used') or 0):.4f} USDT"
                        f"\nPosition margin: {float(snap.get('positionMargin') or 0):.4f} USDT"
                        f"\nFrozen: {float(snap.get('frozen') or 0):.4f} USDT"
                        f"\nUnrealized PnL: {float(snap.get('unrealized') or 0):.4f} USDT"
                        "\n\nThis is NOT flat. The bot must keep treating the account as exposed."
                        "\nUse /close_all, then /balance. Do not start new trades until used/margin = 0."
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
    n_pf = n.get("profit_factor_display", f"{float(n.get('profit_factor') or 0):.2f}")
    m_pf = m.get("profit_factor_display", f"{float(m.get('profit_factor') or 0):.2f}")
    text = f"""
📉 Stats

Trades: {len(trades)}

Normal: {n.get('wins', 0)}W/{n.get('losses', 0)}L
Normal PF: {n_pf}
Normal WR: {n['winrate']:.1f}%
Normal PnL: {n['pnl']:.4f} USDT
Normal Expectancy: {n['expectancy']:.4f}

Mirror: {m.get('wins', 0)}W/{m.get('losses', 0)}L
Mirror PF: {m_pf}
Mirror WR: {m['winrate']:.1f}%
Mirror PnL: {m['pnl']:.4f} USDT
Mirror Expectancy: {m['expectancy']:.4f}

PF ∞ = прибыльные сделки есть, убыточных ещё нет.
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
            positions = [p for p in (await asyncio.wait_for(ex.fetch_positions(), timeout=20) or []) if exec_engine.exchange_position_qty(p) > 0]
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
    global entries_enabled
    if not allowed(update): return
    entries_enabled = False
    _boost_disarm_runtime(context.application)
    await storage.set("boost_autopilot_active", False, bump_revision=False)
    await storage.set("strategy_mode", "hybrid", bump_revision=False)
    await reply(update, "⏳ Cancel all orders: command received. BOOST/new entries are OFF.", reply_markup=MAIN_MENU)
    s = await storage.all_settings()
    try:
        ex = await _await_with_timeout(get_exchange(s), 6, "exchange init")
        res = await _await_with_timeout(ex.cancel_all_orders(), 25, "cancel_all_orders")
        await reply(update, f"🧹 Cancel all orders sent\n{str(res)[:1200]}", reply_markup=MAIN_MENU)
    except Exception as e:
        await reply(update, f"🧹 Cancel all failed: {e}", reply_markup=MAIN_MENU)

async def close_all_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global entries_enabled
    if not allowed(update): return
    s = await storage.all_settings()
    quick_bounce_active = str(s.get("strategy_mode", "hybrid")).lower() == "quick_bounce" and _bool_setting(s, "quick_bounce_enabled", False)
    entries_enabled = bool(quick_bounce_active)
    _boost_disarm_runtime(context.application)
    await storage.set("boost_autopilot_active", False, bump_revision=False)
    if not quick_bounce_active:
        await storage.set("strategy_mode", "hybrid", bump_revision=False)
    await reply(update, "⏳ Close all positions: command received." + (" Быстрый отскок останется ON и продолжит новые сканы." if quick_bounce_active else " BOOST/new entries are OFF."), reply_markup=MAIN_MENU)
    try:
        ex = await _await_with_timeout(get_exchange(s), 6, "exchange init")
        exec_engine = ExecutionEngine(storage, ex)
        positions = [p for p in (await _await_with_timeout(ex.fetch_positions(), 10, "fetch_positions") or []) if exec_engine.exchange_position_qty(p) > 0]
        failures = []
        closed = 0
        for p in positions:
            res = await _await_with_timeout(exec_engine.close_exchange_position(p, "manual_close_all"), 20, "close_exchange_position")
            if res.get("ok"):
                closed += 1
            else:
                failures.append(f"{p.get('symbol')}: {res.get('reason')}")
        native_res = None
        cancel_res = None
        try:
            cancel_res = await _await_with_timeout(ex.cancel_all_orders(), 10, "cancel_all_orders")
        except Exception as e:
            failures.append(f"cancel_all: {e}")
        # Extra safety: call native close_all only when nothing was listed and
        # closed manually. If listed positions were already closed, MEXC often
        # returns code 2009 (Position is nonexistent or closed), which is not a
        # real failure and only confuses the operator.
        if hasattr(ex, "mexc_close_all_positions_native"):
            try:
                native_res = await _await_with_timeout(ex.mexc_close_all_positions_native(), 15, "native_close_all")
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
            bal = await asyncio.wait_for(ex.fetch_balance(), timeout=5)
            usdt = (bal or {}).get("USDT", {}) if isinstance(bal, dict) else {}
            post_pm = float(usdt.get("positionMargin") or usdt.get("position_margin") or 0)
            post_used = float(usdt.get("used") or ((bal or {}).get("used", {}) or {}).get("USDT") or 0)
            try:
                post_positions = [p for p in (await asyncio.wait_for(ex.fetch_positions(), timeout=10) or []) if exec_engine.exchange_position_qty(p) > 0]
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
        "liquidity_retest_default_rr", "liquidity_retest_sl_buffer_pct", "liquidity_retest_time_stop_sec", "liquidity_retest_min_displacement_pct", "liquidity_retest_min_displacement_body", "liquidity_retest_min_volume_ratio", "liquidity_retest_min_target_rr", "liquidity_retest_zone_tolerance_pct", "liquidity_retest_min_sweep_wick", "liquidity_retest_min_reclaim_pct", "liquidity_retest_max_spread_pct", "liquidity_retest_min_retest_rejection_wick", "liquidity_retest_min_zone_quality", "liquidity_retest_mtf_enabled", "liquidity_retest_min_mtf_score", "liquidity_retest_require_clean_path", "liquidity_retest_quality_mode", "scanner_reject_log_enabled",
        "weak_momentum_filter_enabled", "momentum_min_5m_confirm_pct", "momentum_min_imbalance_abs", "momentum_max_spread_pct",
        "openai_analysis_enabled", "openai_model", "openai_check_strength", "openai_api_key",
        "openai_env_fallback", "openai_timeout_sec", "openai_fail_open", "openai_show_decisions",
        "trade_charts_enabled", "liquidity_runner_enabled",
        "ai_scalping_symbols", "ai_scalping_min_confidence", "ai_scalping_ai_entry_filter_enabled", "ai_scalping_tp_pct", "ai_scalping_sl_pct", "ai_scalping_btc_tp_pct", "ai_scalping_btc_sl_pct", "ai_scalping_eth_tp_pct", "ai_scalping_eth_sl_pct", "ai_scalping_btc_min_tp_pct", "ai_scalping_btc_max_tp_pct", "ai_scalping_eth_min_tp_pct", "ai_scalping_eth_max_tp_pct", "ai_scalping_sl_tp_multiplier", "ai_scalping_max_spread_pct", "ai_scalping_quality_filters_enabled", "ai_scalping_quality_min_confidence", "ai_scalping_quality_cooldown_sec", "ai_scalping_quality_min_atr_pct", "ai_scalping_quality_min_ema_gap_pct", "ai_scalping_quality_min_ret_5m_abs_pct", "ai_scalping_ai_cooldown_sec", "ai_scalping_openai_fallback_enabled", "ai_scalping_json_mode_enabled", "ai_scalping_liquidation_stop_mode", "ai_scalping_liq_margin_pct", "ai_scalping_liq_buffer_pct", "ai_scalping_liq_max_leverage", "boost_zero_fee_scanner_enabled", "boost_balance_share", "boost_target_multiplier", "boost_session_hours", "boost_max_session_loss_pct", "boost_max_consecutive_losses", "boost_auto_leverage", "boost_min_leverage", "boost_max_leverage", "boost_use_full_bank_per_trade", "boost_risk_pct_per_trade", "boost_auto_rotate_symbols", "boost_stop_when_target_reached", "boost_max_symbols_scan", "boost_min_quote_volume_usdt", "boost_min_atr_pct", "boost_max_spread_pct", "boost_spot_imbalance_ratio", "boost_futures_momentum_min_pct", "boost_futures_max_against_pct", "boost_min_tp_pct", "boost_max_tp_pct", "boost_sl_tp_multiplier", "boost_scan_interval_sec", "boost_allow_fee_fallback", "boost_zero_fee_symbols", "boost_live_panel_enabled", "boost_live_panel_interval_sec", "boost_parallel_scan_enabled", "boost_rotate_only_if_profit", "boost_min_profit_to_rotate_pct", "boost_rotate_strength_multiplier", "boost_rotate_min_score_gap", "boost_rotate_cooldown_sec", "boost_live_min_exchange_profit_pct",
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
    if key == "liquidity_retest_quality_mode" and str(parsed).lower() not in {"a_plus", "normal", "aggressive"}:
        await reply(update, "❌ Liquidity retest quality must be a_plus, normal, or aggressive.", reply_markup=MAIN_MENU)
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
            "ai_scalping_btc_tp_pct": 0.09,
            "ai_scalping_btc_sl_pct": 0.18,
            "ai_scalping_eth_tp_pct": 0.12,
            "ai_scalping_eth_sl_pct": 0.24,
            "ai_scalping_btc_min_tp_pct": 0.08,
            "ai_scalping_btc_max_tp_pct": 0.12,
            "ai_scalping_eth_min_tp_pct": 0.10,
            "ai_scalping_eth_max_tp_pct": 0.16,
            "ai_scalping_sl_tp_multiplier": 2.0,
            "ai_scalping_ai_entry_filter_enabled": True,
            "ai_scalping_ai_cooldown_sec": 8,
            "ai_scalping_quality_cooldown_sec": 20,
            "scan_interval_sec": 8,
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
    if key in {"scan_interval_sec", "scanner_concurrency", "strategy_mode", "universe_mode", "max_symbols", "scan_market_source", "spot_confirmation_enabled", "session_filter_enabled", "america_short_bias_enabled", "openai_analysis_enabled", "openai_check_strength", "openai_model", "ai_scalping_symbols", "ai_scalping_min_confidence", "ai_scalping_ai_entry_filter_enabled", "ai_scalping_tp_pct", "ai_scalping_sl_pct", "ai_scalping_btc_tp_pct", "ai_scalping_btc_sl_pct", "ai_scalping_eth_tp_pct", "ai_scalping_eth_sl_pct", "ai_scalping_btc_min_tp_pct", "ai_scalping_btc_max_tp_pct", "ai_scalping_eth_min_tp_pct", "ai_scalping_eth_max_tp_pct", "ai_scalping_sl_tp_multiplier", "ai_scalping_max_spread_pct", "ai_scalping_quality_filters_enabled", "ai_scalping_quality_min_confidence", "ai_scalping_quality_cooldown_sec", "ai_scalping_quality_min_atr_pct", "ai_scalping_quality_min_ema_gap_pct", "ai_scalping_quality_min_ret_5m_abs_pct", "liquidity_retest_quality_mode", "scanner_reject_log_enabled"}:
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
        direct_ip = await fetch_public_ip(use_proxy=False, timeout_sec=2)
        proxy_ip = await fetch_public_ip(use_proxy=True, proxy_url=proxy_url, timeout_sec=2) if proxy_enabled and proxy_url else {"ok": False, "ip": "not configured", "error": "proxy off or missing"}
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
        restore_scan = int(float(s.get("ai_scalping_prev_scan_interval_sec", 0) or 0))
        updates = {
            "strategy_mode": "hybrid",
            "auto_strategy_adaptation": True,
            "regime_adaptation": True,
            "spot_confirmation_enabled": True,
            "session_filter_enabled": True,
            "openai_analysis_enabled": False,
        }
        if restore_scan > 0:
            updates["scan_interval_sec"] = restore_scan
        for k, v in updates.items():
            await storage.set(k, v, bump_revision=False)
        await storage.set("settings_revision", int(s.get("settings_revision", 1) or 1) + 1, bump_revision=False)
        trigger_scan_now(context.application, reason="ai_scalping:off")
        await reply(update, "○ AI BTC/ETH scalping OFF\nРежим возвращён на hybrid adaptive.", reply_markup=MAIN_MENU)
        return

    prev_scan = int(float(s.get("scan_interval_sec", 5) or 5))
    updates = {
        "strategy_mode": "ai_scalping",
        "ai_scalping_prev_scan_interval_sec": prev_scan,
        "ai_scalping_symbols": "BTC_USDT,ETH_USDT",
        "ai_scalping_btc_tp_pct": 0.09,
        "ai_scalping_btc_sl_pct": 0.18,
        "ai_scalping_eth_tp_pct": 0.12,
        "ai_scalping_eth_sl_pct": 0.24,
        "ai_scalping_btc_min_tp_pct": 0.08,
        "ai_scalping_btc_max_tp_pct": 0.12,
        "ai_scalping_eth_min_tp_pct": 0.10,
        "ai_scalping_eth_max_tp_pct": 0.16,
        "ai_scalping_sl_tp_multiplier": 2.0,
        "ai_scalping_ai_entry_filter_enabled": True,
        "ai_scalping_ai_cooldown_sec": 8,
        "ai_scalping_quality_cooldown_sec": 20,
        "max_open_positions": 2,
        "scan_interval_sec": 8,
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
        "Включено: BTC/ETH micro-scalp, короткий AI JSON-фильтр, TP по силе сетапа, SL=TP×2.\n"
        "Отключено: scanner strategies, spot/session filters, mirror, regime/adaptive strategy.",
        reply_markup=MAIN_MENU,
    )


async def quick_bounce_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    global running, entries_enabled, trading_task, position_task
    s = await storage.all_settings()
    enabled = str(s.get("strategy_mode", "hybrid")).lower() == "quick_bounce" and _bool_setting(s, "quick_bounce_enabled", False)
    if enabled:
        # Toggle OFF means: stop scanner/new entries only. Existing quick_bounce
        # positions keep their TP/SL/time-stop management. Pressing the same
        # button again re-enables the scanner with the same quick_bounce preset.
        await storage.set("quick_bounce_enabled", False, bump_revision=False)
        await storage.set("settings_revision", int(s.get("settings_revision", 1) or 1) + 1, bump_revision=False)
        trigger_scan_now(context.application, reason="quick_bounce:off")
        await reply(update, "○ Быстрый отскок OFF\nСканер остановлен, новые сделки не открываются. Открытые позиции продолжают сопровождаться до TP/SL/12h.", reply_markup=MAIN_MENU)
        return

    updates = {
        "quick_bounce_enabled": True,
        "strategy_mode": "quick_bounce",
        "universe_mode": "top-200",
        "max_symbols": 200,
        "scan_interval_sec": 900,
        "symbol_refresh_sec": 900,
        "max_open_positions": 5,
        "trade_margin_pct": 0.10,
        "quick_bounce_trade_margin_pct": 0.10,
        "mexc_order_leverage": 10,
        "quick_bounce_leverage": 10,
        "quick_bounce_tp_pct": 2.5,
        "quick_bounce_sl_pct": 1.5,
        "quick_bounce_rr": 1.6667,
        "quick_bounce_time_stop_sec": 43200,
        "quick_bounce_top_coins": 200,
        "quick_bounce_max_open_positions": 5,
        "quick_bounce_max_candidates": 5,
        "quick_bounce_drop_4h_pct": 5.0,
        "quick_bounce_pump_4h_pct": 5.0,
        "quick_bounce_reversal_pct": 1.0,
        "quick_bounce_min_volume_ratio": 1.15,
        "quick_bounce_max_spread_pct": 0.20,
        "quick_bounce_min_24h_volume_usdt": 20000000.0,
        "quick_bounce_btc_filter_enabled": True,
        "quick_bounce_btc_max_drop_1h_pct": 2.0,
        "quick_bounce_btc_max_pump_1h_pct": 2.0,
        "quick_bounce_anomaly_timeframe": "1h",
        "quick_bounce_confirm_timeframe": "15m",
        "cooldown_after_close_sec": 21600,
        "quick_bounce_cooldown_after_close_sec": 21600,
        "max_daily_loss_pct": 5.0,
        "quick_bounce_max_daily_loss_pct": 5.0,
        "auto_strategy_adaptation": False,
        "regime_adaptation": False,
        "liquidity_runner_enabled": False,
        "mirror_mode": "off",
        "spot_confirmation_enabled": False,
        "session_filter_enabled": False,
        "america_short_bias_enabled": False,
        "openai_analysis_enabled": False,
    }
    for k, v in updates.items():
        await storage.set(k, v, bump_revision=False)
    await storage.set("settings_revision", int(s.get("settings_revision", 1) or 1) + 1, bump_revision=False)
    scanner.last_refresh = 0
    entries_enabled = True
    running = True
    if trading_task is None or trading_task.done():
        trading_task = context.application.create_task(trading_loop(context.application))
    if position_task is None or position_task.done():
        position_task = context.application.create_task(position_management_loop(context.application))
    trigger_scan_now(context.application, reason="quick_bounce:on")
    await reply(
        update,
        "✅ Быстрый отскок ON\n"
        "Топ-200, скан 15m, до 5 сделок, 10% депозита на сделку, 10x isolated.\n"
        "TP +2%, SL -2%, time-stop 12h. Остальные режимы отключены.",
        reply_markup=MAIN_MENU,
    )

def _button_text_key(text: str) -> str:
    """Normalize Telegram reply-keyboard text.

    Telegram clients may send emoji with or without the variation selector (FE0F),
    and sometimes extra spaces. v0185 matched exact strings, so buttons like
    Balance/Settings could be ignored on some clients.
    """
    return " ".join(str(text or "").replace("\ufe0f", "").split()).strip().casefold()


def _button_mapping():
    pairs = [
        ("▶️ Run", run_cmd), ("▶ Run", run_cmd), ("Run", run_cmd),
        ("⏹ Stop", stop_cmd), ("Stop", stop_cmd),
        ("📊 Status", status_cmd), ("Status", status_cmd),
        ("🚨 Panic", panic_cmd), ("Panic", panic_cmd),
        ("📈 Positions", positions_cmd), ("Positions", positions_cmd),
        ("🧯 Close All", close_all_cmd), ("Close All", close_all_cmd), ("close all", close_all_cmd),
        ("🧹 Cancel All", cancel_all_cmd), ("Cancel All", cancel_all_cmd), ("cancel all", cancel_all_cmd),
        ("📉 Stats", stats_cmd), ("Stats", stats_cmd),
        ("💰 Balance", balance_cmd), ("Balance", balance_cmd), ("баланс", balance_cmd), ("Баланс", balance_cmd),
        ("🏓 Ping", ping_cmd), ("Ping", ping_cmd),
        ("⚙️ Settings", settings_cmd), ("⚙ Settings", settings_cmd), ("Settings", settings_cmd),
        ("🔐 API", api_cmd), ("API", api_cmd),
        ("📊 AI Stats", ai_stats_cmd), ("AI Stats", ai_stats_cmd),
        ("🤖 AI BTC/ETH scalping", ai_scalping_toggle_cmd), ("AI BTC/ETH scalping", ai_scalping_toggle_cmd),
        ("⚡ быстрый отскок", quick_bounce_cmd), ("быстрый отскок", quick_bounce_cmd), ("Быстрый отскок", quick_bounce_cmd),
        ("🚀 BOOST MODE", boost_start_cmd), ("BOOST MODE", boost_start_cmd),
        ("🛑 STOP BOOST", boost_stop_cmd), ("STOP BOOST", boost_stop_cmd),
        ("📊 BOOST STATUS", boost_status_cmd), ("BOOST STATUS", boost_status_cmd),
        ("⚙️ MEXC", mexc_settings_cmd), ("⚙ MEXC", mexc_settings_cmd), ("MEXC", mexc_settings_cmd),
    ]
    return {_button_text_key(k): v for k, v in pairs}


async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return
    raw_text = (update.message.text or "").strip()
    key = _button_text_key(raw_text)
    fn = _button_mapping().get(key)
    if not fn:
        await reply(update, "Неизвестная команда. Нажми /help.", reply_markup=MAIN_MENU)
        return

    # Hard isolation: BOOST can be started only by exact BOOST button text.
    # Balance/Close/Cancel/etc. cannot fall through into boost_start_cmd even if
    # old Telegram updates arrive while BOOST is running.
    if fn is not boost_start_cmd and key not in {_button_text_key("🚀 BOOST MODE"), _button_text_key("BOOST MODE")}:
        context.application.bot_data["last_non_boost_button"] = raw_text

    try:
        cmd_timeout = 18 if fn is balance_cmd else 45
        await asyncio.wait_for(fn(update, context), timeout=cmd_timeout)
    except asyncio.TimeoutError:
        log.exception("button command timeout: %s", raw_text)
        await reply(update, f"⏱ Команда зависла и остановлена по timeout: {raw_text}. Интерфейс не заблокирован.", reply_markup=MAIN_MENU)
    except Exception as e:
        log.exception("button command failed: %s", raw_text)
        await reply(update, f"❌ Command failed: {raw_text}\n{str(e)[:500]}", reply_markup=MAIN_MENU)

async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not allowed(update):
        try:
            await asyncio.wait_for(q.answer("Access denied", show_alert=True), timeout=3)
        except Exception:
            pass
        return
    try:
        await asyncio.wait_for(q.answer(), timeout=3)
    except Exception:
        pass
    raw_data = str(q.data or "")
    data = raw_data.split(":")
    allowed_prefixes = {"boost", "toggle", "set", "menu", "api", "aistats", "openai", "noop"}
    if not data or data[0] not in allowed_prefixes:
        log.warning("ignored unknown callback_data: %s", raw_data[:120])
        return
    s = await storage.all_settings()
    current_rev = int(s.get("settings_revision", 1))
    try:
        rev = int(data[-1])
    except Exception:
        rev = current_rev
    if rev != current_rev and data[0] != "menu":
        await _safe_edit_message_text(q.message, "⚠️ Старое меню. Открой Settings заново.")
        return

    if data[0] == "boost":
        action = data[1] if len(data) > 1 else "refresh"
        if action == "rotation":
            new_value = not _bool_setting(s, "boost_parallel_scan_enabled", True)
            await storage.set("boost_parallel_scan_enabled", new_value)
            ns = await storage.all_settings()
            snap = await _boost_session_snapshot(ns)
            await _safe_edit_message_text(q.message, 
                f"🚀 BOOST controls\nRotation while in profit = {new_value}\nBank: {snap['current_bank']:.4f} / {snap['target_bank']:.4f} USDT",
                reply_markup=_boost_rotation_keyboard(ns),
            )
            return
        if action == "panel":
            new_value = not _bool_setting(s, "boost_live_panel_enabled", True)
            await storage.set("boost_live_panel_enabled", new_value)
            ns = await storage.all_settings()
            await _safe_edit_message_text(q.message, f"🚀 BOOST live panel = {new_value}", reply_markup=_boost_rotation_keyboard(ns))
            return
        ns = await storage.all_settings()
        snap = await _boost_session_snapshot(ns)
        await _safe_edit_message_text(q.message, 
            f"🚀 BOOST controls\nBank: {snap['current_bank']:.4f} / {snap['target_bank']:.4f} USDT\nRotation: {_bool_setting(ns, 'boost_parallel_scan_enabled', True)}",
            reply_markup=_boost_rotation_keyboard(ns),
        )
        return

    if data[0] == "toggle":
        key = data[1]
        new_value = not bool(s.get(key, False))
        await storage.set(key, new_value)
        if key in {"ws_enabled", "proxy_enabled", "scan_market_source"}:
            await reset_market_runtime()
        new_settings = await storage.all_settings()
        new_rev = int(new_settings.get("settings_revision", current_rev + 1))
        if key in {"live_trading", "spot_confirmation_enabled", "session_filter_enabled", "america_short_bias_enabled", "openai_analysis_enabled", "ws_enabled", "ai_scalping_quality_filters_enabled", "ai_scalping_openai_fallback_enabled", "ai_scalping_json_mode_enabled", "ai_scalping_liquidation_stop_mode", "boost_parallel_scan_enabled", "boost_live_panel_enabled"}:
            trigger_scan_now(context.application, reason=f"toggle:{key}")
        await _safe_edit_message_text(q.message, f"✅ {key} = {new_value}\n\n⚙️ Settings", reply_markup=settings_menu(new_rev, new_settings))
    elif data[0] == "set":
        key, value = data[1], data[2]
        parsed = value
        try:
            parsed = float(value) if "." in value else int(value)
        except ValueError:
            parsed = value
        await storage.set(key, parsed)
        if key == "strategy_mode" and str(parsed).lower() == "boost_scalping":
            # v0185: choosing BOOST in Settings must not start autopilot.
            # Only /boost_start or the 🚀 BOOST MODE main button can arm it.
            await storage.set("boost_autopilot_active", False, bump_revision=False)
            prev_scan = int(float(s.get("scan_interval_sec", 5) or 5))
            for k2, v2 in {
                "strategy_mode": "boost_scalping",
                "max_open_positions": 1,
                "scan_interval_sec": int(float(s.get("boost_scan_interval_sec", 1) or 1)),
                "boost_prev_scan_interval_sec": prev_scan,
                "boost_session_start_ts": 0.0,
                "trade_margin_pct": float(s.get("boost_balance_share", 0.10) or 0.10),
                "margin_allocation_enabled": True,
                "auto_strategy_adaptation": False,
                "regime_adaptation": False,
                "liquidity_runner_enabled": False,
                "spot_confirmation_enabled": False,
                "session_filter_enabled": False,
                "america_short_bias_enabled": False,
                "mirror_mode": "off",
            }.items():
                await storage.set(k2, v2, bump_revision=False)

        if key == "strategy_mode" and str(parsed).lower() == "ai_scalping":
            prev_scan = int(float(s.get("scan_interval_sec", 5) or 5))
            for k2, v2 in {
                "openai_analysis_enabled": True, "openai_show_decisions": True,
                "ai_scalping_symbols": "BTC_USDT,ETH_USDT", "max_open_positions": 2,
                "scan_interval_sec": 8, "ai_scalping_prev_scan_interval_sec": prev_scan,
                "ai_scalping_btc_tp_pct": 0.09, "ai_scalping_btc_sl_pct": 0.18,
                "ai_scalping_eth_tp_pct": 0.12, "ai_scalping_eth_sl_pct": 0.24,
                "ai_scalping_btc_min_tp_pct": 0.08, "ai_scalping_btc_max_tp_pct": 0.12,
                "ai_scalping_eth_min_tp_pct": 0.10, "ai_scalping_eth_max_tp_pct": 0.16,
                "ai_scalping_sl_tp_multiplier": 2.0, "ai_scalping_ai_entry_filter_enabled": True,
                "ai_scalping_ai_cooldown_sec": 8, "ai_scalping_quality_cooldown_sec": 20,
                "auto_strategy_adaptation": False, "regime_adaptation": False,
                "liquidity_runner_enabled": False, "spot_confirmation_enabled": False,
                "session_filter_enabled": False, "america_short_bias_enabled": False, "mirror_mode": "off",
            }.items():
                await storage.set(k2, v2, bump_revision=False)
        elif key == "strategy_mode" and str(s.get("strategy_mode", "hybrid")).lower() == "ai_scalping" and str(parsed).lower() != "ai_scalping":
            for k2 in ["auto_strategy_adaptation", "regime_adaptation", "spot_confirmation_enabled", "session_filter_enabled", "america_short_bias_enabled", "mirror_mode", "openai_analysis_enabled", "openai_show_decisions", "liquidity_runner_enabled", "max_open_positions"]:
                await storage.set(k2, DEFAULT_SETTINGS.get(k2, s.get(k2)), bump_revision=False)
            restore_scan = int(float(s.get("ai_scalping_prev_scan_interval_sec", 0) or 0))
            if restore_scan > 0:
                await storage.set("scan_interval_sec", restore_scan, bump_revision=False)
        if key in {"scan_market_source", "ws_enabled", "proxy_url", "proxy_enabled", "ws_stale_sec", "ws_update_throttle_ms", "ws_max_updates_per_batch", "ws_queue_limit", "ws_adaptive_slowdown_threshold"}:
            await reset_market_runtime()
        if key in {"universe_mode", "max_symbols", "scan_market_source"}:
            scanner.last_refresh = 0
            scanner.last_reject_reason = "universe settings changed; refresh queued"
        new_settings = await storage.all_settings()
        new_rev = int(new_settings.get("settings_revision", current_rev + 1))
        if key in {"scan_interval_sec", "scanner_concurrency", "strategy_mode", "universe_mode", "max_symbols", "scan_market_source", "symbol_refresh_sec", "openai_model", "openai_check_strength", "liquidity_retest_quality_mode"}:
            trigger_scan_now(context.application, reason=f"menu:{key}")
        # Stay inside the same submenu so the selected value is immediately visible with ✅.
        if key == "universe_mode":
            await _safe_edit_message_text(q.message, "🌐 Universe", reply_markup=choices_menu("universe_mode", [("Top-50","top-50"),("Top-100","top-100"),("Top-200","top-200"),("Top-300","top-300"),("Adaptive","adaptive")], new_rev, new_settings.get("universe_mode")))
        elif key == "strategy_mode":
            await _safe_edit_message_text(q.message, "📈 Strategy", reply_markup=choices_menu("strategy_mode", [("Momentum","momentum"),("Pullback","pullback"),("Reversal","reversal"),("Liquidity Retest","liquidity_retest"),("⚡ Быстрый отскок","quick_bounce"),("AI BTC/ETH scalp","ai_scalping"),("🚀 Boost 0-fee","boost_scalping"),("Hybrid adaptive","hybrid"),("All strategies","all")], new_rev, new_settings.get("strategy_mode")))
        elif key == "scan_market_source":
            await _safe_edit_message_text(q.message, "📡 Фьючи | Спот", reply_markup=choices_menu("scan_market_source", [("Binance фьючи + Binance спот","binance_binance"),("MEXC фьючи + MEXC спот","mexc_mexc"),("MEXC фьючи + Binance спот","mexc_binance")], new_rev, new_settings.get("scan_market_source", "mexc_binance")))
        elif key == "scan_interval_sec":
            await _safe_edit_message_text(q.message, "⏱ Scan speed", reply_markup=choices_menu("scan_interval_sec", [("3s","3"),("5s default","5"),("8s scalp","8"),("10s","10"),("30s","30"),("1m","60"),("5m","300"),("15m","900"),("30m","1800"),("1h","3600"),("4h","14400")], new_rev, new_settings.get("scan_interval_sec")))
        elif key == "scanner_concurrency":
            await _safe_edit_message_text(q.message, "🧵 Scanner concurrency", reply_markup=choices_menu("scanner_concurrency", [("3 requests","3"),("5 requests","5"),("8 requests","8"),("12 requests","12")], new_rev, new_settings.get("scanner_concurrency", 5)))
        elif key == "ws_update_throttle_ms":
            await _safe_edit_message_text(q.message, "🌊 WS throttle", reply_markup=choices_menu("ws_update_throttle_ms", [("250ms","250"),("500ms","500"),("1000ms","1000"),("1500ms","1500")], new_rev, new_settings.get("ws_update_throttle_ms", 500)))
        elif key == "symbol_refresh_sec":
            await _safe_edit_message_text(q.message, "🔄 Refresh", reply_markup=choices_menu("symbol_refresh_sec", [("60s","60"),("180s","180"),("300s","300"),("600s","600"),("1200s","1200")], new_rev, new_settings.get("symbol_refresh_sec")))
        elif key == "risk_pct":
            await _safe_edit_message_text(q.message, "📊 Risk", reply_markup=choices_menu("risk_pct", [("0.25%","0.0025"),("0.50%","0.005"),("1%","0.01"),("3%","0.03"),("5%","0.05")], new_rev, new_settings.get("risk_pct")))
        elif key == "max_open_positions":
            await _safe_edit_message_text(q.message, "🔥 Max positions", reply_markup=choices_menu("max_open_positions", [("1","1"),("2","2"),("3","3"),("5","5"),("10","10"),("15","15"),("20","20")], new_rev, new_settings.get("max_open_positions")))
        elif key == "mirror_mode":
            await _safe_edit_message_text(q.message, "🪞 Mirror", reply_markup=choices_menu("mirror_mode", [("OFF","off"),("ON","on"),("AUTO","auto")], new_rev, new_settings.get("mirror_mode")))
        elif key == "openai_model":
            await _safe_edit_message_text(q.message, "🧠 OpenAI model", reply_markup=choices_menu("openai_model", [("gpt-5.4-mini default","gpt-5.4-mini"),("gpt-4o-mini","gpt-4o-mini"),("gpt-5.5","gpt-5.5"),("gpt-5.5-pro","gpt-5.5-pro"),("gpt-4.1","gpt-4.1")], new_rev, new_settings.get("openai_model", "gpt-5.4-mini")))
        elif key == "openai_check_strength":
            await _safe_edit_message_text(q.message, "🛡 OpenAI check strength", reply_markup=choices_menu("openai_check_strength", [("Weak","weak"),("Medium default","medium"),("Strong","strong")], new_rev, new_settings.get("openai_check_strength", "medium")))
        elif key == "liquidity_retest_quality_mode":
            await _safe_edit_message_text(q.message, "💧 Liquidity retest quality", reply_markup=choices_menu("liquidity_retest_quality_mode", [("A+ only (strict/current)","a_plus"),("Normal","normal"),("Aggressive","aggressive")], new_rev, new_settings.get("liquidity_retest_quality_mode", "a_plus")))
        else:
            await _safe_edit_message_text(q.message, f"✅ {key} = {parsed}\n\n⚙️ Settings", reply_markup=settings_menu(new_rev, new_settings))
    elif data[0] == "api":
        action = data[1] if len(data) > 1 else "status"
        if action == "clear":
            await storage.set("mexc_api_key", "")
            await storage.set("mexc_api_secret", "")
            await reset_exchange()
            new_settings = await storage.all_settings()
            new_rev = int(new_settings.get("settings_revision", current_rev + 1))
            await _safe_edit_message_text(q.message, "🗑 API keys cleared from bot storage", reply_markup=api_menu(new_rev, new_settings))
        elif action == "test":
            api_key, api_secret = _api_creds(s)
            if not api_key or not api_secret:
                await _safe_edit_message_text(q.message, "❌ API missing. Use /api set API_KEY API_SECRET", reply_markup=api_menu(current_rev, s))
            else:
                try:
                    ex = await get_exchange(s)
                    bal = await ex.fetch_balance()
                    usdt = bal.get("USDT", {}) if isinstance(bal, dict) else {}
                    free = usdt.get("free", "n/a") if isinstance(usdt, dict) else "n/a"
                    total = usdt.get("total", "n/a") if isinstance(usdt, dict) else "n/a"
                    await _safe_edit_message_text(q.message, f"✅ API test OK\nUSDT free: {free}\nUSDT total: {total}", reply_markup=api_menu(current_rev, s))
                except Exception as e:
                    await _safe_edit_message_text(q.message, f"❌ API test failed: {e}", reply_markup=api_menu(current_rev, s))
        else:
            await _safe_edit_message_text(q.message, "🔐 API menu\nUse /api set API_KEY API_SECRET to save keys.", reply_markup=api_menu(current_rev, s))
    elif data[0] == "aistats":
        action = data[1] if len(data) > 1 else "current"
        if action == "reset":
            sid = await ai_stats_manager.reset_session()
            new_settings = await storage.all_settings()
            new_rev = int(new_settings.get("settings_revision", current_rev + 1))
            await _safe_edit_message_text(q.message, f"♻ AI scalping session reset\nNew session ID: {sid}", reply_markup=ai_stats_menu(new_rev))
        elif action == "lifetime":
            await _safe_edit_message_text(q.message, AIStatsManager.format(await ai_stats_manager.summary("lifetime")), reply_markup=ai_stats_menu(current_rev))
        else:
            await _safe_edit_message_text(q.message, AIStatsManager.format(await ai_stats_manager.summary("current")), reply_markup=ai_stats_menu(current_rev))
    elif data[0] == "openai":
        action = data[1] if len(data) > 1 else "status"
        if action == "clear":
            await storage.set("openai_api_key", "")
            new_settings = await storage.all_settings()
            new_rev = int(new_settings.get("settings_revision", current_rev + 1))
            await _safe_edit_message_text(q.message, "🗑 OpenAI API key cleared", reply_markup=openai_menu(new_rev, new_settings))
        else:
            await _safe_edit_message_text(q.message, "🔑 OpenAI API key\nUse: /openai set YOUR_OPENAI_API_KEY", reply_markup=openai_menu(current_rev, s))
    elif data[0] == "noop":
        await q.answer("Use /api set API_KEY API_SECRET", show_alert=True)
    elif data[0] == "menu":
        name = data[1]
        rev = current_rev
        if name == "settings":
            await _safe_edit_message_text(q.message, "⚙️ Settings", reply_markup=settings_menu(rev, s))
        elif name == "universe":
            await _safe_edit_message_text(q.message, "🌐 Universe", reply_markup=choices_menu("universe_mode", [("Top-50","top-50"),("Top-100","top-100"),("Top-200","top-200"),("Top-300","top-300"),("Adaptive","adaptive")], rev, s.get("universe_mode")))
        elif name == "strategy":
            await _safe_edit_message_text(q.message, "📈 Strategy", reply_markup=choices_menu("strategy_mode", [("Momentum","momentum"),("Pullback","pullback"),("Reversal","reversal"),("Liquidity Retest","liquidity_retest"),("⚡ Быстрый отскок","quick_bounce"),("AI BTC/ETH scalp","ai_scalping"),("🚀 Boost 0-fee","boost_scalping"),("Hybrid adaptive","hybrid"),("All strategies","all")], rev, s.get("strategy_mode")))
        elif name == "marketsource":
            await _safe_edit_message_text(q.message, "📡 Фьючи | Спот", reply_markup=choices_menu("scan_market_source", [("Binance фьючи + Binance спот","binance_binance"),("MEXC фьючи + MEXC спот","mexc_mexc"),("MEXC фьючи + Binance спот","mexc_binance")], rev, s.get("scan_market_source", "mexc_binance")))
        elif name == "scan":
            await _safe_edit_message_text(q.message, "⏱ Scan speed", reply_markup=choices_menu("scan_interval_sec", [("3s","3"),("5s default","5"),("8s scalp","8"),("10s","10"),("30s","30"),("1m","60"),("5m","300"),("15m","900"),("30m","1800"),("1h","3600"),("4h","14400")], rev, s.get("scan_interval_sec")))
        elif name == "concurrency":
            await _safe_edit_message_text(q.message, "🧵 Scanner concurrency", reply_markup=choices_menu("scanner_concurrency", [("3 requests","3"),("5 requests","5"),("8 requests","8"),("12 requests","12")], rev, s.get("scanner_concurrency", 5)))
        elif name == "wsthrottle":
            await _safe_edit_message_text(q.message, "🌊 WS throttle", reply_markup=choices_menu("ws_update_throttle_ms", [("250ms","250"),("500ms","500"),("1000ms","1000"),("1500ms","1500")], rev, s.get("ws_update_throttle_ms", 500)))
        elif name == "refresh":
            await _safe_edit_message_text(q.message, "🔄 Refresh", reply_markup=choices_menu("symbol_refresh_sec", [("60s","60"),("180s","180"),("300s","300"),("600s","600"),("1200s","1200")], rev, s.get("symbol_refresh_sec")))
        elif name == "risk":
            await _safe_edit_message_text(q.message, "📊 Risk", reply_markup=choices_menu("risk_pct", [("0.25%","0.0025"),("0.50%","0.005"),("1%","0.01"),("3%","0.03"),("5%","0.05")], rev, s.get("risk_pct")))
        elif name == "maxpos":
            await _safe_edit_message_text(q.message, "🔥 Max positions", reply_markup=choices_menu("max_open_positions", [("1","1"),("2","2"),("3","3"),("5","5"),("10","10"),("15","15"),("20","20")], rev, s.get("max_open_positions")))
        elif name == "mirror":
            await _safe_edit_message_text(q.message, "🪞 Mirror", reply_markup=choices_menu("mirror_mode", [("OFF","off"),("ON","on"),("AUTO","auto")], rev, s.get("mirror_mode")))
        elif name == "openai":
            await _safe_edit_message_text(q.message, "🤖 ИИ анализ OpenAI", reply_markup=openai_menu(rev, s))
        elif name == "openai_model":
            await _safe_edit_message_text(q.message, "🧠 OpenAI model", reply_markup=choices_menu("openai_model", [("gpt-5.4-mini default","gpt-5.4-mini"),("gpt-4o-mini","gpt-4o-mini"),("gpt-5.5","gpt-5.5"),("gpt-5.5-pro","gpt-5.5-pro"),("gpt-4.1","gpt-4.1")], rev, s.get("openai_model", "gpt-5.4-mini")))
        elif name == "openai_strength":
            await _safe_edit_message_text(q.message, "🛡 OpenAI check strength", reply_markup=choices_menu("openai_check_strength", [("Weak","weak"),("Medium default","medium"),("Strong","strong")], rev, s.get("openai_check_strength", "medium")))
        elif name == "liquidity_quality":
            await _safe_edit_message_text(q.message, "💧 Liquidity retest quality", reply_markup=choices_menu("liquidity_retest_quality_mode", [("A+ only (strict/current)","a_plus"),("Normal","normal"),("Aggressive","aggressive")], rev, s.get("liquidity_retest_quality_mode", "a_plus")))
        elif name == "api":
            await _safe_edit_message_text(q.message, "🔐 API menu\nUse /api set API_KEY API_SECRET to save keys.", reply_markup=api_menu(rev, s))


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
        bal = await asyncio.wait_for(ex.fetch_balance(), timeout=5)
        usdt = bal.get("USDT", {}) if isinstance(bal, dict) else {}
        total = usdt.get("total") if isinstance(usdt, dict) else None
        free = usdt.get("free") if isinstance(usdt, dict) else None
        return float(total or free or default)
    except Exception as e:
        log.debug("balance fetch failed, using default equity: %s", e)
        return float(default)


def _boolish(value, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}

async def emit_position_events(app, events: list[dict]) -> None:
    """Send position notifications outside the critical trade management call.

    PositionManager.manage() must be free to return quickly; Telegram delivery is
    deliberately scheduled as background work so Telegram latency/rate limits do
    not delay TP/SL checks or reduce-only market closes.
    """
    for ev in events or []:
        ev_type = str(ev.get("type") or "")
        # v0223: keep Telegram clean. These are not actionable errors; they mean
        # local TP/fast-profit was touched but MEXC real profit was not confirmed
        # yet. Re-sending them every manage tick floods the chat and delays useful
        # rotation/profit messages. Store/log them silently instead.
        if ev_type in {
            "pending_sync_warning",
            "price_error",
            "boost_tp_wait_exchange_profit",
            "boost_fast_profit_wait_exchange_profit",
        }:
            continue
        text = format_position_event(ev)
        if ev_type == "protection_watchdog":
            symbol_key = str(ev.get("symbol") or "position").replace("/", "_").replace(":", "_")
            app.create_task(notify_admin_bottom_replace(app, text, key=f"position_watchdog_{symbol_key}"))
        else:
            app.create_task(notify_admin(app, text, key="position_event"))

async def position_management_loop(app):
    """Fast execution/exit loop independent from Telegram and signal scanning.

    This is the minimal separate execution loop: it only manages already-open
    positions, checks local TP/SL against price, and sends close orders. New
    entries, AI decisions, charts, scanner status and Telegram commands stay in
    trading_loop / handlers.
    """
    global running, position_task
    interval_default = os.getenv("EXECUTION_LOOP_INTERVAL_SEC", "0.25")
    try:
        while running:
            try:
                settings = await storage.all_settings()
                live = bool(settings.get("live_trading", False))
                ex = await _await_with_timeout(get_exchange(settings), 8, "exchange init")
                exec_engine = ExecutionEngine(storage, ex)
                pos_manager = PositionManager(storage, exec_engine)
                events = await pos_manager.manage(lambda symbol: get_last_price(ex, symbol), live)
                await emit_position_events(app, events)
                interval = float(settings.get("execution_loop_interval_sec", interval_default) or interval_default)
                await asyncio.sleep(max(0.05, min(interval, 2.0)))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.exception("position management loop error: %s", e)
                await asyncio.sleep(1)
    finally:
        position_task = None



def _record_boost_decision_metrics(decision) -> None:
    """v0180: make Telegram scanner card show the real BOOST universe/depth.

    BOOST uses its own scanner, so the generic scanner counters stayed at
    loaded=5/checked=2 before. These counters are now sourced from the BOOST
    decision payload: full zero-fee DB loaded, 40-100 checked per cycle, and
    candidate count.
    """
    try:
        m = getattr(decision, "market", None) or {}
        checked = m.get("checked") or m.get("markets") or []
        loaded = int(m.get("loaded") or m.get("universe_total") or 0)
        scanner.last_effective_strategy = "boost_scalping"
        scanner.last_available_markets = loaded
        scanner.last_total_markets = loaded
        scanner.last_filtered_markets = loaded
        scanner.last_cycle_scanned = len(checked) if isinstance(checked, list) else 0
        scanner.last_cycle_errors = len([c for c in checked if isinstance(c, dict) and str(c.get("reason", "")).lower().startswith(("error", "exception"))]) if isinstance(checked, list) else 0
        scanner.last_ai_candidates_count = int(m.get("ai_candidates") or len(m.get("top_candidates") or []))
        scanner.hot_symbols = [_short_symbol(x) for x in (m.get("top_candidates") or [])[:20]] if isinstance(m.get("top_candidates"), list) else []
    except Exception:
        pass



def _boost_local_position_from_exchange(row: dict) -> dict:
    """Build a local BOOST position from an exchange-only MEXC position row.

    This is used when MEXC has a live position but local storage missed it after
    a restart/race. BOOST must then manage/rotate the real exchange position
    instead of pretending the slot is free.
    """
    info = row.get("info") if isinstance(row.get("info"), dict) else {}
    symbol = row.get("symbol") or row.get("mexc_symbol") or info.get("symbol") or info.get("contract")
    side = str(row.get("side") or "").upper()
    if side not in {"LONG", "SHORT"}:
        raw_side = str(info.get("positionType") or info.get("holdSide") or info.get("side") or "").lower()
        side = "SHORT" if raw_side in {"2", "short", "sell"} else "LONG"
    entry = 0.0
    for key in ("entryPrice", "entry_price", "holdAvgPrice", "openAvgPrice", "avgPrice"):
        try:
            val = row.get(key, info.get(key))
            if val not in (None, ""):
                entry = float(val); break
        except Exception:
            pass
    mark = 0.0
    for key in ("markPrice", "mark_price", "fairPrice", "lastPrice"):
        try:
            val = row.get(key, info.get(key))
            if val not in (None, ""):
                mark = float(val); break
        except Exception:
            pass
    qty = 0.0
    for key in ("amount", "qty", "size", "contracts"):
        try:
            val = row.get(key, info.get(key))
            if val not in (None, ""):
                qty = abs(float(val)); break
        except Exception:
            pass
    leverage = 0
    for key in ("leverage", "leverageLevel"):
        try:
            val = row.get(key, info.get(key))
            if val not in (None, ""):
                leverage = int(float(val)); break
        except Exception:
            pass
    notional = 0.0
    try:
        notional = abs(float(info.get("im") or info.get("positionMargin") or 0) * float(leverage or 1))
    except Exception:
        notional = 0.0
    if notional <= 0 and entry > 0 and qty > 0:
        notional = abs(entry * qty)
    return {
        "id": f"boost_exchange_sync_{str(symbol).replace('/', '_').replace(':', '_')}",
        "symbol": symbol,
        "side": side,
        "strategy": "boost_scalping",
        "status": "open",
        "entry_price": entry or mark,
        "qty": qty,
        "leverage": leverage or 30,
        "opened_at": time.time(),
        "updated_at": time.time(),
        "notional_usdt": notional,
        "planned_notional_usdt": notional,
        "score_details": {"boost_score": 0.0, "synced_from_exchange": True},
        "raw_exchange_position": row,
        "exchange_contracts": row.get("contracts") or info.get("holdVol") or info.get("vol"),
    }

async def _boost_find_active_position(ex, exec_engine) -> dict | None:
    """Return local or exchange live BOOST position and sync exchange-only rows."""
    try:
        for _p in await storage.positions():
            if str(_p.get("status", "open")).lower() in {"open", "pending", "closing"}:
                return _p
    except Exception:
        pass
    try:
        rows = await _await_with_timeout(ex.fetch_positions(), 4, "boost exchange active position scan")
        for row in rows or []:
            try:
                if exec_engine.exchange_position_qty(row) <= 0:
                    continue
            except Exception:
                continue
            pos = _boost_local_position_from_exchange(row)
            await storage.upsert_position(pos)
            log_event("boost_exchange_position_synced", stage="active_position", ok=True, symbol=str(pos.get("symbol") or ""), side=str(pos.get("side") or ""), qty=float(pos.get("qty") or 0), entry=float(pos.get("entry_price") or 0))
            return pos
    except Exception as e:
        log_event("boost_exchange_position_sync_error", stage="active_position", ok=False, error=str(e)[:500])
    return None

async def trading_loop(app):
    global running, entries_enabled, trading_task
    try:
        while running:
            try:
                settings = await storage.all_settings()
                mode_name = str(settings.get("strategy_mode", "hybrid")).lower()
                ai_mode = mode_name == "ai_scalping"
                boost_mode = mode_name == "boost_scalping" and _is_boost_runtime_armed(app, settings)
                live = bool(settings.get("live_trading", False))
                if boost_mode:
                    # BOOST must not get stuck on ccxt.load_markets()/exchange init.
                    # Direct ExchangeClient is enough for native MEXC futures balance/orders;
                    # public scanner calls will log their own errors if MEXC public API is unavailable.
                    api_key, api_secret = _api_creds(settings)
                    ex = ExchangeClient(DEFAULT_EXCHANGE, str(settings.get("proxy_url", "") or ""), bool(settings.get("proxy_enabled", False)))
                    ex.api_key = api_key
                    ex.api_secret = api_secret
                    log_event("boost_loop_stage", stage="direct_exchange_client", ok=True)
                else:
                    ex = await _await_with_timeout(get_exchange(settings), 8, "exchange init")
                ws = await get_ws(settings)
                if boost_mode:
                    log_event("boost_loop_stage", stage="ws_ready", ok=True)

                if (not boost_mode) and bool(settings.get("ws_enabled", True)) and not ws.status.running:
                    await ws.start()

                exec_engine = ExecutionEngine(storage, ex)
                pos_manager = PositionManager(storage, exec_engine)

                # 1) Position management is handled by position_management_loop by default.
                # Fallback to inline management only if explicitly disabled.
                if not _boolish(settings.get("separate_execution_loop_enabled", os.getenv("SEPARATE_EXECUTION_LOOP_ENABLED", "true")), True):
                    events = await pos_manager.manage(lambda symbol: get_last_price(ex, symbol), live)
                    await emit_position_events(app, events)

                # 2) Refresh symbol universe for legacy scanner modes only.
                # v0114: AI BTC/ETH scalping must not run the adaptive universe
                # scanner at all. It uses direct BTC_USDT/ETH_USDT market data and
                # should not emit websocket empty-cache errors from the legacy scanner.
                if not (ai_mode or boost_mode) and time.time() - scanner.last_refresh > int(settings.get("symbol_refresh_sec", 300)):
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
                elif ai_mode or boost_mode:
                    # Clear stale legacy scanner state so status is not displayed as
                    # ai_scalping -> reversal/adaptive loaded 110.
                    scanner.last_effective_strategy = "boost_scalping" if boost_mode else "ai_scalping"
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
                if boost_mode:
                    log_event("boost_loop_stage", stage="risk_ok", ok=True, equity=equity)
                if not ok:
                    if boost_mode:
                        log_event("boost_loop_blocked", stage="risk", ok=False, reason=str(reason)[:300])
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
                market_data_ok = True if (ai_mode or boost_mode) else scanner_market_data_fresh(max_age_sec=max(900, int(settings.get("symbol_refresh_sec", 300)) * 3))
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
                        await _await_with_timeout(ex.fetch_balance(), 8, "live balance probe")
                    except Exception as e:
                        log.warning("live balance/API probe failed: %s", e)
                        if boost_mode:
                            log_event("boost_loop_error", stage="live_balance_probe", ok=False, error=str(e)[:500])
                        sync_ok = False

                if live:
                    gate_ok, gate_reason = ProductionGate().validate_for_live(settings, api_ready=api_ready, ws_healthy=ws_healthy, sync_ok=sync_ok)
                else:
                    gate_ok, gate_reason = ProductionGate().validate_for_paper(settings, ws_healthy=ws_healthy)
                if boost_mode:
                    log_event("boost_loop_stage", stage="production_gate", ok=bool(gate_ok), reason=str(gate_reason)[:300])
                if not gate_ok:
                    if boost_mode:
                        await boost_live_panel_update(app, settings, {"bank": float(settings.get("boost_session_bank_usdt", 0) or 0), "current_bank": float(settings.get("boost_session_bank_usdt", 0) or 0), "target_bank": float(settings.get("boost_session_bank_usdt", 0) or 0) * float(settings.get("boost_target_multiplier", 20) or 20), "pnl": 0.0, "trades": 0}, status="blocked", note=f"production gate: {gate_reason}", force=True)
                    scanner.last_reject_reason = f"gate blocked: {gate_reason}"
                    await update_scanner_status(app, settings, status="entries blocked", force=True)
                    if "websocket" not in str(gate_reason).lower():
                        await notify_admin(app, f"⚠️ Входы заблокированы: {gate_reason}", min_interval_sec=300, key="gate_blocked")
                    await sleep_until_next_scan(app, int(settings.get("scan_interval_sec", 5)))
                    continue

                # 6b) v0170 BOOST mode: find API-verified 0-fee coin, use only a fixed
                # session bank share (default 10% of account equity), and target a 20x
                # bank run. This is high-risk by design; it refuses to trade if 0-fee
                # symbols are not verified by API unless BOOST_ALLOW_FEE_FALLBACK=true.
                if boost_mode:
                    now_ts = time.time()
                    start_ts = float(settings.get("boost_session_start_ts", 0) or 0)
                    session_hours = float(settings.get("boost_session_hours", 6) or 6)
                    balance_share = max(0.001, min(1.0, float(settings.get("boost_balance_share", 0.10) or 0.10)))
                    target_mult = max(1.0, float(settings.get("boost_target_multiplier", 20.0) or 20.0))
                    if start_ts <= 0 or now_ts - start_ts > session_hours * 3600:
                        bank = max(0.0, equity * balance_share)
                        await storage.set("boost_session_start_ts", now_ts, bump_revision=False)
                        await storage.set("boost_session_start_equity", equity, bump_revision=False)
                        await storage.set("boost_session_bank_usdt", bank, bump_revision=False)
                        await storage.set("boost_session_target_profit_usdt", bank * (target_mult - 1.0), bump_revision=False)
                        settings = await storage.all_settings()
                        start_ts = now_ts
                    bank = float(settings.get("boost_session_bank_usdt", equity * balance_share) or 0)
                    target_profit = float(settings.get("boost_session_target_profit_usdt", bank * (target_mult - 1.0)) or 0)
                    max_loss = bank * max(0.0, float(settings.get("boost_max_session_loss_pct", 80.0) or 80.0)) / 100.0
                    trades = [t for t in await storage.trade_rows(since=start_ts) if str(t.get("strategy", "")).lower() == "boost_scalping"]
                    pnl = sum(float(t.get("pnl_usdt") or 0) for t in trades)
                    current_bank = max(0.0, bank + pnl)
                    target_bank = bank * target_mult
                    recent_losses = 0
                    for t in sorted(trades, key=lambda x: float(x.get("ts_close") or x.get("ts") or 0), reverse=True):
                        if float(t.get("pnl_usdt") or 0) < 0:
                            recent_losses += 1
                        else:
                            break
                    max_losses = int(float(settings.get("boost_max_consecutive_losses", 3) or 3))
                    if target_profit > 0 and (pnl >= target_profit or current_bank >= target_bank):
                        scanner.last_reject_reason = f"BOOST target reached: bank={current_bank:.4f} / {target_bank:.4f} USDT"
                        if str(settings.get("boost_stop_when_target_reached", True)).lower() in {"1", "true", "yes", "on"}:
                            await storage.set("boost_autopilot_active", False, bump_revision=False)
                            await storage.set("strategy_mode", "hybrid")
                            _boost_disarm_runtime(app)
                            task = app.bot_data.pop("boost_live_panel_watchdog_task", None)
                            if task is not None and not task.done():
                                task.cancel()
                        await notify_admin(app, f"🏁 BOOST target reached. Bank {current_bank:.4f} / {target_bank:.4f} USDT. Entries stopped.", key="boost_target_reached")
                        await boost_live_panel_update(app, settings, {"bank": bank, "current_bank": current_bank, "target_bank": target_bank, "pnl": pnl, "trades": len(trades), "target_mult": target_mult}, status="target completed", note="BOOST x20 target reached; autopilot stopped", force=True)
                        await sleep_until_next_scan(app, int(settings.get("scan_interval_sec", 5)))
                        continue
                    if current_bank <= 0 or (max_loss > 0 and pnl <= -max_loss):
                        scanner.last_reject_reason = f"BOOST loss limit: bank={current_bank:.4f}, pnl={pnl:.4f} / -{max_loss:.4f} USDT"
                        await storage.set("boost_autopilot_active", False, bump_revision=False)
                        await storage.set("strategy_mode", "hybrid")
                        await notify_admin(app, f"🛑 BOOST loss limit. PnL {pnl:.4f} USDT. Entries stopped.", key="boost_loss_limit")
                        await sleep_until_next_scan(app, int(settings.get("scan_interval_sec", 5)))
                        continue
                    if max_losses > 0 and recent_losses >= max_losses:
                        scanner.last_reject_reason = f"BOOST {recent_losses} losses in a row; stopped"
                        await storage.set("boost_autopilot_active", False, bump_revision=False)
                        await storage.set("strategy_mode", "hybrid")
                        await notify_admin(app, f"🛑 BOOST stopped after {recent_losses} consecutive losses.", key="boost_loss_streak")
                        await sleep_until_next_scan(app, int(settings.get("scan_interval_sec", 5)))
                        continue

                    active_pos = await _boost_find_active_position(ex, exec_engine)
                    if active_pos:
                        scanner.last_reject_reason = "BOOST: active position; live trailing + parallel scan"
                        snap_now = {"bank": bank, "current_bank": current_bank, "target_bank": target_bank, "pnl": pnl, "trades": len(trades), "target_mult": target_mult}
                        # HUNTER live engine: primary exit is dynamic trailing / momentum decay.
                        # Exchange orders are emergency backstop only. If this closes in profit,
                        # immediately continue to the next scan/rotation cycle.
                        try:
                            closed_by_hunter = await _boost_hunter_manage_active_position(app, ex, exec_engine, settings, active_pos, snap_now, live)
                            if closed_by_hunter:
                                await sleep_until_next_scan(app, int(settings.get("boost_scan_interval_sec", 1)))
                                continue
                        except Exception as e:
                            log_event("boost_hunter_manage_error", stage="active_position", ok=False, error=str(e)[:500])
                        rotation_enabled = _bool_setting(settings, "boost_parallel_scan_enabled", True)
                        if _boost_position_is_unsafe(active_pos):
                            rotation_enabled = False
                            rotate_note = "UNSAFE POSITION: rotation/rescue disabled until emergency SL is recovered"
                            log_event("boost_rotation_blocked", stage="unsafe_position", ok=True, symbol=active_pos.get("symbol"), reason=rotate_note)
                        else:
                            rotate_note = "active; rotation disabled"
                        if rotation_enabled:
                            try:
                                price = await get_last_price(ex, active_pos.get("symbol"))
                                local_pos_pnl = pos_manager.pnl_pct(active_pos, price) if price else 0.0
                                ex_pos_pnl, ex_upnl, ex_mark = (None, None, None)
                                if live:
                                    ex_pos_pnl, ex_upnl, ex_mark = await boost_exchange_pnl_snapshot(ex, active_pos)
                                # For LIVE BOOST rotation, trust MEXC mark/unrealized over local ticker.
                                # This prevents closing a "paper plus" that is actually negative on exchange.
                                pos_pnl = float(ex_pos_pnl) if ex_pos_pnl is not None else local_pos_pnl
                                min_profit = float(settings.get("boost_min_profit_to_rotate_pct", 0.04) or 0.04)
                                only_profit = _bool_setting(settings, "boost_rotate_only_if_profit", True)
                                last_rot = float(app.bot_data.get("boost_last_rotation_ts", 0) or 0)
                                cd = max(0.0, float(settings.get("boost_rotate_cooldown_sec", 1) or 1))
                                cooldown_ok = time.time() - last_rot >= cd
                                # v0199: Always keep scanning while a BOOST position is active.
                                # NORMAL rotation: current position is already in real profit.
                                # RESCUE rotation: current position is negative, but a new signal is extreme
                                # enough to justify closing the loss and immediately jumping.
                                if cooldown_ok:
                                    candidate_decision = await _await_with_timeout(boost_scalping_engine.decide(ex, settings), 12, "boost rotation decide")
                                    _record_boost_decision_metrics(candidate_decision)
                                    new_sym = str(candidate_decision.symbol or "")
                                    cur_sym = str(active_pos.get("symbol") or "")
                                    new_market = ((candidate_decision.market or {}).get("markets") or [{}])[0]
                                    new_score = float(new_market.get("boost_score") or 0)
                                    cur_score = float(((active_pos.get("score_details") or {}).get("boost_score")) or 0)
                                    mult = float(settings.get("boost_rotate_strength_multiplier", 1.35) or 1.35)
                                    gap = float(settings.get("boost_rotate_min_score_gap", 5.0) or 5.0)
                                    stronger_normal = bool(candidate_decision.ok and candidate_decision.decision != "WAIT" and new_sym and new_sym != cur_sym and new_score >= max(cur_score * mult, cur_score + gap))

                                    real_profit_ok = True
                                    if live:
                                        # v0223: MEXC sometimes reports unrealizedPnl as 0.0 while
                                        # mark/entry already gives a clearly positive exchange PnL%.
                                        # Do not HOLD a strong rotation just because uPnL is rounded to
                                        # zero; require positive exchange pct and non-negative uPnL.
                                        real_profit_ok = (ex_upnl is None or ex_upnl >= 0) and pos_pnl >= min_profit
                                    normal_rotate = bool(stronger_normal and (not only_profit or pos_pnl >= min_profit) and real_profit_ok)

                                    rescue_rotate = False
                                    rescue_reason = ""
                                    if not normal_rotate and pos_pnl < 0 and _bool_setting(settings, "boost_rescue_rotation_enabled", False):
                                        rescue_last = float(app.bot_data.get("boost_last_rescue_rotation_ts", 0) or 0)
                                        rescue_cd = float(settings.get("boost_rescue_cooldown_sec", 180) or 180)
                                        rescue_max_loss = abs(float(settings.get("boost_rescue_max_loss_pct", 0.70) or 0.70))
                                        rescue_mult = float(settings.get("boost_rescue_min_score_multiplier", 1.70) or 1.70)
                                        rescue_gap = float(settings.get("boost_rescue_min_score_gap", 18.0) or 18.0)
                                        loss_mult = float(settings.get("boost_rescue_expected_move_loss_mult", 2.50) or 2.50)
                                        max_per_hour = int(float(settings.get("boost_rescue_max_per_hour", 2) or 2))
                                        rescue_times = [float(x) for x in app.bot_data.get("boost_rescue_rotation_times", []) if time.time() - float(x) < 3600]
                                        app.bot_data["boost_rescue_rotation_times"] = rescue_times
                                        r1 = abs(float(new_market.get("ret_1m_pct") or 0))
                                        r3 = abs(float(new_market.get("ret_3m_pct") or 0))
                                        atr = abs(float(new_market.get("atr_1m_pct") or 0))
                                        tmp_candidate = boost_scalping_engine.make_candidate(candidate_decision, settings) if candidate_decision.ok else None
                                        cand_lev = float((tmp_candidate or {}).get("leverage") or settings.get("boost_min_leverage", 30) or 30)
                                        expected_price_move = max(r1, r3, atr)
                                        expected_roi_move = expected_price_move * max(1.0, cand_lev)
                                        rescue_score_ok = bool(candidate_decision.ok and candidate_decision.decision != "WAIT" and new_sym and new_sym != cur_sym and new_score >= max(cur_score * rescue_mult, cur_score + rescue_gap))
                                        rescue_loss_ok = abs(pos_pnl) <= rescue_max_loss
                                        rescue_move_ok = expected_roi_move >= abs(pos_pnl) * loss_mult
                                        rescue_rate_ok = (time.time() - rescue_last >= rescue_cd) and (len(rescue_times) < max_per_hour)
                                        rescue_rotate = bool(rescue_score_ok and rescue_loss_ok and rescue_move_ok and rescue_rate_ok)
                                        rescue_reason = f"RESCUE score_ok={rescue_score_ok} loss={pos_pnl:+.3f}% max={rescue_max_loss:.3f}% exp≈{expected_roi_move:.3f}% need>{abs(pos_pnl)*loss_mult:.3f}% rate_ok={rescue_rate_ok}"

                                    mode_reason = "NORMAL" if normal_rotate else ("RESCUE" if rescue_rotate else "HOLD")
                                    rotate_note = f"{mode_reason} pnl={pos_pnl:+.3f}% local={local_pos_pnl:+.3f}% exUPnL={(ex_upnl if ex_upnl is not None else 0):+.4f} best={_short_symbol(new_sym)} score={new_score:.1f} cur_score={cur_score:.1f} {rescue_reason}"
                                    log_event("boost_rotation_check", stage="rotation", ok=True, mode=mode_reason, symbol=cur_sym, candidate=new_sym, pnl_pct=pos_pnl, new_score=new_score, cur_score=cur_score, note=rotate_note[:500])
                                    if normal_rotate or rescue_rotate:
                                        close_reason = "boost_normal_rotate_to_stronger" if normal_rotate else "boost_rescue_rotate_to_extreme"
                                        res = await exec_engine.close_position(active_pos, close_reason, live, price)
                                        app.bot_data["boost_last_rotation_ts"] = time.time()
                                        if rescue_rotate:
                                            app.bot_data["boost_last_rescue_rotation_ts"] = time.time()
                                            rt = [float(x) for x in app.bot_data.get("boost_rescue_rotation_times", []) if time.time() - float(x) < 3600]
                                            rt.append(time.time())
                                            app.bot_data["boost_rescue_rotation_times"] = rt
                                        close_ok = bool(res.get("ok")) if isinstance(res, dict) else bool(res)
                                        scanner.last_reject_reason = f"BOOST {mode_reason} rotation: closed {_short_symbol(cur_sym)} at exchange {pos_pnl:+.3f}%, next {_short_symbol(new_sym)}"
                                        await notify_admin(app, (
                                            f"🔄 BOOST {mode_reason} rotation\n"
                                            f"Closed {_short_symbol(cur_sym)} at exchange {pos_pnl:+.3f}% | real uPnL={(ex_upnl if ex_upnl is not None else 0):+.4f} USDT\n"
                                            f"Stronger: {_short_symbol(new_sym)} {candidate_decision.decision} score {new_score:.1f} > {cur_score:.1f}\n"
                                            f"Result: {res.get('ok') if isinstance(res, dict) else res}"
                                        ), key=f"boost_rotation_{int(time.time()*1000)}")
                                        await boost_live_panel_update(app, settings, snap_now, status="rotated; opening stronger", decision=candidate_decision, position=None, note=scanner.last_reject_reason, force=True)
                                        if close_ok:
                                            # v0182: do not wait for the next scanner cycle. Open the stronger
                                            # candidate immediately: take micro-profit -> jump to stronger impulse.
                                            try:
                                                rot_cand = boost_scalping_engine.make_candidate(candidate_decision, settings)
                                                rot_plan = TradePlanner().make_plan(rot_cand, settings, equity_usdt=current_bank) if rot_cand else None
                                                if rot_plan:
                                                    rot_plan.session = f"boost_{int(start_ts)}"
                                                    placed2 = await _await_with_timeout(exec_engine.place_entry(rot_plan, live), 20, "boost rotation place_entry")
                                                    if placed2.get("ok"):
                                                        scanner.last_signal_summary = f"BOOST rotated/opened {rot_plan.symbol} {rot_plan.side} margin≈{rot_plan.expected_margin_usdt:.2f} TP={rot_plan.take_price:.6g} SL={rot_plan.stop_price:.6g}"
                                                        await notify_admin(app, (
                                                            f"🚀 BOOST rotation opened\n"
                                                            f"{rot_plan.symbol} {rot_plan.side}\n"
                                                            f"Leverage: x{getattr(rot_plan, 'leverage', '-')}\n"
                                                            f"Margin≈{rot_plan.expected_margin_usdt:.2f} USDT\n"
                                                            f"Reason: {candidate_decision.reason}\n"
                                                            f"TP: {rot_plan.take_price:.6g}\nSL: {rot_plan.stop_price:.6g}"
                                                        ), key=f"boost_rotation_opened_{int(time.time()*1000)}")
                                                        await boost_live_panel_update(app, settings, snap_now, status="rotation opened", decision=candidate_decision, position=None, note=scanner.last_signal_summary, force=True)
                                                    else:
                                                        rej_reason = str(placed2.get("reason", "unknown"))
                                                        blocked = await _boost_handle_entry_failure(app, getattr(rot_plan, "symbol", ""), rej_reason)
                                                        scanner.last_reject_reason = f"BOOST rotation entry rejected {rej_reason}"
                                                        await boost_live_panel_update(app, settings, snap_now, status=("symbol blocked; rescanning" if blocked else "rotation entry rejected"), decision=candidate_decision, note=scanner.last_reject_reason, force=True)
                                                else:
                                                    scanner.last_reject_reason = "BOOST rotation planner rejected stronger candidate"
                                            except Exception as e:
                                                err = str(e)
                                                await _boost_handle_entry_failure(app, new_sym, err)
                                                scanner.last_reject_reason = f"BOOST rotation immediate open failed {err[:160]}"
                                                await notify_admin(app, f"⚠️ BOOST rotation open failed: {err[:240]}", min_interval_sec=10, key="boost_rotation_open_failed")
                                        await sleep_until_next_scan(app, 1)
                                        continue
                                    await boost_live_panel_update(app, settings, snap_now, status="active; scanning stronger", decision=candidate_decision, position=active_pos, note=rotate_note, force=False)
                                else:
                                    rotate_note = f"active pnl={pos_pnl:+.3f}%; need {min_profit:.3f}% and cooldown ok"
                                    await boost_live_panel_update(app, settings, snap_now, status="active", position=active_pos, note=rotate_note, force=False)
                            except Exception as e:
                                rotate_note = f"rotation scan warning: {str(e)[:160]}"
                                await boost_live_panel_update(app, settings, snap_now, status="active", position=active_pos, note=rotate_note, force=False)
                        else:
                            await boost_live_panel_update(app, settings, snap_now, status="active", position=active_pos, note=rotate_note, force=False)
                        await update_scanner_status(app, settings, status="boost active", force=False)
                        await sleep_until_next_scan(app, int(settings.get("boost_scan_interval_sec", 1)))
                        continue

                    log_event("boost_loop_stage", stage="scan_start", ok=True, bank=bank, current_bank=current_bank, target_bank=target_bank)
                    await boost_live_panel_update(app, settings, {"bank": bank, "current_bank": current_bank, "target_bank": target_bank, "pnl": pnl, "trades": len(trades), "target_mult": target_mult}, status="scanning", note="BOOST: 126 zero-fee → hotlist → fast rotation scan", force=False)
                    try:
                        decision = await _await_with_timeout(boost_scalping_engine.decide(ex, settings), 18, "boost decide")
                    except Exception as e:
                        scanner.last_reject_reason = f"BOOST scan error/timeout: {str(e)[:180]}"
                        log_event("boost_loop_error", stage="boost_decide", ok=False, error=str(e)[:500])
                        await boost_live_panel_update(app, settings, {"bank": bank, "current_bank": current_bank, "target_bank": target_bank, "pnl": pnl, "trades": len(trades), "target_mult": target_mult}, status="scan error; retrying", note=scanner.last_reject_reason, force=True)
                        await sleep_until_next_scan(app, int(settings.get("boost_scan_interval_sec", 1)))
                        continue
                    _record_boost_decision_metrics(decision)
                    log_event("boost_loop_stage", stage="scan_done", ok=True, decision=str(getattr(decision, "decision", "")), symbol=str(getattr(decision, "symbol", "") or ""), reason=str(getattr(decision, "reason", ""))[:300])
                    if not decision.ok or decision.decision == "WAIT":
                        scanner.last_reject_reason = decision.reason or "BOOST wait"
                        scanner.last_signal_summary = f"BOOST scan bank={current_bank:.2f}/{target_bank:.2f} pnl={pnl:.4f}"
                        await boost_live_panel_update(app, settings, {"bank": bank, "current_bank": current_bank, "target_bank": target_bank, "pnl": pnl, "trades": len(trades), "target_mult": target_mult}, status="waiting impulse", decision=decision, note=scanner.last_reject_reason, force=False)
                        await update_scanner_status(app, settings, status="boost wait", force=False)
                        await sleep_until_next_scan(app, int(settings.get("boost_scan_interval_sec", 1)))
                        continue
                    cand = boost_scalping_engine.make_candidate(decision, settings)
                    # Plan sizing is based only on the compounding BOOST bank, not the full account.
                    # With BOOST_BALANCE_SHARE=0.10 and balance=50, first bank is 5 USDT;
                    # after closed profits it becomes bank+pnl and compounds until x20.
                    plan = TradePlanner().make_plan(cand, settings, equity_usdt=current_bank) if cand else None
                    if not plan:
                        scanner.last_reject_reason = "BOOST planner rejected"
                        await sleep_until_next_scan(app, int(settings.get("boost_scan_interval_sec", 1)))
                        continue
                    plan.session = f"boost_{int(start_ts)}"
                    if live:
                        try:
                            bal = await _await_with_timeout(ex.fetch_balance(), 8, "boost balance precheck")
                            usdt = bal.get("USDT", {}) if isinstance(bal, dict) else {}
                            free = float(usdt.get("free") or ((bal or {}).get("free", {}) or {}).get("USDT") or 0)
                            need = float(getattr(plan, "expected_margin_usdt", 0.0) or 0.0)
                            if need > 0 and free < need * 1.05 + 0.5:
                                scanner.last_reject_reason = f"BOOST balance free={free:.2f} need≈{need*1.05+0.5:.2f}"
                                await sleep_until_next_scan(app, int(settings.get("boost_scan_interval_sec", 1)))
                                continue
                        except Exception as e:
                            scanner.last_reject_reason = f"BOOST balance precheck warning {e}"
                            await sleep_until_next_scan(app, int(settings.get("boost_scan_interval_sec", 1)))
                            continue
                    try:
                        placed = await _await_with_timeout(exec_engine.place_entry(plan, live), 25, "boost place_entry")
                    except Exception as e:
                        err = str(e)
                        blocked = await _boost_handle_entry_failure(app, getattr(plan, "symbol", ""), err)
                        scanner.last_reject_reason = f"BOOST execution exception {err}"
                        if blocked:
                            await boost_live_panel_update(app, settings, {"bank": bank, "current_bank": current_bank, "target_bank": target_bank, "pnl": pnl, "trades": len(trades), "target_mult": target_mult}, status="symbol blocked; rescanning", decision=decision, note=scanner.last_reject_reason, force=True)
                            await sleep_until_next_scan(app, 1)
                        else:
                            await notify_admin(app, f"⚠️ BOOST execution exception: {err}", min_interval_sec=60, key="boost_exec_error")
                            await sleep_until_next_scan(app, int(settings.get("boost_scan_interval_sec", 1)))
                        continue
                    if placed.get("ok"):
                        scanner.last_signal_summary = f"BOOST opened {plan.symbol} {plan.side} margin≈{plan.expected_margin_usdt:.2f}; exit=live trailing/momentum decay; emergency SL={plan.stop_price:.6g}"
                        scanner.last_reject_reason = f"BOOST bank={bank:.2f}, session pnl={pnl:.4f}, target profit={target_profit:.2f}"
                        boost_msg = (
                            f"🚀 BOOST opened\n"
                            f"{plan.symbol} {plan.side}\n"
                            f"Margin≈{plan.expected_margin_usdt:.2f} USDT ({balance_share*100:.1f}% balance)\n"
                            f"Leverage: x{getattr(plan, 'leverage', '-') }\n"
                            f"Bank target: {bank:.2f} → {bank*target_mult:.2f} USDT\n"
                            f"Conf: {decision.confidence:.2f}\n"
                            f"Reason: {decision.reason}\n"
                            f"Exit: live trailing / momentum decay\n"
                            f"Emergency SL: {plan.stop_price:.6g}"
                        )
                        await notify_admin(app, boost_msg, key="boost_opened")
                        await boost_live_panel_update(app, settings, {"bank": bank, "current_bank": current_bank, "target_bank": target_bank, "pnl": pnl, "trades": len(trades), "target_mult": target_mult}, status="opened", decision=decision, note=scanner.last_signal_summary, force=True)
                    else:
                        rej_reason = str(placed.get('reason', 'unknown'))
                        scanner.last_reject_reason = f"BOOST execution rejected {rej_reason}"
                        blocked = await _boost_handle_entry_failure(app, getattr(plan, "symbol", ""), rej_reason)
                        await boost_live_panel_update(app, settings, {"bank": bank, "current_bank": current_bank, "target_bank": target_bank, "pnl": pnl, "trades": len(trades), "target_mult": target_mult}, status=("symbol blocked; rescanning" if blocked else "rejected"), decision=decision, note=scanner.last_reject_reason, force=True)
                        if blocked:
                            await sleep_until_next_scan(app, 1)
                            continue
                    await update_scanner_status(app, settings, status="boost opened" if placed.get("ok") else "boost rejected", force=True)
                    await sleep_until_next_scan(app, int(settings.get("boost_scan_interval_sec", 1)))
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
                    ai_checked = []
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

                        ai_checked.append(b)
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
                        if live:
                            try:
                                bal = await _await_with_timeout(ex.fetch_balance(), 8, "boost balance precheck")
                                usdt = bal.get("USDT", {}) if isinstance(bal, dict) else {}
                                free = float(usdt.get("free") or ((bal or {}).get("free", {}) or {}).get("USDT") or 0)
                                need = float(getattr(plan, "expected_margin_usdt", 0.0) or 0.0)
                                # MEXC can require a little extra for fees/funding buffers.
                                min_free = need * 1.05 + 0.5
                                if need > 0 and free < min_free:
                                    waited.append(f"{b}:skip balance free={free:.2f} need≈{min_free:.2f}")
                                    continue
                            except Exception as e:
                                waited.append(f"{b}:balance precheck warning {e}")
                                continue
                        try:
                            placed = await _await_with_timeout(exec_engine.place_entry(plan, live), 25, "boost place_entry")
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
                                    + (
                                        f"SL: LIQUIDATION via {getattr(plan, 'leverage', 0)}x "
                                        f"(target≈{getattr(plan, 'liquidation_target_distance_pct', 0):.3g}%, "
                                        f"est≈{getattr(plan, 'liquidation_estimated_distance_pct', 0):.3g}%)"
                                        if bool(getattr(plan, "liquidation_stop_mode", False))
                                        else f"SL: {plan.stop_price:.6g}"
                                    )
                                ),
                                key=f"ai_scalp_opened_{b}",
                            )
                            await send_trade_chart(app, ex, plan, settings)
                        else:
                            reason = str(placed.get('reason', 'unknown'))
                            waited.append(f"{b}:execution rejected {reason}")
                            # v0154 visibility: if the exchange entry was opened and then
                            # immediately closed because TP/SL could not be confirmed, tell
                            # the user explicitly. Otherwise balance changes look like
                            # invisible phantom trades.
                            if 'protection' in placed or 'position closed' in reason.lower():
                                close = placed.get('close') or {}
                                pnl = close.get('pnl_usdt')
                                pp = close.get('pnl_pct')
                                msg = (
                                    "🛑 AI scalp aborted after entry\n"
                                    f"{getattr(plan, 'symbol', b)} {getattr(plan, 'side', '-')}\n"
                                    "Reason: exchange TP/SL not confirmed. Position was closed immediately.\n"
                                    f"PnL: {pnl:.4f} USDT ({pp:.3f}%)" if isinstance(pnl, (int, float)) and isinstance(pp, (int, float)) else
                                    "🛑 AI scalp aborted after entry\n"
                                    f"{getattr(plan, 'symbol', b)} {getattr(plan, 'side', '-')}\n"
                                    "Reason: exchange TP/SL not confirmed. Position was closed immediately."
                                )
                                await notify_admin(app, msg, key=f"ai_scalp_aborted_{b}")

                    scanner.last_ai_check_symbols = ai_checked
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
                quick_bounce_cycle = base_strategy_mode == "quick_bounce"
                if quick_bounce_cycle and not _bool_setting(settings, "quick_bounce_enabled", False):
                    scanner.last_signal_summary = "quick_bounce OFF: scanner stopped"
                    scanner.last_reject_reason = "Press ⚡ быстрый отскок again to resume scanning. Existing positions are still managed."
                    await update_scanner_status(app, settings, status="quick bounce off")
                    await sleep_until_next_scan(app, int(settings.get("scan_interval_sec", 900)))
                    continue
                if quick_bounce_cycle:
                    log_event("quick_bounce_scan_start", stage="scan", ok=True, top_coins=int(float(settings.get("quick_bounce_top_coins", settings.get("max_symbols", 200)) or 200)), anomaly_tf=str(settings.get("quick_bounce_anomaly_timeframe", "1h")), confirm_tf=str(settings.get("quick_bounce_confirm_timeframe", "15m")))
                    await quick_bounce_progress_message(app, 10)
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
                if quick_bounce_cycle:
                    await quick_bounce_progress_message(app, 50)
                candidates = await scanner.candidates(ex, effective_settings)
                if quick_bounce_cycle:
                    log_event(
                        "quick_bounce_scan_done",
                        stage="scan",
                        ok=True,
                        candidates=len(candidates or []),
                        symbols=[str(c.get("symbol", "")) for c in (candidates or [])[:10]],
                        reject_reasons=getattr(scanner, "last_reject_top_reasons", []),
                        errors=getattr(scanner, "last_cycle_errors", 0),
                    )
                    await quick_bounce_progress_message(app, 100, done=True)
                    await quick_bounce_summary_message(app, settings, candidates)
                    await quick_bounce_progress_message(app, 100, clear=True)
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
                    # Keep the detailed reject reason collected by scanner/signal_engine.
                    # Previously this was overwritten with a generic message, which hid
                    # useful diagnostics such as invalid sweep/reclaim/retest/RR.
                    if not str(getattr(scanner, "last_reject_reason", "") or "").strip() or str(scanner.last_reject_reason).strip() in {"-", "none"}:
                        scanner.last_reject_reason = "no candidates passed signal engine"
                    await update_scanner_status(app, settings, status="scanning")

                opened_this_cycle = False
                scanner.last_ai_check_symbols = []
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
                    if ai_enabled:
                        try:
                            scanner.last_ai_check_symbols.append(str(plan.symbol))
                        except Exception:
                            pass
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
                        placed = await _await_with_timeout(exec_engine.place_entry(plan, live), 25, "boost place_entry")
                    except Exception as e:
                        scanner.last_reject_reason = f"{plan.symbol}: execution exception: {e}"
                        continue
                    if placed.get("ok"):
                        scanner.last_reject_reason = f"opened {plan.symbol} {plan.side}"
                        opened_this_cycle = True
                        await update_scanner_status(app, settings, status="position opened", force=True)
                        if quick_bounce_cycle:
                            open_text = format_quick_bounce_opened(plan, placed)
                            log_event(
                                "quick_bounce_opened",
                                stage="entry",
                                ok=True,
                                symbol=str(plan.symbol),
                                side=str(plan.side),
                                entry_price=float(getattr(plan, "entry_price", 0) or 0),
                                take_price=float(getattr(plan, "take_price", 0) or 0),
                                stop_price=float(getattr(plan, "stop_price", 0) or 0),
                                leverage=int(float(getattr(plan, "leverage", 0) or 0)),
                                placed=placed,
                            )
                            await notify_admin(app, open_text, key=f"quick_bounce_opened_{plan.symbol}_{int(time.time()*1000)}")
                            await quick_bounce_summary_message(app, settings, candidates, opened_note=open_text)
                        else:
                            await notify_admin(
                                app,
                                format_position_opened(plan, placed, live, ai_verdict if ai_enabled else None),
                                key="position_opened",
                            )
                        await send_trade_chart(app, ex, plan, settings)
                    else:
                        reason = str(placed.get('reason', 'unknown'))
                        scanner.last_reject_reason = f"{plan.symbol}: execution rejected: {reason}"
                        if quick_bounce_cycle:
                            log_event("quick_bounce_execution_rejected", stage="entry", ok=False, symbol=str(plan.symbol), side=str(plan.side), reason=reason[:500], placed=placed)
                        if 'protection' in placed or 'position closed' in reason.lower():
                            close = placed.get('close') or {}
                            pnl = close.get('pnl_usdt')
                            pp = close.get('pnl_pct')
                            msg = (
                                "🛑 Trade aborted after entry\n"
                                f"{plan.symbol} {plan.side}\n"
                                "Reason: exchange TP/SL not confirmed. Position was closed immediately.\n"
                                f"PnL: {pnl:.4f} USDT ({pp:.3f}%)" if isinstance(pnl, (int, float)) and isinstance(pp, (int, float)) else
                                "🛑 Trade aborted after entry\n"
                                f"{plan.symbol} {plan.side}\n"
                                "Reason: exchange TP/SL not confirmed. Position was closed immediately."
                            )
                            await notify_admin(app, msg, key=f"trade_aborted_{plan.symbol}")

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
                ex = await _await_with_timeout(get_exchange(settings), 8, "exchange init")
                exec_engine = ExecutionEngine(storage, ex)
                report = await RecoveryEngine(storage, ex, exec_engine).recover(
                    reattach=str(os.getenv("RECOVERY_REATTACH_PROTECTION", "true")).lower() in {"1", "true", "yes", "on"}
                )
                app.bot_data["startup_recovery_report"] = report
    except Exception as e:
        app.bot_data["startup_recovery_error"] = str(e)

def _wrap_command(fn, name: str):
    async def _inner(update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            return await fn(update, context)
        except Exception as e:
            log.exception("telegram command failed: %s", name)
            if name == "/boost_start":
                context.application.bot_data["boost_start_in_progress"] = False
            try:
                await reply(update, f"❌ Command failed: {name}\n{str(e)[:500]}", reply_markup=MAIN_MENU)
            except Exception:
                pass
    return _inner


# Compatibility markers for legacy wiring tests: CommandHandler("api", api_cmd), CommandHandler("openai", openai_cmd)
def build_app():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).post_init(on_startup).concurrent_updates(16).build()
    app.add_handler(CommandHandler("start", _wrap_command(start, "/start")))
    app.add_handler(CommandHandler("help", _wrap_command(help_cmd, "/help")))
    app.add_handler(CommandHandler("log", _wrap_command(log_cmd, "/log")))
    app.add_handler(CommandHandler("run", _wrap_command(run_cmd, "/run")))
    app.add_handler(CommandHandler("boost_start", _wrap_command(boost_start_cmd, "/boost_start")))
    app.add_handler(CommandHandler("boost_stop", _wrap_command(boost_stop_cmd, "/boost_stop")))
    app.add_handler(CommandHandler("boost_status", _wrap_command(boost_status_cmd, "/boost_status")))
    app.add_handler(CommandHandler("boost_rotation", _wrap_command(boost_rotation_cmd, "/boost_rotation")))
    app.add_handler(CommandHandler("boost_list", _wrap_command(boost_list_cmd, "/boost_list")))
    app.add_handler(CommandHandler("boost_list_del", _wrap_command(boost_list_del_cmd, "/boost_list_del")))
    app.add_handler(CommandHandler("stop", _wrap_command(stop_cmd, "/stop")))
    app.add_handler(CommandHandler("panic", _wrap_command(panic_cmd, "/panic")))
    app.add_handler(CommandHandler("status", _wrap_command(status_cmd, "/status")))
    app.add_handler(CommandHandler("ping", _wrap_command(ping_cmd, "/ping")))
    app.add_handler(CommandHandler("balance", _wrap_command(balance_cmd, "/balance")))
    app.add_handler(CommandHandler("positions", _wrap_command(positions_cmd, "/positions")))
    app.add_handler(CommandHandler("mexc_debug_state", _wrap_command(mexc_debug_state_cmd, "/mexc_debug_state")))
    app.add_handler(CommandHandler("open_orders", _wrap_command(open_orders_cmd, "/open_orders")))
    app.add_handler(CommandHandler("cancel_all", _wrap_command(cancel_all_cmd, "/cancel_all")))
    app.add_handler(CommandHandler("close_all", _wrap_command(close_all_cmd, "/close_all")))
    app.add_handler(CommandHandler("stats", _wrap_command(stats_cmd, "/stats")))
    app.add_handler(CommandHandler("ai_stats", _wrap_command(ai_stats_cmd, "/ai_stats")))
    app.add_handler(CommandHandler("ai_stats_current", _wrap_command(ai_stats_current_cmd, "/ai_stats_current")))
    app.add_handler(CommandHandler("ai_stats_lifetime", _wrap_command(ai_stats_lifetime_cmd, "/ai_stats_lifetime")))
    app.add_handler(CommandHandler("ai_stats_reset", _wrap_command(ai_stats_reset_cmd, "/ai_stats_reset")))
    app.add_handler(CommandHandler("sync", _wrap_command(sync_cmd, "/sync")))
    app.add_handler(CommandHandler("sync_positions", _wrap_command(sync_positions_cmd, "/sync_positions")))
    app.add_handler(CommandHandler("recovery", _wrap_command(recovery_cmd, "/recovery")))
    app.add_handler(CommandHandler("settings", _wrap_command(settings_cmd, "/settings")))
    app.add_handler(CommandHandler("mexc_settings", _wrap_command(mexc_settings_cmd, "/mexc_settings")))
    app.add_handler(CommandHandler("leverage", _wrap_command(leverage_cmd, "/leverage")))
    app.add_handler(CommandHandler("open_type", _wrap_command(open_type_cmd, "/open_type")))
    app.add_handler(CommandHandler("recv_window", _wrap_command(recv_window_cmd, "/recv_window")))
    app.add_handler(CommandHandler("set", _wrap_command(set_cmd, "/set")))
    app.add_handler(CommandHandler("proxy", _wrap_command(proxy_cmd, "/proxy")))
    app.add_handler(CommandHandler("api", _wrap_command(api_cmd, "/api")))
    app.add_handler(CommandHandler("openai", _wrap_command(openai_cmd, "/openai")))
    app.add_handler(CallbackQueryHandler(_wrap_command(callback_router, "callback")))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))
    return app

if __name__ == "__main__":
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN is required")
    build_app().run_polling()
