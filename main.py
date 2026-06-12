import os, time, asyncio, logging, json, re, zipfile
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
from ai_btc_autopilot import BTCVisionAutopilot
from chatgpt_mode import build_chatgpt_log, build_chatgpt_scan_pack, build_chatgpt_runtime_manifest_from_mexc, disable_other_modes, extract_setup_json, execute_setup, chatgpt_log_event, chatgpt_runtime_log_path, tail_chatgpt_runtime_log, build_chatgpt_monitor_text, CHATGPT_MONITOR_INTERVAL_SEC, CHATGPT_SETUP_VERSION
from claude_autopilot import call_claude_for_setup, save_claude_setup_text, normalize_claude_model, claude_model_label, schedule_label, next_schedule_run, schedule_due, CLAUDE_SONNET_46, CLAUDE_OPUS_48, MSK
from claude_runtime_logger import claude_log_event, claude_runtime_log_path, tail_claude_runtime_log
from btc_pattern_backtest import run_btc_pattern_backtest, run_btc_pattern_backtest_1h, run_round_level_backtest, run_strategy_lab_backtest, run_strategy_detail_backtest
from debug_log import tail_text, tail_important, log_event
from runtime_secrets import secret_value, save_secret_backup, clear_secret_backup, apply_secret_backup_to_env, set_runtime_secret_cache, clear_runtime_secret_cache, runtime_secret_cache, ensure_runtime_secrets_loaded, merge_secrets_into_settings, secret_source_report

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
bottom_replace_locks = {}
btc_ai_task = None
claude_scheduler_task = None
monitor_cleanup_task = None

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




# Technical Telegram monitor dedupe: this is NOT trading state/cache.  It only
# remembers ids of monitor cards the bot itself sent, so a small sanitary worker
# can delete duplicate monitor cards if Telegram/client reconnects create visual
# duplicates.  It never touches progress cards, setup lifecycle cards, files or
# trade/exchange state.
MONITOR_DUPLICATE_CLEANUP_KEYS = {"chatgpt_mode_monitor"}
MONITOR_DUPLICATE_KEEP_SEC = 3600
MONITOR_DUPLICATE_CLEANUP_SEC = 300


def _normalize_monitor_recent_records(raw, now: float | None = None) -> list[dict]:
    now = float(now or time.time())
    out = []
    items = raw if isinstance(raw, list) else ([] if raw is None else [raw])
    for item in items:
        mid = None
        ts = now
        if isinstance(item, dict):
            mid = item.get("id") or item.get("message_id")
            try:
                ts = float(item.get("ts") or item.get("time") or now)
            except Exception:
                ts = now
        else:
            mid = item
        try:
            mid = int(mid)
        except Exception:
            continue
        if mid <= 0:
            continue
        if now - ts > MONITOR_DUPLICATE_KEEP_SEC:
            continue
        if not any(int(x.get("id")) == mid for x in out):
            out.append({"id": mid, "ts": ts})
    out.sort(key=lambda x: float(x.get("ts") or 0))
    return out[-240:]


async def _load_monitor_recent_records(app, key: str) -> list[dict]:
    recent_key = f"bottom_recent_monitor_ids_{key}"
    raw = app.bot_data.get(recent_key)
    if raw is None:
        try:
            raw = await storage.get(recent_key, [])
        except Exception:
            raw = []
    records = _normalize_monitor_recent_records(raw)
    app.bot_data[recent_key] = records
    return records


async def _save_monitor_recent_records(app, key: str, records: list[dict]) -> None:
    recent_key = f"bottom_recent_monitor_ids_{key}"
    records = _normalize_monitor_recent_records(records)
    app.bot_data[recent_key] = records
    try:
        await storage.set(recent_key, records, bump_revision=False)
    except Exception:
        pass


async def _record_monitor_message_for_cleanup(app, key: str, message_id) -> None:
    if key not in MONITOR_DUPLICATE_CLEANUP_KEYS:
        return
    try:
        mid = int(message_id)
    except Exception:
        return
    now = time.time()
    records = await _load_monitor_recent_records(app, key)
    records = [r for r in records if int(r.get("id")) != mid]
    records.append({"id": mid, "ts": now})
    await _save_monitor_recent_records(app, key, records)


async def cleanup_monitor_duplicates_once(app, key: str = "chatgpt_mode_monitor") -> dict:
    """Delete duplicate monitor cards known from the last hour, keep newest.

    Telegram Bot API cannot search chat history by text, so this sanitary cleanup
    uses only message_id values returned by Telegram when this bot sent monitor
    cards.  It is intentionally limited to the main monitor key.
    """
    chat_id = first_admin_id()
    if not chat_id or key not in MONITOR_DUPLICATE_CLEANUP_KEYS:
        return {"checked": 0, "deleted": 0, "kept": 0, "failed": 0}
    lock = bottom_replace_locks.get(f"cleanup_{key}")
    if lock is None:
        lock = asyncio.Lock()
        bottom_replace_locks[f"cleanup_{key}"] = lock
    async with lock:
        records = await _load_monitor_recent_records(app, key)
        if len(records) <= 1:
            await _save_monitor_recent_records(app, key, records)
            return {"checked": len(records), "deleted": 0, "kept": len(records), "failed": 0}
        newest = max(records, key=lambda r: float(r.get("ts") or 0))
        newest_id = int(newest.get("id"))
        kept = [newest]
        deleted = 0
        failed = 0
        for rec in records:
            mid = int(rec.get("id"))
            if mid == newest_id:
                continue
            try:
                await asyncio.wait_for(app.bot.delete_message(chat_id=chat_id, message_id=mid), timeout=4)
                deleted += 1
            except Exception as e:
                # If Telegram says it is already gone, drop it from the technical
                # list.  Network/timeouts stay in the list so the next 5-minute
                # cleanup can retry.
                msg = str(e).lower()
                if "message to delete not found" in msg or "message can't be deleted" in msg or "message identifier is not specified" in msg:
                    deleted += 0
                else:
                    failed += 1
                    kept.append(rec)
                log.debug("telegram monitor duplicate cleanup delete skipped: %s", e)
        await _save_monitor_recent_records(app, key, kept)
        try:
            chatgpt_log_event("monitor_duplicate_cleanup", key=key, checked=len(records), deleted=deleted, failed=failed, kept=len(kept))
        except Exception:
            pass
        return {"checked": len(records), "deleted": deleted, "kept": len(kept), "failed": failed}


async def monitor_duplicate_cleanup_loop(app):
    try:
        await asyncio.sleep(60)
        while True:
            try:
                for key in MONITOR_DUPLICATE_CLEANUP_KEYS:
                    await cleanup_monitor_duplicates_once(app, key=key)
            except Exception as e:
                log.debug("monitor duplicate cleanup loop error: %s", e)
            await asyncio.sleep(MONITOR_DUPLICATE_CLEANUP_SEC)
    except asyncio.CancelledError:
        raise

async def notify_admin_bottom_replace(app, text: str, key: str = "live_status", min_interval_sec: int = 0) -> None:
    """Keep one live Telegram status message without duplicating cards.

    v0404: per-key asyncio lock fixes the real duplicate race: two monitor
    refresh tasks could start at the same time, both see no stored message id,
    and both send a fresh card. Now only one update for the same key can run.
    """
    chat_id = first_admin_id()
    if not chat_id:
        return
    lock = bottom_replace_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        bottom_replace_locks[key] = lock
    async with lock:
        now = time.time()
        if min_interval_sec:
            last_key = f"last_bottom_{key}"
            last = float(app.bot_data.get(last_key, 0) or 0)
            if now - last < min_interval_sec:
                return
            app.bot_data[last_key] = now

        msg_key = f"bottom_msg_id_{key}"
        ids_key = f"bottom_msg_ids_{key}"
        old_msg_id = app.bot_data.get(msg_key)
        if not old_msg_id:
            try:
                old_msg_id = await storage.get(msg_key, None)
            except Exception:
                old_msg_id = None

        # Keep a small per-key history of monitor/status cards. This allows the
        # replace mode to delete all known old monitor duplicates, not only the
        # last active_id. It is intentionally scoped by key so progress cards,
        # setup files, limit-filled cards, etc. are never touched.
        raw_ids = app.bot_data.get(ids_key)
        if raw_ids is None:
            try:
                raw_ids = await storage.get(ids_key, [])
            except Exception:
                raw_ids = []
        known_ids = []
        for x in (raw_ids if isinstance(raw_ids, list) else [raw_ids]):
            try:
                xi = int(x)
                if xi not in known_ids:
                    known_ids.append(xi)
            except Exception:
                pass
        try:
            if old_msg_id:
                oi = int(old_msg_id)
                if oi not in known_ids:
                    known_ids.append(oi)
        except Exception:
            pass

        payload = str(text)[:3900]

        # Bottom cards are kept as one latest message per key: delete the previous
        # saved message and send a fresh one so the status stays at the bottom.
        # The per-key lock above prevents races where several monitor refreshes
        # could create duplicates. The main monitor is included deliberately: one
        # actual monitor card, always latest, no edit-in-place pile-up.
        force_bottom_resend = key in {
            "chatgpt_mode_monitor",
            "chatgpt_limit_timeout_event",
            "chatgpt_position_event",
            "chatgpt_setup_lifecycle",
        }

        if old_msg_id and not force_bottom_resend:
            try:
                await asyncio.wait_for(app.bot.edit_message_text(chat_id=chat_id, message_id=int(old_msg_id), text=payload), timeout=6)
                app.bot_data[msg_key] = int(old_msg_id)
                app.bot_data[ids_key] = [int(old_msg_id)]
                try:
                    await storage.set(msg_key, int(old_msg_id), bump_revision=False)
                    await storage.set(ids_key, [int(old_msg_id)], bump_revision=False)
                except Exception:
                    pass
                return
            except Exception as e:
                msg = str(e).lower()
                if "message is not modified" in msg:
                    app.bot_data[msg_key] = int(old_msg_id)
                    app.bot_data[ids_key] = [int(old_msg_id)]
                    return
                log.debug("telegram bottom status edit skipped: %s", e)

        if known_ids:
            for mid in list(known_ids):
                try:
                    await asyncio.wait_for(app.bot.delete_message(chat_id=chat_id, message_id=int(mid)), timeout=4)
                except Exception as de:
                    log.debug("telegram bottom status cleanup delete skipped before resend: %s", de)
            try:
                await storage.set(msg_key, None, bump_revision=False)
                await storage.set(ids_key, [], bump_revision=False)
            except Exception:
                pass
            app.bot_data[msg_key] = None
            app.bot_data[ids_key] = []

        try:
            msg = await asyncio.wait_for(app.bot.send_message(chat_id=chat_id, text=payload), timeout=6)
            new_id = getattr(msg, "message_id", None)
            app.bot_data[msg_key] = new_id
            app.bot_data[ids_key] = [int(new_id)] if new_id else []
            await _record_monitor_message_for_cleanup(app, key, new_id)
            try:
                await storage.set(msg_key, new_id, bump_revision=False)
                await storage.set(ids_key, [int(new_id)] if new_id else [], bump_revision=False)
            except Exception:
                pass
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
    # Do not update Telegram too often; sending a fresh bottom message every loop
    # can rate-limit noisy scans. Force updates still move the card immediately.
    if not force and now - float(app.bot_data.get("scanner_status_last_edit", 0) or 0) < 5:
        return
    text = _scan_status_text(settings, status, last_signal, last_decision)
    old_msg_id = app.bot_data.get("scanner_status_message_id")
    if old_msg_id:
        try:
            await app.bot.delete_message(chat_id=chat_id, message_id=int(old_msg_id))
        except Exception as e:
            # The message may be already deleted/too old; send the new card anyway.
            log.debug("scanner status delete skipped: %s", e)
    try:
        msg = await app.bot.send_message(chat_id=chat_id, text=text)
        app.bot_data["scanner_status_message_id"] = msg.message_id
        app.bot_data["scanner_status_last_edit"] = now
    except Exception as e:
        log.warning("scanner status send failed: %s", e)

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
    old_msg_id = app.bot_data.get("scanner_status_message_id")
    if old_msg_id:
        try:
            await app.bot.delete_message(chat_id=chat_id, message_id=int(old_msg_id))
        except Exception as e:
            log.debug("fresh scanner status delete skipped: %s", e)
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



async def impulse_dump_progress_message(app, pct: int, *, done: bool = False, clear: bool = False) -> None:
    chat_id = first_admin_id()
    if not chat_id:
        return
    old_id = app.bot_data.get("impulse_dump_progress_message_id")
    if old_id:
        try:
            await asyncio.wait_for(app.bot.delete_message(chat_id=chat_id, message_id=int(old_id)), timeout=4)
        except Exception as e:
            log.debug("impulse dump progress delete skipped: %s", e)
        app.bot_data["impulse_dump_progress_message_id"] = None
    if clear:
        log_event("impulse_dump_progress", stage="clear", ok=True, pct=pct)
        return
    text = f"✅ Закончил сканирование {pct}%" if done else f"🔍 Сканирование {pct}%..."
    try:
        msg = await asyncio.wait_for(app.bot.send_message(chat_id=chat_id, text=text), timeout=6)
        app.bot_data["impulse_dump_progress_message_id"] = getattr(msg, "message_id", None)
        log_event("impulse_dump_progress", stage="done" if done else "scan", ok=True, pct=pct, message_id=getattr(msg, "message_id", None))
    except Exception as e:
        log.warning("impulse dump progress send failed: %s", e)
        log_event("impulse_dump_progress_error", stage="send", ok=False, pct=pct, error=str(e)[:300])


async def impulse_dump_summary_message(app, settings: dict, candidates: list[dict] | None = None, *, opened_note: str = "") -> None:
    chat_id = first_admin_id()
    if not chat_id:
        return
    candidates = candidates or []
    try:
        positions = await storage.positions()
    except Exception:
        positions = []
    positions = [p for p in positions if str(p.get("strategy", "")).lower() == "impulse_dump"]
    open_names = [_qb_symbol(p.get("symbol")) for p in positions]
    max_slots = int(float(settings.get("impulse_dump_max_open_positions", settings.get("max_open_positions", 5)) or 5))
    found = int(getattr(scanner, "last_ai_candidates_count", 0) or len(candidates))
    chosen = [_qb_symbol(c.get("symbol")) for c in candidates[:max_slots]]
    free_slots = max(0, max_slots - len(positions))
    picked_now = chosen[:free_slots]
    reserve = chosen[free_slots:free_slots + 1]
    try:
        since = time.time() - 86400
        trades = [t for t in await storage.trade_rows(since=since) if str(t.get("strategy", "")).lower() == "impulse_dump"]
    except Exception:
        trades = []
    closed = [_qb_symbol(t.get("symbol")) for t in trades[-8:]]
    killed = [_qb_symbol(t.get("symbol")) for t in trades if "time" in str(t.get("reason", "")).lower() or "time" in str(t.get("result", "")).lower()]
    pnl = sum(float(t.get("pnl_usdt") or 0) for t in trades)
    sl_streak = app.bot_data.get("impulse_dump_consecutive_sl", 0)
    lines = [
        "🔻 ИМПУЛЬСНЫЙ СЛИВ",
        "",
        f"Сканирование: топ {int(float(settings.get('impulse_dump_top_coins', settings.get('max_symbols', 200)) or 200))}",
        f"Нашёл монет по условиям в круге: {found}",
        "Выбраны лучшие: " + (", ".join(picked_now) if picked_now else "нет"),
    ]
    if reserve:
        lines.append(f"Лучший кандидат при освобождении слота: {reserve[0]}")
    lines += [
        "Открытые на бирже: " + (", ".join(open_names) if open_names else "нет"),
        "Закрытые на бирже: " + (", ".join(closed) if closed else "нет"),
        f"Заполнены {len(positions)}/{max_slots} слотов",
        "Убитые за 24 часа: " + (", ".join(killed[-5:]) if killed else "нет"),
        f"SL подряд: {sl_streak}/3",
        f"Общий плюс по закрытым монетам: ${pnl:+.2f}",
    ]
    if opened_note:
        lines += ["", opened_note]
    text = "\n".join(lines)[:3900]
    old_id = app.bot_data.get("impulse_dump_summary_message_id")
    if old_id:
        try:
            await asyncio.wait_for(app.bot.delete_message(chat_id=chat_id, message_id=int(old_id)), timeout=4)
        except Exception as e:
            log.debug("impulse dump summary delete skipped: %s", e)
    try:
        msg = await asyncio.wait_for(app.bot.send_message(chat_id=chat_id, text=text), timeout=6)
        app.bot_data["impulse_dump_summary_message_id"] = getattr(msg, "message_id", None)
        log_event("impulse_dump_summary", stage="sent", ok=True, found=found, chosen=chosen, open_symbols=open_names, closed_symbols=closed[-8:], time_killed=killed[-5:], slots=f"{len(positions)}/{max_slots}", closed_pnl_usdt=round(pnl, 4), message_id=getattr(msg, "message_id", None))
    except Exception as e:
        log.warning("impulse dump summary send failed: %s", e)
        log_event("impulse_dump_summary_error", stage="send", ok=False, error=str(e)[:300])


def format_impulse_dump_opened(plan, placed: dict) -> str:
    pos = placed.get("position") if isinstance(placed, dict) else None
    pos = pos if isinstance(pos, dict) else plan.__dict__
    entry = float(pos.get("entry_price") or plan.entry_price)
    stop = float(pos.get("stop_price") or plan.stop_price)
    take = float(pos.get("take_price") or plan.take_price)
    _notional, margin, leverage, _margin_type = _position_money_fields(pos)
    details = pos.get("signal_details") if isinstance(pos.get("signal_details"), dict) else getattr(plan, "signal_details", {})
    details = details if isinstance(details, dict) else {}
    trigger_tf = str(details.get("trigger_tf") or "unknown")
    trigger_note = str(details.get("trigger_note") or "")
    move15 = details.get("move_15m_pct")
    move1h = details.get("move_1h_pct")
    move4h = details.get("move_4h_pct")
    change24 = details.get("change_24h_pct")
    tp_pct = details.get("tp_pct")
    sl_pct = details.get("sl_pct")
    tp_context = str(details.get("tp_context_source") or "")
    tp_context_ru = "таймфрейм входа" if tp_context == "entry_trigger_tf" else ("24h минус" if tp_context == "24h_red" else ("local high 4h" if tp_context == "local_4h_high" else tp_context))
    protection_mode = str(pos.get("protection_mode") or "unknown").lower()
    if protection_mode in {"exchange", "exchange_planorder", "exchange_planorder_pending_verify"}:
        protection_line = "защита: реальные SL/TP на бирже"
    elif protection_mode in {"virtual", "local_monitoring"}:
        protection_line = "защита: виртуальные SL/TP"
    else:
        protection_line = f"защита: {protection_mode}"
    def _pct_line(name, value):
        try:
            return f"{name}: {float(value):+.2f}%"
        except Exception:
            return f"{name}: n/a"
    return "\n".join([
        f"🔻 Открыл SHORT {_qb_symbol(plan.symbol)}",
        f"${margin:.2f} плечо x{leverage}",
        f"вход ${_fmt_price(entry)}",
        f"стоп ${_fmt_price(stop)}",
        f"тейк ${_fmt_price(take)}",
        f"условие: {trigger_tf}" + (f" ({trigger_note})" if trigger_note else ""),
        _pct_line("15m", move15),
        _pct_line("1h", move1h),
        _pct_line("4h", move4h),
        _pct_line("24h", change24),
        _pct_line("TP от входа", tp_pct),
        _pct_line("SL от входа", sl_pct),
        f"тейк считается от: {tp_context_ru}",
        protection_line,
    ])


async def orderflow_impulse_progress_message(app, pct: int, *, done: bool = False, clear: bool = False) -> None:
    chat_id = first_admin_id()
    if not chat_id:
        return
    old_id = app.bot_data.get("orderflow_impulse_progress_message_id")
    if old_id:
        try:
            await asyncio.wait_for(app.bot.delete_message(chat_id=chat_id, message_id=int(old_id)), timeout=4)
        except Exception as e:
            log.debug("orderflow impulse progress delete skipped: %s", e)
        app.bot_data["orderflow_impulse_progress_message_id"] = None
    if clear:
        log_event("orderflow_impulse_progress", stage="clear", ok=True, pct=pct)
        return
    text = f"✅ Закончил сканирование {pct}%" if done else f"🔍 Сканирование {pct}%..."
    try:
        msg = await asyncio.wait_for(app.bot.send_message(chat_id=chat_id, text=text), timeout=6)
        app.bot_data["orderflow_impulse_progress_message_id"] = getattr(msg, "message_id", None)
        log_event("orderflow_impulse_progress", stage="done" if done else "scan", ok=True, pct=pct, message_id=getattr(msg, "message_id", None))
    except Exception as e:
        log.warning("orderflow impulse progress send failed: %s", e)
        log_event("orderflow_impulse_progress_error", stage="send", ok=False, pct=pct, error=str(e)[:300])


async def orderflow_impulse_summary_message(app, settings: dict, candidates: list[dict] | None = None, *, opened_note: str = "") -> None:
    chat_id = first_admin_id()
    if not chat_id:
        return
    candidates = candidates or []
    try:
        positions = await storage.positions()
    except Exception:
        positions = []
    positions = [p for p in positions if str(p.get("strategy", "")).lower() == "orderflow_impulse"]
    open_names = [_qb_symbol(p.get("symbol")) for p in positions]
    max_slots = int(float(settings.get("orderflow_impulse_max_open_positions", settings.get("max_open_positions", 3)) or 3))
    found = int(getattr(scanner, "last_ai_candidates_count", 0) or len(candidates))
    chosen = [_qb_symbol(c.get("symbol")) for c in candidates[:max_slots]]
    free_slots = max(0, max_slots - len(positions))
    picked_now = chosen[:free_slots]
    reserve = chosen[free_slots:free_slots + 1]
    try:
        since = time.time() - 86400
        trades = [t for t in await storage.trade_rows(since=since) if str(t.get("strategy", "")).lower() == "orderflow_impulse"]
    except Exception:
        trades = []
    closed = [_qb_symbol(t.get("symbol")) for t in trades[-8:]]
    killed = [_qb_symbol(t.get("symbol")) for t in trades if "time" in str(t.get("reason", "")).lower() or "time" in str(t.get("result", "")).lower()]
    pnl = sum(float(t.get("pnl_usdt") or 0) for t in trades)
    lines = [
        "📊 ORDERFLOW IMPULSE",
        "",
        f"Сканирование: Binance spot top {int(float(settings.get('orderflow_impulse_top_coins', settings.get('max_symbols', 100)) or 100))}",
        f"Нашёл монет по условиям в круге: {found}",
        "Выбраны лучшие: " + (", ".join(picked_now) if picked_now else "нет"),
    ]
    if reserve:
        lines.append(f"Лучший кандидат при освобождении слота: {reserve[0]}")
    lines += [
        "Открытые на бирже: " + (", ".join(open_names) if open_names else "нет"),
        "Закрытые на бирже: " + (", ".join(closed) if closed else "нет"),
        f"Заполнены {len(positions)}/{max_slots} слотов",
        "Убитые за 24 часа: " + (", ".join(killed[-5:]) if killed else "нет"),
        f"Общий плюс по закрытым монетам: ${pnl:+.2f}",
    ]
    if opened_note:
        lines += ["", opened_note]
    text = "\n".join(lines)[:3900]
    old_id = app.bot_data.get("orderflow_impulse_summary_message_id")
    if old_id:
        try:
            await asyncio.wait_for(app.bot.delete_message(chat_id=chat_id, message_id=int(old_id)), timeout=4)
        except Exception as e:
            log.debug("orderflow impulse summary delete skipped: %s", e)
    try:
        msg = await asyncio.wait_for(app.bot.send_message(chat_id=chat_id, text=text), timeout=6)
        app.bot_data["orderflow_impulse_summary_message_id"] = getattr(msg, "message_id", None)
        log_event("orderflow_impulse_summary", stage="sent", ok=True, found=found, chosen=chosen, open_symbols=open_names, closed_symbols=closed[-8:], time_killed=killed[-5:], slots=f"{len(positions)}/{max_slots}", closed_pnl_usdt=round(pnl, 4), message_id=getattr(msg, "message_id", None))
    except Exception as e:
        log.warning("orderflow impulse summary send failed: %s", e)
        log_event("orderflow_impulse_summary_error", stage="send", ok=False, error=str(e)[:300])


def format_knife_reversal_opened(plan, placed: dict) -> str:
    d = getattr(plan, "signal_details", {}) if hasattr(plan, "signal_details") else {}
    d = d if isinstance(d, dict) else {}
    return (
        "🗡 KNIFE REVERSAL OPENED\n"
        f"{getattr(plan, 'symbol', '-')} {getattr(plan, 'side', '-')}\n"
        f"Entry: {float(getattr(plan, 'entry_price', 0) or 0):.8g}\n"
        f"TP: {float(getattr(plan, 'take_price', 0) or 0):.8g} (+5%)\n"
        f"SL: {float(getattr(plan, 'stop_price', 0) or 0):.8g} (below wick)\n"
        f"Wick: {d.get('wick_pct', '-')}% | Reclaim: {d.get('reclaim_pct', '-')}%\n"
        f"Delta: {d.get('spot_delta_ratio', '-')} | Book: {d.get('spot_orderbook_imbalance', '-')}\n"
        f"Margin: {float(getattr(plan, 'expected_margin_usdt', 0) or 0):.4g} USDT | Lev: {int(float(getattr(plan, 'leverage', 0) or 0))}x"
    )

def format_multi_strategy_opened(plan, placed: dict) -> str:
    return "🧠 MULTI STRATEGY\n" + format_orderflow_impulse_opened(plan, placed) if str(getattr(plan, 'strategy', '')).lower() == 'orderflow_impulse' else "🧠 MULTI STRATEGY\n" + format_knife_reversal_opened(plan, placed)

def format_orderflow_impulse_opened(plan, placed: dict) -> str:
    pos = placed.get("position") if isinstance(placed, dict) else None
    pos = pos if isinstance(pos, dict) else plan.__dict__
    entry = float(pos.get("entry_price") or plan.entry_price)
    stop = float(pos.get("stop_price") or plan.stop_price)
    take = float(pos.get("take_price") or plan.take_price)
    _notional, margin, leverage, _margin_type = _position_money_fields(pos)
    side = str(pos.get("side") or getattr(plan, "side", "")).upper()
    details = pos.get("signal_details") if isinstance(pos.get("signal_details"), dict) else getattr(plan, "signal_details", {})
    details = details if isinstance(details, dict) else {}
    protection_mode = str(pos.get("protection_mode") or "unknown").lower()
    protection_line = "защита: реальные SL/TP на бирже" if protection_mode in {"exchange", "exchange_planorder", "exchange_planorder_pending_verify"} else ("защита: виртуальные SL/TP" if protection_mode in {"virtual", "local_monitoring"} else f"защита: {protection_mode}")
    def _pct_line(name, value):
        try:
            return f"{name}: {float(value):+.2f}%"
        except Exception:
            return f"{name}: n/a"
    return "\n".join([
        f"📊 Открыл {side} {_qb_symbol(plan.symbol)}",
        f"${margin:.2f} плечо x{leverage}",
        f"вход ${_fmt_price(entry)}",
        f"стоп ${_fmt_price(stop)}",
        f"тейк ${_fmt_price(take)}",
        f"Binance spot symbol: {details.get('spot_symbol') or 'n/a'}",
        _pct_line("Binance spot move", details.get("spot_move_pct")),
        f"Binance spot delta: {float(details.get('spot_delta_ratio') or 0):+.3f}",
        f"Binance spot OB imbalance: {float(details.get('spot_orderbook_imbalance') or 0):+.3f}",
        f"Binance spot volume: {float(details.get('spot_volume_ratio') or 0):.2f}x",
        f"Binance spot spread: {float(details.get('spot_spread_pct') or 0):.3f}%",
        _pct_line("TP от входа", details.get("tp_pct")),
        _pct_line("SL от входа", details.get("sl_pct")),
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
    """Sleep between full scanner cycles, but allow Run/settings to wake it.

    Important: do not clear a wakeup that was already set before this
    function started. Mode buttons can fire while the loop is between the
    end of one cycle and the sleep call; clearing first loses that signal
    and the bot appears to do nothing until the old interval expires.
    """
    try:
        delay = max(0.0, float(seconds))
    except Exception:
        delay = 5.0
    ev = app.bot_data.get("scan_wakeup_event")
    if ev is None:
        ev = asyncio.Event()
        app.bot_data["scan_wakeup_event"] = ev
    if ev.is_set():
        ev.clear()
        return
    try:
        await asyncio.wait_for(ev.wait(), timeout=delay)
    except asyncio.TimeoutError:
        return
    finally:
        ev.clear()

def _api_creds(settings: dict) -> tuple[str, str]:
    # V79: one simple session source for every command/background sync.
    # No backup-file search.  /api set writes SQLite + runtime env/cache; redeploy
    # can require setting keys again unless Railway Variables are configured.
    try:
        settings = merge_secrets_into_settings(settings or {})
    except Exception:
        settings = settings or {}
    api_key = secret_value(settings, "mexc_api_key", "MEXC_API_KEY")
    api_secret = secret_value(settings, "mexc_api_secret", "MEXC_API_SECRET")
    return api_key, api_secret

async def _ensure_secret_health(reason: str = "manual") -> dict:
    """SQLite-first secret check.

    If keys are active in the current process but missing in SQLite, persist them
    into SQLite once. No old backup/cache files are used.
    """
    try:
        raw = await _repair_sqlite_secrets_from_runtime(reason=reason)
    except Exception:
        try:
            raw = await storage.all_settings()
        except Exception:
            raw = {}
    try:
        merged = merge_secrets_into_settings(raw or {})
        ensure_runtime_secrets_loaded(merged)
        report = secret_source_report(merged)
        log_event("secret_health_v11", ok=True, reason=reason, mexc_key=bool(_api_creds(merged)[0]), mexc_secret=bool(_api_creds(merged)[1]), openai=bool(str(merged.get("openai_api_key") or "").strip()), report=report)
        return merged
    except Exception as e:
        log_event("secret_health_v11", ok=False, reason=reason, error=str(e)[:500])
        return raw or {}



async def _repair_sqlite_secrets_from_runtime(reason: str = "manual") -> dict:
    """Persist currently available runtime/env/exchange secrets into SQLite.

    This is not an old backup/cache restore. It only saves keys that are already
    active in the current process (after /api set, Railway env fallback, or an
    already initialized ExchangeClient) so the next command/redeploy reads them
    from SQLite deterministically.
    """
    repaired = {"mexc_key": False, "mexc_secret": False, "openai": False}
    try:
        current = await storage.all_settings()
    except Exception:
        current = {}

    def clean(v):
        return str(v or "").strip()

    # First priority: explicit SQLite values already present.
    db_mk = clean(current.get("mexc_api_key"))
    db_ms = clean(current.get("mexc_api_secret"))
    db_oa = clean(current.get("openai_api_key"))

    # Runtime/process values. These are allowed only as a live source to write
    # into SQLite; no file cache/backups are read.
    env_mk = clean(os.getenv("MEXC_API_KEY", ""))
    env_ms = clean(os.getenv("MEXC_API_SECRET", ""))
    env_oa = clean(os.getenv("OPENAI_API_KEY", ""))

    # If an exchange client is already initialized, it may still have the key in
    # memory from a successful balance/API call. Save that into SQLite before a
    # later reset loses it.
    global exchange_client
    ex_mk = clean(getattr(exchange_client, "api_key", "")) if exchange_client is not None else ""
    ex_ms = clean(getattr(exchange_client, "api_secret", "")) if exchange_client is not None else ""

    mk = db_mk or env_mk or ex_mk
    ms = db_ms or env_ms or ex_ms
    oa = db_oa or env_oa

    try:
        if mk and not db_mk:
            await storage.set("mexc_api_key", mk, bump_revision=False)
            os.environ["MEXC_API_KEY"] = mk
            repaired["mexc_key"] = True
        if ms and not db_ms:
            await storage.set("mexc_api_secret", ms, bump_revision=False)
            os.environ["MEXC_API_SECRET"] = ms
            repaired["mexc_secret"] = True
        if oa and not db_oa:
            await storage.set("openai_api_key", oa, bump_revision=False)
            os.environ["OPENAI_API_KEY"] = oa
            repaired["openai"] = True
        if any(repaired.values()):
            log_event("sqlite_secret_repair_v11", ok=True, reason=reason, repaired=repaired)
    except Exception as e:
        log_event("sqlite_secret_repair_v11", ok=False, reason=reason, error=str(e)[:500])

    try:
        return await storage.all_settings()
    except Exception:
        return current or {}

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


def _format_setup_lifecycle_text(setup: dict, result: dict | None = None, *, phase: str = "placing") -> str:
    """One replaceable Telegram card for setup entry placement.

    Restores the useful user-facing notice that was accidentally hidden while
    suppressing noisy Position event / limit_timeout spam.  This is only a
    display helper; it does not affect execution, order prices, SL/TP, TTL,
    Claude prompt, or ChatGPT Scan Mode logic.
    """
    trades = setup.get("trades") if isinstance(setup, dict) else []
    trades = trades if isinstance(trades, list) else []
    if phase == "done":
        opened = result.get("opened") if isinstance(result, dict) else []
        placed = [x for x in (opened or []) if isinstance(x, dict) and bool(x.get("ok"))]
        skipped = [x for x in (opened or []) if isinstance(x, dict) and not bool(x.get("ok"))]
        lines = ["✅ Входы по setup обработаны"]
        if placed:
            lines.append("📌 Поставлены/открыты:")
            for r in placed[:10]:
                sym = str(r.get("symbol") or "-")
                side = str(r.get("side") or "").upper()
                order_type = str(r.get("order_type") or "").upper()
                entry = r.get("entry")
                lines.append(f"• {sym} {side} {order_type} {_fmt_price(entry)}".strip())
        else:
            lines.append("📌 Поставлены/открыты: нет")
        if skipped:
            lines.append("❌ Пропущены:")
            for r in skipped[:5]:
                lines.append(f"• {r.get('symbol') or '-'} — {str(r.get('reason') or r.get('error') or '')[:120]}")
        return "\n".join(lines)

    lines = ["📌 Выставляю лимитки:"]
    if not trades:
        lines.append("• нет сделок в setup")
        return "\n".join(lines)
    for t in trades[:10]:
        if not isinstance(t, dict):
            continue
        sym = str(t.get("symbol") or "-")
        side = str(t.get("direction") or t.get("side") or "").upper()
        order_type = str(t.get("order_type") or "LIMIT").upper()
        entry = t.get("entry")
        if order_type == "LIMIT":
            lines.append(f"• {sym} {side} {_fmt_price(entry)}")
        else:
            lines.append(f"• {sym} {side} {order_type} {_fmt_price(entry)}")
    return "\n".join(lines)

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

    if typ == "chatgpt_limit_filled_protected":
        tp_levels = ev.get("tp_levels") if isinstance(ev.get("tp_levels"), list) else []
        tp_orders = ev.get("tp_orders") if isinstance(ev.get("tp_orders"), list) else []
        tp_lines = []
        for idx, tp in enumerate(tp_levels, start=1):
            if not isinstance(tp, dict):
                continue
            price = tp.get("price")
            size = tp.get("size_percent")
            oid = tp_orders[idx - 1] if idx - 1 < len(tp_orders) else None
            suffix = f" ({size})" if size not in (None, "") else ""
            oid_suffix = f" ✅" if oid else ""
            tp_lines.append(f"TP{idx}: {price}{suffix}{oid_suffix}")
        if not tp_lines:
            tp_lines = [f"TP final: {ev.get('take_price')}"]
        expected_tp = ev.get("expected_tp_count")
        verified_tp = ev.get("verified_tp_count")
        if expected_tp is not None or verified_tp is not None:
            verify_line = f"TP verified: {verified_tp if verified_tp is not None else '?'} / {expected_tp if expected_tp is not None else '?'}; SL verified: {'yes' if ev.get('sl_verified') else 'no'}"
        else:
            verify_line = "TP/SL accepted by exchange"
        side = str(ev.get("side") or ev.get("direction") or "").upper()
        side_line = f"Direction: {side}" if side in {"LONG", "SHORT"} else "Direction: неизвестно"
        return "\n".join([
            "📌 Лимитка исполнена",
            f"{symbol}",
            side_line,
            f"Entry: {ev.get('entry_price')}",
            "",
            "✅ Позиция под полной защитой",
            f"SL: {ev.get('stop_price')} ✅",
            *tp_lines,
            verify_line,
            "",
            "SL один на всю позицию; TP разбиты на 3 части.",
        ])

    if typ == "chatgpt_limit_filled_local_protection":
        side = str(ev.get("side") or ev.get("direction") or "").upper()
        side_line = f"Direction: {side}" if side in {"LONG", "SHORT"} else "Direction: неизвестно"
        return "\n".join([
            "📌 Лимитка исполнена",
            f"{symbol}",
            side_line,
            f"Entry: {ev.get('entry_price')}",
            "",
            "🚨 LOCAL PROTECTION MODE",
            "Биржевая SL/TP защита не подтверждена.",
            "Бот сопровождает позицию локально и будет пытаться восстановить защиту.",
        ])


    if typ == "chatgpt_position_closed":
        side = str(ev.get("side") or ev.get("direction") or "").upper()
        reason_label = str(ev.get("reason_label") or "закрыта на бирже; точная причина не подтверждена")
        reason_code = str(ev.get("reason_code") or ev.get("reason") or "UNKNOWN_EXCHANGE_CLOSED")
        confidence = str(ev.get("confidence") or "LOW")
        def _p(x):
            try:
                return _fmt_price(float(x))
            except Exception:
                return str(x)
        lines = [
            "📌 Позиция закрыта",
            f"{symbol}",
            f"Direction: {side if side in {'LONG', 'SHORT'} else 'неизвестно'}",
            f"Причина: {reason_label}",
            f"Confidence: {confidence}",
        ]
        if ev.get("entry_price") not in (None, "", 0, "0"):
            lines.append(f"Entry: {_p(ev.get('entry_price'))}")
        if ev.get("close_price_estimate") not in (None, "", 0, "0"):
            lines.append(f"Last price near close: {_p(ev.get('close_price_estimate'))}")
        if ev.get("stop_price") not in (None, "", 0, "0"):
            lines.append(f"SL: {_p(ev.get('stop_price'))}")
        tp_levels = ev.get("tp_levels") if isinstance(ev.get("tp_levels"), list) else []
        if tp_levels:
            for idx, tp in enumerate(tp_levels, start=1):
                if not isinstance(tp, dict):
                    continue
                price = tp.get("price")
                size = tp.get("size_percent")
                if price not in (None, "", 0, "0"):
                    suffix = f" ({size})" if size not in (None, "") else ""
                    lines.append(f"TP{idx}: {_p(price)}{suffix}")
        elif ev.get("take_price") not in (None, "", 0, "0"):
            lines.append(f"TP final: {_p(ev.get('take_price'))}")
        nearest = ev.get("nearest_level") if isinstance(ev.get("nearest_level"), dict) else {}
        if nearest:
            try:
                lines.append(f"Nearest level: {nearest.get('name')} {_p(nearest.get('price'))} / distance {float(nearest.get('distance_pct') or 0):.2f}%")
            except Exception:
                pass
        try:
            pnl_pct = float(ev.get("pnl_pct") or 0)
            sign = "+" if pnl_pct >= 0 else ""
            lines.append(f"Estimated PnL: {sign}{pnl_pct:.2f}%")
        except Exception:
            pass
        try:
            elapsed = float(ev.get("elapsed_minutes") or 0)
            if elapsed > 0:
                lines.append(f"Время в позиции: {elapsed:.1f} мин")
        except Exception:
            pass
        lines.append("")
        if reason_code.startswith("UNKNOWN"):
            lines.append("Точно SL/TP не подтверждаю: позиция исчезла из exchange snapshot, истории fill в этом событии нет.")
        else:
            lines.append("Это best-effort вывод по последней цене и уровням setup; точный fill смотри в истории ордеров MEXC.")
        return "\n".join(lines)

    if typ == "chatgpt_tp1_breakeven":
        return "\n".join([
            "🎯 TP1 достигнут",
            f"{symbol}",
            "Часть позиции закрыта.",
            f"Стоп перенесён в Б/У: {ev.get('stop_price')}",
        ])

    if typ == "chatgpt_24h_profit_closed":
        try:
            pnl_s = f"{float(ev.get('pnl_pct') or 0):+.2f}%"
        except Exception:
            pnl_s = "плюс"
        return "\n".join([
            "⏰ Позиция старше 24 часов закрыта в плюсе",
            f"{symbol}",
            f"PnL: {pnl_s}",
        ])

    if typ == "chatgpt_24h_negative_hold":
        try:
            pnl_s = f"{float(ev.get('pnl_pct') or 0):+.2f}%"
        except Exception:
            pnl_s = "минус"
        return "\n".join([
            "⏰ Позиция старше 24 часов, но в минусе",
            f"{symbol}",
            f"PnL: {pnl_s}",
            "Действие: не закрываю.",
        ])

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

    if str(typ) in {"position_closed", "closed"}:
        # Legacy event path: make it obvious that this old callback cannot prove SL/TP.
        return "\n".join([
            "📌 Позиция закрыта",
            f"{symbol}",
            "Причина: закрыта на бирже; точная причина не подтверждена",
            "Confidence: LOW",
            "В этом событии нет истории fill/SL/TP. Проверь /log_chatgpt или историю MEXC.",
        ])
    if str(typ) in {"limit_canceled", "limit_cancelled"}:
        reason = str(result.get("reason") or ev.get("reason") or "")
        lines = ["📌 Лимитка отменена", f"{symbol}"]
        if reason:
            lines.append(f"Причина: {reason[:500]}")
        else:
            lines.append("Причина: отмена старой pending-лимитки перед новым setup или истечение TTL")
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
    try:
        ensure_runtime_secrets_loaded(settings or {})
        settings = merge_secrets_into_settings(settings or {})
    except Exception:
        pass
    api_key, api_secret = _api_creds(settings)
    if not (api_key and api_secret):
        log_event("exchange_api_missing_v7", ok=False, report=secret_source_report(settings or {}))
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
        # Short operator log: readable summary only. Full raw JSON is in /log_full.
        settings = await storage.all_settings()
        settings_view = {
            "version": VERSION,
            "btc_ai_autopilot_enabled": settings.get("btc_ai_autopilot_enabled"),
            "btc_ai_live_test_enabled": settings.get("btc_ai_live_test_enabled"),
            "btc_ai_symbol": settings.get("btc_ai_symbol"),
            "live_trading": settings.get("live_trading"),
        }
        positions = await storage.positions()
        btc_positions = [p for p in positions if str(p.get("strategy") or "") == "btc_ai_4h" or str(p.get("symbol") or "").upper() in {"BTC_USDT", "BTCUSDT", "BTC/USDT"}]
        compact_positions = []
        for p in btc_positions[-4:]:
            compact_positions.append({
                "symbol": p.get("symbol"),
                "side": p.get("side"),
                "status": p.get("status"),
                "entry": p.get("entry_price"),
                "sl": p.get("stop_price"),
                "tp1": p.get("partial_take_price"),
                "tp2": p.get("final_take_price") or p.get("take_price"),
                "protection": p.get("protection_status"),
            })
        exchange_positions = []
        try:
            ex = await get_exchange(settings)
            rows = await asyncio.wait_for(ex.fetch_positions(), timeout=12)
            for row in rows or []:
                try:
                    info = row.get("info") or {}
                    sym = str(row.get("symbol") or info.get("symbol") or "")
                    hold = float(info.get("holdVol") or row.get("contracts") or row.get("amount") or 0)
                    if "BTC" in sym.upper() and hold > 0:
                        exchange_positions.append({
                            "symbol": sym,
                            "side": row.get("side") or info.get("positionType"),
                            "holdVol": hold,
                            "entry": row.get("entryPrice") or info.get("holdAvgPrice"),
                            "unrealized": row.get("unrealizedPnl") or info.get("unrealized"),
                        })
                except Exception:
                    pass
        except Exception as e:
            exchange_positions = [{"error": str(e)[:180]}]
        text = tail_text(files=["btc_ai.log", "trade.log", "errors.log"], lines=35, max_chars=1600)
        header = "🧾 BTC AI краткий лог\nSETTINGS=" + json.dumps(settings_view, ensure_ascii=False, default=str) + "\nACTIVE_POSITIONS=" + json.dumps(compact_positions, ensure_ascii=False, default=str) + "\nEXCHANGE_BTC_POSITIONS=" + json.dumps(exchange_positions, ensure_ascii=False, default=str) + "\n"
        msg = header + "```\n" + text[-1600:] + "\n```\nПолный сырой JSON: /log_full"
        await reply(update, msg[:4050], reply_markup=MAIN_MENU)
    except Exception as e:
        await reply(update, f"/log error: {e}", reply_markup=MAIN_MENU)


async def log_full_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return
    try:
        n = 120
        if context.args:
            try:
                n = max(20, min(400, int(context.args[0])))
            except Exception:
                n = 120
        text = tail_text(files=["btc_ai.log", "errors.log", "mexc_raw.log", "trade.log"], lines=n, max_chars=3600)
        try:
            settings = await storage.all_settings()
            settings_view = {
                "version": VERSION,
                "btc_ai_autopilot_enabled": settings.get("btc_ai_autopilot_enabled"),
                "btc_ai_live_test_enabled": settings.get("btc_ai_live_test_enabled"),
                "btc_ai_symbol": settings.get("btc_ai_symbol"),
                "btc_ai_min_trade_probability": settings.get("btc_ai_min_trade_probability"),
                "btc_ai_a_plus_probability": settings.get("btc_ai_a_plus_probability"),
                "btc_ai_balance_share": settings.get("btc_ai_balance_share"),
                "btc_ai_leverage": settings.get("btc_ai_leverage"),
                "scan_market_source": settings.get("scan_market_source"),
                "live_trading": settings.get("live_trading"),
            }
            positions = await storage.positions()
            btc_positions = [p for p in positions if str(p.get("strategy") or "") == "btc_ai_4h" or str(p.get("symbol") or "").upper() in {"BTC_USDT", "BTCUSDT", "BTC/USDT"}]
            pos_text = json.dumps(btc_positions[-8:], ensure_ascii=False, default=str)[:1200]
            set_text = json.dumps(settings_view, ensure_ascii=False, default=str)
        except Exception as e:
            pos_text = f"positions/settings read error: {e}"
            set_text = "{}"
        header = "🧾 BTC AI FULL / MEXC / OpenAI logs\nSETTINGS=" + set_text + "\nACTIVE_POSITIONS=" + pos_text + "\n"
        msg = header + "```\n" + text[-3000:] + "\n```"
        if len(msg) > 4050:
            msg = msg[:900] + "\n...TRUNCATED...\n```\n" + text[-2700:] + "\n```"
        await reply(update, msg, reply_markup=MAIN_MENU)
    except Exception as e:
        await reply(update, f"/log_full error: {e}", reply_markup=MAIN_MENU)


async def test_btc_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle immediate BTC AI live test.

    ON: hard-isolates BTC AI mode, draws MEXC 4H chart, sends chart+compact data to OpenAI,
    then opens a live MEXC BTC MARKET test trade regardless of AI probability to verify exchange mechanics.
    OFF: disables only the test/new-entry trigger. Existing positions are not closed.
    """
    global btc_ai_task, position_task
    if not allowed(update):
        return
    settings = await storage.all_settings()
    currently_on = bool(settings.get("btc_ai_live_test_enabled", False))
    if currently_on:
        await storage.set("btc_ai_live_test_enabled", False, bump_revision=False)
        await storage.set("btc_ai_autopilot_enabled", False, bump_revision=False)
        obj = context.application.bot_data.get("btc_ai_autopilot")
        if obj:
            obj.stop()
        log_event("btc_ai_live_test_toggle", enabled=False, ok=True)
        await reply(update, "⏹ BTC AI LIVE TEST ВЫКЛ. Новые тестовые входы отключены. Открытые сделки НЕ закрываются; сопровождение остается.", reply_markup=MAIN_MENU)
        return

    hard_settings = {
        "btc_ai_live_test_enabled": True,
        # /test_btc is a one-shot manual live test. Do NOT enable the 4H scheduler here.
        # The monitor loop still runs with btc_ai_live_test_enabled=True for TP1->BE/24h safety,
        # but it will not announce/wait for the next candle or start scheduled entries.
        "btc_ai_autopilot_enabled": False,
        "btc_ai_symbol": "BTC_USDT",
        "btc_ai_balance_share": 0.10,
        "btc_ai_leverage": 10,
        "btc_ai_min_trade_probability": 65,
        "btc_ai_a_plus_probability": 85,
        "btc_ai_limit_timeout_sec": 14400,
        "btc_ai_pause_until": 0,
        "btc_ai_pause_manual_override_ts": time.time(),
        "limit_timeout_sec": 14400,
        "live_trading": True,
        "strategy_mode": "hybrid",
        "max_open_positions": 1,
        "openai_analysis_enabled": True,
        "openai_show_decisions": True,
        "boost_autopilot_active": False,
        "boost_parallel_scan_enabled": False,
        "quick_bounce_enabled": False,
        "impulse_dump_enabled": False,
        "orderflow_impulse_enabled": False,
        "cascade_hunter_enabled": False,
        "strongest_coin_enabled": False,
        "liquidity_runner_enabled": False,
        "auto_strategy_adaptation": False,
        "regime_adaptation": False,
        "spot_confirmation_enabled": False,
        "session_filter_enabled": False,
        "america_short_bias_enabled": False,
        "mirror_mode": "off",
        "trade_margin_pct": 0.10,
        "margin_allocation_enabled": True,
        "mexc_order_leverage": 10,
        "scan_market_source": "mexc_binance",
    }
    for k, v in hard_settings.items():
        await storage.set(k, v, bump_revision=False)
    settings = await storage.all_settings()
    ex = await get_exchange(settings)
    executor = ExecutionEngine(storage, ex)
    autopilot = BTCVisionAutopilot(storage, ex, executor)
    context.application.bot_data["btc_ai_autopilot"] = autopilot
    if position_task is None or position_task.done():
        globals()["position_task"] = context.application.create_task(position_management_loop(context.application))
    # Keep the lightweight monitor loop alive after the one-shot test so TP1->BE and 24h exit keep working.
    if btc_ai_task is None or btc_ai_task.done():
        btc_ai_task = context.application.create_task(autopilot.run_loop(context.application))

    log_event("btc_ai_live_test_toggle", enabled=True, ok=True, mode="one_shot_no_scheduler", version=VERSION)
    await reply(update, "🧪 BTC AI LIVE TEST ВКЛ\nСейчас выполню ОДИН тест без ожидания следующей 4H свечи: рисую MEXC 4H график, отправляю в OpenAI и открою MEXC BTC MARKET сделку при любом ответе ИИ. Scheduler 4H НЕ запускается. Все детали будут в /log.", reply_markup=MAIN_MENU)

    async def _run_once():
        try:
            log_event("btc_ai_live_test_start", ok=True, version=VERSION)
            await autopilot.cycle(context.application, force_live_test=True)
            log_event("btc_ai_live_test_finish", ok=True)
        except Exception as e:
            log_event("btc_ai_live_test_error", ok=False, error=str(e)[:1200])
            await reply(update, f"❌ BTC AI LIVE TEST error: {str(e)[:700]}. Сделка не открывается, если ошибка была до execution.", reply_markup=MAIN_MENU)
    context.application.create_task(_run_once())



async def game_btc_ai_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """One-shot BTC AI using market-maker chess prompt. Old BTC AI mode is untouched."""
    global btc_ai_task, position_task
    if not allowed(update): return
    settings_before = await storage.all_settings()
    old_prompt_mode = settings_before.get("btc_ai_prompt_mode")
    old_live_test = settings_before.get("btc_ai_live_test_enabled")
    # Same execution/risk defaults as BTC AI 4H, but no forced trade on WAIT.
    hard_settings = {
        "btc_ai_live_test_enabled": False,
        "btc_ai_autopilot_enabled": False,
        "btc_ai_prompt_mode": "market_maker_chess",
        "btc_ai_symbol": "BTC_USDT",
        "btc_ai_balance_share": 0.10,
        "btc_ai_leverage": 10,
        "btc_ai_min_trade_probability": 65,
        "btc_ai_a_plus_probability": 85,
        "btc_ai_limit_timeout_sec": 14400,
        "btc_ai_pause_until": 0,
        "limit_timeout_sec": 14400,
        "live_trading": True,
        "strategy_mode": "hybrid",
        "max_open_positions": 1,
        "openai_analysis_enabled": True,
        "openai_show_decisions": True,
        "boost_autopilot_active": False,
        "boost_parallel_scan_enabled": False,
        "quick_bounce_enabled": False,
        "impulse_dump_enabled": False,
        "orderflow_impulse_enabled": False,
        "cascade_hunter_enabled": False,
        "strongest_coin_enabled": False,
        "liquidity_runner_enabled": False,
        "auto_strategy_adaptation": False,
        "regime_adaptation": False,
        "spot_confirmation_enabled": False,
        "session_filter_enabled": False,
        "america_short_bias_enabled": False,
        "mirror_mode": "off",
        "trade_margin_pct": 0.10,
        "margin_allocation_enabled": True,
        "mexc_order_leverage": 10,
        "scan_market_source": "mexc_binance",
    }
    for k, v in hard_settings.items():
        await storage.set(k, v, bump_revision=False)
    ex = await get_exchange(await storage.all_settings())
    executor = ExecutionEngine(storage, ex)
    autopilot = BTCVisionAutopilot(storage, ex, executor)
    context.application.bot_data["btc_ai_autopilot"] = autopilot
    if position_task is None or position_task.done():
        globals()["position_task"] = context.application.create_task(position_management_loop(context.application))
    if btc_ai_task is None or btc_ai_task.done():
        btc_ai_task = context.application.create_task(autopilot.run_loop(context.application))

    log_event("game_btc_ai_start", ok=True, version=VERSION, prompt_mode="market_maker_chess")
    await reply(update,
                "♟ GAME BTC AI запущен один раз.\n"
                "Старый BTC AI 4H режим не включается и не меняется.\n"
                "ИИ играет партию против маркетмейкера: сравнит LONG/SHORT/WAIT ходы и выберет лучший.\n"
                "Сделка будет открыта только если Game AI даст tradable LONG/SHORT с порогом 65+. WAIT не форсируется.",
                reply_markup=MAIN_MENU)

    async def _run_once():
        try:
            await autopilot.cycle(context.application, force_live_test=False, manual_once=True)
            log_event("game_btc_ai_finish", ok=True)
        except Exception as e:
            log_event("game_btc_ai_error", ok=False, error=str(e)[:1200])
            await reply(update, f"❌ GAME BTC AI error: {str(e)[:700]}", reply_markup=MAIN_MENU)
        finally:
            # Restore old prompt mode so scheduled classic BTC AI is not touched.
            try:
                if old_prompt_mode is None:
                    await storage.set("btc_ai_prompt_mode", "classic", bump_revision=False)
                else:
                    await storage.set("btc_ai_prompt_mode", old_prompt_mode, bump_revision=False)
                await storage.set("btc_ai_live_test_enabled", bool(old_live_test), bump_revision=False)
            except Exception as e:
                log_event("game_btc_ai_restore_settings_error", ok=False, error=str(e)[:500])
    context.application.create_task(_run_once())

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    await reply(update, f"""
🤖 Liquidity Bot v{VERSION}

Команды:
/start - меню
/help - помощь
/log - краткий BTC AI лог
/log_full [lines] - полный сырой BTC AI/MEXC/OpenAI JSON, токены, prompt, ордера, позиции
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
/status_btc - реальный BTC AI статус по MEXC: позиция, TP/SL, TP1→BE, 24H exit
/backtest_btc_patterns - тест BTC 4H отпечатков свечей и US-open sweep без торговли
/backtest_btc_patterns_1h - тест BTC 1H отпечатков свечей без торговли
/backtest_round_levels - тест BTC/ETH круглых уровней 15m/1H без торговли
/backtest_strategy_lab - Strategy Lab core BTC/ETH 1H/4H без торговли
/backtest_strategy_lab_extra - Strategy Lab Extra: VWAP/BB/RSI/ATR/EMA/Donchian/US-open/SR/imbalance
/backtest_aggressive_lab - Aggressive Strategy Lab: ищет самые доходные стратегии без торговли
/clean_btc_orders - удалить старые активные BTC TP/SL planorders, оставить последнюю актуальную защиту
/ping - отклик ms, RAM, uptime, открыто сейчас и общий счётчик открытий
/balance - futures balance + IP/proxy; если MEXC margin=0 при открытых позициях, показывает estimated margin
/positions - локальные + реальные позиции MEXC + protection mode
/open_orders - обычные + plan/stop/TP-SL ордера MEXC
/cancel_all или /cancel all - жестко отменить normal/plan/stop/TP-SL ордера MEXC, включая ghost/frozen orders
/close_all или /close all - жестко закрыть реальные позиции, отменить все ордера, затем сверить balance/cache
/stats - статистика сделок
/ai_stats - меню статистики AI BTC/ETH scalping
/ai_stats_current - текущая AI session статистика
/ai_stats_lifetime - lifetime AI статистика
/ai_stats_reset - сбросить текущую AI session
/sync - синхронизация позиций/ордеров
/sync_positions - подтянуть реальные позиции MEXC в бота
/recovery - восстановить позиции MEXC после рестарта и проверить TP/SL
/mexc_debug_state [SYMBOL] - raw debug MEXC positions/orders/symbol variants
/test_btc - toggle LIVE BTC AI test: рисует график, отправляет ИИ, открывает TEST market сделку при любом ответе ИИ, пишет полный лог
/game_btc_ai - one-shot Game BTC AI: ИИ играет против маркетмейкера, WAIT не форсируется
/scan_potok 2|3|4|5 - изменить потоки ChatGPT Scan/графиков на текущий запуск бота; default после рестарта = 3

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

async def btc_ai_autopilot_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global btc_ai_task, position_task
    if not allowed(update): return
    settings = await storage.all_settings()
    enabled = not bool(settings.get("btc_ai_autopilot_enabled", False))
    if enabled:
        # Hard OFF everything else; BTC 4H mode owns the bot while enabled.
        hard_settings = {
            "btc_ai_autopilot_enabled": True,
            "btc_ai_symbol": "BTC_USDT",
            "btc_ai_balance_share": 0.10,
            "btc_ai_leverage": 10,
            "btc_ai_min_trade_probability": 65,
            "btc_ai_a_plus_probability": 85,
            "btc_ai_limit_timeout_sec": 14400,
            "btc_ai_pause_until": 0,
            "btc_ai_pause_manual_override_ts": time.time(),
            "limit_timeout_sec": 14400,
            "live_trading": True,
            "strategy_mode": "hybrid",
            "max_open_positions": 1,
            "openai_analysis_enabled": True,
            "openai_show_decisions": True,
            "boost_autopilot_active": False,
            "boost_parallel_scan_enabled": False,
            "quick_bounce_enabled": False,
            "impulse_dump_enabled": False,
            "orderflow_impulse_enabled": False,
            "cascade_hunter_enabled": False,
            "strongest_coin_enabled": False,
            "liquidity_runner_enabled": False,
            "auto_strategy_adaptation": False,
            "regime_adaptation": False,
            "spot_confirmation_enabled": False,
            "session_filter_enabled": False,
            "america_short_bias_enabled": False,
            "mirror_mode": "off",
            "trade_margin_pct": 0.10,
            "margin_allocation_enabled": True,
            "mexc_order_leverage": 10,
            "scan_market_source": "mexc_binance",
        }
        for k, v in hard_settings.items():
            await storage.set(k, v, bump_revision=False)
        ex = await get_exchange(await storage.all_settings())
        executor = ExecutionEngine(storage, ex)
        autopilot = BTCVisionAutopilot(storage, ex, executor)
        context.application.bot_data["btc_ai_autopilot"] = autopilot
        if btc_ai_task is None or btc_ai_task.done():
            btc_ai_task = context.application.create_task(autopilot.run_loop(context.application))
        if position_task is None or position_task.done():
            globals()["position_task"] = context.application.create_task(position_management_loop(context.application))
        next_ts = autopilot.next_msk_close_ts()
        msg = ("✅ BTC AI 4H автопилот ВКЛ\n"
               "Жестко отключено всё остальное.\n"
               "Торговля: MEXC BTC_USDT futures.\n"
               "Данные: MEXC chart/volume/funding/liquidation proxy + Binance spot pressure.\n"
               "Баланс: 10%, плечо x10.\n"
               "Порог: 65%, A+ от 85%.\n"
               "Следующий анализ: " + autopilot._fmt_msk(next_ts))
        await reply(update, msg, reply_markup=MAIN_MENU)
    else:
        # OFF disables only new BTC AI entries/new 4H analyses.
        # Existing positions are NOT closed, and virtual accompaniment remains active
        # via the background loop/position manager: TP1 -> breakeven, TP2/SL handling.
        await storage.set("btc_ai_autopilot_enabled", False, bump_revision=False)
        obj = context.application.bot_data.get("btc_ai_autopilot")
        if obj:
            try:
                await obj.cancel_pending_btc_entries(context.application, reason="btc_ai_4h_off_cancel_pending")
            except Exception:
                pass
        if btc_ai_task is None or btc_ai_task.done():
            ex = await get_exchange(await storage.all_settings())
            executor = ExecutionEngine(storage, ex)
            autopilot = BTCVisionAutopilot(storage, ex, executor)
            context.application.bot_data["btc_ai_autopilot"] = autopilot
            btc_ai_task = context.application.create_task(autopilot.run_loop(context.application))
        if position_task is None or position_task.done():
            globals()["position_task"] = context.application.create_task(position_management_loop(context.application))
        await reply(update, "⏹ BTC AI 4H автопилот ВЫКЛ\nНовые входы отключены. Открытые сделки не закрываются, виртуальное сопровождение остается включено.", reply_markup=MAIN_MENU)

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
    await storage.set("btc_ai_autopilot_enabled", False, bump_revision=False)
    obj = context.application.bot_data.get("btc_ai_autopilot")
    if obj: obj.stop()
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
    await storage.set("btc_ai_autopilot_enabled", False, bump_revision=False)
    obj = context.application.bot_data.get("btc_ai_autopilot")
    if obj: obj.stop()
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



def _btc_status_parse_ts(value) -> float:
    """Return unix seconds from MEXC ms timestamps, unix timestamps or ISO strings."""
    if value in (None, ""):
        return 0.0
    try:
        if isinstance(value, (int, float)):
            v = float(value)
            return v / 1000.0 if v > 10_000_000_000 else v
        txt = str(value).strip()
        if not txt:
            return 0.0
        if txt.replace('.', '', 1).isdigit():
            v = float(txt)
            return v / 1000.0 if v > 10_000_000_000 else v
        return datetime.fromisoformat(txt.replace('Z', '+00:00')).timestamp()
    except Exception:
        return 0.0


def _btc_status_age_text(opened_ts: float) -> str:
    if not opened_ts:
        return "unknown"
    age = max(0, int(time.time() - float(opened_ts)))
    h = age // 3600
    m = (age % 3600) // 60
    return f"{h}h {m}m"


def _btc_status_order_kind(order: dict, side: str = "", entry: float = 0.0) -> str:
    info = order.get("info") if isinstance(order.get("info"), dict) else {}
    kind = str(info.get("_protection_kind") or "").lower()
    typ = str(order.get("type") or info.get("orderType") or info.get("type") or "").lower()
    txt = " ".join(str(x or "").lower() for x in [
        kind, typ, order.get("clientOrderId"), info.get("externalOid"), info.get("clientOrderId"), info.get("_source_endpoint")
    ])
    if kind == "tp" or "tpsl_tp" in typ or "takeprofit" in txt or "take_profit" in txt or "bot_tp" in txt:
        return "TP"
    if kind == "sl" or "tpsl_sl" in typ or "stoploss" in txt or "stop_loss" in txt or "bot_sl" in txt:
        return "SL"
    # MEXC planorders made by BTC AI may not include a readable TP/SL kind.
    # Classify by trigger price relative to the real exchange entry.
    try:
        price = float(order.get("price") or info.get("triggerPrice") or info.get("trigger_price") or info.get("price") or 0)
    except Exception:
        price = 0.0
    side_u = str(side or "").upper()
    try:
        entry_f = float(entry or 0)
    except Exception:
        entry_f = 0.0
    if price > 0 and entry_f > 0 and side_u in {"LONG", "SHORT"}:
        if side_u == "LONG":
            return "TP" if price > entry_f else "SL"
        return "TP" if price < entry_f else "SL"
    trigger_type = str(info.get("triggerType") or "")
    if trigger_type in {"1", "2"}:
        return "TRIGGER"
    return "ORDER"




def _btc_status_order_ts(order: dict) -> float:
    info = order.get("info") if isinstance(order.get("info"), dict) else {}
    vals = [
        info.get("updateTime"), info.get("updatedTime"), info.get("updated_at"),
        info.get("createTime"), info.get("createdTime"), info.get("created_at"),
        order.get("timestamp"), order.get("datetime"),
    ]
    best = 0.0
    for v in vals:
        ts = _btc_status_parse_ts(v)
        if ts and ts > best:
            best = ts
    return best


def _btc_status_order_price(order: dict) -> float:
    info = order.get("info") if isinstance(order.get("info"), dict) else {}
    for k in ("price", "triggerPrice", "trigger_price", "stopPrice"):
        try:
            v = order.get(k) if k in order else info.get(k)
            if v not in (None, "") and float(v) > 0:
                return float(v)
        except Exception:
            pass
    return 0.0


def _btc_status_order_id(order: dict) -> str:
    info = order.get("info") if isinstance(order.get("info"), dict) else {}
    return str(order.get("id") or info.get("id") or info.get("orderId") or info.get("planOrderId") or "")


def _btc_status_split_current_protection(orders: list[dict], side: str, entry: float, batch_window_sec: float = 600.0):
    """Return (current_orders, stale_orders).

    For BTC AI after redeploy, local cache is intentionally empty.  The only
    safe exchange-first rule is: use the latest active SL for the current BTC
    position, then use TP planorders from the same creation batch.  Old active
    BTC planorders may still be live on MEXC, but they must not be used for
    status/BE tracking.
    """
    side_u = str(side or "").upper()
    try:
        entry_f = float(entry or 0)
    except Exception:
        entry_f = 0.0
    annotated = []
    for o in orders or []:
        kind = _btc_status_order_kind(o, side=side_u, entry=entry_f)
        price = _btc_status_order_price(o)
        ts = _btc_status_order_ts(o)
        oid = _btc_status_order_id(o)
        if not oid:
            oid = str(id(o))
        annotated.append({"order": o, "kind": kind, "price": price, "ts": ts, "id": oid})

    if not annotated:
        return [], []

    sls = [x for x in annotated if x["kind"] == "SL"]
    tps = [x for x in annotated if x["kind"] == "TP"]
    current_ids = set()

    if sls:
        latest_sl = max(sls, key=lambda x: (x["ts"], x["id"]))
        current_ids.add(latest_sl["id"])
        sl_ts = latest_sl["ts"]
        if sl_ts > 0:
            same_batch_tps = [x for x in tps if x["ts"] > 0 and abs(x["ts"] - sl_ts) <= batch_window_sec]
        else:
            same_batch_tps = []
        if not same_batch_tps:
            # Fall back to the newest two TPs when exchange timestamps are missing.
            same_batch_tps = sorted(tps, key=lambda x: (x["ts"], x["id"]), reverse=True)[:2]
    else:
        same_batch_tps = sorted(tps, key=lambda x: (x["ts"], x["id"]), reverse=True)[:2]

    # For a LONG, TP1 is the lower active TP above entry and TP2 the higher one.
    # For a SHORT, TP1 is the higher active TP below entry and TP2 the lower one.
    if side_u == "SHORT":
        same_batch_tps = sorted(same_batch_tps, key=lambda x: x["price"], reverse=True)[:2]
    else:
        same_batch_tps = sorted(same_batch_tps, key=lambda x: x["price"])[:2]
    for x in same_batch_tps:
        current_ids.add(x["id"])

    current = [x["order"] for x in annotated if x["id"] in current_ids]
    stale = [x["order"] for x in annotated if x["id"] not in current_ids]
    return current, stale

def _btc_status_local_meta(local_rows: list[dict]) -> dict:
    for p in local_rows or []:
        sym = str(p.get("symbol") or p.get("mexc_symbol") or "").upper().replace('/', '_')
        if str(p.get("strategy") or "") == "btc_ai_4h" or "BTC" in sym:
            if str(p.get("status") or "open").lower() in {"open", "pending", "closing"}:
                return p
    return {}


def _btc_status_deep_values(obj, keys: set[str], limit: int = 12) -> list[str]:
    """Return saved order ids from local BTC AI metadata, including nested exchange_result/protection attempts."""
    out = []
    seen = set()

    def walk(x):
        if len(out) >= limit:
            return
        if isinstance(x, dict):
            for k, v in x.items():
                if str(k) in keys and v not in (None, "", 0, "0"):
                    for part in str(v).replace(";", ",").split(","):
                        oid = part.strip()
                        if oid and oid not in seen:
                            seen.add(oid); out.append(oid)
                            if len(out) >= limit:
                                return
                if isinstance(v, (dict, list, tuple)):
                    walk(v)
        elif isinstance(x, (list, tuple)):
            for v in x:
                walk(v)
                if len(out) >= limit:
                    return

    walk(obj)
    return out



async def backtest_btc_patterns_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manual BTC historical tests only: digital fingerprints + US-open sweep.

    This command never opens/closes orders and never changes trading settings.
    """
    if not allowed(update):
        return
    try:
        await reply(update, "🧪 Запускаю BTC 4H backtest за 3 года. Торговая логика не меняется, сделок не открываю...", reply_markup=MAIN_MENU)
        years = 3.0
        symbol = "BTC_USDT"
        # Optional: /backtest_btc_patterns 2 or /backtest_btc_patterns BTC_USDT 3
        args = list(getattr(context, "args", []) or [])
        if args:
            if str(args[0]).upper().startswith("BTC"):
                symbol = args[0]
                if len(args) > 1:
                    years = max(0.5, min(5.0, float(args[1])))
            else:
                years = max(0.5, min(5.0, float(args[0])))
        settings = await storage.all_settings()
        ex = await get_exchange(settings)
        text, payload = await run_btc_pattern_backtest(ex, symbol=symbol, years=years)
        await reply(update, text[:3900], reply_markup=MAIN_MENU)
    except Exception as e:
        log_event("btc_pattern_backtest_cmd_error", ok=False, error=str(e)[:1200])
        await reply(update, f"❌ /backtest_btc_patterns error: {e}\nСырой лог: /log_full", reply_markup=MAIN_MENU)

async def backtest_btc_patterns_1h_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manual BTC 1H fingerprint test only. Never trades."""
    if not allowed(update):
        return
    try:
        await reply(update, "🧪 Запускаю BTC 1H fingerprint backtest за 3 года. Торговля не меняется, сделок не открываю...", reply_markup=MAIN_MENU)
        years = 3.0
        symbol = "BTC_USDT"
        args = list(getattr(context, "args", []) or [])
        if args:
            if str(args[0]).upper().startswith("BTC"):
                symbol = args[0]
                if len(args) > 1:
                    years = max(0.5, min(5.0, float(args[1])))
            else:
                years = max(0.5, min(5.0, float(args[0])))
        settings = await storage.all_settings()
        ex = await get_exchange(settings)
        text, payload = await run_btc_pattern_backtest_1h(ex, symbol=symbol, years=years)
        await reply(update, text[:3900], reply_markup=MAIN_MENU)
    except Exception as e:
        log_event("btc_pattern_backtest_1h_cmd_error", ok=False, error=str(e)[:1200])
        await reply(update, f"❌ /backtest_btc_patterns_1h error: {e}\nСырой лог: /log_full", reply_markup=MAIN_MENU)


async def _safe_edit_progress_message(app, chat_id: int, message_id: int, text: str):
    """Best-effort edit for long-running backtest progress; never blocks trading."""
    try:
        await asyncio.wait_for(
            app.bot.edit_message_text(chat_id=chat_id, message_id=int(message_id), text=str(text)[:3900]),
            timeout=6,
        )
    except Exception as e:
        # Telegram may answer "message is not modified" or timeout; keep the task alive.
        log_event("telegram_progress_edit_skipped", ok=False, error=str(e)[:250])


async def _round_levels_backtest_background(app, chat_id: int, progress_message_id: int, years: float):
    """Run round-level backtest in background so reply-keyboard button does not hit 45s timeout."""
    lines = [
        "🧪 ROUND LEVEL BACKTEST — progress",
        f"History requested: {years:g}y | BTC/ETH | 1H + 15m",
        "Trading logic: НЕ изменяется, сделок не открываю.",
        "",
    ]
    last_edit_ts = 0.0

    async def progress(line: str):
        nonlocal last_edit_ts
        line = str(line)
        lines.append(line)
        # Keep chat readable and avoid Telegram edit flood during API pagination.
        now = time.time()
        if line.endswith("calculated") or "error" in line.lower() or line.startswith("Round Levels") or now - last_edit_ts >= 2.0:
            last_edit_ts = now
            await _safe_edit_progress_message(app, chat_id, progress_message_id, "\n".join(lines[-18:]))

    try:
        settings = await storage.all_settings()
        ex = await get_exchange(settings)
        text, payload = await run_round_level_backtest(ex, years=years, progress_cb=progress)
        await progress("Report ready")
        try:
            await asyncio.wait_for(app.bot.send_message(chat_id=chat_id, text=text[:3900], reply_markup=MAIN_MENU), timeout=8)
        except Exception as e:
            log_event("round_level_backtest_report_send_error", ok=False, error=str(e)[:400])
            await _safe_edit_progress_message(app, chat_id, progress_message_id, ("\n".join(lines[-12:]) + "\n\n❌ Не смог отправить отчёт в чат. Сырой JSON: /log_full")[:3900])
        log_event("round_level_backtest_background_finish", ok=True, years=years)
    except Exception as e:
        log_event("round_level_backtest_background_error", ok=False, error=str(e)[:1200])
        await _safe_edit_progress_message(app, chat_id, progress_message_id, ("\n".join(lines[-12:]) + f"\n\n❌ Round Levels error: {str(e)[:500]}\nСырой лог: /log_full")[:3900])
    finally:
        app.bot_data["round_levels_backtest_running"] = False


async def backtest_round_levels_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manual BTC/ETH round-level reaction test only. Never trades."""
    if not allowed(update):
        return
    app = context.application
    if bool(app.bot_data.get("round_levels_backtest_running")):
        await reply(update, "⏳ Round Levels backtest уже выполняется. Дождись Report ready или проверь /log_full.", reply_markup=MAIN_MENU)
        return
    try:
        years = 3.0
        args = list(getattr(context, "args", []) or [])
        if args:
            years = max(0.5, min(5.0, float(args[0])))
        msg = await reply(
            update,
            "🧪 ROUND LEVEL BACKTEST — progress\n"
            f"History requested: {years:g}y | BTC/ETH | 1H + 15m\n"
            "Trading logic: НЕ изменяется, сделок не открываю.\n\n"
            "Round Levels started",
            reply_markup=MAIN_MENU,
        )
        chat = update.effective_chat
        if not msg or not chat:
            await reply(update, "❌ Не смог создать progress-сообщение. Попробуй ещё раз. /log_full", reply_markup=MAIN_MENU)
            return
        app.bot_data["round_levels_backtest_running"] = True
        app.create_task(_round_levels_backtest_background(app, int(chat.id), int(msg.message_id), years))
    except Exception as e:
        app.bot_data["round_levels_backtest_running"] = False
        log_event("round_level_backtest_cmd_error", ok=False, error=str(e)[:1200])
        await reply(update, f"❌ /backtest_round_levels error: {e}\nСырой лог: /log_full", reply_markup=MAIN_MENU)



async def _strategy_lab_backtest_background(app, chat_id: int, progress_message_id: int, years: float, mode: str):
    lines = [
        "🧪 STRATEGY LAB BACKTEST — progress",
        f"History requested: {years:g}y | mode={str(mode).upper()} | BTC/ETH",
        "Trading logic: НЕ изменяется, сделок не открываю.",
        "",
    ]
    last_edit_ts = 0.0

    async def progress(line: str, **meta):
        """Live Strategy Lab progress in the same chat message.
        Force an edit on every logical stage so the user sees that the test is alive.
        Heavy candle loading/calculation still runs in background and never trades.
        """
        nonlocal last_edit_ts
        line = str(line)
        if meta:
            try:
                details = []
                for k, v in meta.items():
                    if k in ("symbol", "timeframe", "candles", "variants", "years", "mode"):
                        details.append(f"{k}={v}")
                if details:
                    line = f"{line} ({', '.join(details)})"
            except Exception:
                pass
        lines.append(line)
        last_edit_ts = time.time()
        await _safe_edit_progress_message(app, chat_id, progress_message_id, "\n".join(lines[-22:])[:3900])
        log_event("strategy_lab_live_progress", ok=True, line=str(line)[:300], mode=mode)

    try:
        settings = await storage.all_settings()
        ex = await get_exchange(settings)
        text, payload = await (run_strategy_detail_backtest(ex, years=years, progress_cb=progress) if str(mode).lower().startswith("detail") else run_strategy_lab_backtest(ex, years=years, mode=mode, progress_cb=progress))
        await progress("Report ready")
        try:
            await asyncio.wait_for(app.bot.send_message(chat_id=chat_id, text=text[:3900], reply_markup=MAIN_MENU), timeout=8)
        except Exception as e:
            log_event("strategy_lab_report_send_error", ok=False, error=str(e)[:400])
            await _safe_edit_progress_message(app, chat_id, progress_message_id, ("\n".join(lines[-12:]) + "\n\n❌ Не смог отправить отчёт в чат. Сырой JSON: /log_full")[:3900])
        log_event("strategy_lab_background_finish", ok=True, years=years, mode=mode)
    except Exception as e:
        log_event("strategy_lab_background_error", ok=False, error=str(e)[:1200], years=years, mode=mode)
        await _safe_edit_progress_message(app, chat_id, progress_message_id, ("\n".join(lines[-12:]) + f"\n\n❌ Strategy Lab error: {str(e)[:500]}\nСырой лог: /log_full")[:3900])
    finally:
        app.bot_data["strategy_lab_backtest_running"] = False


async def backtest_strategy_lab_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manual strategy lab. Reads candles and calculates candidate statistics only."""
    if not allowed(update):
        return
    app = context.application
    if bool(app.bot_data.get("strategy_lab_backtest_running")):
        await reply(update, "⏳ Strategy Lab уже выполняется. Дождись Report ready или проверь /log_full.", reply_markup=MAIN_MENU)
        return
    try:
        years = 3.0
        mode = "safe"
        args = list(getattr(context, "args", []) or [])
        for a in args:
            aa = str(a).strip().lower()
            if aa in ("full", "max", "15m"):
                mode = "full"
            else:
                try:
                    years = max(0.5, min(5.0, float(aa)))
                except Exception:
                    pass
        msg = await reply(
            update,
            "🧪 STRATEGY LAB BACKTEST — progress\n"
            f"History requested: {years:g}y | mode={mode.upper()} | BTC/ETH\n"
            "Trading logic: НЕ изменяется, сделок не открываю.\n\n"
            "Strategy Lab started\n\n⏳ Это живое сообщение: этапы загрузки/расчёта будут обновляться здесь.",
            reply_markup=MAIN_MENU,
        )
        chat = update.effective_chat
        if not msg or not chat:
            await reply(update, "❌ Не смог создать progress-сообщение. Попробуй ещё раз. /log_full", reply_markup=MAIN_MENU)
            return
        app.bot_data["strategy_lab_backtest_running"] = True
        app.create_task(_strategy_lab_backtest_background(app, int(chat.id), int(msg.message_id), years, mode))
    except Exception as e:
        app.bot_data["strategy_lab_backtest_running"] = False
        log_event("strategy_lab_cmd_error", ok=False, error=str(e)[:1200])
        await reply(update, f"❌ /backtest_strategy_lab error: {e}\nСырой лог: /log_full", reply_markup=MAIN_MENU)


async def backtest_strategy_lab_extra_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manual extended strategy lab. Reads candles and calculates extended candidate statistics only."""
    if not allowed(update):
        return
    app = context.application
    if bool(app.bot_data.get("strategy_lab_backtest_running")):
        await reply(update, "⏳ Strategy Lab уже выполняется. Дождись Report ready или проверь /log_full.", reply_markup=MAIN_MENU)
        return
    try:
        years = 3.0
        mode = "detail_extra"
        args = list(getattr(context, "args", []) or [])
        for a in args:
            aa = str(a).strip().lower()
            try:
                years = max(0.5, min(5.0, float(aa)))
            except Exception:
                pass
        msg = await reply(
            update,
            "🧪 STRATEGY DETAIL — progress\n"
            f"History requested: {years:g}y | mode=DETAIL | BTC 4H SHORT-only\n"
            "Подробно считаю только BTC 4H RSI divergence SHORT-only: lookback 6/12/24, overbought 60/62/65/68, first-touch SL/TP.\n"
            "Trading logic: НЕ изменяется, сделок не открываю.\n\n"
            "Strategy Detail started\n\n⏳ Это живое сообщение: этапы загрузки/расчёта будут обновляться здесь.",
            reply_markup=MAIN_MENU,
        )
        chat = update.effective_chat
        if not msg or not chat:
            await reply(update, "❌ Не смог создать progress-сообщение. Попробуй ещё раз. /log_full", reply_markup=MAIN_MENU)
            return
        app.bot_data["strategy_lab_backtest_running"] = True
        app.create_task(_strategy_lab_backtest_background(app, int(chat.id), int(msg.message_id), years, mode))
    except Exception as e:
        app.bot_data["strategy_lab_backtest_running"] = False
        log_event("strategy_lab_extra_cmd_error", ok=False, error=str(e)[:1200])
        await reply(update, f"❌ /backtest_strategy_lab_extra error: {e}\nСырой лог: /log_full", reply_markup=MAIN_MENU)


async def backtest_aggressive_lab_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manual aggressive strategy lab. Wider read-only search, no trading side effects."""
    if not allowed(update):
        return
    app = context.application
    if bool(app.bot_data.get("strategy_lab_backtest_running")):
        await reply(update, "⏳ Strategy Lab уже выполняется. Дождись Report ready или проверь /log_full.", reply_markup=MAIN_MENU)
        return
    try:
        years = 3.0
        mode = "aggressive"
        args = list(getattr(context, "args", []) or [])
        for a in args:
            aa = str(a).strip().lower()
            if aa in ("full", "max", "15m"):
                mode = "aggressive_full"
            else:
                try:
                    years = max(0.5, min(5.0, float(aa)))
                except Exception:
                    pass
        msg = await reply(
            update,
            "🔥 AGGRESSIVE STRATEGY LAB — progress\n"
            f"History requested: {years:g}y | mode={mode.upper()} | BTC/ETH | 15m+1H+4H\n"
            "Goal: найти максимальную доходность и устойчивость.\n"
            "Trading logic: НЕ изменяется, сделок не открываю. OpenAI не вызываю.\n\n"
            "Aggressive Lab started\n\n⏳ Живое сообщение: этапы загрузки/расчёта будут обновляться здесь.",
            reply_markup=MAIN_MENU,
        )
        chat = update.effective_chat
        if not msg or not chat:
            await reply(update, "❌ Не смог создать progress-сообщение. Попробуй ещё раз. /log_full", reply_markup=MAIN_MENU)
            return
        app.bot_data["strategy_lab_backtest_running"] = True
        app.create_task(_strategy_lab_backtest_background(app, int(chat.id), int(msg.message_id), years, mode))
    except Exception as e:
        app.bot_data["strategy_lab_backtest_running"] = False
        log_event("aggressive_strategy_lab_cmd_error", ok=False, error=str(e)[:1200])
        await reply(update, f"❌ /backtest_aggressive_lab error: {e}\nСырой лог: /log_full", reply_markup=MAIN_MENU)

async def clean_btc_orders_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel stale active BTC planorders while keeping the latest current TP/SL batch."""
    if not allowed(update):
        return
    started = time.perf_counter()
    s = await storage.all_settings()
    api_key, api_secret = _api_creds(s)
    if not (api_key and api_secret):
        await reply(update, "🧽 Clean BTC Orders\n❌ MEXC API key/secret не настроены. Используй /api set KEY SECRET", reply_markup=MAIN_MENU)
        return
    try:
        ex = await get_exchange(s)
        exec_engine = ExecutionEngine(storage, ex)
        msym = ex.mexc_contract_symbol("BTC_USDT") if hasattr(ex, "mexc_contract_symbol") else "BTC_USDT"

        # 1) Real BTC position from exchange.  If there is no BTC position, every
        # active BTC planorder is stale and can be cancelled safely.
        raw_positions = await asyncio.wait_for(ex.fetch_positions(), timeout=12)
        exchange_positions = _dedupe_exchange_positions([p for p in (raw_positions or []) if exec_engine.exchange_position_qty(p) > 0], ex)
        btc_positions = []
        for p in exchange_positions:
            info = p.get("info") if isinstance(p.get("info"), dict) else {}
            sym = str(p.get("symbol") or p.get("mexc_symbol") or info.get("symbol") or "").upper()
            if "BTC" in sym:
                btc_positions.append(p)

        protect_side = ""
        protect_entry = 0.0
        if btc_positions:
            p0 = btc_positions[0]
            i0 = p0.get("info") if isinstance(p0.get("info"), dict) else {}
            protect_side = str(p0.get("side") or ("LONG" if str(i0.get("positionType")) == "1" else "SHORT" if str(i0.get("positionType")) == "2" else "")).upper()
            for k in ("entryPrice", "entry_price", "average", "holdAvgPrice", "openAvgPrice"):
                try:
                    v = p0.get(k) if k in p0 else i0.get(k)
                    if v not in (None, "") and float(v) > 0:
                        protect_entry = float(v); break
                except Exception:
                    pass

        # 2) Read only active BTC planorders from MEXC.  Do not use local cache.
        orders = []
        raw_ids = []
        out = await asyncio.wait_for(ex._mexc_private_read_any_base("/api/v1/private/planorder/list/orders", query={"symbol": msym, "state": 1, "page_num": 1, "page_size": 100}), timeout=12)
        rows = ex._mexc_rows(out.get("data")) if hasattr(ex, "_mexc_rows") else ((out or {}).get("data") or [])
        for row in rows:
            if not isinstance(row, dict):
                continue
            sym_raw = str(row.get("symbol") or row.get("contract") or "")
            try:
                sym_ok = ex._mexc_normalize_contract_id(sym_raw) == ex._mexc_normalize_contract_id(msym)
            except Exception:
                sym_ok = sym_raw.upper().replace("/", "_").replace(":USDT", "") == "BTC_USDT"
            if not sym_ok:
                continue
            state = str(row.get("state", "1")).lower()
            finished = str(row.get("is_finished", row.get("isFinished", 0))).lower()
            err_code = str(row.get("errorCode", row.get("error_code", 0))).lower()
            finished_yes = finished in {"1", "true", "yes"}
            active = state in {"1", "open", "created", "new"} and (not finished_yes) and err_code in {"0", "", "none"}
            if not active:
                continue
            row = dict(row)
            row.setdefault("_source_endpoint", "/api/v1/private/planorder/list/orders")
            raw_ids.append(str(row.get("id") or row.get("orderId") or ""))
            try:
                orders.append(ex._mexc_parse_order(row))
            except Exception:
                trig = float(row.get("triggerPrice") or row.get("price") or 0 or 0)
                vol = float(row.get("vol") or row.get("volume") or row.get("remainVol") or 0 or 0)
                orders.append({"id": str(row.get("id") or row.get("orderId") or ""), "symbol": "BTC/USDT:USDT", "side": "sell", "type": "planorder", "price": trig, "amount": vol, "remaining": vol, "status": "open", "info": row})

        if not orders:
            await reply(update, "🧽 Clean BTC Orders\nАктивных BTC TP/SL planorders на MEXC не найдено.", reply_markup=MAIN_MENU)
            return

        if btc_positions:
            current_orders, stale_orders = _btc_status_split_current_protection(orders, protect_side, protect_entry)
        else:
            current_orders, stale_orders = [], list(orders)

        current_ids = {_btc_status_order_id(o).split(":", 1)[0] for o in current_orders if _btc_status_order_id(o)}
        stale_unique = []
        seen = set()
        for o in stale_orders:
            oid = _btc_status_order_id(o).split(":", 1)[0].strip()
            if not oid or oid in current_ids or oid in seen:
                continue
            seen.add(oid)
            stale_unique.append(o)

        if not stale_unique:
            lines = ["🧽 Clean BTC Orders", "Старых активных BTC TP/SL не найдено."]
            if current_orders:
                lines.append("Оставлена актуальная защита:")
                for o in current_orders:
                    lines.append(f"KEEP {_btc_status_order_kind(o, protect_side, protect_entry)} price={_btc_status_order_price(o):.2f} id={_btc_status_order_id(o)[:18]}")
            await reply(update, "\n".join(lines)[:4050], reply_markup=MAIN_MENU)
            return

        # 3) Cancel only stale orders.  Never call cancel_all here.
        cancelled = []
        errors = []
        for o in stale_unique:
            oid = _btc_status_order_id(o).split(":", 1)[0].strip()
            try:
                res = await asyncio.wait_for(ex._mexc_private("POST", "/api/v1/private/planorder/cancel", body=[{"symbol": msym, "orderId": oid}]), timeout=10)
                ok = bool((res or {}).get("success", False) or (res or {}).get("code") in (0, "0"))
                cancelled.append({"id": oid, "ok": ok, "price": _btc_status_order_price(o), "kind": _btc_status_order_kind(o, protect_side, protect_entry), "res": res})
            except Exception as e:
                errors.append({"id": oid, "price": _btc_status_order_price(o), "kind": _btc_status_order_kind(o, protect_side, protect_entry), "error": str(e)[:260]})

        log_event(
            "clean_btc_orders",
            ok=(len(errors) == 0),
            active=len(orders), current=len(current_orders), stale=len(stale_unique),
            current_ids=list(current_ids), cancelled=[x.get("id") for x in cancelled], errors=errors[:10],
            side=protect_side, entry=protect_entry,
        )

        lines = [f"🧽 Clean BTC Orders v{VERSION}", "Source: MEXC active BTC planorders state=1"]
        if btc_positions:
            lines.append(f"Position: {protect_side or '?'} entry={protect_entry:.2f}")
        else:
            lines.append("Position: NONE — отменяю все активные BTC TP/SL planorders")
        lines.append(f"Found active: {len(orders)} | keep current: {len(current_orders)} | stale to cancel: {len(stale_unique)}")
        if current_orders:
            lines.append("\nKEEP current protection:")
            for o in current_orders[:4]:
                lines.append(f"KEEP {_btc_status_order_kind(o, protect_side, protect_entry)} price={_btc_status_order_price(o):.2f} id={_btc_status_order_id(o)[:18]}")
        lines.append("\nCancelled stale:")
        for x in cancelled[:12]:
            mark = "✅" if x.get("ok") else "⚠️"
            lines.append(f"{mark} {x['kind']} price={x['price']:.2f} id={str(x['id'])[:18]}")
        if len(cancelled) > 12:
            lines.append(f"... ещё {len(cancelled)-12}")
        if errors:
            lines.append("\nErrors:")
            for e in errors[:6]:
                lines.append(f"❌ {e['kind']} price={e['price']:.2f} id={str(e['id'])[:18]}: {e['error']}")
        lines.append(f"\nTime: {(time.perf_counter() - started):.1f}s")
        await reply(update, "\n".join(lines)[:4050], reply_markup=MAIN_MENU)
    except Exception as e:
        log_event("clean_btc_orders_error", ok=False, error=str(e)[:1200])
        await reply(update, f"🧽 Clean BTC Orders error: {str(e)[:900]}", reply_markup=MAIN_MENU)


async def status_btc_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Exchange-first BTC AI status: real MEXC position, orders, BE/24H state."""
    if not allowed(update):
        return
    started = time.perf_counter()
    s = await _ensure_secret_health("status_btc")
    api_key, api_secret = _api_creds(s)
    if not (api_key and api_secret):
        log_event("status_btc_api_missing_v79", ok=False, report=secret_source_report(s))
        await reply(update, "📊 BTC Status\n❌ MEXC API key/secret не настроены. Используй /api set KEY SECRET", reply_markup=MAIN_MENU)
        return
    try:
        ex = await get_exchange(s)
        exec_engine = ExecutionEngine(storage, ex)
        raw_positions = await asyncio.wait_for(ex.fetch_positions(), timeout=12)
        exchange_positions = _dedupe_exchange_positions([p for p in (raw_positions or []) if exec_engine.exchange_position_qty(p) > 0], ex)
        btc_positions = []
        for p in exchange_positions:
            info = p.get("info") if isinstance(p.get("info"), dict) else {}
            sym = str(p.get("symbol") or p.get("mexc_symbol") or info.get("symbol") or "").upper()
            if "BTC" in sym:
                btc_positions.append(p)
        local_rows = await storage.positions()
        local = _btc_status_local_meta(local_rows)
        lines = [f"📊 BTC Status v{VERSION}", "Source: MEXC exchange-first"]
        lines.append(f"BTC AI: {'ON' if s.get('btc_ai_autopilot_enabled') else 'OFF'} | Live: {'ON' if s.get('live_trading') else 'OFF'}")
        try:
            autopilot = context.application.bot_data.get("btc_ai_autopilot")
            if autopilot is None:
                autopilot = BTCVisionAutopilot(storage, ex, exec_engine)
            lines.append("Next 4H scan: " + autopilot._fmt_msk(autopilot.next_msk_close_ts()))
        except Exception:
            pass

        try:
            ticker = await asyncio.wait_for(ex.fetch_ticker("BTC_USDT"), timeout=8)
            last = float((ticker or {}).get("last") or (ticker or {}).get("close") or 0)
        except Exception:
            last = 0.0
        if last > 0:
            lines.append(f"BTC last: {last:.2f}")

        if not btc_positions:
            lines.append("\nExchange BTC position: NONE")
            try:
                snap = await _hidden_margin_snapshot(ex)
                if snap.get("hidden"):
                    lines.append("⚠️ Но баланс показывает скрытую маржу/exposure:")
                    lines.append(f"Used: {_fmt_money_value(snap.get('used'))} | Position margin: {_fmt_money_value(snap.get('positionMargin'))} | uPnL: {_fmt_money_value(snap.get('unrealized'))}")
            except Exception as e:
                lines.append(f"Hidden-margin check error: {str(e)[:160]}")
        else:
            for pos in btc_positions:
                info = pos.get("info") if isinstance(pos.get("info"), dict) else {}
                side = str(pos.get("side") or ("long" if str(info.get("positionType")) == "1" else "short" if str(info.get("positionType")) == "2" else "")).upper()
                entry = 0.0
                for key in ("entryPrice", "entry_price", "average"):
                    try:
                        v = pos.get(key)
                        if v not in (None, "") and float(v) > 0:
                            entry = float(v); break
                    except Exception:
                        pass
                if entry <= 0:
                    for key in ("holdAvgPrice", "openAvgPrice", "entryPrice"):
                        try:
                            v = info.get(key)
                            if v not in (None, "") and float(v) > 0:
                                entry = float(v); break
                        except Exception:
                            pass
                contracts, contract_size = _position_contract_fields(pos)
                qty = _position_base_qty(pos)
                lev = info.get("leverage") or pos.get("leverage") or s.get("btc_ai_leverage") or s.get("mexc_order_leverage") or "?"
                upnl = info.get("unrealized") or pos.get("unrealizedPnl") or pos.get("unrealized")
                margin = info.get("im") or info.get("oim") or pos.get("initialMargin") or ""
                opened_ts = 0.0
                for key in ("createTime", "createdTime", "created_at", "openTime", "timestamp"):
                    opened_ts = _btc_status_parse_ts(info.get(key) or pos.get(key))
                    if opened_ts:
                        break
                if not opened_ts and local:
                    for key in ("opened_at", "created_at", "entry_filled_at"):
                        opened_ts = _btc_status_parse_ts(local.get(key))
                        if opened_ts:
                            break
                be_or_profit = "n/a"
                if last > 0 and entry > 0 and side in {"LONG", "SHORT"}:
                    ok = (side == "LONG" and last >= entry) or (side == "SHORT" and last <= entry)
                    pct = ((last - entry) / entry * 100.0) if side == "LONG" else ((entry - last) / entry * 100.0)
                    be_or_profit = ("YES" if ok else "NO") + f" ({pct:+.3f}%)"
                lines.append("\nExchange BTC position:")
                lines.append(f"{side or '?'} | entry={entry:.2f} | qty={qty:.8f} BTC | contracts={contracts:.0f} | lev={lev}x")
                est_price_pnl = None
                try:
                    if (upnl in (None, "", "n/a") or str(upnl).lower() == "nan") and last > 0 and entry > 0 and qty > 0 and side in {"LONG", "SHORT"}:
                        est_price_pnl = (last - entry) * qty if side == "LONG" else (entry - last) * qty
                except Exception:
                    est_price_pnl = None
                if margin not in (None, "") or upnl not in (None, "") or est_price_pnl is not None:
                    upnl_text = _fmt_money_value(upnl) if upnl not in (None, "", "n/a") else (f"est {_fmt_money_value(est_price_pnl)}" if est_price_pnl is not None else "n/a")
                    lines.append(f"Margin={_fmt_money_value(margin)} | uPnL={upnl_text}")

                def _pick_info_num(*keys, default=None):
                    for k in keys:
                        try:
                            v = info.get(k) if isinstance(info, dict) else None
                            if v not in (None, ""):
                                return float(v)
                        except Exception:
                            pass
                        try:
                            v = pos.get(k)
                            if v not in (None, ""):
                                return float(v)
                        except Exception:
                            pass
                    return default

                realised = _pick_info_num("realised", "realized", "realizedPnl", "realisedPnl", "realizedProfit")
                fee = _pick_info_num("fee", "openFee", "closeFee", "takerFee", "makerFee")
                total_fee = _pick_info_num("totalFee", "total_fee", "totalFees")
                hold_fee = _pick_info_num("holdFee", "fundingFee", "funding", "funding_fee")
                extra_fee = _pick_info_num("extraTakerFee", "extraTakerFeeRate")

                cost_parts = []
                if realised is not None:
                    cost_parts.append(f"realised={_fmt_money_value(realised)}")
                if fee is not None:
                    cost_parts.append(f"fee={_fmt_money_value(fee)}")
                if total_fee is not None:
                    cost_parts.append(f"totalFee={_fmt_money_value(total_fee)}")
                if hold_fee is not None:
                    cost_parts.append(f"hold/funding={_fmt_money_value(hold_fee)}")
                if extra_fee is not None and abs(float(extra_fee or 0)) > 0:
                    cost_parts.append(f"extra={_fmt_money_value(extra_fee)}")
                if cost_parts:
                    lines.append("Costs/PnL: " + " | ".join(cost_parts))

                try:
                    if upnl not in (None, "", "n/a"):
                        upnl_f = float(upnl)
                    elif est_price_pnl is not None:
                        upnl_f = float(est_price_pnl)
                    else:
                        upnl_f = 0.0
                    realised_f = float(realised) if realised is not None else None
                    fee_f = float(fee) if fee is not None else 0.0
                    total_fee_f = float(total_fee) if total_fee is not None else None
                    hold_fee_f = float(hold_fee) if hold_fee is not None else 0.0
                    # On MEXC futures, realised commonly already includes executed fees/funding impact.
                    # Do not add fee/totalFee again when realised is present, otherwise status double-counts costs.
                    if realised_f is not None:
                        approx_net = upnl_f + realised_f + hold_fee_f
                        approx_note = "uPnL/est + realised + funding"
                    else:
                        approx_net = upnl_f + (total_fee_f if total_fee_f is not None else fee_f) + hold_fee_f
                        approx_note = "uPnL/est + fee + funding"
                    lines.append(f"Approx net now: {_fmt_money_value(approx_net)} ({approx_note}; no double-count fee)")
                except Exception:
                    pass
                lines.append(f"Age: {_btc_status_age_text(opened_ts)} | 24H BE/profit exit allowed now: {be_or_profit}")

        # Local metadata is not source of truth, but useful for AI TP/BE plan display.
        if local:
            reduced = bool(local.get("btc_ai_reduced_mode") or local.get("reduced_mode"))
            tp1 = float(local.get("partial_take_price") or local.get("take_profit_1") or 0)
            tp2 = float(local.get("final_take_price") or local.get("take_profit_2") or local.get("take_price") or 0)
            sl = float(local.get("stop_price") or local.get("stop_loss") or 0)
            details = local.get("signal_details") if isinstance(local.get("signal_details"), dict) else {}
            entry_l = local.get("entry_zone_low") or details.get("entry_zone_low")
            entry_h = local.get("entry_zone_high") or details.get("entry_zone_high")
            lines.append("\nBTC AI plan metadata:")
            lines.append(f"Mode: {'65-74 one TP' if reduced else '75+ TP1/TP2'} | TP1→BE: {'DONE' if (local.get('btc_ai_tp1_be_done') or local.get('breakeven_moved')) else ('ARMED' if local.get('move_sl_to_be_after_tp1') else 'OFF')}")
            if entry_l or entry_h:
                lines.append(f"Entry zone: {entry_l} - {entry_h}")
            lines.append(f"SL={sl:.2f} | TP1={tp1:.2f}" + (f" | TP2={tp2:.2f}" if not reduced and tp2 > 0 else ""))
            if local.get("btc_ai_24h_exit_done"):
                lines.append("24H exit: DONE")
            elif local.get("btc_ai_24h_wait_be_notified"):
                lines.append("24H exit: waiting for breakeven")
        else:
            lines.append("\nBTC AI plan metadata: none in local cache")

        # Protection orders from exchange.
        try:
            orders = await asyncio.wait_for(ex.fetch_open_orders("BTC_USDT"), timeout=12)
        except Exception as e:
            orders = []
            lines.append(f"\nProtection/orders read error: {str(e)[:220]}")

        # MEXC BTC AI protection is created as planorders. In some accounts/endpoint
        # combinations, generic fetch_open_orders() can return [] while active planorders
        # exist. First do a symbol-wide raw planorder read, so /status_btc remains
        # exchange-first even when local cache is empty after redeploy.
        status_planorder_verified = False
        if btc_positions and hasattr(ex, "_mexc_private_read_any_base"):
            try:
                msym = ex.mexc_contract_symbol("BTC_USDT") if hasattr(ex, "mexc_contract_symbol") else "BTC_USDT"
                raw_plan_orders = []
                for q in (
                    # MEXC Futures: state=1 is active/untriggered. state=2/unfiltered list includes history.
                    {"symbol": msym, "state": 1, "page_num": 1, "page_size": 100},
                ):
                    out = await asyncio.wait_for(ex._mexc_private_read_any_base("/api/v1/private/planorder/list/orders", query=q), timeout=8)
                    rows = ex._mexc_rows(out.get("data")) if hasattr(ex, "_mexc_rows") else ((out or {}).get("data") or [])
                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        sym_raw = str(row.get("symbol") or row.get("contract") or "")
                        try:
                            sym_ok = ex._mexc_normalize_contract_id(sym_raw) == ex._mexc_normalize_contract_id(msym)
                        except Exception:
                            sym_ok = sym_raw.upper().replace("/", "_").replace(":USDT", "") == "BTC_USDT"
                        if not sym_ok:
                            continue
                        state = str(row.get("state", "1")).lower()
                        finished = str(row.get("is_finished", row.get("isFinished", 0))).lower()
                        err_code = str(row.get("errorCode", row.get("error_code", 0))).lower()
                        # Only state=1 is active/untriggered. state=2 and unfiltered rows are history/triggered
                        # on MEXC planorder/list/orders and must not be shown as current BTC protection.
                        finished_yes = finished in {"1", "true", "yes"}
                        active = state in {"1", "open", "created", "new"} and (not finished_yes) and err_code in {"0", "", "none"}
                        if not active:
                            continue
                        row = dict(row)
                        row.setdefault("_source_endpoint", "/api/v1/private/planorder/list/orders")
                        if not any(str((o.get("info") or {}).get("id") or o.get("id")) == str(row.get("id") or row.get("orderId")) for o in orders):
                            try:
                                orders.append(ex._mexc_parse_order(row))
                            except Exception:
                                trig = float(row.get("triggerPrice") or row.get("price") or 0 or 0)
                                vol = float(row.get("vol") or row.get("volume") or row.get("remainVol") or 0 or 0)
                                orders.append({"id": str(row.get("id") or row.get("orderId") or ""), "symbol": "BTC/USDT:USDT", "side": "sell", "type": "planorder", "price": trig, "amount": vol, "remaining": vol, "status": "open", "info": row})
                            raw_plan_orders.append(str(row.get("id") or row.get("orderId") or ""))
                if raw_plan_orders:
                    status_planorder_verified = True
                    lines.append(f"\nProtection source: MEXC state=1 active BTC planorders ({len(raw_plan_orders)})")
            except Exception as e:
                lines.append(f"\nPlanorder raw read warning: {str(e)[:220]}")

        if (not orders) and btc_positions and local and hasattr(ex, "mexc_find_active_plan_order"):
            try:
                plan_ids = []
                for kind, keys in [
                    ("tp", {"tp1_order_id", "tp2_order_id", "tp_order_id"}),
                    ("sl", {"sl_order_id", "stop_order_id"}),
                ]:
                    for oid in _btc_status_deep_values(local, keys):
                        row = await asyncio.wait_for(ex.mexc_find_active_plan_order("BTC_USDT", order_id=oid), timeout=8)
                        if row:
                            row = dict(row)
                            row["_protection_kind"] = kind
                            row.setdefault("_source_endpoint", "/api/v1/private/planorder/list/orders")
                            try:
                                orders.append(ex._mexc_parse_order(row))
                            except Exception:
                                trig = float(row.get("triggerPrice") or row.get("price") or 0 or 0)
                                vol = float(row.get("vol") or row.get("volume") or row.get("remainVol") or 0 or 0)
                                orders.append({"id": str(row.get("id") or row.get("orderId") or oid), "symbol": "BTC/USDT:USDT", "side": "sell", "type": f"tpsl_{kind}", "price": trig, "amount": vol, "remaining": vol, "status": "open", "info": row})
                            plan_ids.append(str(oid))
                status_planorder_verified = bool(orders)
                if status_planorder_verified:
                    lines.append(f"\nProtection source: verified saved MEXC planorder ids ({len(orders)})")
            except Exception as e:
                lines.append(f"\nPlanorder id verify warning: {str(e)[:220]}")

        protect_side = ""
        protect_entry = 0.0
        try:
            if btc_positions:
                p0 = btc_positions[0]
                i0 = p0.get("info") if isinstance(p0.get("info"), dict) else {}
                protect_side = str(p0.get("side") or ("LONG" if str(i0.get("positionType")) == "1" else "SHORT" if str(i0.get("positionType")) == "2" else "")).upper()
                for k in ("entryPrice", "entry_price", "average", "holdAvgPrice", "openAvgPrice"):
                    v = p0.get(k) if k in p0 else i0.get(k)
                    if v not in (None, "") and float(v) > 0:
                        protect_entry = float(v); break
        except Exception:
            pass

        if orders:
            if not btc_positions:
                # No real BTC position means there is no valid current protection.
                # Any active BTC planorders are orphaned leftovers and must not be
                # classified as TP/SL for management.  /clean_btc_orders can cancel
                # them manually; the background manager also attempts silent cleanup.
                display_orders = list(orders)
                lines.append("\nMEXC BTC orphan active planorders: POSITION NONE")
                shown = 0
                for o in display_orders:
                    if shown >= 8:
                        break
                    price = _btc_status_order_price(o)
                    amount = float(o.get("amount") or o.get("remaining") or 0)
                    oid = _btc_status_order_id(o)[:18]
                    src = str((o.get("info") or {}).get("_source_endpoint") or "").replace("/api/v1/private/", "")
                    age_ts = _btc_status_order_ts(o)
                    age_txt = datetime.fromtimestamp(age_ts, tz=timezone.utc).strftime("%H:%M:%S UTC") if age_ts else "no-ts"
                    lines.append(f"ORPHAN TRIGGER: price={price:.2f} amount={amount:.8f} id={oid} ts={age_txt} src={src}")
                    shown += 1
                lines.append(f"Orphan summary: active={len(display_orders)} current=0")
                lines.append("⚠️ BTC позиции нет, эти активные triggers не являются защитой. Нажми /clean_btc_orders, если они не исчезли автоматически.")
                log_event("status_btc_orphan_planorders", ok=True, current=0, orphan=len(display_orders), ids=[_btc_status_order_id(o) for o in display_orders[:20]])
            else:
                current_orders, stale_orders = _btc_status_split_current_protection(orders, protect_side, protect_entry)
                display_orders = current_orders or orders
                lines.append("\nMEXC BTC current protection (latest active batch):")
                tp_count = sl_count = other_count = 0
                shown = 0
                for o in display_orders:
                    if shown >= 6:
                        break
                    kind = _btc_status_order_kind(o, side=protect_side, entry=protect_entry)
                    if kind == "TP": tp_count += 1
                    elif kind == "SL": sl_count += 1
                    else: other_count += 1
                    price = _btc_status_order_price(o)
                    amount = float(o.get("amount") or o.get("remaining") or 0)
                    oid = _btc_status_order_id(o)[:18]
                    src = str((o.get("info") or {}).get("_source_endpoint") or "").replace("/api/v1/private/", "")
                    age_ts = _btc_status_order_ts(o)
                    age_txt = datetime.fromtimestamp(age_ts, tz=timezone.utc).strftime("%H:%M:%S UTC") if age_ts else "no-ts"
                    lines.append(f"{kind}: price={price:.2f} amount={amount:.8f} id={oid} ts={age_txt} src={src}")
                    shown += 1
                lines.append(f"Current summary: TP={tp_count} SL={sl_count} OTHER={other_count} current={len(display_orders)}")
                if stale_orders:
                    lines.append(f"Stale active BTC planorders ignored by bot: {len(stale_orders)}")
                    for o in stale_orders[:4]:
                        kind = _btc_status_order_kind(o, side=protect_side, entry=protect_entry)
                        lines.append(f"STALE {kind}: price={_btc_status_order_price(o):.2f} id={_btc_status_order_id(o)[:18]}")
                log_event("status_btc_current_protection_selected", ok=True, current=len(display_orders), stale=len(stale_orders), side=protect_side, entry=protect_entry, current_ids=[_btc_status_order_id(o) for o in display_orders], stale_ids=[_btc_status_order_id(o) for o in stale_orders[:20]])
        else:
            lines.append("\nMEXC BTC open orders/protection: none")
            if btc_positions:
                lines.append("⚠️ ВНИМАНИЕ: BTC позиция есть, но видимых TP/SL на MEXC не найдено.")
                lines.append("Если это не сразу после открытия, нажми 🧯 Panic или Close All, либо проверь /log_full.")

        lines.append(f"\nTime: {(time.perf_counter() - started):.1f}s")
        await reply(update, "\n".join(lines)[:4050], reply_markup=MAIN_MENU)
    except Exception as e:
        log_event("status_btc_error", ok=False, error=str(e)[:1200])
        await reply(update, f"📊 BTC Status error: {str(e)[:900]}", reply_markup=MAIN_MENU)

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
    try:
        ensure_runtime_secrets_loaded(settings or {})
        settings = merge_secrets_into_settings(settings or {})
    except Exception:
        pass
    api_key, api_secret = _api_creds(settings)
    if not (api_key and api_secret):
        # One last SQLite repair attempt from current runtime/exchange env.
        settings = await _repair_sqlite_secrets_from_runtime(reason="balance_missing")
        api_key, api_secret = _api_creds(settings)
    if not (api_key and api_secret):
        report = secret_source_report(settings or {})
        log_event("balance_api_missing_v11", ok=False, report=report)
        return {}, "MEXC API key/secret не сохранены в SQLite. Отправь один раз: /api set API_KEY API_SECRET. source=" + str(report)[:300]
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
        s = await _ensure_secret_health("balance")
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

async def _get_exchange_emergency(settings: dict, timeout_sec: float = 25.0):
    """Get an exchange client for panic commands without a short 6s timeout.

    /close_all and /cancel_all must not fail just because ccxt/load_markets is slow.
    Reuse the existing initialized client when possible; otherwise allow a longer
    init window. ExchangeClient.init already skips slow load_markets internally.
    """
    global exchange_client
    if exchange_client is not None:
        return exchange_client
    return await _await_with_timeout(get_exchange(settings), timeout_sec, "exchange init")


async def cancel_all_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global entries_enabled
    if not allowed(update): return
    entries_enabled = False
    _boost_disarm_runtime(context.application)
    await storage.set("boost_autopilot_active", False, bump_revision=False)
    await storage.set("btc_ai_autopilot_enabled", False, bump_revision=False)
    await storage.set("strategy_mode", "hybrid", bump_revision=False)
    await reply(update, "⏳ Cancel all orders: command received. New entries are OFF. Cancelling MEXC orders now...", reply_markup=MAIN_MENU)
    s = await storage.all_settings()
    results = []
    failures = []
    try:
        ex = await _get_exchange_emergency(s, 25)
        # First cancel BTC explicitly. This catches BTC AI planorders even when
        # order discovery is slow or MEXC rate-limits list endpoints.
        for sym in ["BTC_USDT", "BTC/USDT:USDT", None]:
            try:
                res = await _await_with_timeout(ex.cancel_all_orders(sym), 35, f"cancel_all_orders {sym or '*'}")
                results.append({"symbol": sym or "*", "result": res})
            except Exception as e:
                failures.append(f"{sym or '*'}: {e}")
        ok_count = sum(1 for r in results if r.get("result") is not None)
        msg = f"🧹 Cancel all finished\nRequests sent: {ok_count}\nFailures: {failures[:3] if failures else '-'}"
        await reply(update, msg[:3500], reply_markup=MAIN_MENU)
    except Exception as e:
        await reply(update, f"🧹 Cancel all failed: {e}", reply_markup=MAIN_MENU)


async def close_all_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global entries_enabled
    if not allowed(update): return
    s = await storage.all_settings()
    entries_enabled = False
    _boost_disarm_runtime(context.application)
    await storage.set("boost_autopilot_active", False, bump_revision=False)
    await storage.set("btc_ai_autopilot_enabled", False, bump_revision=False)
    await storage.set("btc_ai_live_test_enabled", False, bump_revision=False)
    await storage.set("strategy_mode", "hybrid", bump_revision=False)
    await reply(update, "⏳ Close all positions: command received. New entries are OFF. HARD closing MEXC positions now...", reply_markup=MAIN_MENU)
    failures = []
    hard_res = None
    post_used = post_pm = None
    local_cache_cleared = False
    post_positions = []
    try:
        ex = await _get_exchange_emergency(s, 45)
        exec_engine = ExecutionEngine(storage, ex)

        # v026 hard panic close: use native MEXC open_positions + holdVol close.
        if hasattr(ex, "mexc_hard_close_all_positions"):
            hard_res = await _await_with_timeout(
                ex.mexc_hard_close_all_positions(None, retries=5),
                90,
                "mexc_hard_close_all_positions",
            )
        else:
            # Fallback for non-MEXC or older client.
            positions = [p for p in (await _await_with_timeout(ex.fetch_positions(), 20, "fetch_positions") or []) if exec_engine.exchange_position_qty(p) > 0]
            close_results = []
            for p in positions:
                close_results.append(await _await_with_timeout(exec_engine.close_exchange_position(p, "manual_close_all"), 35, "close_exchange_position"))
            try:
                await _await_with_timeout(ex.cancel_all_orders(), 35, "cancel all orders after close")
            except Exception as e:
                failures.append(f"cancel_after_all: {e}")
            hard_res = {"ok": True, "results": close_results, "errors": []}

        if isinstance(hard_res, dict):
            failures.extend([str(x)[:220] for x in (hard_res.get("errors") or [])])
            post_positions = hard_res.get("remaining_positions") or []

        await asyncio.sleep(float(os.getenv("POST_CLOSE_BALANCE_CHECK_DELAY_SEC", "1.2")))
        try:
            bal = await asyncio.wait_for(ex.fetch_balance(), timeout=15)
            usdt = (bal or {}).get("USDT", {}) if isinstance(bal, dict) else {}
            post_pm = float(usdt.get("positionMargin") or usdt.get("position_margin") or 0)
            post_used = float(usdt.get("used") or ((bal or {}).get("used", {}) or {}).get("USDT") or 0)
        except Exception as e:
            failures.append(f"post_balance: {e}")
        if not post_positions:
            try:
                post_positions = [p for p in (await asyncio.wait_for(ex.fetch_positions(), timeout=20) or []) if exec_engine.exchange_position_qty(p) > 0]
            except Exception:
                post_positions = []

        exchange_flat = not post_positions and (post_pm is None or float(post_pm or 0) < 0.0001)
        if exchange_flat:
            for lp in await storage.positions():
                try:
                    await storage.remove_position(lp.get("symbol"))
                except Exception:
                    pass
            local_cache_cleared = True

        status = "✅ ALL POSITIONS CLOSED" if exchange_flat else "⚠️ CLOSE SENT, BUT POSITION STILL VISIBLE"
        msg = (
            f"🧯 {status}\n"
            f"Hard close ok: {bool(isinstance(hard_res, dict) and hard_res.get('ok'))}\n"
            f"Exchange open positions: {len(post_positions or [])}\n"
            f"Post used: {post_used if post_used is not None else '-'}\n"
            f"Post position margin: {post_pm if post_pm is not None else '-'}\n"
            f"Local cache cleared: {'yes' if local_cache_cleared else 'no'}\n"
            f"Failures: {failures[:5] if failures else '-'}"
        )
        await reply(update, msg[:3800], reply_markup=MAIN_MENU)
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
        os.environ["OPENAI_API_KEY"] = key
        set_runtime_secret_cache({"openai_api_key": key})
        # V11: SQLite is the persistent source. No backup/cache file write.
        log_event("openai_key_sqlite_set_v11", ok=True, sqlite=True, runtime_env=True, backup=False)
        await reply(update, "✅ OpenAI API key saved", reply_markup=MAIN_MENU)
        return
    if cmd == "clear":
        await storage.set("openai_api_key", "")
        os.environ.pop("OPENAI_API_KEY", None)
        clear_runtime_secret_cache(["openai_api_key"])
        clear_secret_backup(["openai_api_key"])
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
    s = await _ensure_secret_health("api_cmd")
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
        api_key_new = str(context.args[1]).strip()
        api_secret_new = str(context.args[2]).strip()
        await storage.set("mexc_api_key", api_key_new)
        await storage.set("mexc_api_secret", api_secret_new)
        os.environ["MEXC_API_KEY"] = api_key_new
        os.environ["MEXC_API_SECRET"] = api_secret_new
        set_runtime_secret_cache({"mexc_api_key": api_key_new, "mexc_api_secret": api_secret_new})
        # V11: SQLite is the persistent source. No backup/cache file write.
        verify_s = await storage.all_settings()
        verify_key = str(await storage.get("mexc_api_key") or "").strip()
        verify_secret = str(await storage.get("mexc_api_secret") or "").strip()
        log_event("api_keys_sqlite_set_v11", ok=bool(verify_key and verify_secret), sqlite=True, runtime_env=True, backup=False, key_mask=mask_secret(verify_key), secret_mask=mask_secret(verify_secret))
        if not (verify_key and verify_secret):
            await reply(update, "❌ API не сохранился в SQLite. Сделки не запускаю. Повтори /api set API_KEY API_SECRET", reply_markup=MAIN_MENU)
            return
        cleared = 0
        try:
            cleared = await storage.clear_positions()
            log_event("api_set_local_position_cache_cleared", ok=True, cleared=cleared, source="v52_exchange_source_of_truth")
        except Exception as e:
            log_event("api_set_local_position_cache_clear_failed", ok=False, error=str(e)[:500])
        await reset_exchange()
        # V79: immediately sync real MEXC state after the first /api set.
        # This is read-only: it does not open/close positions. It warms the exchange
        # client and BTC autopilot cache so position control starts from exchange truth.
        sync_note = "sync: not run"
        try:
            sync_settings = merge_secrets_into_settings(await storage.all_settings())
            ex = await get_exchange(sync_settings)
            exec_engine = ExecutionEngine(storage, ex)
            raw_positions = await asyncio.wait_for(ex.fetch_positions(), timeout=12)
            positions = [p for p in (raw_positions or []) if exec_engine.exchange_position_qty(p) > 0]
            btc_positions = []
            for p in positions:
                info = p.get("info") if isinstance(p.get("info"), dict) else {}
                sym = str(p.get("symbol") or p.get("mexc_symbol") or info.get("symbol") or "").upper()
                if "BTC" in sym:
                    btc_positions.append(p)
            btc_orders = []
            try:
                btc_orders = await asyncio.wait_for(ex.fetch_open_orders("BTC_USDT"), timeout=12) or []
            except Exception as oe:
                log_event("api_set_exchange_orders_sync_failed_v79", ok=False, error=str(oe)[:500])
            autopilot = context.application.bot_data.get("btc_ai_autopilot") if context and context.application else None
            if autopilot is not None:
                try:
                    autopilot._btc_exchange_position_cache = list(btc_positions)
                    autopilot._btc_exchange_position_seen = bool(btc_positions)
                    autopilot._last_position_sync_ts = 0.0
                    autopilot._last_protection_sync_ts = 0.0
                except Exception:
                    pass
            log_event("api_set_exchange_sync_v79", ok=True, positions=len(positions), btc_positions=len(btc_positions), btc_open_orders=len(btc_orders))
            sync_note = f"Exchange sync: positions={len(positions)}, BTC positions={len(btc_positions)}, BTC orders={len(btc_orders)}"
        except Exception as e:
            log_event("api_set_exchange_sync_v79", ok=False, error=str(e)[:700])
            sync_note = f"Exchange sync error: {str(e)[:160]}"
        await reply(update, f"✅ API saved\nKey: {mask_secret(context.args[1])}\nSecret: {mask_secret(context.args[2])}\nLocal position cache cleared: {cleared}\n{sync_note}\n\nТеперь можно /api test", reply_markup=MAIN_MENU)
        return
    if cmd == "clear":
        await storage.set("mexc_api_key", "")
        await storage.set("mexc_api_secret", "")
        os.environ.pop("MEXC_API_KEY", None)
        os.environ.pop("MEXC_API_SECRET", None)
        clear_runtime_secret_cache(["mexc_api_key", "mexc_api_secret"])
        clear_secret_backup(["mexc_api_key", "mexc_api_secret"])
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
        s = await _ensure_secret_health("balance")
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
        "quick_bounce_max_spread_pct": 0.30,
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


async def impulse_dump_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    global running, entries_enabled, trading_task, position_task
    s = await storage.all_settings()
    enabled = str(s.get("strategy_mode", "hybrid")).lower() == "impulse_dump" and _bool_setting(s, "impulse_dump_enabled", False)
    if enabled:
        await storage.set("impulse_dump_enabled", False, bump_revision=False)
        await storage.set("settings_revision", int(s.get("settings_revision", 1) or 1) + 1, bump_revision=False)
        trigger_scan_now(context.application, reason="impulse_dump:off")
        await reply(update, "○ Импульсный слив OFF\nСканер остановлен, новые SHORT не открываются. Открытые позиции продолжают сопровождаться до TP/SL/24h.", reply_markup=MAIN_MENU)
        return

    updates = {
        "impulse_dump_enabled": True,
        "quick_bounce_enabled": False,
        "strategy_mode": "impulse_dump",
        "universe_mode": "top-200",
        "max_symbols": 200,
        "scan_interval_sec": 900,
        "symbol_refresh_sec": 900,
        "max_open_positions": 5,
        "trade_margin_pct": 0.10,
        "impulse_dump_trade_margin_pct": 0.10,
        "mexc_order_leverage": 10,
        "impulse_dump_leverage": 10,
        "impulse_dump_sl_pct": 2.0,
        "impulse_dump_total_drop_target_pct": 10.0,
        "impulse_dump_min_drop_pct": 3.0,
        "impulse_dump_max_drop_pct": 6.0,
        "impulse_dump_15m_min_drop_pct": 0.1,
        "impulse_dump_15m_max_drop_pct": 6.0,
        "impulse_dump_4h_max_drop_pct": 6.0,
        "impulse_dump_24h_max_drop_pct": 6.0,
        "impulse_dump_time_stop_sec": 86400,
        "impulse_dump_top_coins": 200,
        "impulse_dump_max_open_positions": 5,
        "impulse_dump_max_candidates": 5,
        "impulse_dump_min_volume_ratio": 1.05,
        "impulse_dump_max_spread_pct": 0.30,
        "impulse_dump_min_24h_volume_usdt": 20000000.0,
        "impulse_dump_btc_filter_enabled": True,
        "impulse_dump_btc_max_pump_1h_pct": 1.5,
        "cooldown_after_close_sec": 0,
        "impulse_dump_cooldown_after_close_sec": 0,
        "max_daily_loss_pct": 5.0,
        "impulse_dump_max_daily_loss_pct": 5.0,
        "impulse_dump_stop_after_consecutive_sl": 3,
        "impulse_dump_anomaly_timeframe": "1h",
        "impulse_dump_confirm_timeframe": "15m",
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
    context.application.bot_data["impulse_dump_consecutive_sl"] = 0
    if trading_task is None or trading_task.done():
        trading_task = context.application.create_task(trading_loop(context.application))
    if position_task is None or position_task.done():
        position_task = context.application.create_task(position_management_loop(context.application))
    trigger_scan_now(context.application, reason="impulse_dump:on")
    await reply(
        update,
        "✅ Импульсный слив ON\n"
        "Топ-200, скан 15m, только SHORT, падение 3–6% за 1h/4h, до 5 сделок, 10% депозита, 10x isolated.\n"
        "SL +2% от входа, TP считается до общего 24h падения ≈10%, time-stop 24h. После 3 SL подряд режим стоп до следующего дня.",
        reply_markup=MAIN_MENU,
    )



async def knife_reversal_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    s = await storage.all_settings()
    enabled = str(s.get("strategy_mode", "hybrid")).lower() == "knife_reversal" and _bool_setting(s, "knife_reversal_enabled", False)
    if enabled:
        for _k, _v in {"knife_reversal_enabled": False, "settings_revision": int(s.get("settings_revision", 1) or 1) + 1}.items():
            await storage.set(_k, _v, bump_revision=False)
        trigger_scan_now(context.application, reason="knife_reversal:off")
        await update.message.reply_text("🗡 knife_reversal OFF. Existing positions are still managed.", reply_markup=MAIN_MENU)
        return
    updates = {
        "knife_reversal_enabled": True, "orderflow_impulse_enabled": False, "multi_strategy_enabled": False,
        "strategy_mode": "knife_reversal", "auto_strategy_adaptation": False, "regime_adaptation": False,
        "max_open_positions": 3, "scan_interval_sec": 60,
        "knife_reversal_top_coins": 100, "knife_reversal_scan_interval_sec": 60,
        "knife_reversal_trade_margin_pct": 0.10, "knife_reversal_max_open_positions": 3, "knife_reversal_leverage": 10,
        "knife_reversal_tp_pct": 5.0, "knife_reversal_wick_sl_buffer_pct": 0.20,
        "knife_reversal_min_reclaim_pct": 50.0, "knife_reversal_min_volume_ratio": 2.0,
        "settings_revision": int(s.get("settings_revision", 1) or 1) + 1,
    }
    for _k, _v in updates.items():
        await storage.set(_k, _v, bump_revision=False)
    trigger_scan_now(context.application, reason="knife_reversal:on")
    await update.message.reply_text(
        "🗡 knife_reversal ON\n"
        "Скан: Binance spot top-100 каждые 60s.\n"
        "Вход: LONG после сильного нижнего фитиля и reclaim ≥50%.\n"
        "TP: 5%. SL: чуть ниже low фитиля. AI-check: по тумблеру настроек.",
        reply_markup=MAIN_MENU,
    )

async def multi_strategy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    s = await storage.all_settings()
    enabled = str(s.get("strategy_mode", "hybrid")).lower() == "multi_strategy" and _bool_setting(s, "multi_strategy_enabled", False)
    if enabled:
        for _k, _v in {"multi_strategy_enabled": False, "settings_revision": int(s.get("settings_revision", 1) or 1) + 1}.items():
            await storage.set(_k, _v, bump_revision=False)
        trigger_scan_now(context.application, reason="multi_strategy:off")
        await update.message.reply_text("🧠 multi_strategy OFF. Existing positions are still managed.", reply_markup=MAIN_MENU)
        return
    updates = {
        "multi_strategy_enabled": True, "orderflow_impulse_enabled": True, "knife_reversal_enabled": True, "strongest_coin_enabled": False,
        "strategy_mode": "multi_strategy", "auto_strategy_adaptation": False, "regime_adaptation": False,
        "max_open_positions": 3, "scan_interval_sec": 60, "multi_strategy_top_coins": 100, "multi_strategy_scan_interval_sec": 60, "multi_strategy_max_open_positions": 3,
        "orderflow_impulse_top_coins": 100, "orderflow_impulse_scan_interval_sec": 60, "orderflow_impulse_max_open_positions": 3,
        "knife_reversal_top_coins": 100, "knife_reversal_scan_interval_sec": 60, "knife_reversal_max_open_positions": 3,
        "knife_reversal_tp_pct": 5.0, "knife_reversal_wick_sl_buffer_pct": 0.20,
        "settings_revision": int(s.get("settings_revision", 1) or 1) + 1,
    }
    for _k, _v in updates.items():
        await storage.set(_k, _v, bump_revision=False)
    trigger_scan_now(context.application, reason="multi_strategy:on")
    await update.message.reply_text(
        "🧠 multi_strategy ON\n"
        "Сканирует orderflow_impulse + knife_reversal: top-100 каждые 60s.\n"
        "Общие 3 слота. AI-check общий тумблером настроек. Открывается strongest setup.",
        reply_markup=MAIN_MENU,
    )



async def strongest_coin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    global running, entries_enabled, trading_task, position_task
    s = await storage.all_settings()
    enabled = str(s.get("strategy_mode", "hybrid")).lower() == "strongest_coin" and _bool_setting(s, "strongest_coin_enabled", False)
    if enabled:
        await storage.set("strongest_coin_enabled", False, bump_revision=False)
        await storage.set("settings_revision", int(s.get("settings_revision", 1) or 1) + 1, bump_revision=False)
        trigger_scan_now(context.application, reason="strongest_coin:off")
        await reply(update, "○ Strongest coin OFF\nСканер остановлен, новые сделки не открываются. Открытые позиции продолжают сопровождаться до TP/SL/time-stop.", reply_markup=MAIN_MENU)
        return

    updates = {
        "strongest_coin_enabled": True,
        "cascade_hunter_enabled": False,
        "orderflow_impulse_enabled": False,
        "knife_reversal_enabled": False,
        "multi_strategy_enabled": False,
        "quick_bounce_enabled": False,
        "impulse_dump_enabled": False,
        "strategy_mode": "strongest_coin",
        "universe_mode": "top-200",
        "max_symbols": 200,
        "scan_interval_sec": 60,
        "symbol_refresh_sec": 300,
        "max_open_positions": 1,
        "trade_margin_pct": 0.10,
        "cooldown_after_close_sec": 3600,
        "strongest_coin_top_coins": 200,
        "strongest_coin_scan_interval_sec": 60,
        "strongest_coin_trade_margin_pct": 0.10,
        "strongest_coin_max_open_positions": 1,
        "strongest_coin_leverage": 10,
        "strongest_coin_min_24h_volume_usdt": 5000000.0,
        "strongest_coin_max_spread_pct": 0.15,
        "strongest_coin_min_strength_score": 0.60,
        "strongest_coin_min_rs_btc_15m_pct": 0.50,
        "strongest_coin_btc_panic_5m_pct": -1.50,
        "strongest_coin_min_pullback_pct": 0.35,
        "strongest_coin_max_pullback_pct": 1.80,
        "strongest_coin_max_pullback_depth": 0.45,
        "strongest_coin_stop_buffer_pct": 0.25,
        "strongest_coin_min_sl_pct": 1.20,
        "strongest_coin_max_sl_pct": 2.20,
        "strongest_coin_tp1_r": 1.0,
        "strongest_coin_tp2_r": 2.0,
        "strongest_coin_tp1_fraction": 0.50,
        "strongest_coin_time_stop_sec": 600,
        "strongest_coin_cooldown_after_close_sec": 3600,
        "mexc_order_leverage": 10,
        "scan_market_source": "mexc_binance",
        "spot_confirmation_enabled": False,
        "auto_strategy_adaptation": False,
        "regime_adaptation": False,
        "liquidity_runner_enabled": False,
        "mirror_mode": "off",
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
    trigger_scan_now(context.application, reason="strongest_coin:on")
    await reply(update,
        "✅ Strongest coin ON\n"
        "Binance SPOT top-200 каждые 60s: weighted momentum + RS/BTC + pullback hold. Только LONG.\n"
        "Исполнение: MEXC futures. 1 сделка максимум, 10% баланса, x10 isolated, cooldown монеты 1h. TP1 1R 50%, TP2 2R остаток.",
        reply_markup=MAIN_MENU,
    )

async def cascade_hunter_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    global running, entries_enabled, trading_task, position_task
    s = await storage.all_settings()
    enabled = str(s.get("strategy_mode", "hybrid")).lower() == "cascade_hunter" and _bool_setting(s, "cascade_hunter_enabled", False)
    if enabled:
        await storage.set("cascade_hunter_enabled", False, bump_revision=False)
        await storage.set("settings_revision", int(s.get("settings_revision", 1) or 1) + 1, bump_revision=False)
        trigger_scan_now(context.application, reason="cascade_hunter:off")
        await reply(update, "○ Cascade hunter OFF\nСканер остановлен, новые сделки не открываются. Открытые позиции продолжают сопровождаться до TP/SL/time-stop.", reply_markup=MAIN_MENU)
        return

    updates = {
        "cascade_hunter_enabled": True,
        "orderflow_impulse_enabled": False,
        "knife_reversal_enabled": False,
        "multi_strategy_enabled": False,
        "strongest_coin_enabled": False,
        "quick_bounce_enabled": False,
        "impulse_dump_enabled": False,
        "strategy_mode": "cascade_hunter",
        "universe_mode": "top-100",
        "max_symbols": 100,
        "scan_interval_sec": 60,
        "symbol_refresh_sec": 300,
        "max_open_positions": 3,
        "trade_margin_pct": 0.10,
        "cascade_hunter_trade_margin_pct": 0.10,
        "mexc_order_leverage": 10,
        "cascade_hunter_leverage": 10,
        "cascade_hunter_tp_pct": 4.0,
        "cascade_hunter_sl_pct": 2.0,
        "cascade_hunter_tp1_r": 1.0,
        "cascade_hunter_tp2_r": 2.0,
        "cascade_hunter_tp1_fraction": 0.50,
        "cascade_hunter_time_stop_sec": 14400,
        "cascade_hunter_top_coins": 100,
        "cascade_hunter_max_open_positions": 3,
        "cascade_hunter_scan_interval_sec": 60,
        "cascade_hunter_min_liq_usd_1m": 30000.0,
        "cascade_hunter_min_pressure_ratio": 0.070,
        "cascade_hunter_min_volume_ratio": 2.2,
        "cascade_hunter_min_price_move_pct": 0.45,
        "cascade_hunter_max_spread_pct": 0.12,
        "cascade_hunter_min_24h_volume_usdt": 5000000.0,
        "scan_market_source": "mexc_binance",
        "spot_confirmation_enabled": False,
        "auto_strategy_adaptation": False,
        "regime_adaptation": False,
        "liquidity_runner_enabled": False,
        "mirror_mode": "off",
        "session_filter_enabled": False,
        "america_short_bias_enabled": False,
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
    trigger_scan_now(context.application, reason="cascade_hunter:on")
    await reply(update,
        "✅ Cascade hunter ON\n"
        "Binance SPOT top-100 каждые 60s: pressure + ускорение цены + volume spike + delta; исполнение MEXC futures.\n"
        "Выбирает 1–3 лучших. До 3 сделок, 10% баланса на сделку, x10 isolated. AI-check работает по общему тумблеру настроек.",
        reply_markup=MAIN_MENU,
    )

async def orderflow_impulse_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    global running, entries_enabled, trading_task, position_task
    s = await storage.all_settings()
    enabled = str(s.get("strategy_mode", "hybrid")).lower() == "orderflow_impulse" and _bool_setting(s, "orderflow_impulse_enabled", False)
    if enabled:
        await storage.set("orderflow_impulse_enabled", False, bump_revision=False)
        await storage.set("settings_revision", int(s.get("settings_revision", 1) or 1) + 1, bump_revision=False)
        trigger_scan_now(context.application, reason="orderflow_impulse:off")
        await reply(update, "○ Orderflow impulse OFF\nСканер остановлен, новые сделки не открываются. Открытые позиции продолжают сопровождаться до TP/SL/24h.", reply_markup=MAIN_MENU)
        return

    updates = {
        "orderflow_impulse_enabled": True,
        "quick_bounce_enabled": False,
        "impulse_dump_enabled": False,
        "strategy_mode": "orderflow_impulse",
        "universe_mode": "top-100",
        "max_symbols": 100,
        "scan_interval_sec": 60,
        "symbol_refresh_sec": 300,
        "max_open_positions": 3,
        "trade_margin_pct": 0.10,
        "orderflow_impulse_trade_margin_pct": 0.10,
        "mexc_order_leverage": 10,
        "orderflow_impulse_leverage": 10,
        "orderflow_impulse_tp_pct": 2.0,
        "orderflow_impulse_sl_pct": 3.0,
        "orderflow_impulse_time_stop_sec": 86400,
        "orderflow_impulse_top_coins": 100,
        "orderflow_impulse_max_open_positions": 3,
        "orderflow_impulse_min_volume_ratio": 1.5,
        "orderflow_impulse_scan_interval_sec": 60,
        "orderflow_impulse_min_trend_pct": 0.25,
        "orderflow_impulse_min_imbalance_abs": 0.08,
        "orderflow_impulse_max_spread_pct": 0.20,
        "orderflow_impulse_min_24h_volume_usdt": 5000000.0,
        "scan_market_source": "mexc_binance",
        "spot_confirmation_enabled": True,
        "auto_strategy_adaptation": False,
        "regime_adaptation": False,
        "liquidity_runner_enabled": False,
        "mirror_mode": "off",
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
    trigger_scan_now(context.application, reason="orderflow_impulse:on")
    await reply(
        update,
        "✅ Orderflow impulse ON\n"
        "Binance spot top-100, scan every 60 sec, trend + spot/CVD delta proxy + super volume + orderbook imbalance.\n"
        "До 3 сделок, 10% депозита на монету, x10 isolated. SL 1%, TP 2%, time-stop 4h.",
        reply_markup=MAIN_MENU,
    )


def _claude_api_key(settings: dict | None = None) -> str:
    return str(os.getenv("ANTHROPIC_API_KEY") or (settings or {}).get("anthropic_api_key") or "").strip()


def _boolish(value, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on", "y", "да"}


def _claude_settings_snapshot(settings: dict | None = None) -> dict:
    s = settings or {}
    model = normalize_claude_model(s.get("claude_autopilot_model") or os.getenv("CLAUDE_MODEL") or CLAUDE_SONNET_46)
    enabled = _boolish(s.get("claude_autopilot_enabled"), False)
    cycle_enabled = _boolish(s.get("claude_autopilot_cycle_enabled"), True)
    schedule = str(s.get("claude_autopilot_schedule") or "off").lower()
    chart_resolution = str(s.get("claude_chart_resolution") or os.getenv("CLAUDE_CHART_RESOLUTION") or "960x540").lower().replace("×", "x").replace(" ", "")
    if chart_resolution not in {"1280x720", "960x540"}:
        chart_resolution = "960x540"
    analysis_mode = str(s.get("claude_analysis_mode") or os.getenv("CLAUDE_ANALYSIS_MODE") or "optimal").strip().lower()
    if analysis_mode not in {"optimal", "full"}:
        analysis_mode = "optimal"
    api_key_present = bool(_claude_api_key(s))
    last_ts = float(s.get("claude_autopilot_last_run_ts") or 0)
    nxt = next_schedule_run(schedule, last_ts=last_ts) if cycle_enabled else None
    return {"enabled": enabled, "cycle_enabled": cycle_enabled, "model": model, "schedule": schedule, "chart_resolution": chart_resolution, "analysis_mode": analysis_mode, "api_key_present": api_key_present, "next_run": nxt}


def claude_autopilot_keyboard(settings: dict | None = None) -> InlineKeyboardMarkup:
    snap = _claude_settings_snapshot(settings or {})
    enabled = snap["enabled"]
    model = snap["model"]
    schedule = snap["schedule"]
    chart_resolution = snap.get("chart_resolution", "960x540")
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🟢 Включить" if not enabled else "✅ ВКЛ", callback_data="claude:enable"), InlineKeyboardButton("🔴 Выключить" if enabled else "○ ВЫКЛ", callback_data="claude:disable")],
        [InlineKeyboardButton(("✅ Скан по кругу: ВКЛ" if snap.get("cycle_enabled", True) else "○ Скан по кругу: ВЫКЛ"), callback_data="claude:cycle_toggle")],
        [InlineKeyboardButton(("✅ " if model == CLAUDE_SONNET_46 else "") + "🧠 Sonnet 4.6", callback_data="claude:model:sonnet"), InlineKeyboardButton(("✅ " if model == CLAUDE_OPUS_48 else "") + "🧠 Opus 4.8", callback_data="claude:model:opus")],
        [InlineKeyboardButton(("✅ " if snap.get("analysis_mode", "optimal") == "full" else "") + "🔎 Анализ full", callback_data="claude:analysis:full"), InlineKeyboardButton(("✅ " if snap.get("analysis_mode", "optimal") == "optimal" else "") + "⚡ Анализ optimal", callback_data="claude:analysis:optimal")],
        [InlineKeyboardButton(("✅ " if schedule == "4h" else "") + "⏱ 4H свеча +1м", callback_data="claude:schedule:4h")],
        [InlineKeyboardButton(("✅ " if schedule == "1h" else "") + "⏱ 1H свеча +1м", callback_data="claude:schedule:1h")],
        [InlineKeyboardButton(("✅ " if schedule == "2h" else "") + "⏱ Каждые 2 часа", callback_data="claude:schedule:2h")],
        [InlineKeyboardButton("⏱ Выключить расписание", callback_data="claude:schedule:off")],
        [InlineKeyboardButton(("✅ " if chart_resolution == "1280x720" else "") + "🖼 1280x720", callback_data="claude:resolution:1280"), InlineKeyboardButton(("✅ " if chart_resolution == "960x540" else "") + "🖼 960x540", callback_data="claude:resolution:960")],
        [InlineKeyboardButton("🔑 API Claude", callback_data="claude:api_help")],
        [InlineKeyboardButton("🚀 Запустить сейчас LIVE", callback_data="claude:run")],
        [InlineKeyboardButton("🚨 EXIT / CLOSE ALL", callback_data="claude:exit_confirm")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="noop:claude")],
    ])


def claude_autopilot_menu_text(settings: dict | None = None) -> str:
    snap = _claude_settings_snapshot(settings or {})
    nxt = snap.get("next_run")
    nxt_s = nxt.strftime("%H:%M МСК") if nxt else "-"
    return (
        "🤖 Claude Autopilot LIVE\n\n"
        f"Статус: {'ВКЛ' if snap['enabled'] else 'ВЫКЛ'}\n"
        f"Модель: {claude_model_label(snap['model'])}\n"
        f"Скан по кругу: {'ВКЛ' if snap.get('cycle_enabled', True) else 'ВЫКЛ'}\n"
        f"Расписание: {schedule_label(snap['schedule'])}\n"
        f"Графики: {snap.get('chart_resolution', '960x540')}\n"
        f"Анализ: {'full 15m/1H/4H' if snap.get('analysis_mode') == 'full' else 'optimal 1H/4H'}\n"
        f"Следующий запуск: {nxt_s}\n"
        f"API key: {'есть' if snap['api_key_present'] else 'НЕТ'}\n\n"
        "Основа та же, что ChatGPT Scan Mode: top-200 → top-15 → графики → setup v1.6 → текущий валидатор → LIVE сделки."
    )


async def claude_autopilot_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return
    s = await storage.all_settings()
    await reply(update, claude_autopilot_menu_text(s), reply_markup=claude_autopilot_keyboard(s))


def claude_api_help_text() -> str:
    return (
        "🔑 API Claude\n\n"
        "Чтобы Claude Autopilot мог сам отправлять scan-pack в Claude, сохрани Anthropic API key:\n"
        "/claude_api set sk-ant-...\n\n"
        "Проверить статус:\n"
        "/claude_api status\n\n"
        "Очистить ключ:\n"
        "/claude_api clear\n\n"
        "Ключ хранится в настройках бота как anthropic_api_key. ENV ANTHROPIC_API_KEY тоже поддерживается."
    )


async def claude_api_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return
    args = list(getattr(context, "args", []) or [])
    action = (args[0].lower() if args else "help")
    if action == "status":
        s = await storage.all_settings()
        saved = bool(str(s.get("anthropic_api_key") or "").strip())
        env = bool(str(os.getenv("ANTHROPIC_API_KEY") or "").strip())
        await reply(update, f"🔑 Claude API status\nSaved in bot: {'YES' if saved else 'NO'}\nENV fallback: {'YES' if env else 'NO'}", reply_markup=MAIN_MENU)
        return
    if action == "set":
        key = " ".join(args[1:]).strip()
        if not key or not key.startswith("sk-ant-"):
            await reply(update, "❌ Нужен Anthropic key вида sk-ant-...\nИспользуй: /claude_api set sk-ant-...", reply_markup=MAIN_MENU)
            return
        await storage.set("anthropic_api_key", key)
        os.environ["ANTHROPIC_API_KEY"] = key
        claude_log_event("claude_api_key_saved", key_prefix=key[:10], key_len=len(key), user_id=getattr(update.effective_user, "id", ""))
        await reply(update, "✅ Claude API key сохранён. Теперь Claude Autopilot может запускаться LIVE.", reply_markup=MAIN_MENU)
        return
    if action == "clear":
        await storage.set("anthropic_api_key", "")
        os.environ.pop("ANTHROPIC_API_KEY", None)
        claude_log_event("claude_api_key_cleared", user_id=getattr(update.effective_user, "id", ""))
        await reply(update, "🗑 Claude API key очищен из настроек бота.", reply_markup=MAIN_MENU)
        return
    await reply(update, claude_api_help_text(), reply_markup=MAIN_MENU)


def _claude_raw_dir() -> str:
    path = os.getenv("CLAUDE_RAW_DIR", os.getenv("CHATGPT_LOG_DIR", "/tmp"))
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        path = "/tmp"
    return path

def _save_claude_raw_response(raw_text: str, *, stamp: str, kind: str = "response") -> str:
    safe_kind = re.sub(r"[^a-zA-Z0-9_\-]", "_", str(kind or "response"))
    path = os.path.join(_claude_raw_dir(), f"claude_{safe_kind}-{stamp}.txt")
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(str(raw_text or ""))
            if not str(raw_text or "").endswith("\n"):
                f.write("\n")
    except Exception:
        path = ""
    return path

def _claude_progress_bar(pct: int) -> str:
    try:
        pct = max(0, min(100, int(pct)))
    except Exception:
        pct = 0
    filled = int(round(pct / 10))
    filled = max(0, min(10, filled))
    return "[" + ("█" * filled) + ("░" * (10 - filled)) + f"] {pct}%"


async def _claude_prepare_progress_message(app, chat_id: int, message=None):
    """Keep one Claude progress card per run; delete the previous run card.

    This is best-effort only: Telegram errors must never block scan-pack,
    Claude API, or LIVE execution.
    """
    msg_id = getattr(message, "message_id", None) if message is not None else None
    old_id = app.bot_data.get("claude_progress_message_id")
    if not old_id:
        try:
            old_id = await storage.get("claude_progress_message_id", None)
        except Exception:
            old_id = None
    if old_id and (not msg_id or int(old_id) != int(msg_id)):
        try:
            await _safe_delete_bot_message(app, chat_id, int(old_id))
        except Exception:
            pass
    if message is None:
        message = await _safe_send_bot_message(app, chat_id, "🤖 Claude Autopilot LIVE\n\n" + _claude_progress_bar(0) + "\nЭтап: старт", reply_markup=MAIN_MENU)
        msg_id = getattr(message, "message_id", None) if message is not None else None
    if msg_id:
        app.bot_data["claude_progress_message_id"] = int(msg_id)
        try:
            await storage.set("claude_progress_message_id", int(msg_id), bump_revision=False)
        except Exception:
            pass
    try:
        # Make sure scheduled/manual runs immediately show the actual progress
        # card, not the old plain "schedule triggered" text.
        await _claude_progress_render(app, message, [], pct=0, stage="старт Claude Autopilot")
    except Exception:
        pass
    return message


async def _claude_status(app, message, lines: list[str], line: str, *, pct: int | None = None, stage: str | None = None):
    """Best-effort Claude progress update; never breaks scan/Claude/executor."""
    lines.append(line)
    try:
        return await _claude_progress_render(app, message, lines, pct=pct, stage=stage or line)
    except Exception as e:
        try:
            claude_log_event("claude_progress_update_error", ok=False, pct=pct, stage=str(stage or line)[:300], error=repr(e)[:500])
        except Exception:
            pass
        return message


async def _claude_progress_render(app, message, lines: list[str], *, pct: int | None = None, stage: str | None = None, note: str | None = None):
    """Render Claude progress by replacing the card, not editing it.

    Telegram edit updates were observed to leave the card stuck at 0% on some
    scheduled runs. Replacing the previous progress card is more reliable and is
    still low-volume because this runs only at real milestones. All failures are
    logged and ignored so progress can never stop scanning/trading.
    """
    header = "🤖 Claude Autopilot LIVE"
    parts = [header, ""]
    if pct is not None:
        parts.append(_claude_progress_bar(pct))
        parts.append(f"Этап: {stage or '-'}")
        if note:
            parts.append(str(note)[:400])
        parts.append("")
    # Keep the card compact. Older details remain in /log_claude.
    parts.extend(lines[-10:])
    text = "\n".join(parts)[:3900]

    chat_id = None
    if message is not None:
        chat_id = getattr(message, "chat_id", None)
        if chat_id is None:
            chat = getattr(message, "chat", None)
            chat_id = getattr(chat, "id", None)
    if chat_id is None:
        try:
            chat_id = int(await storage.get("admin_chat_id", 0) or 0)
        except Exception:
            chat_id = None

    old_id = None
    try:
        old_id = app.bot_data.get("claude_progress_message_id")
    except Exception:
        old_id = None
    if not old_id and message is not None:
        old_id = getattr(message, "message_id", None)

    new_message = None
    delete_ok = None
    send_ok = False
    if chat_id:
        if old_id:
            try:
                await _safe_delete_bot_message(app, int(chat_id), int(old_id))
                delete_ok = True
            except Exception:
                delete_ok = False
        try:
            new_message = await _safe_send_bot_message(app, int(chat_id), text, reply_markup=MAIN_MENU)
            new_id = getattr(new_message, "message_id", None) if new_message is not None else None
            if new_id:
                send_ok = True
                app.bot_data["claude_progress_message_id"] = int(new_id)
                try:
                    await storage.set("claude_progress_message_id", int(new_id), bump_revision=False)
                except Exception:
                    pass
                claude_log_event("claude_progress_update", ok=True, method="replace", pct=pct, stage=str(stage or "")[:300], old_message_id=old_id, new_message_id=int(new_id), delete_ok=delete_ok)
                return new_message
        except Exception as e:
            claude_log_event("claude_progress_update_error", ok=False, method="replace", pct=pct, stage=str(stage or "")[:300], old_message_id=old_id, delete_ok=delete_ok, error=repr(e)[:500])

    # Last-resort fallback: try edit if replace could not send.
    if message is not None:
        try:
            edited = await _safe_edit_message_text(message, text)
            claude_log_event("claude_progress_update", ok=bool(edited), method="edit_fallback", pct=pct, stage=str(stage or "")[:300], old_message_id=old_id, send_ok=send_ok)
        except Exception as e:
            claude_log_event("claude_progress_update_error", ok=False, method="edit_fallback", pct=pct, stage=str(stage or "")[:300], error=repr(e)[:500])
    return message


async def _claude_progress_ticker(app, message, lines: list[str], stop_event: asyncio.Event, *, start_pct: int = 5, end_pct: int = 72, stage: str = "формирую scan-pack", interval_sec: float = 30.0, estimate_sec: float = 210.0):
    """Very light Telegram progress ticker for long scan-pack builds.

    It edits one message at most once per interval and is best-effort only, so it
    must never slow down scanning, chart rendering, zip creation, or Claude API.
    """
    started = time.time()
    last_pct = -1
    while not stop_event.is_set():
        try:
            elapsed = max(0.0, time.time() - started)
            ratio = min(1.0, elapsed / max(30.0, float(estimate_sec or 210.0)))
            pct = int(start_pct + (end_pct - start_pct) * ratio)
            pct = max(start_pct, min(end_pct, pct))
            # Avoid editing with the same percent repeatedly.
            if pct != last_pct:
                last_pct = pct
                await _claude_progress_render(app, message, lines, pct=pct, stage=stage, note=f"идёт работа, прошло ~{int(elapsed)} сек")
        except Exception:
            pass
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=max(10.0, float(interval_sec or 30.0)))
        except asyncio.TimeoutError:
            continue


async def _claude_run_cycle(app, chat_id: int, *, trigger: str = "manual", status_message=None):
    """Run one Claude Autopilot cycle using the existing ChatGPT Scan Mode engine."""
    global running, entries_enabled, position_task
    if app.bot_data.get("claude_autopilot_running"):
        if status_message is not None:
            await _safe_edit_message_text(status_message, "⏳ Claude Autopilot уже выполняется. Новый запуск пропущен.")
        else:
            await _safe_send_bot_message(app, chat_id, "⏳ Claude Autopilot уже выполняется. Новый запуск пропущен.", reply_markup=MAIN_MENU)
        return
    app.bot_data["claude_autopilot_running"] = True
    lines: list[str] = []
    pack_path = setup_path = None
    claude_run_id = f"claude_{int(time.time())}_{trigger}"
    os.environ["CLAUDE_AUTOPILOT_LOG_ACTIVE"] = claude_run_id
    claude_log_event("claude_autopilot_run_start", run_id=claude_run_id, trigger=trigger, chat_id=chat_id, bot_version=VERSION)
    try:
        status_message = await _claude_prepare_progress_message(app, chat_id, status_message)
        settings = await storage.all_settings()
        if not _boolish(settings.get("claude_autopilot_enabled"), False) and trigger != "manual_force":
            await _claude_status(app, status_message, lines, "⏸ Автопилот выключен. Запуск пропущен.")
            return
        saved_api = bool(str(settings.get("anthropic_api_key") or "").strip())
        env_api = bool(str(os.getenv("ANTHROPIC_API_KEY") or "").strip())
        api_key = _claude_api_key(settings)
        claude_log_event("claude_api_key_status", saved_in_bot=saved_api, env_fallback=env_api, usable=bool(api_key))
        if not api_key:
            claude_log_event("claude_autopilot_stop_no_api_key", run_id=claude_run_id)
            await _claude_status(app, status_message, lines, "❌ ANTHROPIC_API_KEY не найден. Сделки не открывались.")
            return
        model = normalize_claude_model(settings.get("claude_autopilot_model") or os.getenv("CLAUDE_MODEL") or CLAUDE_SONNET_46)
        max_tokens = int(os.getenv("CLAUDE_MAX_TOKENS", str(settings.get("claude_max_tokens", 6000) or 6000)) or 6000)
        temperature = float(os.getenv("CLAUDE_TEMPERATURE", str(settings.get("claude_temperature", 0.2) or 0.2)))
        chart_resolution = str(settings.get("claude_chart_resolution") or os.getenv("CLAUDE_CHART_RESOLUTION") or "960x540").lower().replace("×", "x").replace(" ", "")
        if chart_resolution not in {"1280x720", "960x540"}:
            chart_resolution = "960x540"
        analysis_mode = str(settings.get("claude_analysis_mode") or os.getenv("CLAUDE_ANALYSIS_MODE") or "optimal").strip().lower()
        if analysis_mode not in {"optimal", "full"}:
            analysis_mode = "optimal"
        claude_pack_timeframes = ["4h", "1h", "15m"] if analysis_mode == "full" else ["4h", "1h"]
        claude_log_event(
            "claude_autopilot_settings",
            run_id=claude_run_id,
            enabled=settings.get("claude_autopilot_enabled"),
            schedule=settings.get("claude_autopilot_schedule"),
            model=model,
            model_label=claude_model_label(model),
            max_tokens=max_tokens,
            temperature=(temperature if model != CLAUDE_OPUS_48 else "default_for_opus"),
            chart_resolution=chart_resolution,
            analysis_mode=analysis_mode,
            pack_timeframes=",".join(claude_pack_timeframes),
            log_timeframes="4h,1h,15m",
            scan_limit=os.getenv("CHATGPT_SCAN_LIMIT", "200"),
        )

        await storage.set("claude_autopilot_last_run_ts", time.time(), bump_revision=False)
        await disable_other_modes(storage)
        entries_enabled = False
        running = True
        if position_task is None or position_task.done():
            position_task = app.create_task(position_management_loop(app))

        await _claude_status(app, status_message, lines, f"⏳ Шаг 1/7: запустил scan top-200...\nПричина: {trigger}\nМодель: {claude_model_label(model)}\nГрафики: {chart_resolution}\nАнализ: {analysis_mode} ({','.join(claude_pack_timeframes)})", pct=10, stage="старт скана")
        apply_mexc_runtime_env({**settings, "mexc_order_leverage": "10", "mexc_order_open_type": "1"})
        ex = await get_exchange(settings)
        ws = await get_ws(settings)
        scan_limit = int(os.getenv("CHATGPT_SCAN_LIMIT", "200") or 200)
        async def _pack_progress(pct: int, stage: str):
            await _claude_status(app, status_message, lines, f"📦 {stage}", pct=pct, stage=stage)

        pack_path = await build_chatgpt_scan_pack(
            ex,
            scanner,
            settings,
            ws_supervisor=ws,
            limit=scan_limit,
            storage=storage,
            chart_resolution=chart_resolution,
            pack_timeframes_override=claude_pack_timeframes,
            pack_label=f"claude_{analysis_mode}",
            progress_cb=_pack_progress,
        )
        try:
            pack_size = os.path.getsize(pack_path)
            pack_names = []
            with zipfile.ZipFile(pack_path, "r") as zf:
                pack_names = zf.namelist()
            image_names = [n for n in pack_names if str(n).lower().endswith((".png", ".jpg", ".jpeg", ".webp"))]
            claude_log_event("claude_scan_pack_built", run_id=claude_run_id, pack_path=pack_path, pack_size_bytes=pack_size, file_count=len(pack_names), image_count=len(image_names), file_names=pack_names, image_names=image_names)
        except Exception as e:
            claude_log_event("claude_scan_pack_log_error", run_id=claude_run_id, pack_path=pack_path, error=repr(e))
        await _claude_status(app, status_message, lines, f"✅ Шаг 1/7: scan готов\n✅ Шаг 2/7: архив собран: {os.path.basename(pack_path)}", pct=85, stage="архив scan-pack готов")
        try:
            with open(pack_path, "rb") as f:
                await app.bot.send_document(
                    chat_id=chat_id,
                    document=f,
                    filename=os.path.basename(pack_path),
                    caption=f"📦 Claude Autopilot scan pack: {os.path.basename(pack_path)}",
                    connect_timeout=float(os.getenv("TELEGRAM_DOCUMENT_CONNECT_TIMEOUT_SEC", "30") or 30),
                    read_timeout=float(os.getenv("TELEGRAM_DOCUMENT_READ_TIMEOUT_SEC", "180") or 180),
                    write_timeout=float(os.getenv("TELEGRAM_DOCUMENT_WRITE_TIMEOUT_SEC", "180") or 180),
                    pool_timeout=float(os.getenv("TELEGRAM_DOCUMENT_POOL_TIMEOUT_SEC", "30") or 30),
                )
            claude_log_event("claude_scan_pack_sent_to_telegram", run_id=claude_run_id, pack_path=pack_path, chat_id=chat_id)
            await _claude_status(app, status_message, lines, "📤 scan-pack отправлен в Telegram", pct=90, stage="архив отправлен")
        except Exception as e:
            chatgpt_log_event("claude_pack_send_failed", error=repr(e))
            claude_log_event("claude_scan_pack_send_to_telegram_failed", run_id=claude_run_id, pack_path=pack_path, error=repr(e))

        async def _progress(msg: str):
            await _claude_status(app, status_message, lines, f"📤 Claude: {msg}", pct=95, stage="жду ответ Claude API")

        await _claude_status(app, status_message, lines, f"⏳ Шаг 3/7: отправляю данные в {claude_model_label(model)}...", pct=95, stage="жду ответ Claude API")
        chatgpt_log_event(
            "claude_api_request_start",
            trigger=trigger,
            pack=pack_path,
            model=model,
            model_label=claude_model_label(model),
            max_tokens=max_tokens,
            temperature=temperature if model != CLAUDE_OPUS_48 else "default_for_opus",
            chart_resolution=chart_resolution,
            scan_limit=scan_limit,
        )
        claude_log_event(
            "claude_api_request_start",
            run_id=claude_run_id,
            trigger=trigger,
            pack_path=pack_path,
            model=model,
            model_label=claude_model_label(model),
            max_tokens=max_tokens,
            temperature=temperature if model != CLAUDE_OPUS_48 else "default_for_opus",
            chart_resolution=chart_resolution,
            analysis_mode=analysis_mode,
            pack_timeframes=",".join(claude_pack_timeframes),
            scan_limit=scan_limit,
        )
        claude_api_started = time.time()
        raw_setup, meta = await call_claude_for_setup(pack_path, api_key=api_key, model=model, max_tokens=max_tokens, temperature=temperature, progress=_progress)
        stamp = datetime.now(MSK).strftime("%H%M_%d%m")
        raw_path = _save_claude_raw_response(raw_setup, stamp=stamp, kind="response")
        claude_log_event(
            "claude_api_response_received",
            run_id=claude_run_id,
            model=model,
            response_id=(meta or {}).get("response_id"),
            http_status=(meta or {}).get("http_status"),
            elapsed_total_sec=round(time.time() - claude_api_started, 3),
            usage=(meta or {}).get("usage"),
            cost_estimate=(meta or {}).get("cost_estimate"),
            image_count=(meta or {}).get("image_count"),
            image_timeframes=(meta or {}).get("image_timeframes"),
            image_total_kb=(meta or {}).get("image_total_kb"),
            payload_mb=(meta or {}).get("payload_mb"),
            selected_symbols=(meta or {}).get("selected_symbols"),
            raw_len=len(raw_setup or ""),
            raw_path=raw_path,
            stop_reason=(meta or {}).get("stop_reason"),
        )
        await _claude_status(app, status_message, lines, "📥 Шаг 4/7: setup получен от Claude", pct=100, stage="setup получен от ИИ")
        chatgpt_log_event(
            "claude_api_response_received",
            model=model,
            response_id=(meta or {}).get("response_id"),
            usage=(meta or {}).get("usage"),
            cost_estimate=(meta or {}).get("cost_estimate"),
            payload_mb=(meta or {}).get("payload_mb"),
            image_count=(meta or {}).get("image_count"),
            image_names=(meta or {}).get("image_names"),
            selected_symbols=(meta or {}).get("selected_symbols"),
            raw_len=len(raw_setup or ""),
            raw_path=raw_path,
            raw_response=raw_setup,
            http_status=(meta or {}).get("http_status"),
            response_body_preview=(meta or {}).get("response_body_preview"),
        )

        await _claude_status(app, status_message, lines, "⏳ Шаг 5/7: проверяю setup v1.6 валидатором...", pct=100, stage="setup получен от ИИ")
        claude_log_event("claude_setup_parse_start", run_id=claude_run_id, raw_len=len(raw_setup or ""), raw_path=raw_path)
        setup = extract_setup_json(raw_setup)
        trades = setup.get("trades") if isinstance(setup, dict) else []
        trade_symbols = [str(t.get("symbol") or "") for t in trades if isinstance(t, dict)] if isinstance(trades, list) else []
        chatgpt_log_event("claude_setup_json_extracted", setup_version=str(setup.get("setup_version") or ""), trades=setup.get("trades"), verdict=setup.get("verdict"), raw_path=raw_path)
        claude_log_event(
            "claude_setup_json_extracted",
            run_id=claude_run_id,
            setup_version=str(setup.get("setup_version") or ""),
            verdict=setup.get("verdict"),
            trade_count=len(trade_symbols),
            trade_symbols=trade_symbols,
            raw_path=raw_path,
        )
        setup_version = str(setup.get("setup_version") or "").strip()
        if setup_version != CHATGPT_SETUP_VERSION:
            claude_log_event("claude_setup_validation_failed", run_id=claude_run_id, setup_version=setup_version or "MISSING", expected=CHATGPT_SETUP_VERSION, trade_symbols=trade_symbols)
            raise ValueError(f"Claude setup_version={setup_version or 'MISSING'}, нужна {CHATGPT_SETUP_VERSION}")
        claude_log_event("claude_setup_validation_ok", run_id=claude_run_id, setup_version=setup_version, verdict=setup.get("verdict"), trade_count=len(trade_symbols), trade_symbols=trade_symbols)

        # Save and send a clean bot-ready setup file, even if Claude wrapped JSON
        # in text/code fences. execute_setup uses the same parsed object below.
        clean_setup_text = json.dumps(setup, ensure_ascii=False, indent=2)
        setup_path = save_claude_setup_text(clean_setup_text, stamp=stamp)
        claude_log_event("claude_setup_saved", run_id=claude_run_id, setup_path=setup_path, setup_bytes=len(clean_setup_text.encode("utf-8")), trade_symbols=trade_symbols)
        await _claude_status(app, status_message, lines, f"✅ Шаг 5/7: setup валиден и сохранён: {os.path.basename(setup_path)}", pct=100, stage="setup получен от ИИ")
        try:
            with open(setup_path, "rb") as f:
                await app.bot.send_document(
                    chat_id=chat_id,
                    document=f,
                    filename=os.path.basename(setup_path),
                    caption=f"📄 Setup от Claude: {os.path.basename(setup_path)}",
                    connect_timeout=float(os.getenv("TELEGRAM_DOCUMENT_CONNECT_TIMEOUT_SEC", "30") or 30),
                    read_timeout=float(os.getenv("TELEGRAM_DOCUMENT_READ_TIMEOUT_SEC", "180") or 180),
                    write_timeout=float(os.getenv("TELEGRAM_DOCUMENT_WRITE_TIMEOUT_SEC", "180") or 180),
                    pool_timeout=float(os.getenv("TELEGRAM_DOCUMENT_POOL_TIMEOUT_SEC", "30") or 30),
                )
            claude_log_event("claude_setup_sent_to_telegram", run_id=claude_run_id, setup_path=setup_path, chat_id=chat_id)
        except Exception as e:
            chatgpt_log_event("claude_setup_send_failed", error=repr(e))
            claude_log_event("claude_setup_send_to_telegram_failed", run_id=claude_run_id, setup_path=setup_path, error=repr(e))

        await _claude_status(app, status_message, lines, "⏳ Шаг 6/7: открываю сделки LIVE текущим ChatGPT executor...", pct=100, stage="setup получен от ИИ")
        try:
            await notify_admin_bottom_replace(app, _format_setup_lifecycle_text(setup, phase="placing"), key="chatgpt_setup_lifecycle", min_interval_sec=0)
            claude_log_event("claude_setup_lifecycle_message_sent", run_id=claude_run_id, phase="placing", trade_symbols=trade_symbols)
        except Exception as e:
            claude_log_event("claude_setup_lifecycle_message_error", run_id=claude_run_id, phase="placing", error=repr(e))
        claude_log_event("claude_execute_setup_start", run_id=claude_run_id, setup_path=setup_path, trades=setup.get("trades"), verdict=setup.get("verdict"), trade_symbols=trade_symbols)
        execute_started = time.time()
        result = await execute_setup(storage, ex, setup)
        opened_rows = result.get("opened") if isinstance(result.get("opened"), list) else []
        placed = [x for x in opened_rows if isinstance(x, dict) and bool(x.get("ok"))]
        skipped = [x for x in opened_rows if isinstance(x, dict) and not bool(x.get("ok"))]
        placed_symbols = [str(x.get("symbol") or "") for x in placed if isinstance(x, dict)]
        skipped_symbols = [str(x.get("symbol") or "") for x in skipped if isinstance(x, dict)]
        claude_log_event(
            "claude_execute_setup_done",
            run_id=claude_run_id,
            elapsed_sec=round(time.time() - execute_started, 3),
            placed_count=len(placed),
            skipped_count=len(skipped),
            placed_symbols=placed_symbols,
            skipped_symbols=skipped_symbols,
            result=result,
        )
        result["setup_installed_at"] = datetime.now(MSK).strftime("%H:%M МСК")
        result["_monitor_persist"] = True
        result["source"] = "claude_autopilot"
        try:
            await notify_admin_bottom_replace(app, _format_setup_lifecycle_text(setup, result, phase="done"), key="chatgpt_setup_lifecycle", min_interval_sec=0)
            claude_log_event("claude_setup_lifecycle_message_sent", run_id=claude_run_id, phase="done", placed_symbols=placed_symbols, skipped_symbols=skipped_symbols)
        except Exception as e:
            claude_log_event("claude_setup_lifecycle_message_error", run_id=claude_run_id, phase="done", error=repr(e))
        # Claude Autopilot already executed the setup; do not leave the monitor in
        # manual "waiting for setup file" mode, otherwise setup time is shown as '-'.
        await storage.set("chatgpt_waiting_setup", False)
        await storage.set("chatgpt_last_setup_result", result)
        try:
            await update_chatgpt_monitor_message(app, ex=ex, setup_result=result)
            claude_log_event("claude_monitor_after_setup_updated", run_id=claude_run_id, setup_installed_at=result.get("setup_installed_at"), placed_symbols=placed_symbols)
        except Exception as e:
            chatgpt_log_event("claude_monitor_after_setup_error", error=repr(e))
            claude_log_event("claude_monitor_after_setup_error", run_id=claude_run_id, error=repr(e))
        nxt = next_schedule_run(str((await storage.all_settings()).get("claude_autopilot_schedule") or "off"), last_ts=float(time.time()))
        nxt_s = nxt.strftime("%H:%M МСК") if nxt else "-"
        summary = [
            "✅ Шаг 7/7: Claude Autopilot LIVE завершён",
            f"📦 Архив: {os.path.basename(pack_path)}",
            f"📄 Setup: {os.path.basename(setup_path)}",
            f"🧠 Claude: {claude_model_label(model)}",
            f"✅ поставлено/открыто: {len(placed)}",
            f"❌ пропущено: {len(skipped)}",
            f"Следующий запуск: {nxt_s}",
        ]
        for r in placed[:5]:
            summary.append(f"✅ {r.get('symbol')} {r.get('side')} {r.get('order_type')} — {r.get('entry')}")
        for r in skipped[:5]:
            summary.append(f"❌ {r.get('symbol')} {r.get('side')} — {str(r.get('reason') or r.get('error') or '')[:120]}")
        final_text = "🤖 Claude Autopilot LIVE\n\n" + _claude_progress_bar(100) + "\nЭтап: setup получен от ИИ / исполнение завершено\n\n" + "\n".join(summary)
        await _safe_edit_message_text(status_message, final_text) if status_message is not None else await _safe_send_bot_message(app, chat_id, final_text, reply_markup=MAIN_MENU)
        chatgpt_log_event("claude_autopilot_done", pack=pack_path, setup=setup_path, model=model, meta=meta, result=result)
        claude_log_event("claude_autopilot_run_done", run_id=claude_run_id, pack=pack_path, setup=setup_path, model=model, result=result)
    except Exception as e:
        chatgpt_log_event("claude_autopilot_failed", error=repr(e), pack=pack_path, setup=setup_path)
        claude_log_event("claude_autopilot_run_failed", run_id=claude_run_id, error=repr(e), pack=pack_path, setup=setup_path, last_status_lines=lines[-30:])
        log.exception("Claude Autopilot failed")
        txt = "❌ Claude Autopilot остановлен\n\n" + "\n".join(lines[-20:]) + f"\n\nОшибка: {str(e)[:1200]}\nСделки НЕ открывались, если ошибка была до execute_setup."
        if status_message is not None:
            await _safe_edit_message_text(status_message, txt)
        else:
            await _safe_send_bot_message(app, chat_id, txt, reply_markup=MAIN_MENU)
    finally:
        app.bot_data["claude_autopilot_running"] = False
        claude_log_event("claude_autopilot_run_finally", run_id=claude_run_id, mirror_active=os.getenv("CLAUDE_AUTOPILOT_LOG_ACTIVE", ""))


async def _claude_emergency_close_all(app, chat_id: int, status_message=None):
    """Global emergency kill switch: stop Claude schedule, cancel orders, close positions."""
    global entries_enabled
    if app.bot_data.get("claude_exit_running"):
        return
    app.bot_data["claude_exit_running"] = True
    lines = []
    try:
        entries_enabled = False
        await storage.set("claude_autopilot_enabled", False, bump_revision=False)
        await storage.set("claude_autopilot_schedule", "off", bump_revision=False)
        await storage.set("chatgpt_waiting_setup", False, bump_revision=False)
        await storage.set("chatgpt_setup_mode", False, bump_revision=False)
        await storage.set("boost_autopilot_active", False, bump_revision=False)
        await storage.set("btc_ai_autopilot_enabled", False, bump_revision=False)
        _boost_disarm_runtime(app)
        lines.append("✅ Автопилот выключен, расписание остановлено")
        if status_message is not None:
            await _safe_edit_message_text(status_message, "🚨 EXIT / CLOSE ALL\n\n" + "\n".join(lines) + "\n⏳ Отменяю лимитки...", )
        s = await storage.all_settings()
        ex = await _get_exchange_emergency(s, 45)
        cancel_fail = []
        for sym in ["BTC_USDT", "BTC/USDT:USDT", None]:
            try:
                await _await_with_timeout(ex.cancel_all_orders(sym), 35, f"cancel_all_orders {sym or '*'}")
            except Exception as e:
                cancel_fail.append(f"{sym or '*'}: {e}")
        lines.append("✅ Команды отмены лимиток отправлены" if not cancel_fail else f"⚠️ Ошибки отмены: {cancel_fail[:3]}")
        if status_message is not None:
            await _safe_edit_message_text(status_message, "🚨 EXIT / CLOSE ALL\n\n" + "\n".join(lines) + "\n⏳ Закрываю позиции market...", )
        close_res = None
        if hasattr(ex, "mexc_hard_close_all_positions"):
            close_res = await _await_with_timeout(ex.mexc_hard_close_all_positions(None, retries=5), 90, "mexc_hard_close_all_positions")
        else:
            exec_engine = ExecutionEngine(storage, ex)
            positions = [p for p in (await _await_with_timeout(ex.fetch_positions(), 20, "fetch_positions") or []) if exec_engine.exchange_position_qty(p) > 0]
            close_res = []
            for p in positions:
                close_res.append(await _await_with_timeout(exec_engine.close_exchange_position(p, "claude_exit_close_all"), 35, "close_exchange_position"))
        lines.append(f"✅ Закрытие позиций выполнено: {str(close_res)[:500]}")
        try:
            await storage.clear_positions()
            lines.append("✅ Local position cache очищен")
        except Exception as e:
            lines.append(f"⚠️ cache clear error: {e}")
        txt = "🚨 EXIT / CLOSE ALL завершён\n\n" + "\n".join(lines) + "\n\nНовые автозапуски остановлены."
        if status_message is not None:
            await _safe_edit_message_text(status_message, txt)
        else:
            await _safe_send_bot_message(app, chat_id, txt, reply_markup=MAIN_MENU)
    except Exception as e:
        log.exception("Claude EXIT/CLOSE ALL failed")
        txt = "🚨 EXIT / CLOSE ALL ошибка\n\n" + "\n".join(lines) + f"\n\nОшибка: {str(e)[:1200]}"
        if status_message is not None:
            await _safe_edit_message_text(status_message, txt)
        else:
            await _safe_send_bot_message(app, chat_id, txt, reply_markup=MAIN_MENU)
    finally:
        app.bot_data["claude_exit_running"] = False


async def _claude_scheduler_loop(app):
    await asyncio.sleep(5)
    while True:
        try:
            s = await storage.all_settings()
            if _boolish(s.get("claude_autopilot_enabled"), False) and _boolish(s.get("claude_autopilot_cycle_enabled"), True):
                sched = str(s.get("claude_autopilot_schedule") or "off").lower()
                last_ts = float(s.get("claude_autopilot_last_run_ts") or 0)
                if schedule_due(sched, last_ts=last_ts):
                    if app.bot_data.get("claude_autopilot_running"):
                        chatgpt_log_event("claude_schedule_skipped_already_running", schedule=sched)
                        await storage.set("claude_autopilot_last_run_ts", time.time(), bump_revision=False)
                    else:
                        chat_id = first_admin_id()
                        if chat_id:
                            app.create_task(_claude_run_cycle(app, int(chat_id), trigger=f"schedule:{sched}", status_message=None))
            await asyncio.sleep(20)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("claude scheduler loop error: %s", e)
            await asyncio.sleep(30)



async def _send_chatgpt_pack_document(app, chat_id: int, pack_path: str, caption: str, attempts: int = 3) -> bool:
    """Send ZIP scan pack with retries and detailed runtime logging."""
    filename = os.path.basename(pack_path or "chatgpt_scan_pack.zip")
    for attempt in range(1, max(1, attempts) + 1):
        try:
            if not pack_path or not os.path.exists(pack_path):
                raise FileNotFoundError(f"pack not found: {pack_path}")
            size_kb = round(os.path.getsize(pack_path) / 1024, 1)
            chatgpt_log_event("mode_pack_send_attempt", attempt=attempt, filename=filename, size_kb=size_kb)
            with open(pack_path, "rb") as f:
                await app.bot.send_document(
                    chat_id=chat_id,
                    document=f,
                    filename=filename,
                    caption=caption,
                    connect_timeout=float(os.getenv("TELEGRAM_DOCUMENT_CONNECT_TIMEOUT_SEC", "45") or 45),
                    read_timeout=float(os.getenv("TELEGRAM_DOCUMENT_READ_TIMEOUT_SEC", "300") or 300),
                    write_timeout=float(os.getenv("TELEGRAM_DOCUMENT_WRITE_TIMEOUT_SEC", "300") or 300),
                    pool_timeout=float(os.getenv("TELEGRAM_DOCUMENT_POOL_TIMEOUT_SEC", "45") or 45),
                )
            chatgpt_log_event("mode_pack_send_ok", attempt=attempt, filename=filename)
            return True
        except Exception as e:
            chatgpt_log_event("mode_pack_send_failed", attempt=attempt, filename=filename, error=repr(e))
            if attempt < max(1, attempts):
                await asyncio.sleep(2 * attempt)
    try:
        tail = tail_chatgpt_runtime_log(80)[:2500]
        await app.bot.send_message(
            chat_id=chat_id,
            text=(
                "❌ ZIP pack собран, но Telegram не смог отправить архив после повторов.\n"
                f"Файл на сервере: {pack_path}\n"
                "Скинь мне /log_chatgpt для диагностики или перезапусти ChatGPT Scan Mode.\n\n"
                f"Последний runtime log:\n{tail}"
            )[:3900],
            reply_markup=MAIN_MENU,
        )
    except Exception:
        pass
    return False


async def _edit_or_send_scan_status(app, chat_id: int, text: str, message_id: int | None = None) -> int | None:
    """Update one ChatGPT Scan status message.

    v0404: scan status cards are sent without reply-keyboard markup because
    Telegram can reject editing some reply-keyboard messages with
    "Message can't be edited".  If edit is rejected, delete the stale card
    before sending the replacement so the chat does not accumulate duplicates.
    """
    payload = str(text)[:3900]
    if message_id:
        try:
            await asyncio.wait_for(app.bot.edit_message_text(chat_id=chat_id, message_id=int(message_id), text=payload), timeout=6)
            return int(message_id)
        except Exception as e:
            if "message is not modified" in str(e).lower():
                return int(message_id)
            chatgpt_log_event("mode_scan_status_edit_failed", error=repr(e))
            try:
                await asyncio.wait_for(app.bot.delete_message(chat_id=chat_id, message_id=int(message_id)), timeout=4)
                chatgpt_log_event("mode_scan_status_old_deleted_after_edit_fail", message_id=message_id)
            except Exception as de:
                chatgpt_log_event("mode_scan_status_old_delete_failed", error=repr(de))
    msg = await app.bot.send_message(chat_id=chat_id, text=payload)
    return getattr(msg, "message_id", None) if msg is not None else message_id


async def _delete_scan_status(app, chat_id: int, message_id: int | None) -> None:
    if not message_id:
        return
    try:
        await asyncio.wait_for(app.bot.delete_message(chat_id=chat_id, message_id=int(message_id)), timeout=4)
    except Exception as e:
        chatgpt_log_event("mode_scan_status_delete_skipped", error=repr(e))


async def _delete_known_bottom_message(app, chat_id: int, key: str) -> None:
    """Best-effort cleanup for the latest persisted live card before scan mode.

    Telegram does not let the bot discover old message ids, so this can remove
    only the last id that newer builds stored. The v0404 lock prevents new
    duplicates going forward.
    """
    msg_key = f"bottom_msg_id_{key}"
    msg_id = app.bot_data.get(msg_key)
    if not msg_id:
        try:
            msg_id = await storage.get(msg_key, None)
        except Exception:
            msg_id = None
    if msg_id:
        try:
            await asyncio.wait_for(app.bot.delete_message(chat_id=chat_id, message_id=int(msg_id)), timeout=4)
        except Exception as e:
            chatgpt_log_event("bottom_message_delete_skipped", key=key, error=repr(e))
    app.bot_data[msg_key] = None
    try:
        await storage.set(msg_key, None, bump_revision=False)
    except Exception:
        pass


async def _delete_user_command_message(update: Update) -> None:
    """Best-effort cleanup of the user's button/command bubble."""
    try:
        msg = getattr(update, "effective_message", None)
        if msg is not None:
            await asyncio.wait_for(msg.delete(), timeout=3)
    except Exception:
        pass

async def _chatgpt_scan_background_job(app, chat_id: int, status_message_id: int | None = None):
    """Run the slow top-200 scan outside Telegram command timeout."""
    try:
        ns = await storage.all_settings()
        apply_mexc_runtime_env({**ns, "mexc_order_leverage": "10", "mexc_order_open_type": "1"})
        ex = await get_exchange(ns)
        ws = await get_ws(ns)
        chatgpt_log_event("mode_scan_call")
        scan_limit = int(os.getenv("CHATGPT_SCAN_LIMIT", "200") or 200)
        status_message_id = await _edit_or_send_scan_status(
            app,
            chat_id,
            "🤖 ChatGPT Scan Mode ON\n⏳ Скан top-200 идёт в фоне. Собираю log.txt + task.txt + manifest.json + графики в ZIP...",
            status_message_id,
        )
        pack_path = await build_chatgpt_scan_pack(ex, scanner, ns, ws_supervisor=ws, limit=scan_limit, storage=storage)
        size_kb = round(os.path.getsize(pack_path) / 1024, 1) if os.path.exists(pack_path) else 0
        # v0404: never send a tiny/empty pack as if it were a real scan.
        # A 6-7 KB ZIP means log/task/manifest exists, but charts are absent.
        import zipfile as _zipfile
        with _zipfile.ZipFile(pack_path, "r") as _z:
            _manifest = json.loads(_z.read("manifest.json").decode("utf-8", errors="replace"))
        expected_png = int(_manifest.get("expected_png_count") or 0)
        actual_png = int(_manifest.get("actual_png_count") or 0)
        min_required_png = max(1, int(expected_png * float(os.getenv("CHATGPT_SCAN_MIN_CHART_RATIO", "0.80") or 0.80))) if expected_png else 1
        if actual_png < min_required_png:
            errors = _manifest.get("generation_errors") or []
            missing = _manifest.get("missing_charts") or []
            raise RuntimeError(
                "scan pack rejected: charts missing "
                f"actual={actual_png}, expected={expected_png}, required={min_required_png}; "
                f"errors={errors[:3]}; missing={missing[:5]}"
            )
        status_message_id = await _edit_or_send_scan_status(
            app,
            chat_id,
            f"🤖 ChatGPT Scan Mode\n✅ ZIP собран: {os.path.basename(pack_path)} ({size_kb} KB)\n📊 графики: {actual_png}/{expected_png}\n📤 Отправляю архив в Telegram...",
            status_message_id,
        )
        ok = await _send_chatgpt_pack_document(
            app,
            chat_id,
            pack_path,
            caption=f"✅ ChatGPT pack готов: {os.path.basename(pack_path)}\nlog.txt + task.txt + manifest.json + raw-графики. Скинь ZIP в ChatGPT. После финального анализа загрузи сюда setup-HHMM_DDMM.txt с setup_version 1.6.",
        )
        if not ok:
            raise RuntimeError("Telegram send_document failed after retries")
        await storage.set("chatgpt_waiting_setup", True)
        await _delete_scan_status(app, chat_id, status_message_id)
        chatgpt_log_event("mode_waiting_setup", pack_path=pack_path)
    except Exception as e:
        chatgpt_log_event("mode_scan_failed", error=repr(e))
        log.exception("chatgpt scan background failed")
        try:
            err_text = "❌ ChatGPT scan failed:\n" + str(e)[:1600] + "\n\nСкинь /log_chatgpt, если нужно добить причину."
            await _edit_or_send_scan_status(app, chat_id, err_text, status_message_id)
        except Exception:
            pass
    finally:
        try:
            app.bot_data["chatgpt_scan_running"] = False
            app.bot_data["chatgpt_monitor_paused_for_scan"] = False
        except Exception:
            pass



async def chatgpt_accept_setup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enter ChatGPT setup import mode without starting a new market scan.

    v0391: build a fast runtime MEXC-symbol manifest on button press.
    This avoids stale/missing scan-pack manifests after redeploy and prevents
    false rejects like ICP_USDT not in BTC/ETH-only context.
    """
    global running, entries_enabled, position_task
    if not allowed(update):
        return
    chatgpt_log_event("mode_accept_setup_requested")
    await disable_other_modes(storage)
    chatgpt_log_event("mode_other_entries_disabled", source="accept_setup")
    entries_enabled = False
    running = True
    if position_task is None or position_task.done():
        position_task = context.application.create_task(position_management_loop(context.application))

    try:
        ns = await storage.all_settings()
        apply_mexc_runtime_env({**ns, "mexc_order_leverage": "10", "mexc_order_open_type": "1"})
        ex = await get_exchange(ns)
        manifest = await build_chatgpt_runtime_manifest_from_mexc(storage, ex, source="accept_setup_button")
        runtime_manifest_msg = (
            f"\n⚡ Быстрый symbol-manifest создан: {len(manifest.get('selected_symbols') or [])} "
            "MEXC Futures symbols. Это не scan top-200, без графиков."
        )
    except Exception as e:
        chatgpt_log_event("mode_accept_setup_runtime_manifest_failed", error=repr(e))
        await storage.set("chatgpt_waiting_setup", False)
        await storage.set("chatgpt_setup_mode", False)
        await reply(
            update,
            "❌ Приём setup не включён\n"
            f"Причина: не смог быстро создать manifest актуальных MEXC symbols: {str(e)[:900]}\n\n"
            "Нажми «📥 Принять setup» ещё раз или запусти ChatGPT Scan Mode.",
            reply_markup=MAIN_MENU,
        )
        return

    await storage.set("chatgpt_waiting_setup", True)
    await storage.set("chatgpt_setup_mode", True)
    chatgpt_log_event("mode_waiting_setup_manual")
    await reply(
        update,
        "📥 Приём setup включён\n"
        "Скан НЕ запускаю. Пришли setup-HHMM_DDMM.txt с setup_version 1.6. "
        "Старые режимы входа отключены, сопровождение позиций остаётся включённым."
        f"{runtime_manifest_msg}",
        reply_markup=MAIN_MENU,
    )


def _current_chatgpt_scan_workers() -> tuple[int, int]:
    """Return current ChatGPT Scan Mode workers for scan and chart stages."""
    try:
        scan_workers = int(os.getenv("CHATGPT_SCAN_CONCURRENCY", "3") or 3)
    except Exception:
        scan_workers = 3
    try:
        chart_workers = int(os.getenv("CHATGPT_CHART_CONCURRENCY", str(scan_workers)) or scan_workers)
    except Exception:
        chart_workers = scan_workers
    return scan_workers, chart_workers


async def scan_potok_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Runtime command: /scan_potok 2, /scan_potok 3, /scan_potok 4, etc.

    It changes both ChatGPT Scan Mode scan workers and chart workers in the
    current running process. Defaults in .env remain 3 after restart.
    Claude Autopilot uses the same scan-pack engine, so it also uses this
    current runtime value for pack creation.
    """
    if not allowed(update):
        return
    args = list(getattr(context, "args", []) or [])
    if not args:
        sw, cw = _current_chatgpt_scan_workers()
        await reply(
            update,
            "🧵 ChatGPT Scan потоки сейчас:\n"
            f"scan={sw}, charts={cw}\n\n"
            "Изменить: /scan_potok 2, /scan_potok 3, /scan_potok 4 или /scan_potok 5.\n"
            "По умолчанию после перезапуска: 3.",
            reply_markup=MAIN_MENU,
        )
        return
    raw = str(args[0]).strip().replace(",", ".")
    try:
        workers = int(float(raw))
    except Exception:
        await reply(update, "❌ Нужна цифра. Пример: /scan_potok 3", reply_markup=MAIN_MENU)
        return
    if workers < 1 or workers > 8:
        await reply(update, "❌ Разрешено 1–8 потоков. Для теста лучше 2, 3, 4 или 5.", reply_markup=MAIN_MENU)
        return
    os.environ["CHATGPT_SCAN_CONCURRENCY"] = str(workers)
    os.environ["CHATGPT_CHART_CONCURRENCY"] = str(workers)
    chatgpt_log_event("scan_potok_changed", workers=workers, source="command")
    note = ""
    if workers >= 5:
        note = "\n⚠️ 5+ потоков могут быть медленнее на слабом VPS или при лимитах MEXC."
    await reply(
        update,
        f"✅ Потоки ChatGPT Scan изменены на {workers}\n"
        f"scan={workers}, charts={workers}\n"
        "Claude Autopilot использует тот же scan-pack engine, поэтому тоже возьмёт это значение для формирования pack.\n"
        "После перезапуска бота снова будет default из .env: 3."
        f"{note}",
        reply_markup=MAIN_MENU,
    )


async def chatgpt_scan_mode_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """One-shot ChatGPT market scan mode.

    Hard disables other entry modes and starts the slow top-200 scan in the
    background, so the Telegram button command does not hit the 45s timeout.
    """
    global running, entries_enabled, trading_task, position_task
    chatgpt_log_event("mode_enter_requested")
    await disable_other_modes(storage)
    chatgpt_log_event("mode_other_entries_disabled")
    entries_enabled = False
    running = True
    if position_task is None or position_task.done():
        position_task = context.application.create_task(position_management_loop(context.application))
    chat_id = update.effective_chat.id if update.effective_chat else first_admin_id()
    await _delete_user_command_message(update)
    if context.application.bot_data.get("chatgpt_scan_running"):
        await context.application.bot.send_message(chat_id=chat_id, text="⏳ ChatGPT Scan уже идёт. Дождись ZIP или проверь /log_chatgpt.", reply_markup=MAIN_MENU)
        return
    context.application.bot_data["chatgpt_scan_running"] = True
    context.application.bot_data["chatgpt_monitor_paused_for_scan"] = True
    await _delete_known_bottom_message(context.application, chat_id, "chatgpt_mode_monitor")
    status_msg = await context.application.bot.send_message(
        chat_id=chat_id,
        text="🤖 ChatGPT Scan Mode ON\nОтключил остальные режимы входа. Скан топ-200 запущен в фоне. Это сообщение будет обновляться, а после отправки ZIP удалится.",
    )
    status_message_id = getattr(status_msg, "message_id", None)
    context.application.create_task(_chatgpt_scan_background_job(context.application, chat_id, status_message_id=status_message_id))


async def chatgpt_exit_mode_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chatgpt_log_event("mode_exit_requested")
    await storage.set("chatgpt_setup_mode", False)
    await storage.set("chatgpt_waiting_setup", False)
    chatgpt_log_event("mode_exit_done")
    await reply(update, "❌ ChatGPT Mode OFF\nОжидание setup.txt отменено. Старые режимы автоматически не включал — включи нужный режим вручную.", reply_markup=MAIN_MENU)


async def log_chatgpt_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return
    try:
        path = chatgpt_runtime_log_path()
        chatgpt_log_event("log_chatgpt_requested", user_id=getattr(update.effective_user, "id", ""))
        if os.path.exists(path) and os.path.getsize(path) > 0:
            chat_id = update.effective_chat.id if update.effective_chat else first_admin_id()
            with open(path, "rb") as f:
                await context.application.bot.send_document(
                    chat_id=chat_id,
                    document=f,
                    filename="chatgpt_mode_runtime.log",
                    caption="📄 Подробный лог ChatGPT Mode. При ошибке скинь этот файл мне.",
                    connect_timeout=float(os.getenv("TELEGRAM_DOCUMENT_CONNECT_TIMEOUT_SEC", "30") or 30),
                    read_timeout=float(os.getenv("TELEGRAM_DOCUMENT_READ_TIMEOUT_SEC", "180") or 180),
                    write_timeout=float(os.getenv("TELEGRAM_DOCUMENT_WRITE_TIMEOUT_SEC", "180") or 180),
                    pool_timeout=float(os.getenv("TELEGRAM_DOCUMENT_POOL_TIMEOUT_SEC", "30") or 30),
                )
        else:
            await reply(update, "Лог ChatGPT Mode пока пустой.", reply_markup=MAIN_MENU)
    except Exception as e:
        await reply(update, f"❌ Не смог отправить /log_chatgpt: {str(e)[:900]}\n\nПоследние строки:\n{tail_chatgpt_runtime_log(40)[:2500]}", reply_markup=MAIN_MENU)


async def log_claude_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return
    try:
        path = claude_runtime_log_path()
        claude_log_event("log_claude_requested", user_id=getattr(update.effective_user, "id", ""))
        if os.path.exists(path) and os.path.getsize(path) > 0:
            chat_id = update.effective_chat.id if update.effective_chat else first_admin_id()
            with open(path, "rb") as f:
                await context.application.bot.send_document(
                    chat_id=chat_id,
                    document=f,
                    filename="claude_autopilot_runtime.log",
                    caption="📄 Подробный лог Claude Autopilot: API, scan-pack, Claude, setup, LIVE-входы, SL/TP и сопровождение.",
                    connect_timeout=float(os.getenv("TELEGRAM_DOCUMENT_CONNECT_TIMEOUT_SEC", "30") or 30),
                    read_timeout=float(os.getenv("TELEGRAM_DOCUMENT_READ_TIMEOUT_SEC", "180") or 180),
                    write_timeout=float(os.getenv("TELEGRAM_DOCUMENT_WRITE_TIMEOUT_SEC", "180") or 180),
                    pool_timeout=float(os.getenv("TELEGRAM_DOCUMENT_POOL_TIMEOUT_SEC", "30") or 30),
                )
        else:
            await reply(update, "Лог Claude Autopilot пока пустой.", reply_markup=MAIN_MENU)
    except Exception as e:
        await reply(update, f"❌ Не смог отправить /log_claude: {str(e)[:900]}\n\nПоследние строки:\n{tail_claude_runtime_log(60)[:3000]}", reply_markup=MAIN_MENU)

def format_chatgpt_setup_execution_report(result: dict) -> str:
    """Human-readable immediate report after setup upload.

    This is intentionally separate from the persistent monitor message: when no
    entries are placed, the user must see exact skip reasons in chat instead of
    guessing from /log_chatgpt.
    """
    if not isinstance(result, dict):
        return "❌ setup обработан, но результат пустой/непонятный"
    opened = result.get("opened") if isinstance(result.get("opened"), list) else []
    placed_rows = [x for x in opened if isinstance(x, dict) and bool(x.get("ok"))]
    skipped_rows = [x for x in opened if isinstance(x, dict) and not bool(x.get("ok"))]
    requested = result.get("requested_trades", result.get("limits_to_place", len(opened)))
    cleanup = result.get("cleanup_summary") or {}
    status = "✅ setup-файл обработан" if bool(result.get("ok", True)) else "❌ setup-файл НЕ исполнен"
    lines = [
        status,
        f"📄 к установке из setup: {requested}",
        f"✅ поставлено новых входов: {len(placed_rows)}",
        f"❌ пропущено: {len(skipped_rows)}",
    ]
    if cleanup:
        lines += [
            "",
            "🧹 очистка перед setup:",
            f"• entry-лимитки: найдено {cleanup.get('entry_found', 0)}, снято {cleanup.get('entry_cancelled', result.get('cancelled_pending_count', 0))}, осталось {cleanup.get('entry_left', 0)}",
            f"• старые условные без позиции: найдено {cleanup.get('orphan_found', 0)}, снято {cleanup.get('orphan_cancelled', 0)}, осталось {cleanup.get('orphan_left', 0)}",
        ]
    if placed_rows:
        lines += ["", "📌 Новые входы:"]
        for row in placed_rows[:6]:
            sym = row.get("symbol") or "-"
            side = str(row.get("side") or row.get("direction") or "").upper()
            order_type = str(row.get("order_type") or "").upper()
            entry = row.get("entry")
            lines.append(f"• {sym} {side or '-'} {order_type or '-'} entry={entry}")
    if skipped_rows:
        lines += ["", "⚠️ Почему не выставлены:"]
        for row in skipped_rows[:10]:
            sym = row.get("symbol") or "-"
            reason = row.get("reason") or ((row.get("result") or {}) if isinstance(row.get("result"), dict) else {}).get("reason") or "unknown"
            lines.append(f"• {sym}: {str(reason)[:350]}")
    msg = result.get("message")
    if msg:
        lines += ["", f"status: {msg}"]
    return "\n".join(lines)[:3900]


async def document_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global running, position_task
    if not allowed(update):
        return
    doc = update.message.document if update.message else None
    name = str(getattr(doc, "file_name", "") or "")
    name_l = name.lower()
    is_setup_txt = bool(doc and name_l.endswith(".txt") and "setup" in name_l)

    if not is_setup_txt:
        chatgpt_log_event("setup_file_rejected_bad_filename", filename=name)
        await reply(update, "❌ setup-файл отклонён\nПричина: нужен .txt файл с setup в имени, например setup-0059_0106.txt", reply_markup=MAIN_MENU)
        return

    waiting = bool(await storage.get("chatgpt_waiting_setup", False))
    if not waiting:
        chatgpt_log_event("setup_file_rejected_not_waiting", filename=name)
        await reply(update, "❌ setup-файл получен, но режим приёма setup не включён.\nНажми «📥 Принять setup» и отправь файл снова.", reply_markup=MAIN_MENU)
        return

    try:
        chatgpt_log_event("setup_document_received", filename=name, file_size=getattr(doc, "file_size", ""))
        chatgpt_log_event("setup_filename_accepted", filename=name)
        tg_file = await context.bot.get_file(doc.file_id)
        safe_name = name.replace("/", "_").replace("\\", "_")
        tmp_path = f"/tmp/{int(time.time())}_{safe_name}"
        await tg_file.download_to_drive(tmp_path)
        chatgpt_log_event("setup_file_downloaded", filename=name, path=tmp_path)
        with open(tmp_path, "r", encoding="utf-8-sig") as f:
            text = f.read()

        try:
            setup = extract_setup_json(text)
        except Exception as e:
            chatgpt_log_event("setup_extract_error", filename=name, error=str(e))
            await reply(update, "❌ setup-файл отклонён\n"
                              f"Причина: JSON setup не найден или повреждён: {str(e)[:700]}\n\n"
                              "Файл setup должен быть чистым JSON object: начинаться с { и заканчиваться }.\n"
                              "Без Markdown, без поясняющего текста, без строк вида setup_version: 1.6 вне JSON.", reply_markup=MAIN_MENU)
            return

        setup_version = str(setup.get("setup_version") or "").strip()
        if setup_version != CHATGPT_SETUP_VERSION:
            chatgpt_log_event("setup_version_rejected", filename=name, setup_version=setup_version or "MISSING", required=CHATGPT_SETUP_VERSION)
            await reply(update, "❌ setup-файл отклонён\n"
                                f"Причина: неподдерживаемая setup_version={setup_version or 'MISSING'}\n"
                                f"Нужна версия: {CHATGPT_SETUP_VERSION}\n"
                                "Старые setup-файлы не поддерживаются.", reply_markup=MAIN_MENU)
            return

        settings = await storage.all_settings()
        apply_mexc_runtime_env({**settings, "mexc_order_leverage": "10", "mexc_order_open_type": "1"})
        ex = await get_exchange(settings)
        running = True
        if position_task is None or position_task.done():
            position_task = context.application.create_task(position_management_loop(context.application))

        try:
            await notify_admin_bottom_replace(context.application, _format_setup_lifecycle_text(setup, phase="placing"), key="chatgpt_setup_lifecycle", min_interval_sec=0)
            chatgpt_log_event("setup_lifecycle_message_sent", phase="placing")
        except Exception as e:
            chatgpt_log_event("setup_lifecycle_message_error", phase="placing", error=repr(e))
        result = await execute_setup(storage, ex, setup)
        try:
            await notify_admin_bottom_replace(context.application, _format_setup_lifecycle_text(setup, result, phase="done"), key="chatgpt_setup_lifecycle", min_interval_sec=0)
            chatgpt_log_event("setup_lifecycle_message_sent", phase="done")
        except Exception as e:
            chatgpt_log_event("setup_lifecycle_message_error", phase="done", error=repr(e))
        opened_rows = result.get("opened") if isinstance(result.get("opened"), list) else []
        placed_count = len([x for x in opened_rows if isinstance(x, dict) and bool(x.get("ok"))])
        skipped_count = len([x for x in opened_rows if isinstance(x, dict) and not bool(x.get("ok"))])
        chatgpt_log_event("setup_file_processed", result=result)
        chatgpt_log_event(
            "setup_execution_user_summary",
            ok=bool(result.get("ok", True)) if isinstance(result, dict) else False,
            requested=result.get("requested_trades", result.get("limits_to_place", "-")) if isinstance(result, dict) else "-",
            placed=placed_count,
            skipped=skipped_count,
            message=result.get("message") if isinstance(result, dict) else "",
            skipped_rows=[x for x in opened_rows if isinstance(x, dict) and not bool(x.get("ok"))][:10],
        )
        try:
            await reply(update, format_chatgpt_setup_execution_report(result), reply_markup=MAIN_MENU)
        except Exception as e:
            chatgpt_log_event("setup_execution_user_summary_send_failed", error=repr(e))

        running = True
        if position_task is None or position_task.done():
            position_task = context.application.create_task(position_management_loop(context.application))
        try:
            from chatgpt_mode import _now_chatgpt_display_short
            result["setup_installed_at"] = _now_chatgpt_display_short()
        except Exception:
            result["setup_installed_at"] = datetime.now(timezone(timedelta(hours=3))).strftime("%H:%M МСК")
        result["_monitor_persist"] = True
        await storage.set("chatgpt_last_setup_result", result)
        await storage.set("chatgpt_waiting_setup", False)
        try:
            await update_chatgpt_monitor_message(context.application, ex=ex, setup_result=result)
        except Exception as e:
            chatgpt_log_event("chatgpt_monitor_after_setup_error", error=str(e))
            await reply(update, f"❌ setup обработан, но monitor не обновился: {str(e)[:900]}", reply_markup=MAIN_MENU)
    except Exception as e:
        chatgpt_log_event("setup_file_processing_failed", filename=name, error=repr(e))
        log.exception("setup.txt processing failed")
        await reply(update, f"❌ setup-файл отклонён\nПричина: {str(e)[:1200]}", reply_markup=MAIN_MENU)

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
        ("🚨 Panic", panic_cmd), ("Panic", panic_cmd), ("/panic", panic_cmd), ("/Panic", panic_cmd),
        ("📈 Positions", positions_cmd), ("Positions", positions_cmd),
        ("🧯 Close All", close_all_cmd), ("Close All", close_all_cmd), ("close all", close_all_cmd), ("/close all", close_all_cmd), ("/Close all", close_all_cmd), ("/close_all", close_all_cmd),
        ("🧹 Cancel All", cancel_all_cmd), ("Cancel All", cancel_all_cmd), ("cancel all", cancel_all_cmd), ("cansel all", cancel_all_cmd), ("/cancel all", cancel_all_cmd), ("/cansel all", cancel_all_cmd), ("/cancel_all", cancel_all_cmd),
        ("📉 Stats", stats_cmd), ("Stats", stats_cmd),
        ("💰 Balance", balance_cmd), ("Balance", balance_cmd), ("баланс", balance_cmd), ("Баланс", balance_cmd),
        ("🏓 Ping", ping_cmd), ("Ping", ping_cmd),
        ("⚙️ Settings", settings_cmd), ("⚙ Settings", settings_cmd), ("Settings", settings_cmd),
        ("🔐 API", api_cmd), ("API", api_cmd),
        ("📊 AI Stats", ai_stats_cmd), ("AI Stats", ai_stats_cmd),
        ("🤖 AI BTC/ETH scalping", ai_scalping_toggle_cmd), ("AI BTC/ETH scalping", ai_scalping_toggle_cmd),
        ("₿ BTC AI 4H автопилот", btc_ai_autopilot_cmd), ("BTC AI 4H автопилот", btc_ai_autopilot_cmd), ("♟ Game BTC AI", game_btc_ai_cmd), ("Game BTC AI", game_btc_ai_cmd), ("/game_btc_ai", game_btc_ai_cmd),
        ("📊 BTC Status", status_btc_cmd), ("BTC Status", status_btc_cmd), ("/status_btc", status_btc_cmd),
        ("🧪 BTC Backtest", backtest_btc_patterns_cmd), ("🧪 BTC Backtest 4H", backtest_btc_patterns_cmd), ("/backtest_btc_patterns", backtest_btc_patterns_cmd), ("🧪 BTC Backtest 1H", backtest_btc_patterns_1h_cmd), ("/backtest_btc_patterns_1h", backtest_btc_patterns_1h_cmd), ("🧪 Round Levels", backtest_round_levels_cmd), ("/backtest_round_levels", backtest_round_levels_cmd), ("🧪 Strategy Lab", backtest_strategy_lab_cmd), ("/backtest_strategy_lab", backtest_strategy_lab_cmd), ("🧪 Strategy Detail", backtest_strategy_lab_extra_cmd), ("🧪 Strategy Lab Extra", backtest_strategy_lab_extra_cmd), ("/backtest_strategy_lab_extra", backtest_strategy_lab_extra_cmd), ("🔥 Aggressive Lab", backtest_aggressive_lab_cmd), ("/backtest_aggressive_lab", backtest_aggressive_lab_cmd),
        ("🧽 Clean BTC Orders", clean_btc_orders_cmd), ("Clean BTC Orders", clean_btc_orders_cmd), ("/clean_btc_orders", clean_btc_orders_cmd),
        ("⚡ быстрый отскок", quick_bounce_cmd), ("быстрый отскок", quick_bounce_cmd), ("Быстрый отскок", quick_bounce_cmd),
        ("🔻 импульсный слив", impulse_dump_cmd), ("импульсный слив", impulse_dump_cmd), ("Импульсный слив", impulse_dump_cmd),
        ("📊 orderflow impulse", orderflow_impulse_cmd), ("orderflow impulse", orderflow_impulse_cmd), ("Orderflow impulse", orderflow_impulse_cmd),
        ("🌊 cascade hunter", cascade_hunter_cmd), ("cascade hunter", cascade_hunter_cmd), ("Cascade hunter", cascade_hunter_cmd),
        ("💪 strongest coin", strongest_coin_cmd), ("strongest coin", strongest_coin_cmd), ("Strongest coin", strongest_coin_cmd),
        ("🗡 knife reversal", knife_reversal_cmd), ("knife reversal", knife_reversal_cmd), ("Knife reversal", knife_reversal_cmd),
        ("🧠 multi strategy", multi_strategy_cmd), ("multi strategy", multi_strategy_cmd), ("Multi strategy", multi_strategy_cmd),
        ("🚀 BOOST MODE", boost_start_cmd), ("BOOST MODE", boost_start_cmd),
        ("🛑 STOP BOOST", boost_stop_cmd), ("STOP BOOST", boost_stop_cmd),
        ("🤖 ChatGPT Scan Mode", chatgpt_scan_mode_cmd), ("ChatGPT Scan Mode", chatgpt_scan_mode_cmd),
        ("🤖 Claude Autopilot", claude_autopilot_cmd), ("Claude Autopilot", claude_autopilot_cmd),
        ("📥 Принять setup", chatgpt_accept_setup_cmd), ("Принять setup", chatgpt_accept_setup_cmd), ("Accept setup", chatgpt_accept_setup_cmd), ("/import_setup", chatgpt_accept_setup_cmd),
        ("📄 Log ChatGPT", log_chatgpt_cmd), ("Log ChatGPT", log_chatgpt_cmd), ("📄 Log Claude", log_claude_cmd), ("Log Claude", log_claude_cmd),
        ("❌ Exit ChatGPT Mode", chatgpt_exit_mode_cmd), ("Exit ChatGPT Mode", chatgpt_exit_mode_cmd),
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
    allowed_prefixes = {"boost", "toggle", "set", "menu", "api", "aistats", "openai", "claude", "noop"}
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


    if data[0] == "claude":
        action = data[1] if len(data) > 1 else "menu"
        if action == "enable":
            await storage.set("claude_autopilot_enabled", True)
            ns = await storage.all_settings()
            await _safe_edit_message_text(q.message, claude_autopilot_menu_text(ns), reply_markup=claude_autopilot_keyboard(ns))
            return
        if action == "disable":
            await storage.set("claude_autopilot_enabled", False)
            ns = await storage.all_settings()
            await _safe_edit_message_text(q.message, claude_autopilot_menu_text(ns), reply_markup=claude_autopilot_keyboard(ns))
            return
        if action == "cycle_toggle":
            cur = _boolish((await storage.all_settings()).get("claude_autopilot_cycle_enabled"), True)
            await storage.set("claude_autopilot_cycle_enabled", not cur)
            ns = await storage.all_settings()
            await _safe_edit_message_text(q.message, claude_autopilot_menu_text(ns), reply_markup=claude_autopilot_keyboard(ns))
            chatgpt_log_event("claude_cycle_toggle", enabled=str(not cur))
            return
        if action == "model":
            choice = data[2] if len(data) > 2 else "sonnet"
            await storage.set("claude_autopilot_model", CLAUDE_OPUS_48 if choice == "opus" else CLAUDE_SONNET_46)
            ns = await storage.all_settings()
            await _safe_edit_message_text(q.message, claude_autopilot_menu_text(ns), reply_markup=claude_autopilot_keyboard(ns))
            return
        if action == "analysis":
            value = data[2] if len(data) > 2 else "optimal"
            if value not in {"optimal", "full"}:
                value = "optimal"
            await storage.set("claude_analysis_mode", value)
            ns = await storage.all_settings()
            await _safe_edit_message_text(q.message, claude_autopilot_menu_text(ns), reply_markup=claude_autopilot_keyboard(ns))
            return
        if action == "schedule":
            value = data[2] if len(data) > 2 else "off"
            if value not in {"off", "4h", "1h", "2h"}:
                value = "off"
            await storage.set("claude_autopilot_schedule", value)
            # Reset the 2h timer from the moment the user selects it.
            if value == "2h":
                await storage.set("claude_autopilot_last_run_ts", time.time(), bump_revision=False)
            ns = await storage.all_settings()
            await _safe_edit_message_text(q.message, claude_autopilot_menu_text(ns), reply_markup=claude_autopilot_keyboard(ns))
            return
        if action == "resolution":
            choice = data[2] if len(data) > 2 else "960"
            value = "1280x720" if choice == "1280" else "960x540"
            await storage.set("claude_chart_resolution", value)
            ns = await storage.all_settings()
            await _safe_edit_message_text(q.message, claude_autopilot_menu_text(ns), reply_markup=claude_autopilot_keyboard(ns))
            return
        if action == "api_help":
            await _safe_edit_message_text(q.message, claude_api_help_text(), reply_markup=claude_autopilot_keyboard(await storage.all_settings()))
            return
        if action == "run":
            ns = await storage.all_settings()
            if not _boolish(ns.get("claude_autopilot_enabled"), False):
                await _safe_edit_message_text(q.message, "⛔ Claude Autopilot выключен. Сначала нажми 🟢 Включить.", reply_markup=claude_autopilot_keyboard(ns))
                return
            if context.application.bot_data.get("claude_autopilot_running"):
                await _safe_edit_message_text(q.message, "⏳ Claude Autopilot уже выполняется. Новый запуск не создан.", reply_markup=claude_autopilot_keyboard(ns))
                chatgpt_log_event("claude_manual_run_skipped_already_running")
                return
            await _safe_edit_message_text(q.message, "🤖 Claude Autopilot LIVE\n\n⏳ Запуск сейчас LIVE...")
            chat_id = q.message.chat_id if q.message else int(first_admin_id() or 0)
            context.application.create_task(_claude_run_cycle(context.application, int(chat_id), trigger="manual", status_message=q.message))
            return
        if action == "exit_confirm":
            await _safe_edit_message_text(
                q.message,
                "🚨 EXIT / CLOSE ALL\n\n⚠️ Подтвердить: выключить Claude Autopilot, остановить расписание, отменить лимитки и закрыть ВСЕ позиции market?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ ДА, ЗАКРЫТЬ ВСЁ", callback_data="claude:exit_do")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="claude:menu")],
                ]),
            )
            return
        if action == "exit_do":
            await _safe_edit_message_text(q.message, "🚨 EXIT / CLOSE ALL\n\n⏳ Выполняю аварийное закрытие...")
            chat_id = q.message.chat_id if q.message else int(first_admin_id() or 0)
            context.application.create_task(_claude_emergency_close_all(context.application, int(chat_id), status_message=q.message))
            return
        ns = await storage.all_settings()
        await _safe_edit_message_text(q.message, claude_autopilot_menu_text(ns), reply_markup=claude_autopilot_keyboard(ns))
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
            os.environ.pop("MEXC_API_KEY", None)
            os.environ.pop("MEXC_API_SECRET", None)
            clear_runtime_secret_cache(["mexc_api_key", "mexc_api_secret"])
            clear_secret_backup(["mexc_api_key", "mexc_api_secret"])
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
            os.environ.pop("OPENAI_API_KEY", None)
            clear_runtime_secret_cache(["openai_api_key"])
            clear_secret_backup(["openai_api_key"])
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
        orderbook = None
        trades = []
        try:
            orderbook = await client.fetch_order_book(spot_symbol, limit=20)
        except Exception:
            orderbook = None
        try:
            trades = await client.fetch_trades(spot_symbol, limit=100)
        except Exception:
            trades = []

        if not candles or len(candles) < 5:
            return None
        vols = [float(c[5]) for c in candles]
        closes = [float(c[4]) for c in candles]
        avg = sum(vols[:-1]) / max(1, len(vols[:-1]))
        move = (closes[-1] - closes[-2]) / closes[-2] * 100 if closes[-2] else 0
        bid_vol = ask_vol = 0.0
        if isinstance(orderbook, dict):
            bid_vol = sum(float(p) * float(q) for p, q in (orderbook.get("bids") or [])[:20])
            ask_vol = sum(float(p) * float(q) for p, q in (orderbook.get("asks") or [])[:20])
        ob_imb = ((bid_vol - ask_vol) / (bid_vol + ask_vol)) if (bid_vol + ask_vol) > 0 else 0.0
        buy_vol = sell_vol = 0.0
        for tr in trades or []:
            try:
                amount = float(tr.get("amount") or 0)
                price = float(tr.get("price") or 0)
                notional = amount * price
                side = str(tr.get("side") or "").lower()
                if side == "buy":
                    buy_vol += notional
                elif side == "sell":
                    sell_vol += notional
            except Exception:
                pass
        delta = buy_vol - sell_vol
        delta_ratio = (delta / (buy_vol + sell_vol)) if (buy_vol + sell_vol) > 0 else 0.0
        return {
            "spot_source": spot_source,
            "spot_price": float(ticker.get("last") or closes[-1]),
            "spot_volume_now": vols[-1],
            "spot_volume_avg": avg,
            "spot_price_change_pct": move,
            "spot_orderbook_imbalance": ob_imb,
            "spot_bid_depth_usdt": bid_vol,
            "spot_ask_depth_usdt": ask_vol,
            "spot_delta_usdt": delta,
            "spot_delta_ratio": delta_ratio,
            "spot_buy_volume_usdt": buy_vol,
            "spot_sell_volume_usdt": sell_vol,
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

    ChatGPT/Claude setup LIMIT timeout events are aggregated into one replaceable
    bottom card. This prevents repeated "Position event / limit timeout" spam
    while still showing the actionable result.
    """
    chatgpt_limit_timeout: list[dict] = []
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
        if ev_type == "limit_timeout":
            result = ev.get("result") if isinstance(ev.get("result"), dict) else {}
            strategy = str(ev.get("strategy") or result.get("strategy") or "").lower()
            # Most ChatGPT pending rows do not carry strategy in the returned
            # event, so treat LIMIT timeout as a replaceable lifecycle notice.
            chatgpt_limit_timeout.append(ev)
            continue
        text = format_position_event(ev)
        if ev_type == "protection_watchdog":
            # v33: ChatGPT Mode protection state is displayed in the single live
            # monitor card. Do not send separate repeated LOCAL PROTECTION MODE
            # cards for missing SL/TP; they spam the chat.
            is_chatgpt = str(ev.get("strategy") or "").lower() == "chatgpt_setup"
            is_missing = str(ev.get("protection_status") or "").upper() not in {"EXCHANGE PROTECTED", "TP + LIQUIDATION STOP", "EMERGENCY SL ONLY"}
            if is_chatgpt and is_missing:
                continue
            symbol_key = str(ev.get("symbol") or "position").replace("/", "_").replace(":", "_")
            app.create_task(notify_admin_bottom_replace(app, text, key=f"position_watchdog_{symbol_key}"))
        else:
            app.create_task(notify_admin(app, text, key="position_event"))

    if chatgpt_limit_timeout:
        symbols = []
        for ev in chatgpt_limit_timeout:
            sym = str(ev.get("symbol") or "-")
            if sym not in symbols:
                symbols.append(sym)
        text = "\n".join([
            "📌 Лимитки отменены по TTL",
            "Reason: limit_timeout",
            "• " + ", ".join(symbols[:12]),
        ])
        app.create_task(notify_admin_bottom_replace(app, text, key="chatgpt_limit_timeout_event", min_interval_sec=0))


# one live master status message only
async def update_chatgpt_monitor_message(app, ex=None, setup_result: dict | None = None) -> None:
    """Refresh one live ChatGPT monitor message from exchange state."""
    try:
        if app.bot_data.get("chatgpt_monitor_paused_for_scan") and setup_result is None:
            return
        settings = await storage.all_settings()
        exchange = ex or await get_exchange(settings)
        text = await build_chatgpt_monitor_text(storage, exchange, setup_result=setup_result)
        await notify_admin_bottom_replace(app, text, key="chatgpt_mode_monitor", min_interval_sec=0)
        chatgpt_log_event("chatgpt_monitor_message_updated")
    except Exception as e:
        chatgpt_log_event("chatgpt_monitor_message_update_error", error=str(e))

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
                try:
                    last_mon = float(app.bot_data.get("chatgpt_monitor_last_update", 0) or 0)
                    if time.time() - last_mon >= float(CHATGPT_MONITOR_INTERVAL_SEC):
                        app.bot_data["chatgpt_monitor_last_update"] = time.time()
                        app.create_task(update_chatgpt_monitor_message(app, ex=ex))
                except Exception as e:
                    chatgpt_log_event("chatgpt_monitor_loop_schedule_error", error=str(e))
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
                native_spot_scan_mode = mode_name in {"orderflow_impulse", "cascade_hunter", "knife_reversal", "multi_strategy"}
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
                if not (ai_mode or boost_mode or native_spot_scan_mode) and time.time() - scanner.last_refresh > int(settings.get("symbol_refresh_sec", 300)):
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
                elif ai_mode or boost_mode or native_spot_scan_mode:
                    # Clear stale legacy futures-universe state so native Binance-SPOT modes
                    # never show MEXC/Binance futures refresh errors or block on futures scans.
                    scanner.last_effective_strategy = "boost_scalping" if boost_mode else (mode_name if native_spot_scan_mode else "ai_scalping")
                    scanner.last_refresh_error = ""
                    if ai_mode or boost_mode:
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
                market_data_ok = True if (ai_mode or boost_mode or native_spot_scan_mode) else scanner_market_data_fresh(max_age_sec=max(900, int(settings.get("symbol_refresh_sec", 300)) * 3))
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
                                    "Reason: exchange TP/SL not confirmed. Position stays open under virtual TP/SL monitor.\n"
                                    f"PnL: {pnl:.4f} USDT ({pp:.3f}%)" if isinstance(pnl, (int, float)) and isinstance(pp, (int, float)) else
                                    "🛑 AI scalp aborted after entry\n"
                                    f"{getattr(plan, 'symbol', b)} {getattr(plan, 'side', '-')}\n"
                                    "Reason: exchange TP/SL not confirmed. Position stays open under virtual TP/SL monitor."
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
                impulse_dump_cycle = base_strategy_mode == "impulse_dump"
                orderflow_impulse_cycle = base_strategy_mode == "orderflow_impulse"
                cascade_hunter_cycle = base_strategy_mode == "cascade_hunter"
                knife_reversal_cycle = base_strategy_mode == "knife_reversal"
                multi_strategy_cycle = base_strategy_mode == "multi_strategy"
                strongest_coin_cycle = base_strategy_mode == "strongest_coin"
                special_native_cycle = orderflow_impulse_cycle or cascade_hunter_cycle or knife_reversal_cycle or multi_strategy_cycle or strongest_coin_cycle
                if quick_bounce_cycle and not _bool_setting(settings, "quick_bounce_enabled", False):
                    scanner.last_signal_summary = "quick_bounce OFF: scanner stopped"
                    scanner.last_reject_reason = "Press ⚡ быстрый отскок again to resume scanning. Existing positions are still managed."
                    await update_scanner_status(app, settings, status="quick bounce off")
                    await sleep_until_next_scan(app, int(settings.get("scan_interval_sec", 900)))
                    continue
                if impulse_dump_cycle and not _bool_setting(settings, "impulse_dump_enabled", False):
                    scanner.last_signal_summary = "impulse_dump OFF: scanner stopped"
                    scanner.last_reject_reason = "Press 🔻 импульсный слив again to resume scanning. Existing positions are still managed."
                    await update_scanner_status(app, settings, status="impulse dump off")
                    await sleep_until_next_scan(app, int(settings.get("scan_interval_sec", 900)))
                    continue
                if orderflow_impulse_cycle and not _bool_setting(settings, "orderflow_impulse_enabled", False):
                    scanner.last_signal_summary = "orderflow_impulse OFF: scanner stopped"
                    scanner.last_reject_reason = "Press 📊 orderflow impulse again to resume scanning. Existing positions are still managed."
                    await update_scanner_status(app, settings, status="orderflow impulse off")
                    await sleep_until_next_scan(app, int(settings.get("scan_interval_sec", 60)))
                    continue
                if cascade_hunter_cycle and not _bool_setting(settings, "cascade_hunter_enabled", False):
                    scanner.last_signal_summary = "cascade_hunter OFF: scanner stopped"
                    scanner.last_reject_reason = "Press 🌊 cascade hunter again to resume scanning. Existing positions are still managed."
                    await update_scanner_status(app, settings, status="cascade hunter off")
                    await sleep_until_next_scan(app, int(settings.get("scan_interval_sec", 60)))
                    continue
                if knife_reversal_cycle and not _bool_setting(settings, "knife_reversal_enabled", False):
                    scanner.last_signal_summary = "knife_reversal OFF: scanner stopped"
                    scanner.last_reject_reason = "Press 🗡 knife reversal again to resume scanning. Existing positions are still managed."
                    await update_scanner_status(app, settings, status="knife reversal off")
                    await sleep_until_next_scan(app, int(settings.get("scan_interval_sec", 60)))
                    continue
                if strongest_coin_cycle and not _bool_setting(settings, "strongest_coin_enabled", False):
                    scanner.last_signal_summary = "strongest_coin OFF: scanner stopped"
                    scanner.last_reject_reason = "Press 💪 strongest coin again to resume scanning. Existing positions are still managed."
                    await update_scanner_status(app, settings, status="strongest coin off")
                    await sleep_until_next_scan(app, int(settings.get("scan_interval_sec", 60)))
                    continue
                if multi_strategy_cycle and not _bool_setting(settings, "multi_strategy_enabled", False):
                    scanner.last_signal_summary = "multi_strategy OFF: scanner stopped"
                    scanner.last_reject_reason = "Press 🧠 multi strategy again to resume scanning. Existing positions are still managed."
                    await update_scanner_status(app, settings, status="multi strategy off")
                    await sleep_until_next_scan(app, int(settings.get("scan_interval_sec", 60)))
                    continue
                if impulse_dump_cycle:
                    # Stop this mode until the next day after 3 stop-losses in a row.
                    try:
                        today_since = time.time() - 86400
                        rows = [t for t in await storage.trade_rows(since=today_since) if str(t.get("strategy", "")).lower() == "impulse_dump"]
                        streak = 0
                        for t in reversed(rows):
                            reason = str(t.get("reason", "") or t.get("result", "")).lower()
                            if "sl" in reason or "stop" in reason:
                                streak += 1
                            elif reason:
                                break
                        app.bot_data["impulse_dump_consecutive_sl"] = streak
                        limit_sl = int(float(settings.get("impulse_dump_stop_after_consecutive_sl", 3) or 3))
                        if limit_sl > 0 and streak >= limit_sl:
                            await storage.set("impulse_dump_enabled", False, bump_revision=False)
                            await storage.set("settings_revision", int(settings.get("settings_revision", 1) or 1) + 1, bump_revision=False)
                            log_event("impulse_dump_stopped_after_sl_streak", stage="risk", ok=False, streak=streak)
                            await notify_admin(app, f"🛑 Импульсный слив остановлен до следующего дня: {streak} SL подряд.", key="impulse_dump_sl_streak_stop")
                            await sleep_until_next_scan(app, int(settings.get("scan_interval_sec", 900)))
                            continue
                    except Exception as e:
                        log.debug("impulse dump SL streak check failed: %s", e)
                if quick_bounce_cycle:
                    log_event("quick_bounce_scan_start", stage="scan", ok=True, top_coins=int(float(settings.get("quick_bounce_top_coins", settings.get("max_symbols", 200)) or 200)), anomaly_tf=str(settings.get("quick_bounce_anomaly_timeframe", "1h")), confirm_tf=str(settings.get("quick_bounce_confirm_timeframe", "15m")))
                    await quick_bounce_progress_message(app, 10)
                if impulse_dump_cycle:
                    log_event("impulse_dump_scan_start", stage="scan", ok=True, top_coins=int(float(settings.get("impulse_dump_top_coins", settings.get("max_symbols", 200)) or 200)), anomaly_tf=str(settings.get("impulse_dump_anomaly_timeframe", "1h")), confirm_tf=str(settings.get("impulse_dump_confirm_timeframe", "15m")))
                    await impulse_dump_progress_message(app, 10)
                if orderflow_impulse_cycle:
                    log_event("orderflow_impulse_scan_start", stage="scan", ok=True, top_coins=int(float(settings.get("orderflow_impulse_top_coins", settings.get("max_symbols", 100)) or 100)), source="binance_spot_orderflow")
                    await orderflow_impulse_progress_message(app, 10)
                if cascade_hunter_cycle:
                    log_event("cascade_hunter_scan_start", stage="scan", ok=True, top_coins=int(float(settings.get("cascade_hunter_top_coins", settings.get("max_symbols", 100)) or 100)), source="binance_spot_cascade_pressure")
                if knife_reversal_cycle:
                    log_event("knife_reversal_scan_start", stage="scan", ok=True, top_coins=int(float(settings.get("knife_reversal_top_coins", 100) or 100)), source="binance_spot_wick_reclaim")
                if strongest_coin_cycle:
                    log_event("strongest_coin_scan_start", stage="scan", ok=True, top_coins=int(float(settings.get("strongest_coin_top_coins", settings.get("max_symbols", 200)) or 200)), source="binance_spot_strongest_coin")
                if multi_strategy_cycle:
                    log_event("multi_strategy_scan_start", stage="scan", ok=True, top_coins=int(float(settings.get("multi_strategy_top_coins", 100) or 100)), source="binance_spot_orderflow+binance_spot_knife_reversal")
                if base_strategy_mode == "all":
                    scanner.last_strategy_reason = "mode=ALL: scanning momentum+pullback+reversal (liquidity_retest is manual-only)"
                elif base_strategy_mode == "hybrid":
                    scanner.last_strategy_reason = f"mode=HYBRID, regime={regime_info.get('regime', 'LOW_VOLATILITY')}"
                else:
                    scanner.last_strategy_reason = f"manual mode={base_strategy_mode}"
                effective_settings = dict(settings)
                effective_settings["market_regime"] = regime_info.get("regime", "LOW_VOLATILITY")
                effective_settings["market_regime_info"] = regime_info
                if special_native_cycle:
                    effective_strategy = base_strategy_mode
                    scanner.last_effective_strategy = effective_strategy
                effective_settings["effective_strategy_mode"] = effective_strategy
                if quick_bounce_cycle:
                    await quick_bounce_progress_message(app, 50)
                if impulse_dump_cycle:
                    await impulse_dump_progress_message(app, 50)
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
                if impulse_dump_cycle:
                    log_event(
                        "impulse_dump_scan_done",
                        stage="scan",
                        ok=True,
                        candidates=len(candidates or []),
                        symbols=[str(c.get("symbol", "")) for c in (candidates or [])[:10]],
                        reject_reasons=getattr(scanner, "last_reject_top_reasons", []),
                        errors=getattr(scanner, "last_cycle_errors", 0),
                    )
                    await impulse_dump_progress_message(app, 100, done=True)
                    await impulse_dump_summary_message(app, settings, candidates)
                    await impulse_dump_progress_message(app, 100, clear=True)
                if cascade_hunter_cycle:
                    log_event("cascade_hunter_scan_done", stage="scan", ok=True, candidates=len(candidates or []), symbols=[str(c.get("symbol", "")) for c in (candidates or [])[:10]], reject_reasons=getattr(scanner, "last_reject_top_reasons", []), stats=getattr(scanner, "last_cascade_scan_stats", {}), errors=getattr(scanner, "last_cycle_errors", 0))
                if orderflow_impulse_cycle:
                    log_event(
                        "orderflow_impulse_scan_done",
                        stage="scan",
                        ok=True,
                        candidates=len(candidates or []),
                        symbols=[str(c.get("symbol", "")) for c in (candidates or [])[:10]],
                        reject_reasons=getattr(scanner, "last_reject_top_reasons", []),
                        stats=getattr(scanner, "last_orderflow_scan_stats", {}),
                        errors=getattr(scanner, "last_cycle_errors", 0),
                    )
                    await orderflow_impulse_progress_message(app, 100, done=True)
                    await orderflow_impulse_summary_message(app, settings, candidates)
                    await orderflow_impulse_progress_message(app, 100, clear=True)
                if knife_reversal_cycle:
                    log_event("knife_reversal_scan_done", stage="scan", ok=True, candidates=len(candidates or []), symbols=[str(c.get("symbol", "")) for c in (candidates or [])[:10]], reject_reasons=getattr(scanner, "last_reject_top_reasons", []), stats=getattr(scanner, "last_knife_scan_stats", {}), errors=getattr(scanner, "last_cycle_errors", 0))
                if strongest_coin_cycle:
                    log_event("strongest_coin_scan_done", stage="scan", ok=True, candidates=len(candidates or []), symbols=[str(c.get("symbol", "")) for c in (candidates or [])[:10]], reject_reasons=getattr(scanner, "last_reject_top_reasons", []), stats=getattr(scanner, "last_strongest_coin_stats", {}), errors=getattr(scanner, "last_cycle_errors", 0))
                if multi_strategy_cycle:
                    log_event("multi_strategy_scan_done", stage="scan", ok=True, candidates=len(candidates or []), symbols=[str(c.get("symbol", "")) for c in (candidates or [])[:10]], stats=getattr(scanner, "last_multi_strategy_stats", {}), errors=getattr(scanner, "last_cycle_errors", 0))
                if scanner.last_slowdown_sec:
                    scanner.last_reject_reason = f"scanner adaptive slowdown {scanner.last_slowdown_sec}s after {scanner.last_cycle_errors} errors"
                    await update_scanner_status(app, settings, status="scanner slowdown", force=True)
                    await asyncio.sleep(scanner.last_slowdown_sec)
                if candidates:
                    top = candidates[0]
                    if special_native_cycle:
                        scanner.last_signal_summary = (
                            f"Binance spot native candidate: {top.get('symbol')} {top.get('side')} "
                            f"conf={top.get('confidence')} strategy={top.get('strategy', effective_strategy)} "
                            f"mode={effective_strategy} count={len(candidates)} | MEXC futures execution"
                        )
                    else:
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
                # Keep /log read-only and token-free. Do not overwrite the last real
                # OpenAI verdict with "pending" when there is no candidate or when
                # a later scan does not actually call OpenAI. This keeps /log showing
                # the last completed AI check instead of a misleading stale pending state.
                ai_enabled_cycle = bool(settings.get("openai_analysis_enabled", False))
                if not ai_enabled_cycle:
                    scanner.last_openai_analysis_status = "AI analysis: OFF"
                elif candidates:
                    prev_ai_status = str(getattr(scanner, "last_openai_analysis_status", "") or "")
                    if not prev_ai_status or prev_ai_status in {"AI analysis: OFF", "AI analysis: no cached check yet"}:
                        scanner.last_openai_analysis_status = "AI analysis: waiting for candidate check"
                for cand in candidates:
                    original_symbol = cand.get("symbol")
                    cand = MirrorEngine(str(settings.get("mirror_mode", "off"))).apply(cand, adaptive_stats)
                    cand = SessionEngine(
                        enabled=bool(settings.get("session_filter_enabled", True)),
                        america_short_bias_enabled=bool(settings.get("america_short_bias_enabled", True)),
                        window_minutes=240,
                    ).apply(cand, settings)

                    if special_native_cycle and str(cand.get("strategy", "")).lower() in {"orderflow_impulse", "knife_reversal", "cascade_hunter"}:
                        # v0268: orderflow_impulse/knife_reversal are Binance-spot-native.
                        # Do not create a MEXC futures candidate and then run a second
                        # spot confirmation pass; that caused symbol mismatches like
                        # RENDER/RNDR and "Spot data unavailable". The candidate already
                        # contains the real Binance spot orderflow metrics; MEXC is only
                        # used later as the execution venue.
                        spot_data = None
                        cand["spot_confirmation"] = "NATIVE_BINANCE_SPOT"
                        cand["spot_confirmed"] = True
                        cand["spot_reason"] = "Binance spot native orderflow scan passed"
                        sd = cand.get("score_details") if isinstance(cand.get("score_details"), dict) else {}
                        log_event(
                            ("knife_reversal_spot_check" if str(cand.get("strategy", "")).lower() == "knife_reversal" else "cascade_hunter_spot_check" if str(cand.get("strategy", "")).lower() == "cascade_hunter" else "orderflow_impulse_spot_check"),
                            stage="spot_native",
                            ok=True,
                            symbol=str(original_symbol),
                            side=str(cand.get("side", "")),
                            spot_source=str(sd.get("spot_source") or "binance_spot_native"),
                            spot_symbol=sd.get("spot_symbol"),
                            spot_move_pct=sd.get("spot_move_pct"),
                            spot_volume_ratio=sd.get("spot_volume_ratio"),
                            spot_delta_ratio=sd.get("spot_delta_ratio"),
                            spot_delta_usdt=sd.get("spot_delta_usdt"),
                            spot_orderbook_imbalance=sd.get("spot_orderbook_imbalance"),
                            spot_spread_pct=sd.get("spot_spread_pct"),
                            reason="Binance spot native setup passed; MEXC futures execution only",
                        )
                    else:
                        spot_enabled = bool(settings.get("spot_confirmation_enabled", True))
                        spot_data = await fetch_spot_data_for_candidate(ex, cand, settings) if spot_enabled else None
                        cand = SpotConfirmationEngine(enabled=spot_enabled).apply(cand, spot_data)
                    cand["strategy_mode"] = base_strategy_mode
                    cand["effective_strategy_mode"] = effective_strategy
                    if special_native_cycle:
                        st_name = str(cand.get("strategy", "orderflow_impulse")).lower()
                        if st_name == "strongest_coin" or strongest_coin_cycle:
                            common_slots = int(float(settings.get("strongest_coin_max_open_positions", 1) or 1))
                        else:
                            common_slots = int(float(settings.get("multi_strategy_max_open_positions", settings.get("cascade_hunter_max_open_positions", settings.get("orderflow_impulse_max_open_positions", 3))) or 3))
                        cand["max_open_positions"] = common_slots
                        if st_name == "knife_reversal":
                            cand["trade_margin_pct"] = float(settings.get("knife_reversal_trade_margin_pct", 0.10) or 0.10)
                            cand["leverage"] = int(float(settings.get("knife_reversal_leverage", 10) or 10))
                        elif st_name == "cascade_hunter":
                            cand["trade_margin_pct"] = float(settings.get("cascade_hunter_trade_margin_pct", 0.10) or 0.10)
                            cand["leverage"] = int(float(settings.get("cascade_hunter_leverage", 10) or 10))
                        elif st_name == "strongest_coin":
                            cand["trade_margin_pct"] = float(settings.get("strongest_coin_trade_margin_pct", 0.10) or 0.10)
                            cand["leverage"] = int(float(settings.get("strongest_coin_leverage", 10) or 10))
                        else:
                            cand["trade_margin_pct"] = float(settings.get("orderflow_impulse_trade_margin_pct", 0.10) or 0.10)
                            cand["leverage"] = int(float(settings.get("orderflow_impulse_leverage", 10) or 10))

                    if not cand.get("allowed_by_session", True):
                        scanner.last_reject_reason = f"{original_symbol}: session filter blocked"
                        if orderflow_impulse_cycle:
                            log_event("orderflow_impulse_open_skipped", stage="session", ok=False, symbol=str(original_symbol), reason="session filter blocked")
                        continue
                    if (not special_native_cycle) and spot_enabled and not cand.get("spot_confirmed", True):
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
                        if orderflow_impulse_cycle:
                            log_event("orderflow_impulse_open_skipped", stage="market_filter", ok=False, symbol=str(original_symbol), reason=str(mf_reason)[:500])
                        continue

                    plan = TradePlanner().make_plan(cand, settings, equity_usdt=equity)
                    if special_native_cycle and plan:
                        if str(plan.strategy).lower() == "strongest_coin" or strongest_coin_cycle:
                            common_slots = int(float(settings.get("strongest_coin_max_open_positions", 1) or 1))
                        else:
                            common_slots = int(float(settings.get("multi_strategy_max_open_positions", settings.get("cascade_hunter_max_open_positions", settings.get("orderflow_impulse_max_open_positions", 3))) or 3))
                        try:
                            plan.max_open_positions = common_slots
                        except Exception:
                            pass
                    if not plan:
                        scanner.last_reject_reason = f"{original_symbol}: planner returned no trade"
                        if orderflow_impulse_cycle:
                            log_event("orderflow_impulse_open_skipped", stage="planner", ok=False, symbol=str(original_symbol), reason="planner returned no trade")
                        continue

                    if orderflow_impulse_cycle:
                        _d = getattr(plan, "signal_details", {}) if hasattr(plan, "signal_details") else {}
                        _d = _d if isinstance(_d, dict) else {}
                        log_event(
                            "orderflow_impulse_candidate_selected",
                            stage="candidate",
                            ok=True,
                            symbol=str(plan.symbol),
                            side=str(plan.side),
                            confidence=float(getattr(plan, "confidence", 0) or cand.get("confidence", 0) or 0),
                            spot_symbol=_d.get("spot_symbol"),
                            spot_move_pct=_d.get("spot_move_pct"),
                            spot_volume_ratio=_d.get("spot_volume_ratio"),
                            spot_delta_ratio=_d.get("spot_delta_ratio"),
                            spot_orderbook_imbalance=_d.get("spot_orderbook_imbalance"),
                            entry_price=float(getattr(plan, "entry_price", 0) or 0),
                            take_price=float(getattr(plan, "take_price", 0) or 0),
                            stop_price=float(getattr(plan, "stop_price", 0) or 0),
                        )

                    # Pre-entry eligibility before OpenAI: do not spend AI tokens and do not show
                    # an "AI approved" message for a candidate that cannot be opened due to
                    # duplicate symbol, occupied slots, or exchange-side position limits.
                    try:
                        pre_ok, pre_reason = await exec_engine.can_enter(plan.symbol, int(getattr(plan, "max_open_positions", 999)), live=live)
                    except Exception as e:
                        pre_ok, pre_reason = False, f"pre-entry check failed: {e}"
                    if not pre_ok:
                        scanner.last_reject_reason = f"{plan.symbol}: pre-entry blocked before AI: {pre_reason}"
                        if orderflow_impulse_cycle:
                            log_event("orderflow_impulse_open_skipped", stage="pre_entry", ok=False, symbol=str(plan.symbol), side=str(plan.side), reason=str(pre_reason)[:500])
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
                    if ai_enabled:
                        try:
                            scanner.last_openai_analysis_status = f"AI analysis: checking {plan.symbol} {plan.side}"[:500]
                        except Exception:
                            pass
                    ai_verdict = await ai_signal_engine.validate(cand, plan, settings)
                    if ai_enabled:
                        try:
                            scanner.last_openai_analysis_status = (
                                f"AI analysis: {plan.symbol} {plan.side} "
                                f"{'APPROVED' if ai_verdict.approved else 'REJECTED'} "
                                f"model={ai_verdict.model} mode={ai_verdict.mode} "
                                f"conf={ai_verdict.confidence:.2f} "
                                f"reason={ai_verdict.reason or ai_verdict.error or '-'}"
                            )[:500]
                            log_event(
                                "openai_analysis_check",
                                stage="entry_filter",
                                ok=bool(ai_verdict.ok),
                                approved=bool(ai_verdict.approved),
                                symbol=str(plan.symbol),
                                side=str(plan.side),
                                strategy=str(getattr(plan, "strategy", cand.get("strategy", "")) or ""),
                                model=str(ai_verdict.model),
                                mode=str(ai_verdict.mode),
                                confidence=float(ai_verdict.confidence or 0),
                                reason=str(ai_verdict.reason or ai_verdict.error or "")[:300],
                            )
                        except Exception:
                            pass
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

                    if special_native_cycle:
                        log_event(("multi_strategy_open_attempt" if multi_strategy_cycle else "knife_reversal_open_attempt" if str(getattr(plan, "strategy", "")).lower() == "knife_reversal" else "cascade_hunter_open_attempt" if str(getattr(plan, "strategy", "")).lower() == "cascade_hunter" else "orderflow_impulse_open_attempt"), stage="entry", ok=True, symbol=str(plan.symbol), side=str(plan.side), strategy=str(getattr(plan, "strategy", "")), live=bool(live), margin_pct=float(getattr(plan, "expected_margin_usdt", 0) or 0), leverage=int(float(getattr(plan, "leverage", 10) or 10)))
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
                        elif impulse_dump_cycle:
                            open_text = format_impulse_dump_opened(plan, placed)
                            log_event(
                                "impulse_dump_opened",
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
                            await notify_admin(app, open_text, key=f"impulse_dump_opened_{plan.symbol}_{int(time.time()*1000)}")
                            await impulse_dump_summary_message(app, settings, candidates, opened_note=open_text)
                        elif cascade_hunter_cycle:
                            log_event("cascade_hunter_opened", stage="entry", ok=True, symbol=str(plan.symbol), side=str(plan.side), entry_price=float(getattr(plan, "entry_price", 0) or 0), take_price=float(getattr(plan, "take_price", 0) or 0), stop_price=float(getattr(plan, "stop_price", 0) or 0), leverage=int(float(getattr(plan, "leverage", 0) or 0)), placed=placed)
                            await notify_admin(app, format_position_opened(plan, placed, live, ai_verdict if ai_enabled else None), key=f"cascade_hunter_opened_{plan.symbol}_{int(time.time()*1000)}")
                        elif multi_strategy_cycle:
                            open_text = format_multi_strategy_opened(plan, placed)
                            log_event("multi_strategy_opened", stage="entry", ok=True, symbol=str(plan.symbol), side=str(plan.side), strategy=str(getattr(plan, "strategy", "")), placed=placed)
                            await notify_admin(app, open_text, key=f"multi_strategy_opened_{plan.symbol}_{int(time.time()*1000)}")
                        elif knife_reversal_cycle or str(getattr(plan, "strategy", "")).lower() == "knife_reversal":
                            open_text = format_knife_reversal_opened(plan, placed)
                            log_event("knife_reversal_opened", stage="entry", ok=True, symbol=str(plan.symbol), side=str(plan.side), entry_price=float(getattr(plan, "entry_price", 0) or 0), take_price=float(getattr(plan, "take_price", 0) or 0), stop_price=float(getattr(plan, "stop_price", 0) or 0), leverage=int(float(getattr(plan, "leverage", 0) or 0)), placed=placed)
                            await notify_admin(app, open_text, key=f"knife_reversal_opened_{plan.symbol}_{int(time.time()*1000)}")
                        elif orderflow_impulse_cycle:
                            open_text = format_orderflow_impulse_opened(plan, placed)
                            _of_details = getattr(plan, "signal_details", {}) if hasattr(plan, "signal_details") else {}
                            _of_details = _of_details if isinstance(_of_details, dict) else {}
                            log_event(
                                "orderflow_impulse_opened",
                                stage="entry",
                                ok=True,
                                symbol=str(plan.symbol),
                                side=str(plan.side),
                                entry_price=float(getattr(plan, "entry_price", 0) or 0),
                                take_price=float(getattr(plan, "take_price", 0) or 0),
                                stop_price=float(getattr(plan, "stop_price", 0) or 0),
                                leverage=int(float(getattr(plan, "leverage", 0) or 0)),
                                tp_pct=_of_details.get("tp_pct"),
                                sl_pct=_of_details.get("sl_pct"),
                                futures_trend_15m_pct=_of_details.get("trend_15m_pct"),
                                futures_trend_1h_pct=_of_details.get("trend_1h_pct"),
                                futures_orderbook_imbalance=_of_details.get("orderbook_imbalance"),
                                futures_volume_ratio=_of_details.get("volume_ratio"),
                                spot_source=_of_details.get("spot_source"),
                                spot_move_pct=_of_details.get("spot_move_pct"),
                                spot_volume_ratio=_of_details.get("spot_volume_ratio"),
                                spot_delta_ratio=_of_details.get("spot_delta_ratio"),
                                spot_delta_usdt=_of_details.get("spot_delta_usdt"),
                                spot_orderbook_imbalance=_of_details.get("spot_orderbook_imbalance"),
                                placed=placed,
                            )
                            await notify_admin(app, open_text, key=f"orderflow_impulse_opened_{plan.symbol}_{int(time.time()*1000)}")
                            await orderflow_impulse_summary_message(app, settings, candidates, opened_note=open_text)
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
                        if impulse_dump_cycle:
                            log_event("impulse_dump_execution_rejected", stage="entry", ok=False, symbol=str(plan.symbol), side=str(plan.side), reason=reason[:500], placed=placed)
                        if orderflow_impulse_cycle:
                            log_event("orderflow_impulse_execution_rejected", stage="entry", ok=False, symbol=str(plan.symbol), side=str(plan.side), reason=reason[:500], placed=placed)
                        if cascade_hunter_cycle:
                            log_event("cascade_hunter_execution_rejected", stage="entry", ok=False, symbol=str(plan.symbol), side=str(plan.side), reason=reason[:500], placed=placed)
                        if 'protection' in placed or 'position closed' in reason.lower():
                            close = placed.get('close') or {}
                            pnl = close.get('pnl_usdt')
                            pp = close.get('pnl_pct')
                            msg = (
                                "🛑 Trade aborted after entry\n"
                                f"{plan.symbol} {plan.side}\n"
                                "Reason: exchange TP/SL not confirmed. Position stays open under virtual TP/SL monitor.\n"
                                f"PnL: {pnl:.4f} USDT ({pp:.3f}%)" if isinstance(pnl, (int, float)) and isinstance(pp, (int, float)) else
                                "🛑 Trade aborted after entry\n"
                                f"{plan.symbol} {plan.side}\n"
                                "Reason: exchange TP/SL not confirmed. Position stays open under virtual TP/SL monitor."
                            )
                            await notify_admin(app, msg, key=f"trade_aborted_{plan.symbol}")

                if candidates and not opened_this_cycle:
                    await update_scanner_status(app, settings, status="signal rejected", force=True)
                elif candidates:
                    await update_scanner_status(app, settings, status="scanning")

                _sleep_sec = int(settings.get("scan_interval_sec", 5) or 5)
                if orderflow_impulse_cycle:
                    _sleep_sec = int(settings.get("orderflow_impulse_scan_interval_sec", settings.get("scan_interval_sec", 60)) or 60)
                    # v0258: orderflow scans must respect the configured 60-second interval.
                    # If the user sets 1 minute, make a real 60s pause after each completed cycle.
                    _sleep_sec = max(60, _sleep_sec)
                await sleep_until_next_scan(app, _sleep_sec)
            except Exception as e:
                log.exception("trading loop error: %s", e)
                await asyncio.sleep(5)
    finally:
        trading_task = None

async def on_startup(app):
    await storage.init()
    try:
        ensure_runtime_secrets_loaded(await storage.all_settings())
        log_event("runtime_secrets_session_ready_v79", ok=True)
    except Exception as e:
        log_event("runtime_secrets_session_ready_failed_v79", ok=False, error=str(e)[:300])

    # V52: local positions are volatile cache only.  On deploy/restart wipe the
    # SQLite position cache so stale BTC AI TP/SL/order ids can never steer live
    # management.  MEXC open_positions + active planorders are the source of
    # truth after boot.  Trades/settings/API keys are intentionally preserved.
    try:
        cleared = await storage.clear_positions()
        log_event("startup_local_position_cache_cleared", ok=True, cleared=cleared, source="v52_exchange_source_of_truth")
        app.bot_data["startup_local_position_cache_cleared"] = cleared
    except Exception as e:
        log_event("startup_local_position_cache_clear_failed", ok=False, error=str(e)[:500])
        app.bot_data["startup_local_position_cache_clear_error"] = str(e)

    settings = await storage.all_settings()
    apply_mexc_runtime_env(settings)
    app.bot_data.setdefault("trading_start_lock", asyncio.Lock())
    app.bot_data.setdefault("scan_wakeup_event", asyncio.Event())

    # V52: Do NOT rebuild local rows through generic RecoveryEngine here.
    # RecoveryEngine can invent fallback TP/SL when the old local row is gone;
    # BTC AI must instead read the live exchange position and latest active
    # MEXC planorders directly.  /status_btc and BTC managers are exchange-first.
    app.bot_data["startup_recovery_report"] = {
        "mode": "disabled_v52_exchange_source_of_truth",
        "local_cache_cleared": app.bot_data.get("startup_local_position_cache_cleared", 0),
    }
    global claude_scheduler_task, monitor_cleanup_task
    try:
        if claude_scheduler_task is None or claude_scheduler_task.done():
            claude_scheduler_task = app.create_task(_claude_scheduler_loop(app))
            log_event("claude_autopilot_scheduler_started_v0402", ok=True)
    except Exception as e:
        log_event("claude_autopilot_scheduler_start_failed_v0402", ok=False, error=str(e)[:500])
    try:
        if monitor_cleanup_task is None or monitor_cleanup_task.done():
            monitor_cleanup_task = app.create_task(monitor_duplicate_cleanup_loop(app))
            log_event("monitor_duplicate_cleanup_started_v0430", ok=True, interval_sec=MONITOR_DUPLICATE_CLEANUP_SEC)
    except Exception as e:
        log_event("monitor_duplicate_cleanup_start_failed_v0430", ok=False, error=str(e)[:500])

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
    app.add_handler(CommandHandler("log_full", _wrap_command(log_full_cmd, "/log_full")))
    app.add_handler(CommandHandler("test_btc", _wrap_command(test_btc_cmd, "/test_btc")))
    app.add_handler(CommandHandler("game_btc_ai", _wrap_command(game_btc_ai_cmd, "/game_btc_ai")))
    app.add_handler(CommandHandler("run", _wrap_command(run_cmd, "/run")))
    app.add_handler(CommandHandler("boost_start", _wrap_command(boost_start_cmd, "/boost_start")))
    app.add_handler(CommandHandler("boost_stop", _wrap_command(boost_stop_cmd, "/boost_stop")))
    app.add_handler(CommandHandler("boost_status", _wrap_command(boost_status_cmd, "/boost_status")))
    app.add_handler(CommandHandler("chatgpt_scan", _wrap_command(chatgpt_scan_mode_cmd, "/chatgpt_scan")))
    app.add_handler(CommandHandler("scan_potok", _wrap_command(scan_potok_cmd, "/scan_potok")))
    app.add_handler(CommandHandler("claude_autopilot", _wrap_command(claude_autopilot_cmd, "/claude_autopilot")))
    app.add_handler(CommandHandler("claude_api", _wrap_command(claude_api_cmd, "/claude_api")))
    app.add_handler(CommandHandler("import_setup", _wrap_command(chatgpt_accept_setup_cmd, "/import_setup")))
    app.add_handler(CommandHandler("chatgpt_exit", _wrap_command(chatgpt_exit_mode_cmd, "/chatgpt_exit")))
    app.add_handler(CommandHandler("log_chatgpt", _wrap_command(log_chatgpt_cmd, "/log_chatgpt")))
    app.add_handler(CommandHandler("log_claude", _wrap_command(log_claude_cmd, "/log_claude")))
    app.add_handler(CommandHandler("cascade_hunter", _wrap_command(cascade_hunter_cmd, "/cascade_hunter")))
    app.add_handler(CommandHandler("boost_rotation", _wrap_command(boost_rotation_cmd, "/boost_rotation")))
    app.add_handler(CommandHandler("boost_list", _wrap_command(boost_list_cmd, "/boost_list")))
    app.add_handler(CommandHandler("boost_list_del", _wrap_command(boost_list_del_cmd, "/boost_list_del")))
    app.add_handler(CommandHandler("stop", _wrap_command(stop_cmd, "/stop")))
    # Emergency command aliases. Telegram treats "/Close all" as command
    # "Close" with arg "all", and unmatched slash commands never reach the
    # reply-keyboard text_router because it excludes filters.COMMAND.  Operators
    # often type/click these variants, so wire them directly to the real actions.
    app.add_handler(CommandHandler(["panic", "Panic", "PANIC"], _wrap_command(panic_cmd, "/panic")))
    app.add_handler(CommandHandler("status", _wrap_command(status_cmd, "/status")))
    app.add_handler(CommandHandler("status_btc", _wrap_command(status_btc_cmd, "/status_btc")))
    app.add_handler(CommandHandler("backtest_btc_patterns", _wrap_command(backtest_btc_patterns_cmd, "/backtest_btc_patterns")))
    app.add_handler(CommandHandler("backtest_btc_patterns_1h", _wrap_command(backtest_btc_patterns_1h_cmd, "/backtest_btc_patterns_1h")))
    app.add_handler(CommandHandler("backtest_round_levels", _wrap_command(backtest_round_levels_cmd, "/backtest_round_levels")))
    app.add_handler(CommandHandler("backtest_strategy_lab", _wrap_command(backtest_strategy_lab_cmd, "/backtest_strategy_lab")))
    app.add_handler(CommandHandler("backtest_strategy_lab_extra", _wrap_command(backtest_strategy_lab_extra_cmd, "/backtest_strategy_lab_extra")))
    app.add_handler(CommandHandler("backtest_aggressive_lab", _wrap_command(backtest_aggressive_lab_cmd, "/backtest_aggressive_lab")))
    app.add_handler(CommandHandler("clean_btc_orders", _wrap_command(clean_btc_orders_cmd, "/clean_btc_orders")))
    app.add_handler(CommandHandler("ping", _wrap_command(ping_cmd, "/ping")))
    app.add_handler(CommandHandler("balance", _wrap_command(balance_cmd, "/balance")))
    app.add_handler(CommandHandler("positions", _wrap_command(positions_cmd, "/positions")))
    app.add_handler(CommandHandler("mexc_debug_state", _wrap_command(mexc_debug_state_cmd, "/mexc_debug_state")))  # legacy test marker: CommandHandler("mexc_debug_state", mexc_debug_state_cmd)
    app.add_handler(CommandHandler("open_orders", _wrap_command(open_orders_cmd, "/open_orders")))
    app.add_handler(CommandHandler(["cancel_all", "cancel", "Cancel", "CANCEL", "cansel", "Cansel", "CANSEL", "cancelall", "CancelAll", "canselall", "CanselAll"], _wrap_command(cancel_all_cmd, "/cancel_all")))
    app.add_handler(CommandHandler(["close_all", "close", "Close", "CLOSE", "closeall", "CloseAll"], _wrap_command(close_all_cmd, "/close_all")))
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
    app.add_handler(MessageHandler(filters.Document.ALL, document_router))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))
    return app

if __name__ == "__main__":
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN is required")
    build_app().run_polling()
