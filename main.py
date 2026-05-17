import os, time, asyncio, logging
import aiohttp
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
import psutil

from config import TELEGRAM_TOKEN, ADMIN_IDS, VERSION, DEFAULT_EXCHANGE
from storage import Storage
from keyboard import MAIN_MENU, settings_menu, choices_menu, api_menu
from adaptive_engine import AdaptiveEngine
from mirror_engine import MirrorEngine
from session_engine import SessionEngine
from spot_confirmation_engine import SpotConfirmationEngine
from risk_engine import RiskEngine
from exchange_client import ExchangeClient
from execution_engine import ExecutionEngine
from sync_engine import SyncEngine
from scanner import Scanner
from production_gate import ProductionGate
from ws_engine import WebSocketSupervisor, futures_source_from_mode
from trade_planner import TradePlanner
from position_manager import PositionManager

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bot")

storage = Storage()
scanner = Scanner()
running = False
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

def _scan_status_text(settings: dict, status: str = "scanning", last_signal: str | None = None, last_decision: str | None = None) -> str:
    last_signal = last_signal or scanner.last_signal_summary or "-"
    last_decision = last_decision or scanner.last_reject_reason or "-"
    return (
        "🔎 Scanner live status\n"
        f"Status: {status}\n"
        f"Фьючи/Спот: {settings.get('scan_market_source', 'mexc_binance')}\n"
        f"Source: {scanner.last_scan_source}\n"
        f"Universe: {settings.get('universe_mode')} | requested: {scanner.last_requested_symbols}\n"
        f"Markets: total={scanner.last_total_markets}, available={scanner.last_available_markets}, filtered={scanner.last_filtered_markets}, loaded={len(scanner.hot_symbols)}\n"
        f"Concurrency: {scanner.last_concurrency} | scanned={scanner.last_cycle_scanned} | errors={scanner.last_cycle_errors} | slowdown={scanner.last_slowdown_sec}s\n"
        f"WS: {('OFF' if not settings.get('ws_enabled', True) else ('healthy' if (ws_supervisor and ws_supervisor.healthy()) else 'REST fallback'))}"
        f" | pending={getattr(getattr(ws_supervisor, 'status', None), 'pending_updates', 0)}"
        f" | dropped={getattr(getattr(ws_supervisor, 'status', None), 'dropped_updates', 0)}\n"
        f"Regime: {scanner.last_regime.get('regime')}\n"
        f"Last signal: {last_signal}\n"
        f"Last decision: {last_decision}\n"
        f"Error: {scanner.last_refresh_error or '-'}\n"
        f"Updated: {time.strftime('%H:%M:%S')}"
    )

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

def _api_creds(settings: dict) -> tuple[str, str]:
    # Telegram-saved credentials have priority. Environment variables remain a fallback
    # for server-side deployment. Secrets are never printed back to chat.
    api_key = str(settings.get("mexc_api_key") or os.getenv("MEXC_API_KEY", "") or "").strip()
    api_secret = str(settings.get("mexc_api_secret") or os.getenv("MEXC_API_SECRET", "") or "").strip()
    return api_key, api_secret


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
    ws_max_updates_per_batch = int(settings.get("ws_max_updates_per_batch", os.getenv("WS_MAX_UPDATES_PER_BATCH", "250")) or 250)
    ws_queue_limit = int(settings.get("ws_queue_limit", os.getenv("WS_QUEUE_LIMIT", "2000")) or 2000)
    ws_adaptive_slowdown_threshold = int(settings.get("ws_adaptive_slowdown_threshold", os.getenv("WS_ADAPTIVE_SLOWDOWN_THRESHOLD", "1000")) or 1000)
    desired_signature = (enabled, venue, proxy_enabled, proxy_url, ws_update_throttle_ms, ws_max_updates_per_batch, ws_queue_limit, ws_adaptive_slowdown_threshold)
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
    )
    ws_supervisor._bot_signature = desired_signature
    return ws_supervisor

async def reply(update: Update, text: str, **kwargs):
    if update.message:
        await update.message.reply_text(text, **kwargs)
    elif update.callback_query:
        await update.callback_query.message.reply_text(text, **kwargs)

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
/ping - отклик, RAM, uptime
/balance - futures balance + IP/proxy
/positions - открытые позиции
/stats - статистика сделок
/sync - синхронизация позиций/ордеров
/proxy on|off|test|set URL
/api status|set KEY SECRET|clear|test - API биржи через чат
/set key value - ручная настройка

Ключевые настройки:
live_trading, risk_pct, max_open_positions, scan_interval_sec, scanner_concurrency,
ws_update_throttle_ms, ws_max_updates_per_batch, ws_queue_limit,
symbol_refresh_sec, universe_mode, strategy_mode, mirror_mode,
spot_confirmation_enabled, session_filter_enabled, america_short_bias_enabled, ws_enabled,
scan_market_source = binance_binance | mexc_mexc | mexc_binance.

По умолчанию: mexc_binance = MEXC фьючи скан + Binance spot подтверждение.
""".strip(), reply_markup=MAIN_MENU)

async def run_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global running, trading_task
    if not allowed(update): return
    lock = context.application.bot_data.get("trading_start_lock")
    if lock is None:
        lock = asyncio.Lock()
        context.application.bot_data["trading_start_lock"] = lock
    async with lock:
        if trading_task and not trading_task.done():
            running = True
            await reply(update, "🟢 Bot already running\nExisting scanner/execution loop is active. New settings will apply on the next scan cycle.", reply_markup=MAIN_MENU)
            return
        running = True
        trading_task = context.application.create_task(trading_loop(context.application))
    await reply(update, "🟢 Bot started\nScanner/execution loop enabled.", reply_markup=MAIN_MENU)

async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global running
    if not allowed(update): return
    running = False
    await reply(update, "🟡 Trading stopped\nOpen positions still managed.", reply_markup=MAIN_MENU)

async def panic_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global running
    if not allowed(update): return
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

    text = (
        "🚨 PANIC MODE\n"
        "Trading disabled. Close workflow executed.\n"
        f"Tracked positions closed: {closed_local}\n"
        f"Exchange-only positions closed: {closed_external}"
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
Live: {s.get('live_trading')}
Strategy: {s.get('strategy_mode')}
Universe: {s.get('universe_mode')}
Risk: {float(s.get('risk_pct',0))*100:.2f}%
Max positions: {s.get('max_open_positions')}
Scan: {s.get('scan_interval_sec')}s
Concurrency: {s.get('scanner_concurrency', 5)} | last scanned={scanner.last_cycle_scanned} | errors={scanner.last_cycle_errors} | slowdown={scanner.last_slowdown_sec}s
Refresh: {s.get('symbol_refresh_sec')}s
Mirror: {s.get('mirror_mode')}
Spot confirmation: {s.get('spot_confirmation_enabled')}
Фьючи/Спот source: {s.get('scan_market_source', 'mexc_binance')}
Session filter: {s.get('session_filter_enabled')}
America short bias: {s.get('america_short_bias_enabled')}
Open positions: {len(positions)}
Revision: {s.get('settings_revision')}
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
    proc = psutil.Process()
    ram = proc.memory_info().rss / 1024 / 1024
    uptime = int(time.time() - started_at)
    await reply(update, f"🏓 Pong\nVersion: {VERSION}\nRAM: {ram:.1f} MB\nUptime: {uptime}s", reply_markup=MAIN_MENU)

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
    except Exception as e:
        balance_error = str(e)[:240]

    proxy_line = f"{proxy_ip.get('ip')}" if proxy_ip.get("ok") else f"{proxy_ip.get('ip')} ({proxy_ip.get('error')})"
    direct_line = f"{direct_ip.get('ip')}" if direct_ip.get("ok") else f"{direct_ip.get('ip')} ({direct_ip.get('error')})"
    text = (
        "💰 Futures Balance\n"
        f"USDT free: {free}\n"
        f"USDT total: {total}\n"
        f"Balance error: {balance_error or '-'}\n\n"
        "🌍 IP diagnostics\n"
        f"Direct IP: {direct_line}\n"
        f"Proxy enabled: {proxy_enabled}\n"
        f"Proxy IP: {proxy_line}"
    )
    await reply(update, text, reply_markup=MAIN_MENU)

async def positions_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    ps = await storage.positions()
    if not ps:
        await reply(update, "📈 Positions: none", reply_markup=MAIN_MENU); return
    lines = ["📈 Positions"]
    for p in ps:
        lines.append(f"{p.get('symbol')} {p.get('side')} {p.get('status')} entry={p.get('entry_price')} SL={p.get('stop_price')} TP={p.get('take_price')}")
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

async def sync_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    s = await storage.all_settings()
    try:
        ex = await get_exchange(s)
        report = await SyncEngine(storage, ex).sync()
        await reply(update, "🔄 Sync\n" + "\n".join(f"{k}: {v}" for k,v in report.items()), reply_markup=MAIN_MENU)
    except Exception as e:
        await reply(update, f"🔄 Sync failed: {e}", reply_markup=MAIN_MENU)

async def settings_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    s = await storage.all_settings()
    rev = int(s.get("settings_revision", 1))
    await reply(update, "⚙️ Settings", reply_markup=settings_menu(rev, s))

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
        "ws_enabled", "ws_require_healthy_for_entries", "ws_stale_sec", "ws_update_throttle_ms", "ws_max_updates_per_batch", "ws_queue_limit", "ws_adaptive_slowdown_threshold",
    }
    if key not in allowed_keys:
        await reply(update, f"❌ Setting is not allowed through /set: {key}", reply_markup=MAIN_MENU)
        return
    if value.lower() in {"true","false","on","off"}:
        parsed = value.lower() in {"true","on"}
    else:
        try: parsed = float(value) if "." in value else int(value)
        except Exception: parsed = value
    await storage.set(key, parsed)
    if key in {"mexc_api_key", "mexc_api_secret", "proxy_url", "proxy_enabled"}:
        await reset_exchange()
    if key in {"proxy_url", "proxy_enabled", "scan_market_source", "ws_enabled", "ws_stale_sec", "ws_update_throttle_ms", "ws_max_updates_per_batch", "ws_queue_limit", "ws_adaptive_slowdown_threshold"}:
        await reset_market_runtime()
    await reply(update, f"✅ Saved\n{key} = {parsed}", reply_markup=MAIN_MENU)

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

async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    text = update.message.text
    mapping = {
        "▶️ Run": run_cmd, "⏹ Stop": stop_cmd, "📊 Status": status_cmd, "🚨 Panic": panic_cmd,
        "📈 Positions": positions_cmd, "📉 Stats": stats_cmd, "💰 Balance": balance_cmd,
        "🏓 Ping": ping_cmd, "⚙️ Settings": settings_cmd, "🔐 API": api_cmd,
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
        new_settings = await storage.all_settings()
        new_rev = int(new_settings.get("settings_revision", current_rev + 1))
        # Stay inside the same submenu so the selected value is immediately visible with ✅.
        if key == "universe_mode":
            await q.edit_message_text("🌐 Universe", reply_markup=choices_menu("universe_mode", [("Top-50","top-50"),("Top-100","top-100"),("Top-200","top-200"),("Top-300","top-300"),("Adaptive","adaptive")], new_rev, new_settings.get("universe_mode")))
        elif key == "strategy_mode":
            await q.edit_message_text("📈 Strategy", reply_markup=choices_menu("strategy_mode", [("Momentum","momentum"),("Pullback","pullback"),("Reversal","reversal"),("Hybrid","hybrid")], new_rev, new_settings.get("strategy_mode")))
        elif key == "scan_market_source":
            await q.edit_message_text("📡 Фьючи | Спот", reply_markup=choices_menu("scan_market_source", [("Binance фьючи + Binance спот","binance_binance"),("MEXC фьючи + MEXC спот","mexc_mexc"),("MEXC фьючи + Binance спот","mexc_binance")], new_rev, new_settings.get("scan_market_source", "mexc_binance")))
        elif key == "scan_interval_sec":
            await q.edit_message_text("⏱ Scan speed", reply_markup=choices_menu("scan_interval_sec", [("1s","1"),("2s","2"),("3s","3"),("5s","5"),("10s","10")], new_rev, new_settings.get("scan_interval_sec")))
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
            await q.edit_message_text("📈 Strategy", reply_markup=choices_menu("strategy_mode", [("Momentum","momentum"),("Pullback","pullback"),("Reversal","reversal"),("Hybrid","hybrid")], rev, s.get("strategy_mode")))
        elif name == "marketsource":
            await q.edit_message_text("📡 Фьючи | Спот", reply_markup=choices_menu("scan_market_source", [("Binance фьючи + Binance спот","binance_binance"),("MEXC фьючи + MEXC спот","mexc_mexc"),("MEXC фьючи + Binance спот","mexc_binance")], rev, s.get("scan_market_source", "mexc_binance")))
        elif name == "scan":
            await q.edit_message_text("⏱ Scan speed", reply_markup=choices_menu("scan_interval_sec", [("1s","1"),("2s","2"),("3s","3"),("5s","5"),("10s","10")], rev, s.get("scan_interval_sec")))
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
    global running, trading_task
    try:
        while running:
            try:
                settings = await storage.all_settings()
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
                        await notify_admin(app, f"📌 {ev['type']} {ev['symbol']}", key="position_event")

                # 2) Refresh symbol universe. Normal scanner progress is kept in one
                # editable live-status message to avoid chat spam.
                if time.time() - scanner.last_refresh > int(settings.get("symbol_refresh_sec", 300)):
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

                # 3) Risk gate for NEW entries only. Use real account equity where available.
                risk = RiskEngine(storage)
                equity = await account_equity_usdt(ex, float(os.getenv("DEFAULT_EQUITY_USDT", "1000")))
                ok, reason = await risk.allow_new_trades(settings, equity=equity)
                if not ok:
                    scanner.last_reject_reason = f"risk blocked: {reason}"
                    await update_scanner_status(app, settings, status="risk paused", force=True)
                    await notify_admin(app, f"🛑 Новые входы на паузе: {reason}", min_interval_sec=300, key="risk_paused")
                    await asyncio.sleep(int(settings.get("scan_interval_sec", 3)))
                    continue

                # 4) Infrastructure gate for NEW entries only.
                ws_enabled = bool(settings.get("ws_enabled", True))
                # Binance WS is optional market radar. If it is stale but scanner
                # successfully refreshed symbols from MEXC REST, market data is still usable.
                ws_healthy = (not ws_enabled) or ws.healthy() or str(scanner.last_scan_source).endswith("_rest")
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
                    await notify_admin(app, f"⚠️ Входы заблокированы: {gate_reason}", min_interval_sec=300, key="gate_blocked")
                    await asyncio.sleep(int(settings.get("scan_interval_sec", 3)))
                    continue

                # 5) Candidate pipeline: detect market regime -> choose effective strategy ->
                # scan futures signals -> mirror -> session -> spot -> filters -> plan -> execute.
                trades = await storage.trade_rows()
                adaptive = AdaptiveEngine()
                adaptive_stats = adaptive.calc_stats(trades)
                regime_info = await scanner.detect_regime(ex, settings)
                effective_strategy = adaptive.choose_strategy(
                    base_mode=str(settings.get("strategy_mode", "hybrid")),
                    trades=trades,
                    regime=str(regime_info.get("regime", "LOW_VOLATILITY")),
                    enabled=bool(settings.get("auto_strategy_adaptation", True)),
                )
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
                    scanner.last_signal_summary = f"{top.get('symbol')} {top.get('side')} conf={top.get('confidence')} strategy={effective_strategy} count={len(candidates)}"
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

                    spot_data = await fetch_spot_data_for_candidate(ex, cand, settings) if bool(settings.get("spot_confirmation_enabled", True)) else None
                    cand = SpotConfirmationEngine(enabled=bool(settings.get("spot_confirmation_enabled", True))).apply(cand, spot_data)

                    if not cand.get("allowed_by_session", True):
                        scanner.last_reject_reason = f"{original_symbol}: session filter blocked"
                        continue
                    if bool(settings.get("spot_confirmation_enabled", True)) and not cand.get("spot_confirmed", True):
                        scanner.last_reject_reason = f"{original_symbol}: spot confirmation failed"
                        continue
                    mf_ok, mf_reason = risk.market_filters(cand, settings)
                    if not mf_ok:
                        scanner.last_reject_reason = f"{original_symbol}: market filter blocked: {mf_reason}"
                        continue

                    plan = TradePlanner().make_plan(cand, settings, equity_usdt=equity)
                    if not plan:
                        scanner.last_reject_reason = f"{original_symbol}: planner returned no trade"
                        continue

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
                            (
                                f"🟢 Position opened\n{plan.symbol} {plan.side}\n"
                                f"Strategy: {plan.strategy}\nEntry: {plan.entry_price:.8f}\n"
                                f"SL: {plan.stop_price:.8f}\nTP: {plan.take_price:.8f}\n"
                                f"Qty: {plan.qty:.6f}\nLive: {live}"
                            ),
                            key="position_opened",
                        )
                    else:
                        scanner.last_reject_reason = f"{plan.symbol}: execution rejected: {placed.get('reason', 'unknown')}"

                if candidates and not opened_this_cycle:
                    await update_scanner_status(app, settings, status="signal rejected", force=True)
                elif candidates:
                    await update_scanner_status(app, settings, status="scanning")

                await asyncio.sleep(int(settings.get("scan_interval_sec", 3)))
            except Exception as e:
                log.exception("trading loop error: %s", e)
                await asyncio.sleep(5)
    finally:
        trading_task = None

async def on_startup(app):
    await storage.init()
    app.bot_data.setdefault("trading_start_lock", asyncio.Lock())

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
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("sync", sync_cmd))
    app.add_handler(CommandHandler("settings", settings_cmd))
    app.add_handler(CommandHandler("set", set_cmd))
    app.add_handler(CommandHandler("proxy", proxy_cmd))
    app.add_handler(CommandHandler("api", api_cmd))
    app.add_handler(CallbackQueryHandler(callback_router))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))
    return app

if __name__ == "__main__":
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN is required")
    build_app().run_polling()
