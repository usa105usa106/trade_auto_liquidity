from __future__ import annotations
import time
import json
import aiosqlite
from typing import Any, Optional
from config import DB_PATH, DEFAULTS

DEFAULT_SETTINGS = {
    "live_trading": DEFAULTS.live_trading,
    "universe_mode": DEFAULTS.universe_mode,
    "max_symbols": DEFAULTS.max_symbols,
    "scan_interval_sec": DEFAULTS.scan_interval_sec,
    "symbol_refresh_sec": DEFAULTS.symbol_refresh_sec,
    "max_open_positions": DEFAULTS.max_open_positions,
    "risk_pct": DEFAULTS.risk_pct,
    "strategy_mode": DEFAULTS.strategy_mode,
    "auto_strategy_adaptation": DEFAULTS.auto_strategy_adaptation,
    "regime_adaptation": DEFAULTS.regime_adaptation,
    "mirror_mode": DEFAULTS.mirror_mode,
    "spot_confirmation_enabled": DEFAULTS.spot_confirmation_enabled,
    "session_filter_enabled": DEFAULTS.session_filter_enabled,
    "america_short_bias_enabled": DEFAULTS.america_short_bias_enabled,
    "max_spread_pct": DEFAULTS.max_spread_pct,
    "max_slippage_pct": DEFAULTS.max_slippage_pct,
    "min_depth_usdt": DEFAULTS.min_depth_usdt,
    "max_daily_loss_pct": DEFAULTS.max_daily_loss_pct,
    "max_consecutive_losses": DEFAULTS.max_consecutive_losses,
    "cooldown_after_close_sec": DEFAULTS.cooldown_after_close_sec,
    "limit_timeout_sec": DEFAULTS.limit_timeout_sec,
    "proxy_enabled": DEFAULTS.proxy_enabled,
    "proxy_url": DEFAULTS.proxy_url,
    "mexc_api_key": "",
    "mexc_api_secret": "",
    "websocket_enabled": True,
    "production_gate_enabled": True,

    "ws_enabled": True,
    "ws_require_healthy_for_entries": True,
    "ws_stale_sec": 10,
    "settings_revision": 1,
}

class Storage:
    def __init__(self, path: str = DB_PATH):
        self.path = path

    async def init(self) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
            """)
            await db.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_open REAL,
                ts_close REAL,
                symbol TEXT,
                side TEXT,
                strategy TEXT,
                mode TEXT,
                entry_price REAL,
                exit_price REAL,
                qty REAL,
                pnl_usdt REAL,
                pnl_pct REAL,
                result TEXT,
                reason TEXT,
                mirror_used INTEGER DEFAULT 0,
                session TEXT,
                raw TEXT
            )
            """)
            await db.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                symbol TEXT PRIMARY KEY,
                side TEXT,
                status TEXT,
                entry_price REAL,
                qty REAL,
                stop_price REAL,
                take_price REAL,
                strategy TEXT,
                order_id TEXT,
                tp_order_id TEXT,
                sl_order_id TEXT,
                opened_at REAL,
                updated_at REAL,
                raw TEXT
            )
            """)
            await db.execute("""
            CREATE TABLE IF NOT EXISTS locks (
                symbol TEXT PRIMARY KEY,
                locked_until REAL,
                reason TEXT
            )
            """)
            await db.commit()
        for k, v in DEFAULT_SETTINGS.items():
            if await self.get(k) is None:
                await self.set(k, v, bump_revision=False)

    async def get(self, key: str, default: Any = None) -> Any:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT value FROM settings WHERE key=?", (key,))
            row = await cur.fetchone()
            if not row:
                return default
            try:
                return json.loads(row[0])
            except Exception:
                return row[0]

    async def set(self, key: str, value: Any, bump_revision: bool = True) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO settings(key,value,updated_at) VALUES(?,?,?)",
                (key, json.dumps(value), time.time()),
            )
            if bump_revision and key != "settings_revision":
                rev = int(await self.get("settings_revision", 1) or 1) + 1
                await db.execute(
                    "INSERT OR REPLACE INTO settings(key,value,updated_at) VALUES(?,?,?)",
                    ("settings_revision", json.dumps(rev), time.time()),
                )
            await db.commit()

    async def all_settings(self) -> dict:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT key,value FROM settings")
            rows = await cur.fetchall()
        out = {}
        for k, v in rows:
            try: out[k] = json.loads(v)
            except Exception: out[k] = v
        return out

    async def upsert_position(self, pos: dict) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
            INSERT OR REPLACE INTO positions(symbol,side,status,entry_price,qty,stop_price,take_price,strategy,order_id,tp_order_id,sl_order_id,opened_at,updated_at,raw)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                pos["symbol"], pos.get("side"), pos.get("status", "open"), pos.get("entry_price"),
                pos.get("qty"), pos.get("stop_price"), pos.get("take_price"), pos.get("strategy"),
                pos.get("order_id"), pos.get("tp_order_id"), pos.get("sl_order_id"),
                pos.get("opened_at", time.time()), time.time(), json.dumps(pos),
            ))
            await db.commit()

    async def remove_position(self, symbol: str) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("DELETE FROM positions WHERE symbol=?", (symbol,))
            await db.commit()

    async def positions(self) -> list[dict]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT raw FROM positions")
            rows = await cur.fetchall()
        return [json.loads(r[0]) for r in rows if r and r[0]]

    async def position_symbols(self) -> set[str]:
        return {p["symbol"] for p in await self.positions()}

    async def add_trade(self, trade: dict) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
            INSERT INTO trades(ts_open,ts_close,symbol,side,strategy,mode,entry_price,exit_price,qty,pnl_usdt,pnl_pct,result,reason,mirror_used,session,raw)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                trade.get("ts_open"), trade.get("ts_close", time.time()), trade.get("symbol"),
                trade.get("side"), trade.get("strategy"), trade.get("mode"),
                trade.get("entry_price"), trade.get("exit_price"), trade.get("qty"),
                trade.get("pnl_usdt"), trade.get("pnl_pct"), trade.get("result"),
                trade.get("reason"), 1 if trade.get("mirror_used") else 0, trade.get("session"),
                json.dumps(trade),
            ))
            await db.commit()

    async def trade_rows(self, since: float | None = None) -> list[dict]:
        q = "SELECT raw FROM trades"
        params = ()
        if since:
            q += " WHERE ts_close>=?"
            params = (since,)
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(q, params)
            rows = await cur.fetchall()
        return [json.loads(r[0]) for r in rows if r and r[0]]

    async def set_lock(self, symbol: str, seconds: int, reason: str) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("INSERT OR REPLACE INTO locks(symbol,locked_until,reason) VALUES(?,?,?)", (symbol, time.time()+seconds, reason))
            await db.commit()

    async def is_locked(self, symbol: str) -> tuple[bool, str]:
        now = time.time()
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT locked_until,reason FROM locks WHERE symbol=?", (symbol,))
            row = await cur.fetchone()
            if not row:
                return False, ""
            if row[0] <= now:
                await db.execute("DELETE FROM locks WHERE symbol=?", (symbol,))
                await db.commit()
                return False, ""
            return True, row[1] or "locked"
