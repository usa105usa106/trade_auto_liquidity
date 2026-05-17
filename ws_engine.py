import asyncio
import json
import logging
import os
import random
import time
from dataclasses import dataclass, field
from typing import Any

import aiohttp

log = logging.getLogger("ws")

@dataclass
class WSStatus:
    enabled: bool = True
    running: bool = False
    connected: bool = False
    reconnects: int = 0
    last_message_ts: float = 0.0
    last_connect_ts: float = 0.0
    last_error: str = ""
    subscribed: str = "binance_futures_miniticker"
    stale_sec: int = 10

    def age(self) -> float:
        if not self.last_message_ts:
            return 10**9
        return max(0.0, time.time() - self.last_message_ts)

    def healthy(self) -> bool:
        if not self.enabled:
            return True
        return self.running and self.connected and self.age() <= self.stale_sec

    def as_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "running": self.running,
            "connected": self.connected,
            "healthy": self.healthy(),
            "reconnects": self.reconnects,
            "last_message_age_sec": round(self.age(), 2),
            "last_error": self.last_error,
            "subscribed": self.subscribed,
        }

class WebSocketSupervisor:
    """
    Hardened public WebSocket supervisor.

    Features:
    - auto reconnect with exponential backoff + jitter
    - heartbeat / timeout protection
    - stale-data detection
    - resubscribe by reconnecting to combined stream URL
    - shared ticker cache for scanner/position manager
    - fail-safe health status for trading loop

    Uses Binance USD-M Futures miniTicker-all stream as the light market radar.
    Heavy OHLCV/orderbook checks still happen only for hot candidates.
    """

    def __init__(self, proxy_url: str = "", proxy_enabled: bool = False, enabled: bool = True):
        self.url = os.getenv("WS_BINANCE_FUTURES_URL", "wss://fstream.binance.com/ws/!miniTicker@arr")
        self.proxy_url = proxy_url if proxy_enabled else ""
        self.status = WSStatus(enabled=enabled, stale_sec=int(os.getenv("WS_STALE_SEC", "10")))
        self.ping_interval = int(os.getenv("WS_PING_INTERVAL_SEC", "15"))
        self.reconnect_base = float(os.getenv("WS_RECONNECT_BASE_SEC", "1"))
        self.reconnect_max = float(os.getenv("WS_RECONNECT_MAX_SEC", "30"))
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._tickers: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        if not self.status.enabled:
            return
        if self._task and not self._task.done():
            return
        self._stop.clear()
        self.status.running = True
        self._task = asyncio.create_task(self._run(), name="ws_supervisor")

    async def stop(self) -> None:
        self._stop.set()
        self.status.running = False
        self.status.connected = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        backoff = self.reconnect_base
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=20, sock_read=max(30, self.ping_interval * 3))
        while not self._stop.is_set():
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    kwargs = {"heartbeat": self.ping_interval, "autoping": True}
                    if self.proxy_url:
                        kwargs["proxy"] = self.proxy_url
                    async with session.ws_connect(self.url, **kwargs) as ws:
                        self.status.connected = True
                        self.status.running = True
                        self.status.last_connect_ts = time.time()
                        self.status.last_error = ""
                        backoff = self.reconnect_base
                        async for msg in ws:
                            if self._stop.is_set():
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                self.status.last_message_ts = time.time()
                                await self._handle_message(msg.data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                raise RuntimeError(f"websocket closed/error: {msg.type}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.status.connected = False
                self.status.reconnects += 1
                self.status.last_error = str(e)[:240]
                sleep_for = min(self.reconnect_max, backoff) + random.uniform(0, 0.25)
                log.warning("WS reconnect in %.2fs after error: %s", sleep_for, e)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=sleep_for)
                except asyncio.TimeoutError:
                    pass
                backoff = min(self.reconnect_max, backoff * 2)
        self.status.connected = False
        self.status.running = False

    async def _handle_message(self, raw: str) -> None:
        data = json.loads(raw)
        # Binance !miniTicker@arr emits list of mini tickers.
        if isinstance(data, list):
            async with self._lock:
                for item in data:
                    symbol = self._normalize_symbol(str(item.get("s", "")))
                    if not symbol:
                        continue
                    last = float(item.get("c") or 0)
                    quote_volume = float(item.get("q") or 0)
                    open_price = float(item.get("o") or 0)
                    pct = ((last - open_price) / open_price * 100.0) if open_price else 0.0
                    self._tickers[symbol] = {
                        "symbol": symbol,
                        "last": last,
                        "quoteVolume": quote_volume,
                        "percentage": pct,
                        "ts": time.time(),
                    }
        elif isinstance(data, dict):
            item = data.get("data", data)
            symbol = self._normalize_symbol(str(item.get("s", "")))
            if symbol:
                async with self._lock:
                    last = float(item.get("c") or item.get("p") or 0)
                    self._tickers[symbol] = {"symbol": symbol, "last": last, "ts": time.time()}

    def _normalize_symbol(self, raw: str) -> str:
        if not raw or not raw.endswith("USDT"):
            return ""
        base = raw[:-4]
        return f"{base}/USDT"

    async def ticker(self, symbol: str, max_age_sec: float | None = None) -> dict[str, Any] | None:
        async with self._lock:
            t = dict(self._tickers.get(symbol) or {})
        if not t:
            return None
        if max_age_sec is not None and time.time() - float(t.get("ts", 0)) > max_age_sec:
            return None
        return t

    async def tickers(self, max_age_sec: float | None = None) -> dict[str, dict[str, Any]]:
        async with self._lock:
            items = {k: dict(v) for k, v in self._tickers.items()}
        if max_age_sec is not None:
            now = time.time()
            items = {k: v for k, v in items.items() if now - float(v.get("ts", 0)) <= max_age_sec}
        return items

    def healthy(self) -> bool:
        return self.status.healthy()

    def status_text(self) -> str:
        st = self.status.as_dict()
        return (
            f"WS enabled: {st['enabled']}\n"
            f"WS running: {st['running']}\n"
            f"WS connected: {st['connected']}\n"
            f"WS healthy: {st['healthy']}\n"
            f"WS last msg age: {st['last_message_age_sec']}s\n"
            f"WS reconnects: {st['reconnects']}\n"
            f"WS last error: {st['last_error'] or '-'}"
        )
