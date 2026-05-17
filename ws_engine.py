import asyncio
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import aiohttp

try:
    from aiohttp_socks import ProxyConnector
except Exception:  # pragma: no cover - optional dependency guard
    ProxyConnector = None

log = logging.getLogger("ws")


def futures_source_from_mode(mode: str) -> str:
    mode = str(mode or "mexc_binance").lower()
    return "binance" if mode.startswith("binance") else "mexc"


@dataclass
class WSStatus:
    enabled: bool = True
    running: bool = False
    connected: bool = False
    reconnects: int = 0
    last_message_ts: float = 0.0
    last_connect_ts: float = 0.0
    last_error: str = ""
    subscribed: str = ""
    venue: str = "mexc"
    stale_sec: int = 20
    processed_messages: int = 0
    dropped_updates: int = 0
    pending_updates: int = 0
    update_throttle_ms: int = 500
    max_updates_per_batch: int = 250
    queue_limit: int = 2000
    adaptive_slowdown_ms: int = 0

    def age(self) -> float | None:
        if not self.last_message_ts:
            return None
        return max(0.0, time.time() - self.last_message_ts)

    def healthy(self) -> bool:
        if not self.enabled:
            return True
        age = self.age()
        return self.running and self.connected and age is not None and age <= self.stale_sec

    def as_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "running": self.running,
            "connected": self.connected,
            "healthy": self.healthy(),
            "reconnects": self.reconnects,
            "last_message_age_sec": None if self.age() is None else round(self.age(), 2),
            "last_error": self.last_error,
            "subscribed": self.subscribed,
            "venue": self.venue,
            "processed_messages": self.processed_messages,
            "dropped_updates": self.dropped_updates,
            "pending_updates": self.pending_updates,
            "update_throttle_ms": self.update_throttle_ms,
            "max_updates_per_batch": self.max_updates_per_batch,
            "queue_limit": self.queue_limit,
            "adaptive_slowdown_ms": self.adaptive_slowdown_ms,
        }


class WebSocketSupervisor:
    """Public futures WebSocket supervisor used as a fast ticker radar.

    Supported venues:
    - mexc futures: wss://contract.mexc.com/edge, sub.tickers
    - binance USD-M futures: wss://fstream.binance.com/ws/!miniTicker@arr

    HTTP proxies are passed directly to aiohttp. SOCKS proxies require
    aiohttp-socks and are attached through ProxyConnector, which fixes the
    earlier "Expected HTTP" error when proxy_url starts with socks5://.
    """

    def __init__(self, proxy_url: str = "", proxy_enabled: bool = False, enabled: bool = True, venue: str = "mexc", update_throttle_ms: int = 500, max_updates_per_batch: int = 1000, queue_limit: int = 2000, adaptive_slowdown_threshold: int = 1000, stale_sec: int | None = None):
        self.venue = futures_source_from_mode(venue)
        self.url = self._url_for_venue(self.venue)
        self.proxy_url = proxy_url if proxy_enabled else ""
        self.status = WSStatus(
            enabled=enabled,
            stale_sec=int(stale_sec if stale_sec is not None else os.getenv("WS_STALE_SEC", "20")),
            venue=self.venue,
            subscribed=self._subscription_name(self.venue),
            update_throttle_ms=max(100, int(update_throttle_ms or 500)),
            max_updates_per_batch=max(25, int(max_updates_per_batch or 1000)),
            queue_limit=max(100, int(queue_limit or 2000)),
        )
        self.ping_interval = int(os.getenv("WS_PING_INTERVAL_SEC", "15"))
        self.reconnect_base = float(os.getenv("WS_RECONNECT_BASE_SEC", "1"))
        self.reconnect_max = float(os.getenv("WS_RECONNECT_MAX_SEC", "30"))
        self.adaptive_slowdown_threshold = max(100, int(adaptive_slowdown_threshold or 1000))
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._tickers: dict[str, dict[str, Any]] = {}
        self._pending: dict[str, dict[str, Any]] = {}
        self._last_flush_ts = 0.0
        self._lock = asyncio.Lock()

    def _url_for_venue(self, venue: str) -> str:
        if venue == "binance":
            return os.getenv("WS_BINANCE_FUTURES_URL", "wss://fstream.binance.com/ws/!miniTicker@arr")
        return os.getenv("WS_MEXC_FUTURES_URL", "wss://contract.mexc.com/edge")

    def _subscription_name(self, venue: str) -> str:
        return "binance_futures_miniticker" if venue == "binance" else "mexc_futures_tickers"

    async def start(self) -> None:
        if not self.status.enabled:
            return
        if self._task and not self._task.done():
            return
        self._stop.clear()
        self.status.running = True
        self._task = asyncio.create_task(self._run(), name=f"ws_supervisor_{self.venue}")

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

    def _is_socks_proxy(self) -> bool:
        scheme = urlparse(self.proxy_url).scheme.lower()
        return scheme.startswith("socks")

    def _session_kwargs(self) -> dict[str, Any]:
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=20, sock_read=max(30, self.ping_interval * 3))
        kwargs: dict[str, Any] = {"timeout": timeout}
        if self.proxy_url and self._is_socks_proxy():
            if ProxyConnector is None:
                raise RuntimeError("SOCKS proxy requires dependency aiohttp-socks")
            kwargs["connector"] = ProxyConnector.from_url(self.proxy_url)
        return kwargs

    def _ws_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {"heartbeat": self.ping_interval, "autoping": True}
        if self.proxy_url and not self._is_socks_proxy():
            kwargs["proxy"] = self.proxy_url
        return kwargs

    async def _subscribe(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        if self.venue == "mexc":
            # MEXC futures all-ticker stream. The server replies with push.tickers.
            await ws.send_json({"method": "sub.tickers", "param": {}})
            # Mark the connection as alive immediately after a successful subscribe send.
            # The ticker cache becomes usable only after push.tickers arrives, but this
            # avoids a misleading "no messages yet" state during the first seconds.
            self.status.last_message_ts = time.time()

    async def _run(self) -> None:
        backoff = self.reconnect_base
        while not self._stop.is_set():
            try:
                async with aiohttp.ClientSession(**self._session_kwargs()) as session:
                    async with session.ws_connect(self.url, **self._ws_kwargs()) as ws:
                        self.status.connected = True
                        self.status.running = True
                        self.status.last_connect_ts = time.time()
                        self.status.last_message_ts = time.time()
                        self.status.last_error = ""
                        backoff = self.reconnect_base
                        await self._subscribe(ws)
                        last_client_ping_ts = 0.0
                        while not self._stop.is_set():
                            try:
                                msg = await ws.receive(timeout=max(5, min(self.status.stale_sec, self.ping_interval)))
                            except asyncio.TimeoutError:
                                # MEXC can go quiet between pushes on some connections. Send an
                                # application-level ping before declaring the socket dead.
                                now = time.time()
                                if self.venue == "mexc" and now - last_client_ping_ts >= self.ping_interval:
                                    await ws.send_json({"method": "ping"})
                                    last_client_ping_ts = now
                                    continue
                                if now - self.status.last_message_ts >= self.status.stale_sec:
                                    raise RuntimeError(f"{self.venue} websocket stale: no messages for {self.status.stale_sec}s")
                                continue
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                self.status.last_message_ts = time.time()
                                updated = await self._handle_message(msg.data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                                raise RuntimeError(f"websocket closed/error: {msg.type}")
                            elif msg.type == aiohttp.WSMsgType.CLOSING:
                                raise RuntimeError("websocket closing")
                            elif msg.type == aiohttp.WSMsgType.PING:
                                await ws.pong()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.status.connected = False
                self.status.reconnects += 1
                self.status.last_error = str(e)[:240]
                sleep_for = min(self.reconnect_max, backoff) + random.uniform(0, 0.25)
                log.warning("%s WS reconnect in %.2fs after error: %s", self.venue, sleep_for, e)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=sleep_for)
                except asyncio.TimeoutError:
                    pass
                backoff = min(self.reconnect_max, backoff * 2)
        self.status.connected = False
        self.status.running = False

    async def _handle_message(self, raw: str) -> bool:
        try:
            data = json.loads(raw)
        except Exception:
            return False
        if self.venue == "binance":
            rows = self._parse_binance_rows(data)
        else:
            rows = self._parse_mexc_rows(data)
        queued = await self._queue_rows(rows)
        flushed = await self._flush_pending(force=False)
        return queued or flushed

    async def _queue_rows(self, rows: list[dict[str, Any]]) -> bool:
        if not rows:
            return False
        limit = self.status.queue_limit
        max_batch = self.status.max_updates_per_batch
        now = time.time()
        async with self._lock:
            # Do not truncate one full-exchange ticker snapshot. MEXC push.tickers can
            # contain 800+ contracts; cutting it to 250 makes the WS cache incomplete
            # and produces false "dropped" growth. queue_limit still protects memory.
            for row in rows:
                symbol = row.get("symbol")
                if not symbol:
                    continue
                if len(self._pending) >= limit and symbol not in self._pending:
                    # Drop oldest pending update to keep Railway memory bounded.
                    try:
                        self._pending.pop(next(iter(self._pending)))
                    except Exception:
                        pass
                    self.status.dropped_updates += 1
                row["ts"] = now
                self._pending[symbol] = row
            self.status.pending_updates = len(self._pending)
        if len(rows) >= self.adaptive_slowdown_threshold:
            self.status.adaptive_slowdown_ms = min(2000, self.status.adaptive_slowdown_ms + 100)
        elif self.status.adaptive_slowdown_ms:
            self.status.adaptive_slowdown_ms = max(0, self.status.adaptive_slowdown_ms - 10)
        return True

    async def _flush_pending(self, force: bool = False) -> bool:
        now = time.time()
        throttle = (self.status.update_throttle_ms + self.status.adaptive_slowdown_ms) / 1000.0
        if not force and now - self._last_flush_ts < throttle:
            return False
        async with self._lock:
            if not self._pending:
                self.status.pending_updates = 0
                return False
            rows = list(self._pending.values())
            self._pending.clear()
            for row in rows:
                self._tickers[row["symbol"]] = row
            self.status.pending_updates = 0
            self.status.processed_messages += len(rows)
            self._last_flush_ts = now
        return True

    def _parse_binance_rows(self, data: Any) -> list[dict[str, Any]]:
        if isinstance(data, dict):
            data = data.get("data", data)
        if not isinstance(data, list):
            data = [data]
        rows: list[dict[str, Any]] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            symbol = self._normalize_binance_symbol(str(item.get("s", "")))
            if not symbol:
                continue
            last = self._to_float(item.get("c"))
            quote_volume = self._to_float(item.get("q"))
            open_price = self._to_float(item.get("o"))
            pct = ((last - open_price) / open_price * 100.0) if open_price else self._to_float(item.get("P"))
            rows.append({
                "symbol": symbol,
                "last": last,
                "quoteVolume": quote_volume,
                "percentage": pct,
                "source": "binance_futures_ws",
            })
        return rows

    def _parse_mexc_rows(self, data: Any) -> list[dict[str, Any]]:
        if isinstance(data, dict) and data.get("channel") in {"pong", "rs.sub.tickers"}:
            return []
        payload = data.get("data") if isinstance(data, dict) else data
        if isinstance(payload, dict) and "data" in payload:
            payload = payload.get("data")
        if isinstance(payload, dict):
            items = [payload]
        elif isinstance(payload, list):
            items = payload
        else:
            items = []
        rows: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            raw_symbol = str(item.get("symbol") or item.get("s") or "")
            symbol = self._normalize_mexc_symbol(raw_symbol)
            if not symbol:
                continue
            last = self._to_float(item.get("lastPrice") or item.get("last") or item.get("p") or item.get("c"))
            quote_volume = self._to_float(item.get("amount24") or item.get("quoteVolume") or item.get("q"))
            open_price = self._to_float(item.get("open") or item.get("o"))
            pct = self._to_float(item.get("riseFallRate") or item.get("percentage") or item.get("P"))
            if not pct and open_price:
                pct = ((last - open_price) / open_price * 100.0)
            if abs(pct) < 1 and pct:
                pct *= 100.0
            rows.append({
                "symbol": symbol,
                "last": last,
                "quoteVolume": quote_volume,
                "percentage": pct,
                "source": "mexc_futures_ws",
            })
        return rows

    async def _handle_binance(self, data: Any) -> bool:
        if isinstance(data, dict):
            data = data.get("data", data)
        if not isinstance(data, list):
            data = [data]
        updated = 0
        async with self._lock:
            for item in data:
                if not isinstance(item, dict):
                    continue
                symbol = self._normalize_binance_symbol(str(item.get("s", "")))
                if not symbol:
                    continue
                last = self._to_float(item.get("c"))
                quote_volume = self._to_float(item.get("q"))
                open_price = self._to_float(item.get("o"))
                pct = ((last - open_price) / open_price * 100.0) if open_price else self._to_float(item.get("P"))
                self._tickers[symbol] = {
                    "symbol": symbol,
                    "last": last,
                    "quoteVolume": quote_volume,
                    "percentage": pct,
                    "ts": time.time(),
                    "source": "binance_futures_ws",
                }
                updated += 1
        return updated > 0

    async def _handle_mexc(self, data: Any) -> bool:
        # Known MEXC variants:
        # {"channel":"push.tickers","data":[{"symbol":"BTC_USDT","lastPrice":...,"amount24":...}]}
        # {"channel":"push.ticker","data":{"symbol":"BTC_USDT",...}}
        if isinstance(data, dict) and data.get("channel") in {"pong", "rs.sub.tickers"}:
            return False
        payload = data.get("data") if isinstance(data, dict) else data
        if isinstance(payload, dict) and "data" in payload:
            payload = payload.get("data")
        if isinstance(payload, dict):
            rows = [payload]
        elif isinstance(payload, list):
            rows = payload
        else:
            rows = []
        updated = 0
        async with self._lock:
            for item in rows:
                if not isinstance(item, dict):
                    continue
                raw_symbol = str(item.get("symbol") or item.get("s") or item.get("contractId") or "")
                symbol = self._normalize_mexc_symbol(raw_symbol)
                if not symbol:
                    continue
                last = self._to_float(item.get("lastPrice", item.get("last", item.get("c", item.get("fairPrice")))))
                quote_volume = self._to_float(item.get("amount24", item.get("quoteVolume", item.get("volume24", item.get("q")))))
                rise_fall_rate = self._to_float(item.get("riseFallRate", item.get("changeRate", 0.0)))
                pct = rise_fall_rate * 100.0 if abs(rise_fall_rate) <= 3 else rise_fall_rate
                self._tickers[symbol] = {
                    "symbol": symbol,
                    "last": last,
                    "quoteVolume": quote_volume,
                    "percentage": pct,
                    "ts": time.time(),
                    "source": "mexc_futures_ws",
                }
                updated += 1
        return updated > 0

    def _to_float(self, value: Any) -> float:
        try:
            return float(value or 0)
        except Exception:
            return 0.0

    def _normalize_binance_symbol(self, raw: str) -> str:
        if not raw or not raw.endswith("USDT"):
            return ""
        base = raw[:-4]
        return f"{base}/USDT"

    def _normalize_mexc_symbol(self, raw: str) -> str:
        if not raw:
            return ""
        raw = raw.replace("-", "_").replace(":USDT", "")
        if "_" in raw:
            base, quote = raw.split("_", 1)
        elif raw.endswith("USDT"):
            base, quote = raw[:-4], "USDT"
        else:
            return ""
        if quote != "USDT" or not base:
            return ""
        return f"{base}/USDT:USDT"

    async def ticker(self, symbol: str, max_age_sec: float | None = None) -> dict[str, Any] | None:
        # Flush a debounced snapshot before reads so scanner sees recent WS data
        # without processing every tick immediately.
        await self._flush_pending(force=False)
        keys = [symbol]
        if symbol.endswith(":USDT"):
            keys.append(symbol.replace(":USDT", ""))
        else:
            keys.append(symbol + ":USDT")
        async with self._lock:
            t = {}
            for k in keys:
                if k in self._tickers:
                    t = dict(self._tickers[k])
                    break
        if not t:
            return None
        if max_age_sec is not None and time.time() - float(t.get("ts", 0)) > max_age_sec:
            return None
        return t

    async def tickers(self, max_age_sec: float | None = None) -> dict[str, dict[str, Any]]:
        await self._flush_pending(force=False)
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
        age = st['last_message_age_sec'] if st['last_message_age_sec'] is not None else 'no messages yet'
        return (
            f"WS venue: {st['venue']}\n"
            f"WS enabled: {st['enabled']}\n"
            f"WS running: {st['running']}\n"
            f"WS connected: {st['connected']}\n"
            f"WS healthy: {st['healthy']}\n"
            f"WS last msg age: {age}s\n"
            f"WS reconnects: {st['reconnects']}\n"
            f"WS processed: {st['processed_messages']} | pending: {st['pending_updates']} | dropped: {st['dropped_updates']}\n"
            f"WS throttle: {st['update_throttle_ms']}ms + slowdown {st['adaptive_slowdown_ms']}ms | batch={st['max_updates_per_batch']} | queue={st['queue_limit']}\n"
            f"WS last error: {st['last_error'] or '-'}"
        )
