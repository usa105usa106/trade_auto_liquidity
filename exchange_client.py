import os
import time
import hmac
import hashlib
import json
import asyncio
from collections import deque
from urllib.parse import urlencode

import aiohttp
import ccxt.async_support as ccxt

try:
    from aiohttp_socks import ProxyConnector
except Exception:  # pragma: no cover
    ProxyConnector = None


class ExchangeClient:
    def __init__(self, exchange_id="mexc", proxy_url: str = "", proxy_enabled: bool = False):
        self.exchange_id = exchange_id.lower()
        self.proxy_url = proxy_url
        self.proxy_enabled = proxy_enabled
        self.exchange = None
        self.api_key = ""
        self.api_secret = ""
        self.time_difference_ms = 0
        self._mexc_private_request_times = deque()
        self._mexc_private_lock = asyncio.Lock()

    async def init(self, api_key: str = "", api_secret: str = ""):
        self.api_key = api_key or ""
        self.api_secret = api_secret or ""
        klass = getattr(ccxt, self.exchange_id)
        config = {
            "apiKey": self.api_key,
            "secret": self.api_secret,
            "enableRateLimit": True,
            "headers": {"User-Agent": "Mozilla/5.0"},
            "options": {
                "defaultType": "swap",
                "adjustForTimeDifference": True,
                "recvWindow": int(os.getenv("MEXC_RECV_WINDOW", "20000")),
                # Prevent ccxt from touching spot-private currency endpoints before swap calls.
                "fetchCurrencies": False,
            },
        }
        if self.proxy_enabled and self.proxy_url:
            config["proxies"] = {"http": self.proxy_url, "https": self.proxy_url}
            config["aiohttp_proxy"] = self.proxy_url
        self.exchange = klass(config)
        await self.exchange.load_markets()
        try:
            if hasattr(self.exchange, "load_time_difference"):
                diff = await self.exchange.load_time_difference()
                self.time_difference_ms = int(diff or 0)
        except Exception:
            # Do not block startup; raw MEXC fallback also syncs from MEXC server time.
            pass
        await self._sync_mexc_time(silent=True)
        return self

    def normalize_symbol(self, symbol: str) -> str:
        """Return an exchange-compatible swap symbol, or raise if none exists."""
        if not self.exchange:
            raise RuntimeError("exchange is not initialized")
        markets = getattr(self.exchange, "markets", None) or {}
        if symbol in markets:
            return symbol
        base, quote = (symbol.split("/", 1) + [""])[:2] if "/" in symbol else (symbol.replace("USDT", ""), "USDT")
        quote = (quote.split(":", 1)[0] or "USDT").upper()
        aliases = [
            symbol,
            f"{base}/{quote}:USDT",
            f"{base}/{quote}",
            f"{base}/USDT:USDT",
            f"{base}/USDT",
        ]
        for candidate in aliases:
            if candidate in markets:
                m = markets[candidate]
                if m.get("swap") or m.get("future") or m.get("type") in {"swap", "future"}:
                    return candidate
        for m in markets.values():
            if m.get("base") == base and m.get("quote") == "USDT" and (m.get("swap") or m.get("future") or m.get("type") in {"swap", "future"}):
                return m["symbol"]
        raise ValueError(f"no compatible swap market for symbol {symbol}")

    def _market(self, symbol: str) -> dict:
        norm = self.normalize_symbol(symbol)
        return (getattr(self.exchange, "markets", {}) or {}).get(norm, {"symbol": norm})

    def _mexc_symbol(self, symbol: str) -> str:
        m = self._market(symbol)
        mid = str(m.get("id") or "")
        if mid:
            return mid
        norm = str(m.get("symbol") or self.normalize_symbol(symbol))
        base = norm.split("/", 1)[0]
        return f"{base}_USDT"

    def _amount_to_mexc_vol(self, symbol: str, amount: float) -> int:
        """MEXC futures API expects integer contract volume, not base coin amount."""
        m = self._market(symbol)
        amount = float(amount or 0)
        contract_size = float(m.get("contractSize") or m.get("contract_size") or 0)
        if contract_size > 0:
            vol = amount / contract_size
        else:
            # Fallback for USDT perpetuals when ccxt metadata is incomplete.
            vol = amount
        vol = int(round(vol))
        return max(1, vol)

    def futures_market_symbols(self) -> list[str]:
        """Return all known USDT swap/futures symbols from loaded exchange markets."""
        markets = getattr(self.exchange, "markets", None) or {}
        out = []
        for m in markets.values():
            try:
                if m.get("quote") != "USDT":
                    continue
                if not (m.get("swap") or m.get("future") or m.get("type") in {"swap", "future"}):
                    continue
                sym = m.get("symbol")
                if sym and sym not in out:
                    out.append(sym)
            except Exception:
                continue
        return out

    async def close(self):
        if self.exchange:
            await self.exchange.close()

    async def fetch_balance(self):
        # For MEXC futures use ONLY native futures API.
        # Do not fallback to ccxt.fetch_balance(): ccxt can call the spot-private
        # /api/v3/capital/config/getall endpoint, which causes the old proxy 403.
        if self.exchange_id == "mexc":
            return await self._mexc_fetch_balance()
        return await self.exchange.fetch_balance({"type": "swap"})

    async def fetch_tickers(self):
        return await self.exchange.fetch_tickers()

    async def fetch_order_book(self, symbol, limit=20):
        return await self.exchange.fetch_order_book(self.normalize_symbol(symbol), limit=limit)

    async def fetch_ticker(self, symbol, params=None):
        return await self.exchange.fetch_ticker(self.normalize_symbol(symbol), params or {})

    async def fetch_ohlcv(self, symbol, timeframe="1m", limit=60, params=None):
        return await self.exchange.fetch_ohlcv(self.normalize_symbol(symbol), timeframe=timeframe, limit=limit, params=params or {})

    async def fetch_order(self, order_id, symbol):
        if not hasattr(self.exchange, "fetch_order"):
            raise NotImplementedError(f"{self.exchange_id} does not support fetch_order")
        return await self.exchange.fetch_order(order_id, self.normalize_symbol(symbol))

    async def fetch_open_orders(self, symbol=None):
        if self.exchange_id == "mexc":
            try:
                return await self._mexc_fetch_open_orders(symbol)
            except Exception:
                # ccxt fallback is still useful for read-only order listing.
                pass
        return await self.exchange.fetch_open_orders(self.normalize_symbol(symbol) if symbol else None)

    async def fetch_positions(self, symbols=None):
        if self.exchange_id == "mexc":
            try:
                return await self._mexc_fetch_positions(symbols)
            except Exception:
                # Keep ccxt as a read fallback, but native MEXC is preferred for sync.
                pass
        if not hasattr(self.exchange, "fetch_positions"):
            raise NotImplementedError(f"{self.exchange_id} does not support fetch_positions")
        norm_symbols = [self.normalize_symbol(s) for s in symbols] if symbols else None
        return await self.exchange.fetch_positions(norm_symbols)

    async def create_order(self, symbol, type_, side, amount, price=None, params=None):
        params = params or {}
        norm = self.normalize_symbol(symbol)

        # MEXC futures orders are sent through the support-recommended host
        # https://api.mexc.com and native endpoint /api/v1/private/order/create.
        # Do not fallback to ccxt for MEXC, because ccxt can route to
        # /api/v1/private/order/submit on contract.mexc.com, which was the
        # endpoint producing CDN 403.
        if self.exchange_id == "mexc":
            return await self._mexc_create_order(symbol, type_, side, amount, price, params, previous_error="")

        return await self.exchange.create_order(norm, type_, side, amount, price, params)

    async def cancel_order(self, order_id, symbol):
        return await self.exchange.cancel_order(order_id, self.normalize_symbol(symbol))

    async def cancel_all_orders(self, symbol=None):
        if self.exchange_id == "mexc":
            return await self._mexc_cancel_all_orders(symbol)
        norm_symbol = self.normalize_symbol(symbol) if symbol else None
        if hasattr(self.exchange, "cancel_all_orders"):
            return await self.exchange.cancel_all_orders(norm_symbol)
        orders = await self.fetch_open_orders(norm_symbol)
        out = []
        for o in orders:
            try:
                out.append(await self.cancel_order(o["id"], o["symbol"]))
            except Exception as e:
                out.append({"id": o.get("id"), "symbol": o.get("symbol"), "error": str(e)})
        return out


    def _mexc_rest_base(self) -> str:
        """Base URL for MEXC futures REST.

        MEXC support recommended using https://api.mexc.com instead of
        https://contract.mexc.com for futures private requests when CDN 403
        appears on the contract host. Keep it configurable, but default to the
        support-recommended host.
        """
        return os.getenv("MEXC_FUTURES_REST_BASE", "https://api.mexc.com").rstrip("/")

    async def _mexc_private_rate_limit(self):
        """Limit private MEXC requests to <=4 per 2 seconds.

        This matches the support recommendation and prevents duplicate order
        attempts from looking like private endpoint spam.
        """
        limit = int(os.getenv("MEXC_PRIVATE_RATE_LIMIT", "4") or "4")
        window = float(os.getenv("MEXC_PRIVATE_RATE_WINDOW", "2") or "2")
        if limit <= 0:
            return
        async with self._mexc_private_lock:
            now = time.monotonic()
            while self._mexc_private_request_times and now - self._mexc_private_request_times[0] >= window:
                self._mexc_private_request_times.popleft()
            if len(self._mexc_private_request_times) >= limit:
                sleep_for = window - (now - self._mexc_private_request_times[0]) + 0.05
                await asyncio.sleep(max(0.05, sleep_for))
                now = time.monotonic()
                while self._mexc_private_request_times and now - self._mexc_private_request_times[0] >= window:
                    self._mexc_private_request_times.popleft()
            self._mexc_private_request_times.append(time.monotonic())

    def _proxy_connector_and_arg(self):
        """Return (connector, proxy_arg) for aiohttp requests.

        aiohttp needs a ProxyConnector for SOCKS proxies, but HTTP/HTTPS proxies
        must be passed as the per-request `proxy=` argument. The previous raw
        MEXC code only handled SOCKS; this keeps both paths explicit and makes
        `/proxy test` and signed MEXC REST use the same proxy route.
        """
        if not (self.proxy_enabled and self.proxy_url):
            return None, None
        from urllib.parse import urlparse
        scheme = urlparse(self.proxy_url).scheme.lower()
        if scheme.startswith("socks"):
            if not ProxyConnector:
                raise RuntimeError("SOCKS proxy requires aiohttp-socks")
            return ProxyConnector.from_url(self.proxy_url), None
        return None, self.proxy_url

    async def _http_session(self):
        connector, _ = self._proxy_connector_and_arg()
        return aiohttp.ClientSession(connector=connector)

    async def _sync_mexc_time(self, silent: bool = False):
        if self.exchange_id != "mexc":
            return 0
        try:
            connector, proxy_arg = self._proxy_connector_and_arg()
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(f"{self._mexc_rest_base()}/api/v1/contract/ping", proxy=proxy_arg, timeout=10) as r:
                    data = await r.json(content_type=None)
            server = int(data.get("data") or data.get("timestamp") or 0)
            if server > 0:
                local = int(time.time() * 1000)
                self.time_difference_ms = server - local
            return self.time_difference_ms
        except Exception:
            if silent:
                return self.time_difference_ms
            raise

    def _mexc_recv_window_header(self) -> str:
        """Return MEXC futures Recv-Window header in seconds.

        Telegram/Railway setting is kept in milliseconds for ccxt-style config
        (for example 20000 = 20 seconds). Current MEXC futures OPEN-API docs
        describe Recv-Window as seconds with a max of 60, so raw contract
        fallback converts ms -> seconds and caps it safely.
        """
        try:
            value = int(float(os.getenv("MEXC_RECV_WINDOW", "20000") or "20000"))
        except Exception:
            value = 20000
        if value > 1000:
            value = int((value + 999) // 1000)
        return str(max(1, min(60, value)))

    def _mexc_request_time(self) -> str:
        return str(int(time.time() * 1000) + int(self.time_difference_ms or 0))

    def _mexc_signature(self, req_time: str, payload: str) -> str:
        raw = f"{self.api_key}{req_time}{payload}"
        return hmac.new(self.api_secret.encode(), raw.encode(), hashlib.sha256).hexdigest()

    async def _mexc_private(self, method: str, path: str, body: dict | None = None, query: dict | None = None, base_url: str | None = None):
        if not self.api_key or not self.api_secret:
            raise RuntimeError("MEXC API key/secret is missing")
        await self._mexc_private_rate_limit()
        body = body or {}
        query = query or {}
        method = method.upper()
        base = (base_url or self._mexc_rest_base()).rstrip("/")
        if method == "GET":
            payload = urlencode(sorted((k, v) for k, v in query.items() if v is not None))
            url = f"{base}{path}" + (f"?{payload}" if payload else "")
            data = None
        else:
            payload = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
            url = f"{base}{path}"
            data = payload
        req_time = self._mexc_request_time()
        headers = {
            "ApiKey": self.api_key,
            "Request-Time": req_time,
            "Signature": self._mexc_signature(req_time, payload),
            "Recv-Window": self._mexc_recv_window_header(),
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
        }
        connector, proxy_arg = self._proxy_connector_and_arg()
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.request(method, url, data=data, headers=headers, proxy=proxy_arg, timeout=15) as r:
                text = await r.text()
                try:
                    out = json.loads(text)
                except Exception:
                    out = {"raw": text}
                if r.status == 401 or r.status == 403 or str(out.get("code")) in {"401", "403", "602", "603"}:
                    # One retry after syncing MEXC server time.
                    await self._sync_mexc_time(silent=True)
                    req_time = self._mexc_request_time()
                    headers["Request-Time"] = req_time
                    headers["Signature"] = self._mexc_signature(req_time, payload)
                    async with session.request(method, url, data=data, headers=headers, proxy=proxy_arg, timeout=15) as r2:
                        text = await r2.text()
                        try:
                            out = json.loads(text)
                        except Exception:
                            out = {"raw": text}
                        if r2.status >= 400 or out.get("success") is False:
                            raise RuntimeError(f"HTTP {r2.status}: {out}")
                        return out
                if r.status >= 400 or out.get("success") is False:
                    raise RuntimeError(f"HTTP {r.status}: {out}")
                return out

    async def _mexc_private_read_any_base(self, path: str, query: dict | None = None):
        """Read native MEXC futures state from supported hosts.

        Support recommended api.mexc.com for private trading after CDN 403 on
        contract.mexc.com. For read-only state sync, MEXC can sometimes expose
        fresher position/order state on one host before the other. Try the
        configured host first, then api.mexc.com and contract.mexc.com.
        """
        bases = []
        for b in (self._mexc_rest_base(), "https://api.mexc.com", "https://contract.mexc.com"):
            b = b.rstrip("/")
            if b not in bases:
                bases.append(b)
        errors = []
        for base in bases:
            try:
                out = await self._mexc_private("GET", path, query=query or {}, base_url=base)
                if isinstance(out, dict):
                    out.setdefault("_base_url", base)
                return out
            except Exception as e:
                errors.append(f"{base}: {e}")
        raise RuntimeError("MEXC read failed on all hosts: " + " | ".join(errors[:3]))

    @staticmethod
    def _mexc_rows(data):
        """Normalize MEXC data/list/result containers into a list of rows."""
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("list", "result", "data", "rows", "items"):
                value = data.get(key)
                if isinstance(value, list):
                    return value
            # Some endpoints return a dict keyed by symbol/order id.
            if data and all(isinstance(v, dict) for v in data.values()):
                return list(data.values())
        return []

    async def _mexc_fetch_balance(self):
        out = await self._mexc_private_read_any_base("/api/v1/private/account/assets")
        assets = out.get("data") or []
        free = total = used = 0.0
        by_currency = {}
        for a in assets if isinstance(assets, list) else []:
            ccy = str(a.get("currency") or a.get("asset") or "").upper()
            if ccy != "USDT":
                continue
            total = float(a.get("equity") or a.get("totalEquity") or a.get("cashBalance") or a.get("balance") or 0)
            free = float(a.get("availableBalance") or a.get("available") or a.get("availableOpen") or a.get("cashBalance") or 0)
            used = max(0.0, total - free)
            extra = {
                "positionMargin": float(a.get("positionMargin") or 0),
                "frozenBalance": float(a.get("frozenBalance") or 0),
                "unrealized": float(a.get("unrealized") or 0),
                "cashBalance": float(a.get("cashBalance") or 0),
                "availableOpen": float(a.get("availableOpen") or a.get("availableBalance") or 0),
                "availableCash": float(a.get("availableCash") or 0),
            }
            by_currency[ccy] = {"free": free, "used": used, "total": total, **extra}
        return {"free": {"USDT": free}, "used": {"USDT": used}, "total": {"USDT": total}, "USDT": by_currency.get("USDT", {"free": free, "used": used, "total": total}), "info": out}

    def _mexc_id_to_symbol(self, mexc_symbol: str) -> str:
        raw = str(mexc_symbol or "").strip()
        if not raw:
            return raw
        markets = getattr(self.exchange, "markets", {}) or {}
        for m in markets.values():
            if str(m.get("id") or "") == raw:
                return str(m.get("symbol") or raw)
        if "_" in raw:
            base, quote = raw.split("_", 1)
            candidate = f"{base}/{quote}:USDT"
            if candidate in markets:
                return candidate
            candidate2 = f"{base}/{quote}"
            if candidate2 in markets:
                return candidate2
            return candidate
        return raw

    def _mexc_contracts_to_amount(self, symbol: str, contracts: float) -> float:
        try:
            m = self._market(symbol)
            contract_size = float(m.get("contractSize") or m.get("contract_size") or 0)
            if contract_size > 0:
                return abs(float(contracts or 0)) * contract_size
        except Exception:
            pass
        return abs(float(contracts or 0))

    def _mexc_position_qty_contracts(self, row: dict) -> float:
        for key in ("holdVol", "vol", "positionVol", "positionAmt", "amount", "contracts"):
            try:
                value = row.get(key)
                if value not in (None, ""):
                    return abs(float(value))
            except Exception:
                pass
        return 0.0

    def _mexc_position_side(self, row: dict) -> str:
        side = str(row.get("positionType") or row.get("holdSide") or row.get("side") or "").lower()
        if side in {"2", "short", "sell"} or "short" in side:
            return "short"
        return "long"

    def _mexc_parse_position(self, row: dict) -> dict:
        mexc_symbol = str(row.get("symbol") or row.get("contract") or "")
        symbol = self._mexc_id_to_symbol(mexc_symbol)
        contracts = self._mexc_position_qty_contracts(row)
        amount = self._mexc_contracts_to_amount(symbol, contracts)
        entry = 0.0
        for key in ("holdAvgPrice", "openAvgPrice", "entryPrice", "avgPrice"):
            try:
                if row.get(key) not in (None, ""):
                    entry = float(row.get(key)); break
            except Exception:
                pass
        mark = 0.0
        for key in ("markPrice", "fairPrice", "lastPrice"):
            try:
                if row.get(key) not in (None, ""):
                    mark = float(row.get(key)); break
            except Exception:
                pass
        side = self._mexc_position_side(row)
        return {
            "symbol": symbol,
            "side": side,
            "contracts": contracts,
            "contractSize": (amount / contracts if contracts else None),
            "amount": amount,
            "entryPrice": entry,
            "markPrice": mark,
            "unrealizedPnl": float(row.get("unrealised") or row.get("unrealizedPnl") or row.get("profit") or 0),
            "info": row,
        }

    async def _mexc_fetch_positions(self, symbols=None):
        queries = [{}]
        if symbols:
            for sym in list(symbols):
                queries.append({"symbol": self._mexc_symbol(sym)})
        all_rows = []
        raw_meta = []
        errors = []
        # Query all positions first, then symbol-specific variants. This catches
        # MEXC cases where one variant returns empty while the other has data.
        for query in queries:
            try:
                out = await self._mexc_private_read_any_base("/api/v1/private/position/open_positions", query=query)
                raw_meta.append({"base": out.get("_base_url"), "query": query})
                all_rows.extend([r for r in self._mexc_rows(out.get("data")) if isinstance(r, dict)])
            except Exception as e:
                errors.append(str(e))
        # De-duplicate by positionId/symbol/side.
        unique = []
        seen = set()
        for r in all_rows:
            key = (str(r.get("positionId") or ""), str(r.get("symbol") or ""), str(r.get("positionType") or r.get("holdSide") or r.get("side") or ""))
            if key in seen:
                continue
            seen.add(key)
            unique.append(r)
        parsed = [self._mexc_parse_position(r) for r in unique]
        parsed = [p for p in parsed if self._mexc_position_qty_contracts(p.get("info", {})) > 0 or float(p.get("contracts") or 0) > 0 or float(p.get("amount") or 0) > 0]
        if symbols:
            wanted = {self.normalize_symbol(x) for x in symbols}
            parsed = [p for p in parsed if p.get("symbol") in wanted]
        for p in parsed:
            p.setdefault("sync_meta", raw_meta[:3])
        if not parsed and errors:
            # Preserve a lightweight debug trail for callers that surface errors.
            return []
        return parsed

    def _mexc_parse_order(self, row: dict) -> dict:
        symbol = self._mexc_id_to_symbol(str(row.get("symbol") or row.get("contract") or ""))
        oid = str(row.get("orderId") or row.get("id") or row.get("planOrderId") or row.get("stopOrderId") or row.get("externalOid") or "")
        side_raw = str(row.get("side") or row.get("positionType") or row.get("holdSide") or "")
        side = "buy" if side_raw in {"1", "2", "buy", "long"} else "sell"
        vol = row.get("vol") or row.get("remainVol") or row.get("volume") or row.get("holdVol") or 0
        return {
            "id": oid,
            "symbol": symbol,
            "side": side,
            "type": row.get("type") or row.get("orderType") or row.get("category") or "unknown",
            "price": float(row.get("price") or row.get("executePrice") or row.get("triggerPrice") or 0),
            "amount": self._mexc_contracts_to_amount(symbol, float(vol or 0)),
            "remaining": self._mexc_contracts_to_amount(symbol, float(row.get("remainVol") or vol or 0)),
            "status": "open",
            "clientOrderId": row.get("externalOid"),
            "info": row,
        }

    async def _mexc_fetch_open_orders(self, symbol=None):
        """Fetch normal open orders plus trigger/TP-SL style orders.

        MEXC can reserve balance in plan/stop/TP-SL orders that are not returned
        by the normal open_orders endpoint. Include all known current-order
        endpoints and ignore unsupported variants instead of hiding normal data.
        """
        candidates = []
        if symbol:
            msym = self._mexc_symbol(symbol)
            candidates.extend([
                ("/api/v1/private/order/list/open_orders/" + msym, {}),
                ("/api/v1/private/order/list/open_orders", {"symbol": msym}),
                ("/api/v1/private/planorder/list/orders", {"symbol": msym, "states": "1"}),
                ("/api/v1/private/stoporder/list/orders", {"symbol": msym, "states": "1"}),
                ("/api/v1/private/stoporder/list/orders", {"symbol": msym, "isFinished": 0}),
            ])
        else:
            candidates.extend([
                ("/api/v1/private/order/list/open_orders", {}),
                ("/api/v1/private/planorder/list/orders", {"states": "1"}),
                ("/api/v1/private/stoporder/list/orders", {"states": "1"}),
                ("/api/v1/private/stoporder/list/orders", {"isFinished": 0}),
            ])
        orders = []
        errors = []
        for path, query in candidates:
            try:
                out = await self._mexc_private_read_any_base(path, query=query)
                rows = [r for r in self._mexc_rows(out.get("data")) if isinstance(r, dict)]
                for r in rows:
                    r.setdefault("_source_endpoint", path)
                    orders.append(self._mexc_parse_order(r))
            except Exception as e:
                errors.append(f"{path}: {e}")
        # De-duplicate.
        unique = []
        seen = set()
        for o in orders:
            key = (o.get("id"), o.get("symbol"), o.get("type"), (o.get("info") or {}).get("_source_endpoint"))
            if key in seen:
                continue
            seen.add(key)
            if symbol and o.get("symbol") != self.normalize_symbol(symbol):
                continue
            unique.append(o)
        return unique

    async def _mexc_cancel_all_orders(self, symbol=None):
        symbols = []
        if symbol:
            symbols = [symbol]
        else:
            try:
                symbols.extend([o.get("symbol") for o in await self._mexc_fetch_open_orders() if o.get("symbol")])
            except Exception:
                pass
            try:
                symbols.extend([p.get("symbol") for p in await self._mexc_fetch_positions() if p.get("symbol")])
            except Exception:
                pass
        seen = []
        for sym in symbols:
            if sym and sym not in seen:
                seen.append(sym)
        results = []
        errors = []
        # Normal order, plan order and stop/TP-SL cleanup.
        cancel_paths = [
            ("/api/v1/private/order/cancel_all", "POST"),
            ("/api/v1/private/planorder/cancel_all", "POST"),
            ("/api/v1/private/stoporder/cancel_all", "POST"),
        ]
        for sym in seen:
            msym = self._mexc_symbol(sym)
            for path, method in cancel_paths:
                try:
                    out = await self._mexc_private(method, path, body={"symbol": msym})
                    results.append({"symbol": self.normalize_symbol(sym), "endpoint": path, "result": out})
                except Exception as e:
                    errors.append({"symbol": sym, "endpoint": path, "error": str(e)})
        if not seen and not symbol:
            # Some accounts may have order reserves but listing may fail; attempt
            # exchange-wide ccxt cancellation as a final fallback.
            try:
                results.append({"symbol": "all", "endpoint": "ccxt.cancel_all_orders", "result": await self.exchange.cancel_all_orders(None)})
            except Exception as e:
                errors.append({"symbol": "all", "error": str(e)})
        return {"ok": len(errors) == 0, "cancelled_symbols": len(results), "results": results, "errors": errors}


    async def mexc_close_all_positions_native(self):
        """Emergency exchange-side close all positions endpoint."""
        if self.exchange_id != "mexc":
            raise NotImplementedError("native close_all is MEXC only")
        return await self._mexc_private("POST", "/api/v1/private/position/close_all", body={})

    async def mexc_account_state(self):
        """Return raw account state used by diagnostics commands."""
        bal = await self._mexc_fetch_balance()
        pos = await self._mexc_fetch_positions()
        orders = await self._mexc_fetch_open_orders()
        return {"balance": bal, "positions": pos, "open_orders": orders}

    def _mexc_usdt_metrics_from_balance(self, balance: dict) -> dict:
        """Extract free/used/margin/unrealized numbers from native MEXC balance."""
        usdt = balance.get("USDT", {}) if isinstance(balance, dict) else {}
        try:
            free = float(usdt.get("free") or (balance.get("free", {}) or {}).get("USDT") or 0)
        except Exception:
            free = 0.0
        try:
            total = float(usdt.get("total") or (balance.get("total", {}) or {}).get("USDT") or 0)
        except Exception:
            total = 0.0
        try:
            used = float(usdt.get("used") or (balance.get("used", {}) or {}).get("USDT") or max(0.0, total - free))
        except Exception:
            used = max(0.0, total - free)
        def f(key, default=0.0):
            try:
                return float(usdt.get(key) or default)
            except Exception:
                return default
        return {
            "free": free,
            "total": total,
            "used": used,
            "position_margin": f("positionMargin"),
            "frozen_balance": f("frozenBalance"),
            "unrealized": f("unrealized"),
        }

    async def _mexc_last_price(self, symbol: str, fallback: float | None = None) -> float:
        try:
            if fallback and float(fallback) > 0:
                return float(fallback)
        except Exception:
            pass
        try:
            t = await self.fetch_ticker(symbol)
            for k in ("last", "close", "bid", "ask"):
                v = t.get(k)
                if v and float(v) > 0:
                    return float(v)
        except Exception:
            pass
        return float(fallback or 0)

    async def _mexc_set_leverage_for_symbol(self, symbol: str, leverage: int, open_type: int) -> dict:
        """Best-effort native leverage setter.

        MEXC accepts leverage in the order body, but some accounts keep the
        previous contract leverage. To avoid accidental 1x positions, set both
        long and short positionType before opening. The endpoint payload is kept
        compatible with common MEXC futures variants.
        """
        leverage = int(leverage or 1)
        if leverage <= 0:
            leverage = 1
        msym = self._mexc_symbol(symbol)
        results, errors = [], []
        endpoint = os.getenv("MEXC_SET_LEVERAGE_ENDPOINT", "/api/v1/private/position/change_leverage")
        # positionType: 1 long, 2 short on MEXC futures. Some accounts accept a
        # symbol-level request without it, so try that too.
        payloads = [
            {"symbol": msym, "leverage": leverage, "openType": int(open_type or 1), "positionType": 1},
            {"symbol": msym, "leverage": leverage, "openType": int(open_type or 1), "positionType": 2},
            {"symbol": msym, "leverage": leverage, "openType": int(open_type or 1)},
        ]
        ok_any = False
        for body in payloads:
            try:
                out = await self._mexc_private("POST", endpoint, body=body)
                results.append(out)
                ok_any = True
            except Exception as e:
                errors.append(str(e)[:240])
        if not ok_any and os.getenv("MEXC_STRICT_LEVERAGE", "true").lower() in {"1", "true", "yes", "on"}:
            raise RuntimeError("MEXC leverage setup failed before order: " + " | ".join(errors[:2]))
        return {"ok": ok_any, "results": results, "errors": errors, "leverage": leverage, "openType": open_type}

    async def _mexc_open_margin_precheck(self, symbol: str, amount: float, price: float | None, leverage: int) -> dict:
        """Return expected margin and balance snapshot before an opening order."""
        last_price = await self._mexc_last_price(symbol, price)
        amount = float(amount or 0)
        leverage = max(1, int(leverage or 1))
        notional = abs(amount * last_price) if amount > 0 and last_price > 0 else 0.0
        expected_margin = notional / leverage if leverage > 0 else notional
        bal = await self._mexc_fetch_balance()
        metrics = self._mexc_usdt_metrics_from_balance(bal)
        return {"price": last_price, "notional": notional, "expected_margin": expected_margin, "balance": metrics}

    async def _mexc_margin_guard_after_open(self, symbol: str, before: dict, expected_margin: float) -> dict:
        """Verify that the new order did not consume far more margin than expected.

        This catches the dangerous case we observed: settings say 5x but MEXC
        effectively opens close to 1x and consumes most account margin.
        """
        await asyncio.sleep(float(os.getenv("MEXC_MARGIN_GUARD_DELAY_SEC", "0.8") or "0.8"))
        bal_after = await self._mexc_fetch_balance()
        after = self._mexc_usdt_metrics_from_balance(bal_after)
        before_used = float((before or {}).get("used") or 0)
        used_delta = max(0.0, float(after.get("used") or 0) - before_used)
        multiplier = float(os.getenv("MEXC_MARGIN_GUARD_MULTIPLIER", "2.5") or "2.5")
        absolute_buffer = float(os.getenv("MEXC_MARGIN_GUARD_ABS_BUFFER_USDT", "2.0") or "2.0")
        threshold = max(float(expected_margin or 0) * multiplier, float(expected_margin or 0) + absolute_buffer)
        ok = True
        action = "none"
        if expected_margin > 0 and used_delta > threshold and os.getenv("MEXC_MARGIN_GUARD_ENABLED", "true").lower() in {"1", "true", "yes", "on"}:
            ok = False
            action = "emergency_close_all"
            # First remove potential child orders, then native close all. This is
            # intentionally defensive because position listing can be stale/empty.
            try:
                await self._mexc_cancel_all_orders(symbol)
            except Exception:
                pass
            try:
                await self.mexc_close_all_positions_native()
            except Exception:
                pass
        return {
            "ok": ok,
            "action": action,
            "expected_margin": expected_margin,
            "used_delta": used_delta,
            "threshold": threshold,
            "before": before,
            "after": after,
        }

    async def _mexc_create_order(self, symbol, type_, side, amount, price=None, params=None, previous_error: str = ""):
        params = params or {}
        reduce_only = bool(params.get("reduceOnly") or params.get("reduce_only"))
        is_opening = not reduce_only
        target_leverage = int(os.getenv("MEXC_ORDER_LEVERAGE", "5") or "5")
        target_open_type = int(os.getenv("MEXC_ORDER_OPEN_TYPE", "1") or "1")
        leverage_setup = {"ok": None}
        margin_pre = None
        if is_opening:
            if os.getenv("MEXC_SET_LEVERAGE_BEFORE_ORDER", "true").lower() in {"1", "true", "yes", "on"}:
                leverage_setup = await self._mexc_set_leverage_for_symbol(symbol, target_leverage, target_open_type)
            margin_pre = await self._mexc_open_margin_precheck(symbol, amount, price, target_leverage)
        is_buy = str(side).lower() == "buy"
        # MEXC side codes: 1 open long, 2 close short, 3 open short, 4 close long.
        if reduce_only:
            mexc_side = 2 if is_buy else 4
        else:
            mexc_side = 1 if is_buy else 3
        t = str(type_).lower()
        if t in {"market", "stop_market"} and not any(k in params for k in ("stopPrice", "triggerPrice", "stopLossPrice")):
            mexc_type = 5  # market
            order_price = 0
        elif any(k in params for k in ("stopPrice", "triggerPrice", "stopLossPrice")):
            # Native plan order. Used for SL/TP fallback only if ccxt fails.
            trigger_price = float(params.get("triggerPrice") or params.get("stopPrice") or params.get("stopLossPrice"))
            body = {
                "symbol": self._mexc_symbol(symbol),
                "vol": self._amount_to_mexc_vol(symbol, amount),
                "side": mexc_side,
                "openType": target_open_type,
                "leverage": target_leverage,
                "triggerPrice": trigger_price,
                "executePrice": 0,
                "orderType": 5,
                "triggerType": 1,
                "trend": 1,
            }
            if params.get("clientOrderId"):
                body["externalOid"] = str(params.get("clientOrderId"))[:32]
            out = await self._mexc_private("POST", "/api/v1/private/planorder/place", body=body)
            return {"id": str((out.get("data") or {}).get("orderId") or (out.get("data") or {}).get("id") or ""), "symbol": self.normalize_symbol(symbol), "type": type_, "side": side, "amount": amount, "price": price, "info": {"raw_fallback": True, "previous_error": previous_error, **out}}
        else:
            mexc_type = 1  # limit
            order_price = float(price or 0)
            if order_price <= 0:
                raise RuntimeError("limit order requires price")
        body = {
            "symbol": self._mexc_symbol(symbol),
            "price": order_price,
            "vol": self._amount_to_mexc_vol(symbol, amount),
            "side": mexc_side,
            "type": mexc_type,
            "openType": target_open_type,
            "leverage": target_leverage,
        }
        if params.get("clientOrderId"):
            body["externalOid"] = str(params.get("clientOrderId"))[:32]
        out = await self._mexc_private("POST", "/api/v1/private/order/create", body=body)
        data = out.get("data")
        oid = data.get("orderId") if isinstance(data, dict) else data
        margin_guard = None
        if is_opening:
            margin_guard = await self._mexc_margin_guard_after_open(
                symbol,
                (margin_pre or {}).get("balance") or {},
                float((margin_pre or {}).get("expected_margin") or 0),
            )
            if not margin_guard.get("ok", True):
                raise RuntimeError(
                    "MEXC margin guard blocked unsafe position: "
                    f"expected_margin={margin_guard.get('expected_margin'):.4f} USDT, "
                    f"used_delta={margin_guard.get('used_delta'):.4f} USDT, "
                    f"threshold={margin_guard.get('threshold'):.4f} USDT. "
                    "Emergency close_all was sent."
                )
        info = {"raw_fallback": True, "previous_error": previous_error, "leverage_setup": leverage_setup, "margin_precheck": margin_pre, "margin_guard": margin_guard, **out}
        return {"id": str(oid or ""), "symbol": self.normalize_symbol(symbol), "type": type_, "side": side, "amount": amount, "price": price, "average": None, "filled": 0, "info": info}
