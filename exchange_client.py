import os
import time
import hmac
import hashlib
import json
import asyncio
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
            # Do not block startup; raw MEXC fallback also syncs from contract server time.
            pass
        await self._sync_mexc_contract_time(silent=True)
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

    def _mexc_contract_symbol(self, symbol: str) -> str:
        m = self._market(symbol)
        mid = str(m.get("id") or "")
        if mid:
            return mid
        norm = str(m.get("symbol") or self.normalize_symbol(symbol))
        base = norm.split("/", 1)[0]
        return f"{base}_USDT"

    def _amount_to_mexc_vol(self, symbol: str, amount: float) -> int:
        """MEXC contract API expects integer contract volume, not base coin amount."""
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
        try:
            return await self.exchange.fetch_balance({"type": "swap"})
        except Exception as e:
            if self.exchange_id == "mexc":
                try:
                    return await self._mexc_contract_fetch_balance()
                except Exception as e2:
                    raise RuntimeError(f"ccxt balance failed: {e}; raw contract balance failed: {e2}")
            raise

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
        return await self.exchange.fetch_open_orders(self.normalize_symbol(symbol) if symbol else None)

    async def fetch_positions(self, symbols=None):
        if not hasattr(self.exchange, "fetch_positions"):
            raise NotImplementedError(f"{self.exchange_id} does not support fetch_positions")
        norm_symbols = [self.normalize_symbol(s) for s in symbols] if symbols else None
        return await self.exchange.fetch_positions(norm_symbols)

    async def create_order(self, symbol, type_, side, amount, price=None, params=None):
        params = params or {}
        norm = self.normalize_symbol(symbol)
        try:
            return await self.exchange.create_order(norm, type_, side, amount, price, params)
        except Exception as e:
            msg = str(e)
            # MEXC often returns bare 403 on unified ccxt order placement. Fall back to
            # native contract API so entry orders are not blocked by ccxt's spot/swap routing.
            if self.exchange_id == "mexc" and ("403" in msg or "Forbidden" in msg or os.getenv("MEXC_FORCE_RAW_ORDERS", "false").lower() in {"1", "true", "yes", "on"}):
                try:
                    return await self._mexc_contract_create_order(symbol, type_, side, amount, price, params, previous_error=msg)
                except Exception as e2:
                    raise RuntimeError(f"ccxt create_order failed: {msg}; raw MEXC contract fallback failed: {e2}")
            raise

    async def cancel_order(self, order_id, symbol):
        return await self.exchange.cancel_order(order_id, self.normalize_symbol(symbol))

    async def cancel_all_orders(self, symbol=None):
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

    async def _http_session(self):
        if self.proxy_enabled and self.proxy_url and ProxyConnector:
            return aiohttp.ClientSession(connector=ProxyConnector.from_url(self.proxy_url))
        return aiohttp.ClientSession()

    async def _sync_mexc_contract_time(self, silent: bool = False):
        if self.exchange_id != "mexc":
            return 0
        try:
            async with await self._http_session() as session:
                async with session.get("https://contract.mexc.com/api/v1/contract/ping", timeout=10) as r:
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

    async def _mexc_contract_private(self, method: str, path: str, body: dict | None = None, query: dict | None = None):
        if not self.api_key or not self.api_secret:
            raise RuntimeError("MEXC API key/secret is missing")
        body = body or {}
        query = query or {}
        method = method.upper()
        if method == "GET":
            payload = urlencode(sorted((k, v) for k, v in query.items() if v is not None))
            url = f"https://contract.mexc.com{path}" + (f"?{payload}" if payload else "")
            data = None
        else:
            payload = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
            url = f"https://contract.mexc.com{path}"
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
        async with await self._http_session() as session:
            async with session.request(method, url, data=data, headers=headers, timeout=15) as r:
                text = await r.text()
                try:
                    out = json.loads(text)
                except Exception:
                    out = {"raw": text}
                if r.status == 401 or r.status == 403 or str(out.get("code")) in {"401", "403", "602", "603"}:
                    # One retry after syncing contract server time.
                    await self._sync_mexc_contract_time(silent=True)
                    req_time = self._mexc_request_time()
                    headers["Request-Time"] = req_time
                    headers["Signature"] = self._mexc_signature(req_time, payload)
                    async with session.request(method, url, data=data, headers=headers, timeout=15) as r2:
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

    async def _mexc_contract_fetch_balance(self):
        out = await self._mexc_contract_private("GET", "/api/v1/private/account/assets")
        assets = out.get("data") or []
        free = total = used = 0.0
        by_currency = {}
        for a in assets if isinstance(assets, list) else []:
            ccy = str(a.get("currency") or a.get("asset") or "").upper()
            if ccy != "USDT":
                continue
            total = float(a.get("equity") or a.get("totalEquity") or a.get("cashBalance") or a.get("balance") or 0)
            free = float(a.get("availableBalance") or a.get("available") or a.get("cashBalance") or 0)
            used = max(0.0, total - free)
            by_currency[ccy] = {"free": free, "used": used, "total": total}
        return {"free": {"USDT": free}, "used": {"USDT": used}, "total": {"USDT": total}, "USDT": by_currency.get("USDT", {"free": free, "used": used, "total": total}), "info": out}

    async def _mexc_contract_create_order(self, symbol, type_, side, amount, price=None, params=None, previous_error: str = ""):
        params = params or {}
        reduce_only = bool(params.get("reduceOnly") or params.get("reduce_only"))
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
                "symbol": self._mexc_contract_symbol(symbol),
                "vol": self._amount_to_mexc_vol(symbol, amount),
                "side": mexc_side,
                "openType": int(os.getenv("MEXC_ORDER_OPEN_TYPE", "1")),
                "triggerPrice": trigger_price,
                "executePrice": 0,
                "orderType": 5,
                "triggerType": 1,
                "trend": 1,
            }
            if params.get("clientOrderId"):
                body["externalOid"] = str(params.get("clientOrderId"))[:32]
            out = await self._mexc_contract_private("POST", "/api/v1/private/planorder/place", body=body)
            return {"id": str((out.get("data") or {}).get("orderId") or (out.get("data") or {}).get("id") or ""), "symbol": self.normalize_symbol(symbol), "type": type_, "side": side, "amount": amount, "price": price, "info": {"raw_fallback": True, "previous_error": previous_error, **out}}
        else:
            mexc_type = 1  # limit
            order_price = float(price or 0)
            if order_price <= 0:
                raise RuntimeError("limit order requires price")
        body = {
            "symbol": self._mexc_contract_symbol(symbol),
            "price": order_price,
            "vol": self._amount_to_mexc_vol(symbol, amount),
            "side": mexc_side,
            "type": mexc_type,
            "openType": int(os.getenv("MEXC_ORDER_OPEN_TYPE", "1")),
            "leverage": int(os.getenv("MEXC_ORDER_LEVERAGE", "1")),
        }
        if params.get("clientOrderId"):
            body["externalOid"] = str(params.get("clientOrderId"))[:32]
        out = await self._mexc_contract_private("POST", "/api/v1/private/order/create", body=body)
        data = out.get("data")
        oid = data.get("orderId") if isinstance(data, dict) else data
        return {"id": str(oid or ""), "symbol": self.normalize_symbol(symbol), "type": type_, "side": side, "amount": amount, "price": price, "average": None, "filled": 0, "info": {"raw_fallback": True, "previous_error": previous_error, **out}}
