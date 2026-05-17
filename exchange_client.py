import os, time, asyncio
import ccxt.async_support as ccxt


class ExchangeClient:
    def __init__(self, exchange_id="mexc", proxy_url: str = "", proxy_enabled: bool = False):
        self.exchange_id = exchange_id.lower()
        self.proxy_url = proxy_url
        self.proxy_enabled = proxy_enabled
        self.exchange = None

    async def init(self, api_key: str = "", api_secret: str = ""):
        klass = getattr(ccxt, self.exchange_id)
        config = {
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
        if self.proxy_enabled and self.proxy_url:
            config["proxies"] = {"http": self.proxy_url, "https": self.proxy_url}
            # ccxt async_support uses aiohttp internally; aiohttp_proxy makes
            # HTTP/SOCKS proxy usage explicit for async requests.
            config["aiohttp_proxy"] = self.proxy_url
        self.exchange = klass(config)
        # Load markets once so user-facing symbols can be mapped to the exact
        # swap symbol used by the selected ccxt venue. A failure here is fatal
        # for live execution because otherwise symbols may silently mismatch.
        await self.exchange.load_markets()
        return self

    def normalize_symbol(self, symbol: str) -> str:
        """Return an exchange-compatible swap symbol, or raise if none exists."""
        if not self.exchange:
            raise RuntimeError("exchange is not initialized")
        markets = getattr(self.exchange, "markets", None) or {}
        if symbol in markets:
            return symbol
        base, quote = (symbol.split("/", 1) + [""])[:2] if "/" in symbol else (symbol.replace("USDT", ""), "USDT")
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
        return await self.exchange.fetch_open_orders(self.normalize_symbol(symbol) if symbol else None)

    async def fetch_positions(self, symbols=None):
        if not hasattr(self.exchange, "fetch_positions"):
            raise NotImplementedError(f"{self.exchange_id} does not support fetch_positions")
        norm_symbols = [self.normalize_symbol(s) for s in symbols] if symbols else None
        return await self.exchange.fetch_positions(norm_symbols)

    async def create_order(self, symbol, type_, side, amount, price=None, params=None):
        params = params or {}
        return await self.exchange.create_order(self.normalize_symbol(symbol), type_, side, amount, price, params)

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
