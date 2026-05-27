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
from debug_log import log_mexc

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
        try:
            await asyncio.wait_for(self.exchange.load_markets(), timeout=float(os.getenv("MEXC_LOAD_MARKETS_TIMEOUT", "8")))
        except Exception as e:
            # MEXC public market loading can time out on some hosts/IPs.  Keep the
            # client usable for native futures private endpoints so Telegram
            # commands such as Balance/Positions/Cancel All do not freeze.
            log_mexc("GET", "load_markets", request={}, response={}, status=0, error=f"load_markets skipped: {e}")
        try:
            if hasattr(self.exchange, "load_time_difference"):
                diff = await asyncio.wait_for(self.exchange.load_time_difference(), timeout=4)
                self.time_difference_ms = int(diff or 0)
        except Exception:
            # Do not block startup; raw MEXC fallback also syncs from MEXC server time.
            pass
        try:
            await asyncio.wait_for(self._sync_mexc_time(silent=True), timeout=4)
        except Exception:
            pass
        return self

    def _split_symbol_parts(self, symbol: str) -> tuple[str, str]:
        """Return (BASE, QUOTE) from any bot/MEXC/ccxt futures spelling.

        Examples: ONDO/USDT:USDT, ONDO_USDT, ONDO-USDT, ONDOUSDT.
        Keeping this centralized prevents `Contract does not exist` caused by
        sending display symbols to native MEXC endpoints.
        """
        raw = str(symbol or "").strip().upper()
        if raw.endswith(":USDT"):
            raw = raw[:-5]
        raw = raw.replace("-", "_")
        if "/" in raw:
            base, quote = raw.split("/", 1)
            quote = quote.split(":", 1)[0] or "USDT"
        elif "_" in raw:
            base, quote = raw.split("_", 1)
        elif raw.endswith("USDT") and len(raw) > 4:
            base, quote = raw[:-4], "USDT"
        else:
            base, quote = raw, "USDT"
        return base.strip("_/:-"), (quote or "USDT").strip("_/:-")

    def normalize_symbol(self, symbol: str) -> str:
        """Return an exchange-compatible swap symbol, or a safe MEXC futures display symbol when ccxt is not initialized."""
        if not self.exchange:
            base, quote = self._split_symbol_parts(symbol)
            return f"{base}/{quote}:USDT"
        markets = getattr(self.exchange, "markets", None) or {}
        if symbol in markets:
            return symbol
        base, quote = self._split_symbol_parts(symbol)
        aliases = [
            symbol,
            f"{base}/{quote}:USDT",
            f"{base}/{quote}",
            f"{base}_USDT",
            f"{base}{quote}",
        ]
        for candidate in aliases:
            if candidate in markets:
                m = markets[candidate]
                if m.get("swap") or m.get("future") or m.get("type") in {"swap", "future"}:
                    return candidate
        for m in markets.values():
            if str(m.get("base") or "").upper() == base and str(m.get("quote") or "").upper() == quote and (m.get("swap") or m.get("future") or m.get("type") in {"swap", "future"}):
                return m["symbol"]
        raise ValueError(f"no compatible swap market for symbol {symbol}")

    def _market(self, symbol: str) -> dict:
        norm = self.normalize_symbol(symbol)
        return (getattr(self.exchange, "markets", {}) or {}).get(norm, {"symbol": norm})

    def _mexc_symbol(self, symbol: str) -> str:
        """Return the native MEXC futures contract id, e.g. XMR_USDT.

        Some ccxt market ids are plain XMR/USDT. MEXC private futures
        endpoints reject that spelling with `Contract does not exist`, so every
        private REST body must use an underscore contract id.
        """
        m = self._market(symbol)
        mid = str(m.get("id") or "")
        if mid:
            return self._mexc_normalize_contract_id(mid)
        norm = str(m.get("symbol") or self.normalize_symbol(symbol))
        return self._mexc_normalize_contract_id(norm)

    def mexc_contract_symbol(self, symbol: str) -> str:
        """Public native MEXC contract id helper, e.g. BTC_USDT.

        Use this for every native MEXC private endpoint.  Display/ccxt symbols
        like BTC/USDT:USDT are allowed inside the bot, but must never be sent
        to `/api/v1/private/*` as `symbol`.
        """
        return self._mexc_symbol(symbol)

    def mexc_symbol_variants(self, symbol: str) -> list[str]:
        """Return all symbol spellings MEXC may use for the same futures pair.

        MEXC futures can use different symbols across endpoints: ccxt symbol
        (SUI/USDT:USDT), contract id (SUI_USDT), compact (SUIUSDT), hyphen
        (SUI-USDT), and sometimes plain SUI/USDT. Keeping these variants in
        local state lets /positions match exchange rows even if one endpoint
        returns a different spelling.
        """
        out = []
        def add(x):
            x = str(x or "").strip()
            if x and x not in out:
                out.append(x)
        try:
            norm = self.normalize_symbol(symbol)
        except Exception:
            norm = str(symbol or "")
        add(symbol); add(norm)
        base, quote = self._split_symbol_parts(norm)
        base = base.upper(); quote = quote.upper()
        add(f"{base}/{quote}:USDT")
        add(f"{base}/{quote}")
        add(f"{base}_{quote}")
        add(f"{base}-{quote}")
        add(f"{base}{quote}")
        try:
            m = self._market(norm)
            add(m.get("id")); add(m.get("symbol"))
            info = m.get("info") or {}
            if isinstance(info, dict):
                for k in ("symbol", "contract", "contractName", "baseCoin", "settleCoin"):
                    if k in info and k not in {"baseCoin", "settleCoin"}:
                        add(info.get(k))
        except Exception:
            pass
        return out

    def _mexc_normalize_contract_id(self, raw: str) -> str:
        raw = str(raw or "").strip().upper().replace("-", "_").replace("/", "_")
        if raw.endswith(":USDT"):
            raw = raw[:-5]
        if "_" not in raw and raw.endswith("USDT"):
            raw = raw[:-4] + "_USDT"
        return raw

    def _mexc_variants_match(self, a: str, b: str) -> bool:
        return self._mexc_normalize_contract_id(a) == self._mexc_normalize_contract_id(b)

    def _precision_digits_from_market(self, symbol: str, kind: str, default: int) -> int:
        try:
            m = self._market(symbol)
            prec = (m.get("precision") or {}).get(kind)
            if prec is None:
                info = m.get("info") or {}
                keys = ("priceScale", "price_scale") if kind == "price" else ("volScale", "amountScale", "quantityScale")
                for k in keys:
                    if isinstance(info, dict) and info.get(k) not in (None, ""):
                        prec = int(float(info.get(k))); break
            if prec is not None:
                # ccxt may provide tick-size decimals as float. Values <1 are
                # a tick size; convert 0.0001 -> 4 digits. Integer values are
                # already digit counts.
                f = float(prec)
                if 0 < f < 1:
                    import math
                    return max(0, min(12, int(round(-math.log10(f)))))
                return max(0, min(12, int(f)))
        except Exception:
            pass
        return default

    def _mexc_price_to_precision(self, symbol: str, price: float) -> float:
        """Round MEXC contract prices to a valid tick.

        MEXC is strict for plan/TP/SL trigger prices and may reject raw Python
        values such as 2138.2175 with code 5003.  Prefer the contract tick
        (priceUnit/tickSize) and use conservative BTC/ETH fallbacks when market
        metadata is incomplete.
        """
        price = float(price or 0)
        if price <= 0:
            return 0.0
        import math
        tick = self._mexc_price_tick(symbol)
        if tick and tick > 0:
            decimals = max(0, min(12, int(round(-math.log10(tick))) if tick < 1 else 0))
            rounded = round(price / tick) * tick
            return float(f"{rounded:.{decimals}f}")
        try:
            return float(self.exchange.price_to_precision(self.normalize_symbol(symbol), price))
        except Exception:
            digits = self._precision_digits_from_market(symbol, "price", self._mexc_fallback_price_digits(price))
            # If market metadata is missing, never send 8-decimal high-price triggers
            # to MEXC stoporder/place; that caused code 2015 precision errors.
            if digits >= 8 and price >= 1:
                digits = self._mexc_fallback_price_digits(price)
            return float(f"{price:.{digits}f}")

    def _mexc_amount_to_precision(self, symbol: str, amount: float) -> float:
        amount = float(amount or 0)
        if amount <= 0:
            return 0.0
        try:
            return float(self.exchange.amount_to_precision(self.normalize_symbol(symbol), amount))
        except Exception:
            digits = self._precision_digits_from_market(symbol, "amount", 6)
            return float(f"{amount:.{digits}f}")

    def sanitize_protection_values(self, symbol: str, qty: float, stop_price: float | None = None, take_price: float | None = None) -> dict:
        """Sanitize qty/TP/SL before storage and native MEXC requests.

        This removes Python float tails like 0.38182571428571427 and prevents
        MEXC 2015 precision errors when placing protection orders.
        """
        q_prec = self._mexc_amount_to_precision(symbol, qty)
        try:
            if float(q_prec or 0) <= 0 < float(qty or 0):
                q_prec = float(qty)
        except Exception:
            pass
        out = {"qty": q_prec}
        if stop_price not in (None, ""):
            out["stop_price"] = self._mexc_price_to_precision(symbol, float(stop_price or 0))
        if take_price not in (None, ""):
            out["take_price"] = self._mexc_price_to_precision(symbol, float(take_price or 0))
        return out

    async def _mexc_reference_price(self, symbol: str, fallback: float = 0.0) -> float:
        """Best reference price for validating TP/SL trigger direction.

        MEXC rejects triggers that are already crossed or too close to mark/last.
        Prefer mark/fair price from contract detail, then ticker, then fallback.
        """
        try:
            msym = self._mexc_symbol(symbol)
            resp = await self._mexc_public("GET", f"/api/v1/contract/ticker?symbol={msym}")
            data = resp.get("data") if isinstance(resp, dict) else None
            row = data[0] if isinstance(data, list) and data else data
            if isinstance(row, dict):
                for key in ("fairPrice", "markPrice", "indexPrice", "lastPrice"):
                    val = row.get(key)
                    if val not in (None, "") and float(val) > 0:
                        return float(val)
        except Exception:
            pass
        try:
            t = await self.fetch_ticker(symbol)
            for key in ("mark", "last", "close"):
                val = t.get(key) if isinstance(t, dict) else None
                if val not in (None, "") and float(val) > 0:
                    return float(val)
        except Exception:
            pass
        return float(fallback or 0)

    def _mexc_price_tick(self, symbol: str) -> float:
        try:
            m = self._market(symbol)
            info = m.get("info") or {}
            for key in ("priceUnit", "tickSize"):
                val = info.get(key) if isinstance(info, dict) else None
                if val not in (None, "") and float(val) > 0:
                    return float(val)
            prec = (m.get("precision") or {}).get("price")
            if prec not in (None, ""):
                f = float(prec)
                return f if 0 < f < 1 else 10 ** (-int(f))
        except Exception:
            pass
        # Conservative fallbacks for MEXC USDT perpetual majors when CCXT
        # metadata is missing/stale.  This prevents 5003 from long float tails.
        try:
            ms = self._mexc_symbol(symbol).upper()
            if ms.startswith("BTC_"):
                return 0.1
            if ms.startswith("ETH_"):
                return 0.01
        except Exception:
            pass
        # Generic safe fallback when markets were not loaded (BOOST avoids load_markets
        # to stay responsive). MEXC rejects long float tails with code 2015; two
        # decimals for high-priced contracts and four decimals for cheaper ones
        # is safer than sending raw Python floats like 260.03963536.
        try:
            px = float(symbol if False else 0)
        except Exception:
            pass
        return 0.0

    def _mexc_fallback_price_digits(self, price: float) -> int:
        price = abs(float(price or 0))
        if price >= 1000:
            return 1
        if price >= 100:
            return 2
        if price >= 1:
            return 4
        return 6

    def _mexc_safe_trigger_price_sync(self, symbol: str, close_side: str, kind: str, trigger_price: float, reference_price: float = 0.0) -> float:
        """Round and keep a trigger on the correct side of a reference price.

        No minimum-distance strategy is applied here; this only prevents already
        crossed triggers and invalid precision.  close_side is the order side
        used to close the position: sell closes LONG, buy closes SHORT.
        """
        ref = float(reference_price or 0)
        px = self._mexc_price_to_precision(symbol, float(trigger_price or 0))
        if px <= 0 or ref <= 0:
            return px
        tick = self._mexc_price_tick(symbol) or max(ref * 0.000001, 1e-12)
        side_l = str(close_side or "").lower()
        kind_l = str(kind or "").lower()
        # LONG close is sell: TP above ref, SL below ref. SHORT close is buy:
        # TP below ref, SL above ref.
        want_above = (side_l == "sell" and kind_l == "tp") or (side_l == "buy" and kind_l == "sl")
        if want_above and px <= ref:
            px = ref + tick
        elif (not want_above) and px >= ref:
            px = ref - tick
        return self._mexc_price_to_precision(symbol, px)

    async def mexc_safe_tpsl_prices(self, symbol: str, side: str, stop_price: float, take_price: float, entry_price: float) -> tuple[float, float, str]:
        """Move TP/SL away from mark/last so MEXC accepts native position TP/SL.

        This mirrors the older bot that worked better: open the position first,
        read mark/position data, then only send triggers that are on the correct
        side of the current price with a tick/min-distance buffer.
        """
        ref = await self._mexc_reference_price(symbol, entry_price)
        ref = float(ref or entry_price or 0)
        tick = self._mexc_price_tick(symbol) or max(ref * 0.00001, 1e-12)
        # v0149: do not enforce a strategy-level minimum distance here.  Only
        # keep triggers one tick on the valid side unless env explicitly asks
        # for a larger safety buffer.
        min_pct = float(os.getenv("MEXC_TPSL_MIN_TRIGGER_PCT", os.getenv("PROTECTION_MIN_TRIGGER_PCT", "0"))) / 100.0
        min_ticks = int(float(os.getenv("MEXC_TPSL_MIN_TRIGGER_TICKS", os.getenv("PROTECTION_MIN_TRIGGER_TICKS", "1"))))
        min_dist = max(ref * min_pct, tick * max(1, min_ticks))
        close_side = str(side or "").lower()
        is_long = close_side == "sell"  # closing LONG is sell; closing SHORT is buy
        sl = float(stop_price or 0)
        tp = float(take_price or 0)
        changed = []
        if is_long:
            if sl > 0 and sl >= ref - min_dist:
                sl = ref - min_dist; changed.append("sl")
            if tp > 0 and tp <= ref + min_dist:
                tp = ref + min_dist; changed.append("tp")
        else:
            if sl > 0 and sl <= ref + min_dist:
                sl = ref + min_dist; changed.append("sl")
            if tp > 0 and tp >= ref - min_dist:
                tp = ref - min_dist; changed.append("tp")
        sl = self._mexc_price_to_precision(symbol, sl) if sl > 0 else 0.0
        tp = self._mexc_price_to_precision(symbol, tp) if tp > 0 else 0.0
        return sl, tp, f"ref={ref:g} tick={tick:g} min_dist={min_dist:g} adjusted={','.join(changed) or 'no'}"

    def _amount_to_mexc_vol(self, symbol: str, amount: float) -> int:
        """MEXC futures API expects integer contract volume, not base coin amount."""
        amount = float(amount or 0)
        contract_size = self._mexc_contract_size(symbol)
        if contract_size > 0:
            vol = amount / contract_size
        else:
            # Fallback for unknown contracts when metadata is incomplete.
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


    async def mexc_fetch_fee_rates(self) -> dict:
        """Best-effort personal MEXC futures fee discovery.

        Returns {"BTC/USDT:USDT": {"maker": 0.0, "taker": 0.0, "source": "..."}}.
        If MEXC changes/blocks the endpoint, returns an empty dict; boost mode
        will then refuse to trade unless BOOST_ALLOW_FEE_FALLBACK=true.
        """
        if self.exchange_id != "mexc":
            return {}
        endpoints = [
            "/api/v1/private/account/tiered_fee_rate",
            "/api/v1/private/account/fee_rate",
            "/api/v1/private/account/feeRate",
        ]
        out_rates = {}
        for ep in endpoints:
            try:
                out = await self._mexc_private_read_any_base(ep, query={})
                rows = self._mexc_rows(out.get("data"))
                if not rows and isinstance(out.get("data"), dict):
                    rows = [out.get("data")]
                for r in rows:
                    if not isinstance(r, dict):
                        continue
                    sym_raw = r.get("symbol") or r.get("contract") or r.get("contractName") or r.get("currency") or ""
                    maker = r.get("makerFeeRate", r.get("makerFee", r.get("maker", r.get("openMakerFee"))))
                    taker = r.get("takerFeeRate", r.get("takerFee", r.get("taker", r.get("openTakerFee"))))
                    try:
                        maker_f = float(maker)
                        taker_f = float(taker)
                    except Exception:
                        continue
                    sym = self._mexc_id_to_symbol(str(sym_raw)) if sym_raw else ""
                    if not sym or "/USDT" not in sym:
                        continue
                    out_rates[self.normalize_symbol(sym)] = {"maker": maker_f, "taker": taker_f, "source": ep, "raw": r}
                if out_rates:
                    return out_rates
            except Exception:
                continue
        return out_rates

    async def mexc_verified_zero_fee_symbols(self, max_symbols: int = 80, allow_fallback: bool = False, manual_symbols: str = "") -> list[str]:
        """Return symbols whose personal futures taker+maker fees are verified as zero.

        Manual symbols are accepted only as a fallback when allowed explicitly;
        by default boost mode requires API-confirmed zero fee and will stay idle
        if the exchange does not expose fee details.
        """
        rates = await self.mexc_fetch_fee_rates()
        zeros = []
        for sym, fr in rates.items():
            try:
                if abs(float(fr.get("maker", 1))) <= 1e-12 and abs(float(fr.get("taker", 1))) <= 1e-12:
                    zeros.append(self.normalize_symbol(sym))
            except Exception:
                pass
        if zeros:
            return sorted(set(zeros))[:max(1, int(max_symbols or 80))]
        if allow_fallback:
            out = []
            for x in str(manual_symbols or "").split(","):
                x = x.strip()
                if not x:
                    continue
                try:
                    out.append(self.normalize_symbol(x))
                except Exception:
                    pass
            return sorted(set(out))[:max(1, int(max_symbols or 80))]
        return []

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
        if self.exchange_id == "mexc" and self.exchange is None:
            msym = self._mexc_symbol(symbol)
            lim = max(5, min(int(limit or 20), 100))
            try:
                resp = await self._mexc_public("GET", f"/api/v1/contract/depth/{msym}", query={"limit": lim})
            except Exception:
                resp = await self._mexc_public("GET", "/api/v1/contract/depth", query={"symbol": msym, "limit": lim})
            data = resp.get("data") if isinstance(resp, dict) else resp
            def rows(key):
                raw = (data or {}).get(key) if isinstance(data, dict) else []
                out = []
                for row in raw or []:
                    try:
                        if isinstance(row, dict):
                            p = row.get("price") or row.get("p")
                            q = row.get("vol") or row.get("volume") or row.get("quantity") or row.get("q")
                        else:
                            p, q = row[0], row[1]
                        out.append([float(p), float(q)])
                    except Exception:
                        continue
                return out
            return {"symbol": self.normalize_symbol(symbol), "bids": rows("bids"), "asks": rows("asks")}
        return await self.exchange.fetch_order_book(self.normalize_symbol(symbol), limit=limit)

    async def fetch_spot_order_book(self, symbol, limit=20):
        """Public SPOT orderbook for BTC/ETH orderbook-imbalance scalping.

        The bot may trade futures, but this method reads MEXC spot depth
        (BTCUSDT/ETHUSDT).  It is intentionally public/no-auth so it does not
        touch private spot endpoints that can fail on restricted accounts.
        """
        base, quote = self._split_symbol_parts(symbol)
        spot_id = f"{base}{quote}".upper()
        lim = max(5, min(int(limit or 20), 100))
        if self.exchange_id == "mexc":
            url = f"https://api.mexc.com/api/v3/depth?symbol={spot_id}&limit={lim}"
            timeout = aiohttp.ClientTimeout(total=6)
            connector = None
            if self.proxy_enabled and self.proxy_url and ProxyConnector:
                try:
                    connector = ProxyConnector.from_url(self.proxy_url)
                except Exception:
                    connector = None
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as r:
                    txt = await r.text()
                    if r.status != 200:
                        raise RuntimeError(f"MEXC spot depth error {r.status}: {txt[:160]}")
                    data = json.loads(txt)
            def rows(key):
                out = []
                for row in data.get(key) or []:
                    try:
                        out.append([float(row[0]), float(row[1])])
                    except Exception:
                        continue
                return out
            return {"bids": rows("bids"), "asks": rows("asks"), "symbol": spot_id, "spot": True}
        # Generic fallback if another exchange supports spot symbols in ccxt.
        return await self.exchange.fetch_order_book(f"{base}/{quote}", limit=lim)

    async def fetch_ticker(self, symbol, params=None):
        if self.exchange_id == "mexc" and self.exchange is None:
            msym = self._mexc_symbol(symbol)
            resp = await self._mexc_public("GET", f"/api/v1/contract/ticker", query={"symbol": msym})
            data = resp.get("data") if isinstance(resp, dict) else None
            row = data[0] if isinstance(data, list) and data else data
            if not isinstance(row, dict):
                row = {}
            last = row.get("lastPrice") or row.get("last") or row.get("fairPrice") or row.get("indexPrice") or 0
            bid = row.get("bid1") or row.get("bid") or row.get("bidPrice") or last
            ask = row.get("ask1") or row.get("ask") or row.get("askPrice") or last
            return {
                "symbol": self.normalize_symbol(symbol),
                "last": float(last or 0), "close": float(last or 0),
                "bid": float(bid or last or 0), "ask": float(ask or last or 0),
                "quoteVolume": float(row.get("amount24") or row.get("volume24") or row.get("holdVol") or 0),
                "info": row,
            }
        return await self.exchange.fetch_ticker(self.normalize_symbol(symbol), params or {})

    async def fetch_ohlcv(self, symbol, timeframe="1m", limit=60, params=None):
        if self.exchange_id == "mexc" and self.exchange is None:
            msym = self._mexc_symbol(symbol)
            tf = str(timeframe or "1m")
            interval_map = {"1m": "Min1", "5m": "Min5", "15m": "Min15", "30m": "Min30", "1h": "Min60", "4h": "Hour4", "1d": "Day1"}
            interval = interval_map.get(tf, tf)
            lim = max(2, min(int(limit or 60), 200))
            resp = await self._mexc_public("GET", f"/api/v1/contract/kline/{msym}", query={"interval": interval, "limit": lim})
            data = resp.get("data") if isinstance(resp, dict) else resp
            rows = []
            if isinstance(data, dict) and all(k in data for k in ("time", "open", "close", "high", "low")):
                vols = data.get("vol") or data.get("volume") or [0] * len(data.get("time") or [])
                for t,o,c,h,l,v in zip(data.get("time") or [], data.get("open") or [], data.get("close") or [], data.get("high") or [], data.get("low") or [], vols):
                    try:
                        ts = int(float(t)); ts = ts*1000 if ts < 10_000_000_000 else ts
                        rows.append([ts, float(o), float(h), float(l), float(c), float(v or 0)])
                    except Exception:
                        continue
            elif isinstance(data, list):
                for r in data:
                    try:
                        ts,o,h,l,c,v = r[0], r[1], r[2], r[3], r[4], (r[5] if len(r)>5 else 0)
                        ts = int(float(ts)); ts = ts*1000 if ts < 10_000_000_000 else ts
                        rows.append([ts, float(o), float(h), float(l), float(c), float(v or 0)])
                    except Exception:
                        continue
            return rows[-lim:]
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



    async def _mexc_public(self, method: str, path: str, query: dict | None = None, base_url: str | None = None):
        """Small native MEXC futures public REST helper.

        Used by BOOST when ccxt market initialization is slow/unavailable.
        """
        method = str(method or "GET").upper()
        query = dict(query or {})
        base = (base_url or self._mexc_rest_base()).rstrip("/")
        qs = urlencode(sorted((k, v) for k, v in query.items() if v is not None))
        url = f"{base}{path}" + (f"?{qs}" if qs else "")
        connector, proxy_arg = self._proxy_connector_and_arg()
        timeout = aiohttp.ClientTimeout(total=float(os.getenv("MEXC_PUBLIC_TIMEOUT", "6") or 6))
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            async with session.request(method, url, headers={"User-Agent": "Mozilla/5.0"}, proxy=proxy_arg) as r:
                text = await r.text()
                try:
                    out = json.loads(text)
                except Exception:
                    out = {"raw": text}
                log_mexc(method, path, request={"query": query}, response=out, status=r.status)
                if r.status >= 400 or (isinstance(out, dict) and out.get("success") is False):
                    raise RuntimeError(f"MEXC public HTTP {r.status}: {str(out)[:240]}")
                return out

    def _mexc_rest_base(self) -> str:
        """Base URL for MEXC futures private REST.

        Private trading requests must not use contract.mexc.com: that host can
        return CDN 403 Access Denied for order/cancel/close endpoints. Even if
        an old .env still contains contract.mexc.com, force the supported
        private REST host to api.mexc.com. WebSocket may still use
        wss://contract.mexc.com/edge separately.
        """
        base = os.getenv("MEXC_FUTURES_REST_BASE", "https://api.mexc.com").rstrip("/")
        if "contract.mexc.com" in base:
            return "https://api.mexc.com"
        return base or "https://api.mexc.com"

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
        body = dict(body or {})
        query = dict(query or {})
        # Native MEXC futures private endpoints require underscore contract ids
        # such as BTC_USDT.  Normalize at the final request boundary so no
        # caller can accidentally send a display/ccxt symbol like BTC/USDT.
        if body.get("symbol") not in (None, ""):
            body["symbol"] = self._mexc_normalize_contract_id(body.get("symbol"))
        if query.get("symbol") not in (None, ""):
            query["symbol"] = self._mexc_normalize_contract_id(query.get("symbol"))
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
                log_mexc(method, path, request={"body": body, "query": query}, response=out, status=r.status)
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
                        log_mexc(method, path, request={"body": body, "query": query, "retry": True}, response=out, status=r2.status)
                        if r2.status >= 400 or out.get("success") is False:
                            log_mexc(method, path, request={"body": body, "query": query, "retry": True}, response=out, status=r2.status, error=out)
                            raise RuntimeError(f"HTTP {r2.status}: {out}")
                        return out
                if r.status >= 400 or out.get("success") is False:
                    log_mexc(method, path, request={"body": body, "query": query}, response=out, status=r.status, error=out)
                    raise RuntimeError(f"HTTP {r.status}: {out}")
                return out

    async def _mexc_private_read_any_base(self, path: str, query: dict | None = None):
        """Read native MEXC futures state from supported hosts.

        Private MEXC REST is pinned to api.mexc.com-compatible hosts. Do not
        fall back to contract.mexc.com here: private order endpoints on that
        host can fail with CDN 403 and leak confusing errors into /close_all.
        """
        bases = []
        for b in (self._mexc_rest_base(), "https://api.mexc.com"):
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
            for key in ("list", "result", "resultList", "data", "rows", "items"):
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
        norm_raw = self._mexc_normalize_contract_id(raw)
        for m in markets.values():
            ids = {
                self._mexc_normalize_contract_id(m.get("id")),
                self._mexc_normalize_contract_id(m.get("symbol")),
            }
            info = m.get("info") or {}
            if isinstance(info, dict):
                ids.add(self._mexc_normalize_contract_id(info.get("symbol")))
                ids.add(self._mexc_normalize_contract_id(info.get("contract")))
                ids.add(self._mexc_normalize_contract_id(info.get("contractName")))
            if norm_raw in ids:
                return str(m.get("symbol") or raw)
        if "_" in norm_raw:
            base, quote = norm_raw.split("_", 1)
            for candidate in (f"{base}/{quote}:USDT", f"{base}/{quote}"):
                if candidate in markets:
                    return candidate
            return f"{base}/{quote}:USDT"
        return raw

    def _mexc_contract_size(self, symbol: str) -> float:
        """Return MEXC futures contract size in base coin.

        MEXC position/order APIs use integer contracts (holdVol/vol).  If ccxt
        market metadata is missing contractSize, falling back to 1 incorrectly
        treats 13 BTC_USDT contracts as 13 BTC.  Hard-code the major MEXC
        contracts used by AI scalping so qty/notional/protection volume stay
        correct even when metadata is incomplete.
        """
        try:
            m = self._market(symbol)
            cs = float(m.get("contractSize") or m.get("contract_size") or 0)
            if cs > 0:
                return cs
        except Exception:
            pass
        sid = self._mexc_normalize_contract_id(symbol)
        fallback = {
            "BTC_USDT": 0.0001,
            "ETH_USDT": 0.01,
        }
        return float(fallback.get(sid, 0.0))

    def _mexc_contracts_to_amount(self, symbol: str, contracts: float) -> float:
        cs = self._mexc_contract_size(symbol)
        if cs > 0:
            return abs(float(contracts or 0)) * cs
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
        variants = self.mexc_symbol_variants(symbol)
        if mexc_symbol and mexc_symbol not in variants:
            variants.append(mexc_symbol)
        return {
            "symbol": symbol,
            "mexc_symbol": self._mexc_normalize_contract_id(mexc_symbol),
            "symbol_variants": variants,
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
        wanted_variants = set()
        if symbols:
            for sym in list(symbols):
                for v in self.mexc_symbol_variants(sym):
                    wanted_variants.add(self._mexc_normalize_contract_id(v))
                    # Query each MEXC-style contract id variant; unsupported
                    # variants are ignored by the endpoint loop below.
                    if "_" in self._mexc_normalize_contract_id(v):
                        queries.append({"symbol": self._mexc_normalize_contract_id(v)})
        all_rows = []
        raw_meta = []
        errors = []
        # Query all known current-position variants. MEXC accounts can return
        # empty rows from one endpoint while account/assets shows positionMargin.
        endpoints = [
            "/api/v1/private/position/open_positions",
        ]
        for query in queries:
            for endpoint in endpoints:
                try:
                    out = await self._mexc_private_read_any_base(endpoint, query=query)
                    raw_meta.append({"base": out.get("_base_url"), "query": query, "endpoint": endpoint})
                    all_rows.extend([r for r in self._mexc_rows(out.get("data")) if isinstance(r, dict)])
                except Exception as e:
                    errors.append(f"{endpoint}: {e}")
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
            def matches_requested(p):
                vals = set()
                vals.add(self._mexc_normalize_contract_id(p.get("symbol")))
                vals.add(self._mexc_normalize_contract_id(p.get("mexc_symbol")))
                for v in p.get("symbol_variants") or []:
                    vals.add(self._mexc_normalize_contract_id(v))
                info = p.get("info") or {}
                if isinstance(info, dict):
                    vals.add(self._mexc_normalize_contract_id(info.get("symbol")))
                    vals.add(self._mexc_normalize_contract_id(info.get("contract")))
                return bool(vals & wanted_variants)
            parsed = [p for p in parsed if matches_requested(p)]
        for p in parsed:
            p.setdefault("sync_meta", raw_meta[:3])
        if not parsed and errors:
            # Preserve a lightweight debug trail for callers that surface errors.
            return []
        return parsed

    def _mexc_parse_order(self, row: dict) -> dict:
        symbol = self._mexc_id_to_symbol(str(row.get("symbol") or row.get("contract") or ""))
        oid = str(row.get("orderId") or row.get("id") or row.get("planOrderId") or row.get("stopOrderId") or row.get("externalOid") or "")
        # Normalize order side to the actual CLOSE direction expected by
        # ProtectionEngine (LONG must be closed with sell, SHORT with buy).
        # MEXC uses different numeric fields across endpoints:
        #   order/plan side: 1 open long, 2 close short, 3 open short, 4 close long
        #   stoporder/position rows may expose positionType/holdSide instead:
        #       1/long = a LONG position, so its TP/SL close side is sell
        #       2/short = a SHORT position, so its TP/SL close side is buy
        src = str(row.get("_source_endpoint") or "").lower()
        side_raw = str(row.get("side") or "").lower()
        pos_raw = str(row.get("positionType") or row.get("holdSide") or "").lower()
        if side_raw in {"2", "buy"}:
            side = "buy"
        elif side_raw in {"4", "sell"}:
            side = "sell"
        elif side_raw == "1":
            side = "buy"
        elif side_raw == "3":
            side = "sell"
        elif pos_raw in {"1", "long"}:
            side = "sell" if ("stoporder" in src or "tpsl" in src or "position/stop" in src) else "buy"
        elif pos_raw in {"2", "short"}:
            side = "buy" if ("stoporder" in src or "tpsl" in src or "position/stop" in src) else "sell"
        else:
            side = ""
        vol = row.get("vol") or row.get("remainVol") or row.get("volume") or row.get("holdVol") or row.get("takeProfitVol") or row.get("stopLossVol") or 0
        price = 0.0
        for key in ("price", "executePrice", "triggerPrice", "stopPrice", "takeProfitPrice", "stopLossPrice", "takeProfitOrderPrice", "stopLossOrderPrice"):
            try:
                if row.get(key) not in (None, "", 0, "0"):
                    price = float(row.get(key)); break
            except Exception:
                pass
        typ = row.get("type") or row.get("orderType") or row.get("category")
        src = str(row.get("_source_endpoint") or "")
        protection_kind = str(row.get("_protection_kind") or "").lower()
        if protection_kind in {"tp", "sl"}:
            typ = f"tpsl_{protection_kind}"
        elif not typ and "stoporder" in src and (row.get("takeProfitPrice") or row.get("stopLossPrice")):
            typ = "tpsl"
        return {
            "id": oid,
            "symbol": symbol,
            "side": side,
            "type": typ or "unknown",
            "price": price,
            "amount": self._mexc_contracts_to_amount(symbol, float(vol or 0)),
            "remaining": self._mexc_contracts_to_amount(symbol, float(row.get("remainVol") or row.get("realityVol") or vol or 0)),
            "status": "open",
            "clientOrderId": row.get("externalOid"),
            "info": row,
        }

    def _mexc_expand_tpsl_row(self, row: dict) -> list[dict]:
        """Split MEXC combined TP/SL rows into explicit pseudo-orders.

        /api/v1/private/stoporder/open_orders returns one row that can contain
        both takeProfitPrice and stopLossPrice. The protection checker expects
        separate TP and SL candidates, so expose both while preserving the raw
        row in info.
        """
        out = []
        base_id = str(row.get("id") or row.get("orderId") or row.get("positionId") or "")
        if row.get("takeProfitPrice") not in (None, "", 0, "0"):
            tp = dict(row)
            tp["id"] = f"{base_id}:TP" if base_id else "TP"
            tp["orderId"] = tp["id"]
            tp["_protection_kind"] = "tp"
            tp["_protection_price"] = row.get("takeProfitPrice")
            tp["triggerPrice"] = row.get("takeProfitPrice")
            out.append(tp)
        if row.get("stopLossPrice") not in (None, "", 0, "0"):
            sl = dict(row)
            sl["id"] = f"{base_id}:SL" if base_id else "SL"
            sl["orderId"] = sl["id"]
            sl["_protection_kind"] = "sl"
            sl["_protection_price"] = row.get("stopLossPrice")
            sl["triggerPrice"] = row.get("stopLossPrice")
            out.append(sl)
        return out or [row]

    async def _mexc_fetch_open_orders(self, symbol=None):
        """Fetch normal open orders plus trigger/TP-SL style orders.

        MEXC can reserve balance in plan/stop/TP-SL orders that are not returned
        by the normal open_orders endpoint. Include all known current-order
        endpoints and ignore unsupported variants instead of hiding normal data.
        """
        candidates = []
        # v0224 fast path: /stoporder/list/orders repeatedly returns [] on current
        # MEXC futures and adds ~1-2s latency per protection/rotation check.
        # Keep the live TP/SL endpoints that matter now: planorder emergency SL,
        # stoporder/open_orders. Legacy stoporder/list
        # can be re-enabled only if MEXC changes visibility again.
        include_legacy_stop_list = str(os.getenv("MEXC_LEGACY_STOPORDER_LIST", "0")).lower() in {"1", "true", "yes", "on"}
        if symbol:
            msym = self._mexc_symbol(symbol)
            candidates.extend([
                ("/api/v1/private/order/list/open_orders/" + msym, {}),
                ("/api/v1/private/order/list/open_orders", {"symbol": msym}),
                ("/api/v1/private/planorder/list/orders", {"symbol": msym, "state": 1, "page_num": 1, "page_size": 100}),
                ("/api/v1/private/planorder/list/orders", {"symbol": msym, "is_finished": 0, "page_num": 1, "page_size": 100}),
                ("/api/v1/private/stoporder/open_orders", {"symbol": msym}),
            ])
            if include_legacy_stop_list:
                candidates.extend([
                    ("/api/v1/private/stoporder/list/orders", {"symbol": msym, "state": 1, "is_finished": 0, "page_num": 1, "page_size": 100}),
                    ("/api/v1/private/stoporder/list/orders", {"symbol": msym, "is_finished": 0, "page_num": 1, "page_size": 100}),
                ])
        else:
            candidates.extend([
                ("/api/v1/private/order/list/open_orders", {}),
                ("/api/v1/private/planorder/list/orders", {"state": 1, "page_num": 1, "page_size": 100}),
                ("/api/v1/private/planorder/list/orders", {"is_finished": 0, "page_num": 1, "page_size": 100}),
                ("/api/v1/private/stoporder/open_orders", {}),
            ])
            if include_legacy_stop_list:
                candidates.extend([
                    ("/api/v1/private/stoporder/list/orders", {"state": 1, "is_finished": 0, "page_num": 1, "page_size": 100}),
                    ("/api/v1/private/stoporder/list/orders", {"is_finished": 0, "page_num": 1, "page_size": 100}),
                ])
        orders = []
        errors = []
        for path, query in candidates:
            try:
                out = await self._mexc_private_read_any_base(path, query=query)
                rows = [r for r in self._mexc_rows(out.get("data")) if isinstance(r, dict)]
                for r in rows:
                    r.setdefault("_source_endpoint", path)
                    for expanded in self._mexc_expand_tpsl_row(r):
                        expanded.setdefault("_source_endpoint", path)
                        orders.append(self._mexc_parse_order(expanded))
            except Exception as e:
                errors.append(f"{path}: {e}")
        # De-duplicate across endpoints. MEXC often exposes the same TP/SL via
        # stoporder/open_orders; /open_orders should not
        # count the same protection twice. Keep TP and SL separate when a combined
        # row was expanded.
        unique = []
        seen = set()
        for o in orders:
            if symbol and o.get("symbol") != self.normalize_symbol(symbol):
                continue
            info = o.get("info") if isinstance(o.get("info"), dict) else {}
            kind = str(info.get("_protection_kind") or "").lower()
            raw_id = str(o.get("id") or info.get("orderId") or info.get("planOrderId") or info.get("stopOrderId") or info.get("positionId") or "")
            base_id = raw_id.split(":", 1)[0]
            price_key = round(float(o.get("price") or info.get("triggerPrice") or info.get("takeProfitPrice") or info.get("stopLossPrice") or 0), 12)
            amount_key = round(float(o.get("amount") or 0), 12)
            key = (o.get("symbol"), base_id, kind, str(o.get("side") or ""), str(o.get("type") or ""), price_key, amount_key)
            if key in seen:
                continue
            seen.add(key)
            unique.append(o)
        return unique


    async def mexc_find_active_plan_order(self, symbol: str, order_id: str = "", external_oid: str = "") -> dict:
        """Return active MEXC futures planorder by id/externalOid for a symbol.

        BOOST emergency SL is created through /planorder/place.  It is not
        visible in /stoporder/open_orders, so the protection watchdog must
        verify it against planorder/list/orders directly before declaring the
        position UNSAFE or canceling/recreating protection.
        """
        if self.exchange_id != "mexc":
            return {}
        msym = self._mexc_symbol(symbol)
        oid = str(order_id or "").strip()
        ext = str(external_oid or "").strip()
        queries = [
            {"symbol": msym, "state": 1, "page_num": 1, "page_size": 100},
            {"symbol": msym, "is_finished": 0, "page_num": 1, "page_size": 100},
            {"state": 1, "page_num": 1, "page_size": 100},
            {"is_finished": 0, "page_num": 1, "page_size": 100},
        ]
        last_err = ""
        for query in queries:
            try:
                out = await self._mexc_private_read_any_base("/api/v1/private/planorder/list/orders", query=query)
                rows = [r for r in self._mexc_rows(out.get("data")) if isinstance(r, dict)]
                for row in rows:
                    sym_ok = self._mexc_normalize_contract_id(row.get("symbol") or row.get("contract")) == self._mexc_normalize_contract_id(msym)
                    if not sym_ok:
                        continue
                    row_ids = {str(row.get(k) or "").strip() for k in ("orderId", "id", "planOrderId")}
                    row_ext = str(row.get("externalOid") or row.get("clientOrderId") or "").strip()
                    state = str(row.get("state", "1")).lower()
                    finished = str(row.get("is_finished", row.get("isFinished", 0))).lower()
                    active = state in {"1", "", "created", "wait", "pending"} and finished in {"0", "false", "", "none"}
                    txt = " ".join(str(row.get(k) or "").lower() for k in ("externalOid", "clientOrderId", "orderType", "type", "side", "reduceOnly"))
                    reduce_ok = str(row.get("reduceOnly") or row.get("reduce_only") or "").lower() in {"1", "true", "yes"}
                    id_ok = (oid and oid in row_ids) or (ext and ext == row_ext)
                    bot_sl_ok = (not oid and not ext and ("bot_sl" in txt or reduce_ok or str(row.get("side") or "") in {"2", "4"}))
                    if active and (id_ok or bot_sl_ok):
                        row = dict(row)
                        row["_source_endpoint"] = "/api/v1/private/planorder/list/orders"
                        row["_protection_endpoint"] = "planorder"
                        return row
            except Exception as e:
                last_err = str(e)[:220]
        if last_err:
            try:
                from debug_log import log_event
                log_event("mexc_planorder_verify_error", symbol=symbol, order_id=oid, external_oid=ext, error=last_err, ok=False)
            except Exception:
                pass
        return {}

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
        # Only call normal/plan cancel_all here.  MEXC TP/SL stoporder cancel_all
        # is account-sensitive and may return code 600 Parameter error when no
        # compatible TP/SL rows exist.  TP/SL rows discovered from
        # /stoporder/open_orders are still cancelled individually below by the
        # exact stoporder/cancel endpoint, so we avoid noisy false errors without
        # skipping real discovered protection cleanup.
        cancel_paths = [
            ("/api/v1/private/order/cancel_all", "POST"),
            ("/api/v1/private/planorder/cancel_all", "POST"),
        ]
        for sym in seen:
            msym = self._mexc_symbol(sym)
            for path, method in cancel_paths:
                try:
                    out = await self._mexc_private(method, path, body={"symbol": msym})
                    results.append({"symbol": self.normalize_symbol(sym), "mexc_symbol": msym, "endpoint": path, "result": out})
                except Exception as e:
                    errors.append({"symbol": sym, "mexc_symbol": msym, "endpoint": path, "error": str(e)})
        # Fallback: cancel individual discovered normal/plan/stop orders. This
        # releases frozen balance when cancel_all misses an endpoint variant.
        try:
            discovered = await self._mexc_fetch_open_orders(symbol)
            for o in discovered:
                info = o.get("info") if isinstance(o.get("info"), dict) else {}
                oid = str(o.get("id") or info.get("orderId") or info.get("planOrderId") or info.get("stopOrderId") or "")
                # Expanded TP/SL pseudo ids look like "123:TP"/"123:SL";
                # MEXC cancel endpoints need the real base order id only.
                oid = oid.split(":", 1)[0].strip()
                if not oid:
                    continue
                msym = self._mexc_symbol(o.get("symbol") or symbol)
                src = str(info.get("_source_endpoint") or "").lower()
                # Do not try every cancel endpoint for every id: MEXC returns
                # code 600 Parameter error when a normal/plan id is sent to
                # stoporder/cancel. Route the id back to the endpoint family
                # where it was discovered, and only use normal order cancel as
                # the safe default when the source is unknown.
                if "planorder" in src:
                    candidates = [("/api/v1/private/planorder/cancel", {"symbol": msym, "orderId": oid})]
                elif "stoporder" in src:
                    candidates = [("/api/v1/private/stoporder/cancel", {"symbol": msym, "orderId": oid})]
                else:
                    candidates = [("/api/v1/private/order/cancel", {"symbol": msym, "orderId": oid})]
                for path, body in candidates:
                    try:
                        out = await self._mexc_private("POST", path, body=body)
                        results.append({"symbol": o.get("symbol"), "mexc_symbol": msym, "order_id": oid, "endpoint": path, "result": out})
                        break
                    except Exception as e:
                        # Unsupported endpoint/order type is expected; keep only
                        # the last compact error if every candidate fails.
                        last_err = str(e)
                else:
                    errors.append({"symbol": o.get("symbol"), "mexc_symbol": msym, "order_id": oid, "endpoint": "individual_cancel", "error": last_err[:220]})
        except Exception as e:
            errors.append({"symbol": symbol or "*", "endpoint": "individual_cancel_discovery", "error": str(e)[:220]})

        if not seen and not symbol:
            # Always try BTC as an emergency default because the BTC AI module can
            # have planorders even when discovery is temporarily empty/rate-limited.
            for msym in ["BTC_USDT"]:
                for path, method in cancel_paths:
                    try:
                        out = await self._mexc_private(method, path, body={"symbol": msym})
                        results.append({"symbol": "BTC/USDT:USDT", "mexc_symbol": msym, "endpoint": path, "result": out, "fallback": True})
                    except Exception as e:
                        errors.append({"symbol": "BTC_USDT", "mexc_symbol": msym, "endpoint": path, "error": str(e)[:220], "fallback": True})
        return {"ok": len(errors) == 0 or len(results) > 0, "cancelled_symbols": len(results), "results": results, "errors": errors}


    async def mexc_close_position_market_native(self, pos: dict) -> dict:
        """Close one MEXC futures position using native order/create.

        This avoids ccxt reduceOnly routing issues and uses the exact MEXC
        side codes from the raw open_positions row:
        positionType 1 = long => side 4 close long; positionType 2 = short => side 2 close short.
        """
        info = pos.get("info", {}) if isinstance(pos.get("info"), dict) else {}
        symbol = info.get("symbol") or pos.get("mexc_symbol") or pos.get("symbol")
        msym = self._mexc_normalize_contract_id(symbol)
        if not msym or "_" not in msym:
            msym = self._mexc_symbol(pos.get("symbol") or symbol)
        raw_vol = info.get("holdVol") or info.get("vol") or pos.get("contracts")
        try:
            raw_f = abs(float(raw_vol or 0))
        except Exception:
            raw_f = 0.0
        # MEXC order/create expects integer contract volume. Native position rows
        # usually expose holdVol as contracts (for SPACEX this can be 11 while
        # base amount is 0.011). Some fallback rows expose only base amount; in
        # that case convert amount -> contracts instead of int(0.011) == 0.
        if raw_f >= 1:
            vol = int(round(raw_f))
        else:
            amount_f = 0.0
            for k in ("amount", "qty", "size"):
                try:
                    if pos.get(k) not in (None, ""):
                        amount_f = abs(float(pos.get(k) or 0)); break
                except Exception:
                    pass
            if amount_f <= 0 and raw_f > 0:
                amount_f = raw_f
            vol = self._amount_to_mexc_vol(self._mexc_id_to_symbol(msym), amount_f) if amount_f > 0 else 0
        if vol <= 0:
            raise RuntimeError(f"cannot close MEXC position: empty holdVol/rawVol={raw_vol!r}")
        pt = str(info.get("positionType") or info.get("holdSide") or pos.get("side") or "").lower()
        # MEXC side: 2 closes short, 4 closes long.
        if pt in {"1", "long", "buy"} or str(pos.get("side", "")).lower() == "long":
            close_side = 4
        elif pt in {"2", "short", "sell"} or str(pos.get("side", "")).lower() == "short":
            close_side = 2
        else:
            raise RuntimeError(f"cannot infer MEXC close side from positionType={pt!r}")
        # For a close order, MEXC docs mark leverage as optional and required
        # only when opening.  Sending an old/default leverage can make an
        # otherwise valid panic close fail with leverage mismatch, so the first
        # payload deliberately omits leverage.
        base_body = {
            "symbol": msym,
            "price": 0,
            "vol": vol,
            "side": close_side,
            "type": 5,
            "openType": int(info.get("openType") or os.getenv("MEXC_ORDER_OPEN_TYPE", "1") or "1"),
        }
        # Some MEXC hedge-mode accounts require positionId for an unambiguous
        # panic close.  Passing it when available keeps the close tied to the
        # exact position row and prevents a no-op close order.
        pid = info.get("positionId") or info.get("id") or pos.get("positionId")
        if pid not in (None, ""):
            try:
                base_body["positionId"] = int(float(pid))
            except Exception:
                base_body["positionId"] = pid

        attempts = []
        errors = []
        variants = []

        def add_variant(d: dict):
            # Never mutate caller dictionaries and never send empty None fields.
            clean = {k: v for k, v in dict(d).items() if v not in (None, "")}
            variants.append(clean)

        open_types = []
        for ot in (base_body.get("openType"), info.get("openType"), os.getenv("MEXC_ORDER_OPEN_TYPE"), 1, 2):
            try:
                oti = int(float(ot))
                if oti in (1, 2) and oti not in open_types:
                    open_types.append(oti)
            except Exception:
                pass
        if not open_types:
            open_types = [1, 2]

        leverage_candidates = []
        for lv in (info.get("leverage"), os.getenv("MEXC_ORDER_LEVERAGE"), 10, 5, 20, 1):
            try:
                lvi = int(float(lv))
                if lvi > 0 and lvi not in leverage_candidates:
                    leverage_candidates.append(lvi)
            except Exception:
                pass

        vol_candidates = []
        for vv in (vol, str(vol)):
            if vv not in vol_candidates:
                vol_candidates.append(vv)

        # 1) documented close by position: no leverage, no reduceOnly.
        # 2) same without positionId for accounts that reject positionId.
        # 3) flashClose/marketCeiling variants for MEXC accounts that need the
        #    emergency-close flags.
        # 4) reduceOnly variants only last, because reduceOnly is one-way only.
        for ot in open_types:
            for vv in vol_candidates:
                b = dict(base_body, openType=ot, vol=vv)
                add_variant(b)
                if "positionId" in b:
                    c = dict(b); c.pop("positionId", None); add_variant(c)
                f = dict(b); f["flashClose"] = True; add_variant(f)
                m = dict(b); m["marketCeiling"] = True; add_variant(m)
        for lv in leverage_candidates:
            for ot in open_types:
                b = dict(base_body, openType=ot, leverage=lv)
                add_variant(b)
                if "positionId" in b:
                    c = dict(b); c.pop("positionId", None); add_variant(c)
        for ot in open_types:
            rb = dict(base_body, openType=ot, reduceOnly=True)
            add_variant(rb)
            if "positionId" in rb:
                c = dict(rb); c.pop("positionId", None); add_variant(c)

        seen_payloads = set()
        for b in variants:
            key = json.dumps(b, sort_keys=True, default=str)
            if key in seen_payloads:
                continue
            seen_payloads.add(key)
            try:
                out = await self._mexc_private("POST", "/api/v1/private/order/create", body=b)
                attempts.append({"body": b, "result": out})
                return {"ok": True, "symbol": self._mexc_id_to_symbol(msym), "mexc_symbol": msym, "vol": vol, "side": close_side, "positionId": pid, "attempts": attempts, "result": out}
            except Exception as e:
                errors.append({"body": b, "error": str(e)[:260]})
                # If the error is a leverage mismatch, continue to the no/other
                # leverage variants; otherwise keep sweeping through safe close
                # variants because side 2/4 cannot open a position.
        raise RuntimeError("MEXC native close failed: " + json.dumps(errors[-6:], ensure_ascii=False))


    def _mexc_position_qty_contracts_from_parsed(self, pos: dict) -> int:
        """Return exact MEXC contract volume from a parsed/open position."""
        info = pos.get("info") if isinstance(pos.get("info"), dict) else {}
        raw_vol = info.get("holdVol") or info.get("vol") or pos.get("contracts")
        try:
            raw_f = abs(float(raw_vol or 0))
        except Exception:
            raw_f = 0.0
        if raw_f >= 1:
            return max(1, int(round(raw_f)))
        amount_f = 0.0
        for key in ("amount", "qty", "size"):
            try:
                if pos.get(key) not in (None, ""):
                    amount_f = abs(float(pos.get(key) or 0)); break
            except Exception:
                pass
        if amount_f <= 0 and raw_f > 0:
            amount_f = raw_f
        if amount_f > 0:
            return self._amount_to_mexc_vol(pos.get("symbol") or info.get("symbol") or "", amount_f)
        return 0

    def _mexc_position_side_matches(self, row: dict, side: str | None) -> bool:
        want = str(side or "").lower()
        if not want:
            return True
        info = row.get("info") if isinstance(row.get("info"), dict) else {}
        vals = {str(v).strip().lower() for v in (row.get("side"), row.get("positionSide"), info.get("side"), info.get("positionSide"), info.get("positionType")) if v not in (None, "")}
        if not vals:
            return True
        if want in {"long", "buy"}:
            return bool(vals & {"long", "buy", "bid", "1"})
        if want in {"short", "sell"}:
            return bool(vals & {"short", "sell", "ask", "2"})
        return True

    async def mexc_find_open_position(self, symbol: str, side: str | None = None) -> dict:
        """Find the live MEXC open position row for symbol/side.

        TP/SL-by-position requires positionId and exact holdVol from
        /api/v1/private/position/open_positions, not local cached qty.
        """
        positions = await self.fetch_positions([symbol])
        want = str(side or "").lower()
        last_seen = None
        for p in positions or []:
            if not self._mexc_position_side_matches(p, want):
                continue
            info = p.get("info") if isinstance(p.get("info"), dict) else {}
            pid = info.get("positionId") or info.get("position_id") or info.get("id") or p.get("id")
            vol = self._mexc_position_qty_contracts_from_parsed(p)
            last_seen = {"pid": pid, "vol": vol, "side": p.get("side") or info.get("positionType")}
            if pid and vol > 0:
                return p
        raise RuntimeError(f"live MEXC position not found for {symbol} side={side or '*'} last_seen={last_seen}")

    async def mexc_place_tpsl_by_position(self, symbol: str, side: str, qty: float, stop_price: float, take_price: float, client_order_id: str = "", live_position: dict | None = None) -> dict:
        """Place real MEXC TP/SL attached to an existing position.

        This uses the documented `/api/v1/private/stoporder/place` endpoint,
        which requires `positionId` and creates TP/SL *by position*.  It is
        different from generic `/planorder/place`; plan orders can exist as
        standalone trigger orders and are easier to mismatch with position side,
        quantity, or symbol format.
        """
        if self.exchange_id != "mexc":
            raise NotImplementedError("native MEXC TP/SL-by-position is MEXC only")
        side_l = str(side or "").lower()
        # v0160: use the already confirmed live position row from execution_engine
        # when available.  The previous code re-fetched the position here; on MEXC
        # that extra fetch can race the just-opened position and fail before any
        # /stoporder/place request is sent, so the bot immediately closed without
        # ever trying native TP/SL.  Passing live_position makes the native TP/SL
        # POST deterministic and visible in /log.
        live_pos = live_position if isinstance(live_position, dict) and live_position else None
        if not live_pos:
            live_pos = await self.mexc_find_open_position(symbol, "long" if side_l == "sell" else "short")
        info = live_pos.get("info") if isinstance(live_pos.get("info"), dict) else {}
        pid = info.get("positionId") or info.get("id")
        vol = self._mexc_position_qty_contracts_from_parsed(live_pos)
        if not pid or vol <= 0:
            raise RuntimeError(f"cannot place TP/SL: missing positionId/holdVol pid={pid!r} vol={vol!r}")
        entry = 0.0
        try:
            entry = float(live_pos.get("entryPrice") or live_pos.get("average") or info.get("holdAvgPrice") or info.get("openAvgPrice") or 0)
        except Exception:
            entry = 0.0
        safe_sl, safe_tp, safe_msg = await self.mexc_safe_tpsl_prices(symbol, side, float(stop_price), float(take_price), entry)
        # v0162: mirror the working bot's MEXC native TP/SL payload.
        # Use stoporder/place by positionId with market TP/SL. Do NOT send
        # takeProfitOrderPrice/stopLossOrderPrice for market TP/SL; on some
        # MEXC accounts those zero limit-price fields make the native TP/SL row
        # fail or not appear.  lossTrend/profitTrend are price source selectors,
        # not LONG/SHORT direction.
        safe_sl = self._mexc_price_to_precision(symbol, safe_sl)
        safe_tp = self._mexc_price_to_precision(symbol, safe_tp)
        # vol from live position is already MEXC contract volume (holdVol).
        # Do not convert contracts to contracts again.
        body = {
            "symbol": self._mexc_symbol(symbol),
            "positionId": int(float(pid)),
            "vol": str(vol),
            "lossTrend": 1,
            "profitTrend": 1,
            "volType": 2,
            "profitLossVolType": "SAME",
            "takeProfitType": 0,
            "stopLossType": 0,
            "takeProfitReverse": 2,
            "stopLossReverse": 2,
            "priceProtect": 0,
        }
        if float(safe_sl or 0) > 0:
            body["stopLossPrice"] = str(safe_sl)
        if float(safe_tp or 0) > 0:
            body["takeProfitPrice"] = str(safe_tp)
        try:
            from debug_log import log_event
            log_event("mexc_stoporder_place_body", symbol=self._mexc_symbol(symbol), side=side, positionId=pid, vol=vol, body=body)
        except Exception:
            pass
        out = await self._mexc_private("POST", "/api/v1/private/stoporder/place", body=body)
        data = out.get("data") if isinstance(out, dict) else None
        oid = data.get("id") if isinstance(data, dict) else data
        return {
            "id": str(oid or ""),
            "symbol": self.normalize_symbol(symbol),
            "type": "position_tpsl",
            "side": side,
            "amount": qty,
            "price": None,
            "info": {"native_mexc_position_tpsl": True, "positionId": pid, "vol": vol, "safe_tpsl": safe_msg, "safe_stop_price": safe_sl, "safe_take_price": safe_tp, **(out if isinstance(out, dict) else {"raw": out})},
        }

    async def mexc_close_all_positions_native(self):
        """Emergency exchange-side close all positions endpoint.

        MEXC close_all is not always enough by itself, so keep this method as
        a thin native endpoint call and use mexc_hard_close_all_positions() for
        operator panic commands.
        """
        if self.exchange_id != "mexc":
            raise NotImplementedError("native close_all is MEXC only")
        return await self._mexc_private("POST", "/api/v1/private/position/close_all", body={})

    async def mexc_hard_close_all_positions(self, symbols: list[str] | None = None, retries: int = 3) -> dict:
        """HARD panic-close MEXC futures positions and all child orders.

        This method is intentionally exchange-native and verification-driven:
        1) read real open_positions from MEXC,
        2) submit market CLOSE orders using exact holdVol contracts,
        3) if MEXC temporarily hides positions while account margin remains,
           run a BTC safety sweep using estimated contract volume,
        4) cancel normal/plan/stop orders after close,
        5) verify open_positions + account margin.

        It does not trust local bot cache. It is used by /close all only.
        """
        if self.exchange_id != "mexc":
            raise NotImplementedError("hard close is MEXC only")
        import asyncio, math
        wanted = list(symbols) if symbols else []  # empty means ALL discovered positions
        results: list[dict] = []
        errors: list[str] = []
        snapshots: list[dict] = []

        async def _positions_for(sym_list: list[str] | None = None) -> list[dict]:
            try:
                rows = await self._mexc_fetch_positions(sym_list)
                return [p for p in rows or [] if float(p.get("contracts") or 0) > 0 or float((p.get("info") or {}).get("holdVol") or 0) > 0]
            except Exception as e:
                errors.append(f"fetch_positions: {e}")
                return []

        async def _position_margin_usdt() -> float:
            try:
                bal = await self.fetch_balance()
                usdt = (bal or {}).get("USDT", {}) if isinstance(bal, dict) else {}
                return float(usdt.get("positionMargin") or usdt.get("position_margin") or 0)
            except Exception as e:
                errors.append(f"balance_position_margin: {e}")
                return 0.0

        async def _panic_close_synthetic_btc(reason: str) -> None:
            """Last-resort BTC sweep when balance shows margin but open_positions is empty.

            MEXC close-side order codes (4 close long, 2 close short) should fail
            harmlessly if that side has no position; they must not open a new one.
            Volume is estimated from positionMargin * leverage / price / contractSize.
            """
            try:
                pm = await _position_margin_usdt()
                if pm <= 0.0001:
                    return
                ticker = await self.fetch_ticker("BTC_USDT")
                price = float(ticker.get("last") or ticker.get("close") or 0)
                lev = int(os.getenv("MEXC_ORDER_LEVERAGE", "10") or "10")
                contract_size = float(os.getenv("MEXC_BTC_CONTRACT_SIZE", "0.0001") or "0.0001")
                est = int(max(1, round((pm * lev) / max(price * contract_size, 1e-12)))) if price > 0 else 0
                max_sweep = int(os.getenv("MEXC_PANIC_MAX_SWEEP_VOL", "1000") or "1000")
                vol = min(est, max_sweep)
                if vol <= 0:
                    return
                # Sweep both close sides and both margin modes. Side 4 closes
                # LONG, side 2 closes SHORT. These close-side codes must not open
                # a new position; failures are logged and the next safe variant is tried.
                for close_side in (4, 2):
                    for open_type in (1, 2):
                        for sweep_vol in sorted(set([vol, max(1, vol + 1), max(1, vol + 2)])):
                            body = {
                                "symbol": "BTC_USDT",
                                "price": 0,
                                "vol": int(sweep_vol),
                                "side": close_side,
                                "type": 5,
                                "openType": open_type,
                            }
                            sweep_variants = [
                                body,
                                dict(body, flashClose=True),
                                dict(body, marketCeiling=True),
                                dict(body, leverage=lev),
                                dict(body, reduceOnly=True),
                            ]
                            for attempt_body in sweep_variants:
                                try:
                                    out = await self._mexc_private("POST", "/api/v1/private/order/create", body=attempt_body)
                                    results.append({"stage": "synthetic_btc_sweep", "reason": reason, "side": close_side, "openType": open_type, "vol": sweep_vol, "position_margin": pm, "price": price, "body": attempt_body, "result": out})
                                    break
                                except Exception as e:
                                    results.append({"stage": "synthetic_btc_sweep_failed", "reason": reason, "side": close_side, "openType": open_type, "vol": sweep_vol, "body": attempt_body, "error": str(e)[:220]})
            except Exception as e:
                errors.append(f"synthetic_btc_sweep: {e}")

        # Build a cleanup list, but DO NOT cancel before close.
        # V33 still cancelled BTC orders before sending /order/create, which could
        # leave Telegram/log output showing only cancel_all calls and no real close.
        # V34 sends close market orders first, then cleans TP/SL/limit orders.
        cleanup_syms: list[str | None] = []
        cleanup_syms.extend(wanted)
        try:
            cleanup_syms.extend([p.get("mexc_symbol") or p.get("symbol") for p in await _positions_for(None)])
        except Exception:
            pass
        cleanup_syms.append("BTC_USDT")
        seen_cancel = []
        for sym in cleanup_syms:
            key = sym or "*"
            if key not in seen_cancel:
                seen_cancel.append(key)

        for attempt in range(1, max(1, int(retries)) + 1):
            positions = await _positions_for(wanted or None)
            if not positions:
                positions = await _positions_for(None)
            snapshots.append({"attempt": attempt, "positions": positions, "position_margin": await _position_margin_usdt()})
            if not positions:
                # If balance still shows margin, MEXC did not expose open_positions; do a BTC safety sweep.
                if (await _position_margin_usdt()) > 0.0001:
                    await _panic_close_synthetic_btc(f"no_open_positions_attempt_{attempt}")
                    await asyncio.sleep(0.8 * attempt)
                    continue
                break
            for pos in positions:
                try:
                    close_res = await self.mexc_close_position_market_native(pos)
                    results.append({"stage": "close_position", "attempt": attempt, "position": {"symbol": pos.get("mexc_symbol") or pos.get("symbol"), "side": pos.get("side"), "contracts": pos.get("contracts"), "info": {"holdVol": (pos.get("info") or {}).get("holdVol"), "positionType": (pos.get("info") or {}).get("positionType")}}, "result": close_res})
                except Exception as e:
                    errors.append(f"close {pos.get('mexc_symbol') or pos.get('symbol')}: {e}")
            await asyncio.sleep(0.8 * attempt)

        # Native close_all as additional fallback.
        try:
            native = await self.mexc_close_all_positions_native()
            results.append({"stage": "native_close_all_fallback", "result": native})
        except Exception as e:
            msg = str(e)
            if "2009" not in msg and "nonexistent" not in msg.lower() and "closed" not in msg.lower():
                errors.append(f"native_close_all_fallback: {e}")

        # Cancel TP/SL and limits after closing.
        for key in seen_cancel:
            sym = None if key == "*" else key
            try:
                res = await self._mexc_cancel_all_orders(sym)
                results.append({"stage": "cancel_after", "symbol": key, "result": res})
            except Exception as e:
                errors.append(f"cancel_after {key}: {e}")

        await asyncio.sleep(1.2)
        remaining = await _positions_for(wanted or None)
        if not remaining:
            remaining = await _positions_for(None)
        pm_after = await _position_margin_usdt()
        ok = len(remaining) == 0 and pm_after < 0.0001
        return {"ok": ok, "results": results, "errors": errors, "remaining_positions": remaining, "position_margin_after": pm_after, "snapshots": snapshots[-3:]}

    def _mexc_plan_trigger_type(self, close_side: str, kind: str) -> int:
        """Return MEXC plan triggerType for a close TP/SL order.

        MEXC docs: triggerType=1 means price >= trigger, triggerType=2 means
        price <= trigger. `trend` is NOT direction; it is the reference price
        type (1 latest, 2 fair, 3 index).

        Closing LONG uses sell orders: TP above current => >=, SL below => <=.
        Closing SHORT uses buy orders: TP below current => <=, SL above => >=.
        """
        side_l = str(close_side or "").lower()
        kind_l = str(kind or "sl").lower()
        if side_l == "sell":  # close LONG
            return 1 if kind_l == "tp" else 2
        if side_l == "buy":   # close SHORT
            return 2 if kind_l == "tp" else 1
        return 1

    async def mexc_place_trigger_market(self, symbol: str, close_side: str, amount: float, trigger_price: float, kind: str = "sl", client_order_id: str = "", leverage: int | None = None) -> dict:
        """Place a native MEXC futures trigger-market close order for TP or SL.

        This is the reliable fallback used by the bot after a position is live.
        It sends a real MEXC plan order with market execution.  Important:
        `triggerType` is the up/down condition; `trend` is only the reference
        price type. Older versions mixed those two fields, which made TP/SL
        orders reject or never appear on MEXC.
        """
        msym = self._mexc_symbol(symbol)
        side_l = str(close_side).lower().strip()
        # Accept both position side (LONG/SHORT) and close order side (buy/sell).
        # MEXC planorder fields need the close order side; LONG closes with sell,
        # SHORT closes with buy.
        if side_l == "long":
            side_l = "sell"
        elif side_l == "short":
            side_l = "buy"
        kind_l = str(kind or "sl").lower()
        if side_l not in {"buy", "sell"}:
            raise ValueError(f"invalid MEXC close_side for trigger-market TP/SL: {close_side}")
        mexc_side = 2 if side_l == "buy" else 4
        trigger_type = self._mexc_plan_trigger_type(side_l, kind_l)
        ref = await self._mexc_reference_price(symbol, 0)
        safe_trigger = self._mexc_safe_trigger_price_sync(symbol, close_side, kind_l, float(trigger_price), ref)
        body = {
            "symbol": msym,
            "vol": self._amount_to_mexc_vol(symbol, amount),
            "side": mexc_side,
            "openType": int(os.getenv("MEXC_ORDER_OPEN_TYPE", "1") or "1"),
            "leverage": int(leverage or os.getenv("MEXC_ORDER_LEVERAGE", "5") or "5"),
            "triggerPrice": safe_trigger,
            "executePrice": 0,
            "orderType": 5,
            "triggerType": trigger_type,
            "trend": int(os.getenv("MEXC_PLAN_TREND", "1") or "1"),
            "executeCycle": int(os.getenv("MEXC_PLAN_EXECUTE_CYCLE", "1") or "1"),
            "reduceOnly": True,
            "priceProtect": int(os.getenv("MEXC_PLAN_PRICE_PROTECT", "0") or "0"),
        }
        if client_order_id:
            body["externalOid"] = str(client_order_id)[:32]
        out = await self._mexc_private("POST", "/api/v1/private/planorder/place", body=body)
        data = out.get("data") if isinstance(out, dict) else {}
        oid = data.get("orderId") if isinstance(data, dict) else data
        return {
            "id": str(oid or ""),
            "symbol": self.normalize_symbol(symbol),
            "type": f"{kind_l}_trigger_market",
            "side": close_side,
            "amount": amount,
            "price": None,
            "info": {"native_mexc_trigger": True, "_protection_kind": kind_l, "_protection_endpoint": "planorder", "safe_trigger_price": safe_trigger, "reference_price": ref, **(out if isinstance(out, dict) else {"raw": out})},
        }

    async def mexc_place_stop_market(self, symbol: str, close_side: str, amount: float, trigger_price: float, client_order_id: str = "", leverage: int | None = None) -> dict:
        return await self.mexc_place_trigger_market(symbol, close_side, amount, trigger_price, kind="sl", client_order_id=client_order_id, leverage=leverage)

    async def mexc_place_take_profit_market(self, symbol: str, close_side: str, amount: float, trigger_price: float, client_order_id: str = "", leverage: int | None = None) -> dict:
        return await self.mexc_place_trigger_market(symbol, close_side, amount, trigger_price, kind="tp", client_order_id=client_order_id, leverage=leverage)

    async def mexc_debug_state(self, symbol: str | None = None) -> dict:
        """Compact raw diagnostics for MEXC state without exposing credentials."""
        endpoints = [
            "/api/v1/private/account/assets",
            "/api/v1/private/position/open_positions",
            "/api/v1/private/order/list/open_orders",
            "/api/v1/private/planorder/list/orders",
            "/api/v1/private/stoporder/list/orders",
        ]
        queries = [{}]
        if symbol:
            queries = []
            for v in self.mexc_symbol_variants(symbol):
                ms = self._mexc_normalize_contract_id(v)
                if "_" in ms:
                    queries.append({"symbol": ms})
            queries.append({})
        report = {"symbol": symbol, "variants": self.mexc_symbol_variants(symbol) if symbol else [], "endpoints": []}
        for ep in endpoints:
            for q in queries[:6]:
                try:
                    out = await self._mexc_private_read_any_base(ep, query=q)
                    data = out.get("data") if isinstance(out, dict) else None
                    rows = self._mexc_rows(data)
                    sample = rows[:2] if rows else (data if isinstance(data, dict) else data)
                    report["endpoints"].append({
                        "endpoint": ep,
                        "query": q,
                        "base": out.get("_base_url") if isinstance(out, dict) else "",
                        "rows": len(rows) if isinstance(rows, list) else 0,
                        "sample": sample,
                    })
                except Exception as e:
                    # Debug output must stay clean for Telegram: do not spam users
                    # with known unavailable MEXC variants/HTML/404 text. Keep the
                    # compact suppressed list only for developers if needed later.
                    report.setdefault("suppressed_errors", []).append({
                        "endpoint": ep,
                        "query": q,
                        "error": str(e)[:220],
                    })
                    continue
        return report

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
        target_leverage = int(params.get("leverage") or os.getenv("MEXC_ORDER_LEVERAGE", "5") or "5")
        target_open_type = int(params.get("openType") or os.getenv("MEXC_ORDER_OPEN_TYPE", "1") or "1")
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
        has_trigger_price = any(k in params for k in ("stopPrice", "triggerPrice"))
        has_attached_tpsl = any(k in params for k in ("stopLossPrice", "takeProfitPrice"))
        # v0146: opening orders may carry attached TP/SL. Do not mis-route those
        # orders to /planorder/place; send them to /order/create with the TP/SL
        # fields included below. Only reduce-only trigger orders use planorder.
        if t in {"market", "stop_market"} and not (has_trigger_price or (has_attached_tpsl and reduce_only)):
            mexc_type = 5  # market
            order_price = 0
        elif has_trigger_price or (has_attached_tpsl and reduce_only):
            # Native plan order. Used for SL/TP fallback only if ccxt fails.
            trigger_price = self._mexc_price_to_precision(symbol, float(params.get("triggerPrice") or params.get("stopPrice") or params.get("stopLossPrice") or params.get("takeProfitPrice")))
            kind = "tp" if params.get("takeProfitPrice") else "sl"
            body = {
                "symbol": self._mexc_symbol(symbol),
                "vol": self._amount_to_mexc_vol(symbol, amount),
                "side": mexc_side,
                "openType": target_open_type,
                "leverage": target_leverage,
                "triggerPrice": trigger_price,
                "executePrice": 0,
                "orderType": 5,
                "triggerType": self._mexc_plan_trigger_type(str(side).lower(), kind),
                "trend": int(os.getenv("MEXC_PLAN_TREND", "1") or "1"),
                "executeCycle": int(os.getenv("MEXC_PLAN_EXECUTE_CYCLE", "1") or "1"),
                "reduceOnly": True,
                "priceProtect": int(os.getenv("MEXC_PLAN_PRICE_PROTECT", "0") or "0"),
            }
            if params.get("clientOrderId"):
                body["externalOid"] = str(params.get("clientOrderId"))[:32]
            out = await self._mexc_private("POST", "/api/v1/private/planorder/place", body=body)
            return {"id": str((out.get("data") or {}).get("orderId") or (out.get("data") or {}).get("id") or ""), "symbol": self.normalize_symbol(symbol), "type": type_, "side": side, "amount": amount, "price": price, "info": {"raw_fallback": True, "previous_error": previous_error, **out}}
        else:
            mexc_type = 1  # limit
            order_price = self._mexc_price_to_precision(symbol, float(price or 0))
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
        # Attach native MEXC TP/SL to the opening order when provided.
        # For /order/create, lossTrend/profitTrend are reference price types
        # (1 latest, 2 fair, 3 index), NOT trigger direction.
        if is_opening:
            if params.get("stopLossPrice") not in (None, "", 0, "0"):
                body["stopLossPrice"] = self._mexc_price_to_precision(symbol, float(params.get("stopLossPrice")))
                body["lossTrend"] = int(params.get("lossTrend") or 1)
                body["stopLossType"] = int(params.get("stopLossType") or 0)
                body["stopLossOrderPrice"] = self._mexc_price_to_precision(symbol, float(params.get("stopLossOrderPrice") or 0))
                body["stopLossReverse"] = int(params.get("stopLossReverse") or 2)
            if params.get("takeProfitPrice") not in (None, "", 0, "0"):
                body["takeProfitPrice"] = self._mexc_price_to_precision(symbol, float(params.get("takeProfitPrice")))
                body["profitTrend"] = int(params.get("profitTrend") or 1)
                body["takeProfitType"] = int(params.get("takeProfitType") or 0)
                body["takeProfitOrderPrice"] = self._mexc_price_to_precision(symbol, float(params.get("takeProfitOrderPrice") or 0))
                body["takeProfitReverse"] = int(params.get("takeProfitReverse") or 2)
            if ("stopLossPrice" in body) or ("takeProfitPrice" in body):
                body["priceProtect"] = int(params.get("priceProtect") or 0)
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
