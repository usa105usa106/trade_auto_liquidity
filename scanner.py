import time
import os
import logging
import asyncio
from collections import Counter, defaultdict
from signal_engine import SignalEngine
from regime_engine import RegimeEngine

log = logging.getLogger(__name__)

class Scanner:
    """
    Real futures-first scanner.

    Flow:
    1. Refresh futures symbols by volume + volatility + regime-aware adaptive sizing.
    2. For hot symbols, fetch OHLCV + orderbook.
    3. Generate candidates via SignalEngine using the effective strategy selected by
       AdaptiveEngine/RegimeEngine in the trading loop.
    4. Return only validated candidates.
    """

    def __init__(self):
        self.last_refresh = 0
        self.hot_symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "DOGE/USDT", "XRP/USDT"]
        self.last_regime = {"regime": "LOW_VOLATILITY", "source": "init"}
        self.last_scan_source = "init"
        self.last_refresh_error = ""
        self.last_total_markets = 0
        self.last_filtered_markets = 0
        self.last_requested_symbols = 0
        self.last_selected_symbols = 0
        self.last_available_markets = 0
        self.last_universe_target_reason = "-"
        self.last_signal_summary = "-"
        self.last_reject_reason = "-"
        self.last_effective_strategy = "-"
        self.last_strategy_reason = "-"
        self.last_concurrency = int(os.getenv("SCANNER_CONCURRENCY", "5"))
        self.last_cycle_errors = 0
        self.last_cycle_scanned = 0
        self.last_slowdown_sec = 0
        self.error_streak = 0
        self.last_reject_top_reasons = []
        self.last_reject_examples = []
        self.last_ai_check_symbols = []
        self.last_ai_candidates_count = 0
        self.engine = SignalEngine(
            min_confidence=float(os.getenv("SIGNAL_MIN_CONFIDENCE", "70")),
            volume_spike_mult=float(os.getenv("SIGNAL_VOLUME_SPIKE_MULT", "1.8")),
            breakout_lookback=int(os.getenv("SIGNAL_BREAKOUT_LOOKBACK", "20")),
            momentum_threshold_pct=float(os.getenv("SIGNAL_MOMENTUM_THRESHOLD_PCT", "0.18")),
            max_candidates_per_cycle=int(os.getenv("SIGNAL_MAX_CANDIDATES_PER_CYCLE", "8")),
        )

    def _safe_float(self, value, default: float = 0.0) -> float:
        try:
            if value is None or value == "":
                return default
            return float(value)
        except Exception:
            return default

    def _ticker_quote_volume(self, ticker: dict) -> float:
        """Return 24h quote volume across CCXT REST and MEXC WS shapes.

        MEXC/CCXT payloads are inconsistent: some rows have quoteVolume, some
        only baseVolume + last/close, and websocket snapshots may keep the raw
        values inside info. The old adaptive universe treated missing
        quoteVolume as zero, so many valid markets were filtered out and
        adaptive mode looked broken.
        """
        info = ticker.get("info") if isinstance(ticker.get("info"), dict) else {}
        direct = (
            ticker.get("quoteVolume") or ticker.get("quoteVolume24h") or
            ticker.get("quote_volume") or ticker.get("amount24") or
            ticker.get("turnover") or ticker.get("turnover24") or
            info.get("quoteVolume") or info.get("quoteVolume24h") or
            info.get("amount24") or info.get("turnover24") or
            info.get("turnover") or info.get("amount")
        )
        quote = self._safe_float(direct, 0.0)
        if quote > 0:
            return quote
        base = self._safe_float(
            ticker.get("baseVolume") or ticker.get("baseVolume24h") or ticker.get("volume") or
            ticker.get("volume24") or ticker.get("vol24") or info.get("volume24") or
            info.get("vol24") or info.get("volume") or info.get("baseVolume"),
            0.0,
        )
        last = self._safe_float(
            ticker.get("last") or ticker.get("close") or ticker.get("mark") or ticker.get("indexPrice") or
            info.get("lastPrice") or info.get("last") or info.get("fairPrice") or
            info.get("indexPrice") or info.get("bid1") or info.get("ask1"),
            0.0,
        )
        return base * last if base > 0 and last > 0 else 0.0

    def _ticker_pct_change(self, ticker: dict) -> float:
        info = ticker.get("info") if isinstance(ticker.get("info"), dict) else {}
        raw = (ticker.get("percentage") or ticker.get("change") or ticker.get("change24h") or
               info.get("riseFallRate") or info.get("changeRate") or info.get("change24h"))
        val = self._safe_float(raw, 0.0)
        # MEXC often sends 0.0123 for 1.23%; CCXT percentage is already 1.23.
        if abs(val) <= 1.0:
            val *= 100.0
        return abs(val)

    def _adaptive_symbol_count(self, settings: dict, regime_info: dict, market_items_count: int) -> int:
        base = int(settings.get("max_symbols", 100) or 100)
        regime = str(regime_info.get("regime", "LOW_VOLATILITY"))
        volatility = float(regime_info.get("volatility", 0.0) or 0.0)
        breadth = int(regime_info.get("breadth_count", market_items_count) or market_items_count)

        factor = 1.0
        reason = f"base={base}"
        if regime == "HIGH_VOLATILITY":
            factor *= 0.55
            reason += ", high-vol: narrower"
        elif regime == "TRENDING":
            factor *= 0.80
            reason += ", trending: leaders"
        elif regime == "CHOPPY":
            factor *= 1.25
            reason += ", choppy: wider"
        else:
            reason += f", regime={regime}"

        if volatility >= 2.0:
            factor *= 0.80
            reason += ", volatility>=2: reduce"
        elif breadth >= 150 and regime in {"CHOPPY", "LOW_VOLATILITY"}:
            factor *= 1.10
            reason += ", broad market: widen"

        n = int(base * factor)
        n = max(10, min(n, 300, max(10, market_items_count)))
        self.last_universe_target_reason = f"adaptive -> {n} ({reason})"
        return n

    async def _fetch_binance_futures_tickers(self, settings: dict | None = None) -> dict:
        import ccxt.async_support as ccxt
        settings = settings or {}
        cfg = {"enableRateLimit": True, "options": {"defaultType": "future"}}
        proxy_enabled = bool(settings.get("proxy_enabled", False))
        proxy_url = str(settings.get("proxy_url", "") or "")
        if proxy_enabled and proxy_url:
            cfg["proxies"] = {"http": proxy_url, "https": proxy_url}
            cfg["aiohttp_proxy"] = proxy_url
        exchange = ccxt.binanceusdm(cfg)
        try:
            await exchange.load_markets()
            return await exchange.fetch_tickers()
        finally:
            await exchange.close()


    async def _fetch_mexc_futures_tickers_rest(self, exchange_client, settings: dict | None = None) -> dict:
        """Native MEXC futures REST ticker fallback.

        This is deliberately independent from the websocket ticker cache and from
        ccxt.fetch_tickers(), because on Railway/MEXC the websocket cache can be
        empty while /api/v1/contract/ticker is healthy.  Returned rows are shaped
        like a minimal ccxt ticker map so the existing universe/ranking code can
        consume them unchanged.
        """
        settings = settings or {}
        # Prefer the project's native MEXC public method when available.
        if hasattr(exchange_client, "_mexc_public"):
            resp = await exchange_client._mexc_public("GET", "/api/v1/contract/ticker")
            data = resp.get("data") if isinstance(resp, dict) else resp
            out = {}
            for row in data or []:
                if not isinstance(row, dict):
                    continue
                raw_symbol = row.get("symbol") or row.get("contractCode") or row.get("instrumentId")
                if not raw_symbol or "USDT" not in str(raw_symbol).upper():
                    continue
                try:
                    norm = exchange_client.normalize_symbol(raw_symbol)
                except Exception:
                    norm = str(raw_symbol).replace("_", "/").replace(":USDT", "")
                    if "/" not in norm and norm.endswith("USDT"):
                        norm = norm[:-4] + "/USDT"
                    if not norm.endswith(":USDT"):
                        norm = norm + ":USDT"
                def _num(*keys, default=0.0):
                    for key in keys:
                        val = row.get(key)
                        if val not in (None, ""):
                            try:
                                return float(val)
                            except Exception:
                                pass
                    return default
                last = _num("lastPrice", "last", "price", "fairPrice", "markPrice")
                pct = _num("riseFallRate", "changeRate", "priceChangePercent")
                # MEXC may return riseFallRate as fraction (0.0123) rather than percent.
                pct = pct * 100.0 if abs(pct) <= 1.0 else pct
                quote_vol = _num("amount24", "quoteVolume", "turnover24", "turnover")
                base_vol = _num("volume24", "volume", "vol24", "holdVol")
                out[norm] = {
                    "symbol": norm,
                    "last": last,
                    "close": last,
                    "percentage": pct,
                    "quoteVolume": quote_vol,
                    "baseVolume": base_vol,
                    "info": row,
                }
            if out:
                return out
            raise RuntimeError("MEXC futures REST returned empty ticker set")

        # Last resort for non-native clients.
        tickers = await exchange_client.fetch_tickers()
        if tickers:
            return tickers
        raise RuntimeError("MEXC futures REST returned empty ticker set")

    async def _fetch_scan_tickers(self, exchange_client, settings: dict, ws_supervisor=None) -> tuple[dict, str]:
        """Return futures tickers for universe scanning using the user-selected source.

        scan_market_source values:
        - binance_binance: Binance futures scan + Binance spot confirmation
        - mexc_mexc: MEXC futures scan + MEXC spot confirmation
        - mexc_binance: MEXC futures scan + Binance spot confirmation (default)

        There is intentionally NO automatic Binance->MEXC fallback here: if the
        user selected Binance futures and it fails/stales, the bot reports the
        error and keeps the previous universe instead of silently changing venue.
        """
        mode = str(settings.get("scan_market_source", "mexc_binance") or "mexc_binance").lower()
        futures_source = "binance" if mode.startswith("binance") else "mexc"

        ws_error = ""
        ws_status = getattr(ws_supervisor, "status", None) if ws_supervisor else None
        ws_enabled = bool(getattr(ws_status, "enabled", True)) if ws_status is not None else bool(ws_supervisor)
        ws_venue = getattr(ws_status, "venue", futures_source) if ws_status is not None else futures_source
        if ws_supervisor and ws_enabled and ws_venue == futures_source:
            try:
                if ws_supervisor.healthy():
                    tickers = await ws_supervisor.tickers(max_age_sec=max(30, int(settings.get("scan_interval_sec", 3)) * 10))
                    if tickers:
                        return tickers, f"{futures_source}_futures_ws"
                    ws_error = f"{futures_source.title()} futures websocket returned empty ticker cache"
                else:
                    ws_error = f"{futures_source.title()} futures websocket unhealthy: {getattr(ws_status, 'last_error', '') or 'no fresh messages'}"
            except Exception as e:
                ws_error = f"{futures_source.title()} futures websocket failed: {e}"

        if futures_source == "binance":
            try:
                tickers = await self._fetch_binance_futures_tickers(settings)
                if tickers:
                    if ws_error:
                        self.last_refresh_error = ws_error
                    return tickers, "binance_futures_rest"
                raise RuntimeError("Binance futures REST returned empty ticker set")
            except Exception as e:
                msg = f"Binance futures scan failed: {e}"
                if ws_error:
                    msg = f"{ws_error}; {msg}"
                raise RuntimeError(msg)

        try:
            tickers = await self._fetch_mexc_futures_tickers_rest(exchange_client, settings)
            if tickers:
                # REST success means the scanner is healthy. Keep the websocket
                # issue as a non-fatal warning in source, not as last_refresh_error,
                # so Telegram does not show "source issue" while we are already
                # scanning fresh REST data.
                source = "mexc_futures_rest"
                if ws_error:
                    self.last_universe_target_reason = f"REST fallback used after websocket issue: {ws_error[:120]}"
                self.last_refresh_error = ""
                return tickers, source
            raise RuntimeError("MEXC futures REST returned empty ticker set")
        except Exception as e:
            msg = f"MEXC futures REST fallback failed: {e}"
            if ws_error:
                msg = f"{ws_error}; {msg}"
            raise RuntimeError(msg)

    def _universe_target_count(self, settings: dict, regime_info: dict, market_items_count: int) -> int:
        mode = str(settings.get("universe_mode", "adaptive") or "adaptive").strip().lower()
        if mode.startswith("top-"):
            try:
                n = int(mode.replace("top-", ""))
            except Exception:
                n = 100
            self.last_universe_target_reason = f"fixed {mode} -> {n}"
            return n
        return self._adaptive_symbol_count(settings, regime_info, market_items_count)

    def _all_futures_symbols(self, exchange_client) -> list[str]:
        try:
            symbols = exchange_client.futures_market_symbols()
            return list(dict.fromkeys(symbols))
        except Exception:
            return []

    def _concurrency_limit(self, settings: dict) -> int:
        try:
            val = settings.get("scanner_concurrency", os.getenv("SCANNER_CONCURRENCY", "5"))
            raw = int(float(val))
        except Exception:
            raw = 5
        # Railway-safe guardrails: 1 avoids bursts, 12 keeps API/container load bounded.
        return max(1, min(raw, 12))

    def _record_cycle_health(self, scanned: int, errors: int, settings: dict) -> None:
        self.last_cycle_scanned = int(scanned)
        self.last_cycle_errors = int(errors)
        threshold = int(settings.get("scanner_error_slowdown_threshold", os.getenv("SCANNER_ERROR_SLOWDOWN_THRESHOLD", "5")) or 5)
        max_slowdown = int(settings.get("scanner_slowdown_max_sec", os.getenv("SCANNER_SLOWDOWN_MAX_SEC", "15")) or 15)
        if errors >= max(1, threshold):
            self.error_streak += 1
            self.last_slowdown_sec = min(max_slowdown, 2 * self.error_streak)
        else:
            self.error_streak = 0
            self.last_slowdown_sec = 0

    async def refresh_symbols(self, exchange_client, settings: dict, ws_supervisor=None):
        self.last_refresh = time.time()
        self.last_refresh_error = ""
        self.last_reject_reason = "-"
        min_quote_volume = float(os.getenv("SIGNAL_MIN_24H_QUOTE_VOLUME", "5000000"))
        try:
            tickers, source = await self._fetch_scan_tickers(exchange_client, settings, ws_supervisor)
            self.last_scan_source = source
            self.last_total_markets = len(tickers or {})
            regime_info = RegimeEngine().detect_from_tickers(tickers) if bool(settings.get("regime_adaptation", True)) else {"regime": "LOW_VOLATILITY", "source": "disabled"}
            regime_info["source"] = f"tickers:{source}"
            self.last_regime = regime_info

            items = []
            seen = set()
            for sym, t in (tickers or {}).items():
                if "USDT" not in str(sym):
                    continue
                quote_volume = self._ticker_quote_volume(t)
                if quote_volume < min_quote_volume:
                    continue
                pct_change = self._ticker_pct_change(t)
                try:
                    norm = exchange_client.normalize_symbol(sym)
                except Exception:
                    continue
                vol_bonus = min(pct_change, 30) / 100
                if regime_info.get("regime") == "CHOPPY":
                    vol_bonus = min(pct_change, 12) / 100
                score = quote_volume * (1 + vol_bonus)
                if norm not in seen:
                    seen.add(norm)
                    items.append((score, norm))

            items.sort(reverse=True)
            self.last_filtered_markets = len(items)

            # Adaptive must not silently keep an old/default universe when the
            # exchange payload has weak/missing quote-volume fields. If the hard
            # volume filter removes everything, rebuild from all normalized USDT
            # tickers with a softer score so adaptive still selects real markets.
            if not items and tickers:
                fallback = []
                fallback_seen = set()
                for sym, t in (tickers or {}).items():
                    if "USDT" not in str(sym):
                        continue
                    try:
                        norm = exchange_client.normalize_symbol(sym)
                    except Exception:
                        continue
                    qv = self._ticker_quote_volume(t)
                    pct_change = self._ticker_pct_change(t)
                    score = qv if qv > 0 else pct_change
                    if norm not in fallback_seen:
                        fallback_seen.add(norm)
                        fallback.append((score, norm))
                fallback.sort(reverse=True)
                if fallback:
                    items = fallback
                    seen = fallback_seen
                    self.last_filtered_markets = len(items)
                    self.last_refresh_error = "adaptive fallback: volume filter returned empty universe"

            all_symbols = self._all_futures_symbols(exchange_client)
            self.last_available_markets = max(len(all_symbols), self.last_total_markets)
            target = self._universe_target_count(settings, regime_info, max(len(items), len(all_symbols), self.last_total_markets))
            target = max(10, min(int(target), 300))
            self.last_requested_symbols = target

            # If the ticker endpoint returns only a small set, supplement from loaded
            # exchange markets so top-100/top-200/adaptive actually change the scan
            # universe instead of always stopping at the ticker count. These extra
            # markets get lower priority but still can be scanned via OHLCV/orderbook.
            if len(items) < target:
                for sym in all_symbols:
                    if sym not in seen:
                        seen.add(sym)
                        items.append((0.0, sym))
                    if len(items) >= target:
                        break

            selected = [s for _, s in items[:target]]
            if selected:
                self.hot_symbols = selected
            self.last_selected_symbols = len(self.hot_symbols)
        except Exception as e:
            self.last_refresh_error = str(e)[:500]
            log.warning("symbol refresh failed: %s", e)

    async def detect_regime(self, exchange_client, settings: dict) -> dict:
        if not bool(settings.get("regime_adaptation", True)):
            self.last_regime = {"regime": "LOW_VOLATILITY", "source": "disabled"}
            return self.last_regime
        anchor = os.getenv("REGIME_ANCHOR_SYMBOL", "BTC/USDT")
        tf = os.getenv("REGIME_TIMEFRAME", os.getenv("SIGNAL_OHLCV_TIMEFRAME", "1m"))
        limit = int(os.getenv("REGIME_OHLCV_LIMIT", "120"))
        try:
            candles = await exchange_client.fetch_ohlcv(anchor, timeframe=tf, limit=limit)
            info = RegimeEngine().detect_from_candles(candles)
            info["source"] = f"{anchor}:{tf}"
            self.last_regime = info
            return info
        except Exception as e:
            log.debug("regime candle detection failed: %s", e)
            return self.last_regime or {"regime": "LOW_VOLATILITY", "source": "fallback"}


    def _binance_spot_to_futures_symbol(self, spot_symbol: str, exchange_client=None) -> str | None:
        """Map Binance spot symbols like RENDER/USDT to a MEXC futures symbol.

        Orderflow impulse is Binance-spot-native for analysis, but execution is
        still done on MEXC futures. Keep the mapping narrow and deterministic.
        """
        raw = str(spot_symbol or "").replace(":USDT", "")
        if "/" in raw:
            base, quote = raw.split("/", 1)
        elif raw.upper().endswith("USDT"):
            base, quote = raw[:-4], "USDT"
        else:
            return None
        base = base.upper().strip()
        quote = quote.upper().strip()
        if quote != "USDT" or not base:
            return None
        aliases = {
            "RNDR": "RENDER",
            "RENDER": "RENDER",
            "TON": "TONCOIN",
            "TONCOIN": "TONCOIN",
        }
        candidates = []
        for b in (base, aliases.get(base, base)):
            sym = f"{b}/USDT:USDT"
            if sym not in candidates:
                candidates.append(sym)
        try:
            available = set(exchange_client.futures_market_symbols()) if exchange_client is not None else set()
        except Exception:
            available = set()
        if available:
            for sym in candidates:
                if sym in available:
                    return sym
            return None
        return candidates[-1]

    async def _fetch_binance_spot_tickers(self, settings: dict | None = None) -> dict:
        import ccxt.async_support as ccxt
        settings = settings or {}
        cfg = {"enableRateLimit": True, "options": {"defaultType": "spot"}}
        proxy_enabled = bool(settings.get("proxy_enabled", False))
        proxy_url = str(settings.get("proxy_url", "") or "")
        if proxy_enabled and proxy_url:
            cfg["proxies"] = {"http": proxy_url, "https": proxy_url}
            cfg["aiohttp_proxy"] = proxy_url
        exchange = ccxt.binance(cfg)
        try:
            await exchange.load_markets()
            return await exchange.fetch_tickers()
        finally:
            await exchange.close()

    async def _orderflow_impulse_candidates(self, exchange_client, settings: dict, max_candidates: int) -> list[dict]:
        """Native Binance spot orderflow scanner.

        v0254: do NOT use ccxt.load_markets()/fetch_tickers here because that
        may hit exchangeInfo and fail on some VPS/regions. This scanner uses
        Binance SPOT public REST endpoints directly:
          - /api/v3/ticker/24hr
          - /api/v3/klines
          - /api/v3/depth
          - /api/v3/aggTrades
        MEXC futures is used only after a Binance spot signal is found.
        """
        import aiohttp
        import socket
        from urllib.parse import urlencode

        self.engine.configure_from_settings(settings)
        top_n = int(float(settings.get("orderflow_impulse_top_coins", 100) or 100))
        min_quote = float(settings.get("orderflow_impulse_min_24h_volume_usdt", 5000000.0) or 0)
        min_vol_ratio = float(settings.get("orderflow_impulse_min_volume_ratio", 1.5) or 1.5)
        # v0256: previous builds wrote 2.0 into persistent settings when the
        # button was toggled. For the agreed orderflow mode, cap the live
        # volume threshold at 1.5 so old DB values do not keep blocking scans.
        if min_vol_ratio > 1.5:
            min_vol_ratio = 1.5
        min_trend = abs(float(settings.get("orderflow_impulse_min_trend_pct", 0.25) or 0.25))
        min_imb = abs(float(settings.get("orderflow_impulse_min_imbalance_abs", 0.08) or 0.08))
        max_spread = abs(float(settings.get("orderflow_impulse_max_spread_pct", 0.20) or 0.20))
        tp_pct = float(settings.get("orderflow_impulse_tp_pct", 2.0) or 2.0)
        sl_pct = float(settings.get("orderflow_impulse_sl_pct", 3.0) or 3.0)
        reject_counts = Counter()
        reject_examples = defaultdict(list)
        errors = 0
        scanned = 0
        prefilter_volume_low = 0
        prefilter_no_futures = 0
        last_error = ""

        def record(symbol: str, reason: str) -> None:
            reason = str(reason or "unknown")[:260]
            bucket = reason.split(":", 1)[0]
            for prefix in (
                "spot spread high", "spot volume low", "spot data unavailable",
                "mexc futures symbol missing", "volume low", "no spot orderflow alignment",
                "binance rest failed",
            ):
                if reason.startswith(prefix):
                    bucket = prefix
                    break
            reject_counts[bucket] += 1
            if len(reject_examples[bucket]) < 3:
                reject_examples[bucket].append(f"{symbol}->{reason}")

        proxy_enabled = bool(settings.get("proxy_enabled", False))
        proxy_url = str(settings.get("proxy_url", "") or "")
        proxy = proxy_url if proxy_enabled and proxy_url else None
        bases = [b.strip().rstrip("/") for b in str(settings.get("binance_spot_base_urls") or os.getenv("BINANCE_SPOT_BASE_URLS", "https://api.binance.com,https://api1.binance.com,https://api2.binance.com,https://api3.binance.com,https://api4.binance.com,https://data-api.binance.vision")).split(",") if b.strip()]
        timeout = aiohttp.ClientTimeout(total=float(settings.get("binance_spot_timeout_sec", os.getenv("BINANCE_SPOT_TIMEOUT_SEC", "7")) or 7))

        async def get_json(session, path: str, params: dict | None = None):
            nonlocal last_error
            params = params or {}
            qs = ("?" + urlencode(params)) if params else ""
            errs = []
            for base in bases:
                url = f"{base}{path}{qs}"
                try:
                    async with session.get(url, proxy=proxy) as resp:
                        text = await resp.text()
                        if resp.status != 200:
                            errs.append(f"{base} {resp.status}: {text[:160]}")
                            continue
                        try:
                            return await resp.json(content_type=None)
                        except Exception as e:
                            errs.append(f"{base} json error: {e} text={text[:160]}")
                except Exception as e:
                    errs.append(f"{base} {type(e).__name__}: {e}")
            last_error = "; ".join(errs)[-500:]
            raise RuntimeError(last_error or f"Binance spot REST failed {path}")

        def spot_symbol_from_id(symbol_id: str) -> str | None:
            sid = str(symbol_id or "").upper()
            if not sid.endswith("USDT") or len(sid) <= 4:
                return None
            base = sid[:-4]
            if base.endswith(("UP", "DOWN", "BULL", "BEAR")) or base in {"USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDP"}:
                return None
            return f"{base}/USDT"

        out: list[dict] = []
        try:
            headers = {"User-Agent": "Mozilla/5.0 liquidity-bot spot-orderflow"}
            connector = aiohttp.TCPConnector(family=socket.AF_INET, ttl_dns_cache=300)
            async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector, trust_env=True) as session:
                raw_tickers = await get_json(session, "/api/v3/ticker/24hr")
                ranked = []
                for t in (raw_tickers or []):
                    try:
                        symbol_id = str(t.get("symbol") or "").upper()
                        sym = spot_symbol_from_id(symbol_id)
                        if not sym:
                            continue
                        qv = self._safe_float(t.get("quoteVolume"), 0.0)
                        if qv < min_quote:
                            prefilter_volume_low += 1
                            continue
                        fut_sym = self._binance_spot_to_futures_symbol(sym, exchange_client)
                        if not fut_sym:
                            prefilter_no_futures += 1
                            continue
                        pct_chg = abs(self._safe_float(t.get("priceChangePercent"), 0.0))
                        ranked.append((qv * (1 + min(pct_chg, 20) / 100.0), sym, symbol_id, fut_sym, t))
                    except Exception:
                        continue
                ranked.sort(reverse=True)
                selected = ranked[:top_n]
                self.last_scan_source = "binance_spot_native_orderflow_rest"
                self.last_total_markets = len(raw_tickers or [])
                self.last_filtered_markets = len(ranked)
                self.last_requested_symbols = top_n
                self.last_selected_symbols = len(selected)
                self.last_orderflow_prefilter_stats = {
                    "binance_spot_total": len(raw_tickers or []),
                    "eligible_after_24h_volume": len(ranked),
                    "prefilter_volume_low": prefilter_volume_low,
                    "prefilter_no_mexc_futures": prefilter_no_futures,
                    "selected_for_orderflow": len(selected),
                    "min_24h_volume_usdt": min_quote,
                    "min_volume_ratio": min_vol_ratio,
                }
                self.hot_symbols = [fut for _, _spot, _sid, fut, _t in selected]

                sem = asyncio.Semaphore(self._concurrency_limit(settings))

                async def scan_one(spot_symbol: str, spot_id: str, futures_symbol: str, ticker: dict) -> dict | None:
                    nonlocal scanned, errors
                    async with sem:
                        scanned += 1
                        try:
                            candles = await get_json(session, "/api/v3/klines", {"symbol": spot_id, "interval": "1m", "limit": 30})
                            if not candles or len(candles) < 10:
                                record(spot_symbol, "spot data unavailable: candles")
                                return None
                            orderbook = await get_json(session, "/api/v3/depth", {"symbol": spot_id, "limit": 20})
                            try:
                                trades = await get_json(session, "/api/v3/aggTrades", {"symbol": spot_id, "limit": 200})
                            except Exception:
                                trades = []
                            closes = [float(c[4]) for c in candles]
                            vols = [float(c[5]) for c in candles]
                            last = closes[-1]
                            prev_5m = closes[-6] if len(closes) >= 6 else closes[-2]
                            spot_move_pct = ((last - prev_5m) / prev_5m * 100.0) if prev_5m else 0.0
                            recent_vol = sum(vols[-3:]) / max(1, min(3, len(vols)))
                            base_vols = vols[:-3] or vols
                            avg_vol = sum(base_vols) / max(1, len(base_vols))
                            vol_ratio = recent_vol / avg_vol if avg_vol else 0.0
                            bids = (orderbook.get("bids") or [])[:20] if isinstance(orderbook, dict) else []
                            asks = (orderbook.get("asks") or [])[:20] if isinstance(orderbook, dict) else []
                            bid_depth = sum(float(p) * float(q) for p, q in bids)
                            ask_depth = sum(float(p) * float(q) for p, q in asks)
                            ob_imb = (bid_depth - ask_depth) / (bid_depth + ask_depth) if (bid_depth + ask_depth) > 0 else 0.0
                            best_bid = float(bids[0][0]) if bids else 0.0
                            best_ask = float(asks[0][0]) if asks else 0.0
                            mid = (best_bid + best_ask) / 2.0 if best_bid and best_ask else last
                            spread_pct = ((best_ask - best_bid) / mid * 100.0) if mid else 999.0
                            buy_vol = sell_vol = 0.0
                            for tr in trades or []:
                                try:
                                    price = float(tr.get("p") or tr.get("price") or 0)
                                    amount = float(tr.get("q") or tr.get("amount") or 0)
                                    notional = price * amount
                                    # Binance aggTrade: m=True means buyer is maker, so aggressor is seller.
                                    buyer_is_maker = bool(tr.get("m"))
                                    if buyer_is_maker:
                                        sell_vol += notional
                                    else:
                                        buy_vol += notional
                                except Exception:
                                    pass
                            total_exec = buy_vol + sell_vol
                            delta = buy_vol - sell_vol
                            delta_ratio = delta / total_exec if total_exec > 0 else 0.0
                            if spread_pct > max_spread:
                                record(spot_symbol, f"spot spread high {spread_pct:.3f}% > {max_spread:.3f}%")
                                return None
                            if vol_ratio < min_vol_ratio:
                                record(spot_symbol, f"volume low {vol_ratio:.2f} < {min_vol_ratio:.2f}")
                                return None
                            side = None
                            if spot_move_pct >= min_trend and delta_ratio > 0 and ob_imb >= min_imb:
                                side = "LONG"
                            elif spot_move_pct <= -min_trend and delta_ratio < 0 and ob_imb <= -min_imb:
                                side = "SHORT"
                            else:
                                record(spot_symbol, f"no spot orderflow alignment move={spot_move_pct:+.3f}% delta={delta_ratio:+.3f} imb={ob_imb:+.3f} vol={vol_ratio:.2f}")
                                return None
                            score = 70 + min(12, abs(spot_move_pct) * 6) + min(12, max(0, vol_ratio - 1) * 4) + min(8, abs(ob_imb) * 40) + min(8, abs(delta_ratio) * 20)
                            details = {
                                "setup": "binance_spot_native_orderflow_rest",
                                "spot_source": "binance_spot_public_rest",
                                "spot_symbol": spot_symbol,
                                "spot_id": spot_id,
                                "spot_move_pct": round(spot_move_pct, 4),
                                "spot_volume_ratio": round(vol_ratio, 4),
                                "spot_orderbook_imbalance": round(ob_imb, 5),
                                "spot_delta_ratio": round(delta_ratio, 5),
                                "spot_delta_usdt": round(delta, 4),
                                "spot_buy_volume_usdt": round(buy_vol, 4),
                                "spot_sell_volume_usdt": round(sell_vol, 4),
                                "spot_bid_depth_usdt": round(bid_depth, 4),
                                "spot_ask_depth_usdt": round(ask_depth, 4),
                                "spot_spread_pct": round(spread_pct, 4),
                                "trigger_trend_pct": round(spot_move_pct, 4),
                                "volume_ratio": round(vol_ratio, 4),
                                "orderbook_imbalance": round(ob_imb, 5),
                                "tp_pct": round(tp_pct, 4),
                                "sl_pct": round(sl_pct, 4),
                                "rr": round(tp_pct / sl_pct, 4) if sl_pct else 2.0,
                                "source": "Binance spot public REST orderflow; MEXC futures execution only",
                            }
                            return self.engine._base(
                                futures_symbol,
                                side,
                                "orderflow_impulse",
                                last,
                                score,
                                spread_pct,
                                bid_depth + ask_depth,
                                0.0,
                                details,
                            )
                        except Exception as e:
                            errors += 1
                            record(spot_symbol, f"spot data unavailable: {type(e).__name__}")
                            log.debug("orderflow spot REST scan failed for %s: %s", spot_symbol, e)
                            return None

                batch_size = max(self._concurrency_limit(settings) * 2, 1)
                for i in range(0, len(selected), batch_size):
                    batch = selected[i:i + batch_size]
                    results = await asyncio.gather(*(scan_one(spot, sid, fut, tick) for _score, spot, sid, fut, tick in batch), return_exceptions=False)
                    out.extend([r for r in results if r])
                    if len(out) >= max_candidates:
                        break
        except Exception as e:
            errors += 1
            self.last_refresh_error = f"Binance spot public REST orderflow failed: {type(e).__name__}: {e}; last={last_error}"[:700]
            log.warning("Binance spot public REST orderflow failed: %s last=%s", e, last_error)
            record("BINANCE", f"binance rest failed: {type(e).__name__}: {last_error or str(e)}")
        self._record_cycle_health(scanned, errors, settings)
        self.last_ai_candidates_count = len(out)
        self.last_orderflow_scan_stats = {
            **getattr(self, "last_orderflow_prefilter_stats", {}),
            "checked_symbols": scanned,
            "errors": errors,
            "candidates": len(out),
        }
        self.last_reject_top_reasons = reject_counts.most_common(8)
        ex = []
        for reason, _count in self.last_reject_top_reasons[:4]:
            ex.extend(reject_examples.get(reason, [])[:2])
        self.last_reject_examples = ex[:8]
        out.sort(key=lambda c: float(c.get("confidence", 0)), reverse=True)
        self.last_signal_summary = f"Binance spot REST candidates={len(out)} scanned={scanned}"
        self.last_reject_reason = "; ".join([f"{r}:{c}" for r, c in self.last_reject_top_reasons[:3]]) or "no Binance spot orderflow setup"
        return out[:max_candidates]



    async def _cascade_hunter_candidates(self, exchange_client, settings: dict, max_candidates: int) -> list[dict]:
        """Binance SPOT cascade-pressure scanner; MEXC futures is execution only.

        Binance futures public endpoints are blocked on some Railway regions (451).
        This mode must therefore scan Binance SPOT only, exactly like
        orderflow_impulse, then execute the selected symbol on MEXC futures.

        Since spot has no liquidation feed/OI/funding, this scanner uses a
        simple cascade-pressure proxy:
          - top-100 Binance spot USDT universe
          - 1m acceleration
          - recent volume expansion
          - aggressive trade delta
          - thin/imbalanced orderbook
        """
        import aiohttp
        import socket
        from urllib.parse import urlencode

        self.engine.configure_from_settings(settings)
        top_n = int(float(settings.get("cascade_hunter_top_coins", 100) or 100))
        min_quote = float(settings.get("cascade_hunter_min_24h_volume_usdt", 5000000.0) or 0)
        # Keep the old setting name so Telegram/settings stay compatible. On
        # spot this means minimum cascade-pressure notional, not real futures
        # liquidation prints.
        # Adaptive pressure filter for Binance SPOT proxy:
        # fixed USD thresholds are too weak for BTC and too strict/noisy for small alts.
        # Use pressure as a share of the last 5 minutes quote volume instead.
        min_pressure_ratio = float(settings.get("cascade_hunter_min_pressure_ratio", 0.035) or 0.035)
        min_pressure_usd = float(settings.get("cascade_hunter_min_liq_usd_1m", 30000.0) or 30000.0)  # legacy/stat only
        min_vol_ratio = float(settings.get("cascade_hunter_min_volume_ratio", 1.8) or 1.8)
        min_move = abs(float(settings.get("cascade_hunter_min_price_move_pct", 0.25) or 0.25))
        max_spread = abs(float(settings.get("cascade_hunter_max_spread_pct", 0.25) or 0.25))
        tp_pct = float(settings.get("cascade_hunter_tp_pct", 2.5) or 2.5)
        sl_pct = float(settings.get("cascade_hunter_sl_pct", 2.0) or 2.0)
        reject_counts = Counter(); reject_examples = defaultdict(list)
        errors = 0; scanned = 0; prefilter_volume_low = 0; prefilter_no_futures = 0; last_error = ""

        def record(symbol: str, reason: str) -> None:
            reason = str(reason or "unknown")[:260]
            bucket = reason.split(":", 1)[0]
            for prefix in (
                "spot spread high", "pressure weak", "volume low", "no acceleration",
                "spot data unavailable", "mexc futures symbol missing", "binance spot unavailable",
            ):
                if reason.startswith(prefix):
                    bucket = prefix
                    break
            reject_counts[bucket] += 1
            if len(reject_examples[bucket]) < 3:
                reject_examples[bucket].append(f"{symbol}->{reason}")

        def spot_symbol_from_id(symbol_id: str) -> str | None:
            sid = str(symbol_id or "").upper()
            if not sid.endswith("USDT") or len(sid) <= 4:
                return None
            base = sid[:-4]
            if base.endswith(("UP", "DOWN", "BULL", "BEAR")) or base in {"USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDP"}:
                return None
            return f"{base}/USDT"

        proxy_enabled = bool(settings.get("proxy_enabled", False)); proxy_url = str(settings.get("proxy_url", "") or "")
        proxy = proxy_url if proxy_enabled and proxy_url else None
        bases = [b.strip().rstrip("/") for b in str(settings.get("binance_spot_base_urls") or os.getenv("BINANCE_SPOT_BASE_URLS", "https://api.binance.com,https://api1.binance.com,https://api2.binance.com,https://api3.binance.com,https://api4.binance.com,https://data-api.binance.vision")).split(",") if b.strip()]
        timeout = aiohttp.ClientTimeout(total=float(settings.get("binance_spot_timeout_sec", os.getenv("BINANCE_SPOT_TIMEOUT_SEC", "7")) or 7))

        async def get_json(session, path: str, params: dict | None = None):
            nonlocal last_error
            params = params or {}; qs = ("?" + urlencode(params)) if params else ""
            errs=[]
            for base in bases:
                try:
                    async with session.get(f"{base}{path}{qs}", proxy=proxy) as resp:
                        text = await resp.text()
                        if resp.status != 200:
                            errs.append(f"{base} {resp.status}: {text[:160]}"); continue
                        try:
                            return await resp.json(content_type=None)
                        except Exception as e:
                            errs.append(f"{base} json error: {e} text={text[:160]}")
                except Exception as e:
                    errs.append(f"{base} {type(e).__name__}: {e}")
            last_error = "; ".join(errs)[-500:]
            raise RuntimeError(last_error or f"Binance spot REST failed {path}")

        out=[]
        try:
            headers={"User-Agent":"Mozilla/5.0 liquidity-bot spot-cascade-hunter"}
            connector=aiohttp.TCPConnector(family=socket.AF_INET, ttl_dns_cache=300)
            async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector, trust_env=True) as session:
                raw_tickers = await get_json(session, "/api/v3/ticker/24hr")
                ranked=[]
                for t in raw_tickers or []:
                    try:
                        spot_id = str(t.get("symbol") or "").upper()
                        spot_symbol = spot_symbol_from_id(spot_id)
                        if not spot_symbol:
                            continue
                        qv = self._safe_float(t.get("quoteVolume"), 0.0)
                        if qv < min_quote:
                            prefilter_volume_low += 1
                            continue
                        futures_symbol = self._binance_spot_to_futures_symbol(spot_symbol, exchange_client)
                        if not futures_symbol:
                            prefilter_no_futures += 1
                            continue
                        pct = abs(self._safe_float(t.get("priceChangePercent"), 0.0))
                        ranked.append((qv*(1+min(pct,30)/100.0), spot_symbol, spot_id, futures_symbol, t))
                    except Exception:
                        continue
                ranked.sort(reverse=True, key=lambda x:x[0]); selected=ranked[:top_n]
                self.last_scan_source="binance_spot_cascade_hunter_rest"
                self.last_total_markets=len(raw_tickers or []); self.last_filtered_markets=len(ranked); self.last_requested_symbols=top_n; self.last_selected_symbols=len(selected)
                self.hot_symbols=[fut for _score,_spot,_sid,fut,_t in selected]
                self.last_cascade_prefilter_stats={
                    "binance_spot_total": len(raw_tickers or []),
                    "eligible_after_24h_volume": len(ranked),
                    "prefilter_volume_low": prefilter_volume_low,
                    "prefilter_no_mexc_futures": prefilter_no_futures,
                    "selected_for_cascade": len(selected),
                    "min_24h_volume_usdt": min_quote,
                    "min_pressure_usd_legacy": min_pressure_usd,
                    "min_pressure_ratio": min_pressure_ratio,
                }
                sem=asyncio.Semaphore(self._concurrency_limit(settings))

                async def scan_one(spot_symbol: str, spot_id: str, futures_symbol: str, ticker: dict):
                    nonlocal scanned, errors
                    async with sem:
                        scanned += 1
                        try:
                            candles=await get_json(session,"/api/v3/klines",{"symbol":spot_id,"interval":"1m","limit":30})
                            if not candles or len(candles)<10:
                                record(spot_symbol,"spot data unavailable: candles"); return None
                            orderbook=await get_json(session,"/api/v3/depth",{"symbol":spot_id,"limit":20})
                            try:
                                trades=await get_json(session,"/api/v3/aggTrades",{"symbol":spot_id,"limit":200})
                            except Exception:
                                trades=[]
                            closes=[float(c[4]) for c in candles]; vols=[float(c[5]) for c in candles]
                            last=closes[-1]; prev=closes[-3] if len(closes)>=3 else closes[-2]
                            price_move=((last-prev)/prev*100.0) if prev else 0.0
                            recent_vol=sum(vols[-3:])/max(1,min(3,len(vols)))
                            base_vols=vols[:-3] or vols
                            avg_vol=sum(base_vols)/max(1,len(base_vols))
                            vol_ratio=recent_vol/avg_vol if avg_vol else 0.0
                            bids=(orderbook.get("bids") or [])[:20] if isinstance(orderbook,dict) else []
                            asks=(orderbook.get("asks") or [])[:20] if isinstance(orderbook,dict) else []
                            bid_depth=sum(float(p)*float(q) for p,q in bids); ask_depth=sum(float(p)*float(q) for p,q in asks)
                            bb=float(bids[0][0]) if bids else 0.0; ba=float(asks[0][0]) if asks else 0.0
                            mid=(bb+ba)/2 if bb and ba else last
                            spread=((ba-bb)/mid*100.0) if mid else 999.0
                            buy=sell=0.0
                            for tr in trades or []:
                                try:
                                    notional=float(tr.get("p") or 0)*float(tr.get("q") or 0)
                                    # Binance aggTrade: m=True -> buyer is maker -> aggressor seller.
                                    if bool(tr.get("m")): sell += notional
                                    else: buy += notional
                                except Exception:
                                    pass
                            delta=buy-sell; total=buy+sell; delta_ratio=delta/total if total else 0.0
                            ob_imb=(bid_depth-ask_depth)/(bid_depth+ask_depth) if (bid_depth+ask_depth)>0 else 0.0
                            # Spot-only proxy for liquidation-cascade pressure.
                            pressure_usd=total*max(0.0, abs(delta_ratio))*max(1.0, min(vol_ratio, 6.0))
                            volume_5m_usd=sum(vols[-5:]) * last if vols and last else 0.0
                            pressure_ratio=pressure_usd / max(volume_5m_usd, 1.0)
                            side=None; liq_side=""
                            if price_move >= min_move and delta_ratio > 0 and ob_imb > -0.20:
                                side="LONG"; liq_side="BUY"
                            elif price_move <= -min_move and delta_ratio < 0 and ob_imb < 0.20:
                                side="SHORT"; liq_side="SELL"
                            if spread>max_spread:
                                record(spot_symbol, f"spot spread high {spread:.3f}% > {max_spread:.3f}%"); return None
                            if pressure_ratio < min_pressure_ratio:
                                record(spot_symbol, f"pressure weak ratio {pressure_ratio:.4f} < {min_pressure_ratio:.4f}"); return None
                            if vol_ratio<min_vol_ratio:
                                record(spot_symbol, f"volume low {vol_ratio:.2f} < {min_vol_ratio:.2f}"); return None
                            if not side:
                                record(spot_symbol, f"no acceleration move={price_move:+.3f} delta={delta_ratio:+.3f} imb={ob_imb:+.3f}"); return None
                            score=70+min(12,pressure_ratio*100.0)+min(10,abs(price_move)*10)+min(8,(vol_ratio-1)*4)+min(8,abs(delta_ratio)*20)+min(4,abs(ob_imb)*20)
                            details={
                                "setup":"binance_spot_cascade_pressure",
                                "spot_source":"binance_spot_public_rest",
                                "spot_symbol":spot_symbol,
                                "spot_id":spot_id,
                                "liq_side":liq_side,
                                "liq_usd_90s":round(pressure_usd,2),
                                "cascade_pressure_usd":round(pressure_usd,2),
                                "real_liquidations":"unavailable_on_spot",
                                "pressure_ratio":round(pressure_ratio,6),
                                "volume_5m_usdt":round(volume_5m_usd,2),
                                "min_pressure_ratio":round(min_pressure_ratio,6),
                                "price_move_2m_pct":round(price_move,4),
                                "volume_ratio":round(vol_ratio,4),
                                "delta_ratio":round(delta_ratio,5),
                                "delta_usdt":round(delta,2),
                                "buy_volume_usdt":round(buy,2),
                                "sell_volume_usdt":round(sell,2),
                                "bid_depth_usdt":round(bid_depth,2),
                                "ask_depth_usdt":round(ask_depth,2),
                                "orderbook_imbalance":round(ob_imb,5),
                                "spread_pct":round(spread,4),
                                "open_interest":0.0,
                                "funding_rate":0.0,
                                "tp_pct":round(tp_pct,4),
                                "sl_pct":round(sl_pct,4),
                                "rr":round(tp_pct/sl_pct,4) if sl_pct else 2.0,
                                "source":"Binance spot public REST cascade-pressure; MEXC futures execution only",
                            }
                            return self.engine._base(futures_symbol, side, "cascade_hunter", last, score, spread, bid_depth+ask_depth, 0.0, details)
                        except Exception as e:
                            errors += 1; record(spot_symbol, f"spot data unavailable: {type(e).__name__}"); return None
                batch=max(self._concurrency_limit(settings)*2,1)
                for i in range(0,len(selected),batch):
                    res=await asyncio.gather(*(scan_one(spot,sid,fut,t) for _score,spot,sid,fut,t in selected[i:i+batch]), return_exceptions=False)
                    out.extend([r for r in res if r])
                    if len(out)>=max_candidates: break
        except Exception as e:
            errors += 1; self.last_refresh_error=f"cascade_hunter Binance spot failed: {type(e).__name__}: {e}; last={last_error}"[:700]
            record("BINANCE", f"binance spot unavailable: {type(e).__name__}: {last_error or str(e)}")
        self._record_cycle_health(scanned, errors, settings)
        self.last_ai_candidates_count=len(out)
        self.last_cascade_scan_stats={
            **getattr(self,"last_cascade_prefilter_stats",{}),
            "checked_symbols":scanned,"errors":errors,"candidates":len(out),"top_coins":top_n,
            "min_liq_usd_1m_legacy":min_pressure_usd,"min_pressure_ratio":min_pressure_ratio,"min_24h_volume_usdt":min_quote,
            "source":"binance_spot_public_rest",
        }
        self.last_reject_top_reasons=reject_counts.most_common(8)
        ex=[]
        for reason,_ in self.last_reject_top_reasons[:4]: ex.extend(reject_examples.get(reason,[])[:2])
        self.last_reject_examples=ex[:8]
        self.last_reject_reason="; ".join([f"{r}:{c}" for r,c in self.last_reject_top_reasons[:3]]) or "no cascade hunter setup"
        self.last_signal_summary=f"Binance spot cascade candidates={len(out)} scanned={scanned}"
        out.sort(key=lambda c: float(c.get("confidence",0)), reverse=True)
        return out[:max_candidates]


    async def _knife_reversal_candidates(self, exchange_client, settings: dict, max_candidates: int) -> list[dict]:
        """Binance spot liquidation-wick scanner; MEXC futures is execution only.
        LONG only: strong downside wick/flush, >=50% reclaim, volume spike, positive delta, bid imbalance.
        """
        import aiohttp, socket
        from urllib.parse import urlencode
        top_n = int(float(settings.get("knife_reversal_top_coins", settings.get("multi_strategy_top_coins", 100)) or 100))
        min_quote = float(settings.get("knife_reversal_min_24h_volume_usdt", 5000000.0) or 0)
        min_wick_pct = float(settings.get("knife_reversal_min_wick_pct", 1.20) or 1.20)
        min_reclaim_pct = float(settings.get("knife_reversal_min_reclaim_pct", 50.0) or 50.0)
        min_vol_ratio = float(settings.get("knife_reversal_min_volume_ratio", 2.0) or 2.0)
        min_delta = float(settings.get("knife_reversal_min_delta_ratio", 0.05) or 0.05)
        min_imb = float(settings.get("knife_reversal_min_imbalance", 0.08) or 0.08)
        max_spread = float(settings.get("knife_reversal_max_spread_pct", 0.25) or 0.25)
        tp_pct = float(settings.get("knife_reversal_tp_pct", 5.0) or 5.0)
        sl_buffer = float(settings.get("knife_reversal_wick_sl_buffer_pct", 0.20) or 0.20)
        reject_counts = Counter(); reject_examples = defaultdict(list)
        scanned = errors = 0; out = []; last_error = ""
        def record(sym, reason):
            reason = str(reason or "unknown")[:90]
            bucket = reason.split(":",1)[0]
            for pref in ("volume low", "no mexc futures", "wick low", "reclaim low", "delta weak", "book weak", "spread high"):
                if reason.startswith(pref): bucket = pref; break
            reject_counts[bucket]+=1
            if len(reject_examples[bucket])<3: reject_examples[bucket].append(f"{sym}->{reason}")
        def to_futures(spot_symbol: str) -> str | None:
            base = spot_symbol[:-4] if spot_symbol.endswith("USDT") else spot_symbol.split("/")[0]
            for s in (f"{base}/USDT:USDT", f"{base}/USDT"):
                if not getattr(exchange_client, "markets", None) or s in exchange_client.markets:
                    return s
            return None
        async def get_json(session, path, params=None):
            nonlocal last_error
            url = "https://api.binance.com" + path
            if params: url += "?" + urlencode(params)
            async with session.get(url) as r:
                txt = await r.text(); last_error = txt[:220]
                if r.status >= 400: raise RuntimeError(f"binance {r.status}: {last_error}")
                return await r.json(content_type=None)
        try:
            if hasattr(exchange_client, "load_markets"):
                try: await exchange_client.load_markets()
                except Exception: pass
            timeout = aiohttp.ClientTimeout(total=float(settings.get("binance_spot_timeout_sec", "7") or 7))
            headers={"User-Agent":"Mozilla/5.0 liquidity-bot"}
            connector=aiohttp.TCPConnector(family=socket.AF_INET, ttl_dns_cache=300)
            async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector, trust_env=True) as session:
                tickers = await get_json(session, "/api/v3/ticker/24hr")
                ranked=[]
                for t in tickers or []:
                    sym=str(t.get("symbol") or "")
                    if not sym.endswith("USDT") or any(x in sym for x in ("UPUSDT","DOWNUSDT","BULLUSDT","BEARUSDT")): continue
                    try: qv=float(t.get("quoteVolume") or 0)
                    except Exception: qv=0
                    if qv < min_quote:
                        record(sym,"volume low"); continue
                    fut=to_futures(sym)
                    if not fut:
                        record(sym,"no mexc futures"); continue
                    ranked.append((qv,sym,fut))
                ranked.sort(reverse=True, key=lambda x:x[0]); selected=ranked[:top_n]
                self.hot_symbols=[f for _,_,f in selected]
                sem=asyncio.Semaphore(self._concurrency_limit(settings))
                async def scan_one(spot_id, futures_symbol):
                    nonlocal scanned, errors
                    async with sem:
                        scanned += 1
                        try:
                            candles = await get_json(session, "/api/v3/klines", {"symbol":spot_id,"interval":"1m","limit":40})
                            if not candles or len(candles)<12: record(spot_id,"no candles"); return None
                            ob = await get_json(session, "/api/v3/depth", {"symbol":spot_id,"limit":20})
                            try: trades=await get_json(session,"/api/v3/aggTrades",{"symbol":spot_id,"limit":200})
                            except Exception: trades=[]
                            lows=[float(c[3]) for c in candles]; highs=[float(c[2]) for c in candles]; closes=[float(c[4]) for c in candles]; opens=[float(c[1]) for c in candles]; vols=[float(c[5]) for c in candles]
                            # Use the last closed candle if possible to avoid entering before reclaim is real.
                            idx=-2 if len(candles)>=3 else -1
                            o,h,l,c = opens[idx], highs[idx], lows[idx], closes[idx]
                            if l <= 0: return None
                            lower_wick = max(0.0, min(o,c)-l)
                            wick_pct = lower_wick / l * 100.0
                            reclaim_pct = ((c-l)/lower_wick*100.0) if lower_wick>0 else 0.0
                            recent_vol = sum(vols[idx-2:idx+1]) / 3 if len(vols)>=5 else vols[idx]
                            base_vols = vols[:-5] or vols
                            avg_vol = sum(base_vols)/max(1,len(base_vols))
                            vol_ratio = recent_vol/avg_vol if avg_vol else 0.0
                            bids=(ob.get("bids") or [])[:20] if isinstance(ob,dict) else []
                            asks=(ob.get("asks") or [])[:20] if isinstance(ob,dict) else []
                            bid_depth=sum(float(p)*float(q) for p,q in bids); ask_depth=sum(float(p)*float(q) for p,q in asks)
                            imb=(bid_depth-ask_depth)/(bid_depth+ask_depth) if bid_depth+ask_depth>0 else 0.0
                            bb=float(bids[0][0]) if bids else 0.0; ba=float(asks[0][0]) if asks else 0.0
                            mid=(bb+ba)/2 if bb and ba else c
                            spread=((ba-bb)/mid*100.0) if mid else 999
                            buy=sell=0.0
                            for tr in trades or []:
                                try:
                                    notional=float(tr.get("p") or 0)*float(tr.get("q") or 0)
                                    if bool(tr.get("m")): sell+=notional
                                    else: buy+=notional
                                except Exception: pass
                            total=buy+sell; delta=buy-sell; dr=delta/total if total>0 else 0.0
                            if spread>max_spread: record(spot_id,f"spread high {spread:.3f}"); return None
                            if wick_pct<min_wick_pct: record(spot_id,f"wick low {wick_pct:.2f}"); return None
                            if reclaim_pct<min_reclaim_pct: record(spot_id,f"reclaim low {reclaim_pct:.1f}"); return None
                            if vol_ratio<min_vol_ratio: record(spot_id,f"volume low {vol_ratio:.2f}"); return None
                            if dr<min_delta: record(spot_id,f"delta weak {dr:+.3f}"); return None
                            if imb<min_imb: record(spot_id,f"book weak {imb:+.3f}"); return None
                            score=72+min(10,wick_pct*2)+min(10,(reclaim_pct-50)/5)+min(8,(vol_ratio-1)*3)+min(8,imb*40)+min(7,dr*25)
                            stop_price = l * (1 - sl_buffer/100.0)
                            details={
                                "setup":"knife_reversal_wick_reclaim", "spot_source":"binance_spot_public_rest", "spot_symbol":spot_id,
                                "wick_low":round(l,8), "wick_pct":round(wick_pct,4), "reclaim_pct":round(reclaim_pct,2),
                                "spot_volume_ratio":round(vol_ratio,4), "spot_delta_ratio":round(dr,5), "spot_delta_usdt":round(delta,2),
                                "spot_buy_volume_usdt":round(buy,2), "spot_sell_volume_usdt":round(sell,2),
                                "spot_orderbook_imbalance":round(imb,5), "spot_bid_depth_usdt":round(bid_depth,2), "spot_ask_depth_usdt":round(ask_depth,2),
                                "spot_spread_pct":round(spread,4), "tp_pct":round(tp_pct,4), "sl_buffer_pct":round(sl_buffer,4),
                                "custom_stop_price":round(stop_price,8), "source":"Binance spot wick reclaim; MEXC futures execution only",
                            }
                            return self.engine._base(futures_symbol,"LONG","knife_reversal",c,score,spread,bid_depth+ask_depth,0.0,details)
                        except Exception as e:
                            errors += 1; record(spot_id, f"spot data unavailable: {type(e).__name__}"); return None
                batch=max(self._concurrency_limit(settings)*2,1)
                for i in range(0,len(selected),batch):
                    results=await asyncio.gather(*(scan_one(sym,fut) for _qv,sym,fut in selected[i:i+batch]), return_exceptions=False)
                    out.extend([r for r in results if r])
                    if len(out)>=max_candidates: break
        except Exception as e:
            errors += 1; self.last_refresh_error=f"knife_reversal Binance REST failed: {type(e).__name__}: {e}; last={last_error}"[:700]
        self._record_cycle_health(scanned, errors, settings)
        self.last_knife_scan_stats={"checked_symbols":scanned,"errors":errors,"candidates":len(out),"top_coins":top_n,"min_24h_volume_usdt":min_quote}
        self.last_reject_top_reasons=reject_counts.most_common(8)
        self.last_reject_reason="; ".join([f"{r}:{c}" for r,c in self.last_reject_top_reasons[:3]]) or "no knife reversal setup"
        out.sort(key=lambda c: float(c.get("confidence",0)), reverse=True)
        return out[:max_candidates]

    async def _multi_strategy_candidates(self, exchange_client, settings: dict, max_candidates: int) -> list[dict]:
        st = dict(settings)
        st["orderflow_impulse_top_coins"] = int(float(settings.get("multi_strategy_top_coins", settings.get("orderflow_impulse_top_coins", 100)) or 100))
        st["knife_reversal_top_coins"] = int(float(settings.get("multi_strategy_top_coins", settings.get("knife_reversal_top_coins", 100)) or 100))
        each = max(max_candidates, 3)
        results = await asyncio.gather(
            self._orderflow_impulse_candidates(exchange_client, st, each),
            self._knife_reversal_candidates(exchange_client, st, each),
            return_exceptions=True,
        )
        out=[]
        for r in results:
            if isinstance(r, list): out.extend(r)
        seen=set(); uniq=[]
        for c in sorted(out, key=lambda x: float(x.get("confidence",0)), reverse=True):
            sym=str(c.get("symbol"));
            if sym in seen: continue
            seen.add(sym); uniq.append(c)
        self.last_multi_strategy_stats={"candidates":len(uniq),"orderflow":len(results[0]) if isinstance(results[0],list) else 0,"knife":len(results[1]) if isinstance(results[1],list) else 0}
        return uniq[:max_candidates]

    async def candidates(self, exchange_client, settings: dict) -> list[dict]:
        preferred_strategy = str(settings.get("effective_strategy_mode") or settings.get("strategy_mode", "hybrid")).lower()
        if preferred_strategy == "orderflow_impulse":
            base_max = int(settings.get("orderflow_impulse_max_candidates", os.getenv("ORDERFLOW_IMPULSE_MAX_CANDIDATES", "3")) or 3)
            return await self._orderflow_impulse_candidates(exchange_client, settings, base_max)
        if preferred_strategy == "cascade_hunter":
            base_max = int(settings.get("cascade_hunter_max_candidates", os.getenv("CASCADE_HUNTER_MAX_CANDIDATES", "3")) or 3)
            return await self._cascade_hunter_candidates(exchange_client, settings, base_max)
        if preferred_strategy == "knife_reversal":
            base_max = int(settings.get("knife_reversal_max_candidates", os.getenv("KNIFE_REVERSAL_MAX_CANDIDATES", "3")) or 3)
            return await self._knife_reversal_candidates(exchange_client, settings, base_max)
        if preferred_strategy == "multi_strategy":
            base_max = int(settings.get("multi_strategy_max_candidates", os.getenv("MULTI_STRATEGY_MAX_CANDIDATES", "3")) or 3)
            return await self._multi_strategy_candidates(exchange_client, settings, base_max)
        if preferred_strategy in {"quick_bounce", "impulse_dump"}:
            prefix = "quick_bounce" if preferred_strategy == "quick_bounce" else "impulse_dump"
            env_prefix = "QUICK_BOUNCE" if preferred_strategy == "quick_bounce" else "IMPULSE_DUMP"
            tf = str(settings.get(f"{prefix}_confirm_timeframe", settings.get(f"{prefix}_timeframe", os.getenv(f"{env_prefix}_CONFIRM_TIMEFRAME", "15m"))) or "15m")
            limit = int(settings.get(f"{prefix}_ohlcv_limit", os.getenv(f"{env_prefix}_OHLCV_LIMIT", "80")) or 80)
        else:
            tf = os.getenv("SIGNAL_OHLCV_TIMEFRAME", "1m")
            limit = int(os.getenv("SIGNAL_OHLCV_LIMIT", "60"))
        regime = str(settings.get("market_regime") or self.last_regime.get("regime", "LOW_VOLATILITY"))
        base_max = int(os.getenv("SIGNAL_MAX_CANDIDATES_PER_CYCLE", "8"))
        if preferred_strategy in {"quick_bounce", "impulse_dump"}:
            prefix = "quick_bounce" if preferred_strategy == "quick_bounce" else "impulse_dump"
            env_prefix = "QUICK_BOUNCE" if preferred_strategy == "quick_bounce" else "IMPULSE_DUMP"
            base_max = int(settings.get(f"{prefix}_max_candidates", os.getenv(f"{env_prefix}_MAX_CANDIDATES", "5")) or 5)
        if bool(settings.get("regime_adaptation", True)) and preferred_strategy not in {"quick_bounce", "impulse_dump"}:
            if regime == "HIGH_VOLATILITY":
                max_candidates = max(2, int(base_max * 0.75))
            elif regime == "CHOPPY":
                max_candidates = min(12, base_max + 2)
            else:
                max_candidates = base_max
        else:
            max_candidates = base_max

        self.engine.configure_from_settings(settings)
        self.last_concurrency = self._concurrency_limit(settings)
        sem = asyncio.Semaphore(self.last_concurrency)
        errors = 0
        scanned = 0
        reject_counts = Counter()
        reject_examples = defaultdict(list)
        market_context = {}
        if preferred_strategy in {"quick_bounce", "impulse_dump"} and bool(settings.get(("quick_bounce" if preferred_strategy == "quick_bounce" else "impulse_dump") + "_btc_filter_enabled", True)):
            try:
                prefix = "quick_bounce" if preferred_strategy == "quick_bounce" else "impulse_dump"
                env_prefix = "QUICK_BOUNCE" if preferred_strategy == "quick_bounce" else "IMPULSE_DUMP"
                btc_tf = str(settings.get(f"{prefix}_anomaly_timeframe", os.getenv(f"{env_prefix}_ANOMALY_TIMEFRAME", "1h")) or "1h")
                btc_candles = await exchange_client.fetch_ohlcv("BTC/USDT:USDT", timeframe=btc_tf, limit=3)
                if btc_candles and len(btc_candles) >= 2:
                    market_context["btc_change_1h_pct"] = (float(btc_candles[-1][4]) - float(btc_candles[-2][4])) / max(float(btc_candles[-2][4]), 1e-12) * 100.0
            except Exception as e:
                log.debug("%s BTC filter unavailable: %s", preferred_strategy, e)

        def _record_reject(symbol: str, reason: str) -> None:
            reason = str(reason or "unknown").strip()[:90] or "unknown"
            # Keep reason buckets readable; exact examples stay below.
            bucket = reason.split(":", 1)[0]
            for prefix in ("no sweep", "no reclaim", "no BOS/displacement", "no retest", "RR low", "spread high", "retest wick low", "zone quality low", "MTF weak", "clean path absent", "confidence", "AI WAIT"):
                if reason.startswith(prefix):
                    bucket = prefix
                    break
            if reason.startswith("liquidity_retest filters"):
                bucket = "liquidity_retest filters"
            reject_counts[bucket] += 1
            if len(reject_examples[bucket]) < 3:
                reject_examples[bucket].append(f"{symbol}->{reason}")

        async def scan_one(symbol: str) -> dict | None:
            nonlocal errors, scanned
            async with sem:
                try:
                    scanned += 1
                    candles = await exchange_client.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
                    if not candles:
                        _record_reject(symbol, "no candles")
                        return None
                    mtf_candles = None
                    ticker = None
                    if preferred_strategy in {"quick_bounce", "impulse_dump"}:
                        try:
                            prefix = "quick_bounce" if preferred_strategy == "quick_bounce" else "impulse_dump"
                            env_prefix = "QUICK_BOUNCE" if preferred_strategy == "quick_bounce" else "IMPULSE_DUMP"
                            anomaly_tf = str(settings.get(f"{prefix}_anomaly_timeframe", os.getenv(f"{env_prefix}_ANOMALY_TIMEFRAME", "1h")) or "1h")
                            anomaly_limit = int(settings.get(f"{prefix}_anomaly_ohlcv_limit", os.getenv(f"{env_prefix}_ANOMALY_OHLCV_LIMIT", "48")) or 48)
                            mtf_candles = {"1h": await exchange_client.fetch_ohlcv(symbol, timeframe=anomaly_tf, limit=anomaly_limit)}
                        except Exception as e:
                            errors += 1
                            _record_reject(symbol, "1h candles error")
                            log.debug("%s 1h candles unavailable for %s: %s", preferred_strategy, symbol, e)
                            return None
                        try:
                            ticker = await exchange_client.fetch_ticker(symbol)
                        except Exception as e:
                            _record_reject(symbol, "ticker unavailable")
                            log.debug("%s ticker unavailable for %s: %s", preferred_strategy, symbol, e)
                    try:
                        orderbook = await exchange_client.fetch_order_book(symbol, limit=20)
                    except Exception as e:
                        errors += 1
                        _record_reject(symbol, f"orderbook error")
                        log.debug("orderbook unavailable for %s: %s", symbol, e)
                        return None
                    candidate = self.engine.analyze_symbol(
                        symbol=symbol,
                        candles=candles,
                        ticker=ticker,
                        orderbook=orderbook,
                        preferred_strategy=preferred_strategy,
                        mtf_candles=mtf_candles,
                        market_context=market_context,
                    )
                    if candidate:
                        candidate["market_regime"] = regime
                        candidate["effective_strategy_mode"] = preferred_strategy
                    else:
                        _record_reject(symbol, getattr(self.engine, "last_reject_reason", "no candidate"))
                    return candidate
                except Exception as e:
                    errors += 1
                    _record_reject(symbol, "scan exception")
                    log.debug("candidate scan failed for %s: %s", symbol, e)
                    return None

        # Scan concurrently, but bounded. We stop scheduling in chunks once enough
        # candidates are found, so top-200 does not always hammer every symbol.
        out: list[dict] = []
        symbols = list(self.hot_symbols)
        batch_size = max(self.last_concurrency * 2, self.last_concurrency)
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            results = await asyncio.gather(*(scan_one(sym) for sym in batch), return_exceptions=False)
            out.extend([r for r in results if r])
            if len(out) >= max_candidates and preferred_strategy not in {"quick_bounce", "impulse_dump"}:
                break

        self._record_cycle_health(scanned, errors, settings)
        self.last_ai_candidates_count = len(out)
        self.last_orderflow_scan_stats = {
            **getattr(self, "last_orderflow_prefilter_stats", {}),
            "checked_symbols": scanned,
            "errors": errors,
            "candidates": len(out),
        }
        self.last_reject_top_reasons = reject_counts.most_common(8)
        ex = []
        for reason, _count in self.last_reject_top_reasons[:4]:
            ex.extend(reject_examples.get(reason, [])[:2])
        self.last_reject_examples = ex[:8]
        out.sort(key=lambda c: float(c.get("confidence", 0)), reverse=True)
        return out[:max_candidates]

