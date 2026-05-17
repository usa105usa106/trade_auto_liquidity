import time
import os
import logging
import asyncio
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
        self.last_signal_summary = "-"
        self.last_reject_reason = "-"
        self.last_concurrency = int(os.getenv("SCANNER_CONCURRENCY", "5"))
        self.last_cycle_errors = 0
        self.last_cycle_scanned = 0
        self.last_slowdown_sec = 0
        self.error_streak = 0
        self.engine = SignalEngine(
            min_confidence=float(os.getenv("SIGNAL_MIN_CONFIDENCE", "70")),
            volume_spike_mult=float(os.getenv("SIGNAL_VOLUME_SPIKE_MULT", "1.8")),
            breakout_lookback=int(os.getenv("SIGNAL_BREAKOUT_LOOKBACK", "20")),
            momentum_threshold_pct=float(os.getenv("SIGNAL_MOMENTUM_THRESHOLD_PCT", "0.18")),
            max_candidates_per_cycle=int(os.getenv("SIGNAL_MAX_CANDIDATES_PER_CYCLE", "8")),
        )

    def _adaptive_symbol_count(self, settings: dict, regime_info: dict, market_items_count: int) -> int:
        base = int(settings.get("max_symbols", 100) or 100)
        regime = str(regime_info.get("regime", "LOW_VOLATILITY"))
        volatility = float(regime_info.get("volatility", 0.0) or 0.0)
        breadth = int(regime_info.get("breadth_count", market_items_count) or market_items_count)

        if regime == "HIGH_VOLATILITY":
            n = int(base * 0.60)  # trade fewer names when everything is moving wildly
        elif regime == "TRENDING":
            n = int(base * 0.80)  # focus on leaders
        elif regime == "CHOPPY":
            n = int(base * 1.20)  # scan wider for clean sweeps/reversions
        else:
            n = base

        if volatility >= 2.0:
            n = int(n * 0.75)
        elif breadth >= 150 and regime in {"CHOPPY", "LOW_VOLATILITY"}:
            n = int(n * 1.10)

        return max(10, min(n, 300, max(10, market_items_count)))

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
            tickers = await exchange_client.fetch_tickers()
            if tickers:
                if ws_error:
                    self.last_refresh_error = ws_error
                return tickers, "mexc_futures_rest"
            raise RuntimeError("MEXC futures REST returned empty ticker set")
        except Exception as e:
            msg = f"MEXC futures scan failed: {e}"
            if ws_error:
                msg = f"{ws_error}; {msg}"
            raise RuntimeError(msg)

    def _universe_target_count(self, settings: dict, regime_info: dict, market_items_count: int) -> int:
        mode = str(settings.get("universe_mode", "adaptive"))
        if mode.startswith("top-"):
            try:
                return int(mode.replace("top-", ""))
            except Exception:
                return 100
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
                quote_volume = float(t.get("quoteVolume") or t.get("quoteVolume24h") or t.get("baseVolume") or 0)
                if quote_volume < min_quote_volume:
                    continue
                pct_change = abs(float(t.get("percentage") or t.get("change") or 0))
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

    async def candidates(self, exchange_client, settings: dict) -> list[dict]:
        tf = os.getenv("SIGNAL_OHLCV_TIMEFRAME", "1m")
        limit = int(os.getenv("SIGNAL_OHLCV_LIMIT", "60"))
        preferred_strategy = str(settings.get("effective_strategy_mode") or settings.get("strategy_mode", "hybrid")).lower()
        regime = str(settings.get("market_regime") or self.last_regime.get("regime", "LOW_VOLATILITY"))
        base_max = int(os.getenv("SIGNAL_MAX_CANDIDATES_PER_CYCLE", "8"))
        if bool(settings.get("regime_adaptation", True)):
            if regime == "HIGH_VOLATILITY":
                max_candidates = max(2, int(base_max * 0.75))
            elif regime == "CHOPPY":
                max_candidates = min(12, base_max + 2)
            else:
                max_candidates = base_max
        else:
            max_candidates = base_max

        self.last_concurrency = self._concurrency_limit(settings)
        sem = asyncio.Semaphore(self.last_concurrency)
        errors = 0
        scanned = 0

        async def scan_one(symbol: str) -> dict | None:
            nonlocal errors, scanned
            async with sem:
                try:
                    scanned += 1
                    candles = await exchange_client.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
                    if not candles:
                        return None
                    try:
                        orderbook = await exchange_client.fetch_order_book(symbol, limit=20)
                    except Exception as e:
                        errors += 1
                        log.debug("orderbook unavailable for %s: %s", symbol, e)
                        return None
                    candidate = self.engine.analyze_symbol(
                        symbol=symbol,
                        candles=candles,
                        ticker=None,
                        orderbook=orderbook,
                        preferred_strategy=preferred_strategy,
                    )
                    if candidate:
                        candidate["market_regime"] = regime
                        candidate["effective_strategy_mode"] = preferred_strategy
                    return candidate
                except Exception as e:
                    errors += 1
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
            if len(out) >= max_candidates:
                break

        self._record_cycle_health(scanned, errors, settings)
        out.sort(key=lambda c: float(c.get("confidence", 0)), reverse=True)
        return out[:max_candidates]

