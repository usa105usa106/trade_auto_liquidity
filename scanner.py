import time
import os
import logging
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

    async def refresh_symbols(self, exchange_client, settings: dict, ws_supervisor=None):
        self.last_refresh = time.time()
        min_quote_volume = float(os.getenv("SIGNAL_MIN_24H_QUOTE_VOLUME", "5000000"))
        try:
            tickers = await ws_supervisor.tickers(max_age_sec=30) if ws_supervisor and ws_supervisor.healthy() else await exchange_client.fetch_tickers()
            regime_info = RegimeEngine().detect_from_tickers(tickers) if bool(settings.get("regime_adaptation", True)) else {"regime": "LOW_VOLATILITY", "source": "disabled"}
            regime_info["source"] = "tickers"
            self.last_regime = regime_info

            items = []
            for sym, t in tickers.items():
                if "USDT" not in sym:
                    continue
                quote_volume = float(t.get("quoteVolume") or 0)
                if quote_volume < min_quote_volume:
                    continue
                pct_change = abs(float(t.get("percentage") or 0))
                try:
                    sym = exchange_client.normalize_symbol(sym)
                except Exception:
                    continue
                # Adaptive score: liquidity first, but hot movers receive priority. In choppy
                # markets we cap the volatility bonus so random pumps do not dominate the list.
                vol_bonus = min(pct_change, 30) / 100
                if regime_info.get("regime") == "CHOPPY":
                    vol_bonus = min(pct_change, 12) / 100
                score = quote_volume * (1 + vol_bonus)
                items.append((score, sym))
            items.sort(reverse=True)
            mode = str(settings.get("universe_mode", "adaptive"))
            if mode.startswith("top-"):
                n = int(mode.replace("top-", ""))
            else:
                n = self._adaptive_symbol_count(settings, regime_info, len(items))
            self.hot_symbols = [s for _, s in items[:max(10, min(n, 300))]] or self.hot_symbols
        except Exception as e:
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

        out = []
        for symbol in list(self.hot_symbols):
            try:
                candles = await exchange_client.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
                if not candles:
                    continue
                try:
                    orderbook = await exchange_client.fetch_order_book(symbol, limit=20)
                except Exception as e:
                    log.debug("orderbook unavailable for %s: %s", symbol, e)
                    continue
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
                    out.append(candidate)
            except Exception as e:
                log.debug("candidate scan failed for %s: %s", symbol, e)
                continue

            if len(out) >= max_candidates:
                break

        out.sort(key=lambda c: float(c.get("confidence", 0)), reverse=True)
        return out[:max_candidates]
